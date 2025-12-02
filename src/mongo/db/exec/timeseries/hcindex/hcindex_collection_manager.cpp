/*
 * Copyright (C) 2024-present MongoDB, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the Server Side Public License, version 1,
 * as published by MongoDB, Inc.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * Server Side Public License for more details.
 *
 * You should have received a copy of the Server Side Public License
 * along with this program. If not, see
 * <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 * As a special exception, the copyright holders give you permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and upon the terms of
 * the Server Side Public License, version 1, as published by MongoDB, Inc.
 */

#include "mongo/db/exec/timeseries/hcindex/hcindex_collection_manager.h"

#include "mongo/db/namespace_string.h"
#include "mongo/util/str.h"
#include "mongo/logv2/log.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kQuery

namespace mongo::timeseries::hcindex {

NamespaceString HCIndexCollectionManager::getSymbolOperationsNamespace(const DatabaseName& dbName, const UUID& collectionUUID) {
    std::string collName = "hcindex.ops.symbols." + collectionUUID.toString();
    std::string fullNs = str::stream() << dbName.toStringForErrorMsg() << "." << collName;
    return NamespaceString::createNamespaceString_forTest(fullNs);
}

NamespaceString HCIndexCollectionManager::getAttributeOperationsNamespace(const DatabaseName& dbName, const UUID& collectionUUID) {
    std::string collName = "hcindex.ops.attributes." + collectionUUID.toString();
    std::string fullNs = str::stream() << dbName.toStringForErrorMsg() << "." << collName;
    return NamespaceString::createNamespaceString_forTest(fullNs);
}

HCIndexCollectionManager::HCIndexCollectionManager(OperationContext* opCtx,
                                                   const DatabaseName& dbName,
                                                   const UUID& collectionUUID,
                                                   DictionaryGranularity granularity)
    : dbName(dbName),
      collectionUUID(collectionUUID),
      granularity(granularity),
      writer(std::make_unique<HCIndexWriter>(collectionUUID, dbName)),
      reader(std::make_unique<HCIndexReader>(dbName, collectionUUID)),
      symbolDictionary(std::make_unique<TemporalSymbolDictionary>(collectionUUID, granularity, writer.get(), reader.get())),
      attributeTable(std::make_unique<TemporalAttributeTable>(collectionUUID, granularity, symbolDictionary.get(), writer.get(), reader.get())) {}

Status HCIndexCollectionManager::initializeForRead(OperationContext* opCtx) {
    // If already initialized, return early
    if (initializedForRead) {
        return Status::OK();
    }

    if (!reader) {
        return Status(ErrorCodes::InternalError, "HCIndex reader not initialized");
    }

    // Initialize the reader by acquiring collections for symbol and attribute operations
    // This uses lock-free acquisitions to avoid lock cycles during query execution
    auto readerInitStatus = reader->initializeCollections(opCtx);
    if (!readerInitStatus.isOK()) {
        LOGV2_WARNING(9999996,
                      "Failed to initialize HCIndex reader collections",
                      "error"_attr = readerInitStatus);
        // Continue anyway - the reader will try to acquire collections on-demand if needed
    }

    initializedForRead = true;
    return Status::OK();
}

void HCIndexCollectionManager::close() {
    // Reset the reader to release acquired collections
    if (reader) {
        reader->close();
    }
    initializedForRead = false;
}

StatusWith<int64_t> HCIndexCollectionManager::encodeMetadata(OperationContext* opCtx,
                                                             const BSONObj& metadata,
                                                             const Timestamp& timestamp) {
    if (!symbolDictionary || !attributeTable) {
        return Status(ErrorCodes::InternalError, "HCIndex structures not initialized");
    }

    // Insert the metadata row into the attribute table
    auto rowIdStatus = attributeTable->insertRow(opCtx, metadata, timestamp);
    if (!rowIdStatus.isOK()) {
        return rowIdStatus.getStatus();
    }

    // Extract the result - we get both the rowId and whether it's a new row
    auto insertResult = rowIdStatus.getValue();
    int64_t rowId = insertResult.rowId;
    bool isNewRow = insertResult.isNewRow;

    // TODO: Use isNewRow flag to track which rows are new so we can build
    // appropriate ADD operations when flushPendingOperations is called.
    // For now, we just return the rowId. The caller can use this information
    // to decide whether to add operations to the writer.
    (void)isNewRow;  // Suppress unused variable warning

    return rowId;
}

StatusWith<BSONObj> HCIndexCollectionManager::decodeMetadata(OperationContext* opCtx,
                                                             int64_t rowId,
                                                             const Timestamp& timestamp) {
    if (!symbolDictionary || !attributeTable) {
        return Status(ErrorCodes::InternalError, "HCIndex structures not initialized");
    }

    // Get the attribute table for this timestamp
    auto tableResult = attributeTable->getTableForTimestamp(timestamp);
    if (!tableResult.isOK()) {
        // Table doesn't exist in memory. Try to create/reconstruct it from disk.
        auto createResult = attributeTable->getOrCreateTableForTimestamp(opCtx, timestamp);
        if (!createResult.isOK()) {
            return createResult.getStatus();
        }
        tableResult = createResult;
    }
    auto* table = tableResult.getValue();

    // Get the row from the attribute table (returns vector of symbol indices)
    auto rowIndices = attributeTable->getRow(rowId, timestamp);
    if (!rowIndices) {
        return Status(ErrorCodes::NoSuchKey,
                      str::stream() << "Row not found for rowId: " << rowId);
    }

    // Get the schema from the attribute table
    const auto& schema = table->getSchema();

    // Decode each symbol index using the symbol dictionary
    BSONObjBuilder builder;
    const auto& indices = rowIndices.value();

    for (size_t i = 0; i < indices.size() && i < schema.size(); ++i) {
        uint32_t symbolIndex = indices[i];

        // Skip missing values (0)
        if (symbolIndex == 0) {
            continue;
        }

        // Decode the symbol
        auto symbolOpt = symbolDictionary->decodeSymbol(symbolIndex, timestamp);
        if (!symbolOpt) {
            return Status(ErrorCodes::BadValue,
                          str::stream() << "Invalid symbol index: " << symbolIndex);
        }

        // Add to BSON with the correct field name from schema
        builder.append(schema[i], symbolOpt.value());
    }

    return builder.obj();
}

Status HCIndexCollectionManager::flushPendingOperations(
    std::function<Status(const std::string&, const std::vector<InsertStatement>&)> flushCallback) {
    if (!writer) {
        return Status(ErrorCodes::InternalError, "HCIndex writer not initialized");
    }

    if (!flushCallback) {
        return Status(ErrorCodes::BadValue, "Flush callback cannot be null");
    }

    // Flush the writer.
    symbolDictionary->flush();
    attributeTable->flush();

    auto pendingSymbolOps = writer->getPendingSymbolOperations();
    auto pendingAttributeOps = writer->getPendingAttributeOperations();

    if (pendingSymbolOps.empty() && pendingAttributeOps.empty()) {
        return Status::OK();  // Nothing to flush
    }

    // Flush symbol operations if any
    if (!pendingSymbolOps.empty()) {
        auto symbolCollName = writer->getSymbolOperationsCollectionName();
        auto status = flushCallback(symbolCollName, pendingSymbolOps);
        if (!status.isOK()) {
            return status;
        }
    }

    // Flush attribute operations if any
    if (!pendingAttributeOps.empty()) {
        auto attributeCollName = writer->getAttributeOperationsCollectionName();
        auto status = flushCallback(attributeCollName, pendingAttributeOps);
        if (!status.isOK()) {
            return status;
        }
    }

    // Clear pending operations after successful flush
    writer->clearPendingOperations();
    return Status::OK();
}

StatusWith<std::vector<int64_t>> HCIndexCollectionManager::queryRows(
    OperationContext* opCtx,
    const ::mongo::MatchExpression* matchExpr,
    const Timestamp& timestamp) {
    LOGV2(9999910, "HCIndexCollectionManager::queryRows called");

    // Initialize the reader for read operations if not already done
    // This is done lazily on first use to avoid issues with stashed transaction resources
    if (!initializedForRead) {
        auto initStatus = initializeForRead(opCtx);
        if (!initStatus.isOK()) {
            LOGV2_WARNING(9999920,
                          "Failed to initialize reader for read operations",
                          "error"_attr = initStatus);
            // Continue anyway - the reader will try to acquire collections on-demand if needed
        }
    }

    if (!attributeTable) {
        LOGV2(9999911, "HCIndex attribute table not initialized");
        return Status(ErrorCodes::InternalError, "HCIndex attribute table not initialized");
    }

    LOGV2(9999912, "Getting attribute table for timestamp",
          "timestamp"_attr = timestamp);

    // Get the attribute table for this timestamp
    auto tableResult = attributeTable->getTableForTimestamp(timestamp);
    if (!tableResult.isOK()) {
        LOGV2(9999913, "Table not in memory, trying to create/reconstruct from disk");
        // Table doesn't exist in memory. Try to create/reconstruct it from disk.
        auto createResult = attributeTable->getOrCreateTableForTimestamp(opCtx, timestamp);
        if (!createResult.isOK()) {
            LOGV2(9999914, "Failed to create/reconstruct table",
                  "error"_attr = createResult.getStatus());
            return createResult.getStatus();
        }
        tableResult = createResult;
    }

    auto table = tableResult.getValue();
    LOGV2(9999915, "Got attribute table",
          "rowCount"_attr = table->getRowCount());

    // Convert the MatchExpression to an AttributeTablePredicate
    auto predicateResult = table->convertMatchExpressionToPredicate(matchExpr);
    if (!predicateResult.isOK()) {
        LOGV2(9999916, "Failed to convert match expression to predicate",
              "error"_attr = predicateResult.getStatus());
        return predicateResult.getStatus();
    }

    LOGV2(9999917, "Converted match expression to predicate",
          "refRowVecSize"_attr = predicateResult.getValue().refRowVec.size());

    // Query the table for matching rows
    auto matchingRowIds = table->queryRows(predicateResult.getValue());
    LOGV2(9999918, "Query completed",
          "matchingRowCount"_attr = matchingRowIds.size());

    // Release acquired collections after query is complete
    // This allows locks to be released and prevents stashed transaction resource issues
    close();

    return matchingRowIds;
}

Status HCIndexCollectionManager::cleanup() {
    // In the future, this could drop the operations collections
    // For now, just clear the structures
    symbolDictionary.reset();
    attributeTable.reset();
    writer.reset();
    reader.reset();
    return Status::OK();
}

}  // namespace mongo::timeseries::hcindex

