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
    : opCtx(opCtx),
      dbName(dbName),
      collectionUUID(collectionUUID),
      granularity(granularity),
      writer(std::make_unique<HCIndexWriter>(collectionUUID, dbName)),
      reader(std::make_unique<HCIndexReader>(opCtx, dbName, collectionUUID)),
      symbolDictionary(std::make_unique<TemporalSymbolDictionary>(opCtx, collectionUUID, granularity, writer.get())),
      attributeTable(std::make_unique<TemporalAttributeTable>(opCtx, collectionUUID, granularity, symbolDictionary.get(), writer.get())) {}

Status HCIndexCollectionManager::initialize() {
    // The structures are already initialized in the constructor
    // In the future, this could create the operations collections if needed
    return Status::OK();
}

StatusWith<int64_t> HCIndexCollectionManager::encodeMetadata(const BSONObj& metadata,
                                                             const Timestamp& timestamp) {
    if (!symbolDictionary || !attributeTable) {
        return Status(ErrorCodes::InternalError, "HCIndex structures not initialized");
    }

    // Insert the metadata row into the attribute table
    auto rowIdStatus = attributeTable->insertRow(metadata, timestamp);
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

StatusWith<BSONObj> HCIndexCollectionManager::decodeMetadata(int64_t rowId,
                                                             const Timestamp& timestamp) {
    if (!symbolDictionary || !attributeTable) {
        return Status(ErrorCodes::InternalError, "HCIndex structures not initialized");
    }

    // Get the attribute table for this timestamp
    auto tableResult = attributeTable->getTableForTimestamp(timestamp);
    if (!tableResult.isOK()) {
        return tableResult.getStatus();
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

