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
#include "mongo/util/timer.h"
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

NamespaceString HCIndexCollectionManager::getBitmapIndexNamespace(const DatabaseName& dbName, const UUID& collectionUUID) {
    std::string collName = "hcindex.idx.bitmaps." + collectionUUID.toString();
    std::string fullNs = str::stream() << dbName.toStringForErrorMsg() << "." << collName;
    return NamespaceString::createNamespaceString_forTest(fullNs);
}

HCIndexCollectionManager::HCIndexCollectionManager(OperationContext* opCtx,
                                                   const DatabaseName& dbName,
                                                   const UUID& collectionUUID,
                                                   HCIndexPeriodEnum period,
                                                   int32_t frequency,
                                                   bool buildMetadataIndex,
                                                   double sparseIndexThreshold,
                                                   double denseIndexThreshold,
                                                   bool dynamicIndexBuild,
                                                   std::vector<std::string> excludedColumns,
                                                   std::vector<std::string> includedColumns)
    : dbName(dbName),
      collectionUUID(collectionUUID),
      period(period),
      frequency(frequency),
      _buildMetadataIndex(buildMetadataIndex),
      _sparseIndexThreshold(sparseIndexThreshold),
      _denseIndexThreshold(denseIndexThreshold),
      _dynamicIndexBuild(dynamicIndexBuild),
      _excludedColumns(std::move(excludedColumns)),
      _includedColumns(std::move(includedColumns)),
      writer(std::make_unique<HCIndexWriter>(collectionUUID, dbName)),
      reader(std::make_unique<HCIndexReader>(dbName, collectionUUID)),
      symbolDictionary(std::make_unique<TemporalSymbolDictionary>(collectionUUID, period, frequency, writer.get(), reader.get())),
      bitmapIndex(buildMetadataIndex ? std::make_unique<TemporalBitmapIndex>(collectionUUID, period, frequency, writer.get(), reader.get()) : nullptr),
      attributeTable(std::make_unique<TemporalAttributeTable>(collectionUUID, period, frequency, symbolDictionary.get(), bitmapIndex.get(), writer.get(), reader.get()))
{
    LOGV2_DEBUG(9999990, 3,
          "HCIndex: HCIndexCollectionManager created",
          "collectionUUID"_attr = collectionUUID,
          "dbName"_attr = dbName,
          "buildMetadataIndex"_attr = buildMetadataIndex,
          "sparseIndexThreshold"_attr = sparseIndexThreshold,
          "denseIndexThreshold"_attr = denseIndexThreshold,
          "dynamicIndexBuild"_attr = dynamicIndexBuild,
          "excludedColumns"_attr = _excludedColumns,
          "includedColumns"_attr = _includedColumns);
    attributeTable->setIncludedIndexColumns(
        std::unordered_set<std::string>(_includedColumns.begin(), _includedColumns.end()));
    attributeTable->setExcludedIndexColumns(
        std::unordered_set<std::string>(_excludedColumns.begin(), _excludedColumns.end()));
}

Status HCIndexCollectionManager::initializeForRead(OperationContext* opCtx) {
    Timer timer;
    // If already initialized, return early
    if (initializedForRead) {
        return Status::OK();
    }

    if (!reader) {
        return Status(ErrorCodes::InternalError, "HCIndex reader not initialized");
    }

    // Initialize the reader by acquiring collections for symbol and attribute operations
    auto readerInitStatus = reader->initializeCollections(opCtx);
    if (!readerInitStatus.isOK()) {
        LOGV2_WARNING(9999990,
                      "Failed to initialize HCIndex reader collections",
                      "error"_attr = readerInitStatus);
        // Continue anyway - the reader will try to acquire collections on-demand if needed
    }

    initializedForRead = true;
    return Status::OK();
}

void HCIndexCollectionManager::close() {
    // Reset the reader to release acquired collections
    Timer timer;
    if (reader) {
        reader->close();
    }
    initializedForRead = false;
    LOGV2_DEBUG(9999990, 3,
          "HCIndexCollectionManager::close ",
          "elapsedMicros"_attr = timer.micros());
}

void HCIndexCollectionManager::prepareForYield() {
    // Release collection pointers held by the reader
    // This allows locks to be yielded safely during query execution
    Timer timer;
    if (reader) {
        reader->prepareForYield();
    }
    LOGV2_DEBUG(9999990, 3,
          "HCIndexCollectionManager::prepareForYield ",
          "elapsedMicros"_attr = timer.micros());
}

Status HCIndexCollectionManager::restoreForYield(OperationContext* opCtx) {
    // Re-acquire collection pointers after yielding
    Timer timer;
    if (!reader) {
        return Status(ErrorCodes::InternalError, "HCIndex reader not initialized");
    }

    auto restoreStatus = reader->restoreForYield(opCtx);
    if (!restoreStatus.isOK()) {
        LOGV2_WARNING(9999990,
                      "Failed to restore HCIndex reader collections after yield",
                      "error"_attr = restoreStatus);
        return restoreStatus;
    }

    LOGV2(9999990,
          "HCIndexCollectionManager::restoreForYield ",
          "elapsedMicros"_attr = timer.micros());
    return Status::OK();
}

StatusWith<int64_t> HCIndexCollectionManager::encodeMetadata(OperationContext* opCtx,
                                                             const BSONObj& metadata,
                                                             const Timestamp& timestamp) {
    if (!symbolDictionary || !attributeTable) {
        return Status(ErrorCodes::InternalError, "HCIndex structures not initialized");
    }

    // Insert the metadata row into the attribute table and generate necessary
    // bitmap indices.
    auto insertStatus = attributeTable->insertRow(opCtx, metadata, timestamp);
    if (!insertStatus.isOK()) {
        return insertStatus.getStatus();
    }

    return insertStatus.getValue().rowId;
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

    // Flush all structures to the writer.
    symbolDictionary->flush();
    attributeTable->flush();
    if (bitmapIndex) {
        bitmapIndex->flush();
    }

    auto pendingSymbolOps = writer->getPendingSymbolOperations();
    auto pendingAttributeOps = writer->getPendingAttributeOperations();
    auto pendingBitmapOps = writer->getPendingBitmapOperations();

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

    // Flush bitmap operations if any
    if (!pendingBitmapOps.empty()) {
        auto bitmapCollName = writer->getBitmapOperationsCollectionName();
        auto status = flushCallback(bitmapCollName, pendingBitmapOps);
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
    Timer timer;

    // NOTE: Initialization should be done by the caller (e.g., TsBucketToCellBlockStage::open())
    // to avoid repeated initialization/close cycles per query.
    // The caller should call initializeForRead() once at the start and close() at the end.
    if (!initializedForRead) {
        LOGV2_WARNING(9999990,
                      "HCIndexCollectionManager::queryRows called but not initialized. "
                      "Caller should call initializeForRead() before queryRows()");
        // Try to initialize anyway as a fallback
        auto initStatus = initializeForRead(opCtx);
        if (!initStatus.isOK()) {
            LOGV2_WARNING(9999990,
                          "Failed to initialize reader for read operations",
                          "error"_attr = initStatus);
            // Continue anyway - the reader will try to acquire collections on-demand if needed
        }
    }

    if (!attributeTable) {
        return Status(ErrorCodes::InternalError, "HCIndex attribute table not initialized");
    }

    // Get the attribute table for this timestamp
    auto tableResult = attributeTable->getTableForTimestamp(timestamp);
    if (!tableResult.isOK()) {
        // Table doesn't exist in memory. Try to create/reconstruct it from disk.
        auto createResult = attributeTable->getOrCreateTableForTimestamp(opCtx, timestamp);
        if (!createResult.isOK()) {
            LOGV2_WARNING(9999990, "Failed to create/reconstruct table",
                  "error"_attr = createResult.getStatus());
            return createResult.getStatus();
        }
        tableResult = createResult;
    }

    // Get the bitmap index if it exists
    BitmapIndex* index = nullptr;
    if (bitmapIndex) {
        auto indexResult = bitmapIndex->getIndexForTimestamp(timestamp);
        if (!indexResult.isOK()) {
            LOGV2_DEBUG(9999990, 3, "HCIndex: Bitmap index not in memory, trying to create/reconstruct from disk");
            // Index may not be in the memory. Try to get it from disk.
            indexResult = bitmapIndex->getOrCreateIndexForTimestamp(opCtx, timestamp);
            if (!indexResult.isOK()) {
                LOGV2_WARNING(9999990, "Failed to create/reconstruct bitmap index",
                      "error"_attr = indexResult.getStatus());
                // Continue anyway - bitmap index is an optimization, not critical
            } else {
                index = indexResult.getValue();
            }
        } else {
            index = indexResult.getValue();
        }
    }

    auto table = tableResult.getValue();

    // Convert the MatchExpression to an AttributeTablePredicate
    auto predicateResult = table->convertMatchExpressionToPredicate(matchExpr);
    if (!predicateResult.isOK()) {
        return predicateResult.getStatus();
    }

    auto& predicate = predicateResult.getValue();
    std::string predicateType;
    if (predicate.isLeaf()) {
        predicateType = "LEAF";
    } else if (predicate.isAnd()) {
        predicateType = "AND";
    } else if (predicate.isOr()) {
        predicateType = "OR";
    } else {
        predicateType = "UNKNOWN";
    }

    // Query the table for matching rows
    auto matchingRowIds = table->queryRows(predicateResult.getValue(), index);

    // Print time took for query rows in debug mode
    LOGV2_DEBUG(9999990, 3,
        "HCIndex Query completed",
        "matchingRowCount"_attr = matchingRowIds.size(),
        "elapsedMicros"_attr = timer.micros());

    return matchingRowIds;
}

Status HCIndexCollectionManager::cleanup() {
    symbolDictionary.reset();
    attributeTable.reset();
    bitmapIndex.reset();
    writer.reset();
    reader.reset();
    return Status::OK();
}


}  // namespace mongo::timeseries::hcindex

