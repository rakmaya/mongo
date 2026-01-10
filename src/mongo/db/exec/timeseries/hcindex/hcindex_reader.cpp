/*
 *    Copyright 2024-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and upon
 *    distribution of the following conditions are met:
 *
 *    - The source code is distributed subject to the license terms in the
 *      LICENSE file in the root directory of this source tree.
 *    - Neither the name of MongoDB, Inc. nor the names of its contributors
 *      may be used to endorse or promote products derived from this software
 *      without specific prior written permission.
 */

#include "mongo/db/exec/timeseries/hcindex/hcindex_reader.h"

#include "mongo/bson/bsonobj.h"
#include "mongo/db/exec/timeseries/hcindex/hcindex_collection_manager.h"
#include "mongo/db/timeseries/hcindex_options.h"
#include "mongo/db/local_catalog/shard_role_api/shard_role.h"
#include "mongo/db/namespace_string.h"
#include "mongo/logv2/log.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kQuery

namespace mongo::timeseries::hcindex {

HCIndexReader::HCIndexReader(const DatabaseName& dbName, const UUID& collectionUUID)
    : dbName(dbName), collectionUUID(collectionUUID) {}

void HCIndexReader::acquireCollections(OperationContext* opCtx) {
    // Tries to acquire symbol operations collection WITHOUT acquiring locks
    auto symbolNss = HCIndexCollectionManager::getSymbolOperationsNamespace(dbName, collectionUUID);
    CollectionAcquisitionRequest symbolAcquisitionRequest(
        symbolNss,
        PlacementConcern::kPretendUnsharded,
        repl::ReadConcernArgs::kImplicitDefault,
        AcquisitionPrerequisites::kRead);

    try {
        symbolOpsCollection = acquireCollectionMaybeLockFree(opCtx, symbolAcquisitionRequest);
    } catch (const std::exception& e) {
        LOGV2(9999990,
              "HCIndexReader::acquireCollections - symbol ops collection doesn't exist yet",
              "error"_attr = e.what());
        // TODO: Raise it?
    }

    // Acquire attribute operations collection WITHOUT acquiring locks
    // This is safe because we're just getting a snapshot of the catalog
    auto attributeNss = HCIndexCollectionManager::getAttributeOperationsNamespace(dbName, collectionUUID);
    CollectionAcquisitionRequest attributeAcquisitionRequest(
        attributeNss,
        PlacementConcern::kPretendUnsharded,
        repl::ReadConcernArgs::kImplicitDefault,
        AcquisitionPrerequisites::kRead);

    try {
        attributeOpsCollection = acquireCollectionMaybeLockFree(opCtx, attributeAcquisitionRequest);
    } catch (const std::exception& e) {
        LOGV2(9999990,
              "HCIndexReader::acquireCollections - attribute ops collection doesn't exist yet",
              "error"_attr = e.what());
        // TODO: Raise it?
    }

    // Acquire bitmap index collection WITHOUT acquiring locks
    // This is safe because we're just getting a snapshot of the catalog
    auto bitmapIndexNss = HCIndexCollectionManager::getBitmapIndexNamespace(dbName, collectionUUID);
    CollectionAcquisitionRequest bitmapIndexAcquisitionRequest(
        bitmapIndexNss,
        PlacementConcern::kPretendUnsharded,
        repl::ReadConcernArgs::kImplicitDefault,
        AcquisitionPrerequisites::kRead);

    try {
        bitmapIndexCollection = acquireCollectionMaybeLockFree(opCtx, bitmapIndexAcquisitionRequest);
    } catch (const std::exception& e) {
        LOGV2(9999990,
              "HCIndexReader::acquireCollections - bitmap index collection doesn't exist yet",
              "error"_attr = e.what());
        // TODO: Raise it?
    }
}

Status HCIndexReader::initializeCollections(OperationContext* opCtx) {
    // Check if already initialized to avoid reacquiring collections
    if (collectionsInitialized) {
        LOGV2(9999990, "HCIndexReader::initializeCollections - already initialized, skipping");
        return Status::OK();
    }

    acquireCollections(opCtx);
    collectionsInitialized = true;
    return Status::OK();
}

StatusWith<std::unique_ptr<SymbolDictionary>> HCIndexReader::constructSymbolDictionary(
    OperationContext* opCtx,
    const Timestamp& windowStart,
    const Timestamp& windowEnd,
    HCIndexPeriodEnum period,
    int32_t frequency,
    const Timestamp& upToTimestamp) {

    auto dict = std::make_unique<SymbolDictionary>(period, frequency, windowStart, windowEnd, nullptr);

    // Change state to Reconstruction for dictionaries being reconstructed by the reader
    auto stateStatus = dict->changeState(SymbolDictionaryState::Reconstruction);
    if (!stateStatus.isOK()) {
        return stateStatus;
    }

    // Use the cached collection acquisition instead of acquiring again
    if (!symbolOpsCollection || !symbolOpsCollection->exists()) {
        // Return an empty dictionary in ReadWrite state so new symbols can be added
        auto readWriteStatus = dict->changeState(SymbolDictionaryState::ReadWrite);
        if (!readWriteStatus.isOK()) {
            return readWriteStatus;
        }
        return std::move(dict);
    }

    // Read and replay operations
    auto cursor = symbolOpsCollection->getCollectionPtr()->getCursor(opCtx);
    while (auto record = cursor->next()) {
        BSONObj doc = record->data.toBson();

        // Only process operations within the window and up to the specified timestamp
        Timestamp docWindowStart = doc.getField("windowStart").timestamp();
        Timestamp docWindowEnd = doc.getField("windowEnd").timestamp();

        if (docWindowStart != windowStart || docWindowEnd != windowEnd) {
            continue;
        }

        if (doc.getField("timestamp").timestamp() > upToTimestamp) {
            continue;
        }

        StringData op = doc.getStringField("op");

        if (op == "INIT" || op == "opADD") {
            // Extract and insert symbols
            BSONObj symbolsObj = doc.getObjectField("symbols");
            for (const auto& elem : symbolsObj) {
                std::string word = elem.fieldName();
                uint32_t index = static_cast<uint32_t>(elem.numberInt());

                auto status = dict->getOrInsertSymbol(StringData(word));
                if (!status.isOK()) {
                    return status.getStatus();
                }
            }
        }
    }

    // Transition the dictionary to ReadWrite after reconstruction is complete.
    // This allows the dictionary to accept new symbols as the timeseries collection continues to
    // receive new measurements with new metadata values. The dictionary was in Reconstruction mode
    // during the replay of operations, and now it's ready to accept new writes.
    auto readWriteStatus = dict->changeState(SymbolDictionaryState::ReadWrite);
    if (!readWriteStatus.isOK()) {
        return readWriteStatus;
    }

    // Return the dictionary (may be empty if no operations were found for this window)
    return std::move(dict);
}

StatusWith<SymbolDictionaryConstructionResult> HCIndexReader::constructSymbolDictionaryWithDelta(
    OperationContext* opCtx,
    const Timestamp& windowStart,
    const Timestamp& windowEnd,
    HCIndexPeriodEnum period,
    int32_t frequency,
    const Timestamp& upToTimestamp,
    SymbolDictionary* baseDictionary) {

    SymbolDictionaryConstructionResult result;

    // Use the cached collection acquisition instead of acquiring again
    if (!symbolOpsCollection || !symbolOpsCollection->exists()) {
        // Operations collection doesn't exist yet - create an empty base dictionary
        auto dict = std::make_unique<SymbolDictionary>(period, frequency, windowStart, windowEnd, nullptr);
        auto stateStatus = dict->changeState(SymbolDictionaryState::ReadWrite);
        if (!stateStatus.isOK()) {
            return stateStatus;
        }
        result.baseDictionary = std::move(dict);
        return result;
    }

    // First pass: scan for INIT operation to determine if this is a delta or base
    boost::optional<Timestamp> refBaseDictionaryWindowStart;
    uint32_t localIndexOffset = 0;
    bool foundInit = false;

    auto cursor = symbolOpsCollection->getCollectionPtr()->getCursor(opCtx);
    while (auto record = cursor->next()) {
        BSONObj doc = record->data.toBson();

        Timestamp docWindowStart = doc.getField("windowStart").timestamp();
        Timestamp docWindowEnd = doc.getField("windowEnd").timestamp();

        if (docWindowStart != windowStart || docWindowEnd != windowEnd) {
            continue;
        }

        if (doc.getField("timestamp").timestamp() > upToTimestamp) {
            continue;
        }

        StringData op = doc.getStringField("op");
        if (op == "INIT") {
            foundInit = true;
            // Check for REF field indicating this references a base dictionary
            if (doc.hasField("REF")) {
                refBaseDictionaryWindowStart = doc.getField("REF").timestamp();
                localIndexOffset = doc.getField("localIndexOffset").numberInt();
            }
            break;
        }
    }

    // Create the appropriate dictionary type
    if (refBaseDictionaryWindowStart) {
        // This is a delta dictionary - references a base
        result.refBaseDictionaryWindowStart = refBaseDictionaryWindowStart;

        auto deltaDict = std::make_unique<DeltaSymbolDictionary>(
            period, frequency, windowStart, windowEnd, baseDictionary, nullptr);
        auto stateStatus = deltaDict->changeState(SymbolDictionaryState::Reconstruction);
        if (!stateStatus.isOK()) {
            return stateStatus;
        }

        // Local index offset is the next symbol index to assign
        deltaDict->setNextSymbolIndex(localIndexOffset);

        // Second pass: replay operations to populate the delta dictionary
        cursor = symbolOpsCollection->getCollectionPtr()->getCursor(opCtx);
        while (auto record = cursor->next()) {
            BSONObj doc = record->data.toBson();

            Timestamp docWindowStart = doc.getField("windowStart").timestamp();
            Timestamp docWindowEnd = doc.getField("windowEnd").timestamp();

            if (docWindowStart != windowStart || docWindowEnd != windowEnd) {
                continue;
            }

            if (doc.getField("timestamp").timestamp() > upToTimestamp) {
                continue;
            }

            StringData op = doc.getStringField("op");
            if (op == "INIT" || op == "opADD") {
                BSONObj symbolsObj = doc.getObjectField("symbols");
                for (const auto& elem : symbolsObj) {
                    std::string word = elem.fieldName();
                    auto status = deltaDict->getOrInsertSymbol(StringData(word));
                    if (!status.isOK()) {
                        return status.getStatus();
                    }
                }
            }
        }

        auto readWriteStatus = deltaDict->changeState(SymbolDictionaryState::ReadWrite);
        if (!readWriteStatus.isOK()) {
            return readWriteStatus;
        }

        result.deltaDictionary = std::move(deltaDict);
    } else {
        // This is a base dictionary (no REF)
        auto dict = std::make_unique<SymbolDictionary>(period, frequency, windowStart, windowEnd, nullptr);
        auto stateStatus = dict->changeState(SymbolDictionaryState::Reconstruction);
        if (!stateStatus.isOK()) {
            return stateStatus;
        }

        // Second pass: replay operations to populate the base dictionary
        cursor = symbolOpsCollection->getCollectionPtr()->getCursor(opCtx);
        while (auto record = cursor->next()) {
            BSONObj doc = record->data.toBson();

            Timestamp docWindowStart = doc.getField("windowStart").timestamp();
            Timestamp docWindowEnd = doc.getField("windowEnd").timestamp();

            if (docWindowStart != windowStart || docWindowEnd != windowEnd) {
                continue;
            }

            if (doc.getField("timestamp").timestamp() > upToTimestamp) {
                continue;
            }

            StringData op = doc.getStringField("op");
            if (op == "INIT" || op == "opADD") {
                BSONObj symbolsObj = doc.getObjectField("symbols");
                for (const auto& elem : symbolsObj) {
                    std::string word = elem.fieldName();
                    auto status = dict->getOrInsertSymbol(StringData(word));
                    if (!status.isOK()) {
                        return status.getStatus();
                    }
                }
            }
        }

        auto readWriteStatus = dict->changeState(SymbolDictionaryState::ReadWrite);
        if (!readWriteStatus.isOK()) {
            return readWriteStatus;
        }

        result.baseDictionary = std::move(dict);
    }

    return result;
}

StatusWith<std::unique_ptr<AttributeTable>> HCIndexReader::constructAttributeTable(
    OperationContext* opCtx,
    const Timestamp& windowStart,
    const Timestamp& windowEnd,
    HCIndexPeriodEnum period,
    int32_t frequency,
    const Timestamp& upToTimestamp,
    ISymbolDictionary* symbolDictionary) {

    auto table = std::make_unique<AttributeTable>(symbolDictionary, nullptr, period, frequency, windowStart, windowEnd);

    // Change state to Reconstruction for tables being reconstructed by the reader
    auto stateStatus = table->changeState(AttributeTableState::Reconstruction);
    if (!stateStatus.isOK()) {
        return stateStatus;
    }

    // Use the cached collection acquisition instead of acquiring again
    // This avoids lock cycles during query execution
    if (!attributeOpsCollection || !attributeOpsCollection->exists()) {
        // Operations collection doesn't exist yet - this is expected on first load after restart
        // Return an empty table in ReadWrite state so new rows can be added
        auto readWriteStatus = table->changeState(AttributeTableState::ReadWrite);
        if (!readWriteStatus.isOK()) {
            return readWriteStatus;
        }
        return std::move(table);
    }

    auto cursor = attributeOpsCollection->getCollectionPtr()->getCursor(opCtx);
    int operationCount = 0;
    while (auto record = cursor->next()) {
        BSONObj doc = record->data.toBson();

        // Only process operations within the window and up to the specified timestamp
        Timestamp docWindowStart = doc.getField("windowStart").timestamp();
        Timestamp docWindowEnd = doc.getField("windowEnd").timestamp();

        if (docWindowStart != windowStart || docWindowEnd != windowEnd) {
            continue;
        }

        if (doc.getField("timestamp").timestamp() > upToTimestamp) {
            continue;
        }

        StringData op = doc.getStringField("op");
        Timestamp docTimestamp = doc.getField("timestamp").timestamp();

        if (op == "INIT") {
            // Extract schema and rows from INIT operation
            BSONObj schemaObj = doc.getObjectField("schema");

            for (const auto& elem : schemaObj) {
                std::string fieldName = elem.String();
                auto status = table->addColumn(StringData(fieldName));
                if (!status.isOK()) {
                    return status;
                }
            }

            // Extract and insert rows from INIT operation
            BSONElement rowsElem = doc.getField("rows");
            if (rowsElem && rowsElem.type() == BSONType::array) {
                auto rowsArray = rowsElem.Array();

                for (const auto& rowElem : rowsArray) {
                    if (rowElem.type() == BSONType::array) {
                        auto row = rowElem.Array();
                        std::vector<uint32_t> indices;
                        for (const auto& indexElem : row) {
                            indices.push_back(static_cast<uint32_t>(indexElem.numberInt()));
                        }
                        // Insert the row into the attribute table
                        auto insertStatus = table->insertRowDirect(indices);
                        if (!insertStatus.isOK()) {
                            return insertStatus.getStatus();
                        }
                    }
                }
            }
            operationCount++;
        } else if (op == "opADD") {
            // Extract attributes from ADD operation
            BSONObj attrsObj = doc.getObjectField("attributes");
            for (const auto& elem : attrsObj) {
                std::string fieldName = elem.fieldName();
                auto status = table->addColumn(StringData(fieldName));
                if (!status.isOK()) {
                    return status;
                }
            }

            // Extract and insert rows from opADD operation
            BSONElement rowsElem = doc.getField("rows");
            if (rowsElem && rowsElem.type() == BSONType::array) {
                auto rowsArray = rowsElem.Array();
                for (const auto& rowElem : rowsArray) {
                    if (rowElem.type() == BSONType::array) {
                        auto row = rowElem.Array();
                        std::vector<uint32_t> indices;
                        for (const auto& indexElem : row) {
                            indices.push_back(static_cast<uint32_t>(indexElem.numberInt()));
                        }
                        // Insert the row into the attribute table
                        auto insertStatus = table->insertRowDirect(indices);
                        if (!insertStatus.isOK()) {
                            return insertStatus.getStatus();
                        }
                    }
                }
            }
        }
    }

    // Transition the table to ReadWrite after reconstruction is complete.
    // This allows the table to accept new data as the timeseries collection continues to receive
    // new measurements. The table was in Reconstruction mode during the replay of operations,
    // and now it's ready to accept new writes.
    auto readWriteStatus = table->changeState(AttributeTableState::ReadWrite);
    if (!readWriteStatus.isOK()) {
        return readWriteStatus;
    }

    // Return the table (may be empty if no operations were found for this window)
    return std::move(table);
}

StatusWith<std::unique_ptr<BitmapIndex>> HCIndexReader::constructBitmapIndex(
    OperationContext* opCtx,
    const Timestamp& windowStart,
    const Timestamp& windowEnd,
    HCIndexPeriodEnum period,
    int32_t frequency,
    const Timestamp& upToTimestamp)
{
    auto index = std::make_unique<BitmapIndex>(period, frequency, windowStart, windowEnd, nullptr);

    // Change state to Reconstruction for indexes being reconstructed by the reader
    auto stateStatus = index->changeState(BitmapIndexState::Reconstruction);
    if (!stateStatus.isOK()) {
        return stateStatus;
    }

    // Use the cached collection acquisition instead of acquiring again
    // This avoids lock cycles during query execution
    if (!bitmapIndexCollection || !bitmapIndexCollection->exists()) {
        // Bitmap index collection doesn't exist yet - this is expected on first load after restart
        // Return an empty index in ReadWrite state so new entries can be added
        auto readWriteStatus = index->changeState(BitmapIndexState::ReadWrite);
        if (!readWriteStatus.isOK()) {
            return readWriteStatus;
        }
        return std::move(index);
    }

    // Read and replay operations

    auto cursor = bitmapIndexCollection->getCollectionPtr()->getCursor(opCtx);
    int operationCount = 0;
    while (auto record = cursor->next()) {
        BSONObj doc = record->data.toBson();

        // Only process operations within the window and up to the specified timestamp
        Timestamp docWindowStart = doc.getField("windowStart").timestamp();
        Timestamp docWindowEnd = doc.getField("windowEnd").timestamp();

        if (docWindowStart != windowStart || docWindowEnd != windowEnd) {
            continue;
        }

        if (doc.getField("timestamp").timestamp() > upToTimestamp) {
            continue;
        }

        StringData op = doc.getStringField("op");
        Timestamp docTimestamp = doc.getField("timestamp").timestamp();

        if (op == "INIT" || op == "opADD") {
            // Extract entries from the document
            // Format: { "entries": [ { "col": columnIndex, "sym": symbolIndex, "rows": [rowId1, rowId2, ...] }, ... ] }
            BSONElement entriesElem = doc.getField("entries");
            if (entriesElem && entriesElem.type() == BSONType::array) {
                auto entriesArray = entriesElem.Array();

                for (const auto& entryElem : entriesArray) {
                    if (entryElem.type() == BSONType::object) {
                        BSONObj entry = entryElem.Obj();
                        size_t columnIndex = static_cast<size_t>(entry.getIntField("col"));
                        uint32_t symbolIndex = static_cast<uint32_t>(entry.getIntField("sym"));

                        BSONElement rowsElem = entry.getField("rows");
                        if (rowsElem && rowsElem.type() == BSONType::array) {
                            auto rowsArray = rowsElem.Array();
                            for (const auto& rowIdElem : rowsArray) {
                                int64_t rowId = rowIdElem.numberLong();
                                auto addStatus = index->addEntry(columnIndex, symbolIndex, rowId);
                                if (!addStatus.isOK()) {
                                    return addStatus;
                                }
                            }
                        }
                    }
                }
            }
            operationCount++;
        }
    }

    // Transition the index to ReadWrite after reconstruction is complete.
    // This allows the index to accept new entries as the timeseries collection continues to
    // receive new measurements. The index was in Reconstruction mode during the replay of
    // operations, and now it's ready to accept new writes.
    auto readWriteStatus = index->changeState(BitmapIndexState::ReadWrite);
    if (!readWriteStatus.isOK()) {
        return readWriteStatus;
    }

    // Return the index (may be empty if no operations were found for this window)
    return std::move(index);
}

void HCIndexReader::close()
{
    symbolOpsCollection.reset();
    attributeOpsCollection.reset();
    bitmapIndexCollection.reset();
    collectionsInitialized = false;
}

void HCIndexReader::prepareForYield() {
    // Release collection pointers to allow locks to be yielded
    symbolOpsCollection.reset();
    attributeOpsCollection.reset();
    bitmapIndexCollection.reset();
}

Status HCIndexReader::restoreForYield(OperationContext* opCtx) {
    // Re-acquire collection pointers after yielding
    if (!collectionsInitialized) {
        return Status(ErrorCodes::InternalError,
                      "HCIndexReader collections not initialized before restore");
    }

    acquireCollections(opCtx);
    return Status::OK();
}

}  // namespace mongo::timeseries::hcindex

