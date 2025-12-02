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
//#include "mongo/db/collection_crud/collection_write_path.h"
#include "mongo/db/local_catalog/shard_role_api/shard_role.h"
#include "mongo/db/namespace_string.h"
#include "mongo/logv2/log.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kQuery

namespace mongo::timeseries::hcindex {

HCIndexReader::HCIndexReader(const DatabaseName& dbName, const UUID& collectionUUID)
    : dbName(dbName), collectionUUID(collectionUUID) {}

Status HCIndexReader::initializeCollections(OperationContext* opCtx) {
    // Check if already initialized to avoid re-acquiring collections
    if (collectionsInitialized) {
        LOGV2(9999999, "HCIndexReader::initializeCollections - already initialized, skipping");
        return Status::OK();
    }

    LOGV2(9999999, "HCIndexReader::initializeCollections - acquiring ops collections (lock-free)");

    // Acquire symbol operations collection WITHOUT acquiring locks
    // This is safe because we're just getting a snapshot of the catalog
    auto symbolNss = HCIndexCollectionManager::getSymbolOperationsNamespace(dbName, collectionUUID);
    CollectionAcquisitionRequest symbolAcquisitionRequest(
        symbolNss,
        PlacementConcern::kPretendUnsharded,
        repl::ReadConcernArgs::kImplicitDefault,
        AcquisitionPrerequisites::kRead);

    try {
        symbolOpsCollection = acquireCollectionMaybeLockFree(opCtx, symbolAcquisitionRequest);
        LOGV2(9999999, "HCIndexReader::initializeCollections - acquired symbol ops collection");
    } catch (const std::exception& e) {
        LOGV2(9999999,
              "HCIndexReader::initializeCollections - symbol ops collection doesn't exist yet",
              "error"_attr = e.what());
        // Symbol ops collection doesn't exist yet - this is expected on first load
        // We'll handle this gracefully in constructSymbolDictionary
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
        LOGV2(9999999, "HCIndexReader::initializeCollections - acquired attribute ops collection");
    } catch (const std::exception& e) {
        LOGV2(9999999,
              "HCIndexReader::initializeCollections - attribute ops collection doesn't exist yet",
              "error"_attr = e.what());
        // Attribute ops collection doesn't exist yet - this is expected on first load
        // We'll handle this gracefully in constructAttributeTable
    }

    collectionsInitialized = true;
    return Status::OK();
}

StatusWith<std::unique_ptr<SymbolDictionary>> HCIndexReader::constructSymbolDictionary(
    OperationContext* opCtx,
    const Timestamp& windowStart,
    const Timestamp& windowEnd,
    DictionaryGranularity granularity,
    const Timestamp& upToTimestamp) {

    auto dict = std::make_unique<SymbolDictionary>(granularity, windowStart, windowEnd, nullptr);

    // Change state to Reconstruction for dictionaries being reconstructed by the reader
    auto stateStatus = dict->changeState(SymbolDictionaryState::Reconstruction);
    if (!stateStatus.isOK()) {
        return stateStatus;
    }

    // Use the cached collection acquisition instead of acquiring again
    // This avoids lock cycles during query execution
    if (!symbolOpsCollection || !symbolOpsCollection->exists()) {
        // Operations collection doesn't exist yet - this is expected on first load after restart
        // Return an empty dictionary
        auto readOnlyStatus = dict->changeState(SymbolDictionaryState::ReadOnly);
        if (!readOnlyStatus.isOK()) {
            return readOnlyStatus;
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

    return std::move(dict);
}

StatusWith<std::unique_ptr<AttributeTable>> HCIndexReader::constructAttributeTable(
    OperationContext* opCtx,
    const Timestamp& windowStart,
    const Timestamp& windowEnd,
    DictionaryGranularity granularity,
    const Timestamp& upToTimestamp,
    SymbolDictionary* symbolDictionary) {

    auto table = std::make_unique<AttributeTable>(symbolDictionary, nullptr, windowStart, windowEnd);

    // Change state to Reconstruction for tables being reconstructed by the reader
    auto stateStatus = table->changeState(AttributeTableState::Reconstruction);
    if (!stateStatus.isOK()) {
        return stateStatus;
    }

    // Use the cached collection acquisition instead of acquiring again
    // This avoids lock cycles during query execution
    if (!attributeOpsCollection || !attributeOpsCollection->exists()) {
        // Operations collection doesn't exist yet - this is expected on first load after restart
        // Return an empty table
        auto readOnlyStatus = table->changeState(AttributeTableState::ReadOnly);
        if (!readOnlyStatus.isOK()) {
            return readOnlyStatus;
        }
        return std::move(table);
    }

    // Read and replay operations
    LOGV2(9999920, "HCIndexReader::constructAttributeTable - starting reconstruction",
          "windowStart"_attr = windowStart,
          "windowEnd"_attr = windowEnd,
          "upToTimestamp"_attr = upToTimestamp);

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

        LOGV2(9999921, "Processing operation",
              "op"_attr = op,
              "timestamp"_attr = docTimestamp);

        if (op == "INIT") {
            // Extract schema and rows from INIT operation
            BSONObj schemaObj = doc.getObjectField("schema");
            LOGV2(9999922, "Processing INIT operation",
                  "schemaFieldCount"_attr = schemaObj.nFields());

            for (const auto& elem : schemaObj) {
                std::string fieldName = elem.String();
                LOGV2(9999923, "Adding column",
                      "fieldName"_attr = fieldName);
                auto status = table->addColumn(StringData(fieldName));
                if (!status.isOK()) {
                    return status;
                }
            }

            // Extract and insert rows from INIT operation
            BSONElement rowsElem = doc.getField("rows");
            if (rowsElem && rowsElem.type() == BSONType::array) {
                auto rowsArray = rowsElem.Array();
                LOGV2(9999924, "Inserting rows",
                      "rowCount"_attr = rowsArray.size());

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

    return std::move(table);
}

void HCIndexReader::close()
{
    symbolOpsCollection.reset();
    attributeOpsCollection.reset();
    collectionsInitialized = false;
}

}  // namespace mongo::timeseries::hcindex

