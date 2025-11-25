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


namespace mongo::timeseries::hcindex {

HCIndexReader::HCIndexReader(const DatabaseName& dbName, const UUID& collectionUUID)
    : dbName(dbName), collectionUUID(collectionUUID) {}

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

    // Get namespace for symbol operations collection using HCIndexCollectionManager
    auto nss = HCIndexCollectionManager::getSymbolOperationsNamespace(dbName, collectionUUID);

    // Try to acquire collection with read lock
    CollectionAcquisitionRequest acquisitionRequest(
        nss,
        PlacementConcern::kPretendUnsharded,
        repl::ReadConcernArgs::kImplicitDefault,
        AcquisitionPrerequisites::kRead);

    boost::optional<CollectionAcquisition> collection;
    try {
        collection = acquireCollection(opCtx, acquisitionRequest, MODE_IS);
    } catch (const std::exception& e) {
        // Operations collection doesn't exist yet - this is expected on first load after restart
        // Return an empty dictionary
        auto readOnlyStatus = dict->changeState(SymbolDictionaryState::ReadOnly);
        if (!readOnlyStatus.isOK()) {
            return readOnlyStatus;
        }
        return std::move(dict);
    }

    if (!collection || !collection->exists()) {
        // Operations collection doesn't exist yet - this is expected on first load after restart
        // Return an empty dictionary
        auto readOnlyStatus = dict->changeState(SymbolDictionaryState::ReadOnly);
        if (!readOnlyStatus.isOK()) {
            return readOnlyStatus;
        }
        return std::move(dict);
    }

    // Read and replay operations
    auto cursor = collection->getCollectionPtr()->getCursor(opCtx);
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

    // Get namespace for attribute operations collection using HCIndexCollectionManager
    auto nss = HCIndexCollectionManager::getAttributeOperationsNamespace(dbName, collectionUUID);

    // Try to acquire collection with read lock
    CollectionAcquisitionRequest acquisitionRequest(
        nss,
        PlacementConcern::kPretendUnsharded,
        repl::ReadConcernArgs::kImplicitDefault,
        AcquisitionPrerequisites::kRead);

    boost::optional<CollectionAcquisition> collection;
    try {
        collection = acquireCollection(opCtx, acquisitionRequest, MODE_IS);
    } catch (const std::exception& e) {
        // Operations collection doesn't exist yet - this is expected on first load after restart
        // Return an empty table
        auto readOnlyStatus = table->changeState(AttributeTableState::ReadOnly);
        if (!readOnlyStatus.isOK()) {
            return readOnlyStatus;
        }
        return std::move(table);
    }

    if (!collection || !collection->exists()) {
        // Operations collection doesn't exist yet - this is expected on first load after restart
        // Return an empty table
        auto readOnlyStatus = table->changeState(AttributeTableState::ReadOnly);
        if (!readOnlyStatus.isOK()) {
            return readOnlyStatus;
        }
        return std::move(table);
    }

    // Read and replay operations
    auto cursor = collection->getCollectionPtr()->getCursor(opCtx);
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

}  // namespace mongo::timeseries::hcindex

