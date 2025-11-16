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

#include "mongo/db/timeseries/hcindex/hcindex_reader.h"

#include "mongo/bson/bsonobj.h"
#include "mongo/db/collection_crud/collection_write_path.h"
#include "mongo/db/local_catalog/shard_role_api/shard_role.h"
#include "mongo/db/namespace_string.h"
#include "mongo/util/str.h"

namespace mongo::timeseries::hcindex {

HCIndexReader::HCIndexReader(OperationContext* opCtx, const UUID& collectionUUID)
    : opCtx(opCtx), collectionUUID(collectionUUID) {}

StatusWith<std::unique_ptr<SymbolDictionary>> HCIndexReader::constructSymbolDictionary(
    const Timestamp& windowStart,
    const Timestamp& windowEnd,
    DictionaryGranularity granularity,
    const Timestamp& upToTimestamp) {
    
    auto dict = std::make_unique<SymbolDictionary>();
    
    // Construct namespace for symbol operations collection
    std::string collectionName = "system.hcindex.ops.symbols." + collectionUUID.toString();
    NamespaceString nss = NamespaceString::makeGlobalConfigCollection(StringData(collectionName));
    
    // Acquire collection with read lock
    CollectionAcquisitionRequest acquisitionRequest(
        nss,
        PlacementConcern::kPretendUnsharded,
        repl::ReadConcernArgs::get(opCtx),
        AcquisitionPrerequisites::kRead);
    
    auto collection = acquireCollection(opCtx, acquisitionRequest, MODE_IS);
    
    if (!collection.exists()) {
        return Status(ErrorCodes::NamespaceNotFound,
                      str::stream() << "Symbol operations collection not found: " << nss.toStringForErrorMsg());
    }
    
    // Read and replay operations
    auto cursor = collection.getCollectionPtr()->getCursor(opCtx);
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
    
    return std::move(dict);
}

StatusWith<std::unique_ptr<AttributeTable>> HCIndexReader::constructAttributeTable(
    const Timestamp& windowStart,
    const Timestamp& windowEnd,
    DictionaryGranularity granularity,
    const Timestamp& upToTimestamp,
    SymbolDictionary* symbolDictionary) {
    
    auto table = std::make_unique<AttributeTable>(symbolDictionary);
    
    // Construct namespace for attribute operations collection
    std::string collectionName = "system.hcindex.ops.attributes." + collectionUUID.toString();
    NamespaceString nss = NamespaceString::makeGlobalConfigCollection(StringData(collectionName));
    
    // Acquire collection with read lock
    CollectionAcquisitionRequest acquisitionRequest(
        nss,
        PlacementConcern::kPretendUnsharded,
        repl::ReadConcernArgs::get(opCtx),
        AcquisitionPrerequisites::kRead);
    
    auto collection = acquireCollection(opCtx, acquisitionRequest, MODE_IS);
    
    if (!collection.exists()) {
        return Status(ErrorCodes::NamespaceNotFound,
                      str::stream() << "Attribute operations collection not found: " << nss.toStringForErrorMsg());
    }
    
    // Read and replay operations
    auto cursor = collection.getCollectionPtr()->getCursor(opCtx);
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
        }
    }
    
    return std::move(table);
}

}  // namespace mongo::timeseries::hcindex

