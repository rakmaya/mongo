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

#include "mongo/db/timeseries/hcindex/hcindex_writer.h"

#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/oid.h"
#include "mongo/db/collection_crud/collection_write_path.h"
#include "mongo/db/database_name.h"
#include "mongo/db/local_catalog/shard_role_api/shard_role.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/storage/write_unit_of_work.h"
#include "mongo/util/str.h"

namespace mongo::timeseries::hcindex {

HCIndexWriter::HCIndexWriter(OperationContext* opCtx, const UUID& collectionUUID)
    : opCtx(opCtx), collectionUUID(collectionUUID) {}

Status HCIndexWriter::writeSymbolInit(const Timestamp& windowStart,
                                      const Timestamp& windowEnd,
                                      DictionaryGranularity granularity,
                                      const std::vector<std::pair<std::string, uint32_t>>& symbols) {
    BSONObjBuilder symbolsBuilder;
    for (const auto& [word, id] : symbols) {
        symbolsBuilder.append(word, static_cast<int>(id));
    }

    BSONObjBuilder docBuilder;
    docBuilder.append("_id", OID::gen());
    docBuilder.append("timestamp", windowStart);
    docBuilder.append("windowStart", windowStart);
    docBuilder.append("windowEnd", windowEnd);
    docBuilder.append("granularity", static_cast<int>(granularity));
    docBuilder.append("op", "INIT");
    docBuilder.append("symbols", symbolsBuilder.obj());

    return _insertOperation(docBuilder.obj(), true);
}

Status HCIndexWriter::writeAttributeInit(const Timestamp& windowStart,
                                         const Timestamp& windowEnd,
                                         DictionaryGranularity granularity,
                                         const std::vector<std::string>& schema,
                                         const std::vector<std::vector<uint32_t>>& rows) {
    BSONObjBuilder schemaBuilder;
    for (size_t i = 0; i < schema.size(); ++i) {
        schemaBuilder.append(std::to_string(i), schema[i]);
    }

    BSONArrayBuilder rowsBuilder;
    for (const auto& row : rows) {
        BSONArrayBuilder rowBuilder;
        for (uint32_t idx : row) {
            rowBuilder.append(static_cast<int>(idx));
        }
        rowsBuilder.append(rowBuilder.arr());
    }

    BSONObjBuilder docBuilder;
    docBuilder.append("_id", OID::gen());
    docBuilder.append("timestamp", windowStart);
    docBuilder.append("windowStart", windowStart);
    docBuilder.append("windowEnd", windowEnd);
    docBuilder.append("granularity", static_cast<int>(granularity));
    docBuilder.append("op", "INIT");
    docBuilder.append("schema", schemaBuilder.obj());
    docBuilder.append("rows", rowsBuilder.arr());

    return _insertOperation(docBuilder.obj(), false);
}

Status HCIndexWriter::writeSymbolAdd(const Timestamp& windowStart,
                                     const Timestamp& windowEnd,
                                     DictionaryGranularity granularity,
                                     const std::vector<std::pair<std::string, uint32_t>>& symbols) {
    BSONObjBuilder symbolsBuilder;
    for (const auto& [word, id] : symbols) {
        symbolsBuilder.append(word, static_cast<int>(id));
    }

    BSONObjBuilder docBuilder;
    docBuilder.append("_id", OID::gen());
    docBuilder.append("timestamp", Timestamp());
    docBuilder.append("windowStart", windowStart);
    docBuilder.append("windowEnd", windowEnd);
    docBuilder.append("granularity", static_cast<int>(granularity));
    docBuilder.append("op", "opADD");
    docBuilder.append("symbols", symbolsBuilder.obj());

    return _insertOperation(docBuilder.obj(), true);
}

Status HCIndexWriter::writeAttributeAdd(const Timestamp& windowStart,
                                        const Timestamp& windowEnd,
                                        DictionaryGranularity granularity,
                                        const std::vector<std::pair<std::string, size_t>>& attributes) {
    BSONObjBuilder attrsBuilder;
    for (const auto& [fieldName, columnIndex] : attributes) {
        attrsBuilder.append(fieldName, static_cast<int>(columnIndex));
    }

    BSONObjBuilder docBuilder;
    docBuilder.append("_id", OID::gen());
    docBuilder.append("timestamp", Timestamp());
    docBuilder.append("windowStart", windowStart);
    docBuilder.append("windowEnd", windowEnd);
    docBuilder.append("granularity", static_cast<int>(granularity));
    docBuilder.append("op", "opADD");
    docBuilder.append("attributes", attrsBuilder.obj());

    return _insertOperation(docBuilder.obj(), false);
}

Status HCIndexWriter::writeFin(const Timestamp& windowStart,
                               const Timestamp& windowEnd,
                               DictionaryGranularity granularity,
                               bool isSymbolOps) {
    BSONObjBuilder docBuilder;
    docBuilder.append("_id", OID::gen());
    docBuilder.append("timestamp", windowEnd);
    docBuilder.append("windowStart", windowStart);
    docBuilder.append("windowEnd", windowEnd);
    docBuilder.append("granularity", static_cast<int>(granularity));
    docBuilder.append("op", "FIN");

    return _insertOperation(docBuilder.obj(), isSymbolOps);
}

Status HCIndexWriter::writeRef(const Timestamp& windowStart,
                               const Timestamp& windowEnd,
                               DictionaryGranularity granularity,
                               const Timestamp& refWindowStart) {
    BSONObjBuilder docBuilder;
    docBuilder.append("_id", OID::gen());
    docBuilder.append("timestamp", windowStart);
    docBuilder.append("windowStart", windowStart);
    docBuilder.append("windowEnd", windowEnd);
    docBuilder.append("granularity", static_cast<int>(granularity));
    docBuilder.append("op", "REF");
    docBuilder.append("refWindowStart", refWindowStart);

    return _insertOperation(docBuilder.obj(), true);
}

Status HCIndexWriter::_insertOperation(const BSONObj& doc, bool isSymbolOps) {
    // Construct the namespace for the operations collection.
    // Symbol operations go to: system.hcindex.ops.symbols.<collectionUUID>
    // Attribute operations go to: system.hcindex.ops.attributes.<collectionUUID>
    std::string collectionName = isSymbolOps ? "system.hcindex.ops.symbols." : "system.hcindex.ops.attributes.";
    collectionName += collectionUUID.toString();

    NamespaceString nss = NamespaceString::makeGlobalConfigCollection(StringData(collectionName));

    // Acquire the collection with write lock
    auto acquisitionRequest = CollectionAcquisitionRequest(
        nss,
        PlacementConcern::kPretendUnsharded,
        repl::ReadConcernArgs::get(opCtx),
        AcquisitionPrerequisites::kWrite);

    auto collection = acquireCollection(opCtx, acquisitionRequest, MODE_IX);

    if (!collection.exists()) {
        return Status(ErrorCodes::NamespaceNotFound,
                      str::stream() << "HCIndex operations collection [" << nss.toStringForErrorMsg()
                                    << "] not found");
    }

    // Insert the document within a write unit of work.
    WriteUnitOfWork wuow(opCtx);

    std::vector<InsertStatement> inserts;
    inserts.emplace_back(doc);
    auto status = collection_internal::insertDocuments(
        opCtx, collection.getCollectionPtr(), inserts.begin(), inserts.end(), nullptr /* opDebug */);

    if (!status.isOK()) {
        return status;
    }

    wuow.commit();
    return Status::OK();
}

}  // namespace mongo::timeseries::hcindex

