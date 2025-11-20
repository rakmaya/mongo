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

#include "mongo/db/exec/timeseries/hcindex/hcindex_writer.h"

#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/oid.h"

namespace mongo::timeseries::hcindex {

HCIndexWriter::HCIndexWriter(const UUID& collectionUUID)
    : collectionUUID(collectionUUID) {}

Status HCIndexWriter::initSymbolDictionary(const Timestamp& windowStart, const Timestamp& windowEnd) {
    WindowKey key = std::make_pair(windowStart, windowEnd);
    isSymbolInitMode[key] = true;
    return Status::OK();
}

Status HCIndexWriter::initAttributeTable(const Timestamp& windowStart, const Timestamp& windowEnd) {
    WindowKey key = std::make_pair(windowStart, windowEnd);
    isAttributeInitMode[key] = true;
    return Status::OK();
}

Status HCIndexWriter::addSymbol(const Timestamp& windowStart,
                                const Timestamp& windowEnd,
                                const std::string& word,
                                uint32_t index) {
    WindowKey key = std::make_pair(windowStart, windowEnd);
    accumulatedSymbols[key].emplace_back(word, index);
    return Status::OK();
}

Status HCIndexWriter::addAttributeRow(const Timestamp& windowStart,
                                      const Timestamp& windowEnd,
                                      const std::vector<uint32_t>& row) {
    WindowKey key = std::make_pair(windowStart, windowEnd);
    accumulatedRows[key].push_back(row);
    return Status::OK();
}

Status HCIndexWriter::addSchemaField(const Timestamp& windowStart,
                                     const Timestamp& windowEnd,
                                     const std::string& fieldName) {
    WindowKey key = std::make_pair(windowStart, windowEnd);
    accumulatedSchema[key].push_back(fieldName);
    return Status::OK();
}

Status HCIndexWriter::addAttribute(const Timestamp& windowStart,
                                   const Timestamp& windowEnd,
                                   const std::string& fieldName,
                                   size_t columnIndex) {
    WindowKey key = std::make_pair(windowStart, windowEnd);
    accumulatedAttributes[key].emplace_back(fieldName, columnIndex);
    return Status::OK();
}

Status HCIndexWriter::_flushSymbols(const Timestamp& windowStart,
                                    const Timestamp& windowEnd,
                                    DictionaryGranularity granularity) {
    WindowKey key = std::make_pair(windowStart, windowEnd);

    auto it = accumulatedSymbols.find(key);
    if (it == accumulatedSymbols.end() || it->second.empty()) {
        // Reset to ADD mode even if nothing was flushed
        isSymbolInitMode[key] = false;
        return Status::OK();
    }

    BSONObjBuilder symbolsBuilder;
    for (const auto& [word, id] : it->second) {
        symbolsBuilder.append(word, static_cast<int>(id));
    }

    bool isInitMode = isSymbolInitMode[key];
    BSONObjBuilder docBuilder;
    docBuilder.append("_id", OID::gen());
    docBuilder.append("timestamp", isInitMode ? windowStart : Timestamp());
    docBuilder.append("windowStart", windowStart);
    docBuilder.append("windowEnd", windowEnd);
    docBuilder.append("granularity", static_cast<int>(granularity));
    docBuilder.append("op", isInitMode ? "INIT" : "opADD");
    docBuilder.append("symbols", symbolsBuilder.obj());

    _addPendingOperation(docBuilder.obj(), true);
    accumulatedSymbols[key].clear();

    // Reset to ADD mode after flush
    isSymbolInitMode[key] = false;
    return Status::OK();
}

Status HCIndexWriter::_flushAttributes(const Timestamp& windowStart,
                                       const Timestamp& windowEnd,
                                       DictionaryGranularity granularity) {
    WindowKey key = std::make_pair(windowStart, windowEnd);

    auto schemaIt = accumulatedSchema.find(key);
    auto rowsIt = accumulatedRows.find(key);
    auto attrsIt = accumulatedAttributes.find(key);

    bool hasSchema = schemaIt != accumulatedSchema.end() && !schemaIt->second.empty();
    bool hasRows = rowsIt != accumulatedRows.end() && !rowsIt->second.empty();
    bool hasAttrs = attrsIt != accumulatedAttributes.end() && !attrsIt->second.empty();

    if (!hasSchema && !hasRows && !hasAttrs) {
        // Reset to ADD mode even if nothing was flushed
        isAttributeInitMode[key] = false;
        return Status::OK();
    }

    BSONObjBuilder schemaBuilder;
    if (hasSchema) {
        for (size_t i = 0; i < schemaIt->second.size(); ++i) {
            schemaBuilder.append(std::to_string(i), schemaIt->second[i]);
        }
    }

    BSONArrayBuilder rowsBuilder;
    if (hasRows) {
        for (const auto& row : rowsIt->second) {
            BSONArrayBuilder rowBuilder;
            for (uint32_t idx : row) {
                rowBuilder.append(static_cast<int>(idx));
            }
            rowsBuilder.append(rowBuilder.arr());
        }
    }

    BSONObjBuilder attrsBuilder;
    if (hasAttrs) {
        for (const auto& [fieldName, columnIndex] : attrsIt->second) {
            attrsBuilder.append(fieldName, static_cast<int>(columnIndex));
        }
    }

    bool isInitMode = isAttributeInitMode[key];
    BSONObjBuilder docBuilder;
    docBuilder.append("_id", OID::gen());
    docBuilder.append("timestamp", isInitMode ? windowStart : Timestamp());
    docBuilder.append("windowStart", windowStart);
    docBuilder.append("windowEnd", windowEnd);
    docBuilder.append("granularity", static_cast<int>(granularity));
    docBuilder.append("op", isInitMode ? "INIT" : "opADD");
    if (hasSchema) {
        docBuilder.append("schema", schemaBuilder.obj());
    }
    if (hasRows) {
        docBuilder.append("rows", rowsBuilder.arr());
    }
    if (hasAttrs) {
        docBuilder.append("attributes", attrsBuilder.obj());
    }

    _addPendingOperation(docBuilder.obj(), false);
    accumulatedSchema[key].clear();
    accumulatedRows[key].clear();
    accumulatedAttributes[key].clear();

    // Reset to ADD mode after flush
    isAttributeInitMode[key] = false;
    return Status::OK();
}

Status HCIndexWriter::flush(const Timestamp& windowStart,
                            const Timestamp& windowEnd,
                            DictionaryGranularity granularity,
                            bool isSymbolOps) {
    if (isSymbolOps) {
        return _flushSymbols(windowStart, windowEnd, granularity);
    } else {
        return _flushAttributes(windowStart, windowEnd, granularity);
    }
    // Note: isSymbolInitMode or isAttributeInitMode is reset to false at the end of
    // _flushSymbols or _flushAttributes respectively
}

Status HCIndexWriter::buildFin(const Timestamp& windowStart,
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

    _addPendingOperation(docBuilder.obj(), isSymbolOps);
    return Status::OK();
}

Status HCIndexWriter::buildRef(const Timestamp& windowStart,
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

    _addPendingOperation(docBuilder.obj(), true);
    return Status::OK();
}

std::vector<InsertStatement> HCIndexWriter::getPendingSymbolOperations() const {
    return pendingSymbolOperations;
}

std::vector<InsertStatement> HCIndexWriter::getPendingAttributeOperations() const {
    return pendingAttributeOperations;
}

void HCIndexWriter::clearPendingOperations() {
    pendingSymbolOperations.clear();
    pendingAttributeOperations.clear();
}

std::string HCIndexWriter::getSymbolOperationsCollectionName() const {
    return std::string("system.hcindex.ops.symbols.") + collectionUUID.toString();
}

std::string HCIndexWriter::getAttributeOperationsCollectionName() const {
    return std::string("system.hcindex.ops.attributes.") + collectionUUID.toString();
}

void HCIndexWriter::_addPendingOperation(const BSONObj& doc, bool isSymbolOps) {
    if (isSymbolOps) {
        pendingSymbolOperations.emplace_back(InsertStatement(doc));
    } else {
        pendingAttributeOperations.emplace_back(InsertStatement(doc));
    }
}

}  // namespace mongo::timeseries::hcindex

