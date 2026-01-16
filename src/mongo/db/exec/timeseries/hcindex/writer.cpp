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

#include "mongo/db/exec/timeseries/hcindex/writer.h"

#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/oid.h"
#include "mongo/util/str.h"

#include <roaring.hh>

namespace mongo::timeseries::hcindex {

HCIndexWriter::HCIndexWriter(const UUID& collectionUUID, const DatabaseName& dbName)
    : collectionUUID(collectionUUID), dbName(dbName) {}

Status HCIndexWriter::initSymbolDictionary(const Timestamp& windowStart,
                                           const Timestamp& windowEnd,
                                           const boost::optional<Timestamp>& refBaseDictionary,
                                           uint32_t localIndexOffset) {
    WindowKey key = std::make_pair(windowStart, windowEnd);
    isSymbolInitMode[key] = true;
    symbolInitParams[key].refBaseDictionary = refBaseDictionary;
    symbolInitParams[key].localIndexOffset = localIndexOffset;
    return Status::OK();
}

Status HCIndexWriter::initAttributeTable(const Timestamp& windowStart, const Timestamp& windowEnd) {
    WindowKey key = std::make_pair(windowStart, windowEnd);
    isAttributeInitMode[key] = true;
    return Status::OK();
}

Status HCIndexWriter::initBitmapIndex(const Timestamp& windowStart, const Timestamp& windowEnd) {
    WindowKey key = std::make_pair(windowStart, windowEnd);
    isBitmapInitMode[key] = true;
    return Status::OK();
}

Status HCIndexWriter::addSymbol(const Timestamp& windowStart,
                                const Timestamp& windowEnd,
                                const std::string& word,
                                uint32_t index,
                                SymbolType symbolType) {
    WindowKey key = std::make_pair(windowStart, windowEnd);
    accumulatedSymbols[key].push_back(SymbolEntry{word, index, symbolType});
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

Status HCIndexWriter::addBitmapEntry(const Timestamp& windowStart,
                                     const Timestamp& windowEnd,
                                     size_t columnIndex,
                                     uint32_t symbolIndex,
                                     const std::set<int64_t>& rowIds) {
    WindowKey key = std::make_pair(windowStart, windowEnd);
    BitmapKey bitmapKey = std::make_pair(columnIndex, symbolIndex);
    // Merge the rowIds into the existing Roaring bitmap for this bitmap key
    auto& roaringBitmap = accumulatedBitmaps[key][bitmapKey];
    for (int64_t rowId : rowIds) {
        roaringBitmap.add(rowId);
    }
    return Status::OK();
}

Status HCIndexWriter::addBitmapEntryRoaring(const Timestamp& windowStart,
                                            const Timestamp& windowEnd,
                                            size_t columnIndex,
                                            uint32_t symbolIndex,
                                            const Roaring64BTree& roaringBitmap) {
    WindowKey key = std::make_pair(windowStart, windowEnd);
    BitmapKey bitmapKey = std::make_pair(columnIndex, symbolIndex);
    // Merge the Roaring bitmap directly (more efficient than going through std::set)
    auto& existingBitmap = accumulatedBitmaps[key][bitmapKey];
    for (uint64_t rowId : roaringBitmap) {
        existingBitmap.add(rowId);
    }
    return Status::OK();
}

Status HCIndexWriter::_flushSymbols(const Timestamp& windowStart,
                                    const Timestamp& windowEnd,
                                    HCIndexPeriodEnum period,
                                    int32_t frequency) {
    WindowKey key = std::make_pair(windowStart, windowEnd);

    auto it = accumulatedSymbols.find(key);
    if (it == accumulatedSymbols.end() || it->second.empty()) {
        // Reset to ADD mode even if nothing was flushed
        isSymbolInitMode[key] = false;
        return Status::OK();
    }

    // Separate base symbols from local symbols
    std::vector<SymbolEntry> baseSymbols;
    std::vector<SymbolEntry> localSymbols;
    for (const auto& entry : it->second) {
        if (entry.type == SymbolType::Local) {
            localSymbols.push_back(entry);
        } else {
            baseSymbols.push_back(entry);
        }
    }

    bool isInitMode = isSymbolInitMode[key];

    // Flush base symbols (if any)
    if (!baseSymbols.empty()) {
        BSONObjBuilder symbolsBuilder;
        for (const auto& entry : baseSymbols) {
            symbolsBuilder.append(entry.word, static_cast<int>(entry.index));
        }

        BSONObjBuilder docBuilder;
        docBuilder.append("_id", OID::gen());
        docBuilder.append("timestamp", isInitMode ? windowStart : Timestamp());
        docBuilder.append("windowStart", windowStart);
        docBuilder.append("windowEnd", windowEnd);
        docBuilder.append("period", static_cast<int>(period));
        docBuilder.append("frequency", frequency);

        // If there is a base dictionary referenced, add a REF operation.
        if (isInitMode && symbolInitParams[key].refBaseDictionary) {
            docBuilder.append("REF", symbolInitParams[key].refBaseDictionary.get());
            docBuilder.append("localIndexOffset",
                              static_cast<long long>(symbolInitParams[key].localIndexOffset));
        }

        docBuilder.append("op", isInitMode ? "INIT" : "opADD");
        docBuilder.append("symbols", symbolsBuilder.obj());

        _addPendingOperation(docBuilder.obj(), OpType::Symbol);
    }

    // Flush local symbols (if any) - these are delta dictionary symbols
    if (!localSymbols.empty()) {
        BSONObjBuilder symbolsBuilder;
        for (const auto& entry : localSymbols) {
            symbolsBuilder.append(entry.word, static_cast<int>(entry.index));
        }

        BSONObjBuilder docBuilder;
        docBuilder.append("_id", OID::gen());
        // Local symbols don't set timestamp (they're always ADD operations to the delta)
        docBuilder.append("timestamp", Timestamp());
        docBuilder.append("windowStart", windowStart);
        docBuilder.append("windowEnd", windowEnd);
        docBuilder.append("period", static_cast<int>(period));
        docBuilder.append("frequency", frequency);

        // Local symbols use "opADD_LOCAL" to distinguish from base dictionary adds
        docBuilder.append("op", "opADD_LOCAL");
        docBuilder.append("symbols", symbolsBuilder.obj());

        _addPendingOperation(docBuilder.obj(), OpType::Symbol);
    }

    accumulatedSymbols[key].clear();

    // Reset to ADD mode after flush
    isSymbolInitMode[key] = false;
    return Status::OK();
}

Status HCIndexWriter::_flushAttributes(const Timestamp& windowStart,
                                       const Timestamp& windowEnd,
                                       HCIndexPeriodEnum period,
                                       int32_t frequency) {
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
    docBuilder.append("period", static_cast<int>(period));
    docBuilder.append("frequency", frequency);
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

    _addPendingOperation(docBuilder.obj(), OpType::Attribute);
    accumulatedSchema[key].clear();
    accumulatedRows[key].clear();
    accumulatedAttributes[key].clear();

    // Reset to ADD mode after flush
    isAttributeInitMode[key] = false;
    return Status::OK();
}

Status HCIndexWriter::_flushBitmaps(const Timestamp& windowStart,
                                    const Timestamp& windowEnd,
                                    HCIndexPeriodEnum period,
                                    int32_t frequency) {
    WindowKey key = std::make_pair(windowStart, windowEnd);

    auto it = accumulatedBitmaps.find(key);
    if (it == accumulatedBitmaps.end() || it->second.empty()) {
        // Reset to ADD mode even if nothing was flushed
        isBitmapInitMode[key] = false;
        return Status::OK();
    }

    // Build the bitmaps document using delta-encoded format
    // Format: { "entries": [ { "col": columnIndex, "sym": symbolIndex, "rowIds": BinData(...) },
    // ... ] } Uses delta encoding for efficient storage (typical 10-80x compression)
    BSONArrayBuilder entriesBuilder;
    for (const auto& [bitmapKey, roaringBitmap] : it->second) {
        BSONObjBuilder entryBuilder;
        entryBuilder.append("col", static_cast<int>(bitmapKey.first));
        entryBuilder.append("sym", static_cast<int>(bitmapKey.second));

        // Serialize Roaring64BTree using delta encoding
        // This is MUCH more efficient than storing individual Long values:
        // - Delta encoding: store differences between consecutive values
        // - Variable-length encoding for small deltas
        // - Typical compression: 10-80x better than BSON array of Longs

        std::vector<char> buffer = _serializeRoaring64BTree(roaringBitmap);

        // Store as BinData (subtype 0 = generic binary)
        entryBuilder.appendBinData("rowIds", buffer.size(), BinDataGeneral, buffer.data());
        entriesBuilder.append(entryBuilder.obj());
    }

    bool isInitMode = isBitmapInitMode[key];
    BSONObjBuilder docBuilder;
    docBuilder.append("_id", OID::gen());
    docBuilder.append("timestamp", isInitMode ? windowStart : Timestamp());
    docBuilder.append("windowStart", windowStart);
    docBuilder.append("windowEnd", windowEnd);
    docBuilder.append("period", static_cast<int>(period));
    docBuilder.append("frequency", frequency);
    docBuilder.append("op", isInitMode ? "INIT" : "opADD");
    docBuilder.append("entries", entriesBuilder.arr());

    _addPendingOperation(docBuilder.obj(), OpType::Bitmap);
    accumulatedBitmaps[key].clear();

    // Reset to ADD mode after flush
    isBitmapInitMode[key] = false;
    return Status::OK();
}

Status HCIndexWriter::flush(const Timestamp& windowStart,
                            const Timestamp& windowEnd,
                            HCIndexPeriodEnum period,
                            int32_t frequency,
                            bool isSymbolOps) {
    if (isSymbolOps) {
        return _flushSymbols(windowStart, windowEnd, period, frequency);
    } else {
        return _flushAttributes(windowStart, windowEnd, period, frequency);
    }
    // Note: isSymbolInitMode or isAttributeInitMode is reset to false at the end of
    // _flushSymbols or _flushAttributes respectively
}

Status HCIndexWriter::flushBitmaps(const Timestamp& windowStart,
                                   const Timestamp& windowEnd,
                                   HCIndexPeriodEnum period,
                                   int32_t frequency) {
    return _flushBitmaps(windowStart, windowEnd, period, frequency);
}

Status HCIndexWriter::buildFin(const Timestamp& windowStart,
                               const Timestamp& windowEnd,
                               HCIndexPeriodEnum period,
                               int32_t frequency,
                               bool isSymbolOps) {
    BSONObjBuilder docBuilder;
    docBuilder.append("_id", OID::gen());
    docBuilder.append("timestamp", windowEnd);
    docBuilder.append("windowStart", windowStart);
    docBuilder.append("windowEnd", windowEnd);
    docBuilder.append("period", static_cast<int>(period));
    docBuilder.append("frequency", frequency);
    docBuilder.append("op", "FIN");

    _addPendingOperation(docBuilder.obj(), isSymbolOps ? OpType::Symbol : OpType::Attribute);
    return Status::OK();
}

Status HCIndexWriter::buildRef(const Timestamp& windowStart,
                               const Timestamp& windowEnd,
                               HCIndexPeriodEnum period,
                               int32_t frequency,
                               const Timestamp& refWindowStart) {
    BSONObjBuilder docBuilder;
    docBuilder.append("_id", OID::gen());
    docBuilder.append("timestamp", windowStart);
    docBuilder.append("windowStart", windowStart);
    docBuilder.append("windowEnd", windowEnd);
    docBuilder.append("period", static_cast<int>(period));
    docBuilder.append("frequency", frequency);
    docBuilder.append("op", "REF");
    docBuilder.append("refWindowStart", refWindowStart);

    _addPendingOperation(docBuilder.obj(), OpType::Symbol);
    return Status::OK();
}

std::vector<InsertStatement> HCIndexWriter::getPendingSymbolOperations() const {
    return pendingSymbolOperations;
}

std::vector<InsertStatement> HCIndexWriter::getPendingAttributeOperations() const {
    return pendingAttributeOperations;
}

std::vector<InsertStatement> HCIndexWriter::getPendingBitmapOperations() const {
    return pendingBitmapOperations;
}

void HCIndexWriter::clearPendingOperations() {
    pendingSymbolOperations.clear();
    pendingAttributeOperations.clear();
    pendingBitmapOperations.clear();
}

std::string HCIndexWriter::getSymbolOperationsCollectionName() const {
    return str::stream() << dbName.toStringForErrorMsg() << ".hcindex.ops.symbols."
                         << collectionUUID.toString();
}

std::string HCIndexWriter::getAttributeOperationsCollectionName() const {
    return str::stream() << dbName.toStringForErrorMsg() << ".hcindex.ops.attributes."
                         << collectionUUID.toString();
}

std::string HCIndexWriter::getBitmapOperationsCollectionName() const {
    return str::stream() << dbName.toStringForErrorMsg() << ".hcindex.idx.bitmaps."
                         << collectionUUID.toString();
}

void HCIndexWriter::_addPendingOperation(const BSONObj& doc, OpType opType) {
    switch (opType) {
        case OpType::Symbol:
            pendingSymbolOperations.emplace_back(InsertStatement(doc));
            break;
        case OpType::Attribute:
            pendingAttributeOperations.emplace_back(InsertStatement(doc));
            break;
        case OpType::Bitmap:
            pendingBitmapOperations.emplace_back(InsertStatement(doc));
            break;
    }
}

std::vector<char> HCIndexWriter::_serializeRoaring64BTree(const Roaring64BTree& bitmap) const {
    // Serialize Roaring64BTree using delta encoding for efficient storage
    // Format: [count:8][delta1:varint][delta2:varint]...
    //
    // Delta encoding: Instead of storing absolute values [0, 1, 5, 100],
    // store differences [0, 1, 4, 95]. Small deltas compress well.
    //
    // Variable-length encoding: Small numbers use fewer bytes
    // - 0-127: 1 byte
    // - 128-16383: 2 bytes
    // - etc.
    //
    // Typical compression: 10-80x better than BSON array of Longs

    std::vector<char> buffer;

    // Count elements
    uint64_t count = 0;
    for (auto it = bitmap.begin(); it != bitmap.end(); ++it) {
        ++count;
    }

    // Reserve space (estimate: 2 bytes per element on average for delta encoding)
    buffer.reserve(8 + count * 2);

    // Write count (8 bytes)
    buffer.resize(8);
    std::memcpy(buffer.data(), &count, sizeof(uint64_t));

    if (count == 0) {
        return buffer;
    }

    // Write delta-encoded values using variable-length encoding
    uint64_t prevValue = 0;
    bool first = true;

    for (uint64_t value : bitmap) {
        uint64_t delta = first ? value : (value - prevValue);
        first = false;
        prevValue = value;

        // Variable-length encoding (similar to Protocol Buffers varint)
        // Each byte stores 7 bits of data + 1 continuation bit
        while (delta >= 0x80) {
            buffer.push_back(static_cast<char>((delta & 0x7F) | 0x80));
            delta >>= 7;
        }
        buffer.push_back(static_cast<char>(delta));
    }

    return buffer;
}

}  // namespace mongo::timeseries::hcindex

