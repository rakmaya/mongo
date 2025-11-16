/**
 *    Copyright (C) 2024-present MongoDB, Inc.
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
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/db/timeseries/hcindex/temporal_attribute_table.h"
#include "mongo/db/timeseries/hcindex/temporal_symbol_dictionary.h"
#include "mongo/base/error_codes.h"

namespace mongo::timeseries::hcindex {

// ============================================================================
// AttributeTable Implementation
// ============================================================================

AttributeTable::AttributeTable(SymbolDictionary* symbolDictionary)
    : symbolDictionary(symbolDictionary) {
}

StatusWith<int64_t> AttributeTable::insertRow(const BSONObj& metadata) {
    std::unique_lock<std::shared_mutex> lock(mutex);

    // Convert metadata to row vector
    auto rowResult = metadataToRow(metadata);
    if (!rowResult.isOK()) {
        return rowResult.getStatus();
    }

    std::vector<uint32_t> row = rowResult.getValue();

    // Check for duplicate row
    auto duplicateRowId = findDuplicateRow(row);
    if (duplicateRowId) {
        return duplicateRowId.value();
    }

    // Insert new row (note: insertRowDirect does not acquire lock, caller must hold it)
    return insertRowDirect(row);
}

StatusWith<int64_t> AttributeTable::insertRowDirect(const std::vector<uint32_t>& row) {
    // Note: Caller must hold the mutex lock. This method does not acquire the lock
    // to avoid deadlock when called from insertRow() which already holds the lock.

    // Pad row with 0s if it's shorter than schema
    std::vector<uint32_t> paddedRow = row;
    while (paddedRow.size() < schema.size()) {
        paddedRow.push_back(0);
    }

    // Validate row size
    if (paddedRow.size() > schema.size()) {
        return Status(ErrorCodes::BadValue,
                      "Row has more columns than schema");
    }

    // Store row and assign ID
    rows.push_back(paddedRow);
    int64_t rowId = nextRowId++;

    return rowId;
}

boost::optional<std::vector<uint32_t>> AttributeTable::getRow(int64_t rowId) const {
    std::shared_lock<std::shared_mutex> lock(mutex);

    if (rowId < 0 || rowId >= static_cast<int64_t>(rows.size())) {
        return boost::none;
    }

    return rows[rowId];
}

std::vector<int64_t> AttributeTable::queryRows(
    const AttributeTablePredicate& predicate) const {
    std::shared_lock<std::shared_mutex> lock(mutex);

    std::vector<int64_t> matchingRowIds;

    // Iterate through all rows
    for (size_t rowIdx = 0; rowIdx < rows.size(); ++rowIdx) {
        const auto& row = rows[rowIdx];
        bool matches = true;

        // Check column-based predicates
        for (const auto& [colIdx, symbolIdx] : predicate.columnMatches) {
            if (colIdx >= row.size() || row[colIdx] != symbolIdx) {
                matches = false;
                break;
            }
        }

        if (!matches) {
            continue;
        }

        // Check field-based predicates
        for (const auto& [fieldName, symbolIdx] : predicate.fieldMatches) {
            auto it = fieldToColumnIndex.find(fieldName);
            if (it == fieldToColumnIndex.end()) {
                // Field not in schema
                matches = false;
                break;
            }

            size_t colIdx = it->second;
            if (colIdx >= row.size() || row[colIdx] != symbolIdx) {
                matches = false;
                break;
            }
        }

        if (matches) {
            matchingRowIds.push_back(rowIdx);
        }
    }

    return matchingRowIds;
}

Status AttributeTable::addColumn(StringData fieldName) {
    std::unique_lock<std::shared_mutex> lock(mutex);

    // Check if column already exists
    if (fieldToColumnIndex.find(std::string(fieldName)) != fieldToColumnIndex.end()) {
        return Status(ErrorCodes::DuplicateKey,
                      "Column already exists: " + std::string(fieldName));
    }

    // Add to schema
    std::string fieldNameStr = std::string(fieldName);
    fieldToColumnIndex[fieldNameStr] = schema.size();
    schema.push_back(fieldNameStr);

    // Extend all existing rows with 0 (missing value)
    for (auto& row : rows) {
        row.push_back(0);
    }

    return Status::OK();
}

const std::vector<std::string>& AttributeTable::getSchema() const {
    std::shared_lock<std::shared_mutex> lock(mutex);
    return schema;
}

size_t AttributeTable::getRowCount() const {
    std::shared_lock<std::shared_mutex> lock(mutex);
    return rows.size();
}

size_t AttributeTable::getMemoryUsageBytes() const {
    std::shared_lock<std::shared_mutex> lock(mutex);

    size_t totalBytes = 0;

    // Schema memory
    for (const auto& fieldName : schema) {
        totalBytes += fieldName.size();
    }
    totalBytes += schema.capacity() * sizeof(std::string);

    // Rows memory
    for (const auto& row : rows) {
        totalBytes += row.capacity() * sizeof(uint32_t);
    }
    totalBytes += rows.capacity() * sizeof(std::vector<uint32_t>);

    // Field to column index map
    for (const auto& [fieldName, colIdx] : fieldToColumnIndex) {
        totalBytes += fieldName.size();
    }

    return totalBytes;
}

StatusWith<std::vector<uint32_t>> AttributeTable::metadataToRow(
    const BSONObj& metadata) {
    // First pass: identify all fields in metadata and add new ones to schema
    for (const auto& elem : metadata) {
        std::string fieldName = elem.fieldName();

        // Extract string value from BSON element
        if (elem.type() != BSONType::string) {
            return Status(ErrorCodes::BadValue,
                          "Metadata field '" + fieldName +
                              "' must be a string, got: " +
                              typeName(elem.type()));
        }

        // Check if field is in schema
        auto it = fieldToColumnIndex.find(fieldName);
        if (it == fieldToColumnIndex.end()) {
            // New field - add to schema
            fieldToColumnIndex[fieldName] = schema.size();
            schema.push_back(fieldName);

            // Extend existing rows with 0
            for (auto& existingRow : rows) {
                existingRow.push_back(0);
            }
        }
    }

    // Second pass: build row according to schema order
    std::vector<uint32_t> row;
    for (const auto& fieldName : schema) {
        auto elem = metadata[StringData(fieldName)];

        if (elem.eoo()) {
            // Field not in metadata - use missing value sentinel
            row.push_back(0);
        } else {
            // Look up value in symbol dictionary
            std::string fieldValue = elem.String();
            auto symbolResult = symbolDictionary->getOrInsertSymbol(fieldValue);
            if (!symbolResult.isOK()) {
                return symbolResult.getStatus();
            }

            uint32_t symbolIndex = symbolResult.getValue();
            row.push_back(symbolIndex);
        }
    }

    return row;
}

boost::optional<int64_t> AttributeTable::findDuplicateRow(
    const std::vector<uint32_t>& row) const {
    // Linear search through existing rows
    for (size_t rowIdx = 0; rowIdx < rows.size(); ++rowIdx) {
        if (rows[rowIdx] == row) {
            return rowIdx;
        }
    }

    return boost::none;
}

// ============================================================================
// TemporalAttributeTable Implementation
// ============================================================================

TemporalAttributeTable::TemporalAttributeTable(OperationContext* opCtx,
                                               const UUID& collectionUUID,
                                               DictionaryGranularity granularity,
                                               TemporalSymbolDictionary* symbolDictionary)
    : collectionUUID(collectionUUID),
      granularity(granularity),
      opCtx(opCtx),
      temporalSymbolDictionary(symbolDictionary) {}

StatusWith<AttributeTable*> TemporalAttributeTable::getOrCreateTableForTimestamp(
    const Timestamp& timestamp) {
    Timestamp windowStart = calculateWindowStart(timestamp);
    return getOrCreateTable(windowStart);
}

StatusWith<AttributeTable*> TemporalAttributeTable::getTableForTimestamp(
    const Timestamp& timestamp) const {
    std::shared_lock<std::shared_mutex> lock(mutex);

    Timestamp windowStart = calculateWindowStart(timestamp);
    auto it = tables.find(windowStart);

    if (it == tables.end()) {
        return Status(ErrorCodes::NoSuchKey,
                      "No attribute table found for timestamp");
    }

    return it->second.get();
}

StatusWith<int64_t> TemporalAttributeTable::insertRow(const BSONObj& metadata,
                                                      const Timestamp& timestamp) {
    auto tableResult = getOrCreateTableForTimestamp(timestamp);
    if (!tableResult.isOK()) {
        return tableResult.getStatus();
    }

    return tableResult.getValue()->insertRow(metadata);
}

StatusWith<int64_t> TemporalAttributeTable::insertRowDirect(
    const std::vector<uint32_t>& row,
    const Timestamp& timestamp) {
    auto tableResult = getOrCreateTableForTimestamp(timestamp);
    if (!tableResult.isOK()) {
        return tableResult.getStatus();
    }

    return tableResult.getValue()->insertRowDirect(row);
}

boost::optional<std::vector<uint32_t>> TemporalAttributeTable::getRow(
    int64_t rowId,
    const Timestamp& timestamp) const {
    auto tableResult = getTableForTimestamp(timestamp);
    if (!tableResult.isOK()) {
        return boost::none;
    }

    return tableResult.getValue()->getRow(rowId);
}

std::vector<int64_t> TemporalAttributeTable::queryRows(
    const AttributeTablePredicate& predicate,
    const Timestamp& timestamp) const {
    auto tableResult = getTableForTimestamp(timestamp);
    if (!tableResult.isOK()) {
        return {};
    }

    return tableResult.getValue()->queryRows(predicate);
}

std::pair<Timestamp, Timestamp> TemporalAttributeTable::getWindowForTimestamp(
    const Timestamp& timestamp) const {
    Timestamp windowStart = calculateWindowStart(timestamp);
    Timestamp windowEnd = calculateWindowEnd(windowStart);
    return {windowStart, windowEnd};
}

Status TemporalAttributeTable::cleanupOldTables(const Timestamp& beforeTimestamp) {
    std::unique_lock<std::shared_mutex> lock(mutex);

    auto it = tables.begin();
    while (it != tables.end()) {
        Timestamp windowEnd = calculateWindowEnd(it->first);
        if (windowEnd <= beforeTimestamp) {
            it = tables.erase(it);
        } else {
            ++it;
        }
    }

    return Status::OK();
}

TemporalAttributeTable::Stats TemporalAttributeTable::getStats() const {
    std::shared_lock<std::shared_mutex> lock(mutex);

    Stats stats{0, 0, 0};

    for (const auto& [windowStart, table] : tables) {
        stats.totalTables++;
        stats.totalRows += table->getRowCount();
        stats.memoryUsageBytes += table->getMemoryUsageBytes();
    }

    return stats;
}

StatusWith<AttributeTable*> TemporalAttributeTable::getOrCreateTable(
    const Timestamp& windowStart) {
    std::unique_lock<std::shared_mutex> lock(mutex);

    auto it = tables.find(windowStart);
    if (it != tables.end()) {
        return it->second.get();
    }

    // Get or create the symbol dictionary for this window
    auto dictResult = temporalSymbolDictionary->getOrCreateDictionaryForTimestamp(windowStart);
    if (!dictResult.isOK()) {
        return dictResult.getStatus();
    }

    // Create new table with the symbol dictionary for this window
    auto table = std::make_unique<AttributeTable>(dictResult.getValue());
    auto* tablePtr = table.get();
    tables[windowStart] = std::move(table);

    return tablePtr;
}

Timestamp TemporalAttributeTable::calculateWindowStart(const Timestamp& timestamp) const {
    uint32_t seconds = timestamp.getSecs();
    uint32_t windowSizeSeconds = 0;

    switch (granularity) {
        case DictionaryGranularity::DAILY:
            windowSizeSeconds = 24 * 60 * 60;  // 86400 seconds
            break;
        case DictionaryGranularity::HOURLY:
            windowSizeSeconds = 60 * 60;  // 3600 seconds
            break;
        case DictionaryGranularity::THIRTY_MIN:
            windowSizeSeconds = 30 * 60;  // 1800 seconds
            break;
        case DictionaryGranularity::TEN_MIN:
            windowSizeSeconds = 10 * 60;  // 600 seconds
            break;
        case DictionaryGranularity::FIVE_MIN:
            windowSizeSeconds = 5 * 60;  // 300 seconds
            break;
        case DictionaryGranularity::AUTO:
            // Default to HOURLY
            windowSizeSeconds = 60 * 60;
            break;
    }

    uint32_t windowStartSeconds = (seconds / windowSizeSeconds) * windowSizeSeconds;
    return Timestamp(windowStartSeconds, 0);
}

Timestamp TemporalAttributeTable::calculateWindowEnd(const Timestamp& windowStart) const {
    uint32_t seconds = windowStart.getSecs();
    uint32_t windowSizeSeconds = 0;

    switch (granularity) {
        case DictionaryGranularity::DAILY:
            windowSizeSeconds = 24 * 60 * 60;
            break;
        case DictionaryGranularity::HOURLY:
            windowSizeSeconds = 60 * 60;
            break;
        case DictionaryGranularity::THIRTY_MIN:
            windowSizeSeconds = 30 * 60;
            break;
        case DictionaryGranularity::TEN_MIN:
            windowSizeSeconds = 10 * 60;
            break;
        case DictionaryGranularity::FIVE_MIN:
            windowSizeSeconds = 5 * 60;
            break;
        case DictionaryGranularity::AUTO:
            windowSizeSeconds = 60 * 60;
            break;
    }

    return Timestamp(seconds + windowSizeSeconds, 0);
}

}  // namespace mongo::timeseries::hcindex

