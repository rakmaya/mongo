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

#include "mongo/db/exec/timeseries/hcindex/temporal_attribute_table.h"
#include "mongo/db/exec/timeseries/hcindex/temporal_symbol_dictionary.h"
#include "mongo/db/exec/timeseries/hcindex/hcindex_writer.h"
#include "mongo/db/exec/timeseries/hcindex/hcindex_reader.h"
#include "mongo/base/error_codes.h"


namespace mongo::timeseries::hcindex {

// ============================================================================
// AttributeTable Implementation
// ============================================================================

AttributeTable::AttributeTable(SymbolDictionary* symbolDictionary,
                               HCIndexWriter* writer,
                               const Timestamp& windowStart,
                               const Timestamp& windowEnd)
    : symbolDictionary(symbolDictionary)
    , writer(writer)
    , _windowStart(windowStart)
    , _windowEnd(windowEnd)
    , _isDirty(false)
{
}

Status AttributeTable::changeState(AttributeTableState newState) {
    std::unique_lock<std::shared_mutex> lock(mutex);

    // Once in ReadOnly state, no transitions are allowed
    if (_state == AttributeTableState::ReadOnly) {
        return Status(ErrorCodes::InternalError, "Cannot change state of read-only table");
    }

    // Validate state transitions
    if (_state == AttributeTableState::NOP) {
        if (newState != AttributeTableState::Reconstruction &&
            newState != AttributeTableState::ReadWrite) {
            return Status(ErrorCodes::InternalError,
                "Invalid state transition from NOP to " + std::to_string(static_cast<int>(newState)));
        }
    } else if (_state == AttributeTableState::Reconstruction) {
        // From Reconstruction, can transition to ReadWrite (to accept new data) or ReadOnly
        if (newState != AttributeTableState::ReadWrite &&
            newState != AttributeTableState::ReadOnly) {
            return Status(ErrorCodes::InternalError,
                "Invalid state transition from Reconstruction to " + std::to_string(static_cast<int>(newState)));
        }
    } else if (_state == AttributeTableState::ReadWrite) {
        // From ReadWrite, can only transition to ReadOnly
        if (newState != AttributeTableState::ReadOnly) {
            return Status(ErrorCodes::InternalError,
                "Invalid state transition from ReadWrite to " + std::to_string(static_cast<int>(newState)));
        }
    }

    _state = newState;
    return Status::OK();
}

StatusWith<InsertRowResult> AttributeTable::insertRow(const BSONObj& metadata) {
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
        return InsertRowResult{duplicateRowId.value(), false};
    }

    // Insert new row (note: insertRowDirect does not acquire lock, caller must hold it)
    auto insertResult = insertRowDirect(row);
    if (!insertResult.isOK()) {
        return insertResult.getStatus();
    }

    return InsertRowResult{insertResult.getValue(), true};
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
    size_t columnIndex = schema.size();
    fieldToColumnIndex[fieldNameStr] = columnIndex;
    schema.push_back(fieldNameStr);

    // Extend all existing rows with 0 (missing value)
    for (auto& row : rows) {
        row.push_back(0);
    }

    // Notify writer of the new schema field (must be done after schema is updated)
    // Note: writeSchema will check state and call writer->addSchemaField if in ReadWrite mode
    auto status = writeSchema(fieldNameStr, columnIndex);
    if (!status.isOK()) {
        return status;
    }

    return Status::OK();
}

void AttributeTable::flush()
{
    std::unique_lock<std::shared_mutex> lock(mutex);

    if (!_isDirty || writer == nullptr) {
        return;  // Nothing to flush
    }

    // Flush pending operations via the writer
    auto status = writer->flush(_windowStart, _windowEnd, symbolDictionary->getGranularity(), false);
    if (!status.isOK()) {
        return;  // Could not flush
    }

    _isDirty = false;
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

            // Write the schema
            auto schemaStatus = writeSchema(fieldName, schema.size() - 1);
            if (!schemaStatus.isOK()) {
                return schemaStatus;
            }

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

    // Write the row
    auto rowStatus = writeRow(row);
    if (!rowStatus.isOK()) {
        return rowStatus;
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

Status AttributeTable::writeSchema(const std::string& fieldName, size_t columnIndex)
{
    // Check state: modifications only allowed in Reconstruction or ReadWrite modes
    if (_state == AttributeTableState::NOP) {
        return Status(ErrorCodes::InternalError, "Table is in NOP state, cannot add schema");
    }
    if (_state == AttributeTableState::ReadOnly) {
        return Status(ErrorCodes::InternalError, "Table is read-only, cannot add schema");
    }

    // In ReadWrite mode, writer must be set for modifications
    if (_state == AttributeTableState::ReadWrite && writer == nullptr) {
        return Status(ErrorCodes::InternalError, "Table in ReadWrite mode requires a writer");
    }

    // In Reconstruction mode, we don't need a writer
    // In ReadWrite mode, we need to call the writer

    if (_state == AttributeTableState::ReadWrite) {
        // If this is the first schema field, we need to initialize the attribute table.
        // Check if schema had any fields BEFORE this one was added (columnIndex == 0 means first field)
        if (columnIndex == 0) {
            auto stat = writer->initAttributeTable(_windowStart, _windowEnd);
            if (!stat.isOK()) {
                return stat;
            }
        }

        auto stat = writer->addSchemaField(_windowStart, _windowEnd, fieldName);
        if (!stat.isOK()) {
            return stat;
        }

        _isDirty = true;

        auto attrStat = writer->addAttribute(_windowStart, _windowEnd, fieldName, columnIndex);
        if (!attrStat.isOK()) {
            return attrStat;
        }
    }

    return Status::OK();
}

Status AttributeTable::writeRow(const std::vector<uint32_t>& row)
{
    // Check state: modifications only allowed in Reconstruction or ReadWrite modes
    if (_state == AttributeTableState::NOP) {
        return Status(ErrorCodes::InternalError, "Table is in NOP state, cannot add row");
    }
    if (_state == AttributeTableState::ReadOnly) {
        return Status(ErrorCodes::InternalError, "Table is read-only, cannot add row");
    }

    // In ReadWrite mode, writer must be set for modifications
    if (_state == AttributeTableState::ReadWrite && writer == nullptr) {
        return Status(ErrorCodes::InternalError, "Table in ReadWrite mode requires a writer");
    }

    // In ReadWrite mode, notify writer of the new row
    if (_state == AttributeTableState::ReadWrite) {
        // If this is the first schema field, we need to initialize the attribute table.
        if (schema.empty() && rows.empty()) {
            auto stat = writer->initAttributeTable(_windowStart, _windowEnd);
            if (!stat.isOK()) {
                return stat;
            }
        }

        _isDirty = true;
        return writer->addAttributeRow(_windowStart, _windowEnd, row);
    }

    return Status::OK();
}


// ============================================================================
// TemporalAttributeTable Implementation
// ============================================================================

TemporalAttributeTable::TemporalAttributeTable(const UUID& collectionUUID,
                                               DictionaryGranularity granularity,
                                               TemporalSymbolDictionary* symbolDictionary,
                                               HCIndexWriter* writer,
                                               HCIndexReader* reader)
    : collectionUUID(collectionUUID),
      granularity(granularity),
      temporalSymbolDictionary(symbolDictionary),
      writer(writer),
      reader(reader) {}

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

bool TemporalAttributeTable::tableExists(const Timestamp& timestamp) const {
    std::shared_lock<std::shared_mutex> lock(mutex);

    Timestamp windowStart = calculateWindowStart(timestamp);
    return tables.find(windowStart) != tables.end();
}

StatusWith<AttributeTable*> TemporalAttributeTable::getOrCreateTableForTimestamp(
    OperationContext* opCtx,
    const Timestamp& timestamp) {
    Timestamp windowStart = calculateWindowStart(timestamp);
    Timestamp windowEnd = calculateWindowEnd(windowStart);

    // Check if table already exists
    {
        std::shared_lock<std::shared_mutex> lock(mutex);
        auto it = tables.find(windowStart);
        if (it != tables.end()) {
            return it->second.get();
        }
    }

    // Table doesn't exist in memory. Try to reconstruct from disk if reader is available.
    if (!reader) {
        // No reader available, create a new empty table
        return getOrCreateTable(opCtx, windowStart);
    }

    // Try to reconstruct the table from disk
    // First, get or create the symbol dictionary for this window
    auto dictResult = temporalSymbolDictionary->getOrCreateDictionaryForTimestamp(opCtx, timestamp);
    if (!dictResult.isOK()) {
        return dictResult.getStatus();
    }
    auto* dictPtr = dictResult.getValue();

    // Reconstruct the attribute table from disk
    auto tableResult = reader->constructAttributeTable(
        opCtx, windowStart, windowEnd, granularity, timestamp, dictPtr);
    if (!tableResult.isOK()) {
        return tableResult.getStatus();
    }

    // Store the reconstructed table in memory for future use
    std::unique_lock<std::shared_mutex> writeLock(mutex);
    auto* tablePtr = tableResult.getValue().get();

    // Set the writer on the reconstructed table so it can accept new data
    if (writer) {
        tablePtr->setWriter(writer);
    }

    tables[windowStart] = std::move(tableResult.getValue());

    return tablePtr;
}

StatusWith<InsertRowResult> TemporalAttributeTable::insertRow(OperationContext* opCtx,
                                                              const BSONObj& metadata,
                                                              const Timestamp& timestamp) {
    auto tableResult = getOrCreateTableForTimestamp(opCtx, timestamp);
    if (!tableResult.isOK()) {
        return tableResult.getStatus();
    }

    return tableResult.getValue()->insertRow(metadata);
}

StatusWith<int64_t> TemporalAttributeTable::insertRowDirect(
    OperationContext* opCtx,
    const std::vector<uint32_t>& row,
    const Timestamp& timestamp) {
    auto tableResult = getOrCreateTableForTimestamp(opCtx, timestamp);
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


void TemporalAttributeTable::flush()
{
    std::unique_lock<std::shared_mutex> lock(mutex);

    // Flush all tables
    for (auto& [windowStart, table] : tables) {
        // Flush the table
        table->flush();
    }
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
    OperationContext* opCtx,
    const Timestamp& windowStart) {
    std::unique_lock<std::shared_mutex> lock(mutex);

    auto it = tables.find(windowStart);
    if (it != tables.end()) {
        return it->second.get();
    }

    // Get or create the symbol dictionary for this window
    auto dictResult = temporalSymbolDictionary->getOrCreateDictionaryForTimestamp(opCtx, windowStart);
    if (!dictResult.isOK()) {
        return dictResult.getStatus();
    }

    // Create new table with the symbol dictionary for this window
    Timestamp windowEnd = calculateWindowEnd(windowStart);
    auto table = std::make_unique<AttributeTable>(dictResult.getValue(), writer, windowStart, windowEnd);

    // Change state to ReadWrite for new tables created by TemporalAttributeTable
    auto stateStatus = table->changeState(AttributeTableState::ReadWrite);
    if (!stateStatus.isOK()) {
        return stateStatus;
    }

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

