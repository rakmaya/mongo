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

#include "mongo/base/error_codes.h"
#include "mongo/db/exec/timeseries/hcindex/bitmap_index.h"
#include "mongo/db/exec/timeseries/hcindex/temporal_bitmap_index.h"
#include "mongo/db/exec/timeseries/hcindex/reader.h"
#include "mongo/db/exec/timeseries/hcindex/temporal_symbol_dictionary.h"
#include "mongo/db/exec/timeseries/hcindex/writer.h"
#include "mongo/logv2/log.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kQuery

namespace mongo::timeseries::hcindex {

TemporalAttributeTable::TemporalAttributeTable(const UUID& collectionUUID,
                                               HCIndexPeriodEnum period,
                                               int32_t frequency,
                                               TemporalSymbolDictionary* symbolDictionary,
                                               TemporalBitmapIndex* bitmapIndex,
                                               HCIndexWriter* writer,
                                               HCIndexReader* reader)
    : collectionUUID(collectionUUID),
      period(period),
      frequency(frequency),
      temporalSymbolDictionary(symbolDictionary),
      temporalBitmapIndex(bitmapIndex),
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

    if (!reader) {
        // No reader available, create a new empty table
        return getOrCreateTable(opCtx, windowStart);
    }

    // First, get or create the symbol dictionary for this window
    auto dictResult = temporalSymbolDictionary->getOrCreateDictionaryForTimestamp(opCtx, timestamp);
    if (!dictResult.isOK()) {
        return dictResult.getStatus();
    }
    auto* dictPtr = dictResult.getValue();

    // Reconstruct the attribute table from disk
    auto tableResult = reader->constructAttributeTable(
        opCtx, windowStart, windowEnd, period, frequency, timestamp, dictPtr);
    if (!tableResult.isOK()) {
        return tableResult.getStatus();
    }

    auto* tablePtr = tableResult.getValue().get();
    if (tablePtr->getRowCount() == 0) {
        // If the table is empty, then we need a new table for this window
        return getOrCreateTable(opCtx, windowStart);
    }

    std::unique_lock<std::shared_mutex> writeLock(mutex);
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

    auto insertStatus = tableResult.getValue()->insertRow(metadata);

    // Insert the metadata row into the attribute table
    if (!insertStatus.isOK()) {
        return insertStatus;
    }

    // Extract the result - we get the rowId, isNewRow flag, and the row vector
    auto& insertResult = insertStatus.getValue();
    int64_t rowId = insertResult.rowId;
    bool hasSchemaChanged = insertResult.hasSchemaChanged;
    const std::vector<uint32_t>& row = insertResult.row;

    // Add the row to the bitmap index for fast metadata predicate lookups
    // We add both new rows and duplicates to the bitmap index since the bitmap
    // tracks (column, value) -> rowIds mapping which needs all rowIds
    if (temporalBitmapIndex) {
        if (hasSchemaChanged) {
            auto const& fieldMap = insertResult.table->getFieldToColumnIndexMap();
            std::unordered_set<std::size_t> includedColumnIndices;

            // For now, this is a hack. We should be using the information gain
            // to determine which columns to include in the bitmap index and then
            // overriding it with the user-specified included/excluded columns.
            if (!includedIndexColumns.empty()) {
                for (const auto& [fieldName, colIdx] : fieldMap) {
                    if (includedIndexColumns.find(fieldName) != includedIndexColumns.end()) {
                        includedColumnIndices.insert(colIdx);
                    }
                }
            } else {
                for (const auto& [fieldName, colIdx] : fieldMap) {
                    if (excludedIndexColumns.find(fieldName) == excludedIndexColumns.end()) {
                        includedColumnIndices.insert(colIdx);
                    }
                }
            }

            temporalBitmapIndex->setIncludedColumns(includedColumnIndices);
        }

        auto bitmapStatus = temporalBitmapIndex->addRow(opCtx, rowId, row, timestamp);
        if (!bitmapStatus.isOK()) {
            LOGV2_WARNING(9999990,
                          "Failed to add row to bitmap index",
                          "rowId"_attr = rowId,
                          "error"_attr = bitmapStatus);
            // Continue anyway - bitmap index is an optimization, not critical
            // TODO: Remove this log and replace with error stats?
        }
    }

    // TODO Collect statistics and find information gain on the index.

    return insertStatus;
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
    const Timestamp& timestamp,
    BitmapIndex* bitmapIndex) const {
    auto tableResult = getTableForTimestamp(timestamp);
    if (!tableResult.isOK()) {
        return {};
    }

    return tableResult.getValue()->queryRows(predicate, bitmapIndex);
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

void TemporalAttributeTable::setExcludedIndexColumns(std::unordered_set<std::string> excludedColumns)
{
    excludedIndexColumns = std::move(excludedColumns);
}

void TemporalAttributeTable::setIncludedIndexColumns(std::unordered_set<std::string> includedColumns)
{
    includedIndexColumns = std::move(includedColumns);
}

void TemporalAttributeTable::flush()
{
    std::unique_lock<std::shared_mutex> lock(mutex);

    for (auto& [windowStart, table] : tables) {
        if (table) {
            table->flush();
        }
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
    auto table = std::make_unique<AttributeTable>(dictResult.getValue(), writer, period, frequency, windowStart, windowEnd);

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
    uint32_t windowSizeSeconds = 0;

    switch (period) {
        case HCIndexPeriodEnum::Hour:
            windowSizeSeconds = frequency * 60 * 60;  // frequency hours in seconds
            break;
        case HCIndexPeriodEnum::Minute:
            windowSizeSeconds = frequency * 60;  // frequency minutes in seconds
            break;
        case HCIndexPeriodEnum::Second:
            windowSizeSeconds = frequency;  // frequency seconds
            break;
    }

    uint32_t seconds = timestamp.getSecs();
    uint32_t windowStartSeconds = (seconds / windowSizeSeconds) * windowSizeSeconds;
    return Timestamp(windowStartSeconds, 0);
}

Timestamp TemporalAttributeTable::calculateWindowEnd(const Timestamp& windowStart) const {
    uint32_t windowSizeSeconds = 0;

    switch (period) {
        case HCIndexPeriodEnum::Hour:
            windowSizeSeconds = frequency * 60 * 60;  // frequency hours in seconds
            break;
        case HCIndexPeriodEnum::Minute:
            windowSizeSeconds = frequency * 60;  // frequency minutes in seconds
            break;
        case HCIndexPeriodEnum::Second:
            windowSizeSeconds = frequency;  // frequency seconds
            break;
    }

    uint32_t seconds = windowStart.getSecs();
    return Timestamp(seconds + windowSizeSeconds, 0);
}

}  // namespace mongo::timeseries::hcindex

