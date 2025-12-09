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

#include "mongo/db/exec/timeseries/hcindex/bitmap_index.h"

#include "mongo/db/exec/timeseries/hcindex/hcindex_writer.h"
#include "mongo/db/exec/timeseries/hcindex/hcindex_reader.h"
#include "mongo/logv2/log.h"

#include <algorithm>

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kQuery

namespace mongo::timeseries::hcindex {

// ============================================================================
// BitmapIndex Implementation
// ============================================================================

BitmapIndex::BitmapIndex(HCIndexPeriodEnum period,
                         int32_t frequency,
                         Timestamp windowStart,
                         Timestamp windowEnd,
                         HCIndexWriter* writer)
    : _period(period),
      _frequency(frequency),
      _windowStart(windowStart),
      _windowEnd(windowEnd),
      _writer(writer) {}

Status BitmapIndex::addEntry(size_t columnIndex, uint32_t symbolIndex, int64_t rowId) {
    std::unique_lock lock(_mutex);
    return addEntryHelper(columnIndex, symbolIndex, rowId);
}

Status BitmapIndex::addEntryHelper(size_t columnIndex, uint32_t symbolIndex, int64_t rowId) {
    if (_state != BitmapIndexState::ReadWrite && _state != BitmapIndexState::Reconstruction) {
        return Status(ErrorCodes::IllegalOperation,
                      "Cannot add entry to bitmap index: index is not in ReadWrite or "
                      "Reconstruction state");
    }

    // Skip missing values (symbolIndex == 0)
    if (symbolIndex == 0) {
        return Status::OK();
    }

    // If key exists and if rowId is already present, then we got nothing to do.
    // Ideally, this should not happen since this function is either called from
    // addRow or called only from the reconstruction path. And addRow is only
    // called when the ingestion path encounters a new rowId.
    auto colIt = _bitmaps.find(columnIndex);
    if (colIt != _bitmaps.end()) {
        auto symIt = colIt->second.find(symbolIndex);
        if (symIt != colIt->second.end() && symIt->second.find(rowId) != symIt->second.end()) {
            return Status::OK();
        }
    }

    if (_state == BitmapIndexState::ReadWrite) {
        // Writer must be set in ReadWrite mode
        if (_writer == nullptr) {
            return Status(ErrorCodes::InternalError,
                          "Could not write. Writer not set in ReadWrite mode");
        }

        // If this is an empty bitmap index, we need to do an INIT operation
        if (_bitmaps.empty()) {
            auto initStatus = _writer->initBitmapIndex(_windowStart, _windowEnd);
            if (!initStatus.isOK()) {
                return Status(ErrorCodes::InternalError,
                              "Could not initialize bitmap index for windowStart: " +
                                  _windowStart.toString());
            }
        }

        // Notify the writer of the new entry
        std::set<int64_t> rowIds{rowId};
        auto status =
            _writer->addBitmapEntry(_windowStart, _windowEnd, columnIndex, symbolIndex, rowIds);
        if (!status.isOK()) {
            return status;
        }

        _bitmaps[columnIndex][symbolIndex].insert(rowId);
        _isDirty = true;
    } else {
        // Reconstruction mode, just update the in-memory index
        _bitmaps[columnIndex][symbolIndex].insert(rowId);
    }

    return Status::OK();
}

Status BitmapIndex::addRow(int64_t rowId, const std::vector<uint32_t>& row) {
    std::unique_lock lock(_mutex);
    if (_state != BitmapIndexState::ReadWrite && _state != BitmapIndexState::Reconstruction) {
        return Status(ErrorCodes::IllegalOperation,
                      "Cannot add row to bitmap index: index is not in ReadWrite or "
                      "Reconstruction state");
    }

    for (size_t columnIndex = 0; columnIndex < row.size(); ++columnIndex) {
        uint32_t symbolIndex = row[columnIndex];
        // Skip missing values (symbolIndex == 0)
        if (symbolIndex != 0) {
            auto status = addEntryHelper(columnIndex, symbolIndex, rowId);
            if (!status.isOK()) {
                return status;
            }
        }
    }
    return Status::OK();
}

std::set<int64_t> BitmapIndex::getRowIds(size_t columnIndex, uint32_t symbolIndex) const {
    std::shared_lock lock(_mutex);

    auto colIt = _bitmaps.find(columnIndex);
    if (colIt == _bitmaps.end()) {
        return {};
    }
    auto symIt = colIt->second.find(symbolIndex);
    if (symIt == colIt->second.end()) {
        return {};
    }
    return symIt->second;
}

std::set<int64_t> BitmapIndex::queryRowIdsAnd(const std::vector<uint32_t>& predicate) const {
    std::shared_lock lock(_mutex);

    std::set<int64_t> result;
    bool firstMatch = true;

    for (size_t columnIndex = 0; columnIndex < predicate.size(); ++columnIndex) {
        uint32_t symbolIndex = predicate[columnIndex];
        // Skip "don't care" columns (symbolIndex == 0)
        if (symbolIndex == 0) {
            continue;
        }

        auto colIt = _bitmaps.find(columnIndex);
        if (colIt == _bitmaps.end()) {
            // No bitmap for this column - no matches possible
            return {};
        }
        auto symIt = colIt->second.find(symbolIndex);
        if (symIt == colIt->second.end()) {
            // No bitmap for this (column, value) - no matches possible
            return {};
        }

        if (firstMatch) {
            result = symIt->second;
            firstMatch = false;
        } else {
            // Intersect with current result
            std::set<int64_t> intersection;
            std::set_intersection(result.begin(), result.end(),
                                  symIt->second.begin(), symIt->second.end(),
                                  std::inserter(intersection, intersection.begin()));
            result = std::move(intersection);
        }

        // Early exit if result is empty
        if (result.empty()) {
            return {};
        }
    }

    return result;
}

std::set<int64_t> BitmapIndex::queryRowIdsOr(
    const std::vector<std::vector<uint32_t>>& predicates) const {
    std::set<int64_t> result;

    for (const auto& predicate : predicates) {
        auto matches = queryRowIdsAnd(predicate);
        result.insert(matches.begin(), matches.end());
    }

    return result;
}

Status BitmapIndex::changeState(BitmapIndexState newState) {
    std::unique_lock lock(_mutex);

    // Validate state transitions
    switch (_state) {
        case BitmapIndexState::NOP:
            if (newState != BitmapIndexState::Reconstruction &&
                newState != BitmapIndexState::ReadWrite) {
                return Status(ErrorCodes::IllegalOperation,
                              "Invalid state transition from NOP: can only transition to "
                              "Reconstruction or ReadWrite");
            }
            break;
        case BitmapIndexState::Reconstruction:
            // From Reconstruction, can transition to ReadWrite (to accept new
            // bitmaps)  or ReadOnly
            if (newState != BitmapIndexState::ReadOnly &&
                newState != BitmapIndexState::ReadWrite) {
                return Status(ErrorCodes::IllegalOperation,
                              "Invalid state transition from Reconstruction: can only transition "
                              "to ReadOnly");
            }
            break;
        case BitmapIndexState::ReadWrite:
            if (newState != BitmapIndexState::ReadOnly) {
                return Status(ErrorCodes::IllegalOperation,
                              "Invalid state transition from ReadWrite: can only transition to "
                              "ReadOnly");
            }
            break;
        case BitmapIndexState::ReadOnly:
            return Status(ErrorCodes::IllegalOperation,
                          "Cannot transition from ReadOnly state");
    }

    _state = newState;
    return Status::OK();
}

BitmapIndexState BitmapIndex::getState() const {
    std::shared_lock lock(_mutex);
    return _state;
}

bool BitmapIndex::hasIndexForColumn(size_t columnIndex) const
{
    std::shared_lock lock(_mutex);
    return _bitmaps.find(columnIndex) != _bitmaps.end();
}

void BitmapIndex::flush() {
    std::unique_lock lock(_mutex);
    if (!_isDirty || _writer == nullptr) {
        return;
    }

    // Flush the accumulated entries to pending operations
    if (!_writer->flushBitmaps(_windowStart, _windowEnd, _period, _frequency).isOK()) {
        return;  // Could not flush
    }

    _isDirty = false;
}

bool BitmapIndex::isEmpty() const
{
    std::shared_lock lock(_mutex);
    return _bitmaps.empty();
}

size_t BitmapIndex::getEntryCount() const {
    std::shared_lock lock(_mutex);
    // Count total number of (column, symbol) pairs
    size_t count = 0;
    for (const auto& [columnIndex, symbolMap] : _bitmaps) {
        count += symbolMap.size();
    }
    return count;
}

size_t BitmapIndex::getTotalRowIdCount() const {
    std::shared_lock lock(_mutex);
    size_t total = 0;
    for (const auto& [columnIndex, symbolMap] : _bitmaps) {
        for (const auto& [symbolIndex, rowIds] : symbolMap) {
            total += rowIds.size();
        }
    }
    return total;
}

size_t BitmapIndex::getMemoryUsageBytes() const {
    std::shared_lock lock(_mutex);
    // Approximate memory usage:
    // - Outer map overhead + per-column overhead
    // - Inner map overhead + per-symbol overhead
    // - Per-element in set (8 bytes + tree node overhead)
    size_t usage = sizeof(BitmapIndex);
    for (const auto& [columnIndex, symbolMap] : _bitmaps) {
        usage += sizeof(size_t);                  // Column key
        usage += 56;                              // unordered_map overhead (approximate)
        for (const auto& [symbolIndex, rowIds] : symbolMap) {
            usage += sizeof(uint32_t);            // Symbol key
            usage += 40;                          // Set overhead (approximate)
            usage += rowIds.size() * 32;          // Each element in set (with tree node overhead)
        }
    }
    return usage;
}

// ============================================================================
// TemporalBitmapIndex Implementation
// ============================================================================

TemporalBitmapIndex::TemporalBitmapIndex(const UUID& collectionUUID,
                                         HCIndexPeriodEnum period,
                                         int32_t frequency,
                                         HCIndexWriter* writer,
                                         HCIndexReader* reader)
    : _collectionUUID(collectionUUID),
      _period(period),
      _frequency(frequency),
      _writer(writer),
      _reader(reader) {}

Timestamp TemporalBitmapIndex::calculateWindowStart(const Timestamp& timestamp) const {
    uint32_t secs = timestamp.getSecs();
    uint32_t windowSeconds;

    switch (_period) {
        case HCIndexPeriodEnum::Hour:
            windowSeconds = 3600 * _frequency;
            break;
        case HCIndexPeriodEnum::Minute:
            windowSeconds = 60 * _frequency;
            break;
        case HCIndexPeriodEnum::Second:
            windowSeconds = _frequency;
            break;
        default:
            // Default to hourly
            windowSeconds = 3600;
    }

    uint32_t windowStart = (secs / windowSeconds) * windowSeconds;
    return Timestamp(windowStart, 0);
}

Timestamp TemporalBitmapIndex::calculateWindowEnd(const Timestamp& windowStart) const {
    uint32_t windowSeconds;

    switch (_period) {
        case HCIndexPeriodEnum::Hour:
            windowSeconds = 3600 * _frequency;
            break;
        case HCIndexPeriodEnum::Minute:
            windowSeconds = 60 * _frequency;
            break;
        case HCIndexPeriodEnum::Second:
            windowSeconds = _frequency;
            break;
        default:
            windowSeconds = 3600;
    }

    return Timestamp(windowStart.getSecs() + windowSeconds, 0);
}

std::pair<Timestamp, Timestamp> TemporalBitmapIndex::getWindowForTimestamp(
    const Timestamp& timestamp) const {
    Timestamp windowStart = calculateWindowStart(timestamp);
    Timestamp windowEnd = calculateWindowEnd(windowStart);
    return {windowStart, windowEnd};
}

StatusWith<BitmapIndex*> TemporalBitmapIndex::getOrCreateIndex(OperationContext* opCtx,
                                                               const Timestamp& windowStart) {
    // First check if index exists (read lock)
    {
        std::shared_lock lock(_mutex);
        auto it = _indexes.find(windowStart);
        if (it != _indexes.end()) {
            return it->second.get();
        }
    }

    // Index doesn't exist, create it (write lock)
    std::unique_lock lock(_mutex);

    // Double-check after acquiring write lock
    auto it = _indexes.find(windowStart);
    if (it != _indexes.end()) {
        return it->second.get();
    }

    // Bitmap Index for this window doesn't exist, we need to create it.
    // However, we need to check if the storage has the index.
    LOGV2(9999940, "HCIndex: getOrCreateIndex - checking reader",
          "hasReader"_attr = (_reader != nullptr),
          "windowStart"_attr = windowStart);
    if (_reader) {
        auto windowEnd = calculateWindowEnd(windowStart);
        auto reconstructResult = _reader->constructBitmapIndex(
            opCtx, windowStart, windowEnd, _period, _frequency, windowStart);

        LOGV2(9999941, "HCIndex: getOrCreateIndex - reconstruction result",
              "isOK"_attr = reconstructResult.isOK(),
              "entryCount"_attr = (reconstructResult.isOK() ?
                  reconstructResult.getValue().get()->getEntryCount() : 0));

        // If reconstruction succeeds and dictionary has symbols, use it
        if (reconstructResult.isOK() && reconstructResult.getValue().get()->getEntryCount() > 0) {
            LOGV2(9999920, "HCIndex: Reconstructed bitmap index for window", "windowStart"_attr = windowStart);
            auto* bitmapPtr = reconstructResult.getValue().get();

            // Set the writer on the reconstructed dictionary so it can accept new symbols
            if (_writer) {
                bitmapPtr->setWriter(_writer);
            }

            _indexes[windowStart] = std::move(reconstructResult.getValue());
            return bitmapPtr;
        }
        // If reconstruction fails, fall through to create a new index. This
        // is not so great.  TODO: Add some flags so we can detect between lack
        // of data and missing data.
    }

    // Create new index
    Timestamp windowEnd = calculateWindowEnd(windowStart);
    auto index = std::make_unique<BitmapIndex>(_period, _frequency, windowStart, windowEnd, _writer);

    // Change state to ReadWrite for new indexes created by TemporalBitmapIndex
    auto status = index->changeState(BitmapIndexState::ReadWrite);
    if (!status.isOK()) {
        return status;
    }

    auto* indexPtr = index.get();
    _indexes[windowStart] = std::move(index);

    return indexPtr;
}

StatusWith<BitmapIndex*> TemporalBitmapIndex::getOrCreateIndexForTimestamp(
    OperationContext* opCtx, const Timestamp& timestamp) {
    Timestamp windowStart = calculateWindowStart(timestamp);
    return getOrCreateIndex(opCtx, windowStart);
}

StatusWith<BitmapIndex*> TemporalBitmapIndex::getIndexForTimestamp(
    const Timestamp& timestamp) const {
    std::shared_lock lock(_mutex);

    Timestamp windowStart = calculateWindowStart(timestamp);
    auto it = _indexes.find(windowStart);
    if (it == _indexes.end()) {
        return Status(ErrorCodes::NoSuchKey,
                      str::stream() << "No bitmap index found for timestamp " << timestamp);
    }
    return it->second.get();
}

Status TemporalBitmapIndex::addRow(OperationContext* opCtx,
                                   int64_t rowId,
                                   const std::vector<uint32_t>& row,
                                   const Timestamp& timestamp) {
    auto indexResult = getOrCreateIndexForTimestamp(opCtx, timestamp);
    if (!indexResult.isOK()) {
        return indexResult.getStatus();
    }
    return indexResult.getValue()->addRow(rowId, row);
}

std::set<int64_t> TemporalBitmapIndex::queryRowIds(const std::vector<uint32_t>& predicate,
                                                   const Timestamp& timestamp) const {
    auto indexResult = getIndexForTimestamp(timestamp);
    if (!indexResult.isOK()) {
        return {};
    }
    return indexResult.getValue()->queryRowIdsAnd(predicate);
}

std::set<int64_t> TemporalBitmapIndex::queryRowIds(size_t columnIndex, uint32_t symbolIndex, const Timestamp& timestamp) const {
    auto indexResult = getIndexForTimestamp(timestamp);
    if (!indexResult.isOK()) {
        return {};
    }
    return indexResult.getValue()->getRowIds(columnIndex, symbolIndex);
}

Status TemporalBitmapIndex::cleanupOldIndexes(const Timestamp& beforeTimestamp) {
    std::unique_lock lock(_mutex);

    auto it = _indexes.begin();
    while (it != _indexes.end()) {
        if (it->first < beforeTimestamp) {
            it = _indexes.erase(it);
        } else {
            // Since the map is ordered, we can stop once we reach timestamps >= beforeTimestamp
            break;
        }
    }

    return Status::OK();
}

bool TemporalBitmapIndex::hasIndexForColumn(size_t columnIndex, const Timestamp& timestamp) const
{
    auto indexResult = getIndexForTimestamp(timestamp);
    if (!indexResult.isOK()) {
        return false;
    }
    return indexResult.getValue()->hasIndexForColumn(columnIndex);
}

void TemporalBitmapIndex::flush() {
    std::shared_lock lock(_mutex);
    for (auto& [windowStart, index] : _indexes) {
        index->flush();
    }
}

TemporalBitmapIndex::Stats TemporalBitmapIndex::getStats() const {
    std::shared_lock lock(_mutex);

    Stats stats{};
    stats.totalIndexes = _indexes.size();

    for (const auto& [windowStart, index] : _indexes) {
        stats.totalEntries += index->getEntryCount();
        stats.memoryUsageBytes += index->getMemoryUsageBytes();
    }

    return stats;
}

}  // namespace mongo::timeseries::hcindex

