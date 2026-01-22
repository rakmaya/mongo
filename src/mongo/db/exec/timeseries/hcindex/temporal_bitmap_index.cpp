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

#include "mongo/db/exec/timeseries/hcindex/temporal_bitmap_index.h"

#include "mongo/db/exec/timeseries/hcindex/reader.h"
#include "mongo/db/exec/timeseries/hcindex/writer.h"
#include "mongo/logv2/log.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kQuery

namespace mongo::timeseries::hcindex {

                        // ------------------------------
                        // class TemporalSymbolDictionary
                        // ------------------------------

//- CONSTRUCTORS


TemporalBitmapIndex::TemporalBitmapIndex(const UUID& collectionUUID,
                                         HCIndexPeriodEnum period,
                                         int32_t frequency,
                                         HCIndexWriter* writer,
                                         HCIndexReader* reader)
    : _collectionUUID(collectionUUID),
      _period(period),
      _frequency(frequency),
      _writer(writer),
      _reader(reader),
      _doRecomputeIndexedColumns(false) {
}


//- ACCESSORS


std::set<int64_t> TemporalBitmapIndex::queryRowIds(const std::vector<uint32_t>& predicate,
                                                   const Timestamp& timestamp) const {
    auto indexResult = getIndexForTimestamp(timestamp);
    if (!indexResult.isOK()) {
        return {};
    }
    return indexResult.getValue()->queryRowIdsAnd(predicate);
}

std::set<int64_t> TemporalBitmapIndex::queryRowIds(size_t columnIndex,
                                                   uint32_t symbolIndex,
                                                   const Timestamp& timestamp) const {
    auto indexResult = getIndexForTimestamp(timestamp);
    if (!indexResult.isOK()) {
        return {};
    }
    return indexResult.getValue()->getRowIds(columnIndex, symbolIndex);
}

bool TemporalBitmapIndex::hasIndexForColumn(size_t columnIndex, const Timestamp& timestamp) const {
    auto indexResult = getIndexForTimestamp(timestamp);
    if (!indexResult.isOK()) {
        return false;
    }
    return indexResult.getValue()->hasIndexForColumn(columnIndex);
}

std::pair<Timestamp, Timestamp> TemporalBitmapIndex::getWindowForTimestamp(
    const Timestamp& timestamp) const {
    Timestamp windowStart = calculateWindowStart(timestamp);
    Timestamp windowEnd = calculateWindowEnd(windowStart);
    return {windowStart, windowEnd};
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


//- MODIFIERS


StatusWith<BitmapIndex*> TemporalBitmapIndex::getOrCreateIndexForTimestamp(
    OperationContext* opCtx, const Timestamp& timestamp) {
    Timestamp windowStart = calculateWindowStart(timestamp);
    return getOrCreateIndex(opCtx, windowStart);
}

Status TemporalBitmapIndex::addRow(OperationContext* opCtx,
                                   int64_t rowId,
                                   const std::vector<uint32_t>& row,
                                   const Timestamp& timestamp) {
    auto indexResult = getOrCreateIndexForTimestamp(opCtx, timestamp);
    if (!indexResult.isOK()) {
        return indexResult.getStatus();
    }

    // If the schema has changed, we need to let bitmap recompute the set of
    // indexed columns.
    if (_doRecomputeIndexedColumns) {
        indexResult.getValue()->setIncludedColumns(_includedColumns);
        _doRecomputeIndexedColumns = false;
    }
    return indexResult.getValue()->addRow(rowId, row);
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

void TemporalBitmapIndex::flush() {
    std::shared_lock lock(_mutex);
    for (auto& [windowStart, index] : _indexes) {
        index->flush();
    }
}

void TemporalBitmapIndex::setExcludedColumns(std::unordered_set<std::size_t> excludedColumns) {
    std::shared_lock lock(_mutex);
    _excludedColumns = std::move(excludedColumns);
    _doRecomputeIndexedColumns = true;
}

void TemporalBitmapIndex::setIncludedColumns(std::unordered_set<std::size_t> includedColumns) {
    std::shared_lock lock(_mutex);
    _includedColumns = std::move(includedColumns);
    _doRecomputeIndexedColumns = true;
}


//- PRIVATE METHODS


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
    if (_reader) {
        auto windowEnd = calculateWindowEnd(windowStart);
        auto reconstructResult = _reader->constructBitmapIndex(
            opCtx, windowStart, windowEnd, _period, _frequency, windowStart);

        // If reconstruction succeeds and dictionary has symbols, use it
        if (reconstructResult.isOK() && reconstructResult.getValue().get()->getEntryCount() > 0) {
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
    auto index =
        std::make_unique<BitmapIndex>(_period, _frequency, windowStart, windowEnd, _writer);

    // Change state to ReadWrite for new indexes created by TemporalBitmapIndex
    auto status = index->changeState(BitmapIndexState::ReadWrite);
    if (!status.isOK()) {
        return status;
    }

    auto* indexPtr = index.get();
    _indexes[windowStart] = std::move(index);

    return indexPtr;
}


}  // namespace mongo::timeseries::hcindex
