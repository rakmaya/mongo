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

#include "mongo/db/exec/timeseries/hcindex/reader.h"
#include "mongo/db/exec/timeseries/hcindex/writer.h"
#include "mongo/logv2/log.h"

#include <algorithm>

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kQuery

namespace mongo::timeseries::hcindex {

                        // -----------------
                        // class BitmapIndex
                        // -----------------

//- CONSTRUCTORS


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


//- ACCESSORS


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

    // Convert Roaring64BTree to std::set<int64_t> for API compatibility
    // TODO: Consider changing API to return Roaring64BTree directly for better performance
    std::set<int64_t> result;
    for (uint64_t rowId : symIt->second) {
        result.insert(rowId);
    }
    return result;
}

std::set<int64_t> BitmapIndex::queryRowIdsAnd(const std::vector<uint32_t>& predicate) const {
    std::shared_lock lock(_mutex);

    // Collect pointers to all relevant Roaring64BTree bitmaps
    std::vector<const Roaring64BTree*> bitmaps;

    for (size_t columnIndex = 0; columnIndex < predicate.size(); ++columnIndex) {
        uint32_t symbolIndex = predicate[columnIndex];

        if (symbolIndex == 0) {
            // Skip "don't care" columns (symbolIndex == 0)
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

        bitmaps.push_back(&symIt->second);
    }

    if (bitmaps.empty()) {
        return {};
    }

    std::set<int64_t> result;

    if (bitmaps.size() == 1) {
        // Single bitmap - just convert to set
        for (uint64_t rowId : *bitmaps[0]) {
            result.insert(rowId);
        }
    } else {
        // Multiple bitmaps - perform intersection by iterating first bitmap
        // and checking membership in all others using contains()
        // This is more efficient than std::set_intersection for Roaring bitmaps
        for (uint64_t rowId : *bitmaps[0]) {
            bool inAll = true;
            for (size_t i = 1; i < bitmaps.size(); ++i) {
                if (!bitmaps[i]->contains(rowId)) {
                    inAll = false;
                    break;
                }
            }
            if (inAll) {
                result.insert(rowId);
            }
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

BitmapIndexState BitmapIndex::getState() const {
    std::shared_lock lock(_mutex);
    return _state;
}

bool BitmapIndex::hasIndexForColumn(size_t columnIndex) const {
    std::shared_lock lock(_mutex);
    return _bitmaps.find(columnIndex) != _bitmaps.end();
}

bool BitmapIndex::isEmpty() const {
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
        for (const auto& [symbolIndex, roaringBitmap] : symbolMap) {
            // Count elements by iterating (Roaring64BTree doesn't have O(1) cardinality)
            // Note: Could be optimized by maintaining a cached count
            for (auto it = roaringBitmap.begin(); it != roaringBitmap.end(); ++it) {
                ++total;
            }
        }
    }
    return total;
}

size_t BitmapIndex::getMemoryUsageBytes() const {
    std::shared_lock lock(_mutex);
    // Approximate memory usage:
    // - Outer map overhead + per-column overhead
    // - Inner map overhead + per-symbol overhead
    // - Roaring64BTree memory (uses getApproximateSize())
    size_t usage = sizeof(BitmapIndex);
    for (const auto& [columnIndex, symbolMap] : _bitmaps) {
        usage += sizeof(size_t);  // Column key
        usage += 56;              // unordered_map overhead (approximate)
        for (const auto& [symbolIndex, roaringBitmap] : symbolMap) {
            usage += sizeof(uint32_t);  // Symbol key
            usage += 40;                // Map entry overhead (approximate)
            // Use Roaring64BTree's built-in memory tracking
            usage += roaringBitmap.getApproximateSize();
        }
    }
    return usage;
}


//- MODIFIERS


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
        if (symIt != colIt->second.end() && symIt->second.contains(rowId)) {
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

        _bitmaps[columnIndex][symbolIndex].add(rowId);
        _isDirty = true;
    } else {
        // Reconstruction mode, just update the in-memory index
        _bitmaps[columnIndex][symbolIndex].add(rowId);
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
        if (_indexedColumns.find(columnIndex) == _indexedColumns.end()) {
            continue;
        }
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
            // bitmaps) or ReadOnly
            if (newState != BitmapIndexState::ReadOnly && newState != BitmapIndexState::ReadWrite) {
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
            return Status(ErrorCodes::IllegalOperation, "Cannot transition from ReadOnly state");
    }

    _state = newState;
    return Status::OK();
}

void BitmapIndex::setExcludedColumns(std::unordered_set<std::size_t> excludedColumns) {
    std::shared_lock lock(_mutex);
    _excludedColumns = std::move(excludedColumns);
}

void BitmapIndex::setIncludedColumns(std::unordered_set<std::size_t> includedColumns) {
    std::shared_lock lock(_mutex);

    // For now, this just overwrites.
    // TODO: Merge with the computed columns based on the information gain
    _indexedColumns = std::move(includedColumns);
}


void BitmapIndex::flush() {
    std::unique_lock lock(_mutex);
    if (!_isDirty || _writer == nullptr) {
        return;
    }

    // Flush the accumulated entries to pending operations
    if (!_writer->flushBitmaps(_windowStart, _windowEnd, _period, _frequency).isOK()) {
        return;
    }

    _isDirty = false;
}
}  // namespace mongo::timeseries::hcindex
