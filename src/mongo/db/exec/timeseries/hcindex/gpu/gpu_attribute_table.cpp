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

#include "mongo/db/exec/timeseries/hcindex/gpu/gpu_attribute_table.h"

#include "mongo/db/exec/timeseries/hcindex/attribute_table.h"

namespace mongo::timeseries::hcindex::gpu {


//- CONSTRUCTORS


GpuAttributeTable::GpuAttributeTable()
    : _backend(createDefaultBackend()) {}

GpuAttributeTable::GpuAttributeTable(std::unique_ptr<GpuBackend> backend)
    : _backend(std::move(backend)) {
    if (!_backend) {
        _backend = createDefaultBackend();
    }
}

GpuAttributeTable::GpuAttributeTable(GpuAttributeTable&& other) noexcept
    : _backend(std::move(other._backend)),
      _gpuColumns(std::move(other._gpuColumns)),
      _filterResultBitmap(other._filterResultBitmap),
      _matchingRowIds(other._matchingRowIds),
      _rowCount(other._rowCount),
      _columnCount(other._columnCount),
      _bitmapSizeBytes(other._bitmapSizeBytes),
      _rowIdsSizeBytes(other._rowIdsSizeBytes),
      _isUploaded(other._isUploaded) {
    // Clear the moved-from object's handles to prevent double-free
    other._filterResultBitmap = nullptr;
    other._matchingRowIds = nullptr;
    other._gpuColumns.clear();
    other._isUploaded = false;
}


//- DESTRUCTOR


GpuAttributeTable::~GpuAttributeTable() {
    release();
}


//- OPERATORS


GpuAttributeTable& GpuAttributeTable::operator=(GpuAttributeTable&& other) noexcept {
    if (this != &other) {
        release();
        _backend = std::move(other._backend);
        _gpuColumns = std::move(other._gpuColumns);
        _filterResultBitmap = other._filterResultBitmap;
        _matchingRowIds = other._matchingRowIds;
        _rowCount = other._rowCount;
        _columnCount = other._columnCount;
        _bitmapSizeBytes = other._bitmapSizeBytes;
        _rowIdsSizeBytes = other._rowIdsSizeBytes;
        _isUploaded = other._isUploaded;

        other._filterResultBitmap = nullptr;
        other._matchingRowIds = nullptr;
        other._gpuColumns.clear();
        other._isUploaded = false;
    }
    return *this;
}


//- ACCESSORS


bool GpuAttributeTable::isGpuAvailable() const {
    return _backend && _backend->isAvailable();
}

DeviceInfo GpuAttributeTable::getDeviceInfo() const {
    if (_backend) {
        return _backend->getDeviceInfo();
    }
    return DeviceInfo{};
}

std::vector<int64_t> GpuAttributeTable::filter(
    const std::vector<GpuColumnPredicate>& predicates) const {

    if (!_isUploaded || !_backend || predicates.empty() || _rowCount == 0) {
        return {};
    }

    size_t bitmapWords = (_rowCount + 31) / 32;

    // Process predicates using the backend
    bool firstPredicate = true;

    for (const auto& pred : predicates) {
        if (pred.columnIndex >= _columnCount) {
            continue;  // Skip invalid column indices
        }

        if (firstPredicate) {
            // First predicate: write directly to result bitmap
            _backend->filterColumn(_gpuColumns[pred.columnIndex],
                                   _rowCount,
                                   pred.op,
                                   pred.value,
                                   _filterResultBitmap);
            firstPredicate = false;
        } else {
            // Subsequent predicates: create temp bitmap and AND with result
            GpuBufferHandle tempBitmap = _backend->allocate(_bitmapSizeBytes);
            _backend->filterColumn(_gpuColumns[pred.columnIndex],
                                   _rowCount,
                                   pred.op,
                                   pred.value,
                                   tempBitmap);
            _backend->andBitmaps(_filterResultBitmap, tempBitmap, bitmapWords);
            _backend->free(tempBitmap);
        }
    }

    // Compact matching row IDs
    size_t matchCount = _backend->compactRowIds(_filterResultBitmap, _rowCount, _matchingRowIds);

    // Copy results back to host
    std::vector<int64_t> result(matchCount);
    if (matchCount > 0) {
        _backend->copyToHost(result.data(), _matchingRowIds, matchCount * sizeof(int64_t));
    }

    return result;
}

std::vector<int64_t> GpuAttributeTable::filterColumn(size_t columnIndex,
                                                     GpuPredicateOp op,
                                                     uint32_t value) const {
    return filter({{columnIndex, op, value}});
}


//- MODIFIERS


void GpuAttributeTable::uploadFromCpu(const AttributeTable& cpuTable) {
    release();

    if (!_backend || !_backend->isAvailable()) {
        return;
    }

    _rowCount = cpuTable.getRowCount();
    if (_rowCount == 0) {
        return;
    }

    const auto& schema = cpuTable.getSchema();
    _columnCount = schema.size();

    if (_columnCount == 0) {
        return;
    }

    // Allocate and upload each column
    _gpuColumns.resize(_columnCount, nullptr);
    size_t columnSizeBytes = _rowCount * sizeof(uint32_t);

    for (size_t colIdx = 0; colIdx < _columnCount; ++colIdx) {
        // Extract column data by iterating rows
        std::vector<uint32_t> columnData;
        columnData.reserve(_rowCount);

        for (size_t rowIdx = 0; rowIdx < _rowCount; ++rowIdx) {
            auto row = cpuTable.getRow(static_cast<int64_t>(rowIdx));
            if (row && colIdx < row->size()) {
                columnData.push_back((*row)[colIdx]);
            } else {
                columnData.push_back(0);  // Missing value
            }
        }

        // Allocate GPU buffer and upload
        _gpuColumns[colIdx] = _backend->allocate(columnSizeBytes);
        _backend->copyToDevice(_gpuColumns[colIdx], columnData.data(), columnSizeBytes);
    }

    // Allocate filter result bitmap (1 bit per row, rounded up to uint32_t words)
    size_t bitmapWords = (_rowCount + 31) / 32;
    _bitmapSizeBytes = bitmapWords * sizeof(uint32_t);
    _filterResultBitmap = _backend->allocate(_bitmapSizeBytes);

    // Allocate space for matching row IDs (worst case: all rows match)
    _rowIdsSizeBytes = _rowCount * sizeof(int64_t);
    _matchingRowIds = _backend->allocate(_rowIdsSizeBytes);

    _isUploaded = true;
}

void GpuAttributeTable::uploadFromCpu(const std::vector<std::vector<uint32_t>>& columns) {
    // Release any existing GPU resources
    release();

    if (!_backend || !_backend->isAvailable()) {
        return;
    }

    _columnCount = columns.size();
    if (_columnCount == 0) {
        return;
    }

    // All columns should have the same size
    _rowCount = columns[0].size();
    if (_rowCount == 0) {
        return;
    }

    // Allocate and upload each column directly (already in columnar format)
    _gpuColumns.resize(_columnCount, nullptr);
    size_t columnSizeBytes = _rowCount * sizeof(uint32_t);

    for (size_t colIdx = 0; colIdx < _columnCount; ++colIdx) {
        const auto& column = columns[colIdx];

        // Allocate GPU buffer and upload
        _gpuColumns[colIdx] = _backend->allocate(columnSizeBytes);

        // Handle columns that are shorter (schema evolution)
        if (column.size() >= _rowCount) {
            _backend->copyToDevice(_gpuColumns[colIdx], column.data(), columnSizeBytes);
        } else {
            // Pad with zeros for missing values
            std::vector<uint32_t> paddedColumn(column);
            paddedColumn.resize(_rowCount, 0);
            _backend->copyToDevice(_gpuColumns[colIdx], paddedColumn.data(), columnSizeBytes);
        }
    }

    // Allocate filter result bitmap (1 bit per row, rounded up to uint32_t words)
    size_t bitmapWords = (_rowCount + 31) / 32;
    _bitmapSizeBytes = bitmapWords * sizeof(uint32_t);
    _filterResultBitmap = _backend->allocate(_bitmapSizeBytes);

    // Allocate space for matching row IDs (worst case: all rows match)
    _rowIdsSizeBytes = _rowCount * sizeof(int64_t);
    _matchingRowIds = _backend->allocate(_rowIdsSizeBytes);

    _isUploaded = true;
}

void GpuAttributeTable::release() {
    if (_backend) {
        // Free column buffers
        for (auto& col : _gpuColumns) {
            if (col) {
                _backend->free(col);
                col = nullptr;
            }
        }
        _gpuColumns.clear();

        // Free bitmap buffer
        if (_filterResultBitmap) {
            _backend->free(_filterResultBitmap);
            _filterResultBitmap = nullptr;
        }

        // Free row IDs buffer
        if (_matchingRowIds) {
            _backend->free(_matchingRowIds);
            _matchingRowIds = nullptr;
        }
    }

    _rowCount = 0;
    _columnCount = 0;
    _bitmapSizeBytes = 0;
    _rowIdsSizeBytes = 0;
    _isUploaded = false;
}


}  // namespace mongo::timeseries::hcindex::gpu

