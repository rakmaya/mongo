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
#include "mongo/db/exec/timeseries/hcindex/gpu/execution_frame.h"

namespace mongo::timeseries::hcindex::gpu {


//- CONSTRUCTORS


GpuAttributeTable::GpuAttributeTable()
    : _context(createExecutionContext(createDefaultBackend())) {}

GpuAttributeTable::GpuAttributeTable(std::unique_ptr<GpuBackend> backend)
    : _context(createExecutionContext(std::move(backend))) {
    if (!_context) {
        _context = createExecutionContext(createDefaultBackend());
    }
}

GpuAttributeTable::GpuAttributeTable(GpuAttributeTable&& other) noexcept
    : _context(std::move(other._context)),
      _gpuColumns(std::move(other._gpuColumns)),
      _rowCount(other._rowCount),
      _columnCount(other._columnCount),
      _isUploaded(other._isUploaded) {
    // Clear the moved-from object's state
    other._gpuColumns.clear();
    other._rowCount = 0;
    other._columnCount = 0;
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
        _context = std::move(other._context);
        _gpuColumns = std::move(other._gpuColumns);
        _rowCount = other._rowCount;
        _columnCount = other._columnCount;
        _isUploaded = other._isUploaded;

        other._gpuColumns.clear();
        other._rowCount = 0;
        other._columnCount = 0;
        other._isUploaded = false;
    }
    return *this;
}


//- ACCESSORS


bool GpuAttributeTable::isGpuAvailable() const {
    return _context && _context->getBackend() && _context->getBackend()->isAvailable();
}

DeviceInfo GpuAttributeTable::getDeviceInfo() const {
    if (_context && _context->getBackend()) {
        return _context->getBackend()->getDeviceInfo();
    }
    return DeviceInfo{};
}

std::vector<int64_t> GpuAttributeTable::filter(
    const std::vector<GpuColumnPredicate>& predicates) const {

    if (!_isUploaded || !_context || predicates.empty() || _rowCount == 0) {
        return {};
    }

    // Acquire a frame for this filter operation
    auto frame = _context->acquireFrame();
    frame->begin();

    // Calculate sizes
    size_t bitmapWords = (_rowCount + 31) / 32;
    size_t bitmapSizeBytes = bitmapWords * sizeof(uint32_t);
    size_t rowIdsSizeBytes = _rowCount * sizeof(int64_t);

    // Allocate transient buffers for this operation
    FrameBufferHandle resultBitmap = frame->allocateTransient(bitmapSizeBytes);
    FrameBufferHandle rowIdsBuffer = frame->allocateTransient(rowIdsSizeBytes);

    // Register external column buffers
    std::vector<FrameBufferHandle> columnHandles;
    columnHandles.reserve(_columnCount);
    size_t columnSizeBytes = _rowCount * sizeof(uint32_t);
    for (size_t i = 0; i < _columnCount; ++i) {
        columnHandles.push_back(frame->registerExternalBuffer(_gpuColumns[i], columnSizeBytes));
    }

    // Process predicates
    bool firstPredicate = true;
    FrameBufferHandle tempBitmap = kInvalidFrameBuffer;

    for (const auto& pred : predicates) {
        if (pred.columnIndex >= _columnCount) {
            continue;  // Skip invalid column indices
        }

        if (firstPredicate) {
            // First predicate: write directly to result bitmap
            frame->filterColumn(columnHandles[pred.columnIndex],
                                _rowCount,
                                pred.op,
                                pred.value,
                                resultBitmap);
            firstPredicate = false;
        } else {
            // Subsequent predicates: use temp bitmap and AND with result
            if (tempBitmap == kInvalidFrameBuffer) {
                tempBitmap = frame->allocateTransient(bitmapSizeBytes);
            }
            frame->filterColumn(columnHandles[pred.columnIndex],
                                _rowCount,
                                pred.op,
                                pred.value,
                                tempBitmap);
            frame->andBitmaps(resultBitmap, tempBitmap, bitmapWords);
        }
    }

    // Submit and wait for completion
    frame->submit();
    frame->waitUntilCompleted();

    // Compact matching row IDs
    size_t matchCount = frame->compactRowIds(resultBitmap, _rowCount, rowIdsBuffer);

    // Copy results back to host
    std::vector<int64_t> result(matchCount);
    if (matchCount > 0) {
        frame->copyToHost(result.data(), rowIdsBuffer, matchCount * sizeof(int64_t));
    }

    // Release frame back to the pool
    _context->releaseFrame(std::move(frame));

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

    if (!_context || !_context->getBackend() || !_context->getBackend()->isAvailable()) {
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

    // Allocate and upload each column using persistent buffers
    _gpuColumns.resize(_columnCount, nullptr);
    size_t columnSizeBytes = _rowCount * sizeof(uint32_t);

    // Use a frame for the upload operations
    auto frame = _context->acquireFrame();
    frame->begin();

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

        // Allocate persistent GPU buffer and upload
        _gpuColumns[colIdx] = _context->allocatePersistent(columnSizeBytes);
        auto colHandle = frame->registerExternalBuffer(_gpuColumns[colIdx], columnSizeBytes);
        frame->copyToDevice(colHandle, columnData.data(), columnSizeBytes);
    }

    frame->submit();
    frame->waitUntilCompleted();
    _context->releaseFrame(std::move(frame));

    _isUploaded = true;
}

void GpuAttributeTable::uploadFromCpu(const std::vector<std::vector<uint32_t>>& columns) {
    // Release any existing GPU resources
    release();

    if (!_context || !_context->getBackend() || !_context->getBackend()->isAvailable()) {
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

    // Allocate and upload each column using persistent buffers
    _gpuColumns.resize(_columnCount, nullptr);
    size_t columnSizeBytes = _rowCount * sizeof(uint32_t);

    // Use a frame for the upload operations
    auto frame = _context->acquireFrame();
    frame->begin();

    for (size_t colIdx = 0; colIdx < _columnCount; ++colIdx) {
        const auto& column = columns[colIdx];

        // Allocate persistent GPU buffer
        _gpuColumns[colIdx] = _context->allocatePersistent(columnSizeBytes);
        auto colHandle = frame->registerExternalBuffer(_gpuColumns[colIdx], columnSizeBytes);

        // Handle columns that are shorter (schema evolution)
        if (column.size() >= _rowCount) {
            frame->copyToDevice(colHandle, column.data(), columnSizeBytes);
        } else {
            // Pad with zeros for missing values
            std::vector<uint32_t> paddedColumn(column);
            paddedColumn.resize(_rowCount, 0);
            frame->copyToDevice(colHandle, paddedColumn.data(), columnSizeBytes);
        }
    }

    frame->submit();
    frame->waitUntilCompleted();
    _context->releaseFrame(std::move(frame));

    _isUploaded = true;
}

void GpuAttributeTable::release() {
    if (_context) {
        // Free persistent column buffers
        for (auto& col : _gpuColumns) {
            if (col) {
                _context->freePersistent(col);
                col = nullptr;
            }
        }
        _gpuColumns.clear();
    }

    _rowCount = 0;
    _columnCount = 0;
    _isUploaded = false;
}


}  // namespace mongo::timeseries::hcindex::gpu

