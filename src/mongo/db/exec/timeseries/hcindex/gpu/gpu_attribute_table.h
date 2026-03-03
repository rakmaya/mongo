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

#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "mongo/db/exec/timeseries/hcindex/gpu/execution_frame.h"
#include "mongo/db/exec/timeseries/hcindex/gpu/gpu_backend.h"

namespace mongo::timeseries::hcindex {

// Forward declaration
class AttributeTable;

}  // namespace mongo::timeseries::hcindex

namespace mongo::timeseries::hcindex::gpu {

// Re-export PredicateOp and ColumnPredicate (for now)
using GpuPredicateOp = PredicateOp;
using GpuColumnPredicate = ColumnPredicate;

/**
 * GPU-accelerated AttributeTable operations.
 *
 * This class provides GPU-accelerated filtering and scanning of AttributeTable
 * data. It maintains a GPU-side copy of the columnar data and provides methods
 * to execute parallel predicate evaluation.
 *
 * Uses the abstract GpuBackend interface to support multiple GPU platforms:
 * - HIP (AMD ROCm / NVIDIA via HIP)
 * - Metal (Apple M chips)
 * - NullBackend (CPU fallback when no GPU available)
 *
 * GPU attribute table partitions the GPU memory for segments; L0 and L1. The
 * L0 segment is entirely for data is modified or read actively. L1 is for data
 * that is read-only and read actively. Internall, the GpuAttributeTableManager
 * keeps 2 LAL (least active list) and maintains them accordingly. Unlike the
 * sloppy AI GPU coding paradigm, we aus GPU Renderer driver techniques. These
 * provide 3 benefits:
 * 1 - Significantly more optimal gpu shader code (eqiuvant of geometric
       shaders written in hip/metal shanding language)
 * 2 - Ability to run on any GPU, no just machines with NPUs/specialized AI
       gpu.
 * 3 - Ensure that we use the PCIE lanes more efficiently. We employ a
       mini-rendering pipeline to ensure that CPU<->GPU operations are kept
       minimal.
 */
class GpuAttributeTable {

public:

    //- CONSTRUCTORS

    /**
     * Create an instance of GpuAttributeTable with the default backend for the current platform.
     */
    GpuAttributeTable();

    /**
     * Create an isntance of GpuAttributeTable with a specific backend and take
     * the ownership of the backend.
     */
    explicit GpuAttributeTable(std::unique_ptr<GpuBackend> backend);

    /**
     * Create an instance of GpuAttributeTable that has the same value as the
     * specfied `other` instance.
     */
    GpuAttributeTable(GpuAttributeTable&& other) noexcept;


    //- DESTRUCTOR


    ~GpuAttributeTable();


    //- OPERATORS


    /**
     * Create an instance of GpuAttributeTable that has the same value as the
     * specfied `other` instance.
     */
    GpuAttributeTable& operator=(GpuAttributeTable&& other) noexcept;


    //- ACCESSORS


    /**
     * Return the number of rows in the GPU table.
     */
    size_t getRowCount() const { return _rowCount; }

    /**
     * Return the number of columns in the GPU table.
     */
    size_t getColumnCount() const { return _columnCount; }

    /**
     * Return true if data is already uploaded to GPU. Otherwise, return false.
     */
    bool isUploaded() const { return _isUploaded; }

    /**
     * Return true if GPU backend is available. Otherwise, return false.
     */
    bool isGpuAvailable() const;

    /**
     * Return information about the GPU device.
     */
    DeviceInfo getDeviceInfo() const;

    /**
     * Filter rows based on a conjunction of the specified column `predicates` and returns row IDs
     * that satisfy ALL predicates.
     *
     */
    std::vector<int64_t> filter(const std::vector<GpuColumnPredicate>& predicates) const;

    /**
     * Filter rows based on a single column predicate.
     */
    std::vector<int64_t> filterColumn(size_t columnIndex,
                                      GpuPredicateOp op,
                                      uint32_t value) const;


    //- MODIFIERS


    /**
     * Upload the specified `attributeTable` data to GPU memory.
     */
    void uploadFromCpu(const AttributeTable& attributeTable);

    /**
     * Upload the specified `columns` directly to GPU memory. This overload is more efficient
     * when the caller already has columnar data (avoids contruction of
     * AttributeTable).
     *
     */
    void uploadFromCpu(const std::vector<std::vector<uint32_t>>& columns);

    /**
     * Release GPU memory.
     */
    void release();


    //- DELETED METHODS


    // Non-copyable, movable
    GpuAttributeTable(const GpuAttributeTable&) = delete;
    GpuAttributeTable& operator=(const GpuAttributeTable&) = delete;

private:
    // Execution context for GPU operations (owns the backend and frame pool)
    std::unique_ptr<ExecutionContext> _context;

    // GPU buffer is a columnar layout for coalesced memory access.
    // Changing this layout will have significant performance impact.
    // These are persistent buffers allocated through the context.
    std::vector<GpuBufferHandle> _gpuColumns;

    size_t _rowCount = 0;
    size_t _columnCount = 0;
    bool _isUploaded = false;
};

}  // namespace mongo::timeseries::hcindex::gpu

