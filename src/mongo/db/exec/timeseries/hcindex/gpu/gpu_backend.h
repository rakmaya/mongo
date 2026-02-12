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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace mongo::timeseries::hcindex::gpu {

/**
 * Supported GPU backend types.
 */
enum class GpuBackendType {
    None,   // No GPU available
    HIP,    // AMD ROCm
    CUDA,   // NVIDIA CUDA
    Metal,  // Apple Metal
};

/**
 * Predicate operations for filtering.
 */
enum class PredicateOp {
    EQ,  // Equal
    NE,  // Not equal
    LT,  // Less than
    LE,  // Less than or equal
    GT,  // Greater than
    GE,  // Greater than or equal
};

/**
 * A single column predicate for GPU filtering.
 */
struct ColumnPredicate {
    size_t columnIndex;
    PredicateOp op;
    uint32_t value;
};

/**
 * GPU device information.
 */
struct DeviceInfo {
    int deviceId = -1;
    std::string name;
    size_t totalMemoryBytes = 0;
    int computeUnits = 0;
    bool available = false;
    GpuBackendType backendType = GpuBackendType::None;
};

/**
 * Opaque handle to a GPU buffer.
 * Each backend implements its own buffer storage.
 */
using GpuBufferHandle = void*;

/**
 * Abstract GPU backend interface. This interface abstracts GPU operations across different
 * platforms:
 */
class GpuBackend {

public:

    //- DESTRUCTOR


    virtual ~GpuBackend() = default;


    //- ACCESSORS


    //
    // Device Management
    //

    /** Return the backend type. */
    virtual GpuBackendType getType() const = 0;

    /** Return the backend name (e.g., "HIP", "CUDA", "Metal"). */
    virtual std::string getName() const = 0;

    /**
     * Return true if the backend is available and functional. Otherwise
     * return false. */
    virtual bool isAvailable() const = 0;

    /** Return the number of available devices. */
    virtual int getDeviceCount() const = 0;

    /** Return information about a specific device. */
    virtual DeviceInfo getDeviceInfo(int deviceId = 0) const = 0;


    //- MODIFIERS


    //
    // Device Management
    //

    /** Set the active device. */
    virtual void setDevice(int deviceId) = 0;

    /** Synchronize all pending operations. */
    virtual void synchronize() = 0;

    //
    // Filter Operations
    //

    /**
     * Execute filter kernel on a single `column` upto the specified `numRows`
     * rows. The predicate operator `op` determines the boolean logic to be
     * compared against the given `value`. The `resultBitmap` contains the
     * output GPU buffer for result bitmap.
     */
    virtual void filterColumn(GpuBufferHandle column,
                              size_t numRows,
                              PredicateOp op,
                              uint32_t value,
                              GpuBufferHandle resultBitmap) = 0;

    /**
     * Bitwise AND two bitmaps (accumulator &= operand).
     */
    virtual void andBitmaps(GpuBufferHandle accumulator,
                            GpuBufferHandle operand,
                            size_t numWords) = 0;

    /**
     * Compact the specified `numRows` bitmap to row IDs and store the matching
     * rows into the given `outputRowIds`. Return the number of matching rows
     */
    virtual size_t compactRowIds(GpuBufferHandle bitmap,
                                 size_t numRows,
                                 GpuBufferHandle outputRowIds) = 0;

    //
    // Memory Management
    //

    /** Allocate GPU buffer of the specified `size` and return the handle to that buffer */
    virtual GpuBufferHandle allocate(size_t sizeBytes) = 0;

    /** Free a GPU buffer. */
    virtual void free(GpuBufferHandle buffer) = 0;

    /** Copy data from host source `src` to to the on device destination `dst`.*/
    virtual void copyToDevice(GpuBufferHandle dst, const void* src, size_t sizeBytes) = 0;

    /** Copy data from on device source `src` to to the host destination `dst`.*/
    virtual void copyToHost(void* dst, GpuBufferHandle src, size_t sizeBytes) = 0;

    /** Fill on device buffer with zeros. */
    virtual void zero(GpuBufferHandle buffer, size_t sizeBytes) = 0;
};


//- NAMESPACE METHODS


/**
 * Get the default GPU backend for the current platform.
 * Returns nullptr if no GPU is available.
 */
std::unique_ptr<GpuBackend> createDefaultBackend();

/**
 * Get a specific GPU backend by type.
 * Returns nullptr if the requested backend is not available.
 */
std::unique_ptr<GpuBackend> createBackend(GpuBackendType type);

/**
 * Get the best available backend type for the current platform.
 */
GpuBackendType detectBestBackend();

}  // namespace mongo::timeseries::hcindex::gpu

