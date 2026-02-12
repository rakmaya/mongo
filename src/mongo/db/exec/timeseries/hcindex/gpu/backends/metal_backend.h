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

#if defined(MONGO_CONFIG_GPU_METAL)

#include "mongo/db/exec/timeseries/hcindex/gpu/gpu_backend.h"

// NOTE: Metal types are handled via PIMPL pattern in the .cpp
// file using metal-cpp. So no forward declaration needed. This
// is nice in theory, but in practice we won't capture the type
// drift if someone chanegs the cpp code. It is better to create
// a certain **code generated** dependency that both cpp and gpu
// types depend on.
//
// While Metal is rarely used in production, it provides a highly
// desired development environment capability for both MongoDB
// engineers and our development community. GPU debugging is very
// hard and having something that operates in a unified memory
// to double check our CPU glue code locally gives a very fast
// development iteration.

namespace mongo::timeseries::hcindex::gpu {

/**
 * Metal backend for Apple M GPUs.
 */
class MetalBackend : public GpuBackend {
public:

    //- CONSTRUCTORS


    MetalBackend();


    //- DESTRUCTOR


    ~MetalBackend() override;


    //- ACCESSORS


    //
    // Device Management
    //

    /** Return the backend type. */
    GpuBackendType getType() const override {
        return GpuBackendType::Metal;
    }

    /** Return the backend name (e.g., "HIP", "CUDA", "Metal"). */
    std::string getName() const override {
        return "Metal";
    }

    /**
     * Return true if the backend is available and functional. Otherwise
     * return false. */
    bool isAvailable() const override;

    /** Return the number of available devices. */
    int getDeviceCount() const override;

    /** Return information about a specific device. */
    DeviceInfo getDeviceInfo(int deviceId = 0) const override;


    //- MODIFIERS


    //
    // Device Management
    //

    /** Set the active device. */
    void setDevice(int deviceId) override;

    /** Synchronize all pending operations. */
    void synchronize() override;

    //
    // Filter Operations
    //

    /**
     * Execute filter kernel on a single `column` upto the specified `numRows`
     * rows. The predicate operator `op` determines the boolean logic to be
     * compared against the given `value`. The `resultBitmap` contains the
     * output GPU buffer for result bitmap.
     */
    void filterColumn(GpuBufferHandle column,
                      size_t numRows,
                      PredicateOp op,
                      uint32_t value,
                      GpuBufferHandle resultBitmap) override;

    /**
     * Bitwise AND two bitmaps (accumulator &= operand).
     */
    void andBitmaps(GpuBufferHandle accumulator,
                    GpuBufferHandle operand,
                    size_t numWords) override;

    /**
     * Compact the specified `numRows` bitmap to row IDs and store the matching
     * rows into the given `outputRowIds`. Return the number of matching rows
     */
    size_t compactRowIds(GpuBufferHandle bitmap,
                         size_t numRows,
                         GpuBufferHandle outputRowIds) override;

    //
    // Memory Management
    //

    /** Allocate GPU buffer of the specified `size` and return the handle to that buffer */
    GpuBufferHandle allocate(size_t sizeBytes) override;

    /** Free a GPU buffer. */
    void free(GpuBufferHandle buffer) override;

    /** Copy data from host source `src` to to the on device destination `dst`.*/
    void copyToDevice(GpuBufferHandle dst, const void* src, size_t sizeBytes) override;

    /** Copy data from on device source `src` to to the host destination `dst`.*/
    void copyToHost(void* dst, GpuBufferHandle src, size_t sizeBytes) override;

    /** Fill on device buffer with zeros. */
    void zero(GpuBufferHandle buffer, size_t sizeBytes) override;

private:
    // Opaque pointer to implementation
    class Impl;
    std::unique_ptr<Impl> _impl;
};

}  // namespace mongo::timeseries::hcindex::gpu

#endif  // MONGO_CONFIG_GPU_METAL

