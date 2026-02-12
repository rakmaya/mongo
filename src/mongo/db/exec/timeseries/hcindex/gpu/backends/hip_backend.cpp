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

#if defined(MONGO_CONFIG_GPU_HIP)

#include "mongo/db/exec/timeseries/hcindex/gpu/backends/hip_backend.h"

#include <stdexcept>

//-----------------------------
// ---- KERNEL DEPENDENCY ----
//-----------------------------

// Forward declarations for kernel launchers defined in filter_kernels.hip
namespace mongo::timeseries::hcindex::gpu::kernels {

void launchFilterKernel(const uint32_t* column,
                        size_t numRows,
                        PredicateOp op,
                        uint32_t value,
                        uint32_t* resultBitmap,
                        hipStream_t stream);

void launchAndBitmaps(uint32_t* accumulator,
                      const uint32_t* operand,
                      size_t numWords,
                      hipStream_t stream);

size_t launchCompactRowIds(const uint32_t* bitmap,
                           size_t numRows,
                           int64_t* outputRowIds,
                           hipStream_t stream);

}  // namespace mongo::timeseries::hcindex::gpu::kernels

namespace mongo::timeseries::hcindex::gpu {

namespace {

void checkHipError(hipError_t error, const char* msg) {
    if (error != hipSuccess) {
        throw std::runtime_error(std::string(msg) + ": " + hipGetErrorString(error));
    }
}

}  // namespace


//- CONSTRUCTORS


HipBackend::HipBackend() {
    int count = 0;
    hipError_t err = hipGetDeviceCount(&count);
    if (err == hipSuccess && count > 0) {
        err = hipStreamCreate(&_stream);
        if (err == hipSuccess) {
            _initialized = true;
        }
    }
}


//- DESTRUCTOR


HipBackend::~HipBackend() {
    if (_stream) {
        hipStreamDestroy(_stream);
    }
}


//- ACCESSORS


//
// Device Management
//

bool HipBackend::isAvailable() const {
    return _initialized;
}

int HipBackend::getDeviceCount() const {
    int count = 0;
    if (hipGetDeviceCount(&count) != hipSuccess) {
        return 0;
    }
    return count;
}

DeviceInfo HipBackend::getDeviceInfo(int deviceId) const {
    DeviceInfo info;
    info.deviceId = deviceId;
    info.backendType = GpuBackendType::HIP;

    hipDeviceProp_t props;
    if (hipGetDeviceProperties(&props, deviceId) != hipSuccess) {
        return info;
    }

    info.name = props.name;
    info.totalMemoryBytes = props.totalGlobalMem;
    info.computeUnits = props.multiProcessorCount;
    info.available = true;

    return info;
}


//- MODIFIERS


//
// Device Management
//

void HipBackend::setDevice(int deviceId) {
    checkHipError(hipSetDevice(deviceId), "hipSetDevice failed");
}

void HipBackend::synchronize() {
    checkHipError(hipDeviceSynchronize(), "hipDeviceSynchronize failed");
}

//
// Filter Operations
//

void HipBackend::filterColumn(GpuBufferHandle column,
                              size_t numRows,
                              PredicateOp op,
                              uint32_t value,
                              GpuBufferHandle resultBitmap) {
    kernels::launchFilterKernel(static_cast<const uint32_t*>(column),
                                numRows,
                                op,
                                value,
                                static_cast<uint32_t*>(resultBitmap),
                                _stream);
}

void HipBackend::andBitmaps(GpuBufferHandle accumulator,
                            GpuBufferHandle operand,
                            size_t numWords) {
    kernels::launchAndBitmaps(static_cast<uint32_t*>(accumulator),
                              static_cast<const uint32_t*>(operand),
                              numWords,
                              _stream);
}

//
// Memory Management
//

GpuBufferHandle HipBackend::allocate(size_t sizeBytes) {
    void* ptr = nullptr;
    checkHipError(hipMalloc(&ptr, sizeBytes), "hipMalloc failed");
    return ptr;
}

void HipBackend::free(GpuBufferHandle buffer) {
    if (buffer) {
        hipFree(buffer);
    }
}

void HipBackend::copyToDevice(GpuBufferHandle dst, const void* src, size_t sizeBytes) {
    checkHipError(hipMemcpy(dst, src, sizeBytes, hipMemcpyHostToDevice), "hipMemcpy H2D failed");
}

void HipBackend::copyToHost(void* dst, GpuBufferHandle src, size_t sizeBytes) {
    checkHipError(hipMemcpy(dst, src, sizeBytes, hipMemcpyDeviceToHost), "hipMemcpy D2H failed");
}

void HipBackend::zero(GpuBufferHandle buffer, size_t sizeBytes) {
    checkHipError(hipMemset(buffer, 0, sizeBytes), "hipMemset failed");
}

size_t HipBackend::compactRowIds(GpuBufferHandle bitmap,
                                 size_t numRows,
                                 GpuBufferHandle outputRowIds) {
    return kernels::launchCompactRowIds(static_cast<const uint32_t*>(bitmap),
                                        numRows,
                                        static_cast<int64_t*>(outputRowIds),
                                        _stream);
}


}  // namespace mongo::timeseries::hcindex::gpu

#endif  // MONGO_CONFIG_GPU_HIP

