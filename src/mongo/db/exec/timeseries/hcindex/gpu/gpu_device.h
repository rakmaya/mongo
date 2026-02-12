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

#include "mongo/db/exec/timeseries/hcindex/gpu/gpu_common.h"

#include <string>

namespace mongo::timeseries::hcindex::gpu {

/**
 * Information about a GPU device.
 * TODO: Detect other device capabilities. Computing ideal BlockSize would be
 * a good thing.
 */
struct GpuDeviceInfo {
    int deviceId = -1;
    std::string name;
    size_t totalMemoryBytes = 0;
    size_t freeMemoryBytes = 0;
    int computeCapabilityMajor = 0;
    int computeCapabilityMinor = 0;
    int multiProcessorCount = 0;
    int maxThreadsPerBlock = 0;
    int warpSize = 0;
    bool available = false;
};


/**
 * GPU device management utilities.
 */
class GpuDevice {
public:
    /**
     * Check if GPU support is available at runtime.
     */
    static bool isAvailable();

    /**
     * Get the number of available GPU devices.
     */
    static int getDeviceCount();

    /**
     * Get information about a specific GPU device.
     */
    static GpuDeviceInfo getDeviceInfo(int deviceId = 0);

    /**
     * Set the current GPU device.
     */
    static void setDevice(int deviceId);

    /**
     * Get the current GPU device ID.
     */
    static int getCurrentDevice();

    /**
     * Synchronize all GPU operations on the current device.
     */
    static void synchronize();
};


#ifndef MONGO_GPU_DISABLED


inline bool GpuDevice::isAvailable() {
    int count = 0;
    gpuError_t err = gpuGetDeviceCount(&count);
    return (err == gpuSuccess && count > 0);
}

inline int GpuDevice::getDeviceCount() {
    int count = 0;
    if (gpuGetDeviceCount(&count) != gpuSuccess) {
        return 0;
    }
    return count;
}

inline GpuDeviceInfo GpuDevice::getDeviceInfo(int deviceId) {
    GpuDeviceInfo info;
    info.deviceId = deviceId;

    gpuDeviceProp_t props;
    if (gpuGetDeviceProperties(&props, deviceId) != gpuSuccess) {
        return info;
    }

    info.name = props.name;
    info.totalMemoryBytes = props.totalGlobalMem;
    info.multiProcessorCount = props.multiProcessorCount;
    info.maxThreadsPerBlock = props.maxThreadsPerBlock;
    info.warpSize = props.warpSize;
    info.available = true;

#ifdef MONGO_CONFIG_GPU_HIP
    // GCN architecture version for AMD
    info.computeCapabilityMajor = props.gcnArch;
    info.computeCapabilityMinor = 0;
#else
    info.computeCapabilityMajor = props.major;
    info.computeCapabilityMinor = props.minor;
#endif

    return info;
}

inline void GpuDevice::setDevice(int deviceId) {
    gpuSetDevice(deviceId);
}

inline int GpuDevice::getCurrentDevice() {
    int deviceId = 0;
    gpuGetDevice(&deviceId);
    return deviceId;
}

inline void GpuDevice::synchronize() {
    gpuDeviceSynchronize();
}

#else


// NOP Implementations


inline bool GpuDevice::isAvailable() {
    return false;
}
inline int GpuDevice::getDeviceCount() {
    return 0;
}
inline GpuDeviceInfo GpuDevice::getDeviceInfo(int) {
    return {};
}
inline void GpuDevice::setDevice(int) {}
inline int GpuDevice::getCurrentDevice() {
    return -1;
}
inline void GpuDevice::synchronize() {}


#endif  // MONGO_GPU_DISABLED

}  // namespace mongo::timeseries::hcindex::gpu

