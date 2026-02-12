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

#include "mongo/db/exec/timeseries/hcindex/gpu/gpu_backend.h"

// Backend implementations
// Note: We always include headers, but the implementations are guarded.
// HIP backend works on both AMD (ROCm) and NVIDIA (via HIP's CUDA backend).
// CUDA-specific backend needs to be implemented later to see if here is any
// performance benefit.
#include "mongo/db/exec/timeseries/hcindex/gpu/backends/null_backend.h"

#if defined(MONGO_CONFIG_GPU_METAL)
#include "mongo/db/exec/timeseries/hcindex/gpu/backends/metal_backend.h"
#endif

#if defined(MONGO_CONFIG_GPU_HIP)
#include "mongo/db/exec/timeseries/hcindex/gpu/backends/hip_backend.h"
#endif

namespace mongo::timeseries::hcindex::gpu {

// Detection order: Metal (Apple) > HIP (AMD/NVIDIA) > None
// Note: HIP also works on NVIDIA via HIP's CUDA backend, so we don't
// need a separate CUDA path. Build with MONGO_CONFIG_GPU_HIP for
// NVIDIA.
// Note: For now only compile time backend selection is supported.
GpuBackendType detectBestBackend() {
#if defined(MONGO_CONFIG_GPU_METAL)
    return GpuBackendType::Metal;
#elif defined(MONGO_CONFIG_GPU_HIP)
    return GpuBackendType::HIP;
#else
    return GpuBackendType::None;
#endif
}

// Note: For now only compile time backend selection is supported.
// TODO: Runtime detection would require dynamic loading of backend
// libraries.
std::unique_ptr<GpuBackend> createBackend(GpuBackendType type) {

    // TODO: For now, unused
    (void)type;

#if defined(MONGO_CONFIG_GPU_METAL)
    return std::make_unique<MetalBackend>();
#elif defined(MONGO_CONFIG_GPU_HIP)
    return std::make_unique<HipBackend>();
#else
    return std::make_unique<NullBackend>();
#endif
}

std::unique_ptr<GpuBackend> createDefaultBackend() {
    return createBackend(detectBestBackend());
}

}  // namespace mongo::timeseries::hcindex::gpu

