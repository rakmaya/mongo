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

/**
 * GPU Platform Functions
 *
 * This header provides a mapping from the respective library functions
 * to a common definition. Starting with HIP which works on both platforms is easier.
 * When compiled with hipcc on AMD, it uses ROCm. When compiled with hipcc on NVIDIA
 * (or with -DHIP_PLATFORM=nvidia), it wraps CUDA. The downside is that there
 * is a layer over CUDA and it is worth testing to see what difference this
 * layer makes.
 */

#if defined(MONGO_CONFIG_GPU_HIP)

#include <hip/hip_runtime.h>

// Runtime API
#define gpuMalloc               hipMalloc
#define gpuFree                 hipFree
#define gpuMemcpy               hipMemcpy
#define gpuMemcpyAsync          hipMemcpyAsync
#define gpuMemcpyHostToDevice   hipMemcpyHostToDevice
#define gpuMemcpyDeviceToHost   hipMemcpyDeviceToHost
#define gpuMemcpyDeviceToDevice hipMemcpyDeviceToDevice
#define gpuMemset               hipMemset
#define gpuMemsetAsync          hipMemsetAsync

// Synchronization
#define gpuDeviceSynchronize    hipDeviceSynchronize
#define gpuStreamSynchronize    hipStreamSynchronize
#define gpuStreamCreate         hipStreamCreate
#define gpuStreamDestroy        hipStreamDestroy

// Error handling
#define gpuGetLastError         hipGetLastError
#define gpuPeekAtLastError      hipPeekAtLastError
#define gpuGetErrorString       hipGetErrorString
#define gpuSuccess              hipSuccess
#define gpuError_t              hipError_t

// Types
#define gpuStream_t             hipStream_t
#define gpuEvent_t              hipEvent_t
#define gpuDeviceProp_t         hipDeviceProp_t

// Device management
#define gpuGetDeviceCount       hipGetDeviceCount
#define gpuGetDevice            hipGetDevice
#define gpuSetDevice            hipSetDevice
#define gpuGetDeviceProperties  hipGetDeviceProperties

// Events (for timing)
#define gpuEventCreate          hipEventCreate
#define gpuEventDestroy         hipEventDestroy
#define gpuEventRecord          hipEventRecord
#define gpuEventSynchronize     hipEventSynchronize
#define gpuEventElapsedTime     hipEventElapsedTime

// Kernel launch configuration (need to autodetect)
// AMD wavefront size
#define GPU_WARP_SIZE           64

#elif defined(MONGO_CONFIG_GPU_CUDA)

#include <cuda_runtime.h>

// Runtime API
#define gpuMalloc               cudaMalloc
#define gpuFree                 cudaFree
#define gpuMemcpy               cudaMemcpy
#define gpuMemcpyAsync          cudaMemcpyAsync
#define gpuMemcpyHostToDevice   cudaMemcpyHostToDevice
#define gpuMemcpyDeviceToHost   cudaMemcpyDeviceToHost
#define gpuMemcpyDeviceToDevice cudaMemcpyDeviceToDevice
#define gpuMemset               cudaMemset
#define gpuMemsetAsync          cudaMemsetAsync

// Synchronization
#define gpuDeviceSynchronize    cudaDeviceSynchronize
#define gpuStreamSynchronize    cudaStreamSynchronize
#define gpuStreamCreate         cudaStreamCreate
#define gpuStreamDestroy        cudaStreamDestroy

// Error handling
#define gpuGetLastError         cudaGetLastError
#define gpuPeekAtLastError      cudaPeekAtLastError
#define gpuGetErrorString       cudaGetErrorString
#define gpuSuccess              cudaSuccess
#define gpuError_t              cudaError_t

// Types
#define gpuStream_t             cudaStream_t
#define gpuEvent_t              cudaEvent_t
#define gpuDeviceProp_t         cudaDeviceProp_t

// Device management
#define gpuGetDeviceCount       cudaGetDeviceCount
#define gpuGetDevice            cudaGetDevice
#define gpuSetDevice            cudaSetDevice
#define gpuGetDeviceProperties  cudaGetDeviceProperties

// Events (for timing)
#define gpuEventCreate          cudaEventCreate
#define gpuEventDestroy         cudaEventDestroy
#define gpuEventRecord          cudaEventRecord
#define gpuEventSynchronize     cudaEventSynchronize
#define gpuEventElapsedTime     cudaEventElapsedTime

// Kernel launch configuration (need to auto detect)
// NVIDIA warp size
#define GPU_WARP_SIZE           32


#else

// No GPU support
#define MONGO_GPU_DISABLED 1


#endif  // GPU platform selection

// Common constants
#define GPU_DEFAULT_BLOCK_SIZE  256
#define GPU_MAX_GRID_SIZE       65535

