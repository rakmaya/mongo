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

#include <hip/hip_runtime.h>
#include <hipcub/hipcub.hpp>

#include "mongo/db/exec/timeseries/hcindex/gpu/gpu_backend.h"

namespace mongo::timeseries::hcindex::gpu::kernels {

// Use the PredicateOp from gpu_backend.h
using PredicateOp = mongo::timeseries::hcindex::gpu::PredicateOp;

/**
 * Kernel: Evaluate predicate on a single column and produce a bitmap result.
 * TODO: Currently, each thread handles 32 rows (one word of the bitmap). This
 * ideally should be determined by evaluating device capabilities to squeze out
 * the most from the GPU.
 */
__global__ void filterColumnKernel(const uint32_t* __restrict__ column,
                                   size_t numRows,
                                   PredicateOp op,
                                   uint32_t value,
                                   uint32_t* __restrict__ resultBitmap) {
    size_t wordIdx = blockIdx.x * blockDim.x + threadIdx.x;
    size_t startRow = wordIdx * 32;

    if (startRow >= numRows) {
        return;
    }

    uint32_t bits = 0;
    size_t endRow = min(startRow + 32, numRows);

    for (size_t row = startRow; row < endRow; ++row) {
        uint32_t colVal = column[row];
        bool match = false;

        switch (op) {
            case PredicateOp::EQ:
                match = (colVal == value);
                break;
            case PredicateOp::NE:
                match = (colVal != value);
                break;
            case PredicateOp::LT:
                match = (colVal < value);
                break;
            case PredicateOp::LE:
                match = (colVal <= value);
                break;
            case PredicateOp::GT:
                match = (colVal > value);
                break;
            case PredicateOp::GE:
                match = (colVal >= value);
                break;
        }

        if (match) {
            bits |= (1u << (row - startRow));
        }
    }

    resultBitmap[wordIdx] = bits;
}

/**
 * Kernel: Bitwise AND two bitmaps.
 */
__global__ void andBitmapsKernel(uint32_t* __restrict__ accumulator,
                                 const uint32_t* __restrict__ operand,
                                 size_t numWords) {
    size_t idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < numWords) {
        accumulator[idx] &= operand[idx];
    }
}

/**
 * Kernel: Count set bits in bitmap and write matching row IDs.
 * Two-pass approach: first count, then write.
 */
__global__ void compactRowIdsKernel(const uint32_t* __restrict__ bitmap,
                                    size_t numRows,
                                    int64_t* __restrict__ outputRowIds,
                                    size_t* __restrict__ outputCount) {
    // This is a simplified single-threaded version.
    // TODO: use hipcub::DeviceSelect or parallel scan.
    if (threadIdx.x == 0 && blockIdx.x == 0) {
        size_t count = 0;
        for (size_t row = 0; row < numRows; ++row) {
            size_t wordIdx = row / 32;
            size_t bitIdx = row % 32;
            if (bitmap[wordIdx] & (1u << bitIdx)) {
                outputRowIds[count++] = static_cast<int64_t>(row);
            }
        }
        *outputCount = count;
    }
}

// Host-side kernel launchers. TODO: Ahem! magic numbers! For now
// I am keeping it this way since I don't have an optimal block size
// detection code. Writing that code now is also not useful since MongoDB
// lacks the required test harness infrastructure.
void launchFilterKernel(const uint32_t* column,
                        size_t numRows,
                        PredicateOp op,
                        uint32_t value,
                        uint32_t* resultBitmap,
                        hipStream_t stream) {
    size_t numWords = (numRows + 31) / 32;
    size_t blockSize = 256;
    size_t numBlocks = (numWords + blockSize - 1) / blockSize;

    hipLaunchKernelGGL(filterColumnKernel,
                       dim3(numBlocks),
                       dim3(blockSize),
                       0,
                       stream,
                       column,
                       numRows,
                       op,
                       value,
                       resultBitmap);
}

void launchAndBitmaps(uint32_t* accumulator,
                      const uint32_t* operand,
                      size_t numWords,
                      hipStream_t stream) {
    size_t blockSize = 256;
    size_t numBlocks = (numWords + blockSize - 1) / blockSize;

    hipLaunchKernelGGL(
        andBitmapsKernel, dim3(numBlocks), dim3(blockSize), 0, stream, accumulator, operand, numWords);
}

size_t launchCompactRowIds(const uint32_t* bitmap,
                           size_t numRows,
                           int64_t* outputRowIds,
                           hipStream_t stream) {
    // Allocate device memory for count
    size_t* d_count;
    hipMalloc(&d_count, sizeof(size_t));
    hipMemset(d_count, 0, sizeof(size_t));

    // Launch kernel (single block nly for now. TODO: optimize with parallel scan)
    // Note: Conversion to parallel scan has some impact on hardware choices.
    // So I am dodging that part for now. For a PoC/reference implementation,
    // this will do.
    hipLaunchKernelGGL(
        compactRowIdsKernel, dim3(1), dim3(1), 0, stream, bitmap, numRows, outputRowIds, d_count);

    // Synchronize and get count
    hipDeviceSynchronize();
    size_t count;
    hipMemcpy(&count, d_count, sizeof(size_t), hipMemcpyDeviceToHost);
    hipFree(d_count);

    return count;
}


}  // namespace mongo::timeseries::hcindex::gpu::kernels

