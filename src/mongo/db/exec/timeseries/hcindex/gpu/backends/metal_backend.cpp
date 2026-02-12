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

#if defined(MONGO_CONFIG_GPU_METAL)

// Metal-cpp implementation defines - must be defined before includes
#define NS_PRIVATE_IMPLEMENTATION
#define MTL_PRIVATE_IMPLEMENTATION
#define CA_PRIVATE_IMPLEMENTATION

#include <Foundation/Foundation.hpp>
#include <Metal/Metal.hpp>

#include "mongo/db/exec/timeseries/hcindex/gpu/backends/metal_backend.h"

#include <cstring>

namespace mongo::timeseries::hcindex::gpu {

/**
 * Implementatoin containing Metal objects using Metal-cpp.
 */

class MetalBackend::Impl {
public:
    MTL::Device* device = nullptr;
    MTL::CommandQueue* commandQueue = nullptr;
    MTL::Library* library = nullptr;
    MTL::ComputePipelineState* filterColumnPipeline = nullptr;
    MTL::ComputePipelineState* andBitmapsPipeline = nullptr;
    MTL::ComputePipelineState* compactRowIdsPipeline = nullptr;
    bool initialized = false;

    Impl() {
        NS::AutoreleasePool* pool = NS::AutoreleasePool::alloc()->init();

        // Get the default Metal device
        device = MTL::CreateSystemDefaultDevice();
        if (!device) {
            pool->release();
            return;
        }

        // Create command queue
        commandQueue = device->newCommandQueue();
        if (!commandQueue) {
            pool->release();
            return;
        }

        // Try to load the Metal library from multiple potential locations
        NS::Error* error = nullptr;

        // Dynamically load the Metal Library
        // WARN: Security Vulnerability. We need to ensure that we
        // are only loading signed libraries i production from a
        // specific deployment controled location.

        // 1. From environment variable MONGO_METAL_LIBRARY_PATH
        const char* envPath = std::getenv("MONGO_METAL_LIBRARY_PATH");
        if (envPath) {
            NS::String* path = NS::String::string(envPath, NS::UTF8StringEncoding);
            NS::URL* url = NS::URL::fileURLWithPath(path);
            library = device->newLibrary(url, &error);
        }

        // 2. ry current working directory
        if (!library) {
            NS::String* cwdPath = NS::String::string("filter_kernels.metallib", NS::UTF8StringEncoding);
            NS::URL* url = NS::URL::fileURLWithPath(cwdPath);
            library = device->newLibrary(url, &error);
        }

        // 3. From the kernels subdirectory (development layout)
        if (!library) {
            // Try kernels subdirectory (development layout)
            NS::String* kernelsPath = NS::String::string("kernels/filter_kernels.metallib", NS::UTF8StringEncoding);
            NS::URL* url = NS::URL::fileURLWithPath(kernelsPath);
            library = device->newLibrary(url, &error);
        }

        // Device is available, but no kernels loaded. The filterColumn/andBitmaps
        // methods will fall back to CPU
        if (!library) {
            initialized = true;
            pool->release();
            return;
        }

        // Create pipeline states

        NS::String* filterFuncName = NS::String::string("filterColumnKernel", NS::UTF8StringEncoding);
        MTL::Function* filterFunc = library->newFunction(filterFuncName);
        if (filterFunc) {
            filterColumnPipeline = device->newComputePipelineState(filterFunc, &error);
            filterFunc->release();
        }

        NS::String* andFuncName = NS::String::string("andBitmapsKernel", NS::UTF8StringEncoding);
        MTL::Function* andFunc = library->newFunction(andFuncName);
        if (andFunc) {
            andBitmapsPipeline = device->newComputePipelineState(andFunc, &error);
            andFunc->release();
        }

        NS::String* compactFuncName = NS::String::string("compactRowIdsKernel", NS::UTF8StringEncoding);
        MTL::Function* compactFunc = library->newFunction(compactFuncName);
        if (compactFunc) {
            compactRowIdsPipeline = device->newComputePipelineState(compactFunc, &error);
            compactFunc->release();
        }

        initialized = true;
        pool->release();
    }

    ~Impl() {
        if (compactRowIdsPipeline) compactRowIdsPipeline->release();
        if (andBitmapsPipeline) andBitmapsPipeline->release();
        if (filterColumnPipeline) filterColumnPipeline->release();
        if (library) library->release();
        if (commandQueue) commandQueue->release();
        if (device) device->release();
    }
};


//- CONSTRUCTORS


MetalBackend::MetalBackend() : _impl(std::make_unique<Impl>()) {}


//- DESTRUCTOR


MetalBackend::~MetalBackend() = default;


//- ACCESSORS


//
// Device Management
//

bool MetalBackend::isAvailable() const {
    return _impl && _impl->initialized && _impl->device != nullptr;
}

int MetalBackend::getDeviceCount() const {
    // Metal typically has one device on Apple Silicon
    return isAvailable() ? 1 : 0;
}

DeviceInfo MetalBackend::getDeviceInfo(int deviceId) const {
    DeviceInfo info;
    info.deviceId = deviceId;
    info.backendType = GpuBackendType::Metal;

    if (!isAvailable() || deviceId != 0) {
        return info;
    }

    NS::AutoreleasePool* pool = NS::AutoreleasePool::alloc()->init();
    info.name = _impl->device->name()->utf8String();
    info.totalMemoryBytes = _impl->device->recommendedMaxWorkingSetSize();

    // Not directly available in Metal API. This is probably where we
    // should also enrich optimal block size.
    info.computeUnits = 0;

    info.available = true;
    pool->release();

    return info;
}


//- MODIFIERS


//
// Device Management
//

void MetalBackend::setDevice(int /*deviceId*/) {
    // Metal only supports one device per process. No multi-gpus
    // possible due to the SOC. This assumption needs to be
    // revisited in the future.
}

void MetalBackend::synchronize() {
    // Due to the unified memory, metal commands are already
    // synchronized when waitUntilCompleted is called
}

//
// Filter Operations
//

void MetalBackend::filterColumn(GpuBufferHandle column,
                                size_t numRows,
                                PredicateOp op,
                                uint32_t value,
                                GpuBufferHandle resultBitmap) {
    if (!isAvailable()) {
        return;
    }

    MTL::Buffer* colBuffer = static_cast<MTL::Buffer*>(column);
    MTL::Buffer* resultBuffer = static_cast<MTL::Buffer*>(resultBitmap);

    // If no GPU pipeline, use CPU fallback on Metal shared memory. This
    // is easy on metal since it is shared memory.
    if (!_impl->filterColumnPipeline) {
        const uint32_t* columnData = static_cast<const uint32_t*>(colBuffer->contents());
        uint32_t* bitmapData = static_cast<uint32_t*>(resultBuffer->contents());

        size_t numWords = (numRows + 31) / 32;

        // Reset to 0 - mimics what we do on the AttributeTable's CPU
        // implementation. TODO: optimize.
        std::memset(bitmapData, 0, numWords * sizeof(uint32_t));

        // Process each row
        for (size_t row = 0; row < numRows; ++row) {
            uint32_t colValue = columnData[row];
            bool matches = false;

            switch (op) {
                case PredicateOp::EQ:
                    matches = (colValue == value);
                    break;
                case PredicateOp::NE:
                    matches = (colValue != value);
                    break;
                case PredicateOp::LT:
                    matches = (colValue < value);
                    break;
                case PredicateOp::LE:
                    matches = (colValue <= value);
                    break;
                case PredicateOp::GT:
                    matches = (colValue > value);
                    break;
                case PredicateOp::GE:
                    matches = (colValue >= value);
                    break;
            }

            if (matches) {
                size_t wordIdx = row / 32;
                size_t bitIdx = row % 32;
                bitmapData[wordIdx] |= (1u << bitIdx);
            }
        }
        return;
    }

    NS::AutoreleasePool* pool = NS::AutoreleasePool::alloc()->init();

    // TODO: Ideally, we could pipeline multiple operations into a single
    // command buffer. Right now, we are assuming "1 backend per
    // AttributeTable'. This is highly inefficient. In short, we should
    // have intelligence here to support multiple command buffers
    // operations and then batch and execute them. To get to that level
    // of efficiency, we will also need some help from the query layers
    // above the hcindex implementation.

    MTL::CommandBuffer* commandBuffer = _impl->commandQueue->commandBuffer();
    MTL::ComputeCommandEncoder* encoder = commandBuffer->computeCommandEncoder();

    encoder->setComputePipelineState(_impl->filterColumnPipeline);
    encoder->setBuffer(colBuffer, 0, 0);
    encoder->setBuffer(resultBuffer, 0, 1);
    encoder->setBytes(&value, sizeof(value), 2);

    uint32_t numRowsU32 = static_cast<uint32_t>(numRows);
    encoder->setBytes(&numRowsU32, sizeof(numRowsU32), 3);

    uint32_t opU32 = static_cast<uint32_t>(op);
    encoder->setBytes(&opU32, sizeof(opU32), 4);

    size_t numWords = (numRows + 31) / 32;
    MTL::Size gridSize = MTL::Size::Make(numWords, 1, 1);
    NS::UInteger threadGroupSize = _impl->filterColumnPipeline->maxTotalThreadsPerThreadgroup();
    if (threadGroupSize > numWords) {
        threadGroupSize = numWords;
    }
    MTL::Size threadgroupSize = MTL::Size::Make(threadGroupSize, 1, 1);

    encoder->dispatchThreads(gridSize, threadgroupSize);
    encoder->endEncoding();

    commandBuffer->commit();
    commandBuffer->waitUntilCompleted();

    pool->release();
}

void MetalBackend::andBitmaps(GpuBufferHandle accumulator,
                              GpuBufferHandle operand,
                              size_t numWords) {
    if (!isAvailable()) {
        return;
    }

    MTL::Buffer* accBuffer = static_cast<MTL::Buffer*>(accumulator);
    MTL::Buffer* opBuffer = static_cast<MTL::Buffer*>(operand);

    // CPU fallback if no shader
    if (!_impl->andBitmapsPipeline) {
        uint32_t* accData = static_cast<uint32_t*>(accBuffer->contents());
        const uint32_t* opData = static_cast<const uint32_t*>(opBuffer->contents());
        for (size_t i = 0; i < numWords; ++i) {
            accData[i] &= opData[i];
        }
        return;
    }

    NS::AutoreleasePool* pool = NS::AutoreleasePool::alloc()->init();

    MTL::CommandBuffer* commandBuffer = _impl->commandQueue->commandBuffer();
    MTL::ComputeCommandEncoder* encoder = commandBuffer->computeCommandEncoder();

    encoder->setComputePipelineState(_impl->andBitmapsPipeline);
    encoder->setBuffer(accBuffer, 0, 0);
    encoder->setBuffer(opBuffer, 0, 1);

    MTL::Size gridSize = MTL::Size::Make(numWords, 1, 1);
    NS::UInteger threadGroupSize = _impl->andBitmapsPipeline->maxTotalThreadsPerThreadgroup();
    if (threadGroupSize > numWords) {
        threadGroupSize = numWords;
    }
    MTL::Size threadgroupSize = MTL::Size::Make(threadGroupSize, 1, 1);

    encoder->dispatchThreads(gridSize, threadgroupSize);
    encoder->endEncoding();

    commandBuffer->commit();
    commandBuffer->waitUntilCompleted();

    pool->release();
}

size_t MetalBackend::compactRowIds(GpuBufferHandle bitmap,
                                   size_t numRows,
                                   GpuBufferHandle outputRowIds) {
    if (!isAvailable()) {
        return 0;
    }

    // TODO: Implement a parallel prefix sum.
    // For now, the compaction uses a simple CPU approach since
    // this is a PoC/reference implementation. More complex setup
    // is needed here to optimize this.
    MTL::Buffer* bitmapBuffer = static_cast<MTL::Buffer*>(bitmap);
    MTL::Buffer* outputBuffer = static_cast<MTL::Buffer*>(outputRowIds);

    const uint32_t* bitmapData = static_cast<const uint32_t*>(bitmapBuffer->contents());
    int64_t* outputData = static_cast<int64_t*>(outputBuffer->contents());

    size_t count = 0;
    size_t numWords = (numRows + 31) / 32;

    for (size_t wordIdx = 0; wordIdx < numWords; ++wordIdx) {
        uint32_t word = bitmapData[wordIdx];
        size_t baseRow = wordIdx * 32;

        while (word != 0) {
            // Count trailing zeros
            int bit = __builtin_ctz(word);
            size_t row = baseRow + bit;
            if (row < numRows) {
                outputData[count++] = static_cast<int64_t>(row);
            }
            // Clear lowest set bit.
            word &= (word - 1);
        }
    }

    return count;
}

//
// Memory Management
//

GpuBufferHandle MetalBackend::allocate(size_t sizeBytes) {
    if (!isAvailable()) {
        return nullptr;
    }

    NS::AutoreleasePool* pool = NS::AutoreleasePool::alloc()->init();
    MTL::Buffer* buffer = _impl->device->newBuffer(sizeBytes, MTL::ResourceStorageModeShared);
    pool->release();

    // Return as opaque handle.
    // TODO: caller must call free(). We should have an assert on this.
    return static_cast<void*>(buffer);
}

void MetalBackend::free(GpuBufferHandle buffer) {
    if (buffer) {
        MTL::Buffer* mtlBuffer = static_cast<MTL::Buffer*>(buffer);
        mtlBuffer->release();
    }
}

void MetalBackend::copyToDevice(GpuBufferHandle dst, const void* src, size_t sizeBytes) {
    if (!dst || !src) return;

    MTL::Buffer* buffer = static_cast<MTL::Buffer*>(dst);
    std::memcpy(buffer->contents(), src, sizeBytes);
}

void MetalBackend::copyToHost(void* dst, GpuBufferHandle src, size_t sizeBytes) {
    if (!dst || !src) return;

    MTL::Buffer* buffer = static_cast<MTL::Buffer*>(src);
    std::memcpy(dst, buffer->contents(), sizeBytes);
}

void MetalBackend::zero(GpuBufferHandle buffer, size_t sizeBytes) {
    if (!buffer) return;

    MTL::Buffer* mtlBuffer = static_cast<MTL::Buffer*>(buffer);
    std::memset(mtlBuffer->contents(), 0, sizeBytes);
}


}  // namespace mongo::timeseries::hcindex::gpu

#endif  // MONGO_CONFIG_GPU_METAL

