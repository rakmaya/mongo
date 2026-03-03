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
#include <functional>
#include <memory>
#include <vector>

#include "mongo/db/exec/timeseries/hcindex/gpu/gpu_backend.h"

namespace mongo::timeseries::hcindex::gpu {

/**
 * Buffer usage hints for allocation optimization.
 */
enum class BufferUsage {
    /** Buffer is written once and read many times (e.g., uploaded data). */
    Static,

    /** Buffer is written and read frequently (e.g., intermediate results). */
    Dynamic,

    /** Buffer is used only within this frame and can be recycled. */
    Transient,

    /** Buffer is used for staging CPU->GPU transfers. */
    Staging,

    /** Buffer is used for readback GPU->CPU transfers. */
    Readback,
};

/**
 * Represents a GPU buffer allocation with metadata.
 */
struct BufferAllocation {
    GpuBufferHandle handle = nullptr;
    size_t sizeBytes = 0;
    BufferUsage usage = BufferUsage::Static;
    bool isMapped = false;
};

/**
 * Handle to a buffer managed by ExecutionFrame.
 * This is an index into the frame's buffer pool.
 */
using FrameBufferHandle = uint32_t;
constexpr FrameBufferHandle kInvalidFrameBuffer = UINT32_MAX;

/**
 * Handle to a command buffer within an ExecutionFrame.
 */
using CommandBufferHandle = uint32_t;
constexpr CommandBufferHandle kInvalidCommandBuffer = UINT32_MAX;
constexpr CommandBufferHandle kDefaultCommandBuffer = 0;

/**
 * Command buffer state.
 */
enum class CommandBufferState {
    /** Command buffer is idle and ready for recording. */
    Idle,

    /** Command buffer is currently recording commands. */
    Recording,

    /** Command buffer has been submitted and is executing or pending. */
    Submitted,

    /** Command buffer execution has completed. */
    Completed,
};

/**
 * CommandBuffer represents a sequence of GPU commands that can be recorded and submitted.
 *
 * Multiple command buffers within a frame enable:
 * - Parallel recording on different CPU threads
 * - Fine-grained synchronization between GPU operations
 * - Better GPU utilization through overlapping execution
 *
 * Lifecycle:
 *   1. begin() - Start recording commands
 *   2. Record operations (filterColumn, copy, etc.)
 *   3. end() - Finish recording
 *   4. submit() - Submit to GPU (via ExecutionFrame)
 *   5. waitUntilCompleted() - Block until done
 *
 * Command buffers can have dependencies on other command buffers, ensuring
 * ordered execution when needed.
 */
class CommandBuffer {

public:

    //- DESTRUCTOR

    virtual ~CommandBuffer() = default;


    //- ACCESSORS

    /** Return the handle of this command buffer. */
    virtual CommandBufferHandle getHandle() const = 0;

    /** Return the current state of this command buffer. */
    virtual CommandBufferState getState() const = 0;

    /** Return true if this command buffer is recording. */
    bool isRecording() const { return getState() == CommandBufferState::Recording; }

    /** Return true if this command buffer has completed execution. */
    bool isCompleted() const { return getState() == CommandBufferState::Completed; }


    //- MODIFIERS

    /** Begin recording commands. */
    virtual void begin() = 0;

    /** End recording commands. */
    virtual void end() = 0;

    /**
     * Add a dependency on another command buffer.
     * This command buffer will not start executing until the dependency completes.
     */
    virtual void addDependency(CommandBufferHandle dependency) = 0;

    /** Submit this command buffer for execution. */
    virtual void submit() = 0;

    /** Wait for this command buffer to complete. */
    virtual void waitUntilCompleted() = 0;

    //
    // Data Transfer Commands
    //

    /** Copy data from host memory to a GPU buffer. */
    virtual void copyToDevice(FrameBufferHandle dst, const void* src, size_t sizeBytes) = 0;

    /** Copy data from a GPU buffer to host memory. */
    virtual void copyToHost(void* dst, FrameBufferHandle src, size_t sizeBytes) = 0;

    /** Fill a buffer with zeros. */
    virtual void zero(FrameBufferHandle buffer, size_t sizeBytes) = 0;

    //
    // Compute Commands
    //

    /** Execute filter kernel on a column. */
    virtual void filterColumn(FrameBufferHandle column,
                              size_t numRows,
                              PredicateOp op,
                              uint32_t value,
                              FrameBufferHandle resultBitmap) = 0;

    /** Bitwise AND two bitmaps. */
    virtual void andBitmaps(FrameBufferHandle accumulator,
                            FrameBufferHandle operand,
                            size_t numWords) = 0;

    /** Compact bitmap to row IDs. */
    virtual size_t compactRowIds(FrameBufferHandle bitmap,
                                 size_t numRows,
                                 FrameBufferHandle outputRowIds) = 0;
};


/**
 * Handle to a GPU fence.
 */
using GpuFenceHandle = uint32_t;
constexpr GpuFenceHandle kInvalidFence = UINT32_MAX;

/**
 * Fence state.
 */
enum class FenceState {
    /** Fence has not been signaled yet. */
    Unsignaled,

    /** Fence has been signaled (GPU work completed). */
    Signaled,
};

/**
 * GpuFence provides fine-grained CPU/GPU synchronization.
 *
 * A fence is a synchronization primitive that allows the CPU to:
 * - Wait for specific GPU operations to complete (not all GPU work)
 * - Poll completion status without blocking
 * - Register callbacks for async notification
 *
 * Unlike waitUntilCompleted() which waits for ALL GPU work, a fence
 * allows waiting for specific operations, enabling better pipelining.
 *
 * Usage:
 *   auto fence = frame->createFence();
 *   frame->copyToDevice(...);
 *   frame->signalFence(fence);     // GPU will signal after copy completes
 *   frame->filterColumn(...);      // More work after the fence point
 *   frame->submit();
 *
 *   // CPU can do other work here...
 *
 *   fence->wait();  // Wait only for the copy to finish
 *   // Use copied data while filterColumn is still running
 */
class GpuFence {

public:

    //- DESTRUCTOR

    virtual ~GpuFence() = default;


    //- ACCESSORS

    /** Return the handle of this fence. */
    virtual GpuFenceHandle getHandle() const = 0;

    /** Return the current state of this fence. */
    virtual FenceState getState() const = 0;

    /** Return true if this fence has been signaled. */
    bool isSignaled() const { return getState() == FenceState::Signaled; }


    //- MODIFIERS

    /**
     * Wait for this fence to be signaled (blocking).
     * @param timeoutNs Maximum time to wait in nanoseconds (0 = infinite)
     * @return true if signaled, false if timeout
     */
    virtual bool wait(uint64_t timeoutNs = 0) = 0;

    /**
     * Reset the fence to unsignaled state for reuse.
     */
    virtual void reset() = 0;

    /**
     * Register a callback to be invoked when the fence is signaled.
     * The callback will be invoked on an unspecified thread.
     * Note: Not all backends support async callbacks.
     */
    using CompletionCallback = std::function<void()>;
    virtual void onSignaled(CompletionCallback callback) = 0;
};


/**
 * ExecutionFrame represents a unit of GPU work.
 *
 * Similar to a render frame in video games, an ExecutionFrame:
 * - Owns one or more command buffers for encoding GPU commands
 * - Manages transient buffer allocations that live only for this frame
 * - Provides begin/end semantics for resource tracking
 * - Enables efficient CPU/GPU pipelining when multiple frames are in flight
 *
 * Multiple Command Buffers:
 *   ExecutionFrame supports multiple command buffers for fine-grained control:
 *   - Each command buffer can be recorded independently (potentially in parallel)
 *   - Command buffers can have dependencies for ordered execution
 *   - Individual command buffers can be submitted and waited on
 *
 * Simple Lifecycle (single command buffer):
 *   1. begin() - Start recording to default command buffer
 *   2. Encode operations (filterColumn, andBitmaps, copy, etc.)
 *   3. submit() - Submit default command buffer to GPU
 *   4. waitUntilCompleted() - Block until GPU finishes
 *   5. reset() - Recycle resources for next use
 *
 * Advanced Lifecycle (multiple command buffers):
 *   1. auto cb1 = frame->createCommandBuffer();
 *   2. auto cb2 = frame->createCommandBuffer();
 *   3. cb1->begin(); cb1->copyToDevice(...); cb1->end();
 *   4. cb2->addDependency(cb1->getHandle());
 *   5. cb2->begin(); cb2->filterColumn(...); cb2->end();
 *   6. frame->submitAll();
 *   7. frame->waitUntilCompleted();
 */
class ExecutionFrame {

public:

    //- DESTRUCTOR

    virtual ~ExecutionFrame() = default;


    //- ACCESSORS

    /** Return true if the frame is currently recording commands. */
    virtual bool isRecording() const = 0;

    /** Return true if all submitted work has completed. */
    virtual bool isCompleted() const = 0;

    /** Return the underlying GPU backend. */
    virtual GpuBackend* getBackend() const = 0;

    /** Return the frame index (for debugging/profiling). */
    virtual uint32_t getFrameIndex() const = 0;

    /** Return the number of command buffers in this frame. */
    virtual size_t getCommandBufferCount() const = 0;

    /** Get a command buffer by handle. Returns nullptr if invalid. */
    virtual CommandBuffer* getCommandBuffer(CommandBufferHandle handle) = 0;

    /** Get the default command buffer (always exists). */
    virtual CommandBuffer* getDefaultCommandBuffer() = 0;


    //- MODIFIERS

    //
    // Frame Lifecycle (simple API using default command buffer)
    //

    /** Begin recording commands to the default command buffer. */
    virtual void begin() = 0;

    /** Submit the default command buffer to the GPU for execution. */
    virtual void submit() = 0;

    /** Block until all submitted command buffers complete. */
    virtual void waitUntilCompleted() = 0;

    /** Reset the frame for reuse, recycling transient resources and command buffers. */
    virtual void reset() = 0;

    //
    // Command Buffer Management (advanced API)
    //

    /**
     * Create a new command buffer within this frame.
     * The command buffer is owned by the frame and recycled on reset().
     */
    virtual CommandBuffer* createCommandBuffer() = 0;

    /**
     * Submit all command buffers that are in the ended state.
     * Respects dependencies between command buffers.
     */
    virtual void submitAll() = 0;

    /**
     * Submit a specific command buffer.
     */
    virtual void submitCommandBuffer(CommandBufferHandle handle) = 0;

    //
    // Fence Management
    //

    /**
     * Create a GPU fence for synchronization.
     * The fence is owned by the frame and recycled on reset().
     */
    virtual GpuFence* createFence() = 0;

    /**
     * Get a fence by handle. Returns nullptr if invalid.
     */
    virtual GpuFence* getFence(GpuFenceHandle handle) = 0;

    /**
     * Signal a fence after all previously recorded commands complete.
     * The fence will transition to Signaled state when the GPU reaches this point.
     */
    virtual void signalFence(GpuFenceHandle fence) = 0;

    /**
     * Wait on the CPU for a fence to be signaled before recording more commands.
     * This creates a GPU-side dependency, not a CPU block.
     */
    virtual void waitFence(GpuFenceHandle fence) = 0;

    /**
     * Return the number of fences in this frame.
     */
    virtual size_t getFenceCount() const = 0;

    //
    // Buffer Management
    //

    /**
     * Allocate a transient buffer that lives only for this frame.
     * The buffer is automatically recycled when the frame is reset.
     */
    virtual FrameBufferHandle allocateTransient(size_t sizeBytes) = 0;

    /**
     * Get the raw GPU buffer handle for a frame buffer.
     */
    virtual GpuBufferHandle getBufferHandle(FrameBufferHandle buffer) const = 0;

    /**
     * Register an external buffer (not owned by the frame) for use in commands.
     * Returns a FrameBufferHandle that can be used with frame operations.
     */
    virtual FrameBufferHandle registerExternalBuffer(GpuBufferHandle externalBuffer,
                                                     size_t sizeBytes) = 0;

    //
    // Data Transfer Commands
    //

    /**
     * Copy data from host memory to a frame buffer.
     */
    virtual void copyToDevice(FrameBufferHandle dst, const void* src, size_t sizeBytes) = 0;

    /**
     * Copy data from a frame buffer to host memory.
     * Note: For async operations, results are only valid after waitUntilCompleted().
     */
    virtual void copyToHost(void* dst, FrameBufferHandle src, size_t sizeBytes) = 0;

    /**
     * Fill a buffer with zeros.
     */
    virtual void zero(FrameBufferHandle buffer, size_t sizeBytes) = 0;

    //
    // Compute Commands - Filter Operations
    //

    /**
     * Execute filter kernel on a column.
     *
     * @param column Buffer containing column data (uint32_t per row)
     * @param numRows Number of rows in the column
     * @param op Predicate operation (EQ, NE, LT, LE, GT, GE)
     * @param value Value to compare against
     * @param resultBitmap Output buffer for result bitmap (1 bit per row)
     */
    virtual void filterColumn(FrameBufferHandle column,
                              size_t numRows,
                              PredicateOp op,
                              uint32_t value,
                              FrameBufferHandle resultBitmap) = 0;

    /**
     * Bitwise AND two bitmaps (accumulator &= operand).
     */
    virtual void andBitmaps(FrameBufferHandle accumulator,
                            FrameBufferHandle operand,
                            size_t numWords) = 0;

    /**
     * Compact bitmap to row IDs.
     * Returns the number of matching rows after waitUntilCompleted().
     */
    virtual size_t compactRowIds(FrameBufferHandle bitmap,
                                 size_t numRows,
                                 FrameBufferHandle outputRowIds) = 0;
};


/**
 * ExecutionContext manages a pool of ExecutionFrames for CPU/GPU pipelining.
 *
 * In video game rendering, multiple frames are typically "in flight" to keep
 * both CPU and GPU busy. This context manages that lifecycle:
 *
 *   Frame N:   [CPU encode] [GPU execute] [idle]
 *   Frame N+1:              [CPU encode]  [GPU execute] [idle]
 *   Frame N+2:                            [CPU encode]  [GPU execute]
 *
 * For database queries, we typically use a simpler model with 1-2 frames.
 */
class ExecutionContext {

public:

    //- DESTRUCTOR

    virtual ~ExecutionContext() = default;


    //- ACCESSORS

    /** Return the number of frames in the pool. */
    virtual size_t getFrameCount() const = 0;

    /** Return the GPU backend used by this context. */
    virtual GpuBackend* getBackend() const = 0;


    //- MODIFIERS

    /**
     * Acquire a frame for recording commands.
     * May block if all frames are in use.
     */
    virtual std::unique_ptr<ExecutionFrame> acquireFrame() = 0;

    /**
     * Release a frame back to the pool after use.
     */
    virtual void releaseFrame(std::unique_ptr<ExecutionFrame> frame) = 0;

    /**
     * Wait for all in-flight frames to complete.
     */
    virtual void waitAll() = 0;

    /**
     * Allocate a persistent buffer (lives beyond any single frame).
     * The caller is responsible for freeing this buffer.
     */
    virtual GpuBufferHandle allocatePersistent(size_t sizeBytes) = 0;

    /**
     * Free a persistent buffer.
     */
    virtual void freePersistent(GpuBufferHandle buffer) = 0;
};


/**
 * Create an ExecutionContext for the given backend.
 *
 * @param backend The GPU backend to use (ownership transferred)
 * @param frameCount Number of frames to keep in the pool (default: 2)
 */
std::unique_ptr<ExecutionContext> createExecutionContext(
    std::unique_ptr<GpuBackend> backend,
    size_t frameCount = 2);

}  // namespace mongo::timeseries::hcindex::gpu

