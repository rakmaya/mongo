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

#include "mongo/db/exec/timeseries/hcindex/gpu/execution_frame.h"

#include <mutex>
#include <queue>

namespace mongo::timeseries::hcindex::gpu {

// Forward declaration
class ExecutionFrameImpl;

/**
 * Concrete implementation of CommandBuffer.
 *
 * Each command buffer:
 * - Maintains its own recording state
 * - Can have dependencies on other command buffers
 * - References the parent frame for buffer resolution and backend access
 */
class CommandBufferImpl : public CommandBuffer {

public:

    CommandBufferImpl(ExecutionFrameImpl* frame, CommandBufferHandle handle)
        : _frame(frame), _handle(handle) {}

    //- ACCESSORS

    CommandBufferHandle getHandle() const override { return _handle; }

    CommandBufferState getState() const override { return _state; }

    //- MODIFIERS

    void begin() override {
        _state = CommandBufferState::Recording;
    }

    void end() override {
        if (_state == CommandBufferState::Recording) {
            _state = CommandBufferState::Idle;  // Ready for submission
        }
    }

    void addDependency(CommandBufferHandle dependency) override {
        _dependencies.push_back(dependency);
    }

    void submit() override;  // Implemented after ExecutionFrameImpl

    void waitUntilCompleted() override;  // Implemented after ExecutionFrameImpl

    // Data transfer commands - delegated to frame
    void copyToDevice(FrameBufferHandle dst, const void* src, size_t sizeBytes) override;
    void copyToHost(void* dst, FrameBufferHandle src, size_t sizeBytes) override;
    void zero(FrameBufferHandle buffer, size_t sizeBytes) override;

    // Compute commands - delegated to frame
    void filterColumn(FrameBufferHandle column,
                      size_t numRows,
                      PredicateOp op,
                      uint32_t value,
                      FrameBufferHandle resultBitmap) override;
    void andBitmaps(FrameBufferHandle accumulator,
                    FrameBufferHandle operand,
                    size_t numWords) override;
    size_t compactRowIds(FrameBufferHandle bitmap,
                         size_t numRows,
                         FrameBufferHandle outputRowIds) override;

    // Internal methods
    void markSubmitted() { _state = CommandBufferState::Submitted; }
    void markCompleted() { _state = CommandBufferState::Completed; }
    void resetState() {
        _state = CommandBufferState::Idle;
        _dependencies.clear();
    }
    const std::vector<CommandBufferHandle>& getDependencies() const { return _dependencies; }

private:
    ExecutionFrameImpl* _frame;
    CommandBufferHandle _handle;
    CommandBufferState _state = CommandBufferState::Idle;
    std::vector<CommandBufferHandle> _dependencies;
};


/**
 * Concrete implementation of GpuFence.
 *
 * Provides CPU/GPU synchronization with:
 * - Polling (non-blocking state check)
 * - Blocking wait with optional timeout
 * - Async callbacks (when supported by backend)
 */
class GpuFenceImpl : public GpuFence {

public:

    GpuFenceImpl(ExecutionFrameImpl* frame, GpuFenceHandle handle)
        : _frame(frame), _handle(handle) {}

    //- ACCESSORS

    GpuFenceHandle getHandle() const override { return _handle; }

    FenceState getState() const override { return _state; }

    //- MODIFIERS

    bool wait(uint64_t timeoutNs) override;  // Implemented after ExecutionFrameImpl

    void reset() override {
        _state = FenceState::Unsignaled;
        _callback = nullptr;
    }

    void onSignaled(CompletionCallback callback) override {
        _callback = std::move(callback);
        // If already signaled, invoke immediately
        if (_state == FenceState::Signaled && _callback) {
            _callback();
        }
    }

    // Internal methods
    void signal() {
        _state = FenceState::Signaled;
        if (_callback) {
            _callback();
        }
    }

private:
    ExecutionFrameImpl* _frame;
    GpuFenceHandle _handle;
    FenceState _state = FenceState::Unsignaled;
    CompletionCallback _callback;
};


/**
 * Concrete implementation of ExecutionFrame.
 *
 * This implementation works with any GpuBackend and provides:
 * - Multiple command buffer support
 * - Transient buffer management with recycling
 * - External buffer registration
 */
class ExecutionFrameImpl : public ExecutionFrame {

public:

    ExecutionFrameImpl(GpuBackend* backend, uint32_t frameIndex)
        : _backend(backend), _frameIndex(frameIndex) {
        // Create the default command buffer
        _commandBuffers.push_back(
            std::make_unique<CommandBufferImpl>(this, kDefaultCommandBuffer));
    }

    ~ExecutionFrameImpl() override {
        // Free all owned transient buffers
        for (auto& alloc : _transientBuffers) {
            if (alloc.handle && _backend) {
                _backend->free(alloc.handle);
            }
        }
    }

    //- ACCESSORS

    bool isRecording() const override {
        // Check if any command buffer is recording
        for (const auto& cb : _commandBuffers) {
            if (cb->getState() == CommandBufferState::Recording) {
                return true;
            }
        }
        return false;
    }

    bool isCompleted() const override {
        // All submitted command buffers must be completed
        for (const auto& cb : _commandBuffers) {
            if (cb->getState() == CommandBufferState::Submitted) {
                return false;
            }
        }
        return _hasSubmitted;  // Only true if we've actually submitted something
    }

    GpuBackend* getBackend() const override { return _backend; }

    uint32_t getFrameIndex() const override { return _frameIndex; }

    size_t getCommandBufferCount() const override { return _commandBuffers.size(); }

    CommandBuffer* getCommandBuffer(CommandBufferHandle handle) override {
        if (handle < _commandBuffers.size()) {
            return _commandBuffers[handle].get();
        }
        return nullptr;
    }

    CommandBuffer* getDefaultCommandBuffer() override {
        return _commandBuffers[kDefaultCommandBuffer].get();
    }

    //- MODIFIERS - Frame Lifecycle (simple API using default command buffer)

    void begin() override {
        _hasSubmitted = false;
        _externalBuffers.clear();
        getDefaultCommandBuffer()->begin();
    }

    void submit() override {
        auto* defaultCb = getDefaultCommandBuffer();
        if (defaultCb->getState() == CommandBufferState::Recording) {
            defaultCb->end();
        }
        submitCommandBuffer(kDefaultCommandBuffer);
    }

    void waitUntilCompleted() override {
        if (_backend) {
            _backend->synchronize();
        }
        // Mark all submitted command buffers as completed
        for (auto& cb : _commandBuffers) {
            if (cb->getState() == CommandBufferState::Submitted) {
                static_cast<CommandBufferImpl*>(cb.get())->markCompleted();
            }
        }
    }

    void reset() override {
        _hasSubmitted = false;
        _externalBuffers.clear();
        _nextTransientIndex = 0;

        // Reset all command buffers but keep only the default one
        for (auto& cb : _commandBuffers) {
            static_cast<CommandBufferImpl*>(cb.get())->resetState();
        }
        // Remove extra command buffers, keeping only the default
        if (_commandBuffers.size() > 1) {
            _commandBuffers.resize(1);
        }

        // Reset and clear fences
        for (auto& fence : _fences) {
            fence->reset();
        }
        _fences.clear();
        _pendingFenceSignals.clear();
    }

    //- MODIFIERS - Command Buffer Management

    CommandBuffer* createCommandBuffer() override {
        CommandBufferHandle handle = static_cast<CommandBufferHandle>(_commandBuffers.size());
        _commandBuffers.push_back(std::make_unique<CommandBufferImpl>(this, handle));
        return _commandBuffers.back().get();
    }

    void submitAll() override {
        // Submit command buffers in dependency order
        // For simplicity, we submit all that are ready (dependencies already completed)
        bool progress = true;
        while (progress) {
            progress = false;
            for (auto& cb : _commandBuffers) {
                auto* impl = static_cast<CommandBufferImpl*>(cb.get());
                if (impl->getState() == CommandBufferState::Idle) {
                    // Check if all dependencies are completed
                    bool depsCompleted = true;
                    for (auto depHandle : impl->getDependencies()) {
                        auto* dep = getCommandBuffer(depHandle);
                        if (dep && dep->getState() != CommandBufferState::Completed) {
                            depsCompleted = false;
                            break;
                        }
                    }
                    if (depsCompleted) {
                        impl->markSubmitted();
                        _hasSubmitted = true;
                        progress = true;
                    }
                }
            }
        }
    }

    void submitCommandBuffer(CommandBufferHandle handle) override {
        if (handle < _commandBuffers.size()) {
            auto* impl = static_cast<CommandBufferImpl*>(_commandBuffers[handle].get());
            impl->markSubmitted();
            _hasSubmitted = true;
        }
    }

    //- MODIFIERS - Fence Management

    GpuFence* createFence() override {
        GpuFenceHandle handle = static_cast<GpuFenceHandle>(_fences.size());
        _fences.push_back(std::make_unique<GpuFenceImpl>(this, handle));
        return _fences.back().get();
    }

    GpuFence* getFence(GpuFenceHandle handle) override {
        if (handle < _fences.size()) {
            return _fences[handle].get();
        }
        return nullptr;
    }

    void signalFence(GpuFenceHandle handle) override {
        // In a synchronous backend, signal immediately after sync
        // For async backends, this would insert a GPU-side signal
        _pendingFenceSignals.push_back(handle);
    }

    void waitFence(GpuFenceHandle handle) override {
        // For synchronous backend, wait immediately
        // For async backends, this would insert a GPU-side wait
        if (handle < _fences.size()) {
            auto* fence = static_cast<GpuFenceImpl*>(_fences[handle].get());
            if (fence->getState() != FenceState::Signaled) {
                // Synchronize to ensure fence is signaled
                if (_backend) {
                    _backend->synchronize();
                }
                fence->signal();
            }
        }
    }

    size_t getFenceCount() const override { return _fences.size(); }

    // Internal: Process pending fence signals (called after sync)
    void processPendingFences() {
        for (auto handle : _pendingFenceSignals) {
            if (handle < _fences.size()) {
                static_cast<GpuFenceImpl*>(_fences[handle].get())->signal();
            }
        }
        _pendingFenceSignals.clear();
    }

    //- MODIFIERS - Buffer Management

    FrameBufferHandle allocateTransient(size_t sizeBytes) override {
        if (!_backend) {
            return kInvalidFrameBuffer;
        }

        // Try to reuse an existing buffer of sufficient size
        for (size_t i = _nextTransientIndex; i < _transientBuffers.size(); ++i) {
            if (_transientBuffers[i].sizeBytes >= sizeBytes) {
                _nextTransientIndex = i + 1;
                return static_cast<FrameBufferHandle>(i);
            }
        }

        // Allocate a new transient buffer
        BufferAllocation alloc;
        alloc.handle = _backend->allocate(sizeBytes);
        alloc.sizeBytes = sizeBytes;
        alloc.usage = BufferUsage::Transient;

        FrameBufferHandle handle = static_cast<FrameBufferHandle>(_transientBuffers.size());
        _transientBuffers.push_back(alloc);
        _nextTransientIndex = _transientBuffers.size();

        return handle;
    }

    GpuBufferHandle getBufferHandle(FrameBufferHandle buffer) const override {
        if (buffer == kInvalidFrameBuffer) {
            return nullptr;
        }

        // Check if it's an external buffer (high bit set)
        if (buffer & 0x80000000) {
            uint32_t extIndex = buffer & 0x7FFFFFFF;
            if (extIndex < _externalBuffers.size()) {
                return _externalBuffers[extIndex].handle;
            }
            return nullptr;
        }

        // It's a transient buffer
        if (buffer < _transientBuffers.size()) {
            return _transientBuffers[buffer].handle;
        }
        return nullptr;
    }

    FrameBufferHandle registerExternalBuffer(GpuBufferHandle externalBuffer,
                                             size_t sizeBytes) override {
        BufferAllocation alloc;
        alloc.handle = externalBuffer;
        alloc.sizeBytes = sizeBytes;
        alloc.usage = BufferUsage::Static;

        // Use high bit to distinguish external buffers
        FrameBufferHandle handle = static_cast<FrameBufferHandle>(
            _externalBuffers.size() | 0x80000000);
        _externalBuffers.push_back(alloc);
        return handle;
    }

    //- MODIFIERS - Data Transfer Commands

    void copyToDevice(FrameBufferHandle dst, const void* src, size_t sizeBytes) override {
        GpuBufferHandle dstHandle = getBufferHandle(dst);
        if (dstHandle && _backend) {
            _backend->copyToDevice(dstHandle, src, sizeBytes);
        }
    }

    void copyToHost(void* dst, FrameBufferHandle src, size_t sizeBytes) override {
        GpuBufferHandle srcHandle = getBufferHandle(src);
        if (srcHandle && _backend) {
            _backend->copyToHost(dst, srcHandle, sizeBytes);
        }
    }

    void zero(FrameBufferHandle buffer, size_t sizeBytes) override {
        GpuBufferHandle handle = getBufferHandle(buffer);
        if (handle && _backend) {
            _backend->zero(handle, sizeBytes);
        }
    }

    //- MODIFIERS - Compute Commands

    void filterColumn(FrameBufferHandle column,
                      size_t numRows,
                      PredicateOp op,
                      uint32_t value,
                      FrameBufferHandle resultBitmap) override {
        GpuBufferHandle colHandle = getBufferHandle(column);
        GpuBufferHandle bitmapHandle = getBufferHandle(resultBitmap);
        if (colHandle && bitmapHandle && _backend) {
            _backend->filterColumn(colHandle, numRows, op, value, bitmapHandle);
        }
    }

    void andBitmaps(FrameBufferHandle accumulator,
                    FrameBufferHandle operand,
                    size_t numWords) override {
        GpuBufferHandle accHandle = getBufferHandle(accumulator);
        GpuBufferHandle opHandle = getBufferHandle(operand);
        if (accHandle && opHandle && _backend) {
            _backend->andBitmaps(accHandle, opHandle, numWords);
        }
    }

    size_t compactRowIds(FrameBufferHandle bitmap,
                         size_t numRows,
                         FrameBufferHandle outputRowIds) override {
        GpuBufferHandle bitmapHandle = getBufferHandle(bitmap);
        GpuBufferHandle outputHandle = getBufferHandle(outputRowIds);
        if (bitmapHandle && outputHandle && _backend) {
            return _backend->compactRowIds(bitmapHandle, numRows, outputHandle);
        }
        return 0;
    }

private:
    GpuBackend* _backend;
    uint32_t _frameIndex;
    bool _hasSubmitted = false;

    // Command buffers (index 0 is the default)
    std::vector<std::unique_ptr<CommandBufferImpl>> _commandBuffers;

    // GPU fences for synchronization
    std::vector<std::unique_ptr<GpuFenceImpl>> _fences;
    std::vector<GpuFenceHandle> _pendingFenceSignals;

    // Transient buffers owned by this frame (recycled on reset)
    std::vector<BufferAllocation> _transientBuffers;
    size_t _nextTransientIndex = 0;

    // External buffers registered for this frame (not owned)
    std::vector<BufferAllocation> _externalBuffers;
};


//- CommandBufferImpl method implementations (need access to ExecutionFrameImpl)

void CommandBufferImpl::submit() {
    if (_frame) {
        _frame->submitCommandBuffer(_handle);
    }
}

void CommandBufferImpl::waitUntilCompleted() {
    if (_frame) {
        _frame->waitUntilCompleted();
    }
}

void CommandBufferImpl::copyToDevice(FrameBufferHandle dst, const void* src, size_t sizeBytes) {
    if (_frame && _state == CommandBufferState::Recording) {
        _frame->copyToDevice(dst, src, sizeBytes);
    }
}

void CommandBufferImpl::copyToHost(void* dst, FrameBufferHandle src, size_t sizeBytes) {
    if (_frame && _state == CommandBufferState::Recording) {
        _frame->copyToHost(dst, src, sizeBytes);
    }
}

void CommandBufferImpl::zero(FrameBufferHandle buffer, size_t sizeBytes) {
    if (_frame && _state == CommandBufferState::Recording) {
        _frame->zero(buffer, sizeBytes);
    }
}

void CommandBufferImpl::filterColumn(FrameBufferHandle column,
                                      size_t numRows,
                                      PredicateOp op,
                                      uint32_t value,
                                      FrameBufferHandle resultBitmap) {
    if (_frame && _state == CommandBufferState::Recording) {
        _frame->filterColumn(column, numRows, op, value, resultBitmap);
    }
}

void CommandBufferImpl::andBitmaps(FrameBufferHandle accumulator,
                                    FrameBufferHandle operand,
                                    size_t numWords) {
    if (_frame && _state == CommandBufferState::Recording) {
        _frame->andBitmaps(accumulator, operand, numWords);
    }
}

size_t CommandBufferImpl::compactRowIds(FrameBufferHandle bitmap,
                                         size_t numRows,
                                         FrameBufferHandle outputRowIds) {
    if (_frame && _state == CommandBufferState::Recording) {
        return _frame->compactRowIds(bitmap, numRows, outputRowIds);
    }
    return 0;
}


//- GpuFenceImpl method implementations

bool GpuFenceImpl::wait(uint64_t timeoutNs) {
    if (_state == FenceState::Signaled) {
        return true;
    }

    // For synchronous backends, synchronize and signal
    if (_frame && _frame->getBackend()) {
        _frame->getBackend()->synchronize();
        _frame->processPendingFences();
    }

    return _state == FenceState::Signaled;
}


/**
 * Concrete implementation of ExecutionContext.
 *
 * Manages a pool of ExecutionFrames and handles persistent buffer allocation.
 */
class ExecutionContextImpl : public ExecutionContext {

public:

    ExecutionContextImpl(std::unique_ptr<GpuBackend> backend, size_t frameCount)
        : _backend(std::move(backend)), _frameCount(frameCount) {
        // Pre-create frames
        for (size_t i = 0; i < frameCount; ++i) {
            _availableFrames.push(
                std::make_unique<ExecutionFrameImpl>(_backend.get(), static_cast<uint32_t>(i)));
        }
    }

    ~ExecutionContextImpl() override {
        waitAll();
        // Frames are automatically cleaned up
    }

    //- ACCESSORS

    size_t getFrameCount() const override { return _frameCount; }

    GpuBackend* getBackend() const override { return _backend.get(); }

    //- MODIFIERS

    std::unique_ptr<ExecutionFrame> acquireFrame() override {
        std::lock_guard<std::mutex> lock(_mutex);

        if (_availableFrames.empty()) {
            // All frames in use - create a new one (expands pool)
            auto frame = std::make_unique<ExecutionFrameImpl>(
                _backend.get(), static_cast<uint32_t>(_frameCount++));
            return frame;
        }

        auto frame = std::move(_availableFrames.front());
        _availableFrames.pop();
        return frame;
    }

    void releaseFrame(std::unique_ptr<ExecutionFrame> frame) override {
        if (!frame) {
            return;
        }

        // Ensure frame work is complete before recycling
        if (!frame->isCompleted()) {
            frame->waitUntilCompleted();
        }
        frame->reset();

        std::lock_guard<std::mutex> lock(_mutex);
        // Downcast back to impl (safe because we created it)
        auto* implPtr = static_cast<ExecutionFrameImpl*>(frame.release());
        _availableFrames.push(std::unique_ptr<ExecutionFrameImpl>(implPtr));
    }

    void waitAll() override {
        if (_backend) {
            _backend->synchronize();
        }
    }

    GpuBufferHandle allocatePersistent(size_t sizeBytes) override {
        if (_backend) {
            return _backend->allocate(sizeBytes);
        }
        return nullptr;
    }

    void freePersistent(GpuBufferHandle buffer) override {
        if (_backend && buffer) {
            _backend->free(buffer);
        }
    }

private:
    std::unique_ptr<GpuBackend> _backend;
    size_t _frameCount;
    std::mutex _mutex;
    std::queue<std::unique_ptr<ExecutionFrameImpl>> _availableFrames;
};


//- NAMESPACE METHODS


std::unique_ptr<ExecutionContext> createExecutionContext(
    std::unique_ptr<GpuBackend> backend,
    size_t frameCount) {
    if (!backend) {
        return nullptr;
    }
    return std::make_unique<ExecutionContextImpl>(std::move(backend), frameCount);
}

}  // namespace mongo::timeseries::hcindex::gpu

