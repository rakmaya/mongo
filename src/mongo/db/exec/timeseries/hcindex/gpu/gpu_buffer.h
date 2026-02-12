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

#ifndef MONGO_GPU_DISABLED

#include "mongo/db/exec/timeseries/hcindex/gpu/gpu_error.h"

#include <cstddef>
#include <memory>
#include <vector>

namespace mongo::timeseries::hcindex::gpu {

/**
 * RAII wrapper for GPU device memory.
 * Automatically allocates on construction and frees on destruction.
 */
template <typename T>
class GpuBuffer {
public:
    GpuBuffer() : _data(nullptr), _size(0), _capacity(0) {}

    explicit GpuBuffer(size_t count) : _data(nullptr), _size(count), _capacity(count) {
        if (count > 0) {
            GPU_CHECK(gpuMalloc(reinterpret_cast<void**>(&_data), count * sizeof(T)));
        }
    }

    ~GpuBuffer() {
        free();
    }

    // Move semantics
    GpuBuffer(GpuBuffer&& other) noexcept
        : _data(other._data), _size(other._size), _capacity(other._capacity) {
        other._data = nullptr;
        other._size = 0;
        other._capacity = 0;
    }

    GpuBuffer& operator=(GpuBuffer&& other) noexcept {
        if (this != &other) {
            free();
            _data = other._data;
            _size = other._size;
            _capacity = other._capacity;
            other._data = nullptr;
            other._size = 0;
            other._capacity = 0;
        }
        return *this;
    }

    // No copy
    GpuBuffer(const GpuBuffer&) = delete;
    GpuBuffer& operator=(const GpuBuffer&) = delete;

    /**
     * Allocate or reallocate buffer to hold at least 'count' elements.
     */
    void resize(size_t count) {
        if (count <= _capacity) {
            _size = count;
            return;
        }
        free();
        if (count > 0) {
            GPU_CHECK(gpuMalloc(reinterpret_cast<void**>(&_data), count * sizeof(T)));
        }
        _size = count;
        _capacity = count;
    }

    /**
     * Copy data from host to device.
     */
    void copyFromHost(const T* hostData, size_t count) {
        if (count > _capacity) {
            resize(count);
        }
        _size = count;
        if (count > 0) {
            GPU_CHECK(gpuMemcpy(_data, hostData, count * sizeof(T), gpuMemcpyHostToDevice));
        }
    }

    void copyFromHost(const std::vector<T>& hostData) {
        copyFromHost(hostData.data(), hostData.size());
    }

    /**
     * Copy data from device to host.
     */
    void copyToHost(T* hostData, size_t count) const {
        size_t copyCount = std::min(count, _size);
        if (copyCount > 0) {
            GPU_CHECK(gpuMemcpy(hostData, _data, copyCount * sizeof(T), gpuMemcpyDeviceToHost));
        }
    }

    void copyToHost(std::vector<T>& hostData) const {
        hostData.resize(_size);
        copyToHost(hostData.data(), _size);
    }

    /**
     * Fill buffer with zeros.
     */
    void zero() {
        if (_size > 0) {
            GPU_CHECK(gpuMemset(_data, 0, _size * sizeof(T)));
        }
    }

    // Accessors
    T* data() { return _data; }
    const T* data() const { return _data; }
    size_t size() const { return _size; }
    size_t capacity() const { return _capacity; }
    size_t sizeBytes() const { return _size * sizeof(T); }
    bool empty() const { return _size == 0; }

private:
    void free() {
        if (_data) {
            gpuFree(_data);
            _data = nullptr;
        }
        _size = 0;
        _capacity = 0;
    }

    T* _data;
    size_t _size;
    size_t _capacity;
};

}  // namespace mongo::timeseries::hcindex::gpu

#endif  // MONGO_GPU_DISABLED

