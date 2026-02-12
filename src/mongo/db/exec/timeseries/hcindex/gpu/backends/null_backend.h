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

#include "mongo/db/exec/timeseries/hcindex/gpu/gpu_backend.h"

namespace mongo::timeseries::hcindex::gpu {

/**
 * Null backend - stub implementation when no GPU is available.
 * TODO: Implement the fallback. Right now all operations are NOPs.
 */
class NullBackend : public GpuBackend {
public:
    GpuBackendType getType() const override {
        return GpuBackendType::None;
    }

    std::string getName() const override {
        return "None";
    }

    bool isAvailable() const override {
        return false;
    }

    int getDeviceCount() const override {
        return 0;
    }

    DeviceInfo getDeviceInfo(int /*deviceId*/) const override {
        return DeviceInfo{};
    }

    void setDevice(int /*deviceId*/) override {}

    void synchronize() override {}

    GpuBufferHandle allocate(size_t /*sizeBytes*/) override {
        return nullptr;
    }

    void free(GpuBufferHandle /*buffer*/) override {}

    void copyToDevice(GpuBufferHandle /*dst*/,
                      const void* /*src*/,
                      size_t /*sizeBytes*/) override {}

    void copyToHost(void* /*dst*/, GpuBufferHandle /*src*/, size_t /*sizeBytes*/) override {}

    void zero(GpuBufferHandle /*buffer*/, size_t /*sizeBytes*/) override {}

    void filterColumn(GpuBufferHandle /*column*/,
                      size_t /*numRows*/,
                      PredicateOp /*op*/,
                      uint32_t /*value*/,
                      GpuBufferHandle /*resultBitmap*/) override {}

    void andBitmaps(GpuBufferHandle /*accumulator*/,
                    GpuBufferHandle /*operand*/,
                    size_t /*numWords*/) override {}

    size_t compactRowIds(GpuBufferHandle /*bitmap*/,
                         size_t /*numRows*/,
                         GpuBufferHandle /*outputRowIds*/) override {
        return 0;
    }
};

}  // namespace mongo::timeseries::hcindex::gpu

