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

#include <sstream>
#include <stdexcept>
#include <string>

namespace mongo::timeseries::hcindex::gpu {

/**
 * Exception class for GPU errors.
 */
class GpuException : public std::runtime_error {
public:

    //- CONSTRUCTORS


    // Construct an instance of the GpuException that is compatible with the
    // c++ standard runtime error.
    GpuException(const std::string& msg, gpuError_t error, const char* file, int line)
        : std::runtime_error(formatMessage(msg, error, file, line)), _errorCode(error) {}


    //- ACCESSORS


    // Return the gpu specific error code.
    gpuError_t errorCode() const {
        return _errorCode;
    }

private:

    //- PRIVATE METHODS

    static std::string formatMessage(const std::string& msg,
                                     gpuError_t error,
                                     const char* file,
                                     int line) {
        std::ostringstream oss;
        oss << "GPU Error at " << file << ":" << line << " - " << msg << " (error code: " << error
            << ", " << gpuGetErrorString(error) << ")";
        return oss.str();
    }

    gpuError_t _errorCode;
};


//- NAMESPACE METHODS


/**
 * Check GPU error and throw exception if not success.
 */
inline void checkGpuError(gpuError_t error, const char* msg, const char* file, int line) {
    if (error != gpuSuccess) {
        throw GpuException(msg, error, file, line);
    }
}

/**
 * Check last GPU error (for kernel launches).
 */
inline void checkLastGpuError(const char* msg, const char* file, int line) {
    gpuError_t error = gpuGetLastError();
    if (error != gpuSuccess) {
        throw GpuException(msg, error, file, line);
    }
}


}  // namespace mongo::timeseries::hcindex::gpu


#define GPU_CHECK(call)                                                              \
    do {                                                                             \
        gpuError_t _gpu_err = (call);                                                \
        ::mongo::timeseries::hcindex::gpu::checkGpuError(                            \
            _gpu_err, #call, __FILE__, __LINE__);                                    \
    } while (0)

#define GPU_CHECK_LAST_ERROR(msg)                                                    \
    ::mongo::timeseries::hcindex::gpu::checkLastGpuError(msg, __FILE__, __LINE__)


#define GPU_SYNC_CHECK(msg)                                                          \
    do {                                                                             \
        GPU_CHECK(gpuDeviceSynchronize());                                           \
        GPU_CHECK_LAST_ERROR(msg);                                                   \
    } while (0)

#else  // MONGO_GPU_DISABLED


// No-op macros when GPU is disabled

#define GPU_CHECK(call) ((void)0)
#define GPU_CHECK_LAST_ERROR(msg) ((void)0)
#define GPU_SYNC_CHECK(msg) ((void)0)


#endif  // MONGO_GPU_DISABLED

