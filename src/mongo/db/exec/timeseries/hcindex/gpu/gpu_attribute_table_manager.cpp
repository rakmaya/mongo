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

#include "mongo/db/exec/timeseries/hcindex/gpu/gpu_attribute_table_manager.h"

#include <chrono>

#include "mongo/db/exec/timeseries/hcindex/gpu/gpu_backend.h"

namespace mongo::timeseries::hcindex::gpu {

namespace {

// Default memory limits (512MB L0, 1GB L1)
constexpr size_t kDefaultL0Limit = 512 * 1024 * 1024;
constexpr size_t kDefaultL1Limit = 1024 * 1024 * 1024;

// Number of frames in the execution context pool
constexpr size_t kFramePoolSize = 3;

uint64_t getCurrentTimeMs() {
    auto now = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               now.time_since_epoch()).count();
}

}  // namespace


//- CONSTRUCTORS

GpuAttributeTableManager::GpuAttributeTableManager()
    : GpuAttributeTableManager(kDefaultL0Limit, kDefaultL1Limit) {}

GpuAttributeTableManager::GpuAttributeTableManager(size_t l0LimitBytes, size_t l1LimitBytes)
    : _l0LimitBytes(l0LimitBytes), _l1LimitBytes(l1LimitBytes) {
    // Create the shared execution context using the factory function
    auto backend = createDefaultBackend();
    _context = createExecutionContext(std::move(backend), kFramePoolSize);
}


//- DESTRUCTOR

GpuAttributeTableManager::~GpuAttributeTableManager() {
    clear();
}


//- ACCESSORS

GpuAttributeTableManager& GpuAttributeTableManager::instance() {
    static GpuAttributeTableManager sInstance;
    return sInstance;
}

bool GpuAttributeTableManager::isGpuAvailable() const {
    return _context && _context->getBackend() != nullptr;
}

size_t GpuAttributeTableManager::getTotalMemoryUsed() const {
    std::lock_guard<std::mutex> lock(_mutex);
    return _l0UsedBytes + _l1UsedBytes;
}

size_t GpuAttributeTableManager::getSegmentMemoryUsed(GpuSegment segment) const {
    std::lock_guard<std::mutex> lock(_mutex);
    return segment == GpuSegment::L0 ? _l0UsedBytes : _l1UsedBytes;
}

size_t GpuAttributeTableManager::getTableCount() const {
    std::lock_guard<std::mutex> lock(_mutex);
    return _tables.size();
}

GpuTableStats GpuAttributeTableManager::getTableStats(const GpuTableId& id) const {
    std::lock_guard<std::mutex> lock(_mutex);
    auto it = _tables.find(id);
    if (it != _tables.end()) {
        return it->second.stats;
    }
    return GpuTableStats{};
}

bool GpuAttributeTableManager::hasTable(const GpuTableId& id) const {
    std::lock_guard<std::mutex> lock(_mutex);
    return _tables.find(id) != _tables.end();
}

GpuAttributeTable* GpuAttributeTableManager::getTable(const GpuTableId& id) {
    std::lock_guard<std::mutex> lock(_mutex);
    auto it = _tables.find(id);
    if (it != _tables.end()) {
        return it->second.table.get();
    }
    return nullptr;
}

const GpuAttributeTable* GpuAttributeTableManager::getTable(const GpuTableId& id) const {
    std::lock_guard<std::mutex> lock(_mutex);
    auto it = _tables.find(id);
    if (it != _tables.end()) {
        return it->second.table.get();
    }
    return nullptr;
}


//- MODIFIERS

GpuAttributeTable* GpuAttributeTableManager::getOrCreate(const GpuTableId& id) {
    std::lock_guard<std::mutex> lock(_mutex);

    // Check if table already exists
    auto it = _tables.find(id);
    if (it != _tables.end()) {
        // Update access time and move to back of LAL
        it->second.stats.lastAccessTime = getCurrentTimeMs();
        it->second.stats.accessCount++;
        return it->second.table.get();
    }

    // Create new table entry
    TableEntry entry;
    entry.table = std::make_unique<GpuAttributeTable>();
    entry.stats.lastAccessTime = getCurrentTimeMs();
    entry.stats.accessCount = 1;
    entry.stats.segment = GpuSegment::L0;

    // Add to L0 LAL (back = most recently used)
    _l0Lal.push_back(id);
    entry.lalIterator = std::prev(_l0Lal.end());

    auto* tablePtr = entry.table.get();
    _tables.emplace(id, std::move(entry));

    return tablePtr;
}

bool GpuAttributeTableManager::remove(const GpuTableId& id) {
    std::lock_guard<std::mutex> lock(_mutex);

    auto it = _tables.find(id);
    if (it == _tables.end()) {
        return false;
    }

    auto& entry = it->second;
    size_t memSize = entry.stats.memorySizeBytes;

    // Remove from appropriate LAL
    if (entry.stats.segment == GpuSegment::L0) {
        _l0Lal.erase(entry.lalIterator);
        _l0UsedBytes -= memSize;
    } else {
        _l1Lal.erase(entry.lalIterator);
        _l1UsedBytes -= memSize;
    }

    _tables.erase(it);
    return true;
}

size_t GpuAttributeTableManager::evict(size_t targetBytes) {
    std::lock_guard<std::mutex> lock(_mutex);

    size_t freedBytes = 0;

    // First, evict from L1 (front of list = least recently used)
    while (freedBytes < targetBytes && !_l1Lal.empty()) {
        const auto& victimId = _l1Lal.front();
        freedBytes += evictOne(victimId);
    }

    // If still need more, evict from L0
    while (freedBytes < targetBytes && !_l0Lal.empty()) {
        const auto& victimId = _l0Lal.front();
        freedBytes += evictOne(victimId);
    }

    return freedBytes;
}

void GpuAttributeTableManager::promoteToL0(const GpuTableId& id) {
    std::lock_guard<std::mutex> lock(_mutex);

    auto it = _tables.find(id);
    if (it == _tables.end() || it->second.stats.segment == GpuSegment::L0) {
        return;
    }

    auto& entry = it->second;
    size_t memSize = entry.stats.memorySizeBytes;

    // Remove from L1 LAL
    _l1Lal.erase(entry.lalIterator);
    _l1UsedBytes -= memSize;

    // Add to L0 LAL
    _l0Lal.push_back(id);
    entry.lalIterator = std::prev(_l0Lal.end());
    entry.stats.segment = GpuSegment::L0;
    _l0UsedBytes += memSize;
}

void GpuAttributeTableManager::demoteToL1(const GpuTableId& id) {
    std::lock_guard<std::mutex> lock(_mutex);

    auto it = _tables.find(id);
    if (it == _tables.end() || it->second.stats.segment == GpuSegment::L1) {
        return;
    }

    auto& entry = it->second;
    size_t memSize = entry.stats.memorySizeBytes;

    // Remove from L0 LAL
    _l0Lal.erase(entry.lalIterator);
    _l0UsedBytes -= memSize;

    // Add to L1 LAL
    _l1Lal.push_back(id);
    entry.lalIterator = std::prev(_l1Lal.end());
    entry.stats.segment = GpuSegment::L1;
    _l1UsedBytes += memSize;
}

void GpuAttributeTableManager::recordAccess(const GpuTableId& id) {
    std::lock_guard<std::mutex> lock(_mutex);

    auto it = _tables.find(id);
    if (it == _tables.end()) {
        return;
    }

    auto& entry = it->second;
    entry.stats.lastAccessTime = getCurrentTimeMs();
    entry.stats.accessCount++;

    // Move to back of LAL (most recently used)
    if (entry.stats.segment == GpuSegment::L0) {
        _l0Lal.erase(entry.lalIterator);
        _l0Lal.push_back(id);
        entry.lalIterator = std::prev(_l0Lal.end());
    } else {
        _l1Lal.erase(entry.lalIterator);
        _l1Lal.push_back(id);
        entry.lalIterator = std::prev(_l1Lal.end());
    }
}

void GpuAttributeTableManager::clear() {
    std::lock_guard<std::mutex> lock(_mutex);

    _tables.clear();
    _l0Lal.clear();
    _l1Lal.clear();
    _l0UsedBytes = 0;
    _l1UsedBytes = 0;
}


//- PRIVATE HELPERS

size_t GpuAttributeTableManager::evictOne(const GpuTableId& id) {
    auto it = _tables.find(id);
    if (it == _tables.end()) {
        return 0;
    }

    auto& entry = it->second;
    size_t memSize = entry.stats.memorySizeBytes;

    // Remove from appropriate LAL
    if (entry.stats.segment == GpuSegment::L0) {
        _l0Lal.erase(entry.lalIterator);
        _l0UsedBytes -= memSize;
    } else {
        _l1Lal.erase(entry.lalIterator);
        _l1UsedBytes -= memSize;
    }

    _tables.erase(it);
    return memSize;
}

void GpuAttributeTableManager::updateMemoryUsage(const GpuTableId& id,
                                                  size_t oldSize,
                                                  size_t newSize) {
    auto it = _tables.find(id);
    if (it == _tables.end()) {
        return;
    }

    auto& entry = it->second;
    entry.stats.memorySizeBytes = newSize;

    if (entry.stats.segment == GpuSegment::L0) {
        _l0UsedBytes = _l0UsedBytes - oldSize + newSize;
    } else {
        _l1UsedBytes = _l1UsedBytes - oldSize + newSize;
    }
}

}  // namespace mongo::timeseries::hcindex::gpu

