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

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <list>

#include "mongo/db/exec/timeseries/hcindex/gpu/execution_frame.h"
#include "mongo/db/exec/timeseries/hcindex/gpu/gpu_attribute_table.h"

namespace mongo::timeseries::hcindex::gpu {

/**
 * Unique identifier for a GPU attribute table within the manager.
 * Typically corresponds to a collection namespace or UUID.
 */
using GpuTableId = std::string;

/**
 * Segment type for GPU memory partitioning.
 */
enum class GpuSegment {
    /** L0: Active data that is frequently modified or read. */
    L0,

    /** L1: Read-only data that is read frequently but not modified. */
    L1,
};

/**
 * Statistics for a managed GPU table.
 */
struct GpuTableStats {
    size_t rowCount = 0;
    size_t columnCount = 0;
    size_t memorySizeBytes = 0;
    uint64_t lastAccessTime = 0;
    uint64_t accessCount = 0;
    GpuSegment segment = GpuSegment::L0;
};

/**
 * GpuAttributeTableManager manages the lifecycle and memory of GPU attribute tables.
 *
 * This manager provides:
 * - Centralized creation and destruction of GpuAttributeTable instances
 * - GPU memory partitioning into L0 (active) and L1 (read-only) segments
 * - Least-Active-List (LAL) eviction policy for memory pressure
 * - Shared ExecutionContext across all managed tables
 * - Statistics and monitoring
 *
 * Memory Management:
 *   L0 Segment: For tables that are actively being written to or queried frequently.
 *               Higher priority, not evicted unless memory is critically low.
 *
 *   L1 Segment: For tables that are read-only and less frequently accessed.
 *               Subject to LAL eviction when memory pressure is high.
 *
 * Usage:
 *   auto& manager = GpuAttributeTableManager::instance();
 *   auto* table = manager.getOrCreate("mydb.mycollection");
 *   table->uploadFromCpu(cpuTable);
 *   auto results = table->filter(predicates);
 */
class GpuAttributeTableManager {

public:

    //- CONSTRUCTORS

    /**
     * Create a manager with default settings.
     */
    GpuAttributeTableManager();

    /**
     * Create a manager with specific memory limits.
     * @param l0LimitBytes Maximum memory for L0 segment
     * @param l1LimitBytes Maximum memory for L1 segment
     */
    GpuAttributeTableManager(size_t l0LimitBytes, size_t l1LimitBytes);


    //- DESTRUCTOR

    ~GpuAttributeTableManager();


    //- ACCESSORS

    /**
     * Return the singleton instance of the manager.
     */
    static GpuAttributeTableManager& instance();

    /**
     * Return true if GPU is available for acceleration.
     */
    bool isGpuAvailable() const;

    /**
     * Return the total GPU memory used by all managed tables.
     */
    size_t getTotalMemoryUsed() const;

    /**
     * Return memory used by a specific segment.
     */
    size_t getSegmentMemoryUsed(GpuSegment segment) const;

    /**
     * Return the number of managed tables.
     */
    size_t getTableCount() const;

    /**
     * Return statistics for a specific table.
     */
    GpuTableStats getTableStats(const GpuTableId& id) const;

    /**
     * Check if a table exists in the manager.
     */
    bool hasTable(const GpuTableId& id) const;

    /**
     * Get a table by ID (returns nullptr if not found).
     */
    GpuAttributeTable* getTable(const GpuTableId& id);
    const GpuAttributeTable* getTable(const GpuTableId& id) const;


    //- MODIFIERS

    /**
     * Get or create a table with the given ID.
     * If the table doesn't exist, creates a new empty table in L0 segment.
     * @return Pointer to the table (never null if GPU is available)
     */
    GpuAttributeTable* getOrCreate(const GpuTableId& id);

    /**
     * Remove a table from the manager and free its GPU memory.
     * @return true if the table was found and removed
     */
    bool remove(const GpuTableId& id);

    /**
     * Evict tables to free at least targetBytes of memory.
     * Uses LAL policy to select victims from L1 segment first.
     * @return Actual bytes freed
     */
    size_t evict(size_t targetBytes);

    /**
     * Promote a table from L1 to L0 segment.
     * L0 tables are protected from eviction.
     */
    void promoteToL0(const GpuTableId& id);

    /**
     * Demote a table from L0 to L1 segment.
     * L1 tables can be evicted under memory pressure.
     */
    void demoteToL1(const GpuTableId& id);

    /**
     * Record an access to update LAL ordering.
     */
    void recordAccess(const GpuTableId& id);

    /**
     * Clear all tables and free all GPU memory.
     */
    void clear();


private:

    /**
     * Internal entry for a managed table.
     */
    struct TableEntry {
        std::unique_ptr<GpuAttributeTable> table;
        GpuTableStats stats;
        // Iterator into LAL for O(1) removal
        std::list<GpuTableId>::iterator lalIterator;
    };

    /**
     * Evict a single table (internal helper).
     */
    size_t evictOne(const GpuTableId& id);

    /**
     * Update memory tracking when a table changes size.
     */
    void updateMemoryUsage(const GpuTableId& id, size_t oldSize, size_t newSize);

    // Shared execution context for all tables
    std::unique_ptr<ExecutionContext> _context;

    // All managed tables keyed by ID
    std::unordered_map<GpuTableId, TableEntry> _tables;

    // Least-Active-Lists for eviction (front = least active)
    std::list<GpuTableId> _l0Lal;  // L0 segment LAL
    std::list<GpuTableId> _l1Lal;  // L1 segment LAL

    // Memory limits
    size_t _l0LimitBytes;
    size_t _l1LimitBytes;

    // Current memory usage
    size_t _l0UsedBytes = 0;
    size_t _l1UsedBytes = 0;

    // Thread safety
    mutable std::mutex _mutex;
};

}  // namespace mongo::timeseries::hcindex::gpu
