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

#include "mongo/base/string_data.h"
#include "mongo/bson/timestamp.h"
#include "mongo/db/exec/timeseries/hcindex/isymbol_dictionary.h"
#include "mongo/db/exec/timeseries/hcindex/delta_symbol_dictionary.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/timeseries/timeseries_gen.h"
#include "mongo/util/uuid.h"

#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace mongo::timeseries::hcindex {

//- FORWARD DECLARATIONS
class HCIndexReader;
class HCIndexWriter;

/**
 * Manages temporal symbol dictionaries for a timeseries collection.
 *
 * Creates and maintains separate symbol dictionaries for each time window,
 * allowing for natural schema evolution and bounded memory usage.
 *
 * Key features:
 * - Time-window scoped: Each window has its own dictionary
 * - Configurable period and frequency: Hour/Minute/Second with custom frequencies
 * - Automatic window management: Creates dictionaries on-demand
 * - Cleanup support: Can remove old dictionaries to free memory
 * - Delta-based storage: Uses base + inherited + local for space efficiency
 * - Thread-safe: Safe for concurrent access from multiple threads
 */
class TemporalSymbolDictionary {
public:
    /**
     * Create a new temporal symbol dictionary manager for managing symbol
     * dictionaries for timeseries collections having the specified
     * 'collectionUUID' with the given 'period' and 'frequency'. Behavior is undefined
     * unless the 'collectionUUID' is valid through the lifetime of this object.
     * The 'writer' can be nullptr if this dictionary is being constructed by a
     * reader (in which case no new operations will be written).
     * The 'reader' can be nullptr if reconstruction from disk is not needed.
     */
    TemporalSymbolDictionary(const UUID& collectionUUID,
                             HCIndexPeriodEnum period,
                             int32_t frequency,
                             HCIndexWriter *writer,
                             class HCIndexReader *reader = nullptr);

    /**
     * Returns a pointer to the symbol dictionary covering the time window that
     * includes the specified 'timestamp' if found. Otherwise, create a new
     * dictionary for the time window covering the 'timestamp' and return a
     * pointer to that dictionary. Note that the returned pointer is valid for
     * the lifetime of this TemporalSymbolDictionary. Returns an error if the
     * dictionary for the time window covering the 'timestamp' cannot be
     * created. opCtx is required for reconstruction from disk if the dictionary
     * is not in memory.
     */
    StatusWith<ISymbolDictionary*> getOrCreateDictionaryForTimestamp(OperationContext* opCtx,
                                                                     const Timestamp& ts);

    /**
     * Returns a pointer to the symbol dictionary covering the time window
     * that includes the specified 'timestamp' if found. Otherwise, return an
     * error. Note that the returned pointer is valid for the lifetime of this
     * TemporalSymbolDictionary.
     */
    StatusWith<ISymbolDictionary*> getDictionaryForTimestamp(const Timestamp& ts) const;

    /**
     * Encode the specified 'word' for the time window that includes the
     * specified 'timestamp' and reuturn the symbol index. If fails, return an
     * error. Note that symbols start from index 1.
     */
    StatusWith<uint32_t> encodeSymbol(StringData word, const Timestamp& timestamp);

    /**
     * Decode the word at the specified 'index' in the dictionary covering the
     * time window that includes the specified 'timestamp' and return the word.
     * If fails, return boost::none.
     */
    boost::optional<StringData> decodeSymbol(uint32_t index, const Timestamp& timestamp) const;

    /**
     * Return the time window boundaries for a given 'timestamp'.
     */
    std::pair<Timestamp, Timestamp> getWindowForTimestamp(const Timestamp& timestamp) const;

    /**
     * Remove dictionaries serving time windows older than the specified
     * 'beforeTimestamp'. This is used for cleanup to free memory from old
     * dictionaries.
     */
    Status cleanupOldDictionaries(const Timestamp& beforeTimestamp);

    /**
     * Statistics about this temporal dictionary.
     * TODO: Find out how stats is done in mongodb! For now this is for
     * debug/testing purpose.
     */
    struct Stats {
        size_t totalDictionaries;
        size_t totalSymbols;
        size_t memoryUsageBytes;
    };

    /**
     * Return the usage statistics.
     */
    Stats getStats() const;

    /**
     * Flush all pending operations to the database.
     */
    void flush();

private:
    /**
     * Create or Fetch the dictionary for the time window that starts at the
     * specified 'windowStart' timestamp. opCtx is required for reconstruction
     * from disk if the dictionary is not in memory.
     */
    StatusWith<SymbolDictionary*> getOrCreateDictionary(OperationContext* opCtx,
                                                        const Timestamp& windowStart);

    /**
     * Create or Fetch the delta dictionary for the time window that starts at the
     * specified 'windowStart' timestamp.
     */
    StatusWith<DeltaSymbolDictionary*> getOrCreateDeltaDictionary(OperationContext* opCtx,
                                                                   const Timestamp& windowStart);

    /**
     * Check if the previous two intervals have similar deltas, and if so,
     * create a new base dictionary by merging the old base with those deltas.
     * Returns the new base (or the existing base if no merge happened).
     */
    SymbolDictionary* maybeCreateNewBase(const Timestamp& windowStart);

    /**
     * Get the previous two delta intervals relative to the given windowStart.
     * Returns nullptrs if the intervals don't exist.
     */
    std::pair<DeltaSymbolDictionary*, DeltaSymbolDictionary*> getPreviousTwoIntervals(
        const Timestamp& windowStart);

    /**
     * Return the window start timestamp for the time window that includes the
     * specified 'timestamp'.
     */
    Timestamp calculateWindowStart(const Timestamp& timestamp) const;

    /**
     * Return the window end timestamp for the time window that starts at the
     * specified 'windowStart' timestamp.
     */
    Timestamp calculateWindowEnd(const Timestamp& windowStart) const;

    /**
     * Return the previous window start timestamp.
     */
    Timestamp calculatePreviousWindowStart(const Timestamp& windowStart) const;

    // Map: windowStart → SymbolDictionary
    // Stores base dictionaries that delta dictionaries reference.
    // We want to clean up older dictionaries.
    // TODO: In future, we can create a projection of this map to an LRU
    // iterator to eject unused dictionaries.
    std::map<Timestamp, std::unique_ptr<SymbolDictionary>> _dictionaries;

    // Delta dictionary support
    // Map: windowStart → DeltaSymbolDictionary
    std::map<Timestamp, std::unique_ptr<DeltaSymbolDictionary>> _deltaDictionaries;

    // Current base dictionary pointer (owned by _dictionaries)
    SymbolDictionary* _currentBase = nullptr;

    // Similarity threshold for creating new bases
    double _similarityThreshold = kDefaultSimilarityThreshold;

    // Collection UUID for this temporal dictionary
    UUID _collectionUUID;

    // Period (hour, minute, second)
    HCIndexPeriodEnum _period;

    // Frequency (1-24 for hour, 1-59 for minute/second)
    int32_t _frequency;

    // Writer (can be nullptr if constructed by reader)
    HCIndexWriter *_writer = nullptr;

    // Reader (can be nullptr if reconstruction from disk is not needed)
    HCIndexReader *_reader = nullptr;

    // Synchronization
    mutable std::shared_mutex _mutex;
};

}  // namespace mongo::timeseries::hcindex
