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
#include "mongo/db/operation_context.h"
#include "mongo/util/uuid.h"

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <shared_mutex>

namespace mongo::timeseries::hcindex {

/**
 * Granularity level of the dictionary
 * TODO. Add AUTO to let the system decide the best granularity based on the
 * density and cardinality of the data.
 */
enum class DictionaryGranularity {
    AUTO,       // Not used. This now defaults to HOURLY
    DAILY,      // every day (00:00:00 UTC)
    HOURLY,     // every hour
    THIRTY_MIN, // every 30 minutes
    TEN_MIN,    // every 10 minutes
    FIVE_MIN,   // every 5 minutes
};

/**
 * Represents a single symbol dictionary for a specific time window.
 *
 * Symbols are encoded as 32-bit unsigned integers (uint32_t):
 * - Valid symbols: 1 to 4,294,967,295 (2^32 - 1)
 * - Reserved value: 0 (used to denote missing values)
 *
 * Key properties:
 * - Immutable: Once a symbol is assigned an index, it never changes
 * - Append-only: New symbols always get new indices
 * - Thread-safe: Uses shared_mutex for concurrent access
 */
class SymbolDictionary {
public:
    /**
     * Return the symbol index for the specified 'word'. If the word is not
     * found, it is inserted and assigned a new index. Insertion will fail if the
     * dictionary is full. Note that the index starts from 1 and 0 index is
     * reserved to represent missing values in fetch requests.
     */
    StatusWith<uint32_t> getOrInsertSymbol(StringData word);

    /**
     * Return the symbol index for the specified 'word' if found. Otherwise,
     * return boost::none.
     */
    boost::optional<uint32_t> getSymbolIndex(StringData word) const;

    /**
     * Return the word at tht specified 'index' if found. Otherwise, return
     * boost::none.
     */
    boost::optional<StringData> getSymbol(uint32_t index) const;

    /**
     * Return the total number of symbols in this dictionary.
     */
    size_t getSymbolCount() const;

    /**
     * Return the memory usage of this dictionary in bytes.
     */
    size_t getMemoryUsageBytes() const;

private:
    // Bidirectional mapping for symbols
    std::unordered_map<std::string, uint32_t> wordToIndex;
    std::vector<std::string> indexToWord;

    // Next symbol index to assign (starts at 1, 0 is reserved)
    uint32_t nextSymbolIndex = 1;

    // Synchronization
    mutable std::shared_mutex mutex;
};

/**
 * Manages temporal symbol dictionaries for a timeseries collection.
 *
 * Creates and maintains separate symbol dictionaries for each time window,
 * allowing for natural schema evolution and bounded memory usage.
 *
 * Key features:
 * - Time-window scoped: Each window has its own dictionary
 * - Configurable granularity: DAILY, HOURLY, THIRTY_MIN, TEN_MIN, FIVE_MIN
 * - Automatic window management: Creates dictionaries on-demand
 * - Cleanup support: Can remove old dictionaries to free memory
 * - Thread-safe: Safe for concurrent access from multiple threads
 */
class TemporalSymbolDictionary {
public:
    /**
     * Create a new temporal symbol dictionary manager for managing symbol
     * dictionaries for timeseries collections having the specified
     * 'collectionUUID' with the given 'granularity'. Behavior is undefined
     * unless 'opCtx' and the 'collectionUUID' is valid through the lifetime of
     * this object.
     */
    TemporalSymbolDictionary(OperationContext* opCtx,
                             const UUID& collectionUUID,
                             DictionaryGranularity granularity);

    /**
     * Returns a pointer to the symbol dictionary covering the time window that
     * includes the specified 'timestamp' if found. Otherwise, create a new
     * dictionary for the time window covering the 'timestamp' and return a
     * pointer to that dictionary. Note that the returned pointer is valid for
     * the lifetime of this TemporalSymbolDictionary. Returns an error if the
     * dictionary for the time window covering the 'timestamp' cannot be
     * created.
     */
    StatusWith<SymbolDictionary*> getOrCreateDictionaryForTimestamp(const Timestamp& ts);

    /**
     * Returns a pointer to the symbol dictionary covering the time window
     * that includes the specified 'timestamp' if found. Otherwise, return an
     * error. Note that the returned pointer is valid for the lifetime of this
     * TemporalSymbolDictionary.
     */
    StatusWith<SymbolDictionary*> getDictionaryForTimestamp(const Timestamp& ts) const;

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

private:
    /**
     * Create or Fetch the dictionary for the time window that starts at the
     * specified 'windowStart' timestamp.
     */
    StatusWith<SymbolDictionary*> getOrCreateDictionary(const Timestamp& windowStart);

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

    // Map: windowStart → SymbolDictionary
    // We want to clean up older dictionaries.
    // TODO: In future, we can create a projection of this map to an LRU
    // iterator to eject unused dictionaries.
    std::map<Timestamp, std::unique_ptr<SymbolDictionary>> dictionaries;

    // Collection UUID for this temporal dictionary
    UUID collectionUUID;

    // Granularity level
    DictionaryGranularity granularity;

    // Context
    OperationContext* opCtx;

    // Synchronization
    mutable std::shared_mutex mutex;
};

}  // namespace mongo::timeseries::hcindex
