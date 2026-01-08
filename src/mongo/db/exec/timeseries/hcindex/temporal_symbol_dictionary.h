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
#include "mongo/db/exec/timeseries/hcindex/hcindex_isymbol_dictionary.h"
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
 * State machine for SymbolDictionary lifecycle:
 * - NOP: Initial state, no operations allowed
 * - Reconstruction: Dictionary is being reconstructed from stored operations
 * - ReadWrite: Dictionary is in normal write mode (new symbols can be added)
 * - ReadOnly: Dictionary is locked, no modifications allowed
 *
 * State transitions:
 * - NOP -> Reconstruction (via changeState)
 * - NOP -> ReadWrite (via changeState)
 * - Reconstruction -> ReadOnly (via changeState)
 * - ReadWrite -> ReadOnly (via changeState)
 * - ReadOnly -> (no transitions allowed)
 */
enum class SymbolDictionaryState {
    NOP,
    Reconstruction,
    ReadWrite,
    ReadOnly
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
class SymbolDictionary : public ISymbolDictionary {

public:

    /**
     * Create a new symbol dictionary with the specified 'writer'.
     * The dictionary starts in NOP state. Use changeState() to transition to
     * Reconstruction, ReadWrite, or ReadOnly states.
     */
    SymbolDictionary(
        HCIndexPeriodEnum period,
        int32_t frequency,
        Timestamp windowStart,
        Timestamp windowEnd,
        HCIndexWriter *writer);

    /**
     * Return the symbol index for the specified 'word'. If the word is not
     * found, it is inserted and assigned a new index. Insertion will fail if the
     * dictionary is full. Note that the index starts from 1 and 0 index is
     * reserved to represent missing values in fetch requests.
     */
    StatusWith<uint32_t> getOrInsertSymbol(StringData word) override;

    /**
     * Insert a symbol directly with the specified index. This is used during
     * reconstruction from stored operations and does not require a writer.
     * Returns an error if the word already exists or if the index is invalid.
     */
    Status insertSymbol(StringData word, uint32_t index);

    /**
     * Change the state of this dictionary. Transitions are restricted:
     * - From NOP: can transition to Reconstruction or ReadWrite
     * - From Reconstruction: can transition to ReadOnly
     * - From ReadWrite: can transition to ReadOnly
     * - From ReadOnly: no transitions allowed
     * Returns an error if the transition is invalid.
     */
    Status changeState(SymbolDictionaryState newState);

    /**
     * Set the writer for this dictionary. This allows a dictionary
     * to be updated later.
     */
    void setWriter(HCIndexWriter* writer) {
        _writer = writer;
    }

    /**
     * Flush any pending operations to the database via the writer.
     */
    void flush();

    /**
     * Return the symbol index for the specified 'word' if found. Otherwise,
     * return boost::none.
     */
    boost::optional<uint32_t> getSymbolIndex(StringData word) const override;

    /**
     * Return the word at tht specified 'index' if found. Otherwise, return
     * boost::none.
     */
    boost::optional<StringData> getSymbol(uint32_t index) const override;

    /**
     * Return the total number of symbols in this dictionary.
     */
    size_t getSymbolCount() const override;

    /**
     * Return the memory usage of this dictionary in bytes.
     */
    size_t getMemoryUsageBytes() const;

    /**
     * Get the window start timestamp.
     */
    Timestamp getWindowStart() const {
        return _windowStart;
    }

    /**
     * Get the window end timestamp.
     */
    Timestamp getWindowEnd() const {
        return _windowEnd;
    }

    /**
     * Check if this dictionary is in a writable state (ReadWrite or Reconstruction).
     */
    bool isWritable() const {
        std::shared_lock<std::shared_mutex> lock(_mutex);
        return _state == SymbolDictionaryState::ReadWrite ||
               _state == SymbolDictionaryState::Reconstruction;
    }

private:
    // Bidirectional mapping for symbols
    std::unordered_map<std::string, uint32_t> _wordToIndex;
    std::vector<std::string> _indexToWord;

    // Next symbol index to assign (starts at 1, 0 is reserved)
    uint32_t _nextSymbolIndex = 1;

    // Period and frequency for time window calculation
    HCIndexPeriodEnum _period;
    int32_t _frequency;

    // Range that we cover
    Timestamp _windowStart;
    Timestamp _windowEnd;

    // Writer
    HCIndexWriter *_writer = nullptr;

    // Current state of the dictionary
    SymbolDictionaryState _state = SymbolDictionaryState::NOP;

    // Whether there are any pending operations
    bool _isDirty = false;

    // Synchronization
    mutable std::shared_mutex _mutex;
};

/**
 * Represents a delta-based symbol dictionary for a specific time window.
 *
 * Uses a 3-level lookup hierarchy to minimize storage:
 * 1. baseDictionary: Full snapshot from a previous epoch (immutable)
 * 2. inheritedDelta: Symbols inherited from a recent interval's delta (lazy)
 * 3. localDelta: New symbols added in THIS interval only
 *
 * Key properties:
 * - Lookup order: localDelta → inheritedDelta → baseDictionary
 * - Lazy inheritance: inheritedDelta is set when first needed, not upfront
 * - Base compaction: When consecutive intervals have similar deltas, merge into new base
 * - Thread-safe: Uses shared_mutex for concurrent access
 */
class DeltaSymbolDictionary : public ISymbolDictionary {
public:
    /**
     * Create a new delta symbol dictionary for the specified time window.
     *
     * @param period Time period granularity
     * @param frequency Frequency within the period
     * @param windowStart Start timestamp of this interval
     * @param windowEnd End timestamp of this interval
     * @param baseDictionary Pointer to the base dictionary (must outlive this object)
     * @param writer Writer for persisting operations (can be nullptr for read-only)
     * @param prevInterval1 Previous interval (N-1) for lazy inheritance (can be nullptr)
     * @param prevInterval2 Previous interval (N-2) for lazy inheritance (can be nullptr)
     */
    DeltaSymbolDictionary(HCIndexPeriodEnum period,
                          int32_t frequency,
                          Timestamp windowStart,
                          Timestamp windowEnd,
                          SymbolDictionary* baseDictionary,
                          HCIndexWriter* writer,
                          DeltaSymbolDictionary* prevInterval1 = nullptr,
                          DeltaSymbolDictionary* prevInterval2 = nullptr);

    /**
     * Look up or insert a symbol using the 3-level hierarchy.
     *
     * Lookup order:
     * 1. Check baseDictionary
     * 2. Check inheritedDelta (if set)
     * 3. Check localDelta
     * 4. If not found anywhere, add to localDelta
     *
     * Uses the prevInterval1 and prevInterval2 provided at construction time
     * for lazy inheritance.
     *
     * @param word The symbol to look up or insert
     * @return Symbol index or error
     */
    StatusWith<uint32_t> getOrInsertSymbol(StringData word) override;

    /**
     * Look up a symbol (read-only, no insertion).
     * Uses the same 3-level lookup hierarchy.
     */
    boost::optional<uint32_t> getSymbolIndex(StringData word) const override;

    /**
     * Decode a symbol index back to its string value.
     * Searches through all levels of the hierarchy.
     */
    boost::optional<StringData> getSymbol(uint32_t index) const override;

    /**
     * Get the effective delta (inheritedDelta + localDelta) for similarity comparison.
     * Returns a set of symbol strings that are in this interval's delta.
     */
    std::set<std::string> getEffectiveDelta() const;

    /**
     * Get just the local delta symbols.
     */
    const std::unordered_map<std::string, uint32_t>& getLocalDelta() const {
        return _localWordToIndex;
    }

    /**
     * Get the inherited delta symbols (empty if not inherited).
     */
    const std::unordered_map<std::string, uint32_t>& getInheritedDelta() const {
        return _inheritedWordToIndex;
    }

    /**
     * Check if this interval has inherited from another interval.
     */
    bool hasInheritedDelta() const {
        return _hasInheritedDelta;
    }

    /**
     * Get the window start timestamp of the interval we inherited from.
     * Only valid if hasInheritedDelta() is true.
     */
    boost::optional<Timestamp> getInheritedFromWindowStart() const {
        return _inheritedFromWindowStart;
    }

    /**
     * Get the base dictionary this interval uses.
     */
    SymbolDictionary* getBaseDictionary() const {
        return _baseDictionary;
    }

    /**
     * Set the base dictionary for this delta dictionary.
     * Used when reconstructing from disk where the base needs to be set after construction.
     */
    void setBaseDictionary(SymbolDictionary* baseDictionary) {
        _baseDictionary = baseDictionary;
        // Recalculate next symbol index based on the new base
        _nextSymbolIndex = baseDictionary ? baseDictionary->getSymbolCount() + 1 : 1;
    }

    /**
     * Set the writer for this dictionary.
     * Used when reconstructing from disk where the writer needs to be set after construction.
     */
    void setWriter(HCIndexWriter* writer) {
        _writer = writer;
    }

    /**
     * Get the window start timestamp.
     */
    Timestamp getWindowStart() const {
        return _windowStart;
    }

    /**
     * Get the window end timestamp.
     */
    Timestamp getWindowEnd() const {
        return _windowEnd;
    }

    /**
     * Get the next symbol index that will be assigned.
     */
    uint32_t getNextSymbolIndex() const {
        return _nextSymbolIndex;
    }

    /**
     * Set the next symbol index to assign.
     * 
     */
    void setNextSymbolIndex(uint32_t nextSymbolIndex) {
        _nextSymbolIndex = nextSymbolIndex;
    }

    /**
     * Change the state of this dictionary.
     */
    Status changeState(SymbolDictionaryState newState);

    /**
     * Flush pending operations to the database.
     */
    void flush();

    /**
     * Get total symbol count (base + inherited + local).
     * Implements ISymbolDictionary interface.
     */
    size_t getSymbolCount() const override;

    /**
     * Get memory usage in bytes.
     */
    size_t getMemoryUsageBytes() const;

    /**
     * Get the window start of the base dictionary this delta references.
     * Returns boost::none if no base dictionary is set.
     */
    boost::optional<Timestamp> getBaseDictionaryWindowStart() const {
        if (_baseDictionary) {
            return _baseDictionary->getWindowStart();
        }
        return boost::none;
    }

private:
    /**
     * Try to inherit delta from the given previous interval.
     * Called lazily when a symbol is not found in base or local.
     */
    bool tryInheritFrom(DeltaSymbolDictionary* prevInterval);

    // Base dictionary (full snapshot, immutable reference)
    SymbolDictionary* _baseDictionary;

    // Previous intervals for lazy inheritance (can be nullptr)
    DeltaSymbolDictionary* _prevInterval1 = nullptr;
    DeltaSymbolDictionary* _prevInterval2 = nullptr;

    // Inherited delta (set lazily when first needed)
    bool _hasInheritedDelta = false;
    boost::optional<Timestamp> _inheritedFromWindowStart;
    std::unordered_map<std::string, uint32_t> _inheritedWordToIndex;
    std::vector<std::string> _inheritedIndexToWord;

    // Local delta (symbols new to THIS interval)
    std::unordered_map<std::string, uint32_t> _localWordToIndex;
    std::vector<std::string> _localIndexToWord;

    // Next symbol index to assign (continues from base + inherited)
    uint32_t _nextSymbolIndex;
    uint32_t _numEncodedSymbols;

    // Time window for this interval
    HCIndexPeriodEnum _period;
    int32_t _frequency;
    Timestamp _windowStart;
    Timestamp _windowEnd;

    // Writer for persistence
    HCIndexWriter* _writer = nullptr;

    // State and dirty tracking
    SymbolDictionaryState _state = SymbolDictionaryState::NOP;
    bool _isDirty = false;

    // Synchronization
    mutable std::shared_mutex _mutex;
};

/**
 * Compute similarity between two sets of symbols.
 * Returns a value between 0.0 (no overlap) and 1.0 (identical).
 * Uses Jaccard similarity: |A ∩ B| / |A ∪ B|
 */
double computeDeltaSimilarity(const std::set<std::string>& delta1,
                               const std::set<std::string>& delta2);

/**
 * Default similarity threshold for triggering base compaction.
 * When two consecutive intervals' deltas have similarity >= this value,
 * a new base dictionary is created.
 */
constexpr double kDefaultSimilarityThreshold = 0.8;

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
