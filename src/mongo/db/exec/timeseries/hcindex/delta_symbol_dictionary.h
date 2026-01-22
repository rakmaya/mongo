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
#include "mongo/db/exec/timeseries/hcindex/symbol_dictionary.h"
#include "mongo/db/timeseries/timeseries_gen.h"

#include <cstdint>
#include <set>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace mongo::timeseries::hcindex {

//- FORWARD DECLARATIONS
class HCIndexWriter;

                        // ===========================
                        // class DeltaSymbolDictionary
                        // ===========================
/**
 * Represents a delta-based symbol dictionary for a specific time window.
 *
 * Uses a 3-level lookup hierarchy to minimize storage:
 * 1. baseDictionary: Full snapshot from a previous epoch (immutable)
 * 2. inheritedDelta: Symbols inherited from a recent interval's delta (lazy)
 * 3. localDelta: New symbols added in THIS interval only
 *
 * Some operation info:
 * - Lookup order: baseDictionary -> inheritedDelta -> localDelta
 * - Lazy inheritance: inheritedDelta is set when first needed, not upfront
 * - Base compaction: When consecutive intervals have similar deltas, merge into
 *   new base.
 */
class DeltaSymbolDictionary : public ISymbolDictionary {
public:

    //- CONSTRUCTORS


    /**
     * Creates a delta symbol dictionary for the time window [`windowStart`, `windowEnd`)
     * that tracks symbol changes relative to `baseDictionary`, with optional lazy
     * inheritance from up to two previous intervals to reduce duplication. The
     * dictionary is configured with the given `period` and `frequency` and persists the
     * updates via `writer` (or operates read-only if nullptr). Behavior is undefined unless
     * the referenced base dictionary is valid througout the life of this delta dictionary.
     * The dictionary is initialized in the NOP state.
     */
    DeltaSymbolDictionary(HCIndexPeriodEnum period,
                          int32_t frequency,
                          Timestamp windowStart,
                          Timestamp windowEnd,
                          SymbolDictionary* baseDictionary,
                          HCIndexWriter* writer,
                          DeltaSymbolDictionary* prevInterval1 = nullptr,
                          DeltaSymbolDictionary* prevInterval2 = nullptr);


    //- ACCESSORS


    /**
     * Returns the symbol index for the specified `word` if found (using the same 3-level lookup
     * hierarchy). Returns boost::none if not found.
     */
    boost::optional<uint32_t> getSymbolIndex(StringData word) const override;

    /**
     * Decode a symbol index back to its string value.
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
    const std::unordered_map<std::string, uint32_t>& getLocalDelta() const;

    /**
     * Get the inherited delta symbols (empty if not inherited).
     */
    const std::unordered_map<std::string, uint32_t>& getInheritedDelta() const;

    /**
     * Check if this interval has inherited from another interval.
     */
    bool hasInheritedDelta() const;

    /**
     * Get the window start timestamp of the interval we inherited from.
     * Only valid if hasInheritedDelta() is true.
     */
    boost::optional<Timestamp> getInheritedFromWindowStart() const;

    /**
     * Get the base dictionary this interval uses.
     */
    SymbolDictionary* getBaseDictionary() const;

    /**
     * Get the window start timestamp.
     */
    Timestamp getWindowStart() const;

    /**
     * Get the window end timestamp.
     */
    Timestamp getWindowEnd() const;

    /**
     * Get the next symbol index that will be assigned.
     */
    uint32_t getNextSymbolIndex() const;

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
    boost::optional<Timestamp> getBaseDictionaryWindowStart() const;


    //- MODIFIERS


    /**
     * Returns the symbol index for the specified `word` using the hierarchy baseDictionary, then
     * inherited deltas (from the constructed prev intervals) and finaly in the localDelta. Inserts
     * the symbol into the localDelta if absent, and returns the symbol index, or an error if the
     * insert fails.
     */
    StatusWith<uint32_t> getOrInsertSymbol(StringData word) override;

    /**
     * Set the base dictionary for this delta dictionary.
     * Used when reconstructing from disk where the base needs to be set after construction.
     */
    void setBaseDictionary(SymbolDictionary* baseDictionary);

    /**
     * Set the writer for this dictionary.
     * Used when reconstructing from disk where the writer needs to be set after construction.
     */
    void setWriter(HCIndexWriter* writer);

    /**
     * Set the next symbol index to assign.
     *
     */
    void setNextSymbolIndex(uint32_t nextSymbolIndex);

    /**
     * Insert a symbol directly into the local dictionary with a specific index.
     * Used during reconstruction from disk where symbols are read with their stored indices.
     * Does not write to the persistence layer (assumes we're reconstructing from persisted data).
     *
     * @param word The symbol to insert
     * @param index The symbol index to use
     * @return Status::OK() on success, error if symbol already exists with different index
     */
    Status insertLocalSymbolDirect(StringData word, uint32_t index);

    /**
     * Change the state of this dictionary.
     */
    Status changeState(SymbolDictionaryState newState);

    /**
     * Flush pending operations to the database.
     */
    void flush();

private:

    //- PRIVATE METHODS


    /**
     * Try to inherit delta from the given previous interval. Called lazily when a symbol is not
     * found in base or local.
     */
    bool tryInheritFrom(DeltaSymbolDictionary* prevInterval);


    //- DATA


    // Base dictionary
    SymbolDictionary* _baseDictionary;

    // Previous intervals for lazy inheritance
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
 * Compute similarity between two sets of symbols and returns a value between 0.0 (no overlap)
 * and 1.0 (identical). For now, this is a rough measure (Jaccard similarity)
 */
double computeDeltaSimilarity(const std::set<std::string>& delta1,
                              const std::set<std::string>& delta2);

/**
 * Default similarity threshold for triggering base compaction. We create a new base dictionary
 * when two consecutive intervals' deltas have similarity >= this value.
 */
constexpr double kDefaultSimilarityThreshold = 0.8;


// ============================================================================
//                          INLINE DEFINITIONS
// ============================================================================

                        // ---------------------------
                        // class DeltaSymbolDictionary
                        // ---------------------------

//- ACCESSORS


inline
const std::unordered_map<std::string, uint32_t>& DeltaSymbolDictionary::getLocalDelta() const {
    return _localWordToIndex;
}

inline
const std::unordered_map<std::string, uint32_t>& DeltaSymbolDictionary::getInheritedDelta() const {
    return _inheritedWordToIndex;
}

inline
bool DeltaSymbolDictionary::hasInheritedDelta() const {
    return _hasInheritedDelta;
}

inline
boost::optional<Timestamp> DeltaSymbolDictionary::getInheritedFromWindowStart() const {
    return _inheritedFromWindowStart;
}

inline
SymbolDictionary* DeltaSymbolDictionary::getBaseDictionary() const {
    return _baseDictionary;
}

inline
Timestamp DeltaSymbolDictionary::getWindowStart() const {
    return _windowStart;
}

inline
Timestamp DeltaSymbolDictionary::getWindowEnd() const {
    return _windowEnd;
}

inline
uint32_t DeltaSymbolDictionary::getNextSymbolIndex() const {
    return _nextSymbolIndex;
}

inline
boost::optional<Timestamp> DeltaSymbolDictionary::getBaseDictionaryWindowStart() const {
    if (_baseDictionary) {
        return _baseDictionary->getWindowStart();
    }
    return boost::none;
}


//- MODIFIERS


inline
void DeltaSymbolDictionary::setBaseDictionary(SymbolDictionary* baseDictionary) {
    _baseDictionary = baseDictionary;
    _nextSymbolIndex = baseDictionary ? baseDictionary->getSymbolCount() + 1 : 1;
}

inline
void DeltaSymbolDictionary::setWriter(HCIndexWriter* writer) {
    _writer = writer;
}

inline
void DeltaSymbolDictionary::setNextSymbolIndex(uint32_t nextSymbolIndex) {
    _nextSymbolIndex = nextSymbolIndex;
}


}  // namespace mongo::timeseries::hcindex
