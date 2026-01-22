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
#include "mongo/db/timeseries/timeseries_gen.h"

#include <cstdint>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace mongo::timeseries::hcindex {

//- FORWARD DECLARATIONS
class HCIndexWriter;

                        // ============================
                        // struct SymbolDictionaryState
                        // ============================
/**
 * - NOP: Initial state, no operations allowed
 * - Reconstruction: Dictionary is being reconstructed from stored operations
 * - ReadWrite: Dictionary is in normal write mode (new symbols can be added)
 * - ReadOnly: Dictionary is locked, no modifications allowed
 *
 * Transitions:
 * - NOP -> Reconstruction or ReadWrite
 * - Reconstruction -> ReadWrite - to accept new data after reconstruction
 * - Reconstruction -> ReadOnly
 * - ReadWrite -> ReadOnly
 * - ReadOnly -> (no transitions allowed)
 */
enum class SymbolDictionaryState {
    NOP,
    Reconstruction,
    ReadWrite,
    ReadOnly
};

                        // ======================
                        // class SymbolDictionary
                        // ======================

/**
 * Represents a single symbol dictionary for a specific time window. Symbols are encoded as 32-bit
 * unsigned integers (uint32_t). Valid symbols: 1 to (2^32 - 1) and 0 is reserved to denote missing
 * values. Some operational aspects are:
 * - Immutable: Once a symbol is assigned an index, it never changes
 * - Append only: New symbols always get new indices
 */
class SymbolDictionary : public ISymbolDictionary {

public:

    //- CONSTRUCTORS


    /**
     * Create a new empty symbol dictionary for the time window spanning
     * `windowStart` to `windowEnd`. The dictionary is configured with the
     * given `period` and `frequency`, and its persistent encoding lifecycle is
     * managed by `writer`. The returned dictionary is initialized in the NOP
     * state.
     */
    SymbolDictionary(
        HCIndexPeriodEnum period,
        int32_t frequency,
        Timestamp windowStart,
        Timestamp windowEnd,
        HCIndexWriter *writer);


    //- ACCESSORS


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
     * Return the memory usage of this dictionary in bytes. This is an
     * approximation and is not exact.
     */
    size_t getMemoryUsageBytes() const;

    /**
     * Return the window start timestamp.
     */
    Timestamp getWindowStart() const;

    /**
     * Return the window end timestamp.
     */
    Timestamp getWindowEnd() const;

    /**
     * Return 'true' if this dictionary is in a writable. Otherwise, return
     * 'false'.
     */
    bool isWritable() const;


    //- MODIFIERS


    /**
     * Return the symbol index for the specified 'word'. If the word is not
     * found, it is inserted and assigned a new index. Insertion will fail if
     * the dictionary is full. Note that the index starts from 1 and 0 index is
     * reserved to represent missing values in fetch requests.
     */
    StatusWith<uint32_t> getOrInsertSymbol(StringData word) override;

    /**
     * Insert the given 'word' into the dictionary with the specified 'index'.
     * Returns an error if the word already exists or if the index is invalid.
     */
    Status insertSymbol(StringData word, uint32_t index);

    /**
     * Change the state of this dictionary. Following transitions are possible:
     * - From NOP: can transition to Reconstruction or ReadWrite
     * - From Reconstruction: can transition to ReadOnly or ReadWrite
     * - From ReadWrite: can transition to ReadOnly
     * - From ReadOnly: no transitions allowed
     * Returns an error if the transition is invalid.
     */
    Status changeState(SymbolDictionaryState newState);

    /**
     * Set the writer for this dictionary. Dictionary cannot accept new symbols
     * unless it is in ReadWrite state and has a valid writer.
     */
    void setWriter(HCIndexWriter* writer);

    /**
     * Flush any pending operations to the database via the writer.
     */
    void flush();

private:

    //- DATA


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


// ============================================================================
//                          INLINE DEFINITIONS
// ============================================================================

                        // ----------------------
                        // class SymbolDictionary
                        // ----------------------

//- ACCESSORS

inline
Timestamp SymbolDictionary::getWindowStart() const {
    return _windowStart;
}

inline
Timestamp SymbolDictionary::getWindowEnd() const {
    return _windowEnd;
}

inline
bool SymbolDictionary::isWritable() const {
    std::shared_lock<std::shared_mutex> lock(_mutex);
    return _state == SymbolDictionaryState::ReadWrite ||
        _state == SymbolDictionaryState::Reconstruction;
}


//- MODIFIERS


inline
void SymbolDictionary::setWriter(HCIndexWriter* writer) {
    _writer = writer;
}

}  // namespace mongo::timeseries::hcindex
