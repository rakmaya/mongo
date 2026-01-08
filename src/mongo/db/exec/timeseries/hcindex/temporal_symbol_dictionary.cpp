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

#include "mongo/db/exec/timeseries/hcindex/temporal_symbol_dictionary.h"
#include "mongo/db/exec/timeseries/hcindex/hcindex_writer.h"
#include "mongo/db/exec/timeseries/hcindex/hcindex_reader.h"
#include "mongo/base/error_codes.h"
#include "mongo/logv2/log.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kQuery

namespace mongo::timeseries::hcindex {

// ============================================================================
// SymbolDictionary Implementation
// ============================================================================

SymbolDictionary::SymbolDictionary(
    HCIndexPeriodEnum period,
    int32_t frequency,
    Timestamp windowStart,
    Timestamp windowEnd,
    HCIndexWriter *writer)
    : _nextSymbolIndex(1)
    , _period(period)
    , _frequency(frequency)
    , _windowStart(windowStart)
    , _windowEnd(windowEnd)
    , _writer(writer)
    , _isDirty(false)
{
}

Status SymbolDictionary::changeState(SymbolDictionaryState newState) {
    std::unique_lock<std::shared_mutex> lock(_mutex);

    // Once in ReadOnly state, no transitions are allowed
    if (_state == SymbolDictionaryState::ReadOnly) {
        return Status(ErrorCodes::InternalError, "Cannot change state of read-only dictionary");
    }

    // Validate state transitions
    if (_state == SymbolDictionaryState::NOP) {
        if (newState != SymbolDictionaryState::Reconstruction &&
            newState != SymbolDictionaryState::ReadWrite) {
            return Status(ErrorCodes::InternalError,
                "Invalid state transition from NOP to " + std::to_string(static_cast<int>(newState)));
        }
    } else if (_state == SymbolDictionaryState::Reconstruction) {
        // From Reconstruction, can transition to ReadWrite (to accept new symbols) or ReadOnly
        if (newState != SymbolDictionaryState::ReadWrite &&
            newState != SymbolDictionaryState::ReadOnly) {
            return Status(ErrorCodes::InternalError,
                "Invalid state transition from Reconstruction to " + std::to_string(static_cast<int>(newState)));
        }
    } else if (_state == SymbolDictionaryState::ReadWrite) {
        // From ReadWrite, can only transition to ReadOnly
        if (newState != SymbolDictionaryState::ReadOnly) {
            return Status(ErrorCodes::InternalError,
                "Invalid state transition from ReadWrite to " + std::to_string(static_cast<int>(newState)));
        }
    }

    _state = newState;
    return Status::OK();
}

StatusWith<uint32_t> SymbolDictionary::getOrInsertSymbol(StringData word) {
    std::unique_lock<std::shared_mutex> lock(_mutex);

    std::string wordStr = std::string(word);

    // Check if word already exists
    auto it = _wordToIndex.find(wordStr);
    if (it != _wordToIndex.end()) {
        return it->second;
    }

    // Check state: modifications only allowed in Reconstruction or ReadWrite modes
    if (_state == SymbolDictionaryState::NOP) {
        return Status(ErrorCodes::InternalError, "Dictionary is in NOP state, cannot insert symbols");
    }
    if (_state == SymbolDictionaryState::ReadOnly) {
        return Status(ErrorCodes::InternalError, "Dictionary is read-only, cannot insert symbols");
    }

    // Check if dictionary is full (max uint32_t is 2^32 - 1, but 0 is reserved)
    if (_nextSymbolIndex == 0) {
        return Status(ErrorCodes::BadValue, "Symbol dictionary is full");
    }

    // In ReadWrite mode, writer must be set for modifications
    if (_state == SymbolDictionaryState::ReadWrite && _writer == nullptr) {
        return Status(ErrorCodes::InternalError, "Dictionary in ReadWrite mode requires a writer");
    }

    // In Reconstruction mode, we don't need a writer
    // In ReadWrite mode, we need to call the writer

    if (_state == SymbolDictionaryState::ReadWrite) {
        // If this is an empty dictionary, we need to do an INIT operation
        if (_indexToWord.empty()) {
            // For base dictionaries: no REF, local index offset starts at 1
            if (!_writer->initSymbolDictionary(_windowStart, _windowEnd, boost::none, 1).isOK()) {
                return Status(ErrorCodes::InternalError, "Could not initialize symbol dictionary");
            }
        }

        _isDirty = true;

        // Insert new symbol
        uint32_t symbolIndex = _nextSymbolIndex++;
        _wordToIndex[wordStr] = symbolIndex;
        _indexToWord.push_back(wordStr);

        // Add the symbol to the writer
        if (!_writer->addSymbol(_windowStart, _windowEnd, wordStr, symbolIndex).isOK()) {
            return Status(ErrorCodes::InternalError, "Could not add symbol to writer");
        }

        return symbolIndex;
    } else {
        // Reconstruction mode: just insert without writer
        uint32_t symbolIndex = _nextSymbolIndex++;
        _wordToIndex[wordStr] = symbolIndex;
        _indexToWord.push_back(wordStr);
        return symbolIndex;
    }
}

void SymbolDictionary::flush()
{
    std::unique_lock<std::shared_mutex> lock(_mutex);

    if (!_isDirty || _writer == nullptr) {
        return;  // Nothing to flush
    }

    // Flush pending operations via the writer
    if (!_writer->flush(_windowStart, _windowEnd, _period, _frequency, true).isOK()) {
        return;  // Could not flush
    }

    _isDirty = false;
}

boost::optional<uint32_t> SymbolDictionary::getSymbolIndex(StringData word) const {
    std::shared_lock<std::shared_mutex> lock(_mutex);

    auto it = _wordToIndex.find(std::string(word));
    if (it != _wordToIndex.end()) {
        return it->second;
    }

    return boost::none;
}

boost::optional<StringData> SymbolDictionary::getSymbol(uint32_t index) const {
    std::shared_lock<std::shared_mutex> lock(_mutex);

    // Index 0 is reserved for missing values
    if (index == 0 || index > _indexToWord.size()) {
        return boost::none;
    }

    // indexToWord is 0-indexed, but symbols start from 1
    return StringData(_indexToWord[index - 1]);
}

size_t SymbolDictionary::getSymbolCount() const {
    std::shared_lock<std::shared_mutex> lock(_mutex);
    return _indexToWord.size();
}

size_t SymbolDictionary::getMemoryUsageBytes() const {
    std::shared_lock<std::shared_mutex> lock(_mutex);

    size_t totalBytes = 0;

    // Memory for wordToIndex map
    for (const auto& [word, index] : _wordToIndex) {
        totalBytes += word.size() + sizeof(uint32_t);
    }

    // Memory for indexToWord vector
    for (const auto& word : _indexToWord) {
        totalBytes += word.size();
    }
    totalBytes += _indexToWord.capacity() * sizeof(std::string);

    return totalBytes;
}

// ============================================================================
// DeltaSymbolDictionary Implementation
// ============================================================================

DeltaSymbolDictionary::DeltaSymbolDictionary(HCIndexPeriodEnum period,
                                              int32_t frequency,
                                              Timestamp windowStart,
                                              Timestamp windowEnd,
                                              SymbolDictionary* baseDictionary,
                                              HCIndexWriter* writer,
                                              DeltaSymbolDictionary* prevInterval1,
                                              DeltaSymbolDictionary* prevInterval2)
    : _baseDictionary(baseDictionary)
    , _prevInterval1(prevInterval1)
    , _prevInterval2(prevInterval2)
    , _nextSymbolIndex(baseDictionary ? baseDictionary->getSymbolCount() + 1 : 1)
    , _numEncodedSymbols(0)
    , _period(period)
    , _frequency(frequency)
    , _windowStart(windowStart)
    , _windowEnd(windowEnd)
    , _writer(writer)
{
}

StatusWith<uint32_t> DeltaSymbolDictionary::getOrInsertSymbol(StringData word) {

    std::unique_lock<std::shared_mutex> lock(_mutex);

    if (_state != SymbolDictionaryState::ReadWrite && _state != SymbolDictionaryState::Reconstruction) {
        // This is an error.
        return Status(ErrorCodes::InternalError, "Cannot insert symbols in ReadOnly state");
    }

    // Do we write to the base dictionary or the local dictionary? If we own the
    // base, then we write to base. We own the base if base dictionary interval
    // and our interval are the same AND the base is in a writable state.
    if (_baseDictionary && _baseDictionary->getWindowStart() == _windowStart &&
        _baseDictionary->isWritable()) {
        _numEncodedSymbols++;
        return _baseDictionary->getOrInsertSymbol(word);
    }

    // Ok, we can't touch the base dictionary. We are referencing it.

    if (_state == SymbolDictionaryState::ReadWrite && _numEncodedSymbols == 0) {
        // Write the INIT operation via the writer
        // Pass the base dictionary's window start as REF if we have a base dictionary
        boost::optional<Timestamp> refBaseDictionaryTime;
        if (_baseDictionary) {
            refBaseDictionaryTime = _baseDictionary->getWindowStart();
        }
        
        // Pass _nextSymbolIndex as the local index offset so the reader knows where
        // to start assigning indices for local symbols
        if (!_writer->initSymbolDictionary(_windowStart, _windowEnd, refBaseDictionaryTime, _nextSymbolIndex).isOK()) {
            return Status(ErrorCodes::InternalError, "Could not initialize symbol dictionary");
        }
        _numEncodedSymbols++;
    }

    std::string wordStr = std::string(word);

    // Step 1: Check base dictionary
    if (_baseDictionary) {
        auto baseIdx = _baseDictionary->getSymbolIndex(word);
        if (baseIdx) {
            return *baseIdx;
        }
    }

    // Step 2: Check inherited delta (if set)
    if (_hasInheritedDelta) {
        auto it = _inheritedWordToIndex.find(wordStr);
        if (it != _inheritedWordToIndex.end()) {
            return it->second;
        }
    }

    // Step 3: Check local delta
    auto localIt = _localWordToIndex.find(wordStr);
    if (localIt != _localWordToIndex.end()) {
        return localIt->second;
    }

    // Step 4: Lazy inheritance - if not inherited yet, try to inherit from N-1 or N-2
    if (!_hasInheritedDelta) {
        // Try N-1 first (more recent)
        if (_prevInterval1 && tryInheritFrom(_prevInterval1)) {
            // Check if we now have the symbol
            auto it = _inheritedWordToIndex.find(wordStr);
            if (it != _inheritedWordToIndex.end()) {
                return it->second;
            }
        }
        // Try N-2 if N-1 didn't have it
        if (!_hasInheritedDelta && _prevInterval2 && tryInheritFrom(_prevInterval2)) {
            auto it = _inheritedWordToIndex.find(wordStr);
            if (it != _inheritedWordToIndex.end()) {
                return it->second;
            }
        }
    }
    
    // Step 5: Not found anywhere - add to local delta
    if (_state == SymbolDictionaryState::NOP) {
        return Status(ErrorCodes::InternalError, "Dictionary is in NOP state, cannot insert symbols");
    }
    if (_state == SymbolDictionaryState::ReadOnly) {
        return Status(ErrorCodes::InternalError, "Dictionary is read-only, cannot insert symbols");
    }

    // Assign new symbol index
    uint32_t symbolIndex = _nextSymbolIndex++;
    _localWordToIndex[wordStr] = symbolIndex;
    _localIndexToWord.push_back(wordStr);
    _isDirty = true;

    // Write to persistence layer if in ReadWrite mode
    if (_state == SymbolDictionaryState::ReadWrite && _writer) {
        if (!_writer->addSymbol(_windowStart, _windowEnd, wordStr, symbolIndex).isOK()) {
            return Status(ErrorCodes::InternalError, "Could not add symbol to writer");
        }
    }

    return symbolIndex;
}

bool DeltaSymbolDictionary::tryInheritFrom(DeltaSymbolDictionary* prevInterval) {
    // Get the effective delta from the previous interval
    auto effectiveDelta = prevInterval->getEffectiveDelta();

    if (effectiveDelta.empty()) {
        return false;
    }

    // Copy all symbols from the previous interval's effective delta
    // We need to renumber them starting from our current _nextSymbolIndex
    for (const auto& symbol : effectiveDelta) {
        // Check if symbol is already in our base
        if (_baseDictionary && _baseDictionary->getSymbolIndex(StringData(symbol))) {
            continue;  // Skip symbols already in base
        }

        uint32_t newIndex = _nextSymbolIndex++;
        _inheritedWordToIndex[symbol] = newIndex;
        _inheritedIndexToWord.push_back(symbol);
    }

    _hasInheritedDelta = true;
    _inheritedFromWindowStart = prevInterval->getWindowStart();
    return true;
}

boost::optional<uint32_t> DeltaSymbolDictionary::getSymbolIndex(StringData word) const {
    std::shared_lock<std::shared_mutex> lock(_mutex);

    std::string wordStr = std::string(word);

    // Check base dictionary
    if (_baseDictionary) {
        auto baseIdx = _baseDictionary->getSymbolIndex(word);
        if (baseIdx) {
            return baseIdx;
        }
    }

    // Check inherited delta
    if (_hasInheritedDelta) {
        auto it = _inheritedWordToIndex.find(wordStr);
        if (it != _inheritedWordToIndex.end()) {
            return it->second;
        }
    }

    // Check local delta
    auto localIt = _localWordToIndex.find(wordStr);
    if (localIt != _localWordToIndex.end()) {
        return localIt->second;
    }

    return boost::none;
}

boost::optional<StringData> DeltaSymbolDictionary::getSymbol(uint32_t index) const {
    std::shared_lock<std::shared_mutex> lock(_mutex);

    if (index == 0) {
        return boost::none;  // 0 is reserved for missing values
    }

    // Check base dictionary first
    if (_baseDictionary) {
        size_t baseSize = _baseDictionary->getSymbolCount();
        if (index <= baseSize) {
            return _baseDictionary->getSymbol(index);
        }
    }

    // Calculate base offset
    size_t baseSize = _baseDictionary ? _baseDictionary->getSymbolCount() : 0;

    // Check inherited delta
    if (_hasInheritedDelta) {
        size_t inheritedOffset = index - baseSize - 1;
        if (inheritedOffset < _inheritedIndexToWord.size()) {
            return StringData(_inheritedIndexToWord[inheritedOffset]);
        }
    }

    // Check local delta
    size_t inheritedSize = _hasInheritedDelta ? _inheritedIndexToWord.size() : 0;
    size_t localOffset = index - baseSize - inheritedSize - 1;
    if (localOffset < _localIndexToWord.size()) {
        return StringData(_localIndexToWord[localOffset]);
    }

    return boost::none;
}

std::set<std::string> DeltaSymbolDictionary::getEffectiveDelta() const {
    std::shared_lock<std::shared_mutex> lock(_mutex);

    std::set<std::string> result;

    // Add inherited delta symbols
    for (const auto& [word, _] : _inheritedWordToIndex) {
        result.insert(word);
    }

    // Add local delta symbols
    for (const auto& [word, _] : _localWordToIndex) {
        result.insert(word);
    }

    return result;
}

Status DeltaSymbolDictionary::changeState(SymbolDictionaryState newState) {
    std::unique_lock<std::shared_mutex> lock(_mutex);

    // Once in ReadOnly state, no transitions are allowed
    if (_state == SymbolDictionaryState::ReadOnly) {
        return Status(ErrorCodes::InternalError, "Cannot change state of read-only dictionary");
    }

    // Validate state transitions (same logic as SymbolDictionary)
    if (_state == SymbolDictionaryState::NOP) {
        if (newState != SymbolDictionaryState::Reconstruction &&
            newState != SymbolDictionaryState::ReadWrite) {
            return Status(ErrorCodes::InternalError,
                "Invalid state transition from NOP");
        }
    } else if (_state == SymbolDictionaryState::Reconstruction) {
        if (newState != SymbolDictionaryState::ReadWrite &&
            newState != SymbolDictionaryState::ReadOnly) {
            return Status(ErrorCodes::InternalError,
                "Invalid state transition from Reconstruction");
        }
    } else if (_state == SymbolDictionaryState::ReadWrite) {
        if (newState != SymbolDictionaryState::ReadOnly) {
            return Status(ErrorCodes::InternalError,
                "Invalid state transition from ReadWrite");
        }
    }

    _state = newState;
    return Status::OK();
}

void DeltaSymbolDictionary::flush() {
    std::unique_lock<std::shared_mutex> lock(_mutex);

    if (!_isDirty || _writer == nullptr) {
        return;
    }

    if (!_writer->flush(_windowStart, _windowEnd, _period, _frequency, true).isOK()) {
        return;
    }

    _isDirty = false;
}

size_t DeltaSymbolDictionary::getSymbolCount() const {
    std::shared_lock<std::shared_mutex> lock(_mutex);

    size_t count = 0;
    if (_baseDictionary) {
        count += _baseDictionary->getSymbolCount();
    }
    count += _inheritedWordToIndex.size();
    count += _localWordToIndex.size();
    return count;
}

size_t DeltaSymbolDictionary::getMemoryUsageBytes() const {
    std::shared_lock<std::shared_mutex> lock(_mutex);

    size_t totalBytes = 0;

    // Inherited delta memory
    for (const auto& [word, _] : _inheritedWordToIndex) {
        totalBytes += word.size() + sizeof(uint32_t);
    }
    for (const auto& word : _inheritedIndexToWord) {
        totalBytes += word.size();
    }

    // Local delta memory
    for (const auto& [word, _] : _localWordToIndex) {
        totalBytes += word.size() + sizeof(uint32_t);
    }
    for (const auto& word : _localIndexToWord) {
        totalBytes += word.size();
    }

    return totalBytes;
}

// ============================================================================
// Similarity Function
// ============================================================================

double computeDeltaSimilarity(const std::set<std::string>& delta1,
                               const std::set<std::string>& delta2) {
    if (delta1.empty() && delta2.empty()) {
        return 1.0;  // Both empty = identical
    }

    if (delta1.empty() || delta2.empty()) {
        return 0.0;  // One empty, one not = no similarity
    }

    // Compute Jaccard similarity: |A ∩ B| / |A ∪ B|
    size_t intersectionSize = 0;
    for (const auto& s : delta1) {
        if (delta2.count(s) > 0) {
            intersectionSize++;
        }
    }

    size_t unionSize = delta1.size() + delta2.size() - intersectionSize;
    return static_cast<double>(intersectionSize) / static_cast<double>(unionSize);
}

// ============================================================================
// TemporalSymbolDictionary Implementation
// ============================================================================

TemporalSymbolDictionary::TemporalSymbolDictionary(const UUID& collectionUUID,
                                                   HCIndexPeriodEnum period,
                                                   int32_t frequency,
                                                   HCIndexWriter* writer,
                                                   HCIndexReader* reader)
    : _collectionUUID(collectionUUID)
    , _period(period)
    , _frequency(frequency)
    , _writer(writer)
    , _reader(reader)
{
}

StatusWith<ISymbolDictionary*> TemporalSymbolDictionary::getOrCreateDictionaryForTimestamp(
    OperationContext* opCtx,
    const Timestamp& ts) {
    Timestamp windowStart = calculateWindowStart(ts);
    return getOrCreateDeltaDictionary(opCtx, windowStart);
}

StatusWith<ISymbolDictionary*> TemporalSymbolDictionary::getDictionaryForTimestamp(
    const Timestamp& ts) const {
    std::shared_lock<std::shared_mutex> lock(_mutex);

    Timestamp windowStart = calculateWindowStart(ts);
    auto it = _deltaDictionaries.find(windowStart);

    if (it == _deltaDictionaries.end()) {
        return Status(ErrorCodes::NoSuchKey, "Dictionary not found for timestamp");
    }

    return it->second.get();
}

StatusWith<uint32_t> TemporalSymbolDictionary::encodeSymbol(StringData word,
                                                            const Timestamp& timestamp) {
    // Note: encodeSymbol is called during write operations where opCtx should be available
    // For now, we pass nullptr and rely on the dictionary being in memory
    // TODO: Update callers to pass opCtx
    auto dictResult = getOrCreateDictionaryForTimestamp(nullptr, timestamp);
    if (!dictResult.isOK()) {
        return dictResult.getStatus();
    }

    return dictResult.getValue()->getOrInsertSymbol(word);
}



boost::optional<StringData> TemporalSymbolDictionary::decodeSymbol(uint32_t index,
                                                                    const Timestamp& timestamp) const {
    auto dictResult = getDictionaryForTimestamp(timestamp);
    if (!dictResult.isOK()) {
        return boost::none;
    }

    return dictResult.getValue()->getSymbol(index);
}



std::pair<Timestamp, Timestamp> TemporalSymbolDictionary::getWindowForTimestamp(
    const Timestamp& timestamp) const {
    Timestamp windowStart = calculateWindowStart(timestamp);
    Timestamp windowEnd = calculateWindowEnd(windowStart);
    return {windowStart, windowEnd};
}

Status TemporalSymbolDictionary::cleanupOldDictionaries(const Timestamp& beforeTimestamp) {
    std::unique_lock<std::shared_mutex> lock(_mutex);

    // First, delete delta dictionaries that are before the cutoff
    auto deltaIt = _deltaDictionaries.begin();
    while (deltaIt != _deltaDictionaries.end()) {
        Timestamp windowEnd = calculateWindowEnd(deltaIt->first);
        if (windowEnd <= beforeTimestamp) {
            deltaIt = _deltaDictionaries.erase(deltaIt);
        } else {
            ++deltaIt;
        }
    }

    // Collect base dictionary window starts that are still referenced by remaining deltas
    std::set<Timestamp> referencedBases;
    for (const auto& [windowStart, delta] : _deltaDictionaries) {
        auto baseWindowStart = delta->getBaseDictionaryWindowStart();
        if (baseWindowStart) {
            referencedBases.insert(*baseWindowStart);
        }
    }

    // Now delete base dictionaries that are before the cutoff AND not referenced
    auto it = _dictionaries.begin();
    while (it != _dictionaries.end()) {
        Timestamp windowEnd = calculateWindowEnd(it->first);
        if (windowEnd <= beforeTimestamp && referencedBases.find(it->first) == referencedBases.end()) {
            // Also ensure we don't delete _currentBase
            if (it->second.get() != _currentBase) {
                it = _dictionaries.erase(it);
            } else {
                ++it;
            }
        } else {
            ++it;
        }
    }

    return Status::OK();
}

void TemporalSymbolDictionary::flush() {
    std::unique_lock<std::shared_mutex> lock(_mutex);

    // Flush all base dictionaries
    for (auto& [windowStart, dict] : _dictionaries) {
        dict->flush();
    }

    // Flush all delta dictionaries
    for (auto& [windowStart, delta] : _deltaDictionaries) {
        delta->flush();
    }
}

TemporalSymbolDictionary::Stats TemporalSymbolDictionary::getStats() const {
    std::shared_lock<std::shared_mutex> lock(_mutex);

    Stats stats{0, 0, 0};

    // Only count delta dictionaries since their stats already include the base dictionary
    for (const auto& [windowStart, dict] : _deltaDictionaries) {
        stats.totalDictionaries++;
        stats.totalSymbols += dict->getSymbolCount();
        stats.memoryUsageBytes += dict->getMemoryUsageBytes();
    }

    return stats;
}

StatusWith<SymbolDictionary*> TemporalSymbolDictionary::getOrCreateDictionary(
    OperationContext* opCtx,
    const Timestamp& windowStart) {

    // First check if dictionary exists (read lock)
    {
        std::shared_lock lock(_mutex);
        auto it = _dictionaries.find(windowStart);
        if (it != _dictionaries.end()) {
            return it->second.get();
        }
    }

    // Index doesn't exist, create it (write lock)
    std::unique_lock lock(_mutex);

    // Check again after the write lock
    auto it = _dictionaries.find(windowStart);
    if (it != _dictionaries.end()) {
        return it->second.get();
    }

    // Try to reconstruct from disk if reader is available
    if (_reader) {
        auto windowEnd = calculateWindowEnd(windowStart);
        auto reconstructResult = _reader->constructSymbolDictionary(
            opCtx, windowStart, windowEnd, _period, _frequency, windowStart);
        // If reconstruction succeeds and dictionary has symbols, use it
        if (reconstructResult.isOK() && reconstructResult.getValue().get()->getSymbolCount() > 0) {
            auto* dictPtr = reconstructResult.getValue().get();

            // Set the writer on the reconstructed dictionary so it can accept new symbols
            if (_writer) {
                dictPtr->setWriter(_writer);
            }

            _dictionaries[windowStart] = std::move(reconstructResult.getValue());
            return dictPtr;
        }
        // If reconstruction fails, fall through to create a new dictionary This
        // is not so great.  TODO: Add some flags so we can detect between lack
        // of data and missing data.
    }

    // Create new dictionary
    auto windowEnd = calculateWindowEnd(windowStart);
    auto dict = std::make_unique<SymbolDictionary>(
        _period, _frequency, windowStart, windowEnd, _writer);

    // Change state to ReadWrite for new dictionaries created by TemporalSymbolDictionary
    auto stateStatus = dict->changeState(SymbolDictionaryState::ReadWrite);
    if (!stateStatus.isOK()) {
        return stateStatus;
    }

    auto* dictPtr = dict.get();
    _dictionaries[windowStart] = std::move(dict);

    return dictPtr;
}

Timestamp TemporalSymbolDictionary::calculateWindowStart(const Timestamp& timestamp) const {
    uint32_t windowSizeSeconds = 0;

    switch (_period) {
        case HCIndexPeriodEnum::Hour:
            windowSizeSeconds = _frequency * 60 * 60;  // frequency hours in seconds
            break;
        case HCIndexPeriodEnum::Minute:
            windowSizeSeconds = _frequency * 60;  // frequency minutes in seconds
            break;
        case HCIndexPeriodEnum::Second:
            windowSizeSeconds = _frequency;  // frequency seconds
            break;
    }

    uint32_t seconds = timestamp.getSecs();
    uint32_t windowStartSeconds = (seconds / windowSizeSeconds) * windowSizeSeconds;
    return Timestamp(windowStartSeconds, 0);
}

Timestamp TemporalSymbolDictionary::calculateWindowEnd(const Timestamp& windowStart) const {
    uint32_t windowSizeSeconds = 0;

    switch (_period) {
        case HCIndexPeriodEnum::Hour:
            windowSizeSeconds = _frequency * 60 * 60;  // frequency hours in seconds
            break;
        case HCIndexPeriodEnum::Minute:
            windowSizeSeconds = _frequency * 60;  // frequency minutes in seconds
            break;
        case HCIndexPeriodEnum::Second:
            windowSizeSeconds = _frequency;  // frequency seconds
            break;
    }

    uint32_t seconds = windowStart.getSecs();
    return Timestamp(seconds + windowSizeSeconds, 0);
}

Timestamp TemporalSymbolDictionary::calculatePreviousWindowStart(const Timestamp& windowStart) const {
    uint32_t windowSizeSeconds = 0;

    switch (_period) {
        case HCIndexPeriodEnum::Hour:
            windowSizeSeconds = _frequency * 60 * 60;
            break;
        case HCIndexPeriodEnum::Minute:
            windowSizeSeconds = _frequency * 60;
            break;
        case HCIndexPeriodEnum::Second:
            windowSizeSeconds = _frequency;
            break;
    }

    uint32_t seconds = windowStart.getSecs();
    if (seconds < windowSizeSeconds) {
        return Timestamp(0, 0);  // No previous window
    }
    return Timestamp(seconds - windowSizeSeconds, 0);
}

// ============================================================================
// Delta Dictionary Support
// ============================================================================

std::pair<DeltaSymbolDictionary*, DeltaSymbolDictionary*>
TemporalSymbolDictionary::getPreviousTwoIntervals(const Timestamp& windowStart) {
    // Note: Caller must hold _mutex

    DeltaSymbolDictionary* prev1 = nullptr;
    DeltaSymbolDictionary* prev2 = nullptr;

    Timestamp prev1Start = calculatePreviousWindowStart(windowStart);
    if (prev1Start.getSecs() > 0) {
        auto it1 = _deltaDictionaries.find(prev1Start);
        if (it1 != _deltaDictionaries.end()) {
            prev1 = it1->second.get();
        }

        Timestamp prev2Start = calculatePreviousWindowStart(prev1Start);
        if (prev2Start.getSecs() > 0) {
            auto it2 = _deltaDictionaries.find(prev2Start);
            if (it2 != _deltaDictionaries.end()) {
                prev2 = it2->second.get();
            }
        }
    }

    return {prev1, prev2};
}

SymbolDictionary* TemporalSymbolDictionary::maybeCreateNewBase(const Timestamp& windowStart) {
    // Note: Caller must hold _mutex (write lock)

    // Get previous two intervals
    auto [prev1, prev2] = getPreviousTwoIntervals(windowStart);

    // If we don't have two previous intervals, use current base
    if (!prev1 || !prev2) {
        return _currentBase;
    }

    // Check similarity of their deltas
    auto delta1 = prev1->getEffectiveDelta();
    auto delta2 = prev2->getEffectiveDelta();

    double similarity = computeDeltaSimilarity(delta1, delta2);

    if (similarity >= _similarityThreshold) {
        // Create a new base by merging old base + prev1's delta + prev2's delta
        auto windowEnd = calculateWindowEnd(windowStart);

        // Create with writer so the new base dictionary gets persisted
        auto newBase = std::make_unique<SymbolDictionary>(
            _period, _frequency, windowStart, windowEnd, _writer);

        // Start in ReadWrite mode so symbols get written via the writer
        auto stateStatus = newBase->changeState(SymbolDictionaryState::ReadWrite);
        if (!stateStatus.isOK()) {
            return _currentBase;  // Fall back to current base
        }

        // Copy symbols from old base
        if (_currentBase) {
            size_t baseCount = _currentBase->getSymbolCount();
            for (uint32_t i = 1; i <= baseCount; ++i) {
                auto symbol = _currentBase->getSymbol(i);
                if (symbol) {
                    (void)newBase->getOrInsertSymbol(*symbol);
                }
            }
        }

        // Add symbols from prev1's effective delta
        for (const auto& symbol : delta1) {
            if (!newBase->getSymbolIndex(StringData(symbol))) {
                (void)newBase->getOrInsertSymbol(StringData(symbol));
            }
        }

        // Add symbols from prev2's effective delta
        for (const auto& symbol : delta2) {
            if (!newBase->getSymbolIndex(StringData(symbol))) {
                (void)newBase->getOrInsertSymbol(StringData(symbol));
            }
        }

        // Transition to ReadOnly after all symbols are added
        (void)newBase->changeState(SymbolDictionaryState::ReadOnly);

        // Store and update current base
        auto* basePtr = newBase.get();
        _dictionaries[windowStart] = std::move(newBase);
        _currentBase = basePtr;

        return _currentBase;
    }

    // Similarity not high enough, keep current base
    return _currentBase;
}

StatusWith<DeltaSymbolDictionary*> TemporalSymbolDictionary::getOrCreateDeltaDictionary(
    OperationContext* opCtx,
    const Timestamp& windowStart) {

    // First check if dictionary exists (read lock)
    {
        std::shared_lock lock(_mutex);
        auto it = _deltaDictionaries.find(windowStart);
        if (it != _deltaDictionaries.end()) {
            return it->second.get();
        }
    }

    // Need to create - acquire write lock
    std::unique_lock lock(_mutex);

    // Double-check after acquiring write lock
    auto it = _deltaDictionaries.find(windowStart);
    if (it != _deltaDictionaries.end()) {
        return it->second.get();
    }

    auto windowEnd = calculateWindowEnd(windowStart);

    // Try to reconstruct from disk if reader is available
    if (_reader && opCtx) {
        auto reconstructResult = _reader->constructSymbolDictionaryWithDelta(
            opCtx, windowStart, windowEnd, _period, _frequency, windowStart, _currentBase);

        if (reconstructResult.isOK()) {
            auto& result = reconstructResult.getValue();

            if (result.isDelta()) {
                // Reconstructed a delta dictionary
                auto* deltaPtr = result.deltaDictionary.get();

                // If we have a reference to a base dictionary, look it up and set it
                if (result.refBaseDictionaryWindowStart) {
                    auto baseIt = _dictionaries.find(*result.refBaseDictionaryWindowStart);
                    if (baseIt != _dictionaries.end()) {
                        deltaPtr->setBaseDictionary(baseIt->second.get());
                        _currentBase = baseIt->second.get();
                    }
                    // If base not found, the delta will operate without base symbols
                    // This could happen if the base was cleaned up - caller should handle
                }

                // Set writer for new symbols
                if (_writer) {
                    deltaPtr->setWriter(_writer);
                }

                _deltaDictionaries[windowStart] = std::move(result.deltaDictionary);
                return deltaPtr;
            } else if (result.baseDictionary) {
                // Reconstructed a base dictionary - this becomes our current base
                auto* basePtr = result.baseDictionary.get();

                // For Delta dictionariies, we won't use empty base dictionary
                // If reader returned an empty, it is assumed that we need to
                // use something more optimal as base.
                if (!_currentBase || basePtr->getSymbolCount() != 0) {
                    // Set writer for new symbols
                    if (_writer) {
                        basePtr->setWriter(_writer);
                    }

                    // Store as base dictionary
                    _dictionaries[windowStart] = std::move(result.baseDictionary);
                    _currentBase = basePtr;
                }

                // Now create a delta dictionary that references this base
                auto [prev1, prev2] = getPreviousTwoIntervals(windowStart);
                auto deltaDict = std::make_unique<DeltaSymbolDictionary>(
                    _period, _frequency, windowStart, windowEnd, _currentBase, _writer, prev1, prev2);

                auto stateStatus = deltaDict->changeState(SymbolDictionaryState::ReadWrite);
                if (!stateStatus.isOK()) {
                    return stateStatus;
                }

                auto* dictPtr = deltaDict.get();
                _deltaDictionaries[windowStart] = std::move(deltaDict);
                return dictPtr;
            }
        }
        // If reconstruction fails, fall through to create a new dictionary
    }

    // Check if we should create a new base based on previous intervals
    SymbolDictionary* base = maybeCreateNewBase(windowStart);

    // If we couldn't create a new base from previous intervals, then we need
    // to create a new empty base and set it to ReadWrite so it can be filled
    // in with new symbols.
    if (!base) {
        auto newBase = std::make_unique<SymbolDictionary>(
            _period, _frequency, windowStart, windowEnd, _writer);
        (void)newBase->changeState(SymbolDictionaryState::ReadWrite);
        base = newBase.get();
        _currentBase = base;
        _dictionaries[windowStart] = std::move(newBase);
    }

    // Get previous two intervals for lazy inheritance
    auto [prev1, prev2] = getPreviousTwoIntervals(windowStart);

    // Create new delta dictionary with prev intervals stored internally
    auto deltaDict = std::make_unique<DeltaSymbolDictionary>(
        _period, _frequency, windowStart, windowEnd, base, _writer, prev1, prev2);

    auto stateStatus = deltaDict->changeState(SymbolDictionaryState::ReadWrite);
    if (!stateStatus.isOK()) {
        return stateStatus;
    }

    auto* dictPtr = deltaDict.get();
    _deltaDictionaries[windowStart] = std::move(deltaDict);

    return dictPtr;
}
}  // namespace mongo::timeseries::hcindex

