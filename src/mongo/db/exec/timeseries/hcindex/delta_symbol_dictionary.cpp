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

#include "mongo/db/exec/timeseries/hcindex/delta_symbol_dictionary.h"

#include "mongo/base/error_codes.h"
#include "mongo/db/exec/timeseries/hcindex/writer.h"
#include "mongo/util/str.h"

namespace mongo::timeseries::hcindex {

                        // ---------------------------
                        // class DeltaSymbolDictionary
                        // ---------------------------


//- CONSTRUCTORS


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


//- ACCESSORS



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

    // Base dictionary memory (if we own it)
    if (_baseDictionary && _baseDictionary->getWindowStart() == _windowStart) {
        totalBytes += _baseDictionary->getMemoryUsageBytes();
    }

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


//- MODIFIERS


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

    // Check base, then inherited and then finally local.

    if (_baseDictionary) {
        auto baseIdx = _baseDictionary->getSymbolIndex(word);
        if (baseIdx) {
            return *baseIdx;
        }
    }

    if (_hasInheritedDelta) {
        auto it = _inheritedWordToIndex.find(wordStr);
        if (it != _inheritedWordToIndex.end()) {
            return it->second;
        }
    }

    auto localIt = _localWordToIndex.find(wordStr);
    if (localIt != _localWordToIndex.end()) {
        return localIt->second;
    }

    // Ok, we are here means we need to insert this. If inherited structure is not set
    // then lets adopt one and check in the inherited section again.
    if (!_hasInheritedDelta) {
        // Try N-1 first (most recent) and then the N-2.

        if (_prevInterval1 && tryInheritFrom(_prevInterval1)) {
            auto it = _inheritedWordToIndex.find(wordStr);
            if (it != _inheritedWordToIndex.end()) {
                return it->second;
            }
        }

        if (!_hasInheritedDelta && _prevInterval2 && tryInheritFrom(_prevInterval2)) {
            auto it = _inheritedWordToIndex.find(wordStr);
            if (it != _inheritedWordToIndex.end()) {
                return it->second;
            }
        }
    }

    // All lookups for the symbol failed. We need to add to the local delta.
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
        if (!_writer->addSymbol(_windowStart, _windowEnd, wordStr, symbolIndex, HCIndexWriter::SymbolType::Local).isOK()) {
            return Status(ErrorCodes::InternalError, "Could not add symbol to writer");
        }
    }

    return symbolIndex;
}

Status DeltaSymbolDictionary::insertLocalSymbolDirect(StringData word, uint32_t index) {
    std::unique_lock<std::shared_mutex> lock(_mutex);

    std::string wordStr(word);

    auto it = _localWordToIndex.find(wordStr);
    if (it != _localWordToIndex.end()) {
        if (it->second != index) {
            return Status(ErrorCodes::InternalError,
                          str::stream() << "Symbol '" << wordStr
                                        << "' already exists with different index: " << it->second
                                        << " vs " << index);
        }
        // Already exists with same index, nothing to do
        return Status::OK();
    }

    // Insert into local dictionary with the specified index
    _localWordToIndex[wordStr] = index;

    // Ensure _localIndexToWord is large enough. The index is absolute, so we need to
    // calculate the local offset
    size_t baseSize = _baseDictionary ? _baseDictionary->getSymbolCount() : 0;
    size_t inheritedSize = _hasInheritedDelta ? _inheritedIndexToWord.size() : 0;

    // For local symbols, the index should be > baseSize + inheritedSize
    // The local offset is: index - baseSize - inheritedSize - 1 (since indices are 1-based)
    if (index <= baseSize + inheritedSize) {
        return Status(ErrorCodes::InternalError,
                      str::stream() << "Local symbol index " << index
                                    << " should be greater than base + inherited size ("
                                    << baseSize + inheritedSize << ")");
    }

    size_t localOffset = index - baseSize - inheritedSize - 1;

    // Ensure vector is large enough
    // TODO: Based on density prediction, reserve early on!
    if (localOffset >= _localIndexToWord.size()) {
        _localIndexToWord.resize(localOffset + 1);
    }
    _localIndexToWord[localOffset] = wordStr;

    if (index >= _nextSymbolIndex) {
        _nextSymbolIndex = index + 1;
    }

    return Status::OK();
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

double computeDeltaSimilarity(const std::set<std::string>& delta1,
                               const std::set<std::string>& delta2) {
    if (delta1.empty() && delta2.empty()) {
        return 1.0;
    }

    if (delta1.empty() || delta2.empty()) {
        return 0.0;
    }

    // Compute Jaccard similarity: |A ∩ B| / |A ∪ B|
    // TODO: Find the library that does this within the mongodb code!
    size_t intersectionSize = 0;
    for (const auto& s : delta1) {
        if (delta2.count(s) > 0) {
            intersectionSize++;
        }
    }

    size_t unionSize = delta1.size() + delta2.size() - intersectionSize;
    return static_cast<double>(intersectionSize) / static_cast<double>(unionSize);
}


//- PRIVATE METHODS


bool DeltaSymbolDictionary::tryInheritFrom(DeltaSymbolDictionary* prevInterval) {
    // Get the effective delta from the previous interval
    auto effectiveDelta = prevInterval->getEffectiveDelta();

    if (effectiveDelta.empty()) {
        return false;
    }

    // Copy all symbols from the previous interval's effective delta
    // We need to renumber them though!
    for (const auto& symbol : effectiveDelta) {
        if (_baseDictionary && _baseDictionary->getSymbolIndex(StringData(symbol))) {
            continue;
        }

        uint32_t newIndex = _nextSymbolIndex++;
        _inheritedWordToIndex[symbol] = newIndex;
        _inheritedIndexToWord.push_back(symbol);
    }

    _hasInheritedDelta = true;
    _inheritedFromWindowStart = prevInterval->getWindowStart();
    return true;
}


}  // namespace mongo::timeseries::hcindex

