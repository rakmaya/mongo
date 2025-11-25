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


namespace mongo::timeseries::hcindex {

// ============================================================================
// SymbolDictionary Implementation
// ============================================================================

SymbolDictionary::SymbolDictionary(
    DictionaryGranularity granularity,
    Timestamp windowStart,
    Timestamp windowEnd,
    HCIndexWriter *writer)
    : _nextSymbolIndex(1)
    , _granularity(granularity)
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
            if (!_writer->initSymbolDictionary(_windowStart, _windowEnd).isOK()) {
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
    if (!_writer->flush(_windowStart, _windowEnd, _granularity, true).isOK()) {
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
// TemporalSymbolDictionary Implementation
// ============================================================================

TemporalSymbolDictionary::TemporalSymbolDictionary(const UUID& collectionUUID,
                                                   DictionaryGranularity granularity,
                                                   HCIndexWriter* writer,
                                                   HCIndexReader* reader)
    : _collectionUUID(collectionUUID)
    , _granularity(granularity)
    , _writer(writer)
    , _reader(reader)
{
}

StatusWith<SymbolDictionary*> TemporalSymbolDictionary::getOrCreateDictionaryForTimestamp(
    OperationContext* opCtx,
    const Timestamp& ts) {
    Timestamp windowStart = calculateWindowStart(ts);
    return getOrCreateDictionary(opCtx, windowStart);
}

StatusWith<SymbolDictionary*> TemporalSymbolDictionary::getDictionaryForTimestamp(
    const Timestamp& ts) const {
    std::shared_lock<std::shared_mutex> lock(_mutex);

    Timestamp windowStart = calculateWindowStart(ts);
    auto it = _dictionaries.find(windowStart);

    if (it == _dictionaries.end()) {
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

    auto it = _dictionaries.begin();
    while (it != _dictionaries.end()) {
        Timestamp windowEnd = calculateWindowEnd(it->first);
        if (windowEnd <= beforeTimestamp) {
            it = _dictionaries.erase(it);
        } else {
            ++it;
        }
    }

    return Status::OK();
}

void TemporalSymbolDictionary::flush() {
    std::unique_lock<std::shared_mutex> lock(_mutex);

    // Flush all dictionaries
    for (auto& [windowStart, dict] : _dictionaries) {
        // Flush the dictionary
        dict->flush();
    }
}

TemporalSymbolDictionary::Stats TemporalSymbolDictionary::getStats() const {
    std::shared_lock<std::shared_mutex> lock(_mutex);

    Stats stats{0, 0, 0};

    for (const auto& [windowStart, dict] : _dictionaries) {
        stats.totalDictionaries++;
        stats.totalSymbols += dict->getSymbolCount();
        stats.memoryUsageBytes += dict->getMemoryUsageBytes();
    }

    return stats;
}

StatusWith<SymbolDictionary*> TemporalSymbolDictionary::getOrCreateDictionary(
    OperationContext* opCtx,
    const Timestamp& windowStart) {
    std::unique_lock<std::shared_mutex> lock(_mutex);

    auto it = _dictionaries.find(windowStart);
    if (it != _dictionaries.end()) {
        return it->second.get();
    }

    // Try to reconstruct from disk if reader is available
    if (_reader) {
        auto windowEnd = calculateWindowEnd(windowStart);
        auto reconstructResult = _reader->constructSymbolDictionary(
            opCtx, windowStart, windowEnd, _granularity, windowStart);
        if (reconstructResult.isOK()) {
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
        _granularity, windowStart, windowEnd, _writer);

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
    uint32_t seconds = timestamp.getSecs();
    uint32_t windowSizeSeconds = 0;

    switch (_granularity) {
        case DictionaryGranularity::DAILY:
            windowSizeSeconds = 24 * 60 * 60;  // 86400 seconds
            break;
        case DictionaryGranularity::HOURLY:
            windowSizeSeconds = 60 * 60;  // 3600 seconds
            break;
        case DictionaryGranularity::THIRTY_MIN:
            windowSizeSeconds = 30 * 60;  // 1800 seconds
            break;
        case DictionaryGranularity::TEN_MIN:
            windowSizeSeconds = 10 * 60;  // 600 seconds
            break;
        case DictionaryGranularity::FIVE_MIN:
            windowSizeSeconds = 5 * 60;  // 300 seconds
            break;
        case DictionaryGranularity::AUTO:
            // Default to HOURLY
            windowSizeSeconds = 60 * 60;
            break;
    }

    uint32_t windowStartSeconds = (seconds / windowSizeSeconds) * windowSizeSeconds;
    return Timestamp(windowStartSeconds, 0);
}

Timestamp TemporalSymbolDictionary::calculateWindowEnd(const Timestamp& windowStart) const {
    uint32_t seconds = windowStart.getSecs();
    uint32_t windowSizeSeconds = 0;

    switch (_granularity) {
        case DictionaryGranularity::DAILY:
            windowSizeSeconds = 24 * 60 * 60;
            break;
        case DictionaryGranularity::HOURLY:
            windowSizeSeconds = 60 * 60;
            break;
        case DictionaryGranularity::THIRTY_MIN:
            windowSizeSeconds = 30 * 60;
            break;
        case DictionaryGranularity::TEN_MIN:
            windowSizeSeconds = 10 * 60;
            break;
        case DictionaryGranularity::FIVE_MIN:
            windowSizeSeconds = 5 * 60;
            break;
        case DictionaryGranularity::AUTO:
            windowSizeSeconds = 60 * 60;
            break;
    }

    return Timestamp(seconds + windowSizeSeconds, 0);
}

}  // namespace mongo::timeseries::hcindex

