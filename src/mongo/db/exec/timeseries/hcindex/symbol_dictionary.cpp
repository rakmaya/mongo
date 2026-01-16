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

#include "mongo/db/exec/timeseries/hcindex/symbol_dictionary.h"

#include "mongo/base/error_codes.h"
#include "mongo/db/exec/timeseries/hcindex/writer.h"
#include "mongo/util/str.h"

namespace mongo::timeseries::hcindex {

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

Status SymbolDictionary::insertSymbol(StringData word, uint32_t index) {
    std::unique_lock<std::shared_mutex> lock(_mutex);

    // Check state: modifications only allowed in Reconstruction mode for direct insertion
    if (_state != SymbolDictionaryState::Reconstruction) {
        return Status(ErrorCodes::InternalError,
            "Direct symbol insertion only allowed in Reconstruction state");
    }

    std::string wordStr = std::string(word);

    // Check if word already exists
    auto it = _wordToIndex.find(wordStr);
    if (it != _wordToIndex.end()) {
        if (it->second != index) {
            return Status(ErrorCodes::InternalError,
                str::stream() << "Symbol '" << wordStr
                              << "' already exists with different index: " << it->second
                              << " vs " << index);
        }
        // Already exists with same index, nothing to do
        return Status::OK();
    }

    // Check if index is valid (not 0, which is reserved)
    if (index == 0) {
        return Status(ErrorCodes::BadValue, "Symbol index 0 is reserved for missing values");
    }

    // Ensure _indexToWord is large enough
    if (index > _indexToWord.size()) {
        _indexToWord.resize(index);
    }

    // Insert the symbol
    _wordToIndex[wordStr] = index;
    _indexToWord[index - 1] = wordStr;  // indexToWord is 0-indexed

    // Update next symbol index if needed
    if (index >= _nextSymbolIndex) {
        _nextSymbolIndex = index + 1;
    }

    return Status::OK();
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

        // Add the symbol to the writer (Base symbol since this is a SymbolDictionary)
        if (!_writer->addSymbol(_windowStart, _windowEnd, wordStr, symbolIndex, HCIndexWriter::SymbolType::Base).isOK()) {
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

}  // namespace mongo::timeseries::hcindex
