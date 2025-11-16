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

#include "mongo/db/timeseries/hcindex/temporal_symbol_dictionary.h"
#include "mongo/base/error_codes.h"

namespace mongo::timeseries::hcindex {

// ============================================================================
// SymbolDictionary Implementation
// ============================================================================

StatusWith<uint32_t> SymbolDictionary::getOrInsertSymbol(StringData word) {
    std::unique_lock<std::shared_mutex> lock(mutex);

    std::string wordStr = std::string(word);

    // Check if word already exists
    auto it = wordToIndex.find(wordStr);
    if (it != wordToIndex.end()) {
        return it->second;
    }

    // Check if dictionary is full (max uint32_t is 2^32 - 1, but 0 is reserved)
    if (nextSymbolIndex == 0) {
        return Status(ErrorCodes::BadValue, "Symbol dictionary is full");
    }

    // Insert new symbol
    uint32_t symbolIndex = nextSymbolIndex++;
    wordToIndex[wordStr] = symbolIndex;
    indexToWord.push_back(wordStr);

    return symbolIndex;
}

boost::optional<uint32_t> SymbolDictionary::getSymbolIndex(StringData word) const {
    std::shared_lock<std::shared_mutex> lock(mutex);

    auto it = wordToIndex.find(std::string(word));
    if (it != wordToIndex.end()) {
        return it->second;
    }

    return boost::none;
}

boost::optional<StringData> SymbolDictionary::getSymbol(uint32_t index) const {
    std::shared_lock<std::shared_mutex> lock(mutex);

    // Index 0 is reserved for missing values
    if (index == 0 || index > indexToWord.size()) {
        return boost::none;
    }

    // indexToWord is 0-indexed, but symbols start from 1
    return StringData(indexToWord[index - 1]);
}

size_t SymbolDictionary::getSymbolCount() const {
    std::shared_lock<std::shared_mutex> lock(mutex);
    return indexToWord.size();
}

size_t SymbolDictionary::getMemoryUsageBytes() const {
    std::shared_lock<std::shared_mutex> lock(mutex);

    size_t totalBytes = 0;

    // Memory for wordToIndex map
    for (const auto& [word, index] : wordToIndex) {
        totalBytes += word.size() + sizeof(uint32_t);
    }

    // Memory for indexToWord vector
    for (const auto& word : indexToWord) {
        totalBytes += word.size();
    }
    totalBytes += indexToWord.capacity() * sizeof(std::string);

    return totalBytes;
}

// ============================================================================
// TemporalSymbolDictionary Implementation
// ============================================================================

TemporalSymbolDictionary::TemporalSymbolDictionary(OperationContext* opCtx,
                                                   const UUID& collectionUUID,
                                                   DictionaryGranularity granularity)
    : collectionUUID(collectionUUID), granularity(granularity), opCtx(opCtx) {}

StatusWith<SymbolDictionary*> TemporalSymbolDictionary::getOrCreateDictionaryForTimestamp(
    const Timestamp& ts) {
    Timestamp windowStart = calculateWindowStart(ts);
    return getOrCreateDictionary(windowStart);
}

StatusWith<SymbolDictionary*> TemporalSymbolDictionary::getDictionaryForTimestamp(
    const Timestamp& ts) const {
    std::shared_lock<std::shared_mutex> lock(mutex);

    Timestamp windowStart = calculateWindowStart(ts);
    auto it = dictionaries.find(windowStart);

    if (it == dictionaries.end()) {
        return Status(ErrorCodes::NoSuchKey, "Dictionary not found for timestamp");
    }

    return it->second.get();
}

StatusWith<uint32_t> TemporalSymbolDictionary::encodeSymbol(StringData word,
                                                            const Timestamp& timestamp) {
    auto dictResult = getOrCreateDictionaryForTimestamp(timestamp);
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
    std::unique_lock<std::shared_mutex> lock(mutex);

    auto it = dictionaries.begin();
    while (it != dictionaries.end()) {
        Timestamp windowEnd = calculateWindowEnd(it->first);
        if (windowEnd <= beforeTimestamp) {
            it = dictionaries.erase(it);
        } else {
            ++it;
        }
    }

    return Status::OK();
}

TemporalSymbolDictionary::Stats TemporalSymbolDictionary::getStats() const {
    std::shared_lock<std::shared_mutex> lock(mutex);

    Stats stats{0, 0, 0};

    for (const auto& [windowStart, dict] : dictionaries) {
        stats.totalDictionaries++;
        stats.totalSymbols += dict->getSymbolCount();
        stats.memoryUsageBytes += dict->getMemoryUsageBytes();
    }

    return stats;
}

StatusWith<SymbolDictionary*> TemporalSymbolDictionary::getOrCreateDictionary(
    const Timestamp& windowStart) {
    std::unique_lock<std::shared_mutex> lock(mutex);

    auto it = dictionaries.find(windowStart);
    if (it != dictionaries.end()) {
        return it->second.get();
    }

    // Create new dictionary
    auto dict = std::make_unique<SymbolDictionary>();
    auto* dictPtr = dict.get();
    dictionaries[windowStart] = std::move(dict);

    return dictPtr;
}

Timestamp TemporalSymbolDictionary::calculateWindowStart(const Timestamp& timestamp) const {
    uint32_t seconds = timestamp.getSecs();
    uint32_t windowSizeSeconds = 0;

    switch (granularity) {
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

    switch (granularity) {
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

