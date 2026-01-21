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
#include "mongo/db/exec/timeseries/hcindex/delta_symbol_dictionary.h"
#include "mongo/db/exec/timeseries/hcindex/symbol_dictionary.h"
#include "mongo/db/exec/timeseries/hcindex/writer.h"
#include "mongo/db/exec/timeseries/hcindex/reader.h"
#include "mongo/base/error_codes.h"

namespace mongo::timeseries::hcindex {

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

