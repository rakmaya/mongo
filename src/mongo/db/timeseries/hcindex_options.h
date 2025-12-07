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
 *    As a special exception, the copyright holders of this program grant you
 *    permission to link this program with other programs to produce an
 *    executable, regardless of the license terms of the other programs, and to
 *    convey the resulting combined work. Corresponding source for a non-source
 *    form of this combined work must include the corresponding source for the
 *    parts of MongoDB or other programs used in the combined work. However, as
 *    a special exception, the source code distributed under this exception may
 *    be distributed under the terms of the Server Side Public License only
 *    under the condition that you also comply with the Server Side Public
 *    License (or a modified version thereof) with respect to all of the code
 *    that is not part of the combined work.
 *
 *    As a special exception, the copyright holders give you permission to link
 *    portions of this program with other programs to produce an executable,
 *    regardless of the license terms of those programs, and to convey the
 *    resulting combined work. Corresponding source for a non-source form of
 *    such a combined work must include the corresponding source for the parts
 *    of MongoDB or other programs used in the combined work. However, as a
 *    special exception, the source code distributed under this exception may
 *    be distributed under the terms of the Server Side Public License only
 *    under the condition that you also comply with the Server Side Public
 *    License (or a modified version thereof) with respect to all of the code
 *    that is not part of the combined work.
 */

#pragma once

#include "mongo/bson/timestamp.h"
#include "mongo/db/exec/timeseries/hcindex/temporal_symbol_dictionary.h"
#include "mongo/db/timeseries/timeseries_gen.h"
#include "mongo/util/assert_util.h"
#include <boost/optional/optional.hpp>

namespace mongo::timeseries {

/**
 * Represents a time-window configuration for HCIndex structures.
 * Combines period (hour, minute, second) with frequency (1-24 for hour, 1-59 for minute/second).
 */
struct HCIndexTimeWindow {
    HCIndexPeriodEnum period;
    int32_t frequency;

    HCIndexTimeWindow(HCIndexPeriodEnum p, int32_t f) : period(p), frequency(f) {
        validateFrequency();
    }

    void validateFrequency() const {
        switch (period) {
            case HCIndexPeriodEnum::Hour:
                uassert(8765401,
                        "HCIndex frequency for 'hour' period must be between 1 and 24",
                        frequency >= 1 && frequency <= 24);
                break;
            case HCIndexPeriodEnum::Minute:
                uassert(8765402,
                        "HCIndex frequency for 'minute' period must be between 1 and 59",
                        frequency >= 1 && frequency <= 59);
                break;
            case HCIndexPeriodEnum::Second:
                uassert(8765403,
                        "HCIndex frequency for 'second' period must be between 1 and 59",
                        frequency >= 1 && frequency <= 59);
                break;
        }
    }

    /**
     * Get the window size in seconds for this period and frequency.
     */
    uint32_t getWindowSizeSeconds() const {
        switch (period) {
            case HCIndexPeriodEnum::Hour:
                return frequency * 60 * 60;  // frequency hours in seconds
            case HCIndexPeriodEnum::Minute:
                return frequency * 60;  // frequency minutes in seconds
            case HCIndexPeriodEnum::Second:
                return frequency;  // frequency seconds
        }
        MONGO_UNREACHABLE;
    }

    /**
     * Calculate the window start timestamp for a given timestamp.
     * Aligns the timestamp to the nearest window boundary.
     */
    Timestamp calculateWindowStart(const Timestamp& timestamp) const {
        uint32_t seconds = timestamp.getSecs();
        uint32_t windowSizeSeconds = getWindowSizeSeconds();

        // Align to window boundary by flooring to nearest multiple
        uint32_t windowStart = (seconds / windowSizeSeconds) * windowSizeSeconds;
        return Timestamp(windowStart, 0);
    }

    /**
     * Calculate the window end timestamp for a given window start.
     * Returns the timestamp of the last second in the window.
     */
    Timestamp calculateWindowEnd(const Timestamp& windowStart) const {
        uint32_t windowSizeSeconds = getWindowSizeSeconds();
        uint32_t windowEnd = windowStart.getSecs() + windowSizeSeconds - 1;
        return Timestamp(windowEnd, 0);
    }

    /**
     * Convert to DictionaryGranularity for internal use.
     * Maps period+frequency combinations to the appropriate granularity level.
     *
     * Mapping:
     * - Hour period: HOURLY (1 hour windows)
     * - Minute period: THIRTY_MIN (30 min), TEN_MIN (10 min), FIVE_MIN (5 min) based on frequency
     * - Second period: Not directly supported, defaults to FIVE_MIN
     */
    hcindex::DictionaryGranularity toDictionaryGranularity() const {
        switch (period) {
            case HCIndexPeriodEnum::Hour:
                return hcindex::DictionaryGranularity::HOURLY;
            case HCIndexPeriodEnum::Minute:
                // Map minute frequency to available granularities
                if (frequency >= 30) {
                    return hcindex::DictionaryGranularity::THIRTY_MIN;
                } else if (frequency >= 10) {
                    return hcindex::DictionaryGranularity::TEN_MIN;
                } else {
                    return hcindex::DictionaryGranularity::FIVE_MIN;
                }
            case HCIndexPeriodEnum::Second:
                // For second-level granularity, use the finest available (FIVE_MIN)
                return hcindex::DictionaryGranularity::FIVE_MIN;
        }
        MONGO_UNREACHABLE;
    }
};

/**
 * Get the effective HCIndexTimeWindow from HCIndexOptions.
 * Returns HOURLY with frequency 1 as default if options are not specified.
 */
inline HCIndexTimeWindow getEffectiveHCIndexTimeWindow(
    const boost::optional<HCIndexOptions>& hcindexOptions) {
    if (!hcindexOptions) {
        return HCIndexTimeWindow(HCIndexPeriodEnum::Hour, 1);
    }

    auto period = hcindexOptions->getPeriod().value_or(HCIndexPeriodEnum::Hour);
    auto frequency = hcindexOptions->getFrequency().value_or(1);

    return HCIndexTimeWindow(period, frequency);
}

}  // namespace mongo::timeseries

