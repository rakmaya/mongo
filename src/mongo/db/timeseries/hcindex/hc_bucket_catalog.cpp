/*
 * Copyright (C) 2024-present MongoDB, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the Server Side Public License, version 1,
 * as published by MongoDB, Inc.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * Server Side Public License for more details.
 *
 * You should have received a copy of the Server Side Public License
 * along with this program. If not, see
 * <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 * As a special exception, the copyright holders give you permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and upon the terms of
 * the Server Side Public License, version 1, as published by MongoDB, Inc.
 */

#include "mongo/db/timeseries/hcindex/hc_bucket_catalog.h"
#include "mongo/db/timeseries/hcindex/temporal_symbol_dictionary.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/util/time_support.h"
#include "mongo/bson/bsontypes.h"

namespace mongo::timeseries::hcindex {

HCBucketCatalog::HCBucketCatalog(OperationContext* opCtx,
                                 const UUID& collectionUUID,
                                 const TimeseriesOptions& timeseriesOptions,
                                 std::shared_ptr<HCIndexCollectionManager> hcindexMgr)
    : _opCtx(opCtx),
      _collectionUUID(collectionUUID),
      _timeseriesOptions(timeseriesOptions),
      _hcindexMgr(hcindexMgr) {}

StatusWith<std::vector<std::shared_ptr<HCBatch>>> HCBucketCatalog::stageInsertBatch(
    const OperationId& opId,
    const std::vector<BSONObj>& measurements,
    const OID& bucketId) {
    
    if (measurements.empty()) {
        return Status(ErrorCodes::BadValue, "Measurements vector cannot be empty");
    }

    if (!_hcindexMgr) {
        return Status(ErrorCodes::BadValue, "HCIndexCollectionManager is null");
    }

    std::vector<std::shared_ptr<HCBatch>> result;
    std::map<Timestamp, std::shared_ptr<HCBatch>> batchesByWindow;

    // Process each measurement
    for (const auto& measurement : measurements) {
        // Extract timestamp
        auto timestampStatus = _extractTimestamp(measurement);
        if (!timestampStatus.isOK()) {
            return timestampStatus.getStatus();
        }
        Timestamp ts = timestampStatus.getValue();

        // Calculate time window
        Timestamp windowStart = _getTimeWindowStart(ts);

        // Get or create batch for this window
        auto it = batchesByWindow.find(windowStart);
        std::shared_ptr<HCBatch> batch;
        if (it == batchesByWindow.end()) {
            batch = std::make_shared<HCBatch>(opId, bucketId, _timeseriesOptions.getTimeField());
            batch->timeWindowStart = windowStart;
            batch->timeWindowEnd = _getTimeWindowEnd(windowStart);
            batchesByWindow[windowStart] = batch;
        } else {
            batch = it->second;
        }

        // Encode metadata to rowId
        BSONObj metadata;
        if (_timeseriesOptions.getMetaField()) {
            auto metaField = *_timeseriesOptions.getMetaField();
            auto metaElem = measurement.getField(metaField);
            if (!metaElem.eoo()) {
                // If the metadata field is an object, extract it directly
                // Otherwise, wrap it in a BSON object with the field name
                if (metaElem.type() == BSONType::object) {
                    metadata = metaElem.Obj();
                } else {
                    metadata = BSON(metaField << metaElem);
                }
            }
        }

        auto rowIdStatus = _hcindexMgr->encodeMetadata(metadata, ts);
        if (!rowIdStatus.isOK()) {
            return rowIdStatus.getStatus();
        }

        // Add to batch
        batch->measurements.push_back(measurement);
        batch->rowIds.push_back(rowIdStatus.getValue());
    }

    // Convert map to vector
    for (auto& [windowStart, batch] : batchesByWindow) {
        result.push_back(batch);
    }

    return result;
}

Status HCBucketCatalog::prepareCommit(std::shared_ptr<HCBatch> batch) {
    if (!batch) {
        return Status(ErrorCodes::BadValue, "HCBatch cannot be null");
    }
    // Mark batch as prepared (implementation depends on batch state management)
    return Status::OK();
}

Status HCBucketCatalog::finish(std::shared_ptr<HCBatch> batch) {
    if (!batch) {
        return Status(ErrorCodes::BadValue, "HCBatch cannot be null");
    }
    // Complete batch promise and remove from active batches
    batch->promise.emplaceValue();
    return Status::OK();
}

Status HCBucketCatalog::abort(std::shared_ptr<HCBatch> batch, const Status& status) {
    if (!batch) {
        return Status(ErrorCodes::BadValue, "HCBatch cannot be null");
    }
    // Set error on batch promise
    batch->promise.setError(status);
    return Status::OK();
}

Status HCBucketCatalog::clear() {
    _activeBatches.clear();
    return Status::OK();
}

StatusWith<Timestamp> HCBucketCatalog::_extractTimestamp(const BSONObj& measurement) {
    auto timeField = _timeseriesOptions.getTimeField();
    auto timeElem = measurement.getField(timeField);
    
    if (timeElem.eoo()) {
        return Status(ErrorCodes::BadValue,
                      "Measurement missing timeField: " + timeField);
    }

    if (timeElem.type() != BSONType::date) {
        return Status(ErrorCodes::BadValue,
                      "timeField must be a Date, got: " + std::string(typeName(timeElem.type())));
    }

    return Timestamp(timeElem.date().toMillisSinceEpoch() / 1000, 0);
}

uint32_t HCBucketCatalog::_getWindowSizeSeconds() const {
    uint32_t windowSizeSeconds = 0;
    DictionaryGranularity granularity = _hcindexMgr->getGranularity();

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
            windowSizeSeconds = 60 * 60;  // Default to HOURLY (3600 seconds)
            break;
    }

    return windowSizeSeconds;
}

Timestamp HCBucketCatalog::_getTimeWindowStart(const Timestamp& timestamp) {
    uint32_t seconds = timestamp.getSecs();
    uint32_t windowSizeSeconds = _getWindowSizeSeconds();

    uint32_t windowStartSeconds = (seconds / windowSizeSeconds) * windowSizeSeconds;
    return Timestamp(windowStartSeconds, 0);
}

Timestamp HCBucketCatalog::_getTimeWindowEnd(const Timestamp& timestamp) {
    uint32_t seconds = timestamp.getSecs();
    uint32_t windowSizeSeconds = _getWindowSizeSeconds();
    return Timestamp(seconds + windowSizeSeconds, 0);
}

}  // namespace mongo::timeseries::hcindex

