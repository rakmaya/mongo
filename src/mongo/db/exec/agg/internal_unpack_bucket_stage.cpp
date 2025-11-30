/**
 *    Copyright (C) 2025-present MongoDB, Inc.
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

#include "mongo/db/exec/agg/internal_unpack_bucket_stage.h"

#include "mongo/db/exec/agg/document_source_to_stage_registry.h"
#include "mongo/db/exec/matcher/matcher.h"
#include "mongo/db/exec/timeseries/hcindex/hcindex_collection_manager.h"
#include "mongo/db/matcher/expression_leaf.h"
#include "mongo/db/matcher/extensions_callback_noop.h"
#include "mongo/db/pipeline/document_path_support.h"
#include "mongo/db/pipeline/document_source_internal_unpack_bucket.h"
#include "mongo/db/query/compiler/parsers/matcher/expression_parser.h"
#include "mongo/db/timeseries/bucket_catalog/global_bucket_catalog.h"
#include "mongo/logv2/log.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage

namespace mongo {

namespace {
// Helper function to replace "meta" field prefix with the actual metadata field name in BSON
BSONObj replaceMetaFieldInBSON(const BSONObj& bson, StringData actualMetaField) {
    BSONObjBuilder builder;
    for (auto elem : bson) {
        std::string fieldName = std::string(elem.fieldNameStringData());

        // Check if the field name starts with "meta." (dotted path)
        if (fieldName.find("meta.") == 0) {
            // Replace "meta." with the actual metadata field name + "."
            std::string newFieldName = std::string(actualMetaField) + "." + fieldName.substr(5);
            builder.appendAs(elem, newFieldName);
        } else if (fieldName == "meta") {
            // Replace "meta" with the actual metadata field name
            builder.appendAs(elem, actualMetaField);
        } else if (elem.type() == BSONType::object) {
            // Recursively process nested objects
            auto nestedObj = replaceMetaFieldInBSON(elem.Obj(), actualMetaField);
            builder.append(elem.fieldName(), nestedObj);
        } else if (elem.type() == BSONType::array) {
            // For arrays, we need to process each element
            BSONArrayBuilder arrayBuilder(builder.subarrayStart(elem.fieldName()));
            for (auto arrayElem : elem.Obj()) {
                if (arrayElem.type() == BSONType::object) {
                    auto nestedObj = replaceMetaFieldInBSON(arrayElem.Obj(), actualMetaField);
                    arrayBuilder.append(nestedObj);
                } else {
                    arrayBuilder.append(arrayElem);
                }
            }
            arrayBuilder.done();
        } else {
            builder.append(elem);
        }
    }
    return builder.obj();
}
}  // namespace

boost::intrusive_ptr<exec::agg::Stage> documentSourceInternalUnpackBucketToStageFn(
    const boost::intrusive_ptr<DocumentSource>& documentSource) {
    auto dsInternalUnpackBucket =
        boost::dynamic_pointer_cast<const DocumentSourceInternalUnpackBucket>(documentSource);

    tassert(10565500, "expected 'DocumentSourceInternalUnpackBucket' type", dsInternalUnpackBucket);

    LOGV2(9999980, "HCIndex: Creating InternalUnpackBucketStage from DocumentSource", "hasMetadataFilter"_attr = (!dsInternalUnpackBucket->_hcindexMetadataFilterBSON.isEmpty()));

    auto stage = make_intrusive<exec::agg::InternalUnpackBucketStage>(
        dsInternalUnpackBucket->kStageNameInternal,
        dsInternalUnpackBucket->getExpCtx(),
        dsInternalUnpackBucket->_sharedState,
        dsInternalUnpackBucket->_eventFilterDeps,
        dsInternalUnpackBucket->_unpackToBson,
        dsInternalUnpackBucket->_sampleSize);

    // Pass the HCIndex metadata filter if present
    if (!dsInternalUnpackBucket->_hcindexMetadataFilterBSON.isEmpty()) {
        LOGV2(9999981, "HCIndex: Setting metadata filter on Stage", "filter"_attr = dsInternalUnpackBucket->_hcindexMetadataFilterBSON);

        // Get the actual metadata field name from the bucket spec
        auto metaField = dsInternalUnpackBucket->_sharedState->_bucketUnpacker.getMetaField();
        BSONObj filterBSON = dsInternalUnpackBucket->_hcindexMetadataFilterBSON;

        // If the metadata field is not "meta", replace it in the BSON
        if (metaField && *metaField != "meta"_sd) {
            filterBSON = replaceMetaFieldInBSON(filterBSON, *metaField);
            LOGV2(9999985, "HCIndex: Replaced meta field in filter", "originalFilter"_attr = dsInternalUnpackBucket->_hcindexMetadataFilterBSON, "newFilter"_attr = filterBSON);
        }

        // Make sure the BSON is owned so it outlives the MatchExpression
        if (!filterBSON.isOwned()) {
            filterBSON = filterBSON.getOwned();
        }

        auto parseResult = MatchExpressionParser::parse(
            filterBSON,
            dsInternalUnpackBucket->getExpCtx(),
            ExtensionsCallbackNoop(),
            MatchExpressionParser::kAllowAllSpecialFeatures);

        if (parseResult.isOK()) {
            auto matchExpr = std::move(parseResult.getValue());

            // Set the backing BSON on the MatchExpression to ensure the BSONElements remain valid
            if (auto comparisonExpr = dynamic_cast<ComparisonMatchExpressionBase*>(matchExpr.get())) {
                comparisonExpr->setBackingBSON(filterBSON);
                LOGV2(9999986, "HCIndex: Set backing BSON on ComparisonMatchExpression");
            }

            stage->setHCIndexMetadataFilter(std::move(matchExpr));
            LOGV2(9999983, "HCIndex: Successfully parsed metadata filter");
        } else {
            LOGV2_ERROR(9999984, "HCIndex: Failed to parse metadata filter", "error"_attr = parseResult.getStatus());
        }
    }

    return stage;
}

namespace exec {
namespace agg {

REGISTER_AGG_STAGE_MAPPING(_internalUnpackBucket,
                           DocumentSourceInternalUnpackBucket::id,
                           documentSourceInternalUnpackBucketToStageFn);

InternalUnpackBucketStage::InternalUnpackBucketStage(
    StringData stageName,
    const boost::intrusive_ptr<ExpressionContext>& pExpCtx,
    const std::shared_ptr<InternalUnpackBucketSharedState>& sharedState,
    DepsTracker depsTracker,
    const bool unpackToBson,
    const boost::optional<long long> sampleSize)
    : Stage(stageName, pExpCtx),
      _eventFilterDeps(std::move(depsTracker)),
      _sharedState(sharedState),
      _unpackToBson(unpackToBson),
      _sampleSize(sampleSize) {}

GetNextResult InternalUnpackBucketStage::doGetNext() {
    LOGV2(9999979, "InternalUnpackBucketStage::doGetNext called", "hasMetadataFilter"_attr = (_hcindexMetadataFilter != nullptr));
    // BREAKPOINT: Set breakpoint here to see if this is being called
    tassert(5521502, "calling doGetNext() when '_sampleSize' is set is disallowed", !_sampleSize);

    // Otherwise, fallback to unpacking every measurement in all buckets until the child stage is
    // exhausted.
    if (auto measure = getNextMatchingMeasure()) {
        return GetNextResult(std::move(*measure));
    }

    auto nextResult = pSource->getNext();
    LOGV2(9999998, "pSource->getNext() returned", "isAdvanced"_attr = nextResult.isAdvanced());
    while (nextResult.isAdvanced()) {
        auto bucket = nextResult.getDocument().toBson();
        auto bucketMatchedQuery = _sharedState->_wholeBucketFilter &&
            exec::matcher::matchesBSON(_sharedState->_wholeBucketFilter.get(), bucket);

        // Set HCIndexCollectionManager and OperationContext if this is an HCIndex-enabled collection
        auto collUUID = pExpCtx->getUUID();
        LOGV2(9999980, "HCIndex: doGetNext checking for HCIndex", "hasUUID"_attr = collUUID.has_value(), "hasMetadataFilter"_attr = (_hcindexMetadataFilter != nullptr));
        if (collUUID) {
            auto& bucketCatalog = timeseries::bucket_catalog::GlobalBucketCatalog::get(
                pExpCtx->getOperationContext()->getServiceContext());
            auto hcindexMgr = timeseries::bucket_catalog::getHCIndexManager(
                bucketCatalog, *collUUID);
            LOGV2(9999981, "HCIndex: Got HCIndexManager", "hasMgr"_attr = (hcindexMgr != nullptr));
            if (hcindexMgr) {
                _sharedState->_bucketUnpacker.setHCIndexCollectionManager(hcindexMgr.get());
                _sharedState->_bucketUnpacker.setOperationContext(pExpCtx->getOperationContext());

                // If we have an HCIndex metadata filter, query for matching rowIds using the bucket timestamp
                if (_hcindexMetadataFilter) {
                    _hcindexMatchingRowIds.clear();
                    _hcindexRowIdsInitialized = false;

                    // Extract the bucket timestamp from control.min.<timeField>
                    auto timeField = _sharedState->_bucketUnpacker.bucketSpec().timeField();
                    auto controlObj = bucket.getObjectField("control");
                    auto minObj = controlObj.getObjectField("min");
                    auto bucketTimestampElem = minObj.getField(timeField);

                    LOGV2(9999982, "HCIndex: Extracted bucket timestamp element", "hasElem"_attr = bucketTimestampElem.ok(), "type"_attr = (int)bucketTimestampElem.type());
                    if (bucketTimestampElem && bucketTimestampElem.type() == BSONType::date) {
                        // Convert Date_t (milliseconds) to Timestamp (seconds)
                        auto dateT = bucketTimestampElem.Date();
                        auto seconds = dateT.toMillisSinceEpoch() / 1000;
                        auto bucketTimestamp = Timestamp(seconds, 0);
                        LOGV2(9999983, "HCIndex: Querying attribute table for bucket",
                              "bucketTimestamp"_attr = bucketTimestamp);

                        auto queryResult = hcindexMgr->queryRows(
                            pExpCtx->getOperationContext(), _hcindexMetadataFilter.get(), bucketTimestamp);

                        if (queryResult.isOK()) {
                            auto rowIds = queryResult.getValue();
                            _hcindexMatchingRowIds.insert(rowIds.begin(), rowIds.end());
                            _hcindexRowIdsInitialized = true;
                            std::string rowIdStr;
                            for (auto id : _hcindexMatchingRowIds) {
                                if (!rowIdStr.empty()) rowIdStr += ", ";
                                rowIdStr += std::to_string(id);
                            }
                            LOGV2(9999984, "HCIndex: Found matching rowIds",
                                  "count"_attr = _hcindexMatchingRowIds.size(),
                                  "rowIds"_attr = rowIdStr);
                        } else {
                            LOGV2(9999985, "HCIndex: Query failed",
                                  "error"_attr = queryResult.getStatus());
                        }
                    }
                }
            }
        }

        _sharedState->_bucketUnpacker.reset(std::move(bucket), bucketMatchedQuery);

        uassert(
            5346509,
            str::stream()
                << "A bucket with _id "
                << _sharedState->_bucketUnpacker.bucket()[timeseries::kBucketIdFieldName].toString()
                << " contains an empty data region",
            _sharedState->_bucketUnpacker.hasNext());
        if (auto measure = getNextMatchingMeasure()) {
            return GetNextResult(std::move(*measure));
        }
        nextResult = pSource->getNext();
    }

    return nextResult;
}

boost::optional<Document> InternalUnpackBucketStage::getNextMatchingMeasure() {
    int measurementCount = 0;
    int matchedCount = 0;
    LOGV2(9999984, "getNextMatchingMeasure called",
          "hasEventFilter"_attr = (_sharedState->_eventFilter != nullptr),
          "hasHCIndexFilter"_attr = (_hcindexMetadataFilter != nullptr),
          "_hcindexRowIdsInitialized"_attr = _hcindexRowIdsInitialized,
          "unpackToBson"_attr = _unpackToBson);
    while (_sharedState->_bucketUnpacker.hasNext()) {
        // Check HCIndex metadata filter first if present
        if (_hcindexMetadataFilter && _hcindexRowIdsInitialized) {
            auto rowId = _sharedState->_bucketUnpacker.getCurrentRowId();
            LOGV2(9999990, "HCIndex: Checking rowId",
                  "rowId"_attr = rowId,
                  "matchingRowIds"_attr = _hcindexMatchingRowIds.size(),
                  "isInSet"_attr = (rowId >= 0 && _hcindexMatchingRowIds.find(rowId) != _hcindexMatchingRowIds.end()));
            if (rowId >= 0 && _hcindexMatchingRowIds.find(rowId) == _hcindexMatchingRowIds.end()) {
                // This measurement's rowId doesn't match the metadata predicate, skip it
                _sharedState->_bucketUnpacker.skipRow();  // Skip without unpacking
                // We need to read the measurement to advance the row iterator
                //auto _ = _sharedState->_bucketUnpacker.getNext();
                continue;
            }
        }

        if (_sharedState->_eventFilter) {
            if (_unpackToBson) {
                auto measure = _sharedState->_bucketUnpacker.getNextBson();
                bool matches = _sharedState->_bucketUnpacker.bucketMatchedQuery() ||
                    exec::matcher::matchesBSON(_sharedState->_eventFilter.get(), measure);
                LOGV2(9999985, "Checked measurement (BSON)",
                      "matches"_attr = matches,
                      "bucketMatched"_attr = _sharedState->_bucketUnpacker.bucketMatchedQuery());
                if (matches) {
                    return Document(measure);
                }
            } else {
                auto measure = _sharedState->_bucketUnpacker.getNext();
                // MatchExpression only takes BSON documents, so we have to make one. As an
                // optimization, only serialize the fields we need to do the match.
                BSONObj measureBson = _eventFilterDeps.needWholeDocument
                    ? measure.toBson()
                    : document_path_support::documentToBsonWithPaths(measure,
                                                                     _eventFilterDeps.fields);
                bool matches = _sharedState->_bucketUnpacker.bucketMatchedQuery() ||
                    exec::matcher::matchesBSON(_sharedState->_eventFilter.get(), measureBson);
                LOGV2(9999986, "Checked measurement (Document)",
                      "matches"_attr = matches,
                      "bucketMatched"_attr = _sharedState->_bucketUnpacker.bucketMatchedQuery(),
                      "measureBson"_attr = measureBson);
                if (matches) {
                    LOGV2(9999987, "Returning matched measurement",
                          "measure"_attr = measure.toBson());
                    return measure;
                }
            }
        } else {
            LOGV2(9999988, "No event filter, returning measurement");
            return _sharedState->_bucketUnpacker.getNext();
        }
    }
    LOGV2(9999989, "No more measurements");
    return {};
}

}  // namespace agg
}  // namespace exec
}  // namespace mongo
