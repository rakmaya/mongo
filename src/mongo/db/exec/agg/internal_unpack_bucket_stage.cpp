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
#include "mongo/db/exec/timeseries/hcindex/collection_manager.h"
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

// Set backing BSON on all ComparisonMatchExpressions in the tree. This needs
// to removed once we have a plan-rewrite that embeds appropriate plan stage
// information that overrides the meta field name appropriately in the
// MatchExpression.
void setBackingBSONOnAllComparisons(MatchExpression* expr, const BSONObj& backingBSON) {
    if (!expr) {
        return;
    }

    // PoC implementation only supports ComparisonMatchExpressions. I am not
    // doing a full generic MatchExpression rewrite because this will be easier
    // if we can pull this logic up to the pre-optimizer stage.
    if (auto comparisonExpr = dynamic_cast<ComparisonMatchExpressionBase*>(expr)) {
        comparisonExpr->setBackingBSON(backingBSON);
    }

    for (size_t i = 0; i < expr->numChildren(); ++i) {
        setBackingBSONOnAllComparisons(expr->getChild(i), backingBSON);
    }
}

// We need to change the generic "meta" symbology to the actual metadata field
// name used in the time-series collection.
BSONObj replaceMetaFieldInBSON(const BSONObj& bson, StringData actualMetaField) {
    BSONObjBuilder builder;
    for (auto elem : bson) {
        std::string fieldName = std::string(elem.fieldNameStringData());

        // TODO: Currently this handles only the simple case of a prefix match,
        // object and arrays. This could easily be generatized. Lets do that
        // once we decide the right place for this in the query pipeline.

        if (fieldName.find("meta.") == 0) {
            std::string newFieldName = std::string(actualMetaField) + "." + fieldName.substr(5);
            builder.appendAs(elem, newFieldName);
        } else if (fieldName == "meta") {
            builder.appendAs(elem, actualMetaField);
        } else if (elem.type() == BSONType::object) {
            auto nestedObj = replaceMetaFieldInBSON(elem.Obj(), actualMetaField);
            builder.append(elem.fieldName(), nestedObj);
        } else if (elem.type() == BSONType::array) {
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

}

boost::intrusive_ptr<exec::agg::Stage> documentSourceInternalUnpackBucketToStageFn(
    const boost::intrusive_ptr<DocumentSource>& documentSource) {
    auto dsInternalUnpackBucket =
        boost::dynamic_pointer_cast<const DocumentSourceInternalUnpackBucket>(documentSource);

    tassert(10565500, "expected 'DocumentSourceInternalUnpackBucket' type", dsInternalUnpackBucket);

    auto stage = make_intrusive<exec::agg::InternalUnpackBucketStage>(
        dsInternalUnpackBucket->kStageNameInternal,
        dsInternalUnpackBucket->getExpCtx(),
        dsInternalUnpackBucket->_sharedState,
        dsInternalUnpackBucket->_eventFilterDeps,
        dsInternalUnpackBucket->_unpackToBson,
        dsInternalUnpackBucket->_sampleSize);

    // If we are handling an HCIndex enabled collection, then we need to rewrite
    // the metadata filters.
    if (!dsInternalUnpackBucket->_hcindexMetadataFilterBSON.isEmpty()) {

        auto metaField = dsInternalUnpackBucket->_sharedState->_bucketUnpacker.getMetaField();
        BSONObj filterBSON = dsInternalUnpackBucket->_hcindexMetadataFilterBSON;

        // If user has customized it, replace "meta" with the actual one.
        if (metaField && *metaField != "meta"_sd) {
            filterBSON = replaceMetaFieldInBSON(filterBSON, *metaField);
        }

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
            setBackingBSONOnAllComparisons(matchExpr.get(), filterBSON);
            stage->setHCIndexMetadataFilter(std::move(matchExpr));
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
    tassert(5521502, "calling doGetNext() when '_sampleSize' is set is disallowed", !_sampleSize);

    // Otherwise, fallback to unpacking every measurement in all buckets until the child stage is
    // exhausted.
    if (auto measure = getNextMatchingMeasure()) {
        return GetNextResult(std::move(*measure));
    }

    auto nextResult = pSource->getNext();
    while (nextResult.isAdvanced()) {
        auto bucket = nextResult.getDocument().toBson();
        auto bucketMatchedQuery = _sharedState->_wholeBucketFilter &&
            exec::matcher::matchesBSON(_sharedState->_wholeBucketFilter.get(), bucket);

        // Set HCIndexCollectionManager and OperationContext if this is an
        // HCIndex-enabled collection. Extract the row-ids. This simply gets to
        // the row-ids. If there are filters on the measurements, we expect
        // that to be pipelined after the extraction.
        auto collUUID = pExpCtx->getUUID();
        if (collUUID) {
            auto& bucketCatalog = timeseries::bucket_catalog::GlobalBucketCatalog::get(
                pExpCtx->getOperationContext()->getServiceContext());
            auto hcindexMgr = timeseries::bucket_catalog::getHCIndexManager(
                bucketCatalog, *collUUID);
            if (hcindexMgr) {
                _sharedState->_bucketUnpacker.setHCIndexCollectionManager(hcindexMgr.get());
                _sharedState->_bucketUnpacker.setOperationContext(pExpCtx->getOperationContext());

                // If we have an HCIndex metadata filter, query for matching
                // rowIds using the bucket timestamp
                if (_hcindexMetadataFilter) {
                    _hcindexMatchingRowIds.clear();
                    _hcindexRowIdsInitialized = false;

                    // TODO: Right now we are assuming control.min.time as the
                    // timestamp field. Need to verify if there are cases where
                    // this will not be available.
                    auto timeField = _sharedState->_bucketUnpacker.bucketSpec().timeField();
                    auto controlObj = bucket.getObjectField("control");
                    auto minObj = controlObj.getObjectField("min");
                    auto bucketTimestampElem = minObj.getField(timeField);

                    if (bucketTimestampElem && bucketTimestampElem.type() == BSONType::date) {
                        // Convert Date_t (milliseconds) to Timestamp (seconds)
                        auto dateT = bucketTimestampElem.Date();
                        auto seconds = dateT.toMillisSinceEpoch() / 1000;
                        auto bucketTimestamp = Timestamp(seconds, 0);

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
                        } else {
                            LOGV2_ERROR(9999985, "HCIndex: Query failed",
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
    while (_sharedState->_bucketUnpacker.hasNext()) {
        // Check HCIndex metadata filter first if present
        if (_hcindexMetadataFilter && _hcindexRowIdsInitialized) {
            auto rowId = _sharedState->_bucketUnpacker.getCurrentRowId();
            if (rowId >= 0 && _hcindexMatchingRowIds.find(rowId) == _hcindexMatchingRowIds.end()) {
                // Ahem! It is about time MongoDB had a proper iterator. Without
                // it many of the primimitives cannot be reasonably implemented.
                // For now I have added the skipRow, to not force an unpack.
                _sharedState->_bucketUnpacker.skipRow();
                // Without this, I need to upack like below!
                //auto _ = _sharedState->_bucketUnpacker.getNext();
                continue;
            }
        }

        if (_sharedState->_eventFilter) {
            if (_unpackToBson) {
                auto measure = _sharedState->_bucketUnpacker.getNextBson();
                bool matches = _sharedState->_bucketUnpacker.bucketMatchedQuery() ||
                    exec::matcher::matchesBSON(_sharedState->_eventFilter.get(), measure);
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
                if (matches) {
                    return measure;
                }
            }
        } else {
            return _sharedState->_bucketUnpacker.getNext();
        }
    }
    return {};
}

}  // namespace agg
}  // namespace exec
}  // namespace mongo
