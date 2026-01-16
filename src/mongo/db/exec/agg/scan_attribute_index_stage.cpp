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
 *    all of the code used herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version of the file(s), but you
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used herein.
 */

#include "mongo/db/exec/agg/scan_attribute_index_stage.h"

#include "mongo/db/exec/agg/document_source_to_stage_registry.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/exec/document_value/value.h"
#include "mongo/db/exec/timeseries/hcindex/collection_manager.h"
#include "mongo/db/exec/timeseries/hcindex/temporal_attribute_table.h"
#include "mongo/db/pipeline/document_source_scan_attribute_index.h"
#include "mongo/db/timeseries/bucket_catalog/global_bucket_catalog.h"
#include "mongo/bson/timestamp.h"
#include "mongo/logv2/log.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kQuery

namespace mongo {

boost::intrusive_ptr<exec::agg::Stage> documentSourceScanAttributeIndexToStageFn(
    const boost::intrusive_ptr<DocumentSource>& source) {
    auto* documentSource = dynamic_cast<DocumentSourceScanAttributeIndex*>(source.get());
    tassert(10980401, "expected 'DocumentSourceScanAttributeIndex' type", documentSource);

    auto matchExprClone = documentSource->_matchExpr ? documentSource->_matchExpr->clone() : nullptr;
    return make_intrusive<exec::agg::ScanAttributeIndexStage>(
        documentSource->getSourceName(), documentSource->getExpCtx(), std::move(matchExprClone));
}

namespace exec::agg {

REGISTER_AGG_STAGE_MAPPING(scanAttributeIndexStage,
                           DocumentSourceScanAttributeIndex::id,
                           documentSourceScanAttributeIndexToStageFn);

ScanAttributeIndexStage::ScanAttributeIndexStage(
    StringData stageName,
    const boost::intrusive_ptr<ExpressionContext>& pExpCtx,
    std::unique_ptr<MatchExpression> matchExpr)
    : Stage(stageName, pExpCtx), _matchExpr(std::move(matchExpr)) {}

GetNextResult ScanAttributeIndexStage::doGetNext() {
    // If this is the first call, query the attribute table for matching rowIds
    if (_rowIdIndex == 0 && _matchingRowIds.empty()) {
        auto opCtx = pExpCtx->getOperationContext();
        auto collUUID = pExpCtx->getUUID();

        if (!opCtx || !collUUID) {
            return GetNextResult::makeEOF();
        }

        // Get the HCIndexCollectionManager from the GlobalBucketCatalog
        auto& bucketCatalog = timeseries::bucket_catalog::GlobalBucketCatalog::get(
            opCtx->getServiceContext());
        auto hcindexMgr = timeseries::bucket_catalog::getHCIndexManager(bucketCatalog, *collUUID);

        if (!hcindexMgr) {
            return GetNextResult::makeEOF();
        }

        // Get current timestamp for window calculation
        Timestamp currentTimestamp(Date_t::now());

        // Query the attribute table for matching rowIds
        // The manager will initialize lazily on first use
        auto queryResult = hcindexMgr->queryRows(opCtx, _matchExpr.get(), currentTimestamp);
        if (!queryResult.isOK()) {
            return GetNextResult::makeEOF();
        }

        _matchingRowIds = queryResult.getValue();
    }

    // If we've already emitted all rowIds, return EOF
    if (_rowIdIndex >= _matchingRowIds.size()) {
        return GetNextResult::makeEOF();
    }

    // Emit the next rowId as a document
    int64_t rowId = _matchingRowIds[_rowIdIndex++];
    Document doc{{"rowId", rowId}};

    return GetNextResult(std::move(doc));
}

}  // namespace exec::agg
}  // namespace mongo

