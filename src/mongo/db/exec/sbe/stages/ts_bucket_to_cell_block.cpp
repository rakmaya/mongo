/**
 *    Copyright (C) 2023-present MongoDB, Inc.
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

#include "mongo/db/exec/sbe/stages/ts_bucket_to_cell_block.h"

#include "mongo/base/string_data.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/column/bsoncolumn.h"
#include "mongo/db/exec/sbe/size_estimator.h"
#include "mongo/db/exec/sbe/values/block_interface.h"
#include "mongo/db/exec/sbe/values/bson.h"
#include "mongo/db/exec/sbe/values/slot.h"
#include "mongo/db/exec/sbe/values/ts_block.h"
#include "mongo/db/exec/sbe/values/value.h"
#include "mongo/db/exec/timeseries/hcindex/hcindex_collection_manager.h"
#include "mongo/db/timeseries/timeseries_constants.h"
#include "mongo/logv2/log.h"
#include "mongo/util/assert_util.h"

#include <algorithm>
#include <cstddef>
#include <string>

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kQuery

namespace mongo::sbe {
TsBucketToCellBlockStage::TsBucketToCellBlockStage(std::unique_ptr<PlanStage> input,
                                                   value::SlotId bucketSlot,
                                                   std::vector<value::PathRequest> pathReqs,
                                                   value::SlotVector blocksOut,
                                                   boost::optional<value::SlotId> metaOut,
                                                   value::SlotId bitmapOutSlotId,
                                                   const std::string& timeField,
                                                   PlanNodeId nodeId,
                                                   bool participateInTrialRunTracking)
    : PlanStage("ts_bucket_to_cellblock"_sd,
                nullptr /* yieldPolicy */,
                nodeId,
                participateInTrialRunTracking),
      _bucketSlotId(bucketSlot),
      _pathReqs(pathReqs),
      _blocksOutSlotId(std::move(blocksOut)),
      _metaOutSlotId(metaOut),
      _bitmapOutSlotId(bitmapOutSlotId),
      _timeField(timeField),
      _pathExtractor(pathReqs, _timeField) {
    _children.emplace_back(std::move(input));
}

std::unique_ptr<PlanStage> TsBucketToCellBlockStage::clone() const {
    auto cloned = std::make_unique<TsBucketToCellBlockStage>(_children[0]->clone(),
                                                             _bucketSlotId,
                                                             _pathReqs,
                                                             _blocksOutSlotId,
                                                             _metaOutSlotId,
                                                             _bitmapOutSlotId,
                                                             _timeField,
                                                             _commonStats.nodeId,
                                                             participateInTrialRunTracking());

    // Clone HCIndex members
    if (_hcindexMetadataFilter) {
        cloned->_hcindexMetadataFilter = _hcindexMetadataFilter->clone();
    }
    if (_collectionUUID) {
        cloned->_collectionUUID = _collectionUUID;
    }
    cloned->_hcindexMgr = _hcindexMgr;

    return cloned;
}

void TsBucketToCellBlockStage::prepare(CompileCtx& ctx) {
    _children[0]->prepare(ctx);

    // Gets the incoming accessor for buckets.
    _bucketAccessor = _children[0]->getAccessor(ctx, _bucketSlotId);

    _blocksOutAccessor.resize(_pathReqs.size());
}

value::SlotAccessor* TsBucketToCellBlockStage::getAccessor(CompileCtx& ctx, value::SlotId slot) {
    if (slot == _bitmapOutSlotId) {
        return &_bitmapOutAccessor;
    }

    if (_metaOutSlotId && slot == *_metaOutSlotId) {
        return &_metaOutAccessor;
    }

    for (size_t i = 0; i < _pathReqs.size(); ++i) {
        if (slot == _blocksOutSlotId[i]) {
            return &_blocksOutAccessor[i];
        }
    }

    return _children[0]->getAccessor(ctx, slot);
}

void TsBucketToCellBlockStage::open(bool reOpen) {
    auto optTimer(getOptTimer(_opCtx));

    _commonStats.opens++;
    _children[0]->open(reOpen);

    // Until we have valid data, we disable access to slots.
    disableSlotAccess();
}

PlanState TsBucketToCellBlockStage::getNext() {
    auto optTimer(getOptTimer(_opCtx));

    // We are about to call getNext() on our child so do not bother saving our internal state in
    // case it yields as the state will be completely overwritten after the getNext() call.
    disableSlotAccess();

    // Before throwing away the TSBlocks we currently hold onto, count how many of them were
    // decompressed.
    for (size_t i = 0; i < _tsBlockStorage.size(); ++i) {
        _specificStats.numStorageBlocks++;
        _specificStats.numStorageBlocksDecompressed += _tsBlockStorage[i]->decompressed();
    }

    for (auto& acc : _blocksOutAccessor) {
        acc.reset();
    }

    auto state = _children[0]->getNext();
    if (state == PlanState::IS_EOF) {
        return trackPlanState(state);
    }
    state = trackPlanState(state);

    initCellBlocks();

    _specificStats.numCellBlocksProduced += _blocksOutAccessor.size();

    return state;
}

void TsBucketToCellBlockStage::close() {
    auto optTimer(getOptTimer(_opCtx));

    trackClose();
    _children[0]->close();
}

std::unique_ptr<PlanStageStats> TsBucketToCellBlockStage::getStats(bool includeDebugInfo) const {
    auto ret = std::make_unique<PlanStageStats>(_commonStats);

    ret->children.emplace_back(_children[0]->getStats(includeDebugInfo));
    if (includeDebugInfo) {
        BSONObjBuilder bob;
        bob.appendNumber("numCellBlocksProduced",
                         static_cast<long long>(_specificStats.numCellBlocksProduced));
        bob.appendNumber("numStorageBlocks",
                         static_cast<long long>(_specificStats.numStorageBlocks));
        bob.appendNumber("numStorageBlocksDecompressed",
                         static_cast<long long>(_specificStats.numStorageBlocksDecompressed));

        ret->debugInfo = bob.obj();
    }
    return ret;
}

const SpecificStats* TsBucketToCellBlockStage::getSpecificStats() const {
    return &_specificStats;
}

std::vector<DebugPrinter::Block> TsBucketToCellBlockStage::debugPrint() const {
    auto ret = PlanStage::debugPrint();

    DebugPrinter::addIdentifier(ret, _bucketSlotId);

    ret.emplace_back(DebugPrinter::Block("pathReqs[`"));
    for (size_t idx = 0; idx < _pathReqs.size(); ++idx) {
        if (idx) {
            ret.emplace_back(DebugPrinter::Block("`,"));
        }
        DebugPrinter::addIdentifier(ret, _blocksOutSlotId[idx]);
        ret.emplace_back("=");

        ret.emplace_back(_pathReqs[idx].toString());
    }
    ret.emplace_back(DebugPrinter::Block("`]"));

    if (_metaOutSlotId) {
        ret.emplace_back("meta =");
        DebugPrinter::addIdentifier(ret, *_metaOutSlotId);
    }

    ret.emplace_back("bitmap =");
    DebugPrinter::addIdentifier(ret, _bitmapOutSlotId);

    DebugPrinter::addNewLine(ret);
    DebugPrinter::addBlocks(ret, _children[0]->debugPrint());

    return ret;
}

size_t TsBucketToCellBlockStage::estimateCompileTimeSize() const {
    size_t size = sizeof(*this);
    size += size_estimator::estimate(_children);
    return size;
}

void TsBucketToCellBlockStage::doSaveState() {
    if (!slotsAccessible()) {
        return;
    }

    for (size_t i = 0; i < _blocksOutAccessor.size(); ++i) {
        // Copy the CellBlock, which will force any data that's unowned by SBE (owned by storage)
        // memory to be copied. This also means that any already decompressed data will get copied,
        // and will not need to be decompressed again.
        auto [cellBlockTag, cellBlockVal] = _blocksOutAccessor[i].getViewOfValue();

        auto [cpyTag, cpyVal] = value::copyValue(cellBlockTag, cellBlockVal);
        _blocksOutAccessor[i].reset(true, cpyTag, cpyVal);
    }

    if (_metaOutSlotId) {
        prepareForYielding(_metaOutAccessor, slotsAccessible());
    }
}



void TsBucketToCellBlockStage::initCellBlocks() {
    auto [bucketTag, bucketVal] = _bucketAccessor->getViewOfValue();
    tassert(11093509, "Expected bsonObject tag type", bucketTag == value::TypeTags::bsonObject);

    BSONObj bucketObj(value::getRawPointerView(bucketVal));
    if (_metaOutSlotId) {
        auto metaElt = bucketObj[timeseries::kBucketMetaFieldName];
        auto [metaTag, metaVal] = bson::convertFrom<true>(metaElt);
        _metaOutAccessor.reset(false, metaTag, metaVal);
    }

    auto [nMeasurements, tsBlocks, cellBlocks] = _pathExtractor.extractCellBlocks(bucketObj);
    _tsBlockStorage = std::move(tsBlocks);
    tassert(11093510,
            "Number of cell blocks doesn't match the number of accessors",
            cellBlocks.size() == _blocksOutAccessor.size());
    for (size_t i = 0; i < cellBlocks.size(); ++i) {
        _blocksOutAccessor[i].reset(true,
                                    value::TypeTags::cellBlock,
                                    value::bitcastFrom<value::CellBlock*>(cellBlocks[i].release()));
    }

    // Create bitmap for filtering measurements
    std::unique_ptr<value::ValueBlock> bitmap;

    // Try to apply HCIndex filtering if available
    if (_hcindexMetadataFilter && _hcindexMgr && _collectionUUID) {
        // Initialize matching rowIds for this bucket (calls queryRows() once per bucket)
        initializeHCIndexMatchingRowIds(bucketObj);

        // Build bitmap using cached rowIds (no lock acquisition)
        bitmap = createHCIndexFilteredBitmap(bucketObj, nMeasurements);
    }

    // Fall back to all-1s bitmap if HCIndex filtering is not available or fails
    if (!bitmap) {
        bitmap = std::make_unique<value::MonoBlock>(nMeasurements,
                                                    value::TypeTags::Boolean,
                                                    value::bitcastFrom<bool>(true));
    }

    _bitmapOutAccessor.reset(true,
                             value::TypeTags::valueBlock,
                             value::bitcastFrom<value::ValueBlock*>(bitmap.release()));
}

void TsBucketToCellBlockStage::initializeHCIndexMatchingRowIds(const BSONObj& bucketObj) {
    LOGV2(9999989, "HCIndex: initializeHCIndexMatchingRowIds called");

    // Clear previous cache
    _hcindexMatchingRowIds.clear();
    _hcindexRowIdsInitialized = false;

    // Check if we have the necessary components
    if (!_hcindexMgr || !_hcindexMetadataFilter || !_opCtx) {
        LOGV2(9999990, "HCIndex: Missing required components for HCIndex filtering",
              "hasMgr"_attr = (_hcindexMgr != nullptr),
              "hasFilter"_attr = (_hcindexMetadataFilter != nullptr),
              "hasOpCtx"_attr = (_opCtx != nullptr));
        return;
    }

    // Check if bucket has HCIndex flag in the meta section
    auto metaElt = bucketObj[timeseries::kBucketMetaFieldName];
    if (metaElt.eoo()) {
        LOGV2(9999991, "HCIndex: Bucket does not have meta field");
        return;
    }

    auto metaObj = metaElt.Obj();
    auto hcindexFlag = metaObj["hcindex"];

    if (hcindexFlag.eoo() || !hcindexFlag.trueValue()) {
        LOGV2(9999991, "HCIndex: Bucket does not have hcindex flag set in meta");
        return;
    }

    // Get the windowStart from the meta field
    auto windowStartElt = metaObj["windowStart"];
    if (windowStartElt.eoo()) {
        LOGV2(9999992, "HCIndex: windowStart not found in meta");
        return;
    }

    if (windowStartElt.type() != BSONType::timestamp) {
        LOGV2(9999992, "HCIndex: windowStart is not a Timestamp");
        return;
    }

    Timestamp ts = windowStartElt.timestamp();
    LOGV2(9999993, "HCIndex: Got windowStart timestamp", "timestamp"_attr = ts);

    // Call queryRows() to get matching rowIds
    // The manager will initialize lazily on first use (in queryRows())
    // to avoid issues with stashed transaction resources during pipeline cleanup
    LOGV2(9999994, "HCIndex: Calling queryRows to get matching rowIds");

    auto queryResult = _hcindexMgr->queryRows(_opCtx, _hcindexMetadataFilter.get(), ts);

    if (!queryResult.isOK()) {
        LOGV2(9999995, "HCIndex: queryRows failed", "error"_attr = queryResult.getStatus());
        return;
    }

    auto rowIds = queryResult.getValue();
    _hcindexMatchingRowIds.insert(rowIds.begin(), rowIds.end());
    _hcindexRowIdsInitialized = true;

    LOGV2(9999996, "HCIndex: Initialized matching rowIds", "count"_attr = _hcindexMatchingRowIds.size());
}

std::unique_ptr<value::ValueBlock> TsBucketToCellBlockStage::createHCIndexFilteredBitmap(
    const BSONObj& bucketObj, size_t nMeasurements) {
    LOGV2(9999990, "HCIndex: createHCIndexFilteredBitmap called", "nMeasurements"_attr = nMeasurements);

    // Check if bucket has HCIndex flag in the meta section
    auto metaElt = bucketObj[timeseries::kBucketMetaFieldName];
    if (metaElt.eoo()) {
        LOGV2(9999991, "HCIndex: Bucket does not have meta field");
        return nullptr;
    }

    auto metaObj = metaElt.Obj();
    auto hcindexFlag = metaObj["hcindex"];

    if (hcindexFlag.eoo() || !hcindexFlag.trueValue()) {
        LOGV2(9999991, "HCIndex: Bucket does not have hcindex flag set in meta");
        return nullptr;
    }

    LOGV2(9999992, "HCIndex: Bucket has hcindex flag, building filtered bitmap");

    // Get the rowId column from the data section
    auto dataObj = bucketObj[timeseries::kBucketDataFieldName].Obj();
    auto rowIdElt = dataObj["rowId"];

    if (rowIdElt.eoo()) {
        LOGV2(9999993, "HCIndex: rowId field not found in data");
        return nullptr;
    }

    // Extract the rowId values from the BSONColumn
    BSONColumn rowIdColumn(rowIdElt);
    std::vector<bool> bitmap;
    bitmap.reserve(nMeasurements);

    size_t idx = 0;
    for (auto elem : rowIdColumn) {
        if (idx >= nMeasurements) break;
        if (!elem.eoo()) {
            int64_t rowId = elem.Long();
            // Check if this rowId is in the cached matching set
            bool matches = _hcindexMatchingRowIds.count(rowId) > 0;
            bitmap.push_back(matches);
        } else {
            bitmap.push_back(false);
        }
        idx++;
    }

    LOGV2(9999995, "HCIndex: Built bitmap", "size"_attr = bitmap.size());

    // Count true values in bitmap
    size_t trueCount = 0;
    for (bool b : bitmap) {
        if (b) trueCount++;
    }
    LOGV2(9999996, "HCIndex: Bitmap true count", "trueCount"_attr = trueCount, "totalSize"_attr = bitmap.size());

    // Convert bitmap to ValueBlock
    // Create a BoolBlock from the bitmap vector
    return std::make_unique<value::BoolBlock>(std::move(bitmap));
}
}  // namespace mongo::sbe
