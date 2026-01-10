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

#pragma once

#include "mongo/db/exec/sbe/stages/stages.h"
#include "mongo/db/exec/sbe/values/ts_block.h"
#include "mongo/db/matcher/expression.h"
#include "mongo/util/modules.h"
#include "mongo/util/uuid.h"

namespace mongo::timeseries::hcindex {

// FORWARD DECLARATION
class HCIndexCollectionManager;

}

namespace mongo::sbe {
/**
 * Given an input stage with a single slot containing a time series bucket BSON document, produces a
 * CellBlock for each path in 'pathReqs' into the output slots 'blocksOut'.
 *
 * Debug string representations:
 *
 *  ts_bucket_to_cellblock bucketSlot pathReqs[blocksOut[0] = paths[0], ...,
 *      blocksOut[N] = paths[N]] metaOut = meta? bitmapSlotId
 *
 * The 'meta' slot contains the bucket's 'meta' field. The 'bitmapSlotId' contains an all 1s
 * bitmap which has 'numMeasurements' entries.
 */
class TsBucketToCellBlockStage final : public PlanStage {
public:
    TsBucketToCellBlockStage(std::unique_ptr<PlanStage> input,
                             value::SlotId bucketSlotId,
                             std::vector<value::PathRequest> pathReqs,
                             value::SlotVector blocksOut,
                             boost::optional<value::SlotId> metaOutSlotId,
                             value::SlotId bitmapOutSlotId,
                             const std::string& timeField,
                             PlanNodeId nodeId,
                             bool participateInTrialRunTracking = true);

    std::unique_ptr<PlanStage> clone() const final;

    void prepare(CompileCtx& ctx) final;
    value::SlotAccessor* getAccessor(CompileCtx& ctx, value::SlotId slot) final;
    void open(bool reOpen) final;
    PlanState getNext() final;
    void close() final;

    std::unique_ptr<PlanStageStats> getStats(bool includeDebugInfo) const final;
    const SpecificStats* getSpecificStats() const final;
    void doDebugPrint(std::vector<DebugPrinter::Block>& ret,
                      DebugPrintInfo& debugPrintInfo) const final;
    size_t estimateCompileTimeSize() const final;

    /**
     * Set the HCIndex metadata filter for this stage.
     */
    void setHCIndexMetadataFilter(std::unique_ptr<MatchExpression> filter) {
        _hcindexMetadataFilter = std::move(filter);
    }

    /**
     * Set the collection UUID for HCIndex operations. This is needed to access
     * the HCIndex operations collections. This is a temporary solution until we
     * have a propr PlanStage that will setup the Dictionary, AttributeTable,
     * and BitMap index before we get to the unpack stage.
     */
    void setCollectionUUID(UUID collectionUUID) {
        _collectionUUID = collectionUUID;
    }

    /**
     * Sets the HCIndexCollectionManager for decoding metadata.
     */
    void setHCIndexCollectionManager(timeseries::hcindex::HCIndexCollectionManager* manager) {
        _hcindexMgr = manager;
    }

protected:
    void doSaveState() final;

    void doRestoreState() final;

    void doAttachCollectionAcquisition(const MultipleCollectionAccessor& mca) override {
        return;
    }

private:
    PlanState advanceChild();

    void initCellBlocks();

    /**
     * Initialize HCIndex matching rowIds for the current bucket.  This is
     * called once per bucket before unpacking measurements.  Note that we cache
     * the matching rowIds to avoid lock (re)acquisition during execution.
     */
    void initializeHCIndexMatchingRowIds(const BSONObj& bucketObj);

    /**
     * Create a filtered bitmap for HCIndex-encoded buckets.
     * Returns nullptr if the bucket is not HCIndex-encoded or filtering fails.
     */
    std::unique_ptr<value::ValueBlock> createHCIndexFilteredBitmap(
        const BSONObj& bucketObj,
        size_t nMeasurements);

    const value::SlotId _bucketSlotId;
    const std::vector<value::PathRequest> _pathReqs;
    const value::SlotVector _blocksOutSlotId;
    const boost::optional<value::SlotId> _metaOutSlotId;
    const value::SlotId _bitmapOutSlotId;
    const std::string _timeField;

    value::TsBucketPathExtractor _pathExtractor;

    value::SlotAccessor* _bucketAccessor = nullptr;
    std::vector<value::OwnedValueAccessor> _blocksOutAccessor;
    value::OwnedValueAccessor _metaOutAccessor;
    value::OwnedValueAccessor _bitmapOutAccessor;

    std::vector<std::unique_ptr<value::TsBlock>> _tsBlockStorage;

    TsBucketToBlockStats _specificStats;

    // HCIndex-related members
    std::unique_ptr<MatchExpression> _hcindexMetadataFilter;
    boost::optional<UUID> _collectionUUID;
    timeseries::hcindex::HCIndexCollectionManager* _hcindexMgr = nullptr;
    bool _hcindexInitialized = false;

    // Cache of matching rowIds for the current bucket
    // Populated once per bucket to avoid lock acquisition during execution
    std::set<int64_t> _hcindexMatchingRowIds;
    bool _hcindexRowIdsInitialized = false;
};
}  // namespace mongo::sbe
