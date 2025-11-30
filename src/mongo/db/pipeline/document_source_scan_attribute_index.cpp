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

#include "mongo/db/pipeline/document_source_scan_attribute_index.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/exec/document_value/value.h"

namespace mongo {

const DocumentSource::Id& DocumentSourceScanAttributeIndex::id = DocumentSource::allocateId("$_internalScanAttributeIndex");

DocumentSourceScanAttributeIndex::DocumentSourceScanAttributeIndex(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    std::unique_ptr<MatchExpression> matchExpr)
    : DocumentSource(kStageName, expCtx), _matchExpr(std::move(matchExpr)) {}

boost::intrusive_ptr<DocumentSourceScanAttributeIndex> DocumentSourceScanAttributeIndex::create(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    std::unique_ptr<MatchExpression> matchExpr) {
    return make_intrusive<DocumentSourceScanAttributeIndex>(expCtx, std::move(matchExpr));
}

boost::intrusive_ptr<DocumentSource> DocumentSourceScanAttributeIndex::createFromBson(
    BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& expCtx) {
    // For now, this stage is only created internally, not from user BSON
    uasserted(7999900, "$_internalScanAttributeIndex is an internal stage and cannot be created from BSON");
}

Value DocumentSourceScanAttributeIndex::serialize(const SerializationOptions& opts) const {
    return Value(Document{{getSourceName(), Document()}});
}

boost::intrusive_ptr<DocumentSource> DocumentSourceScanAttributeIndex::clone(
    const boost::intrusive_ptr<ExpressionContext>& newExpCtx) const {
    auto matchExprClone = _matchExpr ? _matchExpr->clone() : nullptr;
    return make_intrusive<DocumentSourceScanAttributeIndex>(newExpCtx, std::move(matchExprClone));
}

}  // namespace mongo

