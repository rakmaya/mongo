# HCIndex (High Cardinality Indexing) for MongoDB Timeseries

The goal is to make MongoDB competitive in the field of realtime analytics,
business insights and observability. Databases like like Clickhouse, Apache
Pinot, Apache Druid and Apache Doris leads in these areas. Most small to medium
enterprise teams utilizes these systems and large (Uber/Google scale) have
their own solutions. Fundamental attribute of the data in this space is that
users have to process and query high-cardinality dataset. Another
characteristics is most of the user workflows has a certain time-ephimeral
quality; orders processed over the last hour, cluster usage in the last hour,
ai-token used in in the last minute, cpu/memory utilzation in the last 10
seconds etc...

Time parametrized high cardinality indexing strategy provides a solution to
address high-cardinality data ingestion and simplifies user workflows when it
comes to defining indexes and data-models. The pimary characteristics of the
datase in these spaces are:

- A large volume of data is ingested over a short span of time, usually in
    batches while still operating is the 10s of millisecond space.
- Data is extremly sparse (e.g. "clothing sold by store-X in the last hour",
    "number of containers restarted in the last 10 seconds") and is spread out
    over a large number of dimensions. That is, **ingestion cardinality** is
    extremely high.
- When zoomed out (e.g. all clothing sold by store-X), the data is very dense
    across some dimensions and sparse across others. The concept of "zoom out"
    is relative to the query. And thus **query-cardinality** can be high across
    some dimensions and low across others.
- Two user personas:
  - Human driven Data Insight: Users don't know what they are looking for but
      they keep querying until they find something interesting. They will pull
      large number of data over certain time-windows. **Scan-optimized**
      workflow.
  - Automated Systems: User will set up a system to look for a specific
      projection of data and will query it frequently (e.g. "Alert if order
      failure rate exceeds 5% whtin the last 5 minutes for any McDonalds in
      NYC"). **Seek-optimized** workflow.

One of the main user-side experience difference is that user rarely wants to
be concerned with figuring out what schema/indexes are required. When fully
implemented, the idea is that we auto-generate the indexes.

## Overview

Fundamentally, time parametrized high cardinality index assumes that all index
structure are ephimeral and all data organized for scan-optimal workflow.
Inverted indexes are dynamically built based on the column density and query
patterns along certain dimensions and persists for certain time-windows only.
This allows the index to be useful where it can be. It is not worth building
any inverted incides if the entire dataset is unique across all dimensions
since index lookup itself becomes a scan. Due to the time-parametrization, we
can also quickly drop indexes when data gets older. Example e-commerce or
observability space rarely need full metadata index beyond the most recent
n-hours and this n is super small for observability space. It also enables
auto rollups to be detected and optimized (More on this later).

One of the major benefit that MongoDB has over Clickhouse and Pinot is its
bucketing model. This model provides enough primitives to build the the
time-parametrized index structures. From my own personal experience through
this reference implementation, I felt that to some extent easier to build-on
than M3DB and Pinot. Some pushdown of predicates to the bucket level is harder
compared to those 2 databases, but it is not terribly hard either.


## Reference Implementation

This implementation is only a reference to proof-test the concept. It is not
production ready and is missing many features and optimizations. Goal of
reference implementation is to validate the concept and ensure that idea
provides the necessary performance and memory benefits in real-world scenarios.
Major components are:

1. **Symbol Dictionary**: Mapping metadata values to small integers
2. **Attribute Table**: Storing metadata as rows of integer indices
3. **Efficient Encoding**: Converting metadata to compact integer arrays
4. **Schema Evolution**: Dynamically adding columns as new fields appear
5. **Time Parametrized Index Structures**: Storing dictionaries and tables as
   sequences of operations (INIT, opADD, FIN, REF) rather than complete snapshots
6. **Inverted Index**: The reverse index on selected fields on certain buckets.
   Time/sequence parametrized index structures allows us to capitalize that in
   analytics, not all ranges are equally queried and not all dimensions are
   equally important.

I am calling this "Phase-0" since this is not a production-grade
implementation. I have outlined what is needed at the minimum in the Phase-1
and subsequent possibilities in other phases, if found beneficial.

### Separation of symbol dictionary and row encoding

By separating metadata into a **symbol dictionary** and **attribute table**:

- Metadata becomes easier to rip through (more compact structure)
- Integer comparisons can be utilized to optimally scan the attribute table
- Memory usage is independent of total collection cardinality. It is depended on
  cardinality within a bucket.
- Cons of this approach is that query predicates need to be rewritten to use
  integer values rather than original string values. In addition to this, the
  transformation itself is bucket depended. Row-1 in bucket X only makes sense in
  the context of bucket X.

### Why is this advantageous in analytics?

Analytics workloads follow a **two-phase pattern**:

1. **Exploration Phase (Scan-Optimized)**
   - Users explore data without knowing what they are looking for
   - System cannot predict query patterns, so building indexes upfront is wasteful
   - Time-parametrized bucket scans are efficient: scan only the time windows of
     interest
   - Metadata is already compressed (integers vs strings), making scans fast
   - Attribute table acts like an inverted index for many rudimentary filtering
   - SIMD-friendly encoding of attribute table allows for efficient scanning
     using CPU vector instructions

2. **Query Phase (Seek-Optimized)**
   - Once users find something interesting, they write specific queries
   - Inverted indexes are built dynamically on frequently-queried fields
   - Indexes are **bucket-specific** and **time-parametrized**, making them
     compact. That is, not every bucket needs to have the same index.
   - Dimensional statistics guide which fields to index within each bucket
   - For common patterns (e.g., aggregations), predicates can be pushed to bucket level, avoiding unpacking entirely

This approach is superior to building all-encompassing indexes upfront because
  - Indexes only exist where they are useful
  - Sparse data doesn't create bloated indexes

## Performance Tests - Time Series Benchmark Suite

Time Series Benchmark Suite is a set of performance tests that are designed to
emulate real-world workloads. It is used by many teams that deals with
timeseries data to justify the technology choices to their CTO/CIO.

### Ingestion Throughput (Rows/second)
Each row will have a number of metrics depending on the test cases.

**Ingester Settings**
Using 1 worker/1 shard
Insert Batch Size: 1000

| Test Case | System | MongoDB TS | MongoDB TS + HCIndex | TimescaleDB | Clickhouse |
| --- | --- | --- | --- | --- | --- |
| DevOps | M1 Max | 28.3K - 28.7K | 72.2-72.8K | 68.9K - 82.2K | 26.2K - 26.8K |
| E-Commerce | M1 Max | 8.9K - 9.4K | 31.4 - 31.6K | 26.6K - 27.0K | 57.1K - 61.2K|

Notes: - TimescaleDB does a unique-data indexing across all the tags (similar to
MongoDB's timeseries). It also generates rudimentary indexes for exact match
lookups. This maps squarely to what HCIndex does. TimescaleDB scales well across
reasonably useful cardinality size, but does poorly as cardinality gets into the
high range. Clickhouse is the exact opposite (in most cases). It does extremly
well for extreme high-cardinality cases, but does poorly for high volume
datasets that falls into a small number of "chunks" (buckets).  The MergeTree
optimization in Clickhouse shows its power here since it does not have to sort
the high-cardinality data during ingestion. This is very similar to what HCIndex
does since it excludes all indexing for data that falls on both density
extremes. Note that this is purely from the single-node perspective. Behaviors
change when we move to multi-node clusters where TimescaleDB closes the gap with
Clickhouse on the extreme high-cardinality from the ingestion side. However it
does so by sacrificing query fan-out performance.

#### E-Commerce Dataset ####
Simulates a random user's order being ingested into the database where there is
at least 1 order every 50ms.

```text
Number of rows: 71,980
Number of total metrics: 1,223,660
Simulation Parameters:
  - seed: 123
  - scale: 1,000,000
  - interval: 50ms
  - window: 1 hour (2025-01-23 09:00:00 to 2025-01-23 09:59:59)
```

1. IDENTIFICATION TAGS (Unique per order)
   - order_id: 71,980 unique
   - session_id: 71,980 unique
   - cart_id: 71,980 unique
2. CUSTOMER TAGS
   - user_id: 63,567 unique (out of scale=1,000,000 possible)
   - Only ~6.4% of users made purchases in this 1-hour window
   - user_segment: 5 (VIP, at_risk, dormant, new, returning)
   - customer_tier: 4 (bronze, gold, platinum, silver)
   - is_first_order: 2 (true, false)
3. TEMPORAL TAGS
   - order_date: 1 (only 2025-01-23) - ( b/c of 1-hour simulation range)
   - order_hour: 1 (only hour 9) - (b/c of 1-hour simulation range)
   - day_of_week: 1 (only Friday) - (b/c of 1-hour simulationrange)
   - is_weekend: 1 (only false) - (b/c of 1-hour simulation range)
   - is_holiday: 2 (true, false)
   - fiscal_quarter: 1 (only Q1) - (b/c of 1-hour simulation range)
   - season: 1 (only winter) (b/c of 1-hour simulation time range)
4. GEOGRAPHIC TAGS
   - country: 7 (AU, CA, DE, FR, JP, UK, US)
   - region: 7 (east, midwest, northeast, pacific, south, southwest, west)
5. CHANNEL TAGS
   - sales_channel: 5 (in_store, marketplace, mobile_app, phone, web)
   - platform: 4 (Android, iOS, web_desktop, web_mobile)
   - device_type: 3 (desktop, mobile, tablet)
   - browser: 5 (Chrome, Edge, Firefox, Opera, Safari)
   - referral_source: 6 (affiliate, direct, email, organic, paid_search, social)
   - campaign_id: 50 (campaign_0 to campaign_49)
6. PRODUCT TAGS
   - primary_category: 10 (automotive, beauty, books, clothing, electronics, grocery, health, home, sports, toys)
   - primary_subcategory: 100 (subcat_0 to subcat_99)
   - primary_brand: 200 (brand_0 to brand_199)
   - primary_product_id: 71,443
   - product_count: 5 (1-5 products per order)
   - has_multiple_categories: 2 (true, false)
7. STATUS TAGS
   - order_status: 7 (cancelled, confirmed, delivered, pending, processing, returned, shipped)

#### DevOps Dataset ####
Simulates a number of metrics across various dimensions generated, every 30 seconds.
```text
Number of rows: 107,100
Number of total metrics: 1,201,900
Simulation Parameters:
  - seed: 123
  - scale: 100
  - interval: 30s
  - window: 1 hour (2025-01-23 09:00:00 to 2025-01-23 09:59:59)
```

### Query Performance
**TODO**: Once we have the PlanStage extraction. Reference implementation does not have
the optimization to scan the AttributeTable (needs to unpack the buckets).


## High level design

Index structures themselves are time-parametrized. There are no index snapshots.
Instead of storing complete snapshots, HCIndex uses a **sequence of operations**
stored in timeseries collections:

**Note**: Not all the features mentioned here are coded in the reference
implementation (see later sections).

### Operation Types

1. **INIT** - Bootstrap operation for a new time window
   - Contains: symbol-to-id pairs, attribute schema, initial rows
   - Creates fresh Dictionary and AttributeTable for that window

2. **opADD** - Incremental additions (two variants)
   - Symbol variant: Add new symbol-to-id mappings
   - Attribute variant: Add new column to schema or add new row to the table.
       New row in the attribute table implies a new unique row within the
       interval.

3. **FIN** - Finalize operation
   - Marks Dictionary and AttributeTable as complete/immutable
   - No more operations can be added after FIN
   - FIN is optional since during query time, system looks for INIT and
     subsequent opADDs. FIN just serves as a marker to indicate that no more
     changes will be made even if new insert is requested by the user.

4. **REF** - Reference operation (optimization)
   - Only used in the Symbol Dictionary
   - Indicates this window reuses dictionary from a previous window
   - Avoids duplicating identical dictionaries
   - Example: In the case of the Merchant Transactions dataset, Since most
     common items are in the base dictionary, we can reuse it
     However, order numbers are an extremely high cardinality field and that we
     see in the current window are unique to this window and will be added to the
     local dictionary. In the below example, we are reusing the base dictionary
     (see REF) that contains bulk of the data from a previous window.
   ```
    {
    _id: ObjectId('695f114c524101152468cc9d'),
    timestamp: Timestamp({ t: 1765810680, i: 0 }),
    windowStart: Timestamp({ t: 1765810680, i: 0 }),
    windowEnd: Timestamp({ t: 1765810740, i: 0 }),
    period: 1,
    frequency: 1,
    REF: Timestamp({ t: 1765810560, i: 0 }),
    localIndexOffset: Long('179'),
    op: 'INIT',
    symbols: {
      '190': 304,
      '191': 305,
      '192': 306,
      '193': 307,
      '194': 308,
      '195': 309
    }
   ```

### Key Advantage: Partial Reconstruction

To interpret data from 09:00-09:25 in a window that runs 09:00-09:59:

- Only reconstruct operations up to 09:25
- No need to read ahead to FIN at 09:59:59
- Referenced dictionaries can also be reconstructed partially
- Referenced dictionaries + local window specific opADDs = complete dictionary
  and this allows for efficient seeks. High cardinality data that is unique to
  the local window does not need to be read into memory if we are not looking
  at the window.
- Enables efficient streaming queries on partial time ranges
- Reduces latency for older-window queries

## Collection Naming Convention

- **Symbol Operations**: `hcindex.ops.symbols.<collectionUUID>` (in user's database)
- **Attribute Operations**: `hcindex.ops.attributes.<collectionUUID>` (in user's database)

The `.ops` segment explicitly indicates these are operation streams, not
reconstructed structures. Operations collections are created in the same
database as the original timeseries collection to avoid namespace validation
issues.

## Inverted Index For RowIDs

For high density (tags that maps to more than 1 rows) having an inverted index
can be useful to eliminate the need to scan all the rows for a given tag. However,
in the analytics space, it common to have certain dimensions that are very sparse
and not worth building an inverted index for. Thus it is important to be able to
dynamically build inverted indexes for those significant dimensions only.

## Time-partitioned bitmap (roaring) index for metadata fields

### Indexing Model

For each metadata column (except those explicitly configured as sparse or
detected as cardinality-exploding), we maintain a time-scoped bitmap index of
the form:

```scss
(column, value, window) → bitmap(RowIDs)
```

RowIDs are local to the time window. The index is a write-path append-only
structure with no per-row deletions; eviction is performed by dropping entire
the entire window.

### Metadata Index Options

The index is controlled via `Timeseries.hcindex_options`

| Option               | Description                                               |
| -------------------- | --------------------------------------------------------- |
| buildMetadataIndex   | Enables or disables metadata indexing. Default is true.   |
| sparseIndexThreshold | Values occurring < X% treated as sparse (not pre-indexed) |
| denseIndexThreshold  | Values occurring > Y% always indexed                      |
| dynamicIndexBuild    | Values between thresholds may be indexed on demand        |
| excludedColumns      | List of columns to never index                            |
| includedColumns      | List of columns to always index                           |

- `valueFrequency < sparseThreshold` → no index
- `valueFrequency > denseThreshold` → always indexed
- `otherwise` → indexed dynamically based on observed queries

### Regex Support

Regex predicates are supported using

```scss
regex → matching values → OR(bitmaps) → matching RowIDs
```

1. Enumerate metadata values in the window dictionary that match the regex
2. For each matching value, fetch corresponding bitmaps
3. OR bitmaps to compute matching row set
4. For columns without bitmap coverage, fall back to scan within the window

This enables regex filtering without scanning the full time window whenever a
matching dictionary and bitmap exist.

### Time-Window and Dictionary Construction

Each window is associated with a metadata dictionary of the form:

```scss
window → {value → localValueID}
```

Dictionaries contain only values initially observed or promoted in that
window, except when inheritance rules apply

**Dictionary Inheritance**: We do not maintain a global dictionary. Instead, if
symbols are references in a composition of a base dictionary+local dictionary.
If values are observed only window-X, the symbol will be present in the local
dictionary for that window. This ensures that extremly high cardinality data
will not blow up global index space. Dictionary inheritince has 3 components:

1 - Base Dictionary (non-modifiable referenced dictionary from any of the previous window)
2 - Inherited Local Symbols (non-modifianble local dictionary from any of the last 2 windows)
3 - Local Symbols (modifiangle local dictionary for the current window)

This avoids dictionary growth for sparse metadata and bounds per-window state.

### Offline Dictionary Merge

As windows age out of the active write path, dictionaries may be merged offline
into a more compact representation. This should be done outside the ingestion
path and does not affect active windows.

### Regex Dictionary Representation

Each window dictionary may be implemented as one of:

| Dictionary | Notes                                                                  |
| ---------- | ---------------------------------------------------------------------- |
| Trie       | Fast prefix/suffix and moderate regex; inexpensive incremental updates |
| FST        | More compact; faster regex enumeration; ideal under heavy regex load   |

Selection criteria:

- Smaller or dynamically changing value sets → Trie
- Larger or stable value sets with heavy regex traffic → FST

Inherited windows share the same dictionary representation.

### Collection Naming Convention

Only bitmap is implemented in the reference implementation. Others needs to be
built accordingly.

- **Bitmap Index**: `hcindex.idx.bitmap.<collectionUUID>` Bitmap index that
  corresponds to a specific windowStart to windowEnd.
- **Trie**: `hcindex.idx.trie.<collectionUUID>` Trie to work with pre/suffix
    expressions.
- **FST**: `hcindex.idx.fst.<collectionUUID>` for more complex regex.


### Phase 0 Reference Implementation

Reference Implementation MVP will do the following to get the initial PoC
version

1. Implement basic PoC functionality for HC Index (Ingestion & Query)
   1. PoC Implementation of Symbol Dictionary and Attribute Table
   2. Query pipeline changes for basic find/filter (Class & SBE)
2. Basic Bitmap Index (could be a map/roaring) for metadata fields
3. Basic unit tests

Known Bugs: Iteration of the unpack row has a bug. TODO: It doesn't evaluate
'end' correctly.

## Engineering Milestones

Take this "what is there" (Phase 0) and "what we need" (Phase 1+). Owners of
the respective stack should take "what we need" as only a suggestion and not a
recommendation.

### Phase 0 Implementation

1. Basic implemenation of Dictionary and Attribute Table
2. Compact roaring Bitmap Index
3. Query path integration (find/match/count etc...)
4. Expression rewrites (splits the expressions into bucket-matching vs
   measurement matching). This needs to be moved out to proper PlatStage
   operations.
5. Minimum explain plan visibility.
6. Minimal unit tests
7. Local database only (mongos integration not tested)
8. And you also get some bugs!

Few things are also disabled, like verfication after write etc...


### Phase 1 Implementation

A chunk of changes are required on the query side to move foward into
production. In addition to the following, there are many TODO comments in the
reference implementation that need to be addressed.

1. PlanStage implementations that will provide better re-write/optimize the queries
   1. Better pushdowns. (e.g. count should use the Attribute+Bitmap instead of unpack-stage)
1. Compaction for Attribute Table and Bitmap Index for insertOne operations.
   1. Also create BitmapIndex summaries. This can void us requiring a full read
      of the index to serve aggregations (e.g. count)
3. Support more aggregation functions. I have only tested basic ones Phase-0
4. Implement Trie (Prefix/Suffix expressions are relatively common in analytics)
5. More Intelligent Indexing. 99% of the users should never have to specify
   indexing configurations.
   1. Auto-creation of bitmap index based on dynamic density/information gain.
   2. Drop non-referenced dictionaries from the older timespan from the memory
6. SIMD/AVX optimizations for scanning AttributeTable
7. Unit Tests


### Phase 2 Implementation

1. Implement FST (full regex support)
3. Add support for offline dictionary merge
4. Add support for costmodel integration
5. Add support for explain plan visibility
6. Implement Aggregation Buckets & Query Pushdown to use these buckets
    1. e.g. topK, summarize queries should git the aggregations. These
       aggregations can be created in an ephimeral concept as well.
7. Integration Tests

### Considerations For Future Work

- Additional compression for inherited dictionaries
- Costmodel integration with query planner
- Dictionary inheritance made visible in explain plans
- Future research areas (if anyone is interested, let me know):
  - Subcluster indexing is a good one to research!
  - Very dense values reduce pruning efficiency. A good problem to research on!

## Reference Implementation Code Doc

This doc is mostly generated via augment! I will do a more thorough check after
the year-end calibration period!

## File Structure/Locations

Most of the code: (there are changes on multiple other files)
```
src/mongo/db/exec/timeseries/hcindex/
```

## Simulation Code

```
src/mongo/db/exec/timeseries/hcindex/tssim
```

### Component Hierarchy

```
HCIndexCollectionManager (Central Manager)
├── TemporalSymbolDictionary
│   └── SymbolDictionary (per time window)
├── TemporalAttributeTable
│   └── AttributeTable (per time window)
├── HCIndexWriter
└── HCIndexReader
```

### Write Path Data Flow

```
User Insert
    ↓
makeNewDocumentForWrite() [timeseries_write_ops_utils_internal.cpp]
    ↓
HCIndexCollectionManager.encodeMetadata()
    ↓
TemporalAttributeTable.insertRow()
    ↓
TemporalSymbolDictionary.getOrInsertSymbol() [for each field value]
    ↓
Returns rowId (int64_t)
    ↓
Store rowId in bucket's data.rowId field (BSONColumn)
    ↓
HCIndexWriter.writeSymbolOp() [INIT/ADD operations]
HCIndexWriter.writeAttributeOp() [INIT/ADD operations]
    ↓
Operations stored in hcindex.ops.symbols.* and hcindex.ops.attributes.*
```

### Read Path Data Flow

```
Query Execution (find/aggregation)
    ↓
InternalUnpackBucketStage.doGetNext()
    ↓
BucketUnpacker.reset() [detects HCIndex bucket]
    ↓
Initialize _rowIdColumnIterator from data.rowId BSONColumn
    ↓
getNextMatchingMeasure() [for each measurement]
    ↓
BucketUnpacker.getNext() [unpacks measurement]
    ↓
getDecodedMetadataForMeasurement(index) [if HCIndex bucket]
    ↓
Extract rowId from cached iterator (O(1) per measurement)
    ↓
HCIndexCollectionManager.decodeMetadata(rowId, timestamp)
    ↓
TemporalAttributeTable.getRow() → symbol indices
    ↓
TemporalSymbolDictionary.decodeSymbols() → BSONObj
    ↓
Add decoded metadata to measurement document
    ↓
Apply event filter (if metadata predicates exist)
    ↓
Return matching measurement to query engine
```

### TODO: Fix the read path for Dictionary to detect base vs reference.

The writer (hcindex_writer) needs to write the opADD in different sections of
the doc so that we can distinguish what belongs in the base
dictinary and the delta dictionary. Right now, this creates a bug. This is an
easy fix.

### TODO: Could we use timeseries collection to store attribute table and index structures ?

Currently, we are using the document collection. The only problem is that we
will need some sort of batching logic to optimize for insertOne operation.
However, if we use timeseries collection, this could come for almost free? But
we will need PoC here... Note that in M3DB implementation using the timeseries
implementation was used to store these structures and it proved to be the most
efficient model.

### TODO: Remove Query Pipeline hacks

MongoDB's query planner creates a match stage before the unpack bucket stage and
it renames the provided metadata field name (I am using "metadata") to the
bucket-level field name (meta). However, for HCIndex collections:

- The bucket's meta field contains only window metadata (windowStart, windowEnd,
  hcindex flag)
- The actual measurement metadata is encoded as rowIds in the data section
- When unpacking, the rowIds are decoded back to the original metadata with the
  original field names

**Hack for now**: Reverse the name in the event filter:

- Detect when a $match stage before the unpack bucket stage contains metadata
  predicates
- Rename the field names back from meta to the original metadata field name
  using copyExpressionAndApplyRenames()
- Set this renamed expression as the event filter
- Remove the $match stage since its predicates are now applied as event filters
  after unpacking

**Query Pipeline Optimization: Match Stage Swapping Disabled for HCIndex**

MongoDB's query planner normally optimizes pipelines by pushing `$match` stages
before `$_internalUnpackBucket` to filter buckets early. However, this optimization
is **disabled for HCIndex collections** because:

- HCIndex metadata is encoded as rowIds in the bucket's data section, not in the
  bucket-level `meta` field
- The query planner cannot evaluate metadata predicates at the bucket level without
  first decoding the rowIds
- Pushing `$match` before unpacking would filter out valid buckets

**Current Short-Circuit**: The `canSwapWithMatch` constraint is set to `false` for
HCIndex collections, preventing the query planner from reordering stages. Metadata
predicates are instead applied as event filters after unpacking and decoding.

**More Optimization**: HCIndex's bitmap/trie should be used for many
bucket-level filtering optimization:

1. $count/stats etc... can just hit the index structures (right now it needs to unpack)
2. Teach the query planner to recognize HCIndex collections
3. Replace the `$match` stage with a custom `$_internalHCIndexScan` stage that:
   - Uses the bucket-level inverted index to identify matching buckets
   - Returns bucket IDs without unpacking
4. Follow the scan with `$_internalUnpackBucket` to unpack only matching buckets
5. Apply remaining predicates as event filters if needed

### TODO: Make Verify Function Work with HCIndex

The metadata verifier in `timeseries_write_ops_utils_internal.cpp` currently
skips HCIndex batches entirely. Implement HCIndex-aware verification that
validates:

- rowIds are valid (within bounds of attribute table)
- rowIds correspond to valid metadata
- Window metadata is consistent

### TODO: Verify ExpressionContext::getUUID() is Set in All Situations

The read path relies on `ExpressionContext::getUUID()` to retrieve the
collection UUID for HCIndexCollectionManager lookup. This is used in
`InternalUnpackBucketStage::doGetNext()` to get HCIndexCollectionManager. Need
to verify that `getUUID()` is reliably set in all query execution contexts:

- All query path
- Aggregation pipelines
- Filtered queries
- Sorted queries
- Indexed queries

