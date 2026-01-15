# HCIndex (High Cardinality Indexing) for MongoDB Timeseries

The goal is to make MongoDB competitive in the field of data analytics. OLAP
uses a lot of high-cardinality metadata. A time/sequenced data model is one of
the most common data models in analytics. However, MongoDB's timeseries model is
not optimized for high-cardinality metadata. HCIndex is a proposed solution to
this problem. The pimary characteristics of analytics are:

- A large volume of data is ingested over a short span of time, usually in
  batches.
- Data is extremly sparse (e.g. "clothing sold by store-X in the last hour",
  "number of containers re-started in the last 10 seconds") and is spread out over
  a large number of dimensions. That is **ingestion cardinality** is extremely high.
- When zoomed out (e.g. all clothing sold by store-X), the data is very dense
  across some dimensions and sparse across others. The concept of "zoom out" is
  relative to the query. And thus **query-cardinality** can be high across some
  dimensions and low across others.
- Two user personas:
  - Human driven Data Insight: Users don't know what they are looking for but
    they keep querying until they find something interesting. They will pull
    large number of data over certain time-windows. **Scan-optimized** workflow.
  - Automated Systems: User will set up a system to look for a specific
    projection of data and will query it frequently (e.g. "Alert if order failure
    rate exceeds 5% whtin the last 5 minutes for any McDonalds in NYC").
    **Seek-optimized** workflow.

One of the main user-side experience difference is that user rarely wants to
be concerned with figuring out what schema/indexes are required. When fully
implemented, the idea is that we auto-generate the indexes.

## Overview

Fundamentally, time parametrized high cardinality index assumes that all index
structure are ephimeral and all data organized for scan-optimal workflow.
Inverted indexes are dynamically built based on the query patterns or along
certain dimensions, but for certain time-windows only. This allows the index to
be useful where it can be. It is not worth building any inverted incides if the
entire dataset is unique across all dimensions since index lookup itself
becomes a scan. Due to the time-parametrization, we can also quickly drop
indexes when data gets older. Example e-commerce or observability space rarely
need full metadata index beyond the most recent n-hours and this n is super
small for observability space.

One of the major benefit that MongoDB has over Clickhouse and Apache Pinot is
its bucketing model. This model provides enough primitives to build the the
time-parametrized index structures. It is, to some extent easier to build-on
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

This approach is superior to building all-encompassing indexes upfront because:

- Indexes only exist where they are useful
- Sparse data doesn't create bloated indexes
- Memory usage scales linearly with data and number of hot-buckets and not total
  cardinality

## Test Case - Merchant Transactions

Merchant Transactions dataset contains generated synthetic data for
transactions across merchants. This is a scenario where most transactions are
unique when we factor order number into account. Final writes are written in
batches. This is reasonable assumption since in extremly high volume use-cases
there is usually an aggregator or collector layer that batches the db writes.

### Why is this a good test case?

- High cardinality metadata
- Large number of unique transactions
- Use of Dimensional analytics (e.g. "What is the total sales for all
  McDonalds in New York?")

| Parameter    | Unique Values | Notes                                   |
| ------------ | ------------- | --------------------------------------- |
| Chains       | 10            | McDonals etc..                          |
| Merchant Id  | 10            | Denotes a specific store within a chain |
| Cities       | 12            | New York, San Francisco etc..           |
| Product Type | 4             | Sandwiches, Drinks, Combos, Dessert     |
| Product Item | 10            | Chicken Sandwich, Coke etc..            |
| Order Status | 2             | COMPLETE, FAILED                        |
| Order Number | XX            | Unique order number per merchant        |


### Test 1 - 40K Unique Transactions - Batched

This emulates a scenario where we have a large number of unique transactions,
but each transaction is relatively small set of fields. This is a common
scenario in e-commerce.

Batch size: 1000

| Ingestion    | HC Index Enabled | Regular Collection |
| ------------ | ---------------- | ------------------ |
| M1 Max (OPT) | 502 ms           | 3.0 sec            |
| M3 Max (OPT) | 352 ms           | 2.1 sec            |

| Component                     | HC Index Enabled    | Regular Collection   |
| ----------------------------- | ------------------- | -------------------- |
| Symbol Dictionary size        | 16,086 bytes        | -                    |
| Symbol Dictionary storageSize | 24,576 bytes        | -                    |
| Symbol Dictionary totalSize   | 24,576 bytes        | -                    |
| Attribute Table size          | 2,363,553 bytes     | -                    |
| Attribute Table storageSize   | 749,568 bytes       | -                    |
| Attribute Table totalSize     | 749,568 bytes       | -                    |
| Inverted Bitmap size          | 334,122 bytes       | -                    |
| Inverted Bitmap storageSize   | 237,568 bytes       | -                    |
| Inverted Bitmap totalSize     | 237,568 bytes       | -                    |
| Bucket Collection size        | 226,966 bytes       | 20,543,939 bytes     |
| Bucket Collection storageSize | 221,184 bytes       | 2,244,608 bytes      |
| Bucket Collection totalSize   | 221,184 bytes       | 7,413,760 bytes      |
| **Total Size**                | **2,940,966 bytes** | **20,543,939 bytes** |
| **Total storageSize**         | **1,232,896 bytes** | **2,244,608 bytes**  |
| **Total totalSize**           | **1,232,896 bytes** | **2,244,608 bytes**  |


### Test 2 - 40K Unique Transactions - Streaming

This simulates the use-case where the user is writing to db as the events
comes. Current implementation of bitmap index and attribute table is not
optimized to buffer internally.


| Ingestion    | HC Index Enabled | Regular Collection |
| ------------ | ---------------- | ------------------ |
| M1 Max (OPT) | 5.5 sec           | 7.66 sec            |
| M3 Max (OPT) | TBD sec           | TBD sec            |



| Component                     | HC Index Enabled    | Regular Collection   |
| ----------------------------- | ------------------- | -------------------- |
| Symbol Dictionary size        | 138,486 bytes        | -                    |
| Symbol Dictionary storageSize | 40,960 bytes        | -                    |
| Symbol Dictionary totalSize   | 40,960 bytes        | -                    |
| Attribute Table size          | 7,602,313 bytes     | -                    |
| Attribute Table storageSize   | 1,376,256 bytes       | -                    |
| Attribute Table totalSize     | 1,376,256 bytes       | -                    |
| Inverted Bitmap size          | 13,275,383 bytes       | -                    |
| Inverted Bitmap storageSize   | 1,945,600 bytes       | -                    |
| Inverted Bitmap totalSize     | 1,945,600 bytes       | -                    |
| Bucket Collection size        | 160,923 bytes       | 20,543,939 bytes     |
| Bucket Collection storageSize | 155,648 bytes       | 2,244,608 bytes      |
| Bucket Collection totalSize   | 155,648 bytes       | 7,421,952 bytes      |
| **Total Size**                | **21,177,105 bytes** | **20,543,989 bytes** |
| **Total storageSize**         | **3,518,464 bytes** | **2,244,608 bytes**  |
| **Total totalSize**           | **3,518,464 bytes** | **7,421,952 bytes**  |




## Test Case - Observability Metrics - Platform Metrics

Observability metrics represent one of the highest-volume workloads in modern infrastructure monitoring. This test case simulates a realistic cloud platform deployment with metrics from multiple subsystems (compute, network, disk, application, database) across a distributed infrastructure.

### Why is this a good test case?

**Volume Characteristics:**

- **Burst writes**: Metrics arrive in synchronized bursts every 10 seconds from all hosts
- **Multiple metric streams**: Each host emits 4-5 separate metric documents per interval (system, disk, network_rx, network_tx, and optionally application/database)
- **Batch ingestion**: All metrics for a time interval are written together, mimicking real-world aggregation pipelines (Prometheus, Datadog, etc.)

**Cardinality Characteristics:**

- **Moderate per-interval cardinality**: When examining a 10-second window, cardinality is manageable (~500-2000 unique combinations)
- **Exploding temporal cardinality**: Over hours/days, cardinality explodes due to:
  - Container restarts generating new container IDs
  - Auto-scaling creating/destroying pods
  - Rolling deployments changing pod names
  - Database table metrics (25 tables × N postgres instances)
- **Multi-dimensional filtering**: Queries often filter on combinations like `region=us-east-1 AND service=api-gateway AND environment=production`

**Real-World Patterns:**

- **Ephemeral infrastructure**: Kubernetes pods and containers are short-lived, causing continuous churn in metadata values
- **Table-level database metrics**: Postgres service emits per-table metrics (read/write requests, row counts) for 25 tables, significantly increasing cardinality
- **Time-of-day patterns**: CPU and request metrics vary by hour (business hours vs off-hours)
- **Service-specific metadata**: Different namespaces and services have different metric patterns

### Dataset Configuration

| Dimension              | Unique Values | Notes                                                          |
| ---------------------- | ------------- | -------------------------------------------------------------- |
| **Regions**            | 3             | us-east-1, us-west-2, eu-west-1                                |
| **Availability Zones** | 9             | 3 per region (e.g., us-east-1a, us-east-1b, us-east-1c)        |
| **Environments**       | 3             | production, staging, development                               |
| **Services**           | 16            | api-gateway, user-service, postgres, redis, etc.               |
| **Namespaces**         | 7             | default, backend, database, cache, messaging, ingress, logging |
| **Hosts**              | ~470          | 3-10 replicas per service (simulates auto-scaling)             |
| **Containers**         | Dynamic       | New ID on each restart (5% chance per hour)                    |
| **Clusters**           | 9             | Generated from region + environment (e.g., prod-cluster-01)    |
| **Metric Types**       | 6             | system, disk, network_rx, network_tx, application, database    |
| **Database Tables**    | 25            | users, orders, products, etc. (postgres only)                  |

**Estimated Documents per Interval (10s):**

- Base metrics (system, disk, network_rx, network_tx): `470 hosts × 4 = 1,880 docs`
- Application metrics (default/backend/ingress namespaces): `~150 hosts × 1 = 150 docs`
- Database metrics (postgres service with 25 tables): `~N postgres hosts × 25 = 25N docs`
- **Total**: ~2,030+ documents per 10-second interval

**Data Generation:**

```sh
# Generate 1 hour of metrics (360 intervals)
python load_observability.py --hours 1 --interval 10 --hcindex

# Dry-run to preview data structure
python load_observability.py --dry-run --hours 0.1 --interval 10
```


## Test Case - Financial Market Data

Record Count: 1,340,000 (rounded down to nearest 10k since simulation
randomizes parameters)


| Ingestion    | HC Index Enabled | Regular Collection |
| ------------ | ---------------- | ------------------ |
| M1 Max (OPT) | 14.4 sec           | 36.8 sec            |
| M3 Max (OPT) | TBD sec           | TBD sec            |


Common observability queries that filter on metadata dimensions:

**Query 1: Single service health**

```javascript
db.observability_hc.find({
  "metadata.service": "api-gateway",
  "metadata.region": "us-east-1",
  "metadata.environment": "production",
  timestamp: {
    $gte: ISODate("2026-01-14T06:00:00Z"),
    $lt: ISODate("2026-01-14T07:00:00Z"),
  },
});
```

**Query 2: Database table-specific metrics**

```javascript
db.observability_hc.find({
  "metadata.service": "postgres",
  "metadata.table_name": "users",
  "metadata.metric_type": "database",
  timestamp: {
    $gte: ISODate("2026-01-14T06:00:00Z"),
    $lt: ISODate("2026-01-14T07:00:00Z"),
  },
});
```

**Query 3: Cross-region application metrics**

```javascript
db.observability_hc.find({
  "metadata.metric_type": "application",
  "metadata.environment": "production",
  timestamp: {
    $gte: ISODate("2026-01-14T06:00:00Z"),
    $lt: ISODate("2026-01-14T07:00:00Z"),
  },
});
```

| Query Type               | Traditional | HCIndex | Improvement     |
| ------------------------ | ----------- | ------- | --------------- |
| **Single service (1hr)** | TODO ms     | TODO ms | TODO% faster    |
| **Table-specific (1hr)** | TODO ms     | TODO ms | TODO% faster    |
| **Cross-region (1hr)**   | TODO ms     | TODO ms | TODO% faster    |
| **Memory usage**         | TODO MB     | TODO MB | TODO% reduction |

### Aggregation Queries

Typical observability dashboards aggregate metrics across dimensions:

**Query 1: Average CPU by service**

```javascript
db.observability_hc.aggregate([
  {
    $match: {
      "metadata.metric_type": "system",
      "metadata.environment": "production",
      timestamp: {
        $gte: ISODate("2026-01-14T06:00:00Z"),
        $lt: ISODate("2026-01-14T07:00:00Z"),
      },
    },
  },
  {
    $group: {
      _id: "$metadata.service",
      avgCPU: {$avg: "$cpu_percent"},
      maxCPU: {$max: "$cpu_percent"},
    },
  },
]);
```

**Query 2: Database throughput by table**

```javascript
db.observability_hc.aggregate([
  {
    $match: {
      "metadata.metric_type": "database",
      "metadata.service": "postgres",
      timestamp: {
        $gte: ISODate("2026-01-14T06:00:00Z"),
        $lt: ISODate("2026-01-14T07:00:00Z"),
      },
    },
  },
  {
    $group: {
      _id: "$metadata.table_name",
      totalReads: {$sum: "$read_request_count"},
      totalWrites: {$sum: "$write_request_count"},
    },
  },
]);
```

| Query Type                 | Traditional | HCIndex | Improvement  |
| -------------------------- | ----------- | ------- | ------------ |
| **CPU by service**         | TODO ms     | TODO ms | TODO% faster |
| **DB throughput by table** | TODO ms     | TODO ms | TODO% faster |
| **Network by region**      | TODO ms     | TODO ms | TODO% faster |
| **Error rate by AZ**       | TODO ms     | TODO ms | TODO% faster |

### Cardinality Analysis

Understanding how cardinality evolves over time is critical for HCIndex effectiveness:

| Time Window | Unique Combinations | Container Churn | Notes                          |
| ----------- | ------------------- | --------------- | ------------------------------ |
| 10 seconds  | TODO                | 0%              | Single interval, minimal churn |
| 1 minute    | TODO                | ~0.1%           | Some container restarts        |
| 10 minutes  | TODO                | ~1%             | Auto-scaling events            |
| 1 hour      | TODO                | ~5%             | Multiple restarts, scaling     |
| 24 hours    | TODO                | ~100%+          | Full daily cycle, deployments  |

**Key Insight**: HCIndex's time-parametrized dictionaries prevent cardinality explosion by maintaining separate symbol mappings per time window, avoiding global dictionary bloat from ephemeral metadata values.

## Critical Design Principles

### Immutability of Symbol Indices

Once a symbol is assigned an index, it NEVER changes

- Existing buckets remain valid forever
- No need to rewrite bucket metadata
- Safe concurrent access without locks
- Enables efficient caching

### Append-Only Attribute Table

Rows are NEVER modified, only appended

- Existing bucket references remain valid
- No need to update bucket metadata
- Safe concurrent access
- Enables efficient indexing

### Separation of Concerns

Metadata encoding happens at write time, not query time

- Cleaner architecture
- Faster query execution
- Better caching strategies
- Easier to reason about correctness

### Backward Compatibility

Support both traditional and HCIndex metadata

- Gradual migration path
- No breaking changes
- Can mix V3 and V4 buckets
- Easier rollout and rollback

## Time Parametrized Index Structures

Index structures themselves are time-parametrized. There are no index snapshots.
Instead of storing complete snapshots, HCIndex uses a **sequence of operations**
stored in timeseries collections:

### Operation Types

1. **INIT** - Bootstrap operation for a new time window
   - Contains: symbol-to-id pairs, attribute schema, initial rows
   - Creates fresh Dictionary and AttributeTable for that window

2. **opADD** - Incremental additions (two variants)
   - Symbol variant: Add new symbol-to-id mappings
   - Attribute variant: Add new column to schema

3. **FIN** - Finalize operation
   - Marks Dictionary and AttributeTable as complete/immutable
   - No more operations can be added after FIN
   - FIN is optional since during query time, system looks for INIT and
     subsequent opADDs. FIN just serves as a marker to indicate that no more
     changes will be made even if more opADDs are requested.

4. **REF** - Reference operation (optimization)
   - Only applicable to the INIT operation
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
- Reduces latency for early-window queries

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
cardinality-exploding), we maintain a time-scoped bitmap index of the form:

```scss
(column, value, window) → bitmap(RowIDs)
```

RowIDs are local to the time window. The index is a write-path append-only
structure with no per-row deletions; eviction is performed by dropping entire
windows.

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

### Dictionary Inheritance

We do not maintain a global dictionary. Instead:

- If a sparse column observes a new value in a later window,
- The ingestion path does not build a fresh dictionary for that value,
- The window dictionary is marked as referencing the dictionary of the window
  where that value originated.

This avoids dictionary growth for sparse metadata and bounds per-window state.

### Offline Dictionary Merge

As windows age out of the active write path, dictionaries may be merged offline
into a more compact representation. This occurs outside the ingestion path and
does not affect active windows

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

Only bitmap is in the reference implementation. Others needs to be built
accordingly.

- **Bitmap Index**: `hcindex.idx.bitmap.<collectionUUID>` Bitmap index that
  corresponds to a specific windowStart to windowEnd.
- **Trie**: `hcindex.idx.trie.<collectionUUID>` Trie to work with pre/suffix
    expressions.
- **FST**: `hcindex.idx.fst.<collectionUUID>` for more complex regex.

### Exection Summary

Given predicate `(column REGEX pattern)` over time interval `[t0, t1)`:

```
1. Identify windows overlapping the interval
2. For each window:
     enumerate matching values via Trie/FST
3. OR bitmaps for matched values
4. Union bitmap results across all windows
5. Return matching row IDs
```

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

### Phase 1 Implementation

1. Compact roaring/compact bitmap index and compact attribute tables
2. Implement Trie (Prefix/Suffix expressions are relatively common in analytics)
3. Integrate with the ingestion path
   1. Add support for dynamic density calculation using information gain.
   2. Add support for building the bitmap index on demand.
4. SIMD/AVX optimizations for scanning AttributeTable
5. Unit Tests
6. mongos pushdowns

### Phase 2 Implementation

1. Implement FST (full regex support)
2. Add support for dictionary inheritance
3. Add support for offline dictionary merge
4. Add support for costmodel integration
5. Add support for explain plan visibility
6. Implement Aggregation Buckets & Query Pushdown to use these buckets
7. Integration Tests

### Considerations For Future Work

- Value demotion under memory pressure
- Autotuning thresholds
- Additional compression for inherited dictionaries
- Costmodel integration with query planner
- Dictionary inheritance made visible in explain plans
- Regex over free-form text ideally requires n-gram or substring indexing (future work)
- Very dense values reduce pruning efficiency. A good problem to research on!
- Subcluster indexing is a good future research!

## A Code Doc
This doc is mostly generated via augment! I will do a more thorough check after
the year-end calibration period!

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

**Issue**: The metadata verifier in `timeseries_write_ops_utils_internal.cpp` currently skips HCIndex batches entirely.
**Current Workaround**: Added `!batch->isHCIndexBatch &&` checks to skip verification
**Proper Solution**: Implement HCIndex-aware verification that validates:

- rowIds are valid (within bounds of attribute table)
- rowIds correspond to valid metadata
- Window metadata is consistent

### TODO: Verify ExpressionContext::getUUID() is Set in All Situations

**Issue**: The read path relies on `ExpressionContext::getUUID()` to retrieve
the collection UUID for HCIndexCollectionManager lookup.
**Current Implementation**: Used in `InternalUnpackBucketStage::doGetNext()` to get HCIndexCollectionManager
**Concern**: Need to verify that `getUUID()` is reliably set in all query execution contexts:

- Simple find() queries
- Aggregation pipelines
- Filtered queries
- Sorted queries
- Indexed queries
  **Action Items**:

1. Add assertions to verify UUID is set when HCIndex is enabled
2. Add tests for various query types
3. Document assumptions about UUID availability
   **Priority**: High (correctness depends on this)

## File Structure/Locations

Most of the code: (there are changes on multiple other files)
```
src/mongo/db/exec/timeseries/hcindex/
```

## Simulation Code

```
src/mongo/db/exec/timeseries/hcindex/tssim
```

