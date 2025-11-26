# Symbol Dictionary and Attribute Table Design

## Symbol Dictionary

### Purpose
Maps metadata string values to small unsigned 32-bit integers for efficient storage and comparison.

### Key Properties
- **Immutable Indices**: Once assigned, symbol indices never change
- **Symbols Start from 1**: Index 0 is reserved for missing values
- **Thread-Safe**: Uses `stdx::shared_mutex` for concurrent access
- **Efficient Lookup**: O(1) hash table for word-to-index mapping

### Data Structure
```cpp
class TemporalSymbolDictionary {
    std::unordered_map<std::string, uint32_t> wordToIndex;  // word → index
    std::vector<std::string> indexToWord;                    // index → word
    mutable stdx::shared_mutex mutex;
};
```

### Example
```
Symbol Dictionary for window 09:00-09:59:
  "nyc" → 1
  "webapp" → 2
  "pod01" → 3
  "pod02" → 4
  
Index 0 is reserved for missing values (never assigned to symbols)
```

### Operations
- **getOrInsertSymbol(word)**: Get existing index or create new one
- **getSymbolIndex(word)**: Get index for word (returns 0 if not found)
- **getSymbol(index)**: Get word for index
- **size()**: Number of symbols in dictionary

## Attribute Table

### Purpose
Stores metadata as rows of integer indices, enabling efficient filtering and deduplication.

### Key Properties
- **Append-Only Rows**: Rows never modified, only appended
- **Dynamic Schema**: Columns added as new fields appear
- **Row Deduplication**: Identical rows return same rowId
- **Missing Value Sentinel**: 0 represents missing values in cells
- **Thread-Safe**: Uses `stdx::shared_mutex` for concurrent access

### Data Structure
```cpp
class TemporalAttributeTable {
    std::vector<std::string> schema;                    // Column names
    std::vector<std::vector<uint32_t>> rows;            // Row data (indices)
    std::map<std::vector<uint32_t>, int64_t> rowMap;    // Deduplication
    mutable stdx::shared_mutex mutex;
};
```

### Example
```
Attribute Table for window 09:00-09:59:
Schema: ["cluster", "service", "pod"]

rowId | cluster | service | pod
  0   |    1    |    2    |  3    (nyc, webapp, pod01)
  1   |    1    |    2    |  4    (nyc, webapp, pod02)
  2   |    1    |    3    |  3    (nyc, api, pod01)

When inserting [1, 2, 3] again → returns rowId 0 (deduplication)
```

### Operations
- **insertRow(indices)**: Insert row, return rowId (with deduplication)
- **getRow(rowId)**: Get row data for rowId
- **queryRows(predicate)**: Find rowIds matching predicate
- **addColumn(name)**: Add new column to schema
- **size()**: Number of rows in table

## Temporal Scoping

### Time Windows
Each time window has its own Symbol Dictionary and Attribute Table:

```
09:00-09:59 → Dictionary #1, Table #1
10:00-10:59 → Dictionary #2, Table #2
11:00-11:59 → Dictionary #3, Table #3
```

### Window Calculation
```cpp
enum class DictionaryGranularity {
    DAILY,      // 00:00:00 - 23:59:59
    HOURLY,     // HH:00:00 - HH:59:59
    THIRTY_MIN, // MM:00 or MM:30
    TEN_MIN,    // MM:00, MM:10, MM:20, ...
    FIVE_MIN    // MM:00, MM:05, MM:10, ...
};
```

### Benefits
- **Bounded Size**: Dictionary only contains current window's symbols
- **Natural Evolution**: New fields appear in new windows
- **Better Compression**: Fewer unique symbols per window
- **Temporal Locality**: Recent dictionaries stay in cache

## Integration with Buckets

### Bucket Metadata
```javascript
{
  _id: ObjectId(...),
  control: {version: 4, min: {...}, max: {...}, count: 100},
  meta: {
    symbolDictWindow: {
      collection: "system.hcindex.ops.symbols.<UUID>",
      windowStart: ISODate("2024-01-01T09:00:00Z"),
      windowEnd: ISODate("2024-01-01T09:59:59Z"),
      granularity: "HOURLY"
    },
    attributeTableWindow: {
      collection: "system.hcindex.ops.attributes.<UUID>",
      windowStart: ISODate("2024-01-01T09:00:00Z"),
      windowEnd: ISODate("2024-01-01T09:59:59Z"),
      granularity: "HOURLY"
    }
  },
  data: {
    timestamp: BinData(7, ...),
    cpu: BinData(7, ...),
    rowIds: BinData(7, ...)  // One rowId per measurement
  }
}
```

### Key Points
- **rowIds in data section**: Each measurement has its own rowId
- **Window info in meta**: Bucket knows which dictionary/table to use
- **Immutable references**: Window info never changes after bucket creation

## Write Path Integration

### Encoding Process
```
Raw Metadata: {cluster: "nyc", service: "webapp", pod: "pod01"}
  ↓
Symbol Dictionary Lookup:
  "nyc" → 1
  "webapp" → 2
  "pod01" → 3
  ↓
Attribute Table Insert:
  [1, 2, 3] → rowId 42
  ↓
Store in Bucket:
  data.rowIds: [42, 42, 43, ...]
```

## Query Path Integration

### Query Execution
```
User Query: {meta.cluster: "nyc", timestamp: {$gte: 09:00, $lt: 11:00}}
  ↓
Determine affected windows: [09:00-09:59, 10:00-10:59]
  ↓
For each window:
  - Get Symbol Dictionary
  - Look up "nyc" → index 1
  - Get Attribute Table
  - Query rows where cluster=1 → rowIds [42, 43, 45]
  ↓
Generate bucket predicate:
  {meta.symbolDictWindow.windowStart: 09:00, data.rowIds: {$in: [42,43,45]}}
  ↓
Scan buckets and filter
```

## Schema Evolution

### Adding New Fields
When a new metadata field appears:

1. **Old Window**: Schema stays fixed
2. **New Window**: New column added to schema
3. **Existing Rows**: Implicitly padded with 0 (missing value)
4. **New Rows**: Include value for new column

### Example
```
Window 09:00-09:59:
  Schema: ["cluster", "service"]
  Rows: [[1, 2], [1, 3]]

Window 10:00-10:59:
  New field "pod" appears
  Schema: ["cluster", "service", "pod"]
  Rows: [[1, 2, 0], [1, 3, 4]]  // 0 for missing
```

## Performance Characteristics

### Memory Usage
- **Per Symbol**: ~50 bytes (string + index)
- **Per Row**: ~12 bytes (3 uint32_t values)
- **Per Dictionary**: 1-10 MB (typical)
- **Per Table**: 1-5 MB (typical)

### Lookup Performance
- **Symbol Lookup**: O(1) hash table
- **Row Query**: O(n) scan (can be optimized with indexes)
- **Deduplication**: O(1) hash table

### Concurrent Access
- **Read-Heavy**: Shared locks for concurrent lookups
- **Write-Heavy**: Exclusive locks for insertions
- **No Blocking**: Readers don't block writers (separate locks)

## Persistence

### Operation-Based Storage
Instead of storing snapshots, operations are stored in timeseries:

```javascript
// Symbol Dictionary Operations
{op: "INIT", symbols: [("nyc", 1), ("webapp", 2)]}
{op: "opADD", symbols: [("pod01", 3)]}
{op: "FIN"}

// Attribute Table Operations
{op: "INIT", schema: ["cluster", "service"], rows: [[1, 2]]}
{op: "opADD", attributes: [("pod", 2)]}
{op: "FIN"}
```

### Reconstruction
To reconstruct a dictionary/table:
1. Scan operations for the window
2. Process INIT to create fresh structure
3. Process opADD to add symbols/columns
4. Process FIN to mark complete
5. Cache result for queries

### Benefits
- **Compact Storage**: Only deltas, not full snapshots
- **Audit Trail**: Complete history of changes
- **Partial Reconstruction**: Can reconstruct up to any timestamp
- **Deduplication**: REF operations avoid duplicates

## State Machine Design

### Four States

1. **NOP** (No Operation)
   - Initial state after construction
   - No operations allowed
   - Transition: Only to Reconstruction

2. **Reconstruction**
   - Dictionary/table being reconstructed from stored operations
   - Read operations allowed
   - Transition: To ReadOnly when reconstruction complete

3. **ReadOnly**
   - Locked state, no modifications allowed
   - Read operations allowed
   - Transition: Cannot transition from this state
   - Used for: Querying historical data

4. **ReadWrite**
   - Normal write mode
   - Both read and write operations allowed
   - Transition: To ReadOnly when finalized
   - Used for: Active time windows

### State Transitions

```
NOP → Reconstruction → ReadOnly
  ↓
ReadWrite → ReadOnly
```

### Implementation Details

- **changeState()**: Validates transitions, prevents invalid state changes
- **Modifications fail in NOP/ReadOnly**: Ensures immutability
- **Reconstruction mode**: Doesn't invoke writers
- **ReadWrite mode**: Doesn't require writers (can be set later via setWriter())

## Integration with Query Execution

### Per-Measurement Metadata Decoding

When unpacking HCIndex buckets during query execution:

1. **Initialization (reset())**
   - Detect HCIndex bucket by checking for `hcindex` flag in metadata
   - Initialize `_rowIdColumnIterator` from data.rowId BSONColumn
   - Cache iterator for efficient sequential access

2. **Per-Measurement Decoding (getNext())**
   - Extract current rowId from cached iterator (O(1))
   - Advance iterator for next measurement
   - Call `HCIndexCollectionManager::decodeMetadata(rowId, timestamp)`
   - Return decoded metadata to query engine

3. **Performance Optimization**
   - Iterator caching eliminates O(n) loop per measurement
   - Sequential access pattern matches bucket unpacking order
   - Overall complexity: O(n) for bucket instead of O(n²)

### Example Query Execution

```
Query: db.myts.find({metadata.host: "server1"})

For each bucket:
  1. reset() detects HCIndex, initializes iterator
  2. For measurement 0:
     - Extract rowId from iterator (O(1))
     - Decode: rowId 42 → {host: "server1", region: "us-east"}
     - Include in results
  3. For measurement 1:
     - Extract rowId from iterator (O(1))
     - Decode: rowId 43 → {host: "server2", region: "us-west"}
     - Filter out (doesn't match)
  4. For measurement 2:
     - Extract rowId from iterator (O(1))
     - Decode: rowId 42 → {host: "server1", region: "us-east"}
     - Include in results
```

