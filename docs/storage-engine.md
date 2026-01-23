# Triple Store Storage Engine Design

A single-file storage engine optimized for triple store workloads.

## Goals

- Read-heavy access patterns (thousands to millions of triples per query)
- Small write batches (dozens to hundreds of triples)
- Snapshot isolation (MVCC)
- Near-instant crash recovery
- Built-in change tracking for subscriptions
- Fast open/close for multi-tenant deployments (1000s of databases)

## Requirements

| Requirement | Specification |
|-------------|---------------|
| Scale | Thousands to billions of triples |
| Read pattern | Scans touching thousands-millions of triples |
| Write pattern | Small batches, mix of inserts/updates, rare deletes (~1%) |
| Durability | Zero data loss after server ack |
| Isolation | Snapshot isolation (MVCC) |
| Transactions | Single-request atomic (not cross-request) |
| Recovery | Near-instant startup |
| Multi-tenancy | 1000s of files, fast open/close |
| Change tracking | Which triples changed + changes since HLC |
| Performance target | <100ms for operations on millions of rows |

---

## Why Not Existing Solutions?

### SQLite
- **Pro**: Battle-tested, single file, good general performance
- **Con**: Row-oriented storage not optimal for aggregations over millions of rows
- **Con**: No built-in change tracking with HLC
- **Con**: MVCC via WAL has recovery time proportional to WAL size

### RocksDB/LevelDB (LSM-tree)
- **Pro**: Excellent write throughput
- **Con**: Read amplification (multiple levels to check)
- **Con**: Compaction can cause latency spikes
- **Con**: Not a single file (directory of files)

### LMDB (Copy-on-Write B-tree)
- **Pro**: Instant recovery, built-in MVCC, memory-mapped
- **Con**: Single writer limitation
- **Con**: High write amplification (full page copy)
- **Con**: File growth without compaction

### Why Custom?
A custom design allows us to:
1. Optimize index layout for triple store access patterns
2. Build change tracking into the core (not bolted on)
3. Balance recovery time vs write amplification
4. Tune for specific query patterns (aggregations, range scans)

---

## File Format

### Overall Structure

```
+-------------------------------------------------------------+
| Superblock (Page 0) - 8KB                                    |
+-------------------------------------------------------------+
| Transaction Log Region (configurable size, e.g., 64MB)       |
| - Circular buffer of change records                          |
| - Used for recovery and subscription queries                 |
+-------------------------------------------------------------+
| Page Allocation Bitmap                                       |
| - Tracks free/used pages                                     |
+-------------------------------------------------------------+
| B-Tree Pages (Data + Indexes)                                |
| - Primary index: (entity_id, attribute_id) -> value          |
| - Attribute index: attribute_id -> [(entity_id, value)]      |
| - Value indexes: (attribute_id, value) -> [entity_id]        |
+-------------------------------------------------------------+
| Overflow Pages                                               |
| - Large strings (>~1KB inline threshold)                     |
| - Future: blobs                                              |
+-------------------------------------------------------------+
```

### Page Size: 8KB

| Page Size | Pros | Cons |
|-----------|------|------|
| 4KB | Matches OS page size, less write amplification | More tree depth, more I/O for large scans |
| 8KB | Good balance, fewer tree levels | Slightly more write amplification |
| 16KB | Excellent for scans | Higher write amplification, memory overhead |

8KB balances:
- ~100-200 triples per leaf page (good scan efficiency)
- Reasonable write amplification
- Compatibility with most filesystem block sizes

### Superblock Layout (Page 0)

```
Offset  Size    Field
----------------------------------------------
0       8       Magic number: "ENSOTRPL"
8       4       Format version
12      4       Page size (8192)
16      8       File size in bytes
24      8       Total page count
32      8       Primary index root page
40      8       Attribute index root page
48      8       Free list head page
56      8       Last checkpoint LSN
64      16      Last checkpoint HLC
80      8       Transaction log start offset
88      8       Transaction log end offset
96      8       Transaction log capacity
104     8       Active transaction count
112     8       Next transaction ID
120     8       Schema version (for migrations)
128     896     Reserved for future use
1024    7168    Checkpoint metadata (active snapshots, etc.)
```

---

## Triple Storage Format

### Triple Record Layout

Each triple is stored as a variable-length record:

```
+-------------------------------------------------------------+
| Triple Record                                                |
+----------+--------------------------------------------------+
| Offset   | Field                                             |
+----------+--------------------------------------------------+
| 0        | entity_id (16 bytes)                              |
| 16       | attribute_id (16 bytes)                           |
| 32       | created_txn (8 bytes) - transaction that created  |
| 40       | deleted_txn (8 bytes) - transaction that deleted  |
|          |   (0 = not deleted, MAX = visible to all)         |
| 48       | created_hlc (16 bytes) - HLC timestamp            |
| 64       | value_type (1 byte)                               |
|          |   0x01 = null                                     |
|          |   0x02 = boolean                                  |
|          |   0x03 = number (f64)                             |
|          |   0x04 = string (inline, <=1KB)                   |
|          |   0x05 = string (overflow reference)              |
|          |   0x06 = date (future)                            |
|          |   0x07 = blob (future)                            |
| 65       | value_data (variable)                             |
|          |   boolean: 1 byte                                 |
|          |   number: 8 bytes (f64)                           |
|          |   string inline: 2-byte length + data             |
|          |   string overflow: 8-byte page + 4-byte length    |
+----------+--------------------------------------------------+
```

**Minimum record size**: 66 bytes (null value)
**Typical record size**: 74-150 bytes (small string)

### MVCC Visibility Rules

A triple is visible to transaction `T` if:
```
created_txn <= T.snapshot_txn AND
(deleted_txn == 0 OR deleted_txn > T.snapshot_txn)
```

For updates: mark the old triple as deleted and insert a new one (both in same transaction).

---

## Index Design

### Index 1: Primary Index (Entity-Attribute-Value)

**Purpose**: Point lookups and entity scans
**Key**: `(entity_id, attribute_id)`
**Value**: Full triple record (embedded in leaf)

```
B-tree structure:
- Internal nodes: [(key, child_page), ...]
- Leaf nodes: [triple_record, triple_record, ...]
- Leaf nodes are doubly-linked for range scans
```

**Operations**:
- Point lookup `(e, a)`: O(log n)
- All triples for entity `e`: O(log n + k)
- All triples: O(n) via leaf scan

### Index 2: Attribute Index

**Purpose**: Scans by attribute (e.g., "all users with attribute 'age'")
**Key**: `(attribute_id, entity_id)`
**Value**: Pointer to triple in primary index (page_id, slot_id)

**Operations**:
- All triples with attribute `a`: O(log n + k)

### Index 3: Value Indexes (Per-Type)

#### Numeric Value Index
**Key**: `(attribute_id, value: f64, entity_id)`
**Value**: Pointer to triple

Enables:
- Range queries: `age > 25 AND age < 65`
- Aggregations: `SUM(age)`, `AVG(age)` via index scan
- Min/Max: O(log n) via B-tree extremes

#### String Value Index
**Key**: `(attribute_id, value_prefix: [u8; 32], entity_id)`
**Value**: Pointer to triple

Enables:
- Exact match: `name = "John"`
- Prefix queries: `name STARTS WITH "Jo"`
- For `CONTAINS`/`ENDS WITH`: requires full scan of attribute

#### Boolean Value Index
**Structure**: Bitmap per (attribute_id, value)

```
attribute_id -> {
  true: RoaringBitmap of entity_ids,
  false: RoaringBitmap of entity_ids
}
```

Enables:
- `published = true`: O(1) bitmap lookup
- COUNT: O(1) via bitmap cardinality

### Index Storage Overhead

Estimated overhead per triple:
- Primary index: ~0 (data is stored here)
- Attribute index: ~26 bytes (key + pointer)
- Numeric index: ~34 bytes (when applicable)
- String index: ~58 bytes (when applicable)
- Boolean index: ~0.125 bytes (1 bit in bitmap)

**Total overhead**: ~1.3-1.5x raw data size

---

## Transaction Log (WAL)

### Purpose

1. **Durability**: Ensure committed transactions survive crashes
2. **Recovery**: Replay uncommitted transactions after crash
3. **Change Tracking**: Support "changes since HLC X" for subscriptions

### Log Record Format

```
+-------------------------------------------------------------+
| Log Record                                                   |
+----------+--------------------------------------------------+
| 0        | record_length (4 bytes)                           |
| 4        | record_type (1 byte)                              |
|          |   0x01 = BEGIN                                    |
|          |   0x02 = INSERT                                   |
|          |   0x03 = UPDATE (old + new value)                 |
|          |   0x04 = DELETE                                   |
|          |   0x05 = COMMIT                                   |
|          |   0x06 = CHECKPOINT                               |
| 5        | transaction_id (8 bytes)                          |
| 13       | hlc_timestamp (16 bytes)                          |
| 29       | payload (variable, depends on type)               |
|          |   INSERT: triple_record                           |
|          |   UPDATE: old_value + new_triple_record           |
|          |   DELETE: entity_id + attribute_id                |
| N-4      | CRC32 checksum (4 bytes)                          |
+----------+--------------------------------------------------+
```

### Circular Buffer Design

The transaction log is a **circular buffer** with:
- Fixed size (configurable, default 64MB)
- Head pointer (next write position)
- Tail pointer (oldest unneeded record)
- Checkpoint marker (recovery start point)

### Checkpointing Strategy

For **near-instant recovery**, checkpoint aggressively:

1. **Checkpoint triggers**:
   - Every N transactions (default: 1000)
   - Every M bytes written (default: 4MB)
   - On clean shutdown
   - Periodic timer (default: 30 seconds)

2. **Checkpoint process**:
   - Flush all dirty pages to disk
   - Write checkpoint record to log
   - Update superblock with checkpoint position
   - fsync

3. **Recovery process**:
   - Read superblock to get checkpoint position
   - Replay only records after checkpoint
   - Typical replay: <1000 records = <10ms

### Change Tracking for Subscriptions

The log doubles as a change feed:

```rust
// Query changes since HLC timestamp
fn changes_since(hlc: HybridLogicalClock) -> impl Iterator<Item = ChangeRecord> {
    // Binary search log for first record >= hlc
    // Iterate forward, yielding INSERT/UPDATE/DELETE records
}
```

**Retention policy**: Keep log records for at least `subscription_retention_period` (configurable, default 1 hour) even if checkpoint has advanced.

---

## MVCC Implementation

### Transaction Lifecycle

```
1. BEGIN
   - Assign transaction_id (monotonic counter)
   - Capture snapshot_txn (highest committed txn at start)
   - Record HLC timestamp

2. READ
   - Use snapshot_txn for visibility check
   - See only triples where: created_txn <= snapshot_txn < deleted_txn

3. WRITE
   - Write to transaction log immediately
   - Update in-memory indexes
   - Mark affected pages as dirty

4. COMMIT
   - Write COMMIT record to log
   - fsync log
   - Update committed_txn counter
   - Flush dirty pages (async, before next checkpoint)

5. ABORT
   - Discard dirty pages
   - No log cleanup needed (uncommitted records ignored on recovery)
```

### Garbage Collection

Old triple versions can be reclaimed when:
1. `deleted_txn < min(active_snapshot_txns)`
2. Triple is not needed for subscription retention

GC runs:
- During checkpoint (opportunistic)
- When free page count drops below threshold
- Manual trigger via API

---

## Concurrency Model

### Read-Write Concurrency

```
+-------------------------------------------------------------+
| Readers: Multiple concurrent readers allowed                 |
| - Each gets consistent snapshot                              |
| - No blocking on writers                                     |
| - Uses MVCC for isolation                                    |
+-------------------------------------------------------------+
| Writers: Single writer at a time (mutex)                     |
| - Writes to log are serialized                               |
| - But readers are not blocked                                |
| - Writer holds lock only during page modification            |
+-------------------------------------------------------------+
```

### Page-Level Locking

```rust
enum PageLock {
    Read,   // Multiple readers OK
    Write,  // Exclusive access
}

// Lock ordering: always lock pages in page_id order to prevent deadlocks
```

---

## Memory Management

### Page Cache

```rust
struct PageCache {
    cache: LruCache<PageId, Page>,
    dirty_pages: HashSet<PageId>,
    max_memory: usize,
}
```

**Default memory budget**: 64MB per database (adjustable)

For 1000s of databases:
- Cold databases: 0 cached pages (fully evicted)
- Warm databases: ~1-10MB
- Hot databases: up to configured limit

### Memory-Mapped I/O Consideration

**Chosen: Traditional read/write with page cache**
- More control over memory usage
- Better for multi-tenant with many databases
- Explicit cache management

Consider mmap for specific hot paths if benchmarks show benefit.

---

## ID Generation Strategy

### Entity ID Format (16 bytes)

```
+-------------------------------------------------------------+
| Entity ID (128 bits)                                         |
+-------------------------------------------------------------+
| Timestamp (48 bits) | Random (80 bits)                       |
| - Milliseconds      | - Cryptographically random             |
|   since epoch       |                                        |
+-------------------------------------------------------------+
```

**Benefits**:
- Roughly sorted by creation time (helps with locality)
- Still globally unique
- Compatible with UUIDv7

### Attribute ID Format (16 bytes)

```
Hash of canonical attribute name:
  attribute_id = BLAKE3(entity_type + "/" + field_name)[0..16]

Example:
  "users/name" -> 0x7a3f...
  "posts/name" -> 0x2b1c... (different!)
```

---

## Overflow Pages

### Large Value Storage

For values exceeding inline threshold (~1KB):

```
+-------------------------------------------------------------+
| Overflow Page                                                |
+-------------------------------------------------------------+
| Header (16 bytes):                                           |
|   - next_page (8 bytes): chain pointer, 0 if last           |
|   - data_length (4 bytes): bytes in this page               |
|   - flags (4 bytes): compression, etc.                      |
+-------------------------------------------------------------+
| Data (page_size - 16 bytes):                                |
|   - Raw bytes of value                                       |
+-------------------------------------------------------------+
```

**Chaining**: For very large values, pages are chained.

```rust
const INLINE_THRESHOLD: usize = 1024; // 1KB

fn store_value(value: &[u8]) -> StoredValue {
    if value.len() <= INLINE_THRESHOLD {
        StoredValue::Inline(value.to_vec())
    } else {
        let pages = allocate_overflow_pages(value);
        StoredValue::Overflow { first_page: pages[0], total_length: value.len() }
    }
}
```

---

## Code Architecture

### Module Structure

```
server/src/storage/
├── mod.rs              # Public API
├── file.rs             # File I/O, page read/write
├── page.rs             # Page types and layouts
├── btree.rs            # B-tree implementation
├── wal.rs              # Transaction log
├── mvcc.rs             # Visibility, snapshots
├── cache.rs            # Page cache
├── indexes/
│   ├── mod.rs
│   ├── primary.rs      # (entity_id, attribute_id) index
│   ├── attribute.rs    # attribute_id index
│   └── value.rs        # Value indexes (numeric, string, boolean)
├── overflow.rs         # Large value storage
├── gc.rs               # Garbage collection
└── recovery.rs         # Crash recovery
```

### Public API

```rust
pub struct Database {
    pub fn open(path: &Path, options: Options) -> Result<Self>;
    pub fn close(self) -> Result<()>;
}

pub struct Transaction<'db> {
    pub fn begin(db: &'db Database) -> Result<Self>;

    // Read operations
    pub fn get(&self, entity_id: &Id, attribute_id: &Id) -> Result<Option<Triple>>;
    pub fn scan_entity(&self, entity_id: &Id) -> Result<impl Iterator<Item = Triple>>;
    pub fn scan_attribute(&self, attribute_id: &Id) -> Result<impl Iterator<Item = Triple>>;
    pub fn query(&self, query: Query) -> Result<QueryResult>;

    // Write operations
    pub fn insert(&mut self, triple: Triple) -> Result<()>;
    pub fn update(&mut self, triple: Triple) -> Result<()>;
    pub fn delete(&mut self, entity_id: &Id, attribute_id: &Id) -> Result<()>;

    pub fn commit(self) -> Result<()>;
    pub fn abort(self);
}

pub struct ChangeStream {
    pub fn since(db: &Database, hlc: HybridLogicalClock) -> Result<Self>;
    pub fn next(&mut self) -> Option<ChangeRecord>;
}
```

---

## Implementation Phases

### Phase 1: Core Storage (Foundation)
1. File format and superblock
2. Page management (read/write/allocate/free)
3. Basic B-tree implementation
4. Primary index (entity_id, attribute_id)
5. Simple transaction (single-threaded, no MVCC)
6. Basic durability (fsync on commit)

### Phase 2: Durability & Recovery
1. Transaction log (WAL)
2. Checkpointing
3. Crash recovery
4. HLC integration

### Phase 3: MVCC & Concurrency
1. Snapshot isolation
2. Multi-reader support
3. Visibility checks
4. Garbage collection

### Phase 4: Secondary Indexes
1. Attribute index
2. Numeric value index
3. String value index (prefix)
4. Boolean bitmap index

### Phase 5: Advanced Features
1. Overflow pages for large values
2. Change stream API
3. Compression (optional)
4. Performance tuning

### Phase 6: Multi-Tenancy Optimization
1. Database pool management
2. Memory budget enforcement
3. Fast open/close
4. Cross-database resource management

---

## Tradeoffs & Alternatives

### B-tree vs LSM-tree

**Chosen: B-tree**

| Factor | B-tree | LSM-tree |
|--------|--------|----------|
| Read performance | Better | Worse (read amplification) |
| Write performance | Good | Better |
| Space amplification | Lower | Higher |
| Predictable latency | Yes | No (compaction spikes) |

**Rationale**: Read-heavy workload with small writes favors B-tree.

### WAL vs Shadow Paging

**Chosen: WAL with aggressive checkpointing**

| Factor | WAL | Shadow Paging (LMDB-style) |
|--------|-----|---------------------------|
| Write amplification | Lower | Higher (full page copy) |
| Recovery complexity | Higher | Lower (instant) |
| Space overhead | Log size | 2x for modified pages |

**Rationale**: WAL with frequent checkpoints gives near-instant recovery while avoiding shadow paging's write amplification.

### Single Writer vs Multi-Writer

**Chosen: Single writer with mutex**

- Simpler implementation
- Writes are small batches (not a bottleneck)
- Multi-writer adds complexity (lock ordering, deadlock prevention)
- Can revisit if write throughput becomes an issue

---

## Open Questions for Future

1. **Compression**: Per-page compression (LZ4/Zstd) could reduce I/O significantly
2. **Bloom filters**: For negative lookups (entity doesn't exist)
3. **Columnar storage hybrid**: For very large aggregations
4. **Parallel query execution**: For aggregations over millions of rows
5. **Tiered storage**: Hot data in faster storage, cold data compressed/archived
