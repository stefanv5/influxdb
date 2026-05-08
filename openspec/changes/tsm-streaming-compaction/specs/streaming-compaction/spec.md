## ADDED Requirements

### Requirement: BlockValueIterator holds at most one decoded block
The `BlockValueIterator` SHALL hold at most one decoded block in memory at any time. When the current block is exhausted, the iterator SHALL return false from `Next()` without automatically advancing to the next block. The upper layer (`KeyAwareMergingIterator`) SHALL control block advancement via `NextBlock()`.

#### Scenario: Single block decoding
- **WHEN** `Init()` is called on a `BlockValueIterator`
- **THEN** exactly one block is decoded and the iterator's memory usage is O(block_size)

#### Scenario: Block exhaustion does not auto-advance
- **WHEN** all values in the current block have been consumed via `Next()`
- **THEN** `Next()` returns false and no new block is decoded until `NextBlock()` is called

### Requirement: KeyAwareMergingIterator enforces key boundaries
The `KeyAwareMergingIterator` SHALL ensure each output block contains values from exactly one key. It SHALL find the lexicographically smallest key across all iterators, activate only iterators whose `currentKey` matches that key, merge their values via a heap, and only move to the next key when the current key is fully consumed.

#### Scenario: Different key sets across files
- **WHEN** File 0 has keys [cpu, mem], File 1 has keys [cpu, disk], File 2 has keys [net, disk]
- **THEN** output blocks are ordered: cpu (from files 0,1), disk (from files 1,2), mem (from file 0), net (from file 2)

#### Scenario: Key boundary isolation
- **WHEN** processing key "cpu" and File 0's "cpu" block is exhausted, revealing key "mem" in the next block
- **THEN** the "mem" block is cached (not consumed) and processing continues with other iterators' "cpu" values

### Requirement: Heap merge uses (timestamp, fileIdx) composite ordering
The value heap SHALL sort entries by `(timestamp, fileIdx)` ascending. For entries with the same timestamp, the entry with the smaller `fileIdx` (older file) SHALL be popped first, so that the newer file's value naturally becomes the dedup winner.

#### Scenario: Same timestamp dedup
- **WHEN** File 0 and File 1 both have a value at timestamp=200
- **THEN** File 0's entry is popped first, and when File 1's entry is popped, it overwrites File 0's value as the dedup winner

#### Scenario: Ordered output guarantee
- **WHEN** values are popped from the heap and collected into output buffers
- **THEN** values in each output block are strictly ordered by timestamp ascending

### Requirement: popAndDedup consumes all same-timestamp entries
The `popAndDedup()` method SHALL pop the top entry, then consume all subsequent entries with the same timestamp, keeping the value from the highest `fileIdx`. This prevents the v1 bug where the last same-timestamp value is lost when the heap empties.

#### Scenario: Three entries with same timestamp
- **WHEN** heap contains entries (ts=200, file=0), (ts=200, file=1), (ts=200, file=2)
- **THEN** all three are consumed and the output value is from file=2

#### Scenario: Heap empties after consuming same-timestamp entries
- **WHEN** the last three entries in the heap all have timestamp=200
- **THEN** all three are consumed correctly and the output value is from the highest file index

### Requirement: Tombstone per-value filtering
The `BlockValueIterator` SHALL filter tombstoned values at the `Next()` level by checking each value's timestamp against the iterator's loaded tombstone ranges. Tombstoned values SHALL NOT enter the heap or output buffer.

#### Scenario: Partial block tombstone
- **WHEN** a block contains values at ts=[100, 200, 300] and tombstone covers [150, 250]
- **THEN** only values at ts=100 and ts=300 are yielded

#### Scenario: Full block tombstone
- **WHEN** a block's entire time range is covered by a tombstone
- **THEN** `Next()` returns false for that block (all values filtered)

### Requirement: Init() tombstone boundary handled in activateIteratorsForKey
When `Init()` reads a first block where all values are tombstoned, the iterator SHALL NOT get stuck. The `activateIteratorsForKey()` method SHALL loop through blocks (calling `NextBlock()`) until a non-tombstoned value is found or the key changes.

#### Scenario: First block all tombstoned
- **WHEN** Block A (ts=100, tombstoned) and Block B (ts=200, valid) exist for key "cpu"
- **THEN** `activateIteratorsForKey` skips Block A and pushes Block B's value into the heap

#### Scenario: Multiple consecutive all-tombstone blocks
- **WHEN** Blocks A, B, C are all tombstoned and Block D has valid values, all for the same key
- **THEN** `activateIteratorsForKey` skips A, B, C and pushes D's first valid value

#### Scenario: All blocks for a key are tombstoned
- **WHEN** every block for key "cpu" is fully tombstoned across all files
- **THEN** no values are pushed to the heap for "cpu" and processing moves to the next key

### Requirement: Tombstone consumed during compaction output
The compaction output file SHALL NOT contain tombstone files. Tombstones are "consumed" during compaction: values are filtered at the iterator level, and the output TSM file contains only non-deleted data.

#### Scenario: Compaction with tombstones
- **WHEN** a compaction runs on files that have associated .tombstone files
- **THEN** the output TSM file contains only non-tombstoned values and no .tombstone file is created for it

### Requirement: Fast mode unchanged
The existing `tsmBatchKeyIterator` SHALL continue to be used for fast compaction (level 1-2). The streaming iterator SHALL only be used for full compaction.

#### Scenario: Fast compaction dispatch
- **WHEN** `Compactor.compact(fast=true, ...)` is called
- **THEN** `tsmBatchKeyIterator` is used (existing behavior)

#### Scenario: Full compaction dispatch
- **WHEN** `Compactor.compact(fast=false, ...)` is called
- **THEN** `KeyAwareMergingIterator` (streaming) is used

### Requirement: Output block size control
The streaming iterator SHALL respect `DefaultMaxPointsPerBlock` (1000) for output block sizing, matching the existing `k.size` chunking behavior.

#### Scenario: Buffer fills at size limit
- **WHEN** 1000 values have been collected in the output buffer
- **THEN** `Next()` returns true and `Read()` emits a block of up to 1000 values

### Requirement: Memory usage is constant per key
The streaming implementation SHALL use O(files x block_size) memory regardless of how many blocks a key spans across files.

#### Scenario: Large shard memory profile
- **WHEN** 10 files each have 100 blocks for the same key, each block has 1000 values
- **THEN** peak memory is approximately 551 KB (10 iterators x 50 KB per decoded block + 1 KB heap + 50 KB output buffer), not 100+ MB

### Requirement: Value pooling reduces GC pressure
`valueEntry` objects SHALL be pooled via `sync.Pool` and reused across heap operations.

#### Scenario: Heap entry reuse
- **WHEN** a `valueEntry` is popped from the heap and the iterator advances
- **THEN** the entry is returned to the pool and reused for the next push

### Requirement: Encoding buffer pooling
Intermediate encoding buffers used by `encodeValues()` SHALL be pooled via `sync.Pool`.

#### Scenario: Encode buffer reuse
- **WHEN** `encodeValues()` encodes a block
- **THEN** the intermediate byte buffer is obtained from pool and returned after use
