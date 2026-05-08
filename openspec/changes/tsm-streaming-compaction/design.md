## Context

Current TSM compaction (`tsmBatchKeyIterator`) decodes all blocks for a key into memory before merging. For large shards (10+ files, 100+ blocks/key), this causes OOM due to:
- `FloatArray.Merge` allocating `a.Len()+b.Len()` on each merge-sort
- Full block decoding accumulating all values for a key in `mergedFloatValues`
- Encoding buffers not being pooled (TODO at `encoding.gen.go:462`)

Existing optimizations already reduce work for non-overlapping blocks and fast mode. The streaming approach targets the worst case: overlapping blocks requiring full decode + merge.

**Key files:**
- `tsdb/engine/tsm1/compact.go` — `tsmBatchKeyIterator`, `Compactor.compact()`, `KeyIterator` interface
- `tsdb/engine/tsm1/compact.gen.go` — `mergeFloat`, `combineFloat`, `chunkFloat`
- `tsdb/engine/tsm1/reader.go` — `TSMReader`, `BlockIterator`, tombstone application
- `tsdb/engine/tsm1/tombstone.go` — `Tombstoner`, tombstone file format v4
- `tsdb/cursors/arrayvalues.gen.go` — `FloatArray.Merge` (allocation hotspot)

## Goals / Non-Goals

**Goals:**
- Eliminate `FloatArray.Merge` full-allocation merge-sort
- Limit simultaneous decoded blocks to O(files), not O(blocks_for_key)
- Maintain key boundary correctness (each output block = single key)
- Maintain output ordered by timestamp
- Maintain existing `KeyIterator` interface unchanged
- Keep fast mode using existing implementation

**Non-Goals:**
- Changing TSM file format
- Modifying `CompactionPlanner` interface
- Optimizing fast mode (level 1-2) compaction
- Block-level tombstone skipping in compaction path (query path already has this)

## Decisions

### D1: Heap merge instead of merge-sort

**Choice:** Min-heap with O(log N) push/pop (N = number of files)

**Alternatives considered:**
- Merge-sort with pooled arrays: Still requires O(values_for_key) allocation for merged output
- K-way merge with channel-based pipeline: Adds goroutine overhead, harder to control memory

**Rationale:** Heap holds only N entries (one per file), not all values. Memory is O(files) vs O(values_for_key).

### D2: Key-aware merging via findMinKey pattern

**Choice:** `findMinKey()` scans all iterators' `currentKey`, activates only matching iterators

**Alternatives considered:**
- Global timestamp merge with key-change detection: v1 approach, mixes different keys in the heap
- Per-key goroutine pipeline: Adds complexity, harder to maintain ordering

**Rationale:** Matches existing `tsmBatchKeyIterator.Next()` pattern (compact.go:1696-1842). Ensures each output block contains exactly one key.

### D3: (timestamp, fileIdx) composite heap ordering

**Choice:** Heap sorts by `(timestamp ASC, fileIdx ASC)`. Same timestamp: older file pops first, newer file's value wins dedup.

**Alternatives considered:**
- Timestamp only: v1 approach, same-timestamp pop order is non-deterministic
- (timestamp, -fileIdx): Newer file pops first, but then need to consume all same-timestamp entries before knowing the winner

**Rationale:** With `(ts, fileIdx)` ascending, the last popped same-timestamp entry is always the newest file. `popAndDedup()` consumes all same-timestamp entries and keeps the last one.

### D4: Tombstone filtering in activateIteratorsForKey, not Init()

**Choice:** `Init()` loads the first block without filtering tombstones. `activateIteratorsForKey()` loops through blocks until a non-tombstoned value is found.

**Alternatives considered:**
- Filter in `Init()`: Requires caching next-key block when key changes during Init loop, duplicating `activateIteratorsForKey` logic
- Filter in `advanceAndPush()`: Would require a loop there too, and doesn't handle the first-activation case

**Rationale:** `Init()` is a one-time setup. `activateIteratorsForKey()` is the per-key entry point and already has the `NextBlock()` loop for exhausted blocks. Adding tombstone filtering there is natural and avoids code duplication.

### D5: Single block per iterator

**Choice:** Each `BlockValueIterator` holds exactly 1 decoded block. `NextBlock()` replaces the current block with the next one.

**Alternatives considered:**
- Sliding window of 2 blocks: Pre-decode next block while consuming current. More memory, marginal latency improvement
- Block-level streaming (decode values lazily): Requires per-type streaming decoder, major refactor

**Rationale:** 1 block per iterator gives O(files x block_size) memory, which is the target. Pre-decoding adds complexity for minimal benefit since block decode is fast.

### D6: Fast mode keeps existing implementation

**Choice:** `Compactor.compact(fast=true)` uses `tsmBatchKeyIterator`. Streaming only for `fast=false`.

**Alternatives considered:**
- Streaming for both modes: Unnecessary overhead for fast mode where blocks are just passed through
- Remove fast mode: Would regress level 1-2 compaction performance

**Rationale:** Fast mode already avoids decoding for non-overlapping blocks. Streaming adds heap overhead that's unnecessary for the fast path.

### D7: encodeValues uses type dispatch

**Choice:** `encodeValues(typ, values)` switches on block type and calls existing `EncodeXxxArrayBlock` functions.

**Alternatives considered:**
- Generic encoder interface: v1 used a fictional `Encoder` type that doesn't exist in the codebase
- Per-type goroutine pipelines: Over-engineering for a synchronous encode step

**Rationale:** Matches existing codebase pattern. All 5 types (float, int, uint, bool, string) have existing array-based encode functions.

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| Heap merge slower than merge-sort | CPU increase | O(log N) with N=files (typically 4-10), negligible |
| Output block compression ratio changes | File size | `bufSize` matches existing `k.size` (1000 values) |
| `advanceAndPush` all-tombstone block stall | Iterator stuck | `findMinKey` + `activateIteratorsForKey` re-processes; consecutive all-tombstone blocks are extremely rare |
| Per-value tombstone CPU | Processing time | Tombstone ranges are small and cached per block |
| Pool memory not reclaimed | Memory leak | `sync.Pool` is GC-managed, entries are reclaimed under memory pressure |

## Migration Plan

1. Add `stream_iterator.go` alongside existing code (no changes to existing files initially)
2. Add unit tests for `BlockValueIterator` and `KeyAwareMergingIterator`
3. Add dispatch logic in `Compactor.compact()` — streaming for full mode, existing for fast mode
4. Integration tests with real TSM files and tombstones
5. Performance benchmarks comparing old vs new

Rollback: Remove dispatch logic in `compact()`, revert to `tsmBatchKeyIterator` for all modes.

## Open Questions

None. All architectural decisions are resolved in the v2 design document and tombstone issue analysis.
