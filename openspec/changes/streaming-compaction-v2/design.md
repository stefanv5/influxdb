## Context

InfluxDB TSM compaction merges multiple TSM files into fewer, larger files. Two approaches exist:

- **Batch** (`tsmBatchKeyIterator`): Reads one block per file at a time, detects time-range overlaps, passes non-overlapping blocks through as raw data, only decodes overlapping groups. Zero copies of raw data (mmap slices).
- **Streaming** (`KeyAwareMergingIterator`): Current implementation collects ALL blocks for a key from ALL files, copies each raw block (`make+copy`), decodes ALL blocks, merges ALL, re-encodes ALL. Worse than batch in every metric.

The streaming approach was introduced to replace a per-value heap merge that had ~40B/value interface boxing overhead. The typed array optimization (using `tsdb.FloatArray` etc.) successfully eliminated boxing, but the subsequent refactoring to "collect-all-then-process" introduced unnecessary memory overhead.

## Goals / Non-Goals

**Goals:**
- Streaming compaction MUST use no more memory than the batch approach for the same input
- Non-overlapping blocks MUST pass through without decode/re-encode
- Raw block data MUST NOT be copied (use mmap slices directly)
- Overlapping blocks MUST still be decoded via typed arrays (preserve boxing elimination)
- Maintain the `KeyIterator` interface contract

**Non-Goals:**
- Changing the batch iterator (`tsmBatchKeyIterator`)
- Modifying the `KeyIterator` interface
- Changing compaction scheduling or file selection logic
- Supporting partial block reads (the `readMin/readMax` tracking from batch) — full blocks only in streaming path

## Decisions

### Decision 1: Per-file block buffer instead of per-key collection

**Choice**: Each `BlockValueIterator` maintains a small buffer of unread blocks (`[]*block`). The merging iterator reads one block at a time from each file, finds the minKey, and processes matching blocks.

**Why**: The current approach collects all blocks for a key across all files into `keyBlocks`. If a key has 100 blocks across 3 files, all 100 are in memory simultaneously. The batch approach buffers only 1 block per file (3 total), processing them incrementally.

**Alternative considered**: Keep collecting all blocks per key but eliminate copies. Rejected because it still requires O(allBlocksForKey) memory peak.

### Decision 2: Overlap detection before decode

**Choice**: After collecting matching blocks for a key, sort by minTime and check pairwise time-range overlap. If no overlaps and no tombstones, pass all blocks through as raw data. If overlaps exist, decode only the overlapping group into typed arrays.

**Why**: In typical workloads, most blocks across files do NOT overlap (they cover different time ranges). Decoding them is wasted work. The batch iterator already proves this optimization is correct and safe.

**Alternative considered**: Always decode all blocks (current approach). Rejected because it wastes CPU and memory on the common non-overlapping case.

### Decision 3: Reuse existing `block` struct from compact.go

**Choice**: Use the existing `type block struct` (key, minTime, maxTime, typ, b, tombstones) already defined in `compact.go`. The `b` field holds the raw mmap slice — no copy needed.

**Why**: The batch iterator already uses this struct. It has all necessary fields including `overlapsTimeRange()` and tombstone tracking. No need to invent a new type.

**Alternative considered**: Keep `rawBlockEntry` and `chunkEntry` types. Rejected because they duplicate what `block` already provides.

### Decision 4: Chunking uses captured min/maxTime before encode

**Choice**: When chunking merged typed arrays into output blocks, capture `minTime`/`maxTime` from the array BEFORE calling `Encode*ArrayBlock`, because `TimeArrayEncodeAll` mutates timestamps in-place (converts to deltas).

**Why**: This was a hard-won bug fix from v1. The `TimeArrayEncodeAll` function at `batch_timestamp.go:34` reinterprets the `[]int64` as `[]uint64` and overwrites with deltas. Reading `enc.Timestamps[sz-1]` after encode gives the last delta (1), not the original timestamp (100).

### Decision 5: Merged array uses manual copy for first merge (not `*a = *b`)

**Choice**: When merging the first decoded block into the merged typed array, use manual `append` to copy timestamps and values. For subsequent blocks, use `tsdb.*Array.Merge()`.

**Why**: `FloatArray.Merge()` when `a.Len() == 0` does `*a = *b` which copies slice headers, NOT backing arrays. If `b` is reused (e.g., local variable in a loop), the next decode overwrites the shared backing array. This was another hard-won bug fix from v1.

## Risks / Trade-offs

**[Risk] Partial block reads not supported** → The batch iterator supports reading a subset of a block's time range (`readMin/readMax`). The streaming v2 does NOT support this — it always processes full blocks. This means if a block partially overlaps, the entire block is decoded. Mitigation: Full-block decode is still cheaper than the current approach of decoding ALL blocks.

**[Risk] Overlap detection adds sorting overhead** → Blocks must be sorted by minTime before overlap checking. Mitigation: Sorting is O(n log n) where n = number of blocks for a key (typically 2-4), negligible compared to decode cost.

**[Risk] Reusing mmap slices means TSMReader must stay open** → Raw blocks reference mmap'd file data. The TSMReader must not be closed until all blocks for that reader are consumed. Mitigation: Already guaranteed by `defer tsm.Close()` in `compact()` after the iteration loop.

**[Trade-off] No partial block read → more decode work for edge cases** → If a block at a time boundary partially overlaps, the entire block is decoded instead of just the overlapping portion. This is slightly more work than batch's partial read, but still far better than the current "decode everything" approach.
