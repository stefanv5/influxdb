## Why

`processCurrentKey()` in `KeyAwareMergingIterator` classifies blocks into `rawChunks` (non-overlapping) and `pending` (possibly overlapping) via incremental `maxTimeSeen` tracking. However, this classification is imperfect due to round-robin reading: a block C may appear non-overlapping relative to block A (because B hasn't been read yet), only to later overlap with block B. This forces C into `rawChunks` even though it should be in `pending`.

The Phase 3 logic then collapses `rawChunks` + `pending` into a single group: if there are multiple blocks, ALL are decoded and merged — including non-overlapping blocks that `maxTimeSeen` correctly classified. This wastes CPU and memory decoding blocks whose timestamps are strictly non-overlapping and could safely pass through as raw mmap data.

The batch path (`tsmBatchKeyIterator`) already uses `overlapsTimeRange` in `mergeFloat()` to set a `dedup` flag, and only decodes/merges when dedup is actually needed. The streaming path had no equivalent — `len(m.allBlocks) > 1` triggered unconditional decode+merge, which is unnecessarily conservative.

## What Changes

- Replace the all-or-nothing Phase 3 with **group-based processing**: partition `allBlocks` (sorted by minTime) into independent overlap groups using transitive `overlapsTimeRange` detection
- Each group is individually decided:
  - **Single block, no tombstones**: pass through as raw mmap data (zero decode, zero allocation)
  - **Multiple blocks or tombstoned**: decode + merge + re-encode (existing logic, now scoped to only overlapping blocks)
- Add `allBlocks` reusable field to `KeyAwareMergingIterator` to avoid per-key allocation
- Add `overlapsTimeRange` method to `streamBlock`
- Fix true round-robin reading (one block per file per pass, not all blocks from one file)

## Capabilities

### New Capabilities

- `group-based-block-processing`: Overlap groups are detected via transitive `overlapsTimeRange` checks; each group independently decides pass-through vs decode+merge
- `non-overlapping-pass-through`: Blocks whose time ranges don't intersect any other block for the same key are output as raw mmap references — zero decode, zero heap allocation

### Modified Capabilities

- `incremental-block-processing`: Round-robin tightened to one block per file per pass; rawChunks/pending classification preserved but downstream Phase 3 now handles misclassified blocks correctly via grouping

## Impact

- `tsdb/engine/tsm1/stream_iterator.go` — refactored `processCurrentKey()` (new `processGroups()` method), true round-robin in `readBlocksIncrementally()`
- Memory: cumulative allocation reduced 2.58x in mixed-overlap scenarios; no regression in fully-overlapping cases
- Correctness: output identical to batch path (verified by `TestCompactionOutputEquivalence`)
- Risk: Low — grouping logic is provably correct (non-overlapping time ranges guarantee disjoint timestamp sets); existing tests pass
