## Why

The current streaming compaction (`KeyAwareMergingIterator`) has devolved into a "collect-all-blocks-then-process" pattern that is worse than the original batch approach (`tsmBatchKeyIterator`). It copies every raw block (`make+copy`), decodes ALL blocks unconditionally, and re-encodes everything — losing the batch optimization that passes non-overlapping blocks through as raw data. The result is higher memory usage and more allocations than the batch path it was meant to improve upon.

## What Changes

- Replace the current `processCurrentKey()` collect-all-decode-all pattern with an overlap-aware merge strategy modeled on `tsmBatchKeyIterator.combineFloat()`
- Non-overlapping blocks pass through as raw mmap slices (zero decode, zero re-encode, zero copy)
- Only overlapping blocks are decoded into typed arrays and merged
- Eliminate `make([]byte, len(raw))` copies — raw blocks stay as mmap references
- Per-file block buffer (`buf []blocks`) instead of per-key collection of all blocks
- Maintain typed array merge (no per-value interface boxing) for overlapping blocks

## Capabilities

### New Capabilities

- `overlap-aware-merge`: Detect time-range overlaps across files and selectively decode only overlapping blocks, passing non-overlapping blocks through as raw data

### Modified Capabilities

(none — this is an implementation refactor, no spec-level behavior change)

## Impact

- `tsdb/engine/tsm1/stream_iterator.go` — major rewrite of `KeyAwareMergingIterator` internals
- `tsdb/engine/tsm1/stream_iterator_test.go` — update tests for new merge behavior
- `tsdb/engine/tsm1/compact.go` — no changes expected (same `KeyIterator` interface)
- Memory: peak usage drops from O(allBlocksForKey × blockSize) to O(numFiles × blockSize)
- Allocations: eliminates per-block `make+copy` for all blocks, eliminates decode/encode for non-overlapping blocks
