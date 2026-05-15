## Why

`readBlocksForKey()` collects ALL blocks for a key from ALL files before any processing begins. For keys with many blocks (e.g., high-compression string fields spanning hundreds of blocks across 3+ files), this means all block metadata and mmap references are held simultaneously. More critically, if any blocks overlap, ALL blocks are decoded — even those that don't participate in the overlap. The memory peak is O(allBlocksForKey) instead of O(overlappingBlocks).

## What Changes

- Replace the collect-all-then-process pattern in `processCurrentKey()` with an incremental approach
- Non-overlapping blocks are detected and output immediately as they are read, without waiting for all blocks to be collected
- Only overlapping blocks are buffered for decode/merge
- Introduce `maxTimeSeen` tracking to determine overlap in a single pass
- Same `needsDedup()` logic for the buffered (overlapping) subset

## Capabilities

### New Capabilities

- `incremental-block-processing`: Read blocks one at a time per file, detect overlap incrementally, output non-overlapping blocks immediately, buffer only overlapping blocks for decode/merge

### Modified Capabilities

(none — this is an internal optimization, no spec-level behavior change)

## Impact

- `tsdb/engine/tsm1/stream_iterator.go` — rewrite `processCurrentKey()` and `readBlocksForKey()`
- Memory peak drops from O(allBlocksForKey) to O(overlappingBlocks)
- No change to `KeyIterator` interface or `compact.go`
- Output blocks are identical (same merge logic, just different processing order)
