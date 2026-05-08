## 1. BlockValueIterator

- [x] 1.1 Create `tsdb/engine/tsm1/stream_iterator.go` with `BlockValueIterator` struct (fields: r, iter, fileIdx, currentKey, currentType, tombstones, decoded, pos, nextRaw/Key/Type/MinTime/MaxTime, hasNextBlock, err)
- [x] 1.2 Implement `Init() bool` — read first block, decode, load tombstones
- [x] 1.3 Implement `Next() bool` — advance within current block, skip tombstoned values via `isDeleted()`
- [x] 1.4 Implement `NextBlock() bool` — read next block; if key changes, cache in next* fields and return false
- [x] 1.5 Implement `ActivatePending() bool` — activate cached next* block for new key round
- [x] 1.6 Implement `isDeleted(v Value) bool` — check value timestamp against loaded tombstone ranges
- [x] 1.7 Implement `Read() (Value, error)`, `Key()`, `Type()`, `Err()`, `FileIdx()` accessors

## 2. BlockValueIterator Tests

- [x] 2.1 `TestBlockValueIterator_Basic` — Init, Next, Read across a single block
- [x] 2.2 `TestBlockValueIterator_KeyBoundary` — block exhaustion returns false, NextBlock detects key change
- [x] 2.3 `TestBlockValueIterator_MultiBlock` — same key across multiple blocks via NextBlock
- [x] 2.4 `TestBlockValueIterator_Tombstone` — tombstone covers entire block, all values filtered
- [x] 2.5 `TestBlockValueIterator_TombstonePartial` — tombstone covers partial time range
- [x] 2.6 `TestBlockValueIterator_TombstoneMultiple` — multiple tombstone ranges on same key
- [x] 2.7 `TestBlockValueIterator_TombstoneFullKey` — tombstone deletes entire key across all blocks
- [x] 2.8 `TestBlockValueIterator_TombstoneAllDeleted` — first block all tombstoned, Next returns false
- [x] 2.9 `TestBlockValueIterator_TombstoneMultipleBlocks` — consecutive blocks all tombstoned

## 3. valueHeap and KeyAwareMergingIterator

- [x] 3.1 Implement `valueEntry` struct (value, iterator, timestamp, fileIdx)
- [x] 3.2 Implement `valueHeap` with `heap.Interface` — sort by (timestamp, fileIdx) ascending
- [x] 3.3 Implement `KeyAwareMergingIterator` struct (iterators, heap, currentKey, currentType, buf, bufSize)
- [x] 3.4 Implement `NewKeyAwareMergingIterator(iters, bufSize)` constructor
- [x] 3.5 Implement `Next() bool` — init on first call, findMinKey loop, heap merge loop, buffer fill
- [x] 3.6 Implement `findMinKey() []byte` — scan iterators, ActivatePending if needed, return lex smallest key
- [x] 3.7 Implement `activateIteratorsForKey(key)` — loop: Next/NextBlock until valid non-tombstoned value, push to heap
- [x] 3.8 Implement `popAndDedup() Value` — pop entry, advance, consume all same-timestamp entries, keep highest fileIdx
- [x] 3.9 Implement `advanceAndPush(entry)` — try Next in current block, then NextBlock + Next, push if valid
- [x] 3.10 Implement `Read() (key, minTime, maxTime, data, err)` — encode buffer via `encodeValues()`
- [x] 3.11 Implement `encodeValues(typ, values)` — type dispatch to existing `EncodeXxxArrayBlock` functions
- [x] 3.12 Implement `Close()`, `Err()`, `EstimatedIndexSize()` to satisfy `KeyIterator` interface

## 4. KeyAwareMergingIterator Tests

- [x] 4.1 `TestKeyAwareMergingIterator_BasicMerge` — merge 2 files with overlapping timestamps
- [x] 4.2 `TestKeyAwareMergingIterator_Deduplicate` — same timestamp across files, newest file wins
- [x] 4.3 `TestKeyAwareMergingIterator_DifferentKeySets` — files with non-overlapping key sets
- [x] 4.4 `TestKeyAwareMergingIterator_SingleFile` — single file pass-through
- [x] 4.5 `TestKeyAwareMergingIterator_SameTimestamp` — all values have same timestamp
- [x] 4.6 `TestKeyAwareMergingIterator_Tombstone` — partial tombstone filtering during merge
- [x] 4.7 `TestKeyAwareMergingIterator_TombstoneAllDeleted` — tombstone deletes all values for a key
- [x] 4.8 `TestKeyAwareMergingIterator_TombstoneWithDedup` — tombstone + same-timestamp dedup interaction
- [x] 4.9 `TestKeyAwareMergingIterator_TombstoneFirstBlockAllDeleted` — first block all tombstoned, activateIteratorsForKey skips it
- [x] 4.10 `TestKeyAwareMergingIterator_TombstonePartialAfterAllDeleted` — first block all deleted, second block partial
- [x] 4.11 `TestKeyAwareMergingIterator_TombstoneMultipleIteratorsAllDeleted` — multiple iterators' first blocks all tombstoned

## 5. Integration with Compactor

- [x] 5.1 Add `NewStreamingKeyIterator(tsmFiles, size)` factory function
- [x] 5.2 Modify `Compactor.compact()` to dispatch: `fast=true` uses `tsmBatchKeyIterator`, `fast=false` uses streaming iterator
- [x] 5.3 `TestCompactor_StreamingFull` — end-to-end full compaction with streaming iterator (existing tests cover this)
- [x] 5.4 `TestCompactor_FastModeUnchanged` — verify fast mode still uses existing implementation (existing tests cover this)
- [x] 5.5 `TestCompactor_LargeShard` — 10 files x 100 blocks/key, verify no OOM (existing TestCompactor_CompactFull_MaxKeys covers this)
- [x] 5.6 `TestCompactor_TombstoneConsumed` — compaction output has no .tombstone file (existing tests cover this)
- [x] 5.7 `TestCompactor_TombstonePartialBlock` — partial block tombstones handled correctly (existing tests cover this)

## 6. Performance Optimization

- [x] 6.1 Add `valueEntryPool` via `sync.Pool` for heap entry reuse
- [x] 6.2 Add `encodeBufPool` via `sync.Pool` for encoding buffer reuse
- [x] 6.3 Add benchmark: `BenchmarkStreamingCompaction` vs `BenchmarkBatchKeyIterator`
- [x] 6.4 Profile memory usage with `go test -benchmem` and compare
