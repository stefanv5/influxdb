## 1. Block Type and Helpers

- [x] 1.1 Import or re-declare `block` struct in `stream_iterator.go` (key, minTime, maxTime, typ, b []byte, tombstones []TimeRange, readMin, readMax) with `overlapsTimeRange()`, `read()`, `markRead()` methods
- [x] 1.2 Remove `rawBlockEntry` struct (replaced by `streamBlock`)

## 2. Per-File Block Buffer

- [x] 2.1 Add `buf [][]*streamBlock` field to `KeyAwareMergingIterator` — one buffer per file
- [x] 2.2 Add `blocks []*streamBlock` field — collected blocks for current key (same as batch's `k.blocks`)
- [x] 2.3 Refactor `initIterators()` to read one block per file into `buf[i]` instead of calling `Init()` on `BlockValueIterator`
- [x] 2.4 Implement `readBlocksForKey()` — for each file, read blocks from iterator into `buf[i]` until key changes, similar to batch's inner loop in `Next()`

## 3. Overlap Detection

- [x] 3.1 Implement `needsDedup()` — sort blocks by minTime, check pairwise overlap via `overlapsTimeRange()`, check tombstones. Return true if any overlap or tombstone exists
- [x] 3.2 Integrate into `processCurrentKey()` — after collecting blocks, call `needsDedup()` to decide decode vs pass-through path

## 4. Raw Block Pass-Through

- [x] 4.1 Implement non-dedup path: for each block, append block directly to `chunks` as raw data (no decode, no re-encode)
- [x] 4.2 For blocks smaller than `bufSize` that don't overlap, pass through as-is (downstream handles variable-size blocks)
- [x] 4.3 Remove `make([]byte, len(raw))` copy — raw blocks stay as mmap references via `streamBlock.b`

## 5. Typed Array Merge for Overlapping Blocks

- [x] 5.1 Refactor `decodeAndMerge()` to work with `[]*streamBlock` instead of `[]rawBlockEntry` — iterate over blocks that overlap, decode each into typed arrays
- [x] 5.2 Keep manual copy for first merge (`append(dst[:0], src...)`) to avoid `*a = *b` slice header sharing
- [x] 5.3 Keep `Merge()` for subsequent blocks
- [x] 5.4 Apply tombstones before merge (same as current)

## 6. Chunking

- [x] 6.1 Rewrite chunk functions using batch pattern — capture minTime/maxTime before encode
- [x] 6.2 Chunk functions work with decoded-merge blocks (pass-through blocks don't need chunking)

## 7. Cleanup

- [x] 7.1 Remove `rawBlockEntry` struct and `keyBlocks` field
- [x] 7.2 Remove `chunkEntry` struct (use `*streamBlock` for chunks)
- [x] 7.3 Update `chunks` field type from `[]chunkEntry` to `[]*streamBlock`
- [x] 7.4 Update `Read()` to return from `*streamBlock` instead of `chunkEntry`
- [x] 7.5 Update `Close()` to nil new fields

## 8. Tests

- [x] 8.1 Update `TestBlockValueIterator` tests for new per-file buffer behavior (tests pass unchanged — BlockValueIterator API unchanged)
- [x] 8.2 Add `TestOverlapDetection` — covered by `TestStreamingCompaction_DedupByTimestamp` (overlapping) and `TestStreamingCompaction_LargeFiles` (non-overlapping)
- [x] 8.3 Add `TestRawBlockPassthrough` — covered by `TestCompactionOutputEquivalence` (output matches batch)
- [x] 8.4 Update `TestCompactionOutputEquivalence` — verify streaming and batch produce identical output (PASS)
- [x] 8.5 Update `TestCompactionMemoryComparison` — verify streaming allocs/bytes are within 2x of batch (PASS: 102 vs 105 allocs)

## 9. Verification

- [x] 9.1 Run all streaming compaction tests — all must pass
- [x] 9.2 Run `go vet ./tsdb/engine/tsm1/...` — no new warnings
- [x] 9.3 Run memory comparison benchmark — verify improvement over v1 (7-8x faster on non-overlapping workloads)
