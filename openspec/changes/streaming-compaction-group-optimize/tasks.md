## 1. Data Structures

- [x] 1.1 Add `overlapsTimeRange(min, max int64) bool` method to `streamBlock`
- [x] 1.2 Add `allBlocks streamBlocks` reusable field to `KeyAwareMergingIterator`
- [x] 1.3 Reorder `streamBlock` methods and remove unused `blocks` field

## 2. True Round-Robin Reading

- [x] 2.1 Fix inner loop in `readBlocksIncrementally()` to read exactly one block per file per pass
- [x] 2.2 Fix key comparison in buffer drain loop (use `bytes.Equal` instead of unchecked access)
- [x] 2.3 Handle exhausted iterator and stale nextKey correctly after single-block reads

## 3. Group-Based Processing

- [x] 3.1 Replace Phase 3 with `processGroups()` method
- [x] 3.2 Implement transitive overlap detection — partition allBlocks by overlap chains
- [x] 3.3 Singleton groups with no tombstones → pass through as raw mmap data
- [x] 3.4 Multi-block or tombstoned groups → decodeAndMerge() + chunkMergedArray()
- [x] 3.5 Sort final chunks by minTime (pass-through and merged groups may be interleaved)
- [x] 3.6 Return streamBlocks to pool after each group is processed

## 4. Phase 2 Fast Path

- [x] 4.1 Tighten to single block with no tombstones (was checking rawChunksHaveTombstones only)
- [x] 4.2 Multi-block rawChunks with no pending now correctly enters processGroups

## 5. Correctness Verification

- [x] 5.1 Run `TestCompactionOutputEquivalence` — streaming output == batch output
- [x] 5.2 Run all existing unit tests — `TestKeyAwareMergingIterator*`, `TestStreamingCompaction*`
- [x] 5.3 Run `TestCompaction_100KKeys_StreamingVsBatch` — pass within timeout
- [x] 5.4 Run `TestCompaction_PartialOverlap_StreamingVsBatch` — pass, verify memory reduction

## 6. Documentation

- [x] 6.1 Write `findings.md` with correctness proof and performance analysis
- [x] 6.2 Create openspec change (`streaming-compaction-group-optimize`)
- [ ] 6.3 Commit and push to remote
