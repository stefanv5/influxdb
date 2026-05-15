## 1. Data Structures

- [ ] 1.1 Add `pending []*streamBlock` field to `KeyAwareMergingIterator` for buffering potentially-overlapping blocks
- [ ] 1.2 Keep existing `chunks []*streamBlock` field for committed non-overlapping output

## 2. Incremental Read Loop

- [ ] 2.1 Replace `readBlocksForKey()` with `readBlocksIncrementally()` that reads one block at a time round-robin across files
- [ ] 2.2 Track `maxTimeSeen int64` across all blocks read for the current key
- [ ] 2.3 For each block read: if `block.minTime > maxTimeSeen`, append to `chunks` (non-overlapping); else append to `pending` (possibly overlapping)
- [ ] 2.4 Update `maxTimeSeen = max(maxTimeSeen, block.maxTime)` after each block
- [ ] 2.5 Continue reading from each file until all files report key change or exhaustion

## 3. Pending Evaluation

- [ ] 3.1 After all blocks read: if `pending` is empty, done (all blocks already in `chunks`)
- [ ] 3.2 If `pending` non-empty: sort by minTime, run `needsDedup()` on pending subset
- [ ] 3.3 If pending has no internal overlap: append pending blocks to `chunks` in time order
- [ ] 3.4 If pending has overlap: `decodeAndMerge()` pending blocks, `chunkMergedArray()`, prepend decoded chunks before non-overlapping ones

## 4. Output Ordering

- [ ] 4.1 Ensure final `chunks` is sorted by minTime (non-overlapping prefix + decoded/merged suffix)

## 5. Cleanup

- [ ] 5.1 Remove old `readBlocksForKey()` method
- [ ] 5.2 Remove `m.blocks` field (replaced by `chunks` + `pending`)

## 6. Tests

- [ ] 6.1 Run existing tests — all must pass (output equivalence guaranteed)
- [ ] 6.2 Add `TestIncrementalNonOverlap` — verify non-overlapping blocks are output without buffering all blocks
- [ ] 6.3 Run memory comparison benchmark — verify improvement for non-overlapping keys
- [ ] 6.4 Run `go vet ./tsdb/engine/tsm1/...` — no new warnings
