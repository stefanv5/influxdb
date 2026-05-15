## 1. Encode Buffer Pooling

- [x] 1.1 Add `sync.Pool` for encode intermediate byte slices in `encoding.gen.go` (replace `var vb []byte` / `var tb []byte` with pool Get/Put in all 5 `Encode*ArrayBlock` functions)

## 2. Typed Array Decode in BlockValueIterator

- [x] 2.1 Replace `decoded []Value` field in `BlockValueIterator` with typed array fields (`floatArr tsdb.FloatArray`, `integerArr tsdb.IntegerArray`, etc.) plus `valueType byte` and `pos int`
- [x] 2.2 Refactor `Init()` to dispatch to `Decode*ArrayBlock` based on block type, storing result in the appropriate typed array
- [x] 2.3 Refactor `NextBlock()` to decode into typed arrays with reuse
- [x] 2.4 Refactor `ActivatePending()` to decode into typed arrays with reuse
- [x] 2.5 Refactor `Next()` to advance within the active typed array, skipping tombstoned indices
- [x] 2.6 Refactor `Read()` to construct `Value` from the active typed array at current position
- [x] 2.7 Update `Close()` to nil all typed array fields

## 3. Batch-Style Key-Level Merge

- [x] 3.1 Replace `heap valueHeap` and `buf []Value` in `KeyAwareMergingIterator` with typed merge arrays (`mergedFloat tsdb.FloatArray`, etc.) and per-reader block buffer `mergeBuf []blocks`
- [x] 3.2 Implement `mergeFloat()` — collect blocks for current key, decode overlaps, merge sorted arrays, dedup by timestamp (higher fileIdx wins)
- [x] 3.3 Implement `mergeInteger()`, `mergeUnsigned()`, `mergeBoolean()`, `mergeString()` following the same pattern
- [x] 3.4 Refactor `Next()` to use batch merge instead of heap pop — iterate through merged blocks
- [x] 3.5 Refactor `Read()` to return from merged blocks directly
- [x] 3.6 Remove `valueEntry`, `valueHeap`, `advanceAndPush`, `popAndDedup`, `activateIteratorForBlock` and related heap code
- [x] 3.7 Update `Close()` to nil all merge state

## 4. Resource Cleanup

- [x] 4.1 Add `defer tsm.Close()` in `compact()` after creating the streaming iterator
- [x] 4.2 Ensure `Close()` is idempotent (check existing implementation)

## 5. Memory Comparison Tests

- [x] 5.1 Create test helper that generates TSM files with configurable key count, value count, and block type distribution
- [x] 5.2 Write benchmark `BenchmarkStreamingCompaction` measuring allocs/op and bytes/op
- [x] 5.3 Write benchmark `BenchmarkBatchCompaction` measuring allocs/op and bytes/op with same input data
- [x] 5.4 Write test `TestCompactionMemoryComparison` asserting streaming allocs/bytes are within 2x of batch
- [x] 5.5 Write test `TestCompactionOutputEquivalence` asserting streaming and batch produce identical output blocks

## 6. Verification

- [x] 6.1 Run existing compaction tests (`TestStreamingCompaction*`, `TestCompactor*`) — all must pass
- [x] 6.2 Run `go vet ./tsdb/engine/tsm1/...` — no new warnings
- [x] 6.3 Run new benchmark and comparison tests — verify memory improvement
