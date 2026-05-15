## Why

Streaming compaction (`CompactFull`) uses significantly more memory than non-streaming compaction (`CompactFast`). Profiling shows the streaming path allocates ~104KB per block (1000 values) due to `DecodeBlock` returning `[]Value` interface slices (causing per-value heap boxing) and `valueEntry` heap merge structs, while the non-streaming path decodes directly into typed arrays with near-zero per-value overhead. This makes streaming compaction unsuitable for memory-constrained deployments and causes excessive GC pressure.

## What Changes

- Replace `DecodeBlock` (returns `[]Value` interface) with type-specific `DecodeFloatArrayBlock` / `DecodeIntegerArrayBlock` / etc. in `BlockValueIterator`, eliminating interface boxing allocations
- Replace per-value `valueEntry` heap merge in `KeyAwareMergingIterator` with batch-style key-level merge using typed arrays (similar to `tsmBatchKeyIterator.combineFloat`), eliminating ~64KB/block heap entry overhead
- Pool intermediate byte slices (`vb`/`tb`) in `Encode*ArrayBlock` functions using `sync.Pool`
- Ensure `iter.Close()` is called in the compaction path to release resources explicitly
- Add unit tests that benchmark and compare memory allocation (allocs, bytes) between streaming and non-streaming compaction paths

## Capabilities

### New Capabilities
- `typed-array-decode`: BlockValueIterator decodes blocks directly into typed arrays (tsdb.FloatArray, etc.) instead of []Value interface slices, eliminating interface boxing overhead
- `batch-key-merge`: KeyAwareMergingIterator merges values at the key level using typed arrays instead of per-value heap entries, matching the efficiency of the batch path
- `encode-buffer-pooling`: sync.Pool for intermediate byte slices in Encode*ArrayBlock functions
- `memory-comparison-test`: Unit tests that measure and compare allocs/op and bytes/op between streaming and non-streaming compaction

### Modified Capabilities
- `iterator-resource-cleanup`: Extend to ensure streaming iterator Close() is called from compaction path

## Impact

- **Code**: `tsdb/engine/tsm1/stream_iterator.go` (major refactor), `tsdb/engine/tsm1/encoding.gen.go` (buffer pooling), `tsdb/engine/tsm1/compact.go` (Close call), new test file
- **API**: Internal only — `BlockValueIterator`, `KeyAwareMergingIterator` interfaces change but are not exported
- **Dependencies**: None new — uses existing `sync.Pool` and `tsdb.*Array` types
- **Risk**: Medium — core compaction logic change requires thorough testing; existing compaction tests must continue to pass
