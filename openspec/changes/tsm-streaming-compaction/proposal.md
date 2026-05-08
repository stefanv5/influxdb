## Why

Current `tsmBatchKeyIterator` compaction causes OOM in large shard scenarios. It decodes all blocks for a key into memory before merging via `FloatArray.Merge`, which allocates `a.Len()+b.Len()` on every merge. For 10 files x 100 blocks/key x 1000 values, peak memory reaches ~100+ MB per key. The streaming approach reduces this to ~551 KB (constant, independent of blocks-per-key).

## What Changes

- Replace full-allocation merge-sort with heap-based streaming merge for full compaction mode
- Introduce `BlockValueIterator` that holds only 1 decoded block per TSM reader at a time
- Introduce `KeyAwareMergingIterator` that enforces key boundaries during heap merge (each output block contains values from exactly one key)
- Implement per-value tombstone filtering during iteration (instead of post-decode `Exclude()`)
- Keep existing `tsmBatchKeyIterator` for fast mode (level 1-2 compaction) unchanged
- Pool `valueEntry` objects and encoding buffers to reduce GC pressure

## Capabilities

### New Capabilities

- `streaming-compaction`: Heap-based streaming merge for TSM compaction with key-aware boundaries, per-value tombstone filtering, and (timestamp, fileIdx) dedup ordering

### Modified Capabilities

(none - this is a new implementation alongside the existing one; no existing spec-level requirements change)

## Impact

- **New file**: `tsdb/engine/tsm1/stream_iterator.go` — `BlockValueIterator`, `KeyAwareMergingIterator`, `valueHeap`, `encodeValues`
- **Modified file**: `tsdb/engine/tsm1/compact.go` — `Compactor.compact()` dispatches to streaming iterator for full mode
- **No breaking changes**: TSM file format unchanged, `KeyIterator` interface unchanged, `CompactionPlanner` interface unchanged
- **Performance**: O(files x block_size) memory vs O(blocks_for_key x block_size), eliminates `FloatArray.Merge` allocation hotspot
- **Compatibility**: Fast mode uses existing implementation; streaming only activates for full compaction
