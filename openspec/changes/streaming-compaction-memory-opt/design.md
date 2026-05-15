## Context

The streaming compaction path (`CompactFull` → `KeyAwareMergingIterator`) was introduced to process TSM files incrementally, holding only one decoded block per reader in memory at a time. However, the implementation uses `DecodeBlock` which returns `[]Value` interface slices, causing every concrete value to be heap-boxed (~40B per value). Additionally, a per-value `valueEntry` heap is used for merge/dedup, adding ~64B per value. The non-streaming path (`CompactFast` → `tsmBatchKeyIterator`) avoids both issues by decoding directly into typed arrays (`tsdb.FloatArray`, etc.) and merging at the block level.

Current memory cost per block (1000 values, 3 readers):
- Streaming: ~104KB (interface boxing + heap entries + key copies)
- Non-streaming: ~negligible per-value overhead (typed arrays, block-level merge)

## Goals / Non-Goals

**Goals:**
- Reduce streaming compaction per-block memory to be comparable to non-streaming path
- Eliminate interface boxing in the decode path
- Eliminate per-value heap allocations in the merge path
- Pool encode intermediate buffers
- Ensure proper resource cleanup via Close()
- Provide benchmark tests comparing streaming vs non-streaming memory consumption

**Non-Goals:**
- Changing the non-streaming (batch) compaction path
- Changing the public API of TSMReader or FileStore
- Implementing full sync.Pool for all internal buffers (only encode intermediates for now)
- Optimizing for concurrent compaction sharing (future work)

## Decisions

### Decision 1: Typed array decode in BlockValueIterator

**Choice:** Replace `DecodeBlock(buf, it.decoded[:0])` with type-dispatched decode using `DecodeFloatArrayBlock`, `DecodeIntegerArrayBlock`, etc. Store results in `tsdb.FloatArray` / `tsdb.IntegerArray` / etc. fields on `BlockValueIterator`.

**Rationale:** `DecodeFloatArrayBlock` decodes directly into `[]float64` + `[]int64` slices with zero interface boxing. The batch path already uses this API successfully.

**Alternatives considered:**
- Pool `[]Value` slices with sync.Pool: Still incurs boxing cost per value; only reduces allocation frequency, not per-value overhead.
- Generic decode with `any`: Go generics don't eliminate interface boxing for sealed interfaces.

**Design:**
```go
type BlockValueIterator struct {
    // ... existing fields ...
    // Replace: decoded []Value
    // With typed arrays:
    floatArr    tsdb.FloatArray
    integerArr  tsdb.IntegerArray
    unsignedArr tsdb.UnsignedArray
    booleanArr  tsdb.BooleanArray
    stringArr   tsdb.StringArray
    valueType   byte  // which array is active
    pos         int   // position within active array
}
```

The `Init()`, `NextBlock()`, `ActivatePending()` methods dispatch to the appropriate `Decode*ArrayBlock` based on block type. `Read()` returns values from the active typed array at current position.

### Decision 2: Batch-style key-level merge replacing per-value heap

**Choice:** Replace the `valueEntry` heap merge with a batch-style approach: for each key, collect all blocks from all readers, decode into typed arrays, and merge/dedup using sorted array operations (similar to `tsmBatchKeyIterator.combineFloat`).

**Rationale:** The current heap merge processes one value at a time, requiring a `valueEntry` struct allocation per value. Batch merge processes all values for a key at once using contiguous typed arrays, with zero per-value allocations.

**Alternatives considered:**
- Object pool for `valueEntry`: Reduces allocation count but still has per-value overhead and GC pressure.
- Arena allocator: Go doesn't have native arena support; would require unsafe or custom allocator.

**Design:**
```go
type KeyAwareMergingIterator struct {
    // ... existing fields ...
    // Replace: heap valueHeap, buf []Value
    // With batch merge state:
    mergedFloat    tsdb.FloatArray
    mergedInteger  tsdb.IntegerArray
    mergedUnsigned tsdb.UnsignedArray
    mergedBoolean  tsdb.BooleanArray
    mergedString   tsdb.StringArray
    mergeBuf       []blocks  // per-reader block buffer
}
```

For each key: collect blocks → decode overlaps into typed arrays → merge sorted arrays → dedup by timestamp (higher fileIdx wins) → encode output block.

### Decision 3: sync.Pool for Encode intermediate buffers

**Choice:** Add `sync.Pool` for the `vb` and `tb` byte slices in `EncodeFloatArrayBlock` and siblings.

**Rationale:** These are allocated per block encode and discarded immediately after. Pooling them eliminates repeated allocation. The existing `TODO(edd): These need to be pooled` comment confirms this was intended.

**Design:**
```go
var encodeBufPool = sync.Pool{
    New: func() interface{} {
        b := make([]byte, 0, 4096)
        return &b
    },
}
```

Each `Encode*ArrayBlock` function gets a buffer from the pool and returns it after use.

### Decision 4: Defer iter.Close() in compaction path

**Choice:** Add `defer tsm.Close()` in `compact()` after creating the iterator.

**Rationale:** The `Close()` method already exists and properly nils all internal slices. Currently it's never called, leaving resources for GC to reclaim during the compaction window.

## Risks / Trade-offs

- **[Correctness risk]** Changing decode/merge logic could introduce data corruption → Mitigated by existing comprehensive compaction tests; add specific memory comparison tests
- **[Performance regression]** Batch merge may have different CPU characteristics than heap merge → Mitigated by benchmark tests; batch merge should be faster due to better cache locality
- **[Complexity]** Type-dispatched decode adds switch statements → Mitigated by the fact that `encodeValues` already has this pattern; follow the same structure
- **[Backward compatibility]** Internal-only changes; no public API changes → No compatibility risk
