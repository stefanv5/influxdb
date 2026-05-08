## Context

The `KeyAwareMergingIterator` in `tsdb/engine/tsm1/stream_iterator.go` is the core of the streaming compaction path (`CompactFull`). It merges values from multiple TSM files using a min-heap ordered by `(timestamp, fileIdx)`. The `popAndDedup` function deduplicates values with the same timestamp, keeping the one from the newest file (highest `fileIdx`).

The bug: `popAndDedup`'s dedup loop only checks `next.timestamp != top.timestamp` to decide when to stop dedup. It does not verify that `next` and `top` belong to the same key. When `advanceAndPush` advances an iterator past the current key's last block, the iterator moves to the next key's block. If `NextBlock()` returns `true` (same key, new block) and the new block's value has the same timestamp as the value being deduped, the dedup loop continues — but the value might be from a different key (if the iterator transitioned to a new key between same-key blocks in the same file).

Non-streaming compaction (`tsmBatchKeyIterator`) avoids this by loading all blocks for a key upfront and merging typed slices explicitly. The streaming approach processes one value at a time through the heap, making it vulnerable to cross-key mixing during dedup.

## Goals / Non-Goals

**Goals:**
- Prevent cross-key value mixing in the streaming compaction heap
- Ensure compaction never aborts due to type mismatches — log and skip instead
- Maintain backward compatibility with `KeyIterator` interface
- Zero data loss for valid values

**Non-Goals:**
- Fixing the non-streaming compaction path (it already works correctly)
- Changing the `KeyIterator` interface
- Detecting or correcting TSM file corruption at the storage layer
- Performance optimization of the heap merge (only correctness fixes)

## Decisions

### D1: Add `key` field to `valueEntry` for identity tracking

**Decision**: Add a `key []byte` field to the `valueEntry` struct, populated at entry creation time from `iter.currentKey`.

**Rationale**: The heap only sorts by `(timestamp, fileIdx)`. There is no way to distinguish entries from different keys after they enter the heap. By snapshotting the key into each entry, the dedup loop can verify key equality.

**Alternatives considered**:
- Check `iter.Key()` at dedup time: Rejected because the iterator's key changes after `advanceAndPush`, so `iter.Key()` may no longer reflect the key of the entry that was popped.
- Add key to heap ordering: Rejected because it would change the merge order semantics (keys should merge independently, not group together).

### D2: Break dedup loop on key mismatch

**Decision**: In `popAndDedup`, after checking `next.timestamp != top.timestamp`, also check `!bytes.Equal(next.key, top.key)`. If keys differ, break the loop.

**Rationale**: Entries from different keys with the same timestamp are NOT duplicates — they belong to different fields. The dedup loop must only merge entries from the same key.

**Alternatives considered**:
- Skip mismatched entries and continue: Rejected because it would silently discard entries from different keys, causing data loss.
- Move mismatched entries to a side buffer: Rejected as over-engineering; the entry stays in the heap and will be processed when its key becomes current.

### D3: Defer winner's iterator advancement to after dedup

**Decision**: In `popAndDedup`, advance the winner's iterator AFTER the dedup loop completes (not immediately after popping). This ensures the winner's next value is pushed to the heap only after all same-timestamp entries have been resolved.

**Rationale**: Advancing the winner immediately (current behavior) can push a value into the heap while the dedup loop is still running. If the dedup loop pops a different entry and advances that iterator, both iterators may push values from different keys into the heap simultaneously.

**Alternatives considered**:
- Keep current behavior with key check only: Risky because the winner's `advanceAndPush` could still push a value from a different key (if the winner's iterator moved past the current key's last block).
- Don't advance during dedup at all: Rejected because stale entries would remain in the heap.

### D4: Defensive type checking in `encodeValues`

**Decision**: Change `encodeValues` to use `append`-based array building instead of pre-allocated arrays. When a type mismatch is detected, log the error to `m.err` and `continue` (skip the value) instead of returning an error.

**Rationale**: Even with the dedup fix, defensive coding prevents compaction abort on unexpected data. The error is recorded for diagnostics but does not halt the compaction process.

**Alternatives considered**:
- Return error immediately: Current behavior — causes compaction to abort, which is too aggressive.
- Panic on mismatch: Rejected as dangerous in production.

### D5: Fix sentinel value conflict for `currentType`

**Decision**: Introduce `blockTypeUnset = byte(0xFF)` as the sentinel value for "not yet set", replacing the use of `0` which collides with `BlockFloat64`.

**Rationale**: `BlockFloat64 = byte(0)`. When `currentType` is reset to `0` (line 731), it is indistinguishable from "type is Float64". This causes `blockTypeForValue` to be called on every value for Float64 keys (redundant recalculation). More critically, it creates ambiguity in the type-setting logic at line 416-417.

**Alternatives considered**:
- Use a separate `bool` flag: Rejected as adding unnecessary state.
- Use `-1` cast to byte: Works but `0xFF` is clearer as "invalid/unset".

### D6: Handle nil data in `Read()`

**Decision**: If `encodeValues` returns `nil` data (all values skipped due to type mismatch), `Read()` returns `nil, 0, 0, nil, nil`. The caller (`writeNewFiles`) should skip nil data blocks.

**Rationale**: If all values in a block are type-mismatched (extremely unlikely with D1+D2 in place), the compaction should not abort. Returning nil data signals "nothing to write" without error.

## Risks / Trade-offs

- **[Risk]** Skipping type-mismatched values in `encodeValues` could silently drop data if the dedup fix (D2) is insufficient → **Mitigation**: The `m.err` field records the mismatch for diagnostics. Monitoring should alert on repeated type mismatch errors.
- **[Risk]** The `key []byte` snapshot in `valueEntry` adds memory overhead per heap entry → **Mitigation**: Keys are typically short (< 100 bytes). The heap holds at most `numFiles` entries, so overhead is negligible.
- **[Risk]** Changing sentinel from `0` to `0xFF` could break if `0xFF` is ever used as a valid block type → **Mitigation**: Block types are currently 0-4. `0xFF` is far from the valid range.
- **[Trade-off]** Defensive type checking in `encodeValues` adds a branch per value → **Mitigation**: Branch prediction will optimize the happy path. The check is a single type assertion, negligible cost.
