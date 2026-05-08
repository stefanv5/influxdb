## Why

Production compaction fails with "type mismatch: expected StringValue, got FloatValue". The root cause is in `KeyAwareMergingIterator.popAndDedup()`: the dedup loop only compares timestamps when deciding which entries are duplicates, without verifying they belong to the same key. When `advanceAndPush` advances an iterator past the current key's boundary (into a different field's block), a value from a different key with the same timestamp can be incorrectly treated as a duplicate of the current key's value, causing type mixing in the output buffer.

## What Changes

- Add `key` field to `valueEntry` struct to capture the key at entry creation time
- Add key-equality check in `popAndDedup` dedup loop to prevent cross-key deduplication
- Add defensive type checking in `encodeValues` so type mismatches are logged and skipped rather than aborting the entire compaction
- Handle nil data in `Read()` for the case where all values are skipped due to type mismatch
- Fix sentinel value conflict where `BlockFloat64 == 0` collides with the "unset" state of `currentType`

## Capabilities

### New Capabilities
- `type-safe-dedup`: Ensures the streaming merge iterator never mixes values from different keys during deduplication, even when timestamps collide across keys

### Modified Capabilities

## Impact

- `tsdb/engine/tsm1/stream_iterator.go`: Core changes to `valueEntry`, `popAndDedup`, `advanceAndPush`, `encodeValues`, `Read`, and sentinel value handling
- `tsdb/engine/tsm1/stream_iterator_test.go`: New tests for cross-key type scenarios and edge cases
- No API or interface changes: `KeyIterator` interface unchanged, fully backward compatible
- No impact on non-streaming compaction path (`tsmBatchKeyIterator`)
