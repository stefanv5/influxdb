## 1. Data Structure Changes

- [x] 1.1 Add `key []byte` field to `valueEntry` struct in stream_iterator.go
- [x] 1.2 Add `blockTypeUnset = byte(0xFF)` constant for sentinel value
- [x] 1.3 Update all `valueEntry` creation sites (`activateIteratorForBlock`, `advanceAndPush`) to populate the `key` field from `iter.currentKey`

## 2. popAndDedup Fix

- [x] 2.1 Add key-equality check in `popAndDedup` dedup loop: break when `!bytes.Equal(next.key, top.key)`
- [x] 2.2 Defer winner's iterator advancement to after the dedup loop: move `advanceAndPush(top.iterator)` from line 645 to after the dedup loop
- [x] 2.3 After dedup loop, advance all losers' iterators that are still on the current key (call `advanceAndPush` for each)

## 3. encodeValues Defensive Type Check

- [x] 3.1 Change `encodeValues` to use append-based array building instead of pre-allocated `NewXxxArrayLen(len(values))`
- [x] 3.2 On type assertion failure in each case (Float, Integer, Unsigned, Boolean, String): record error in `m.err` and `continue` instead of returning error
- [x] 3.3 Return `nil, nil` when the output array is empty after filtering

## 4. Read() nil Data Handling

- [x] 4.1 In `Read()`, handle nil `data` from `encodeValues`: return `nil, 0, 0, nil, nil` instead of error

## 5. Sentinel Value Fix

- [x] 5.1 In `moveToNextKey`, set `m.currentType = blockTypeUnset` instead of `0`
- [x] 5.2 In `Next()` buf-filling loop, check `m.currentType == blockTypeUnset` instead of `== 0`

## 6. Tests

- [x] 6.1 Add test: same timestamp, same key, different files — dedup proceeds correctly
- [x] 6.2 Add test: same timestamp, different keys — dedup stops, no cross-key mixing
- [x] 6.3 Add test: defensive encodeValues — type mismatch skips value, records error
- [x] 6.4 Add test: Float64 key type detection — sentinel value does not cause redundant recalculation
- [x] 6.5 Add test: Read() with nil encoded data returns nil without error
