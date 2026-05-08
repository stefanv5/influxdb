## ADDED Requirements

### Requirement: BlockValueIterator reuses decoded slice capacity
`BlockValueIterator` SHALL persist its `decoded []Value` field across block boundaries. When calling `DecodeBlock`, it SHALL pass `it.decoded[:0]` to reuse the existing slice capacity instead of passing `nil`.

#### Scenario: First block decode allocates new slice
- **WHEN** `Init()` is called for the first time
- **THEN** `DecodeBlock` receives `it.decoded[:0]` (which is effectively `nil` on first call) and returns a newly allocated `[]Value` slice

#### Scenario: Subsequent block decode reuses slice capacity
- **WHEN** `NextBlock()` or `ActivatePending()` is called after the first block has been consumed
- **THEN** `DecodeBlock` receives `it.decoded[:0]` with existing capacity, and the returned slice reuses the underlying array when the new block has equal or fewer values

#### Scenario: Larger block grows slice capacity
- **WHEN** a new block has more values than the current slice capacity
- **THEN** `DecodeBlock` allocates a larger array as needed via `append`, and the new capacity is retained for subsequent blocks

### Requirement: All decode paths reuse the slice
All three decode paths in `BlockValueIterator` — `Init()`, `NextBlock()`, and `ActivatePending()` — SHALL pass `it.decoded[:0]` to `DecodeBlock` instead of `nil`.

#### Scenario: Init reuses slice
- **WHEN** `Init()` calls `DecodeBlock`
- **THEN** it passes `it.decoded[:0]` as the second argument

#### Scenario: NextBlock reuses slice
- **WHEN** `NextBlock()` decodes a same-key block
- **THEN** it passes `it.decoded[:0]` as the second argument to `DecodeBlock`

#### Scenario: ActivatePending reuses slice
- **WHEN** `ActivatePending()` decodes the pending block
- **THEN** it passes `it.decoded[:0]` as the second argument to `DecodeBlock`
