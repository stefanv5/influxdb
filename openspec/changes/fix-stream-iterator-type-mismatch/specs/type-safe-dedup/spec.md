## ADDED Requirements

### Requirement: valueEntry SHALL capture key identity at creation time
The `valueEntry` struct SHALL include a `key []byte` field that stores a snapshot of the iterator's current key at the time the entry is created. This field MUST be populated in all code paths that create `valueEntry` instances (`activateIteratorForBlock`, `advanceAndPush`).

#### Scenario: Entry created from iterator with known key
- **WHEN** a `valueEntry` is created from a `BlockValueIterator` whose current key is `"cpu,host=A#!~#value"`
- **THEN** the entry's `key` field SHALL equal `"cpu,host=A#!~#value"`

#### Scenario: Entry key is independent of iterator state changes
- **WHEN** a `valueEntry` is created and the iterator subsequently advances to a different key
- **THEN** the entry's `key` field SHALL remain unchanged (snapshot semantics)

### Requirement: popAndDedup SHALL NOT deduplicate entries from different keys
The `popAndDedup` dedup loop SHALL check both timestamp equality AND key equality before treating entries as duplicates. If the next entry's key differs from the current top entry's key, the loop MUST break even if timestamps match.

#### Scenario: Same timestamp, same key — dedup proceeds
- **WHEN** the heap contains two entries with timestamp=100, both for key `"cpu~field_1"`, from file0 (fileIdx=0) and file1 (fileIdx=1)
- **THEN** the entry from file1 (higher fileIdx) SHALL be kept and the other discarded

#### Scenario: Same timestamp, different key — dedup stops
- **WHEN** the heap contains entry A (timestamp=100, key="cpu~field_1", fileIdx=0) and entry B (timestamp=100, key="cpu~field_2", fileIdx=1)
- **THEN** entry A SHALL be returned and entry B SHALL remain in the heap for later processing

#### Scenario: Different timestamp — dedup stops (existing behavior preserved)
- **WHEN** the top entry has timestamp=100 and the next entry has timestamp=200
- **THEN** the dedup loop SHALL break regardless of key equality

### Requirement: Winner's iterator advancement SHALL be deferred until after dedup
In `popAndDedup`, the winning entry's iterator SHALL be advanced AFTER the dedup loop completes, not immediately after the initial pop. This prevents the winner's next value from entering the heap while same-timestamp entries are still being processed.

#### Scenario: Winner advancement after dedup
- **WHEN** `popAndDedup` pops a winning entry and the dedup loop processes 2 additional same-timestamp entries
- **THEN** the winner's iterator SHALL be advanced exactly once, after all same-timestamp entries have been resolved

### Requirement: encodeValues SHALL handle type mismatches gracefully
The `encodeValues` function SHALL NOT return an error when a value's type does not match the expected block type. Instead, it SHALL record the mismatch in `m.err` and skip the value, continuing with the remaining values.

#### Scenario: Single type-mismatched value in buffer
- **WHEN** `encodeValues` is called with `typ=BlockString` and values `[StringValue("a"), FloatValue(3.14), StringValue("b")]`
- **THEN** the output block SHALL contain values `["a", "b"]` and `m.err` SHALL contain the type mismatch error

#### Scenario: All values type-mismatched
- **WHEN** `encodeValues` is called with `typ=BlockString` and all values are `FloatValue`
- **THEN** the output SHALL be `nil` and `m.err` SHALL contain the type mismatch error

#### Scenario: No type mismatch
- **WHEN** `encodeValues` is called with `typ=BlockFloat64` and all values are `FloatValue`
- **THEN** all values SHALL be encoded into the output block and `m.err` SHALL be unchanged

### Requirement: Read SHALL handle nil encoded data
The `Read` method SHALL handle the case where `encodeValues` returns nil data (all values skipped due to type mismatch) without returning an error.

#### Scenario: encodeValues returns nil
- **WHEN** `Read()` is called and `encodeValues` returns nil data
- **THEN** `Read()` SHALL return `nil, 0, 0, nil, nil` (no error)

### Requirement: currentType sentinel SHALL NOT collide with valid block types
The `currentType` field SHALL use a sentinel value that does not collide with any valid block type constant (`BlockFloat64=0`, `BlockInteger=1`, `BlockBoolean=2`, `BlockString=3`, `BlockUnsigned=4`).

#### Scenario: Float64 key type detection
- **WHEN** the first value for a key is a `FloatValue` and `currentType` is the sentinel value
- **THEN** `currentType` SHALL be set to `BlockFloat64` (0) exactly once, not recalculated on subsequent values

#### Scenario: Sentinel value distinctness
- **WHEN** `currentType` is the sentinel value
- **THEN** it SHALL NOT equal any of `BlockFloat64`, `BlockInteger`, `BlockBoolean`, `BlockString`, or `BlockUnsigned`
