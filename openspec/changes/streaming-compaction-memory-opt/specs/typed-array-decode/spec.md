## ADDED Requirements

### Requirement: BlockValueIterator SHALL decode blocks into typed arrays
`BlockValueIterator` SHALL decode TSM blocks directly into typed arrays (`tsdb.FloatArray`, `tsdb.IntegerArray`, `tsdb.UnsignedArray`, `tsdb.BooleanArray`, `tsdb.StringArray`) instead of `[]Value` interface slices. The decode SHALL use `DecodeFloatArrayBlock`, `DecodeIntegerArrayBlock`, etc. based on the block type.

#### Scenario: Float block decoded without interface boxing
- **WHEN** `BlockValueIterator.Init()` encounters a `BlockFloat64` block
- **THEN** the block SHALL be decoded via `DecodeFloatArrayBlock` into `tsdb.FloatArray`, and `Read()` SHALL return `FloatValue` constructed from the array elements without allocating a `[]Value` slice

#### Scenario: Integer block decoded without interface boxing
- **WHEN** `BlockValueIterator.Init()` encounters a `BlockInteger` block
- **THEN** the block SHALL be decoded via `DecodeIntegerArrayBlock` into `tsdb.IntegerArray`

#### Scenario: All block types supported
- **WHEN** `BlockValueIterator` encounters any of Float64, Integer, Unsigned, Boolean, or String block types
- **THEN** the iterator SHALL decode into the corresponding typed array and SHALL NOT use `DecodeBlock` or `[]Value`

#### Scenario: Reuse typed arrays across blocks
- **WHEN** `BlockValueIterator` advances to the next block of the same type
- **THEN** the typed array SHALL be reused (capacity retained) by passing it as the destination to `Decode*ArrayBlock`

### Requirement: Read() SHALL return typed values without interface allocation
`BlockValueIterator.Read()` SHALL return a `Value` interface by constructing it from the typed array at the current position. For value types (FloatValue, IntegerValue, etc.) which are small structs, Go may inline the construction to avoid heap allocation.

#### Scenario: Read returns correct value from typed array
- **WHEN** the iterator is positioned at index `i` in a float array with `Timestamps[i]=100` and `Values[i]=3.14`
- **THEN** `Read()` SHALL return a `FloatValue{unixnano: 100, value: 3.14}`

#### Scenario: No retained references after advancing
- **WHEN** the iterator advances past index `i`
- **THEN** the value at index `i` SHALL NOT be retained in any iterator-owned slice
