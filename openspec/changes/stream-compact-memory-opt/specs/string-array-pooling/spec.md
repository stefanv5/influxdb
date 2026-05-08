## ADDED Requirements

### Requirement: KeyAwareMergingIterator pools typed arrays for encodeValues
`KeyAwareMergingIterator` SHALL persist reusable typed arrays (`FloatArray`, `IntegerArray`, `UnsignedArray`, `BooleanArray`, `StringArray`) as struct fields. The `encodeValues` method SHALL reuse these arrays instead of calling `tsdb.NewXxxArrayLen(0)` on every invocation.

#### Scenario: First encode allocates array
- **WHEN** `encodeValues` is called for the first time for a given type
- **THEN** the corresponding typed array is initialized (if nil) and used for encoding

#### Scenario: Subsequent encode reuses array
- **WHEN** `encodeValues` is called again with the same type
- **THEN** the Timestamps and Values slices of the existing array are reset to `[:0]` and reused, preserving underlying capacity

#### Scenario: Type switch between blocks
- **WHEN** the block type changes between consecutive `Read()` calls (e.g., float to string)
- **THEN** the correct typed array for the new type is used, and the old type's array is left untouched for potential future reuse

### Requirement: Array reset before each encode
Before populating a typed array in `encodeValues`, the method SHALL reset both `Timestamps` and `Values` to `[:0]` to ensure no stale data from a previous encode persists.

#### Scenario: Reset ensures clean state
- **WHEN** `encodeValues` begins processing values for a block
- **THEN** the Timestamps slice is `arr.Timestamps[:0]` and Values slice is `arr.Values[:0]`

### Requirement: All five block types pool their arrays
The pooling SHALL apply to all five block types: `BlockFloat64`, `BlockInteger`, `BlockUnsigned`, `BlockBoolean`, and `BlockString`.

#### Scenario: Float array pooling
- **WHEN** encoding float values
- **THEN** `m.floatArr` is reused (or initialized if nil)

#### Scenario: String array pooling
- **WHEN** encoding string values
- **THEN** `m.stringArr` is reused (or initialized if nil)
