## ADDED Requirements

### Requirement: Read() nil-fies buffer elements after encoding
After `encodeValues` completes successfully in `Read()`, the method SHALL iterate through all elements in `m.buf` and set each to the zero value `Value{}`. This breaks references to `StringValue` and other reference-type values, allowing GC to reclaim the underlying string data.

#### Scenario: String values become unreachable after encoding
- **WHEN** `Read()` completes encoding a block of string values
- **THEN** every element in `m.buf` is set to `Value{}`, and the string data previously held by `StringValue.value` becomes unreachable from `m.buf`

#### Scenario: Float values are also nil-fied
- **WHEN** `Read()` completes encoding a block of float values
- **THEN** every element in `m.buf` is set to `Value{}` (float values are not reference types, but uniform treatment avoids type-switch complexity)

#### Scenario: Error path does not nil-fy
- **WHEN** `encodeValues` returns an error
- **THEN** `m.buf` is NOT nil-fied (the values may be needed for diagnostic logging)

### Requirement: Key buffer reuse
`KeyAwareMergingIterator` and `streamingKeyIterator` SHALL each persist a reusable `keyBuf []byte` field. The `Read()` method SHALL copy the key into this buffer instead of allocating a new `[]byte` on every call.

#### Scenario: First Read allocates key buffer
- **WHEN** `Read()` is called for the first time
- **THEN** a `[]byte` buffer is allocated to hold the key

#### Scenario: Subsequent Read reuses key buffer
- **WHEN** `Read()` is called and the key length is <= the buffer capacity
- **THEN** the existing buffer is reused without allocation

#### Scenario: Longer key grows buffer
- **WHEN** `Read()` is called and the key length exceeds the buffer capacity
- **THEN** a new buffer is allocated with sufficient capacity, replacing the old one
