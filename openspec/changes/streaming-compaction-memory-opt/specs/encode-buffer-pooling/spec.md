## ADDED Requirements

### Requirement: Encode intermediate buffers SHALL be pooled
The `EncodeFloatArrayBlock`, `EncodeIntegerArrayBlock`, `EncodeUnsignedArrayBlock`, `EncodeBooleanArrayBlock`, and `EncodeStringArrayBlock` functions SHALL use `sync.Pool` for the intermediate `vb` and `tb` byte slices used during compression.

#### Scenario: Encode reuses pooled buffers
- **WHEN** `EncodeFloatArrayBlock` is called twice in succession
- **THEN** the second call SHALL reuse the `vb` and `tb` byte slices from the first call's pool return, avoiding fresh allocation

#### Scenario: Pool handles varying buffer sizes
- **WHEN** an encode requires a buffer larger than the pooled buffer's capacity
- **THEN** a new buffer SHALL be allocated and the pooled buffer SHALL be returned to the pool

#### Scenario: Nil pool buffer parameter still works
- **WHEN** `EncodeFloatArrayBlock` is called with `b nil`
- **THEN** the function SHALL get a buffer from the pool and return the packed result correctly
