## MODIFIED Requirements

### Requirement: Streaming iterator Close() SHALL be called from compaction path
The `streamingKeyIterator.Close()` method SHALL be called when the compaction completes or errors, ensuring all internal resources (decoded arrays, heap entries, typed arrays, key buffers) are explicitly released.

#### Scenario: Close called on successful compaction
- **WHEN** `compact()` creates a streaming iterator and `writeNewFiles` completes successfully
- **THEN** `iter.Close()` SHALL be called before `compact()` returns

#### Scenario: Close called on compaction error
- **WHEN** `compact()` creates a streaming iterator and `writeNewFiles` returns an error
- **THEN** `iter.Close()` SHALL still be called (via defer) before `compact()` returns

#### Scenario: Close is idempotent
- **WHEN** `Close()` is called multiple times on the same iterator
- **THEN** subsequent calls SHALL be no-ops and SHALL NOT panic
