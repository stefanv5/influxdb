## ADDED Requirements

### Requirement: Unit test SHALL compare memory allocation between streaming and non-streaming
A benchmark test SHALL measure and compare `allocs/op` and `bytes/op` between the streaming compaction path (`KeyAwareMergingIterator`) and the non-streaming path (`tsmBatchKeyIterator`) using identical input data.

#### Scenario: Streaming allocs are within 2x of non-streaming
- **WHEN** both paths process the same TSM files with the same number of keys and values
- **THEN** the streaming path's `allocs/op` SHALL be at most 2x the non-streaming path's `allocs/op`

#### Scenario: Streaming bytes alloc are within 2x of non-streaming
- **WHEN** both paths process the same TSM files with the same number of keys and values
- **THEN** the streaming path's `bytes/op` SHALL be at most 2x the non-streaming path's `bytes/op`

### Requirement: Unit test SHALL verify correctness of streaming output matches non-streaming
The test SHALL verify that the streaming path produces byte-identical TSM output to the non-streaming path for the same input.

#### Scenario: Identical output for same input
- **WHEN** both paths compact the same set of TSM files
- **THEN** the output blocks SHALL be identical (same keys, same timestamps, same values, same order)
