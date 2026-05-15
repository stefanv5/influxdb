## ADDED Requirements

### Requirement: KeyAwareMergingIterator SHALL merge at key level using typed arrays
`KeyAwareMergingIterator` SHALL collect all blocks for a given key from all readers, decode them into typed arrays, and merge/dedup using sorted array operations. This replaces the per-value `valueEntry` heap merge.

#### Scenario: Merge blocks from multiple readers for same key
- **WHEN** 3 readers each have blocks for key "cpu.usage" with overlapping time ranges
- **THEN** the iterator SHALL decode all blocks into typed arrays, merge by timestamp, dedup by keeping the value from the highest fileIdx, and produce a single output block

#### Scenario: Dedup by timestamp keeps newest file
- **WHEN** two readers have values at timestamp T=100 for the same key
- **THEN** the value from the reader with higher fileIdx SHALL be kept

#### Scenario: No per-value heap allocations
- **WHEN** the iterator processes a block of 1000 values
- **THEN** the merge SHALL NOT allocate `valueEntry` structs or `[]byte` key copies per value

### Requirement: Merge SHALL produce sorted output
The merged output SHALL be sorted by timestamp in ascending order, matching the contract expected by the TSM writer.

#### Scenario: Overlapping time ranges produce sorted output
- **WHEN** reader A has values at timestamps [1,3,5] and reader B has values at [2,4,6] for the same key
- **THEN** the merged output SHALL contain timestamps [1,2,3,4,5,6] in order

#### Scenario: Exact timestamp dedup
- **WHEN** reader A and reader B both have a value at timestamp T=3 for the same key
- **THEN** the output SHALL contain exactly one value at T=3 (from the higher fileIdx reader)
