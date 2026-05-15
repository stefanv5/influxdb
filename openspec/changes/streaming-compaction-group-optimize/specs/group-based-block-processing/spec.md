## ADDED Requirements

### Requirement: Overlap-based block grouping

The iterator SHALL partition all blocks for a key into independent overlap groups using transitive `overlapsTimeRange` detection. Blocks from different groups have strictly disjoint time ranges and SHALL be processed independently.

#### Scenario: Non-overlapping blocks form separate groups
- **WHEN** blocks A[1-1000], B[1500-2500], C[2000-3000] share a key
- **THEN** A forms group 1 (singleton, no overlap with B or C); B and C form group 2 (overlap at [2000,2500])

#### Scenario: All blocks form one group
- **WHEN** blocks A[1-1000], B[500-1500], C[1200-2000] share a key
- **THEN** all three blocks form a single group (transitive overlap chain: A↔B, B↔C)

#### Scenario: Two independent overlap chains
- **WHEN** blocks A[1-100], B[50-150], C[200-300], D[250-350] share a key
- **THEN** A and B form group 1; C and D form group 2; groups 1 and 2 are independent (100 < 200)

### Requirement: Singleton pass-through

A group containing exactly one block with no tombstones SHALL pass through as raw mmap data without decode or re-encode. The block's minTime, maxTime, and raw bytes are directly referenced.

#### Scenario: Single non-overlapping block passes through
- **WHEN** a singleton group has block [1-1000] with no tombstones
- **THEN** the block is output as an `outputBlock` referencing the original mmap bytes; the `streamBlock` is returned to the pool

#### Scenario: Single block with tombstones is decoded
- **WHEN** a singleton group has block [1-1000] with tombstone [500, 600]
- **THEN** the block is decoded, tombstoned values removed, and re-encoded (existing behavior)

### Requirement: Group decode and merge

A group containing multiple blocks OR a tombstoned block SHALL be decoded into typed arrays, merged with deduplication, and re-encoded into output blocks. This uses the existing `decodeAndMerge()` and `chunkMergedArray()` methods.

#### Scenario: Overlapping group is decoded and merged
- **WHEN** group 2 contains B[1500-2500] and C[2000-3000]
- **THEN** both blocks are decoded into typed arrays, merged with dedup (same timestamp from higher fileIdx wins), and re-encoded

#### Scenario: Non-overlapping blocks misclassified by maxTimeSeen
- **WHEN** rawChunks contains C (misclassified as non-overlapping due to round-robin timing) and pending contains B; C and B overlap
- **THEN** the grouping puts C and B in the same group, triggering decode+merge; A (truly non-overlapping) remains in its own group and passes through

### Requirement: Output sorted by minTime

The final output blocks for a key SHALL be sorted by minTime. Pass-through singleton groups and merged groups may be interleaved, so a final sort is applied.

#### Scenario: Interleaved groups sorted
- **WHEN** group 1 outputs block [1-1000], group 2 outputs blocks [1500-2000] and [2000-3000] from merge
- **THEN** final chunks are sorted: [1-1000], [1500-2000], [2000-3000]

### Requirement: No heap allocation for allBlocks buffer

The `allBlocks` buffer SHALL be a reusable field on `KeyAwareMergingIterator`. Its backing array grows to capacity on the first key and is reused for all subsequent keys with zero allocation.

#### Scenario: First key allocates, subsequent keys reuse
- **WHEN** 10 keys are processed, each with 3 blocks
- **THEN** the first key's `allBlocks` slice grows to capacity 3; keys 2-10 reuse the same backing array with `append(allBlocks[:0], ...)`
