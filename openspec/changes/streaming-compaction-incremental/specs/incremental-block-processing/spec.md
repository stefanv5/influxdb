## ADDED Requirements

### Requirement: Incremental overlap detection via maxTimeSeen

The iterator SHALL maintain a running `maxTimeSeen` across all blocks read for the current key. When a new block's `minTime > maxTimeSeen`, it SHALL be classified as non-overlapping and output immediately.

#### Scenario: Non-overlapping block detected incrementally
- **WHEN** file 0 has block [1,100] and file 1 has block [101,200] for the same key
- **THEN** after reading file 0's block, maxTimeSeen=100; when file 1's block arrives with minTime=101 > 100, it is classified as non-overlapping and output immediately

#### Scenario: Overlapping block detected incrementally
- **WHEN** file 0 has block [1,100] and file 1 has block [50,150] for the same key
- **THEN** after reading file 0's block, maxTimeSeen=100; when file 1's block arrives with minTime=50 <= 100, it is classified as potentially overlapping and buffered in pending

#### Scenario: Block overlaps with earlier block from same file
- **WHEN** file 0 has blocks [1,100], [101,200] and file 1 has block [150,250]
- **THEN** file 0 block [1,100] is output immediately (minTime=1 > maxTimeSeen=0); file 0 block [101,200] is output immediately (minTime=101 > maxTimeSeen=100); file 1 block [150,250] is pending (minTime=150 <= maxTimeSeen=200)

### Requirement: Two-buffer model (chunks + pending)

The iterator SHALL maintain two separate buffers for the current key:
- `chunks`: non-overlapping blocks committed to output
- `pending`: blocks that may overlap, awaiting full evaluation

#### Scenario: No overlapping blocks
- **WHEN** all blocks for a key are non-overlapping
- **THEN** all blocks are in `chunks`, `pending` is empty, no decode occurs

#### Scenario: Some blocks overlap
- **WHEN** 10 blocks exist for a key, 3 overlap with each other
- **THEN** 7 non-overlapping blocks are in `chunks`, 3 overlapping blocks are in `pending`; only the 3 pending blocks are decoded and merged

#### Scenario: All blocks overlap
- **WHEN** all blocks for a key overlap with each other
- **THEN** `chunks` is empty, all blocks are in `pending`; same behavior as current collect-all approach (no regression)

### Requirement: Per-file incremental reading

The iterator SHALL read blocks one at a time from each file rather than collecting all blocks from one file before moving to the next. This enables early detection of non-overlapping blocks.

#### Scenario: Round-robin reading across files
- **WHEN** 3 files each have 10 blocks for the same key
- **THEN** the iterator reads one block from file 0, then one from file 1, then one from file 2, then back to file 0, etc.

#### Scenario: File exhausted before others
- **WHEN** file 0 has 2 blocks and file 1 has 10 blocks for the same key
- **THEN** after file 0 is exhausted, the iterator continues reading only from file 1

### Requirement: Pending blocks sorted before overlap check

After all blocks for a key have been read, the iterator SHALL sort `pending` by minTime and check for pairwise overlap using the same `needsDedup()` logic.

#### Scenario: Pending has no internal overlap
- **WHEN** pending contains blocks [150,250] and [300,400] (no overlap between them)
- **THEN** pending blocks are passed through as raw data without decode

#### Scenario: Pending has internal overlap
- **WHEN** pending contains blocks [150,250] and [200,350] (overlap)
- **THEN** both blocks are decoded into typed arrays and merged

### Requirement: Output ordering

The output blocks for a key SHALL be ordered by minTime. Non-overlapping blocks (from `chunks`) that precede the overlapping region in time SHALL appear before the decoded/merged blocks.

#### Scenario: Mixed output ordering
- **WHEN** chunks has [1,100] and [101,200], pending has overlapping blocks [150,250] and [200,350] which merge to [150,350]
- **THEN** output order is: [1,100], [101,200], [150,350] (sorted by minTime)

### Requirement: No memory regression for overlapping keys

When all blocks for a key overlap, the iterator's memory usage SHALL NOT exceed the current collect-all approach.

#### Scenario: All blocks overlap
- **WHEN** a key has 100 blocks across 3 files, all overlapping
- **THEN** peak memory is the same as current approach (all blocks buffered in pending, all decoded)
