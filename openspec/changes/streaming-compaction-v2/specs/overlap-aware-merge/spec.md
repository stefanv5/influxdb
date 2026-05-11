## ADDED Requirements

### Requirement: Per-file block buffering

The merging iterator SHALL read blocks one at a time from each underlying `BlockValueIterator`, maintaining a per-file buffer of unread blocks. It SHALL NOT collect all blocks for a key before processing.

#### Scenario: Three files with same key
- **WHEN** three TSM files each contain blocks for key "cpu,host=A"
- **THEN** the iterator reads one block per file (3 blocks total), finds the minimum key, and processes those 3 blocks before reading more

#### Scenario: File has multiple blocks for same key
- **WHEN** a single TSM file has 10 blocks for key "cpu,host=A" covering time ranges [1,100], [101,200], ..., [901,1000]
- **THEN** the iterator buffers all 10 blocks from that file and includes them in the merge for that key

### Requirement: Overlap detection

The merging iterator SHALL detect whether blocks for the same key have overlapping time ranges. Blocks SHALL be sorted by minTime before overlap checking.

#### Scenario: Non-overlapping blocks across files
- **WHEN** file 0 has block [1,100] and file 1 has block [101,200] for the same key
- **THEN** no overlap is detected and both blocks pass through as raw data

#### Scenario: Overlapping blocks across files
- **WHEN** file 0 has block [1,100] and file 1 has block [50,150] for the same key
- **THEN** overlap IS detected and both blocks are decoded into typed arrays for merge

#### Scenario: Blocks with tombstones
- **WHEN** any block for a key has tombstone ranges
- **THEN** overlap IS forced (dedup required) regardless of time-range overlap

### Requirement: Raw block pass-through for non-overlapping blocks

When no overlap is detected, the merging iterator SHALL pass blocks through as raw encoded data without decoding or re-encoding. The raw data SHALL be the mmap slice from the TSM reader — no copy.

#### Scenario: Non-overlapping blocks pass through
- **WHEN** 3 blocks for a key have non-overlapping time ranges and no tombstones
- **THEN** `Read()` returns each block's raw mmap data with correct minTime/maxTime, and zero decode/encode operations occur

#### Scenario: Output data is identical to input
- **WHEN** a non-overlapping block passes through
- **THEN** the output bytes are bit-identical to the input TSM block bytes

### Requirement: Typed array merge for overlapping blocks

When overlap IS detected, the merging iterator SHALL decode overlapping blocks into typed arrays (`tsdb.FloatArray`, etc.) and merge them. The merge SHALL use sorted merge with dedup by timestamp (higher file index wins on collision).

#### Scenario: Overlapping float blocks merge correctly
- **WHEN** file 0 has float values [1→1.0, 2→2.0] and file 1 has float values [2→2.2, 3→3.0] for the same key
- **THEN** the merged output contains [1→1.0, 2→2.2, 3→3.0] (file 1 wins on timestamp 2)

#### Scenario: Multiple value types
- **WHEN** blocks contain integer, unsigned, boolean, or string values with overlaps
- **THEN** each type uses its corresponding typed array merge (no per-value interface boxing)

### Requirement: No raw block copies

The merging iterator SHALL NOT allocate new byte slices to hold raw block data. Raw blocks from `BlockValueIterator.RawBlock()` are mmap slices and SHALL be referenced directly.

#### Scenario: Memory allocation for raw blocks
- **WHEN** the iterator processes 1000 blocks across 3 files
- **THEN** zero `make([]byte, ...)` calls are made for raw block storage

### Requirement: Time range captured before encode

When chunking merged typed arrays into output blocks, the iterator SHALL capture minTime and maxTime from the timestamp array BEFORE calling `Encode*ArrayBlock`. This is required because `TimeArrayEncodeAll` mutates the input timestamps in-place (converts to deltas).

#### Scenario: Float chunk captures correct time range
- **WHEN** a merged float array has timestamps [1, 2, ..., 100] and is chunked into one block
- **THEN** the chunk entry has minTime=1 and maxTime=100 (not the post-encode deltas)

### Requirement: First merge uses manual copy

When merging the first decoded block into a merged typed array, the iterator SHALL use manual `append` to copy timestamps and values. It SHALL NOT use `*a = *b` or `Merge()` when the destination array is empty, because `Merge()` with an empty destination copies slice headers rather than backing arrays.

#### Scenario: First block merge is independent
- **WHEN** the first block for a key is decoded into a local `tsdb.FloatArray`
- **THEN** the merged array gets its own copy of timestamps and values (not a shared reference to the local array's backing store)

### Requirement: Per-type chunk functions

The iterator SHALL have separate chunk functions for each value type (float, integer, unsigned, boolean, string). Each function SHALL encode using the corresponding `Encode*ArrayBlock` function and store the result as a `chunkEntry` with data, minTime, and maxTime.

#### Scenario: Integer blocks chunk correctly
- **WHEN** a merged integer array has 250 values and bufSize=100
- **THEN** three chunk entries are produced: [0..99], [100..199], [200..249] with correct time ranges
