## Context

The current `processCurrentKey()` in `KeyAwareMergingIterator` follows a three-phase pattern:

1. `readBlocksForKey()` — collect ALL blocks for the key from ALL files into `m.blocks`
2. `needsDedup()` — sort and check pairwise overlap
3. Pass-through (non-overlapping) or decode+merge (overlapping)

Phase 1 is the bottleneck: it buffers every block regardless of whether it overlaps. For a key with 100 blocks across 3 files where only 3 blocks overlap, all 100 are buffered.

The batch iterator (`tsmBatchKeyIterator`) has the same pattern — it also collects all blocks before processing. This is not a regression from the streaming refactor; it's an inherent limitation of both approaches.

## Goals / Non-Goals

**Goals:**
- Non-overlapping blocks MUST be output immediately upon reading, without waiting for sibling files
- Overlapping blocks MUST be buffered and processed via the existing decode+merge path
- Memory peak for non-overlapping keys MUST be O(1) per file, not O(totalBlocks)
- Output MUST be identical to the current approach (same blocks, same order within a key)

**Non-Goals:**
- Changing the `KeyIterator` interface
- Modifying the batch iterator
- Supporting incremental decode within a single overlapping group (full merge still requires all overlapping blocks)

## Decisions

### Decision 1: maxTimeSeen tracking for incremental overlap detection

**Choice**: Maintain a running `maxTimeSeen` across all blocks read for the current key. When a new block's `minTime > maxTimeSeen`, it cannot overlap with any previously read block and can be output immediately.

**Why**: Within a single TSM file, blocks for the same key are sorted by time and never overlap. Overlap can only occur between blocks from different files. If block B.minTime > maxTimeSeen (the maximum maxTime of all blocks read so far from all files), then B cannot overlap with any block from any file.

**Alternative considered**: Check overlap against only blocks from OTHER files. Rejected because `maxTimeSeen` is simpler and equally correct — if B.minTime > maxTimeSeen, B is strictly after everything.

### Decision 2: Two-buffer model (output + pending)

**Choice**: Use two slices:
- `chunks` — non-overlapping blocks already committed to output
- `pending` — blocks that might overlap with each other, awaiting full evaluation

When all blocks for a key have been read:
- If `pending` is empty: `chunks` already contains everything, done
- If `pending` is non-empty: run `needsDedup()` on `pending`, decode+merge if needed, prepend decoded chunks before the non-overlapping ones

**Why**: This cleanly separates the two paths. Non-overlapping blocks never touch the decode path. Overlapping blocks are isolated in `pending`.

**Alternative considered**: Single pass with inline decode. Rejected because it complicates the merge logic (overlapping blocks may need to be merged with previously decoded blocks).

### Decision 3: Per-file incremental reading

**Choice**: Instead of reading all blocks from each file in a nested loop, read ONE block at a time round-robin across files. After each block, check overlap and decide: output or buffer.

**Why**: Round-robin ensures we see blocks from all files early, enabling early detection of non-overlapping blocks. If file A has blocks [1,100], [101,200] and file B has [150,250], we can output A[1,100] immediately after reading it (before reading B's block).

**Alternative considered**: Read all blocks from one file first, then the next. Rejected because we'd buffer all of file A's blocks before seeing file B's, delaying the overlap check.

### Decision 4: pending blocks sorted before overlap check

**Choice**: After collecting all blocks for a key, sort `pending` by minTime and run the same `needsDedup()` logic as before. If no overlap within `pending`, pass them through. If overlap, decode+merge.

**Why**: Reuses existing, tested logic. The only change is that `pending` is smaller than the full `m.blocks` would have been.

## Risks / Trade-offs

**[Risk] Round-robin adds iterator management complexity** → Each file's iterator state must be tracked independently. Mitigation: The `BlockValueIterator` already manages per-file state; we just need to read one block at a time instead of all at once.

**[Risk] Output order may differ from current** → Currently all blocks from file 0 come before file 1. With round-robin, blocks are interleaved. Mitigation: Within a key, blocks are sorted by time (not file) before output. The `KeyIterator` contract only requires sorted keys and correct data, not file ordering.

**[Risk] Edge case: all blocks overlap** → If every block overlaps, `pending` contains all blocks — same as current. No regression, just no improvement.

**[Trade-off] Slightly more complex control flow** → The incremental loop is harder to reason about than collect-all-then-process. Mitigated by clear separation of output vs pending buffers.
