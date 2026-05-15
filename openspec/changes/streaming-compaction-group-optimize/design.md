## Context

The current `processCurrentKey()` in `KeyAwareMergingIterator` follows a three-phase pattern:

1. `readBlocksIncrementally()` — read blocks round-robin, classify via `maxTimeSeen` into `rawChunks` (non-overlapping) and `pending` (possibly overlapping)
2. Phase 2 fast path — single block with no tombstones → pass through
3. Phase 3 — `rawChunks` + `pending` sorted, then `len(allBlocks) > 1 || hasTombstones` → decode+merge ALL

Phase 3 is the bottleneck: non-overlapping blocks that `maxTimeSeen` correctly routed to `rawChunks` are decoded and re-encoded anyway, because they share a key with overlapping blocks. For example, blocks A[1-1000], B[2000-3000], C[1500-2500]: A is non-overlapping with both B and C, but gets decoded because it's in the same allBlocks list.

### Why `maxTimeSeen` classification is imperfect

Due to round-robin reading, a block's `minTime > maxTimeSeen` at the moment of reading doesn't guarantee it's non-overlapping with ALL blocks for the key:

```
File 0: A[1-1000],    B[2000-3000]
File 1:                C[1500-2500]

Reading order: A(File0) → C(File1) → B(File0)

A: minTime=1    > maxTimeSeen(-∞) → rawChunks
C: minTime=1500 > maxTimeSeen(1000) → rawChunks  ← misclassified!
B: minTime=2000 < maxTimeSeen(2500) → pending
```

C and B overlap, but C was placed in `rawChunks`. Phase 3 must handle this by reconsidering rawChunks alongside pending.

### Correctness of pass-through for non-overlapping singletons

If two blocks have non-overlapping time ranges, their timestamp sets are strictly disjoint:
```
∀ ta ∈ A, tb ∈ B: ta ≤ A.maxTime < B.minTime ≤ tb  →  ta ≠ tb
```
Therefore a singleton group with no tombstones can safely pass through — there are no duplicate timestamps to deduplicate.

## Goals / Non-Goals

**Goals:**
- Non-overlapping singleton blocks within a key MUST pass through as raw mmap data
- Only overlapping groups MUST be decoded and merged
- Output MUST be identical to batch path
- No per-key allocation for the allBlocks buffer

**Non-Goals:**
- Changing the batch path
- Changing the `maxTimeSeen` classification in Phase 1 (it remains a heuristic)
- Handling partial block reads (reading only overlapped portions within a block)

## Decisions

### Decision 1: Group-based overlap detection

**Choice:** After sorting allBlocks by minTime, scan left-to-right. Maintain a `groupStart` index. A new block starts a new group if it does NOT overlap any block in `[groupStart, i)`. Otherwise it extends the current group.

**Why:** This partitions blocks into independent overlap chains. Blocks in different groups have strictly disjoint time ranges — no dedup needed between groups. Within a group, overlapping blocks undergo the normal decode+merge path.

**Design:**
```go
groupStart := 0
for i := 1; i <= len(allBlocks); i++ {
    // allBlocks[i] starts a new group if it doesn't overlap any block in [groupStart, i)
    startNewGroup := i == len(allBlocks)
    if !startNewGroup {
        overlaps := false
        for j := groupStart; j < i && !overlaps; j++ {
            if allBlocks[i].overlapsTimeRange(allBlocks[j].minTime, allBlocks[j].maxTime) {
                overlaps = true
            }
        }
        startNewGroup = !overlaps
    }
    if !startNewGroup { continue }

    group := allBlocks[groupStart:i]
    if len(group) > 1 || hasTombstones {
        decodeAndMerge(group)  // existing logic
    } else {
        passThrough(group[0])  // raw mmap bytes → outputBlock
    }
    groupStart = i
}
```

**Alternatives considered:**
- Fix `maxTimeSeen` to be perfect: Requires reading ALL blocks before classifying any, defeats the purpose of incremental processing
- Pairwise overlap check on allBlocks: Would still need grouping for correctness; the transitive grouping is the natural extension

### Decision 2: Reusable allBlocks buffer

**Choice:** Add `allBlocks streamBlocks` field to `KeyAwareMergingIterator`. Reuse via `m.allBlocks = append(m.allBlocks[:0], ...)`.

**Why:** Avoids `make([]*streamBlock, 0, len(rawChunks)+len(pending))` per key. The backing array grows to the maximum needed capacity on the first key, then is reused for all subsequent keys.

### Decision 3: True round-robin (one block per file per pass)

**Choice:** In `readBlocksIncrementally()`, read at most ONE block per file per outer loop pass. Previously the inner loop consumed all blocks from a file's buffer in one pass.

**Why:** This ensures `maxTimeSeen` sees blocks from all files interleaved, making it more likely to correctly classify non-overlapping blocks early. With batch-per-file reading, all blocks from file 0 are consumed before seeing file 1, delaying overlap detection.

## Risks / Trade-offs

- **[Correctness]** Grouping assumes non-overlapping time ranges ⇒ no duplicate timestamps. This is mathematically proven given TSM's guarantee that `minTime`/`maxTime` are exact block boundaries. The batch path has relied on `overlapsTimeRange` for dedup decisions for years.
- **[Edge case]** All blocks overlap → one big group, same as current behavior. No regression.
- **[Edge case]** Single key with 100 non-overlapping blocks → 100 singleton groups, all pass through. Previously all 100 would be decoded. This is the best-case improvement.
- **[Complexity]** `processGroups()` is ~70 lines vs the old ~30-line Phase 3. The control flow is more nuanced but the invariants are documented.
