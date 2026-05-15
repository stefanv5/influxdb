# Findings

## 流式 Compact 内存模型

### BlockValueIterator
- 每个实例以 mmap 切片方式持有 1 个 block 的原始编码数据，不解码
- 加上 key/tombstones 元数据: ~1 KB
- 总计: 每个 iterator 持有 mmap 引用，几乎零堆分配

### KeyAwareMergingIterator
- Per-file buffer: `buf[i]` 持有 pool 的 `streamBlock` 指针
- Per-file block pool: `blockPool[i]` 最多 `maxPoolPerFile=8` 个复用对象
- Merged typed arrays: `mergedFloat`, `mergedInteger` 等，跨 key 复用底层数组
- Output chunks: 轻量 `outputBlock` (56 bytes)，解码后立即归还 `streamBlock` 到池
- 4 文件场景: N × 17 KB + 复用数组 ≈ **< 100 KB** 稳态内存

---

## 非重叠多 Block 场景分析

### 场景设定

3 个文件，同一个 key `cpu`，时间完全不重叠：

```
File 0: block A — [1,   1000]  (1000 个 unique timestamps, 已编码为 A.b)
File 1: block B — [2000, 3000]  (1000 个 unique timestamps, 已编码为 B.b)
File 2: block C — [4000, 5000]  (1000 个 unique timestamps, 已编码为 C.b)
```

**关键事实：三个 block 的时间范围无交集。不同 block 之间不存在重复 timestamp，没有 dedup 需求，没有重叠需要 merge。**

### 当前代码的数据流

#### Phase 1: `readBlocksIncrementally` — round-robin 逐 block 读取

```
maxTimeSeen = -∞

第 1 轮:
  File0 → A: minTime=1    > maxTimeSeen(-∞) → rawChunks, maxTimeSeen = max(-∞, 1000) = 1000
  File1 → B: minTime=2000 > maxTimeSeen(1000) → rawChunks, maxTimeSeen = max(1000, 3000) = 3000
  File2 → C: minTime=4000 > maxTimeSeen(3000) → rawChunks, maxTimeSeen = max(3000, 5000) = 5000

各文件 iterator 耗尽（每个 key 每个文件只有一个 block），循环结束
```

**Phase 1 完成了正确判断：A、B、C 全部不重叠，全进 rawChunks，pending 为空。**

#### Phase 2: 快速路径

```go
// stream_iterator.go:671
if len(m.pending) == 0 && len(m.rawChunks) == 1 && len(m.rawChunks[0].tombstones) == 0 {
```

`len(m.rawChunks)` = 3，不等于 1。条件不满足，进入 Phase 3。

#### Phase 3: `processGroups` — 全量 decode+merge

```go
// stream_iterator.go:719
if len(m.allBlocks) > 1 || hasTombstones {
    m.decodeAndMerge()   // ← 全部解码、合并
    m.chunkMergedArray() // ← 重新编码
}
```

**执行过程：**

```
Step 1: DecodeFloatArrayBlock(A.b) → []int64 × 1000 + []float64 × 1000  (堆分配 ~16KB)
Step 2: DecodeFloatArrayBlock(B.b) → []int64 × 1000 + []float64 × 1000  (堆分配 ~16KB)
Step 3: DecodeFloatArrayBlock(C.b) → []int64 × 1000 + []float64 × 1000  (堆分配 ~16KB)

Step 4: FloatArray.Merge: 对 3000 个元素做 3 次归并排序 + 复制 (~48KB 临时内存)
Step 5: chunkMergedArray: 按 bufSize=1000 切分 → 3 次 EncodeFloatArrayBlock → 3 个重新编码的 block
```

**产生的 3 个 output block 与原始 A.b, B.b, C.b 中的数据完全相同**——白白走了一圈 decode→merge→re-encode。

### 为什么 decode+merge 是不必要的

| | Block A | Block B | Block C |
|--|---------|---------|---------|
| 时间范围 | [1, 1000] | [2000, 3000] | [4000, 5000] |
| 与 A 有交集？ | - | 否 (2000 > 1000) | 否 (4000 > 1000) |
| 与 B 有交集？ | 否 | - | 否 (4000 > 3000) |
| 有 tombstone？ | 无 | 无 | 无 |
| 需要 merge？ | 否 | 否 | 否 |

Merge 只在以下情况下需要：
1. **时间范围重叠** — 两个 block 的 `[minTime, maxTime]` 有交集，可能存在重复 timestamp 需要 dedup
2. **Tombstone 存在** — block 中有数据需要被删除
3. **跨文件相同 timestamp** — 不同文件在相同时间点有不同值，需要选最新文件的值

A、B、C 三条都不满足。它们只是同一个 key 在不同时间段的三个独立 block。

### 零 Decode 路径（应该做的）

原始数据已经以编码形式存在于 mmap 中——`A.b`, `B.b`, `C.b` 是直接可用的字节切片。只需要：

```go
m.chunks = []outputBlock{
    {key: "cpu", minTime: 1,    maxTime: 1000, b: A.b},  // mmap 直接引用
    {key: "cpu", minTime: 2000, maxTime: 3000, b: B.b},  // mmap 直接引用
    {key: "cpu", minTime: 4000, maxTime: 5000, b: C.b},  // mmap 直接引用
}
// A, B, C 的 streamBlock 归还 pool
```

**零解码、零堆分配、零 re-encode。** 与 decode+merge 路径对比：

| | Decode+Merge 路径 | 零 Decode 路径 |
|--|-------------------|----------------|
| Block 解码 | 3 × DecodeFloatArrayBlock | 0 |
| 堆分配 | ~48KB (3×16KB 解码 + merge 临时) | 0 |
| Merge 排序 | 对 3000 个元素排序 | 0 |
| Re-encode | 3 × EncodeFloatArrayBlock | 0 |
| 输出 | 重新编码的 bytes | 直接引用 mmap |
| 数据正确性 | 正确 | 正确 |

**用图表示：**

```
当前代码的数据流:
  Encoded(A.b) ──decode──→ FloatArray ──┐
  Encoded(B.b) ──decode──→ FloatArray ──┤ Merge → re-encode → Chunk1, Chunk2, Chunk3
  Encoded(C.b) ──decode──→ FloatArray ──┘

零 decode 的数据流:
  Encoded(A.b) ───→ outputBlock  ───→ 直接写入
  Encoded(B.b) ───→ outputBlock  ───→ 直接写入
  Encoded(C.b) ───→ outputBlock  ───→ 直接写入
```

### `maxTimeSeen` 的「假阳性」问题

`readBlocksIncrementally` 中的 `maxTimeSeen` 分类并不完美。由于 round-robin 跨文件读取，会出现误分类：

```
File 0: A[1-1000],    B[3000-4000]
File 1:                C[2000-2500]

读取顺序: A(File0) → C(File1) → B(File0)

A: minTime=1    > maxTimeSeen(-∞) → rawChunks, maxTimeSeen=1000
C: minTime=2000 > maxTimeSeen(1000) → rawChunks, maxTimeSeen=2500   ← 误分类!
B: minTime=3000 > maxTimeSeen(2500) → rawChunks, maxTimeSeen=4000
```

C 的 `minTime=2000` 确实大于当时的 `maxTimeSeen=1000`，被判定为"不重叠"。但它与后面来的 B 有重叠（B 和 C 属于不同维度：B 来自 File0 但它的 minTime=3000 > C.maxTime=2500，所以实际不重叠）。

**真正的危险场景：**

```
File 0: A[1-1000],    B[2000-3000]  ← B 在 File0 中在 A 后面，key 相同
File 1:                C[1500-2500]

读取顺序: A(File0) → C(File1) → B(File0)

A: minTime=1    > maxTimeSeen(-∞) → rawChunks, maxTimeSeen=1000
C: minTime=1500 > maxTimeSeen(1000) → rawChunks, maxTimeSeen=max(1000,2500)=2500
B: minTime=2000 < maxTimeSeen(2500) → pending  ← B 与 C 重叠!

结果: rawChunks=[A, C], pending=[B]
```

此时 `pending` 非空，进入 processGroups。A[1-1000] 被牵连进 decode+merge——它本来完全可以 pass-through。

### 分组处理的解决方案

在 `processGroups` 中，不把 rawChunks + pending 当作一个整体，而是按**重叠关系**分组：

```go
allBlocks = [A(1-1000), C(1500-2500), B(2000-3000)]  // 按 minTime 排序

分组检测:
  C.minTime(1500) > A.maxTime(1000) → 不重叠 → A 自成一组
  B.minTime(2000) ≤ C.maxTime(2500) → 重叠 → C 和 B 同组

组 1: [A] — 单 block, 无 tombstone → pass-through
组 2: [C, B] — 多 block, 重叠 → decode+merge
```

**只有真正重叠的 block 才走 decode+merge 路径。**

### 两个版本的对比

#### 当前版本 `processGroups`

```go
if len(m.allBlocks) > 1 || hasTombstones {
    // 多 block 就全部 decode+merge
    m.decodeAndMerge()
    m.chunkMergedArray()
}
```

逻辑：block 数 > 1 → 全部 decode+merge。
优点：简单，不可能出错。
缺点：`maxTimeSeen` 正确分类了的不重叠 block（rawChunks 中有多个）被无差别解码。

#### 分组处理版本

```go
groupStart := 0
for i := 1; i <= len(allBlocks); i++ {
    // 检测 allBlocks[i] 是否与组内任何 block 重叠
    startNewGroup := i == len(allBlocks) || !allBlocks[i].overlapsAny(allBlocks[groupStart:i])
    if startNewGroup {
        group := allBlocks[groupStart:i]
        if len(group) > 1 || groupHasTombstones {
            decode+merge(group)
        } else {
            passThrough(group[0])
        }
        groupStart = i
    }
}
```

逻辑：按重叠关系分组，每组独立决策。
优点：不重叠的 block 零开销 pass-through。
缺点：代码行数多 ~30 行。

### 性能影响评估

| 场景 | 当前版本 | 分组版本 |
|------|---------|---------|
| 单 block key（最高频） | Phase 2 pass-through，零开销 ✓ | 同 ✓ |
| 多 block, 全部重叠 | decode+merge 全部 | decode+merge 全部（等价） |
| 多 block, 完全不重叠 | **decode+merge 全部**（浪费） | pass-through 全部 ✓ |
| 多 block, 部分重叠 | **decode+merge 全部**（浪费） | 只 merge 重叠组 ✓ |

在 CompactFull 场景（合并多个 snapshot/level 的文件），不同文件通常覆盖相似的时间范围，block 大概率重叠，所以当前版本的浪费在实际生产中可能不显著。但在以下场景浪费明显：

- 增量 compact 合并不重叠的时间分片
- 跨 generation 的全量合并
- 数据迁移/重平衡引出不同时间段的文件

### 代码中已具备的其他正确设计

#### `outputBlock` 与 `streamBlock` 的解耦

```go
// outputBlock — 轻量（56 bytes），只含 TSM writer 需要的字段
type outputBlock struct {
    key              []byte
    minTime, maxTime int64
    b                []byte    // mmap 切片直接引用
}

// streamBlock — 完整（~96 bytes），含池化管理、readMin/readMax、tombstones、fileIdx
type streamBlock struct {
    key                    []byte
    minTime, maxTime       int64
    typ                    byte
    b                      []byte
    tombstones             []TimeRange
    readMin, readMax       int64
    fileIdx                int
}
```

`processCurrentKey` 完成后，`streamBlock` 立即归还池，`outputBlock` 留在 `m.chunks` 直到 `Next()` 逐个消费。这确保了池化对象的最大化复用。

#### `m.allBlocks` 复用

```go
// stream_iterator.go:266
allBlocks streamBlocks  // 可复用缓冲区，避免每个 key 的 make 分配

// stream_iterator.go:696
m.allBlocks = append(m.allBlocks[:0], m.rawChunks...)
m.allBlocks = append(m.allBlocks, m.pending...)
```

底层数组随第一个 key 增长到最终容量，后续所有 key 零分配复用。

#### `readBlocksIncrementally` 的 round-robin

每轮每个文件最多读一个 block，确保 `maxTimeSeen` 尽早看到所有文件的 block：

```go
for {
    readAny := false
    for i, iter := range m.iterators {
        // ...每个文件最多读一个 block...
        iter.NextBlock()  // 无内层 for 循环
        readAny = true
    }
    if !readAny { break }
}
```

这比内层 for 循环一口气读完一个文件的所有同 key block 更公平——能更早检测跨文件重叠。

---

## 关于分组优化的正确性论证

### 质疑

> 分组优化的前提是"时间范围不重叠的 block 之间不存在重复 timestamp"，但这个前提不成立。两个 block 的时间范围 [1,1] 和 [2,2] 不重叠，但 File 0 的 ts=1 和 File 1 的 ts=1 是重复的。

### 结论：这个质疑不成立

#### 1. TSM Block 的时间范围是精确的数学边界

TSM 文件格式保证：对于任意 block，其内部所有 timestamp 都在 `[minTime, maxTime]` 闭区间内：

```
∀ block b:  ∀ ts ∈ b.values:  b.minTime ≤ ts ≤ b.maxTime
```

这不是运行时估算值，而是 block 写入时记录的精确元数据。

#### 2. 时间范围不重叠 ⇒ timestamp 集合无交集（直接证明）

设有两个 block A 和 B，其时间范围不重叠。不失一般性，设：

```
A.maxTime < B.minTime
```

对 A 中任意 timestamp `ta` 和 B 中任意 timestamp `tb`：

```
ta ≤ A.maxTime < B.minTime ≤ tb
∴ ta < tb
∴ ta ≠ tb
```

A 和 B 的 timestamp 集合严格分隔，不存在重复。**这是传递性的直接推论，不需要运行时验证。**

#### 3. 反驳 counterexample

用户提出的 counterexample：

> Block A: [1,1], Block B: [2,2]。File 0 的 ts=1 和 File 1 的 ts=1 是重复的。

- Block A[1,1] 只包含 `{ts=1}`
- Block B[2,2] 只包含 `{ts=2}`
- Block A 和 Block B 之间没有重复 timestamp

用户进一步说「File 1 的 ts=1」——但如果 File 1 中真的存在 ts=1，那它必然在 File 1 的**某个 block C** 中。Block C 的时间范围必须覆盖 ts=1，这意味着 C.minTime ≤ 1 ≤ C.maxTime。而 Block A[1,1] 也覆盖 ts=1，所以 A 和 C 的时间范围重叠：

```
A.minTime(1) ≤ C.maxTime(≥1) 且  A.maxTime(1) ≥ C.minTime(≤1)
```

**A 和 C 会被 `overlapsTimeRange` 检测到重叠，归入同一组，走 decode+merge。** 这才是正确的——重复 timestamp 在**时间重叠的 block 之间**产生，分组检测会捕获它。

#### 4. 什么情况才真的需要 merge

| 情况 | 检测方式 | 分组处理 |
|------|---------|---------|
| 两个 block 时间范围重叠 | `overlapsTimeRange` | 同组 → merge |
| block 有 tombstone | `len(blk.tombstones) > 0` | 该 block 需 merge |
| 同 timestamp 不同值（dedup） | 本质是时间重叠 | 同组内 dedup |

时间不重叠的 block 不可能有重复 timestamp，不需要 merge。

#### 5. Batch 路径依赖同样的前提

`tsmBatchKeyIterator.mergeFloat()` 正是这样做判断的（`compact.gen.go:1043-1056`）：

```go
dedup := k.mergedFloatValues.Len() != 0
if len(k.blocks) > 0 && !dedup {
    dedup = len(k.blocks[0].tombstones) > 0 || k.blocks[0].partiallyRead()

    // ★ 只有相邻 block 时间重叠才触发 dedup ★
    for i := 1; !dedup && i < len(k.blocks); i++ {
        dedup = k.blocks[i].overlapsTimeRange(k.blocks[i-1].minTime, k.blocks[i-1].maxTime) || ...
    }
}
```

`dedup=false` 时（block 间无时间重叠且无 tombstone），走 pass-through 路径：

```go
// compact.gen.go:1134-1154
for ; i < len(k.blocks); i++ {
    if count < k.size { break }
    k.merged = append(k.merged, k.blocks[i])  // 直接 pass-through，不解码
}
```

**Batch 路径从来没有「多 block ⇒ 无条件 merge」的逻辑。** `dedup` flag 只在存在时间重叠或 tombstone 时置 true。流式路径当前 `len(m.allBlocks) > 1 → merge` 是比 batch 路径更保守的简化实现，不是正确性要求。

#### 6. 分组算法的正确性（归纳证明）

**不变式：** 处理完 `allBlocks[groupStart:i)` 这组后，组内所有 timestamp 已正确 dedup；且组间的 timestamp 集合严格分隔。

**初始化：** groupStart=0，空组满足不变式。

**归纳步：** 假设不变式在处理完上一组后成立。当前组为 `allBlocks[groupStart:i)`。

块 `allBlocks[i]` 与当前组的关系检测：

```go
overlaps := false
for j := groupStart; j < i && !overlaps; j++ {
    if allBlocks[i].overlapsTimeRange(allBlocks[j].minTime, allBlocks[j].maxTime) {
        overlaps = true
    }
}
```

- **如果 overlaps=true**：`allBlocks[i]` 与组内至少一个 block 时间重叠，可能存在重复 timestamp → 加入当前组，继续扩展。
- **如果 overlaps=false**：`allBlocks[i]` 与组内**所有** block 都不重叠。根据 §2 的证明，`allBlocks[i]` 中的任何 timestamp 不可能出现在组内任何 block 中。当前组可以安全结算，`allBlocks[i]` 开始新组。

组结算：
- `len(group) == 1 && !hasTombstones`：组内只有一个 block，不可能有跨 block 的重复 timestamp，无 tombstone → pass-through
- `len(group) > 1 || hasTombstones`：组内可能有重叠，需要 decode+merge

因为 `allBlocks` 按 minTime 排序，处理顺序就是时间顺序，最终 `m.chunks` 自然有序。

#### 7. 总结

当前代码的 `len(m.allBlocks) > 1 → decode+merge` 是**保守的简化策略**，不是正确性约束。它把不重叠的多 block 做了无用的 decode+merge，但保证结果正确。

分组优化是安全的，前提是：

```
overlapsTimeRange 检查  +  minTime/maxTime 元数据精确性
```

这两个条件在 TSM 文件格式中都是保证的，batch 路径已依赖它们十年。分组优化只是把 batch 路径的同款逻辑搬到了流式路径。
