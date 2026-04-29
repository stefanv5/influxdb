# InfluxDB TSM 流式 Compaction 设计

## 1. 背景与问题

### 1.1 当前实现问题

当前 `tsmBatchKeyIterator` 的实现在大 Shard 场景下存在严重的内存问题：

```
当前 tsmBatchKeyIterator 的数据结构:

type tsmBatchKeyIterator struct {
    readers []*TSMReader           // 所有 TSM readers
    blocks  blocks                 // ⚠️ 所有同 key 的 blocks 全量累积
    buf     []blocks              // ⚠️ 每个 reader 一个 block 缓冲区
    merged  blocks                // 已合并的 blocks

    // ⚠️ OOM 的根源：解码后的值全量存储
    mergedFloatValues    *tsdb.FloatArray
    mergedIntegerValues  *tsdb.IntegerArray
    mergedUnsignedValues *tsdb.UnsignedArray
    mergedBooleanValues  *tsdb.BooleanArray
    mergedStringValues   *tsdb.StringArray
}
```

### 1.2 内存问题分析

| 数据结构 | 问题 | 内存占用 |
|----------|------|----------|
| `k.blocks` | 累积所有同 key 的 blocks | O(all blocks for one key) |
| `mergedFloatValues` | 解码后的值全量存储 | O(all values for one key) |
| `allValues` (mergeFloat) | 排序时的临时数组 | O(all values) |
| `tsmGeneration.files` | 一个 generation 的所有文件 | O(files per gen) |

### 1.3 大 Shard 场景估算

假设大 Shard 配置：
- 10,000 个 series
- 每个 series 100,000 个 point
- 10 个 TSM 文件

```
单次 compaction 的内存峰值:
- 假设同 key 跨 10 个文件，平均每个 key 10 个 blocks
- 每 block 1000 points
- 每 point 约 50 bytes (Float)

单 key 内存: 10 blocks × 1000 points × 50 bytes × 2 (decode buffer) = 1 MB
假设 10% 的 key 同时在处理: 1000 keys × 1 MB = 1 GB

但实际可能更大:
- 很多 key 的 blocks 可能同时累积在 k.blocks
- 解码时的临时对象
- GC 开销
```

---

## 2. 设计目标

### 2.1 核心目标

1. **消除全量 block 累积**: 不再等待所有同 key blocks 齐备
2. **消除全量值存储**: 不再将所有值 decode 到内存
3. **边读边吐**: 实现 Value 级别的流式 merge

### 2.2 性能目标

| 指标 | 当前 | 目标 |
|------|------|------|
| 内存峰值 | O(values_per_key) | O(1) 或 O(file_count) |
| 大 Shard OOM | 容易发生 | 不发生 |
| 延迟 | 低延迟但高内存 | 稳定低延迟 |

### 2.3 兼容性目标

1. 保持现有 CompactionPlan 不变
2. 保持 TSM 文件格式不变
3. 保持 Compactor 接口不变
4. 结果与当前实现一致（相同的数据合并结果）

---

## 3. 核心设计

### 3.1 设计原则

```
当前流程 (Block-level Aggregate):
  读取所有同 key 的 blocks → k.blocks (累积)
  ↓
  解码所有 blocks → mergedFloatValues (全量存储)
  ↓
  合并/排序/去重 → allValues (临时数组)
  ↓
  重新编码 → 输出

流式流程 (Value-level Merge):
  Value 级别的 k-way merge → 边读边吐 → 输出
           ↓
  优势: 不需要等所有 block，不需要全量解码
```

### 3.2 架构总览

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        流式 Compaction 架构                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                      CompactionPlan                              │    │
│  │                      (不变，沿用现有)                           │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                 │                                       │
│                                 ▼                                       │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                      Compactor.Run                             │    │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐               │    │
│  │  │ TSMReader1 │  │ TSMReader2 │  │ TSMReaderN │               │    │
│  │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘               │    │
│  │         │               │               │                        │    │
│  │         └───────────────┼───────────────┘                        │    │
│  │                         ▼                                        │    │
│  │  ┌─────────────────────────────────────────────────────────┐   │    │
│  │  │              BlockValueIterator (per reader)            │   │    │
│  │  │                                                          │   │    │
│  │  │  BlockIterator → Lazy Decode → Value Iterator           │   │    │
│  │  │                    (按需解码，不累积)                   │   │    │
│  │  └─────────────────────────────────────────────────────────┘   │    │
│  │                         │                                       │    │
│  │                         ▼                                       │    │
│  │  ┌─────────────────────────────────────────────────────────┐   │    │
│  │  │            MergingValueIterator                         │   │    │
│  │  │                                                          │   │    │
│  │  │  min-heap                                                │   │    │
│  │  │  ┌──────────────────────────────────────────────────┐  │   │    │
│  │  │  │ iter1.value(ts=100)                                │  │   │    │
│  │  │  │ iter2.value(ts=150)                                │  │   │    │
│  │  │  │ iter3.value(ts=120)                                │  │   │    │
│  │  │  └──────────────────────────────────────────────────┘  │   │    │
│  │  │                                                          │   │    │
│  │  │  Pop(ts=100) → emit → push iter1.next()                │   │    │
│  │  │                                                          │   │    │
│  │  │  Tombstone filter (value-level)                         │   │    │
│  │  │                                                          │   │    │
│  │  │  Deduplicate (by timestamp)                            │   │    │
│  │  └─────────────────────────────────────────────────────────┘   │    │
│  │                         │                                       │    │
│  │                         ▼                                       │    │
│  │  ┌─────────────────────────────────────────────────────────┐   │    │
│  │  │              StreamingBlockEncoder                     │   │    │
│  │  │                                                          │   │    │
│  │  │  Values → Encode → TSMBlock → Write                    │   │    │
│  │  │  (边编码边写，不需等所有值)                            │   │    │
│  │  └─────────────────────────────────────────────────────────┘   │    │
│  │                         │                                       │    │
│  │                         ▼                                       │    │
│  │  ┌─────────────────────────────────────────────────────────┐   │    │
│  │  │              TSMWriter                                   │   │    │
│  │  │              (不变)                                      │   │    │
│  │  └─────────────────────────────────────────────────────────┘   │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. 核心数据结构

### 4.1 ValueIterator 接口

```go
// ValueIterator 逐个 yield 值，避免全量解码
type ValueIterator interface {
    Next() bool           // 前进到下一个有效值
    Read() (Value, error) // 获取当前值
    Err() error           // 获取错误
    Close() error         // 释放资源

    // 辅助方法
    Type() byte                    // 返回数据类型
    Key() []byte                   // 返回当前 key
}

// KeyIterator 保持向后兼容
type KeyIterator interface {
    Next() bool
    Read() (key []byte, minTime, maxTime int64, block []byte, err error)
    Close() error
    Err() error
    EstimatedIndexSize() int
}
```

### 4.2 BlockValueIterator

从 TSM BlockIterator 读取，按需解码为 Value：

```go
type BlockValueIterator struct {
    r       *TSMReader
    iter    *BlockIterator  // 现有 BlockIterator

    // 当前 block 的状态
    currentKey    []byte
    currentType   byte
    currentBlock  []byte      // 原始数据
    tombstones     []TimeRange // tombstone 范围

    // 已解码的值和位置
    decoded   []Value
    pos       int

    // 当前 key 的时间范围
    readMin, readMax int64

    err error
}

func (b *BlockValueIterator) Next() bool {
    // 1. 如果当前 block 还有未读的值，消耗它
    for b.pos < len(b.decoded) {
        b.pos++
        if b.pos < len(b.decoded) {
            // 检查 tombstone - 只有真正被删除的值才跳过
            v := b.decoded[b.pos]
            if !b.isDeleted(v) {
                return true
            }
            // 被删除，继续
        }
    }

    // 2. 读取下一个 block（lazy decode）
    if !b.iter.Next() {
        return false
    }

    key, minTime, maxTime, typ, _, raw, err := b.iter.Read()
    if err != nil {
        b.err = err
        return false
    }

    b.currentKey = key
    b.currentType = typ
    b.currentBlock = raw
    b.tombstones = b.r.TombstoneRange(key)
    b.readMin = minTime
    b.readMax = maxTime

    // 按需解码
    b.decoded = DecodeBlock(raw)
    b.pos = 0

    // 检查第一个值是否被删除
    if b.pos < len(b.decoded) && b.isDeleted(b.decoded[b.pos]) {
        continue // 跳过
    }

    return len(b.decoded) > 0 && b.pos < len(b.decoded)
}

func (b *BlockValueIterator) isDeleted(v Value) bool {
    ts := v.UnixNano()
    for _, tr := range b.tombstones {
        if tr.Min <= ts && ts <= tr.Max {
            return true
        }
    }
    return false
}

func (b *BlockValueIterator) Read() (Value, error) {
    if b.err != nil {
        return nil, b.err
    }
    if b.pos >= len(b.decoded) {
        return nil, fmt.Errorf("no value to read")
    }
    return b.decoded[b.pos], nil
}
```

### 4.3 MergingValueIterator

核心的 k-way merge 实现，使用最小堆：

```go
type valueEntry struct {
    value     Value
    iterator  *BlockValueIterator
    timestamp int64
}

type valueHeap []*valueEntry

func (h valueHeap) Len() int            { return len(h) }
func (h valueHeap) Less(i, j int) bool   { return h[i].timestamp < h[j].timestamp }
func (h valueHeap) Swap(i, j int)        { h[i], h[j] = h[j], h[i] }
func (h *valueHeap) Push(x any)          { *h = append(*h, x.(*valueEntry)) }
func (h *valueHeap) Pop() any            { old := *h; n := len(old); x := old[n-1]; *h = old[0 : n-1]; return x }

type MergingValueIterator struct {
    iterators []*BlockValueIterator
    heap     valueHeap

    // 当前输出的值
    current Value
    currentKey []byte
    currentType byte

    // 去重状态
    lastTimestamp int64
    lastValue    Value

    err error
}

func NewMergingValueIterator(iters []*BlockValueIterator) *MergingValueIterator {
    m := &MergingValueIterator{
        iterators: iters,
        heap:      make(valueHeap, 0, len(iters)),
        lastTimestamp: math.MaxInt64,
    }

    // 初始化堆，填入每个 iterator 的第一个值
    for _, iter := range iters {
        if iter.Next() {
            v, _ := iter.Read()
            m.heap = append(m.heap, &valueEntry{
                value:     v,
                iterator:  iter,
                timestamp: v.UnixNano(),
            })
        }
    }
    heap.Init(&m.heap)

    return m
}

func (m *MergingValueIterator) Next() bool {
    for {
        // 1. 如果有上次的 lastValue（同 timestamp 的旧值），丢弃它
        if m.lastValue != nil {
            m.lastValue = nil
        }

        // 2. 从堆顶取最小值
        if len(m.heap) == 0 {
            return false
        }

        entry := heap.Pop(&m.heap).(*valueEntry)

        // 3. 推进该 iterator 到下一个值
        if entry.iterator.Next() {
            v, _ := entry.iterator.Read()
            entry.value = v
            entry.timestamp = v.UnixNano()
            heap.Push(&m.heap, entry)
        }

        // 4. 去重逻辑：检查是否是新的 timestamp
        if entry.timestamp != m.lastTimestamp {
            // 遇到新 timestamp，上一个 lastValue 是"最新的"
            if m.lastValue != nil {
                m.currentKey = entry.iterator.Key()
                m.currentType = entry.iterator.Type()
                m.current = m.lastValue
                return true
            }
            m.lastTimestamp = entry.timestamp
        }

        // 5. 更新 lastValue（保留最新值）
        // 由于堆的特性，新进入的值 timestamp >= 旧值
        m.lastValue = entry.value
    }
}

func (m *MergingValueIterator) Read() (Value, error) {
    return m.current, nil
}

func (m *MergingValueIterator) Key() []byte {
    return m.currentKey
}

func (m *MergingValueIterator) Type() byte {
    return m.currentType
}
```

### 4.4 StreamingKeyIterator

将 MergingValueIterator 适配为 KeyIterator 接口：

```go
type StreamingKeyIterator struct {
    mergeIter *MergingValueIterator

    // 当前 block 编码状态
    encoder   Encoder  // 根据类型选择
    buf       []Value
    size      int      // 当前 block 的值数量
    maxSize   int      // 最大值数量

    currentKey    []byte
    currentMinTime int64
    currentMaxTime int64

    err error
}

func (s *StreamingKeyIterator) Next() bool {
    for {
        // 1. 如果当前 block 满了，encode 并返回
        if s.size >= s.maxSize {
            return true
        }

        // 2. 从 merge iterator 取下一个值
        if !s.mergeIter.Next() {
            // merge 完成
            if s.size > 0 {
                // 还有未输出的值
                return true
            }
            return false
        }

        v, _ := s.mergeIter.Read()

        // 3. 检查是否是新 key
        if s.currentKey == nil || !bytes.Equal(s.currentKey, s.mergeIter.Key()) {
            if s.size > 0 {
                // 当前 block 有数据，作为上一个 key 的结尾
                return true
            }
            // 新 key
            s.currentKey = s.mergeIter.Key()
            s.currentMinTime = v.UnixNano()
        }

        s.currentMaxTime = v.UnixNano()
        s.buf = append(s.buf, v)
        s.size++

        if s.size >= s.maxSize {
            return true
        }
    }
}

func (s *StreamingKeyIterator) Read() (key []byte, minTime, maxTime int64, block []byte, err error) {
    if s.size == 0 {
        return nil, 0, 0, nil, nil
    }

    // Encode 当前 block
    block = s.encoder.Encode(s.buf[:s.size])
    minTime = s.currentMinTime
    maxTime = s.currentMaxTime
    key = s.currentKey

    // 重置状态
    s.buf = s.buf[:0]
    s.size = 0

    return key, minTime, maxTime, block, nil
}
```

---

## 5. 关键设计决策

### 5.1 Lazy Decode vs Full Decode

```
当前 (full decode):
┌──────────────────────────────────────────────────┐
│  k.blocks = [blk1, blk2, blk3, blk4, ...]        │
│  ↓                                               │
│  for _, blk := range k.blocks {                  │
│      decoded := decodeBlock(blk.b)  // 全解码   │
│      allValues = append(allValues, decoded...)  │
│  }                                               │
│  allValues.Sort()  // 全量排序                   │
│  allValues.Deduplicate()  // 全量去重            │
└──────────────────────────────────────────────────┘

流式 (lazy decode):
┌──────────────────────────────────────────────────┐
│  heap = [iter1.next(), iter2.next(), iter3.next()]│
│  ↓                                                │
│  for {                                            │
│      entry := heap.PopMin()  // 取最小 timestamp │
│      emit(entry.value)                            │
│                                                   │
│      // 按需推进 iterator                         │
│      if entry.iterator.HasNext() {               │
│          next := entry.iterator.Next()           │
│          heap.Push(next)                         │
│      }                                           │
│  }                                               │
└──────────────────────────────────────────────────┘
```

**优势:**
- 不需要等待所有同 key 的 blocks
- 不需要全量存储解码后的值
- 内存使用量恒定为 O(file_count)

### 5.2 Tombstone 处理

```
当前 (block-level skip):
┌──────────────────────────────────────────────────┐
│  blk.tombstones = [[100, 200]]                   │
│  if blk.minTime >= 100 && blk.maxTime <= 200 {   │
│      skip entire block  // 整个 block 跳过      │
│  }                                               │
└──────────────────────────────────────────────────┘

问题: 如果 block 时间范围是 [150, 250]，只有部分 tombstone，
      当前逻辑会错误地跳过整个 block

流式 (value-level filter):
┌──────────────────────────────────────────────────┐
│  for value in block {                            │
│      for _, ts := range block.tombstones {       │
│          if ts.Min <= value.ts <= ts.Max {       │
│              skip this value  // 只跳过被删除的值│
│              continue                          │
│          }                                     │
│      }                                         │
│      emit value                                 │
│  }                                               │
└──────────────────────────────────────────────────┘
```

**优势:**
- 精确到每个值，不误删
- 充分利用 tombstone 信息

### 5.3 去重逻辑

```
问题: 同一 timestamp 的值可能来自不同 TSM 文件（更新写入）

当前逻辑:
1. 收集所有同 timestamp 的值
2. 按顺序处理，取最后一个（最新的）

流式逻辑:
1. 用 lastTimestamp 追踪当前正在处理的 timestamp
2. 同一 timestamp 的第一个值被标记为候选（lastValue）
3. 当遇到新 timestamp 时，上一个候选值就是"最新的"
4. 后续同 timestamp 的值被丢弃

示例:
  TSM1: [ts=100, val=1], [ts=200, val=2]
  TSM2: [ts=150, val=3], [ts=200, val=4]  <- 同一 timestamp 200 的更新

  处理流程:
  1. heap = [TSM1.ts=100, TSM2.ts=150]
  2. Pop 100 → emit 100, push TSM1.ts=200
     heap = [TSM2.ts=150, TSM1.ts=200]
  3. Pop 150 → emit 150, push TSM2.ts=200
     heap = [TSM1.ts=200, TSM2.ts=200]
  4. Pop 200 (TSM1) → lastValue = TSM1.val=2
     Pop 200 (TSM2) → lastValue = TSM2.val=4 (更新)
     遇到新 timestamp (∞)，emit lastValue = 4
```

### 5.4 Block 大小控制

```
问题: 流式处理无法预知整个 key 的数据量

解决方案:
1. 预设 maxSize（如 1000 points per block）
2. 当达到 maxSize 时，先 emit 当前 block
3. 继续从下一个值开始新的 block

Trade-off:
- block 太小：索引开销大，压缩率低
- block 太大：内存峰值增加

建议: 保持与现有实现相同的 maxSize (DefaultMaxPointsPerBlock = 1000)
```

---

## 6. 内存对比

| 场景 | 当前实现 | 流式实现 |
|------|----------|----------|
| 10 files, 1000 keys/file, 100 blocks/key | 峰值: 10,000 blocks + 1B values | O(10) values |
| 单 key 跨 10 files | 需要等所有 blocks | 边读边处理 |
| 大 Shard (10000 series) | 容易 OOM | 稳定 O(1) |

**具体数值对比:**

```
假设:
- 10 个 TSM 文件
- 1000 个 key
- 每个 key 100 个 block
- 每个 block 1000 个值

当前:
- k.blocks 累积: 10 files × 1000 keys × 100 blocks = 10,000,000 blocks (实际上同 key 同时累积)
- mergedFloatValues: 1000 keys × 100 blocks × 1000 values × 50 bytes = 5 GB

流式:
- heap: 10 个 valueEntry
- current block: 1000 values × 50 bytes = 50 KB
- 总内存: O(10) = 几 KB ~ 几 MB
```

---

## 7. 性能优化点

### 7.1 预分配内存池

```go
var valueEntryPool = sync.Pool{
    New: func() any {
        return &valueEntry{}
    },
}

func (m *MergingValueIterator) pushToHeap(v Value, iter *BlockValueIterator) {
    entry := valueEntryPool.Get().(*valueEntry)
    entry.value = v
    entry.iterator = iter
    entry.timestamp = v.UnixNano()
    heap.Push(&m.heap, entry)
}

func (m *MergingValueIterator) popFromHeap() *valueEntry {
    entry := heap.Pop(&m.heap).(*valueEntry)
    valueEntryPool.Put(entry)
    return entry
}
```

### 7.2 批量解码优化

```go
type BlockValueIterator struct {
    // ... existing fields ...

    // 批量解码缓存
    decodeCache []Value
}
```

### 7.3 时间窗口限制

如果某个 key 的数据量过大，可以限制内存使用：

```go
const MaxValuesPerKey = 100000

type StreamingKeyIterator struct {
    // ... existing fields ...

    valuesSeen int64
}

func (s *StreamingKeyIterator) Next() bool {
    // 如果超过阈值，先 emit 当前 block
    if s.valuesSeen >= MaxValuesPerKey {
        if s.size > 0 {
            return true
        }
        // 跳过剩余值...（或实现分段）
    }
    // ...
}
```

---

## 8. 实现步骤

### Phase 1: ValueIterator 接口定义

```
任务:
1. 定义 ValueIterator 接口
2. 实现 BlockValueIterator (基于现有 BlockIterator)
3. 添加 tombstone per-value 过滤

文件:
- tsdb/engine/tsm1/iterator.go (新增)

测试:
- TestBlockValueIterator_Next
- TestBlockValueIterator_Tombstone
```

### Phase 2: MergingValueIterator

```
任务:
1. 实现 valueHeap 类型
2. 实现 MergingValueIterator
3. 实现 deduplicate logic
4. 添加 Value 缓存池

文件:
- tsdb/engine/tsm1/merge_iterator.go (新增)

测试:
- TestMergingValueIterator_BasicMerge
- TestMergingValueIterator_Deduplicate
- TestMergingValueIterator_Tombstone
```

### Phase 3: StreamingKeyIterator

```
任务:
1. 实现 Encoder 接口和具体实现
2. 实现 StreamingKeyIterator
3. 处理 block size 限制
4. 适配 KeyIterator 接口

文件:
- tsdb/engine/tsm1/stream_iterator.go (新增)

测试:
- TestStreamingKeyIterator_Basic
- TestStreamingKeyIterator_BlockSize
- TestStreamingKeyIterator_KeyBoundary
```

### Phase 4: 集成与替换

```
任务:
1. 替换 compact.go 中的 tsmBatchKeyIterator 使用
2. 保留 tsmBatchKeyIterator 作为 fallback (fast mode)
3. 添加配置选项选择实现
4. 端到端测试

文件:
- tsdb/engine/tsm1/compact.go 修改
- tsdb/engine/tsm1/compact_test.go 修改

测试:
- TestCompactor_Streamed vs TestCompactor_Batch
- 大 Shard OOM 测试
- 性能基准测试
```

### Phase 5: 优化与调优

```
任务:
1. 性能 profiling
2. 内存优化
3. 边界情况处理
4. 文档更新
```

---

## 9. 兼容性考虑

### 9.1 配置

```go
type Config struct {
    // 流式 compaction 启用
    EnableStreamingCompaction bool

    // 流式模式下每个 block 的最大值数量
    // 保持与现有实现兼容
    StreamingMaxPointsPerBlock int
}
```

### 9.2 回退策略

如果流式实现出现问题，应该能够回退到现有实现：

```go
func (c *Compactor) compact(fast bool, tsmFiles []string, ...) {
    if c.enableStreaming && !fast {
        return c.compactStream(tsmFiles, ...)
    }
    return c.compactBatch(tsmFiles, ...)  // 现有实现
}
```

### 9.3 现有代码兼容

```
保留现有实现:
- tsmBatchKeyIterator 保留用于 fast compaction (CompactFast)
- 只在 full compaction (CompactFull) 使用流式

这样:
- 不破坏现有逻辑
- 可以逐步迁移
- 便于对比测试
```

---

## 10. 潜在风险与缓解

### 10.1 风险: block 粒度变小

**风险:** 流式处理可能导致相同 key 的数据分散在更多 block 中，影响压缩率

**缓解:**
- 设置合适的 maxSize
- 在 block 边界检查是否可以合并（时间相邻）

### 10.2 风险: tombstone 处理复杂化

**风险:** per-value tombstone 检查增加 CPU 开销

**缓解:**
- tombstone 通常很少
- 可以缓存 tombstone 信息
- 使用 bloom filter 快速跳过无 tombstone 的 block

### 10.3 风险: 去重逻辑错误

**风险:** 流式去重可能丢失最新值

**缓解:**
- 使用 lastTimestamp 机制确保同 timestamp 的最后一个值被选中
- 添加充分测试覆盖各种边界情况

---

## 11. 附录: 关键代码路径对比

### 现有实现

```
tsmBatchKeyIterator.Next()
  │
  ├─► 读取所有 buf[i] 的 blocks (累积到 k.blocks)
  │
  ├─► 找到 min key
  │
  ├─► 收集所有同 key blocks → k.blocks
  │
  └─► k.merge()
        │
        └─► mergeFloat()
              │
              ├─► decode all blocks
              ├─► append to allValues
              ├─► sort allValues
              └─► deduplicate & re-encode
```

### 流式实现

```
StreamingKeyIterator.Next()
  │
  ├─► MergingValueIterator.Next()
  │     │
  │     ├─► heap.Pop() 取得最小 timestamp
  │     │
  │     ├─► iterator.Next() 推进
  │     │
  │     └─► heap.Push() 重新插入
  │
  └─► 检查 block 是否满
        │
        └─► 如果满，encode 并返回
```

---

*文档版本: 1.0*
*最后更新: 2026-04-29*
