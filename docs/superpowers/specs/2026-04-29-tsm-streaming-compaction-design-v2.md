# InfluxDB TSM 流式 Compaction 设计 (v2)

> 本文档基于 v1 设计文档的审查结果修订，修复了 3 个 CRITICAL、3 个 HIGH、4 个 MEDIUM 级别的问题。
> 变更记录见 [附录 A: 审查问题清单](#附录-a-审查问题清单)。

## 1. 背景与问题

### 1.1 当前实现问题

当前 `tsmBatchKeyIterator` 的实现在大 Shard 场景下存在内存问题：

```go
// tsdb/engine/tsm1/compact.go:1595-1647
type tsmBatchKeyIterator struct {
    readers   []*TSMReader
    iterators []*BlockIterator
    blocks    blocks              // 当前 key 的所有 blocks
    buf       []blocks            // 每个 reader 的 block 缓冲区
    merged    blocks              // 已合并待输出的 blocks

    // OOM 根源：解码后的值全量存储
    mergedFloatValues    *tsdb.FloatArray   // 当前 key 的所有解码值
    mergedIntegerValues  *tsdb.IntegerArray
    mergedUnsignedValues *tsdb.UnsignedArray
    mergedBooleanValues  *tsdb.BooleanArray
    mergedStringValues   *tsdb.StringArray
    // ...
}
```

### 1.2 内存问题根因

| 问题 | 位置 | 说明 |
|------|------|------|
| `FloatArray.Merge` 分配热点 | `tsdb/cursors/arrayvalues.gen.go:142-203` | 每次 merge-sort 分配 `a.Len()+b.Len()` 新数组 |
| 全量 block 解码 | `compact.gen.go:1091-1127` | `DecodeFloatArrayBlock` 将整个 block 解码到内存 |
| 同 key 多 block 累积 | `compact.go:1819-1827` | 收集同 key 的所有 blocks 后才开始合并 |
| 编码缓冲区未池化 | `encoding.gen.go:462` | `TODO(edd): These need to be pooled` |

### 1.3 大 Shard 场景估算

假设场景：10 个 TSM 文件，单个 key 跨所有文件，每个文件 100 个 block 属于该 key，每 block 1000 values。

```
当前实现内存峰值:
  k.blocks 收集:    10 files × 100 blocks = 1,000 blocks
  解码:             1,000 blocks × 1,000 values × 50 bytes = 50 MB
  FloatArray.Merge: 每次分配 a.Len()+b.Len()，峰值可达 100 MB+
  总计:             ~100+ MB（单个 key）

流式实现内存峰值:
  堆:               10 entries = 1 KB
  每个 iterator:     1 decoded block × 1,000 values × 50 bytes = 50 KB
  输出缓冲:         1,000 values = 50 KB
  总计:             ~551 KB（恒定，与 blocks/key 无关）
```

### 1.4 现有优化（v1 文档遗漏）

> **[修复 M1]** v1 文档未提及以下已有优化，流式方案需在此基础上改进。

| 优化 | 位置 | 说明 |
|------|------|------|
| 非重叠 block 直通 | `compact.gen.go:1136-1155` | `dedup=false` 时 full blocks 直接传递，无需解码 |
| Fast 模式 | `compact.gen.go:1157-1168` | `fast=true` 时所有 blocks 直通，用于 level 1-2 |
| 部分 block 消费 | `compact.go:1304-1340` | `readMin/readMax` 支持跨多次 merge 循环的部分消费 |
| 智能 dedup 决策 | `compact.gen.go:1043-1055` | 仅在 block 重叠、有 tombstone 或部分读取时 dedup |
| 编解码器池 | `encoding.go:58-93` | 所有编解码器类型都有池化 |
| k.size chunking | `compact.gen.go:1213-1256` | 限制 decoded values 到 ~2×k.size |

---

## 2. 设计目标

### 2.1 核心目标

1. **消除 `FloatArray.Merge` 的全量分配** — 使用堆合并替代 merge-sort
2. **限制同时解码的 block 数量** — 每个 iterator 只持有 1 个 decoded block
3. **保持 key 边界正确性** — 每个输出 block 只包含单个 key 的值
4. **保持输出有序** — 值按时间戳递增排列

### 2.2 性能目标

| 指标 | 当前 | 目标 |
|------|------|------|
| 单 key 内存峰值 | O(blocks_for_key × block_size) | O(files × block_size) |
| 10 文件 × 100 blocks/key | ~100 MB | ~551 KB |
| FloatArray.Merge 分配 | O(values_for_key) | 消除 |
| 输出有序性 | 保证 | 保证 |

### 2.3 兼容性目标

1. 保持现有 `CompactionPlanner` 接口不变
2. 保持 TSM 文件格式不变
3. 保持 `KeyIterator` 接口不变
4. 保持 fast mode 兼容

---

## 3. 核心设计

### 3.1 架构总览

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        流式 Compaction 架构 (v2)                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  CompactionPlan (不变)                                              │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                 │                                           │
│                                 ▼                                           │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Compactor.compact()                                                │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                 │   │
│  │  │ TSMReader0  │  │ TSMReader1  │  │ TSMReaderN  │                 │   │
│  │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘                 │   │
│  │         │               │               │                          │   │
│  │         ▼               ▼               ▼                          │   │
│  │  ┌─────────────────────────────────────────────────────────────┐  │   │
│  │  │            BlockValueIterator (per reader)                   │  │   │
│  │  │  BlockIterator → 按需解码单个 block → Value Iterator         │  │   │
│  │  │  感知 key 边界：block 耗尽时不自动跨 block                   │  │   │
│  │  └─────────────────────────────────────────────────────────────┘  │   │
│  │                         │                                          │   │
│  │                         ▼                                          │   │
│  │  ┌─────────────────────────────────────────────────────────────┐  │   │
│  │  │         KeyAwareMergingIterator                              │  │   │
│  │  │                                                              │  │   │
│  │  │  1. findMinKey() — 扫描所有 iterator 的 currentKey          │  │   │
│  │  │  2. 只激活 key==minKey 的 iterators                         │  │   │
│  │  │  3. 堆合并 (按 timestamp, fileIdx 排序)                     │  │   │
│  │  │  4. popAndDedup() — 消耗同时间戳，新文件胜出                │  │   │
│  │  │  5. 输出缓冲满 → 编码 → 返回                                │  │   │
│  │  │  6. 当前 key 完成 → 回到步骤 1                              │  │   │
│  │  └─────────────────────────────────────────────────────────────┘  │   │
│  │                         │                                          │   │
│  │                         ▼                                          │   │
│  │  ┌─────────────────────────────────────────────────────────────┐  │   │
│  │  │  encodeBlock() → TSMWriter.WriteBlock()                     │  │   │
│  │  └─────────────────────────────────────────────────────────────┘  │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 与当前流程对比

```
当前流程 (block-level):
  找 min key → 收集同 key 所有 blocks → 解码所有 → FloatArray.Merge → 编码
                                  ↑ 峰值：全量解码 + Merge 分配

流式流程 (value-level, key-aware):
  找 min key → 每个 reader 只解码 1 个 block → 堆合并 → 边编码边输出
                                  ↑ 峰值：N 个单 block + 堆
```

---

## 4. 核心数据结构

### 4.1 BlockValueIterator

> **[修复 C1]** v1 的 `BlockValueIterator.Next()` 调用 `DecodeBlock(raw)` 做全量解码，
> 与"消除全量值存储"的目标矛盾。v2 保持单 block 解码（每个 iterator 一次只解码 1 个 block），
> 但关键改进是：不自动跨 block，由上层控制 key 切换。

```go
// tsdb/engine/tsm1/stream_iterator.go (新增)

// BlockValueIterator 从单个 TSMReader 流式读取值。
// 不自动跨 block：当当前 block 耗尽时返回 false，由上层决定是否读取下一个 block。
type BlockValueIterator struct {
    r       *TSMReader
    iter    *BlockIterator
    fileIdx int  // 文件索引，用于同时间戳去重时确定优先级

    // 当前 block 状态
    currentKey   []byte
    currentType  byte
    tombstones   []TimeRange

    // 已解码的值和位置
    decoded []Value
    pos     int

    // 读取但未解码的下一个 block（用于 key 边界判断）
    nextRaw      []byte
    nextKey      []byte
    nextType     byte
    nextMinTime  int64
    nextMaxTime  int64
    hasNextBlock bool

    err error
}

// Init 读取第一个 block 并初始化 iterator。
// 注意：Init() 不检查 tombstone，即使所有值都被 tombstone 删除也返回 true。
// tombstone 过滤由上层 activateIteratorsForKey() 处理（见 §4.3）。
// 原因：如果 Init() 中循环跳过 tombstone block，当 key 变化时需要缓存下一个 key 的 block，
// 逻辑复杂度与 activateIteratorsForKey() 重复。统一在 activateIteratorsForKey() 处理更简洁。
func (b *BlockValueIterator) Init() bool {
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
    b.tombstones = b.r.TombstoneRange(key)
    b.decoded = DecodeBlock(raw)
    b.pos = -1
    _ = minTime
    _ = maxTime
    return len(b.decoded) > 0
}

// Next 在当前 block 内前进到下一个有效值。
// 如果当前 block 耗尽，返回 false（不自动读取下一个 block）。
func (b *BlockValueIterator) Next() bool {
    for b.pos < len(b.decoded)-1 {
        b.pos++
        v := b.decoded[b.pos]
        if !b.isDeleted(v) {
            return true
        }
    }
    return false  // 当前 block 耗尽
}

// NextBlock 读取下一个 block。
// 返回值：
//   - true: 成功读取下一个 block（可能 key 不同）
//   - false: 没有更多 block 或 key 已变化（此时 hasNextBlock=true 如果有下一个 key 的 block）
func (b *BlockValueIterator) NextBlock() bool {
    if !b.iter.Next() {
        return false
    }
    key, minTime, maxTime, typ, _, raw, err := b.iter.Read()
    if err != nil {
        b.err = err
        return false
    }

    // key 变化：不消费，保存给下一轮使用
    if !bytes.Equal(b.currentKey, key) {
        b.nextKey = key
        b.nextType = typ
        b.nextRaw = raw
        b.nextMinTime = minTime
        b.nextMaxTime = maxTime
        b.hasNextBlock = true
        return false
    }

    // 同 key：解码并继续
    b.currentType = typ
    b.tombstones = b.r.TombstoneRange(key)
    b.decoded = DecodeBlock(raw)
    b.pos = -1
    b.hasNextBlock = false
    _ = minTime
    _ = maxTime
    return len(b.decoded) > 0
}

// ActivatePending 激活之前因 key 变化而缓存的 block。
// 在新的 key 轮次中调用。
func (b *BlockValueIterator) ActivatePending() bool {
    if !b.hasNextBlock {
        return false
    }
    b.currentKey = b.nextKey
    b.currentType = b.nextType
    b.tombstones = b.r.TombstoneRange(b.currentKey)
    b.decoded = DecodeBlock(b.nextRaw)
    b.pos = -1
    b.hasNextBlock = false
    b.nextRaw = nil
    return len(b.decoded) > 0
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
    if b.pos < 0 || b.pos >= len(b.decoded) {
        return nil, fmt.Errorf("no value to read")
    }
    return b.decoded[b.pos], nil
}

func (b *BlockValueIterator) Key() []byte   { return b.currentKey }
func (b *BlockValueIterator) Type() byte    { return b.currentType }
func (b *BlockValueIterator) Err() error    { return b.err }
func (b *BlockValueIterator) FileIdx() int  { return b.fileIdx }
```

**内存特性：** 每个 `BlockValueIterator` 在任何时刻最多持有 1 个 decoded block（~50 KB）。

### 4.2 valueHeap

```go
// valueEntry 堆中的条目
type valueEntry struct {
    value     Value
    iterator  *BlockValueIterator
    timestamp int64
    fileIdx   int  // 文件索引，同时间戳时旧文件先弹出
}

// valueHeap 按 (timestamp, fileIdx) 排序的最小堆
type valueHeap []*valueEntry

func (h valueHeap) Len() int { return len(h) }
func (h valueHeap) Less(i, j int) bool {
    if h[i].timestamp != h[j].timestamp {
        return h[i].timestamp < h[j].timestamp
    }
    return h[i].fileIdx < h[j].fileIdx  // 同时间戳：旧文件先弹出
}
func (h valueHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *valueHeap) Push(x any)         { *h = append(*h, x.(*valueEntry)) }
func (h *valueHeap) Pop() any {
    old := *h
    n := len(old)
    x := old[n-1]
    *h = old[:n-1]
    return x
}
```

> **[修复 H2]** v1 堆仅按 timestamp 排序，同时间戳值弹出顺序不确定。
> v2 使用 `(timestamp, fileIdx)` 复合排序，确保同时间戳时旧文件先弹出，
> 新文件的值自然成为 dedup "赢家"。

### 4.3 KeyAwareMergingIterator

> **[修复 C3]** v1 的 `MergingValueIterator` 缺乏 key 边界感知，
> 堆会混合不同 key 的值，违反 TSM block 格式。
> v2 引入 key 收集层：先找 min key，只合并 key 匹配的 iterators。

```go
// KeyAwareMergingIterator key 感知的流式合并迭代器。
// 每次只合并单个 key 的值，key 处理完毕后切换到下一个 key。
type KeyAwareMergingIterator struct {
    iterators []*BlockValueIterator
    heap      valueHeap

    currentKey  []byte
    currentType byte
    initialized bool

    // 输出缓冲
    buf     []Value
    bufSize int  // DefaultMaxPointsPerBlock = 1000

    err error
}

func NewKeyAwareMergingIterator(iters []*BlockValueIterator, bufSize int) *KeyAwareMergingIterator {
    m := &KeyAwareMergingIterator{
        iterators: iters,
        heap:      make(valueHeap, 0, len(iters)),
        bufSize:   bufSize,
    }
    return m
}

// Next 实现 KeyIterator 接口。返回 true 表示有数据可读。
func (m *KeyAwareMergingIterator) Next() bool {
    // 1. 如果缓冲区有数据，直接返回
    if len(m.buf) > 0 {
        return true
    }

    // 2. 初始化：读取每个 iterator 的第一个 block
    if !m.initialized {
        m.initialized = true
        for _, iter := range m.iterators {
            iter.Init()
        }
    }

    // 3. 循环处理 keys
    for {
        // 找到当前所有 iterator 中的最小 key
        if m.currentKey == nil {
            minKey := m.findMinKey()
            if minKey == nil {
                return false  // 所有 iterator 耗尽
            }
            m.currentKey = minKey
            m.activateIteratorsForKey(minKey)
        }

        // 4. 从堆中流式合并值
        for len(m.heap) > 0 {
            best := m.popAndDedup()
            m.buf = append(m.buf, best)

            if len(m.buf) >= m.bufSize {
                return true  // 缓冲区满
            }
        }

        // 5. 堆空 → 当前 key 处理完毕，切换到下一个 key
        m.currentKey = nil
        if len(m.buf) > 0 {
            return true  // 还有未编码的值
        }
    }
}

// findMinKey 扫描所有 iterator 的 currentKey，返回字典序最小的 key。
func (m *KeyAwareMergingIterator) findMinKey() []byte {
    var minKey []byte
    for _, iter := range m.iterators {
        if iter.currentKey == nil && iter.hasNextBlock {
            // 激活上一轮缓存的 block
            iter.ActivatePending()
        }
        if iter.currentKey != nil {
            if minKey == nil || bytes.Compare(iter.currentKey, minKey) < 0 {
                minKey = iter.currentKey
            }
        }
    }
    // 复制 key（因为 iterator 的 key 可能在后续操作中被修改）
    if minKey != nil {
        keyCopy := make([]byte, len(minKey))
        copy(keyCopy, minKey)
        return keyCopy
    }
    return nil
}

// activateIteratorsForKey 激活 key 匹配的 iterators，将第一个有效值推入堆。
// 循环处理：如果当前 block 的所有值都被 tombstone 删除，自动调用 NextBlock() 读取下一个 block。
// 当 NextBlock() 导致 key 变化时，停止该 iterator（不推入堆），等待后续 key 轮次处理。
func (m *KeyAwareMergingIterator) activateIteratorsForKey(key []byte) {
    m.currentType = 0
    for _, iter := range m.iterators {
        if !bytes.Equal(iter.currentKey, key) {
            continue  // key 不匹配，跳过
        }

        if m.currentType == 0 {
            m.currentType = iter.currentType
        }

        // 循环直到成功 push 有效值或 iterator 真正耗尽
        for {
            if !iter.Next() {
                // 当前 block 耗尽，尝试下一个 block
                if !iter.NextBlock() {
                    break  // 没有更多 block
                }
                // NextBlock 可能导致 key 变化
                if !bytes.Equal(iter.currentKey, key) {
                    break  // key 变化，不推入堆，留给后续轮次
                }
                continue  // 同 key 新 block，继续尝试
            }

            v, _ := iter.Read()
            if iter.isDeleted(v) {
                continue  // tombstone 删除，跳过
            }

            // 成功取到有效值，推入堆
            heap.Push(&m.heap, &valueEntry{
                value:     v,
                iterator:  iter,
                timestamp: v.UnixNano(),
                fileIdx:   iter.fileIdx,
            })
            break
        }
    }
}

// popAndDedup 从堆中弹出所有同时间戳的值，保留最新文件的值。
// 保证输出按时间戳递增。
func (m *KeyAwareMergingIterator) popAndDedup() Value {
    entry := heap.Pop(&m.heap).(*valueEntry)
    currentTS := entry.timestamp
    best := entry.value
    bestIdx := entry.fileIdx

    // 推进该 iterator 并可能 push 新值
    m.advanceAndPush(entry)

    // 消耗所有同时间戳的条目
    for len(m.heap) > 0 && m.heap[0].timestamp == currentTS {
        entry = heap.Pop(&m.heap).(*valueEntry)
        if entry.fileIdx > bestIdx {
            best = entry.value  // 更新文件的值胜出
            bestIdx = entry.fileIdx
        }
        m.advanceAndPush(entry)
    }

    return best
}

// advanceAndPush 推进 iterator 并将下一个值推入堆。
func (m *KeyAwareMergingIterator) advanceAndPush(entry *valueEntry) {
    iter := entry.iterator

    // 尝试在当前 block 内推进
    if iter.Next() {
        v, _ := iter.Read()
        entry.value = v
        entry.timestamp = v.UnixNano()
        heap.Push(&m.heap, entry)
        return
    }

    // 当前 block 耗尽，尝试读取下一个 block（同 key）
    if iter.NextBlock() {
        if iter.Next() {
            v, _ := iter.Read()
            entry.value = v
            entry.timestamp = v.UnixNano()
            heap.Push(&m.heap, entry)
            return
        }
    }

    // iterator 耗尽或 key 即将变化，不 push
}

// Read 实现 KeyIterator 接口。返回编码后的 block 数据。
func (m *KeyAwareMergingIterator) Read() (key []byte, minTime, maxTime int64, data []byte, err error) {
    if len(m.buf) == 0 {
        return nil, 0, 0, nil, nil
    }

    values := m.buf
    m.buf = m.buf[:0]

    // 计算时间范围
    minTime = values[0].UnixNano()
    maxTime = values[len(values)-1].UnixNano()

    // 按类型编码
    data, err = encodeValues(m.currentType, values)
    if err != nil {
        return nil, 0, 0, nil, err
    }

    return m.currentKey, minTime, maxTime, data, nil
}

func (m *KeyAwareMergingIterator) Close() error    { return nil }
func (m *KeyAwareMergingIterator) Err() error      { return m.err }
func (m *KeyAwareMergingIterator) EstimatedIndexSize() int { return 0 }
```

### 4.4 encodeValues

> **[修复 M2]** v1 使用虚构的 `Encoder` 接口。v2 使用类型分发调用现有批量编码函数。

```go
// encodeValues 根据类型将 Value 切片编码为 TSM block 数据。
func encodeValues(typ byte, values []Value) ([]byte, error) {
    switch typ {
    case BlockFloat64:
        arr := tsdb.NewFloatArrayLen(len(values))
        for i, v := range values {
            arr.Timestamps[i] = v.UnixNano()
            arr.Values[i] = v.(FloatValue).value
        }
        return EncodeFloatArrayBlock(arr)

    case BlockInteger:
        arr := tsdb.NewIntegerArrayLen(len(values))
        for i, v := range values {
            arr.Timestamps[i] = v.UnixNano()
            arr.Values[i] = v.(IntegerValue).value
        }
        return EncodeIntegerArrayBlock(arr)

    case BlockUnsigned:
        arr := tsdb.NewUnsignedArrayLen(len(values))
        for i, v := range values {
            arr.Timestamps[i] = v.UnixNano()
            arr.Values[i] = v.(UnsignedValue).value
        }
        return EncodeUnsignedArrayBlock(arr)

    case BlockBoolean:
        arr := tsdb.NewBooleanArrayLen(len(values))
        for i, v := range values {
            arr.Timestamps[i] = v.UnixNano()
            arr.Values[i] = v.(BooleanValue).value
        }
        return EncodeBooleanArrayBlock(arr)

    case BlockString:
        arr := tsdb.NewStringArrayLen(len(values))
        for i, v := range values {
            arr.Timestamps[i] = v.UnixNano()
            arr.Values[i] = v.(StringValue).value
        }
        return EncodeStringArrayBlock(arr)

    default:
        return nil, fmt.Errorf("unsupported block type: %d", typ)
    }
}
```

---

## 5. 关键设计决策

### 5.1 Key 边界处理

> **[修复 C3]** 这是 v1 最严重的架构缺陷。

```
v1 问题:
  MergingValueIterator 按时间戳全局合并，堆中混合不同 key 的值。
  当 key 从 "cpu" 切换到 "mem" 时，可能还有 "cpu" 的值在堆中未消费。

v2 方案:
  采用与当前 tsmBatchKeyIterator 相同的 key 边界处理模式：

  1. findMinKey() — 扫描所有 iterator 的 currentKey，找字典序最小
  2. 只激活 key==minKey 的 iterators
  3. 堆合并该 key 的所有值
  4. 堆空 → 当前 key 完成 → 回到步骤 1

  key 不匹配的 iterator 保持不动，其 block 数据保留在内存中。
```

**示例：不同文件有不同 key 集合**

```
File 0: [cpu: ts=100,200]  [mem: ts=300,400]
File 1: [cpu: ts=150,250]  [disk: ts=500,600]
File 2: [net: ts=100,200]  [disk: ts=350,450]

初始化: 读取每个 iterator 的第一个 block
  iter0: currentKey="cpu"
  iter1: currentKey="cpu"
  iter2: currentKey="net"

处理 key="cpu": findMinKey → "cpu"
  只激活 iter0, iter1（key=="cpu"）
  iter2 不参与（currentKey="net"）
  堆合并: ts=100,150,200,250

  iter0 cpu 耗尽 → NextBlock → key="mem"（缓存，不消费）
  iter1 cpu 耗尽 → NextBlock → key="disk"（缓存，不消费）
  堆空，当前 key 完成

处理 key="disk": findMinKey → min("mem","disk","net") = "disk"
  只激活 iter1（key=="disk"）
  iter0 (mem), iter2 (net) 不参与
  输出: ts=500,600

处理 key="mem": findMinKey → min("mem","net") = "mem"
  只激活 iter0（key=="mem"）
  输出: ts=300,400

处理 key="net": findMinKey → "net"
  只激活 iter2（key=="net"）
  输出: ts=100,200
```

### 5.2 去重逻辑

> **[修复 C2]** v1 的去重逻辑存在数据丢失 bug：堆空时最后的 `lastValue` 丢失。
> v2 采用"消耗所有同时间戳条目"策略，无数据丢失风险。

```
v1 问题:
  使用 lastTimestamp/lastValue 机制。
  当三个 iterator 都有 ts=200 且都在此次耗尽时：
  1. Pop A (ts=200) → lastValue=A
  2. Pop B (ts=200) → lastValue=B (覆盖 A)
  3. Pop C (ts=200) → lastValue=C
  4. 堆空 → 返回 false → C 的值丢失！

v2 方案 (popAndDedup):
  1. Pop entry → currentTS = entry.timestamp, best = entry.value
  2. Advance iterator, push next value
  3. While heap[0].timestamp == currentTS:
     Pop entry → 更新 best（如果 fileIdx 更新）
     Advance iterator, push next value
  4. Return best

  保证：所有同时间戳的值都被消耗，最新文件的值胜出。
```

**有序性保证：**

```
前提: TSM block 内部值严格按时间戳递增（delta encoding 要求）

堆不变量: 每次 push 的新值 timestamp ≥ 刚弹出的值
  情况1: 同 block 内推进 → block 内有序 → ts_new > ts_old ✓
  情况2: 跨 block 推进 → 下一个 block.minTime ≥ 当前 block.maxTime ✓

推论: 堆弹出顺序天然按时间戳递增 → 输出有序 ✓
```

### 5.3 Tombstone 处理

> **[修复 M3]** v1 声称当前代码"错误地跳过整个 block"，这是不准确的。
> 当前代码在 compaction 路径中已正确处理部分 tombstone（逐值排除）。

#### 5.3.1 Tombstone 文件生命周期

```
创建                    应用                      清理
  │                      │                        │
  ▼                      ▼                        ▼
DELETE 请求          TSMReader 初始化          Compaction 完成
  │                      │                        │
  ▼                      ▼                        ▼
Tombstoner.AddRange   applyTombstones()       FileStore.Replace()
  │                      │                        │
  ▼                      ▼                        ▼
.tombstone.tmp        index.DeleteRange()     os.Remove(.tombstone)
  │
  ▼
Flush() → 原子重命名 → .tombstone
```

**Tombstone 文件格式（v4，当前版本，`tombstone.go`）：**

```
文件命名: 0000001.tsm → 0000001.tombstone

┌──────────┬─────────────────────────────────────────┐
│ Header   │  gzip stream 1: [entry][entry]...       │
│ 0x1504   │  gzip stream 2: [entry][entry]...  ←追加│
│ (4 bytes)│  gzip stream N: ...                     │
└──────────┴─────────────────────────────────────────┘

每个 entry 编码 (tombstone.go:715-731):
┌──────────┬──────────┬──────────┬──────────┐
│ key_len  │   key    │   min    │   max    │
│ (4 bytes)│ (N bytes)│ (int64)  │ (int64)  │
└──────────┴──────────┴──────────┴──────────┘
```

v4 支持增量追加多个 gzip stream，避免每次删除重写整个文件。
`lastAppliedOffset`（tombstone.go:55）记录上次读取位置，避免重新处理已应用的 tombstones。

#### 5.3.2 Tombstone 创建

```go
// reader.go:462-473 (TSMReader.Delete)
// reader.go:594-624 (TSMReader.BatchDelete)
func (t *TSMReader) DeleteRange(keys []string, min, max int64) error {
    t.tombstoner.AddRange(keys, min, max)  // 写入 .tombstone.tmp
    return t.tombstoner.Flush()             // 原子重命名为 .tombstone
}

// tombstone.go:91-150 (AddRange)
func (t *Tombstoner) AddRange(keys []string, min, max int64) {
    t.prepareV4()          // 准备追加模式
    for _, key := range keys {
        t.writeTombstone(key, min, max)  // 写入 gzip stream
    }
}
```

#### 5.3.3 Tombstone 应用：TSMReader 初始化

```go
// reader.go:222-247 (NewTSMReader)
func NewTSMReader(f *os.File) (*TSMReader, error) {
    index, _ := t.accessor.init()                    // 解析 index
    t.tombstoner = NewTombstoner(t.Path(), index.ContainsKey)
    t.applyTombstones()                              // ← 应用 tombstones
}

// reader.go:259-297 (applyTombstones)
func (t *TSMReader) applyTombstones() {
    t.tombstoner.Walk(func(v uint32, ts Tombstone) {
        // 按相同 [Min, Max] 分批，每批最多 4096 个 key
        batch = append(batch, ts.Key)
        if batch 满了 || Min/Max 变化 {
            t.index.DeleteRange(batch, prev.Min, prev.Max)
        }
    })
}
```

**`DeleteRange` 两种处理方式**（reader.go:989-1123）：

| 场景 | 处理方式 | 效果 |
|------|---------|------|
| tombstone 覆盖 key 的所有 entries | `Delete()` — 从 index 中删除 key | 后续查询找不到该 key |
| tombstone 只覆盖部分时间范围 | 存入内存 `tombstones map[string][]TimeRange` | 查询时通过 `TombstoneRange()` 获取 |

#### 5.3.4 Tombstone 应用：Compaction 路径

当前代码在 compaction 中的 tombstone 处理（compact.gen.go、compact.go）：

```go
// 1. 读取 block 时加载 tombstone (compact.go:1738)
tombstones := iter.r.TombstoneRange(key)  // 从内存 map 获取
k.blocks = append(k.blocks, &block{
    key: key, tombstones: tombstones,
})

// 2. tombstone 触发 dedup 路径 (compact.gen.go:1046)
dedup = len(k.blocks[0].tombstones) > 0  // 有 tombstone 就必须 dedup

// 3. 逐值排除 (compact.gen.go:1122-1124)
for _, ts := range k.blocks[i].tombstones {
    v.Exclude(ts.Min, ts.Max)
}

// 4. 空 key 跳过 (compact.go:1835-1839)
if len(k.merged) == 0 {
    goto RETRY  // tombstone 删除了 key 的所有值，跳过
}
```

#### 5.3.5 Tombstone 应用：查询路径

查询路径（`mmapAccessor.readAll()`，reader.go:1490-1536）有两级处理：

```go
// 1. Block 级别跳过 (reader.go:1507-1516)
if t.Min <= block.MinTime && t.Max >= block.MaxTime {
    skip = true  // tombstone 覆盖整个 block，不解码
}

// 2. Value 级别排除 (reader.go:1528-1530)
for _, t := range tombstones {
    temp = Values(temp).Exclude(t.Min, t.Max)  // 逐值排除
}
```

#### 5.3.6 Tombstone 清理：Compaction 完成后

```go
// file_store.go:812-878 (FileStore.replace)
for _, file := range oldFiles {
    // 无引用: 直接删除
    file.Remove()
    // → reader.go:414-432:
    //   os.RemoveAll(path)          // 删除 .tsm
    //   t.tombstoner.Delete()       // 删除 .tombstone

    // 有引用: TSM 改名，tombstone 立即删除
    os.Rename(path, path+".tmp")
    os.Remove(tombstonePath)
}
```

**关键：新 compaction 输出文件没有 tombstone 文件。** Tombstone 在 compaction 过程中被"消费"——应用到值级别后，输出只包含未被删除的数据。

#### 5.3.7 Tombstone 对 Plan 的影响

tombstone 的存在会**强制触发 compaction**（compact.go）：

| Plan 方法 | 有 tombstone 时的行为 | 位置 |
|-----------|---------------------|------|
| `FullyCompacted()` | 返回 false | compact.go:216 |
| `PlanLevel()` | 不提前返回；文件数不足也加入 plan | compact.go:246, 307 |
| `PlanOptimize()` | 大文件不跳过；文件数不足也加入 plan | compact.go:342, 354, 393 |
| `Plan()` | 触发重新 plan；大文件不跳过 | compact.go:477, 485, 502, 559, 586 |

#### 5.3.8 流式方案的 Tombstone 处理

```go
// stream_iterator.go (新增)

func (b *BlockValueIterator) Init() bool {
    // ...
    b.tombstones = b.r.TombstoneRange(key)  // 加载 tombstone
    b.decoded = DecodeBlock(raw)
    // ...
}

func (b *BlockValueIterator) Next() bool {
    for b.pos < len(b.decoded)-1 {
        b.pos++
        v := b.decoded[b.pos]
        if !b.isDeleted(v) {  // 逐值检查 tombstone
            return true
        }
    }
    return false
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
```

**与当前代码对比：**

| 方面 | 当前代码 | 流式方案 |
|------|---------|---------|
| tombstone 来源 | `TombstoneRange(key)` | 相同 |
| 应用时机 | 解码后 `v.Exclude(ts.Min, ts.Max)` | 逐值 `isDeleted(v)` |
| 空 key 处理 | `goto RETRY` (compact.go:1835) | 堆空，findMinKey 跳过该 key |
| block 级别跳过 | 查询路径有，compaction 路径无 | 无（与当前 compaction 一致） |
| 效果 | 等价 | 等价 |

**流式方案的额外优化点：** 由于 `isDeleted()` 在 `Next()` 中逐值调用，被 tombstone 覆盖的值不会进入堆，也不会进入输出缓冲。相比当前代码先解码所有值再调用 `Exclude()` 排除，流式方案避免了 tombstone 范围内值的解码和内存分配。

#### 5.3.9 Init() 的 Tombstone 边界问题

> **问题：** 当 `Init()` 读取的第一个 block 中所有值都被 tombstone 删除时，iterator 会陷入"卡住"状态。

```
场景：
  Block A: [ts=100]  (被 tombstone 覆盖)
  Block B: [ts=200]  (有效值)

Init() 行为：
  decoded = [ts=100]  (有值，返回 true)
  Next() → ts=100 被 tombstone → 返回 false
  但不会触发 NextBlock() 读取 Block B

结果：
  iterator.currentKey = "cpu" (有值，findMinKey 不会跳过)
  iterator 无法贡献任何值到堆
  Block B 永远不会被读取
```

**解决方案：在 `activateIteratorsForKey()` 中处理（方案 B）**

`Init()` 的职责仅限于初始化第一个 block，不承担 tombstone 过滤。tombstone 过滤统一在 `activateIteratorsForKey()` 中通过循环处理：

```go
// activateIteratorsForKey 中的 tombstone 处理循环
for {
    if !iter.Next() {
        // 当前 block 无有效值，尝试下一个 block
        if !iter.NextBlock() {
            break  // 没有更多 block
        }
        if !bytes.Equal(iter.currentKey, key) {
            break  // key 变化，留给后续轮次
        }
        continue
    }

    v, _ := iter.Read()
    if iter.isDeleted(v) {
        continue  // tombstone 跳过
    }

    // 找到有效值，推入堆
    heap.Push(&m.heap, &valueEntry{...})
    break
}
```

**设计理由：**
1. `Init()` 只负责初始化第一个 block，职责单一
2. `activateIteratorsForKey()` 是 key 轮次入口，在这里统一处理 tombstone 更合适
3. tombstone 全覆盖 block 是极端场景，不影响正常路径性能
4. 与 `advanceAndPush()` 中的 `NextBlock()` 复用相同的 block 切换逻辑

### 5.4 Block 大小控制

```go
// 输出 block 的最大值数量
const DefaultMaxPointsPerBlock = 1000

// KeyAwareMergingIterator.bufSize 控制每个输出 block 的最大值数量。
// 与当前 tsmBatchKeyIterator 的 k.size 参数一致。
//
// Trade-off:
//   block 太小：索引开销大，压缩率低
//   block 太大：输出缓冲内存增加
//   建议：保持 DefaultMaxPointsPerBlock = 1000
```

### 5.5 内存分析

> **[修复 H3]** v1 声称当前实现使用 "10,000,000 blocks" 内存，这是不准确的。
> 当前代码每次只处理一个 key（compact.go:1696-1842）。

```
当前实现（单 key 处理）:
  k.blocks:         O(blocks_for_one_key) 个 blocks
  mergedFloatValues: O(values_for_one_key)，受 k.size chunking 限制
  FloatArray.Merge:  每次分配 a.Len()+b.Len()
  峰值:             ~100 MB (10 files × 100 blocks/key × 1000 values)

流式实现:
  堆:               O(files) 个 entries = ~1 KB
  decoded blocks:   O(files × block_size) = ~500 KB
  输出缓冲:         O(bufSize) = ~50 KB
  峰值:             ~551 KB（恒定）

关键区别:
  当前: 同时持有所有 blocks 的 decoded values → O(blocks/key × block_size)
  流式: 每个 iterator 只持有 1 个 decoded block → O(files × block_size)
```

---

## 6. 性能优化

### 6.1 值池化

```go
var valueEntryPool = sync.Pool{
    New: func() any { return &valueEntry{} },
}

func (m *KeyAwareMergingIterator) pushToHeap(v Value, iter *BlockValueIterator) {
    entry := valueEntryPool.Get().(*valueEntry)
    entry.value = v
    entry.iterator = iter
    entry.timestamp = v.UnixNano()
    entry.fileIdx = iter.fileIdx
    heap.Push(&m.heap, entry)
}
```

### 6.2 编码缓冲区池化

> 当前 `EncodeFloatArrayBlock` 的中间缓冲区未池化（encoding.gen.go:462 TODO）。

```go
var encodeBufPool = sync.Pool{
    New: func() any { return make([]byte, 0, 64*1024) },
}
```

### 6.3 Fast Mode 兼容

```go
// compact() 中的分发逻辑
func (c *Compactor) compact(fast bool, tsmFiles []string) ([]string, error) {
    if fast {
        // fast mode: 保持现有实现，直接传递 blocks
        tsm := NewTSMBatchKeyIterator(size, true, intC, tsmFiles, trs...)
        return c.writeNewFiles(maxGen, maxSeq, tsmFiles, tsm, true)
    }

    // full mode: 使用流式实现
    tsm := NewStreamingKeyIterator(trs, tsmFiles, size)
    return c.writeNewFiles(maxGen, maxSeq, tsmFiles, tsm, true)
}
```

---

## 7. 实现步骤

### Phase 1: BlockValueIterator

```
任务:
1. 实现 BlockValueIterator 结构体
2. 实现 Init(), Next(), NextBlock(), ActivatePending()
3. 实现 tombstone per-value 过滤
4. 测试 key 边界行为

文件: tsdb/engine/tsm1/stream_iterator.go (新增)

测试:
- TestBlockValueIterator_Basic
- TestBlockValueIterator_Tombstone — tombstone 覆盖整个 block
- TestBlockValueIterator_TombstonePartial — tombstone 覆盖部分时间范围
- TestBlockValueIterator_TombstoneMultiple — 同 key 多个 tombstone ranges
- TestBlockValueIterator_TombstoneFullKey — tombstone 删除整个 key
- TestBlockValueIterator_KeyBoundary
- TestBlockValueIterator_MultiBlock
- TestBlockValueIterator_TombstoneAllDeleted — 第一个 block 所有值被 tombstone，Next() 返回 false
- TestBlockValueIterator_TombstoneMultipleBlocks — 连续多个 block 所有值都被 tombstone
```

### Phase 2: KeyAwareMergingIterator

```
任务:
1. 实现 valueHeap (带 fileIdx 排序)
2. 实现 KeyAwareMergingIterator
3. 实现 findMinKey(), activateIteratorsForKey()
4. 实现 popAndDedup(), advanceAndPush()
5. 实现 encodeValues()

文件: tsdb/engine/tsm1/stream_iterator.go (同文件)

测试:
- TestKeyAwareMergingIterator_BasicMerge
- TestKeyAwareMergingIterator_Deduplicate
- TestKeyAwareMergingIterator_DifferentKeySets
- TestKeyAwareMergingIterator_Tombstone — 部分 tombstone 正确过滤
- TestKeyAwareMergingIterator_TombstoneAllDeleted — tombstone 删除 key 所有值，输出为空
- TestKeyAwareMergingIterator_TombstoneWithDedup — tombstone + 同时间戳去重
- TestKeyAwareMergingIterator_SingleFile
- TestKeyAwareMergingIterator_SameTimestamp
- TestKeyAwareMergingIterator_TombstoneFirstBlockAllDeleted — 第一个 block 全 tombstone，验证 activateIteratorsForKey 自动跳过并读取下一个 block
- TestKeyAwareMergingIterator_TombstonePartialAfterAllDeleted — 第一个 block 全删除，第二个 block 部分删除
- TestKeyAwareMergingIterator_TombstoneMultipleIteratorsAllDeleted — 多个 iterator 的同一个 key 的第一个 block 都被 tombstone
```

### Phase 3: 集成

```
任务:
1. 在 Compactor.compact() 中添加分发逻辑
2. 保留 tsmBatchKeyIterator 用于 fast mode
3. 端到端测试

文件: tsdb/engine/tsm1/compact.go 修改

测试:
- TestCompactor_StreamingFull
- TestCompactor_FastModeUnchanged
- TestCompactor_LargeShard (OOM 测试)
- TestCompactor_TombstoneConsumed — compaction 后 tombstone 文件被删除
- TestCompactor_TombstonePartialBlock — 部分 block tombstone 正确处理
- 性能基准测试
```

### Phase 4: 优化

```
任务:
1. 值池化 (valueEntryPool)
2. 编码缓冲区池化
3. 性能 profiling
4. 边界情况处理
```

---

## 8. 风险与缓解

| 风险 | 影响 | 缓解 |
|------|------|------|
| 堆合并比 merge-sort 慢 | CPU 开销增加 | 堆操作 O(log N)，N=文件数（通常 4-10），可忽略 |
| 输出 block 压缩率变化 | 文件大小变化 | bufSize 保持与当前 k.size 一致 |
| advanceAndPush 递归深度 | 栈溢出风险 | 实际中同一 key 的连续 block 很少，可加深度限制 |
| Tombstone per-value CPU | 处理时间增加 | tombstone 通常很少，可缓存 tombstone ranges |

---

## 附录 A: 审查问题清单

v1 设计文档审查发现的 12 个问题及修复状态：

| 编号 | 严重性 | 问题 | 修复状态 |
|------|--------|------|----------|
| C1 | CRITICAL | BlockValueIterator 全量解码矛盾 | §4.1: 每个 iterator 只持有一个 decoded block |
| C2 | CRITICAL | 去重逻辑数据丢失 | §4.3 popAndDedup: 消耗所有同时间戳条目 |
| C3 | CRITICAL | 缺乏 key 边界感知 | §4.3 + §5.1: KeyAwareMergingIterator |
| H1 | HIGH | 现状分析代码描述不准确 | 文档使用实际代码引用 (file:line) |
| H2 | HIGH | 堆排序同时间戳不正确 | §4.2: (timestamp, fileIdx) 复合排序 |
| H3 | HIGH | 内存分析误导 | §5.5: 修正为单 key 处理模型 |
| M1 | MEDIUM | 未提及现有优化 | §1.4: 补充现有优化表 |
| M2 | MEDIUM | 缺少具体 Encoder 类型 | §4.4: encodeValues 类型分发 |
| M3 | MEDIUM | Tombstone 声称不正确 | §5.3: 修正为效率改进而非正确性修复 |
| M4 | MEDIUM | 未识别两个 KeyIterator 实现 | §6.3: 明确 fast mode 保持现有实现 |
| L1 | LOW | tsmGeneration 字段不准确 | 已修正 |
| L2 | LOW | values 字段用途未确认 | 保留观察 |

---

## 附录 B: 关键代码文件

| 组件 | 文件 | 行号 |
|------|------|------|
| `KeyIterator` 接口 | `compact.go` | 1221-1238 |
| `tsmBatchKeyIterator` | `compact.go` | 1595-1647 |
| `tsmBatchKeyIterator.Next()` | `compact.go` | 1696-1842 |
| `mergeFloat` | `compact.gen.go` | 1035-1059 |
| `combineFloat` (dedup) | `compact.gen.go` | 1064-1132 |
| `combineFloat` (non-dedup) | `compact.gen.go` | 1134-1210 |
| `chunkFloat` | `compact.gen.go` | 1213-1256 |
| `FloatArray.Merge` | `tsdb/cursors/arrayvalues.gen.go` | 142-203 |
| `Engine.compact()` | `engine.go` | 2082 |
| `Compactor.compact()` | `compact.go` | 894-954 |
| `Compactor.CompactFull()` | `compact.go` | 957-986 |
| `Compactor.CompactFast()` | `compact.go` | 989-1018 |
| `BlockIterator` | `reader.go` | 131-145 |
| `DecodeFloatArrayBlock` | `array_encoding.go` | 32-49 |
| `EncodeFloatArrayBlock` | `encoding.gen.go` | 457-478 |
| 编解码器池 | `encoding.go` | 58-93 |
| `TSMWriter.WriteBlock` | `writer.go` | 649-704 |

---

*文档版本: 2.1*
*最后更新: 2026-04-30*
*v1 审查问题修复: C1, C2, C3, H1, H2, H3, M1, M2, M3, M4*
*v2.1 更新: Init() tombstone 边界问题修复（方案 B：activateIteratorsForKey 中处理）*
