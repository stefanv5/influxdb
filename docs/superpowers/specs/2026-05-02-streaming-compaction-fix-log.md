# TSM 流式 Compaction 修改记录

> 本文档记录 `stefan_stream_compact` 分支上的所有修改、发现的问题及未完成的工作。
> 创建日期: 2026-05-02

---

## 一、修改的文件清单

### 1.1 已修改的文件

| 文件 | 修改类型 | 说明 |
|------|---------|------|
| `tsdb/engine/tsm1/compact.go` | 修改 | 在 `compact()` 函数中添加流式/批量分叉 |

### 1.2 新增的文件

| 文件 | 说明 |
|------|------|
| `tsdb/engine/tsm1/stream_iterator.go` | 流式迭代器核心实现 (~700 行) |
| `tsdb/engine/tsm1/stream_iterator_test.go` | 单元测试 (~1700 行) |
| `tsdb/engine/tsm1/stream_compact_e2e_test.go` | 端到端测试 |
| `docs/superpowers/specs/2026-04-29-tsm-streaming-compaction-design-v2.md` | 设计文档 v2 |

### 1.3 已修改文件详情

#### compact.go 修改内容

```go
// compact.go 中的修改：将 fast 和非 fast 模式分离
func (c *Compactor) compact(fast bool, tsmFiles []string) ([]string, error) {
    // ... 省略中间代码 ...

    var tsm KeyIterator
    if fast {
        var err error
        tsm, err = NewTSMBatchKeyIterator(size, fast, intC, tsmFiles, trs...)
        if err != nil {
            return nil, err
        }
    } else {
        // 非 fast 模式使用流式迭代器
        tsm = NewStreamingKeyIterator(tsmFiles, trs, size, intC)
    }

    return c.writeNewFiles(maxGeneration, maxSequence, tsmFiles, tsm, true)
}
```

---

## 二、核心代码修改

### 2.1 stream_iterator.go 主要结构

```go
// BlockValueIterator - 按需解码单个 block 的迭代器
type BlockValueIterator struct {
    r       *TSMReader
    iter    *BlockIterator
    fileIdx int

    currentKey  []byte
    currentType byte
    tombstones  []TimeRange
    decoded     []Value
    pos         int

    nextRaw  []byte
    nextKey  []byte
    nextType byte
    nextMin  int64
    nextMax  int64

    hasNextBlock bool
    initialized  bool   // true if Init() has been called
    exhausted    bool   // true when iterator has no more blocks  // ← 新增
    err          error
}

// KeyAwareMergingIterator - key 感知的流式合并迭代器
type KeyAwareMergingIterator struct {
    iterators []*BlockValueIterator
    heap      valueHeap
    currentKey  []byte
    currentType byte
    blockKey    []byte
    buf     []Value
    bufSize int
    initialized bool
    exhausted   bool
    closed      bool
    err         error
    interrupt chan struct{}
    estimatedIndexSize int
}
```

### 2.2 关键修改点

#### 2.2.1 advanceAndPush() 修改

```go
func (m *KeyAwareMergingIterator) advanceAndPush(iter *BlockValueIterator) {
    if iter.Next() {
        v := iter.Read()
        entry := &valueEntry{
            value:     v,
            iterator:  iter,
            timestamp: v.UnixNano(),
            fileIdx:   iter.FileIdx(),
        }
        heap.Push(&m.heap, entry)
        return
    }

    // Current block exhausted - call NextBlock() to pre-fetch next key's block
    // This populates hasNextBlock/nextKey for the next key transition
    if iter.NextBlock() {
        // Same key, new block - get next value
        if iter.Next() {
            v := iter.Read()
            entry := &valueEntry{
                value:     v,
                iterator:  iter,
                timestamp: v.UnixNano(),
                fileIdx:   iter.FileIdx(),
            }
            heap.Push(&m.heap, entry)
            return
        }
        // Fall through - block exhausted or all tombstoned
    }
    // Either key changed (hasNextBlock now set) or iterator exhausted
    iter.exhausted = true
}
```

#### 2.2.2 moveToNextKey() 修改

```go
func (m *KeyAwareMergingIterator) moveToNextKey() bool {
    // First, activate ALL pending blocks for all iterators.
    for _, iter := range m.iterators {
        if iter.Err() != nil {
            continue
        }
        if iter.hasNextBlock && len(iter.nextKey) > 0 {
            if !iter.ActivatePending() {
                if iter.Err() != nil {
                    m.err = iter.Err()
                }
            } else {
                // Successfully activated a pending block - iterator is no longer exhausted
                iter.exhausted = false  // ← 关键修复
            }
        }
    }

    // Now find minimum key among all iterators
    minKey := m.findMinKey()
    if minKey == nil {
        return false
    }

    m.currentKey = minKey
    m.currentType = 0
    m.activateIteratorsForKey(minKey)
    return true
}
```

#### 2.2.3 findMinKey() 修改

```go
func (m *KeyAwareMergingIterator) findMinKey() []byte {
    var minKey []byte
    for _, iter := range m.iterators {
        if iter.Err() != nil {
            continue
        }
        // Skip exhausted iterators  // ← 新增检查
        if iter.exhausted {
            continue
        }
        key := iter.Key()
        if key == nil {
            continue
        }
        if minKey == nil {
            minKey = append([]byte(nil), key...)
        } else if bytes.Compare(key, minKey) < 0 {
            minKey = append([]byte(nil), key...)
        }
    }
    return minKey
}
```

---

## 三、发现的问题及修复

### 3.1 问题 1: 不同 key 集合的迭代器合并问题

**现象**:
```
TestKeyAwareMergingIterator_DifferentKeySets 失败
  期望: 3 keys [cpu, disk, mem]
  实际: 2 keys [cpu, disk]  // mem 被遗漏
```

**测试场景**:
```go
// File 1: cpu, mem
vals1 := map[string][]tsm1.Value{
    "cpu,host=A#!~#value": {tsm1.NewValue(1, 1.1)},
    "mem,host=A#!~#value": {tsm1.NewValue(1, 2.1)},
}

// File 2: cpu, disk
vals2 := map[string][]tsm1.Value{
    "cpu,host=A#!~#value": {tsm1.NewValue(2, 1.2)},
    "disk,host=A#!~#value": {tsm1.NewValue(1, 3.1)},
}
```

**根本原因**:
1. `advanceAndPush()` 处理完当前值后，标记 `iterator.exhausted = true`
2. 但此时 `iterator.currentKey` 仍然是上一个 key (如 "cpu")
3. `findMinKey()` 检查 iterators 时，耗尽的 iterator 被跳过
4. 导致即使有下一个 key (如 "mem") 也会被遗漏

**修复方案**:
1. 在 `advanceAndPush()` 中，标记 exhausted 前调用 `NextBlock()` 预取下一个 block
2. 在 `moveToNextKey()` 中，激活待处理块后重置 `exhausted = false`
3. 在 `findMinKey()` 中跳过 `exhausted == true` 的 iterators

**修复状态**: ✅ 已修复，测试通过

### 3.2 问题 2: 字节切片引用问题

**现象**: `findMinKey()` 返回的 key 可能被后续修改

**根本原因**: `iter.Key()` 返回的是 iterator 内部的 `currentKey` 切片引用

**修复方案**: 使用 `append([]byte(nil), key...)` 创建防御性拷贝

**修复状态**: ✅ 已修复

---

## 四、测试状态

### 4.1 单元测试 (全部通过)

```bash
$ go test -v -run "TestBlockValueIterator|TestKeyAwareMergingIterator" ./tsdb/engine/tsm1/
PASS
ok  github.com/influxdata/influxdb/tsdb/engine/tsm1  1.656s
```

| 测试名称 | 描述 | 状态 |
|---------|------|------|
| `TestBlockValueIterator_Basic` | 基本迭代 | ✅ PASS |
| `TestBlockValueIterator_KeyBoundary` | key 边界感知 | ✅ PASS |
| `TestBlockValueIterator_MultiBlock` | 多 block 迭代 | ✅ PASS |
| `TestBlockValueIterator_Tombstone` | tombstone 处理 | ✅ PASS |
| `TestKeyAwareMergingIterator_BasicMerge` | 基本合并 | ✅ PASS |
| `TestKeyAwareMergingIterator_Deduplicate` | 同时间戳去重 | ✅ PASS |
| `TestKeyAwareMergingIterator_DifferentKeySets` | 不同 key 集合 | ✅ PASS |
| `TestKeyAwareMergingIterator_SingleFile` | 单文件多 key | ✅ PASS |
| `TestKeyAwareMergingIterator_SameTimestamp` | 同时间戳 | ✅ PASS |
| `TestKeyAwareMergingIterator_Tombstone*` | tombstone 相关 | ✅ PASS |
| `TestKeyAwareMergingIterator_Interrupt*` | 中断处理 | ✅ PASS |

### 4.2 端到端测试 (全部通过)

```bash
$ go test -v -run "TestStreamingCompaction" ./tsdb/engine/tsm1/
PASS
```

| 测试名称 | 描述 | 状态 |
|---------|------|------|
| `TestStreamingCompaction_LargeFiles` | 3 文件 × 50M 点 (~393 MB) | ✅ PASS |
| `TestStreamingCompaction_LargeFilesWithOverlap` | 重叠时间范围去重 | ✅ PASS |
| `TestStreamingCompaction_ManyKeys` | 10,000 keys × 1,000 点 | ✅ PASS |
| `TestStreamingCompaction_Tombstones` | tombstone 处理 | ✅ PASS |
| `TestStreamingCompaction_AllValueTypes` | 所有值类型 | ✅ PASS |
| `TestStreamingCompaction_DedupByTimestamp` | 同时间戳去重 | ✅ PASS |
| `TestStreamingCompaction_UnsortedBlocks` | 未排序 block | ✅ PASS |
| `TestStreamingCompaction_Interrupt` | 中断处理 | ✅ PASS |
| `TestStreamingCompaction_2GBLargeFile` | 3 文件 × 45M 点 (~2 GB) | ✅ PASS |

**修复内容** (2026-05-02):
1. **Key 排序**: 使用零填充格式 (`%02d`, `%04d`, `%05d`) 确保字典序正确
2. **文件命名**: 使用 `DefaultFormatFileName(generation, sequence)` 替代 `fmt.Sprintf("%09d.tsm", i+1)`
3. **新增 2GB 测试**: `TestStreamingCompaction_2GBLargeFile` 验证大文件场景

---

## 五、未完成的工作

### 5.1 已知问题

| 优先级 | 问题 | 影响 | 状态 |
|--------|------|------|------|
| ~~HIGH~~ | ~~大文件 e2e 测试 keys 排序问题~~ | ~~无法验证 2G+ 文件场景~~ | ✅ 已修复 |
| MEDIUM | 性能基准测试缺失 | 无法验证内存峰值是否符合设计 (~551 KB) | 待添加 |

### 5.2 计划功能

| 优先级 | 功能 | 说明 | 状态 |
|--------|------|------|------|
| ~~HIGH~~ | ~~修复 e2e 测试的 key 排序~~ | ~~使用字典序正确的 host 名称~~ | ✅ 已完成 |
| ~~HIGH~~ | ~~添加 2GB 大文件测试~~ | ~~验证流式 compaction 处理 2G+ 文件~~ | ✅ 已完成 |
| MEDIUM | 添加性能基准测试 | 对比流式 vs 批量内存使用 | 待添加 |
| LOW | 值池化 (valueEntryPool) | 减少 GC 压力 | 待实现 |
| LOW | 缓冲区池化 (encodeBufPool) | 减少编码内存分配 | 待实现 |

### 5.3 已修复的 e2e 测试问题

**问题**: TSM 文件命名和 key 排序不符合 TSMWriter 要求。

**修复方案** (已实施):
1. **Key 排序**: 使用零填充格式确保字典序正确
   - 10 keys: `cpu,host=%02d#!~#value` (00-09)
   - 1,000 keys: `cpu,host=%04d#!~#value` (0000-0999)
   - 10,000 keys: `cpu,host=%05d#!~#value` (00000-09999)
2. **文件命名**: 使用 `DefaultFormatFileName(generation, sequence)` 生成正确的 TSM 文件名
3. **新增测试**: `TestStreamingCompaction_2GBLargeFile` 验证 2GB+ 场景

---

## 六、设计文档参考

| 文档 | 说明 |
|------|------|
| `docs/superpowers/specs/2026-04-29-tsm-streaming-compaction-design.md` | v1 设计文档 |
| `docs/superpowers/specs/2026-04-29-tsm-streaming-compaction-design-v2.md` | v2 设计文档 (推荐) |

---

## 七、Git 状态

```
$ git status --short
 M tsdb/engine/tsm1/compact.go
?? .claude/
?? .opencode/
?? docs/superpowers/specs/2026-04-29-tsm-streaming-compaction-design-v2.md
?? tsdb/engine/tsm1/stream_compact_e2e_test.go
?? tsdb/engine/tsm1/stream_iterator.go
?? tsdb/engine/tsm1/stream_iterator_test.go
```

---

## 八、下一步工作

1. ~~**立即**: 修复 e2e 测试的 key 排序问题~~ ✅ 已完成
2. ~~**立即**: 运行 `TestStreamingCompaction_LargeFiles` 验证 2G+ 文件场景~~ ✅ 已完成
3. ~~**立即**: 添加 2GB 大文件 compaction 测试~~ ✅ 已完成
4. **短期**: 添加性能基准测试，对比内存使用
5. **中期**: 添加值池化和缓冲区池化
6. **长期**: 性能调优和边界情况处理

---

*文档版本: 1.1*
*最后更新: 2026-05-02 (更新: 修复 e2e 测试 key 排序 + 新增 2GB 测试)*
*分支: stefan_stream_compact*
