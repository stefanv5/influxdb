# InfluxDB TSM Compaction 完整流程分析

## 1. 概述

Compaction 是 InfluxDB 将写入优化的 WAL 数据转换为读取优化的 TSM 文件的核心过程。它不仅处理新数据，还负责合并现有 TSM 文件、删除 tombstone 记录、重组数据以提高压缩率。

```
写入流程:
WAL → Cache → (Snapshot) → TSM File (Level 1) → ... → TSM File (Level 4)
                              ↑                          ↑
                          这里触发                     最终 level
                      Cache Snapshot
```

## 2. 触发入口

### 2.1 Engine 层的 Compaction 触发

入口在 `tsdb/engine/tsm1/engine.go` 中，Engine 定期检查是否需要 compaction：

```go
// Run will perform compaction and snapshot the cache when necessary
func (e *Engine) Run(ctx context.Context, opChan <-chan influxdb.Operator) {
    for {
        select {
        case <-ctx.Done():
            return
        default:
        }

        // 检查是否需要 snapshot cache
        if e.CacheSnapshotNeeded() {
            if err := e.writeSnapshot(); err != nil {
                e.logger.Error("error writing snapshot", zap.Error(err))
            }
        }

        // 检查是否需要 level compaction
        if e.CompactionNeeded() {
            compact := e.CompactionPlan()
            for _, group := range compact {
                // 执行 compaction
            }
        }
    }
}
```

### 2.2 何时触发

Compaction 有两个主要触发条件：

1. **Cache Snapshot** - 当 Cache 达到一定大小或时间阈值时
   - 配置: `CacheFlushWriteColdDuration` (默认 10m)
   - 配置: `CacheMaxMemory` (最大 cache 大小)

2. **Level Compaction** - 当存在需要合并的 TSM 文件时
   - 由 `CompactionPlanner` 生成计划

## 3. Plan 生成详解

### 3.1 文件组织结构

TSM 文件命名格式: `[generation]-[sequence].tsm`

```
000001-01.tsm  ← generation=1, sequence=1, level=1
000001-02.tsm  ← generation=1, sequence=2, level=2
000001-03.tsm  ← generation=1, sequence=3, level=3
000001-04.tsm  ← generation=1, sequence=4, level=4
000002-01.tsm  ← generation=2, sequence=1, level=1
...
```

**Generation vs Level:**
- **Generation**: 同一批写入的数据，generation id 递增
- **Level**: 文件被压缩的层级，sequence 越小 level 越低
  - Sequence 1-3 → Level = Sequence
  - Sequence >= 4 → Level = 4

### 3.2 Generation 收集 (`findGenerations`)

```go
func (c *DefaultPlanner) findGenerations() tsmGenerations {
    tsmStats := c.FileStore.Stats()  // 获取所有 TSM 文件统计
    
    // 按 generation 分组
    generations := make(map[int]*tsmGeneration, len(tsmStats))
    for _, f := range tsmStats {
        group := generations[f.Generation]
        if group == nil {
            group = newTsmGeneration(f.Generation)
            generations[f.Generation] = group
        }
        group.files = append(group.files, f)
    }
    
    // 按 generation id 降序排序 (新的在前)
    orderedGenerations := make(tsmGenerations, 0, len(generations))
    for _, g := range generations {
        orderedGenerations = append(orderedGenerations, g)
    }
    sort.Sort(orderedGenerations)  // 按 id 降序
    
    return orderedGenerations
}
```

**核心数据结构:**
```go
type tsmGeneration struct {
    id    int           // generation id
    files []ExtFileStat // 该 generation 的所有文件
}

type ExtFileStat struct {
    Path         string
    Size         uint32
    HasTombstone bool
    Generation   int
    Sequence     int  // 用于计算 level
    FirstBlockCount int
}
```

### 3.3 Level 计算

```go
func (t *tsmGeneration) level() int {
    // Level 0: Cache snapshot 产生的临时文件
    // Level 1-3: 由较低 sequence 的文件 compact 而来
    // Level 4: 最高 level，sequence >= 4
    
    if t.files[0].Sequence < 4 {
        return t.files[0].Sequence  // sequence 1,2,3 → level 1,2,3
    }
    return 4  // sequence 4+ → level 4
}
```

### 3.4 相邻 Generation 分组 (`groupAdjacentGenerations`)

这是 Plan 生成的关键算法，将相邻的同 level generation 组成一个 compaction group：

```go
func (c *DefaultPlanner) groupAdjacentGenerations(
    generations tsmGenerations, 
    levelTestFn leveltestFnType  // 用于判断是否同 level
) []tsmGenerations {
    var currentGen tsmGenerations
    var groups tsmGenerationGroups
    
    for i := 0; i < len(generations); i++ {
        if c.isInUse(generations[i]) {
            // 如果文件被占用，结束当前 group，但该 generation 不加入任何 group
            moveToNextGroup()
            continue
        }
        
        if len(currentGen) == 0 ||
           levelTestFn(currentGen.level(), generations[i].level()) ||
           // 孤儿文件：当前 generation level < 下一个 generation level
           (i < len(generations)-1 && generations[i].level() < generations[i+1].level()) {
            currentGen = append(currentGen, generations[i])
        } else {
            moveToNextGroup()
            currentGen = append(currentGen, generations[i])
        }
    }
    moveToNextGroup()
    return groups
}
```

**分组逻辑图解:**

```
假设 generations 如下 (id:level):
[G10:4] [G9:4] [G8:3] [G7:3] [G6:3] [G5:2] [G4:2] [G3:1] [G2:1] [G1:1]

执行 groupAdjacentGenerations(..., levelTestFn = 同 level):
  遍历 G10: 当前无，添加 → currentGen=[G10]
  遍历 G9:  G10.level(4) == G9.level(4)，同 level，继续 → currentGen=[G10,G9]
  遍历 G8:  G9.level(4) != G8.level(3)，不同 level，moveToNextGroup → groups=[[G10,G9]]
           新起 currentGen=[G8]
  遍历 G7:  G8.level(3) == G7.level(3)，同 level，继续 → currentGen=[G8,G7]
  遍历 G6:  G7.level(3) == G6.level(3)，同 level，继续 → currentGen=[G8,G7,G6]
  遍历 G5:  G6.level(3) != G5.level(2)，不同 level，moveToNextGroup → groups=[[G10,G9],[G8,G7,G6]]
           新起 currentGen=[G5]
  遍历 G4:  G5.level(2) == G4.level(2)，同 level，继续 → currentGen=[G5,G4]
  遍历 G3:  G4.level(2) != G3.level(1)，不同 level，moveToNextGroup → groups=[[G10,G9],[G8,G7,G6],[G5,G4]]
           新起 currentGen=[G3]
  遍历 G2:  G3.level(1) == G2.level(1)，同 level，继续 → currentGen=[G3,G2]
  遍历 G1:  G2.level(1) == G1.level(1)，同 level，继续 → currentGen=[G3,G2,G1]
  最后 moveToNextGroup → groups=[[G10,G9],[G8,G7,G6],[G5,G4],[G3,G2,G1]]

结果: 4 个 groups
```

### 3.5 三种 Planner 策略

#### A. `Plan(lastWrite time.Time)` - 全量/冷数据 Compaction

用于处理 Level 4+ 的文件，当写入冷很久后触发：

```go
func (c *DefaultPlanner) Plan(lastWrite time.Time) ([]CompactionGroup, int64) {
    generations := c.findGenerations()
    
    // 条件1: forceFull 标记 或 很久没写入 (超过 compactFullWriteColdDuration)
    if forceFull || (time.Since(lastWrite) > compactFullWriteColdDuration && len(generations) > 1) {
        // 收集所有 generation 的所有文件
        return []CompactionGroup{allFiles}, 1
    }
    
    // 条件2: 文件 store 没变化且没有 tombstones
    if !needCompaction(generations) {
        return nil, 0
    }
    
    // 查找 level >= 4 的 generations
    // 从最老的开始，合并到 level 4 的 groups
    
    // 跳过"太大"的文件（超过 2GB 且 block 足够满）
    // 最终返回需要 compact 的 groups
}
```

#### B. `PlanLevel(level int)` - 特定 Level Compaction

```go
func (c *DefaultPlanner) PlanLevel(level int) ([]CompactionGroup, int64) {
    generations := c.findGenerations()
    
    // 条件: 单个 generation 且无 tombstones → 跳过
    if len(generations) <= 1 && !generations.hasTombstones() {
        return nil, 0
    }
    
    // 按 level 分组
    groups := c.groupAdjacentGenerations(generations,
        func(currentLevel, candidateLevel int) bool { 
            return currentLevel == candidateLevel 
        })
    
    // 过滤只保留指定 level 的 groups
    levelGroups := filterByLevel(groups, level)
    
    // 触发阈值
    minGenerations := 4
    if level == 1 {
        minGenerations = 8  // Level 1 需要更多文件
    }
    
    // 按 minGenerations 分块
    cGroups := []CompactionGroup{}
    for _, group := range levelGroups {
        for _, chunk := range group.chunk(minGenerations) {
            cGroups = append(cGroups, toCompactionGroup(chunk))
        }
    }
    
    if !c.acquire(cGroups) {
        return nil, int64(len(cGroups))
    }
    return cGroups, int64(len(cGroups))
}
```

#### C. `PlanOptimize()` - 索引优化

将不同 generation 但同 level 的文件合并，优化查询性能：

```go
func (c *DefaultPlanner) PlanOptimize(lastWrite time.Time) ([]CompactionGroup, int64, int64) {
    generations := c.findGenerations()
    
    // 已经完全 compact → 跳过
    if fullyCompacted || time.Since(lastWrite) < c.compactFullWriteColdDuration {
        return nil, 0, 0
    }
    
    // level >= candidateLevel 即可合并（比 PlanLevel 更激进）
    groups := c.groupAdjacentGenerations(generations, 
        func(currentLevel, candidateLevel int) bool { 
            return currentLevel >= candidateLevel 
        })
    
    // ...
}
```

### 3.6 文件锁定机制 (`acquire/release`)

```go
func (c *DefaultPlanner) acquire(groups []CompactionGroup) bool {
    c.mu.Lock()
    defer c.mu.Unlock()
    
    // 检查是否有文件已被占用
    for _, g := range groups {
        for _, f := range g {
            if _, ok := c.filesInUse[f]; ok {
                return false  // 文件被占用，无法获取
            }
        }
    }
    
    // 标记所有文件为 in-use
    for _, g := range groups {
        for _, f := range g {
            c.filesInUse[f] = struct{}{}
        }
    }
    return true
}

func (c *DefaultPlanner) Release(groups []CompactionGroup) {
    c.mu.Lock()
    defer c.mu.Unlock()
    for _, g := range groups {
        for _, f := range g {
            delete(c.filesInUse, f)
        }
    }
}
```

## 4. Compactor 执行

### 4.1 入口: `CompactFull` / `CompactFast`

```go
func (c *Compactor) CompactFull(tsmFiles []string, logger *zap.Logger, pointsPerBlock int) ([]string, error) {
    // 1. 检查是否启用
    // 2. 尝试获取文件锁
    if !c.add(tsmFiles) {
        return nil, errCompactionInProgress{}
    }
    defer c.remove(tsmFiles)
    
    // 3. 执行 compact
    files, err := c.compact(false, tsmFiles, logger, pointsPerBlock)
    
    return files, err
}
```

### 4.2 核心 compact 方法

```go
func (c *Compactor) compact(fast bool, tsmFiles []string, logger *zap.Logger, pointsPerBlock int) ([]string, error) {
    // 1. 找出最大 generation 和 sequence
    var maxGeneration, maxSequence int
    minSeqByGen := make(map[int]int)
    
    for _, f := range tsmFiles {
        gen, seq, err := c.FileStore.ParseFileName(f)
        if gen > maxGeneration {
            maxGeneration = gen
        }
        if gen == maxGeneration && seq > maxSequence {
            maxSequence = seq
        }
        minSeqByGen[gen] = min(minSeqByGen[gen], seq)
    }
    
    // 2. 计算输出 level
    // 确保不小于任何输入文件的 level
    var maxInputLevel int
    for _, minSeq := range minSeqByGen {
        maxInputLevel = max(maxInputLevel, min(minSeq, 4))
    }
    if maxSequence+1 < maxInputLevel {
        maxSequence = maxInputLevel - 1  // 防止 level 回退
    }
    
    // 3. 创建 TSMReader
    var trs []*TSMReader
    for _, file := range tsmFiles {
        tr, err := c.FileStore.TSMReader(file)
        if err != nil {
            return nil, err
        }
        defer tr.Unref()  // 引用计数
        trs = append(trs, tr)
    }
    
    // 4. 创建 KeyIterator（关键数据结构）
    tsm := NewTSMBatchKeyIterator(pointsPerBlock, fast, DefaultMaxSavedErrors, intC, tsmFiles, trs...)
    
    // 5. 写入新文件
    return c.writeNewFiles(maxGeneration, maxSequence, tsmFiles, tsm, true, logger)
}
```

### 4.3 文件选择流程图

```
                    ┌─────────────────────────────────────┐
                    │        CompactionPlan()              │
                    └─────────────────────────────────────┘
                                      │
                    ┌────────────────▼───────────────────┐
                    │    FileStore.Stats() 获取所有文件    │
                    └─────────────────────────────────────┘
                                      │
                    ┌────────────────▼───────────────────┐
                    │    按 Generation 分组               │
                    │    generations: map[int]*tsmGeneration│
                    └─────────────────────────────────────┘
                                      │
                    ┌────────────────▼───────────────────┐
                    │    按 Generation ID 降序排序         │
                    │    (新的在前，老的在后)              │
                    └─────────────────────────────────────┘
                                      │
                    ┌────────────────▼───────────────────┐
                    │    groupAdjacentGenerations()       │
                    │    相邻同 level 的 generation 组团    │
                    └─────────────────────────────────────┘
                                      │
                    ┌────────────────▼───────────────────┐
                    │    按 level 过滤 (PlanLevel)        │
                    │    或 level >= 4 (Plan)              │
                    └─────────────────────────────────────┘
                                      │
                    ┌────────────────▼───────────────────┐
                    │    检查触发阈值                     │
                    │    Level 1: >= 8 个 generations     │
                    │    Level 2-4: >= 4 个 generations  │
                    └─────────────────────────────────────┘
                                      │
                    ┌────────────────▼───────────────────┐
                    │    acquire() 尝试锁定文件           │
                    │    成功 → 返回 CompactionGroup     │
                    │    失败 → 返回 nil                 │
                    └─────────────────────────────────────┘
```

## 5. TSM 文件读取

### 5.1 TSM 文件格式

```
┌────────────────────────────────────────────────────────────────────────────┐
│                          TSM File Structure                                │
├──────────┬──────────┬─────────────────────────────┬────────────────────────┤
│  Magic   │ Version  │         Data Blocks          │         Index          │
│ (4 bytes)│ (1 byte) │                             │                        │
├──────────┴──────────┴─────────────────────────────┴────────────────────────┤
│                                                                           │
│                     ┌───────────────────────┐                              │
│                     │      Index Start      │◄──────┘                        │
│                     │     (8 bytes)        │                                │
│                     └───────────────────────┘                              │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

**Data Block 格式:**
```
┌──────────┬────────────────┬─────────────────────┐
│ Checksum │   Block Type   │      Data           │
│ (4 bytes)│   (1 byte)     │   (N bytes)         │
└──────────┴────────────────┴─────────────────────┘
```

**Index 格式:**
```
┌────────┬────────┬──────────┬──────────┬──────────┬──────────┐
│ Key1   │ Type   │ Entry 1  │ Entry 2  │ Entry 3  │ ...      │
│ (2+N)  │        │ (20 B)   │ (20 B)   │ (20 B)   │          │
├────────┴────────┴──────────┴──────────┴──────────┴──────────┤
│                     Key 2                                        │
│                     ...                                           │
└─────────────────────────────────────────────────────────────────┘
```

### 5.2 TSMReader 初始化

```go
func NewTSMReader(f *os.File, options ...TsmReaderOption) (*TSMReader, error) {
    t := &TSMReader{}
    
    // 1. 获取文件信息
    stat, _ := f.Stat()
    t.size = stat.Size()
    t.lastModified = stat.ModTime().UnixNano()
    
    // 2. 初始化 mmap accessor
    t.accessor = &mmapAccessor{
        f:            f,
        mmapWillNeed: t.madviseWillNeed,
    }
    
    // 3. 通过 accessor 初始化 index
    index, err := t.accessor.init()  // mmap 文件，解析 index
    t.index = index
    
    // 4. 初始化 tombstoner
    t.tombstoner = NewTombstoner(t.Path(), index.ContainsKey)
    
    // 5. 应用 tombstones
    t.applyTombstones()
    
    return t, nil
}
```

### 5.3 indirectIndex 结构

```go
type indirectIndex struct {
    b       []byte  // 整个 index 的 mmap 后的字节
    
    // indirect index 的核心：offsets 数组
    // 每个 key 在 index 中的起始位置（4字节偏移量）
    // 通过二分查找快速定位 key
    offsets []byte  
    
    minKey, maxKey []byte  // 用于快速跳过
    minTime, maxTime int64
    
    tombstones map[string][]TimeRange  // 部分删除的时间范围
}
```

### 5.4 BlockIterator 迭代读取

```go
type BlockIterator struct {
    r       *TSMReader
    i       int           // 当前 key 索引
    n       int           // 总 key 数
    key     []byte
    cache   []IndexEntry  // 避免分配的缓存
    entries []IndexEntry  // 当前 key 的所有 entries
    err     error
    typ     byte
}

func (b *BlockIterator) Next() bool {
    // 1. 如果有更多 entries，消耗一个
    if len(b.entries) > 0 {
        b.entries = b.entries[1:]
        if len(b.entries) > 0 {
            return true
        }
    }
    
    // 2. 读取下一个 key 的 entries
    if b.n - b.i > 0 {
        b.key, b.typ, b.entries = b.r.Key(b.i, &b.cache)
        b.i++
        
        if b.n != b.r.KeyCount() {
            // 并发 delete 发生，迭代器失效
            b.err = fmt.Errorf("delete during iteration")
            return false
        }
        
        if len(b.entries) > 0 {
            return true
        }
    }
    
    return false
}

func (b *BlockIterator) Read() (key, minTime, maxTime, typ, checksum, buf, err) {
    if b.err != nil {
        return nil, 0, 0, 0, 0, nil, b.err
    }
    
    // 读取当前 entry 的数据块
    checksum, buf, err = b.r.ReadBytes(&b.entries[0], nil)
    return b.key, b.entries[0].MinTime, b.entries[0].MaxTime, b.typ, checksum, buf, err
}
```

### 5.5 文件读取流程图

```
                    ┌─────────────────────────────────────┐
                    │   NewTSMBatchKeyIterator()          │
                    │   创建 tsmBatchKeyIterator          │
                    └─────────────────────────────────────┘
                                      │
                    ┌────────────────▼───────────────────┐
                    │   为每个 TSMReader 创建 BlockIterator│
                    │   iterators = [BI1, BI2, BI3, ...]  │
                    └─────────────────────────────────────┘
                                      │
                                      ▼
            ┌───────────────────────────────────────────┐
            │           tsmBatchKeyIterator.Next()        │
            └───────────────────────────────────────────┘
                                      │
            ┌─────────────────────────┴───────────────────┐
            │                                               │
            ▼                                               ▼
    ┌───────────────┐                              ┌───────────────┐
    │ len(k.merged) │                              │ len(k.merged) │
    │    > 0        │                              │    == 0       │
    └───────────────┘                              └───────────────┘
            │                                               │
            │                                               ▼
            │                               ┌───────────────────────────┐
            │                               │ k.hasMergedValues()       │
            │                               └───────────────────────────┘
            │                                       │
            ▼                                       ▼
    ┌───────────────┐                      ┌─────────────────┐
    │ 消耗 merged   │                      │      YES        │
    │ 返回 true     │                      │  k.merge()      │
    └───────────────┘                      └─────────────────┘
                                                     │
                                                     ▼
                                             ┌─────────────────┐
                                             │ len(k.blocks)   │
                                             │    > 0          │
                                             └─────────────────┘
                                                     │
            ┌─────────────────────────────────────────┴───────────────┐
            │                                                       │
            ▼                                                       ▼
    ┌───────────────┐                                       ┌───────────────┐
    │ k.blocks 有值  │                                       │ k.blocks 空   │
    │ → k.merge()   │                                       │ 读取更多 block│
    └───────────────┘                                       └───────────────┘
            │                                                       │
            ▼                                                       ▼
    ┌───────────────┐                                       ┌─────────────────┐
    │ 检查 merged   │                                       │ 对每个 buf[i]   │
    │ 是否还有剩余  │                                       │ 调用 iter.Next()│
    └───────────────┘                                       │ 读取 block      │
            │                                               └─────────────────┘
            ▼                                                       │
    ┌───────────────┐                                               │
    │ YES          │                                               ▼
    │ 返回 true    │                                    ┌───────────────────┐
    └───────────────┘                                    │ 找到 min key       │
                                                       │ (字典序最小)      │
                                                       └───────────────────┘
                                    ┌───────────────────┘
                                    ▼
                            ┌───────────────────┐
                            │ 收集所有同 key    │
                            │ 的 blocks         │
                            │ → k.blocks        │
                            └───────────────────┘
                                    │
                                    ▼
                            ┌───────────────────┐
                            │ k.merge()         │
                            │ 合并同 key 数据   │
                            └───────────────────┘
                                    │
                                    ▼
                            ┌───────────────────┐
                            │ k.merged 有值     │
                            │ 返回 true        │
                            └───────────────────┘
```

## 6. 数据合并 (merge)

### 6.1 核心数据结构

```go
type tsmBatchKeyIterator struct {
    readers []*TSMReader           // TSM readers
    values  map[string][]Value     // 缓存的值（未使用？）
    
    // 每个 reader 的位置
    pos []int
    
    errs TSMErrors
    errSet map[string]struct{}
    
    // fast 模式: 不合并 block，直接使用
    fast bool
    
    // 每个 key 的最大 value 数量
    size int
    
    // 当前处理的 key
    key []byte
    typ byte
    
    // TSM 文件名
    tsmFiles []string
    currentTsm string
    
    // 每个 reader 的 BlockIterator
    iterators []*BlockIterator
    
    // 每个 reader 的 block 缓冲区
    buf []blocks
    
    // 已合并待消费的 blocks
    merged blocks
    
    // 按类型合并的 buffer（这里是 OOM 的根源）
    mergedFloatValues    *tsdb.FloatArray
    mergedIntegerValues  *tsdb.IntegerArray
    mergedUnsignedValues *tsdb.UnsignedArray
    mergedBooleanValues  *tsdb.BooleanArray
    mergedStringValues   *tsdb.StringArray
    
    interrupt chan struct{}
    maxErrors int
    overflowErrors int
}

type block struct {
    key              []byte      // 测量 + 字段名
    minTime, maxTime int64       // 该 block 的时间范围
    typ              byte        // 数据类型
    b                []byte     // 原始数据（含 checksum）
    tombstones       []TimeRange // 该 block 的删除范围
    
    readMin, readMax int64       // 已读取的时间范围
}
```

### 6.2 mergeFloat 详解

当 `fast=false` 时，执行完整合并：

```go
func (k *tsmBatchKeyIterator) mergeFloat() {
    var allValues FloatArray
    
    for _, blk := range k.blocks {
        // 1. 应用 tombstone 过滤
        if len(blk.tombstones) > 0 {
            // 过滤掉 tombstone 范围内的时间点
            // 但仍然需要先 decode 整个 block
        }
        
        // 2. Decode block
        decoded := decodeBlock(blk.b)  // 全量 decode
        
        // 3. 追加到 allValues
        allValues.Append(decoded...)
    }
    
    // 4. 排序（如果 block 的时间有重叠）
    allValues.Sort()
    
    // 5. 去重（保留最新的值）
    allValues.Deduplicate()
    
    // 6. 重新 encode 为新的 block
    newBlock := encodeBlock(allValues)
    
    // 7. 保存到 k.merged
    k.merged = append(k.merged, &block{
        key: k.key,
        minTime: allValues.MinTime(),
        maxTime: allValues.MaxTime(),
        typ: BlockFloat64,
        b: newBlock,
    })
}
```

### 6.3 Fast 模式 (CompactFast)

当 `fast=true` 时，不做 decode/re-encode，直接拼接 block：

```go
func (k *tsmBatchKeyIterator) mergeFloatFast() {
    for _, blk := range k.blocks {
        // 直接将原始 block 加入 merged
        k.merged = append(k.merged, blk)
    }
    // 问题：tombstone 过滤在这里被忽略！
    // fast 模式的 compaction 无法正确处理 tombstone
}
```

### 6.4 数据合并流程图

```
同 Key 的多个 Block 来自不同 TSM 文件:
                    
┌─────────────────────────────────────────────────────────────────────┐
│                      Key: "cpu,host=server1"                        │
├─────────────────┬─────────────────┬─────────────────┬───────────────┤
│   TSM-1 Block   │   TSM-2 Block   │   TSM-3 Block   │   ...        │
│                 │                 │                 │               │
│ minTime: 100    │ minTime: 200    │ minTime: 150    │               │
│ maxTime: 300    │ maxTime: 400    │ maxTime: 350    │               │
│                 │                 │                 │               │
│ [100,150,f]     │ [200,250,f]     │ [150,200,f]     │               │
│ [150,200,f]     │ [250,300,f]     │ [200,250,f]     │               │
│ [200,300,f]     │ [300,400,f]     │ [250,350,f]     │               │
│                 │                 │                 │               │
│                 │  Tombstone:     │                 │               │
│                 │  [250,300]      │                 │               │
└─────────────────┴─────────────────┴─────────────────┴───────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│                          merge()                                     │
├─────────────────────────────────────────────────────────────────────┤
│  1. 按时间排序所有值                                                 │
│     [100,150], [150,200], [200,250], [250,300], [300,350], [300,400] │
│                                                                     │
│  2. 应用 Tombstone 过滤 [250,300]                                   │
│     [100,150], [150,200], [200,250],    [300,350], [300,400]        │
│                                                                     │
│  3. 去重（保留同一时间戳的最新值）                                   │
│     [100,150], [150,200], [200,250], [300,350], [300,400]           │
│     (假设 [300,400] 是更新的数据)                                   │
│                                                                     │
│  4. 重新 encode 成新 block                                          │
└─────────────────────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         Output Block                                │
├─────────────────────────────────────────────────────────────────────┤
│ minTime: 100, maxTime: 400                                         │
│                                                                     │
│ [100,150], [150,200], [200,250], [300,350], [300,400]               │
└─────────────────────────────────────────────────────────────────────┘
```

## 7. 文件写入

### 7.1 writeNewFiles 主流程

```go
func (c *Compactor) writeNewFiles(
    generation, sequence int, 
    src []string,          // 源文件列表（用于日志）
    iter KeyIterator,     // 数据迭代器
    throttle bool,        // 是否限速
    logger *zap.Logger
) ([]string, error) {
    var files []string
    
    for {
        sequence++
        
        // 生成输出文件名
        fileName := filepath.Join(c.Dir, 
            c.formatFileName(generation, sequence) + ".tsm.tmp")
        
        // 写入一个 TSM 文件
        rollToNext, err := c.write(fileName, iter, throttle, logger)
        
        if rollToNext {
            // 文件达到大小限制，需要创建新文件
            files = append(files, fileName)
            continue
        } else if errors.Is(err, ErrNoValues) {
            // 空文件（只有 tombstone），删除
            os.RemoveAll(fileName)
            break
        } else if err != nil {
            // 错误，清理已写文件
            return nil, c.RemoveTmpFilesOnErr(files, err)
        }
        
        files = append(files, fileName)
        break
    }
    
    return files, nil
}
```

### 7.2 write 单个文件

```go
func (c *Compactor) write(path string, iter KeyIterator, throttle bool, logger *zap.Logger) (rollToNext bool, err error) {
    // 1. 打开文件
    fd, _ := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0666)
    
    // 2. 创建 TSMWriter
    var w TSMWriter
    if iter.EstimatedIndexSize() > 64*1024*1024 {
        // 大索引使用磁盘缓冲
        w, _ = NewTSMWriterWithDiskBuffer(limitWriter)
    } else {
        w, _ = NewTSMWriter(limitWriter)
    }
    
    // 3. 循环写入 blocks
    lastLogSize := w.Size()
    for iter.Next() {
        // 检查是否被中断
        select { case <-interrupt: return false, errCompactionAborted{} default: }
        
        // 读取下一个 block
        key, minTime, maxTime, block, err := iter.Read()
        
        // 写入 block
        if err := w.WriteBlock(key, minTime, maxTime, block); errors.Is(err, ErrMaxBlocksExceeded) {
            // 达到最大 block 数，写索引，提示需要新文件
            w.WriteIndex()
            return true, err
        }
        
        // 检查是否达到文件大小限制
        if w.Size() > tsdb.MaxTSMFileSize {
            w.WriteIndex()
            return true, errMaxFileExceeded
        }
    }
    
    // 4. 写索引
    w.WriteIndex()
    
    return false, nil
}
```

### 7.3 写入流程图

```
                    ┌─────────────────────────────────────┐
                    │      writeNewFiles()                │
                    └─────────────────────────────────────┘
                                      │
                    ┌────────────────▼───────────────────┐
                    │      sequence++                    │
                    │      生成新文件名                   │
                    │      "000001-05.tsm.tmp"          │
                    └─────────────────────────────────────┘
                                      │
                    ┌────────────────▼───────────────────┐
                    │      write()                        │
                    │      打开 tmp 文件                  │
                    │      创建 TSMWriter                 │
                    └─────────────────────────────────────┘
                                      │
                                      ▼
            ┌───────────────────────────────────────────┐
            │           iter.Next()                      │
            │           循环读取 block                   │
            └───────────────────────────────────────────┘
                                      │
            ┌─────────────────────────┴─────────────────┐
            │                                           │
            ▼                                           ▼
    ┌───────────────┐                          ┌───────────────┐
    │ iter.Next()   │                          │ !iter.Next()  │
    │   == true     │                          │   (结束)      │
    └───────────────┘                          └───────────────┘
            │                                           │
            ▼                                           ▼
    ┌───────────────┐                          ┌───────────────┐
    │ iter.Read()   │                          │ w.WriteIndex()│
    │ 获取 block   │                          │ 写索引到文件  │
    └───────────────┘                          └───────────────┘
            │                                           │
            ▼                                           ▼
    ┌───────────────┐                          ┌───────────────┐
    │ w.WriteBlock │                          │ w.Close()     │
    │ 写入 block   │                          │ 关闭文件      │
    └───────────────┘                          └───────────────┘
            │                                           │
            ├──────────────────┬──────────────────────┘
            ▼                  ▼
    ┌───────────────┐   ┌───────────────┐
    │ 大小超限？    │   │ block 数量   │
    │ > 2GB         │   │ 超限？       │
    └───────────────┘   └───────────────┘
            │                  │
            ▼                  ▼
    ┌───────────────┐   ┌───────────────┐
    │ YES → 写索引  │   │ YES → 写索引  │
    │ rollToNext    │   │ rollToNext    │
    │ = true        │   │ = true        │
    └───────────────┘   └───────────────┘
            │                  │
            └────────┬─────────┘
                     ▼
            ┌───────────────┐
            │ 返回 rollTo  │
            │ = true       │
            └───────────────┘
                     │
                     ▼
            ┌───────────────┐
            │ writeNewFiles│
            │ 继续写下一   │
            │ 个文件       │
            └───────────────┘
```

## 8. TSMWriter 写 block

### 8.1 WriteBlock

```go
func (w *tsmWriter) WriteBlock(key []byte, minTime, maxTime int64, block []byte) error {
    // 1. 获取或创建当前 block 的 index entry
    entry := w.getOrCreateIndexEntry(key, blockType)
    
    // 2. 追加数据到 buffer
    n, err := w.bw.Write(block)
    
    // 3. 更新 index entry
    entry.Offset = w.currentOffset
    entry.Size = uint32(n - 4)  // 减去 checksum
    entry.MinTime = minTime
    entry.MaxTime = maxTime
    
    return nil
}
```

### 8.2 WriteIndex

```go
func (w *tsmWriter) WriteIndex() error {
    // 1. 按 key 排序所有 index entries
    sort.Sort(w.index)
    
    // 2. 编码每个 key 和它的 entries
    for _, e := range w.index {
        // 写入: keyLen(2) + key(N) + type(1) + count(2) + entries...
        w.indexBytes = append(w.indexBytes, e.marshalBinary()...)
    }
    
    // 3. 写入索引数据和 footer
    w.bw.Write(w.indexBytes)
    w.bw.Write(indexFooter)  // 包含 index 开始位置
}
```

## 9. 完整流程总览

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Compaction 完整流程                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                        1. 计划阶段                                   │   │
│  │  ┌────────────────────────────────────────────────────────────────┐ │   │
│  │  │ Engine.Run() → CompactionNeeded() → CompactionPlan()          │ │   │
│  │  └────────────────────────────────────────────────────────────────┘ │   │
│  │                                │                                      │   │
│  │                                ▼                                      │   │
│  │  ┌────────────────────────────────────────────────────────────────┐ │   │
│  │  │ DefaultPlanner.Plan() / PlanLevel()                           │ │   │
│  │  │                                                                │ │   │
│  │  │ 1. FileStore.Stats() → 获取所有 TSM 文件信息                    │ │   │
│  │  │ 2. findGenerations() → 按 generation 分组                      │ │   │
│  │  │ 3. groupAdjacentGenerations() → 同 level 相邻分组              │ │   │
│  │  │ 4. 检查触发阈值 (Level 1: 8个, Level 2-4: 4个)                 │ │   │
│  │  │ 5. acquire() → 锁定文件                                        │ │   │
│  │  └────────────────────────────────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐       │
│  │                        2. 执行阶段                                 │       │
│  │  ┌────────────────────────────────────────────────────────────────┐ │       │
│  │  │ Compactor.CompactFull() / CompactFast()                      │ │       │
│  │  └────────────────────────────────────────────────────────────────┘ │       │
│  │                                │                                      │       │
│  │                                ▼                                      │       │
│  │  ┌────────────────────────────────────────────────────────────────┐ │       │
│  │  │ compact()                                                     │ │       │
│  │  │                                                                │ │       │
│  │  │ 1. 解析文件名获取 maxGeneration, maxSequence                   │ │       │
│  │  │ 2. 为每个 TSM 文件创建 TSMReader                               │ │       │
│  │  │ 3. 创建 TSMReader.BlockIterator() x N                        │ │       │
│  │  │ 4. 创建 NewTSMBatchKeyIterator(readers...)                    │ │       │
│  │  │ 5. writeNewFiles() → 写入合并后的 TSM                          │ │       │
│  │  └────────────────────────────────────────────────────────────────┘ │       │
│  └─────────────────────────────────────────────────────────────────────┘       │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐     │
│  │                        3. 迭代读取                                   │     │
│  │  ┌────────────────────────────────────────────────────────────────┐ │     │
│  │  │ tsmBatchKeyIterator.Next()                                    │ │     │
│  │  │                                                                │ │     │
│  │  │ for each TSMReader:                                           │ │     │
│  │  │   while block.key == currentMinKey:                           │ │     │
│  │  │     iter.Read() → 读取 raw block data                         │ │     │
│  │  │     加入 buf[i]                                               │ │     │
│  │  │                                                                │ │     │
│  │  │ 找到字典序最小的 key                                           │ │     │
│  │  │ 收集所有同 key 的 blocks → k.blocks                            │ │     │
│  │  │ k.merge() → 合并去重                                           │ │     │
│  │  └────────────────────────────────────────────────────────────────┘ │     │
│  └─────────────────────────────────────────────────────────────────────┘     │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐     │
│  │                        4. 数据合并                                   │     │
│  │  ┌────────────────────────────────────────────────────────────────┐ │     │
│  │  │ mergeFloat() / mergeInteger() / ...                        │ │     │
│  │  │                                                                │ │     │
│  │  │ 1. decode 所有 blocks 到内存                                   │ │     │
│  │  │ 2. 应用 tombstone 过滤                                        │ │     │
│  │  │ 3. 排序（按时间）                                             │ │     │
│  │  │ 4. 去重（同时间戳保留最新）                                   │ │     │
│  │  │ 5. re-encode 为新 block                                       │ │     │
│  │  │ 6. 加入 k.merged                                              │ │     │
│  │  └────────────────────────────────────────────────────────────────┘ │     │
│  └─────────────────────────────────────────────────────────────────────┘     │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐     │
│  │                        5. 文件写入                                   │     │
│  │  ┌────────────────────────────────────────────────────────────────┐ │     │
│  │  │ write() → TSMWriter                                          │ │     │
│  │  │                                                                │ │     │
│  │  │ for iter.Next():                                              │ │     │
│  │  │   key, minTime, maxTime, block := iter.Read()                 │ │     │
│  │  │   w.WriteBlock(key, minTime, maxTime, block)                 │ │     │
│  │  │   检查是否需要 rollToNext (> 2GB 或 block 数超限)             │ │     │
│  │  │                                                                │ │     │
│  │  │ w.WriteIndex() → 写入索引                                      │ │     │
│  │  │ w.Close() → 关闭文件                                           │ │     │
│  │  └────────────────────────────────────────────────────────────────┘ │     │
│  └─────────────────────────────────────────────────────────────────────┘     │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐     │
│  │                        6. 文件替换                                   │     │
│  │  ┌────────────────────────────────────────────────────────────────┐ │     │
│  │  │ os.Rename("000001-05.tsm.tmp", "000001-05.tsm")              │ │     │
│  │  │ 删除原始文件                                                   │ │     │
│  │  │ FileStore.Reopen() → 加载新文件                               │ │     │
│  │  └────────────────────────────────────────────────────────────────┘ │     │
│  └─────────────────────────────────────────────────────────────────────┘     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 10. OOM 根源分析

### 10.1 当前实现的内存问题

| 位置 | 问题 | 内存占用 |
|------|------|----------|
| `k.blocks` | 累积所有同 key 的 blocks | O(all blocks for one key) |
| `mergedFloatValues` 等 | 解码后的值全量存储 | O(all values for one key) |
| `allValues` 在 mergeFloat | 排序时的临时数组 | O(all values) |
| `tsmGeneration.files` | 一个 generation 的所有文件信息 | O(files per gen) |
| `FileStore.Stats()` | 全量返回所有文件的统计信息 | O(all files) |

### 10.2 大 Shard 场景

假设一个大 Shard:
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

但实际可能更大!
- 很多 key 可能同时累积
- 解码时的临时对象
- GC 开销
```

### 10.3 流式改造关键点

1. **移除 `k.blocks` 全量累积**
   - 使用堆/优先队列追踪每个 iterator 的当前 block
   - 只在需要时才读取下一个 block

2. **边读边吐**
   - 不再等待所有同 key blocks 齐备
   - 实现 k-way merge stream，每读到一个合并后的值就输出

3. **迭代器式 decode**
   - 不再 decode 到 `mergedFloatValues`
   - 实现 `ValueIterator` 接口，逐个 yield 值

4. **分块处理**
   - 限制同时在内存中的 key 数量
   - 超阈值时先写盘，释放内存

## 11. 关键代码路径

```
Engine.Run()
  └─► CompactionPlan()
        └─► DefaultPlanner.Plan() / PlanLevel()
              ├─► findGenerations()
              ├─► groupAdjacentGenerations()
              ├─► acquire()
              └─► 返回 CompactionGroup[]

Engine.compact()
  └─► Compactor.CompactFull() / CompactFast()
        └─► compact()
              ├─► FileStore.TSMReader() × N
              ├─► NewTSMBatchKeyIterator()
              └─► writeNewFiles()
                    ├─► write() × N
                    │     ├─► iter.Next()
                    │     ├─► iter.Read()
                    │     └─► TSMWriter.WriteBlock()
                    └─► TSMWriter.WriteIndex()

tsmBatchKeyIterator.Next()
  ├─► 从各 reader 读取 blocks
  ├─► 找到 min key
  ├─► 收集同 key blocks
  └─► merge()
        ├─► mergeFloat() / mergeInteger() / ...
        │     ├─► decode blocks
        │     ├─► apply tombstone
        │     ├─► sort
        │     ├─► deduplicate
        │     └─► re-encode
        └─► 返回 merged block
```

---

*文档版本: 1.0*
*最后更新: 2026-04-29*
