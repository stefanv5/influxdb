# Module 1: 写入引擎 (WAL + Cache + TSM 压缩) - 深度审计报告

> **小白导读**: 想象你开了一家快递站。每天有大量包裹（数据）涌入。
> - **WAL** = 快递单存根（Cache 写入后落盘，返回前等待同步，防丢件）
> - **Cache** = 暂存区（包裹先放这里，攒够一波再处理）
> - **TSM 文件** = 仓库货架（压缩存放，按编号排列，方便查找）
> - **Compaction** = 仓库整理（把小箱子合并成大箱子，腾出空间）
>
> 数据的生命周期：**收到包裹 → Shard 校验并登记字段 → 暂存(Cache) → 写 WAL 并等待 sync → 入库(TSM) → 整理(Compaction)**

## 1. 写入全链路总览

### 1.1 从 HTTP 请求到磁盘的完整路径

```mermaid
sequenceDiagram
    participant Client as 写入客户端
    participant HTTP as HTTP Handler
    participant PW as PointsWriter
    participant Store as Store
    participant Point as models.Point
    participant Shard as Shard
    participant Engine as TSM1 Engine
    participant Cache as Cache (16分区)
    participant WAL as WAL (Snappy压缩)
    participant Disk as 磁盘

    Client->>HTTP: POST /write?db=mydb&rp=autogen
    HTTP->>HTTP: 1 解析 Line Protocol -> []models.Point
    HTTP->>PW: 2 WritePointsPrivileged(writeCtx, ...)
    PW->>PW: 2a MapShards → 每个 shard 一个 goroutine
    PW->>Store: 2b WriteToShard(writeCtx, shardID, points)
    Store->>Shard: 2c epoch guard → Shard.WritePoints(points, tracker)
    Shard->>Shard: 3 validateSeriesAndFields<br/>字段类型/series 校验
    Shard->>Shard: 4 saveFieldsAndMeasurements<br/>保存字段与 measurement 元数据
    Shard->>Engine: 5 Engine.WritePoints(points, tracker)

    Engine->>Engine: 6 遍历每个 Point 的每个 Field
    Engine->>Engine: 7 构建 composite key: "cpu,host=web#!~#value"
    Engine->>Engine: 8 seriesTypeMap 补充类型检查
    Engine->>Engine: 9 转换为 TSM Value (Float/Integer/String/...)

    Engine->>Cache: 10 Cache.WriteMulti(values)
    Cache->>Cache: 11 计算 addedSize, 检查 maxSize
    Cache->>Cache: 12 乐观增加 size (原子操作)
    Cache->>Cache: 13 写入 ring 分区 (per-partition RLock)
    Cache->>Cache: 14 entry.add(values) 追加到 entry

    Engine->>WAL: 15 WAL.WriteMulti(values)
    WAL->>WAL: 16 Encode -> Snappy 压缩
    WAL->>WAL: 17 rollSegment() 检查是否需要新 segment
    WAL->>Disk: 18 写入 _NNNNN.wal 文件
    WAL->>WAL: 19 scheduleSync() 并等待 fsync

    Engine-->>HTTP: 返回 error / nil
    HTTP-->>Client: 204 No Content / 400 / 500

    Note over Shard,Engine: field type conflict 主要由 Shard 字段元数据校验发现；<br/>Engine 的 seriesTypeMap 是可选补充检查
    Note over Cache,WAL: 当前实现先写 Cache，再写 WAL；<br/>WAL 失败会返回错误，但 Cache 已经包含这批值
```

### 1.2 每一步的代码实现

#### 步骤 1: Line Protocol 解析

```go
// models/points.go:1641 — MakeKey
func MakeKey(name []byte, tags Tags) []byte {
    // 格式: "measurement,tag1=val1,tag2=val2"
    // tags 按字典序排列
}

// models/points.go:1469 — Point.Key
func (p *point) Key() []byte {
    return p.key  // 已解析好的 key
}
```

**Line Protocol 格式**: `measurement,tag1=val1,tag2=val2 field1=1.0,field2="hello" 1234567890000000000`

> **具体案例**: 写入一条 CPU 数据
>
> ```
> curl -X POST 'http://localhost:8086/write?db=mydb' \
>   -d 'cpu,host=web01,region=us-east value=87.3 1704067200000000000'
> ```
>
> 这条数据经过以下步骤：
>
> ```
> 1. HTTP 解析: measurement="cpu", tags={host="web01", region="us-east"}
>    fields={value=87.3}, timestamp=1704067200000000000 (2024-01-01 00:00:00 UTC)
>
> 2. Key 构建: "cpu,host=web01,region=us-east#!~#value"
>    (注意: tags 按字典序排列, 用 #!~# 分隔 key 和 field)
>
> 3. Shard 校验: `validateSeriesAndFields` 检查 series、field 元数据与字段类型
>    - 如果是新字段 → 放入待保存列表
>    - 如果已存在且类型匹配 → 继续
>    - 如果字段类型冲突 → 报错 "field type conflict"
>
> 4. Shard 保存元数据: `saveFieldsAndMeasurements(fieldsToCreate)`
>
> 5. Engine 补充检查与转换: `seriesTypeMap` 可选检查后，`NewFloatValue(1704067200000000000, 87.3)`
>
> 6. 写入 Cache: xxhash(key) % 16 → 分区 7 → entry.add(value)
>
> 7. 写入 WAL: 编码 → Snappy 压缩 → 写入 _00001.wal → 等待 fsync
> ```

**models.Point 内部结构**:

```go
// models/points.go:228 — point (未导出)
type point struct {
    time          time.Time
    key           []byte                    // "measurement,tag1=val1,tag2=val2"
    fields        []byte                    // "field1=1.0,field2=\"hello\""
    ts            []byte                    // "1234567890000000000" (字符串)
    cachedFields  map[string]interface{}    // 缓存解析后的 fields
    cachedName     string
    cachedTags     Tags
    it             fieldIterator
}
```

#### 步骤 2a–2e: PointsWriter → Store → Shard（HTTP 与 Shard 之间的两层）

真实调用链比总览图多两层，而且一条请求会按 shard 拆成多路并发写入：

```mermaid
flowchart TD
    H["HTTP Handler"] --> P["PointsWriter.WritePointsPrivileged<br/>points_writer.go:474"]
    P --> M["MapShards()<br/>points_writer.go:486"]
    M --> G["每个 shard 一个 goroutine<br/>points_writer.go:494-506"]
    G --> W["writeToShard()<br/>points_writer.go:545"]
    W --> S["Store.WriteToShard(writeCtx, shardID, points)<br/>store.go:1861"]
    S --> E["epoch.StartWrite() + guard.Wait()<br/>store.go:1882-1891"]
    E --> I["sh.IsIdle() → SetCompactionsEnabled(true)<br/>store.go:1894-1896"]
    I --> SP["Shard.WritePoints(points, tracker)<br/>store.go:1898"]
```

- **每 shard 一个 goroutine，首个错误即返回**：`WritePointsPrivileged` 用 `chan error` 收集，任一 shard 返回非 nil 立即返回（`points_writer.go:494-510`），其余 goroutine 不会被取消；整体还受 `w.WriteTimeout` 约束，超时返回 `ErrTimeout`（`points_writer.go:512-537`）。
- **ErrShardNotFound 重试一次**：`writeToShard` 在 `WriteToShard` 返回 `tsdb.ErrShardNotFound` 时先 `CreateShard(database, retentionPolicy, shard.ID, true)`，再重写一次（`points_writer.go:551-562`）。
- **epoch guard**：`Store.WriteToShard` 先 `epoch.StartWrite()`，对每个返回的 guard 调用 `guard.Wait()`，等待进行中的 drop/snapshot 完成后再写（`store.go:1882-1891`），末尾 `defer epoch.EndWrite(gen)`。
- **冷 shard 被写路径顺手唤醒**：`if isIdle, _ := sh.IsIdle(); isIdle { sh.SetCompactionsEnabled(true) }`（`store.go:1894-1896`）。
- **WriteContext 只服务统计**：`WriteContext` 当前只有 `UserId`（`store.go:1855-1859`），只在 `s.statsTracker(sh.database, sh.retentionPolicy, writeCtx.UserId)` 处被消费（`store.go:1898`）。`Store.WriteToShard`、`Shard.WritePoints`、`Engine.WritePoints` 都**没有** `context.Context` 形参，因此不存在"可取消的写路径"。

#### 步骤 3-5: Shard.WritePoints — 字段元数据校验与保存

```go
// tsdb/shard.go:562-609 — WritePoints
func (s *Shard) WritePoints(points []models.Point, tracker StatsTracker) error {
    s.mu.RLock()
    defer s.mu.RUnlock()

    engine, err := s.engineNoLock()
    if err != nil {
        return err
    }

    var writeError error
    atomic.AddInt64(&s.stats.WriteReq, 1)

    points, fieldsToCreate, err := s.validateSeriesAndFields(points, tracker)
    if err != nil {
        if _, ok := err.(PartialWriteError); !ok {
            return err
        }
        // 部分写失败时，丢弃冲突点，继续写入剩余 points。
        writeError = err
    }

    if numFieldsCreated, err := s.saveFieldsAndMeasurements(fieldsToCreate); err != nil {
        return err
    } else {
        atomic.AddInt64(&s.stats.FieldsCreated, int64(numFieldsCreated))
    }

    // 包装 tracker: 先转发外层 AddedPoints，再累加本 shard 的统计。
    engineTracker := tracker
    engineTracker.AddedPoints = func(points, values int64) {
        if tracker.AddedPoints != nil {
            tracker.AddedPoints(points, values)
        }
        atomic.AddInt64(&s.stats.WritePointsOK, points)
        atomic.AddInt64(&s.stats.WriteValuesOK, values)
    }

    if err := engine.WritePoints(points, engineTracker); err != nil {
        atomic.AddInt64(&s.stats.WritePointsErr, int64(len(points)))
        atomic.AddInt64(&s.stats.WriteReqErr, 1)
        return fmt.Errorf("engine: %w", err)
    }

    atomic.AddInt64(&s.stats.WriteReqOK, 1)
    return writeError
}
```

`field type conflict` 的主路径在 Shard 层：`validateSeriesAndFields` 会对照已知字段元数据，发现同一个 measurement/field 的类型变化时生成错误；`saveFieldsAndMeasurements` 再把新增字段和 measurement 元数据写入索引/元数据结构。`Engine.WritePoints` 里的 `seriesTypeMap` 只在启用 `INFLUXDB_SERIES_TYPE_CHECK_ENABLED` 时作为补充检查，防止绕过 Shard 元数据校验或并发路径中出现遗漏。

#### 步骤 6-9: Engine.WritePoints — Point 到 Value 的转换

```go
// tsdb/engine/tsm1/engine.go:1346 — WritePoints
func (e *Engine) WritePoints(points []models.Point, tracker tsdb.StatsTracker) error {
    values := make(map[string][]Value, len(points))
    var keyBuf []byte
    var seriesErr error
    var npoints, nvalues int64

    for _, p := range points {
        // 步骤 3: 构建 composite key
        keyBuf = append(keyBuf[:0], p.Key()...)           // "cpu,host=web"
        keyBuf = append(keyBuf, keyFieldSeparator...)     // "#!~#"
        baseLen := len(keyBuf)

        iter := p.FieldIterator()  // 零分配的字段迭代器
        t := p.Time().UnixNano()

        npoints++
        var nValuesForPoint int64

        // 步骤 4: 遍历每个 field
        for iter.Next() {
            // 构建完整 key: "cpu,host=web#!~#value"
            keyBuf = append(keyBuf[:baseLen], iter.FieldKey()...)

            // 步骤 5: 可选的补充类型冲突检查 (radix.Tree 快速路径)
            // 注意: field type conflict 主要已由 Shard 字段元数据校验处理。
            // seriesTypeMap 是一个功能标志 (feature flag)
            // 仅当环境变量 INFLUXDB_SERIES_TYPE_CHECK_ENABLED 设置时才初始化 (engine.go:297-299)
            // 默认为 nil，此分支不会执行
            if e.seriesTypeMap != nil {
                if v, ok := e.seriesTypeMap.Get(keyBuf); !ok {
                    if typ, err := e.Type(keyBuf); err != nil {
                        // 类型未知，可以尝试插入
                    } else if typ != iter.Type() {
                        seriesErr = tsdb.ErrFieldTypeConflict
                        e.seriesTypeMap.Insert(keyBuf, int(typ))
                        continue
                    }
                    vv, ok := e.seriesTypeMap.Insert(keyBuf, int(iter.Type()))
                    if !ok || vv != int(iter.Type()) {
                        seriesErr = tsdb.ErrFieldTypeConflict
                        continue
                    }
                } else if v != int(iter.Type()) {
                    seriesErr = tsdb.ErrFieldTypeConflict
                    continue
                }
            }

            // 步骤 6: 转换为 TSM Value
            var v Value
            switch iter.Type() {
            case models.Float:
                fv, err := iter.FloatValue()
                if err != nil { return err }
                v = NewFloatValue(t, fv)
            case models.Integer:
                iv, err := iter.IntegerValue()
                if err != nil { return err }
                v = NewIntegerValue(t, iv)
            case models.Unsigned:
                uv, err := iter.UnsignedValue()
                if err != nil { return err }
                v = NewUnsignedValue(t, uv)
            case models.String:
                v = NewStringValue(t, iter.StringValue())
            case models.Boolean:
                bv, err := iter.BooleanValue()
                if err != nil { return err }
                v = NewBooleanValue(t, bv)
            default:
                return fmt.Errorf("unknown field type for %s: %s", string(iter.FieldKey()), p.String())
            }

            nValuesForPoint++
            values[string(keyBuf)] = append(values[string(keyBuf)], v)
        }

        nvalues += nValuesForPoint
        if tracker.AddedMeasurementPoints != nil {
            tracker.AddedMeasurementPoints(models.ParseName(keyBuf), 1, nValuesForPoint)
        }
    }

    // 步骤 7: 写入 Cache (在写入前获取 e.mu.RLock()，engine.go:1440)
    e.mu.RLock()
    defer e.mu.RUnlock()

    if err := e.Cache.WriteMulti(values); err != nil {
        return err
    }

    // 步骤 12: 写入 WAL；WriteMulti 返回前会等待 WAL sync 完成。
    // 如果 WAL 写入或 sync 失败，函数返回错误，但 Cache 已经写入。
    if e.WALEnabled {
        if _, err := e.WAL.WriteMulti(values); err != nil {
            return err
        }
    }

    if tracker.AddedPoints != nil {
        tracker.AddedPoints(npoints, nvalues)
    }
    return seriesErr
}
```

**Key 构建示例**:

> **小白解释**: InfluxDB 的 key 不是简单的字段名，而是把**表名 + 标签 + 字段名**拼在一起。
> 为什么要用 `#!~#` 分隔符？因为它足够特殊，不会出现在正常数据中，避免歧义。
> 就像你的身份证号：省+市+区+出生日期，每一部分都有固定含义。

```mermaid
sequenceDiagram
    participant Point as 一条数据点
    participant Key as Key 构建器
    participant Cache as Cache

    Point->>Key: cpu,host=web value=3.14 12345
    Key->>Key: p.Key() = "cpu,host=web" (表名+标签)
    Key->>Key: 拼接 "#!~#" (分隔符)
    Key->>Key: iter.FieldKey() = "value" (字段名)
    Key->>Key: 完整 key = "cpu,host=web#!~#value"

    Note over Key: 如果有多个字段:<br>"cpu,host=web#!~#value" → 3.14<br>"cpu,host=web#!~#status" → "running"

    Key->>Cache: 存入 Cache 的对应分区
```

**FieldIterator — 零分配字段遍历**:

```go
// models/points.go:179 — FieldIterator (接口，非结构体)
type FieldIterator interface {
    Next() bool               // 是否还有下一个 field
    FieldKey() []byte         // 当前 field 的 key
    Type() models.FieldType   // 当前 field 的类型
    StringValue() string
    IntegerValue() (int64, error)
    UnsignedValue() (uint64, error)
    BooleanValue() (bool, error)
    FloatValue() (float64, error)
    Reset()
}
```

#### 步骤 7-11: Cache.WriteMulti — 16 分区并发写入

```go
// tsdb/engine/tsm1/cache.go:319-371 — WriteMulti
func (c *Cache) WriteMulti(values map[string][]Value) error {
    c.init()  // 懒初始化 ring store

    // 步骤 8: 计算总大小，检查限制
    var addedSize uint64
    for _, v := range values {
        addedSize += uint64(Values(v).Size())
    }

    limit := c.maxSize // maxSize 读取无需加锁
    n := c.Size() + addedSize  // Size() = size + snapshotSize
    if limit > 0 && n > limit {
        atomic.AddInt64(&c.stats.WriteErr, 1)
        return ErrCacheMemorySizeLimitExceeded(n, limit)
        // 没有驱逐! 直接拒绝写入!
    }

    // 步骤 9: 先取出 store 引用 (只有 RLock)，随后乐观增加 size
    c.mu.RLock()
    store := c.store
    c.mu.RUnlock()

    c.increaseSize(addedSize)

    // 步骤 10: 写入 ring 分区
    var werr error
    for k, v := range values {
        newKey, err := store.write(k, v)   // k 本身就是 string，不再做 []byte 转换
        if err != nil {
            werr = err
            addedSize -= uint64(Values(v).Size())
            c.decreaseSize(uint64(Values(v).Size()))
        }
        if newKey {                       // 新 key 还要为 map key 记账
            addedSize += uint64(len(k))
            c.increaseSize(uint64(len(k)))
        }
    }

    // 步骤 11: 统计与 lastWriteTime
    if werr != nil {
        atomic.AddInt64(&c.stats.WriteDropped, 1)
        atomic.AddInt64(&c.stats.WriteErr, 1)
    }
    c.updateMemSize(int64(addedSize))
    atomic.AddInt64(&c.stats.WriteOK, 1)

    c.mu.Lock()
    c.lastWriteTime = time.Now()
    c.mu.Unlock()

    return werr
}
```

**Cache 没有驱逐机制!** 当 `Size() > maxSize` 时，`WriteMulti` 直接返回 `ErrCacheMemorySizeLimitExceeded`。只有通过 `Snapshot()` + `ClearSnapshot()` 才能释放内存。

**Ring 分区写入 — 16 分区哈希环**:

```go
// tsdb/engine/tsm1/ring.go:16 — 最大分区数
const partitions = 16  // ring 支持的最大分区数 (并非实际使用数)

// tsdb/engine/tsm1/cache.go:22 — 实际使用的分区数
const ringShards = 16  // 默认使用 16 个分区

// tsdb/engine/tsm1/ring.go:32 — ring
type ring struct {
    partitions []*partition  // len = n；Cache 默认传入 ringShards=16
}

// tsdb/engine/tsm1/ring.go:79 — getPartition
func (r *ring) getPartition(key []byte) *partition {
    h := xxhash.Sum64(key)
    return r.partitions[h%uint64(len(r.partitions))]
}
```

> **注意**: `ring.go` 中 `const partitions = 16` 定义的是**最大值**（`newring(n)` 校验 n <= 16），
> 实际分区数由 `cache.go:22` 的 `const ringShards = 16` 决定。
> `newring()` 的注释（ring.go:46-50）声称 `n` "must always be a power of 2"，支持的值集为
> `{1, 2, 4, 8, 16}`。源码会同时验证 `n <= 16` 和 `n&(n-1) == 0`，
> 因此 `n=3`、`n=15`、`n=32` 都会失败。Cache 固定使用 16（2 的幂）。

```go
// tsdb/engine/tsm1/ring.go:227 — partition.write
func (p *partition) write(key []byte, values Values) (bool, error) {
    // 热路径: RLock 检查 entry 是否存在
    p.mu.RLock()
    e := p.store[string(key)]
    p.mu.RUnlock()

    if e != nil {
        // entry 已存在: 追加 values
        return false, e.add(values)
    }

    // 冷路径: 升级为写锁 (双重检查)
    p.mu.Lock()
    defer p.mu.Unlock()

    if e = p.store[string(key)]; e != nil {
        return false, e.add(values)  // 其他 goroutine 已创建
    }

    // 创建新 entry
    e, err := newEntryValues(values)
    p.store[string(key)] = e
    return true, nil  // newKey = true
}
```

**entry.add — 追加值（含类型冲突检查）**:

```go
// tsdb/engine/tsm1/cache.go:36 — entry
type entry struct {
    mu     sync.RWMutex
    values Values
    vtype  byte  // BlockFloat64 / BlockInteger / ...
}

// tsdb/engine/tsm1/cache.go:72 — add
func (e *entry) add(values []Value) error {
    // 空值直接返回
    if len(values) == 0 {
        return nil
    }

    // 类型冲突检查：如果 entry 已有类型，新值必须匹配
    if e.vtype != 0 {
        for _, v := range values {
            if e.vtype != valueType(v) {
                return tsdb.ErrFieldTypeConflict
            }
        }
    }

    e.mu.Lock()
    // 如果 entry 当前为空，直接设置（而非追加，避免多余分配）
    if len(e.values) == 0 {
        e.values = values
        e.vtype = valueType(values[0])
        e.mu.Unlock()
        return nil
    }

    // 追加到已有值
    e.values = append(e.values, values...)
    e.mu.Unlock()
    return nil
}
```

**已知边界（代码缺陷，不是设计保证）**：`valueType` 没有覆盖 `UnsignedValue`（`cache.go:798-812` 的 `default: return 0`），所以 unsigned entry 的 `vtype` 始终是 0，`entry.add` 里 `if e.vtype != 0` 的类型守卫被整体跳过。触发链：`fields.idx` 打开失败只 Warn 后继续（`engine.go:779-787`）→ WAL 重放 unsigned 值后 `vtype` 停在 0 → 再写同 field 的 float 不会被拦截 → snapshot 时 `encodeUnsignedBlockUsing` 的 `v.(UnsignedValue)` 断言 panic（`encoding.go:740-752`）。正常路径由 Shard 的字段元数据校验兜住，所以这是元数据损坏场景下的缺陷。

> **小白解释**: Cache 就像一个有 16 个抽屉的大柜子。每个 key 通过哈希决定放进哪个抽屉。
> 不同抽屉可以同时操作（并发），但同一个抽屉内要排队（锁）。
> 如果柜子满了（超过 maxSize），直接拒绝新数据，不会丢掉旧数据。

```mermaid
sequenceDiagram
    participant Writer as 写入 goroutine
    participant Cache as Cache
    participant Ring as Ring (16分区)
    participant Part as 目标分区
    participant Entry as entry

    Writer->>Cache: WriteMulti(values)
    Cache->>Cache: 计算 addedSize (所有 value 的内存大小)

    alt Size + addedSize > maxSize
        Cache-->>Writer: 返回 ErrCacheMemorySizeLimitExceeded<br>(柜子满了，拒绝!)
    else 有空间
        Cache->>Cache: increaseSize(addedSize) — 原子操作先占位

        loop 每个 key
            Cache->>Ring: xxhash(key) % 16 → 选择抽屉号
            Ring->>Part: 路由到目标分区

            alt entry 已存在 (这个 key 之前写过)
                Part->>Entry: RLock → entry.add(values) 追加
            else entry 不存在 (新 key)
                Part->>Part: Lock → 双重检查 → 创建新 entry
                Part->>Entry: newEntryValues(values)
            end
        end

        Cache-->>Writer: 返回 nil (成功)
    end
```

#### 步骤 12-16: WAL.WriteMulti — 持久化到 WAL

```go
// tsdb/engine/tsm1/wal.go:83 — WAL
type WAL struct {
    // goroutines waiting for the next fsync
    syncCount   uint64
    syncWaiters chan chan error

    mu            sync.RWMutex
    lastWriteTime time.Time

    path string

    // write variables
    currentSegmentID     int
    currentSegmentWriter *WALSegmentWriter

    // cache and flush variables
    once    sync.Once
    closing chan struct{}

    syncDelay time.Duration       // fsync 延迟时间 (默认 0 = 立即 fsync)

    SegmentSize int               // segment 文件滚动阈值 (默认 10MB)
    stats       *WALStatistics
    limiter     limiter.Fixed     // 并发写入限制器
}
```

**WAL 写入路径 — writeToLog**:

> **注意**: `writeToLog` 使用 `bytesPool` (pool.LimitedBytes) 从对象池获取编码缓冲区，
> 减少高频写入时的内存分配。当前源码在 `writeToLog` 中没有直接调用
> `l.limiter`; 慢盘下的等待主要由 WAL 写锁、`syncWaiters` 队列和 fsync 完成信号共同形成。

```go
// tsdb/engine/tsm1/wal.go:398 — writeToLog
func (l *WAL) writeToLog(entry WALEntry) (int, error) {
    // 1. 从 bytesPool 获取编码缓冲区 (减少分配)
    bytes := bytesPool.Get(entry.MarshalSize())
    b, err := entry.Encode(bytes)
    if err != nil {
        bytesPool.Put(bytes)
        return -1, err
    }

    // 2. 从 bytesPool 获取压缩缓冲区, Snappy 压缩
    encBuf := bytesPool.Get(snappy.MaxEncodedLen(len(b)))
    compressed := snappy.Encode(encBuf, b)
    bytesPool.Put(bytes)

    // 3. 每个写入者创建自己的 fsync 完成通道
    syncErr := make(chan error)

    segID, err := func() (int, error) {
        // 4. 获取写锁；编码和压缩已在锁外完成
        l.mu.Lock()
        defer l.mu.Unlock()

        // 5. 检查是否正在关闭
        select {
        case <-l.closing:
            return -1, ErrWALClosed
        default:
        }

        // 6. 滚动 segment (如果当前 segment > 10MB)
        if err := l.rollSegment(); err != nil {
            return -1, fmt.Errorf("error rolling WAL segment: %v", err)
        }

        // 7. 写入当前 segment
        if err := l.currentSegmentWriter.Write(entry.Type(), compressed); err != nil {
            return -1, fmt.Errorf("error writing WAL entry: %v", err)
        }

        // 8. 将本写入的等待通道放入 syncWaiters；队列满则写入失败
        select {
        case l.syncWaiters <- syncErr:
        default:
            return -1, fmt.Errorf("error syncing wal")
        }
        l.scheduleSync()

        atomic.StoreInt64(&l.stats.CurrentBytes, int64(l.currentSegmentWriter.size))
        l.lastWriteTime = time.Now().UTC()
        return l.currentSegmentID, nil
    }()

    // 9. 归还压缩缓冲区到 bytesPool
    bytesPool.Put(encBuf)

    if err != nil {
        return segID, err
    }

    // 10. 等待 scheduleSync/sync() 对 syncErr 写入 fsync 结果
    return segID, <-syncErr
}
```

**WAL Segment 文件命名**: `_<NNNNN>.wal` (如 `_00001.wal`, `_00002.wal`)

**Segment 滚动**:

```go
// tsdb/engine/tsm1/wal.go:466 — rollSegment
func (l *WAL) rollSegment() {
    if l.currentSegmentWriter == nil ||
       l.currentSegmentWriter.size > DefaultSegmentSize {  // 10MB
        l.newSegmentFile()
    }
}

// tsdb/engine/tsm1/wal.go:561 — newSegmentFile
func (l *WAL) newSegmentFile() error {
    l.currentSegmentID++
    fileName := filepath.Join(l.path, fmt.Sprintf("%s%05d.%s",
        WALFilePrefix, l.currentSegmentID, WALFileExtension))
    // 关闭旧 writer, 打开新文件
    // ...
}
```

**fsync 批量调度 — 延迟批处理机制**:

> **注意**: `scheduleSync` 并非立即触发 fsync，而是启动一个延迟批处理循环。
> `syncDelay` 字段控制 fsync 的延迟时间：默认值 0 表示每次写入都立即 fsync；
> 非零值（如 100ms）会等待一段时间，将多个写入的 fsync 合并为一次。

```go
// tsdb/engine/tsm1/wal.go:255 — scheduleSync
func (l *WAL) scheduleSync() {
    // CAS: 如果已有 goroutine 在负责 fsync，则直接返回
    // 后续写入者的 syncErr 通道会被加入 syncWaiters 队列
    if !atomic.CompareAndSwapUint64(&l.syncCount, 0, 1) {
        return
    }

    // 启动一个后台 goroutine 负责定期 fsync
    go func() {
        var timerCh <-chan time.Time

        // syncDelay == 0 时使用已关闭的 channel（立即触发）
        // syncDelay > 0 时使用 Ticker 定期触发
        if l.syncDelay == 0 {
            timerChrw := make(chan time.Time)
            close(timerChrw)  // 关闭后读取永远不阻塞
            timerCh = timerChrw
        } else {
            t := time.NewTicker(l.syncDelay)
            defer t.Stop()
            timerCh = t.C
        }

        for {
            select {
            case <-timerCh:
                l.mu.Lock()
                if len(l.syncWaiters) == 0 {
                    // 没有等待者，重置 syncCount，退出循环
                    atomic.StoreUint64(&l.syncCount, 0)
                    l.mu.Unlock()
                    return
                }
                l.sync()  // fsync 并通知所有等待者
                l.mu.Unlock()
            case <-l.closing:
                // WAL 正在关闭，退出
                atomic.StoreUint64(&l.syncCount, 0)
                return
            }
        }
    }()
}
```

**WAL struct 中 `syncDelay` 字段** (`wal.go:101-104`):

```go
// syncDelay 设置 fsync 前的等待时间。默认值 0 表示每次写入都立即 fsync。
// 必须在 WAL 打开之前设置非默认值。
syncDelay time.Duration
```

**fsync 批量机制工作流程**:
1. 第一个写入者通过 CAS 成为 "fsync 负责人"，启动后台 goroutine
2. 后续写入者将自己的 `syncErr` 通道放入 `syncWaiters` 队列（容量 1024）
3. 后台 goroutine 根据 `syncDelay` 定期检查 `syncWaiters`：
   - 如果有等待者：执行 `sync()`（fsync 当前 segment + 通知所有等待者）
   - 如果没有等待者：重置 `syncCount`，退出循环（下次写入会重新启动）
4. 收到 `l.closing` 信号时退出

**WriteWALEntry 二进制格式**:

```mermaid
flowchart TD
    subgraph "WAL Segment 文件格式"
        A["Segment Entry Header:<br/>WalEntryType(1B): 1=Write, 2=Delete, 3=DeleteRange<br/>Compressed Length(4B，压缩后长度)"] --> B["Compressed Payload (Snappy)"]
    end

    subgraph "WriteWALEntry 编码 (压缩前；每个 key 前 1B 值类型)"
        C["Value Type: 1B<br/>1=Float, 2=Integer, 3=Boolean, 4=String, 5=Unsigned"]
        C --> D["Key Len: 2B"]
        D --> E["Key: NB (如 'cpu,host=web#!~#value')"]
        E --> F["Count: 4B (值的数量)"]
        F --> G["Timestamp: 8B (UnixNano)"]
        G --> H["Value: NB (类型相关)"]
        H --> I["重复 Timestamp+Value..."]
    end
```

**值类型的二进制编码**:

> **重要区分**: WAL entry 中的 Type 字节表示**每个 key 的值类型** (float64EntryType=1, integerEntryType=2, booleanEntryType=3, stringEntryType=4, unsignedEntryType=5)，
> 这与 TSM block 类型常量 (BlockFloat64=0, BlockInteger=1, BlockBoolean=2, BlockString=3, BlockUnsigned=4) 是**不同的编号系统**。
>
> | 类型 | WAL Type 字节 | TSM Block Type |
> |------|--------------|----------------|
> | Float | 1 (float64EntryType) | 0 (BlockFloat64) |
> | Integer | 2 (integerEntryType) | 1 (BlockInteger) |
> | Boolean | 3 (booleanEntryType) | 2 (BlockBoolean) |
> | String | 4 (stringEntryType) | 3 (BlockString) |
> | Unsigned | 5 (unsignedEntryType) | 4 (BlockUnsigned) |

| 类型 | WAL Type 字节 | Timestamp | Value | 每个值大小 |
|------|--------------|-----------|-------|-----------|
| Float | 1 | 8B | 8B (float64) | 16B |
| Integer | 2 | 8B | 8B (int64) | 16B |
| Boolean | 3 | 8B | 1B (bool) | 9B |
| String | 4 | 8B | 4B(len) + NB | 12+N B |
| Unsigned | 5 | 8B | 8B (uint64) | 16B |

#### 步骤 12b: Snapshot 触发条件

```go
// tsdb/engine/tsm1/engine.go:2121 — ShouldCompactCache
func (e *Engine) ShouldCompactCache(t time.Time) bool {
    sz := e.Cache.Size()

    // 条件 1: Cache 为空则不压缩
    if sz == 0 {
        return false
    }
    // 条件 2: Cache 大小超过阈值 (默认 25MB)
    if sz > e.CacheFlushMemorySizeThreshold {
        return true
    }
    // 条件 3: 写入冷超过时长 (默认 10m，仅在未达 25MB 阈值时生效)
    // 注意: 使用 e.Cache.LastWriteTime() 而非 e.lastWriteTime
    return t.Sub(e.Cache.LastWriteTime()) > e.CacheFlushWriteColdDuration
}
```

**Snapshot 流程**:

```go
// tsdb/engine/tsm1/cache.go:375-421 — Snapshot
func (c *Cache) Snapshot() (*Cache, error) {
    c.init()

    c.mu.Lock()
    defer c.mu.Unlock()

    // 防止并发 snapshot
    if c.snapshotting {
        return nil, ErrSnapshotInProgress
    }
    c.snapshotting = true
    c.snapshotAttempts++

    // 懒创建 snapshot Cache（首次调用时 c.snapshot 为 nil）
    if c.snapshot == nil {
        store, err := newring(ringShards)
        if err != nil {
            return nil, err
        }
        c.snapshot = &Cache{store: store}
    }

    // 如果上次 snapshot 失败且有数据，返回旧 snapshot 重试
    if c.snapshot.Size() > 0 {
        return c.snapshot, nil
    }

    // O(1) 交换: live store ↔ snapshot store
    c.snapshot.store, c.store = c.store, c.snapshot.store

    // 保存 snapshot 大小，重置 live store
    snapshotSize := c.Size()
    atomic.StoreUint64(&c.snapshot.size, snapshotSize)  // snapshot cache 大小
    atomic.StoreUint64(&c.snapshotSize, snapshotSize)   // live cache 的 snapshotSize

    c.store.reset()
    atomic.StoreUint64(&c.size, 0)  // 原子操作重置 live cache 大小
    c.lastSnapshot = time.Now()

    c.updateCachedBytes(snapshotSize)
    c.updateSnapshots()

    return c.snapshot, nil
}
```

> 注意顺序：`c.init()`、`snapshotAttempts++` 与懒创建 `c.snapshot` 都必须在访问 `c.snapshot.Size()` **之前**完成；否则首次快照会在 nil 指针上调用 `Size()` 直接 panic。

> **小白解释**: Snapshot 就像"拍照"——把当前暂存区的数据冻结起来，然后清空暂存区继续接新货。
> 关键技巧：不是复制数据，而是直接**交换两个柜子的指针**（O(1) 操作，极快）。

```mermaid
sequenceDiagram
    participant BG as 后台 goroutine
    participant Cache as Cache
    participant Live as Live Store (活跃)
    participant Snap as Snapshot Store (快照)

    BG->>Cache: Snapshot()

    alt 正在 snapshot
        Cache-->>BG: 返回 ErrSnapshotInProgress
    else 上次 snapshot 失败且有数据
        Cache-->>BG: 返回旧 snapshot (重试)
    else 正常情况
        Note over Live, Snap: O(1) 指针交换!
        Cache->>Live: 交换: live.store ↔ snapshot.store
        Note over Live: live store 变空了
        Note over Snap: snapshot store 拿到了旧数据

        Cache->>Cache: snapshotSize = Size() (记住旧数据大小)
        Cache->>Cache: size = 0 (活跃区大小归零)

        Cache-->>BG: 返回 snapshot (可以慢慢处理)
    end
```

**Snapshot 后的写入流程**:

```mermaid
sequenceDiagram
    participant Writer as 写入 goroutine
    participant Live as Live Store
    participant Snapshot as Snapshot Store
    participant Compactor as Compactor

    Writer->>Live: 新写入进入 live store

    Note over Live, Snapshot: Snapshot() 执行
    Live->>Snapshot: 交换指针
    Note over Live: live store 清空

    Writer->>Live: 继续写入 live store

    Compactor->>Snapshot: 读取 snapshot
    Compactor->>Compactor: Deduplicate() — 排序去重
    Compactor->>Compactor: WriteSnapshot() — 写入 TSM 文件

    Compactor->>Snapshot: ClearSnapshot()
    Note over Snapshot: snapshot 清空
```

## 2. Value 类型系统

### 2.1 Value 接口

```go
// tsdb/engine/tsm1/encoding.go:96 — Value 接口
type Value interface {
    UnixNano() int64      // 时间戳 (纳秒)
    Value() interface{}   // 值 (float64/int64/uint64/string/bool)
    Size() int            // 内存大小 (字节)
    String() string       // 字符串表示
    internalOnly()        // 未导出方法 — 禁止包外实现
}
```

### 2.2 五种具体类型

```go
// tsdb/engine/tsm1/encoding.go:322 — FloatValue
type FloatValue struct {
    unixnano int64    // 8 字节
    value    float64  // 8 字节
}  // Size() = 16

// tsdb/engine/tsm1/encoding.go:584 — IntegerValue
type IntegerValue struct {
    unixnano int64  // 8 字节
    value    int64  // 8 字节
}  // Size() = 16

// tsdb/engine/tsm1/encoding.go:703 — UnsignedValue
type UnsignedValue struct {
    unixnano int64   // 8 字节
    value    uint64  // 8 字节
}  // Size() = 16

// tsdb/engine/tsm1/encoding.go:457 — BooleanValue
type BooleanValue struct {
    unixnano int64  // 8 字节
    value    bool   // 1 字节
}  // Size() = 9

// tsdb/engine/tsm1/encoding.go:822 — StringValue
type StringValue struct {
    unixnano int64   // 8 字节
    value    string  // len(value) 字节
}  // Size() = 8 + len(value)
```

**Block 类型常量**:

```go
// tsdb/engine/tsm1/encoding.go:14-28
BlockFloat64  = byte(0)
BlockInteger  = byte(1)
BlockBoolean  = byte(2)
BlockString   = byte(3)
BlockUnsigned = byte(4)
```

### 2.3 Values 集合操作

```go
// tsdb/engine/tsm1/encoding.gen.go:17 — Values
type Values []Value

// tsdb/engine/tsm1/encoding.gen.go:36 — Size
func (a Values) Size() int {
    sz := 0
    for _, v := range a {
        sz += v.Size()
    }
    return sz
}

// tsdb/engine/tsm1/encoding.gen.go:61 — Deduplicate
func (a Values) Deduplicate() Values {
    // 已排序且无重复时直接返回原切片 (零分配)
    // 否则原地 sort.Stable(a)，再就地压缩重复项 (相同时间戳保留最后一个)
    // 返回 a[:i+1] —— 与原切片共享底层数组，不是"新切片"
}
```

## 3. TSM 文件格式

> **小白解释**: TSM 文件就像一本书：
> - **Header** = 封面（写明书名和版本）
> - **Blocks** = 正文（每一页是一个数据块，记录了某个 key 的一段时间的数据）
> - **Index** = 目录（记录每个 key 在哪一页、时间范围是什么）
> - **Footer** = 书签（告诉你目录在第几页）
>
> 查找数据时：先看书签(Footer) → 翻到目录(Index) → 找到 key → 翻到对应页(Block) → 读取数据

### 3.1 文件整体布局

```
┌────────┬────────────────────────────────────┬─────────────┬──────────────┐
│ Header │               Blocks               │    Index    │    Footer    │
│5 bytes │              N bytes               │   N bytes   │   8 bytes    │
└────────┴────────────────────────────────────┴─────────────┴──────────────┘
```

### 3.2 Header (5 字节)

```
┌───────────────────┐
│  Magic   │ Version│
│ 4 bytes  │ 1 byte │
└───────────────────┘
```

```go
// tsdb/engine/tsm1/writer.go:81 — MagicNumber
MagicNumber uint32 = 0x16D116D1
Version     byte   = 1
```

### 3.3 Block 格式

```
┌─────────┬─────────────────────────────────────┐
│  CRC32  │              Data                   │
│ 4 bytes │             N bytes                 │
└─────────┴─────────────────────────────────────┘
```

**Data 内部格式 (packBlock)**:

```
┌──────┬─────────────────┬──────────────┬─────────────────┐
│ Type │ Timestamp Len   │  Timestamps  │     Values      │
│1 byte│ 1-10 bytes(uvi) │  N bytes     │    M bytes      │
└──────┴─────────────────┴──────────────┴─────────────────┘
```

```go
// tsdb/engine/tsm1/encoding.go:943 — packBlock
func packBlock(buf []byte, typ byte, ts []byte, values []byte) []byte {
    sz := 1 + binary.MaxVarintLen64 + len(ts) + len(values)
    if cap(buf) < sz {
        buf = make([]byte, sz)
    }
    b := buf[:sz]
    b[0] = typ                           // 字节 0: block 类型
    i := binary.PutUvarint(b[1:1+binary.MaxVarintLen64], uint64(len(ts)))
    i += 1                               // 跳过 type 字节
    copy(b[i:], ts)                      // 时间戳压缩字节
    copy(b[i+len(ts):], values)          // 值压缩字节 (无长度前缀)
    return b[:i+len(ts)+len(values)]
}
```

### 3.4 Index 格式

**每个 key 的索引条目**:

```
┌─────────┬─────────┬──────┬───────┬─────────┬─────────┬────────┬────────┐
│ Key Len │   Key   │ Type │ Count │Min Time │Max Time │ Offset │  Size  │
│ 2 bytes │ N bytes │1 byte│2 bytes│ 8 bytes │ 8 bytes │8 bytes │4 bytes │
└─────────┴─────────┴──────┴───────┴─────────┴─────────┴────────┴────────┘
```

**IndexEntry (28 字节)**:

```go
// tsdb/engine/tsm1/writer.go:180 — IndexEntry
type IndexEntry struct {
    MinTime int64   // 8 字节
    MaxTime int64   // 8 字节
    Offset  int64   // 8 字节
    Size    uint32  // 4 字节
}  // 总计: 28 字节
```

### 3.5 Footer (8 字节)

```
┌─────────┐
│Index Ofs│
│ 8 bytes │
└─────────┘
```

指向 Index 的起始位置。

> **注意**: `writer.go:8` 的文件头注释声称 Footer 是 "4 bytes"，这是**错误的**。
> 实际 `WriteIndex()` 在 `writer.go:726-731` 使用 `binary.BigEndian.PutUint64` 写入 8 字节的 uint64。
> Footer 实际大小为 **8 字节**。

### 3.6 压缩算法

> **小白解释**: 压缩就像打包行李——把散乱的衣服卷起来，塞进更小的空间。
> 不同类型的数据用不同的压缩方式，就像衣服、鞋子、液体要用不同的打包方法：
> - **Float (浮点数)**: 用 Gorilla XOR 编码——连续相似的数字只记录差异
> - **Integer (整数)**: 用 Delta + Simple8b——记录差值，小数字占更少空间
> - **Timestamp (时间戳)**: 用 Delta + 缩放——时间间隔通常固定，差值很小
> - **Boolean (布尔值)**: 位打包——8 个 true/false 只占 1 字节
> - **String (字符串)**: Snappy 压缩——通用压缩算法

```mermaid
flowchart TD
    A["压缩算法选择"] --> B{"数据类型?"}
    B -->|"Float"| C["Gorilla XOR 编码"]
    B -->|"Integer/Unsigned"| D["Delta + ZigZag + Simple8b/RLE"]
    B -->|"Timestamp"| E["Delta + 10 的幂缩放 + Simple8b/RLE"]
    B -->|"Boolean"| F["位打包 (1 bit/值)"]
    B -->|"String"| G["Snappy 压缩"]

    C --> H["首值: 64 bits 原始<br>后续: 0-bit (相同) / 1+leading+trailing (不同)"]
    D --> I["差值 → ZigZag 编码 → Simple8b 打包"]
    E --> J["差值 ÷ 10 的幂（缩小）→ Simple8b 打包"]
```

**Gorilla XOR 编码示例**:

> **小白解释**: Gorilla 编码的核心思想是——相邻的浮点数通常很相似。
> 比如温度传感器每秒报一次：25.1, 25.1, 25.2, 25.1... 大部分值几乎一样。
> 所以：第一个值完整记录(64位)，后面的值只记录和前一个的**差异部分**。
> 如果完全相同，只用 1 个 bit (0)；如果不同，记录差异的前导零和有效位。

```
输入: [1.0, 1.0, 2.0]

值 1.0 (首值): 完整记录 64 bits
  → 0011111111110000000000000000000000000000000000000000000000000000

值 1.0 (第二个): 和前一个完全相同
  XOR = 1.0 XOR 1.0 = 0 (全零)
  → 只写 1 bit: 0

值 2.0 (第三个): 和前一个不同
  XOR = 2.0 XOR 1.0 = 0x3FF0000000000000 (不为零)
  → 写 1 bit (标记不同) + 5 bits (前导零数) + 6 bits (有效位数) + 有效位
  → 大约 20 bits

总计: 64 + 1 + ~20 = ~85 bits (原始 3×64=192 bits，压缩率 44%!)
```

## 4. TSM Writer — 写入 TSM 文件

### 4.1 tsmWriter.Write — 逐步写入

```go
// tsdb/engine/tsm1/writer.go:592-644 — tsmWriter.Write
func (t *tsmWriter) Write(key []byte, values Values) error {
    // 1. 验证 key 长度 (maxKeyLength = 65535)
    if len(key) > maxKeyLength {
        return ErrMaxKeyLengthExceeded
    }
    if len(values) == 0 {
        return nil
    }

    // 2. 写入 header (首次)
    if t.n == 0 {
        if err := t.writeHeader(); err != nil {
            return err
        }
    }

    // 3. 编码 values 为 block，并从编码后的 block 读取类型
    block, err := values.Encode(nil)
    if err != nil {
        return err
    }
    blockType, err := BlockType(block)
    if err != nil {
        return err
    }

    // 4. 计算 CRC32
    var checksum [crc32.Size]byte
    binary.BigEndian.PutUint32(checksum[:], crc32.ChecksumIEEE(block))

    // 5. 写入 block: CRC32 + Data
    if _, err := t.w.Write(checksum[:]); err != nil {
        return err
    }
    n, err := t.w.Write(block)
    if err != nil {
        return err
    }
    n += len(checksum)

    // 6. 记录到索引
    t.index.Add(key, blockType,
        values[0].UnixNano(),             // MinTime
        values[len(values)-1].UnixNano(), // MaxTime
        t.n,                              // Offset
        uint32(n))                        // Size

    // 7. 更新偏移量
    t.n += int64(n)

    // 8. 单个 key 的 block 数达到上限 (maxIndexEntries = 65535)
    if len(t.index.Entries(key)) >= maxIndexEntries {
        return ErrMaxBlocksExceeded
    }
    return nil
}
```

> 两个常量都在 `writer.go:97-100`：`maxIndexEntries = (1 << (indexCountSize*8)) - 1`（=65535）、`maxKeyLength = (1 << (2*8)) - 1`（=65535）；对应错误 `ErrMaxBlocksExceeded` / `ErrMaxKeyLengthExceeded` 在 `writer.go:114-118`。

### 4.2 tsmWriter.WriteIndex — 写入索引

```go
// tsdb/engine/tsm1/writer.go:708-734 — WriteIndex
func (t *tsmWriter) WriteIndex() error {
    // 1. 记录当前偏移量 (Index 起始位置)
    indexPos := t.n

    // 2. 没有任何索引条目 → ErrNoValues (调用方据此丢弃空文件)
    if t.index.KeyCount() == 0 {
        return ErrNoValues
    }

    // 3. 让 directIndex 在写索引期间周期性 fsync
    if f, ok := t.wrapped.(syncer); ok {
        t.index.(*directIndex).f = f
    }

    // 4. 写入所有 key 的索引 (error 必须返回)
    if _, err := t.index.WriteTo(t.w); err != nil {
        return err
    }

    // 5. 写入 Footer: 8 字节 Index 偏移量 (error 必须返回)
    var buf [8]byte
    binary.BigEndian.PutUint64(buf[:], uint64(indexPos))
    _, err := t.w.Write(buf[:])
    return err
}
```

### 4.3 directIndex.Add — 索引构建

```go
// tsdb/engine/tsm1/writer.go:274 — directIndex.Add
func (d *directIndex) Add(key []byte, blockType byte, minTime, maxTime int64,
    offset int64, size uint32) {

    // 第一个 key: 初始化
    if len(d.key) == 0 {
        d.key = key
        d.indexEntries = &indexEntries{}
        d.indexEntries.Type = blockType
        d.indexEntries.entries = append(d.indexEntries.entries, IndexEntry{...})
        d.keyCount++
        return
    }

    // 使用 bytes.Compare 进行三路比较 (而非 bytes.Equal)
    cmp := bytes.Compare(d.key, key)
    if cmp == 0 {
        // 同一个 key: 追加 IndexEntry
        d.indexEntries.entries = append(d.indexEntries.entries, IndexEntry{...})
    } else if cmp < 0 {
        // 新 key (按字典序更大): 刷新旧 key 的索引数据
        d.flush(d.w)
        d.key = key
        d.indexEntries.Type = blockType
        d.indexEntries.entries = append(d.indexEntries.entries, IndexEntry{...})
        d.keyCount++
    } else {
        // key 乱序: 直接 panic
        panic(fmt.Sprintf("keys must be added in sorted order: %s < %s",
            string(key), string(d.key)))
    }
}
```

`directIndex` 不拷贝 `key`，而是直接保存传入的 `[]byte` 引用；调用方必须保证该 key
在索引 entry flush 前内容稳定。这个约束也解释了为什么 TSM 写入必须按 key 单调递增。

**directIndex.flush — 写入一个 key 的索引**:

```go
// tsdb/engine/tsm1/writer.go — flush
// 数据结构: d.key (当前 key), d.indexEntries (当前 key 的所有 IndexEntry)
func (d *directIndex) flush(w io.Writer) {
    // 1. 写入 2 字节 key 长度 + key 字节
    binary.BigEndian.PutUint16(buf, uint16(len(d.key)))
    w.Write(d.key)

    // 2. 写入 1 字节 type + 2 字节 count
    w.WriteByte(d.indexEntries.Type)
    binary.BigEndian.PutUint16(buf, uint16(len(d.indexEntries.entries)))

    // 3. 写入每个 IndexEntry (28 字节)
    d.indexEntries.WriteTo(w)
}
```

## 5. 压缩算法详解

### 5.1 Float: Gorilla XOR 编码

```go
// tsdb/engine/tsm1/float.go:29 — FloatEncoder
type FloatEncoder struct {
    val   float64
    err   error

    leading  uint64   // 注意: 是 uint64, 不是 uint8
    trailing uint64   // 注意: 是 uint64, 不是 uint8

    buf      bytes.Buffer
    bw       *bitstream.BitWriter

    first    bool     // true 表示下一个写入的是首值
    finished bool
}
```

```go
// tsdb/engine/tsm1/float.go:87 — Write
func (s *FloatEncoder) Write(v float64) {
    // NaN 只允许由 Flush() 在 finished=true 后作为结束 sentinel 写入；
    // 用户值 NaN 会记录错误并返回。
    if math.IsNaN(v) && !s.finished {
        s.err = fmt.Errorf("unsupported value: NaN")
        return
    }

    if s.first {
        // 首值: 原始 64 bits
        s.val = v
        s.first = false
        s.bw.WriteBits(math.Float64bits(v), 64)
        return
    }

    // 后续值: XOR
    vDelta := math.Float64bits(v) ^ math.Float64bits(s.val)

    if vDelta == 0 {
        // 情况1: 相同值 → 写入 0-bit
        s.bw.WriteBit(bitstream.Zero)
    } else {
        s.bw.WriteBit(bitstream.One)

        leading := uint64(bits.LeadingZeros64(vDelta))
        trailing := uint64(bits.TrailingZeros64(vDelta))

        // 源码直接按 5 位掩码保留低 5 位；原始 leading==32 会变成 0。
        // 后面的 leading>=32 分支在当前顺序下不可达，属于遗留防御代码。
        leading &= 0x1F
        if leading >= 32 { leading = 31 }
        // trailing 的计算: significant = 64 - leading - trailing
        // 因此 trailing = 64 - leading - meaningful_bits
        // trailing 不单独存储，而是通过 significant bits 数量隐式表达

        // 情况2: XOR!=0 且 leading/trailing 可复用
        if s.leading != ^uint64(0) && leading >= s.leading && trailing >= s.trailing {
            s.bw.WriteBit(bitstream.Zero)  // 复用已有 leading/trailing
            significant := 64 - s.leading - s.trailing
            s.bw.WriteBits(vDelta>>s.trailing, significant)
        } else {
            // 情况3: XOR!=0 且需要新的 leading/trailing
            s.leading, s.trailing = leading, trailing
            s.bw.WriteBit(bitstream.One)   // 标记新的 leading/trailing
            s.bw.WriteBits(leading, 5)
            significant := 64 - leading - trailing
            // significant==64 时写入 0，解码端按 Gorilla 规则还原为 64
            s.bw.WriteBits(significant, 6)
            s.bw.WriteBits(vDelta>>trailing, significant)
        }
    }

    s.val = v
}
```

```mermaid
flowchart TD
    A["输入值 v"] --> B{"math.IsNaN(v) 且 !finished?"}
    B -->|"是"| C["记录 err=unsupported value: NaN<br>不写入用户 NaN"]
    B -->|"否"| D{"first?"}
    D -->|"是"| E["写原始 64 bits<br>first=false"]
    D -->|"否"| F["vDelta = bits(v) XOR bits(prev)"]
    F --> G{"vDelta == 0?"}
    G -->|"是"| H["写 0-bit"]
    G -->|"否"| I["写 1-bit<br>计算 leading/trailing"]
    I --> J{"可复用历史 leading/trailing?"}
    J -->|"是"| K["写 0-bit + significant bits"]
    J -->|"否"| L["写 1-bit + 5-bit leading + 6-bit sigbits + payload"]
```

> **具体案例**: 用户写入 `[1.0, NaN]` 时，第二个值不会被编码，`e.err`
> 记录 `unsupported value: NaN`。`Flush()` 收尾时设置 `finished=true`，
> 然后调用 `Write(math.NaN())` 写入内部 NaN sentinel；`Bytes()` 只返回
> 当前 buffer 和 error。

### 5.2 Integer: Delta + ZigZag + Simple8b / RLE

```go
// tsdb/engine/tsm1/int.go:31-37 — 三种编码类型（写在 block 首字节高 4 位）
const (
    intUncompressed     = 0 // 每个 ZigZag 差值 8B 原样存储
    intCompressedSimple = 1 // simple8b 位打包
    intCompressedRLE    = 2 // 游程编码
)

// tsdb/engine/tsm1/int.go:40-45 — IntegerEncoder
type IntegerEncoder struct {
    prev   int64
    rle    bool     // 到目前为止所有 ZigZag 差值是否相同（初始 true）
    values []uint64 // ZigZag 编码后的差值
    // 没有 enc *simple8b.Encoder 字段：simple8b 只在 Bytes() 里按需编码
}
```

```go
// tsdb/engine/tsm1/int.go:65-77 — Write
func (e *IntegerEncoder) Write(v int64) {
    delta := v - e.prev          // 先差分，差值可能为负
    e.prev = v
    enc := ZigZagEncode(delta)   // -1→1, 0→0, 1→2, -2→3
    if len(e.values) > 1 {       // 至少有两个差值后才更新 rle
        e.rle = e.rle && e.values[len(e.values)-1] == enc
    }
    e.values = append(e.values, enc)
}
```

**Bytes() 的三路选择**（`int.go:79-93`）：

```mermaid
flowchart TD
    A["Bytes()"] --> B{"rle && len(values) > 2 ?"}
    B -->|"是"| C["encodeRLE()"]
    B -->|"否"| D{"存在 value > simple8b.MaxValue ?"}
    D -->|"是"| E["encodeUncompressed()"]
    D -->|"否"| F["encodePacked()"]
```

| 输出 | 头 1B（高 4 位） | 后续字节 |
|------|------------------|----------|
| RLE | `2 << 4` = 0x20 | 首个差值 8B（大端） + 首个增量 uvarint + 重复次数 uvarint（`len-1`） |
| Uncompressed | `0 << 4` = 0x00 | 每个 ZigZag 差值 8B（大端） |
| Packed | `1 << 4` = 0x10 | 首个差值 8B（不压缩） + `simple8b.EncodeAll(values[1:])` 的 64-bit words |

> 阈值是 `simple8b.MaxValue`（`1<<60 - 1`）；RLE 分支要求 `len(values) > 2`，只有两个相同差值时仍走 packed。每个 64-bit word 装 **1–240** 个值（随位宽自适应，`int.go:158`），不是固定 10 个。

**案例**：输入 `[100, 102, 105, 103]`
- 差值 `[100, 2, 3, -2]` → ZigZag `[200, 4, 6, 3]`
- `rle` 在第二个差值处变为 false（200 ≠ 4）
- `Bytes()`：不满足 RLE；最大值 200 < `MaxValue` → **Packed**
- 输出 = `0x10` + `200` 的 8B 大端 + `simple8b([4, 6, 3])`

### 5.3 Timestamp: Delta + Scale + Simple8b / RLE

```go
// tsdb/engine/tsm1/timestamp.go:85-117 — reduce（逆序原地差分）
func (e *encoder) reduce() (max, divisor uint64, rle bool, deltas []uint64) {
    deltas = e.ts                 // 原地差分，不新分配
    max, divisor = 0, 1e12        // divisor 从 1e12 开始，单调除 10
    rle = true
    for i := len(deltas) - 1; i > 0; i-- {   // 必须逆序，否则会覆盖前一个原值
        deltas[i] = deltas[i] - deltas[i-1]
        v := deltas[i]
        if v > max { max = v }
        for divisor > 1 && v%divisor != 0 { divisor /= 10 }
        rle = i == len(deltas)-1 || rle && (deltas[i+1] == deltas[i])
    }
    return
}
```

> **关键**：`deltas[0]` 仍是**第一个原始时间戳**，不是差值；差值从 `deltas[1]` 开始。

```go
// tsdb/engine/tsm1/timestamp.go:119-139 — Bytes
if len(e.ts) == 0 { return e.bytes[:0], nil }
max, div, rle, dts := e.reduce()
if rle && len(e.ts) > 1 {                       // 注意：时间戳 RLE 是 len > 1
    return e.encodeRLE(e.ts[0], e.ts[1], div, len(e.ts))
}
if max > simple8b.MaxValue {                    // 1<<60 - 1
    return e.encodeRaw()
}
return e.encodePacked(div, dts)
```

| 输出 | 头 1B | 后续字节 | 源码 |
|------|-------|----------|------|
| RLE | `2<<4 \| log10(div)` | 首个时间戳 8B + `delta/div` uvarint + 个数 uvarint | `timestamp.go:194-216` |
| Raw | `0<<4` | 每个原始时间戳 8B | `timestamp.go:181-192` |
| Packed | `1<<4 \| log10(div)` | 首个时间戳 8B（不压缩） + `simple8b(dts[1:]/div)` | `timestamp.go:141-179` |

> 低 4 位存 `log10(divisor)`；解码时用 `v * div` 乘回去。原先"缩放到纳秒"的说法方向相反：编码是**除以** 10 的幂把差值变小。

**案例**：`[0, 1e9, 2e9, 3e9]`
- `reduce()`：差值全为 `1e9`，`divisor=1e9`（1e12→…→1e9），`max=1e9`，`rle=true`
- `len>1` → **RLE**：头 `0x20 | log10(1e9)=9` → `0x29`；首时间戳 0（8B）；`delta/div=1`（1B）；个数 4（1B）→ 共 11B。

### 5.4 Boolean: 1B 头 + uvarint 计数 + MSB-first 位图

```
┌──────────────┬──────────────────┬─────────────────────────┐
│ Type<<4 (1B) │ Count (uvarint)  │ packed bits (⌈n/8⌉ 字节) │
│ 0x10         │ n = 值总数        │ MSB-first               │
└──────────────┴──────────────────┴─────────────────────────┘
```

- `Write` 逐位左移并置最低位（`e.b = e.b<<1; if b { e.b |= 1 }`），所以第一个值落在字节的 bit7（`bool.go:48-66`）。
- `flush()` 在 8 位未满时补 0 对齐；`Bytes()` 先 flush，再写 `booleanCompressedBitPacked<<4`（=0x10）与 `uvarint(n)`（`bool.go:67-99`）。
- 解码端靠 `uvarint(n)` 界定有效位数，位序为 `pos := 7 - (i & 0x7)`（`bool.go:149-160`）。

### 5.5 String: 1B 头 + 整块 Snappy

```
┌──────────────┬──────────────────────────────────────────────────┐
│ Type<<4 (1B) │ snappy( uvarint(len₁)‖s₁ ‖ uvarint(len₂)‖s₂ ‖ … ) │
│ 0x10         │                                                  │
└──────────────┴──────────────────────────────────────────────────┘
```

- `Write` 只做 `PutUvarint(len)` + 原始字节追加，**不排序、不差分**（`string.go:42-50`）。
- `Bytes()` 对**整个累积缓冲**做一次 `snappy.Encode`，前缀 1 字节 `stringCompressedSnappy<<4`（`string.go:53-58`）。
- 因此压缩单位是 block（同一 key 的一段值），不是单个字符串。

## 6. Cache Snapshot → TSM 写入

### 6.1 WriteSnapshot — 自适应并发

> **重要**: `Engine.WriteSnapshot()` (engine.go:1957-2014) 在执行 `Cache.Snapshot()` **之前**，
> 会先调用 `e.WAL.CloseSegment()` 关闭当前 WAL segment，然后调用 `e.WAL.ClosedSegments()`
> 获取已关闭的 segment 文件列表。这确保了 WAL 和 Cache 的一致性快照。
>
> ```go
> // engine.go:1920-1941 — WriteSnapshot 内部流程
> e.mu.Lock()
> defer e.mu.Unlock()
>
> // 1. 先关闭当前 WAL segment
> if err = e.WAL.CloseSegment(); err != nil { return }
>
> // 2. 获取已关闭的 segment 文件列表
> segments, err = e.WAL.ClosedSegments()
>
> // 3. 再执行 Cache Snapshot
> snapshot, err = e.Cache.Snapshot()
> ```
>
> 之后 `snapshot.Deduplicate()` 排序去重，再调用 `Compactor.WriteSnapshot()` 写入 TSM 文件。
> 写入成功后通过 `WAL.Remove(closedFiles)` 删除已处理的 WAL segment。

```go
// tsdb/engine/tsm1/compact.go:888-958 — WriteSnapshot
func (c *Compactor) WriteSnapshot(cache *Cache, logger *zap.Logger) ([]string, error) {
    // 1. 入口先检查 snapshots 是否被禁用
    c.mu.RLock()
    enabled := c.snapshotsEnabled
    intC := c.snapshotsInterrupt
    c.mu.RUnlock()

    if !enabled {
        return nil, errSnapshotsDisabled
    }

    start := time.Now()

    // 注意: 使用 cache.Count() 而非 cache.Cardinality()
    card := cache.Count()

    // 根据 series 基数决定是否限流
    throttle := card < 3e6 && c.snapshotLatencies.avg() < 15*time.Second

    // 根据 series 基数选择并发度
    concurrency := card / 2e6
    if concurrency < 1 {
        concurrency = 1
    }

    // 特殊情况: 高基数使用最大并发且不限流
    if card >= 3e6 {
        concurrency = 4
        throttle = false
    }

    // 按 partition 分片，并行写入
    splits := cache.Split(concurrency)
    type res struct {
        files []string
        err   error
    }
    resC := make(chan res, concurrency)
    for i := 0; i < concurrency; i++ {
        go func(sp *Cache) {
            iter := NewCacheKeyIterator(sp, tsdb.DefaultMaxPointsPerBlock, intC)
            files, err := c.writeNewFiles(c.FileStore.NextGeneration(), 0, nil, iter, throttle, logger)
            resC <- res{files: files, err: err}
        }(splits[i])
    }

    // join 所有 goroutine 的结果
    var errs []error
    files := make([]string, 0, concurrency)
    for i := 0; i < concurrency; i++ {
        result := <-resC
        if result.err != nil {
            errs = append(errs, result.err)
        }
        files = append(files, result.files...)
    }

    // 2. 写完后再次检查 enabled: 期间可能被 DisableSnapshots()
    c.mu.Lock()
    enabled = c.snapshotsEnabled
    c.lastSnapshotDuration = time.Since(start).Truncate(time.Second)
    c.snapshotLatencies.add(time.Since(start))
    c.mu.Unlock()

    if !enabled {
        return nil, errSnapshotsDisabled
    }

    return files, errors.Join(errs...)
}
```

### 6.2 CacheKeyIterator — 从 Cache 到 TSM Block

> **小白解释**: CacheKeyIterator 就像一个"翻译官"——把 Cache 中的内存数据翻译成 TSM 文件格式。
> 它按 key 排序（保证 TSM 文件内部有序），再把每个 key 的数据按 `DefaultMaxPointsPerBlock` 切成多个 block 并**提前编码**。

真实接口分两层，理解时必须分开：

```mermaid
flowchart TD
    subgraph "层 1: 编排 (Engine / Compactor)"
        E1["Engine.WriteSnapshot()<br/>e.mu.Lock"] --> E2["WAL.CloseSegment() + ClosedSegments()<br/>Cache.Snapshot()"]
        E2 --> E3["snapshot.Deduplicate()"]
        E3 --> E4["Compactor.WriteSnapshot(cache, logger)"]
        E4 --> E5["Cache.Split(concurrency)"]
        E5 --> E6["n × NewCacheKeyIterator(subCache, 1000, intC)"]
    end

    subgraph "层 2: 编码与落盘"
        K1["cache.Keys() → 已排序的 key 列表"] --> K2["go encode() × GOMAXPROCS"]
        K2 --> K3["cache.values(key)<br/>只 entry.mu.RLock，不取 Cache 锁"]
        K3 --> K4["按 size=1000 切块<br/>encode*BlockUsing"]
        K4 --> K5["blocks[i] (已编码字节)"]
        K5 --> K6["Next() / Read()"]
        K6 --> K7["writeNewFiles → Compactor.write"]
        K7 --> K8["TSMWriter.WriteBlock + WriteIndex"]
    end

    E6 --> K1
```

**接口要点**（与早期版本不同）：
- `Cache` **没有** `Read(key) []Value` 这样的导出方法；编码器用的是未导出的 `cache.values(key)`（`cache.go:661-670`），它只对 `entry.mu` 加读锁，不加 `Cache.mu`。
- `CacheKeyIterator.Read()` 返回的是 `(key, minTime, maxTime, block, err)`，其中 `block` 是**已经编码好的字节**（`compact.go:1891-1919`），不是 `[]Value`。
- 一个 key 可能对应**多个** block，`Next()` 会在同一个 key 上被返回多次。

### 6.3 CacheKeyIterator + writeNewFiles：从 Cache 到 TSM 的字节流

**阶段 1：生成快照并切分（`engine.go:1976-2014`、`compact.go:888-957`）**

1. `Engine.WriteSnapshot` 在 `e.mu.Lock()` 内完成 `WAL.CloseSegment()`、`WAL.ClosedSegments()` 与 `Cache.Snapshot()`（`engine.go:1976-1994`）。这三步必须原子，否则 WAL 与 Cache 的交界处会漏数据。
2. 释放 `e.mu` 之后才做 `snapshot.Deduplicate()`（`engine.go:2000-2009`），因为它要遍历整个快照，不能阻塞写入。
3. `Compactor.WriteSnapshot` 用 `cache.Count()` 得到 series 基数，算出 `concurrency`，再 `cache.Split(concurrency)`；每个子 Cache 一个 goroutine。

**阶段 2：按 key 编码（`compact.go:1780-1882`）**

- `NewCacheKeyIterator` 先 `cache.Keys()` 拿到**已排序**的 key 列表，随后启动 `GOMAXPROCS(0)` 个 `encode()` goroutine，用原子 `idx` 把 key 空间分片。
- 每个 key 调 `c.cache.values(key)` 取到 `Values` 的**别名**（不加 Cache 锁，只 `entry.mu.RLock`），再按 `c.size` 切块：

```go
// tsdb/engine/tsm1/compact.go:1841-1878（节选）
key := c.order[i]
values := c.cache.values(key)

for len(values) > 0 {
    end := len(values)
    if end > c.size {                  // c.size = tsdb.DefaultMaxPointsPerBlock = 1000
        end = c.size
    }
    minTime, maxTime := values[0].UnixNano(), values[end-1].UnixNano()

    switch values[0].(type) {          // 按类型选择 encode*BlockUsing
    case FloatValue:
        b, err = encodeFloatBlockUsing(nil, values[:end], tenc, fenc)
    // ... Integer / Unsigned / Boolean / String
    }

    values = values[end:]
    c.blocks[i] = append(c.blocks[i], cacheBlock{k: key, minTime: minTime, maxTime: maxTime, b: b, err: err})
}
```

- `blocks[i]` 是该 key 的 block 队列；`Next()` 逐块交付，`Read()` 返回队首。因此同一个 key 会被返回多次（每个 block 一次）。

**阶段 3：写文件（`compact.go:1143-1300`）**

| 常量 / 分支 | 值 | 源码 |
|-------------|----|------|
| `DefaultMaxPointsPerBlock` | 1000 | `tsdb/config.go:53` |
| 激进压缩每块点数 `DefaultAggressiveMaxPointsPerBlock` | 10000 | `tsdb/config.go:57` |
| 单个 TSM 文件上限 `MaxTSMFileSize` | 2GB | `tsdb/config.go:86` |
| 索引估算超过 64MB → 磁盘缓冲索引 | `64*1024*1024` | `compact.go:1224` |
| `throttle` 为真 → `limiter.NewWriterWithRate` | — | `compact.go:1218` |

- `w.Size() > tsdb.MaxTSMFileSize` 或 `WriteBlock` 返回 `ErrMaxBlocksExceeded` → `rollToNext=true`，`writeNewFiles` 递增 `sequence` 后新建 `.tsm.tmp`（`compact.go:1275-1291`、`1152-1163`）。
- 整个 key 都被 tombstone 覆盖时写出 0 个 block → `WriteIndex()` 返回 `ErrNoValues` → 空 `.tsm.tmp` 被 `os.RemoveAll`（`compact.go:1164-1172`）。

**案例（2500 个 FloatValue，t=1..2500）**：

| 块 | 值范围 | 点数 | minTime | maxTime | 编码后首字节 |
|----|--------|------|---------|---------|--------------|
| 0 | `values[0:1000]` | 1000 | 1 | 1000 | `0x00`（BlockFloat64） |
| 1 | `values[1000:2000]` | 1000 | 1001 | 2000 | `0x00` |
| 2 | `values[2000:2500]` | 500 | 2001 | 2500 | `0x00` |

同一个 key 被 `Next()` 返回 3 次，每次 `Read()` 给出一块。

> 注意：block 的首字节是 `packBlock` 写入的块类型（`BlockFloat64 = 0x00`，见 `encoding.go:15-28`）；Gorilla 的编码类型位于后续 value 负载的首字节（`0x10`），不要与 block 类型混淆。

> **依赖关系**：`cache.values(key)` 的注释明确写着 "assumes the data is already sorted"（`cache.go:661-670`）。这个前提由 `Engine.WriteSnapshot` 先调用 `snapshot.Deduplicate()` 保证（`engine.go:2000-2009`）；如果绕过它直接调用 `CacheKeyIterator`，`minTime > maxTime` 的校验会失败（`compact.go:1270-1272`）。

### 6.4 Snapshot 的失败与重试

Snapshot 有四条非成功路径，任何一条都不会静默丢数据，但会改变后续重试行为：

```mermaid
sequenceDiagram
    participant E as Engine.WriteSnapshot
    participant C as Cache
    participant Comp as Compactor
    participant FS as FileStore
    participant WAL as WAL

    E->>C: Snapshot()
    alt snapshot.Size() == 0
        E->>C: ClearSnapshot(true)（engine.go:2000-2003）
        Note over E: 空快照直接返回，不写 TSM
    else 有数据
        E->>Comp: WriteSnapshot(snapshot, logger)
        alt 写 TSM 失败
            Comp-->>E: err
            E->>C: defer ClearSnapshot(false)（engine.go:2046-2051）
            Note over C: 只把 snapshotting 置 false；snapshotSize 保留
        else 写 TSM 成功
            E->>FS: FileStore.Replace(nil, newFiles)（engine.go:2064）
            alt Replace 失败
                FS-->>E: err
                E->>E: 逐个 os.Remove(newFiles)（engine.go:2067-2074）
                E->>C: ClearSnapshot(false)
                Note over E: 返回错误；下轮 WriteSnapshot 重试
            else Replace 成功
                E->>C: ClearSnapshot(true)（engine.go:2077）
                E->>WAL: WAL.Remove(closedFiles)（engine.go:2080-2084）
            end
        end
    end
```

- **空 snapshot 短路**：`snapshot.Size() == 0` 时只 `ClearSnapshot(true)` 就返回（`engine.go:2000-2003`）。
- **写 TSM 失败**：`writeSnapshotAndCommit` 的 `defer` 在 `err != nil` 时调用 `ClearSnapshot(false)`（`engine.go:2046-2051`）。
- **Replace 失败**：把刚写出的文件逐个 `os.Remove`，再走同一个 `defer`（`engine.go:2064-2081`）。下一轮 `WriteSnapshot` 会重新写。
- **重试语义**：`ClearSnapshot(false)` 只把 `snapshotting` 置 false，**不清** `snapshotSize`（`cache.go:449-468`）；下一轮 `Snapshot()` 读到 `c.snapshot.Size() > 0` 会直接返回旧 snapshot（`cache.go:400-404`），于是同一批数据被重新写一次。

## 7. TSM 文件读取 — 从磁盘到查询结果

### 7.1 mmap 初始化 — 零拷贝映射

TSM 文件通过 mmap 映射到进程虚拟地址空间，读取操作变为直接内存访问。

```mermaid
sequenceDiagram
    participant Query as 查询引擎
    participant FS as FileStore
    participant Reader as TSMReader
    participant mmap as mmapAccessor
    participant OS as 操作系统

    Query->>FS: Read(key, timestamp)
    FS->>Reader: TSMReader.read(key, timestamp)
    Reader->>mmap: read(key, timestamp)

    Note over mmap: 步骤 1: 在 Index 中查找 key
    mmap->>mmap: index.Entry(key, timestamp)
    Note over mmap: 二分查找 offsets 数组<br>定位 key 的 IndexEntry

    Note over mmap: 步骤 2: 从 mmap 切片读取 block
    mmap->>mmap: b[entry.Offset+4 : entry.Offset+entry.Size]
    Note over mmap: 跳过 4 字节 CRC<br>返回原始编码数据 (零拷贝)

    Note over mmap: 步骤 3: 解码 block
    mmap->>mmap: DecodeBlock(rawBytes)
    Note over mmap: 根据类型分发:<br>Float → Gorilla XOR 解码<br>Integer → Simple8b 解码<br>...

    mmap-->>Query: []Value
```

#### mmapAccessor.init() — 完整初始化

```go
// tsdb/engine/tsm1/reader.go:1486 — mmapAccessor.init
func (m *mmapAccessor) init() (*indirectIndex, error) {
    m.mu.Lock()
    defer m.mu.Unlock()

    // 1. 验证文件头: Magic=0x16D116D1, Version=1
    if _, err := m.f.Seek(0, 0); err != nil {
        return nil, fmt.Errorf("init: failed to seek: %v", err)
    }
    if err := verifyVersion(m.f); err != nil {
        return nil, err
    }
    if _, err := m.f.Seek(0, 0); err != nil {
        return nil, err
    }

    // 2. 获取文件大小
    stat, err := m.f.Stat()
    if err != nil {
        return nil, err
    }

    // 3. mmap 整个文件
    m.b, err = mmap(m.f, 0, int(stat.Size()))
    if err != nil {
        return nil, NewMmapError(err)
    }
    if len(m.b) < 8 {
        return nil, fmt.Errorf("mmapAccessor: byte slice too small for indirectIndex")
    }
    // m.b 现在是整个 TSM 文件的内存视图

    // 4. 可选: MADV_WILLNEED 提示内核预读
    if m.mmapWillNeed {
        if err := madviseWillNeed(m.b); err != nil {
            return nil, err
        }
    }

    // 5. 读取 Footer (最后 8 字节) → Index 起始偏移
    indexOfsPos := len(m.b) - 8
    indexStart := binary.BigEndian.Uint64(m.b[indexOfsPos:indexOfsPos+8])
    if indexStart >= uint64(indexOfsPos) {
        return nil, fmt.Errorf("mmapAccessor: invalid indexStart")
    }

    // 6. 解析 Index Section
    m.index = NewIndirectIndex()
    if err := m.index.UnmarshalBinary(m.b[indexStart:indexOfsPos]); err != nil {
        return nil, err
    }

    // 7. 标记访问计数，允许后续 free 逻辑立即释放资源
    m.incAccess()
    atomic.StoreUint64(&m.freeCount, 1)

    return m.index, nil
}
```

### 7.2 indirectIndex — Index 解析与二分查找

Index Section 包含所有 key 和 block 的元数据。解析时扫描字节流，记录每个 key 的偏移位置到 `offsets` 数组。

```go
// tsdb/engine/tsm1/reader.go:1325 — indirectIndex.UnmarshalBinary
func (d *indirectIndex) UnmarshalBinary(b []byte) error {
    d.b = b  // 保存 Index 字节引用

    var minTime, maxTime int64 = math.MaxInt64, 0

    var offsets []int32
    var i int32
    for i < int32(len(b)) {
        offsets = append(offsets, i)  // 记录 key 偏移

        // 跳过 key: keyLen(2B) + type(1B) + key(NB)
        i += 3 + int32(binary.BigEndian.Uint16(b[i:i+2]))

        // 读取 block 条目数
        count := int32(binary.BigEndian.Uint16(b[i:i+2]))
        i += 2

        // 跟踪全局 minTime/maxTime (边界验证)
        minT := int64(binary.BigEndian.Uint64(b[i : i+8]))
        if minT < minTime { minTime = minT }

        i += (count - 1) * indexEntrySize

        maxT := int64(binary.BigEndian.Uint64(b[i+8 : i+16]))
        if maxT > maxTime { maxTime = maxT }

        i += indexEntrySize
    }

    // 记录 minKey/maxKey (首尾 key)
    _, d.minKey = readKey(b[offsets[0]:])
    _, d.maxKey = readKey(b[offsets[len(offsets)-1]:])
    d.minTime = minTime
    d.maxTime = maxTime

    // 将 offsets 写入 mmap 匿名内存 (用于二分查找)
    d.offsets, _ = mmap(nil, 0, len(offsets)*4)
    for i, v := range offsets {
        binary.BigEndian.PutUint32(d.offsets[i*4:i*4+4], uint32(v))
    }
}
```

**二分查找定位 key**:

```go
func (d *indirectIndex) Seek(key []byte) int {
    return sort.Search(len(d.offsets)/4, func(i int) bool {
        offset := binary.BigEndian.Uint32(d.offsets[i*4 : i*4+4])
        _, k := readKey(d.b[offset:])
        return bytes.Compare(k, key) >= 0
    })
}
```

### 7.3 Block 解码 — 从原始字节到 Value

```go
// tsdb/engine/tsm1/reader.go:1630-1646 — mmapAccessor.readBlock
func (m *mmapAccessor) readBlock(entry *IndexEntry, values []Value) ([]Value, error) {
    m.incAccess()

    m.mu.RLock()
    defer m.mu.RUnlock()

    // mmap 已被 Close/Unmap 时长度会不足，必须返回 ErrTSMClosed
    if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
        return nil, ErrTSMClosed
    }
    //TODO: Validate checksum
    var err error
    values, err = DecodeBlock(m.b[entry.Offset+4:entry.Offset+int64(entry.Size)], values)
    if err != nil {
        return nil, err
    }

    return values, nil
}
```

**DecodeBlock 分发**（`tsdb/engine/tsm1/encoding.go:251-319`，是包级函数，不是 `mmapAccessor` 的方法）：

```go
func DecodeBlock(block []byte, vals []Value) ([]Value, error) {
    if len(block) <= encodedBlockHeaderSize {
        return nil, fmt.Errorf("decode of short block: got %v, exp %v", len(block), encodedBlockHeaderSize)
    }

    blockType, err := BlockType(block)
    if err != nil {
        return nil, fmt.Errorf("error decoding block type: %v", err)
    }

    switch blockType {
    case BlockFloat64:
        var buf []FloatValue
        decoded, err := DecodeFloatBlock(block, &buf)
        if len(vals) < len(decoded) {
            vals = make([]Value, len(decoded))
        }
        for i := range decoded {
            vals[i] = decoded[i]
        }
        return vals[:len(decoded)], err

    // BlockInteger / BlockUnsigned / BlockBoolean / BlockString 的代码结构完全相同：
    // 分别调用 DecodeIntegerBlock / DecodeUnsignedBlock / DecodeBooleanBlock /
    // DecodeStringBlock，逐值拷贝进 []Value，并以 return vals[:len(decoded)], err 返回。

    default:
        return nil, fmt.Errorf("unknown block type: %d", blockType)
    }
}
```

> 注意：每个分支都把解码错误原样返回（`return vals[:len(decoded)], err`），不会把错误吞掉后返回半截结果。

### 7.4 KeyCursor — 多文件定位

查询时同一 key 可能存在于多个 TSM 文件中（Cache + 多个 compaction 级别）。`KeyCursor` 负责定位所有包含该 key 的文件和 block 位置。

> **注意**: `KeyCursor` 本身**不做**归并和去重。归并和去重发生在更上层的 cursor/iterator 层
> (如 `intAscendingCursor`、`floatAscendingCursor` 等)，它们从多个来源读取数据并合并排序。
> Cache 数据也在 iterator 层与 TSM 数据归并，Cache 的值优先级更高（相同时间戳保留 Cache 的值）。

```mermaid
sequenceDiagram
    participant Query as 查询引擎
    participant KC as KeyCursor
    participant FS as FileStore
    participant Cache as Cache
    participant TSM1 as TSM 文件 1
    participant TSM2 as TSM 文件 2

    Query->>KC: Next() → 获取下一个值
    KC->>FS: locations(key, timestamp)
    FS->>FS: 遍历所有文件, 收集匹配的 IndexEntry
    Note over FS: 过滤 tombstone<br>过滤时间范围<br>返回 []location{file, entry}

    KC->>KC: 按时间排序所有 location

    loop 每个 location
        KC->>TSM1: ReadAt(entry)
        TSM1-->>KC: []Value (已解码的原始 block 数据)
    end

    KC-->>Query: 所有 location 的原始 Value 序列
    Note over Query: 归并和去重在上层 cursor/iterator 层完成
```

#### FileStore.locations — 查找所有匹配位置

```go
// tsdb/engine/tsm1/file_store.go:1136 — locations
// 注意: ascending 参数决定扫描方向，影响 readMin/readMax 的设置
func (f *FileStore) locations(key []byte, t int64, ascending bool) []*location {
    var cache []IndexEntry
    locations := make([]*location, 0, len(f.files))

    for _, fd := range f.files {
        minTime, maxTime := fd.TimeRange()

        // 1. 根据扫描方向跳过不相关文件
        if ascending && maxTime < t {
            continue  // 升序: 文件最大时间 < 查找时间，跳过
        } else if !ascending && minTime > t {
            continue  // 降序: 文件最小时间 > 查找时间，跳过
        }

        // 2. 获取 tombstone 范围 (而非 HasTombstone)
        tombstones := fd.TombstoneRange(key)

        // 3. 使用 ReadEntries 获取此 key 的所有 IndexEntry (而非 Contains + Entries)
        entries := fd.ReadEntries(key, &cache)
    LOOP:
        for i := 0; i < len(entries); i++ {
            ie := entries[i]

            // 4. 检查 tombstone: 跳过完全被 tombstone 覆盖的 block
            for _, t := range tombstones {
                if t.Min <= ie.MinTime && t.Max >= ie.MaxTime {
                    continue LOOP
                }
            }

            // 5. 根据扫描方向检查时间范围
            if ascending && ie.MaxTime < t {
                continue
            } else if !ascending && ie.MinTime > t {
                continue
            }

            location := &location{r: fd, entry: ie}

            // 6. 设置 readMin/readMax 用于查询时过滤
            if ascending {
                location.readMin = math.MinInt64
                location.readMax = t - 1   // 标记查找时间之前的数据为已读
            } else {
                location.readMin = t + 1   // 标记查找时间之后的数据为已读
                location.readMax = math.MaxInt64
            }

            locations = append(locations, location)
        }
    }
    return locations
}
```

### 7.5 Cache + TSM 归并 — 热数据优先

查询时 Cache 中的数据（未 snapshot 的最新写入）与 TSM 文件中的数据归并。Cache 数据优先级更高。

```go
// tsdb/engine/tsm1/array_cursor_iterator.gen.go:18-44 — buildFloatArrayCursor
func (q *arrayCursorIterator) buildFloatArrayCursor(ctx context.Context, name []byte, tags models.Tags, field string, opt query.IteratorOptions) (tsdb.FloatArrayCursor, error) {
    var err error
    key := q.seriesFieldKeyBytes(name, tags, field)

    // 1. Cache.Values 同时读取 snapshot cache 和 live cache。
    //    snapshot 先入结果，live cache 后入结果，最后 Deduplicate。
    cacheValues := q.e.Cache.Values(key)

    // 2. KeyCursor 从 TSM 文件读取冷数据。
    keyCursor := q.e.KeyCursor(ctx, key, opt.SeekTime(), opt.Ascending)

    // 3. asc / desc 两条分支各自懒创建 cursor，并检查 reset 返回的 error。
    if opt.Ascending {
        if q.asc.Float == nil {
            q.asc.Float = newFloatArrayAscendingCursor()
        }
        err = q.asc.Float.reset(opt.SeekTime(), opt.StopTime(), cacheValues, keyCursor)
        if err != nil {
            return nil, err
        }
        return q.asc.Float, nil
    }

    if q.desc.Float == nil {
        q.desc.Float = newFloatArrayDescendingCursor()
    }
    err = q.desc.Float.reset(opt.SeekTime(), opt.StopTime(), cacheValues, keyCursor)
    if err != nil {
        return nil, err
    }
    return q.desc.Float, nil
}
```

这里要区分两条读路径：InfluxQL 的 `Engine.CreateIterator` 返回的是 `query.Iterator`（point cursor/iterator 模型），它会在 `createCallIterator` 或 `createVarRefIterator` 中按 series 构建迭代器；`ArrayCursor` 是更底层的 storage reads / TSM1 array cursor 路径，用来批量读 Cache + TSM 的 typed array。不要把 InfluxQL 的 `CreateIterator` 直接等同为 `ArrayCursor`。

## 8. 架构设计意图

### 8.1 为什么用 16 分区哈希环

```mermaid
flowchart LR
    A["写入 key='cpu,host=web'"] --> B["xxhash(key) = 12345"]
    B --> C["12345 % 16 = 5"]
    C --> D["分区 5"]
```

- **并发**: 16 个分区可以并发写入
- **锁粒度**: 每个分区独立的 RWMutex
- **均匀分布**: xxhash 保证均匀分布

### 8.2 为什么 Cache 没有驱逐

- **数据完整性**: 驱逐可能导致最新数据丢失
- **Snapshot 语义**: 整个 Cache 一次性快照写入 TSM
- **简单可靠**: 无驱逐策略，逻辑简单

### 8.3 为什么当前实现先写 Cache 再写 WAL

顺序的直接后果与合理性：

- **可见性优先**：Cache 是查询可见性的来源（查询同时读 Cache 与 TSM），WAL 是崩溃恢复的来源。先写 Cache 让数据立刻可查，再同步写 WAL 保证重启可恢复（`engine.go:1443-1452`）。
- **WAL 侧是同步成本**：`WAL.writeToLog` 在调用者 goroutine 内同步完成 `entry.Encode` 与 `snappy.Encode`（`wal.go:398-420`），一次 `WriteMulti` 只产生 1 个 entry（`wal.go:311-324`）；没有"异步压缩"，也没有"多个 entry 一起压缩"。
- **只有 fsync 是批量的**：写入者把 `syncErr` 放入 `syncWaiters`（容量 1024），CAS 选出的负责人执行一次 `sync()`（`bw.Flush + f.Sync`）并通知所有等待者（`wal.go:439-461`）。
- **代价**：WAL 失败时 Cache 已写入，写放大与延迟全部落在 WAL 上；这正是 `Engine.WritePoints` 返回错误但数据仍可见的原因。

### 8.4 跨层锁、所有权与文件生命周期

**操作 × 锁**（R = 读锁，L = 写锁，— = 不涉及）：

| 操作 | `Engine.e.mu` | `Cache.mu` | `partition.mu` | `entry.mu` | `FileStore.mu` | `file.refs` | `WAL.mu` | `Tombstoner.mu` |
|------|---------------|------------|----------------|------------|----------------|-------------|----------|-----------------|
| WritePoints | R（`engine.go:1440-1452`） | R（只读 store 指针，`cache.go:335-337`） | R/L（`ring.go:227-234`） | L/R（`cache.go:87-97`） | — | — | L（`wal.go:398-420`） | — |
| WriteSnapshot 阶段 1 | L（`engine.go:1976-1994`） | L（`Snapshot`）+ R（`ClearSnapshot`） | — | — | — | — | L（`CloseSegment`） | — |
| Dedup + Compactor | — | — | — | L（`entry.deduplicate`） | R（`NextGeneration`） | — | — | — |
| FileStore.Replace | R（`engine.go:2063-2064`） | — | — | — | L（`file_store.go`） | — | — | — |
| DELETE | **不取** | L（`DeleteRange`，`cache.go:619-621`） | R/L | L（`filter`） | R（`Apply`） | — | L（`DeleteRange`） | L（`AddRange`） |
| Query KeyCursor | — | R（`Values`） | R | R | R（`locations`） | Ref/Unref（`file_store.go:1443-1477`） | — | R |
| Compaction | — | — | — | — | L（`Replace`） | Ref/Unref | — | L |

**三个并发场景的结论**：

- **写 vs snapshot**：snapshot 只在换指针与关 WAL segment 时短暂持有 `e.mu` 写锁；`Deduplicate` 与 `Compactor.WriteSnapshot` 都在锁外（`engine.go:2000-2014`），不阻塞写入。但 `Deduplicate` 要遍历全部快照数据，期间写入继续进入新的 live ring。
- **写 vs DELETE**：DELETE 不取 `e.mu`，停的是 compaction 而不是 `WritePoints`；但 `Cache.DeleteRange` 持有 Cache 全局写锁（`cache.go:619-621`），期间所有写入的 `c.mu.RLock` 都被阻塞。
- **查询 vs Replace**：`KeyCursor` 对每个命中文件 `Ref()`，`Close()` 时 `Unref()`（`file_store.go:1443-1477`）；被 `Ref` 的文件不会被 `Replace` 删除，查询结束 `Unref` 后才可回收。

**snapshot / split 的所有权**：`Cache.Split` 返回的子 Cache 只带 `store` 指针，不继承 `size/maxSize/snapshot`（`cache.go:506-520`）。所有子 goroutine 必须在 `ClearSnapshot(true)` 之前 join（`compact.go:924-950`），否则 `snapStore.reset()` 会与仍在读子 ring 的编码 goroutine 竞争。

```mermaid
flowchart LR
    WP["WritePoints"] -->|"RLock，一次请求"| EMU["Engine.e.mu"]
    WS["WriteSnapshot 阶段 1"] -->|"Lock，一次快照"| EMU
    DEL["DELETE"] -->|"不取 e.mu，改为停 compaction"| SFL["sfile.DisableCompactions + Wait"]
    QRY["Query KeyCursor"] -->|"Ref / Unref"| REF["file.refs"]
    EMU -->|"RLock"| CMU["Cache.mu"]
    CMU -->|"RLock / Lock"| PMU["partition.mu"]
    PMU -->|"Lock / RLock"| ENMU["entry.mu"]
```

## 9. 架构收益

| 维度 | 收益 |
|------|------|
| **写入吞吐** | 16 分区并发 + 乐观锁 + fsync 批量 |
| **内存效率** | Snapshot O(1) 交换，无 GC 停顿 |
| **压缩比** | Gorilla XOR (Float) + Simple8b (Integer) |
| **数据安全** | 当前 `Engine.WritePoints` 先 `Cache.WriteMulti(values)`，再 `WAL.WriteMulti(values)` 并等待 sync；WAL 用于重启恢复，但不是先于 Cache 写入，WAL 失败会返回错误且 Cache 已写入 |
| **查询性能** | TSM 文件排序 + Index 二分查找 |

## 10. 潜在隐患与瓶颈

### 10.1 Cache 满时直接拒绝写入

```go
if limit > 0 && n > limit {
    return ErrCacheMemorySizeLimitExceeded(n, limit)
}
```

没有驱逐机制，Cache 满时写入直接失败。判定点是 `Cache.WriteMulti` 的 `if limit > 0 && n > limit`（`cache.go:328-332`），其中 `n = Size() + addedSize`；`limit` 来自 `cache-max-memory-size`，默认 **1GB**（`tsdb/config.go:25`，`engine.go:248` 传入 `NewCache`）。

### 10.2 Snapshot 期间内存占用不会额外翻倍

`Cache.Size()` = `size + snapshotSize`（`cache.go:471-474`）。默认 `maxSize` 是 **1GB**、快照阈值是 **25MB**（`tsdb/config.go:25,29`），所以 snapshot 期间旧数据继续占用 `Size()` 预算，而不是"额外再送一份 25MB"。

### 10.3 WAL fsync 批量延迟

CAS 批量机制导致单个写入可能等待整个 flush 周期。`syncWaiters` 容量为 **1024**（`wal.go:128`）；`syncDelay` 默认 **0**（`wal.go:101-105`），即每次写入都立即 fsync，只有显式配置非零值才会合并 fsync。

### 10.4 分区写锁竞争

高基数场景下，同一分区的写操作串行化：热路径只持 `p.mu.RLock`，但 `entry.add` 还要竞争 `entry.mu`（`cache.go:87-97`）；`partition.write` 的完整临界区见 `ring.go:227-252`。

### 10.5 Gorilla 编码的 CPU 开销

XOR + leading/trailing zeros 计算在每次 Write 时执行：后续值与前一值 XOR 后先写 1 bit 标志；需要新窗口时再写 5-bit leading + 6-bit sigbits + payload（`float.go:101-135`）。最坏情况一个值约 `1+5+6+64 = 76` bit，值完全相同时只需 1 bit。

### 10.6 Simple8b 的打包效率

当差值分布不均匀时，Simple8b 可能无法有效打包：每个 64-bit word 装 **1–240** 个值，随位宽自适应（`int.go:158` 的注释）；只要有一个差值超过 `simple8b.MaxValue`（`1<<60 - 1`）就整体退回 8B/值的未压缩格式（`int.go:85-90`）。

## 11. TSM-to-TSM 压缩流程

```mermaid
flowchart TD
    subgraph "Compaction 级别"
        CS["Cache Snapshot: WriteSnapshot()<br>(从 Cache 写入的新 generation TSM 文件)"]
        L1["Level 1: PlanLevel(1)<br>(多个小 TSM 文件合并)"]
        L2["Level 2: PlanLevel(2)<br>(多个 L1 合并)"]
        L3["Level 3: PlanLevel(3)<br>(多个 L2 合并)"]
        L4["Level 4: Plan()<br>(Full Compaction，合并所有级别)"]
        L5["Level 5: PlanOptimize()<br>(Optimize，冷 shard 优化重写)"]
    end

    CS -->|"Compactor.Compact()"| L1
    L1 -->|"Compactor.Compact()"| L2
    L2 -->|"Compactor.Compact()"| L3
    L3 -->|"Compactor.Compact()"| L4
    L4 -->|"冷 shard / holdoff 后"| L5

    subgraph "每一步的处理"
        A["选择同级别的多个 TSM 文件"] --> B["按 key 排序归并"]
        B --> C["去重 (相同时间戳保留最新)"]
        C --> D["编码为新的 TSM block"]
        D --> E["写入新的 TSM 文件"]
        E --> F["删除旧的 TSM 文件"]
    end
```

> **注意**: Cache Snapshot（`WriteSnapshot()`）不是 compaction 级别，而是由 `ShouldCompactCache()` 触发的
> 独立流程，将 Cache 数据写入新的 generation。`Compactor.writeNewFiles` 会先递增 sequence，
> 因此首个输出通常是 `000000001-000000001.tsm` 这类文件；后续 compaction level 由 sequence 和 planner 解释。
> 当前实现没有可调度的 Level 0：sequence=1 的 cache snapshot 文件被 `tsmGeneration.level()`
> 归为 Level 1。

**Compaction 调度逻辑** (`engine.go:2111-2153`):
- Level 1 (`PlanLevel(1)`): 合并多个小 TSM 文件，高优先级 (`compactHiPriorityLevel`)
- Level 2 (`PlanLevel(2)`): 合并多个 L1 文件，高优先级 (`compactHiPriorityLevel`)
- Level 3 (`PlanLevel(3)`): 合并多个 L2 文件，低优先级 (`compactLoPriorityLevel`)
- Level 4 (`Plan(e.LastModified())`): Full compaction，合并所有级别文件为一个最终文件
- Level 5 (`PlanOptimize(e.LastModified())`): Optimize compaction，经过 holdoff 后由 `compactOptimize` 执行，权重最低

**触发所需的 generation 数**（`compact.go:352-381`）：

| 层级 | Planner | 触发所需 generation 数 | 说明 |
|------|---------|------------------------|------|
| Level 1 | `PlanLevel(1)` | **8** | 小 TSM 文件攒够 8 个才合并 |
| Level 2 | `PlanLevel(2)` | 4 | 由 L1 输出攒够 4 个 |
| Level 3 | `PlanLevel(3)` | 4 | 由 L2 输出攒够 4 个 |
| Level 4 | `Plan(...)` | 4 | Full compaction，另受写冷时长（默认 4h）约束 |
| Level 5 | `PlanOptimize(...)` | 4 | Optimize，另有 1h/5min holdoff（`engine.go:2171-2175`） |

`PlanLevel` 先按 `group.chunk(minGenerations)` 切组，再判断 `len(chunk) < minGenerations && !hasTombstones`。这种"不够数"的组不会立即丢弃：只要它**后面还存在 `groups[j].level() >= level` 的更高层 generation**，这一组仍会被加入待办、往上升一层（`compact.go:370-379`）。带 tombstone 的组不受 `minGenerations` 限制，直接参与合并。

## 12. Scheduler 组件

```go
// tsdb/engine/tsm1/scheduler.go:9
var defaultWeights = [TotalCompactionLevels]float64{0.4, 0.3, 0.2, 0.1, 0.01}

type Scheduler struct {
    maxConcurrency int
    stats          *EngineStatistics

    // queues is the depth of work pending for each compaction level
    queues  [TotalCompactionLevels]int  // TotalCompactionLevels = 5
    weights [TotalCompactionLevels]float64
}
```

Scheduler 负责决定何时执行哪种 compaction:
- 根据当前活跃的 compaction 数量和级别选择下一个任务
- 通过比较各级别运行中的 compaction 总数与 `maxConcurrency` 来控制并发数（使用原子操作从 `stats` 读取）
- 使用加权评分算法（`queues[i] * weights[i]`）选择下一个任务，权重为 `[0.4, 0.3, 0.2, 0.1, 0.01]`（低级别优先，Optimize 最低）
- 优先执行低级别的 compaction（L1→L2），保持写入路径畅通

**并发限额 `limits()`**（`scheduler.go:73-91`）：

- `hiLimit = maxConcurrency * 4 / 5`、`loLimit = maxConcurrency / 5 + 1`（两者至少为 1）。
- 当 `level3Running + level4Running + level5Running >= loLimit` 且 `maxConcurrency - (level1Running + level2Running) == 0` 时，把候选范围截断为 `end = 2`，本轮只在 L1/L2 里挑（`scheduler.go:52-56`）。目的是把剩余的并发名额留给写入路径依赖的低级别 compaction，避免 L3+ 把并发占满。
- 当前 `nextByQueueDepths` 只消费 `loLimit`；`hiLimit` 由 `limits()` 一并返回但未被该函数使用，属于为后续策略预留的接口。

## 13. Tombstone 处理

### 13.1 DELETE 的外层编排

`DELETE`/`DELETE RANGE` 在 TSM1 Engine 里的入口是 `DeleteSeriesRange`（`engine.go:1461`），它把 `min/max` 包装成 predicate 后调用 `DeleteSeriesRangeWithPredicate`（`engine.go:1469`）。真正的外层编排都在后者里，顺序不能调换：

```mermaid
sequenceDiagram
    participant Client as DELETE 请求
    participant Engine as TSM1 Engine
    participant TSI as TSI Index
    participant FS as FileStore
    participant Cache as Cache
    participant WAL as WAL

    Client->>Engine: DeleteSeriesRange(itr, min, max)
    Engine->>Engine: DeleteSeriesRangeWithPredicate(itr, predicate)
    Engine->>TSI: ① DisableCompactions() + Wait()（engine.go:1474-1477）
    Engine->>TSI: ② RetainFileSet()（engine.go:1479-1483）
    Note over Engine: 第一个待删 series 到来时只执行一次
    Engine->>Engine: ③ disableLevelCompactions(true)（engine.go:1539-1540）
    Engine->>FS: ④ sfile.DisableCompactions() + sfile.Wait()（engine.go:1542-1544）
    loop 每累计 50MB 或 min/max 变化
        Engine->>Engine: ⑤ deleteSeriesRange(batch, min, max)（engine.go:1549-1556）
        Engine->>FS: ⑥ FileStore.Apply → tombstone → Cache → WAL（§13.2）
    end
    Engine->>Engine: ⑦ index.Rebuild()（engine.go:1575）
    Engine->>TSI: ⑧ EnableCompactions() + fileSet.Release()（defer）
```

1. **停压缩是 DELETE 正确性的前置条件**：`DeleteSeriesRangeWithPredicate` 先对 TSI 索引 `DisableCompactions()` + `Wait()` + `RetainFileSet()`（`engine.go:1474-1484`）。第一个待删 series 到来时，再 `e.disableLevelCompactions(true)`、`e.sfile.DisableCompactions()`、`e.sfile.Wait()`，并用 `defer` 恢复（`engine.go:1533-1547`）。源码注释说明：不停止正在运行的 compaction，刚写入现有 TSM 文件的 tombstone 会被 compaction 抹掉，删除的 measurement/series 在 compaction 完成后会重新出现；这里**只停 level compaction、不停 snapshot**，否则写 tombstone 耗时过长会把 Cache 写满并导致写入被拒。
2. **50MB 分批**：`sz` 累计 `len(key)`；当 `sz >= deleteFlushThreshold`（= `50*1024*1024`，`engine.go:97-98`）或 batch 的 min/max 发生变化时，先调用 `deleteSeriesRange(batch, min, max)` 并清空 batch（`engine.go:1549-1557`）。循环结束后再删一次尾批（`engine.go:1568-1573`）。
3. **删除后索引对账**：所有批次结束后 `e.index.Rebuild()`（`engine.go:1575`）。
4. **空操作短路**：`deleteSeriesRange` 先 `FileStore.Apply` 检查 `r.OverlapsTimeRange(min,max)`；若没有任何 TSM 文件重叠且 `e.Cache.store.count() == 0`，直接 `return nil`——不写 tombstone、不写 WAL（`engine.go:1595-1612`）。注意 Cache 侧只看"有没有值"，**不判断时间范围**，所以 cache 非空时一定走慢路径。
5. **Cache.DeleteRange 逐步代码**：`c.init()` → `c.mu.Lock()` → 对每个 key `c.store.entry(k)`；全时间范围 `[MinInt64, MaxInt64]` 直接 `decreaseSize(origSize+len(k))` + `store.remove(k)`；部分范围先 `e.filter(min,max)`，`e.count()==0` 才 remove，否则 `decreaseSize(origSize-e.size())`（`cache.go:619-649`）。

**案例（可手工推演）**：

- 100 万个 series，composite key 平均 50B：`sz` 累计到 `50MB / 50B = 1,048,576` 个 key 附近触发一次 flush；不足 50MB 的尾批在循环结束后再删一次。
- 删除范围 `[2020-01-01, 2020-12-31]`，所有 TSM 文件时间范围都是 2024 年且 Cache 为空：`overlapsTimeRangeMinMax=false`，`deleteSeriesRange` 直接返回，`.tombstone` 不产生、WAL 不追加。

### 13.2 单文件 tombstone 写入

Tombstone 用于标记已删除的数据。一次 `DELETE`/`DELETE RANGE` 的副作用顺序与写入路径**相反**：先让 TSM 上的删除持久化，再清 Cache，最后才写 WAL。单个文件的真实顺序（`tsdb/engine/tsm1/engine.go:1634-1685`）是：

1. **TSM tombstone 先落盘**：`FileStore.Apply` 对每个重叠文件调用 `r.BatchDelete()`（`engine.go:1634`）→ `batch.DeleteRange(...)`（`reader.go:718-739`，内部即 `Tombstoner.AddRange`，`reader.go:734`）→ `batch.Commit()`（`reader.go:741-746`：`Flush()` + `applyTombstones()`）。任一文件失败则 `batch.Rollback()`（`engine.go:1650`）。
2. **再挑出 Cache 中的 key**：`e.Cache.ApplyEntryFn(...)` 按已排序的 seriesKeys 匹配（`engine.go:1665`）。
3. **清 Cache**：`e.Cache.DeleteRange(deleteKeys, min, max)`（`engine.go:1681`，Cache 全局写锁）。
4. **最后写 WAL**：`e.WAL.DeleteRange(deleteKeys, min, max)`（`engine.go:1685`，仅 `WALEnabled` 时）。

顺序动机：tombstone 是 TSM 上删除的唯一持久记录；先提交它，即使随后 Cache/WAL 失败，重启后磁盘上的删除仍然生效。WAL 放在最后用于重放加速与记录，**不是**"先写日志"。

```go
// tsdb/engine/tsm1/tombstone.go
type Tombstone struct {
    Key    []byte
    Min    int64  // 最小时间戳
    Max    int64  // 最大时间戳
}
```

**Tombstone 文件格式**: 每个 TSM 文件对应一个 `.tombstone` 文件。
读取数据时，`FileStore.locations()` 会检查 tombstone 范围，
跳过完全被 tombstone 覆盖的 block（`file_store.go:1160-1165`）。

> **注意**: Tombstone 不会立即物理删除数据。只有在 compaction 时，
> 被 tombstone 标记的数据才会被真正移除。

**Tombstone 处理全链路**:

```mermaid
sequenceDiagram
    participant Client as DELETE 请求
    participant Engine as TSM1 Engine
    participant FS as FileStore / TSMFile
    participant Tomb as Tombstoner
    participant Cache as Cache
    participant WAL as WAL
    participant Disk as .tombstone 文件

    Client->>Engine: DELETE FROM cpu WHERE time >= '10:00' AND time < '11:00'
    Engine->>Engine: DeleteSeriesRange → deleteSeriesRange(keys, min, max)
    Engine->>FS: FileStore.Apply(逐个重叠文件)
    FS->>Tomb: ① BatchDelete().DeleteRange() → Tombstoner.AddRange()
    Tomb->>Disk: ② prepareV4 / writeTombstoneV3：复制现有 tombstone 到 tmp
    FS->>Tomb: ③ batch.Commit()：Flush() + applyTombstones()
    Tomb->>Disk: ④ gz.Close → bw.Flush → tmp.Sync → rename(tmp, .tombstone) → SyncDir
    Note over FS,Tomb: 任一文件失败：batch.Rollback() 并返回错误（engine.go:1650）
    Engine->>Cache: ⑤ ApplyEntryFn 按已排序 seriesKeys 挑出 cache key（engine.go:1665）
    Engine->>Cache: ⑥ DeleteRange(deleteKeys, min, max)（cache.go:619）
    Engine->>WAL: ⑦ WAL.DeleteRange(deleteKeys, min, max)（engine.go:1685，仅 WALEnabled）
    Note over Engine: ⑧ index.Rebuild() 对账（engine.go:1575）

    Note over Engine: 查询时
    Engine->>FS: ⑨ FileStore.locations() 读取 TombstoneRange
    FS->>Disk: 读取 .tombstone
    FS->>FS: 整块被覆盖 → 跳过；部分覆盖 → 解码后 Exclude

    Note over Engine: Compaction 时
    Engine->>Engine: ⑩ NewTSMBatchKeyIterator(...)
    Engine->>Engine: ⑪ Next() 收集同 key blocks + blk.tombstones
    Engine->>Engine: ⑫ merge*/combine* 执行 v.Exclude(ts.Min, ts.Max)
    Engine->>FS: ⑬ chunk*/Encode*ArrayBlock 重写输出
```

当前 compaction 路径在 `Compactor.compact()` 中创建
`NewTSMBatchKeyIterator(size, fast, DefaultMaxSavedErrors, intC, tsmFiles, trs...)`；
`tsmBatchKeyIterator.Next()` 为每个 block 保存 `blk.tombstones = iter.r.TombstoneRange(key)`。
随后 `merge()` 按 block 类型分发到 `compact.gen.go` 生成的
`mergeFloat/mergeInteger/mergeUnsigned/mergeBoolean/mergeString`，这些函数再调用对应
`combine*`。当需要解码归并或 block 带 tombstone 时，`combine*` 会对解码后的类型化数组执行
`v.Exclude(ts.Min, ts.Max)`，最后 `chunk*` 重新编码输出；如果一个 key 的 merged 输出为空，
`Next()` 会继续读取下一个 key。

### 13.3 Tombstoner 与 v3/v4 格式

**Tombstoner 结构** (`tombstone.go:32-59`):

```go
type Tombstoner struct {
    mu sync.RWMutex

    // Path 是要记录 tombstone 的 TSM 文件的完整路径。
    Path string

    // FilterFn 是一个可选的过滤函数，用于在加载 tombstone 时过滤 key。
    // 如果为 nil，则不过滤。
    FilterFn func(k []byte) bool

    // tombstones 是已写入但尚未刷新到磁盘的 tombstone 列表。
    tombstones []Tombstone

    // tombstoneStats 是 tombstone 统计信息的缓存。
    tombstoneStats TombstoneStat

    // statsLoaded 为 false 时表示统计信息可能与磁盘不同步，需要重新加载。
    statsLoaded bool

    // 以下字段用于 pending 写入（尚未提交到磁盘）。
    // 如果为 nil，则表示当前没有 pending 写入。
    gz                *gzip.Writer    // gzip 压缩器，用于 v4 格式写入
    bw                *bufio.Writer   // 缓冲写入器
    pendingFile       *os.File        // 正在写入的临时文件
    tmp               [8]byte         // 临时缓冲区，用于编码整数
    lastAppliedOffset int64           // 上次应用的偏移量，用于增量追加

    // obs 是可选的观察者，当 tombstone 文件被写入时通知。
    obs tsdb.FileStoreObserver
}
```

**v4 vs v3 格式**:
- **v4**（header `0x1504`）：`prepareV4()` 创建 `<tsm>.tombstone.tmp`；若已有 v4 文件，用 `io.Copy` 把旧内容整段复制到 tmp，之后 `AddRange` 复用同一个 `pendingFile`/`gz` 追加新条目（`tombstone.go:302-340`）。
- **v3**（header `0x1503`）：`writeTombstoneV3()` 每次把所有 tombstone 全量重写到新的临时文件（`tombstone.go:271-300`）；`prepareV4()` 遇到非 v4 header 返回 `errIncompatibleVersion` 时走这条回退路径。
- 两者的 `commit()` 完全相同：`gz.Close → bw.Flush → tmp.Sync → rename(tmp, .tombstone) → SyncDir`（`tombstone.go:372-403`）。**没有任何一版是"直接追加到 .tombstone 文件"**——写入始终发生在 tmp 文件上，最后一次原子 rename 生效。

## 14. WAL 恢复流程

Engine 启动时，`Open()` 调用 `reloadCache()` 从 WAL 文件恢复 Cache：

```go
// tsdb/engine/tsm1/engine.go:2740 — reloadCache
func (e *Engine) reloadCache() error {
    // 1. 获取所有 WAL segment 文件
    files, err := segmentFileNames(e.WAL.Path())

    // 2. 临时禁用 Cache 大小限制 (防止恢复过程中被拒绝)
    limit := e.Cache.MaxSize()
    defer func() { e.Cache.SetMaxSize(limit) }()
    e.Cache.SetMaxSize(0)

    // 3. 创建 CacheLoader 顺序读取 WAL 文件
    loader := NewCacheLoader(files)
    loader.WithLogger(e.logger)

    // 4. 逐条读取 WAL entry，写入 Cache
    if err := loader.Load(e.Cache); err != nil {
        return err
    }

    // 5. 恢复 Cache 大小限制
    return nil
}
```

**恢复顺序**: WAL 文件按文件名排序（`_00001.wal`, `_00002.wal`, ...），
顺序重放。同一个 key 的多次写入会追加到 Cache entry 中，
最终在 compaction 时去重。

**WALSegmentReader 的字节语义**（`wal.go:1131-1213`）：

`CacheLoader` 不直接解析字节，它通过 `WALSegmentReader` 逐条还原 entry：

1. `Next()` 先读 `Type(1B) + Length(4B)`（`wal.go:1131-1150`）；`Length` 是 **Snappy 压缩后**的长度。
2. 按 `Length` 读满 payload，再 `snappy.DecodedLen` + `snappy.Decode`（`wal.go:1155-1175`）。
3. 按 `WalEntryType` 实例化 `WriteWALEntry`/`DeleteWALEntry`/`DeleteRangeWALEntry`，再调用 `UnmarshalBinary`（`wal.go:1177-1191`）。
4. **只有 `UnmarshalBinary` 成功才累加 `r.n`**（`wal.go:1191-1195`），所以 `Count()` 永远停在"最后一条完整 entry 之后"的安全截断点。

> **注意**：`Next()` 在读取/解压失败时仍返回 `true`（源码注释写明要让调用方去 `Read()` 取错误），错误放在 `r.err`，必须由随后的 `Read()` 返回（`wal.go:1141-1145`、`1200-1206`）。只调 `Next()` 不调 `Read()` 会静默跳过错误。

**字节案例**：`_00002.wal` 在偏移 `131072` 处只剩半条记录（`Type+Length` 已读、payload 不足）：
- `r.Next()` 的 `io.ReadFull` 返回 `io.ErrUnexpectedEOF`，`r.err` 置位，`r.n` 不变；
- `r.Count()` 仍是 `131072`（上一条完整 entry 的末尾）；
- `CacheLoader.Load` 用 `f.Truncate(131072)` 截掉这半条，前面已重放的数据保留，该 entry 整体丢弃。

WAL 段文件的写入、`syncDelay` 批量 fsync 与 segment 滚动见 **15 号文档 §7.2**；本节的 `CacheLoader.Load` 与那边的 `WALSegmentReader` 是同一套字节格式。

**WAL 恢复流程 Mermaid 图**:

```mermaid
sequenceDiagram
    participant Open as Engine.Open()
    participant Cache as Cache
    participant WAL as WAL 目录
    participant Loader as CacheLoader
    participant Entry as WAL Entry

    Open->>WAL: ① segmentFileNames(path)
    Note over WAL: 扫描目录，按文件名排序:<br>_00001.wal, _00002.wal, _00003.wal

    Open->>Cache: ② 保存 MaxSize (默认 1GB)
    Open->>Cache: ③ SetMaxSize(0) — 临时禁用限制
    Note over Cache: 恢复过程中不受容量限制<br>防止 ErrCacheMemorySizeLimitExceeded

    Open->>Loader: ④ NewCacheLoader(sortedFiles)

    loop 每个 WAL 文件 (按序)
        Loader->>WAL: ⑤ 打开 _NNNNN.wal
        loop 每个 entry
            Loader->>Entry: ⑥ Read() → Type + Compressed Payload
            Entry->>Entry: ⑦ Snappy 解压
            Entry->>Entry: ⑧ 解析 key + values

            alt WriteWALEntry (Type=1)
                Entry->>Cache: ⑨ Cache.WriteMulti(values)
                Note over Cache: 同一 key 的多次写入<br>追加到 entry.values
            else DeleteWALEntry (Type=2)
                Entry->>Cache: ⑩ Cache.Delete(key)
            else DeleteRangeWALEntry (Type=3)
                Entry->>Cache: ⑪ Cache.DeleteRange(key, min, max)
            end
        end
        Loader->>WAL: ⑫ 关闭文件
    end

    Open->>Cache: ⑬ SetMaxSize(原始值) — 恢复限制
    Note over Open: 恢复完成<br>Cache 包含崩溃前未持久化的所有数据
```

**WAL Segment 滚动案例**:

> **具体案例**: 假设 SegmentSize = 10MB，连续写入数据
>
> ```
> t=0s    打开 _00001.wal，开始写入
> t=5s    _00001.wal 大小 = 8MB，继续写入
> t=7s    _00001.wal 大小 = 11MB > 10MB
>         → rollSegment() 触发
>         → 关闭 _00001.wal 的 writer
>         → 创建 _00002.wal
>         → 新写入进入 _00002.wal
>
> t=10s   _00002.wal 大小 = 12MB > 10MB
>         → 创建 _00003.wal
>
> t=15s   Cache Snapshot 触发
>         → WAL.CloseSegment() 关闭 _00003.wal
>         → Cache.Snapshot() O(1) 交换
>         → Compactor.WriteSnapshot() 写入 TSM 文件
>         → WAL.Remove([_00001.wal, _00002.wal, _00003.wal])
>         → 删除已持久化的 WAL 文件
> ```
>
> 恢复时只重放 Snapshot 之后的 WAL 文件（如果有新写入在 Snapshot 之后、崩溃之前）。

## 15. Engine.Open() 初始化序列

```go
// tsdb/engine/tsm1/engine.go:771-817 — Open
func (e *Engine) Open() error {
    // 1. 创建数据目录 (error 必须返回)
    if err := os.MkdirAll(e.path, 0777); err != nil {
        return err
    }

    // 2. 清理临时文件 (error 必须返回)
    if err := e.cleanup(); err != nil {
        return err
    }

    // 3. 加载字段元数据 (fields.idx)；失败只 Warn，不中断启动
    fieldPath := filepath.Join(e.path, "fields.idx")
    fields, err := tsdb.NewMeasurementFieldSet(fieldPath, e.logger)
    if err != nil {
        e.logger.Warn("error opening fields.idx: Rebuilding.", zap.String("path", fieldPath), zap.Error(err))
    }

    e.mu.Lock()
    e.fieldset = fields
    e.mu.Unlock()

    e.index.SetFieldSet(fields)

    // 4. 打开 WAL (error 必须返回)
    if e.WALEnabled {
        if err := e.WAL.Open(); err != nil {
            return fmt.Errorf("error opening WAL for %q: %w", e.WAL.Path(), err)
        }
    }

    // 5. 打开 FileStore (mmap 所有 TSM 文件, error 必须返回)
    if err := e.FileStore.Open(); err != nil {
        return err
    }

    // 6. 从 WAL 恢复 Cache (error 必须返回)
    if e.WALEnabled {
        if err := e.reloadCache(); err != nil {
            return err
        }
    }

    // 7. 打开 Compactor
    e.Compactor.Open()

    // 8. 启用 compaction (如果配置允许)
    if e.enableCompactionsOnOpen {
        e.SetCompactionsEnabled(true)
    }

    return nil
}
```

**关键顺序**:
- WAL 必须在 FileStore 之前打开（WAL 文件需要被恢复到 Cache）
- FileStore 必须在 reloadCache 之前打开（需要知道哪些 TSM 文件已存在）
- Compactor 必须在最后打开（依赖 FileStore 和 Cache 已就绪）
