# Module 12: 后台服务 (Retention + Precreator + Lease 机制) - 深度审计报告

> **小白导读**: 想象你经营一家仓储物流公司，除了日常收发快递（读写数据），还需要一群"后台管家"默默维护仓库秩序：
>
> - **Retention Service（数据保留服务）**= 仓库清理员。每个货架（ShardGroup）都有保质期，过期的货物必须清走。
>   清理分两步：先在账本上划掉（软删除），再真正搬走货物（物理删除），最后把空货架也拆掉（元数据修剪）。
>
> - **Precreator Service（预创建服务）**= 仓库调度员。它提前把新货架搭好，这样新货物到了就能直接上架，
>   不用临时手忙脚乱地搭架子（避免写入时触发 Raft 共识的延迟）。
>
> - **Lease 机制（租约）**= 仓库钥匙管理。在多仓库（集群）场景下，同一时间只允许一个人持有钥匙执行特定任务，
>   防止重复劳动。在单机模式下，钥匙管理简化为"永远给你"。
>
> **后台管家的工作节奏**：
> - Retention 每 30 分钟巡查一圈（ticker 驱动）
> - Precreator 每 10 分钟巡查一圈（time.After 驱动）
> - 两者都不使用租约，每个节点各自负责自己的清理和预创建工作

## 1. 后台服务总览

### 1.1 三大后台服务对比

```mermaid
flowchart TB
    subgraph 后台服务
        RetSvc["Retention Service<br/>数据保留/过期清理<br/>services/retention/service.go"]
        PreSvc["Precreator Service<br/>ShardGroup 预创建<br/>services/precreator/service.go"]
        CQSvc["Continuous Query Service<br/>连续查询执行<br/>services/continuous_querier/service.go"]
    end

    subgraph 驱动方式
        Ticker["time.NewTicker<br/>定时轮询"]
        TimeAfter["time.After<br/>定时轮询"]
        Lease["AcquireLease<br/>租约驱动"]
    end

    subgraph 租约使用
        NoLease["不使用租约<br/>每个节点独立执行"]
        UsesLease["使用租约<br/>OSS: 恒成功，无互斥"]
    end

    RetSvc --> Ticker --> NoLease
    PreSvc --> TimeAfter --> NoLease
    CQSvc --> Lease --> UsesLease

    style RetSvc fill:#e1f5fe
    style PreSvc fill:#f3e5f5
    style CQSvc fill:#fff3e0
```

| 服务 | 源码位置 | 驱动方式 | 默认间隔 | 是否使用租约 | 职责 |
|------|----------|----------|----------|-------------|------|
| **Retention Service** | `services/retention/service.go:50` | `time.NewTicker` | 30 分钟 | 否 | 删除过期数据和 ShardGroup |
| **Precreator Service** | `services/precreator/service.go:13` | `time.After` | 10 分钟 | 否 | 预创建即将到期的 ShardGroup |
| **CQ Service** | `services/continuous_querier/service.go` | `time.Timer`（`RunInterval`，默认 1s）+ 每次执行前 `AcquireLease` 闸门 | 1 秒 | 名义上是（OSS 无互斥） | 执行连续查询 |

### 1.2 为什么 Retention 和 Precreator 不使用租约

```mermaid
flowchart LR
    subgraph "CQ Service（名义租约，OSS 无互斥）"
        CQ1["节点 A"] -->|"AcquireLease 恒成功"| Result1["执行 CQ"]
        CQ2["节点 B"] -->|"AcquireLease 恒成功"| Skip1["也执行 CQ（可能重复）"]
        Note1["OSS: Leases.Acquire / NewLeases<br/>无生产调用方"]
    end

    subgraph "Retention Service（不使用租约）"
        R1["节点 A"] -->|"删除本地 Shard"| DiskA["本地磁盘"]
        R2["节点 B"] -->|"删除本地 Shard"| DiskB["本地磁盘"]
        Note2["删除是幂等操作<br/>每个节点删自己的本地文件"]
    end

    subgraph "Precreator Service（不使用租约）"
        P1["节点 A"] -->|"预创建"| Meta["MetaClient（幂等）"]
        P2["节点 B"] -->|"预创建"| Meta
        Note3["创建是幂等操作<br/>ShardGroupByTimestamp 检查防重复"]
    end

    style CQ1 fill:#c8e6c9
    style CQ2 fill:#c8e6c9
```

**设计决策**：
- **CQ 名义上使用租约**：设计意图是“多节点只让 leader 执行”；但 OSS 的 `Client.AcquireLease`（`services/meta/client.go:127-133`）恒成功、不读写共享状态，该保护在本仓库不生效——多进程共连同一 meta 时 CQ 可能重复执行。
- **Retention 不使用租约**：`services/retention/` 全目录无 lease 符号，服务按 ticker 独立运行；每个节点删除的是自己本地磁盘上的 Shard 文件，天然分布式。
- **Precreator 不使用租约**：`services/precreator/` 同样无 lease 符号；`ShardGroupByTimestamp` 检查保证幂等性，重复创建不会产生副作用。

### 1.3 服务生命周期

> 本节只覆盖 Retention/Precreator 的 Open/Close；三个服务的依赖注入与 CQ 生命周期见 §1.4。

```mermaid
sequenceDiagram
    participant Main as influxd 主进程
    participant RetSvc as Retention Service
    participant PreSvc as Precreator Service

    Main->>RetSvc: NewService(config) → SetOSSMetaClient / TSDBStore / DropShardMetaRef
    Main->>RetSvc: WithLogger(logger)
    Main->>RetSvc: Open() → 创建 done，启动 run() goroutine
    Note over RetSvc: 首个 DeletionCheck 要等满一个 check-interval（30m）

    Main->>PreSvc: NewService(config) → MetaClient
    Main->>PreSvc: WithLogger(logger)
    Main->>PreSvc: Open() → 创建 done，启动 runPrecreation()
    Note over PreSvc: 首个 precreate 要等满一个 check-interval（10m）

    Note over RetSvc,PreSvc: 两个服务独立运行，互不依赖

    Main->>Main: 收到 SIGTERM/SIGINT
    Main->>RetSvc: Close() → close(done) + wg.Wait()
    Note over RetSvc: 若当前 DeletionCheck 正在执行，会阻塞到它结束
    Main->>PreSvc: Close() → close(done) + wg.Wait()
    Main->>Main: 进程退出
```

**生命周期语义**:
- **首个 tick 延迟**：`Open()` 只是 `go run()` / `go runPrecreation()`；`run()` 先
  `time.NewTicker`（`services/retention/service.go:139-151`），`runPrecreation()` 先
  `time.After(checkInterval)`（`services/precreator/service.go:72-86`）。因此第一次检查要等满一个
  间隔，`Open()` 不会立刻执行。
- **Close 不中断当前检查**：`Close()` 只 `close(done)` 再 `wg.Wait()`（`retention/service.go:121-131`、
  `precreator/service.go:59-69`）。`DeletionCheck`/`precreate` 没有 context、没有超时，正在执行时会
  一直跑到结束，`Close()` 随之阻塞。运维停机时间可能因此拉长（见 §11.9）。
- **participants 精简**：原图里的 `Meta`/`TSDB` 没有任何消息，已删除。

### 1.4 服务装配与启动/关闭契约

三个后台服务都由 `cmd/influxd/run/server.go` 手工装配（没有 DI 容器），字段是导出的，为 nil 时
多数情况下只在运行时 panic 或静默失效。

```mermaid
flowchart LR
    Main["influxd main"] --> AR["appendRetentionPolicyService<br>server.go:300"]
    Main --> AP["appendPrecreatorService<br>server.go:396"]
    Main --> AC["appendContinuousQueryService<br>server.go:416"]
    AR --> R1["retention.NewService(c)"]
    R1 --> R2["SetOSSMetaClient(s.MetaClient)"]
    R1 --> R3["srv.TSDBStore = s.TSDBStore"]
    R1 --> R4["srv.DropShardMetaRef = OSSDropShardMetaRef(...)"]
    AP --> P1["precreator.NewService(c)"]
    P1 --> P2["srv.MetaClient = s.MetaClient"]
    AC --> C1["continuous_querier.NewService(c)"]
    C1 --> C2["srv.MetaClient / QueryExecutor / Monitor"]
    C1 --> C3["Monitor.RegisterDiagnosticsClient('cq', srv)"]
```

| 服务 | 字段 | 谁注入 | 为 nil 的后果 |
|------|------|--------|----------------|
| Retention | `MetaClient` | `srv.SetOSSMetaClient(s.MetaClient)`（`server.go:305`） | 首次 `DeletionCheck` 解引用时 panic |
| Retention | `TSDBStore` | `srv.TSDBStore = s.TSDBStore`（`server.go:306`） | 首次 `DeletionCheck` 调 `ShardIDs()` 时 panic |
| Retention | `DropShardMetaRef` | `retention.OSSDropShardMetaRef(s.MetaClient)`（`server.go:307`） | `Open()` 直接返回 `invalid nil for retention service DropShardMetaRef`（`service.go:107-109`），服务完全不运行 |
| Precreator | `MetaClient` | `srv.MetaClient = s.MetaClient`（`server.go:401`） | 首次 `precreate` panic |
| CQ | `MetaClient` | `srv.MetaClient = s.MetaClient`（`server.go:421`） | `Open()` 的 `assert(s.MetaClient != nil, ...)` panic |
| CQ | `QueryExecutor` | `srv.QueryExecutor = s.QueryExecutor`（`server.go:422`） | `Open()` 的 `assert(... != nil)` panic |
| CQ | `Monitor` | `srv.Monitor = s.Monitor`（`server.go:423`） | 赋成 nil 接口后 `s.Monitor.Enabled()` 会 panic；若跳过赋值则用 `NewService` 的 `nullMonitor(0)`，`cq_query` 统计不落盘 |

```go
// services/retention/service.go:18-52 — OSS 接口与适配器
type OSSMetaClient interface {
    Databases() []meta.DatabaseInfo
    DeleteShardGroup(database, policy string, id uint64) error
    DropShard(id uint64) error
    PruneShardGroups() error
}

type MetaClient interface {
    OSSMetaClient
    NodeID() uint64
}

const ossNodeID uint64 = math.MaxUint64   // 不可能与 Enterprise 节点 ID 冲突

type ossMetaClientAdapter struct{ OSSMetaClient }

func (c *ossMetaClientAdapter) NodeID() uint64 { return ossNodeID }
```

```go
// services/retention/service.go:102-131 — Open/Close
func (s *Service) Open() error {
    if !s.config.Enabled || s.done != nil { return nil }
    if s.DropShardMetaRef == nil {
        return fmt.Errorf("invalid nil for retention service DropShardMetaRef")
    }
    s.done = make(chan struct{})
    s.wg.Add(1)
    go func() { defer s.wg.Done(); s.run() }()
    return nil
}

func (s *Service) Close() error {
    if !s.config.Enabled || s.done == nil { return nil }
    close(s.done)
    s.wg.Wait()          // 等当前 DeletionCheck 结束
    s.done = nil
    return nil
}
```

```go
// services/precreator/service.go:28-69 — NewService/Open/Close
func NewService(c Config) *Service {
    return &Service{
        checkInterval: time.Duration(c.CheckInterval),
        advancePeriod: time.Duration(c.AdvancePeriod),
        Logger:        zap.NewNop(),
    }
}

func (s *Service) Open() error {
    if s.done != nil { return nil }
    s.done = make(chan struct{})
    s.wg.Add(1)
    go s.runPrecreation()
    return nil
}
```

```go
// services/continuous_querier/service.go:104-150 — NewService/Open/Close
func NewService(c Config) *Service {
    s := &Service{
        Config:      &c,
        Monitor:     nullMonitor(0),
        RunInterval: time.Duration(c.RunInterval),
        RunCh:       make(chan *RunRequest),      // 无缓冲
        lastRuns:    map[string]time.Time{},
        ...
    }
    return s
}

func (s *Service) Open() error {
    if s.stop != nil { return nil }
    assert(s.MetaClient != nil, "MetaClient is nil")
    assert(s.QueryExecutor != nil, "QueryExecutor is nil")
    s.stop = make(chan struct{})
    s.wg.Add(1)
    go s.backgroundLoop()
    return nil
}
```

**失败案例**：只调用 `retention.NewService(c)` + `Open()`，忘记赋 `DropShardMetaRef`。
`Open()` 在 `service.go:107-109` 直接报错 `invalid nil for retention service DropShardMetaRef`，
`influxd` 启动失败——这是三个服务里唯一一处显式的 nil 校验。

**启动/关闭语义**（与 §1.3 一致）:
- 首个 `DeletionCheck`/`precreate` 要等满一个 interval（`retention/service.go:139-151`、
  `precreator/service.go:72-86`）。
- `Close()` 只发信号；若当前检查正在执行，会阻塞到它结束（`retention/service.go:121-131`）。
- `Service.Run`/`ContinuousQuerier` 在 OSS 无生产调用方，只有测试与内嵌调用。

## 2. Retention Service 全链路

### 2.1 Service 结构体

```go
// services/retention/service.go:18-28 — 两个 MetaClient 接口
type OSSMetaClient interface {
    Databases() []meta.DatabaseInfo
    DeleteShardGroup(database, policy string, id uint64) error
    DropShard(id uint64) error           // service.go:21 — 清理 shard 元数据引用
    PruneShardGroups() error
}

type MetaClient interface {
    OSSMetaClient                        // 嵌入 OSSMetaClient
    NodeID() uint64                      // Enterprise 节点 ID；OSS 用 ossMetaClientAdapter 返回 ossNodeID
}

// services/retention/service.go:50 — Service
type Service struct {
    MetaClient                           // 嵌入 MetaClient (OSS 或 Enterprise 实现)
    TSDBStore interface {
        ShardIDs() []uint64
        DeleteShard(shardID uint64) error
        SetShardNewReadersBlocked(shardID uint64, blocked bool) error
        ShardInUse(shardID uint64) (bool, error)
    }
    DropShardMetaRef func(shardID uint64, owners []uint64) error

    config Config
    wg     sync.WaitGroup
    done   chan struct{}

    logger *zap.Logger
}
```

> **审计校准** (service.go:18-28):
> - `MetaClient` 接口包含 `DropShard(id uint64) error` (line 21)。
> - 实际是**两个**接口：`OSSMetaClient` (Databases, DeleteShardGroup, DropShard, PruneShardGroups)
>   和 `MetaClient` (嵌入 `OSSMetaClient` + `NodeID() uint64`)。
> - Service 通过嵌入 `MetaClient` 获得全部方法；OSS 用 `ossMetaClientAdapter`
>   (service.go:40-47) 包装 `OSSMetaClient` 并提供 `NodeID() = ossNodeID`。

### 2.2 配置默认值

```go
// services/retention/config.go:18 — NewConfig
func NewConfig() Config {
    return Config{Enabled: true, CheckInterval: toml.Duration(30 * time.Minute)}
}
```

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `enabled` | `true` | 是否启用保留策略服务 |
| `check-interval` | `30m` | 检查间隔，每 30 分钟扫描一次过期数据 |

### 2.3 run() 主循环全链路

```mermaid
sequenceDiagram
    participant Ticker as time.Ticker (30min)
    participant Run as run() goroutine
    participant Meta as MetaClient
    participant TSDB as TSDBStore
    participant Disk as 本地磁盘

    loop 每 30 分钟
        Ticker->>Run: ticker.C 触发

        Note over Run: === Phase 1: 识别过期 ShardGroup ===
        Run->>Meta: Databases() → 获取所有数据库
        loop 每个 Database
            loop 每个 RetentionPolicy
                Run->>Meta: r.DeletedShardGroups()
                Meta-->>Run: 已软删除的 ShardGroup 列表
                Note over Run: 收集已删除 ShardGroup 的 Shard ID

                Run->>Meta: r.ExpiredShardGroups(now)
                Meta-->>Run: 过期的 ShardGroup 列表
                loop 每个过期 ShardGroup
                    Run->>Meta: DeleteShardGroup(db, rp, g.ID)
                    Note over Meta: 设置 DeletedAt = now<br/>这是软删除，不是物理删除
                    Note over Run: 收集新删除 ShardGroup 的 Shard ID
                end
            end
        end

        Note over Run: === Phase 2: 并发安全地物理删除本地 Shard ===
        Run->>TSDB: ShardIDs() → 获取本地所有 Shard ID
        loop 每个本地 Shard
            alt Shard ID 在 deletedShardIDs 中
                Run->>TSDB: SetShardNewReadersBlocked(id, true)
                Run->>TSDB: ShardInUse(id)
                alt 仍有 reader 使用
                    Run->>TSDB: SetShardNewReadersBlocked(id, false)
                    Note over Run: 本轮跳过，等待下次检查
                else 没有 reader 使用
                    Run->>TSDB: DeleteShard(id)
                    TSDB->>Disk: 删除 Shard 文件和 WAL
                    Run->>Meta: DropShardMetaRef(id, owners)
                end
            end
        end
        loop deletedShardIDs 中剩余的 shard
            alt metadata 引用但本地不存在 (phantom shard)
                Run->>Meta: DropShardMetaRef(id, owners)
            end
        end

        Note over Run: === Phase 3: 修剪元数据 ===
        Run->>Meta: PruneShardGroups()
        Note over Meta: 删除 DeletedAt > 14 天且 Shards 为空的 ShardGroup 元数据

        Note over Run: === Phase 4: 错误处理 ===
        alt 有错误发生
            Run->>Run: retryNeeded = true
            Note over Run: 日志提示下次 check_interval 会重试
        end
    end
```

### 2.4 Phase 1: 识别过期 ShardGroup

```go
// services/retention/service.go:180-214 — Phase 1
deletedShardIDs := make(map[uint64]deletionInfo)

// 1. 收集已软删除的 ShardGroup 的 Shard ID
dbs := s.MetaClient.Databases()
for _, d := range dbs {
    for _, r := range d.RetentionPolicies {
        for _, g := range r.DeletedShardGroups() {
            for _, sh := range g.Shards {
                deletedShardIDs[sh.ID] = deletionInfo{db: d.Name, rp: r.Name}
            }
        }

        // 2. 找到过期的 ShardGroup 并软删除
        for _, g := range r.ExpiredShardGroups(time.Now().UTC()) {
            if err := s.MetaClient.DeleteShardGroup(d.Name, r.Name, g.ID); err != nil {
                retryNeeded = true
                continue
            }
            for _, sh := range g.Shards {
                deletedShardIDs[sh.ID] = deletionInfo{db: d.Name, rp: r.Name}
            }
        }
    }
}
```

**过期判断逻辑** (`services/meta/data.go:1275`)：

```go
// services/meta/data.go:1275 — ExpiredShardGroups
func (rpi *RetentionPolicyInfo) ExpiredShardGroups(t time.Time) []*ShardGroupInfo {
    var groups = make([]*ShardGroupInfo, 0)
    for i := range rpi.ShardGroups {
        if rpi.ShardGroups[i].Deleted() {
            continue  // 跳过已删除的
        }
        // 关键判断: EndTime + Duration < now
        // 即 ShardGroup 的结束时间加上保留时长已经过了当前时间
        if rpi.Duration != 0 && rpi.ShardGroups[i].EndTime.Add(rpi.Duration).Before(t) {
            groups = append(groups, &rpi.ShardGroups[i])
        }
    }
    return groups
}
```

**软删除逻辑** (`services/meta/data.go:454-472`)：

```go
// services/meta/data.go:454 — DeleteShardGroup
func (data *Data) DeleteShardGroup(database, policy string, id uint64) error {
    rpi, err := data.RetentionPolicy(database, policy)
    // ... 错误检查 ...

    // 找到 ShardGroup 并设置 DeletedAt 时间戳
    for i := range rpi.ShardGroups {
        if rpi.ShardGroups[i].ID == id {
            rpi.ShardGroups[i].DeletedAt = time.Now().UTC()  // 软删除标记
            return nil
        }
    }
    return ErrShardGroupNotFound
}
```

> **小白解释**: 软删除就像在账本上画个叉，表示"这个货架要拆了"，但货物还在原地。
> 真正搬走货物是 Phase 2 的事。

### 2.4a deletionInfo — 函数局部类型与 owners 提取

`DeletionCheck` 在函数内部定义了一个**函数局部 struct** `deletionInfo` (service.go:161-172)，
并通过 `newDeletionInfo` 闭包从 `meta.ShardInfo` 提取 `Owners`（每个 owner 的 `NodeID`），
用于后续 phantom-shard 检查和 `DropShardMetaRef` 调用。

```go
// services/retention/service.go:161-172 — deletionInfo (函数局部类型)
type deletionInfo struct {
    db     string
    rp     string
    owners []uint64
}
newDeletionInfo := func(db, rp string, si meta.ShardInfo) deletionInfo {
    owners := make([]uint64, len(si.Owners))
    for i, o := range si.Owners {
        owners[i] = o.NodeID
    }
    return deletionInfo{db: db, rp: rp, owners: owners}
}
deletedShardIDs := make(map[uint64]deletionInfo)
```

**关键设计点**:
- **函数局部类型**: `deletionInfo` 只在 `DeletionCheck` 作用域内可见，不污染包级命名空间。
  这符合 Go "小作用域优先" 的惯用法——该结构只服务于本轮删除检查，不需要外部复用。
- **owners 提取**: `meta.ShardInfo.Owners` 是 `[]ShardOwner{NodeID uint64}`，而
  `DropShardMetaRef(shardID, owners []uint64)` 接收的是裸 `[]uint64`。`newDeletionInfo`
  做的就是 `[]ShardOwner → []uint64` 的扁平化转换，避免在 phantom-shard 分支
  (service.go:284-300) 重复遍历 `si.Owners`。
- **OSS 场景下 owners 为空**: OSS 每个 ShardGroup 只有 1 个 Shard 且 `ShardInfo.Owners`
  为空 (Module 06 已确认)，所以 `newDeletionInfo` 返回的 `owners` 在 OSS 中恒为空切片。
  但 Enterprise 模式下 `owners` 非空，`isOSS()` 分支 (service.go:288) 会据此判断
  "本节点是否应该清理这个 phantom shard 的 meta 引用"。

> **具体案例**: phantom shard 的 owners 提取
>
> 假设 Enterprise 集群有 3 个节点 (NodeID=1,2,3)，shard 42 的 `ShardInfo.Owners = [{1}, {3}]`
> (即 shard 42 的数据副本存在节点 1 和节点 3 上)。
>
> 1. 节点 1 的 Retention Service 发现 shard 42 过期，`newDeletionInfo("mydb", "autogen", sh)`
>    返回 `deletionInfo{db:"mydb", rp:"autogen", owners:[1, 3]}`。
> 2. 节点 1 本地 `DeleteShard(42)` 成功，调用 `DropShardMetaRef(42, [1, 3])`。
> 3. 节点 3 因为网络延迟还没删完，它的 `deletedShardIDs[42]` 仍保留同样的 owners。
> 4. 节点 3 下一轮检查时，`isOSS()` 返回 false，`slices.Contains(info.owners, s.NodeID())`
>    (NodeID=3 ∈ [1,3]) 为 true，于是节点 3 也执行本地 `DeleteShard(42)` + `DropShardMetaRef`。
>
> 如果跳过 `newDeletionInfo` 的 owners 提取，phantom-shard 分支就无法判断"这个 shard
> 是否归属本节点"，可能导致非 owner 节点误删 meta 引用或 owner 节点漏删。

### 2.5 Phase 2: 并发安全地物理删除本地 Shard

> **审计校准** (service.go:217-281): 真实代码用 **defer-based unblock 模式**，
> 把 shard 删除包在一个返回 `rErr` 的匿名函数里。阻塞解除**不是**内联调用，
> 而是由 defer 在函数返回时按条件执行。

```go
// services/retention/service.go:217-281 — Phase 2
for _, id := range s.TSDBStore.ShardIDs() {
    if info, ok := deletedShardIDs[id]; ok {
        delete(deletedShardIDs, id)

        err := func() (rErr error) {           // 匿名函数，rErr 被 defer 捕获
            // ... (logger operation 设置略)

            // ① 阻止新 reader 进入
            if err := s.TSDBStore.SetShardNewReadersBlocked(id, true); err != nil {
                return fmt.Errorf("error blocking new readers for shard: %w", err)
            }
            // ② defer: 仅当 rErr != nil 且不是 ErrShardNotFound 时才解除阻塞
            //    ErrShardNotFound 特意 SUPPRESS 解除阻塞 (shard 已不在 store，无需 unblock)
            defer func() {
                if rErr != nil && !errors.Is(rErr, tsdb.ErrShardNotFound) {
                    s.TSDBStore.SetShardNewReadersBlocked(id, false)
                }
            }()

            // ③ 检查旧 reader 是否仍在使用
            if inUse, err := s.TSDBStore.ShardInUse(id); err != nil {
                return fmt.Errorf("error checking if shard is in-use: %w", err)
            } else if inUse {
                return errors.New("can not delete an in-use shard")   // service.go:250
            }

            // ④ 物理删除
            if err := s.TSDBStore.DeleteShard(id); err != nil {
                return fmt.Errorf("error deleting shard from store: %w", err)
            }
            return nil
        }()

        // ⑤ ErrShardNotFound 不算失败 — 继续清理 meta reference
        if err != nil && !errors.Is(err, tsdb.ErrShardNotFound) {
            retryNeeded = true
            continue
        }

        // ⑥ 清理 metadata 引用 (DropShardMetaRef)
        if err := s.DropShardMetaRef(id, info.owners); err != nil {
            retryNeeded = true
        }
    }
}

// deletedShardIDs 中剩余的是 metadata 有引用、但本地 store 不存在的过期 shard (phantom)。
for id, info := range deletedShardIDs {
    if s.isOSS() || slices.Contains(info.owners, s.NodeID()) {
        if err := s.DropShardMetaRef(id, info.owners); err != nil {
            retryNeeded = true
        }
    }
}
```

**关键设计** (service.go:237-244):
- 阻塞解除由 `defer` 在匿名函数返回时执行，**不是**内联调用。
- defer 的条件是 `rErr != nil && !errors.Is(rErr, tsdb.ErrShardNotFound)`：
  - 失败 (非 ErrShardNotFound) → 解除阻塞，下轮重试。
  - `ErrShardNotFound` → **特意图省略**解除阻塞 (shard 已不在 store，`SetShardNewReadersBlocked` 也会失败)。
- `ShardInUse` 返回 true 时返回 `errors.New("can not delete an in-use shard")`，
  由 defer 解除阻塞（因为这是 `rErr != nil` 路径，且错误不是 `ErrShardNotFound`）。
- 成功路径 (`rErr == nil`) 不解除阻塞——shard 已删除，阻塞标记随 shard 一起消失。

### 2.6 Phase 3: 修剪元数据

```go
// services/retention/service.go:302-309 — Phase 3
if err := s.MetaClient.PruneShardGroups(); err != nil {
    log.Info("Problem pruning shard groups", zap.Error(err))
    retryNeeded = true
}
```

`PruneShardGroups` 分两层：`client.go` 只负责 Clone/commit 的发布外壳，真正的筛选循环在
`data.go` 的 `Data.PruneShardGroups`。

```go
// services/meta/client.go:680-691 — 薄封装（Clone → mutate → commit）
func (c *Client) PruneShardGroups() error {
    expiration := time.Now().Add(ShardGroupDeletedExpiration)  // -14 天
    c.mu.Lock()
    defer c.mu.Unlock()
    data := c.cacheData.Clone()
    changed := data.PruneShardGroups(expiration)
    if changed {
        return c.commit(data)
    }
    return nil
}
```

```go
// services/meta/data.go:476-492 — 真正的筛选循环
func (data *Data) PruneShardGroups(expiration time.Time) bool {
    var changed bool
    for i, d := range data.Databases {
        for j, rp := range d.RetentionPolicies {
            var remainingShardGroups []ShardGroupInfo
            for _, sgi := range rp.ShardGroups {
                if sgi.DeletedAt.IsZero() || !expiration.After(sgi.DeletedAt) || len(sgi.Shards) > 0 {
                    remainingShardGroups = append(remainingShardGroups, sgi)
                    continue
                }
                changed = true
            }
            data.Databases[i].RetentionPolicies[j].ShardGroups = remainingShardGroups
        }
    }
    return changed
}
```

**两个条件缺一不可**：
1. `len(sgi.Shards) == 0`——所有节点都完成了物理删除，并且 `DropShardMetaRef` 已把 `ShardInfo`
   从元数据里移除；
2. `sgi.DeletedAt` 早于 `now-14d`。

> **小白解释**: 软删除后元数据还要保留 14 天。这不是"给慢节点的宽限期"，而是两道门：
> 第一道门（`Shards` 为空）确认物理删除与 meta 引用清理都已完成；第二道门（14 天）是在清空后
> 继续保留墓碑，给运维留出排查窗口。只要组里还剩任何一个 `ShardInfo`，无论过多久都不会被修剪。

> 该函数的发布语义（Clone → mutate → commit、失败不广播）见 §2.8。

**ShardGroupDeletedExpiration 常量**：

```go
// services/meta/client.go:36 — ShardGroupDeletedExpiration
ShardGroupDeletedExpiration = -2 * 7 * 24 * time.Hour  // -14 天
```

> **小白解释**: 软删除后，元数据还要保留 14 天。为什么？因为集群中的其他节点可能还没来得及
> 物理删除自己的 Shard 文件。14 天的缓冲期确保所有节点都有足够时间完成清理。

### 2.7 Phase 4: 错误处理与重试

```go
// services/retention/service.go:311-313
if retryNeeded {
    log.Info("One or more errors occurred during shard deletion and will be retried on the next check", logger.DurationLiteral("check_interval", time.Duration(s.config.CheckInterval)))
}
```

**重试策略**: 不使用指数退避，而是在下一个 `check_interval`（默认 30 分钟）自然重试。
这是因为保留策略检查本身就是周期性的，不需要额外的重试机制。

### 2.8 MetaClient 的写入模型：Clone → mutate → commit

所有 meta 写操作（`CreateShardGroup`、`PrecreateShardGroups`、`PruneShardGroups`…）都走同一套发布模型：

1. 取 `c.mu` 写锁；
2. `data := c.cacheData.Clone()` 深拷贝当前快照；
3. 在副本上调用 `Data` 层的纯内存方法（失败时副本被丢弃，`cacheData` 不变）；
4. `c.commit(data)`：`Index++` → `snapshot()` 落盘 → `c.cacheData = data` → `close(c.changed)` → `c.changed = make(chan struct{})`。

```mermaid
sequenceDiagram
    participant Svc as 调用方（Retention/Precreation/...）
    participant C as meta.Client
    participant D as Data 副本
    participant Disk as meta.db
    participant W as WaitForDataChanged 的 watcher

    Svc->>C: CreateShardGroup / PruneShardGroups / ...
    C->>C: c.mu.Lock() → defer Unlock()
    C->>C: data := c.cacheData.Clone()
    C->>D: 在副本上 mutate
    D-->>C: changed
    C->>C: data.Index++
    C->>Disk: snapshot(tmp → Write → Sync → RenameFile)
    Disk-->>C: nil / err
    alt snapshot 失败
        C-->>Svc: return err（cacheData 不变，changed 不广播）
    else 成功
        C->>C: c.cacheData = data
        C->>W: close(c.changed)
        C->>C: c.changed = make(chan struct{})
        C-->>Svc: return nil
    end
```

```go
// services/meta/client.go:948-964 — commit
func (c *Client) commit(data *Data) error {
    data.Index++
    if err := snapshot(c.path, data); err != nil {   // 先落盘
        return err
    }
    c.cacheData = data                               // 再换指针
    close(c.changed)                                 // 广播
    c.changed = make(chan struct{})
    return nil
}
```

**读路径**：`Databases()`、`RetentionPolicy()`、`ShardGroupsByTimeRange()` 等只加 `c.mu.RLock()`
读 `c.cacheData` 指针，拿到的是不可变快照（写入方永远改副本，不改已发布的 `Data`）。
`ShardGroupsByTimeRange`（`client.go:615-634`）的 `RLock` 覆盖整个调用：指针读取、
`RetentionPolicy` 查找、`Deleted()`/`Overlaps()` 过滤以及逐个 `append`。元素按值复制
（`ShardGroupInfo` 是值类型，其 `Shards` 切片与缓存共享底层数组），不做深拷贝，因此仍很快。

**失败语义**：
- `snapshot` 失败 → `cacheData` 仍是旧指针，磁盘仍是旧版本，`changed` 不广播，调用方收到 error。
- 下一次重试安全：`Index` 只加在副本上，失败的那次不会让编号跳号。
- 注意：`snapshot` 只 `f.Sync()` + rename，没有 `SyncDir`（详见 07 号文档 §9.2/§3.2.x）。

**规模案例**：10 个 DB × 3 个 RP × 1000 个 ShardGroup。Retention 每 30 分钟调用一次
`PruneShardGroups`，每次都要 `Clone()` 整棵元数据树；`c.mu.Lock()` 覆盖
`MarshalBinary + os.Create + Write + fsync + rename`。这段时间内所有 `RLock`
（包括写路径 `PointsWriter.MapShards` 里的 `RetentionPolicy()`）都会阻塞——这是 meta 写入
最大的延迟代价。

> 逐行详解见 07 号文档 §3.2（`commit`）与 §9（snapshot）；本节只保留摘要。

## 3. TSDBStore.DeleteShard — 物理删除完整流程

### 3.1 DeleteShard 全链路

```mermaid
sequenceDiagram
    participant Caller as Retention Service
    participant Store as TSDBStore
    participant Shard as Shard
    participant Index as InmemIndex
    participant SFile as SeriesFile
    participant Epoch as EpochTracker
    participant Disk as 本地磁盘

    Caller->>Store: DeleteShard(shardID)

    Note over Store: 步骤 1: 获取 Shard
    Store->>Store: s.Shard(shardID)
    alt Shard 不存在
        Store-->>Caller: return ErrShardNotFound (store.go:981)
        Note over Caller: Retention 用 !errors.Is(err, ErrShardNotFound)<br/>过滤后继续 DropShardMetaRef (service.go:263)
    end

    Note over Store: 步骤 2: 标记为待删除
    Store->>Store: s.mu.Lock()
    Store->>Store: 检查 pendingShardDeletes
    alt 已在删除中
        Store->>Store: s.mu.Unlock()
        Store-->>Caller: return nil（幂等）
    end
    Store->>Store: delete(s.shards, shardID)
    Store->>Store: s.pendingShardDeletes[shardID] = struct{}{}
    Store->>Store: 获取同数据库其他 Shard 列表
    Store->>Store: 获取 EpochTracker
    Store->>Store: s.mu.Unlock()

    Note over Store: 步骤 3: 清理孤立 Series
    Store->>Shard: Index() → 获取 SeriesIDSet
    loop 每个同数据库其他 Shard
        Store->>Shard: Index() → SeriesIDSet
        Store->>Store: ss.Diff(otherSeriesIDSet)
        Note over Store: 移除其他 Shard 也有的 Series ID<br/>最终 ss 只剩孤立 Series
    end

    alt 存在孤立 Series
        Store->>SFile: sfile = seriesFile(db)
        alt 使用 InmemIndex
            loop 每个孤立 Series
                Store->>SFile: sfile.SeriesKey(id) → key
                Store->>Index: index.DropSeriesGlobal(key)
            end
        end
        loop 每个孤立 Series
            Store->>SFile: sfile.DeleteSeriesID(id)
        end
    end

    Note over Store: 步骤 4: Epoch 屏障（不针对普通写入）
    Store->>Epoch: epoch.StartWrite()
    Epoch-->>Store: guards（仅来自 WaitDelete，可为空）, gen
    Note over Store: guards 只来自 WaitDelete（DeleteSeries / DeleteMeasurement）<br>DeleteShard 从不安装 guard，普通写入也不进 guards
    Note over Store: 真正的隔离：步骤 2 已从 s.shards 摘除（新写入拿到 ErrShardNotFound）<br>sh.Close() 用 Shard.mu 等待在途写入

    Note over Store: 步骤 5: 关闭 Shard
    Store->>Shard: sh.Close()

    Note over Store: 步骤 6: 删除文件
    Store->>Disk: os.RemoveAll(sh.path)
    Store->>Disk: os.RemoveAll(sh.walPath)

    Note over Store: 步骤 7: 延迟清理 (defer, store.go:1007-1013)
    Store->>Store: s.mu.Lock()
    Store->>Store: delete(s.epochs, shardID)
    Store->>Store: delete(s.pendingShardDeletes, shardID)
    Store->>Store: s.mu.Unlock()
    Note over Store: removeIndexType 在成功路径 else 分支 INLINE<br/>(store.go:1139)，不在 defer 里

    Store-->>Caller: return nil (或 ErrShardNotFound)
```

### 3.2 步骤 1: 获取 Shard

```go
// tsdb/store.go:978 — DeleteShard
func (s *Store) DeleteShard(shardID uint64) error {
    sh := s.Shard(shardID)
    if sh == nil {
        return ErrShardNotFound  // tsdb/store.go:33 — 不是 return nil
    }
```

> **审计校准** (`tsdb/store.go:978-982`): Shard 不存在时返回 `ErrShardNotFound`，**不是** `return nil`。
> `ErrShardNotFound` 只表示 **store 不再持有该 shard**，不等于磁盘已清空。
> `DeleteShard` 在把 shard 从 `s.shards` 摘除（`:996`）之后，仍可能因 `Index()`（`:1016`）、
> `walkShards` 系列清理、`sh.Close()`（`:1128`）、`os.RemoveAll`（`:1133-1135`）失败而返回
> 其它错误；此时文件留在磁盘，而 shard 已不在 `s.shards`。
>
> **4 步失败链**:
> 1. `DeleteShard` 返回非 `ErrShardNotFound` 错误 → Retention 置 `retryNeeded=true; continue`
>    （`services/retention/service.go:262-267`），且 `:219` 已把 id 从 `deletedShardIDs` 删除，
>    **本轮不会再走 phantom 分支**。
> 2. 下一轮：`DeletedShardGroups()` 重新把该 id 放回 `deletedShardIDs`（组里仍有这个 `ShardInfo`）。
> 3. 但 `ShardIDs()` 只返回 `s.shards` 的 key（`tsdb/store.go:1389-1394`），该 id 已不在其中 →
>    Phase 2 的本地删除循环根本不会遍历到它。
> 4. 同一轮的 phantom 循环（`services/retention/service.go:284-300`）在 OSS 直接
>    `DropShardMetaRef` → `meta.Client.DropShard` 删除 `ShardInfo`。磁盘目录
>    `data/<db>/<rp>/<id>` 与 WAL 目录成为孤儿，之后无人再尝试删除。
>
> 排查命令：`ls data/<db>/<rp>/<id>` 与对应 WAL 目录，人工确认后删除。

### 3.3 步骤 2: 标记为待删除

```go
// tsdb/store.go:988-1005
s.mu.Lock()
if _, ok := s.pendingShardDeletes[shardID]; ok {
    // 已在删除中，避免重复删除
    s.mu.Unlock()
    return nil
}
delete(s.shards, shardID)              // 从活跃 Shard 表中移除
s.pendingShardDeletes[shardID] = struct{}{}  // 标记为待删除

db := sh.Database()
shards := s.filterShards(byDatabase(db))  // 获取同数据库其他 Shard
epoch := s.epochs[shardID]
s.mu.Unlock()
```

**为什么先从 `s.shards` 移除？** 因为 `s.shards` 是 Shard 的索引表，
查询和写入都会通过它查找 Shard。移除后，新的读写请求不会再访问这个 Shard。

### 3.4 步骤 3: 清理孤立 Series

> **审计校准** (store.go:1042-1116): 真实代码使用 **NoFlush 批量删除**模式：
> `sfile.DeleteSeriesID(id, NoFlush)` 返回 partition `p`，收集到 `partitionIDs` map；
> 循环结束后再 `sfile.FlushSegments(partitionIDs)` 一次性刷盘。
> 每删除 `DeleteLogTrigger = 10_000` 个 series 打一条进度日志。
> InmemIndex 路径用 `errs []error` + `errors.Join` 收集错误。

```go
// tsdb/store.go:1042-1116
// 获取被删除 Shard 的 SeriesIDSet
index, err := sh.Index()
ss := index.SeriesIDSet()

// 与同数据库其他 Shard 的 SeriesIDSet 做 Diff
s.walkShards(shards, func(sh *Shard) error {
    index, err := sh.Index()
    ss.Diff(index.SeriesIDSet())  // 移除其他 Shard 也有的 Series
    return nil
})

// ss 现在只包含"孤立"的 Series（只存在于被删除的 Shard 中）
if ss.Cardinality() > 0 {
    sfile := s.seriesFile(db)
    if sfile != nil {
        // InmemIndex 需要先从内存索引移除 (错误用 errs []error 收集)
        if index.Type() == InmemIndexName {
            var errs []error
            ss.ForEach(func(id uint64) {
                skey := sfile.SeriesKey(id)
                name, tagsBuf = ParseSeriesKeyInto(skey, tagsBuf)
                keyBuf = models.AppendMakeKey(keyBuf, name, tagsBuf)
                if tmpErr := index.DropSeriesGlobal(keyBuf); tmpErr != nil {
                    errs = append(errs, tmpErr)
                }
            })
            if len(errs) != 0 {
                return errors.Join(errs...)   // store.go:1072
            }
        }

        // NoFlush 批量删除: 每个 series 删除时不立即刷盘
        const DeleteLogTrigger = 10_000       // store.go:1076
        seriesCount := ss.Cardinality()
        var deletedCount atomic.Uint64
        var partitionIDs = make(map[int]struct{}, SeriesFilePartitionN)  // store.go:1080

        ss.ForEach(func(id uint64) {
            p, err := sfile.DeleteSeriesID(id, NoFlush)   // store.go:1083 — NoFlush!
            if err != nil {
                sfile.Logger.Error("cannot delete series in shard", ...)
            } else {
                partitionIDs[p.id] = struct{}{}            // store.go:1092 — 收集涉及的 partition
                deleted := deletedCount.Add(1)
                if deleted%DeleteLogTrigger == 0 {          // store.go:1095 — 每 10_000 条打日志
                    s.Logger.Info(fmt.Sprintf("DeleteShard: %d series deleted", DeleteLogTrigger), ...)
                }
            }
        })

        // 循环结束后一次性刷盘所有涉及的 partition
        if err := sfile.FlushSegments(partitionIDs); err != nil {   // store.go:1108
            sfile.Logger.Error("error while flushing a series file segment", ...)
        }
    }
}
```

**NoFlush 批量的性能意义**: SeriesFile 按 partition 组织，每次刷盘是昂贵的磁盘 IO。
如果每个 series 删除都立即刷盘，百万级 series 会产生百万次 IO。NoFlush 模式把删除
累积在内存，最后按 partition 批量刷盘，IO 次数从 O(seriesN) 降到 O(partitionN)。

**Series 清理的必要性**: 如果不清理孤立 Series，SeriesFile 会无限增长。
SeriesFile 是所有数据库共享的全局索引，包含每个 Series 的 key（measurement + tags）。
删除不再有任何数据引用的 Series 可以回收空间。

**Diff 操作的数学含义**:
```
原始 SeriesIDSet = {1, 3, 5, 7, 9}   (被删除 Shard 的 Series)
其他 Shard A     = {1, 2, 5, 8}
其他 Shard B     = {3, 6, 9}

Diff(A): {1, 3, 5, 7, 9} - {1, 2, 5, 8} = {3, 7, 9}
Diff(B): {3, 7, 9} - {3, 6, 9} = {7}

最终孤立 Series = {7}  → 只有 Series 7 需要从 SeriesFile 中删除
```

### 3.5 步骤 4-6: Epoch 同步、关闭、删除文件

```go
// tsdb/store.go:1118-1141
// 步骤 4: Epoch 同步 — 等待正在进行的写入完成
guards, gen := epoch.StartWrite()
defer epoch.EndWrite(gen)
for _, guard := range guards {
    guard.Wait()  // 阻塞直到所有 in-flight 写入完成
}

// 步骤 5: 关闭 Shard
if err := sh.Close(); err != nil {
    return err
}

// 步骤 6: 删除文件
if err := os.RemoveAll(sh.path); err != nil {
    return err
}
return os.RemoveAll(sh.walPath)
```

**Epoch 同步的真实作用**: epoch 只登记 **series/measurement 删除** 的 guard——
`epochTracker.StartWrite` 的 guards 只来自 `e.deletes`（`tsdb/epoch_tracker.go:63-80`），
而 `e.deletes` 只由 `WaitDelete` 填充；`WaitDelete` 的生产调用方只有
`DeleteMeasurement`（`store.go:1292`）与 `DeleteSeries`（`store.go:1757`）。
`DeleteShard` **从不安装 guard**，所以这里的 `guard.Wait()` 不会因为“有普通写入”而阻塞。

shard 删除与写入的隔离实际靠三件事：
- `delete(s.shards, shardID)` 之后，新写入在 `WriteToShard` 拿到 `ErrShardNotFound`（`store.go:1871-1875`）；
- 在途写入与 `sh.Close()` 由 `Shard.mu` 串行（`shard.go:562-564` 读锁 vs `:412-415` 写锁）；
- 关闭后 `ready()` 返回 `ErrEngineClosed`（`shard.go:451-458`）。

**写 / series-delete / shard-delete 三方 wait 关系**:

| 操作 | 调用 `StartWrite` | 调用 `WaitDelete` | 会不会等普通写入 | 被谁等 |
|------|------------------|-------------------|------------------|--------|
| 普通写入 `WriteToShard` | 是（`store.go:1861+`） | 否 | — | `Shard.mu` 与 `sh.Close()` 串行 |
| `DeleteSeries` / `DeleteMeasurement` | 是 | **是**（`:1757` / `:1292`） | 是（等 `StartWrite` 之后、`EndWrite` 之前的写入） | — |
| `DeleteShard` | 是（`store.go:1119-1123`） | 否 | **否** | 仅等已存在的 series/measurement 删除 guard |
| `DeleteDatabase` | 是 | 否 | 否 | — |

`guard.Matches(points)` / 条件过滤只影响 `DeleteSeries`/`DeleteMeasurement` 对“哪些写入需要等待”的判定，
与 shard 删除无关。

### 3.6 步骤 7: 延迟清理

> **审计校准** (store.go:1007-1013): defer **只**做两件事：`delete(s.epochs, shardID)`
> 和 `delete(s.pendingShardDeletes, shardID)`。`removeIndexType` **不在** defer 里，
> 而是 INLINE 在成功路径的 else 分支 (store.go:1139)。

```go
// tsdb/store.go:1007-1013 — defer (只清理两个 map)
defer func() {
    s.mu.Lock()
    defer s.mu.Unlock()
    delete(s.epochs, shardID)                // 清除 Epoch 追踪器
    delete(s.pendingShardDeletes, shardID)   // 清除待删除标记
    // 注意: removeIndexType 不在这里 — 它在成功路径的 else 分支 (store.go:1139)
}()
```

```go
// tsdb/store.go:1132-1141 — 成功路径的 else 分支 (INLINE，非 defer)
if err := os.RemoveAll(sh.path); err != nil {
    return err
} else if err = os.RemoveAll(sh.walPath); err != nil {
    return err
} else {
    // Remove index type from the database on success
    s.databases[db].removeIndexType(sh.IndexType())   // store.go:1139 — INLINE
    return nil
}
```

**为什么 defer 只清理两个 map？** 无论 DeleteShard 成功还是失败，都需要清理
`pendingShardDeletes`（否则该 Shard 永远无法被重新创建）和 `epochs`。
而 `removeIndexType` 只在**成功删除文件后**才该执行——如果删除失败，
索引类型统计不应被移除，所以它放在成功路径的 else 分支而非 defer。

## 4. pendingShardDeletes 机制

### 4.1 防止删除与创建的竞态

```mermaid
sequenceDiagram
    participant RetSvc as Retention Service
    participant Store as TSDBStore
    participant Writer as 写入请求

    par 删除流程
        RetSvc->>Store: DeleteShard(100)
        Store->>Store: delete(s.shards, 100)
        Store->>Store: s.pendingShardDeletes[100] = true
        Note over Store: Shard 100 正在被删除...

        Writer->>Store: CreateShard("mydb", "autogen", 100)
        Store->>Store: 检查 s.shards[100] → 不存在
        Store->>Store: 检查 s.pendingShardDeletes[100] → 存在!
        Store-->>Writer: return ErrShardDeletion
        Note over Writer: 服务端返回 PartialWriteError (HTTP 400)<br>由客户端决定是否重试

        Store->>Store: os.RemoveAll(path)
        Store->>Store: delete(s.pendingShardDeletes, 100)
        Note over Store: Shard 100 删除完成

        Writer->>Store: CreateShard("mydb", "autogen", 100) (重试)
        Store->>Store: 检查 s.pendingShardDeletes[100] → 不存在
        Store->>Store: 创建新 Shard
        Store-->>Writer: return nil
    end
```

### 4.2 CreateShard 中的检查

```go
// tsdb/store.go:892-896 — CreateShard
// Shard may be undergoing a pending deletion. While the shard can be
// recreated, it must wait for the pending delete to finish.
if _, ok := s.pendingShardDeletes[shardID]; ok {
    return ErrShardDeletion
}
```

**为什么需要这个机制？**

1. **文件系统竞态**: 如果 `DeleteShard` 正在 `os.RemoveAll(path)`，而 `CreateShard` 同时在 `os.MkdirAll(path)`，
   可能导致目录结构损坏
2. **Series 清理竞态**: `DeleteShard` 正在清理孤立 Series，而新 Shard 可能正在写入相同的 Series
3. **Epoch 冲突**: 新 Shard 的 Epoch 和旧 Shard 的 Epoch 可能产生冲突

## 5. Precreator Service

### 5.1 Service 结构体

```go
// services/precreator/service.go:13 — Service
type Service struct {
    checkInterval time.Duration
    advancePeriod time.Duration

    Logger *zap.Logger

    done chan struct{}
    wg   sync.WaitGroup

    MetaClient interface {
        PrecreateShardGroups(now, cutoff time.Time) error
    }
}
```

### 5.2 配置默认值

```go
// services/precreator/config.go:11-18
const (
    DefaultCheckInterval = 10 * time.Minute   // 每 10 分钟检查一次
    DefaultAdvancePeriod = 30 * time.Minute   // 提前 30 分钟预创建
)
```

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `enabled` | `true` | 是否启用预创建服务 |
| `check-interval` | `10m` | 检查间隔 |
| `advance-period` | `30m` | 提前多久预创建 |

### 5.3 runPrecreation 主循环

```go
// services/precreator/service.go:72-86
func (s *Service) runPrecreation() {
    defer s.wg.Done()

    for {
        select {
        case <-time.After(s.checkInterval):  // 注意: 使用 time.After 而非 Ticker
            if err := s.precreate(time.Now().UTC()); err != nil {
                s.Logger.Info("Failed to precreate shards", zap.Error(err))
            }
        case <-s.done:
            s.Logger.Info("Terminating precreation service")
            return
        }
    }
}
```

> **小白解释**: `time.After` 和 `time.NewTicker` 的区别：
> - `Ticker`: 固定间隔触发，不管上一次处理是否完成
> - `time.After`: 上一次处理完成后，再等待一个间隔才触发下一次
>
> 如果 `precreate()` 耗时 5 分钟，使用 `time.After` 的实际间隔是 10+5=15 分钟；
> 而使用 `Ticker` 会在 10 分钟时到期。由于这里是单 goroutine 同步调用，**不会并发执行**，
> 到期的 tick 只会留在容量为 1 的 channel 里（多次到期被合并成一次）。

### 5.4 precreate 函数

```go
// services/precreator/service.go:89-92
func (s *Service) precreate(now time.Time) error {
    cutoff := now.Add(s.advancePeriod).UTC()  // now + 30 分钟
    return s.MetaClient.PrecreateShardGroups(now, cutoff)
}
```

### 5.5 PrecreateShardGroups 实现

```mermaid
flowchart TD
    Start["PrecreateShardGroups(from, to)"] --> Iterate["遍历所有 Database"]
    Iterate --> CheckDB["每个 Database 的每个 RP"]
    CheckDB --> Empty{"ShardGroups 为空?"}
    Empty -->|"是"| Skip1["跳过: 没有历史数据"]
    Empty -->|"否"| LastGroup["获取最后一个 ShardGroup:<br/>rp.ShardGroups[len-1]"]
    LastGroup --> Checks{"检查条件:<br/>1. !g.Deleted()<br/>2. g.EndTime.Before(to)<br/>3. g.EndTime.After(from)"}
    Checks -->|"不满足"| Skip2["跳过"]
    Checks -->|"满足"| CalcTime["nextTime = g.EndTime + 1ns"]
    CalcTime --> Exists{"ShardGroupByTimestamp<br/>已存在?"}
    Exists -->|"是"| Skip3["跳过: 幂等保护"]
    Exists -->|"否"| Create["createShardGroup<br/>(db, rp, nextTime)"]
    Create --> Commit["commit(data)<br/>持久化元数据"]

    style Start fill:#e8f5e9
    style Create fill:#fff9c4
    style Commit fill:#e3f2fd
```

```go
// services/meta/client.go:766-816 — PrecreateShardGroups
func (c *Client) PrecreateShardGroups(from, to time.Time) error {
    c.mu.Lock()
    defer c.mu.Unlock()
    data := c.cacheData.Clone()
    var changed bool

    for _, di := range data.Databases {
        for _, rp := range di.RetentionPolicies {
            if len(rp.ShardGroups) == 0 {
                continue  // 没有历史数据，跳过
            }
            // 获取最后一个（最新的）ShardGroup
            g := rp.ShardGroups[len(rp.ShardGroups)-1]

            // 条件: 未删除 && 结束时间在 cutoff 之前 && 结束时间在 from 之后
            if !g.Deleted() && g.EndTime.Before(to) && g.EndTime.After(from) {
                // 新 ShardGroup 从旧 ShardGroup 结束时间 + 1ns 开始
                nextShardGroupTime := g.EndTime.Add(1 * time.Nanosecond)

                // 幂等检查: 如果已存在则跳过
                if sg, _ := data.ShardGroupByTimestamp(di.Name, rp.Name, nextShardGroupTime); sg != nil {
                    continue
                }

                // 创建新 ShardGroup
                newGroup, err := createShardGroup(data, di.Name, rp.Name, nextShardGroupTime)
                if err != nil {
                    continue
                }
                changed = true
            }
        }
    }

    if changed {
        return c.commit(data)
    }
    return nil
}
```

**时间窗口逻辑**:

```
from (now)                    to (now + 30min)
  |                              |
  |    g.EndTime                 |
  |      |                       |
  |      +---+                   |
  |      |   | ← ShardGroup     |
  |      +---+                   |
  |                              |
  |  如果 g.EndTime 落在这个区间内  |
  |  则预创建下一个 ShardGroup      |
```

**没有“追赶”机制（重要）**:
- 每次每 RP 最多只建 1 组：`g := rp.ShardGroups[len-1]`，条件满足时只创建一个后继组。
- `g.EndTime.After(from)` 中 `from = now`，要求最后一组的结束时间在未来。服务停机
  3 天、`ShardGroupDuration = 1d` 时，最后一组早已结束，`After(now)` 恒为 false，
  **本 RP 会一直跳过**，不会按 `check_interval` 逐步补齐。
- 停机期间的组只能由写入路径 `MapShards → CreateShardGroup` 按需创建，首笔写入会付建组延迟。
- 创建失败只 `Info` 记录并 `continue`（`client.go:796-799`），不会重试。

> 这里的 `Clone → mutate → commit` 发布语义见 §2.8。

## 6. ShardGroup 生命周期

### 6.1 完整生命周期状态机

```mermaid
stateDiagram-v2
    [*] --> Created: CreateShardGroup(timestamp)
    Created --> Active: 数据写入开始
    Active --> Expired: EndTime + RP.Duration < now
    Active --> SoftDeleted: DeleteShardGroup 设置 DeletedAt
    Expired --> SoftDeleted: DeleteShardGroup 设置 DeletedAt
    Active --> SoftDeleted: DropShard 删除组内最后一个 shard
    SoftDeleted --> Created: 旧时间戳再次写入 / CreateShardGroup 新建组
    SoftDeleted --> Pruned: 组内无 shard 且 DeletedAt 超过 14 天
    Pruned --> [*]

    note right of Created
        StartTime = timestamp.Truncate(ShardGroupDuration)
        EndTime = StartTime + ShardGroupDuration
        若 EndTime > MaxNanoTime 则钳到 MaxNanoTime+1
    end note

    note right of Active
        写路径按需命中（ShardGroupAt → Contains）
        OSS 每个组固定 1 个 Shard
    end note

    note right of Expired
        已进入删除候选
        但写路径只按 RP Duration 过滤，
        组本身不感知 Expired
    end note

    note right of SoftDeleted
        DeletedAt ≠ zero
        物理文件可能仍在磁盘上
        DropShard 删掉组内最后一个 shard 时
        也会副作用置 DeletedAt
    end note

    note right of Pruned
        两个条件缺一不可:
        组内无 shard 且 DeletedAt 早于 now-14d
    end note
```

**状态说明**:
- `Expired` = “已进入删除候选”。如果组未被软删/截断，写路径仍可能命中；正常 HTTP 写入会
  先在 `MapShards` 被 `now - RP.Duration` 过滤掉（`coordinator/points_writer.go:309-320`）。
  例外是绕过写入路径的场景（importer、直接调用 meta）。
- `SoftDeleted` = `DeletedAt != zero`；物理文件可能仍在。`DropShard` 删除组内最后一个
  `ShardInfo` 时会副作用置 `DeletedAt`（`services/meta/data.go:313-321`）。
- `Pruned` = `len(Shards)==0` **且** `DeletedAt` 早于 `now-14d`（`data.go:476-492`）。
- `SoftDeleted → Created` 是回边：同一时间段再次写入会由 `CreateShardGroup` 新建一个组
  （`ShardGroupByTimestamp` 会跳过已删除组）。

**Truncate 分支（Enterprise/运维路径）**:
- 唯一业务写入点是 `Data.TruncateShardGroups`（`services/meta/data.go:835-858`）；
  `services/meta/client.go:670` 只是 `Clone → Truncate → commit` 的封装。
- 只被接口声明与 mock 引用（`coordinator/meta_client.go:30`、`internal/meta_client.go:47`、
  `coordinator/meta_client_test.go:35`），**OSS 1.x 无生产调用方**。
- `EndTime` 字段本身不变，变的是路由判据：`ShardGroupByTimestamp` 用
  `!sgi.Truncated() || timestamp.Before(sgi.TruncatedAt)`（`data.go:1263-1271`）；
  而写路径 `ShardGroupAt` 只用 `Contains`（原始 `StartTime/EndTime`），**不看** `TruncatedAt`。
- `Compacting` 是 TSM 引擎状态，不属于 ShardGroup 生命周期，见 05 号文档第 10 章。

### 6.2 ShardGroupDuration 自动计算

```go
// services/meta/data.go:1376 — shardGroupDuration
func shardGroupDuration(d time.Duration) time.Duration {
    if d >= 180*24*time.Hour || d == 0 { // 6 months or 0 (永久保留)
        return 7 * 24 * time.Hour   // 7 天
    } else if d >= 2*24*time.Hour { // 2 天
        return 1 * 24 * time.Hour   // 1 天
    }
    return 1 * time.Hour            // 1 小时
}
```

| RP Duration | ShardGroupDuration | 说明 |
|-------------|-------------------|------|
| 永久 (`0`) | 7 天 | 长期保留，大 ShardGroup 减少文件数 |
| >= 6 个月 | 7 天 | 同上 |
| >= 2 天 | 1 天 | 中等保留期 |
| < 2 天 | 1 小时 | 短保留期，小 ShardGroup 便于快速清理 |

### 6.3 ShardGroupInfo 结构

```go
// services/meta/data.go:1403 — ShardGroupInfo
type ShardGroupInfo struct {
    ID          uint64
    StartTime   time.Time
    EndTime     time.Time
    DeletedAt   time.Time      // 零值 = 未删除，非零 = 软删除时间
    Shards      []ShardInfo
    TruncatedAt time.Time      // 非零表示该组在 TruncatedAt 之后不再被路由（Enterprise/运维工具路径；OSS 无生产调用方）
}

// Deleted() 判断是否已软删除
func (sgi *ShardGroupInfo) Deleted() bool {
    return !sgi.DeletedAt.IsZero()
}

// Contains() 判断时间点是否在 ShardGroup 范围内
func (sgi *ShardGroupInfo) Contains(t time.Time) bool {
    return !t.Before(sgi.StartTime) && t.Before(sgi.EndTime)
}
```

## 7. Lease 机制

### 7.1 Lease 结构体

```go
// services/meta/data.go:1799 — Lease
type Lease struct {
    Name       string    `json:"name"`       // 租约名称
    Expiration time.Time `json:"expiration"` // 过期时间
    Owner      uint64    `json:"owner"`      // 持有者节点 ID
}

// services/meta/config.go:12
DefaultLeaseDuration = 60 * time.Second  // 默认 60 秒
```

### 7.2 单机模式 vs 集群模式

```mermaid
flowchart TD
    subgraph "单机模式 (client.go:128-133)"
        S1["AcquireLease(name)"] --> S2["创建 Lease"]
        S2 --> S3["Expiration = now + 60s"]
        S3 --> S4["return &lease, nil"]
        Note1["永远成功<br/>不需要真正的锁竞争"]
    end

    subgraph "集群模式 (Leases.Acquire, data.go:1824-1847；OSS 无调用方)"
        C1["Leases.Acquire(name, nodeID)"] --> C2{"租约存在?"}
        C2 -->|"不存在"| C3["创建新租约<br/>Owner = nodeID<br/>Expiration = now + 60s"]
        C2 -->|"存在"| C4{"已过期<br/>OR<br/>同一 Owner?"}
        C4 -->|"是"| C5["续期/接管<br/>Expiration = now + 60s"]
        C4 -->|"否"| C6["return error<br/>'another node has the lease'"]
    end

    style S4 fill:#c8e6c9
    style C3 fill:#c8e6c9
    style C5 fill:#c8e6c9
    style C6 fill:#ffcdd2
```

### 7.3 单机模式实现

```go
// services/meta/client.go:127-133 — AcquireLease (单机)
func (c *Client) AcquireLease(name string) (*Lease, error) {
    l := Lease{
        Name:       name,
        Expiration: time.Now().Add(DefaultLeaseDuration),  // 60s
    }
    return &l, nil  // 永远成功
}
```

> **小白解释**: 单机模式下，`AcquireLease` 就像一个永远说"好"的门卫。
> 你问他要钥匙，他永远给你，因为整个公司只有你一个人。

### 7.4 集群模式实现

```go
// services/meta/data.go:1824-1847 — Acquire (集群)
func (leases *Leases) Acquire(name string, nodeID uint64) (*Lease, error) {
    leases.mu.Lock()
    defer leases.mu.Unlock()

    l := leases.m[name]
    if l != nil {
        // 租约已存在
        if time.Now().After(l.Expiration) || l.Owner == nodeID {
            // 已过期 → 接管; 同一 Owner → 续期
            l.Expiration = time.Now().Add(leases.d)
            l.Owner = nodeID
            return l, nil
        }
        // 其他节点持有且未过期 → 拒绝
        return l, errors.New("another node has the lease")
    }

    // 租约不存在 → 创建新租约
    l = &Lease{
        Name:       name,
        Expiration: time.Now().Add(leases.d),
        Owner:      nodeID,
    }
    leases.m[name] = l
    return l, nil
}
```

> **OSS 可达性**: 上面的 `Leases.Acquire` 需要显式 `nodeID`，只有 Enterprise 的 meta 层会调用；
> `Leases`/`NewLeases`（`data.go:1806-1821`）在 OSS 全树**没有任何调用方**（连测试都没有）。
> OSS 的 `Client.AcquireLease(name)`（`client.go:127-133`）是另一条路径，恒成功。
> 因此本节的 leader/follower 互斥在 OSS 中不存在。

### 7.5 CQ Service 如何使用租约

`backgroundLoop` (service.go:216-245) 有**两个**触发分支，不仅靠 timer，还监听
`s.RunCh` 这个按需请求通道——外部 `Run(name, t)` 调用 (service.go:210) 会向 `RunCh`
发送 `*RunRequest`，让 CQ 立即跑一轮而不必等下一个 `RunInterval`。

```go
// services/continuous_querier/service.go:216 — backgroundLoop
func (s *Service) backgroundLoop() {
    leaseName := "continuous_querier"
    t := time.NewTimer(s.RunInterval)
    defer t.Stop()
    defer s.wg.Done()
    for {
        select {
        case <-s.stop:
            s.Logger.Info("Terminating continuous query service")
            return
        // ↓↓↓ 分支 A: 按需请求 (service.go:226-233)
        case req := <-s.RunCh:
            if !s.hasContinuousQueries() {
                continue   // 注意: 不 Reset timer，等下一个 tick 或下一次 RunCh
            }
            if _, err := s.MetaClient.AcquireLease(leaseName); err == nil {
                s.Logger.Info("Running continuous queries by request", zap.Time("at", req.Now))
                s.runContinuousQueries(req)
            }
        // ↓↓↓ 分支 B: 定时触发 (service.go:234-243)
        case <-t.C:
            if !s.hasContinuousQueries() {
                t.Reset(s.RunInterval)
                continue
            }
            if _, err := s.MetaClient.AcquireLease(leaseName); err == nil {
                s.runContinuousQueries(&RunRequest{Now: time.Now()})
            }
            t.Reset(s.RunInterval)
        }
    }
}
```

**两个分支的关键差异**:
- **分支 B (timer)**: 每轮结束后 `t.Reset(s.RunInterval)`，保证固定周期。
- **分支 A (RunCh)**: **不 Reset timer**。这意味着 `RunCh` 触发后，下一个 `t.C` 仍按
  原计划到达——`RunCh` 是"额外加跑一轮"，不是"提前消费下一个 tick"。如果 `RunCh`
  触发时恰好 `t.C` 已就绪，`select` 随机选一个，但两个分支都会跑 `runContinuousQueries`，
  所以不会丢执行，只是可能多跑一轮 (CQ 内部用 `lastRuns` 去重，重复触发会被跳过)。
- **日志区分**: 分支 A 打 `"Running continuous queries by request"` + `req.Now`，
  分支 B 无额外日志。运维可通过日志区分"手动触发"与"定时触发"。
- **租约前置（仅形式）**: 两个分支都会先调用 `AcquireLease("continuous_querier")`。OSS 的
  `Client.AcquireLease`（`services/meta/client.go:127-133`）**无条件返回成功**，既不检查也不写入
  共享状态，因此**不提供任何互斥**；`Leases.Acquire` 与 `NewLeases`（`services/meta/data.go:1806-1847`）
  在 OSS 全树无生产调用方。多进程共连同一 meta 时 CQ 可能重复执行；leader/follower 语义只存在于
  Enterprise 与测试 mock（`service_test.go`）。租约也没有显式 release，只有 60s 过期/续期
  （`DefaultLeaseDuration`，`services/meta/config.go:12`）——该值在 OSS 中同样不起作用。

```mermaid
flowchart TD
    A["backgroundLoop select"] --> B{"哪个 case 就绪?"}
    B -->|"s.stop 关闭"| Z["return"]
    B -->|"req := <-s.RunCh<br/>(按需请求)"| C["hasContinuousQueries()?"]
    B -->|"<-t.C<br/>(定时 tick)"| D["hasContinuousQueries()?"]
    C -->|"否"| A
    C -->|"是"| E["AcquireLease(continuous_querier)"]
    D -->|"否"| F["t.Reset(RunInterval)"]
    D -->|"是"| E
    F --> A
    E -->|"成功 (OSS 恒成立)"| G["runContinuousQueries(req)"]
    E -->|"失败 (OSS 不会发生)"| H["跳过本轮"]
    G --> I{"是 RunCh 分支?"}
    I -->|"是 (分支 A)"| A
    I -->|"否 (分支 B)"| J["t.Reset(RunInterval)"]
    J --> A
    H --> A
```

> **具体案例**: 手动 `Run()` 在非窗口边界只推进 `lastRuns`
>
> 假设 `RunInterval = 10s`，`GROUP BY time(1m)`，无 RESAMPLE，当前时间 00:00:03。
>
> 1. 用户调用 `Service.Run("mydb", "myCQ", time.Now())`。OSS 中该方法**只有测试/内嵌调用方**，
>    `influxd inspect` 没有 CQ 子命令。
> 2. `Run()` 删除 `s.lastRuns["mydb\x1fmyCQ"]`，然后向**无缓冲**的 `RunCh` 发送
>    `&RunRequest{Now: 00:00:03}`（`service.go:196-210`）。若服务未 `Open()` 或已 `Close()`，
>    这里会永久阻塞且持有 `s.mu`。
> 3. `backgroundLoop` 选中 `RunCh` 分支（分支 A）→ `hasContinuousQueries()` →
>    `AcquireLease`（OSS 恒成功）→ `runContinuousQueries(req)`。
> 4. `HasRun=false` → `shouldRunContinuousQuery` 返回 `(true, now=00:00:03)`。随后
>    `cq.LastRun = truncate(00:00:03) = 00:00:00` 并写入 `lastRuns`（`service.go:338-339`）。
> 5. 窗口计算：`startTime = truncate(00:00:03 + 1m - 1m - 1ns) = 00:00:00`；
>    `endTime = truncate(00:00:03 + 1m - 1m) = 00:00:00`。`endTime.After(startTime) == false`
>    → **空跑返回 `(false, nil)`，不执行查询、不写任何点**。
> 6. 分支 A 不 Reset timer，00:00:10 的 tick 仍会到来；但 `nextRun = lastRun + 1m = 00:01:00 > now`，
>    跳过。直到 00:01:00 的 tick 才执行 `[00:00, 00:01)`。
>
> **结论**：清除 `lastRuns` 只保证“下一次调度从最老未算窗口重算”，不保证在任意时刻立即产生一次写入。
> `Run()` 的主要可观测效果是把 `lastRuns`/`HasRun` 推进到 `now` 所在的桶边界。若希望 `Run()` 重算历史
> 窗口，必须配置 `RESAMPLE FOR > interval`（此时 `startTime` 向前多回溯 `FOR`，`endTime` 仍截断到当前
> 桶边界，因此仍不包含当前未完成的桶）。

**写回路径与点数统计**: CQ 的 `SELECT ... INTO` 走与用户写入完全相同的
`PointsWriter → MapShards → Store.WriteToShard → Shard.WritePoints → Engine.WritePoints(Cache+WAL)`
路径。目标库/RP 由 `query.Executor` 的 `RewriteStatement + NormalizeStatement` 解析（默认库 =
CQ 所在库，默认 RP 由 `setIntoRP` 填入），详见 05 号文档 §1.2 步骤 ⑨.5。写入点数来自 Executor
合成的 `{time, written}` 结果行（`coordinator/statement_executor.go:611-626`），CQ 从
`res.Series[0].Values[0][1]` 取值并写入 `cq_query`（05 号文档 §9.2）。

**为什么 CQ 需要租约而 Retention/Precreator 不需要？**

| 服务 | 操作性质 | 幂等性 | 是否需要租约 |
|------|----------|--------|-------------|
| CQ | 计算 + 写入 | 非幂等（重复执行产生重复数据） | 名义上是（OSS 恒成功、无互斥） |
| Retention | 删除本地文件 | 幂等（删两次等于删一次） | 否 |
| Precreator | 创建元数据 | 幂等（ShardGroupByTimestamp 检查） | 否 |

## 8. 具体案例

### 8.1 案例一: Retention 删除过期数据

> **场景**: RP Duration = 7 天，ShardGroupDuration = 1 天，数据在 t=0 时写入。
> OSS 每个 ShardGroup 只有 1 个 Shard，本案例按单 shard 推演。

```
时间线:
t=0        写入数据 → ShardGroup A (StartTime=Day0, EndTime=Day1), Shard 1
t=1d       新数据写入 ShardGroup B (StartTime=Day1, EndTime=Day2), Shard 2
t=8d+1s    Retention Service 的 ticker 触发
```

**Retention Service 执行过程**:

```
Step 1: MetaClient.Databases() → [{Name: "mydb", RPs: [{Name: "autogen", Duration: 7d}]}]

Step 2: 遍历 mydb.autogen:
  - r.DeletedShardGroups() → [] (没有已删除的)
  - r.ExpiredShardGroups(now=8d+1s):
    - ShardGroup A: EndTime=Day1, EndTime + Duration = Day1 + 7d = Day8
      Day8.Before(8d+1s) = true → 过期!
    - ShardGroup B: EndTime=Day2, EndTime + Duration = Day9
      Day9.Before(8d+1s) = false → 不过期

  - MetaClient.DeleteShardGroup("mydb", "autogen", shardGroupA.ID)
    → 设置 ShardGroupA.DeletedAt = 8d+1s (软删除)
  - 收集 ShardGroup A 的 Shard ID: {1}

Step 3: TSDBStore.ShardIDs() → [1, 2]
  - Shard 1 在 deletedShardIDs 中:
      SetShardNewReadersBlocked(1, true)   // 阻止新 reader
      ShardInUse(1) → false                 // 没有旧查询持有
      DeleteShard(1)                        // 先从 s.shards 摘除，再删文件
      DropShardMetaRef(1, owners=[])        // OSS: meta.Client.DropShard(1) 移除 ShardInfo
  - Shard 2 不在 deletedShardIDs 中 → 跳过

  注意: 组内最后一个 ShardInfo 被移除时，DropShard 会在 DeletedAt 仍为零值的情况下
  副作用置 DeletedAt（data.go:313-321；本案例里 DeleteShardGroup 已经设过，不会被覆盖）。

Step 4: MetaClient.PruneShardGroups()
  - ShardGroupA: DeletedAt = 8d+1s, len(Shards) == 0
  - expiration = now - 14d = 8d+1s - 14d，expiration.After(DeletedAt) = false
  - → 不修剪（第二道门未满足）
```

> **为什么不是 7d？** `ExpiredShardGroups` 的判断是
> `EndTime.Add(rpi.Duration).Before(t)`，即 `EndTime + RP_Duration < now`。
> ShardGroup A 的 EndTime = Day1，所以条件是 `Day8 < now`：只有 `t > Day8` 才过期，
> `t = Day8` 时 `Before` 返回 false。

**14 天后 (t=22d+1s)**:

```
Step 4: MetaClient.PruneShardGroups()
  - DeletedAt = 8d+1s, expiration = now - 14d = 8d+1s
  - expiration.After(DeletedAt) = false → 仍不修剪
  - 下一个 tick (t=22d+1s+30min): expiration = 8d+1s+30min > 8d+1s → 修剪
  - ShardGroup A 的元数据从 MetaClient 中移除
```

> 若 Step 3 漏掉 `DropShardMetaRef`，`len(Shards)` 永远为 1，第二道门虽然满足、
> 第一道门却永远不满足——14 天后也不会修剪。

### 8.2 案例二: Precreator 预创建 ShardGroup

> **场景**: checkInterval=10m, advancePeriod=30m, ShardGroupDuration=1d

```
时间线:
t=0:00    当前 ShardGroup: StartTime=Day0, EndTime=Day1
t=0:00    Precreator ticker 触发
```

**t=0:00 时的执行过程**:

```
precreate(now=0:00):
  cutoff = now + 30min = 0:30

PrecreateShardGroups(from=0:00, to=0:30):
  遍历 mydb.autogen:
    last ShardGroup = {EndTime: Day1}
    检查: !Deleted() = true ✓
    检查: EndTime.Before(0:30) = Day1.Before(0:30) = false ✗
    → 跳过（ShardGroup 还没到要结束的时候）
```

**t=Day0+23:30 时的执行过程**:

```
precreate(now=Day0+23:30):
  cutoff = now + 30min = Day0+24:00 = Day1

PrecreateShardGroups(from=Day0+23:30, to=Day1):
  遍历 mydb.autogen:
    last ShardGroup = {EndTime: Day1}
    检查: !Deleted() = true ✓
    检查: EndTime.Before(Day1) = Day1.Before(Day1) = false ✗
    → 跳过（EndTime 恰好等于 cutoff，Before 返回 false）
```

**t=Day1-10min (23:50) 时的执行过程**:

```
precreate(now=Day1-10min):
  cutoff = now + 30min = Day1 + 20min

PrecreateShardGroups(from=Day1-10min, to=Day1+20min):
  遍历 mydb.autogen:
    last ShardGroup = {EndTime: Day1}
    检查: !Deleted() = true ✓
    检查: EndTime.Before(Day1+20min) = true ✓
    检查: EndTime.After(Day1-10min) = true ✓
    → 条件全部满足!

    nextTime = Day1 + 1ns
    ShardGroupByTimestamp(mydb, autogen, Day1+1ns) → nil (不存在)

    createShardGroup(mydb, autogen, Day1+1ns)
    → 创建新 ShardGroup: StartTime=Day1, EndTime=Day2
    → commit(data)

日志: "New shard group successfully precreated"
```

**预创建的价值**:

```
没有预创建:
  t=Day1:00:00.001  写入请求到达
  t=Day1:00:00.002  发现没有 ShardGroup → 触发创建
  t=Day1:00:00.050  Raft 共识完成
  t=Day1:00:00.051  写入成功
  延迟: 50ms (受 Raft 共识影响)

有预创建:
  t=Day0+23:50      Precreator 预创建完成
  t=Day1:00:00.001  写入请求到达
  t=Day1:00:00.002  ShardGroup 已存在 → 直接写入
  t=Day1:00:00.003  写入成功
  延迟: 2ms (无需等待 Raft)
```

### 8.3 案例三: pendingShardDeletes 竞态保护

> **场景**: Retention Service 删除 Shard 100 的同时，写入请求尝试创建 Shard 100

```
时间线:
t=0ms    Retention Service: DeleteShard(100)
         → s.mu.Lock()
         → delete(s.shards, 100)
         → s.pendingShardDeletes[100] = struct{}{}
         → s.mu.Unlock()
         → 开始清理 Series（耗时操作）

t=5ms    写入请求: CreateShard("mydb", "autogen", 100)
         → s.mu.Lock()
         → s.shards[100] 不存在
         → s.pendingShardDeletes[100] 存在!
         → s.mu.Unlock()
         → return ErrShardDeletion
         → 服务端转成 PartialWriteError (HTTP 400) 上抛，不做退避重试

t=100ms  DeleteShard(100) 完成
         → os.RemoveAll(path)
         → defer: delete(s.pendingShardDeletes, 100)

t=110ms  写入请求重试: CreateShard("mydb", "autogen", 100)
         → s.pendingShardDeletes[100] 不存在
         → 创建新 Shard 100
         → 成功
```

## 9. 架构设计意图

### 9.1 为什么 Retention 使用 Ticker 而 Precreator 使用 time.After

```mermaid
flowchart LR
    subgraph "Ticker (Retention)"
        T1["t=0: 触发 + 处理 5min"] --> T2["t=30: 触发 + 处理 5min"]
        T2 --> T3["t=60: 触发 + 处理 5min"]
        NoteT["固定节拍，channel 容量 1<br/>处理超时会丢 tick 或立即补跑"]
    end

    subgraph "time.After (Precreator)"
        A1["t=0: 触发 + 处理 2min"] --> A2["t=12: 触发 + 处理 2min"]
        A2 --> A3["t=24: 触发 + 处理 2min"]
        NoteA["完成后再计时<br/>周期漂移，但不会堆积"]
    end
```

**设计理由**:
- 两者都是**单 goroutine 同步调用**（`run()` / `runPrecreation()`），`DeletionCheck` 与
  `precreate` 都不会与自己的下一次调用并发执行——不存在“并发执行”问题。
- **Retention 用 Ticker**：固定节拍。`time.Ticker` 的 channel 容量为 1，如果一次
  `DeletionCheck` 超过 30 分钟，期间到期的多个 tick 会被合并成一次（丢 tick），恢复后立即补跑一次。
- **Precreator 用 time.After**：每次 `select` 都新建 Timer，计时从“处理完成”开始，
  因此实际周期是 `checkInterval + 处理耗时`，会漂移但不会堆积。
- 两种驱动方式都无法避免彼此的写锁竞争（`c.mu.Lock()`），选型只是历史与节拍需求，不是并发保护。

### 9.2 为什么软删除后还要等 14 天

```mermaid
sequenceDiagram
    participant Node1 as 节点 A
    participant Meta as MetaClient (共享)
    participant Node2 as 节点 B

    Note over Node1,Node2: 场景: 集群中的两步删除

    Node1->>Meta: DeleteShardGroup(ID=42)
    Note over Meta: DeletedAt = Day1<br/>元数据标记为软删除

    Node1->>Node1: DeleteShard(本地) → 成功
    Note over Node1: 节点 A 完成物理删除

    Note over Node2: 节点 B 可能因为网络延迟<br/>或负载过高，延迟处理

    Node2->>Node2: DeleteShard(本地) → 成功 (Day3)
    Note over Node2: 节点 B 延迟 2 天完成

    Note over Meta: 如果 PruneShardGroups 在 Day2 执行:<br/>元数据被移除，但节点 B 还没删完<br/>节点 B 的 Retention Service<br/>下次检查时找不到 ShardGroup 元数据<br/>→ Shard 文件永远留在磁盘上!

    Note over Meta: 两道门: ① len(Shards)==0<br/>(所有节点的物理删除与 meta 引用清理都已完成)<br/>② DeletedAt 超过 14 天（保留墓碑给运维排查）
```

> 14 天不是“给慢节点的宽限期”。第一道门（`len(Shards)==0`）已经保证每个节点的
> `DeleteShard` + `DropShardMetaRef` 都完成；第二道门是在清空后继续保留墓碑，
> 给运维留出排查窗口（见 §2.6 的两个条件）。

### 9.3 为什么 DeleteShard 要做 Series Diff

```mermaid
flowchart TD
    subgraph "不做 Diff 的问题"
        S1["Shard 100 有 Series: {A, B, C}"] --> S2["直接删除 Series A, B, C"]
        S2 --> S3["但 Series A 也存在于 Shard 200 中!"]
        S3 --> S4["Series A 被误删<br/>Shard 200 的查询结果缺少 Series A"]
    end

    subgraph "做 Diff 的正确流程"
        D1["Shard 100 有 Series: {A, B, C}"] --> D2["Shard 200 有 Series: {A, D}"]
        D2 --> D3["Diff: {A,B,C} - {A,D} = {B,C}"]
        D3 --> D4["只删除 Series B, C<br/>Series A 被保留（Shard 200 还在用）"]
    end

    style S4 fill:#ffcdd2
    style D4 fill:#c8e6c9
```

### 9.4 为什么 Precreator 用 g.EndTime + 1ns 定位下一组

```
ShardGroup A: [StartTime=Day0, EndTime=Day1)
ShardGroup B: [StartTime=Day1, EndTime=Day2)

为什么是 +1ns 而不是 =EndTime?
→ ShardGroup 的 Contains() 判断是: !t.Before(StartTime) && t.Before(EndTime)
→ 即 StartTime <= t < EndTime (左闭右开区间)
→ 传 A.EndTime 时，Contains(A) 已经为 false；但 CreateShardGroup 还会跑
  区间收缩循环，用 !timestamp.Before(endI) 判断“时间戳不早于旧组结束”，
  +1ns 让定位时间戳严格晚于旧组 EndTime，明确落在下一段半开区间，
  不会被收缩逻辑判回旧区间。
新 ShardGroup 的 `StartTime` 仍由 `timestamp.Truncate(ShardGroupDuration)` 计算，
通常等于旧组的 `EndTime`，不是 `EndTime + 1ns`。
```

## 10. 架构收益

| 维度 | 收益 | 说明 |
|------|------|------|
| **存储管理** | 自动过期清理 | 无需人工干预，数据按 RP Duration 自动淘汰 |
| **写入性能** | 预创建避免写入延迟 | ShardGroup 提前创建，写入时无需等待 Raft 共识 |
| **数据安全** | 两阶段删除 | 软删除 → 物理删除；Prune 需 `len(Shards)==0` 且墓碑满 14 天 |
| **Series 清理** | Diff 精确删除 | 只删除孤立 Series，不影响其他 Shard 的查询 |
| **并发安全** | Epoch 同步 | 确保删除前所有 in-flight 写入完成 |
| **竞态保护** | pendingShardDeletes | 防止删除和创建的竞态条件 |
| **幂等性** | 所有操作可重试 | 失败后下一个周期自动重试，无需人工干预 |
| **可测试性** | 匿名接口依赖注入 | MetaClient 和 TSDBStore 可以轻松 mock |
| **单机简化** | Lease 永远成功 | 单机模式下 AcquireLease 直接返回成功、无互斥（也不是真正的分布式锁） |
| **资源回收** | PruneShardGroups | 14 天后清理元数据，防止元数据无限增长 |

## 11. 潜在隐患与瓶颈

### 11.1 Retention 检查间隔导致的清理延迟

**问题**: 默认 30 分钟的检查间隔意味着过期数据最多可能多保留 30 分钟。
对于 Duration 很短的 RP（如 1 小时），这可能导致存储空间使用超出预期。

**影响**: 存储空间临时超出预期，但不会影响数据正确性。

**缓解**: 可以调小 `check-interval`，但会增加 CPU 和 MetaClient 的负载。

### 11.2 Precreator 的 time.After 导致间隔不稳定

**问题**: `time.After` 在 select 中每次循环都会创建新的 Timer，
如果 `precreate()` 耗时较长，实际间隔会大于 `checkInterval`。

```go
// 每次循环都创建新的 Timer，不会重用旧的
case <-time.After(s.checkInterval):
    s.precreate(time.Now().UTC())
```

**影响**: 在高负载场景下，预创建可能延迟，导致写入时临时创建 ShardGroup。

**对比**: `time.NewTicker` 保持固定节拍（channel 容量 1，处理超时会丢 tick 或立即补跑）；
两者都是单 goroutine 同步调用，都不会并发执行，只是周期语义不同。

### 11.3 DeleteShard 的 Series Diff 在高基数下性能差

**问题**: `SeriesIDSet.Diff()` 需要遍历所有同数据库其他 Shard 的 SeriesIDSet。
当 Series 基数很高（百万级）且 Shard 数量很多时，Diff 操作可能耗时较长。

**影响**: DeleteShard 变慢，`pendingShardDeletes` 持续时间变长，
CreateShard 可能频繁返回 `ErrShardDeletion`。

**缓解**: SeriesIDSet 使用 Roaring Bitmap 实现，Diff 操作本身是高效的位运算。

### 11.4 DeleteShard 可能被谁阻塞

**问题**: `DeleteShard` 在 `epoch.StartWrite()` 之后会等待所有已安装的 guard；
但 shard 删除本身不装 guard，所以阻塞来源不是普通写入，而是：

- 同 shard 上在途的 `DeleteSeries` / `DeleteMeasurement`（罕见）；
- `sh.Close()` 等待 `Shard.mu` 上的在途写入；
- 高基数时 `SeriesIDSet.Diff()` 与 `FlushSegments` 的耗时。

**影响**: `DeleteShard` 完成前，`pendingShardDeletes` 持续时间变长；
同一 shard 的重新创建会更久地返回 `ErrShardDeletion`。
普通写入不会让删除长时间阻塞，因为新写入在 `s.shards` 摘除后立即拿到 `ErrShardNotFound`。

### 11.5 PruneShardGroups 的 14 天缓冲期假设

**问题**: `PruneShardGroups` 有两个条件：`len(Shards)==0` 且 `DeletedAt` 早于 `now-14d`。
其中 `len(Shards)` 只有在所有节点都完成 `DeleteShard` + `DropShardMetaRef` 后才会归零。
因此 14 天不是“慢节点宽限期”，真正的风险是**第一道门永远不满足**：某个节点的
`DeleteShard` 中途失败会留下孤儿目录，下一轮 phantom 分支把 `ShardInfo` 删掉后
`len(Shards)` 归零，元数据会按 14 天规则正常修剪，但磁盘目录已经无人清理
（见 §3.2 的 4 步失败链）。

**影响**: 元数据被修剪后，孤儿目录只占磁盘、无法再被自动清理。

**缓解**: 监控 `data/<db>/<rp>/<id>` 与 `ShardIDs()` 的差集；必要时人工删除。

### 11.6 全局锁竞争

**问题**: `PruneShardGroups` 和 `PrecreateShardGroups` 都使用 `c.mu.Lock()`，
如果 Retention Service 和 Precreator Service 同时触发，会产生锁竞争。

**影响**: MetaClient 操作延迟增加。

**缓解**: 两个服务的检查间隔不同（30min vs 10min），同时触发的概率较低。

### 11.7 软删除期间的查询行为

**问题**: ShardGroup 被软删除后（DeletedAt 非零），`ExpiredShardGroups` 会跳过它（`Deleted()` 返回 true），
但 `ShardGroupByTimestamp` 也可能跳过已删除的 ShardGroup。

```go
// data.go:1263
if sgi.Contains(timestamp) && !sgi.Deleted() && ... {
    return &rpi.ShardGroups[i]
}
```

**影响**: 如果查询时间范围恰好落在已软删除但未物理删除的 ShardGroup 内，
查询可能返回不完整的结果。

**设计选择**: 这是有意为之 — 软删除表示数据已被标记为过期，不应再被查询到。

### 11.8 Precreator 预创建失败只记录 Info

Retention 删除失败在当前源码中多处使用 `Error`，phantom shard 使用 `Warn`，
不能再概括为“Retention 删除失败日志级别过低”。更准确的低可见性风险在
Precreator：预创建失败只记录 `Info`，写入仍可在后续路径按需创建 shard group。

```go
// services/precreator/service.go:79 — precreate 失败路径
s.Logger.Info("Failed to precreate shards", zap.Error(err))
```

> **审计校准** (precreator/service.go:79): 日志消息是 `"Failed to precreate shards"`，
> **不是** `"Failed to precreate shard groups"`。

**影响**: 预创建失败不会立刻造成数据丢失，但会把创建成本推迟到写入路径，
高写入压力下可能增加延迟尖峰。

**没有追赶机制**: 预创建只覆盖 `(now, now+advancePeriod]` 内的下一组。
`g.EndTime.After(from)`（`from = now`）一旦不满足，本 RP 就永久跳过，直到写入路径
`MapShards → CreateShardGroup` 建出新组。每次每 RP 最多建 1 组，失败只记 `Info`。
停机 3 天 + `ShardGroupDuration = 1d` → 预创建触发 0 次，首笔写入要付建组延迟。

### 11.9 Close 会等待当前检查结束

**问题**: `Close()` 只 `close(done)` + `wg.Wait()`；`run()`/`runPrecreation()` 要等
`DeletionCheck`/`precreate` 返回后才会重新进入 `select` 看到 `done`。高基数 shard 的
`SeriesIDSet.Diff()`、`FlushSegments` 或 `PruneShardGroups` 的 meta commit 都可能耗时
数秒到数分钟。

**影响**: 停机/滚动重启被拉长；`influxd` 的信号处理会卡在 `Close()`。

**缓解**: 没有 context/超时可用；只能降低单次检查的工作量（更短的 `check-interval`）
或接受停机延迟。

## 12. 源码校准：Retention 物理删除并发保护

Retention 的真实物理删除不是简单调用 `DeleteShard`。当前源码先收集过期组并软删，
再遍历**本地** shard 逐个 block → InUse → DeleteShard → DropShardMetaRef；最后只剩
“metadata 有引用、本地不存在”的 phantom shard 走只删 meta 的分支。两段循环是**分开**的。

```mermaid
flowchart TD
    A["DeletionCheck()"] --> B["Phase 1: DeletedShardGroups() + ExpiredShardGroups(now)"]
    B --> C["DeleteShardGroup(db, rp, id) 软删，置 DeletedAt"]
    C --> D["收集 deletedShardIDs"]
    D --> E["Phase 2: 遍历本地 TSDBStore.ShardIDs()"]
    E --> F{"id 在 deletedShardIDs 中?"}
    F -->|"否"| E
    F -->|"是"| G["SetShardNewReadersBlocked(id, true)"]
    G --> H{"ShardInUse(id)?"}
    H -->|"是"| I["defer 解除 block<br/>本轮跳过"]
    H -->|"否"| J["DeleteShard(id)"]
    J -->|"nil 或 ErrShardNotFound"| K["DropShardMetaRef(id, owners)"]
    J -->|"其他 error"| L["retryNeeded = true<br/>保留 meta ref"]
    K --> M["Phase 3: 遍历 deletedShardIDs 的剩余项"]
    L --> M
    M --> N["phantom: 只做 DropShardMetaRef<br/>不碰磁盘"]
    N --> O["Phase 4: PruneShardGroups()"]
```

> 注意 Phase 2 的 `SetShardNewReadersBlocked`/`ShardInUse` 只对**本地仍在**的 shard 生效；
> 本地不存在的 shard 走 Phase 3，Phase 3 不会尝试 `DeleteShard`。

接口是**节选**（仅列出本节涉及的方法）；完整定义见 §2.1：

```go
// services/retention/service.go — 节选
type OSSMetaClient interface {
    Databases() []meta.DatabaseInfo
    DeleteShardGroup(database, policy string, id uint64) error
    DropShard(id uint64) error
    PruneShardGroups() error
}

type MetaClient interface {
    OSSMetaClient
    NodeID() uint64
}

type Service struct {
    MetaClient
    TSDBStore interface {
        ShardIDs() []uint64
        DeleteShard(shardID uint64) error
        SetShardNewReadersBlocked(shardID uint64, blocked bool) error
        ShardInUse(shardID uint64) (bool, error)
    }
    DropShardMetaRef func(shardID uint64, owners []uint64) error
}
```

**ErrShardNotFound 语义**: `tsdb.Store.DeleteShard` 在 store 不再持有该 shard 时返回
`ErrShardNotFound`。Retention 层用 `!errors.Is(err, tsdb.ErrShardNotFound)` 过滤该错误并继续调用
注入的 `DropShardMetaRef`——但 `ErrShardNotFound` 只表示 **store 不再持有该 shard**，
**不等于磁盘已清空**。

**4 步失败链（孤儿目录）**:

```mermaid
flowchart TD
    A["DeleteShard: 先从 s.shards 摘除<br/>store.go:997"] --> B["Index / walkShards / sh.Close / os.RemoveAll 失败<br/>store.go:1016,1128,1133-1135"]
    B --> C["返回非 ErrShardNotFound 错误<br/>shard 已不在 s.shards，文件仍在磁盘"]
    C --> D["Retention 本轮 retryNeeded=true; continue<br/>service.go:262-267，且本轮不再走 phantom"]
    D --> E["下一轮 ShardIDs() 不含该 id<br/>store.go:1389-1394 → Phase 2 碰不到"]
    E --> F["phantom 循环直接 DropShardMetaRef<br/>service.go:284-300 → ShardInfo 被删"]
    F --> G["data/db/rp/id 与 WAL 目录成为孤儿"]
```

排查命令：`ls data/<db>/<rp>/<id>` 与对应 WAL 目录，人工确认后删除。

**案例 A: shard 42 本地仍在，但有查询持有 reader**

1. Retention 先 `SetShardNewReadersBlocked(42, true)`，阻止新的 reader 进入。
2. `ShardInUse(42)` 返回 true，说明旧查询还没释放。
3. 匿名函数返回 `errors.New("can not delete an in-use shard")`，defer 解除 block，本轮跳过；
   不删除磁盘文件，也不删 meta ref。
4. 下一轮检查时，如果 reader 已释放，才执行 `DeleteShard(42)` 和 `DropShardMetaRef(42)`。

**案例 B: shard 42 只在 metadata 中（phantom）**

1. `DeletedShardGroups()` 仍返回含 shard 42 的组，于是 `deletedShardIDs[42]` 被填入。
2. 本地 `ShardIDs()` 不含 42 → Phase 2 的循环根本不会碰到它。
3. Phase 3 遍历剩余项，OSS 分支（`isOSS()`）直接 `DropShardMetaRef(42, owners=[])`
   → `meta.Client.DropShard(42)` 删除 `ShardInfo`；磁盘上没有对应目录，无需删除。
4. 组内最后一个 `ShardInfo` 被移除时，`DropShard` 会副作用置 `DeletedAt`（`data.go:313-321`）。

> 若案例 B 的 shard 其实是“`DeleteShard` 中途失败但已摘除”的那种（见 4 步失败链），
> 磁盘目录就会留成孤儿——这正是本编号为 P1 的风险。
