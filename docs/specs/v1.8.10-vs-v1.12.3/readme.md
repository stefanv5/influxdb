# InfluxDB v1.8.10 → v1.12.3 版本差异总览

> **小白导读**：
> - 这不是一次“只改了几行”的升级。区间内有 **527 个提交、602 个文件、+79,310 / −45,239 行**，存储、索引、查询、HTTP、依赖都被动过。
> - 最大的变化在 **compaction**：v1.8.10 的调度器只建模 4 个队列，optimize 和 full 混在 level 4；v1.12.3 拆成 **L1–L5 五个队列**，L5 optimize 有独立 limiter、独立执行路径和 holdoff。
> - v1.12.3 还修了两个会直接表现为“磁盘一直涨”的调度 bug：**混合 level compaction 把 L4 降回 L2**，以及 **某些情况下 full compaction 根本不排期**。
> - 写入路径有一次 **Go API 级重构**：`context.Context` + 统计回调被换成 `tsdb.WriteContext` + `StatsTracker`；同时新增按 retention policy 配置的写入时间窗口，超窗写入会被拒绝。
> - 查询侧更多是 **安全与正确性修复**：`SHOW QUERIES` 越权、DST 跨边界丢行、`SHOW TAG VALUES` 跨 RP、array cursor 时间戳重复（不缺失）。
> - 如果你只想知道“升级会不会坏”，先读 [§5 升级影响清单](#5-升级影响清单)；如果你要核对代码，按 [§7 证据与方法](#7-证据与方法) 的命令逐条复现。

本文是 `docs/specs/v1.8.10-v1.12.3/` 的总览。七篇子文档分别深入写入、索引、查询、存储/compaction、元数据/后台、HTTP/Flux、构建/性能：

| 子文档 | 覆盖范围 |
|---|---|
| [01 写入链路](01_write_path_diff.md) | HTTP 写入、解析后的校验、`WriteContext`、WAL/Cache、字段错误语义、RP 写入窗口 |
| [02 索引与 Series](02_index_series_diff.md) | SeriesFile、TSI LogFile/IndexFile、MANIFEST、TagValueSeriesIDCache、TSI 并发 |
| [03 查询引擎](03_query_engine_diff.md) | InfluxQL iterator、DST、`SHOW TAG VALUES`、`SHOW QUERIES`、array cursor、window aggregate |
| [04 存储与 Compaction](04_storage_compaction_diff.md) | 4→5 级模型、Scheduler、Planner、level 回退、full/optimize、FileStore、purger |
| [05 元数据与后台](05_meta_background_diff.md) | Meta 状态机、shard group、retention、precreator、CQ、shard 生命周期 |
| [06 HTTP 与 Flux](06_http_flux_diff.md) | HTTP 路由、TLS、`/debug/vars`、v2 API、Flux 升级与 storage 读取入口 |
| [07 构建与性能](07_build_perf_diff.md) | 工具链/依赖、构建执行、CI 编排、打包、发布、分发安装、可观测性、测试基础设施（含性能回归缺口）、性能优化 |

---

## 1. 版本身份与基线

### 1.1 两个版本是谁

| 项 | OLD | NEW |
|---|---|---|
| 版本 | **v1.8.10** | **v1.12.3** |
| commit | `688e697c51`（`688e697c51fd5353725da078555adbeff0363d01`） | `f3d5cb68ab`（`f3d5cb68ab25a07d89f76d8c18e52c362cc37bfb`） |
| commit 日期 | 2021-10-11 | 2026-02-27 |
| 与 `docs/specs/` 17 篇主文档的关系 | **不是**主文档基准 | **是**主文档的代码基准 |
| 区间规模 | — | `527 commits`、`602 files`、`+79,310 / −45,239` |

复核命令与输出：

```bash
git show -s --format=%H v1.8.10
# 输出含 tag 对象头（tag / Tagger），最后一行才是 commit hash：
# 688e697c51fd5353725da078555adbeff0363d01（等价写法：git rev-parse 'v1.8.10^{}' 或 git log -1 --format=%H v1.8.10）

git rev-parse v1.12.3
# f3d5cb68ab25a07d89f76d8c18e52c362cc37bfb

git rev-list --count v1.8.10..f3d5cb68ab
# 527

git diff --shortstat v1.8.10 f3d5cb68ab
# 602 files changed, 79310 insertions(+), 45239 deletions(-)
```

**与主文档的关系必须说清楚**：`docs/specs/readme.md` 第 5 行明确写着，17 篇主文档以 v1.12.3（commit `f3d5cb68ab`）为基准，不是 v1.8.10。当前工作树只在 `f3d5cb68ab` 之上增加了文档提交（数量会随文档演进，不在此硬编码）：`git diff --stat f3d5cb68ab HEAD -- '*.go'` 输出为空。因此本组差异文档里所有“新版本行为”都可以直接在当前工作树核对，等价于 `v1.12.3`。

### 1.2 变化集中在哪里

按顶层目录聚合 `git diff --numstat v1.8.10 f3d5cb68ab`（含生成代码，只列主要目录）：

| 目录 | 文件数 | 新增行 | 删除行 | 主要变化 |
|---|---:|---:|---:|---|
| `storage` | 44 | 23,693 | 8,919 | array cursor、window aggregate、Flux storage 读取 |
| `tsdb` | 97 | 13,497 | 5,507 | compaction、FileStore、TSI、SeriesFile、WAL/Cache |
| `services` | 55 | 9,218 | 3,455 | httpd、meta、retention、continuous querier |
| `pkg` | 44 | 5,221 | 1,360 | TLS、file、metrics、limiter |
| `query` | 33 | 5,149 | 684 | InfluxQL iterator、TaskManager、statement rewriter |
| `flux` | 28 | 4,373 | 939 | Flux 升级、window/bare aggregate 规则 |
| `cmd` | 65 | 3,448 | 3,904 | influxd 启动、influx_inspect、influx_tools |

另有结构性新增：`kit/`（0 → 34 文件，2.x 共享层）、`monitor/stats.go`（monitor 15 → 17 文件）、`pkg/`（110 → 125 文件）。`kit/` 中只有 `platform/errors`、`errors`、`prom`、`tracing` 被 1.x 业务代码引用，`check`/`io`/`metric`/`signals`/`transport/http` 暂无 1.x 调用点（07 §3.11.2）。

提交标题前缀的粗略构成（`git log --format='%s' v1.8.10..f3d5cb68ab`）：

| 类型 | 数量 | 说明 |
|---|---:|---|
| `fix` | 223 | 正确性、并发、错误处理、兼容性修复 |
| `chore` | 124 | 依赖、日志、重构、清理 |
| `feat` | 103 | 新功能、新配置、新观测项 |
| `build` | 22 | Go/依赖/构建链 |
| `test` | 10 | 测试框架与测试修复 |
| `refactor` | 10 | 结构调整 |
| 其余 | 35 | Merge 30、revert 2、perf 1、improvement 1、docs 1 |

### 1.3 六条结论

1. **存储层变化最大，而且不是“纯优化”**：compaction 的队列模型、level 计算、full/optimize 调度都改了；1.12.2 修 full 调度、1.12.3 修 level 回退（`95e86be3f4`、`5e9d85c972`）；区间内共三个磁盘/压缩调度问题，其中“无界增长”特指 #26670。
2. **写入路径有 Go API 级重构**：`WriteContext` + `StatsTracker` 取代了 `context.Context` 传参；`time` 字段从静默丢弃变成返回 `PartialWriteError`（engine 层也不再跳过，字段可能落盘，见 01 §7.1，待回归）；新增 RP 级 `FUTURE LIMIT` / `PAST LIMIT`。
3. **查询侧的主线是安全与正确性**：`SHOW QUERIES` 越权、DST 丢行、`SHOW TAG VALUES` 跨 RP、array cursor 时间戳重复（不缺失），都是能被用户观察到的行为变化。
4. **运维面新增 4 项可核对能力**：① TLS 证书 SIGHUP 热重载；② `/debug/vars` 增加 `config`/`stats`（`compact-throughput-usage-percentage`）；③ `cq` 由 `{name,tags,values}` 改为扁平 `values`；④ `/api/v2/delete` 与 `/api/v2/buckets` 新增。**未见 TSM/WAL 磁盘格式版本升级**，TSI MANIFEST 写入改为临时文件 + rename（进程崩溃安全，但 rename 后未 `SyncDir`，掉电恢复窗口见 02 §7.9），meta protobuf 与 `fields.idx` 都是兼容扩展（后者新增 `fields.idxl` 增量日志）。
5. **构建基线跨度很大**：Go 1.13 → Go 1.24，Flux v0.65.1 → v0.196.1，InfluxQL 伪版本 → v1.4.1；升级前应先过工具链和依赖行为差异。
6. **构建/交付要按五个模块看**：构建执行（`build()`/`archive()`）、CI 编排、打包、发布、分发安装各自有独立的角色/结构/流程/案例/风险（07 §3.4–§3.8）；性能优化按 compaction/TSI/query/cache/build 分组，且**没有跨版本基准数字**（07 §4）。旧版 `perf_test` 的 AWS/S3 性能回归在 v1.12.3 线没有替代（07 §3.10.5）。

### 1.4 模块 owner 与主要引用点

> 同一事实只在一个小节展开。下表用于从“模块”反查 owner；模块边界与阅读顺序见 01 §3.0.1。

| 模块 | owner（主落点） | 其他篇引用点 |
|---|---|---|
| HTTP 写入入口 | 01 §3.0.2 | 06 §3.1、§3.19（路由与 v2 API） |
| 写入 API / 统计 / 身份 | 01 §3.1、§3.2 | 05 §3.16b（订阅）；02 §3.15.3（Series 统计） |
| 字段元数据 `fields.idx` / `fields.idxl` | 01 §3.3–§3.5、§5.3、§7.3 | 02 §3.12（wire/兼容）；主 06 §8.5 |
| Coordinator 编排与 RP 写窗口 | 01 §3.6 | 05 §3.3（RP 窗口字段与 SQL） |
| Store/Shard 写入口、坏 shard | 01 §3.7、§3.13 | 05 §3.10（badShards 跨层摘要） |
| Cache/Ring/WAL/Engine | 01 §3.8–§3.10 | 04 §3.16（snapshot 写出与临时文件清理） |
| 写路径 Series 注册 | 01 §3.7.1 | 02 §3.9.3（TSI→SeriesFile） |
| SeriesFile / SSEG / SIDX | 02 §3.1、§3.9 | 主 14 §6（SIDX 69B header） |
| TSI LogFile / IndexFile / MANIFEST | 02 §3.2、§3.6 | 主 02 §4.0、§4.2（启动与 MANIFEST 格式） |
| TSI 查询与授权旁路 | 02 §3.15 | 03 §3.17（InfluxQL 迭代器） |
| 4→5 级 compaction / Planner / full / optimize | 04 §3 | 01 §3.11（与写入路径的边界） |
| FileStore / purger | 04 §3.15 | 02 §3.14（索引侧衔接） |
| meta / retention / CQ / precreator | 05 §3 | 01 §3.6（写窗口）；02 §3.11（删除语义） |
| HTTP 路由 / TLS / v2 / Flux | 06 §3 | 03 §3.16（`query/control`） |
| 构建 / CI / 打包 / 发布 / 安装 / 性能 | 07 §3–§7 | 本文件 §3.7（07 模块导航） |

---

## 2. 全局变更地图

下图把七大块和它们之间的主要依赖画在一起。节点里写的是真实结构体/函数/配置名，不是概念占位符。图例：实线 = 调用或主数据流（caller→callee，或计划/统计数据的流向）；虚线 = 旁路或影响。

```mermaid
flowchart TB
    subgraph W["① 写入链路"]
        W1["HTTP /write"] --> W2["PointsWriter<br/>WriteContext(UserId)<br/>MapShards 计算 RP WriteWindow"]
        W2 -->|每 shard 一个 goroutine（并发）| STK["Store.WriteToShard<br/>WriteContext + statsTracker(db,rp,login)"]
        STK --> SH["Shard.WritePoints<br/>shard.go:562"]
        SH --> W3["validateSeriesAndFields<br/>time 字段 -> PartialWriteError（继续写）"]
        W3 --> CF["saveFieldsAndMeasurements<br/>-> fields.idxl"]
        CF --> W4["Engine.WritePoints<br/>Cache + WAL"]
        W3 -.->|非 Partial 错误| WE["写请求失败（HTTP 500）"]
        CF -.->|错误| WE
        W4 -.->|错误| WE
        W2 -.-> WD["PartialWriteError（400）<br/>全部 shard 执行后返回；有效点已写入<br/>（RP 窗口 / retention 丢弃）"]
        W2 -.-> WG["空 shard group -> 普通 error（500）"]
        W2 -.-> SUB["Subscriber.Send<br/>（调用方 goroutine 同步执行）"]
        W2 -.-> CR["ErrShardNotFound -> CreateShard<br/>-> 重试一次"]
        W4 -.-> W5["TSM snapshot<br/>（后台 compactCache tick 触发）"]
    end

    subgraph I["② 索引 / Series"]
        I1["SeriesFile<br/>建/删 series<br/>删：DeleteSeriesID(NoFlush)<br/>+ 批末 FlushSegments"]
        I2["TSI LogFile / IndexFile<br/>MANIFEST 原子写<br/>TagValueIterator 缩短锁"]
        I3["TagValueSeriesIDCache<br/>Put 复用一次转换（Get 仍 3 次）"]
    end

    subgraph Q["③ 查询引擎"]
        Q1["InfluxQL Executor<br/>DST offset 修复<br/>SHOW TAG VALUES 单 RP"]
        Q2["TaskManager<br/>SHOW QUERIES 授权过滤"]
        Q3["storage/reads<br/>WindowAggregate pushdown<br/>array cursor 闭区间"]
    end

    subgraph S["④ 存储 / Compaction"]
        S0["Engine.compact<br/>tick / 1h 启动、5m optimize holdoff<br/>SetDepth + 分发 S3/S4 + Release"]
        S1["Scheduler<br/>4 队列 -> 5 队列<br/>queues + weights、next()"]
        S2["PlanCompactions / planCompactionsInner<br/>L1 到 L5<br/>L5 入口 1：PlanOptimize（冷写>=4h）<br/>L5 入口 2：L4 -> L5 IsGroupOptimized（不受冷写）<br/>单世代/已优化组 -> aggressive ppb"]
        S3["compactOptimize<br/>接收 S2 决定的 ppb<br/>optimizedCompactionLimiter"]
        S4["compactGroup 执行<br/>CompactFull/Fast → Compactor.compact<br/>（minSeqByGen 防回退）"]
        S5["FileStore<br/>TSM 文件名缓存<br/>purger 并发 map<br/>（TSMReader first-block 缓存）"]
        RCP["ReleaseCompactionPlans<br/>（tick 末释放未用计划）"]
    end

    subgraph M["⑤ 元数据 / 后台"]
        M1["Meta<br/>无重叠 ShardGroup<br/>PruneShardGroups"]
        M2["Retention<br/>SetShardNewReadersBlocked<br/>ShardInUse + 批量 fsync"]
        M3a["Precreator / CQ<br/>CQ 另依赖 QueryExecutor 与 Monitor"]
        M3b["cq 统计 / diagnostics<br/>Statistics(Name=cq) -> /debug/vars<br/>RegisterDiagnosticsClient -> SHOW DIAGNOSTICS"]
    end

    subgraph H["⑥ HTTP / Flux"]
        H1["TLSConfigManager<br/>SIGHUP 证书重载<br/>client cert / 权限检查"]
        LSN["HTTP Service.Open / tlsManager.Listen<br/>TLS transport 覆盖全部路由"]
        H2["/query<br/>time_format<br/>CSV 不转义 tag"]
        H3["/debug/vars<br/>config / cq / compact-throughput"]
        H4["/api/v2/delete + buckets"]
        HF["serveFluxQuery<br/>handler.go:2175"]
        H5["Flux v0.196.1<br/>query/control 资源配额"]
    end

    subgraph B["⑦ 构建 / 依赖 / 交付"]
        B1["Go 1.13 -> 1.24<br/>（全部 Go 组件）"]
        B2a["InfluxQL 1.4.1"]
        B2b["protobuf 工具链升级<br/>meta/fields .pb.go（wire 兼容）"]
        B2c["JWT v4（HTTP 鉴权）"]
        B2d["xxhash v2"]
        B4["Flux v0.65.1 -> v0.196.1"]
        B3a["构建执行<br/>build() / archive()<br/>4 个目标 + cache v2"]
        B3b["CI 编排<br/>release / on_push / nightly<br/>+ GitHub Actions semantic"]
        B3c["打包<br/>packager + config.yaml<br/>deb/rpm/tar/zip"]
        B3d["发布<br/>rsign + S3 releases/TAG<br/>changelog + slack"]
        B3e["分发 / 安装<br/>systemd unit + init.sh<br/>pre/post-install、post-uninstall"]
        B5["测试基础设施<br/>inmem/tsi1/race + 属性测试<br/>（性能回归已移除）"]
        B6["性能优化<br/>compaction / TSI / query / cache / build<br/>（无基准数字）"]
    end

    W2 --> M1
    W3 --> I1
    W3 --> I2
    W5 --> S5
    S0 --> S2
    S0 --> S1
    S0 --> S3
    S0 --> S4
    S3 --> S4
    S4 --> S5
    S0 --> RCP
    S2 --> S5
    Q1 --> I2
    Q1 --> Q2
    Q3 --> I2
    Q3 --> S5
    H2 --> Q1
    H4 --> M1
    HF --> H5
    H5 --> Q3
    I2 --> I3
    I2 --> I1
    H1 --> LSN
    LSN --> H2
    LSN --> H3
    LSN --> H4
    LSN --> HF
    M2 --> S5
    M2 --> I1
    M2 --> M1
    M3a --> M1
    M3a --> Q1
    M3a --> W2
    M3b --> H3
    B1 -.-> W4
    B1 -.-> H5
    B2a -.-> Q1
    B2b -.-> M1
    B2b -.-> CF
    B2c -.-> H2
    B2d -.-> S5
    B2d -.-> I2
    B4 -.-> H5
```

**怎么读这张图：**

1. 先看 **④ 存储/Compaction**：这是本次区间里行为变化最集中、最容易影响磁盘和 CPU 的块。`S0`→`S2`（`Engine.compact` 调用 `PlanCompactions`），`S0`→`S1` 选级、`S0`→`S3`/`S4` 分发执行（`S3`→`S4`），`S4`→`S5`（`fileStore.Replace`/purger 写回）是新的 5 级调度主链；边按调用方向画，不表示单一路径。
2. 再看 **① 写入链路** 与 **② 索引/Series**：写入校验、`WriteContext`、TSI 锁与缓存分配都在这条线上，影响写入延迟和错误语义。
3. 最后看 **③/⑤/⑥/⑦**：查询、后台、HTTP/Flux、构建依赖大多是外围行为或可观测性变化，但 `M2` retention 和 `H1` TLS 会直接影响运维。
4. 这张图只画变更主题与主要依赖，不是完整调用图；`B1`/`B2a`–`B2d` 的虚线是示例性影响边，错误/重试分支与省略的次要依赖见各专题。
5. `B3a`–`B3e` 是构建/交付的五个并列主题（构建执行、CI、打包、发布、安装），`B5`/`B6` 是测试与性能主题；它们不画代码依赖边，展开位置见 §3.7 的“07 模块导航”。

---

## 3. 差异分类表

分类是“按变化性质”切的，不是按目录切的；同一个 commit 可能同时出现在两类里（例如 compaction 调度既影响性能也影响正确性）。每行都给代表 commit 和对应子文档。

### 3.1 新功能

| 代表项 | 变化要点（旧 → 新） | 代表 commit | 子文档 |
|---|---|---|---|
| RP 写入时间窗口 | v1.8.10 没有窗口概念；v1.12.3 支持 `FUTURE LIMIT` / `PAST LIMIT`，窗口外的点返回 `PartialWriteError`，并让 `SHOW RETENTION POLICIES` 增加 future/past 两列 | `07c261a21a` | [01](01_write_path_diff.md) |
| Level 5 optimize 独立调度 | 旧版 optimize 复用 level 4 计划，且 `compactFull` 硬编码 `optimize=false`，计划实际按 full 执行；新版拆出 L5、独立 `compactOptimize`（单世代/已优化组用 aggressive points-per-block）、独立 limiter 与 holdoff | `f738344a81`、`0fdb6adcb2` | [04](04_storage_compaction_diff.md) |
| TLSConfigManager 与证书热重载 | 旧版启动时 `tls.LoadX509KeyPair` 静态加载；新版管理器支持 SIGHUP 重载、客户端证书、证书/私钥权限检查（TLS 1.3 两版都可配置） | `049b1f7e00`、`aa1775bfc2`、`e3b058616c` | [06](06_http_flux_diff.md) |
| `/query` 的 `time_format` 参数 | 旧版只有 `epoch`；新版支持 `epoch` / `rfc3339`，默认仍是 `epoch` | `b9a0155ca9` | [06](06_http_flux_diff.md) |
| `/debug/vars` 扩展 | 新增 `config`、`stats.compact-throughput-usage-percentage` 等诊断；`cq` 由 Statistic 包装改为扁平 values | `269f1b6281`、`f75be56eee`、`0e656d6dd5` | [06](06_http_flux_diff.md)、[07](07_build_perf_diff.md) |
| `/api/v2/buckets` 部分支持 | 旧版无 buckets CRUD；新版支持 create/delete/list/get/update 的子集 | `ce40acb298` | [06](06_http_flux_diff.md) |
| Window aggregate 下推 | 旧版 storage read 无窗口聚合下推；新版增加 `ReadWindowAggregate`、`WindowAggregate` 与 MeanCount cursor | `a2154f143c`、`599700b41b` | [03](03_query_engine_diff.md) |
| Flux 查询控制器 | 旧版 `flux/control` 是薄壳，基本不限内存/并发；新版 `query/control` 增加并发配额、单查询/总内存配额和队列，配置段为 `[flux-controller]` | `b9ee9e5b44`、`f1aa051787` | [03](03_query_engine_diff.md)、[07](07_build_perf_diff.md) |

### 3.2 行为变更

| 代表项 | 变化要点（旧 → 新） | 代表 commit | 子文档 |
|---|---|---|---|
| 写入 Go API 签名 | 旧版 `Engine.WritePoints(points)` / `WritePointsWithContext(ctx)`、`Store.WriteToShard(shardID, points)`；新版改为 `WritePoints(points, StatsTracker)`、`WriteToShard(WriteContext, ...)` | `dd3baf6d4a`、`6795ec6c01` | [01](01_write_path_diff.md) |
| `time` 字段错误语义 | 旧版静默跳过字段 `time`（只有该字段的点两版都整点丢弃，`Dropped≥1`）；新版返回 `PartialWriteError`（混合字段点 `Dropped=0`，点仍写入），但调用方会收到错误；engine 层也不再跳过 `time`（`v1.8.10:tsdb/engine/tsm1/engine.go:1330-1333` → `v1.12.3:tsdb/engine/tsm1/engine.go:1366-1367` 删除），字段可能落盘，与错误文本不一致（01 §7.1，待回归） | `1356a21321` | [01](01_write_path_diff.md) |
| `SHOW TAG VALUES` 的 RP 作用域 | 旧版跨所有 RP 收集；新版按语句中的 RP 限定，多个不同 RP 直接报错；该限制只对 TSI 生效 | `519e23b86a` | [03](03_query_engine_diff.md) |
| `SHOW SERIES CARDINALITY` 近似化 | 旧版带 FROM/WHERE 的非 `EXACT` 查询走精确 `count(distinct(_seriesKey))`；新版改走 `count_hll(sum_hll(_seriesKey))` | `de1a0eb2a9` | [03](03_query_engine_diff.md) |
| `SHOW QUERIES` 授权过滤 | 旧版非管理员能看到其他数据库的查询；新版按 `AuthorizeDatabase(ReadPrivilege, db)` 过滤；列数同时 5→6（+`user`），按固定列解析的脚本会错位 | `2b7164440f` | [03](03_query_engine_diff.md)、[06](06_http_flux_diff.md) |
| array cursor 时间区间 | 底层存储两版都是闭区间；旧版把半开 stop 原样传入，会多返回恰好落在 stop 的点；新版构造 `CursorRequest` 时统一 `end-1`，对 Flux 仍是 `[start, stop)`（InfluxQL 不走 `storage/reads`，见 03 §2.1） | `8edf7a4e2f` | [03](03_query_engine_diff.md) |
| `/api/v2/delete` 语义 | v1.8.10 不存在该端点（v1.10.0 的 `2fe52d8edc` 引入）；区间内修正初版的 nil `Sources`、db/rp 混入 measurement、measurement 引号 | `f365bb7e3a`、`c31e99574f`、`e3e9232de5`、`2fe52d8edc` | [06](06_http_flux_diff.md) |
| CSV 输出转义 | 旧版 CSV formatter 对 tag 做转义；新版 `HashKey(false)`，CSV 中不再出现反斜杠转义 | `2dc3dcb3d1` | [06](06_http_flux_diff.md) |
| 字段长度上限 | 旧版没有字段长度限制；新版 `MaxFieldValueLength = 1048576`，字符串字段超过 1 MiB 返回 `PartialWriteError`，可用 `skip-field-size-validation` 关闭 | `6d22e69ef1` | [01](01_write_path_diff.md) |

### 3.3 性能优化

| 代表项 | 变化要点（旧 → 新） | 代表 commit | 子文档 |
|---|---|---|---|
| TSM 文件名解析缓存 | 旧版 `tsmGeneration.level()` 每次都 `parseFileName`；新版在打开/重命名时缓存 generation/sequence | `f0d5c73253` | [04](04_storage_compaction_diff.md) |
| compaction 规划 CPU 与首 block 缓存 | 旧版规划反复解码 block 统计；`2a0353ffd2` 引入 `firstBlockCount` 缓存与 `PT_SmartOptimize`/`PT_NoOptimize` 特例，`95e86be3f4` 再移除这两个特例（修 #26670） | `2a0353ffd2`、`95e86be3f4` | [04](04_storage_compaction_diff.md) |
| optimize 频率与并发控制 | 旧版仅在 `len(level4Groups) == 0` 时调用 `PlanOptimize()`，且 `compactFull` 硬编码 `optimize=false`（这些计划实际按 full 执行）；新版用 `lastWrite`、1 小时启动 holdoff、5 分钟间隔和独立 limiter 控制 | `e984a226f2`、`f738344a81` | [04](04_storage_compaction_diff.md) |
| TSI series cache 字符串分配 | 旧版 `Put` 在 exists/元素/map key 处重复 `string(...)`；新版 `Put` 一次转换、复用同一 backing string（`Get` 未变，仍每次转换 3 次） | `f3d5cb68ab` | [02](02_index_series_diff.md) |
| TagValueIterator 锁持有时间 | 旧版 `defer RUnlock` 覆盖整个 iterator 构造；新版查到 `logTagKey` 后立即释放 RLock，但随后在锁外 range 共享 map，并发写会 fatal（02 §7.7） | `e51acc4882` | [02](02_index_series_diff.md) |
| purger 锁竞争 | 旧版用全局 mutex 管理待删文件；新版 `gensyncmap.Map` + 缩小临界区 | `d5a48e0662` | [04](04_storage_compaction_diff.md) |
| retention 删除 fsync 策略 | v1.8.10 的 `DeleteSeriesID` 只 `Flush()` 缓冲、不 `file.Sync()`；区间内 `b4e781eff6` 给 `SeriesSegment.Flush` 加上 `file.Sync`；新版 `1420aaf2bc` 改为 `NoFlush` + `FlushSegments` 按 partition 批量 fsync；另有写路径：建新 series 的请求每个 (TSI 分区, SeriesFile 分区) 组合至多一次 fsync（单 shard 上限 64，跨 shard 叠加；v1.8.10 为 0） | `b4e781eff6`、`1420aaf2bc` | [05](05_meta_background_diff.md) |
| `fields.idx` 增量持久化 | 旧版每个含新字段的写请求都重写整个 `fields.idx`；新版把变更追加到 `fields.idxl`；启动时 ApplyChanges / Close / LoadMetadataIndex 重建 / Restore·Import 时合并回 `fields.idx`；注意删除 measurement 的变更不会写入 `fields.idxl`（02 §7.8） | `80c10c8c04` | [01](01_write_path_diff.md)、[02](02_index_series_diff.md) |

> 这 8 行里只有 4 行直接进 07 §4.2/§4.3/§4.5 的优化组（TSM 文件名缓存、首块缓存、TSI cache、purger），retention fsync 进 §4.7 #18（持久化增强）；optimize holdoff 的修复 `0fdb6adcb2` 在 §4.7 #7，但 optimize 频率/并发的实现（`e984a226f2`/`f738344a81`）不在 07 §4，owner 在 04；TagValueIterator 并发问题与 `fields.idx` 增量日志也不在 07 §4，owner 分别在 02 §7.7 与 02 §3.12。07 §4.7 另把 4 条正确性修复、2 条 v1.12 新功能、1 条持久化增强单列为“非优化项”，§4.8 说明为什么没有跨版本基准数字。

### 3.4 正确性修复

| 代表项 | 变化要点（旧 → 新） | 代表 commit | 子文档 |
|---|---|---|---|
| 混合 level compaction 的 level 回退 | 旧版输出 sequence 只看 max generation，可能把 L4 降成 L2；新版用 `minSeqByGen` + `maxInputLevel` 保证输出 level 不回退 | `5e9d85c972` | [04](04_storage_compaction_diff.md) |
| 嵌套低层文件的规划（1.12 线回归） | 1.12 线把嵌套处理从「遇低层即停」改为 `Plan` 反向扫描；这是 #26911 的回归修复，不是「v1.8.10 漏压 → v1.12.3 修好」 | `d1d68d656d` | [04](04_storage_compaction_diff.md) |
| full compaction 未排期（区间内回归） | v1.8.10 用 `PlanLevel`/`Plan`/`PlanOptimize`；1.12.1 引入的 `PT_SmartOptimize`/`PT_NoOptimize` 特例会让文件被 planner 锁住却不排期，表现为磁盘无界增长；1.12.2 移除特例并把 holdoff 移到 `Engine.compact` | `95e86be3f4` | [04](04_storage_compaction_diff.md) |
| retention 孤儿 shard / 挂起 | 旧版 `DeleteShard` 返回错误、shard 已被摘除时也会 prune：14 天后只要 `DeletedAt` 过期就删组，不检查 shard 是否删净，留下孤儿文件；读者阻塞时整轮停在 `Shard.Close`、当轮不会 prune，解除阻塞后仍按 14 天规则修剪。新版 `PruneShardGroups` 要求无 shard 且已过 `DeletedAt`，并用 `SetShardNewReadersBlocked`/`ShardInUse` 跳过 in-use shard | `7bd3f89d18`、`b4bd607eef` | [05](05_meta_background_diff.md) |
| TSI MANIFEST 与部分打开清理 | 旧版 `ioutil.WriteFile` 直接覆盖 MANIFEST；新版临时文件 + `os.Rename` 原子替换。旧版部分打开失败可能泄漏引用；新版补齐清理与 iterator close | `061cf55f2a`、`d3db48e93d`、`a428043f84` | [02](02_index_series_diff.md) |
| 非法 series key 越界 panic | 旧版对非法 series key 可能 index out of range；新版在 `series_file.go`、`series_cursor.go`、`log_file.go`、`tsdb/index.go`、`tsdb/series_partition.go` 增加边界检查 | `6af0be9234` | [02](02_index_series_diff.md) |
| 空 shard group 写入 panic | 旧版 `ShardFor` 对 `len(Shards)==0` 直接 `HashID()%0`，写入触发 panic；新版 `MapShards` 检测空组并返回错误。旧版写入触发 `CreateShard` 时会反复重试打开同一个坏 shard，新版 `badShards` 记住打开错误、避免重复尝试 | `694607a22c`、`54ac7e54ed` | [01](01_write_path_diff.md) |
| 删除 series 的锁 | 区间内 `1bc0eb4795` 引入 `seriesKeysLock` 时漏了一条 `RUnlock`，`81b42fd344` 修复；`21ab48416e` 另修 `Store.DeleteSeries` 的读锁泄漏。v1.8.10 本身没有该锁，不存在“旧版泄漏” | `1bc0eb4795`、`81b42fd344`、`21ab48416e` | [02](02_index_series_diff.md)、[05](05_meta_background_diff.md) |
| GROUP BY 跨 DST 边界 | 旧版先把 `end` 做时区换算、再加 offset，秋季回拨时丢行；新版把 `end += Offset` 移到时区计算之前 | `d60741b506` | [03](03_query_engine_diff.md) |
| array cursor 优化重复数据 | 旧版只判断 `pos == 0`，在 cache 与 TSM 混合的第二个 `Next()` 里可能重复拷贝 TSM 点；新版增加 `c.tsm.pos == 0` | `31cb61f556` | [03](03_query_engine_diff.md) |

### 3.5 构建与依赖

| 代表项 | 变化要点（旧 → 新） | 代表 commit | 子文档 |
|---|---|---|---|
| Go 工具链 | `go 1.13` → `go 1.24.0`，最终 `toolchain go1.24.13` | `0574163566`、`e484c4d871`、`542ce7f684`、`d65673b1f5` | [07](07_build_perf_diff.md) |
| Flux | `v0.65.1` → `v0.196.1` | `03b6ed2bed` | [07](07_build_perf_diff.md) |
| InfluxQL | `v1.1.1-0.20200828144457` → `v1.4.1` | `77d6f20894` | [07](07_build_perf_diff.md) |
| protobuf 代码生成 | 生成器从 `protoc-gen-gogo` 切到官方 `protoc-gen-go`（最终 `protoc-gen-go v1.34.1` / `protoc v5.29.2`），重新生成 meta/fields 等 `.pb.go`；wire 格式兼容 | `8b38d0e2bf`、`d4b16dcd98` | [07](07_build_perf_diff.md) |
| JWT 与 x/net | `dgrijalva/jwt-go/v4` → `golang-jwt/jwt/v4 v4.5.2`，x/net 升级 | `f8635640cc` | [07](07_build_perf_diff.md) |
| xxhash | `cespare/xxhash v1.1.0` → `cespare/xxhash/v2 v2.3.0` | `45a8227ad6` | [07](07_build_perf_diff.md) |
| 构建执行与产物 | `build()`/`archive()` 直接跑在 cross-builder；4 个目标 × 3 条命令；`influxdb-cache-v2-<go.mod checksum>` 缓存 module/cargo/go-build；`influx_tools` 不再出制品、`influx_stress` 删除 | `3d4f3a2444`、`88c1c31406`、`6a3b02cac4` | [07](07_build_perf_diff.md) §3.4 |
| CI 编排 | 旧版 5 个 job/`on_push`；新版 3 条 workflow（release/on_push/nightly）+ 参数化 `unit_test` + `fluxtest`；新增 GitHub Actions `semantic.yml` | `59e84cc48b`、`8f78c4bb02` | [07](07_build_perf_diff.md) §3.5 |
| 打包 / 发布 | Python 2.7 `build.py` + fpm → `packager` + `rsign`；S3 `releases/<tag>/`；`changelog`/`publish_changelog`；`test_pkgs_64bit` 不 gate 发布 | `d8ecd34fa4`、`d628f8b374`、`b4908528a8` | [07](07_build_perf_diff.md) §3.6–§3.7 |
| 分发 / 安装 | unit 内容不变；启动脚本新增 `buildtsi`；SysV `init.sh` 用 `setpriv`；`post-uninstall` 不再删 unit；`logrotate` 删除 | `7f42c16d85`、`70543bed1d` | [07](07_build_perf_diff.md) §3.8 |
| arrow / flux array | v1.8.10 的 `storage/reads/flux_table.go`、`flux_tags_cache.go` 用 `apache/arrow/go/arrow/array`；v1.12.3 的 `storage/flux/{table,table.gen,tags_cache,window}.go` 改用 `influxdata/flux/array`（提交 `8fae484a66`，v1.10.0 进入） | `8fae484a66` | [07](07_build_perf_diff.md) §3.2.1 |

### 3.6 测试、可观测性与性能回归

| 代表项 | 变化要点（旧 → 新） | 代表 commit | 子文档 |
|---|---|---|---|
| compaction 属性测试 | 旧版以表驱动用例为主；新版增加 property/不变量校验框架（相邻性），作用于既有表驱动用例，未做随机生成 | `ba2c2b6432` | [07](07_build_perf_diff.md) |
| CI 测试矩阵 | `unit_test` 参数化（inmem/tsi1/race），32bit 移除；`taskset -c 0-7`；`gotestsum` | `59e84cc48b`、`3a739948a6` | [07](07_build_perf_diff.md) §3.10.3 |
| 包安装测试 | 旧版 `pkg_run_test` 在 EC2 上跑 3 个功能冒烟脚本（auth/default-config/self-signed-certs）；新版 `test_pkgs_64bit` 只在 Docker 内做 install/uninstall，断言 3 个二进制 + 8 个 man page | — | [07](07_build_perf_diff.md) §3.10.4 |
| 性能回归测试 | 旧版 `perf_test`（AWS r5.2xlarge + terraform + S3 结果）；v1.12.3 无替代，CI 不跑 `-bench` | — | [07](07_build_perf_diff.md) §3.10.5 |
| FileStore merge 指标 | 新增每种类型的 `merge_duration`、`merge_count`、`merge_values_count`、`merge_over4_count` | `133c290fc2` | [04](04_storage_compaction_diff.md) |
| compact throughput 诊断 | `/debug/vars` 新增 `stats.compact-throughput-usage-percentage` | `0e656d6dd5` | [06](06_http_flux_diff.md)、[07](07_build_perf_diff.md) |
| `/debug/vars` 的 config/cq | 新增 `config` 诊断；`cq` 输出形状改为扁平 values（`cq` 本身旧版已有） | `269f1b6281`、`f75be56eee` | [06](06_http_flux_diff.md) |
| 查询字节指标 | `SHOW STATS` 增加 per-user `userquerybytes`；`/debug/vars` 增加 `fluxQueryRespBytes` | `4dcc8776c1`、`db523227a2` | [06](06_http_flux_diff.md) |
| access log 默认配置 | `access-log-status-filters` 补进 `influxd config` 默认输出（功能本身旧版已有） | `a2e029fa7f` | [06](06_http_flux_diff.md) |
| `LastModifiedOrErr` | 旧版 `LastModified` 吞掉错误；新版暴露错误供调用方判断 | `bd3eb349a4` | [05](05_meta_background_diff.md) |
| shard/丢点日志 | 新增启动 shard 数量日志、改进 dropped point 日志、减少 phantom shard 噪声 | `3c87f524ed`、`31b41a6a2d`、`c168cc5aca` | [05](05_meta_background_diff.md) |

---

### 3.7 07 模块导航（构建 / 交付 / 测试 / 性能）

| 07 模块 | 07 小节 | readme 落点 |
|---|---|---|
| 工具链 / 依赖 | §3.1、§3.2（含 §3.2.1 Flux array 迁移） | `B1`/`B2a`–`B2d`/`B4`；§3.5 表 |
| 构建执行 | §3.4（含产物清单 §3.4.1、构建性能 §3.4.2） | `B3a`；§3.5 表“构建执行与产物” |
| CI 编排 | §3.5（含 §3.5.1 静态检查、§3.5.2 GitHub Actions semantic） | `B3b`；§3.5/§3.6 表 |
| 打包 | §3.6 | `B3c`；§3.5 表“打包 / 发布” |
| 发布 | §3.7（含 §3.7.1 版本号） | `B3d`；§3.5 表“打包 / 发布” |
| 分发 / 安装 | §3.8 | `B3e`；§3.5 表“分发 / 安装” |
| 可观测性 | §3.9（§3.9.1–§3.9.6） | `H3`；§3.6 表 |
| 测试基础设施 | §3.10（§3.10.1–§3.10.5） | `B5`；§3.6 表 |
| 目录 / 共享库 / 工具 | §3.11（§3.11.1–§3.11.3） | §1.2 目录表（总览图不画节点） |
| 性能优化 | §4（§4.1–§4.8） | `B6`；§3.3 表 |
| 兼容 / 风险 / 证据 | §5–§8 | §5 升级影响清单、§10 风险 |

> 测试基础设施、性能优化、安装生命周期这三块在总览图里只有主题节点（`B5`/`B6`/`B3e`），完整内容只在 07；总览图不画它们的内部调用链。

## 4. Top 10 最重要的变化

排序依据是“对磁盘/CPU/数据正确性/升级兼容性的综合影响”，不是提交数量。

### 1. Compaction 从 4 队列模型变成 5 队列模型

**旧**：`scheduler` 的 `defaultWeights` 是 `[4]float64{0.4, 0.3, 0.2, 0.1}`，L4 同时承载 full 和 optimize；并且只有 `len(level4Groups) == 0` 时才会调用 `PlanOptimize()`，而 `compactFull` 硬编码 `optimize=false`，所以这些 optimize 计划实际按 full 执行（`v1.8.10:tsdb/engine/tsm1/scheduler.go:7`、`v1.8.10:tsdb/engine/tsm1/engine.go:2117`、`v1.8.10:tsdb/engine/tsm1/engine.go:2221-2222`）。
**新**：`TotalCompactionLevels = 5`，L5 是真正的 optimize，有独立权重 `0.01`、独立队列、`compactOptimize`（单世代/已优化组才用 aggressive points-per-block）、`optimizedCompactionLimiter`，以及启动 1 小时/间隔 5 分钟的 holdoff（`v1.12.3:tsdb/engine/tsm1/engine.go:107-113`、`v1.12.3:tsdb/engine/tsm1/scheduler.go:8`、`v1.12.3:tsdb/engine/tsm1/engine.go:2171-2175`、`:2529`）。
**影响**：L5 在单世代或组内已有优化文件时用 aggressive points-per-block（默认 10000）重写，其余多世代未优化组仍用 1000；索引更小但单次 compaction 更重；它不再被 full compaction 完全阻塞，但默认只允许 1 个并发 optimize，且启动后 1 小时内不**调度** L5（仍会规划）。L5 有两条入口：`PlanOptimize` 触发的 L5 需 shard 至少 `compact-full-write-cold-duration`（默认 4h）无写入；`planCompactionsInner` 对 `Plan` 产出的 L4 组做 `IsGroupOptimized`，命中即提升为 L5，**不受**冷写门槛约束——已优化 shard 被回填数据后，热 shard 上也会执行 L5。热 shard 不会被 `PlanOptimize` 选中，但升级后不要指望“重启立刻回收磁盘”。

### 2. 混合 level compaction 的 level 回退修复

**旧**：`compact()` 的 `maxGeneration`/`maxSequence` 循环在 generation 变大时把 `maxSequence` 重置为该 generation 的 sequence；配合 `tsmGeneration.level()` 的 `min(sequence, 4)`，当 L1 文件 generation 更高、L4 文件 generation 更低时，输出 sequence 可能只有 2，L4 数据被降成 L2（`v1.8.10:tsdb/engine/tsm1/compact.go:907-922`、`:182`）。
**新**：先按 generation 记录 `minSeqByGen`，计算 `maxInputLevel = max(min(minSeq, 4))`，再保证 `maxSequence + 1 >= maxInputLevel`（`v1.12.3:tsdb/engine/tsm1/compact.go:976-1011`，commit `5e9d85c972`）。
**影响**：冷写/强制 compaction 混合 L1+L4 时不会再发生 level 回退，避免数据被反复重压和磁盘放大；这是 1.12.3 才有的修复。

### 3. full compaction 在某些情况下不排期的修复

**v1.8.10**：用 `PlanLevel` + `Plan` + 可选 `PlanOptimize`，没有 `PT_*` 特例（`v1.8.10:tsdb/engine/tsm1/engine.go:2110-2121`）。**区间内回归**：1.12.1 引入的 `PT_SmartOptimize`/`PT_NoOptimize` 会让 planner 把文件标记为 in-use 却不释放，部分 TSM 文件永远不进入 full compaction，用户看到的是磁盘持续增长（commit `95e86be3f4` 的提交说明明确写了 “unbounded increase in disk space”）。**v1.12.2**（`95e86be3f4`，1.12 分支 clean backport）：移除这两个 plan type，首 block 缓存（`firstBlockCount`）承担原本的性能目的，optimize holdoff 移到 `Engine.compact`（`v1.12.3:tsdb/engine/tsm1/engine.go:2216-2235`、`v1.12.3:tsdb/engine/tsm1/reader.go:638`）。
**影响**：该 in-use 泄漏由 1.12.1 引入、1.12.2 的 `95e86be3f4` 移除。**1.12.1 升级到 1.12.3 可消除该泄漏（1.12.2 已包含 `95e86be3f4`）**；**v1.8.10→1.12.3 不会经历该回归**（v1.8.10 没有 `PT_*` 特例，由 `PlanOptimize` 兜底），跨版本改善有限（04 §5.5），不应把该修复算作 v1.8.10→1.12.3 的收益。升级后仍建议观察 `tsmFullCompactions`、`tsmFullCompactionsActive` 和 shard 目录大小；仓库无跨版本实测/基准。

### 4. Retention 的孤儿 shard 与挂起修复

**旧**：删除失败/未完成也会 prune——14 天后只要 `DeletedAt` 过期就删组，不检查 shard 是否删净，留下永远不会被 retention 清理的孤儿文件；retention 还可能卡在 `Shard.Close`，导致后续 shard 都不删（commit `7bd3f89d18`、`b4bd607eef`）。
**新**：`PruneShardGroups` 只在 `DeletedAt` 已过且 `len(sgi.Shards) == 0` 时移除；retention 删除前 `SetShardNewReadersBlocked(id, true)`，再用 `ShardInUse(id)` 跳过仍有读者的 shard（`v1.12.3:services/meta/data.go:476`、`v1.12.3:services/retention/service.go:234-251`）。
**影响**：磁盘回收更可靠，retention 不再被单个卡住的 shard 拖死；但“卡住”的 shard 会保留下来并在日志中提示人工处理。

### 5. 写入路径的 API 重构与错误语义变化

**旧**：`Engine.WritePoints(points)` 和 `Engine.WritePointsWithContext(ctx, points)`；`time` 字段被静默跳过；没有 RP 写入窗口（`v1.8.10:tsdb/engine/tsm1/engine.go:1294`、`v1.8.10:tsdb/field_validator.go:23`）。
**新**：`WriteContext{UserId}` 只贯穿 `PointsWriter`/`Store`；`StatsTracker` 由 `Store.statsTracker(db,rp,login)` 创建并传给 `Shard.WritePoints`/`Engine.WritePoints`，二者只收 tracker（`6795ec6c01` 引入显式 tracker，`dd3baf6d4a` 引入 WriteContext）；`time` 字段返回 `PartialWriteError`，但 engine 层也不再跳过该字段（字段可能落盘，见 01 §7.1）；RP 可用 `FUTURE LIMIT` / `PAST LIMIT` 拒绝窗口外写入（`v1.12.3:tsdb/store.go:1855`、`v1.12.3:tsdb/field_validator.go:45`、`v1.12.3:coordinator/points_writer.go:211-245`）。
**影响**：嵌入式/企业版调用方必须改签名；普通 HTTP 客户端**仅在**请求含字段名 `time`、点超出 RP `FUTURE LIMIT`/`PAST LIMIT`、或字符串字段 >1 MiB 时看到 400/`partial write`，其余写入行为不变（`tsdb/field_validator.go:31-56`、`coordinator/points_writer.go:237-245`）；`dropped=0` 的 `time` 字段警告要与真正丢点区分；按用户统计的写入指标依赖 `WriteContext.UserId`。

### 6. TSI 的崩溃恢复、并发与分配优化

**旧**：MANIFEST 直接 `ioutil.WriteFile` 覆盖，写到一半失败会破坏现有清单；部分打开的 TSI 可能泄漏引用；非法 series key 可能 panic；`TagValueSeriesIDCache` 每次查 map 都做 `string(...)`（`v1.8.10:tsdb/index/tsi1/partition.go:1303`、`v1.8.10:tsdb/index/tsi1/cache.go:59-67`、`v1.8.10:tsdb/index/tsi1/log_file.go:414`）。
**新**：MANIFEST 写临时文件后 `os.Rename` 原子替换；部分打开路径补齐清理和 iterator close；series key 增加边界检查；cache 的 `Put` 复用一次转换，`Get` 仍每次转换 3 次（`v1.12.3:tsdb/index/tsi1/partition.go:1490-1520`、`v1.12.3:tsdb/index/tsi1/cache.go:98-105`）。
**影响**：MANIFEST 改为临时文件 + `os.Rename`，部分打开路径补齐 iterator close（崩溃恢复的机制变化，无故障注入基准）；TSI `Put` 在缓存未命中、需要插入时复用 3 个字符串，**少 1–3 次分配**，但 `Get` 仍每次转换 3 次，`exists` 命中早退时新版反而**多 3 次**（07 §6.10，机制推演非基准）。`.tsl` 现在按大小或年龄（默认 4h）轮转，并有每小时 `runPeriodicCompaction`，`compact-full-write-cold-duration` 同时控制该年龄、默认 4h（`v1.12.3:tsdb/index/tsi1/index.go:44-48`；每小时 tick 在 `v1.12.3:tsdb/index/tsi1/partition.go:979-1000`）。但 `TagValueIterator` 在释放 RLock 后再遍历 `tagValues`，02 §7.7 已判定为 v1.12.3 的并发回归（可 fatal），建议用 race 复现触发条件（见 §10）。

### 7. 查询正确性与安全修复

**旧**：非管理员执行 `SHOW QUERIES` 可以看到其他数据库的查询；`GROUP BY ... OFFSET` 跨 DST 秋季回拨时丢行；`SHOW TAG VALUES` 跨所有 RP；array cursor 优化在 cache+TSM 混合时可能重复拷贝点（commit `2b7164440f`、`d60741b506`、`519e23b86a`、`31cb61f556`）。
**新**：`executeShowQueriesStatement` 接收 `CoarseAuthorizer` 并按库过滤；`end += Offset` 移到时区计算之前；`SHOW TAG VALUES` 限定单个 RP；array cursor 增加 `c.tsm.pos == 0` 条件（`v1.12.3:query/task_manager.go:136-144`、`v1.12.3:query/iterator.go:860`、`v1.12.3:coordinator/statement_executor.go:1127`、`v1.12.3:tsdb/engine/tsm1/array_cursor.gen.go:113`）。
**影响**：安全漏洞关闭，查询结果更正确；但 `SHOW TAG VALUES` 涉及多个不同 RP 时会从“返回并集”变成“报错”，`SHOW SERIES CARDINALITY FROM ...` 也从精确变为 HLL 近似（需要精确值时显式加 `EXACT`），依赖旧行为的脚本需要改。

### 8. Storage read 的区间语义与窗口聚合下推

**旧**：storage cursor 按 `[start, stop)` 请求，和底层 file store 的 `[start, stop]` 不一致；cursor 错误在部分路径被吞掉；没有 window aggregate 下推（commit `8edf7a4e2f`、`fe6c64b21e`、`a2154f143c`）。
**新**：`CursorRequest.EndTime` 明确为 inclusive，`newMultiShardArrayCursors` 在构造请求时做 `end - 1` 转换（`v1.12.3:tsdb/cursors/cursor.go:64-71`、`v1.12.3:storage/reads/array_cursor.go:81-90`）；cursor 错误在 tsm1 数组游标/reset 路径已上抛，但 group/filter 的 `Err()` 仍返回 nil（03 §7.1/§7.2）；`ReadWindowAggregate`/`WindowAggregate` 与 MeanCount cursor 支持窗口聚合下推（`v1.12.3:storage/flux/reader.go:85`、`v1.12.3:storage/reads/aggregate_resultset.go:50`、`v1.12.3:storage/reads/array_cursor.gen.go:199`）。
**影响**：边界点不再因半开/闭区间差异被漏掉或重复；Flux window aggregate 下推到 storage read（`ReadWindowAggregate`/`WindowAggregate` 在存储侧聚合，避免把窗口内原始点全部搬到 Flux），**减少量取决于窗口/点数，无基准**（07 §4.4）。但自定义 cursor 调用方如果仍按 `[start, stop)` 传参，需要检查是否多包含了一个边界点。

### 9. HTTP/TLS 运维能力增强

**旧**：httpd 在启动时 `tls.LoadX509KeyPair`，证书更新必须重启；`/query` 只有 `epoch`；`/debug/vars` 没有 config/stats/compact-throughput；v1.8.10 无 `/api/v2/delete`（v1.10.0 引入，初版语义 bug 已在 1.12.3 修完：nil Sources、不设 db/rp、去引号；TLS 静态加载见 `v1.8.10:services/httpd/service.go:115`，无 v2 delete 见 `v1.8.10:services/httpd/handler.go:186-243` 的路由表）。
**新**：`TLSConfigManager` 支持 SIGHUP 重载、客户端证书、证书/私钥权限检查（`v1.12.3:services/httpd/service.go:162`）；`time_format` 支持 `epoch`/`rfc3339`（`v1.12.3:services/httpd/handler.go:640`）；`/debug/vars` 增加 config/stats，`cq` 改为扁平输出（`v1.12.3:services/httpd/handler.go:2356`）；v2 delete（v1.10.0 新增）在区间内修正为 nil `Sources`、不设 db/rp、去 measurement 引号（`v1.12.3:services/httpd/handler.go:975,1075,1085`）。
**影响**：证书轮换无需重启；新增可采集项：`/debug/vars` 的 `config`（`[data]` 段 17 键）、`stats.compact-throughput-usage-percentage`、`cq` 扁平 values；`userquerybytes` 默认关闭；`merge_*` 只进 tracing span，不进 `/debug/vars`/`SHOW STATS`。但 TLS 文件权限检查、`https-insecure-certificate` 修复和多 RP 查询限制都可能改变现有部署行为。

### 10. Flux 控制器与构建基线升级

**旧**：Flux 走 `flux/control` 薄控制器，基本不限制内存/并发；构建基线是 `go 1.13`、Flux `v0.65.1`、InfluxQL `v1.1.1-0.20200828144457`、`dgrijalva/jwt-go/v4`（`v1.8.10:go.mod:3`、`:21`、`:22`）。
**新**：Flux 改走 `query/control`，提供 `[flux-controller]` 的 `query-concurrency`、`query-queue-size`、`query-max-memory-bytes`、`total-max-memory-bytes`、`query-initial-memory-bytes`（默认 0 表示不限制）；构建基线是 `go 1.24.0`/`toolchain go1.24.13`、Flux `v0.196.1`、InfluxQL `v1.4.1`、`golang-jwt/jwt/v4 v4.5.2`、`xxhash/v2 v2.3.0`（`v1.12.3:query/control/controller.go:71-110`、`v1.12.3:go.mod:3-5`、`:21-23`、`:13`、`:17`）。
**影响**：可以显式治理 Flux 查询的并发和内存，但不配置时仍接近旧行为；构建机必须安装 Go 1.24。Flux/InfluxQL 大版本升级带来可核对的 5 项差异：① InfluxQL v1.4.1 新增 `count_hll`/`sum_hll`/`merge_hll`，`CREATE/ALTER RETENTION POLICY` 支持 `FUTURE LIMIT`/`PAST LIMIT`（`FUTURE` 必须写在 `PAST` 前）；② `SHOW RETENTION POLICIES` 由 5 列变 7 列（新增 `futureWriteLimit`/`pastWriteLimit`）；③ `SHOW TAG VALUES` 涉及多个不同 RP 时报错（仅 TSI）；④ 非 `EXACT` 的 `SHOW SERIES CARDINALITY` 改为 HLL 近似（标准误差 0.40625%）；⑤ CSV 的 tag 不再转义，`/query` 新增 `time_format`。

---

## 5. 升级影响清单

### 5.1 会影响运维/配置/数据格式/API 的变化

| 影响面 | 变化 | 升级动作 / 风险 | 证据 |
|---|---|---|---|
| 配置 | 新增 `max-concurrent-optimized-compactions`，默认/非法值按 1 处理，且不超过 `max-concurrent-compactions` | 如果旧配置没有该项，行为等于 1；需要更高 optimize 并发时显式设置，但不要超过总 compaction 上限 | `v1.12.3:tsdb/config.go:159-162`、`v1.12.3:tsdb/store.go:513-524` |
| 运维 | `PlanOptimize` 触发的 L5 需同时满足：启动 holdoff 1h、两次间隔 ≥5m、shard 冷写 ≥4h；holdoff 内仍会规划但不调度；另有 `IsGroupOptimized` 把已优化的 L4 组提升为 L5，不受冷写门槛约束 | 重启后磁盘空间不会立刻回收；热 shard 不会被 `PlanOptimize` 选中，但已优化 shard 回填后可能被提升为 L5；容量告警窗口要相应放宽 | `v1.12.3:tsdb/engine/tsm1/engine.go:2171-2175`、`tsdb/engine/tsm1/compact.go:452`、`tsdb/config.go:38` |
| 运维 | TLS 证书支持 SIGHUP 重载、客户端证书、权限检查；`https-insecure-certificate` 修复 | 证书轮换流程可从“重启”改为“发 SIGHUP”；确认证书/私钥文件权限满足新检查 | `v1.12.3:services/httpd/service.go:162`、commit `aa1775bfc2`、`049b1f7e00`、`ba705ec658` |
| SQL/DDL | `CREATE/ALTER RETENTION POLICY` 支持 `FUTURE LIMIT` / `PAST LIMIT`；`SHOW RETENTION POLICIES` 增加两列 | 新语句需要 InfluxQL v1.4.1；旧客户端如果按固定列数解析 `SHOW RETENTION POLICIES` 会受影响 | `v1.12.3:coordinator/statement_executor.go:852-871`、commit `07c261a21a` |
| 写入 API | 窗口外的点返回 `PartialWriteError`，错误里带 database/RP 与 dropped 数 | 客户端要按 400/partial write 处理；错误文本会区分 retention policy 与 write window；如需按原因监控，只能采集 400 错误文本或监控 `write.writeDrop` | `v1.12.3:coordinator/points_writer.go:211-245`、`:314-350`、`:516-521` |
| 写入校验 | 新增 `MaxFieldValueLength = 1 MiB`，字符串字段超限返回 `PartialWriteError`；`skip-field-size-validation` 可关闭 | 超大字符串字段从成功变 400；检查客户端/导入工具 | `v1.12.3:tsdb/field_validator.go:12-36`、`v1.12.3:tsdb/config.go:120`、commit `6d22e69ef1` |
| HTTP 查询 API | 新增 `time_format=epoch / rfc3339`，默认 `epoch`；CSV 的 tag 不再转义 | 默认行为不变；显式用 `time_format` 的客户端要确认 `epoch` 参数是否被忽略；CSV 解析器要能接受未转义 tag | `v1.12.3:services/httpd/handler.go:640`、`:810`、`v1.12.3:services/httpd/response_writer.go:228` |
| SQL 查询语义 | `SHOW TAG VALUES` 涉及多个不同 RP 时报错；INMEM 索引**仍会**跨 RP 返回并集（该限制只对 TSI 生效） | 检查依赖“跨 RP 并集”的脚本；INMEM shard 共享索引，**该限制不生效**，不能靠它阻止跨 RP 返回 | `v1.12.3:coordinator/statement_executor.go:1127-1145`、commit `519e23b86a` |
| SQL 查询语义 | 非管理员 `SHOW QUERIES` 只看到有权限的库 | 监控账号如果非管理员，看到的行数会变少；这是安全修复，不建议回退 | `v1.12.3:query/task_manager.go:136-144`、commit `2b7164440f` |
| SQL 查询结果 | `SHOW SERIES CARDINALITY FROM ...`（非 `EXACT`）从精确 `count(distinct(_seriesKey))` 变为 HLL 近似 | 结果数值会变（HLL 标准误差 `1.04/sqrt(2^16)=0.40625%`，250 个 series 约 ±1.0）；需要精确值显式加 `EXACT` | `v1.12.3:query/statement_rewriter.go:219-243`、commit `de1a0eb2a9` |
| 元数据/shard group | `CreateShardGroup` 与已有 group 求差集，保证不重叠；`DropShard` 不再覆盖首次 `DeletedAt` | 同一时间戳可能落到不同 StartTime/EndTime 的 group；依赖固定 shard group 边界的脚本需回归 | `v1.12.3:services/meta/data.go:382`、`:299`、commit `3c811bcd81`、`7bd3f89d18` |
| V2 API | v1.8.10 无 `/api/v2/delete`；v1.10.0 引入，区间内修正 nil `Sources`、db/rp、引号；`/api/v2/buckets` 新增部分 CRUD | 对从 1.8.10 升级的用户是新增能力，不存在“兼容旧 bug”；delete 仍不支持按 field 删除 | commit `f365bb7e3a`、`c31e99574f`、`e3e9232de5`、`2fe52d8edc`、`ce40acb298` |
| 数据格式 | **未见 TSM/WAL 文件格式版本升级**；TSI MANIFEST 内容格式未变，写入改为临时文件 + rename（进程崩溃安全；rename 后未 `SyncDir`，掉电可能回退到旧 MANIFEST 且引用已删除文件，见 02 §7.9）；meta protobuf 追加 optional 字段 7/8 | 不需要离线重写 TSM；新旧 meta 快照按 protobuf 兼容方式读取；仍建议升级前备份 meta | `v1.12.3:tsdb/engine/tsm1/writer.go:82,85`（magic `0x16D116D1`、version 1，两版相同）；`git diff v1.8.10 f3d5cb68ab -- tsdb/engine/tsm1/encoding.go tsdb/engine/tsm1/wal.go` 是代码整理/WAL 配置生效，非格式变化；MANIFEST 权限收紧为 0644；`v1.12.3:tsdb/index/tsi1/partition.go:1490-1520`；`v1.12.3:services/meta/internal/meta.pb.go:509-521` |
| 字段元数据 | `fields.idx` 仍是 protobuf，但新增 `fields.idxl` 增量日志，启动时 ApplyChanges / Close / LoadMetadataIndex 重建 / Restore·Import 时合并回 `fields.idx` | 升级不需要重建；降级到不识别 `.idxl` 的旧版本前先确认合并/备份，否则升级后的字段变更可能不可见；端到端回滚案例见 §9.5 | `v1.12.3:tsdb/shard.go:51`、commit `80c10c8c04` |
| Flux 资源 | 新增 `[flux-controller]`：`query-concurrency`、`query-queue-size`、`query-max-memory-bytes`、`total-max-memory-bytes`、`query-initial-memory-bytes`（`query/control/controller.go:81`）；默认 0 表示不限制 | 不配置时行为接近旧版；配置后 Flux 查询可能排队或被拒绝 | `v1.12.3:query/control/controller.go:71-110`、`v1.12.3:cmd/influxd/run/config.go:45` |
| Go API | `Engine.WritePoints` 与 `Shard.WritePoints` 增加 `StatsTracker`；`Store.WriteToShard` 改为 `WriteContext`；`SeriesFile.DeleteSeriesID` 增加 `flush bool` 并返回 partition，`CreateSeriesListIfNotExists` 增 `StatsTracker`；`Store.MeasurementNames` 增 `retentionPolicy`；`flux/control` 整包移除（→`query/control`）；Flux storage `ReadFilter`/`ReadGroup` 增 `ReadWindowAggregate` 且 `alloc` 改值类型；`pkg/limiter.Rate` 增 `Tokens()`/`Limit()`；`Tags.HashKey(bool)`/`AppendHashKey(dst, bool)` | 嵌入式/企业版/自定义工具需要改编译；不要继续调用已移除的 `WritePointsWithContext`、`WriteToShardWithContext`，以及已删除的 `ConetextKey`/`StatPointsWritten`/`StatValuesWritten`；完整清单见 01 §5.2 / 02 §5.5 | `v1.8.10:tsdb/engine/tsm1/engine.go:1294`、`v1.8.10:tsdb/shard.go:500`、`v1.8.10:tsdb/store.go:1412`、`v1.8.10:tsdb/series_file.go:198`；`v1.12.3:tsdb/engine/tsm1/engine.go:1346`、`v1.12.3:tsdb/shard.go:562`、`v1.12.3:tsdb/store.go:1861`、`:1905`、`v1.12.3:tsdb/series_file.go:182,203`、`v1.12.3:storage/flux/reader.go:65-91`、`v1.12.3:pkg/limiter/writer.go:18-23`、`v1.12.3:models/points.go:2211,2229` |
| 构建 | Go 1.24、Flux 0.196.1、InfluxQL 1.4.1、protobuf 工具链、JWT v4，所有目标 `CGO_ENABLED=1`；产物收窄为 `influx`/`influx_inspect`/`influxd`，架构仅 linux-amd64/arm64、darwin-amd64、windows-amd64 | 源码/自建包需 Go 1.24 + CGO 工具链；Linux 还需 Rust `libunwind` 与 cross-builder 镜像提供的 `xcc`/`find_rustlib_path`，脱离镜像自建 CI 要复刻 musl/Rust 工具链（07 §3.3、§3.4、§3.7、§7 风险 2）；CI 构建机升级 Go；检查 Flux/InfluxQL 行为差异；不再产出 i386/armhf/armel/static_* 与 influx_tools/influx_stress，使用这些产物的部署需自建或换架构；仓库根 `Dockerfile`/`appveyor.yml` 仍指向旧工具链，不能用于 1.12.3 构建；发布路径为 `releases/<tag>/`，`logger.Level` 类型已变化；`compact-throughput` 日志键由 `throughput_bytes_per_second` 改名（`82729cf8ff`），按旧键的告警/grep 会静默失效；安装生命周期（`buildtsi`、`post-uninstall`、`logrotate`）见 07 §3.8/§3.10.5 | `v1.12.3:go.mod:3-5`、`:21-23`、`:57`；`.circleci/config.yml:149-179`；07 §3.3/§7 风险 2；`v1.8.10:go.mod:3`、`:21-22` |
| 监控 | 新增 `/debug/vars` 的 config/stats 与查询字节指标；新增 merge 指标（`merge_*` 仅查询启用 tracing 时写入 span）；`tsmOptimizeCompactions*` 由恒 0/L4 混用变为真实 L5 指标；注意 cq 形状变化、`httpd.valuesWrittenOK` 被删除 | 监控采集器可以增加 `/debug/vars` 的 config/stats key；`merge_*` 不进 `/debug/vars`/`SHOW STATS`、无跨请求聚合，需要查 tracing span（07 §3.9.6）；`tsmOptimizeCompactions*` 的看板/告警需按 L5 重新配阈值（04 §5.3）；注意 `SHOW STATS` 的 per-user 项可能增加基数；旧面板需删 `valuesWrittenOK` | `v1.12.3:services/httpd/handler.go:2356`、commit `133c290fc2`、`4dcc8776c1` |

> 上表之外还需检查 4 类变化：SIGHUP 语义反转（旧版默认终止进程）；订阅 `Subscriber` API 与 `total-buffer-bytes`；WAL 段大小配置生效；systemd 启动 `buildtsi`。监控面板还要补删 `httpd.valuesWrittenOK`。

### 5.2 不改变磁盘格式/HTTP 契约（数据不迁移，但两项必须回归）

这些变化主要改调度、缓存、锁和内存分配，不改变磁盘格式或 HTTP 契约。

**数据迁移：不需要。行为回归：需要，且不能归入“通常不需要”。**

1. **需要修复/等上游：`TagValueIterator` 并发 `fatal`。** 新版查到 `logTagKey` 后立即释放 RLock，随后在锁外 `range tk.tagValues`；并发写入在 `f.mu` 写锁下更新同一 map，可触发 `fatal error: concurrent map iteration and map write`（`recover` 无效）。高并发写入 + TSI 查询场景先用 `go test -race` 复现（02 §7.7、本文 §10）。
2. **需要评估：retention 删除的 fsync 窗口后移。** v1.8.10 删除 series 只 `Flush()`、不 `file.Sync()`（0 次 fsync）；区间内先改为每次删除 fsync，再改为 `NoFlush` + `FlushSegments` 按 partition 批量 fsync（每条删除最多 8 个 SeriesFile 分区，`SeriesFilePartitionN=8`）。相对 v1.8.10 是新增持久化保障，相对“每次 fsync”的中间版本则把持久化点后移；对删除持久性敏感的场景做掉电测试（02 §3.9.3、本文 §10）。另有写路径：建新 series 的请求每个 (TSI 分区, SeriesFile 分区) 组合至多一次 fsync（单 shard 上限 8×8=64，跨 shard 叠加；v1.8.10 为 0，02 §3.9.3）。

**其余机制变化（无需迁移）**：

- TSM 文件名 generation/sequence 缓存（`f0d5c73253`）与 first-block 缓存（`2a0353ffd2`）：减少重复 `parseFileName` 与规划期 block 解码，无基准。
- 5 级 scheduler、L5 独立 limiter/holdoff（`f738344a81`、`0fdb6adcb2`）：L5 权重 `0.01`、启动 holdoff 1h、间隔 5m，默认 1 个并发 optimize。
- purger 并发 map 与缩小锁区（`d5a48e0662`）：删除文件列表从全局 mutex 改为并发 map，缩短临界区。
- TSI series cache 字符串复用（`f3d5cb68ab`）：插入路径少 1–3 次分配，`exists` 命中早退时多 3 次；`Get` 路径未优化（07 §6.10）。
- file store merge 指标（`133c290fc2`）：`merge_*` 仅查询启用 tracing 时写入 span，不进 `/debug/vars`/`SHOW STATS`。
- compaction 属性测试（`ba2c2b6432`）：测试基础设施，不改变运行时行为。

> 这些项的机制、commit 与“为什么没有基准数字”见 [07 构建与性能](07_build_perf_diff.md) §4；旧 `perf_test` 的 AWS/S3 回归见 07 §3.10.5。

---

## 6. 推荐阅读路径

### 6.1 只看存储（compaction / TSM / FileStore）

问题：**磁盘为什么涨/不涨？compaction 到底怎么排期？L4/L5 有什么区别？**

1. [04 存储与 Compaction](04_storage_compaction_diff.md)：先看 4→5 级模型、Scheduler、Planner、level 回退与 full 调度修复。
2. [01 写入链路](01_write_path_diff.md)：看 Cache snapshot 如何产生 L1 文件，以及写入错误如何回到客户端。
3. [02 索引与 Series](02_index_series_diff.md)：TSI/SeriesFile 的删除、flush 与 compaction 边界。
4. 主文档：[01 Write Engine](../01_write_engine_spec.md)、[04 Background Tasks](../04_background_tasks_spec.md)、[16 FileStore + KeyCursor](../16_filestore_keycursor_spec.md)、[17 TSM File Format](../17_tsm_file_format_spec.md)。

### 6.2 只看查询（InfluxQL / array cursor / storage reads）

问题：**同一个查询为什么结果变了？是 bug 修复还是语义变化？**

1. [03 查询引擎](03_query_engine_diff.md)：DST、`SHOW TAG VALUES`、`SHOW QUERIES`、array cursor、window aggregate。
2. [02 索引与 Series](02_index_series_diff.md)：TSI 锁、series key 边界、series cache。
3. [06 HTTP 与 Flux](06_http_flux_diff.md)：`/query` 的 `time_format`、CSV 输出、Flux 入口。
4. 主文档：[03 Query Engine](../03_query_engine_spec.md)、[08 Array Cursor](../08_array_cursor_spec.md)、[09 Storage Reads](../09_storage_reads_spec.md)、[11 InfluxQL Execution](../11_influxql_execution_spec.md)。

### 6.3 只看运维/API（retention / TLS / debug / v2 API）

问题：**升级后运维流程要不要改？监控项和 API 有没有变？**

1. [05 元数据与后台](05_meta_background_diff.md)：retention 孤儿 shard/挂起、PruneShardGroups、shard 生命周期、CQ。
2. [06 HTTP 与 Flux](06_http_flux_diff.md)：TLS 热重载、`time_format`、`/debug/vars`、v2 delete/buckets。
3. [07 构建与性能](07_build_perf_diff.md)：Go/Flux/InfluxQL 升级、构建链、观测项。
4. 主文档：[10 HTTP Service](../10_http_service_spec.md)、[12 Background Services](../12_background_services_spec.md)、[07 State Machine](../07_state_machine_spec.md)。

### 6.4 做升级评估（从 1.8.10 升到 1.12.3）

建议按“风险从高到低”读，不要从 01 顺读：

1. 本文 [§5 升级影响清单](#5-升级影响清单)：先确认配置、API、数据格式、Go API 的影响。
2. [04 存储与 Compaction](04_storage_compaction_diff.md)：确认 level 回退、full 调度、L5 holdoff 对磁盘/CPU 的影响。
3. [05 元数据与后台](05_meta_background_diff.md)：确认 retention 行为、孤儿 shard、CQ。
4. [01 写入链路](01_write_path_diff.md)：确认 `WriteContext`、`time` 字段、RP 写入窗口。
5. [06 HTTP 与 Flux](06_http_flux_diff.md)：确认 TLS、`time_format`、v2 API、CSV。
6. [03 查询引擎](03_query_engine_diff.md)：确认 `SHOW TAG VALUES`、`SHOW QUERIES`、DST、cursor 边界。
7. [07 构建与性能](07_build_perf_diff.md)：确认 Go 1.24 与依赖升级。

最后按 [§9.4](#94-案例-d从-v1810-升级首启同一个数据目录) 做一次升级首启、按 [§9.5](#95-案例-e从-v1123-回滚到-v1810fieldsidxl-与-meta-写窗口) 做一次回滚演练；两条路径都给出了可核对的输出与失败判据。

---

## 7. 证据与方法

### 7.1 复核命令

```bash
# 1. 确认两个基线
git rev-parse 'v1.8.10^{}' v1.12.3

# 2. 看区间规模
git diff --shortstat v1.8.10 f3d5cb68ab

# 3. 看某个子系统的提交
git log --oneline v1.8.10..f3d5cb68ab -- tsdb/engine/tsm1

# 4. 看旧版某段代码（带行号）
git show v1.8.10:tsdb/engine/tsm1/compact.go | nl -ba | sed -n '176,188p'

# 5. 看新版同一段代码（tag 与工作树等价）
git show v1.12.3:tsdb/engine/tsm1/compact.go | nl -ba | sed -n '219,231p'

# 6. 确认工作树只多了 docs 提交
git diff --stat f3d5cb68ab HEAD -- '*.go'
# 输出为空
```

### 7.2 引用约定

- 旧版本：`v1.8.10:path:line`，例如 `v1.8.10:tsdb/engine/tsm1/engine.go:2117`。
- 新版本：`v1.12.3:path:line`，例如 `v1.12.3:tsdb/engine/tsm1/engine.go:2216`。
- commit：用短 SHA，例如 `5e9d85c972`；需要完整信息时用 `git show -s --format='%H %ad %s' --date=short <sha>`。
- 代码块/表格里的行号以 `git show v1.8.10:<path> | nl -ba` 和当前工作树 `nl -ba` 为准。
- 对“没有变化”的结论，必须给出对应的 `git diff` 无输出或 `git log` 无提交作为证据；没有做穷尽审查的领域会标“待核实”。

### 7.3 方法与边界

1. 先用 `git log --oneline v1.8.10..f3d5cb68ab -- <dir>` 找候选提交，再用 `git show --stat` 排除纯文档/纯测试提交。
2. 对每个候选提交，至少核对一处旧代码和一处新代码；只根据 commit message 的结论会标“提交说明依据”。
3. **不做基准测试推断**：本文不写“提升 X%”这类数字，除非 commit/测试里有可复核的数据；没有就只解释机制。
4. 本文是总览，不追求列全 527 个提交；未列出的提交可能是小重构、日志、测试或依赖微调。七篇子文档才是逐项清单。
5. 版本边界：NEW 的代码基准是 `f3d5cb68ab`；当前工作树在其上只有若干 docs 提交（数量随文档演进，不逐一列举），`git diff f3d5cb68ab HEAD -- '*.go'` 为空。

---

## 8. 统一术语表

| 术语 | 含义 | v1.8.10 | v1.12.3 | 主要文档 |
|---|---|---|---|---|
| Compaction level | 文件名编码的层级 `level = min(sequence, 4)`（上限 L4）；L5 只是调度/规划层级 | 有 L1–L4 概念，optimize 复用 L4 | 有 L1–L5 概念，L5 专指 optimize（`OptimizeCompactionLevel = 5`，调度用；输出文件层级仍 ≤ L4） | [04](04_storage_compaction_diff.md)、[17](../17_tsm_file_format_spec.md) |
| 4 级 vs 5 级 | 调度器建模的队列数量 | `scheduler` 的 `queues/weights` 是 `[4]`，L4 同时放 full + optimize | `TotalCompactionLevels = 5`，L4 full、L5 optimize 分开 | [04](04_storage_compaction_diff.md) |
| generation | TSM 文件名第一段，表示一次创建批次 | 每次 `parseFileName` 从路径解析 | 打开/重命名时缓存到 `TSMReader.generation`，经 `Stats()` 暴露为 `FileStat`（`ExtFileStat` 内嵌） | [04](04_storage_compaction_diff.md)、[17](../17_tsm_file_format_spec.md) |
| sequence | TSM 文件名第二段，编码 level | 每次 `parseFileName` 解析 | 缓存到 `TSMReader.sequence`（经 `Stats()` 暴露为 `FileStat`），compaction 输出用 `minSeqByGen` 防回退 | [04](04_storage_compaction_diff.md)、[17](../17_tsm_file_format_spec.md) |
| WriteContext | 写入请求的请求级上下文，目前只含 `UserId` | 不存在；用 `context.Context` + context value 传统计 | `tsdb.WriteContext{UserId}` 贯穿 `PointsWriter`/`Store`；`Shard`/`Engine` 只收 `StatsTracker` | [01](01_write_path_diff.md)、[06](../06_cluster_communication_spec.md) |
| StatsTracker | 写入统计回调集合 | 不存在；统计通过 `context.Context` 的指针传递 | `tsdb.StatsTracker`，支持 `AddedPoints` 等回调 | [01](01_write_path_diff.md) |
| optimizedCompactionLimiter | 限制并发 optimize compaction 的 `limiter.Fixed` | 不存在；optimize 与 full 共用总 limiter | 独立 limiter，默认 1，且不超过总 compaction 并发 | [04](04_storage_compaction_diff.md) |
| L5 optimize | 只重写/重整索引、提高 points-per-block 的 compaction | 作为 L4 optimize 路径规划，但 `compactFull` 硬编码 `optimize=false`，实际按 full 执行 | 独立 L5，`compactOptimize` + `PointsPerBlock` + holdoff | [04](04_storage_compaction_diff.md) |
| Array cursor | storage read 的批量类型化读取抽象 | 底层存储是闭区间，但旧版把半开 stop 原样传入（会多返回 stop 点）；错误可能被吞 | `EndTime` inclusive，构造时统一 `end-1`，对 Flux 对外仍是 `[start, stop)`（InfluxQL 不走该路径）；tsm1 数组游标已上抛错误，group/filter 的 `Err()` 仍为 nil | [03](03_query_engine_diff.md)、[08](../08_array_cursor_spec.md) |
| TSI | Time Series Index，按 measurement/tag 查 series 的索引 | MANIFEST 非原子写，部分打开清理不完整 | MANIFEST 原子替换、清理补齐、series cache 减少分配 | [02](02_index_series_diff.md)、[14](../14_series_file_deep_dive_spec.md) |
| SeriesFile | series key 到 series ID 的持久化身份系统 | `DeleteSeriesID(id)` 每条只 `Flush()` 缓冲，无 `file.Sync()` | `DeleteSeriesID(id, flush)` + `FlushSegments` 批量 fsync（区间内 `b4e781eff6` 先加 Sync） | [02](02_index_series_diff.md)、[14](../14_series_file_deep_dive_spec.md) |
| fields.idxl | `fields.idx` 的增量变更日志 | 不存在；每个含新字段的写请求全量重写 `fields.idx` | 变更追加到 `fields.idxl`；启动时 ApplyChanges / Close / LoadMetadataIndex 重建 / Restore·Import 合并回 `fields.idx`（删除 measurement 的变更不落盘，02 §7.8） | [01](01_write_path_diff.md)、[02](02_index_series_diff.md) |
| MaxFieldValueLength | 单个字段值的长度上限 | 无限制 | 1 MiB；超过返回 `PartialWriteError`，`skip-field-size-validation` 可关闭 | [01](01_write_path_diff.md)、[13](../13_line_protocol_parser_spec.md) |
| Flux 查询控制器 | Flux 查询的并发/内存/队列管理 | `flux/control` 薄壳，基本不限制 | `query/control`，`[flux-controller]` 可配置并发、队列、内存配额 | [03](03_query_engine_diff.md)、[06](06_http_flux_diff.md) |
| PartialWriteError | 部分写入错误，含 `Reason`、`Dropped`、db/rp | 主要用于丢点/类型冲突 | 新增 `time` 字段错误、RP 写入窗口、带 db/rp 的上下文 | [01](01_write_path_diff.md)、[10](../10_http_service_spec.md) |
| RP 写入窗口 | retention policy 允许的时间范围 | 不存在 | `FUTURE LIMIT` / `PAST LIMIT`，由 `WriteWindow.WithinWindow` 判定 | [01](01_write_path_diff.md)、[07](../07_state_machine_spec.md) |
| TLSConfigManager | 管理 TLS 配置与证书热重载的组件 | 不存在；启动时静态 `tls.LoadX509KeyPair` | `pkg/tlsconfig.TLSConfigManager`，支持 SIGHUP、client cert、权限检查；TLS 1.3 两版均可配置 | [06](06_http_flux_diff.md)、[10](../10_http_service_spec.md) |
| CursorRequest | storage cursor 的请求结构 | `StartTime`/`EndTime` 语义未注释，按半开区间使用 | 明确 `StartTime`/`EndTime` 均为 inclusive | [03](03_query_engine_diff.md)、[08](../08_array_cursor_spec.md) |

---

## 9. 具体案例

**案例索引**（先按问题定位；没有“本文案例”的行直接去子文档）：

| 问题 / Top 10 项 | 本文案例 | 子文档案例 |
|---|---|---|
| 混合 level compaction 把 L4 降成 L2（Top 2） | §9.1 | 04 §6.2 |
| RP 写入窗口拒绝“太老”的点（Top 5） | §9.2 | 01 §6.3（`time` 字段）、05 §3.15c（CQ 补算） |
| `time` 字段从静默消失到 400（Top 5） | §9.3 | 01 §6.3 |
| 从 v1.8.10 升级首启 / 回滚（§5 升级清单、Top 10 构建基线） | §9.4、§9.5 | 01 §5.3、05 §7.8、07 §6.1 |
| DST 跨回退丢行（Top 7） | — | 03 §6.1 |
| `SHOW TAG VALUES` 的跨 RP 语义（Top 7） | — | 03 §6.2 |
| TLS 证书热重载（Top 9） | — | 06 §6.3 |
| `/debug/vars` 新旧输出（Top 10） | — | 06 §6.4、07 §6.6 |
| Flux 并发排队与 500（Top 10） | — | 06 §6.5 |
| retention 删除 / 孤儿 shard（Top 4） | — | 05 §6.1、05 §6.3 |
| TSI / Series 删除（Top 6） | — | 02 §6.2 |
| 5 级调度与 purger 时序（Top 1） | — | 04 §6.1、04 §6.5 |


### 9.1 案例 A：混合 level compaction 为什么把 L4 降成了 L2

**输入**（两个 TSM 文件，文件名格式 `generation-sequence.tsm`）：

| 文件 | generation | sequence | 推导 level |
|---|---:|---:|---:|
| `000000001-000000004.tsm` | 1 | 4 | L4 |
| `000000002-000000001.tsm` | 2 | 1 | L1 |

**旧版推演**（`v1.8.10:tsdb/engine/tsm1/compact.go:907-922` 的 max 逻辑，配合 `:182` 的 `level()`）：

1. 遍历文件，`maxGeneration = 2`；当 generation 从 1 变到 2 时，`maxSequence` 被重置为该 generation 的 sequence，即 1。
2. 输出文件名使用 `maxGeneration=2`、`maxSequence+1=2`，得到 `000000002-000000002.tsm`。
3. 下次 `tsmGeneration.level()` 读到 sequence=2，判定为 **L2**。原本的 L4 数据被“降级”。

**新版推演**（`v1.12.3:tsdb/engine/tsm1/compact.go:976-1011`）：

1. `minSeqByGen = {1: 4, 2: 1}`。
2. `maxInputLevel = max(min(4,4), min(1,4)) = 4`。
3. `maxSequence+1 = 2 < 4`，所以把 `maxSequence` 提升到 3；输出 sequence=4，仍是 **L4**。

**落地结果与回归对应**：旧版输出 `000000002-000000002.tsm`（seq=2，L2）；新版输出 `000000002-000000004.tsm`（seq=4，L4）。修复 commit 是 `5e9d85c972`（#27233）；回归测试是 `TestCompactor_CompactFull_MixedLevelNoRegression`（`v1.12.3:tsdb/engine/tsm1/compact_test.go:272-311`），它用 4 个输入文件断言 `gotGen=4` 且 `gotSeq>=4`，本案例的 2 文件输入是同一不变量的最小化。

**影响**：旧版在冷写/强制 compaction 混合高低 level 文件时会制造 level 回退，后续 planner 可能把这份数据当 L2 再压一遍，造成额外写放大和磁盘占用。新版保证“输出 level 不低于输入最高 level”，**仅当旧版会把 L4 降为 L2 时**，新版单次 compaction 承载的数据量更多、更重；不是所有输入都更大，且无基准；CPU/IO 观察项要一起看。

### 9.2 案例 B：RP 写入窗口如何拒绝一个“太老”的点

**DDL**（InfluxQL v1.4.1 语法）：

```sql
CREATE RETENTION POLICY rp ON db DURATION 24h REPLICATION 1
  FUTURE LIMIT 15m PAST LIMIT 10m;
```

> InfluxQL v1.4.1 的 `CREATE RETENTION POLICY` 语法要求 `FUTURE` 在 `PAST` 之前
> （`parser.go:281-296`）；反过来写会报 `found FUTURE, expected ;`。

**写入**：一个时间戳为 `now - 20m` 的点，**同一请求里还有一个窗口内、且与它落在同一 shard group 的点**（24h RP 默认 shard group 为 1h；把 `now` 取在整点后 30 分钟，窗口内点取 `now - 5m` 即可保证同组）。关键点是 `MapShards` 的 `list` 只装本次请求里新创建/取回的 shard group，不预载库里已有的组。

**旧版**：`MapShards` 只检查 `p.Time().Before(min)`（`min = now - rp.Duration`）和 shard group 覆盖范围；`now-20m` 在 24h 内，所以会被接受并写入（`v1.8.10:coordinator/points_writer.go:206-210`）。

**新版**：

1. `NewWriteWindow(rp)` 记录 `before = now - 10m`、`after = now + 15m`（`v1.12.3:coordinator/points_writer.go:211-245`）。
2. `WithinWindow(now-20m)` 发现 `checkBefore && t.Before(before)`，返回 `false, before, WriteWindowLowerBound`。
3. 第一轮 `MapShards` 对 `now-20m` 直接 `continue`、不建组；同请求里同组的 `now-5m` 建出覆盖该小时的组并 `list.Add`（`v1.12.3:coordinator/points_writer.go:314-321`）。
4. 第二轮 `list.ShardGroupAt(now-20m)` 命中该组，再判一次 `WithinWindow` 失败 → `AddDropped(p, before, WriteWindowLowerBound)`，`WriteWindowDropped=1`（`:337-350`）。
5. `WritePointsPrivileged` 返回 `PartialWriteError`，reason 由 `SummariseDropped()` 生成：`dropped 0 points outside retention policy of duration 24h0m0s and 1 points outside write window (-10m0s to 15m0s) - ...`（`v1.12.3:coordinator/points_writer.go:516-521`）。

> 反例：库里只有这一个点（或空库）时，第一轮不会建组，第二轮 `sg == nil` → `AddDropped(p, min, RetentionPolicyBound)`，即 `RetentionDropped=1`、`WriteWindowDropped=0`，错误文本是 “outside retention policy”，不是 “outside write window”。

**影响**：客户端会收到 400/partial write，必须决定重试、丢弃还是修正时间戳；监控上只有合并后的 `write.writeDrop`；错误文本会区分 retention policy 与 write window（单点/空库会落到前者），要按原因区分只能采集 400 错误文本。这不是“默认开启”的行为，只有显式设置 `PAST LIMIT` / `FUTURE LIMIT` 才会生效。

### 9.3 案例 C：`time` 字段从“静默消失”到“明确报错”

**输入**：`cpu time=1,value=2`（字段名 `time` 与时间戳字段冲突）。

- 旧版：`defaultFieldValidator.Validate` 遇到 `time` 字段直接 `continue`，返回 `nil`；点仍然写入，客户端完全不知道字段被丢了（`v1.8.10:tsdb/field_validator.go:23-26`）。
- 新版：遇到 `TimeBytes` 时构造 `PartialWriteError{Reason: "invalid field name: ... Field \"time\" has been stripped from point.", Dropped: 0}`；点仍会写入，但 `Shard.WritePoints` 会把该错误返回给 writer（`v1.12.3:tsdb/field_validator.go:45-57`、`v1.12.3:tsdb/shard.go:728-731`）。
- 影响：HTTP 客户端第一次可能看到 `partial write ... dropped=0`；这是“提醒”而不是“整点丢弃”，但足以让严格校验的写入端失败。

**已确认（源码）**：

- HTTP 返回 400 + `partial write ... dropped=0`；点不被丢弃（`Dropped: 0`，`v1.12.3:tsdb/field_validator.go:45-53`）。
- `ValidateAndCreateFields` 在 `time` 分支 `continue`，不会把 `time` 加进字段集合，所以 `SHOW FIELD KEYS FROM cpu` **不含** `time`（`v1.12.3:tsdb/field_validator.go:43-56`）。
- engine 层删除了跳过 `time` 的 `continue`（`v1.8.10:tsdb/engine/tsm1/engine.go:1330-1333` → `v1.12.3:tsdb/engine/tsm1/engine.go:1366-1367`），所以 `time` 会作为普通 field key 进入 `values`/TSM，而不是被 engine 丢掉。

**待回归（运行时）**：源码只证明“engine 不再 skip”，不证明查询层如何呈现。写后执行：

```bash
influx -database db0 -execute 'INSERT cpu,host=A time=1,value=2'
influx -database db0 -execute 'SHOW FIELD KEYS FROM cpu'
# 预期：只有 value float（time 未进字段集合）
influx -database db0 -execute 'SELECT "time" FROM cpu'
# 预期：返回保留的时间维度（与 SELECT * 的 time 列同一列），读不到孤儿字段值；
#       SELECT * 的 time 列不是该字段，不能作为判据
influx_inspect dumptsm -index -filter-key time \
  /var/lib/influxdb/data/db0/autogen/<shard>/<file>.tsm
# 预期：若出现 cpu,host=A#!~#time，说明字段已落盘到 TSM
```

01 §7.1 仍把“TSM/查询层是否出现 `time`”标为待回归；本案例不把“字段已被剥离”当成既成事实。

---

### 9.4 案例 D：从 v1.8.10 升级首启（同一个数据目录）

**输入**：

- v1.8.10 实例：`/var/lib/influxdb/{data,wal,meta}`；先造一个可核对的最小数据集：
  ```sql
  CREATE DATABASE db0;
  INSERT cpu,host=A value=1 1700000000000000000
  INSERT cpu,host=B value=2 1700000000000000000
  ```
- 升级包 `influxdb_1.12.3_amd64.deb`；升级前记录基线并备份：
  ```bash
  influx -execute 'SHOW SERIES CARDINALITY ON db0'          # 预期 2
  influx -database db0 -execute 'SHOW FIELD KEYS FROM cpu'  # 预期 value float
  influx -execute 'SHOW RETENTION POLICIES ON db0'          # 5 列：name/duration/shardGroupDuration/replicaN/default
  find /var/lib/influxdb/data/db0 -name '*.tsm' -printf '%P %s\n' | sort > /tmp/tsm.before   # 仅作参考，不作为升级失败判据
  cp -a /var/lib/influxdb /var/lib/influxdb.bak-1810
  ```

**步骤**：

1. `systemctl stop influxdb`（SIGTERM，等进程退出；v1.8.10 没有 `fields.idxl`，不需要合并）。
2. `dpkg -i influxdb_1.12.3_amd64.deb`；`systemctl start influxdb`。
3. 按 07 §6.3 核对启动：`influxd-systemd-start.sh` 先跑 `buildtsi`，PID 立即写入 `/var/lib/influxdb/influxd.pid`，`/health` 接受 `20x/40x`（常见 204 或 401）。
4. 执行下面的检查。

**预期输出**：

| 检查 | 命令 | 预期 |
|---|---|---|
| 新配置段 | `influxd config \| grep -A6 '^\[flux-controller\]'` | 出现 `[flux-controller]`，含 `query-concurrency = 0`、`query-initial-memory-bytes = 0`、`query-max-memory-bytes = 0`、`total-max-memory-bytes = 0`、`query-queue-size = 0`（零值不省略；`cmd/influxd/run/config.go:45`） |
| 空过滤切片 | `influxd config \| grep 'access-log-status-filters'` | `access-log-status-filters = []`（旧版 `nil` slice 被 TOML 省略；`services/httpd/config.go:96`） |
| `/debug/vars` 新键 | `curl -s localhost:8086/debug/vars \| jq 'has("config") and has("stats")'` | `true`；`jq '.stats'` 含 `compact-throughput-usage-percentage`；`jq '.cq'` 若有 CQ 是扁平 `{queryFail,queryOk}`（07 §6.6） |
| RP 新列 | `influx -execute 'SHOW RETENTION POLICIES ON db0'` | **7 列**：`name,duration,shardGroupDuration,replicaN,futureWriteLimit,pastWriteLimit,default`；未设窗口的 RP 两列为 `0s`（`coordinator/statement_executor.go:862-872`） |
| 数据未丢 | `influx -execute 'SHOW SERIES CARDINALITY ON db0'`；`influx -database db0 -execute 'SHOW FIELD KEYS FROM cpu'` | `2`；`value float` |
| TSM 格式未升级 | `grep -n 'MagicNumber uint32\|Version byte' tsdb/engine/tsm1/writer.go`（或直接看 `v1.8.10`/`v1.12.3` 的 `writer.go:82,85`）；`influx_inspect verify -dir /var/lib/influxdb/data` | magic `0x16D116D1`、version `1` 两版相同，checksum 通过。**不要用 `find`/`diff` 文件集合作为判据**：升级首启的 WAL replay→cache snapshot 可能新建 TSM，L1–L5 compaction 也会合法重写/删除文件（01 §3.13、01 §3.8.1、04 §5.4）；文件集合差异不等于升级失败 |

**失败判据**：启动失败先看 `journalctl -u influxdb` 的 `buildtsi` 段；若 `systemctl start` 卡满 `TimeoutStartUSec=90s`，按 07 §5.2.2/§6.3 处理。**没有固定启动时间**：本文不填 `buildtsi`/启动耗时。

**证据**：`v1.12.3:cmd/influxd/run/config.go:41-55`、`v1.12.3:services/httpd/config.go:96`、`v1.12.3:coordinator/statement_executor.go:852-875`、`v1.12.3:tsdb/engine/tsm1/writer.go:82,85`、07 §5.2.3/§6.3/§6.6。

### 9.5 案例 E：从 v1.12.3 回滚到 v1.8.10（`fields.idxl` 与 meta 写窗口）

**输入**：承接 §9.4 的 `db0`（2 条 series）；另备从 v1.8.10 源码构建的 `influxdb_1.8.10_amd64.deb`（回滚包）。在 v1.12.3 上继续：

```sql
INSERT cpu,host=A newf=1.0 1700000001000000000
ALTER RETENTION POLICY autogen ON db0 DURATION 24h REPLICATION 1
  FUTURE LIMIT 15m PAST LIMIT 10m;
```

预期：`SHOW FIELD KEYS FROM cpu` 含 `value` 与 `newf`；`SHOW RETENTION POLICIES ON db0` 的 `futureWriteLimit=15m0s`、`pastWriteLimit=10m0s`。新字段会同步追加到 shard 目录的 `fields.idxl`（`v1.12.3:tsdb/shard.go:51,1814-1825,1913-1935`）。

**分支 A：正常 SIGTERM 后降级**

1. `systemctl stop influxdb`。`MeasurementFieldSet.Close` 调用 `WriteToFile`，把 `fields.idxl` 合并进 `fields.idx`。注意 `fields.idxl` 的删除是在 `OpenFile` **成功后由 `defer` 无条件执行**的：成功、序列化失败、`fd.Write` 失败、rename 失败都会删，只有 `OpenFile` 本身失败才保留（`v1.12.3:tsdb/shard.go:1681-1694,1855-1871`）；所以“文件不存在”不能单独证明合并成功。
2. 判据（三项同时满足才可降级）：① `Close` 返回 nil；② `fields.idx` 已含最新字段，`grep -a newf <shard>/fields.idx` 命中；③ `ls <shard>/fields.idxl` 为 “No such file or directory”。任一项不满足都不要降级；`fields.idxl` 不存在只说明 `OpenFile` 成功过，不能证明新字段已写入 `fields.idx`（见 01 §5.3）。
3. 安装 v1.8.10 包并启动，执行：
   ```bash
   influx -database db0 -execute 'SHOW FIELD KEYS FROM cpu'
   ```
   预期：`value float`、`newf float` 都在（旧版直接读 `fields.idx`）。

**分支 B：`kill -9` 后直接降级**

1. `kill -9 $(cat /var/lib/influxdb/influxd.pid)`。进程没有机会 Close，`fields.idxl` 保留，`fields.idx` 不含 `newf`。
2. 安装 v1.8.10 并启动，执行 `SHOW FIELD KEYS FROM cpu`。预期：**只有 `value`**，没有 `newf`（`v1.8.10` 不识别 `fields.idxl`，且 `v1.8.10:tsdb/engine/tsm1/engine.go:810-814` 在 TSI + 非空 `fields.idx` 时直接返回，不会扫 TSM 补字段）。
3. 此时 `SELECT * FROM cpu` 不会出现 `newf` 列；对同一 series 再写 `newf`，旧版会把它当“新字段”重新创建，或按已有类型报冲突。
4. **meta 写窗口字段的丢失**：在 v1.8.10 上做任意一次 meta 提交，例如 `CREATE DATABASE probe`。旧版 `RetentionPolicyInfo` 没有 field 7/8（`v1.8.10:services/meta/internal/meta.proto:48-55`），`Data.marshal()` 重建 RP 时只写已知字段（`v1.8.10:services/meta/data.go:1194-1213` 的 `RetentionPolicyInfo.marshal`），因此 `FUTURE LIMIT`/`PAST LIMIT` 被抹掉。
5. 观察方式：重新装回 v1.12.3 并启动，`SHOW RETENTION POLICIES ON db0` 的两列变为 `0s`（若没有 meta 提交，仍应是 `15m0s`/`10m0s`）。`fields.idxl` 若还在，重新升级会由 `ApplyChanges` 合并回来（`v1.12.3:tsdb/shard.go:2243-2268,2128`），所以**不要手工删除 `fields.idxl`**。

**预期对照**：

| 分支 | 停止方式 | `fields.idxl` | v1.8.10 的 `SHOW FIELD KEYS` | meta 写窗口 |
|---|---|---|---|---|
| A | SIGTERM | 合并后删除 | 含 `newf` | 未提交则保留；在旧版提交后丢失 |
| B | `kill -9` | 保留 | 不含 `newf` | 未提交则保留；在旧版提交后丢失 |

**建议**：降级前正常 `SIGTERM`，并确认 `Close` 返回 nil、`fields.idx` 已含最新字段、`fields.idxl` 不存在，三者同时满足才可降级；若只能 `kill -9`，先以 v1.12.3 重启再正常停止，或保留 `fields.idxl` 并在回滚后尽快重新升级。跨大版本回滚前备份 `meta.db`；升级后在旧版做的任何 meta 变更都会使写窗口字段不可逆丢失（05 §7.8、01 §5.3）。

**证据**：`v1.12.3:tsdb/shard.go:51,1681-1694,1814-1825,1853-1911,2128,2243-2268`；`v1.8.10:services/meta/internal/meta.proto:48-55`、`:services/meta/data.go:1194-1213`；`v1.8.10:tsdb/engine/tsm1/engine.go:810-814`；`v1.12.3:services/meta/internal/meta.proto:51-59`；`v1.12.3:services/meta/data.go:1300-1344`；01 §5.3、05 §7.8。

## 10. 仍存在的风险

| 风险 | 证据 | 影响 | 建议 |
|---|---|---|---|
| L5 optimize 可能被饿死或延迟 | `defaultWeights` 中 L5 权重 `0.01`，启动 holdoff 1h、间隔 5m、`PlanOptimize` 的 shard 冷写门槛 ≥4h（`v1.12.3:tsdb/engine/tsm1/scheduler.go:8`、`tsdb/engine/tsm1/engine.go:2171-2175`、`tsdb/engine/tsm1/compact.go:452`、`tsdb/config.go:38`）；L4 组还可在 `planCompactionsInner` 里经 `IsGroupOptimized` 提升为 L5（`engine.go:2376-2388`），不受该门槛 | 高负载集群上 optimize 可能长期排不到；重启后磁盘回收慢；热 shard 不会被 `PlanOptimize` 选中，但已优化 shard 回填后可能被提升为 L5 | 观察 `tsmOptimizeCompactionQueue`/`tsmOptimizeCompactionsActive`；必要时提高 `max-concurrent-optimized-compactions`，但不要挤占 level compaction |
| retention 仍可能被“卡住”的 shard 阻塞 | `ShardInUse` 为 true 时直接返回 `can not delete an in-use shard`（`v1.12.3:services/retention/service.go:246-251`） | 单个长期读者会让该 shard 永远不删；日志会提示，但不会自动恢复 | 监控 retention 错误日志与 shard 目录；排查长查询/泄漏的 reader |
| `TagValueIterator` 释放 RLock 后遍历共享 map（v1.12.3 并发回归） | `v1.12.3:tsdb/index/tsi1/log_file.go:422-437` 在 `RUnlock` 后调用 `tk.TagValueIterator()`；而 `logTagKey.TagValueIterator()` 会 `range tk.tagValues`，`execSeriesEntry` 又在 `f.mu` 写锁下更新同一 map（`:740-749`、`:1406-1411`） | 查询与写入并发会触发 `fatal error: concurrent map iteration and map write`（runtime 直接退出，`recover` 无效）。02 §3.6/§7.7 已判定这是 v1.12.3 的并发回归并用最小程序复现，本文与 02 结论一致 | 高并发写入 + TSI 查询场景先做 `-race` 回归确认触发条件；等上游把 `range` 移回锁内 |
| retention 的 fsync 持久化窗口 | v1.8.10 的 `SeriesSegment.Flush` 只刷缓冲、不 `file.Sync()`（`v1.8.10:tsdb/series_segment.go:210-216`）；区间内 `b4e781eff6` 加上 `file.Sync()`；`1420aaf2bc` 又改为 `DeleteSeriesID(id, NoFlush)` + `FlushSegments(partitionIDs)`，后者失败只记录日志（`v1.12.3:tsdb/series_segment.go:218-227`、`v1.12.3:tsdb/series_file.go:203-220`、retention 路径 `v1.12.3:services/retention/service.go:254` → `v1.12.3:tsdb/store.go:1083-1114`；`tsdb/engine/tsm1/engine.go:1852-1885` 是另一条 DELETE series 路径） | 相对 v1.8.10 是新增持久化保障；相对“每次删除 fsync”的中间版本，批量策略把持久化点后移，崩溃窗口内未 flush 的删除可能不持久 | 升级后关注删除后的 series 是否短暂“复活”；对删除持久性敏感的场景做掉电测试 |
| TSI cache 仍不是零分配 | `Put` 仍然对 `name/key/value` 各做一次 `string(...)`（`v1.12.3:tsdb/index/tsi1/cache.go:98-105`） | 高基数 tag 查询仍有分配压力，只是避免了重复转换 | 不要把它当成“完全消除分配”；高基数场景继续关注 heap |
| `SHOW TAG VALUES` 的限制只对 TSI 生效 | `SHOW TAG VALUES` 单 RP 限制在 `executeShowTagValues` 中，但 INMEM shard 共享索引（commit `519e23b86a` 说明） | INMEM 用户**仍会**看到跨 RP 的 tag value 并集；TSI 用户对多个不同 RP 查询会直接报错 | 升级前确认索引类型；需要跨 RP 并集的脚本改为显式多次查询 |
| CSV 不再转义可能破坏旧解析器 | `HashKey(false)` 只影响 CSV formatter（`v1.12.3:services/httpd/response_writer.go:228`；commit `2dc3dcb3d1`） | tag 值中含逗号/反斜杠时，CSV 列内容与旧版不同 | 用真实 tag 值做回归；解析器按标准 CSV 处理而不是按旧转义格式 |
| `/debug/vars` 的 `config` 暴露绝对路径 | `tsdb.Config.Diagnostics()` 输出 17 个键，含 `dir`/`wal-dir`（`v1.12.3:tsdb/config.go:288-308`） | 采集端若整段落盘会记录数据目录；未来扩展 `[http]`/`[tls]` 面更大（07 §7 风险 6） | 限制采集权限与保留期，关注上游是否扩展 `config` 键 |
| `compact-throughput-usage-percentage` 启动瞬间接近 100% | `NewRate` 创建时 `AllowN(now, burstLimit)` 耗尽初始 burst（`v1.12.3:pkg/limiter/writer.go:27`） | 以此做告警会在每次重启时误报 | 告警加启动宽限期，或改用持续高占用判定（07 §7 风险 7） |
| systemd 启动期 `buildtsi` 可能超时 | 每次 start 都跑 `influx_inspect buildtsi`，脚本 `#!/bin/bash -e` 且无 `\|\| true`（`v1.12.3:.circleci/packages/influxdb/fs/usr/lib/influxdb/scripts/influxd-systemd-start.sh:1,47-49`） | 启动时间**机制上随 series 数增长（复杂度/耗时未实测）**；判定式为 `buildtsi wall + influxd start > TimeoutStartUSec`（unit 未设 `TimeoutStartSec`，默认 90s），超过后形成“重启—超时—再重启” | 按 10^4/10^5/10^6 series 分层测 `buildtsi wall`（07 §5.2.2）；评估后提高 `TimeoutStartSec` 或提前离线 buildtsi |
| `[flux-controller]` 不在 `etc/config.sample.toml` | `grep -n "flux-controller" etc/config.sample.toml` 无结果，但 `cmd/influxd/run/config.go:45` 定义了该段 | 运维只能靠 `influxd config` 或源码猜参数名 | 升级时用 `influxd config` 导出默认值并纳入自有配置模板（07 §7 风险 10） |
| `fields.idxl` 的降级风险 | 新版把字段变更追加到 `fields.idxl`，仅在 Close / ApplyChanges / LoadMetadataIndex 重建 / Restore·Import 时合并回 `fields.idx`（`v1.12.3:tsdb/shard.go:51`、`:1681-1698`；commit `80c10c8c04`） | 旧版本不认识 `.idxl`；如果降级前未正常关闭/合并，升级后的字段变更可能不可见 | 升级/降级前备份 shard 的 `fields.idx`/`fields.idxl`；降级前用新版正常关闭以触发合并。完整两条分支与预期输出见 §9.5 |
| compaction 修复的版本归属：1.12.2 与 1.12.3 各含一个 | `95e86be3f4` 明确说修复的是 “introduced with 1.12.1”；`5e9d85c972` 是 1.12.3 的 backport | **修复归属**：full compaction 未排期修复（`95e86be3f4` #26670）在 v1.12.2 已包含；level 回退修复（`5e9d85c972` #27233）只在 v1.12.3。因此 1.12.1 两个 bug 都在；1.12.2 只剩 level 回退；1.12.3 两者都已修 | 直接升级到 1.12.3（或至少 1.12.2 再评估 level 回退） |

---

## 附：一句话记忆

- **存储**：4 队列变 5 队列，L4 full / L5 optimize 分家；1.12.2/1.12.3 各修了一个“磁盘一直涨/放大”的调度 bug（另有 #26911）；区间内共三个磁盘/压缩调度问题，“无界增长”特指 #26670。
- **写入**：`WriteContext` + `StatsTracker` 取代 context 传参；`time` 字段会报 partial write；RP 可以设置写入窗口。
- **查询**：`SHOW QUERIES` 不再越权、DST 不再丢行、`SHOW TAG VALUES` 只认一个 RP、array cursor 不再重复点。
- **运维**：TLS 可以 SIGHUP 热重载；`/debug/vars` 更全；retention 不再被单个 shard 拖死，但卡住的 shard 仍需人工处理。
- **升级**：先去 Go 1.24 工具链，再按 [§5](#5-升级影响清单) 过配置/API/数据格式，最后用 [§6.4](#64-做升级评估从-1810-升到-1123) 的顺序做回归。
