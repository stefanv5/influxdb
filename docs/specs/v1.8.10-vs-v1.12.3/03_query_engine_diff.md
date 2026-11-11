# 版本差异 3: 查询引擎、Flux 存储读取与 InfluxQL 执行（v1.8.10 → v1.12.3）

> **版本基线**
>
> - OLD：`v1.8.10` = commit `688e697c51`（2021-10-11）
> - NEW：`v1.12.3` = commit `f3d5cb68ab`（`docs/specs/` 这批文档的代码基准）
> - 区间：527 commits，602 files，+79,310 / −45,239
> - 本文件覆盖目录（`query/`、`storage/reads/`、`storage/flux/`、`tsdb/cursors/`、`tsdb/engine/tsm1/array_cursor*`）：
>   93 files，+30,372 / −9,998；涉及 69 个提交（含 array cursor 相关 tsm1 文件）。
> - NEW 的当前工作树只额外包含若干 docs 提交（当前触及 `docs/specs/` 的为 8 个、全部 docs 提交为 10 个；数量随文档演进），代码与 `f3d5cb68ab` 相同。
>
> 引用格式：旧 `v1.8.10:path:line`，新 `v1.12.3:path:line`（即当前工作树行号）。
> 新版本行为的详细讲解见 [03 查询引擎](../03_query_engine_spec.md)、
> [08 Array Cursor](../08_array_cursor_spec.md)、[09 Storage Reads](../09_storage_reads_spec.md)、
> [11 InfluxQL 执行](../11_influxql_execution_spec.md)；HTTP/Flux 外围差异见
> [06 HTTP / Flux 差异](06_http_flux_diff.md)。

> **小白导读**
>
> 1. **一句话结论：InfluxQL 老引擎只是“打补丁”，Flux 读路径则是“换了发动机”。**
>    InfluxQL 的 `ExecutionContext` / `TaskManager` / `Iterator` 模型几乎原样保留，
>    改的是若干正确性 bug 和三个 HLL 算子；Flux 侧则把 2.x 的查询控制器
>    （`query/control`）整包搬进 1.x，并新增“窗口聚合下推”这一整层。
> 2. **旧版 Flux 控制器是“裸奔”的**：`flux/control/controller.go` 只有 127 行，
>    编译完直接 `program.Start`，内存分配器是 `&memory.Allocator{}`——不限内存、
>    不排队、没有控制器级配额/指标、不管理取消（但 flux 分配器仍统计每查询的 `MaxAllocated`/`TotalAllocated`，写入查询统计与日志）。新版 `query/control` 有 1108+157+111 行，
>    维护 7 状态状态机、并发配额、单查询/全局内存池、队列和 11 组 Prometheus 指标。
> 3. **查询从“拉取点”升级为“下推聚合”**：Flux 的 `window + mean/count/sum/min/max/first/last`
>    以及裸聚合、`group + 聚合` 现在由存储层直接算完再上送。
>    为此 `storage/reads.Store` 新增 `WindowAggregate`，`GroupCursor` 新增 `Aggregate()`，
>    proto 新增 `ReadWindowAggregateRequest` / `MultiPointsFrame`。
> 4. **表生成器从 10 种表变成 25 种表**：`storage/flux/table.gen.go`（4792 行）
>    为每种 Flux array 类型生成 5 个表：普通、window、window-selector、empty-window-selector、group。
>    旧版只有普通表和 group 表。
> 5. **一个边界值的修正会改变结果**：存储游标的时间范围从 `[start, stop)`
>    改成 `[start, stop]`，调用方统一传 `EndTime = stop - 1`。
>    以前恰好落在 `stop` 上的点是否返回，取决于哪条路径，现在统一“不返回”。
> 6. **元查询悄悄变了**：`SHOW SERIES CARDINALITY FROM ...`（非 EXACT）从精确
>    `count(distinct(_seriesKey))` 变成 HLL 近似；`SHOW TAG VALUES` 开始遵守
>    指定的 retention policy，多 RP 直接报错；`SHOW FIELD KEY CARDINALITY`
>    改走 `_fieldKeys` 系统迭代器。

---

## 1. 结论速览

| # | 变更点 | v1.8.10 | v1.12.3 | 影响 | 证据 |
|---|--------|---------|---------|------|------|
| 1 | Flux 查询控制器 | `flux/control`，127 行，无队列/无内存限制/无状态机 | `query/control`（controller 1108 行 + memory 157 + metrics 111），7 状态、队列、内存池 | 资源可控；错误码/取消语义变化 | `v1.8.10:flux/control/controller.go:73` / `v1.12.3:query/control/controller.go:175` |
| 2 | 控制器配置 | 无（构造参数写死） | `[flux-controller]`：`query-concurrency`、`query-initial-memory-bytes`、`query-max-memory-bytes`、`total-max-memory-bytes`、`query-queue-size` | 默认 0 = 不限制；配错会拒绝启动 | `v1.12.3:query/control/controller.go:77`-`:109`、`cmd/influxd/run/config.go:45` |
| 3 | 查询指标 | `PrometheusCollectors()` 返回 nil | `qc_requests_total` / `qc_functions_total` / `qc_*_active` / `qc_*_duration_seconds` / `qc_memory_unused_bytes` | 可观测性从 0 到 1 | `v1.8.10:flux/control/controller.go:125` / `v1.12.3:query/control/metrics.go:34`、`:44`、`:64` |
| 4 | 每用户查询字节 | 无 | `userquerybytes` 统计（tag `user`），开关 `user-query-bytes-enabled`（#27188） | 可按用户计费；默认关闭 | `v1.12.3:services/httpd/config.go:68`、`services/httpd/handler.go:505`、`:484` |
| 5 | Flux 读接口 | `ReadFilter` / `ReadGroup` | 追加 `ReadWindowAggregate`，`alloc *memory.Allocator` → `alloc memory.Allocator` | 外部实现（企业版/自定义 Reader）编译失败 | `v1.8.10:flux/stdlib/influxdata/influxdb/storage.go:90` / `v1.12.3:flux/stdlib/influxdata/influxdb/storage.go:111`-`:116` |
| 6 | 存储 Store 接口 | `ReadFilter` / `ReadGroup` | 追加 `WindowAggregate` | 下推窗口聚合的入口 | `v1.8.10:storage/reads/store.go:76` / `v1.12.3:storage/reads/store.go:81` |
| 7 | GroupCursor | 无 `Aggregate()` | `Aggregate() *datatypes.Aggregate` | 上层据此决定列布局（是否保留 `_time`） | `v1.12.3:storage/reads/store.go:75`、`storage/reads/group_resultset.go:288`、`:339` |
| 8 | 聚合类型 | sum / count | sum/count/min/max/first/last/mean + meanCount 双聚合 | 7 种聚合可下推 | `v1.8.10:storage/reads/array_cursor.go:25`-`:27` / `v1.12.3:storage/reads/array_cursor.go:20`-`:56` |
| 9 | 存储层数组游标 | 23 个类型（sum/count/filter/multishard） | 50 个类型（新增 limit/window×7/window-mean-count） | 窗口聚合在游标层完成 | `v1.8.10:storage/reads/array_cursor.gen.go:174` / `v1.12.3:storage/reads/array_cursor.gen.go:27`、`:50`、`:199` |
| 10 | 游标时间范围 | `EndTime = end`（`[start, stop)`） | `EndTime = end - 1`（存储侧 `[start, stop]`） | 落在 stop 的点不再返回；各路径统一 | `v1.8.10:storage/reads/array_cursor.go:100` / `v1.12.3:storage/reads/array_cursor.go:90` |
| 11 | 游标错误传播 | `shard.Next` 错误被 `_` 丢弃；`reset` 无返回值 | `reset`/`build*Cursor` 返回 `error`；`windowAggregateResultSet.Err()` 会上抛，但 filter/group 的 `Err()` 仍恒 nil（见 §7.1/§7.2） | nil 游标 panic 减少；但仍有漏点（见第 7 节） | `v1.8.10:storage/reads/array_cursor.go:133` / `v1.12.3:storage/reads/array_cursor.go:126`、`tsdb/engine/tsm1/array_cursor.gen.go:42` |
| 12 | array cursor 优化判定 | `pos==0 && len(res)>=len(tsm)` | 追加 `c.tsm.pos==0`（#26691） | 修复“整块走 TSM”误判导致的错值/重复 | `v1.8.10:tsdb/engine/tsm1/array_cursor.gen.go:107` / `v1.12.3:tsdb/engine/tsm1/array_cursor.gen.go:113` |
| 13 | 谓词表示 | gogo/protobuf；`TagRefValue string`；只在读时转换 | protobuf-go；`TagRefValue bytes`；计划期转换一次 | 读路径少一次 AST→PB 转换 | `v1.8.10:storage/reads/flux_predicate.go:19` / `v1.12.3:flux/stdlib/influxdata/influxdb/flux_predicate.go:19`、`rules.go:179` |
| 14 | Flux 表类型 | 10 种（普通 + group） | 25 种（+window / window-selector / empty-window-selector） | 空窗口、selector、createEmpty 可下推 | `v1.8.10:storage/reads/flux_table.gen.go:25`、`:101` / `v1.12.3:storage/flux/table.gen.go:107`、`:320`、`:416` |
| 15 | 聚合下推规则 | 0 条聚合规则 | 4 条：window / windowByTime / bare / group | Flux 计划层出现 `ReadWindowAggregate*` | `v1.8.10:flux/stdlib/influxdata/influxdb/rules.go:12`-`:20` / `v1.12.3:flux/stdlib/influxdata/influxdb/rules.go:22`-`:33` |
| 16 | InfluxQL 新增算子 | 无 | `count_hll` / `sum_hll` / `merge_hll` | 常量内存基数估计 | `v1.12.3:query/call_iterator.go:56`、`:1538`、`:1589`；`query/compile.go:787` |
| 17 | `SHOW SERIES CARDINALITY` | 非 EXACT 且带 FROM/WHERE 时走精确 distinct | 走 `count_hll(sum_hll(_seriesKey))` 近似 | 结果从精确变近似（有误差上界） | `v1.8.10:query/statement_rewriter.go:183`-`:222` / `v1.12.3:query/statement_rewriter.go:219`-`:243` |
| 18 | `SHOW FIELD KEY CARDINALITY` | 扫描实际数据源 | 子查询走 `_fieldKeys` 系统迭代器 | 索引级完成，语义依赖 field key 索引 | `v1.8.10:query/statement_rewriter.go:55`-`:95` / `v1.12.3:query/statement_rewriter.go:57`-`:101`；`tsdb/shard.go:2291` |
| 19 | `SHOW TAG VALUES` 的 RP | 重写时丢弃 `Sources`，扫描所有 RP | 保留 `Sources`，只查指定 RP；多 RP 报错 | 结果变准；多 RP 查询由“能跑”变“报错” | `v1.8.10:query/statement_rewriter.go:266` / `v1.12.3:query/statement_rewriter.go:317`；`coordinator/statement_executor.go:1143` |
| 20 | `GROUP BY time` + DST | `end += offset` 在时区修正之后 | 先加 offset 再做时区修正（#25082） | DST 回退窗口不再丢行 | `v1.8.10:query/iterator.go:864` / `v1.12.3:query/iterator.go:860` |
| 21 | 多语句错误结果 | `send` 覆盖 `StatementID`，`ErrNotExecuted` 数量少 1 | 显式传 `StatementID`，循环从 `i+1` 到末尾 | 结果条数与 ID 修正 | `v1.8.10:query/execution_context.go:93`、`query/executor.go:383` / `v1.12.3:query/execution_context.go:93`、`query/executor.go:432` |
| 22 | `SHOW QUERIES` | 5 列，无按库鉴权过滤 | 6 列（+`user`），按 READ 权限过滤（#27196） | 越权泄漏修复 | `v1.8.10:query/task_manager.go:152` / `v1.12.3:query/task_manager.go:34`、`:144` |
| 23 | 编译期 panic | 直接调用 `Compile` 时 panic（HTTP 路径由 `Executor.recover` 兜底返回 `statement_id=-1` 的 error） | `Compile` defer recover，返回 `likely malformed statement...` | 直接调用从 panic 变 error；HTTP 路径两版都不崩溃进程 | `v1.8.10:query/compile.go:106` / `v1.12.3:query/compile.go:106`、`:118`-`:128` |
| 24 | 窗口 mean/count 的整数精度 | 无窗口 mean 下推（能力区间内新增） | 初版 `float64` 累加，后改为 int64/uint64 原生累加（#21334） | 大整数不再丢精度 | `v1.12.3:storage/reads/array_cursor.gen.go:2030`、`:2146`；commit `4cf19f8254` |
| 25 | 结果序列化 | `SeriesFrame.data_type` 已写（gen.go 10 处）；空 series 先建后删 | 类型改由 `startSeries(dataType,tags)` 填；有数据才建；新增 `MultiPointsFrame`/`DataTypeMulti` | 空 series 不再占位；双聚合新增一条 oneof 分支 | `v1.8.10:storage/reads/response_writer.gen.go:40` / `v1.12.3:storage/reads/response_writer.go:205`、`:216`、`storage/reads/response_writer.gen.go:89` |

---

## 2. 变更地图

### 2.1 两条查询路径：仍然完全分离

```mermaid
flowchart TB
    subgraph HTTP["HTTP 入口 services/httpd"]
        H1["serveQuery (InfluxQL)"]
        H2["serveFluxQuery (Flux)"]
    end

    subgraph IQ["InfluxQL 点式迭代器路径（未重构）"]
        I1["Executor.executeQuery"] --> I1b["RewriteStatement / NormalizeStatement"]
        I1 -. "错误/系统 measurement：直接写 results，绕过 send" .-> I2R["results channel"]
        I1b --> I3["StatementExecutor.ExecuteStatement"]
        I3 --> I4["query.Select"]
        I3 --> I2["ExecutionContext.send / Send"]
        I2 --> I2R
        I4 --> I5["query.Prepare"] --> I5b["preparedStatement.Select (select.go:113)"] --> I6["buildCursor"]
        I6 --> I7["有 call：buildExprIterator / buildCallIterator"]
        I6 -. "无 call（buildAuxIterator 内部含 Merge :823 / Limit :846）" .-> I7b["buildAuxIterator"]
        I7b --> I7c["NewIteratorScanner → newScannerCursor"]
        I7 --> I8["引擎层（每个 series）：NewCallIterator → Reduce/Stream"]
        I8 --> I8b["NewParallelMergeIterator（同一 TagSet 内）"]
        I8b --> I9["Select 层：Iterators.Merge → NewMergeIterator"]
        I9 --> I9b["Merge 内再 NewCallIterator（count→sum / sum_hll→merge_hll）"]
        I9b --> I10["NewIntervalIterator → NewFillIterator"]
        I10 --> I10b["NewLimitIterator（LIMIT/OFFSET）"]
        I10b --> I11["Emitter.Emit"]
        I7c --> I11
        I11 -->|"非 SELECT INTO"| I2
        I11 -. "SELECT ... INTO：逐行不 Send，循环后 Flush → ctx.Send 写入计数" .-> I12["writeInto → PointsWriter"]
    end

    subgraph FX["Flux 列式读取路径（新增控制层 + 下推层）"]
        F1["query/control.Controller.query"] --> F2["createQuery / compileQuery / enqueueQuery"]
        F2 -. "异步：queryQueue 或每查询 goroutine" .-> F3["executeQuery → Query.pump"]
        F3 --> F4a["storageflux.storeReader.ReadWindowAggregate"]
        F4a --> F5["services/storage.Store.WindowAggregate → WindowAggregateLimit"]
        F3 --> F4b["storeReader.ReadFilter"]
        F4b --> F5b["Store.ReadFilter → ReadFilterLimit"]
        F5b --> F6b["reads.NewFilteredResultSet"]
        F6b --> F9b["storageflux.new{Type}Table"]
        F3 --> F4c["storeReader.ReadGroup"]
        F4c --> F5c["Store.ReadGroup → ReadGroupLimit"]
        F5c --> F6c["reads.NewGroupResultSet"]
        F6c --> F9c["storageflux.new{Type}GroupTable（聚合经 newAggregateArrayCursor）"]
        F5 --> F6["reads.NewWindowAggregateResultSet"]
        F6 --> F7["NewWindowAggregateArrayCursor"]
        F7 --> F8["newFloatWindowMeanArrayCursor 等"]
        F8 --> F9["storageflux.newFloatWindowTable / newFloatWindowSelectorTable（仅窗口路径）"]
        F9 --> F10["table.do → colReader（窗口路径再 splitWindows）"]
        F9b --> F10
        F9c --> F10
        F11["newIndexSeriesCursor → row.Query=shard 游标迭代器"]
        F5 -. "下推读取" .-> F11
        F5b -. "结果集 row.Query" .-> F11
        F5c -. "结果集 row.Query" .-> F11
    end

    H1 --> I1
    H2 --> F1
    I7 -. "tsdb.Shard.CreateIterator（点迭代器）" .-> T1["tsdb/engine/tsm1 point iterators"]
    F11 -. "tsdb.CreateCursorIterators（arrayCursorIterator）" .-> T2["tsdb/engine/tsm1 arrayCursorIterator"]
    T1 --> T3["Cache + TSM KeyCursor"]
    T2 --> T3
```

要点：

- InfluxQL 从来不经过 `storage/reads`：`coordinator/statement_executor.go:647` 调用
  `query.Select(ctx, stmt, e.ShardMapper, sopt)`，`query/select.go:622` 的 `buildCursor`
  通过 `IteratorCreator.CreateIterator` 拿到**点迭代器**。
- Flux 才走 `storage/reads` / `storage/flux`：`WindowAggregateLimit`（`services/storage/store.go:234`）、
  `ReadFilterLimit`（`:126`）与 `ReadGroupLimit`（`:177`）都经 `newIndexSeriesCursor`
  （`services/storage/series_cursor.go:40`）调用 `tsdb.CreateCursorIterators`（`tsdb/cursor.go:39`）
  拿到**数组游标**（`arrayCursorIterator`）；filter/group 的结果集再由 `row.Query` 创建游标
  （`storage/reads/array_cursor.go:118`）。
- 因此本文件里的“数组游标/批量读取”改动只影响 Flux（以及 `influx_tools`），
  不影响 InfluxQL 的 `GROUP BY`。
- InfluxQL 编译/Select/迭代器引擎三层、`services/storage` 适配层与数组游标契约的模块卡见
  §3.15-§3.19；其中 §3.10 是 Flux 计划下推，§3.11 是 `storage/flux` 表执行。

### 2.2 Flux 查询生命周期（新控制器）

```mermaid
sequenceDiagram
    participant H as httpd.serveFluxQuery
    participant C as control.Controller
    participant Q as control.Query
    participant K as flux.Compiler
    participant P as flux.Program
    participant X as flux.Query (exec)
    participant W as worker goroutine

    H->>C: Query(ctx, compiler)
    C->>C: createQuery (context.WithCancel)
    alt createQuery 失败（controller shutdown）
        C-->>H: 返回错误（未创建 Query）
    else
        C->>Q: tryCompile → Compiling
        C->>K: Compile(ctx, runtime.Default)
        K-->>C: Program
        alt Compile 失败
            C->>Q: setErr（Compiling → Errored）
            C-->>H: 返回错误（父 ctx 取消则 Canceled）
        else
            C->>Q: tryQueue → Queueing
            alt tryQueue 失败（状态迁移非法）
                C-->>H: codes.Internal
            else
                C->>C: enqueueQuery（queryQueue 或每查询一个 goroutine）
                alt 队列满 / controller 关闭
                    C-->>H: ResourceExhausted / codes.Internal
                else
                    C-->>H: 立即返回 Query（异步执行）
                    W->>Q: tryExec → Executing
                    alt tryExec 失败（已取消/关闭）
                        W->>Q: setErr（参数 codes.Internal；parentCtx 已取消时改写为 Canceled/context.Canceled）
                    else
                        W->>C: q.c.createAllocator(q)
                        W->>Q: q.recordUnusedMemory()
                        W->>P: program.Start(ctx, alloc)
                        alt Start 失败
                            W->>Q: setErr(err)
                        else
                            P-->>W: exec
                            W->>Q: q.pump(exec, ctx.Done())
                            loop pump
                                X-->>Q: Results()
                                Q-->>H: results channel
                            end
                        end
                    end
                    H->>Q: Cancel()（defer 注册，函数返回时执行）
                    H->>Q: Done()
                    Q->>Q: transitionTo(Finished) → cancel()
                    Q->>Q: 排空 results 直到 close
                    Q->>Q: exec.Done() → stats → runtimeErrs → deps.Finish()
                    Q->>C: finish(q)
                    Q->>Q: MemoryManager.Release() → recordUnusedMemory()
                    Q->>C: countQueryRequest(q, label)
                end
            end
        end
    end
    Note over H,Q: Cancel/Done 由 Query() 成功后的 defer 注册；失败分支直接返回错误
```

### 2.3 storage/reads 接口的扩张

```mermaid
classDiagram
    class Store {
        <<interface>>
        ReadFilter()
        ReadGroup()
        WindowAggregate()
        TagKeys()
        TagValues()
        GetSource()
    }
    class GroupResultSet {
        <<interface>>
        Next()
        Err()
        ...
    }
    class GroupCursor {
        <<interface>>
        Next()
        Cursor()
        Keys()
        PartitionKeyVals()
        Err()
        Aggregate()
        ...
    }
    class ResultSet {
        <<interface>>
        Next()
        Cursor()
        Err()
        ...
    }
    Store --> ResultSet
    Store --> GroupResultSet
    GroupResultSet --> GroupCursor
```

新增的两块是 `Store.WindowAggregate` 与 `GroupCursor.Aggregate()`；`ResultSet.Err()` 是旧接口就有的，
但旧实现的 `resultSet.Err()` 恒为 `nil`（`v1.8.10:storage/reads/resultset.go:33`），
新实现仍然恒为 `nil`（`v1.12.3:storage/reads/resultset.go:33`），
真正的错误改由 `windowAggregateResultSet.Err()` 返回（`storage/reads/aggregate_resultset.go:148`）。

---

## 3. 逐项差异

### 3.1 Flux 查询控制器：`flux/control` → `query/control`

**旧版：编译完直接跑，没有可执行的资源上限与控制器级账本。**

```go
// v1.8.10:flux/control/controller.go:92 — Controller.Query
func (c *Controller) Query(ctx context.Context, compiler flux.Compiler) (flux.Query, error) {
	requestStart := time.Now()
	for _, dep := range c.deps {
		ctx = dep.Inject(ctx)
	}
	p, err := c.compile(ctx, compiler, requestStart)
	if err != nil {
		return nil, err
	}
	alloc := &memory.Allocator{}      // 无上限；但仍统计 Max/TotalAllocated
	return p.Start(ctx, alloc)
}
```

整个文件 127 行，只有 `Controller`（`deps`、`logger` 两个字段）、`query` 包装器（只为补
`CompileDuration`/`ExecuteDuration`）和 `program` 包装器。没有队列，没有 `Shutdown`，
没有取消后的结果排空，也没有 Prometheus 指标（`PrometheusCollectors()` 返回 `nil`，
`v1.8.10:flux/control/controller.go:125`）。但旧版的 `&memory.Allocator{}` 并非空壳：它维护 `MaxAllocated`/`TotalAllocated`，`lang.Query.Done` 写入 `flux.Statistics`，旧 HTTP handler 的 `logFluxQuery` 也输出 `stat_max_allocated`/`stat_total_allocated`（`v1.8.10:services/httpd/handler.go:1456-1475`）；缺的是限额、队列、全局池和控制器级指标。

**新版：从 2.x 整包搬来的控制器**（commit `b9ee9e5b44` “pull in controller from 2.x”，
`f1aa051787` 把参数做成配置）。

```go
// v1.12.3:query/control/controller.go:246 — Controller.query
func (c *Controller) query(ctx context.Context, compiler flux.Compiler, deps *dependency.Span) (flux.Query, error) {
	q, err := c.createQuery(ctx, compiler, deps)
	if err != nil {
		return nil, handleFluxError(err)
	}
	if err := c.compileQuery(q, compiler); err != nil {
		q.setErr(err); c.finish(q); c.countQueryRequest(q, labelCompileError)
		return nil, q.Err()
	}
	if err := c.enqueueQuery(q); err != nil {
		q.setErr(err); c.finish(q); c.countQueryRequest(q, labelQueueError)
		return nil, q.Err()
	}
	return q, nil
}
```

对应结构：

| 机制 | 旧 | 新 | 证据 |
|------|----|----|------|
| 状态机 | 无 | `Created → Compiling → Queueing → Executing → Errored/Finished/Canceled` | `v1.12.3:query/control/controller.go:999`-`:1023`（`isFinishedState` 在 `:1047`-`:1050`） |
| 并发配额 | 无 | `ConcurrencyQuota` 个 `processQueryQueue` goroutine + `WaitGroup` | `:77`、`:219`-`:231`、`:427` |
| 队列 | 无 | 有界 `chan *Query`；满则 `codes.ResourceExhausted` “queue length exceeded” | `:386`-`:424` |
| 单查询内存 | `memory.Allocator{}` | `queryMemoryManager`：初始配额 + 硬上限 + 全局池 CAS 申请 | `query/control/memory.go:73`-`:116` |
| 全局内存池 | 无 | `unusedMemoryBytes`，`MaxMemoryBytes - ConcurrencyQuota*Initial` | `controller.go:191`-`:199`、`memory.go:31`-`:45` |
| 取消 | 只依赖调用方 ctx | `context.WithCancel` + `Query.Cancel` + `pump` 在 ctx.Done 时 `exec.Cancel()` | `controller.go:292`、`:631`、`:896`-`:930` |
| 关闭 | 无 | `Shutdown(ctx)` 取消全部查询，`abort` 兜底 | `controller.go:516`-`:554` |
| panic | 无 recover | `compileQuery` / `executeQuery` defer recover 并打栈 | `:345`-`:365`、`:439`-`:460` |
| 错误映射 | 原样返回 | `handleFluxError`：`codes.NotFound→ENotFound` 等 | `:1063`-`:1108` |
| 指标 | 无 | `qc_requests_total`、`qc_functions_total`、`qc_all_active`、`qc_compiling_active`、`qc_queueing_active`、`qc_executing_active`、`qc_memory_unused_bytes`、4 个 duration 直方图 | `query/control/metrics.go:34`-`:90` |

**内存管理器的工作方式**（`query/control/memory.go`）：

```go
// v1.12.3:query/control/memory.go:73 — queryMemoryManager.RequestMemory
if q.limit+want > q.m.memoryBytesQuotaPerQuery {
	return 0, errors.New("query hit hard limit")   // 单查询硬上限
}
for {
	unused := int64(math.MaxInt64)
	if !q.m.unlimited {                             // 默认 unlimited：不读全局池
		unused = q.m.getUnusedMemoryBytes()
		if unused < want {
			return 0, errors.New("not enough capacity")    // 全局池不够
		}
	}
	given := q.giveMemory(want, unused)                 // 尽量翻倍给，减少反复申请
	if !q.m.unlimited {
		if !q.m.trySetUnusedMemoryBytes(unused, unused-given) { continue }  // CAS 重试
	}
	q.limit += given
	q.given += given
	return given, nil
}
```

`FreeMemory` 是空实现（`memory.go:143`-`:147`）：内存只在 `Release()`（查询 `Done()`）时
一次性归还，**运行中不会因为释放中间结果而退还配额**。这是后面第 7 节要讲的风险。

**语义影响**

1. 默认行为仍然“不限制”：`control.NewConfig()` 返回零值，`Config.complete()` 把
   `MemoryBytesQuotaPerQuery==0` 解释为 `math.MaxInt64`、`InitialMemoryBytesQuotaPerQuery==0`
   继承前者；`ConcurrencyQuota==0` 时 `queryQueue=nil`，来一个查询起一个 goroutine
   （`controller.go:117`-`:131`、`:201`-`:203`、`:394`-`:415`）。所以**语义上不限额、不排队**；
   但每个查询仍会安装 `memory.ResourceAllocator`（`Limit=MaxInt64`，分配计账走 CAS，
   `memory.go:73`-`:116`），存在固定的计账开销，未做基准，不宣称零性能影响。
2. 一旦配置了 `query-concurrency > 0`，`QueueSize` 必须为正，否则 `Config.validate` 直接报错
   （`controller.go:134`-`:150`）；服务起不来，而不是运行期降级。
3. 取消语义变强：客户端断开后，`pump` 会显式 `exec.Cancel()` 并继续排空结果，
   避免底层 Flux 程序卡在写 channel 上（`controller.go:896`-`:930`）。
4. 控制器只在 `flux-enabled=true` 时创建（`cmd/influxd/run/server.go:327`-`:347`），
   指标也只在此时注册（`:346`）。

### 3.2 每用户查询字节统计（#27188）：跨层改动，不在 `query/*`

这条经常被误认为“查询引擎统计”，实际实现全在 `services/httpd`：

```go
// v1.12.3:services/httpd/handler.go:505
func (h *Handler) addQueryBytesForUser(user string, n int64) {
	if !h.Config.UserQueryBytesEnabled {
		return
	}
	counter, _ := h.queryBytesPerUser.LoadOrStore(user, &atomic.Int64{})
	counter.Add(n)
}
```

- 开关：`UserQueryBytesEnabled` / toml `user-query-bytes-enabled`，默认 `false`
  （`v1.12.3:services/httpd/config.go:68`）。
- 计数点覆盖四条路径：InfluxQL 普通响应 `:913`、chunked 响应 `:821`、
  Flux 响应 `:2259`、storage 读响应 `:2094`。
- 输出：`/debug/vars` 与 `SHOW STATS` 中多出 `userquerybytes` 统计（tag `user`，
  匿名记为 `StatAnonymousUser`），见 `services/httpd/handler.go:484`-`:496`。

**影响**：默认关闭，升级不改变行为；开启后每个 distinct user 在 `queryBytesPerUser` 中占一个
`atomic.Int64` 条目，并在 `SHOW STATS` / `/debug/vars` 中生成一条 `userquerybytes` 统计
（匿名用户合并为 `StatAnonymousUser`）。行数 = 进程启动以来出现过的 user 数；该 map 没有删除路径
（`services/httpd/handler.go:505-511`），所以统计条数随运行时间单调增长。1000 个 user ≈ 1000 条
Statistic；关闭开关时 0 条。

### 3.3 InfluxQL Executor：`StatementID` 与 `ErrNotExecuted`（#22273）

旧版 `ExecutionContext.send` 会用 `ctx.statementID` **覆盖**调用方设置的 `StatementID`：

```go
// v1.8.10:query/execution_context.go:93 — send
func (ctx *ExecutionContext) send(result *Result) error {
	result.StatementID = ctx.statementID   // 覆盖
	...
}
```

而补发未执行语句的循环又从当前 `i` 开始、到 `len-1` 结束：

```go
// v1.8.10:query/executor.go:383-390
for ; i < len(query.Statements)-1; i++ {
	if err := ctx.send(&Result{StatementID: i, Err: ErrNotExecuted}); err == ErrQueryAborted {
		return
	}
}
```

于是：第 `i` 条语句失败时会多发一条（被覆盖成同一个 ID）的 `ErrNotExecuted`，
最后一条语句永远不会补发。新版删除覆盖逻辑（`v1.12.3:query/execution_context.go:93`），
并在需要时显式传 ID（`query/executor.go:383` 的 normalizer 分支），循环改为：

```go
// v1.12.3:query/executor.go:432-439
for i++; i < len(query.Statements); i++ {
	if err := ctx.send(&Result{StatementID: i, Err: ErrNotExecuted}); err == ErrQueryAborted {
		return
	}
}
```

**语义影响**：`SELECT ...; SELECT ...; SELECT ...` 中第一条报错时，旧版结果条数/ID 错位，
新版严格每剩一条语句一条 `ErrNotExecuted`。客户端如果按“结果条数 == 语句数”做校验，
升级后行为才正确。

**三语句实例**：`SELECT 1; BAD; SELECT 3`（第 2 条执行失败，`BAD` 为任意报错语句）：

| 版本 | 返回结果（`StatementID` / `Err`） | 说明 |
|------|-----------------------------------|------|
| v1.8.10 | `0` / nil、`1` / 执行错误、`1` / `ErrNotExecuted` | `send` 覆盖 ID；补发循环从 `i=1` 开始，重复第 2 条，最后一条不补 |
| v1.12.3 | `0` / nil、`1` / 执行错误、`2` / `ErrNotExecuted` | 补发循环 `i++` 后从 3 开始，最后一条得到 ID 2 |

旧版结果条数也是 3，但 ID 序列是 `0,1,1`，客户端按 ID 对齐会错位；新版是 `0,1,2`。

**仍绕过 `ctx.send` 的两个早退**：系统 measurement 拒绝（`_series`/`_fieldKeys` 等，`query/executor.go:362-365`）与 `RewriteStatement` 失败（`:373-377`）仍直接 `results <- &Result{Err: err}`，不设置 `StatementID`（零值 0），也不经 `send`。当第 2 条及以后语句命中这两个分支时，客户端看到的仍是 ID 0（与第 1 条重复），随后补发循环从 `i++` 开始给剩余语句 `ErrNotExecuted`（`query/executor.go:432-439`）。只有 normalize 与执行错误走 `ctx.send(&Result{StatementID:i})`（`:383-415`），因此上面的 `0,1,2` 实例只覆盖 normalize 与执行错误两条路径。

另外，区间内 `FineAuthorizer` 新增了 `OptimizeSeriesRead` 钩子（`2cf2103cc4` #25207）：
接口定义在 `query/executor.go:93`-`:94`，`openAuthorizer`/`voidAuthorizer` 的实现是 no-op
（`:118`-`:119`、`:147`-`:148`）。OSS 里它恒为 open/true，企业版可据此把授权条件下推成索引过滤、
减少 series 扫描（`tsdb/index.go:2986`-`:2999`）。

**模块卡：`FineAuthorizer.OptimizeSeriesRead`（企业版扩展点）**

- 接口：`query/executor.go:93-94`；OSS 的 `openAuthorizer`/`voidAuthorizer` 都是
  no-op（`:118-119`、`:147-148`）。
- 调用：`tsdb/index.go:2986-2999` 在 `MeasurementTagKeyValuesByExpr` 里调用；
  返回错误时回退到未优化的 `expr`/`auth`，不会让查询失败。
- 边界：OSS 恒为 `true` 或 `false`，不能据此判断“权限已过滤”；真正的下推过滤只存在于
  企业版。OSS 读者可把本节当作扩展点说明跳过。

### 3.4 InfluxQL TaskManager：user 字段、超时日志、SHOW QUERIES 鉴权

```go
// v1.12.3:query/task_manager.go:34
var queryFieldNames []string = []string{"qid", "query", "database", "duration", "status", "user"}
```

- `Task` 增加 `userID`，来自 `ExecutionOptions.UserID`（`query/executor.go:206`、
  `task_manager.go:216`）；`QueryInfo.User` 暴露到 JSON（`:288`，赋值见 `:305`）。
- `LogTimedoutQueries`（`task_manager.go:76`）打开时，`waitForQuery` 在超时分支打
  `"query killed for exceeding timeout limit"`，带 query/database/user/timeout（`:329`-`:337`）。
- `SHOW QUERIES` 结果按 `CoarseAuthorizer.AuthorizeDatabase(ReadPrivilege, db)` 过滤
  （`task_manager.go:144`），修复 #27196 的越权泄漏。
- 慢查询日志追加 `user`（`:231`-`:233`）。
- `SIGTERM` 时可选择把当前查询 dump 到日志（`LogCurrentQueries`，`task_manager.go:173`-`:181`，
  commit `fd1d51690d`）。

**兼容性**：`SHOW QUERIES` 多一列；老的 5 列客户端若按列数严格解析会受影响。
`QueryInfo` JSON 多一个 `user` 字段（向后兼容）。InfluxQL 的超时/取消链路本身没有重构：
`ExecutionContext.watch`（`v1.12.3:query/execution_context.go:32`）与
`TaskManager.waitForQuery`（`v1.12.3:query/task_manager.go:311`）与旧版结构一致，
差异只在日志与鉴权。

### 3.5 InfluxQL 新增 HLL 算子：`count_hll` / `sum_hll` / `merge_hll`

这是 InfluxQL 执行链在区间内**唯一新增的算子族**（commit `903b8cd0ea`），
目的是让 `SHOW SERIES CARDINALITY` 这类查询用常量内存做基数估计。

旧版没有任何 HLL 入口（`v1.8.10:query/select.go:258` 的分支列表里没有 `count_hll`）。
新版的三个入口：

```go
// v1.12.3:query/select.go:258 — buildExprIterator 的外层 switch expr.Name
case "count_hll", "derivative", "non_negative_derivative", ..., "elapsed":
...
```

```go
// v1.12.3:query/select.go:273 — 上述分支内的二级 switch expr.Name
case "count_hll":
	return NewCountHllIterator(input, opt)      // v1.12.3:query/select.go:274-275
```

```go
// v1.12.3:query/select.go:516-517 — b.callIterator（buildCallIterator）内的分发
case "min", "max", "sum", "first", "last", "mean", "sum_hll", "merge_hll":
	return b.callIterator(ctx, expr, opt)
```

```go
// v1.12.3:query/call_iterator.go:1538 — NewSumHllIterator
// v1.12.3:query/call_iterator.go:1589 — NewCountHllIterator（只接受 StringIterator，输出 UnsignedPoint）
// v1.12.3:query/call_iterator.go:56   — NewCallIterator 分发到 sum_hll / merge_hll
```

关键细节：

- `sum_hll` 在**分片内**累加 sketch，跨分片合并时 `Iterators.Merge` 把表达式替换成
  `merge_hll`：

  ```go
  // v1.12.3:query/iterator.go:157
  // When merging the sum_hll() function, use merge_hll() to sum the counted points.
  if call.Name == "sum_hll" {
      opt.Expr = &influxql.Call{Name: "merge_hll", Args: call.Args}
  }
  ```

- 出错不返回 `error`，而是把错误编码进字符串：`HLLERROR ...` 前缀；
  正常 sketch 用 `HLL_` + base64（`query/functions.go:2169`、`:2213` 的
  `unmarshalPlus` / `marshalPlus`）。代码注释给出的设计动机是“返回 error 会让所有聚合函数慢约 1%”
  （`query/functions.go:2192-2193`），**该数字没有基准出处**，本文只把它当作设计动机，
  不作为跨版本性能结论。可复核的控制流是：错误被编码成 `HLLERROR ...` 字符串，
  `AggregateString` 不返回 `error`。
- `count_hll` 遇到长度合法但内容非法的 sketch 时静默返回 0（`query/functions.go:2256`-`:2262`），
  错误被吞掉——这是有意的性能取舍；但过短的字符串会直接 panic 而不是返回 0（见 §7.6），排障时需要知道。
- `query/compile.go:787` 的 `compileCountHll` 只接受 1 个参数，并把
  `OnlySelectors` 置 false；`compile.go:309` 负责字段级分发。

**数值案例（两个 shard、250 个 series）**：

- **输入/前提**：只有当索引是 TSI 且 `opt.Dimensions` 为空（没有 `GROUP BY`/Dimensions）时，
  `Engine.createCallIterator` 才走 `_seriesKey` 快路径，每个 shard 各产生一个
  `sum_hll(_seriesKey)` sketch（`tsdb/engine/tsm1/engine.go:2917-2927`）。为可复算，把 `_seriesKey` 的固定字节串取为
  `k1`…`k100`（分片 1）与 `k101`…`k250`（分片 2）；HLL 只对字节串做哈希，两组 key 互不重叠，
  逻辑并集为 250。
- **步骤与中间数值**：
  1. 分片内 `sum_hll` 用 `hll.NewDefaultPlus()`（`p=16`）逐 key `Add`，输出 `HLL_`+base64
     （`query/functions.gen.go.tmpl:108-130`、`query/functions.go:2213-2240`）。上述 key 集合实测
     两个 sketch 的二进制长度分别为 **318B / 465B**（version=2、p=16、sparse flag=1）。
  2. `Iterators.Merge` 把表达式改写成 `merge_hll`（`query/iterator.go:157-162`）；
     `StringMergeHllReducer` 先接收分片 1 的 sketch，再 `Plus.Merge` 分片 2。`Merge` 会把 receiver
     的 sparse 表示 `toNormal()`，所以合并结果一定是 **dense**：版本 1B + p 1B + flag 1B +
     长度 4B + 65536B dense list = **65543B**（`pkg/estimator/hll/hll.go:203-230`、`:240-293`）。
  3. `count_hll` 对合并结果调用 `Plus.Count()`；dense 路径走 LogLog-Beta
     （`pkg/estimator/hll/hll.go:171-190`）。对上述固定 key 集合实测 **`Count()=250`**，
     与 `EXACT` 的 250 相等；合并前两个 sparse sketch 的线性计数也分别精确为 100、150。
- **预期输出**：非 EXACT 的 `SHOW SERIES CARDINALITY` / `count_hll(merge_hll(sum_hll(_seriesKey)))`
  → `250`；`EXACT` 的 `count(distinct(_seriesKey))` → `250`。HLL 只提供概率界：
  p=16 时标准误差 `0.40625%`，n=250 时 **1σ≈±1.0、95%≈±2.0**；本例实测估计值恰为 250，
  属于偶然相等。只有显式 `EXACT` 的 `count(distinct(_seriesKey))` 才是精确值；
  n 增大时绝对误差按 `0.40625% × n` 增长。
- **误差上界**：标准误差为 `1.04/sqrt(m)`。本仓库 `p=16`、`m=2^16=65536`，
  `1.04/256 = 0.40625% ≈ 0.41%`；250 个元素对应约 **±1.0**。原文的 `0.81%` 是 p=14 的
  `1.04/128`，与 `DefaultPrecision=16` 不符；`±2` 也相应偏大一倍。
- **为什么不能再用“sparse 可能精确”解释合并结果**：单个 shard 在**未发生 `Merge` 且不同 key 数
  ≤ m=65536** 时保持 sparse，`Count()` 必走 `linearCount`（`hll.go:176-178`）；一旦不同 key 数超过
  65536（`Add` 内 `sparseList.Len() > h.m`，`hll.go:155-158`）或接收过任意一次 `Merge`
  （receiver 无条件 `toNormal`，`hll.go:211-213`），就是 dense，走 LogLog-Beta。
  本例 100/150 < 65536，因此合并前确定是线性计数；合并后确定 dense。要精确值必须显式写 `EXACT`。
- **非法 sketch 边界（可复算）**：

  | 输入 | `unmarshalPlus`/`Count` 行为 | 结果 |
  |---|---|---|
  | `"abc"`（len=3） | `s[:4]` 越界 | panic |
  | `"abcd"`（len=4，前缀非 `HLL_`） | `s[:9]` 越界 | panic |
  | `"HLL_"`（len=4） | data 为空，返回 `(nil,nil)`；`Count()` 的 nil receiver 返回 0 | 0 |
  | `"HLL_AAAA"`（8B） | base64 解出 3B，`UnmarshalBinary` 要求 ≥12B → error | 0 |
  | `"HLLERROR boom"` | 解析出 error | 0 |

  源码：`query/functions.go:2169-2200`（切片与前缀）、`:2256-2262`（置 0）、
  `pkg/estimator/hll/hll.go:171-175`（nil → 0）、`:295-300`（≥12B 校验）。

**行为变化**：`SHOW SERIES CARDINALITY` 的非 EXACT 路径从精确变近似（见 3.6）。
另外 HLL 对**字符串值**直接 `[]byte(p.Value)`，对数值类型用 BigEndian 二进制
（`query/functions.gen.go.tmpl` 的 `SumHllReducer`），输入类型决定估计口径。

### 3.6 InfluxQL 元查询重写：四个 SHOW 的行为变化

**(a) `SHOW SERIES CARDINALITY`：精确 → 近似（带 FROM/WHERE 时）**

```go
// v1.12.3:query/statement_rewriter.go:220-241 — 新增 HLL 分支
if !stmt.Exact && stmt.Dimensions == nil && stmt.Limit == 0 && stmt.Offset == 0 {
	return &influxql.SelectStatement{
		Fields: []*influxql.Field{{
			Expr: &influxql.Call{
				Name: "count_hll",
				Args: []influxql.Expr{&influxql.Call{
					Name: "sum_hll",
					Args: []influxql.Expr{&influxql.VarRef{Val: "_seriesKey"}},
				}},
			},
			Alias: "cardinality estimation", // Alias 是 influxql.Field 的字段，不属于 influxql.Call
		}},
		Sources: rewriteSources2(stmt.Sources, stmt.Database),
		...
	}, nil
}
```

旧版没有这段，非 EXACT 查询落到 `count(distinct(_seriesKey))`（精确、耗内存）：

```go
// v1.8.10:query/statement_rewriter.go:206-211
Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{
	&influxql.Call{Name: "distinct", Args: []influxql.Expr{&influxql.VarRef{Val: "_seriesKey"}}},
}},
```

但注意**裸查询仍然走索引精确计数**：新版保留了开头的早退分支
（`v1.12.3:query/statement_rewriter.go:203`-`:205`），返回原始语句，
由 `executeShowSeriesCardinalityStatement` 调 `TSDBStore.SeriesCardinality`
（`coordinator/statement_executor.go:923`）。所以：
`SHOW SERIES CARDINALITY`（无 FROM/WHERE）= 精确；
`SHOW SERIES CARDINALITY FROM cpu WHERE ...` = HLL 近似。

**数值案例**：3 个 series —— `cpu,host=a`、`cpu,host=b`、`mem,host=a`。

| 查询 | 重写结果 | 执行路径 | 输出 |
|------|----------|----------|------|
| `SHOW SERIES CARDINALITY`（无 FROM/WHERE） | 保持原语句（`v1.12.3:query/statement_rewriter.go:203-205`） | `TSDBStore.SeriesCardinality(db)`（`coordinator/statement_executor.go:923`） | `cardinality estimation = 3`（精确） |
| `SHOW SERIES CARDINALITY FROM cpu` | `count_hll(sum_hll(_seriesKey))`（`:219-243`） | 扫描 2 个 `_seriesKey`（`cpu,host=a`、`cpu,host=b`） | `cardinality estimation = 2` |
| 同上旧版 | `count(distinct(_seriesKey))`（`v1.8.10:query/statement_rewriter.go:207`） | 扫描数据块 | 同为 2，但走精确去重、内存 O(series) |
| `SHOW SERIES CARDINALITY FROM cpu EXACT` | `stmt.Exact` 使 HLL 分支不成立 → 落到 `count(distinct)` | 数据扫描 | 2 |
| `SHOW FIELD KEY CARDINALITY FROM cpu` | `_fieldKeys` 子查询（`:90-92`） | 只读 `_fieldKeys` 迭代器，`fieldKey=value` 去重 | 1（两个 series 的字段都叫 `value`） |

**为什么 3/2 这种小基数看不出近似**：`p=16` 时稀疏阶段用 `m'=2^25` 的线性计数（`pkg/estimator/hll/hll.go:171-175`），Merge 后转 dense 时 LogLog-Beta 对小基数也会四舍五入回整数（`hll.go:176-190`，测试 `TestPlusCount`/`TestHLL_Merge_Normal` 对 5/10 个元素均断言精确值）。近似只在大基数或大量 register 时才显现；不能把旧稿的“3 vs 近似 2”当作精度差异——这里的 2 是因为 `FROM cpu` 只包含 2 个 series。

**(b) `SHOW FIELD KEY CARDINALITY`：数据扫描 → `_fieldKeys` 索引**

旧版直接把测量名替换成真实数据源（`rewriteSources2`），再对 `_fieldKey` 做
`count(distinct(...))`（`v1.8.10:query/statement_rewriter.go:72`-`:86`）。
新版改成子查询，从 `_fieldKeys` 系统迭代器取 `fieldKey`/`fieldType`：

```go
// v1.12.3:query/statement_rewriter.go:90-92 — 子查询源
Sources: influxql.Sources{&influxql.SubQuery{Statement: &influxql.SelectStatement{
	Fields: influxql.Fields([]*influxql.Field{
		{Expr: &influxql.VarRef{Val: "fieldKey"}},
		{Expr: &influxql.VarRef{Val: "fieldType"}},
	}),
	Sources:    rewriteSources(stmt.Sources, "_fieldKeys", stmt.Database),
	Condition:  rewriteSourcesCondition(stmt.Sources, nil),
	OmitTime:   true,
	Dedupe:     true,
	IsRawQuery: true,
}}},
```

配套的 `tsdb.NewFieldKeysIterator`（`v1.12.3:tsdb/shard.go:2291`）曾加过
aux 参数校验，随后因为“企业版到处误用”被 `cc26b7653c`（#23875）回退。
因此当前版本**不会校验** aux 列，调用方传错列名会得到空/异常结果而不是错误。

**(c) `SHOW TAG VALUES`：保留 Sources，单 RP**

旧版重写返回的新语句**没有 `Sources` 字段**：

```go
// v1.8.10:query/statement_rewriter.go:266
return &influxql.ShowTagValuesStatement{
	Database:   stmt.Database,
	Op:         stmt.Op,
	TagKeyExpr: stmt.TagKeyExpr,
	Condition:  condition,
	SortFields: stmt.SortFields,
	Limit:      stmt.Limit,
	Offset:     stmt.Offset,
}, nil
```

新版补上 `Sources: stmt.Sources`（`v1.12.3:query/statement_rewriter.go:317`），
执行器据此限定 shard：

```go
// v1.12.3:coordinator/statement_executor.go:1129 — 收集 RP
rps := make([]string, 0, len(di.RetentionPolicies))
for _, m := range q.Sources.Measurements() {
	if len(m.RetentionPolicy) > 0 { rps = append(rps, m.RetentionPolicy) }
}
...
} else {
	for _, rp := range rps {
		if rp != rps[0] {
			return fmt.Errorf("only one retention policy allowed in SHOW TAG VALUES query: \"%s\", \"%s\"", rp, rps[0])
		}
	}
}
```

同时 `NormalizeStatement` 对 `ShowTagValuesStatement` 跳过 measurement 归一化，
以便“不写 RP 时跨所有 RP”（`coordinator/statement_executor.go:1419`）。
另外 `SHOW TAG KEYS` 新增 `WITH KEY`（`query/statement_rewriter.go:398`-`:425`，
commit `17b9ea8723`）。

**(d) `SHOW MEASUREMENTS` 新增通配库/RP**（`611a4370a2` #22388）：`ON *.*` 展开所有 db×rp，
`ON db.*` 展开该库所有 RP；`ON *`、`ON *.rp` 报错
（`v1.12.3:coordinator/statement_executor.go:733`-`:770`）。它依赖 influxql v1.2.0 新增的
`WildcardDatabase`/`WildcardRetentionPolicy` AST 字段：v1.8.10 的解析器在 `ON` 后只接受
`ParseIdent`，`ON *.*` 直接解析失败。

### 3.7 `storage/reads` 契约：`WindowAggregate`、`GroupCursor.Aggregate()`、proto

**(a) Store 接口与结果集**

```go
// v1.12.3:storage/reads/store.go:81
WindowAggregate(ctx context.Context, req *datatypes.ReadWindowAggregateRequest) (ResultSet, error)

// v1.12.3:storage/reads/store.go:75 — GroupCursor 新增
Aggregate() *datatypes.Aggregate
```

返回结果集的读取方法只有 `ReadFilter` / `ReadGroup`（`v1.8.10:storage/reads/store.go:76`-`:78`），
`GroupCursor` 没有 `Aggregate()`；`Store` 另有 `TagKeys` / `TagValues` / `GetSource`。

**(b) 请求 proto 新增**

```proto
// v1.12.3:storage/reads/datatypes/storage_common.proto:86-94
message ReadWindowAggregateRequest {
  google.protobuf.Any ReadSource = 1;
  TimestampRange range = 2;
  Predicate predicate = 3;
  int64 WindowEvery = 4;
  int64 Offset = 6;
  repeated Aggregate aggregate = 5;
  Window window = 7;
}
```

同时 `Aggregate.AggregateType` 从 `NONE/SUM/COUNT` 扩到
`...Min=3 / Max=4 / First=5 / Last=6 / Mean=7`（`:74`-`:78`），
响应新增 `DataTypeMulti = 5` 与 `MultiPointsFrame`（`:126`、`:179`），
用于 mean+count 双聚合一次返回两列。

**(c) 分片方向选择**

```go
// v1.12.3:services/storage/store.go:195
descending := !reads.IsAscendingGroupAggregate(req)
shardIDs, err := s.findShardIDs(database, rp, descending, start, end)
// v1.12.3:services/storage/store.go:252
descending := !reads.IsAscendingWindowAggregate(req)
```

旧版 `ReadGroup` 固定 `findShardIDs(..., false, ...)`（`v1.8.10:services/storage/store.go:162`），
即永远升序。新版为了 `last()` 的“降序游标 + limit 1”优化，会让 shard 列表反向
（`storage/reads/group_resultset.go:46`、`aggregate_resultset.go:28`）。
`IsAscendingWindowAggregate` 只在“单聚合且为 last 且窗口为单窗口”时返回 false
（`aggregate_resultset.go:28`-`:47`），否则仍然升序。

**(d) 谓词 proto 更换实现**

```proto
// v1.12.3:storage/reads/datatypes/predicate.proto:44
bytes TagRefValue = 9;     // 旧：string tag_ref_value = 9
```

生成器从 gogo 切到 `google.golang.org/protobuf`（`option go_package = ".;datatypes"`），
枚举常量名从 `NodeTypeLogicalExpression` 变成 `Node_TypeLogicalExpression`。
**线格式（字段号/类型）没变，但 Go API 变了**：`storage/reads.Store.GetSource`
的返回类型从 `github.com/gogo/protobuf/proto.Message` 变成
`google.golang.org/protobuf/proto.Message`（`v1.12.3:storage/reads/store.go:9`、`:86`）。
任何外部实现 `Store` / `Reader` 的代码都要改 import。

枚举常量名也统一变成 `<Message>_<Enum>_<Value>`：`FrameTypePoints` → `ReadResponse_FrameTypePoints`
（`v1.12.3:storage/reads/datatypes/storage_common.pb.go:199`）、`HintNone` → `ReadGroupRequest_HintNone`
（`:81`）、`AggregateTypeMean` → `Aggregate_AggregateTypeMean`（`:141`）、`GroupBy` →
`ReadGroupRequest_GroupBy`（`:32`）。`TimestampRange` 从值类型变成指针（旧 proto 标了
`(gogoproto.nullable) = false`），取字段前要判空；`Storage` service 新增
`rpc ReadWindowAggregate`（`storage_common.proto:16`）。

**(e) 谓词转换时机前移**

旧版在 `storage/reads/flux_reader.go` 每次 `ReadFilter`/`ReadGroup`/`handleRead`
都调用 `toStoragePredicate(spec.Predicate)`（`v1.8.10:storage/reads/flux_reader.go:57`、`:77`、`:121`、`:245`）。
新版把语义树转成 `datatypes.Predicate` 的工作放到物理计划规则里
（`v1.12.3:flux/stdlib/influxdata/influxdb/rules.go:179`），`ReadFilterSpec.Predicate`
直接就是 `*datatypes.Predicate`（`flux/stdlib/influxdata/influxdb/storage.go:69`），
reader 少一次遍历。`storage/reads/flux_predicate.go` 整个文件（183 行）被删除。
同一提交（`7210fa6355` #20767）还在 `query/encode.go` 新增 120 行 `NoContentDialect`：
`no-content` 方言把响应状态设为 204 并丢弃查询结果（仍消费 results），另有
`no-content-with-error` 变体，供 Flux 客户端做“只关心成败”的查询。

**模块卡：`NoContentDialect`（`query/encode.go`，`v1.8.10` 无此文件）**

- 入口：Flux 请求的 `dialect` 注册 `no-content` / `no-content-with-error`
  （`query/encode.go:12-13`）。
- 行为：`SetHeaders` 写 HTTP 204（`:34-36`）；`NoContentEncoder.Encode` 仍然
  `defer results.Release()` 并逐表消费结果，但不写任何字节（`:40-54`）。
- 变体：`NoContentWithErrorDialect` 把运行期错误编码成 CSV；客户端用“响应体长度是否为 0”
  判断有无错误（`:56-70`）。
- 风险：`no-content` 会吞掉运行期错误；需要区分成功/失败时必须用
  `no-content-with-error`，且客户端要检查响应体而不是只看 204。

### 3.8 `array_cursor.gen.go`：从 23 个类型到 50 个类型

`storage/reads/array_cursor.gen.go` 的规模变化：+3423 / −399。

| 能力 | 旧（v1.8.10） | 新（v1.12.3） |
|------|---------------|---------------|
| 基础 filter/multishard | 有 | 有（错误处理加强） |
| filter 游标跨 series 条件复用 | `reset` 复用 `c.filter` 但不更新 `cond`；含 `or` 的谓词跨 series 会沿用上一条 series 的条件 | `else { c.filter.cond = cond }`（5 处生成代码），修复旧代码因不更新 `cond` 导致的丢点/多点，`5976e41d54` #23522 |
| 单聚合 sum/count | `floatArraySumCursor`、`integer*CountArrayCursor` | 能力保留，旧的非窗口实现被窗口化实现取代（`newWindowSumArrayCursor` 等，窗口为零值时即整段） |
| limit（first/last 非窗口聚合） | 无 | `newLimitArrayCursor` / `floatLimitArrayCursor`（`:27`、`:367`） |
| 窗口 first/last | 无 | `newWindowFirstArrayCursor` / `newWindowLastArrayCursor`（`:50`、`:76`） |
| 窗口 count/sum | 无 | `:102`、`:125` |
| 窗口 min/max | 无 | `:145`、`:162` |
| 窗口 mean | 无 | `:179` |
| 窗口 mean+count | 无 | `:199`，输出 `cursors.MeanCountArray` |
| 空游标 | 有（每种类型一个） | 保留 |

`newAggregateArrayCursor` 的分发从“只认 Sum/Count”扩展到 7 种：

```go
// v1.12.3:storage/reads/array_cursor.go:20
func newAggregateArrayCursor(ctx context.Context, agg []*datatypes.Aggregate, cursor cursors.Cursor) (cursors.Cursor, error) {
	switch agg[0].Type {
	case datatypes.Aggregate_AggregateTypeFirst, datatypes.Aggregate_AggregateTypeLast:
		return newLimitArrayCursor(cursor), nil       // 单窗口 selector 只需第一个点
	}
	return NewWindowAggregateArrayCursor(ctx, agg, interval.Window{}, cursor)
}
```

```go
// v1.8.10:storage/reads/array_cursor.go:24
switch agg.Type {
case datatypes.AggregateTypeSum:   return newSumArrayCursor(cursor)
case datatypes.AggregateTypeCount: return newCountArrayCursor(cursor)
default: panic("invalid aggregate")
}
```

**时间范围语义**（commit `8edf7a4e2f`）：

```go
// v1.12.3:storage/reads/array_cursor.go:79-91
// The range of the cursor is [start, end)...
// When we construct the CursorRequest, we translate the time range
// from [start, stop) to [start, stop]...
req: cursors.CursorRequest{Ascending: asc, StartTime: start, EndTime: end - 1}
```

旧版直接 `EndTime: end`（`v1.8.10:storage/reads/array_cursor.go:100`）。
`tsdb/cursors.CursorRequest` 的新注释明确写了 `StartTime`/`EndTime` 都是 **inclusive**
（`v1.12.3:tsdb/cursors/cursor.go:64`-`:71`）。
影响：Flux 的 `range(start, stop)` 语义是 `[start, stop)`。v1.8.10 的 Flux 数据读路径
（`ReadFilter`/`ReadGroup`；`ReadWindowAggregate` 是 v1.12.3 才加入的第三条数据路径）
在 Flux 层传 `Range.End = Bounds.Stop`，`storage/reads` 又把它原样作为闭区间 `EndTime=end`，
底层游标只裁 `> c.end`，因此**只要该 series 在 `t==stop` 有数据点就必然返回**
（cache 与 TSM 两条来源都保留 `== end`）。v1.12.3 的三条数据路径在 Flux 层仍传 `Bounds.Stop`，
但 `storage/reads/array_cursor.go:90` 统一传 `EndTime=end-1`，因此**必不返回 `t==stop`**。
InfluxQL 不受影响：它不经过 `storage/reads`，上界由语句的 `TimeRange` 决定
（`query/compile.go:1198`，来自 `WHERE time`），两版相同。

**逐类型错误返回**：`reset` 从无返回值变成 `error`，`build*ArrayCursor` 同步改签名
（`v1.12.3:tsdb/engine/tsm1/array_cursor.gen.go:42`、
`tsdb/engine/tsm1/array_cursor_iterator.gen.go:18`），
`multiShardArrayCursors.createCursor` 在 `cur == nil || err != nil` 时返回 nil
（`storage/reads/array_cursor.go:126`），不再把错误丢掉后对 nil 游标调 `Next()`。

**案例（窗口 mean/count、mean 与 limit 的数值输出）**：

- **输入**：同一个 float 序列，时间戳/值（秒）为
  `(0,1)(5,2)(9,3)(10,4)(15,5)(20,6)`；窗口 `every=10s`、`offset=0`。
- **步骤与输出**：
  1. `newFloatWindowMeanCountArrayCursor` 以 `GetLatestBounds(ts).Stop()` 作为窗口终点：
     第一个窗口 `[0,10)` 收 0/5/9，输出时间戳 **10**、mean `(1+2+3)/3=2`、count 3；
     第二个窗口 `[10,20)` 收 10/15，输出 **20**、mean `(4+5)/2=4.5`、count 2；
     第三个窗口 `[20,30)` 收 20，输出 **30**、mean 6、count 1。
  2. 因此 `MeanCountArray` 为 `Timestamps=[10,20,30]`、`Values0=[2,4.5,6]`、
     `Values1=[3,2,1]`；输出时间戳是窗口 **stop**，不是 start，空窗口不产生点
     （`storage/reads/array_cursor.gen.go:1096-1225`）。
  3. `newFloatWindowMeanArrayCursor` 对同一输入只输出单列
     `Timestamps=[10,20,30]`、`Values=[2,4.5,6]`；mean+count 只是多一列
     （`array_cursor.gen.go:988-1080`）。
  4. limit 快路径：升序 cursor 上 `first()` 返回第一个点 `(0,1)`；`last()` 使用同一
     `floatLimitArrayCursor`，但由 `services/storage`/`multiShardArrayCursors` 以
     `Ascending=false` 读取，游标第一批的第一个点就是 `(20,6)`，因此输出 `(20,6)`
     （`array_cursor.gen.go:367-394`、`array_cursor.go:20-25`）。
- **源码**：`storage/reads/array_cursor.gen.go:27-48`（limit 分发）、`:367-394`、
  `:988-1225`（窗口 mean/mean+count）。

### 3.9 group/aggregate resultset 的变化

**旧版：`Cursor()` 现场组装聚合游标。**

```go
// v1.8.10:storage/reads/group_resultset.go:332
func (c *groupByCursor) Cursor() cursors.Cursor {
	cur := c.mb.createCursor(*c.rows[c.i-1])
	if c.agg != nil {
		cur = c.mb.newAggregateCursor(c.ctx, c.agg, cur)
	}
	return cur
}
```

**新版：`Next()` 里提前建好，`Cursor()` 只返回结果。**

```go
// v1.12.3:storage/reads/group_resultset.go:343
func (c *groupByCursor) Next() bool {
	if c.i < len(c.seriesRows) {
		c.i++
		c.cursor, c.err = c.createCursor(*c.seriesRows[c.i-1])
		return c.err == nil
	}
	return false
}
// v1.12.3:storage/reads/group_resultset.go:360
func (c *groupByCursor) Cursor() cursors.Cursor { return c.cursor }
```

原因：`newAggregateArrayCursor` 现在会返回错误（例如对 String 做 mean），
必须在 `Next()` 里把错误转成 `Next()==false`（错误本身仍被 `groupByCursor.Err()` 吞掉，见 §7.1），不能等到 `Cursor()`。

**新增 `windowAggregateResultSet`**（`storage/reads/aggregate_resultset.go`，旧版无此文件）：

- `NewWindowAggregateResultSet` 校验聚合个数：只允许 1 个，或 `mean+count` 两个，
  其余返回 `errors.InternalError`（`:50`-`:63`）。
- `createCursor` 判断窗口是否 `math.MaxInt64`（裸聚合），是则 `newAggregateArrayCursor`，
  否则 `NewWindowAggregateArrayCursor`（`:119`-`:131`）。
- `Err()` 返回创建游标时的错误（`:148`），这是新结果集里唯一真正会报错的地方。

**旧版 `resultSet.agg` 是死代码**：`NewFilteredResultSet` 从未给它赋值
（`v1.8.10:storage/reads/resultset.go:25`-`:31`），所以旧版 filter 路径的聚合分支永远不生效；
真正生效的只有 group 路径。新版删掉了这个字段（`resultSet` 只剩 `ctx/seriesCursor/seriesRow/arrayCursors`，
`v1.12.3:storage/reads/resultset.go:14`-`:19`）。

**案例（group sum 遇到 string：错误被折叠成“无数据”）**：

- **输入**：`group(columns:["host"]) |> sum()`，三个 group 各一条 series：
  `host=web01` 有 100 个 float 点（值 1…100），`host=web02` 有 1 个 string 点，
  `host=web03` 有 50 个 float 点（值 1…50）。
- **步骤与输出**：
  1. web01、web03 分别建出 `newWindowSumArrayCursor`，各输出一行
     `_value=5050`、`_value=1275`（无窗口时 sum 整段）。
  2. web02 的 cursor 是 `cursors.StringArrayCursor`，`newWindowSumArrayCursor` 返回
     `EInvalid: unsupported input type for sum aggregate: string`，`groupByCursor.Next()`
     因此返回 `false`，`groupByCursor.Err()` 仍返回 `nil`
     （`storage/reads/array_cursor.gen.go:125-142`、`group_resultset.go:343-347`、`:333`）。
  3. `storage/flux` 的 group 循环看到 `cur == nil`，关闭该 group 并继续下一个
     （`storage/flux/reader.go:324-329`）。
- **预期输出**：2 行（web01=5050、web03=1275），不是 3 行；`WriteGroupResultSet` 的
  `gc.Err()` 检查也拿不到错误（`response_writer.go:116`）。若一个 group 内有多条 series，
  失败之后的 series 会被截断（`storage/flux/table.gen.go:963-983`）。
- **窗口聚合个数校验**：`NewWindowAggregateResultSet` 只接受 1 个聚合，或
  `[Mean, Count]` 两个；`[Mean, Sum]` 返回 `InternalError`，`[Mean, Count]` 接受并走
  `MeanCountArray`（`storage/reads/aggregate_resultset.go:50-63`、`:119-131`）。
- **源码**：`storage/reads/group_resultset.go:281`、`:333`、`:343-347`；
  `storage/flux/reader.go:324-329`；`storage/flux/table.gen.go:963-983`。

### 3.10 Flux 物理计划下推：`flux/stdlib/influxdata/influxdb/rules.go`（owner: 计划层）

**归属与角色**：本节属于 **Flux 物理计划层**（包 `flux/stdlib/influxdata/influxdb`），不属于
`storage/flux` 表执行层。它负责把 `From` 生成的 `ReadRange`/`ReadGroup` 与上层
`window`/聚合算子改写成带聚合信息的物理节点；`storage/flux` 只消费改写后的
`ReadWindowAggregate`/`ReadGroup` 请求。规则漏判的后果是“退回 Flux 层计算，结果正确但慢”；
表/游标层的失败则是 panic、nil 游标或吞错（§7.1-§7.3）。两者必须在章节上分开。

**注册顺序**（`v1.12.3:flux/stdlib/influxdata/influxdb/rules.go:21-33`）：
`FromStorageRule → PushDownRangeRule → PushDownFilterRule → PushDownGroupRule →
PushDownReadTagKeysRule → PushDownReadTagValuesRule → SortedPivotRule →
PushDownWindowAggregateRule → PushDownWindowAggregateByTimeRule →
PushDownBareAggregateRule → PushDownGroupAggregateRule`，共 11 条。
旧版 `init()` 只注册 6 条、没有任何聚合下推（`v1.8.10:flux/stdlib/influxdata/influxdb/rules.go:12-20`）。

**匹配与产出**：

| 规则 | 匹配模式 | 产出 | 证据 |
|------|----------|------|------|
| `PushDownWindowAggregateRule` | `ReadRange \|> window \|> {count,sum,min,max,mean,first,last}` | `ReadWindowAggregatePhysSpec`（WindowEvery/Offset/CreateEmpty） | `:676-775` |
| `PushDownWindowAggregateByTimeRule` | `ReadWindowAggregate \|> duplicate(_start/_stop) \|> window(inf)` | 设置 `TimeColumn`，供 `aggregateWindow` 使用 | `:777-833` |
| `PushDownBareAggregateRule` | `ReadRange \|> 聚合` | `ReadWindowAggregate`，`WindowEvery=MaxInt64` | `:837-863` |
| `PushDownGroupAggregateRule` | `ReadGroup \|> {count,sum,first,last,min,max}` | `ReadGroup` + `AggregateMethod`（单聚合） | `:866-957` |

**流程**：

```mermaid
flowchart LR
    A["FromNode"] -->|"FromStorageRule"| B["FromStorage（logical，FromStorageProcedureSpec）"]
    B -->|"PushDownRangeRule"| B1["ReadRangePhysSpec"]
    B -->|"PushDownGroupRule"| B2["ReadGroupPhysSpec"]
    B1 -->|"Range / Filter"| C["ReadRange + datatypes.Predicate"]
    C -->|"window(every, period) + agg"| D["PushDownWindowAggregateRule"]
    C -->|"裸聚合"| E["PushDownBareAggregateRule"]
    D --> F["ReadWindowAggregatePhysSpec"]
    E --> EB["ReadWindowAggregatePhysSpec（WindowEvery=MaxInt64；与 F 同一物理节点、窗口参数不同）"]
    F -->|"duplicate(_start/_stop) + window(inf)"| G["PushDownWindowAggregateByTimeRule"]
    G --> H["TimeColumn=_stop / _start"]
    B2 -->|"ReadGroup + agg"| I["PushDownGroupAggregateRule"]
    I --> J["ReadGroupPhysSpec.AggregateMethod"]
```

**代码卡**：

- `canPushWindowedAggregate`（`:697-726`）只检查聚合列是 `_value`，**不检查字段类型**；
- `isPushableWindow`（`:728-742`）要求 `every==period`、非负 offset、`TimeColumn=_time`、
  `CreateEmpty` 为 false；
- `PushDownGroupAggregateRule`（`:885-953`）拒绝多聚合（`len(group.AggregateMethod)>0`），
  且 `mean` 不在 group 可下推集合内。

**计划期谓词**：`PushDownFilterRule` 在 `rules.go:179` 调用 `ToStoragePredicate`，
`ReadFilterSpec.Predicate` 从此固定为 `*datatypes.Predicate`（`storage.go:69`）。
旧版每次 `ReadFilter`/`ReadGroup`/`handleRead` 都重新转换（`v1.8.10:storage/reads/flux_reader.go:57`、`:77`、`:121`、`:245`）。

**案例**：`aggregateWindow(every: 1m, fn: mean)` 的完整下推轨迹见 §6.3；
`from |> range |> filter` 的 `ReadFilter` 非窗口表见 §3.11。

**风险**：① `mean` 只能窗口下推，group 下推不支持；② min/max 不校验类型，
对 String 字段下推后会在 `storage/reads/array_cursor.gen.go:158`/`:175` panic（§7.3）；
③ 规则只在计划期生效，`ReadFilterSpec.Predicate` 一旦定型，运行期没有“退回逐点过滤”的兜底，
谓词转换错误会直接影响读取范围。

### 3.11 `storage/flux`：表生成与列读取（`table.gen.go` / `table.go`，owner: 执行层）

**归属与角色**：本节属于 **Flux 存储读执行层**（包 `storage/flux`）。它把
`services/storage` 返回的 `reads.ResultSet`/`GroupResultSet` 转成 Flux `Table`，
负责窗口切分、列读取和空表语义；不负责“是否下推”。下推规则见 §3.10。

**先厘清搬家**：旧版 Flux 的 reader/table 在 `storage/reads/flux_reader.go`、
`storage/reads/flux_table.go`、`storage/reads/flux_table.gen.go`、`storage/reads/flux_tags_cache.go`；
新版整体搬到独立包 `storage/flux`（`package storageflux`），
`cmd/influxd/run/server.go:41` 用别名 `reads` 导入。`storage/reads` 不再依赖 Flux 的
`flux.Table` API，只保留 `flux/interval` 与 `flux/values`（窗口计算需要）。

**表类型：10 → 25。** 对 5 种 Flux array 类型（Float/Integer/Unsigned/String/Boolean，`flux/array`），
每种生成 5 个结构体：

| 表类型 | 用途 | 旧 | 新证据 |
|--------|------|----|--------|
| `<T>Table` | filter 读、无聚合 | 有 | `table.gen.go:32` |
| `<T>GroupTable` | group 读、无窗口 | 有 | `table.gen.go:657` |
| `<T>WindowTable` | 窗口 count/sum/mean 等非 selector | 无 | `floatWindowTable` `:107` |
| `<T>WindowSelectorTable` | 窗口 first/last/min/max | 无 | `floatWindowSelectorTable` `:320` |
| `<T>EmptyWindowSelectorTable` | `createEmpty=true` 的空窗口 | 无 | `floatEmptyWindowSelectorTable` `:416` |

合计 25 个 `*Table struct`；旧版 `storage/reads/flux_table.gen.go` 只有 10 个
（`v1.8.10:storage/reads/flux_table.gen.go:25`、`:101`）。

**列式读取的核心**（`storage/flux/table.go`）：

```go
// v1.12.3:storage/flux/table.go:139 — colReader
type colReader struct {
	refCount int64            // allocateBuffer 据此判断能否复用
	key     flux.GroupKey
	colMeta []flux.ColMeta
	cols    []array.Array
	l       int
}
func (cr *colReader) Floats(j int) *array.Float { ... }   // CheckColType + 类型断言，不复制底层数组
```

- `table.allocateBuffer(l)` 复用上一次的 `colReader`：如果引用计数已归零就重置，
  否则新建（`table.go:124`-`:137`）。
- `table.init(advance)` 在构造时先推进一次，**提前**确定 `empty`
  （`table.go:73`-`:75`）；`table.do` 开头若 `t.err != nil` 直接返回
  （`table.go:87`-`:89`），旧版没有这一步，初始化错误会被吞掉。
- `readTags` 跳过不在列定义里的 tag（`table.go:194`-`:211`）。这是 group aggregate
  必需的：group 聚合只输出 group key 里的 tag，但 `rs.Tags()` 返回完整 tag 集。

**窗口表的实现要点**（以 `floatWindowTable` 为例，`table.gen.go:107`-`:320`）：

1. `createNextBufferTimes`：`createEmpty=true` 时按 `window.NextBounds` 枚举窗口，
   并用 `getWindowBoundsFor` 把 `_start`/`_stop` **裁剪到查询 range**；
   否则从游标读时间戳，用 `window.PrevBounds(window.GetLatestBounds(ts))` 反推。
2. `nextAt(ts)` + `isInWindow(ts, stop)`：判断某个点是否属于某窗口，
   容许 stop 被 range 截断（注释详细解释了 `(start, stop]` 的判定）。
3. `mergeValues`：把窗口内多个点压成一个值；整数 `count` 通过 `fillValue=0`
   把空窗口填 0（`table.go:272`-`:280`，`fillValue` 由 `reader.go:739`-`:744` 传入）。
4. `WindowSelectorTable` 的窗口边界由 `window.GetLatestBounds(ts)` 计算；仅在
   `timeColumn == ""`（非 `aggregateWindow`）时 selector 的 `_time` 取数据点时间，
   `aggregateWindow` 下 `_time` 是窗口 `_start`/`_stop`。`EmptyWindowSelectorTable`
   只在窗口内无数据时补空行；整表无数据时 `advance()` 直接返回 `false`，表被标记
   为 empty，不会产生任何行（`table.gen.go:463-466`；`table.go:73-75`、`:77-101`）。

**窗口切分**：当 spec 不带 `TimeColumn`（即不是 `aggregateWindow`）时，
`windowAggregateIterator.handleRead` 用 `splitWindows` 把“一张表含多窗口”
拆成“一窗口一张表”，并重写 group key 的 `_start`/`_stop`：

```go
// v1.12.3:storage/flux/window.go:20
func splitWindows(ctx context.Context, alloc memory.Allocator, in flux.Table, selector bool, f func(t flux.Table) error) error
// v1.12.3:storage/flux/reader.go:695-700
if timeColumn == "" {
	tableFn := f
	f = func(table flux.Table) error {
		return splitWindows(wai.ctx, wai.alloc, table, selector, tableFn)
	}
}
```

selector 且值为 null 时，`splitWindows` 产出一张空表（`window.go:70`-`:75`），
以保持 `first()/last()` 对空窗口的语义。

> 下推规则、注册顺序与匹配/产出见 §3.10；本节只保留表执行层的列读取与窗口切分。

**结果形态**：非 selector 窗口聚合的输出列是 `_start,_stop,_value,tags...`（无 `_time`），
selector 则保留 `_time`（`reader.go:410`-`:452`、`:514`-`:581`）。
这与 Flux 自身 `window()+aggregate` 的输出一致，因此下推对用户透明。

**案例（filter 表与 group 表的列布局）**：

- **输入**：`from(bucket:"db/rp") |> range(start:10:00, stop:10:02) |>
  filter(fn:(r)=>r.host=="a")`；存储返回两行
  `(10:00:30, host=a, region=west, _value=60)`、`(10:01:30, host=a, region=west, _value=70)`。
- **filter 表（`newFloatTable`）**：`determineTableColsForSeries` 生成
  `_start,_stop,_time,_value,host,region`（所有 series tag 都是列，`reader.go:454-481`、
  `reader.go:193-205`）；两行的 `_time` 分别为 10:00:30、10:01:30，`_value` 为 60、70。
  此路径 `readTags` 不会跳过 region，因为 region 在列定义里。
- **group 表（`group(columns:["host"]) |> count()`）**：group key 为 `_start,_stop,host`，
  `determineTableColsForGroup` 生成 `_start,_stop,host,_value`（聚合分支没有 `_time`，
  `reader.go:514-560`）；`readTags` 遍历完整 tag 集时发现 `region` 不在 `t.cols` 中，
  直接 `continue` 跳过（`table.go:194-211`）。预期一行
  `(_start=10:00, _stop=10:02, host=a, _value=2)`。
- **错误路径**：`table.do` 开头若 `t.err != nil` 直接返回，不再把初始化错误吞掉
  （`table.go:87-89`）。
- **源码**：`storage/flux/reader.go:454-481`、`:514-560`；`storage/flux/table.go:87-89`、
  `:185-211`；`storage/flux/table.gen.go:107-320`（窗口表列布局）。

**三条读取路径**（与 §3.10 的计划层对应）：

| 路径 | 入口 | 产出表 | 证据 |
|------|------|--------|------|
| `ReadFilter` | `storeReader.ReadFilter`（`reader.go:65`） | `new{Type}Table` | `table.gen.go:32` |
| `ReadGroup` | `ReadGroup`（`reader.go:75`） | `new{Type}GroupTable` | `table.gen.go:657` |
| `ReadWindowAggregate` | `ReadWindowAggregate`（`reader.go:85`） | `WindowTable` / `WindowSelectorTable` / `EmptyWindowSelectorTable` | `table.gen.go:107`/`:320`/`:416` |

**失败模式**（与计划层分开）：

| 失败点 | 现象 | 归属 |
|--------|------|------|
| 规则未下推 | 原始点表，结果正确但慢 | §3.10 计划层 |
| `table.do` 初始化错误 | 直接返回，不再被吞掉（`table.go:87-89`） | 本节执行层 |
| min/max 类型不支持 | `array_cursor.gen.go:158`/`:175` panic | §3.19 游标契约 / §7.3 |

**风险**：① `allocateBuffer` 在旧 `colReader` 的 `refCount>0` 时**新建** `colReader` 与
`len(cols)` 个 `array.Array`（`storage/flux/table.go:124-137`），不复用旧 buffer；
`table.do` 在回调 `f` 返回后立即 `Release()`，之后才 `advance()` 取下一批（`table.go:91-98`）。
因此风险是**内存峰值**而非数据错误：若调用方在 `f` 内保留 `ColReader` 超过一次 `advance()`，
每多持有一份就多一份 Arrow 缓冲；列数据已由 `arrow.NewInt`/`toArrowBuffer` 从 cursor 复制
（`storage/flux/table.gen.go:98-100`、`table.go:242-243`），不会在下次 `Next()` 时被改写。
量级示例：5 列 float × 1000 点 × 8B ≈ 40KB/份。② `readTags` 跳过不在列定义里的 tag，若 spec 的
group key 与表列不一致会静默丢 tag；③ 整数 `count` 用 `fillValue=0` 补空窗口，`createEmpty=false`
时不会产生空窗口行，二者语义不同。

### 3.12 结果序列化：`response_writer` 的变化

本节的全部新行为来自 `1d92b7a8fc`（#23846 “response writer simplification”）；
`32aa970eba`（#21291）只贡献 `MultiPointsFrame` / `DataTypeMulti` 与 mean+count 写出路径。

**旧版：先建 SeriesFrame，没点再回退。**

```go
// v1.8.10:storage/reads/response_writer.go:197 — startSeries 先占位
f := w.getSeriesFrame(next)
w.sf = f.Series
...
w.res.Frames = append(w.res.Frames, datatypes.ReadResponse_Frame{Data: f})
```

`WriteResultSet` 先 `startSeries` 再 `streamCursor`（`v1.8.10:storage/reads/response_writer.go:67`-`:89`）。
真正的回退在每个 `stream<Type>ArraySeries/ArrayPoints` 内部：`ss := len(w.res.Frames)-1` 是**局部**变量，
`seriesValueCount == 0` 时 `putSeriesFrame` 后 `w.res.Frames = w.res.Frames[:ss]`
（`v1.8.10:storage/reads/response_writer.gen.go:108`-`:112` 等 10 处）。`ResponseWriter.ss` 字段只在
`startSeries` 写入（`:202`），全文件从未读取，是死字段，新版已删除。

**新版：有点才建，类型由 `startSeries` 参数传入。**

```go
// v1.12.3:storage/reads/response_writer.go:205
func (w *ResponseWriter) startSeries(dataType datatypes.ReadResponse_DataType, next models.Tags) {
	f := w.getSeriesFrame(len(next))
	sf := f.Series
	sf.DataType = dataType
	...
}
// v1.12.3:storage/reads/response_writer.gen.go:71 — streamFloatArrayPoints
if seriesValueCount == 0 {
	w.startSeries(datatypes.ReadResponse_DataTypeFloat, tags)
}
```

- `SeriesFrame.data_type` 在 v1.8.10 就已写入：`stream*ArraySeries` 与 `stream*ArrayPoints` 共 10 处
  （`v1.8.10:storage/reads/response_writer.gen.go:40`、`:53`、`:143`、`:156`、`:246`、`:259`、`:349`、`:362`、`:452`、`:465`）。
  #23846 只是把赋值收进 `startSeries(dataType, tags)` 并在空 series 时不建 frame。区间内真正新增的是
  `DataTypeMulti = 5` 与 `MultiPointsFrame`（#21291）。
- 新增 `MeanCountArray` 的写出路径（`streamMeanCountArrayPoints`，对应
  `ReadResponse_Frame_MultiPoints` / `AnyPoints`）。
- `Flush` 改为 `defer` 回收所有 frame（`response_writer.go:262`-`:299`），
  即使写失败也会把 buffer 放回池子；旧版在 `stream.Send` 失败时直接返回，frame 留在切片里不被回收。
- 错误传播：`WriteResultSet` 结束返回 `rs.Err()`（`:91`），
  `WriteGroupResultSet` 每组结束检查 `gc.Err()`（`:116`）。
- 大小估算从生成代码的 `f.Size()`/`w.sf.Size()` 改成 `proto.Size(fr)`/`proto.Size(sf)`
  （`v1.12.3:storage/reads/response_writer.go:202`、`:216`）；数组侧 `a.Size()` 沿用旧实现
  （`v1.8.10:storage/reads/response_writer.gen.go:81` 已是 `w.sz += a.Size()`）。
  注释说明 `w.sz` 只是估算值，累加 `a.Size()` 是 O(batch) 且便宜；`frame.Size()` 在 varint
  数字上需要遍历，可能更贵（`v1.12.3:storage/reads/response_writer.go:382-386`）。
  **这是实现选择，仓库没有该路径的 benchmark**，不作为性能结论。

**frame 案例**：

输入 A（空 series）：Storage read 返回 2 个 group，`cpu,host=a` 在窗口内有 2 个点（1 个 FloatArray batch），`cpu,host=b` 在窗口内 0 个点（`cur.Next()` 返回空数组）。

| 版本 | 对 A 的帧操作 | 对 B 的帧操作 | 最终 `Frames` |
|------|----------------|----------------|----------------|
| v1.8.10 | `startSeries` 先压入 SeriesFrame；`streamFloatArrayPoints` 再压入 FloatPoints，循环 0 次后 `w.res.Frames[:ss]` 同时回退 SeriesFrame 与空 FloatPoints（`v1.8.10:storage/reads/response_writer.gen.go:66-100`） | 同样先建 SeriesFrame + 空 FloatPoints，再回退 | `[SeriesFrame(A), FloatPoints(A)]`，2 帧；B 的 2 个 frame 是瞬态分配 |
| v1.12.3 | 首个非空 batch 才 `startSeries(DataTypeFloat, tags)`，同批创建 FloatPoints（`response_writer.gen.go:71-90`） | 循环第一次 `a.Len()==0` 直接 break，`startSeries` 从未调用 | 同样 2 帧；B 不产生任何 frame，`WrittenN=0` |

输入 B（mean+count 双聚合）：1 个 series、3 个时间窗口 `Timestamps=[10,20,30]`、`Values0=[2,4.5,6]`、`Values1=[3,2,1]`。

| 项 | 结果 |
|----|------|
| 帧数 | 2：`SeriesFrame(data_type=DataTypeMulti=5)` + `MultiPointsFrame`（`ReadResponse_Frame_MultiPoints`） |
| `MultiPointsFrame` 内容 | 3 个时间戳 + 3 个 mean + 3 个 count，共 3 列 |
| `MeanCountArray.Size()` | `3*8 + 3*8 + 3*8 = 72` 字节（`tsdb/cursors/arrayvalues.go:33-37`） |
| `w.sz` 估算 | 累加 72（用 `a.Size()`，不是 `proto.Size(frame)`） |
| 旧客户端 | 旧 gogo 代码把 `MultiPointsFrame` 当未知 oneof 跳过（`skipStorageCommon`），不报错但读不到 mean+count 两个值 |

`Flush` 的回收差异：旧版在 `stream.Send` 失败时直接返回，已 append 的 frame 留在 `w.res.Frames`；新版 `Flush` 用 `defer` 统一把 frame 放回池（`response_writer.go:262-299`），写失败也回收。

**兼容性**：gRPC 响应多一个 `MultiPointsFrame` oneof 分支。旧版 gogo 生成代码没有
unknown 字段（`v1.8.10:storage/reads/datatypes/storage_common.pb.go:183-188`），
反序列化遇到未知 tag/oneof 时走 `skipStorageCommon` **跳过并继续，不返回错误**
（同文件 `:2993-3003` 的 `skippy` 分支），因此旧客户端不会因新增帧报错，但也读不到
mean+count 的两个值；**mean+count 双聚合的客户端必须升级**。

### 3.13 `tsdb/cursors`：MeanCountArray 与游标契约

```go
// v1.12.3:tsdb/cursors/arrayvalues.go:5
type MeanCountArray struct {
	Timestamps []int64
	Values0    []float64   // mean
	Values1    []int64     // count
}
// v1.12.3:tsdb/cursors/cursor.go:42
type MeanCountArrayCursor interface {
	Cursor
	Next() *MeanCountArray
}
```

旧版没有这个类型（`v1.8.10:tsdb/cursors/arrayvalues.go` 只有 5 种数组的 `Size()`）。
它的 `Size()` 单独实现（`arrayvalues.go:33`-`:37`），因为它是三列数组，形状与模板生成的二元数组不同，模板不适用。

另外：

- `CursorRequest` 的 `StartTime`/`EndTime` 明确写成 inclusive（`cursor.go:64`-`:71`）。
- 6 种模板生成数组的 `Len()` 改为 nil-safe（`v1.12.3:tsdb/cursors/arrayvalues.gen.go:29-34` 等 6 处；手写的 `MeanCountArray` 不在其中，见下）；
  来自 `bc80e881fa` #24784（“do not panic when empty tags are queried”），避免对 nil 数组取长度时 panic。
- 新增 `tsdb/cursors/mock`（`cursor_iterator.go`、`integer_array_cursor.go`），
  供上层单测注入可控游标。

**消费案例**：3 个窗口的 `MeanCountArray`：

| 字段 | 值 |
|------|-----|
| `Timestamps` | `[10, 20, 30]`（ns；数值不影响 Size） |
| `Values0`（mean） | `[2, 4.5, 6]` |
| `Values1`（count） | `[3, 2, 1]` |
| `Len()` | 3 |
| `Size()` | `3*8 + 3*8 + 3*8 = 72` 字节 |
| `MinTime()` / `MaxTime()` | 10 / 30 |

序列化端把它写成一个 `ReadResponse_Frame_MultiPoints`（§3.12），三列长度必须相等；`MeanCountArrayCursor.Next()` 返回 `*MeanCountArray`，调用方必须在同一个 cursor 上成对消费 mean 与 count，不能拆成两个 cursor。`CursorRequest{StartTime:0, EndTime:100}` 两端 inclusive（`cursor.go:64-71`）：要表达 `[0,100)` 需传 `EndTime:99`（ns 精度下为 `100-1`）。

nil 边界要分开看：6 种模板生成的数组（Float/Integer/Unsigned/String/Boolean/Timestamp）`Len()` 对 nil receiver 返回 0（`arrayvalues.gen.go:29-34` 等）；手写的 `MeanCountArray.Len()`/`Size()` **没有 nil 判断**（`arrayvalues.go:28-37`），对 nil 指针调用会 panic。

### 3.14 tsm1 数组游标：#26691、错误传播、降序 reset

**(a) #26691：优化条件漏判 `c.tsm.pos`**

```go
// v1.8.10:tsdb/engine/tsm1/array_cursor.gen.go:107
if pos == 0 && len(c.res.Timestamps) >= len(tvals.Timestamps) {
// v1.12.3:tsdb/engine/tsm1/array_cursor.gen.go:113
if pos == 0 && c.tsm.pos == 0 && len(c.res.Timestamps) >= len(tvals.Timestamps) {
```

这段是升序游标的“全部点都能由 TSM 块直接提供”的快路径：直接把 `tvals` 赋给结果，
跳过逐点归并。旧条件只检查结果缓冲的 `pos==0` 和长度，没有检查 **TSM 侧当前位置**
`c.tsm.pos`。当 TSM 位置不为 0 时，会把从块中间开始的 `tvals` 当成整块返回，导致
**时间戳重复（不是缺失）**。commit `31cb61f556` 补上 `c.tsm.pos == 0`，并新增
`tsdb/engine/tsm1/array_cursor_optimization_bug_test.go`（166 行）复现。

**案例（#26691 逐步复现）**：

- **输入**（`array_cursor_optimization_bug_test.go:20-140`）：响应缓冲 1000 点；
  cache 995 点，时间戳 `100..1094`（步长 1）；TSM 单块 8 点
  `[10,10000,20000,30000,40000,50000,60000,70000]`；`reset(seek=10, end=71000)`。
- **步骤与数值**：

  | 调用 | 进入时 cache.pos | 进入时 tsm.pos | 返回点数 | 返回时间戳 |
  |---|---:|---:|---:|---|
  | `reset` | 0 | 0 | — | 解码 8 点块，`Search(>=10)=0` |
  | `Next()` 第 1 次 | 0 | 0 | 1000 | `10` + cache `100..1094`（995 点）+ TSM `10000,20000,30000,40000` |
  | `Next()` 第 2 次，旧版 | 995 | 5 | 8 | `10,10000..70000`；其中 `10,10000,20000,30000,40000` 重复，`50000,60000,70000` 是新增 |
  | `Next()` 第 2 次，新版 | 995 | 5 | 3 | `50000,60000,70000` |
  | `Next()` 第 3 次 | 995 | 0（下一块为空） | 0 | — |

  第 1 次调用走完归并主循环后 `pos=996`，剩余 4 个槽复制 `tvals[1:5]`，因此
  `cache.pos=995`、`tsm.pos=5`。旧版第 2 次的条件 `pos==0 && len(res)>=len(tvals)`
  （`1000>=8`）成立，整块复制 8 点；新版因 `c.tsm.pos==5 != 0` 走普通复制，只取
  `tvals[5:]` 的 3 点（`array_cursor.gen.go:107→113`、`:120-138`）。
- **预期输出**：唯一时间戳共 `995+8=1003` 个。旧版发出 `1000+8=1008` 个（5 个重复），
  新版发出 `1000+3=1003` 个；两版都不丢点，差异只是旧版重复。
- **降序 reset 的边界（可复算）**：`pos = sort.Search(len, values[i].UnixNano() > seek) - 1`
  等于“最后一个 `<= seek` 的下标”。`seek` 小于首元素 → `-1`；等于首元素 → `0`；
  大于等于尾元素 → `len-1`。`Next()` 的循环条件是 `tsm.pos >= 0 && cache.pos >= 0`，
  所以 `-1` 时直接返回空数组，不会越界（`array_cursor.gen.go:190-215`、`:245-260`）。

**(b) 错误传播**

`reset` 现在返回 `error`，`reset()` 读首块的 `Read*ArrayBlock` 错误不再被 `_` 丢弃：

```go
// v1.12.3:tsdb/engine/tsm1/array_cursor.gen.go:42
func (c *floatArrayAscendingCursor) reset(seek, end int64, cacheValues Values, tsmKeyCursor *KeyCursor) error {
	var err error
	...
	c.tsm.values, err = c.tsm.keyCursor.ReadFloatArrayBlock(c.tsm.buf)
	if err != nil {
		return err
	}
	...
}
```

旧版 `c.tsm.values, _ = ...ReadFloatArrayBlock(...)`（`v1.8.10:tsdb/engine/tsm1/array_cursor.gen.go:49`），
读块失败后继续用零值/旧缓冲。只有 `reset()` 读首块检查错误；`nextTSM()` 读后续块仍是 `c.tsm.values, _ = ...`（10 处），10 个游标的 `Err()` 仍 `return nil`。

**(c) 降序 reset 重写**

```go
// v1.12.3:tsdb/engine/tsm1/array_cursor.gen.go:190
func (c *floatArrayDescendingCursor) reset(seek, end int64, cacheValues Values, tsmKeyCursor *KeyCursor) error {
	...
	c.cache.pos = sort.Search(len(c.cache.values), func(i int) bool {
		return c.cache.values[i].UnixNano() > seek
	})
	c.cache.pos--
	...
	c.tsm.pos = sort.Search(c.tsm.values.Len(), func(i int) bool {
		return c.tsm.values.Timestamps[i] > seek
	})
	c.tsm.pos--
```

旧版用 `>= seek` 再根据“是否正好等于 seek / 是否越界”做 ±1 修正
（`v1.8.10:tsdb/engine/tsm1/array_cursor.gen.go:183`-`:213`）。
新写法等价于“最后一个 `<= seek` 的位置”，但边界更统一：空数组时
`sort.Search` 返回 0，减一得 -1，不需要 `if len(...)>0` 特判。
降序 `Next()` 里的 `pos -= 2` 末尾裁剪逻辑保留不变
（`v1.12.3:tsdb/engine/tsm1/array_cursor.gen.go:288`-`:294`）。

### 3.15 InfluxQL 编译层：`query/compile.go`（未重构，新增 `count_hll` 入口）

**角色/owner**：把 `*influxql.SelectStatement` 编译成 `query.Statement`，负责字段表达式校验、
时间/分组/填充选项、`OnlySelectors` 判定和函数分发。它不创建游标，也不执行查询。

**结构**：`compiledStatement` 保存 `Options CompileOptions`、`OnlySelectors`、`FunctionCalls`、
`HasDistinct`、`InheritedInterval`、`HasAuxiliaryFields`（`query/compile.go:27-93`）。
字段表达式由 `compiledField.compileExpr` 按类型分发（`:252-316`）：`*influxql.Call` 走
`compileFunction`（默认分支 `:315`，定义 `:390`；`count_hll` 特判到 `compileCountHll`，`:309-310`/`:787`），
`VarRef`/通配/正则在 `compileExpr` 内联处理（置 `HasAuxiliaryFields`/`AllowWildcard` 后返回，`:252-272`），**不调用** `compileSymbol`；后者由 `compileFunction`/`compileCountHll` 等调用（`:368`、`:796`）。`CompileOptions` 由 `Prepare` 传入
（`query/select.go:81-88`）。

**流程**：

```mermaid
flowchart LR
    A["influxql.ParseStatement"] --> B["query.Compile (compile.go:106)"]
    B --> C["newCompiler + stmt.Clone"]
    C --> D["preprocess：时间/分组/fill"]
    D --> E["compile → compiledField.compileExpr (compile.go:252)"]
    E --> F{"compileExpr 按表达式类型"}
    F -->|"Call count_hll"| G["compileCountHll：1 个参数 (compile.go:787)"]
    F -->|"其他 Call"| H["compileFunction 分发 (compile.go:315/390)"]
    F -->|"VarRef/通配/Regex"| I["内联置 HasAuxiliaryFields / AllowWildcard 后 return nil (compile.go:252-272)"]
    G --> J["compiledStatement"]
    H --> J
    I --> J
    G -.->|"内部调用"| K["compileSymbol (compile.go:368)"]
    H -.->|"内部调用"| K
    K --> J
```

**代码卡**：

- `v1.12.3:query/compile.go:106-138`：`Compile`；`:118-127` 的 `defer recover` 只包住
  `RewriteDistinct`/`RewriteTimeFields`/`RewriteRegexConditions`，**不包** `newCompiler`、`Clone`、
  `preprocess`、`compile`。
- `v1.12.3:query/compile.go:309-310` 字段级分发到 `count_hll`；`:787-793` 校验参数个数并置
  `OnlySelectors=false`。
- 旧版同位置没有 `count_hll` 分支、也没有 recover（`v1.8.10:query/compile.go:106-127`）。

**案例（编译期 panic → 错误）**：输入
`SELECT * FROM cpu WHERE ( host =~ /foo/ ^ other AND env =~ /bar/ ) and time >= now()-15m`。
两版在 `RewriteRegexConditions` 中都会把 `*influxql.BinaryExpr` 当 `*influxql.RegexLiteral` 断言，
panic 文本为 `interface conversion: influxql.Expr is *influxql.BinaryExpr, not *influxql.RegexLiteral`，但两条调用路径的结果不同：

- **直接调用 `query.Compile`（单测路径）**：旧版没有 recover，调用方看到 panic（`v1.8.10:query/compile.go:106-127`）；新版在 `:118` 注册 `defer recover` 包住 `RewriteDistinct`/`RewriteTimeFields`/`RewriteRegexConditions`，返回错误 `likely malformed statement, unable to rewrite: interface conversion: influxql.Expr is *influxql.BinaryExpr, not *influxql.RegexLiteral`（测试期望见 `query/compile_test.go:364`）。
- **HTTP `/query` 路径**：旧版 panic 发生在 `Executor.executeQuery` 的 goroutine 内，被 `defer e.recover` 捕获（`v1.8.10:query/executor.go:253-255,404-420`）：`RecoveredPanics += 1`、日志打印 `query [panic:...]`，并向结果通道发送 `Result{StatementID: -1, Err: "<原查询> [panic:interface conversion: ...]"}`；handler 在取结果前已写 `200 OK`，客户端看到 HTTP **200** + JSON `{"results":[{"statement_id":-1,"error":"SELECT ... [panic:interface conversion: ...]"}]}`，**进程不崩溃**。只有 `INFLUXDB_PANIC_CRASH=true`（`query.PanicCrashEnv`，`init` 时读取）才 `os.Exit(1)`。新版走正常错误返回（`statement_id: 0` 的 error result），同样不崩溃。
- 注意：若 panic 发生在 `c.compile` 之前（新版 `defer` 尚未注册），仍由 `Executor.recover` 兜底（§7.8）。

不要把旧版 HTTP 场景写成"进程 panic"：只有直接调用 `query.Compile` 的测试路径才会 panic，HTTP 路径由 `Executor.recover` 兜底返回错误结果。

**风险**：`defer` 位置（§7.8）；`count_hll` 只接受 1 个参数，`SELECT count_hll(a,b)` 返回错误
而不是 HLL；`OnlySelectors=false` 会影响 aux 字段校验（`compile.go:1019-1024`）。

### 3.16 Select 层：`query/select.go` 的 `Prepare → buildCursor → Merge → Interval → Fill → Limit`

**角色/owner**：`query.Select` 是 InfluxQL 查询的编排层：编译、准备 `IteratorCreator`、
构建游标、套 merge/interval/fill/limit，最后交给 `Emitter`。它位于引擎层之上，
不直接读 TSM/Cache。

**结构**：`preparedStatement{stmt, opt, ic, columns, maxPointN, now}`；
`exprIteratorBuilder` 持有 `IteratorCreator/sources/opt/selector/writeMode`；
`valueMapper` 把字段表达式映射成列；`buildCursor` 负责时间列、aux 列和最终 `Cursor`。
旧版结构相同（`v1.8.10:query/select.go:100-125`），差异只在错误返回和 `buildCallIterator` 的包装。

**流程**：

```mermaid
flowchart TB
    A["query.Select (select.go:91)"] --> B["Prepare -> Compile -> preparedStatement"]
    B --> C["preparedStatement.Select (select.go:113)"]
    C --> D["buildCursor (select.go:622)"]
    D --> E{"valueMapper.calls 为空?"}
    E -->|"是"| F["buildAuxIterator -> NewIteratorScanner -> scannerCursor<br/>（内部已含 Iterators.Merge :823 与 NewLimitIterator :846）"]
    E -->|"否"| G["buildExprIterator -> buildCallIterator (select.go:214)"]
    G --> H["引擎层 CreateIterator"]
    H --> I["Iterators.Merge (iterator.go:122) -> NewMergeIterator (:172)"]
    I --> J["Merge 内 NewCallIterator：count->sum、sum_hll->merge_hll (:150-164)"]
    J --> K["NewIntervalIterator / NewFillIterator (select.go:568-570)"]
    K --> L["NewLimitIterator (select.go:846/876)"]
    F --> M["Emitter"]
    L --> M
```

**代码卡**：

- `Prepare`/`Select`（`:81-97`）、`preparedStatement.Select`（`:113-136`）、`buildCursor`（`:622`）；
- `buildCallIterator` 先清空 `opt.Limit/opt.Offset`（`:214-218`），由外层统一套 limit；
- `Iterators.Merge`（`query/iterator.go:122-164`）在 `count` 时把表达式改成 `sum`、
  在 `sum_hll` 时改成 `merge_hll`；旧版只改 `count`（`v1.8.10:query/iterator.go:139-146`）。
- `NewLimitIterator` 在 `buildAuxIterator`（`:846`）与 `buildFieldIterator`（`:876`）各有一处；
  旧版对应 `:847`/`:877`。

**案例**：§6.1 的 `GROUP BY time(24h, 12h)` DST 回退是本节 `NewIntervalIterator` 的数值推演；
`SELECT count(value) FROM cpu GROUP BY host` 在 Select 层把每个 series 的 `count` 改写为
`sum`：两个 shard 的同一 series 各输出 `count=3`、`count=2`，Merge 后得到 `5`，而不是
对原始点重新计数。

**风险**：① `defer em.Close()`（`coordinator/statement_executor.go:558`）丢弃 `Close` 返回值，
`MergeIterator.Close` 的 `errors.Join`（`query/iterator.gen.go:161-173`）在 InfluxQL 内部
调用点仍看不到；② `LIMIT/OFFSET` 是 per-series（引擎层 `newLimitIterator`，
`tsdb/engine/tsm1/engine.go:3050-3056`），不是全局；③ `buildCallIterator` 清空 limit 后，
若某个算子没有外层 limit 包装，LIMIT 语义会变化（当前所有路径都由 `buildCursor` 收口）。

### 3.17 InfluxQL 迭代器引擎层：`tsdb/engine/tsm1/engine.go`（未重构）

**角色/owner**：引擎层负责把 InfluxQL 的 `IteratorOptions` 变成每个 series 的点迭代器：
索引定位 → `TagSets` → `createTagSetIterators` → 每个 series 包 `NewCallIterator` →
同一 TagSet 内 `NewParallelMergeIterator`。它不负责跨 TagSet 的合并，也不做 interval/fill/limit 的最终收口。

**两层边界**：

```mermaid
flowchart LR
    subgraph ENGINE["引擎层（tsdb/engine/tsm1/engine.go）"]
        A["createCallIterator (:2903)"] --> B["TagSets + LimitTagSets"]
        B --> C["createTagSetIterators (:3093)"]
        C --> D["每个 series: NewCallIterator (:2985)"]
        D --> E["NewParallelMergeIterator (:2993)"]
    end
    subgraph SELECT["Select 层（query/select.go）"]
        F["Iterators.Merge (iterator.go:122)"] --> G["NewMergeIterator (:172)"]
        G --> H["NewCallIterator（count->sum / sum_hll->merge_hll）"]
        H --> I["Interval -> Fill -> Limit -> Emitter"]
    end
    E --> F
    E --> T["Cache + TSM KeyCursor（点迭代器）"]
```

**结构**：`TagSet`/`TagSets` 由索引返回；`createTagSetIterators` 为每个 series 建点游标；
`NewParallelMergeIterator(inputs, opt, GOMAXPROCS(0))` 只并行化同一 TagSet 内的 series；
`opt.Limit/Offset` 在 `Dimensions != GroupBy` 时下推到每个 series（`:3050-3056`）。

**代码卡**：

- `createCallIterator`（`:2903-3004`）：先 `MeasurementExists`，TSI 的 `_seriesKey` 快路径
  可直接 `NewCallIterator(seriesIterator, opt)`；否则 TagSets → per-series Call → ParallelMerge。
- 旧版相同位置：`v1.8.10:tsdb/engine/tsm1/engine.go:2515`、`:2575`、`:2583`。
- `NewCallIterator` 的分发在 `query/call_iterator.go:39-60`（含 `sum_hll`/`merge_hll`）。

**案例（数值）**：`SELECT count(value) FROM cpu GROUP BY host`，`host=a` 在 shard 1 有 3 点、
在 shard 2 有 2 点，`host=b` 在 shard 1 有 4 点。

- 引擎层：每个 `(shard, series)` 一个点迭代器，包 `count` 后分别得到 3、2、4；
  `NewParallelMergeIterator` 在 TagSet 内合并。
- Select 层：`Iterators.Merge` 把 `count` 改写为 `sum`（`query/iterator.go:150-156`），
  同一 series 的 3+2=5，`host=b`=4；输出两行。
- 若没有 Select 层的 `count→sum` 改写，两个 shard 的 count 会被再计数成 2，而不是 5。
- DST 案例（完整推演见 §6.1）：`SELECT count(value) ... GROUP BY time(24h, 12h) tz('America/Los_Angeles')`，
  2023-11-05 回退窗口旧版为 `[11-04T19:00Z, 11-05T19:00Z)`、收 24 点，新版为
  `[11-04T19:00Z, 11-05T20:00Z)`、收 25 点；差异来自 `end += Offset` 放在 `Zone(end)`
  之前（`query/iterator.go:857-866` vs 旧 `:864`）。

**风险**：① 每个 TagSet 只在进入时检查一次 `InterruptCh`（`:2966-2972`），长 TagSet 内
中断延迟取决于游标；② `NewParallelMergeIterator` 的 goroutine 数与 `GOMAXPROCS` 相关，
series 很多时内存/调度压力在引擎层；③ per-series LIMIT 与用户直觉的全局 LIMIT 不同（主 03 §3.1）。

### 3.18 `services/storage`：Flux 请求 → shard/SeriesCursor → `reads.ResultSet`

**角色/owner**：`services/storage.Store` 实现 `reads.Store`，把 Flux 的
`ReadFilter`/`ReadGroup`/`ReadWindowAggregate` 请求翻译成 shard ID、`SeriesCursor` 和
`reads.ResultSet`。它位于 `storage/flux`（协议/表）与 `tsdb`（shard/索引）之间，
是“谁把 Flux spec 变成 reads 请求”的唯一适配层。

**结构**：`Store{TSDBStore *tsdb.Store, MetaClient, Logger}`；`indexSeriesCursor` 持有
`sqry tsdb.SeriesCursor`、`fields`、`row reads.SeriesRow`、`cond/measurementCond`。
关键方法：`ReadFilterLimit`（`store.go:126`）、`ReadGroupLimit`（`:177`）、
`WindowAggregateLimit`（`:234`）、`findShardIDs`（`:63`）、`validateArgs`（`:88`）、
`newIndexSeriesCursor`（`series_cursor.go:40`）。旧版对应 `v1.8.10:services/storage/store.go:109`（`ReadFilter`）、
`:147`（`ReadGroup`）、`:60`（`findShardIDs`）、`:85`（`validateArgs`）、`series_cursor.go:40`。

**代码卡**：`ReadFilterLimit`（`store.go:126-176`）的 `GetReadSource → validateArgs →
findShardIDs → newIndexSeriesCursor → NewFilteredResultSet`；`ReadGroupLimit`（`:177-232`）
的 `newCursor` 闭包；`WindowAggregateLimit`（`:234-277`）的降序判定；
`indexSeriesCursor` 的谓词拆分（`series_cursor.go:64-100`）。

**流程**：

```mermaid
flowchart TD
    A["storage/flux.storeReader.Read*"] --> B["Store.ReadFilterLimit / ReadGroupLimit / WindowAggregateLimit"]
    B --> C["GetReadSource + validateArgs (:88)"]
    C --> D["findShardIDs (:63)"]
    D --> D1{"limitShardIDs 非空?"}
    D1 -->|"是"| D2["limitToShardIDs (:149-154)"]
    D1 -->|"否"| E{"len(shardIDs)==0?"}
    D2 --> E
    E -->|"是"| F["return (nil, nil)"]
    E -->|"否"| G["newIndexSeriesCursor (:40)"]
    G --> G1{"ic==nil?"}
    G1 -->|"是"| F2["return (nil, nil) (:157-160)"]
    G1 -->|"否"| H["tsdb.CreateCursorIterators"]
    H --> I["reads.NewFilteredResultSet / NewGroupResultSet / NewWindowAggregateResultSet（按入口三选一）"]
```

**三条兄弟路径**：

| 路径 | 入口 | 方向/结果集 |
|------|------|-------------|
| `ReadFilterLimit` | `store.go:126-176` | `findShardIDs(..., false, ...)` → `NewFilteredResultSet` |
| `ReadGroupLimit` | `store.go:177-232` | `descending := !IsAscendingGroupAggregate` → `NewGroupResultSet`（`newCursor` 闭包可重复建 cursor） |
| `WindowAggregateLimit` | `store.go:234-277` | `descending := !IsAscendingWindowAggregate` → `NewWindowAggregateResultSet` |

**案例（shard 内容 → SeriesCursor → 结果行）**：

- **输入**：`filter(fn:(r)=>r.host=="web" and r._value>50)`，时间范围内有两个 shard：
  shard 3 含 `(cpu,host=web,_value=60)`、`(cpu,host=web,_value=70)`、
  `(cpu,host=db,_value=80)`；shard 4 含 `(cpu,host=web,_value=90)`。
- **步骤与数值**：
  1. `validateArgs` 补默认 RP；`findShardIDs` 按时间范围返回 `[3,4]`
     （`services/storage/store.go:63-86`、`:126-150`）。
  2. `newIndexSeriesCursor` 调 `NodeToExpr` 得到 `host=="web" and _value>50`；
     `HasFieldKeyOrValue` 置 `hasValueExpr=true`，于是
     `measurementCond = RewriteExprRemoveFieldValue(cond) = host=="web"`，
     `opt.Condition = RewriteExprRemoveFieldKeyAndValue(cond) = host=="web"`；
     每行再 `Reduce` 成 `ValueCond = "$ > 50"`（`$` 是存储层的字段占位符，
     即 `_value > 50`；host 已由索引过滤）
     （`services/storage/series_cursor.go:64-100`、`:198-207`）。
  3. `ReadFilterLimit` 返回 `NewFilteredResultSet`。
- **预期输出**：3 行 `(web,60)(web,70)(web,90)`；db/80 被 `host=="web"` 排除。
- **空 shard**：`findShardIDs` 返回 `[]` → `ReadFilterLimit` 返回 `(nil, nil)`，
  `storage/flux.filterIterator.Do` 直接 `return nil`（`store.go:150-153`、
  `storage/flux/reader.go:151-154`）。
- **错误**：`ReadSource==nil` → `missing read source`；db/RP 不存在 → `validateArgs` 错误；
  `NodeToExpr` 失败 → `newIndexSeriesCursor` 返回错误（`series_cursor.go:65-66`）。
- **源码**：`services/storage/store.go:126-176`、`services/storage/series_cursor.go:40-100`。

**风险**：① 空 shard/空 cursor 都折叠成 `(nil, nil)`，上层无法区分“没数据”和“读失败”；
② `ReadGroupLimit` 的 `newCursor` 每次调用都重建 series cursor，group 数多时开销在适配层；
③ 谓词拆分依赖 `HasFieldKeyOrValue` 与 `RewriteExprRemove*`。若表达式含 `OR` 且某个分支是
字段/值条件，例如 `host='web' OR _value>50`，`RewriteExprRemoveFieldValue` 会把 `_value>50`
分支替换为 `true`（`storage/reads/influxql_predicate.go:224-236`），`Reduce` 后
`measurementCond` 为 `true` → 置 nil（`services/storage/series_cursor.go:74-77`），
**不再按 measurement 过滤**（索引需遍历范围内所有 measurement 的 series）；tag 条件仍在
`opt.Condition` 中，最后由每行 `ValueCond` 过滤。只有纯 tag 的 `AND` 条件能完整下推到索引。

### 3.19 数组游标跨包契约：`storage/reads` ↔ `tsm1` ↔ `storage/flux`

**角色/owner**：`tsdb/cursors` 是 `storage/reads` 与 `tsm1` 之间的共享类型契约；
`storage/reads/array_cursor*` 负责类型分发、时间范围和结果集；
`tsdb/engine/tsm1/array_cursor*` 负责 `KeyCursor`→数组；`storage/flux` 负责
`row.Query`→表。三层的所有权不同，错误传播也不同。

**结构**：

- 5 种数组（Float/Integer/Unsigned/String/Boolean）加三列 `MeanCountArray`
  （`tsdb/cursors/arrayvalues.go:5-9`）；`MeanCountArrayCursor`（`cursor.go:42-45`）。
- `CursorRequest.StartTime`/`EndTime` 都是 **inclusive**（`cursor.go:64-71`）；
  `storage/reads` 构造请求时把 `[start, stop)` 转成 `[start, stop]`（`array_cursor.go:78-90`）。
- `tsdb/cursors/arrayvalues.gen.go:29-34` 等 6 处的 `Len()` 是 nil-safe；这是防御性契约，
  不是“任意 nil 数组都可当空数组消费”。

**代码卡**：`MeanCountArray`（`tsdb/cursors/arrayvalues.go:5-9`）、
`CursorRequest` 的 inclusive 注释（`cursor.go:64-71`）、tsm1 `reset` 返回 error
（`tsdb/engine/tsm1/array_cursor.gen.go:42-61`）；旧版 `reset` 无返回值、读块错误 `_` 丢弃
（`v1.8.10:tsdb/engine/tsm1/array_cursor.gen.go:41-53`）。

**流程**：

```mermaid
flowchart LR
    A["services/storage newIndexSeriesCursor (:40)"] --> B["tsdb.CreateCursorIterators (tsdb/cursor.go:39)"]
    B --> B2["[]CursorIterator → SeriesRow.Query (services/storage/series_cursor.go:62)"]
    B2 --> C["tsm1 arrayCursorIterator.Next(CursorRequest)"]
    C --> D["floatArrayAscendingCursor.reset (array_cursor.gen.go:42)"]
    D --> E["KeyCursor.ReadFloatArrayBlock"]
    E --> F["cursors.FloatArray"]
    F --> G["storage/reads newAggregateArrayCursor / window cursor"]
    G --> H["storage/flux colReader -> Table"]
```

**跨包所有权与错误表**：

| 层 | 负责 | 错误行为 |
|----|------|----------|
| `storage/reads/array_cursor.go:20-56` | 聚合分发：first/last→limit，其余→window | `NewWindowAggregateArrayCursor` 的 min/max default 分支 panic（`gen.go:158`/`:175`） |
| `storage/reads/array_cursor.go:78-90` | `[start,stop)` → `[start,stop]` | 无错误 |
| `storage/reads/array_cursor.go:118-128` | multiShard `createCursor` | `cur==nil \|\| err!=nil` → nil，错误被折叠 |
| `tsm1/array_cursor.gen.go:42-61` | reset 首块读取 | 返回 `error` |
| `tsm1/array_cursor.gen.go:113` | 优化快路径 | 追加 `c.tsm.pos == 0`（#26691） |
| `tsm1/array_cursor.gen.go:190-215` | 降序 reset | `sort.Search(...)` 后 `-1` |
| `tsm1/array_cursor.gen.go` 的 `nextTSM` | 后续块读取 | 仍 `_` 丢弃；10 个游标的 `Err()` 恒 nil |
| `storage/flux/reader.go:183-186` | nil cursor | 当作“该 series 没数据”继续 |

**案例（#26691，跨包契约视角）**：

- **输入/步骤**：完整输入与逐步数值见 §3.14(a)。本层只关心所有权：
  `services/storage.newIndexSeriesCursor` 调用 `tsdb.CreateCursorIterators` 得到 `[]CursorIterator`，
  放进 `SeriesRow.Query`；`storage/reads` 的 `createCursor` 从 `row.Query[0]` 取出
  `tsm1.arrayCursorIterator`（`storage/reads/array_cursor.go:121-122`），其 `Next(CursorRequest)` 调用
  `floatArrayAscendingCursor.reset/Next`；
  `reset` 只检查首块读错误，后续块仍走 `nextTSM` 的 `_` 丢弃路径。
- **输出**：第 1 次 `Next()` 返回 1000 点（cache 995 + TSM 5），第 2 次旧版返回 8 点
  （5 个重复）、新版返回 3 点；累计唯一时间戳 1003。跨包可见的差异只有“重复”，
  上层 `storage/flux` 的 `colReader` 会照原样把重复时间戳写进表，不会去重。
- **源码**：`storage/reads/array_cursor.gen.go`（本层不归并，只消费数组）、
  `tsdb/engine/tsm1/array_cursor.gen.go:107-138`、`:190-260`；
  回归测试 `tsdb/engine/tsm1/array_cursor_optimization_bug_test.go:20-140`。

**风险**：① 错误只在 `reset()` 首块被检查，后续块仍丢弃，`Err()` 恒 nil；② `createCursor`
在 `cur==nil || err!=nil` 时返回 nil，filter/group 结果集把错误折叠成“无数据”；
③ `arrayCursorIterator` 对每种 (类型, 方向) 只缓存一个 cursor（`array_cursor_iterator.gen.go:23-38`）；
`reset()` 覆盖 `c.cache.values`、`c.tsm.values`（复用 `c.tsm.buf`）与 `c.tsm.keyCursor`，`Next()` 复用并覆盖 `c.res`
（`array_cursor.gen.go:42-58`、`:78-84`）；`KeyCursor.key` 直接别名 `q.key[:0]`，下一次
`seriesFieldKeyBytes` 会改写（`array_cursor_iterator.go:79-83`）。因此返回数组与 `KeyCursor`
只在**本次 `Next()` 返回后到下一次 `Next()` 前**有效；违约后果：数组被下一批覆盖（读到错值）、
tombstone 按错 key 查（删错点）。消费方要么在回调内消费，要么 `copy`。见主 08 §2.3/§9.4。


---

## 4. 优化与收益

> 本节只写**能从代码机制推出**的收益；没有基准数据就不写数字。

### 4.1 计算下推：把聚合从“查询进程”挪到“存储进程”

这是区间内最大的性能结构变化。旧版 Flux 的 `window + mean` 会把窗口内所有原始点
编码成 `FloatPointsFrame` 交给上层，再由 Flux 的 `mean` 算子逐点计算；新版在
`ReadWindowAggregate` 里直接返回每窗口一个值。1.x OSS 的 storage 与 query 在同一进程，
收益主要是**序列化/拷贝与分配**；企业版/分离式部署才额外省网络。

| 下推 | 旧路径 | 新路径 | 机制收益 | 证据 |
|------|--------|--------|----------|------|
| window + count/sum/mean | 传递 N 个原始点 + 上层聚合 | 传递 W 个窗口值 | 序列化/拷贝从 O(N) 降到 O(W) | `rules.go:676`、`array_cursor.gen.go:102`/`:125`/`:179` |
| window + first/last/min/max | 同上 | 每窗口一个点（selector） | 同上；`last` 还可用降序游标提前停 | `array_cursor.gen.go:50`/`:76`/`:145`/`:162` |
| 裸聚合 | 全量点上传 | `WindowEvery=MaxInt64`，单值 | O(1) 返回 | `rules.go:837` |
| group + count/sum/first/last/min/max | 每组全量点 | 每组一个值 | 组内聚合在存储层完成 | `rules.go:866` |
| mean + count | 两遍或上层组合 | 一次扫描输出 `MeanCountArray` | 少一次全量扫描 | `aggregate_resultset.go:50`-`:63`、`array_cursor.gen.go:199` |

### 4.2 非窗口 first/last 的 limit 快路径

`newAggregateArrayCursor` 对所有**非窗口聚合**中的 `first`/`last` 直接套 `newLimitArrayCursor`
（`storage/reads/array_cursor.go:20`-`:25`，只按 `agg[0].Type` 分支，不判断窗口）。调用方有两处：
裸聚合（`storage/reads/aggregate_resultset.go:128`-`:130`，窗口为 `math.MaxInt64`）与 group 聚合
（`group_resultset.go:307`、`:355`）。`last` 还会把 shard 顺序反转（group 聚合 `services/storage/store.go:195`、窗口聚合 `:252`），
于是游标返回第一个点后 limit 就停止，不需要读完整个 series。
旧版只能读完全部点再取最后一条。

### 4.3 整数 mean/count 的原生累加（#21334）

```go
// v1.12.3:storage/reads/array_cursor.gen.go:2146
var sum int64
...
// :2167
c.res.Values0[pos] = float64(sum) / float64(count)
```

窗口 mean/count 游标本身是区间内新增的（#21107 / #21291），v1.8.10 没有对应实现。
它最初用 `var sum float64`，每个 int64 先转 float64 再累加，超过 2^53 会丢精度；
#21334 改成 int64/uint64 原生累加，只在最后做一次除法
（`:2030`、`:2146`、`:3037`、`:3153`）。**收益**：正确性 + 少一次每点转换。

### 4.4 时间范围少读一个点（#21347）

`EndTime = end - 1`（`storage/reads/array_cursor.go:90`）让底层闭区间恰好等价于
Flux 的 `[start, stop)`。旧版 `EndTime=end`，因此每条 series 的每次 cursor 请求
**最多多返回 1 个点**（恰好 `t==stop` 的那个点）；裁剪循环只对 `> c.end` 的点执行
（`v1.8.10:tsdb/engine/tsm1/array_cursor.gen.go:137-143`），旧版 `end=stop` 时该点不进入裁剪，
新版 `end=stop-1` 时才把它裁掉。所以收益是“正确性 + **≤1 点/请求**”，不存在旧版多走一轮裁剪；
读取块数不变（block 仍整体解码），是常数级收益，不是数量级。实测口径：同一 block 内放
`stop-1/stop/stop+1` 三个点，记录 `Next()` 返回数组与 `pos`。

### 4.5 谓词转换前移

旧版每次 `ReadFilter`/`ReadGroup` 都从 Flux semantic AST 重新构造 protobuf 谓词
（`v1.8.10:storage/reads/flux_reader.go:57`、`:77`、`:121`、`:245`）；
新版在物理计划阶段一次完成（`rules.go:179`），`ReadFilterSpec.Predicate` 直接复用。
**收益**：每个读请求少一次 AST 遍历和一次正则/字面量重建。

### 4.6 表缓冲与序列化

- `table.allocateBuffer` 复用 `colReader`，避免每批数据重新分配 4+ 列数组
  （`storage/flux/table.go:124`-`:137`）。
- `response_writer` 的 `Flush` 用 `defer` 回收 frame 到 `buffer.*` 池
  （`response_writer.go:262`-`:292`）；旧版失败路径会丢池。
- 大小估算改用 `proto.Size`（数组侧仍是旧的 `a.Size()`），避免 varint 编码下反复计算
  （`v1.12.3:storage/reads/response_writer.go:202`、`:216`）。
- `SeriesFrame` 只在有数据时创建（`response_writer.gen.go:89`），
  空 series 不再产生一次分配 + 一次截断。

### 4.7 常量内存基数估计（#20603 / #20745）

HLL sketch 的大小只与精度参数有关，与 series 数无关：p=16 时单个 shard 的 sparse sketch
为 318B/465B（100/150 key），任一 `Merge` 后统一为 **65543B**（`pkg/estimator/hll/hll.go:33`）。
对 `SHOW SERIES CARDINALITY FROM ...` 这类查询，旧版 `count(distinct(_seriesKey))`
需要把 distinct 集合放在内存里（最坏 O(series)）；新版每个 shard 持有一个 sketch，
非 EXACT 查询的峰值 = 同时持有的 sketch 数 × 65543B，sketch 数 ≤ shard 数，
因此**相对 series 数是 O(1)，相对 shard 数不是**。代价是**结果变成近似值**（见第 5 节）。

### 4.8 索引级 `SHOW FIELD KEY CARDINALITY`（#23871）

新版重写走 `_fieldKeys` 系统迭代器（`query/statement_rewriter.go:90`-`:103`），
不再扫描实际数据块。旧版 `rewriteSources2` 会按真实 measurement 建 cursor，
需要读 TSM/cache。**收益**：从“读数据”变成“读索引”。

### 4.9 控制器可观测性与资源上限（#21100 / #21108）

`query/control` 带来 11 组指标和可配置的并发/内存上限。这不是“变快”，
而是把 Flux 查询从“不可控”变成“可限流、可观测、可排队”。
默认 0 值仍是不限制，因此**没有配置就没有收益**。

### 4.10 正确性修复本身就是收益

#26691（错位数据）、#23522（filter 条件复用导致丢点/多点）、#24791（nil 游标 panic）、
#24975（Close 吞错）、#22273（结果条数）、
#25082（DST 丢行）、#21792（编译 panic）、#26143（多子查询 panic）、#27196（越权）
都在第 3/7/8 节逐项给出证据；它们不直接提升吞吐，但把“偶发错误结果/进程崩溃”变成可诊断的错误。

---

## 5. 行为变化与兼容性

### 5.1 升级会不会坏？先看这张表

| 变更 | 旧行为 | 新行为 | 会不会坏 | 迁移建议 |
|------|--------|--------|----------|----------|
| Flux 是否启用 | `flux-enabled=false` 默认 | 同左，默认仍关闭 | 不会 | 不开启 Flux 则本文件大半改动与运行时无关 |
| Flux 控制器默认值 | 无限制 | 零值 = 无限制 | 不会 | 想限流才配 `[flux-controller]` |
| `[flux-controller]` 配置 | 不存在 | 新增；`query-concurrency>0` 时 `query-queue-size` 必须 >0；内存上限仅在 `total-max-memory-bytes>0` 时生效（0=不限，且绕过 `>= concurrency*initial` 校验） | **配错会启动失败**；0 值不会 | `query-concurrency`/`query-queue-size`/`total-max-memory-bytes`/`query-max-memory-bytes`（及 `query-initial-memory-bytes`）一起配 |
| `ReadFilterSpec.Predicate` 类型 | `*semantic.FunctionExpression` | `*datatypes.Predicate` | **外部实现编译失败** | 改用 `ToStoragePredicate` 在计划期转换 |
| `Reader` 接口 | 5 个方法（含 Close） | 6 个（+`ReadWindowAggregate`），alloc 变接口 | **编译失败** | 实现新方法；`memory.Allocator` 用接口值 |
| `Store.GetSource` 返回类型 | gogo `proto.Message` | protobuf-go `proto.Message` | **编译失败** | 换 import；线格式不变。**同主题另有编译期破坏**：`services/storage.GetReadSource(any types.Any)`（值）→ `GetReadSource(any *anypb.Any)`（指针），只换 import 不能编译（`v1.8.10:services/storage/store.go:28`；`v1.12.3:services/storage/store.go:28`） |
| `storage/reads.NewFilteredResultSet` | `(ctx, req *datatypes.ReadFilterRequest, cur SeriesCursor)` | `(ctx, start, end int64, seriesCursor SeriesCursor)` | **编译失败** | 传 `req.Range.GetStart()/GetEnd()`（`v1.8.10:storage/reads/resultset.go:25`；`v1.12.3:storage/reads/resultset.go:25`） |
| `storage/reads.NewReader` | `NewReader(s Store) influxdb.Reader` | 删除；改用 `storage/flux.NewReader(s storage.Store) query.Reader` | **编译失败** | 换包与 import（`v1.8.10:storage/reads/flux_reader.go:30`；`v1.12.3:storage/flux/reader.go:61`） |
| `query.Aggregate*Points` 包级函数 | `AggregateFloatPoints` / `AggregateIntegerPoints` / `AggregateUnsignedPoints` / `AggregateStringPoints` / `AggregateBooleanPoints` | 删除；生成器改为 reducer 的 `Aggregate*Bulk` 方法 | **编译失败** | 改用 reducer 方法或自建包装（`v1.8.10:query/functions.gen.go:27,511,995,1479,1963`） |
| `services/httpd.WriteError` | 导出 | 删除 | **编译失败** | 自行 `w.WriteResponse(Response{Err: err})`（`v1.8.10:services/httpd/response_writer.go:76`） |
| 游标 stop 边界 | Flux `ReadFilter`/`ReadGroup` 必返回 `t==stop` 的点 | `EndTime=stop-1`，必不返回 | **边界结果变化（仅 Flux）**；InfluxQL 不走 `storage/reads`，两版相同 | 只有 Flux `range(stop:...)` 且 stop 上有数据点的查询会少一个点 |
| `SHOW SERIES CARDINALITY FROM ...` | 精确 | HLL 近似 | **结果数值变化** | 需要精确时加 `EXACT` |
| `SHOW TAG VALUES FROM "db"."rp"."m"` | 扫描所有 RP | 只扫指定 RP；多 RP 报错 | **结果变化/报错** | 去掉多 RP；不写 RP 仍跨所有 RP |
| `SHOW TAG KEYS ... WITH KEY` | 语法不支持 | 支持 | 不会（新增能力） | — |
| `SHOW MEASUREMENTS ON *.*` / `ON db.*` | 语法不支持（`ON` 后必须是标识符） | 支持通配库/RP | 不会（新增能力） | —；见 `v1.12.3:coordinator/statement_executor.go:733`-`:770`、`611a4370a2` #22388 |
| `SHOW FIELD KEY CARDINALITY` | 扫数据块得到 field key 集合 | 读 `_fieldKeys` 系统迭代器（`fieldKey`/`fieldType`） | 索引与数据一致时**结果相同**；索引陈旧/损坏时新版返回索引集合、旧版返回数据集合，差异方向取决于索引状态，不能称“更准”；新版不分配测量 cursor、不读 TSM/cache，但**未做基准，不能称“更快”** | 索引陈旧时先重建/确认 TSI 健康 |
| 窗口 `mean`（区间内新增能力） | v1.8.10 无窗口下推 | 初版 float64 累加，现 int64 累加 | **大整数结果更准** | 无需迁移 |
| `SHOW QUERIES` 列 | 5 列 | 6 列（+user） | 严格按列数解析的客户端会坏 | 按列名解析 |
| `SHOW QUERIES` 可见性 | 非管理员可见他人库 | 只看有 READ 权限的库 | **行为收紧** | 预期之内 |
| 多语句错误结果 | 数量/ID 错 | 每条剩余语句一条 | 客户端结果对齐逻辑变化 | 按 `StatementID` 对齐 |
| `MergeIterator.Close` | 恒 nil | `errors.Join` 返回合并错误 | InfluxQL 内部调用点仍丢弃（`defer em.Close()`，`statement_executor.go:558`） | 直接使用 `query` 包的代码需检查 `Close()` 返回值 |
| `Iterators.Close` | nil 元素 panic | 跳过 nil，返回首个错误 | 不会 | — |
| 编译畸形语句 | `Compile` 内 panic 冒泡到 `Executor.recover`，返回 `StatementID=-1` 的错误结果；仅 `INFLUXDB_PANIC_CRASH=true` 时进程退出 | `Compile` 的 rewrite 阶段局部 recover，返回“likely malformed statement”错误 | 错误位置更靠近编译期，不是“从崩溃变为不崩溃” | — |
| `max-select-point` 错误文案 | `exceeed` | `exceeded` | 匹配旧文案的告警规则会失效 | 更新正则 |
| EXPLAIN / EXPLAIN ANALYZE / InfluxQL 错误消息 | 浮点字面量旧格式化精度 | influxql v1.4.1 新精度 | 输出文本变化；按字面量比对 EXPLAIN 或错误串的测试/告警会失配 | 更新期望值；结果类型不变（`8648298012` #24764、`77d6f20894` #26181、`v1.12.3:go.mod:23`） |
| proto 生成代码的 Go 常量名 | `FrameTypePoints`/`DataTypeFloat`/`HintNone`/`GroupBy` 等 | `<Message>_<Enum>_<Value>` 前缀（如 `ReadResponse_FrameTypePoints`）；`TimestampRange` 变指针 | **源码不兼容** | 按新常量名重编译；取 range 前判空（`v1.12.3:storage/reads/datatypes/storage_common.pb.go:32`、`:81`、`:141`、`:199`） |
| 每用户查询字节 | 无 | 可选 `userquerybytes` | 默认关闭 | 需要计费时打开 |
| Flux 窗口聚合结果分表 | 由 Flux `window()` 分表 | 下推后 `splitWindows` 也按窗口分表 | 语义一致 | 依赖“一 series 一张表”的下游需验证 |

### 5.2 关键结论

1. **InfluxQL 的字段类型映射未改，但输出文本有已知差异**：`int64`/`uint64`/`float64`/`string`/`bool`
   的映射未改；已知差异有 HLL 近似、整数 mean 精度，以及 influxql 库 v1.1.1→v1.4.1
   （`8648298012` #24764、`77d6f20894` #26181）带来的 EXPLAIN / EXPLAIN ANALYZE / 语法错误消息 /
   tracing span 中浮点字面量的格式化精度变化。v1.4.1 另新增 `FUTURE`/`PAST` 关键字与
   `FUTURE LIMIT`/`PAST LIMIT` 语法（配合 #25517 的 RP 写入窗口），未加引号的 `future`/`past`
   标识符需要加引号。该口径与 [07 构建与性能差异](07_build_perf_diff.md)、readme 一致。
2. **Flux 仍然是可选路径**：`flux-enabled` 默认 `false`（`services/httpd/config.go:76`），
   关闭时 `/api/v2/query` 直接走 `serveFluxQueryDisabled`（`services/httpd/handler.go:376`-`:380`）。
   不开 Flux 的部署不会 `control.New`、也不会注册指标；`query/control` 包本身仍编译进二进制，
   只是不启用，同样不会走 `storage/flux`。
3. **线格式与 Go API 要分开看**：proto 字段号未变（gogo→protobuf-go 只是生成器），
   但 Go 类型/枚举名/接口签名变了。**二进制兼容、源码不兼容**。
4. **错误语义更“显式”，但不能断言没有性能回退**：编译器 panic 变错误、
   窗口聚合结果集的错误会上抛（filter/group 路径仍会吞错，见 §7.1/§7.2）、Close 错误返回。
   本文**没有 v1.8.10 vs v1.12.3 的查询基准**，因此不能写“默认配置下没有性能回退”。
   已识别的机制级开销：① 每个 Flux 查询安装 `memory.ResourceAllocator`，每次申请走
   CAS 循环（`query/control/memory.go:73-116`）；② 新版控制器状态机与新增指标；
   ③ 可确认的语义变化是 `ReadGroup` 与 `WindowAggregate` 的 `last()` 单窗口路径反转 shard 顺序
   （`services/storage/store.go:195`、`:252`），它只改变扫描方向，对非 `last` 查询无影响。
   需要结论时应按 §4 口径做基准，而不是写“唯一可能”。

---

## 6. 具体案例

### 6.1 案例 A：`GROUP BY time(24h, 12h)` + `count()` 跨 DST 回退

**查询**（等价于 `query/iterator_test.go` 的 `TestGroupByIterator_DST_End`，
commit `d60741b506`）：

```sql
SELECT count(value) FROM a
WHERE time >= '2023-11-02T00:00:00Z' AND time < '2023-11-06T00:00:00Z'
GROUP BY time(24h, 12h) tz('America/Los_Angeles')
```

输入是每小时一个点（`value=1`）。2023-11-05 当地发生 DST 回退：
`2023-11-05T09:00:00Z` 时时钟从 02:00 PDT 拨回 01:00 PST。

**手推第四个窗口**（`2023-11-04T12:00-07:00` 开始；取窗口内一点 `t = 2023-11-05T08:00:00Z`，
忽略测试数据点的 +1s 偏移）：

| 步骤 | 旧代码 | 新代码 |
|------|--------|--------|
| `t_pre = t - offset` | `2023-11-04T20:00:00Z` | 同左 |
| `zone = Zone(t_pre)` | `-7h`（PDT） | 同左 |
| `dt = (t_pre+zone) mod 24h` | `13h` | 同左 |
| `start_raw = t_pre - dt` | `2023-11-04T07:00:00Z` | 同左 |
| `start = start_raw + offset` | `2023-11-04T19:00:00Z` | 同左 |
| `startOffset = Zone(start)`，修正 | `o=0`，不变 | 同左 |
| `end_raw = t_pre + (24h-dt)` | `2023-11-05T07:00:00Z` | 同左 |
| **先加 offset？** | 否 | 是 → `2023-11-05T19:00:00Z` |
| `endOffset = Zone(end)` | `Zone(07:00Z) = -7h` | `Zone(19:00Z) = -8h`（PST） |
| `o = zone - endOffset` | `0` | `+1h` |
| 修正 `end += o`（`abs(o) < 24h`） | 不加 | 加 → `2023-11-05T20:00:00Z` |
| **最终窗口** | `[11-04T19:00Z, 11-05T19:00Z)` = 24 小时 | `[11-04T19:00Z, 11-05T20:00Z)` = 25 小时 |
| 该窗口点数 | 24（少 1） | 25 |

旧代码的问题：`end_raw` 是“未加 offset 的 UTC 整点”，它正好落在 DST 切换前；
`Zone(end_raw)` 返回旧偏移 `-7h`，与 `zone` 相同，于是 `o=0`，跳过了 DST 修正；
之后才加 `offset`，窗口就短了一小时。新代码先把 `end` 移到真实时间轴
（`19:00Z`），此时 `Zone` 已经返回新偏移 `-8h`，`o=+1h` 被正确应用。

**预期结果**（回归测试）：

```
_window              count
2023-11-01T12:00-07:00   19
2023-11-02T12:00-07:00   24
2023-11-03T12:00-07:00   24
2023-11-04T12:00-07:00   25   ← 回退多出的一小时
2023-11-05T12:00-08:00    4
```

这个案例说明：**同一个 SQL 在旧版会少一行/少一个点**，升级后结果会变（变正确）。
如果业务对“DST 当天计数”做过补偿逻辑，需要重新核对。

### 6.2 案例 B：tag 过滤 + `SHOW TAG VALUES` 的 RP 语义

**查询**：

```sql
SHOW TAG VALUES FROM "mydb"."autogen"."cpu" WITH KEY = "host"
```

假设 `mydb` 有 `autogen` 和 `weekly` 两个 RP，且 `weekly.cpu` 里有
`host=stale-host`，`autogen.cpu` 里只有 `host=a`、`host=b`。

| 阶段 | 旧版（v1.8.10） | 新版（v1.12.3） |
|------|------------------|------------------|
| `RewriteStatement` | `rewriteShowTagValuesStatement` 返回的语句**丢掉 `Sources`**（`v1.8.10:query/statement_rewriter.go:266`） | 保留 `Sources: stmt.Sources`（`v1.12.3:query/statement_rewriter.go:317`） |
| `NormalizeStatement` | 对 measurement 做 DB/RP 归一化 | 对 `ShowTagValuesStatement` 跳过归一化（`coordinator/statement_executor.go:1419`） |
| 选择 shard | `for _, rpi := range di.RetentionPolicies`（所有 RP） | 先收集 `q.Sources` 里的 RP；没有则所有 RP（`:1129`-`:1148`） |
| 结果 | `a`、`b`、`stale-host` | `a`、`b` |
| 多 RP 查询 | 能跑，结果混合 | 返回 `only one retention policy allowed...`（`:1143`） |

**兼容性**：这是“结果变少但变对”的修复。依赖旧行为（跨 RP 混合）的报表需要改写；
显式写多个 RP 的查询从“静默混合”变成显式报错。
注意该修复只对 TSI 生效：INMEM shard 共享索引，限制 shard 不会限制 tag values
（commit `519e23b86a` 的说明）。

补充：如果是普通 `SELECT` 的 tag 过滤（`SELECT * FROM cpu WHERE host='a'`），
InfluxQL 路径**没有行为变化**：条件仍在 `IteratorOptions.Condition` 里下推到索引
（`query/select.go:622` 的 `buildCursor`），旧新一致。
Flux 路径的 `filter(fn: (r) => r.host == "a")` 也只是把谓词转换提前到计划期
（`rules.go:179`），谓词语义不变；`startsWith` 仍然不被 Index 支持，
`NodeToExpr` 会返回 `startsWith not implemented`（`storage/reads/influxql_predicate.go:112`-`:115`）。

### 6.3 案例 C：Flux `aggregateWindow(every: 1m, fn: mean)` 的下推轨迹

```flux
from(bucket: "mydb/autogen")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "cpu" and r.host == "a")
  |> aggregateWindow(every: 1m, fn: mean)
```

- 旧版：`ReadRange` + `filter` 下推，`window`/`mean` 在 Flux 层执行，存储返回全部原始点。
- 新版：`PushDownWindowAggregateRule` 产出 `ReadWindowAggregate(mean, every=1m)`，
  `PushDownWindowAggregateByTimeRule` 设 `TimeColumn=_stop`；存储侧
  `newFloatWindowMeanArrayCursor`（`array_cursor.gen.go:988`）→ `newFloatWindowTable`
  （`table.gen.go:117`）。因为 `TimeColumn!=""`，**不** `splitWindows`。
- 边界：游标 `EndTime=stop-1`，落在 stop 的点不参与；`_start/_stop` 裁剪到 range。

**收益**：1 小时 1 分钟窗口、每 series 3600 点 → 上层只收 60 个 mean 值；
`mean+count` 还能一次扫描返回两列（`MeanCountArray`）。
**注意**：如果下游依赖“原始点表”做 `keep()`/`map()`，下推后表里只有窗口值，
但这是 Flux 语义本身决定的（`aggregateWindow` 的输出就是窗口值），不是 1.x 的额外变化。

**数值案例（1 分钟窗口、3 个窗口）**：

- **输入**：`range(start: 10:00:00, stop: 10:03:00)`，点
  `(10:00:00,1)(10:00:20,2)(10:00:40,3)(10:01:00,4)(10:01:30,6)(10:02:00,8)`。
- **步骤与输出**：窗口 `[10:00,10:01)` → mean `(1+2+3)/3=2`；`[10:01,10:02)` →
  `(4+6)/2=5`；`[10:02,10:03)` → `8`。`aggregateWindow` 的 `TimeColumn=_stop`，表列是
  `_start,_stop,_time,_value`，`_time` 等于窗口 stop，三行：
  `(10:00,10:01,10:01,2)`、`(10:01,10:02,10:02,5)`、`(10:02,10:03,10:03,8)`。
- **边界**：Flux 的 `range(start, stop)` 是 `[start, stop)`，`storage/reads` 把请求改成
  `EndTime=stop-1`（`storage/reads/array_cursor.go:78-90`），所以恰好 `10:03:00.000` 的点
  不参与；`10:00:00.000` 的点属于第一个窗口。
- **对照**：若用 `window(every:1m) |> mean()`（`TimeColumn==""`），`splitWindows` 会把
  一张表拆成 3 张表，每张列为 `_start,_stop,_value`（无 `_time`）；`aggregateWindow`
  （`TimeColumn=_stop`）不拆表，产出一张 3 行表并带 `_time`
  （`storage/flux/reader.go:694-700`、`storage/flux/table.gen.go:299-310`、
  `storage/flux/reader.go:410-452`）。
- **源码**：`flux/stdlib/influxdata/influxdb/rules.go:676-742`、
  `storage/reads/array_cursor.gen.go` 的窗口 mean 游标、`storage/flux/table.gen.go:117`。

---

## 7. 仍存在的风险

### 7.1 组游标的错误仍然被吞掉

`GroupCursor.Next()` 现在会在建游标失败时返回 `false`，但 `Err()` 仍然是 `nil`：

```go
// v1.12.3:storage/reads/group_resultset.go:343
c.cursor, c.err = c.createCursor(*c.seriesRows[c.i-1])
return c.err == nil
// v1.12.3:storage/reads/group_resultset.go:333
func (c *groupByCursor) Err() error { return nil }
// 同样：groupNoneCursor.Err() 在 :281 也返回 nil
```

上层 `storage/flux` 的 group 循环恰好把“Cursor 为 nil”当作“没数据”跳过：

```go
// v1.12.3:storage/flux/reader.go:325
if cur == nil {
	gc.Close()
	gc = rs.Next()
	continue
}
```

也就是说，**sum over string 之类的类型错误在 group 路径不会暴露**：首个 series 建游标失败时整个 group 被跳过（`storage/flux/reader.go:324-329`）；后续 series 失败时 `advanceCursor` 提前结束，聚合只覆盖已读到的 series——是**截断**（`group_resultset.go:343-347`、`storage/flux/table.gen.go:963-983`）。
`WriteGroupResultSet` 里新增的 `gc.Err()` 检查（`response_writer.go:116`）永远看不到错误。
`writer` 只有在 **WindowAggregate** 路径能拿到错误；filter/group 的 `Err()` 均为 nil。

### 7.2 filter 结果集的 `Err()` 仍是 nil

```go
// v1.12.3:storage/reads/resultset.go:33
func (r *resultSet) Err() error { return nil }
```

而 `multiShardArrayCursors.createCursor` 在 `cur == nil || err != nil` 时返回 nil
（`array_cursor.go:126`），`filterIterator` 把 nil 当“该 series 没数据”继续
（`storage/flux/reader.go:183`-`:186`）。因此 filter 读路径的底层读块错误
**不会出现在最终错误里**。这是 #24791 修了一半的地方：`reset()` 首块的错误从 tsm1 传到了
`createCursor`，但 `createCursor` 在 `cur == nil || err != nil` 时直接返回 nil，把错误折叠掉；即使接上 `ResultSet.Err()` 也没有错误可拿（主文档 09 §6.4 同口径）。

### 7.3 `min`/`max` 窗口游标对不支持的类型直接 panic

```go
// v1.12.3:storage/reads/array_cursor.gen.go:158
panic(fmt.Sprintf("unsupported for aggregate min: %T", cur))
// :175 同样是 max；而 sum/mean 返回 error（:138、:192）
```

下推规则 `canPushWindowedAggregate` 只检查聚合列是 `_value`
（`flux/stdlib/influxdata/influxdb/rules.go:697`-`:726`），**不检查字段类型**；
Flux v0.196.1 的 `builtin min/max` 也没有类型约束（`universe.flux:1822`）。因此

    from(bucket: "db/rp") |> range(start: -1h)
      |> filter(fn: (r) => r._field == "s")
      |> window(every: 1m) |> min()

会一路下推（`PushDownFilterRule` 把 `_field` 谓词合并进 `ReadRange` 并删除 filter 节点），
在 `newWindowMinArrayCursor`/`newWindowMaxArrayCursor` 的 default 分支 panic
（`storage/reads/array_cursor.gen.go:157`-`:160`、`:174`-`:177`）。**可达**。

兜底不在 InfluxQL 的 `Executor.recover`：panic 发生在 Flux source goroutine
（`readWindowAggregateSource.run` → `processTables` → 建表/建游标），由 Flux `execute` 包的
`(*executionState).recover`（`flux@v0.196.1/execute/recover.go:16`，`//go:build !debug`）接住并转成
`codes.Internal`；`query/control` 的 `executeQuery` recover（`v1.12.3:query/control/controller.go:443`）
只覆盖查询协程。注意 `execute/recover_debug.go` 在 `debug` 构建标签下把 recover 变成空实现——
debug 构建里该 panic 会直接崩进程。

`v1.12.3:storage/flux/table.gen.go:3791`/`:4710` 的 `EInvalid` 分支（min/max + String/Boolean）
在这两条路径上**不可达**：`windowAggregateResultSet.createCursor`（`aggregate_resultset.go:119-133`）
与 `groupByCursor.createCursor`（`group_resultset.go:352-357`）先调用
`newWindowMin/MaxArrayCursor`，对非 float/int/uint 游标在 `storage/reads/array_cursor.gen.go:157-160`
/`:174-177` 直接 `panic("unsupported for aggregate min/max")`，不会把 `EInvalid` 返回给表生成器。
只有 `createCursor` 返回错误而非 panic 的路径（如 sum/mean）才会走到 `EInvalid`。

### 7.4 内存配额只增不减

```go
// v1.12.3:query/control/memory.go:143
func (q *queryMemoryManager) FreeMemory(bytes int64) {
	// Not implemented. ...
}
```

flux v0.196.1 的 `ResourceAllocator.Free` 只减 `bytesAllocated`、**不回调** `Manager.FreeMemory`（`flux@v0.196.1/memory/allocator.go:162-176`，全模块无 `FreeMemory(` 调用点），因此即使实现 `FreeMemory` 也不会退池。
`ResourceAllocator` 在查询过程中释放的中间结果不会退还给全局池，
只有 `Query.Done()` 里的 `Release()` 才一次性归还（`memory.go:151`-`:157`）。
后果：多个长查询即使已经把大表释放，仍可能因为“曾经申请过”而把池子占满，
新查询收到 `not enough capacity`。配置 `total-max-memory-bytes` 时要按
“峰值 × 并发”估算，而不是按“活跃内存”。

### 7.5 控制器配置不在示例配置里，且内存/并发/队列字段互相约束

`[flux-controller]` 只出现在 `cmd/influxd/run/config.go:45`，
`etc/config.sample.toml` 里没有这个 section（`git show v1.8.10:etc/config.sample.toml`
和当前文件都没有）。运维只能从代码/文档得知字段名。
校验边界（`query/control/controller.go`）：

- `ConcurrencyQuota > 0` 只强制 `QueueSize > 0`（`:146`-`:150`）。
- `MaxMemoryBytes` 的 `>= ConcurrencyQuota * InitialMemoryBytesQuotaPerQuery` 检查包在
  `if c.MaxMemoryBytes != 0` 内（`:160`-`:164`）。**`total-max-memory-bytes=0` 会绕过该校验**，
  `New()` 走 `unlimited=true`（`:195`-`:199`），内存仍不受限。
- `query-max-memory-bytes=0` 时 `complete()` 令其等于 `math.MaxInt64`，`Initial` 继承之
  （`:118`-`:126`）；若 `ConcurrencyQuota>0` 且 total 非零，`int64(ConcurrencyQuota)*Initial`
  会溢出为负数，不等式校验放行；`RequestMemory` 的 `q.limit+want` 同样会溢出
  （`memory.go:76`），单查询硬上限实际失效，只剩全局池约束。
- 因此 0 值一律表示“不限制”，且不保证校验能发现危险组合；生产环境应把
  `query-concurrency`、`query-queue-size`、`total-max-memory-bytes`、
  `query-max-memory-bytes`（及 `query-initial-memory-bytes`）一起配置。
- `ConcurrencyQuota`/`QueueSize` 是 `int32`，实际边界写在源码注释里：`ConcurrencyQuota` 受
  WaitGroup delta 限制为 `math.MaxInt32`（`controller.go:72-76`）；`QueueSize` 的
  `make(chan *Query, N)` 在 `N > 1<<45-12` 时 panic，取 `1<<45-12` 会立即 OOM，
  `math.MaxInt32` 在作者本机可启动（`controller.go:98-108`）。两者都没有编译期/启动期上限校验，
  危险值只靠 OOM/panic 暴露。

**端到端失败案例（`query-concurrency=1`、`query-queue-size=1`、3 个并发查询）**：

| 查询 | 路径 | 结果 |
|------|------|------|
| Q1 | `tryExec` 成功 → `Executing` | 占用唯一的执行槽 |
| Q2 | `enqueueQuery` → 队列（深度 1）→ worker 稍后 `tryExec` | 在队列中等待 |
| Q3 | `enqueueQuery` 时队列已满 | `codes.ResourceExhausted`（“queue length exceeded”，`controller.go:386-424`） |
| Q2 客户端断开 | `Cancel()` 取消父 ctx → `pump` 观察 `done` → `exec.Cancel()`；随后 `Done()` | `State()` 在 `Done()` 前对外报告 `Canceled`；`Done()` 内部转 `Finished` 并继续排空结果（`:896-930`、`:694-719`、`:735-760`） |
| 任一查询 `RequestMemory` 超过单查询硬上限 | `q.limit+want > memoryBytesQuotaPerQuery` | `errors.New("query hit hard limit")`，查询转 `Errored`（`memory.go:73-116`） |

状态迁移为 `Created → Compiling → Queueing → Executing → Canceled/Errored/Finished`；
队列满/内存硬上限都是**显式返回码**，不是静默排队或 OOM。

### 7.6 HLL 的误差与“吞错”

- `count_hll` 对**长度合法但内容非法**的 sketch 返回 0（`query/functions.go:2256`-`:2262`），
  具体输入/输出对照见 §3.5 的“非法 sketch 边界”表；
  `unmarshalPlus` 的错误被丢掉，查询不会失败。但 `unmarshalPlus` 直接切片
  `s[:len(hllPrefix)]` / `s[:len(hllErrorPrefix)]`（`query/functions.go:2170`-`:2173`）：
  `len(s) < 4`，或 `4 <= len(s) < 9` 且前缀不是 `HLL_`（`hllErrorPrefix` 是 9 字节）时**越界 panic**。
  正常重写路径输入永远是 `HLL_`+base64（`marshalPlus`），只有 `SELECT count_hll("tag")`
  这类直接对原始字符串取 sketch 的查询才会触发；panic 由上层 recover 兜住。
- `SHOW SERIES CARDINALITY FROM ...` 从此是估计值。HLL 的标准误差约为
  `1.04/sqrt(m)`（`pkg/estimator/hll` 的精度参数决定），但**本仓库没有暴露精度配置**，
  文档也不提示“这是估计”。需要精确值的运维必须显式写 `EXACT`。
- `sum_hll` 对数值用 BigEndian 编码、对字符串直接取字节，
  同一个逻辑值以不同字段类型写入会被当成不同元素。

### 7.7 `SHOW FIELD KEY CARDINALITY` 的条件改写不完整

新版重写里，子查询的条件是 `rewriteSourcesCondition(stmt.Sources, nil)`，
外层 `Condition` 仍是 `stmt.Condition`（`query/statement_rewriter.go:90`-`:103`）。
旧版外层用的是 `rewriteSources2` 后的数据源 + `stmt.Condition`。
如果 `WHERE` 里含有 measurement 限定（例如 `WHERE "host"='a'` 与
`FROM cpu, mem` 组合），外层条件不会跟着子查询的 `_fieldKeys` 源改写；
再加上 3.6(b) 提到的 aux 校验被回退，**行为差异需要按具体查询验证**，标记为待核实。

### 7.8 编译期 recover 覆盖不全

`Compile` 的 `defer recover` 注册在 `c.compile` 之后
（`query/compile.go:106`-`:128`，`defer` 在 `:118`）：`newCompiler`（`:107`）、`stmt.Clone()`（`:108`）、
`c.preprocess`（`:109`）、`c.compile`（`:112`）里的 panic 仍然会向上冒。真正的兜底是
`Executor.recover`（`query/executor.go:453`-`:469`），
它默认只记录并返回错误，只有 `INFLUXDB_PANIC_CRASH=true` 才 `os.Exit(1)`。

### 7.9 `ProfilerResults` 的类型断言

```go
// v1.12.3:query/control/controller.go:608
func (q *Query) ProfilerResults() (flux.ResultIterator, error) {
	p := q.program.(*lang.AstProgram)
```

仓库内 `ProfilerResults` 只有 `Query` 的实现与测试 mock（`query/encode_test.go:139`），
无业务调用点。该方法**先**断言 `q.program.(*lang.AstProgram)`，**再**检查 `len(p.Profilers)`
（`controller.go:608-612`），所以只要调用方（Flux 运行时 profiling 路径）调用它、而 compiler
不是 `lang.AstProgram`（自定义 Compiler），即使没有 profiler 也会 panic。影响面取决于是否存在
这样的调用方，不是由 profiler 数量决定；这属于“新控制器引入的隐式契约”。

---

## 8. 证据索引

### 8.1 结构性变更

| 主题 | v1.8.10 | v1.12.3 | 提交 / PR |
|------|---------|---------|-----------|
| Flux 控制器换成 2.x 实现 | `flux/control/controller.go:73`-`:127` | `query/control/controller.go:50`-`:1108` | `b9ee9e5b44` #21100 |
| 控制器参数可配置 | 无 | `query/control/controller.go:71`-`:163`、`cmd/influxd/run/config.go:45`、`:76` | `f1aa051787` #21108 |
| 控制器内存池 | 无 | `query/control/memory.go:11`-`:157` | `b9ee9e5b44` |
| 控制器指标 | `flux/control/controller.go:125` 返回 nil | `query/control/metrics.go:31`-`:111` | `b9ee9e5b44` |
| 控制器接线 | `cmd/influxd/run/server.go:299` | `cmd/influxd/run/server.go:327`-`:347` | `b9ee9e5b44` |
| 每用户查询字节 | 无 | `services/httpd/config.go:68`、`handler.go:484`-`:511`、`:821`/`:913`/`:2094`/`:2259` | `4dcc8776c1` #27188 |
| 查询结果 StatementID | `query/execution_context.go:93` 覆盖 | `query/execution_context.go:93` 不覆盖；`query/executor.go:432` | `0090c5b111` #22273 |
| TaskManager user/超时日志 | `query/task_manager.go:148`、`:281` | `query/task_manager.go:34`、`:76`、`:144`、`:329` | `32d8b002d8` #26981、`88d2760bc8` #23978、`2b7164440f` #27196 |
| 编译 panic 恢复 | 无 | `query/compile.go:106`、`:118`-`:127` | `98361e2073` #21792 |
| 多子查询 panic | `query/select.go:588`-`:594` 强制 `*VarRef` | `query/select.go:588`-`:594` 传 `expr.Args[0]` | `9e00f0de98` #26143 |
| DST offset | `query/iterator.go:864` | `query/iterator.go:856`-`:866` | `d60741b506` #25082 |
| MergeIterator.Close | 恒 nil | `query/iterator.gen.go:161`-`:173` `errors.Join` | `5fda409f39` #24975 |
| Iterators.Close | 无 nil 检查 | `query/iterator.go:46`-`:64` | `acc4105b8c` #21666 |
| 作者权限优化钩子 | 无 | `query/executor.go:93`-`:94`、`:169`-`:173`；`tsdb/index.go:2986`-`:2999` | `2cf2103cc4` #25207 |

### 8.2 storage/reads 与 storage/flux

| 主题 | v1.8.10 | v1.12.3 | 提交 / PR |
|------|---------|---------|-----------|
| Store 新增 WindowAggregate | `storage/reads/store.go:76`-`:78` | `storage/reads/store.go:81` | `a2154f143c` #21107 |
| GroupCursor.Aggregate | 无 | `storage/reads/store.go:75`、`group_resultset.go:288`、`:339` | `a2154f143c` #21107 |
| ReadWindowAggregateRequest | 无 | `storage/reads/datatypes/storage_common.proto:86`-`:94` | `a2154f143c` #21107 |
| 聚合枚举扩展 | `NONE/SUM/COUNT`（`storage_common.proto:78`-`:83`） | `...Min/Max/First/Last/Mean`（`:74`-`:78`） | `a2154f143c`、`e74ece2c91` #21281、`32aa970eba` #21291 |
| MultiPointsFrame | 无 | `storage_common.proto:126`、`:138`、`:179` | `32aa970eba` #21291 |
| 聚合游标扩展 | `array_cursor.go:20`-`:29` | `array_cursor.go:20`-`:56`；`array_cursor.gen.go:27`-`:220` | `a2154f143c`、`e74ece2c91` |
| 时间范围 [start,stop] | `array_cursor.go:100` | `array_cursor.go:78`-`:90` | `8edf7a4e2f` #21347 |
| 游标错误传播 | `array_cursor.go:133` `cur, _ =` | `array_cursor.go:126`；`array_cursor_iterator.gen.go:18` | `fe6c64b21e` #24791 |
| 整数 mean 原生累加 | float64 | `array_cursor.gen.go:2030`、`:2146`、`:3037`、`:3153` | `4cf19f8254` #21334 |
| 谓词转换前移 | `storage/reads/flux_reader.go:57`/`:77`/`:121`/`:245` | `rules.go:179`、`storage.go:69` | `7210fa6355` #20767 |
| 谓词 proto 换生成器 | `predicate.proto:51` `string tag_ref_value` | `predicate.proto:44` `bytes TagRefValue` | `8b38d0e2bf` #22606、`d4b16dcd98` #24949 |
| Flux reader/table 搬家 | `storage/reads/flux_reader.go`、`flux_table.go` | `storage/flux/reader.go`、`table.go`、`table.gen.go` | `31d4d742e8` #21114 |
| 表类型 10→25 | `flux_table.gen.go:25`、`:101` | `storage/flux/table.gen.go:32`、`:107`、`:320`、`:416`、`:657` | `a2154f143c`、`32aa970eba` |
| 表 empty 提前判定 | `flux_table.go:60` | `storage/flux/table.go:63`、`:73`-`:75`、`:87`-`:89` | `31d4d742e8` |
| 窗口切分 | 无 | `storage/flux/window.go:20`、`reader.go:694`-`:699` | `a2154f143c` |
| 下推规则 | `rules.go:12`-`:20`（6 条，0 条聚合） | `rules.go:22`-`:33`（11 条 = 6 基础 + `FromStorageRule` + 4 聚合） | `599700b41b` #21124、`e74ece2c91` #21281、`fbfd4b4651` #21074 |
| 响应 writer 重写（延迟建 frame / dataType 参数化 / defer 回收 / rs.Err·gc.Err / proto.Size） | `response_writer.go:67`-`:89`、`:197`-`:214`、`:255`-`:286` | `response_writer.go:71`-`:127`、`:205`-`:217`、`:262`-`:299` | `1d92b7a8fc` #23846 |
| 响应 MultiPoints / DataTypeMulti | 无 | `storage_common.proto:126`、`:138`、`:179`；`response_writer.go:342`-`:360` | `32aa970eba` #21291 |
| 组/窗口结果集 | `group_resultset.go` 旧实现 | `aggregate_resultset.go:28`-`:160`、`group_resultset.go:46`-`:370` | `a2154f143c` |

### 8.3 InfluxQL 与游标底层

| 主题 | v1.8.10 | v1.12.3 | 提交 / PR |
|------|---------|---------|-----------|
| HLL 算子 | 无 | `query/call_iterator.go:56`、`:1538`、`:1589`；`query/functions.go:2159`-`:2268` | `903b8cd0ea` #20603 |
| SHOW SERIES CARDINALITY 近似 | `query/statement_rewriter.go:183`-`:222` | `query/statement_rewriter.go:199`-`:243` | `de1a0eb2a9` #20745 |
| SHOW FIELD KEY CARDINALITY 索引化 | `query/statement_rewriter.go:55`-`:95` | `query/statement_rewriter.go:57`-`:101`、`tsdb/shard.go:2291` | `f5da0f50f4` #23871、回退 `cc26b7653c` #23875 |
| SHOW TAG VALUES Sources | `query/statement_rewriter.go:266` | `query/statement_rewriter.go:317`、`coordinator/statement_executor.go:1129`-`:1148` | `519e23b86a` #21983 |
| SHOW TAG KEYS WITH KEY | 无 | `query/statement_rewriter.go:398`-`:425` | `17b9ea8723` #20793 |
| MeanCountArray | 无 | `tsdb/cursors/arrayvalues.go:5`-`:37`、`cursor.go:42` | `32aa970eba` #21291 |
| CursorRequest inclusive | 无注释 | `tsdb/cursors/cursor.go:50`-`:71` | `8edf7a4e2f` #21347 |
| 数组 Len nil-safe | 直接 `len` | `tsdb/cursors/arrayvalues.gen.go:29`-`:34` 等 | `bc80e881fa` #24784 |
| array cursor 优化判定 | `tsdb/engine/tsm1/array_cursor.gen.go:107` | `:113`（`c.tsm.pos == 0`） | `31cb61f556` #26691 |
| 降序 reset | `tsdb/engine/tsm1/array_cursor.gen.go:183`-`:213` | `:190`-`:215` | `8edf7a4e2f` |
| tsm1 reset 返回错误 | `:41`-`:53` | `:42`-`:61` | `fe6c64b21e` |

---

> **交叉引用**：查询引擎结构见 [03 查询引擎](../03_query_engine_spec.md)；
> 数组游标逐行讲解见 [08 Array Cursor](../08_array_cursor_spec.md)；
> storage/reads 的 predicate/resultset 细节见 [09 Storage Reads](../09_storage_reads_spec.md)；
> InfluxQL 从 HTTP 到 Emitter 的完整链路见 [11 InfluxQL 执行](../11_influxql_execution_spec.md)。
