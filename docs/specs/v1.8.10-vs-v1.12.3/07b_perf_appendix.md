# 附录 B：性能、可观测性与测试（v1.8.10 → v1.12.3）

> 本文件是 [`07_build_perf_diff.md`](07_build_perf_diff.md) 的附录 B，保留 §3.9–§3.11、§4 与 §6.10–§6.14 的全文。
> **基线**
> - OLD（旧）: `v1.8.10` = commit `688e697c51`（2021-10-11）
> - NEW（新）: `v1.12.3` = commit `f3d5cb68ab`（本仓库 `docs/specs/` 的代码基准）
> - 区间: `git rev-list --count v1.8.10..f3d5cb68ab` = **527 commits**；`git diff --shortstat` = **602 files changed, 79,310 insertions(+), 45,239 deletions(-)**。当前工作树只比 `f3d5cb68ab` 多若干 docs 提交（数量随文档演进），代码相同。
> - 本文覆盖: `go.mod` / `go.sum`、`.circleci/*`、`scripts/ci/*`、`releng/*`、`Dockerfile`、`Makefile`、`kit/*`、`pkg/*`、`monitor/*`、`tests/*`、`stress/*`，以及**构建执行 / CI 编排 / 打包 / 发布 / 分发安装**、**测试基础设施（含性能回归缺口）**和**跨子系统性能优化**（compaction、TSI 缓存、purger、query control、array cursor、TSM 文件名缓存等）。各子系统的深度细节请看对应主文档，本文只给机制与 commit。
> - 旧版本证据统一来自 `git show v1.8.10:<path>`；新版本证据来自当前工作树（代码等价于 `f3d5cb68ab`）。

> **范围与关系**
> - **收录**：§3.9–§3.11（可观测性、测试基础设施、目录/共享库/工具）、§4（18 项优化的完整推导与逐条表）、§6.10–§6.14（分配计数、锁等待、tar 实测、构建计数、历史性能基线）**全文**。
> - **与主文档**：主文档保留同编号标题与分组摘要；本附录保留原始代码块、表格、公式、实测输出与全部证据，编号与主文档一致。
> - **跨文档引用**：`07 §3.x` 仍指向主文档的同编号标题；`07 §4.x` 的完整内容在本附录，主文档 §4 只保留分组摘要与关键结论。
> - **基线**：OLD = `v1.8.10`（`688e697c51`，2021-10-11），NEW = `v1.12.3`（`f3d5cb68ab`）；旧版证据来自 `git show v1.8.10:<path>`，新版来自当前工作树（代码等价于 `f3d5cb68ab`）。
> - **返回**：[主文档](07_build_perf_diff.md) · [附录 A：构建、CI、打包、发布与分发](07a_build_release_appendix.md)

## 3. 逐项差异

### 3.9 可观测性

#### 3.9.1 `config`：把生效配置塞进 `/debug/vars`

旧版 `serveExpvar` 只输出 `system` / `cmdline` / `memstats` / 各服务统计：

```go
// v1.8.10:services/httpd/handler.go:1517-1530
	if val := expvar.Get("cmdline"); val != nil {
		if !first {
			fmt.Fprintln(w, ",")
		}
		first = false
		fmt.Fprintf(w, "\"cmdline\": %s", val)
	}
	if val := expvar.Get("memstats"); val != nil {
		...
		fmt.Fprintf(w, "\"memstats\": %s", val)
	}
```

新版插入 `config` 段，并显式处理 `toml.Size` / `toml.Duration` 两种包装类型：前者转成 `uint64`，后者转成可读字符串（如 `48m0s`）。二者其实都能直接 `json.Marshal`，这里是为统一数值类型与可读性：

```go
// v1.12.3:services/httpd/handler.go:2356-2382
	if val := diags["config"]; val != nil {
		jv, err := parseConfigDiagnostics(val)
		...
		first = false
		_, err = fmt.Fprintf(w, "\"config\": %s", data)
		...
	}
```

```go
// v1.12.3:services/httpd/handler.go:2701-2726 — parseConfigDiagnostics
func parseConfigDiagnostics(d *diagnostics.Diagnostics) (map[string]interface{}, error) {
	if len(d.Rows) == 0 {
		return nil, fmt.Errorf("no config diagnostic data available")
	}

	m := make(map[string]interface{})
	for i, col := range d.Columns {
		if i >= len(d.Rows[0]) {
			continue
		}

		val := d.Rows[0][i]
		switch v := val.(type) {
		case toml.Size:
			m[col] = uint64(v)
		case toml.Duration:
			m[col] = time.Duration(v).String()
		default:
			m[col] = v
		}
	}

	return m, nil
}
```

**错误分支的后果**：上面的代码块省略了 `parseConfigDiagnostics`、`json.Marshal`、`fmt.Fprintln/Fprintf` 的失败处理；它们都调用 `httpError(500)`（`:2356-2382`），但进入 `config` 段前响应头已隐式 200、`{` 与前置板块已写出（`:2291-2322`），500 只能追加到 body，客户端得到 **200 + 截断 JSON**。`parseConfigDiagnostics` 在 `len(Rows)==0` 时返回 `no config diagnostic data available`（`:2701-2726`），企业扩展返回空诊断时可达；该缺陷与 06 §3.12c/§3.12e 的 `cq` 截断同类。

**这个 `config` 是 `[data]` 段的配置**（`monitor/service.go:164-166` 注册的是 `m.TSDBConfig`），不是整个 `influxd.conf`。同提交 `269f1b6281` 给 `tsdb.Config.Diagnostics()` 补了三个键：

```go
// v1.12.3:tsdb/config.go:304-306
		"aggressive-points-per-block":            c.AggressivePointsPerBlock,
		"compact-throughput":                     c.CompactThroughput,
		"compact-throughput-burst":               c.CompactThroughputBurst,
```

#### 3.9.2 `stats`：compaction 吞吐占用率

这是一个「新建文件级」的改动。`v1.8.10` 的 `monitor/` 里没有 `stats.go`（`git ls-tree v1.8.10 monitor/` 只有 15 个文件），新版新增：

```go
// v1.12.3:monitor/stats.go:20-34
// CompactThroughputUsage calculates the percentage of burst capacity currently consumed by compaction.
func (s *stats) CompactThroughputUsage() float64 {
	percentage := 100 * (1 - rate.Limit(s.comp.limiter.Tokens())/s.comp.limiter.Limit())
	return float64(percentage)
}

func (s *stats) Diagnostics() (*diagnostics.Diagnostics, error) {
	compactThroughputUsage := s.CompactThroughputUsage()
	compactThroughputUsageTrunc := math.Round(compactThroughputUsage*100.0) / 100.0
	d := map[string]interface{}{
		"compact-throughput-usage-percentage": compactThroughputUsageTrunc,
	}

	return diagnostics.RowFromMap(d), nil
}
```

注册条件与数据来源：

```go
// v1.12.3:monitor/service.go:156-162
	if m.Limiter != nil {
		m.RegisterDiagnosticsClient("stats", &stats{
			comp: compactThroughputStats{
				limiter: m.Limiter,
			},
		})
	}
```

```go
// v1.12.3:cmd/influxd/run/server.go:525（提交 0e656d6dd5）
	s.Monitor.WithCompactThroughputLimiter(s.TSDBStore.EngineOptions.CompactionThroughputLimiter)
```

为了让 `Tokens()`/`Limit()` 可用，`pkg/limiter` 的接口被扩宽：

```go
// v1.12.3:pkg/limiter/writer.go:18-23
type Rate interface {
	WaitN(ctx context.Context, n int) error
	Burst() int
	Tokens() float64
	Limit() rate.Limit
}
```

**注意语义**：`1 - Tokens/Limit` 衡量的是**令牌桶的瞬时占用**，不是「过去一分钟平均吞吐」。公式的分母是 `limiter.Limit()`（速率上限，tokens/s），不是 burst 容量；只有 `burst == limit`（默认两者都是 48 MiB）时 `1 - Tokens/Limit` 才等于 burst 占用率。`store.loadShards` 只把 burst 钳制到 ≥ throughput（`tsdb/store.go:527-539`），允许 `burst > throughput`；此时令牌桶满时结果是 `100×(1 - burst/limit)`，例如 burst=2×limit 时为 **−100%**，按非默认配置读告警会误判。`limiter.NewRate` 在创建时会先 `AllowN(now, burstLimit)` 把初始 burst 花掉（`pkg/limiter/writer.go:27`），所以新启动的实例在第一次补充令牌前会读到接近 100%。且 `compact-throughput` 的默认值是 48 MiB/s（`tsdb/config.go:44,213`），此时 `store.loadShards` 因 `throughput > 0` 创建 limiter（`tsdb/store.go:527-539`），`stats` 诊断项默认就会注册（`monitor/service.go:156-162`）；只有把它**显式设为 0** 才走 `unlimited` 分支、`Limiter == nil`，`stats` 不注册。

#### 3.9.3 `cq`：CQ 统计扁平化

旧版 `cq` 从未被注册成 diagnostics client（`git show v1.8.10:cmd/influxd/run/server.go | grep RegisterDiagnosticsClient` 无 `"cq"`），但它本来就以 `Name:"cq"` 走 `Server.Statistics` 进入 `/debug/vars`，只是带 `{name,tags,values}` 包装。新版的注册只影响 `SHOW DIAGNOSTICS`；`/debug/vars` 的扁平化来自 `serveExpvar` 里对 `Name=="cq"` 的特判：

```go
// v1.12.3:cmd/influxd/run/server.go:425-427
	if s.Monitor != nil {
		s.Monitor.RegisterDiagnosticsClient("cq", srv)
	}
```

```go
// v1.12.3:services/httpd/handler.go:2452-2477（节选）
	for _, s := range stats {
		if s.Name == "cq" {
			jv, err := parseCQStatistics(&s.Statistic)
			...
			_, err = fmt.Fprintf(w, "\"cq\": %s", data)
			...
			continue
		}
```

```go
// v1.12.3:services/httpd/handler.go:2728-2734
func parseCQStatistics(s *models.Statistic) (map[string]interface{}, error) {
	if len(s.Values) == 0 {
		return nil, fmt.Errorf("no cq statistics data available")
	}

	return s.Values, nil
}
```

**行为差异**：别的 statistic 走 `json.Marshal(s)`，输出 `{"name":...,"tags":...,"values":{...}}`；`cq` 只输出 `Values` 那一层。如果监控侧有按 `name` 字段解析的通用逻辑，对 `cq` 需要特判。

#### 3.9.4 `userquerybytes`：按用户计量查询响应字节

```go
// v1.12.3:services/httpd/handler.go:483-498
	// Add per-user query bytes as separate statistics (one per user) if enabled
	if h.Config.UserQueryBytesEnabled && !h.queryBytesPerUser.IsEmpty() {
		h.queryBytesPerUser.Range(func(user string, counter *atomic.Int64) bool {
			userTag := user
			if user == "" {
				userTag = StatAnonymousUser
			}
			userTags := models.NewTags(tags).Merge(map[string]string{StatUserTagKey: userTag}).Map()
			stats = append(stats, models.Statistic{
				Name:   "userquerybytes",
				Tags:   userTags,
				Values: map[string]interface{}{statUserQueryRespBytes: counter.Load()},
			})
			return true
		})
	}
```

计数器本体是泛型化的 `sync.Map`：

```go
// v1.12.3:pkg/data/gensyncmap/gensyncmap.go:5-7,29-31
type Map[K comparable, V any] struct {
	m sync.Map
}
...
func (m *Map[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
```

四个累加点（`handler.go:821`、`:913`、`:2094`、`:2259`）分别覆盖 chunked query、非 chunked query、Prometheus remote read（`POST /api/v1/prom/read`）与 Flux（`POST /api/v2/query`）路径。开关：

```go
// v1.12.3:services/httpd/config.go:68
	UserQueryBytesEnabled    bool              `toml:"user-query-bytes-enabled"`
```

提交 `4dcc8776c1`（#27188）说明它也可以通过 `INFLUXDB_HTTP_USER_QUERY_BYTES_ENABLED` 打开——这走的是通用环境变量覆盖机制，不是专门的读取代码：**运行入口**是 `cmd/influxd/run/command.go:84` 的 `config.ApplyEnvOverrides(cmd.Getenv)`，**`influxd config` 打印入口**是 `cmd/influxd/run/config_command.go:46` 的 `config.ApplyEnvOverrides(os.Getenv)`。映射规则在 `toml/toml.go:147-151,255-280`：TOML 路径的 `-` 换成 `_`、转大写，并以 `INFLUXDB` 为前缀（如 `[http] user-query-bytes-enabled` → `INFLUXDB_HTTP_USER_QUERY_BYTES_ENABLED`）。

#### 3.9.5 动态日志级别（`zap.AtomicLevel`）

```go
// v1.8.10:logger/config.go:8-19
type Config struct {
	Format       string        `toml:"format"`
	Level        zapcore.Level `toml:"level"`
	SuppressLogo bool          `toml:"suppress-logo"`
}

func NewConfig() Config {
	return Config{
		Format: "auto",
		Level:  zapcore.InfoLevel,
	}
}
```

```go
// v1.12.3:logger/config.go:8-19
type Config struct {
	Format       string          `toml:"format"`
	Level        zap.AtomicLevel `toml:"level"`
	SuppressLogo bool            `toml:"suppress-logo"`
}

func NewConfig() Config {
	return Config{
		Format: "auto",
		Level:  zap.NewAtomicLevel(),
	}
}
```

差异不只是类型：`zapcore.Level` 是一个 `int8`，日志级别在 `zap.New(core)` 时被固化进 core；`zap.AtomicLevel` 内部是原子变量，所有从同一个 `Config.Level` 派生出来的 logger 共享同一个级别，**调用 `Level.SetLevel()` 会立刻对所有子 logger 生效**。提交 `a743838f9d`（#26182）还顺带改了 `cmd/influx_inspect/verify/seriesfile/command.go`、`cmd/influx_tools/compact/command.go`、`toml/toml_test.go` 与 `cmd/influxd/run/config_test.go`。

`zap.NewAtomicLevel()` 的默认值是 `InfoLevel`，与旧的 `zapcore.InfoLevel` 一致，所以**默认行为不变**。

#### 3.9.6 file store 归并指标

这是「为未来优化做测量」的典型：先加指标，再决定要不要改算法。

```go
// v1.12.3:tsdb/engine/tsm1/file_store.go:163-173
var (
	TsmGroupMergeTimeSuffix        = "merge_duration"
	TsmGroupMergeCountSuffix       = "merge_count"
	TsmGroupMergeValuesCountSuffix = "merge_values_count"
	TsmGroupMergeOver4CountSuffix  = "merge_over4_count"
	floatBlocksDecodedCounter      = metrics.MustRegisterCounter("float_blocks_decoded", metrics.WithGroup(tsmGroup))
	floatBlocksSizeCounter         = metrics.MustRegisterCounter("float_blocks_size_bytes", metrics.WithGroup(tsmGroup))
	floatBlocksMergeTimer          = metrics.MustRegisterTimer("float_blocks_"+TsmGroupMergeTimeSuffix, metrics.WithGroup(tsmGroup))
	floatBlocksMergeCount          = metrics.MustRegisterCounter("float_blocks_"+TsmGroupMergeCountSuffix, metrics.WithGroup(tsmGroup))
	floatBlocksMergeOver4Count     = metrics.MustRegisterCounter("float_blocks_"+TsmGroupMergeOver4CountSuffix, metrics.WithGroup(tsmGroup))
	floatBlocksMergeValuesCount    = metrics.MustRegisterCounter("float_blocks_"+TsmGroupMergeValuesCountSuffix, metrics.WithGroup(tsmGroup))
```

埋点在生成的 KeyCursor 代码里（`file_store.gen.go.tmpl` 模板 + `.gen.go` 产物），仅当查询带 tracing span、`c.col != nil` 时，每次 `Read*Block` 才统计归并次数与值个数：

```go
// v1.12.3:tsdb/engine/tsm1/file_store.gen.go（ReadFloatBlock 尾部，提交 133c290fc2）
	if c.col != nil {
		c.col.GetTimer(floatBlocksMergeTimer).AddSince(start)
		c.col.GetCounter(floatBlocksMergeCount).Add(mergeCount)
		c.col.GetCounter(floatBlocksMergeValuesCount).Add(int64(mergeValuesCount))
		if mergeCount > 4 {
			// We want to know when there are "many" (over 4) merge operations in a single Read call.
			// If we see this is high, doing a tree merge of blocks may be beneficial compared to the current sequential
			// merge (n^2 versus nlogn in number of values).
			c.col.GetCounter(floatBlocksMergeOver4Count).Add(1)
		}
	}
```

`> 4` 这个阈值直接把「是否值得换成树形归并」这个判断写进了指标语义：当前实现是顺序归并，k 个 block 合并是 O(n·k) 级别；如果某个请求的 tracing span 里 `merge_over4_count` 偏高，说明该请求在付平方级代价。注意输出路径：指标挂在查询的 tsm1 `metrics.Group` 上，只有查询启用 tracing 时才创建（`Engine.CreateIterator`），最终在 instrumented iterator 的 `Close()` 里 `span.SetFields`，只进 tracing span；`pkg/metrics` 没有 Gather/expvar 导出路径，因此它们不进入 `/debug/vars`/`SHOW STATS`，也没有跨请求聚合。

配套的 `pkg/metrics.Timer` 增加了累加语义（旧版只有 `Update`，是覆盖不是累加）：

```go
// v1.12.3:pkg/metrics/timer.go:26-27
// AddSince adds to the timer's current value the difference between since and the current time.
func (t *Timer) AddSince(since time.Time) { t.val.Add(int64(time.Since(since))) }
```

而且内部字段从 `int64` + `atomic.LoadInt64(&t.val)` 换成了 `atomic.Int64`（`timer.go:10,18,21,30`），消除了「忘记用 atomic 包」的可能。

> 交叉引用：这组 `merge_*` 指标服务的是**查询 tracing 链路**（tsm1 key cursor 的 instrumented iterator），不是 `monitor.Monitor` 的 expvar 统计。机制细节见 [03 查询引擎差异](03_query_engine_diff.md)（array cursor / storage read 路径）与主文档 [16 FileStore + KeyCursor](../16_filestore_keycursor_spec.md)；不要把它当成可以从 `/debug/vars` 或 `SHOW STATS` 采集的指标。

### 3.10 测试基础设施

> 原有三个子模块（property/CheckedClose/CI 矩阵）保留；另补两个被审查判定为缺口、原文档只有一句话的模块：包安装功能冒烟（§3.10.4）与性能基准回归（§3.10.5）。

#### 3.10.1 property-based compaction 测试

提交 `ba2c2b6432`（#26783）引入了一个**不变量验证框架**，而不是又一组用例：

```go
// v1.12.3:tsdb/engine/tsm1/compact_property_test.go:11-23
// CompactionProperty represents a property that compaction groups should satisfy
type CompactionProperty struct {
	Name        string
	Description string
	Validator   func(allFiles []string, groups []tsm1.CompactionGroup) error
}

// AdjacentFileProperty validates that compaction groups don't create gaps
var AdjacentFileProperty = CompactionProperty{
	Name:        "Adjacency Rule",
	Description: "Files should not have non-adjacent files within the same compaction level - if files A and C are in the same level group, any file B between them should also be in a group at the same level or higher",
	Validator:   validateFileAdjacency,
}
```

这条不变量的物理意义在注释里写得很清楚：

```go
// v1.12.3:tsdb/engine/tsm1/compact_property_test.go:34-39
// The following example will highlight an adjacency violation:
// Given the following list of files [01-01.tsm, 02-02.tsm, 03-03.tsm] let's say we have the following compaction plans created
// Group 1: [01-01.tsm, 03-03.tsm] & Group 2: [02-02.tsm]
// This violates file adjacency as the first compaction group sees [01-01.tsm, X, 03-03.tsm] and the second group: [X, 02-02.tsm, X]
// these are non-contiguous blocks, when the first group performs compaction we will have two files that are out of order compacted together.
// This rule is important to maintain the ordering of files, with improper ordering we cannot determine which point is the newest point when overwrites occur.
// We always want the newest write to win.
```

对回归的价值：compaction planner 在 2025 年被重写了三次（`2a0353ffd2`、`d1d68d656d`、`95e86be3f4`），每次都要动 `plan()` 的分组逻辑。有了「相邻性」这条可机械验证的不变量，改完之后可以批量扫描所有 plan 结果，而不是靠人肉补用例。`7f56ad35ac`（#26816）就是在这个框架里修了「排序后再做相邻性检查」的问题——框架先落地，然后立刻抓到了自己的 bug。案例见 §6.4。

#### 3.10.2 `CheckedClose`

```go
// v1.12.3:pkg/testing/helper/helpers.go:10-25
// CheckedClose returns a function that can be used with a defer to check that a
// Closer's Close method does not return an error.
func CheckedClose(t require.TestingT, c io.Closer) func() {
	return func() {
		if h, ok := t.(interface{ Helper() }); ok {
			h.Helper()
		}
		require.NoError(t, c.Close())
	}
}
```

提交 `0f3436d10d`（#27217）。当前使用点集中在 `services/httpd/service_test.go`：5 处 = 4 处 `defer th.CheckedClose(t, s)()` + 1 处 `t.Cleanup(th.CheckedClose(t, s))`（`:53,103,176,205,261`）；`t.Cleanup` 在测试结束时运行，语义与 `defer` 不同。它的价值更多是**示范性**：`defer f.Close()` 静默吞错是 Go 测试里最常见的漏检模式之一，有了统一 helper 之后新代码有地方抄。

#### 3.10.3 CI 测试矩阵

| 维度 | v1.8.10 | v1.12.3 |
|------|---------|---------|
| 普通测试 | `test.sh 0`（64bit，含 `--package`） | `unit_test` + `INFLUXDB_DATA_INDEX_VERSION=inmem` |
| race | `test.sh 1`，`GORACE=halt_on_error=1` | `unit_test` + `race: true`，`taskset -c 0-7` 限制到 8 核 |
| 32bit | `test.sh 2`（`--arch=i386`） | **无** |
| TSI | `test.sh 3` | `unit_test` + `data: tsi1` |
| Flux 一致性 | 无独立 job | `fluxtest` → `./test-flux.sh`（按 go.mod 的 flux 模块版本下载测试集；仅伪版本 `v0.0.0-...-<rev>` 时取 revision） |
| 结果格式 | `go-junit-report`（build.py 内） | `gotestsum --junitfile` + `store_test_results` |

`taskset` 那段注释解释了一个诡异的 CI 波动：

```yaml
# v1.12.3:.circleci/config.yml:319-332
                  # "resource_class: xlarge" creates a Docker container with eight
                  # virtual cpu cores. However, applications like "nproc" return
                  # the host machine's core count (which in this case is 36).
                  # When less cores are available than advertised, the tests
                  # sometimes fail.
                  taskset -c 0-7 \
                    gotestsum \
                      --format=standard-verbose \
                      --junitfile=junit-race-<< parameters.data >>/influxdb.junit.xml \
                      -- -race ./...
```

这套矩阵降低回归风险的方式很直接：**同一个提交要在 inmem 和 tsi1 两种索引实现下都通过**，且 race 版本跑全量包。因为 `tsdb/index/inmem` 与 `tsdb/index/tsi1` 是两套独立实现，只测一种会漏掉另一套的分支。代价是 32bit 环境被移除，`test.sh 2` 的 i386 覆盖不再存在。

#### 3.10.4 包安装测试：3 个功能冒烟脚本 → 只做安装/卸载

**旧版（v1.8.10）**：`pkg_run_test` job（`v1.8.10:.circleci/config.yml:61-109`）在 AWS EC2 上跑安装后的功能冒烟：

1. `terraform validate` + `terraform apply`（`scripts/ci/main.tf`，默认 `t3.micro`、Ubuntu focal，`main.tf:25-58`），把 `scripts/ci/tests/` 作为 `additional_files_dir` 传到 `/home/ubuntu/files`。
2. SSH 到实例执行 `sudo apt-get install -y /home/ubuntu/influxdb.deb`。
3. 遍历 `/home/ubuntu/files/` 下所有脚本，逐个 `sudo` 执行（`config.yml:93-101`）。
4. `terraform destroy`（`when: always`）。

三个脚本（`v1.8.10:scripts/ci/tests/`）：

| 脚本 | 实际验证 |
|---|---|
| `auth-enabled.sh` | 启动服务；把 `auth-enabled`/`ping-auth-enabled` 改为 true；`influx -execute "CREATE USER admin ... WITH ALL PRIVILEGES"`；stop/start；最后还原配置 |
| `default-config.sh` | 用默认配置 `service influxdb start` 再 `stop`，验证包安装后默认配置能起服务 |
| `self-signed-certs.sh` | 打开 `https-enabled`，用 `openssl req -x509` 生成自签证书，改证书/私钥路径，`service influxdb start`/`stop`，最后还原配置 |

**新版（v1.12.3）**：`test_pkgs_64bit`（`.circleci/config.yml:258-279`）在 `ubuntu-2204` machine 上执行 `releng/packages/spec/clean_install/run.bash -D/-R`：

- `run.bash` 对 deb 用 `ubuntu:latest`、`ubuntu:rolling` 两个 Docker 镜像，对 rpm 用 `centos:7`（`run.bash:55-70`）。
- `_install_uninstall.bash` 只做三件事：`apt-get install`/`yum localinstall`；断言 `influx`、`influxd`、`influx_inspect` 在 `$PATH`；用 `dpkg -c`/`rpm -qlp` 检查 8 个 man page 会被安装到 `/usr/share/man/man1/`；然后卸载并断言三个二进制消失（`_install_uninstall.bash:19-87`）。
- **不启动服务**（脚本注释明确说 Docker 里通常不能起服务，只断言二进制在 PATH，`_install_uninstall.bash:3-8`）。

**测试范围收窄**：

| 能力 | v1.8.10 | v1.12.3 |
|---|---|---|
| deb 安装/卸载 | 有（EC2 Ubuntu） | 有（Docker ubuntu:latest/rolling） |
| rpm 安装/卸载 | 无 | 有（Docker centos:7） |
| 默认配置能起服务 | 有（`default-config.sh`） | **无** |
| auth 开启后能建管理员并重启 | 有（`auth-enabled.sh`） | **无** |
| 自签 TLS 证书能起服务 | 有（`self-signed-certs.sh`） | **无** |
| 发布门禁 | `pkg_run_test` requires `build_and_test` | `test_pkgs_64bit` 与 `sign_packages` 并行；`publish_packages` 只 requires `sign_packages`，安装测试失败**不阻断发布** |

这不是“完全删除”，而是**从功能冒烟退化为文件级安装/卸载**。仓库内没有这三个功能脚本的替代品；如果是有意收窄，应把 auth/TLS/默认配置的启动验证放到发布前的人工清单，或补一个能起 systemd 的 machine executor 用例。风险见 §7 风险 14。

#### 3.10.5 性能基准与回归：旧 perf_test/AWS/S3 → 无替代

**旧版角色**：`perf_test` job（`v1.8.10:.circleci/config.yml:111-134`）是一个真机性能回归：在 AWS EC2 上安装刚打出的 deb，运行 `influxdb-comparisons` 的写入/查询基准，把结果写入 InfluxDB Cloud 2 并保留日志。一次运行的输入、步骤与预期产物见 §6.14。

**旧版结构**：

- job 参数：`format`（`http` / `flux-http`）、`record_ingest_results`（bool）；`machine` 镜像 `ubuntu-2004:202010-01`；`no_output_timeout: 20m`；`requires: build_and_test`；workflow 里只对 `1.8` 分支跑（`:200-209`）。
- `scripts/ci/perf_test.sh`：
  - 用 SSM 取 Ubuntu 20.04 AMI，起 `r5.2xlarge`（8 vCPU/64 GiB 级）、100 GB EBS、固定 security group/subnet/key（`:12-25`）。
  - `trap` 终止实例；把 `influx_tools-<sha>` 上传到 `s3://perftest-binaries-influxdb/influx_tools/`，并维护 `latest_<branch>.txt`（`:54-59`）。
  - `scp` deb、`influx_tools`、`run_perftest.sh` 到实例；`apt-get install` deb；改 `flux-enabled=true`、`index-version="tsi1"`；`systemctl start influxdb`；挂 32G tmpfs 作数据集目录（`:63-73`）。
- `scripts/ci/run_perftest.sh`：
  - 装 Telegraf，输出到 InfluxDB Cloud 2 的 `${CLOUD2_BUCKET}`（`:30-85`）。
  - 克隆 `influxdb-comparisons`，构建 `bulk_data_gen`、`bulk_load_influx`、`bulk_query_gen`、`query_benchmarker_influxdb`（`:106-115`）。
  - 固定参数：`scale_var=1000`（cardinality ≈ 10^6）、`queries=500`、`batch=5000`、`workers=4`、`duration=30s`（`:125-143`）。
  - 用例：`window-agg`、`group-agg`、`bare-agg`、`ungrouped-agg`、`group-window-transpose-low/high-card`、`iot`、`metaquery`、`multi-measurement`（`:236-315`）。
  - 每个数据集写入后 `force_compaction`（停服务，对每个 shard 跑 `/home/ubuntu/influx_tools compact-shard -force`，`:146-162`），再跑查询。
  - 日志在退出时上传到 `s3://perftest-logs-influxdb/oss/<branch>/<commit>-<timestamp>.log`；`CIRCLE_TEARDOWN=true` 时回调 CircleCI 触发 `aws_destroy_by_name`（`:17-27`）。
- 两个 AWS 清理 workflow：`aws_destroy_daily` 按日期清理 `oss-perftest-*`；`aws_destroy_callback` 按参数构造实例名销毁（`v1.8.10:.circleci/config.yml:135-225`）。

**为什么现在没有**：v1.12.3 的工作树里不存在这些文件：`git cat-file -e v1.12.3:scripts/ci/perf_test.sh`、`run_perftest.sh`、`main.tf`、`tests/auth-enabled.sh` 全部失败；`scripts/ci/` 只剩 `CHANGELOG_frozen.md` 与 `install-rust.sh`；`.circleci/config.yml` 里没有任何 `perf` 字样。v1.8.10 与 v1.12.3 不在同一条发布线上（`git merge-base v1.8.10 f3d5cb68ab` = `2e371db2a0`，v1.8.10 不是 v1.12.3 的祖先），所以这不是“v1.12.3 某个提交删除了它”，而是**1.8 线的性能基础设施没有进入 1.12.3 线**。`git log --all -- scripts/ci/perf_test.sh` 显示它最后在 3.0 重写时被 `09783cbb73` 删除，但该提交不在 v1.12.3 的祖先链上。

**当前缺口**：

1. CI 不跑任何 `go test -bench`：仓库里有 60 个包含 `func Benchmark` 的 `_test.go` 文件（共 356 个函数；v1.8.10 为 56 个文件 / 348 个函数），但 `.circleci/config.yml` 没有任何 `-bench` 调用。
2. 没有真机写入/查询基准：§4 的所有“优化”都没有跨版本前后数值。
3. 没有性能门槛：没有 job 会在回归超过阈值时失败。
4. 可用的替代信号只剩：`tsmCompactionsQueue`/`tsmOptimizeCompactions*` 等队列指标、查询 tracing 里的 `merge_*` 指标（仅 tracing span，见 §3.9.6）、以及本地手跑 `go test -bench`。
5. `influx_tools compact-shard` 不再出制品，旧 harness 即使从 1.8 分支搬回来也要先自己构建 `influx_tools`。

**对 §4 的约束**：因此 §4 只写机制、commit 和调用点推导，不写“提升 X%”；需要数字时必须先用本节的方法本地或自建 CI 测。

### 3.11 目录、共享库与工具

#### 3.11.1 代码生成与工具

仓库里所有 `.pb.go` 都由 `generate.sh` 生成，两版安装的插件不同：

```bash
# v1.8.10:generate.sh:3-4 — gogo 插件
go install github.com/gogo/protobuf/protoc-gen-gogofaster
go install github.com/gogo/protobuf/protoc-gen-gogo
```

```bash
# v1.12.3:generate.sh:3-5 — 官方插件 + 两个代码生成器
go install google.golang.org/protobuf/cmd/protoc-gen-go
go install golang.org/x/tools/cmd/stringer
go install github.com/benbjohnson/tmpl
```

依赖侧同步换血：旧 `go.mod:18` 把 `github.com/gogo/protobuf v1.3.2` 列为**直接**依赖（`protoc-gen-gogo` 来自它）；新 `go.mod:57` 的直接依赖是 `google.golang.org/protobuf v1.34.1`，gogo 在 `go.mod:126` 降为 `// indirect`。`git diff --stat v1.8.10 f3d5cb68ab -- '*.pb.go'` = **13 files, +9148/−15082**：官方生成器产出的代码更薄，wire 格式不变，旧数据与旧客户端不受影响。

工具侧还有两处变化：

- 旧版 `scripts/protoc-gen-gogo`、`scripts/protoc-gen-gogofaster` 是 `go run` 包装器（`v1.8.10:scripts/protoc-gen-gogo:1-2`）；新版删除了这两个，保留/新增 `scripts/protoc-gen-go`（`go run google.golang.org/protobuf/cmd/protoc-gen-go "$@"`，`:1-2`），同时 `generate.sh` 直接 `go install` 官方插件。
- 新版 `releng/protoc-gen` 在 cross-builder 容器里跑 `go generate ./...`，并用文件时间戳把新生成文件的 owner 改回当前用户（`releng/protoc-gen:20-46`）；它从 `.circleci/config.yml` 读 `cross-container-tag` 默认值。旧版没有这个 helper。

生成流程本身没变，仍是“`go generate ./...` + 脏工作区即失败”；`check_changes` 的幂等性校验见 §3.5.1，两版逐字相同。变的只是插件、包装器与 helper。风险：`generate.sh` 的插件版本来自 `go.mod` 解析，若 `go.mod` 的 protobuf 版本与提交时不同，`generate.sh` 产物可能与已提交 `.pb.go` 不一致；CI 的 `static_code_checks` 会重跑 `./generate.sh`（`.circleci/config.yml:281-290`）并因 `git status` 非空失败，属于可检出风险（§7 风险 13）；本地用同一 `toolchain go1.24.13` 重跑可复现。

#### 3.11.2 共享库层：`kit/`、`pkg/`、`monitor/`、`tests/`

| 目录 | v1.8.10 | v1.12.3 | 变化说明 |
|---|---|---|---|
| `kit/` | 不存在 | 34 个文件 | 提交 `78724e5c20`（#21086）从 2.x 移植：`check`、`errors`、`io`、`metric`、`platform`、`prom`、`signals`、`tracing`、`transport/http` |
| `pkg/` | 110 个文件 | 125 个文件 | 新增 `file/file.go`（`MoveFileWithReplacement`）、`tar/untar.go`、`errors/error_capture.go`、`data/gensyncmap`、`testing/helper`、`testing/selfsigned`、`testttp`、`tlsconfig/certconfig.go`、`tlsconfig/configmanager.go`；**无删除** |
| `monitor/` | 15 个文件 | 17 个文件 | 新增 `stats.go` + `stats_test.go`；`service.go` 增加 `TSDBConfig`、`Limiter` 字段 |
| `tests/` | 8 个文件 | 9 个文件 | 新增 `slowproxy_test.go`（提交 `91609fdd3f`，用于复现 restore 竞态） |

**`kit` 与 `pkg` 的边界**：`pkg/` 是 1.x 自己的通用库（文件、tar、错误、TLS、测试 helper、限流等），可以被 1.x 任意包引用；`kit/` 是从 2.x 搬来的“跨产品共享层”，命名与 API 按 2.x 的语义，**不是 1.x 内部库**。判断一个改动该放哪：1.x 内部复用放 `pkg/`；与 2.x/企业版保持同源、将来可能整体替换的放 `kit/`。

`kit/` 各子包在 1.x 的实际消费者（非测试代码）：

| `kit/` 子包 | 文件数 | 非 `kit` 消费者 |
|---|---:|---|
| `platform/errors` | 4 | `query/control/controller.go`、`storage/flux/reader.go`、`storage/flux/table.gen.go`、`storage/flux/window.go`、`storage/reads/array_cursor.gen.go` |
| `platform`（`id.go`/`id_test.go`） | 2 | **无 1.x 调用点**（只有自身测试） |
| `errors` | 2 | `query/control/controller.go`、`storage/reads/aggregate_resultset.go` |
| `prom` | 5 | `query/control/controller.go` |
| `tracing` | 3 | `logger/fields.go`、`query/control/controller.go` |
| `check` / `io` / `metric` / `signals` / `transport/http` | 4 / 2 / 2 / 2 / 8 | **无 1.x 调用点**（只有自身测试） |

也就是说：34 个文件中，真正被 1.x 业务代码消费的是 `platform/errors`、`errors`、`prom`、`tracing` 四类；`check`（/health、/ready）、`io`、`metric`、`signals`、`transport/http` 目前是“随 2.x 一起搬进来但 1.x 未使用”的库存。引入提交 `78724e5c20` 是结构性变化：2.x 的共享库层进入 1.x，后续升级/替换要同时看 2.x 的 API 变化。

`pkg/errors.Capture` 是一个小而关键的语义修正：

```go
// v1.12.3:pkg/errors/error_capture.go:14-21
func Capture(rErr *error, fn func() error) func() {
	return func() {
		err := fn()
		if *rErr == nil {
			*rErr = err
		}
	}
}
```

它保证 `defer errors.Capture(&err, f.Close)()` **不会覆盖已有的主错误**——这是旧版 `defer f.Close()` 模式最常见的「真错误被 Close 错误吃掉」问题的解法。`monitor/stats.go` 与 `gensyncmap` 分别服务 §3.9.2 与 §3.9.4。

#### 3.11.3 工具、目录与 `releng/`/`scripts/` 删除

| 对象 | v1.8.10 | v1.12.3 | 变化说明 |
|---|---|---|---|
| `stress/` | 63 个文件（含 `stress/v2` 全套） | 0 | 提交 `6b8ec8cfe0`（#20892）删除 `influx_stress` 工具 |
| `influx_tools` 源码 | `cmd/influx_tools/*` | 仍在 | 但不再出制品（§3.4.1） |
| 根 `Makefile` | 不存在 | 4 行 | 只是 Flux 构建 shim（`export PKG_CONFIG=./pkg-config.sh` + `go build ./...`），不是完整构建入口 |
| 根 `Dockerfile` | `golang:1.13.8` + `dep ensure` | `golang:1.15.10` + `dep ensure` | 仍是死代码，不能用于 1.12.3 构建 |
| `appveyor.yml` | `GOROOT=C:\go112` + `dep` | 逐字未变 | 死代码；Windows 构建走 CircleCI `xcc windows` |
| `releng/` | 16 个文件：`packages/`、`raw-binaries/`、`source-tarball/`、`unit-tests/`、`_go_versions.sh` 等 | 5 个文件：`get-version`、`protoc-gen`、`packages/spec/clean_install/*`、`source-tarball/...` | `raw-binaries`、`unit-tests` 整套删除；`packages` 只剩 clean_install spec；新增 `get-version`、`protoc-gen` |
| `scripts/ci/` | 7 个文件：`perf_test.sh`、`run_perftest.sh`、`main.tf`、`terraform.tfvars`、`tests/*` | 2 个文件：`CHANGELOG_frozen.md`、`install-rust.sh` | 性能/安装测试基础设施随 1.8 线一起不在这条线上（§3.10.5） |
| `releng/get-version` | 不存在 | 存在但无调用点 | 见 §3.7.1；属于“未接线/可删除”代码 |
| `releng/source-tarball/fs/usr/local/bin/influxdb_tarball.bash` | 存在整套 Dockerfile/build.bash | 只剩脚本 | 构建链已由 `.circleci/config.yml` 的源码包步骤取代 |

`influx_tools`/`influx_stress` 的下游影响见 §3.4.1。死代码清单除 `Dockerfile`、`appveyor.yml` 外，还应加上 `releng/get-version`（若确认不接线）和 `releng/source-tarball` 的残留脚本（若不再被引用）。

## 4. 优化与收益

> **口径**：原 §4.1 的 18 行平铺表把“纯性能优化、正确性修复、v1.12 新增能力、持久化增强”混在一起。本节按子系统拆成 5 组纯优化，另设“非优化项”单列 7 行；每组给机制、commit、影响面、可观测指标。**没有基准数字的地方明确标“无基准”**，不把 commit message 里的局部观测当成跨版本收益。

### 4.1 分组、判定标准与总览

判定一条变更是否算“相对 v1.8.10 的性能优化”（D2-16 要求的选择标准）：

1. **稳态路径工作量**：是否减少每次写入/查询/规划/删除的 CPU、内存分配、IO 次数或锁持有时间。
2. **可观测指标**：是否有队列、计数器、tracing span、benchmark 或可复现的调用次数可以验证。
3. **相对基线有净变化**：v1.8.10 已经具备的机制不算；v1.12 新增能力即使内部很快，也不是“相对 v1.8.10 的优化”。
4. **不是修复/持久化**：修 bug、加持久化保障、加资源配额单独归类。

| 组 | 小节 | 原表行 | 主要机制 | 可观测指标 |
|---|---|---|---|---|
| compaction / TSM | §4.2 | #1、#2、#3、#11（#10 交叉） | 文件名缓存、首块缓存、分组结构、purger 空转短路 | `tsmCompactionsQueue`、`tsmOptimizeCompactions*`、planner CPU、shard 目录大小 |
| TSI / index | §4.3 | #8、#9 | cache key 字符串复用、xxhash v2 `Sum64String` | `go test -bench` 的 allocs/op、heap profile |
| query / storage-read | §4.4 | #12、#16 | array cursor 条件修正、`_fieldKeys` 系统迭代器 | 结果正确性、series 扫描量、查询延迟 |
| cache / lock / allocation / IO | §4.5 | #10、#15、#17（#8/#9 交叉） | purger 缩锁、`EqualFold`、tar 缓冲 | mutex profile、allocs/op、syscall 次数 |
| build / CI | §4.6 | 新增组 | cache v2、去嵌套 Docker、目标收窄 | CI wall time、cache hit、产物数 |
| **非优化项** | §4.7 | #4–#7、#13、#14、#18 | 修复、新功能、持久化 | 正确性/功能/持久化验证 |

> 原表另有两行在 v1.8.10 中**已经是现在的代码**，对 v1.8.10→v1.12.3 无净变化，故不列入任何组：“单 shard 组的 meta 查询”（`5d922e9d0e`，2020-11；`v1.8.10:services/meta/data.go:1370-1375` 与 `v1.12.3:services/meta/data.go:1480-1485` 逐字相同）与“tag index 缓冲按需增长”（`f646cbc540`，2021-04；`v1.8.10:models/points.go:650-660` 已有 grow 分支）。

### 4.2 compaction / TSM

| # | 优化点 | 旧（v1.8.10） | 新（v1.12.3） | 机制 | commit |
|---|---|---|---|---|---|
| 1 | TSM 文件名解析 | `findGenerations()` 在文件集变化时对每个文件 `ParseFileName(f.Path)`；`LastModified` 未变则 `:620-625` 早退，但缓存世代的 `level()` 每次都重新解析 `files[0]`，1s tick 的 `PlanLevel`/`Plan`/`PlanOptimize` 会反复调用 | `TSMReader` 在创建/重命名时各解析一次，缓存到 `TSMReader.generation/sequence`，经 `Stats()` 暴露为 `FileStat`；`level()` 直接读 | 规划期解析从「每世代每次 `level()` 一次」降为 0（仅创建/重命名时各一次） | `f0d5c73253` (#26905) |
| 2 | compaction planner 的 CPU 占用 | 仅在世代 >2GB 时调用 `BlockCount(files[0],1)` 读盘解码（阈值 1000；v1.8.10 没有 aggressive 概念） | 首次 `ExtStats()` 读盘一次，并把 `count` 与 `err` 一起以 `valid=true` 缓存（失败不重试）；失败时 `FirstBlockCount=0`，`FileStore.Stats()` 记 warning，planner 按「未优化」处理 | 「每次规划都解压超大世代首块」变成「每个 reader 首次 `ExtStats()` 时一次」；`FileStore.Stats()` 另有 `lastFileStats` 二级缓存 | `2a0353ffd2` (#26432) |
| 3 | compaction 分组结构 | `tsmGeneration` 持有 `parseFileName` 闭包 | 结构体减为 `{id, files}` | 去掉间接调用，planner 更易推理 | `f0d5c73253` |
| 11 | purger 空转 | `add(nil)` 也会拿锁、起一个空转 goroutine（v1.8.10 无 `added` 日志） | 开头 `if len(files) == 0 { return }` | 降低无意义操作；日志噪声是 1.12 线 `purge()` 重构（`logger.Info("added", ...)`）新增的，不是旧版成本 | `e883e8bec3` (#26762) |

**影响面**：compaction 规划期 CPU、锁持有时间与超大世代首块的解码次数；不改变 TSM 文件格式。

> #10（purger 锁）以“锁竞争”为主，归入 §4.5；这里不重复计分。

**4.2.1 为什么「缓存文件名解析」值得单独一个 PR？**

`DefaultPlanner.findGenerations()` 在每个 shard 的 compaction tick 里被调用。旧版实现：

```go
// v1.8.10:tsdb/engine/tsm1/compact.go:616-644（节选）
	last := c.lastFindGenerations
	lastGen := c.lastGenerations
	if !last.IsZero() && c.FileStore.LastModified().Equal(last) {
		return lastGen // 文件集未变：一个文件名都不解析
	}
	for _, f := range tsmStats {
		gen, _, _ := c.ParseFileName(f.Path)
		...
		group := generations[gen]
```

`ParseFileName` 要做 `filepath.Base` + 两次 `strings.Index`（先 `.` 后 `-`）+ 两次 `strconv.ParseUint(..., 10, 32)`。旧版有两条解析路径：`findGenerations` 本身在 `LastModified` 未变时短路（`:620-625`），所以文件集稳定时不会重新遍历 N 个文件；但缓存下来的 `tsmGeneration` 仍持有 `parseFileName` 闭包，`level()` 每次调用都重新解析 `t.files[0].Path`（`:177-188`）。1s tick 的 `PlanLevel`/`Plan`/`PlanOptimize` 会反复调用 `level()`（`v1.8.10:tsdb/engine/tsm1/compact.go:254-286,494,549`；`tsmGenerations.level()` 还会遍历组内每个世代），因此旧版规划期仍在按「世代数 × 调用次数」持续解析，而不是只在文件集变化那一拍付钱。新版把结果挂到 `FileStat`：

```go
// v1.12.3:tsdb/engine/tsm1/file_store.go:236-246
type FileStat struct {
	Path             string
	HasTombstone     bool
	Size             uint32
	LastModified     int64
	MinTime, MaxTime int64
	MinKey, MaxKey   []byte
	Generation       int
	Sequence         int
}
```

```go
// v1.12.3:tsdb/engine/tsm1/reader.go:450-460
func (t *TSMReader) parseAndCacheFileName(path string) error {
	if t.parseFileNameFunc != nil {
		var err error
		t.generation, t.sequence, err = t.parseFileNameFunc(path)
		return err
	} else {
		// If parseFileNameFunc is nil, we are in a test or another TSMReader use
		// that does not involve compaction planning, and was not created by the FileStore
		return nil
	}
}
```

新版 `findGenerations` 直接从 `FileStore.Stats()` 的 `ExtFileStat.Generation` 分组，`tsmGeneration.level()` 读 `files[0].Sequence`（`v1.12.3:tsdb/engine/tsm1/compact.go:219-229,700-724`），不再调用 `ParseFileName`。调用次数对比见 §6.5。

**副作用（重要）**：`parseFileNameFunc == nil` 时 `generation/sequence` 保持零值。如果某个测试或工具直接 `NewTSMReader()` 而没传 `WithParseFileNameFunc`，得到的 `FileStat.Generation` 全是 `0`。为此 `fileStore` 接口新增了 `SupportsCompactionPlanning()`，并在 `NewDefaultPlanner` 里直接 panic：

```go
// v1.12.3:tsdb/engine/tsm1/compact.go:182-186
func NewDefaultPlanner(fs fileStore, writeColdDuration time.Duration) *DefaultPlanner {
	if !fs.SupportsCompactionPlanning() {
		// This should only happen due to developer mistakes.
		panic("fileStore must support compaction planning")
	}
```

`cmd/influx_tools/compact/command.go:153-155` 因此显式返回 `false`。

**4.2.2 `FirstBlockCount` 缓存的代价与收益**

```go
// v1.12.3:tsdb/engine/tsm1/reader.go:31-35
type blockCountCache struct {
	valid bool
	count int
	err   error
}
```

v1.8.10 **没有** aggressive points-per-block 判定；它只用 `BlockCount(files[0],1)` 判断“>2GB 且满块则跳过”，阈值是 `DefaultMaxPointsPerBlock`(1000)、谓词是 `==`；新版各规划点改为 `FirstBlockCount >= DefaultMaxPointsPerBlock`（`compact.go:513,592,643`），这是 10000 点的 aggressive 块也能被识别为满块的前提。首块缓存（`2a0353ffd2` #26432，v1.12.1）与 aggressive 判定（`f04105bede` #25594 在 v1.12.0 引入，默认值由 `d8bcbd894c` #26100 调为 10000）都是 v1.12 线新增的机制，不是 v1.8.10 的行为。新版把这个值缓存在 `TSMReader` 上，通过 `ExtStats()` 暴露：

```go
// v1.12.3:tsdb/engine/tsm1/reader.go:687-698
func (t *TSMReader) ExtStats() (ExtFileStat, error) {
	extStats := t.Stats().ToExtFileStat()

	var errs []error
	if firstBlockCount, err := t.firstBlockCount(); err == nil {
		extStats.FirstBlockCount = firstBlockCount
	} else {
		errs = append(errs, err)
	}

	return extStats, errors.Join(errs...)
}
```

`errors.Join` 是 Go 1.20 引入的——这也解释了为什么这个改动必须建立在 Go ≥ 1.20 的前提上。缓存语义是：首次调用 `firstBlockCount()` 时读盘一次，并把 `count` 与 `err` 一起以 `valid=true` 写入缓存，之后不重试、也没有回退路径；失败时 `ExtStats()` 返回 joined error，`FileStore.Stats()` 只 `logger.Warn`，该文件的 `FirstBlockCount` 保持 0，planner 因而不跳过 >2GB 世代（`compact.go:592,643`），`isFileOptimized` 也判为未优化。`FileStore.Stats()` 自身另有 `lastFileStats` 缓存，在 `replace`、`Apply`、`DeleteRange`、`Close` 时置 nil，下次 `Stats()` 惰性重建。代价是 `Stats()` 对每个文件调用 `ExtStats()`，每个 reader 首次 `Stats()` 都会解一次首块（旧版仅 >2GB 世代）；收益是这些解码只发生一次，而非每次规划。

**可观测指标**：`tsmCompactionsQueue`（1–3）、`tsmOptimizeCompactionsQueue`、`tsmFullCompactions*`（§3.9 与 04 篇）；planner CPU 没有专门指标，需要 pprof。**无基准**：仓库没有 planner tick 的 benchmark，§6.5 只给调用点推导的下界。

### 4.3 TSI / index

| # | 优化点 | 旧（v1.8.10） | 新（v1.12.3） | 机制 | commit |
|---|---|---|---|---|---|
| 8 | TSI 缓存 key 分配 | `Put()` 内 `string(name)`/`string(key)`/`string(value)` 共做 3~6 次转换（用作 map 查找时可被编译器优化为零分配） | 一次性转换并在 evictor 元素与 map key 间共享 | 插入路径少 1~3 次分配；`exists` 命中早退时新版反多 3 次 | `f3d5cb68ab` (#27255) |
| 9 | 缓存分区哈希 | `xxhash v1`，key 以 `[]byte` 传递 | `xxhash/v2`（汇编实现）；TSI 的 `index.go` 仍调 `Sum64(key)`，只有 `tsm1.ring` 走 `Sum64String`，所以 TSI 侧收益来自 v1→v2 的实现替换，不是消除 `string→[]byte` | commit message 原文：观测到 "converting string keys to byte slices taking between 3-5% of cpu usage"；该观测对应 ring/key 的字符串路径 | `45a8227ad6` (#578/#25622)；`tsdb/index/tsi1/index.go:378,383`、`tsdb/engine/tsm1/ring.go:80,86` |

**影响面**：TSI series/tag cache 的插入与查询分配、哈希 CPU；不改变 TSI 文件格式。

**机制**：`TagValueSeriesIDCache.Put` 在旧版对同一个 key 反复 `string([]byte)`；新版先在 `nameStr/keyStr/valueStr` 三个局部变量里转换一次，evictor 元素与 `map[string]...` 共用同一份字符串（`v1.12.3:tsdb/index/tsi1/cache.go:98-135`）。`Get` 路径没有同样优化，仍每次转换 3 次；所以收益只在**写入/插入**侧，查询侧的分配压力还在（05/02 篇的风险口径一致）。

**#9 的边界**：`xxhash/v2` 不是只服务 TSI；它同时被 `tsm1` cache ring、`series_file`、`bloom`、`hll`、`rhh` 使用。放在 TSI 组是因为同一提交改了 TSI 的 `index.go` 哈希调用点；但 `Sum64String` 只在 `tsm1.ring` 上，字符串路径的主要收益不在 TSI。其余使用点见 §4.5 的 allocation/IO 交叉。

**可观测指标**：`go test -run '^$' -bench 'BenchmarkLogFile_AddSeries_100000_1_1\|BenchmarkIndex_CreateSeriesListIfNotExists\|BenchmarkIndex_ConcurrentWriteQuery' -benchmem ./tsdb/index/tsi1`（benchmark 存在，CI 未跑）；heap profile 看 `tsi1.(*TagValueSeriesIDCache).Put`。**无基准**：仓库没有覆盖 `Put` 的 microbenchmark，也没有跨版本 allocs/op 记录。§6.10 给出 `testing.AllocsPerRun` 计数 harness 与按逃逸分析推导的预期（**机制推演，非基准**）：同一 tuple 的 `exists` 命中路径旧版 0 alloc、新版 3 alloc（3 次 `string()` 先于 `exists` 执行且逃逸）；插入路径新版共享这 3 份字符串，旧版除元素字段 3 次外还要为 map key 再转换，按分支多 1–3 次。

### 4.4 query / storage-read

| # | 优化点 | 旧（v1.8.10） | 新（v1.12.3） | 机制 | commit |
|---|---|---|---|---|---|
| 12 | array cursor 优化判定 | `pos == 0 && len(res) >= len(tvals)` | 增加 `c.tsm.pos == 0` 前置条件 | 修复「整块直出」优化在错误位置触发、cache+TSM 混合时重复拷贝点（时间戳重复，不缺失） | `31cb61f556` (#26691) |
| 16 | `SHOW FIELD KEY CARDINALITY` | 走通用路径 | 走 `_fieldKeys` 系统迭代器 | 避免全量 series 扫描 | `f5da0f50f4` (#23871) |

**影响面**：查询结果正确性、storage read 的重复拷贝、`SHOW FIELD KEY CARDINALITY` 的扫描量；不改变 HTTP API。

**4.4.1 array cursor 的条件修正**

```go
// v1.8.10:tsdb/engine/tsm1/array_cursor.gen.go.tmpl:107
			if pos == 0 && len(c.res.Timestamps) >= len(tvals.Timestamps){
```

```go
// v1.12.3:tsdb/engine/tsm1/array_cursor.gen.go.tmpl:112
			if pos == 0 && c.tsm.pos == 0 && len(c.res.Timestamps) >= len(tvals.Timestamps){
```

这是「优化条件过宽」的典型：原意是「整个 block 都落在缓冲窗口内，可以跳过归并直接吐 TSM 数据」，但 `pos == 0` 只说明结果侧在起点，不说明 TSM 侧也在起点。`c.tsm.pos == 0` 补上了另一半前提。配套新增 `array_cursor_optimization_bug_test.go`（166 行）专门盯这个场景。它更接近“修复一个错误的优化”，放在 query 组是因为收益体现在查询结果与重复拷贝，而不是压缩/索引。

**#16 机制**：`SHOW FIELD KEY CARDINALITY` 在旧版走通用 series 扫描；新版改写为 `_fieldKeys` 系统迭代器（`f5da0f50f4`），避免全量 `_series` 扫描。可观测指标是 `EXPLAIN` 的 iterator 树和查询耗时；没有专门的 benchmark。

### 4.5 cache / lock / allocation / IO

| # | 优化点 | 旧（v1.8.10） | 新（v1.12.3） | 机制 | commit |
|---|---|---|---|---|---|
| 10 | purger 锁竞争 | `sync.RWMutex`，扫描+Close+Remove 全程持锁 | `gensyncmap.Map` + `mu` 只保护 `running` 标志 | 删除路径不再阻塞 `FileStore` 主锁 | `d5a48e0662` (#27146) |
| 15 | 大小写不敏感比较 | `strings.ToLower(a) == strings.ToLower(b)` | `strings.EqualFold(a, b)` | 消除两侧临时字符串分配；但 `ExistsIgnoreCase` 在 OSS 树内只被同文件的 `Union`（导出函数）调用，`Union` 在 OSS 树内没有生产调用点，所以这是库函数清理，不是服务端热路径优化（§6.10 B） | `2529799abb` (#18147)；`pkg/slices/strings.go:6-40` |
| 17 | 备份 tar 拷贝窗口 | `io.CopyN`（`LimitReader` → 32KB 窗口） | `Stream(..., bufSize, ...)` 可配置，默认 1MB | **当前 Go 版本下没有减少 write 次数**：源是 `*os.File`，它实现 `io.WriterTo`，`io.CopyBuffer` 会忽略传入 buffer；10MiB 文件 `bufSize=0` 与 `1MB` 都是 326 次 `Write`（§6.12） | `bac5cedb3c` (#26964)；`pkg/tar/stream.go:67-103` |

**影响面**：删除/备份路径的锁竞争、`write` syscall 次数、临时字符串分配；不改变磁盘格式。

> #8/#9 也属于 allocation/哈希组；为避免重复，主表放在 §4.3，这里只交叉引用。

**4.5.1 purger 的「锁外做重活」**

旧版：

```go
// v1.8.10:tsdb/engine/tsm1/file_store.go:1385-1392
func (p *purger) add(files []TSMFile) {
	p.mu.Lock()
	for _, f := range files {
		p.files[f.Path()] = f
	}
	p.mu.Unlock()
	p.purge()
}
```

新版：

```go
// v1.12.3:tsdb/engine/tsm1/file_store.go:1607-1620
func (p *purger) add(files []TSMFile) {
	if len(files) == 0 {
		return
	}

	var fileNames []string
	for _, f := range files {
		fileName := f.Path()
		fileNames = append(fileNames, fileName)
		p.files.Store(fileName, f)
	}

	p.purge(fileNames)
}
```

关键在 `p.files.Store`（`sync.Map` 语义，无全局锁）与 `purge()` 里 `p.mu` 只保护 `running` 标志、goroutine 内部用 `hasFiles()` 在持锁状态下决定是否退出：

```go
// v1.12.3:tsdb/engine/tsm1/file_store.go:1685-1698
func (p *purger) hasFiles() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	has := false

	// Avoid calling Len() which iterates over the whole map.
	p.files.Range(func(k string, v TSMFile) bool {
		has = true
		return false // stop iteration after finding the first file
	})
	p.running = has
	return has
}
```

注释里「避免调用 `Len()`（会遍历整个 map）」是另一半优化：`sync.Map` 没有 O(1) 的 `Len()`，所以用「找到第一个就停」的 `Range` 代替。两个分支也要注意：`InUse` 的文件留在 map 里，goroutine 每秒重试直到引用释放；`Close`/`Remove` 失败的文件也会 `Delete(k)`，不再重试（可能留下 `.tsm.tmp` 直到下次启动清理），见 [04 存储与 Compaction 差异](04_storage_compaction_diff.md) §6.5。

**可观测指标**：mutex profile（`go test -mutexprofile`，需要压测场景）、`go test -bench` 的 allocs/op、备份时 `strace -c` 的 `write` 次数。**无基准**：没有跨版本 purger/tar 的 benchmark；§6.11/§6.12 给的是计数/时序 harness。**#17 的实测结论**：`StreamRenameFile` 的源是 `*os.File`，它自 Go 1.22 起实现 `io.WriterTo`（Go API `go1.22.txt` 的 `pkg os, method (*File) WriteTo`），`io.CopyBuffer` 因而不使用传入的 1MB buffer；对 10MiB 普通文件，`bufSize=0`（回落默认 1MB）与 `bufSize=1MB` 都是 326 次 `Write`（含 header/trailer），与旧版 `io.CopyN` 的 32KB 路径相同。该 PR 把 buffer 做成可配并加了参数，但在当前 Go 版本下不会减少备份写次数，反而每个文件仍 `make([]byte, 1MB)`（`stream.go:71-73`）。

### 4.6 build / CI

原 §4 完全没有 build/CI 组；这一组只列机制，不给耗时数字。

| 机制 | 旧 | 新 | 证据 | 影响的指标 |
|---|---|---|---|---|
| 构建缓存 | 无跨 run 的 Go module/build 缓存；每次在容器内 `go mod download` + 编译 | `restore_cache/save_cache` 键 `influxdb-cache-v2-{{ checksum "go.mod" }}`，保存 `/go/pkg/mod`、`/root/.cargo`、`/root/.cache/go-build` | `.circleci/config.yml:24-28,186-191`；提交 `6a3b02cac4` | 首次/命中 run 的 wall time、`go mod download` 网络流量 |
| 去嵌套 Docker | `test.sh` 每个测试容器先 `docker build -f Dockerfile_build_*`，再在容器内跑 `build.py` | job 直接跑在 `cross-builder` 镜像里 | `v1.8.10:test.sh:56-93`；`.circleci/config.yml:17-19` | 镜像构建时间、Docker layer cache 依赖 |
| 目标收窄 | linux 7 种架构 + darwin/windows = 9 个构建组合；`build.py --platform all --arch all` 时 9 × 5 target = **45 次 `go build`**，`test.sh 0` 只走宿主 linux/amd64 的 **5 次** | linux amd64/arm64 + darwin amd64 + windows amd64 = 4 个；4 × 3 command = **12 次 `go build`** | `v1.8.10:build.py:89-101,813-848`；`.circleci/config.yml:149-179` | 编译次数、归档数（§6.13） |
| man page 预生成 | 每次打包在容器里 `make -C man install` | `build_binaries` 与 `build_packages` 各预生成一次 | `.circleci/config.yml:44-46,203-206` | 重复工作（2 次）；相对编译的 wall time 占比未测 |
| Rust 缓存 | 无 Rust 步骤 | cache 保存 `/root/.cargo` | `.circleci/config.yml:190` | 首次/命中 run 的 Rust 依赖编译 |

**可观测指标**：CircleCI 的 job wall time（仓库不记录）、cache hit/miss 日志、`artifacts/` 文件数（4 个 tar + 8 个校验和）、`bins/` 目录。**无基准**：没有跨版本构建耗时记录；不能声称“cache v2 让构建快了 X%”。**可复核的计数**见 §6.13：旧版全矩阵 45 次、旧 CI 路径 5 次、新版 12 次（不是 27 次：旧 `build.py` 的 `targets` 有 5 个，不是 3 个）。

### 4.7 非优化项单列（修复 / 新功能 / 持久化）

| # | 项目 | 性质 | 为什么不是“相对 v1.8.10 的性能优化” | commit |
|---|---|---|---|---|
| 4 | compaction 级别回退 | **正确性修复** | 旧版会把 L4 写回 L2，导致重复压缩/写放大；新版保证输出 level ≥ 输入最高 level（`compact.go:976-1011`）。**仅当旧版会把 L4 降为 L2 时**，新版单位文件承载的数据量更多、read/write 字节随输入总字节增长；不是所有输入都更大，也无基准 | `5e9d85c972` (#27233) |
| 5 | 低级别文件嵌套 | **正确性修复**（1.12 线回归） | 修复 planner 对“低级别文件出现在高级别之后”的处理；v1.8.10 由 `PlanOptimize` 兜底，不是旧版缺陷 | `d1d68d656d` (#26911) |
| 6 | full compaction 不被调度 | **正确性修复**（1.12.1 回归） | 1.12.1 的 `PT_SmartOptimize`/`PT_NoOptimize` 特例把文件标成 in-use 却不释放，导致磁盘无界增长；删特例是修复，不是优化 | `95e86be3f4` (#26670) |
| 7 | optimize compaction holdoff | **正确性修复**（开发期 bug） | `StartOptHoldOff` 按值传参导致 holdoff 永不生效；该 bug 未进入任何 release，修复后 L5 才会真正被 holdoff | `0fdb6adcb2` (#26488) |
| 13 | 查询并发/内存上限 | **v1.12 新功能** | 旧版 `flux/control` 是无配额的薄壳；新版 `query/control` 增加队列/并发/内存配额。默认 0 仍不限制，只有显式配置才生效；相对 v1.8.10 是“新增能力”，不是“更快” | `b9ee9e5b44`、`f1aa051787` |
| 14 | 写窗口检查 | **v1.12 新功能** | v1.8.10 没有 RP 写入窗口；写窗口是 v1.12.0 新增（`07c261a21a`），`19f65f50b7` 只是把每点两次 `time.Now()` 降为每批一次。相对 v1.8.10 没有该功能，不能算优化 | `19f65f50b7` (#25558) |
| 18 | retention 删除的 fsync | **持久化增强** | v1.8.10 删除 series 只 `Flush()` 缓冲、不 `file.Sync()`（0 次 fsync）；新版按 partition `FlushSegments`，是 0 → ≤ `SeriesFilePartitionN` 次 fsync，删除延迟可能变大。只有相对 v1.10.0–v1.12.2“每条 series 一次 fsync”的中间版本才是减少 | `b4e781eff6`、`1420aaf2bc` (#27114) |

**#13 的默认行为**：`query-concurrency=0` 时 `queryQueue=nil`、内存配额取 `MaxInt64`；`query-initial-memory-bytes`/`query-max-memory-bytes`/`total-max-memory-bytes`/`query-queue-size` 只在显式设置后生效，且只作用于 `[flux-controller]` 的 Flux 查询。**#14 的默认行为**：不设置 `FUTURE LIMIT`/`PAST LIMIT` 时窗口检查不启用。**#18 的证据**：`v1.8.10:tsdb/series_segment.go:210-216` 只有 `bufio.Flush`，`v1.12.3:tsdb/series_segment.go:218-227` 才调用 `file.Sync`；`DeleteSeriesID(id, NoFlush)` + `FlushSegments` 的调用点在 `services/retention/service.go:254` → `tsdb/store.go:1083-1114`。

### 4.8 测量方法与缺失基准

**结论先行**：v1.12.3 **没有性能回归基础设施**；§4 的所有收益都是机制层面的，不能给出“提升 X%”。旧版的测量方法见 §3.10.5（perf_test/AWS/S3），新版没有替代。

可用的测量手段与缺口：

| 组 | 可运行的方法 | 现有覆盖 | 缺口 |
|---|---|---|---|
| compaction/TSM | 自写 Go benchmark 包住 `PlanCompactions`，固定 100 个 generation，统计 `level()`/`ParseFileName` 调用次数；或 pprof CPU | 仓库无 planner benchmark | 没有跨版本调用次数/耗时记录；§6.5 只给调用点推导下界 |
| TSI/index | `go test -run '^$' -bench 'BenchmarkLogFile_AddSeries_100000_1_1\|BenchmarkIndex_CreateSeriesListIfNotExists\|BenchmarkIndex_ConcurrentWriteQuery' -benchmem ./tsdb/index/tsi1` | 有 benchmark，CI 不跑 | 没有专门覆盖 `TagValueSeriesIDCache.Put` 的 microbenchmark |
| query/storage-read | `go test -run '^$' -bench 'BenchmarkSelect_Raw\|BenchmarkSelect_Top' -benchmem ./query`；`EXPLAIN` 看 iterator 树 | 有 query benchmark，CI 不跑 | array cursor/`_fieldKeys` 没有对应 benchmark |
| cache/lock/IO | `go test -mutexprofile` + 自建并发删除/备份场景；`strace -c` 看 syscall | 无 purger/tar benchmark | 没有锁竞争或 syscall 的跨版本数据 |
| build/CI | CircleCI job wall time、cache hit 日志、`artifacts/` 文件数 | 有流水线，不记录基准 | 没有构建耗时基线 |
| 端到端 | 旧 `perf_test` 的 `influxdb-comparisons` 流程 | 仅 1.8 线存在 | v1.12.3 无替代（§3.10.5） |

**为什么不能给数字**：

1. `grep -n 'bench' .circleci/config.yml` 无命中；CI 从不执行 benchmark。
2. commit message 只有局部观测（例如 `45a8227ad6` 说 string→[]byte 占 3–5% CPU），不是 v1.8.10→v1.12.3 的端到端对比。
3. 旧 perf_test 的硬件（r5.2xlarge）、数据集（scale_var=1000、500 queries、30s）、telegraf 上报都在 1.8 线（一次运行的完整输入/产物见 §6.14）；直接拿它的历史结果与 1.12.3 对比会混入硬件/数据/查询集变化。
4. §6.5 的“16241 次 `level()` 落点 vs 0 次解析”是**按源码调用点推导的下界**，不是实测耗时；它只能证明工作量方向，不能证明墙钟收益。16241 的构成见 §6.5 的表：旧版 `tsmGenerations.level()` 本身会遍历组内每个世代，所以是二次增长；此前写的 797 漏算了这一层。

如果要把 §4 从“机制清单”升级成“收益报告”，最小闭环是：恢复一个可在 CI 跑的 benchmark job（或恢复 perf_test 的容器化版本）、固定数据集与硬件、把结果写入可比的时间序列、给每组设阈值。这属于后续工作，不在本次文档修复范围内。

## 6. 具体案例

### 6.10 案例：TSI cache `Put` 的分配计数（机制推演，非基准）

**输入**：`TagValueSeriesIDCache` 容量 10；同一 tuple `name=[]byte("cpu")`、`key=[]byte("host")`、`value=[]byte("A")`；先用 `ss=tsdb.NewSeriesIDSet(1)` 插入一次，使后续 `Put` 走 `exists` 命中早退。

**harness**（`tsdb/index/tsi1` 包内测试，保存为 `cache_doc_test.go`；已补全 `package`/`testing`/`tsdb` 导入，但本机 tsi1 包因 Flux 构建失败，无法在这里验证编译/运行，见文末）：

```go
package tsi1

import (
	"testing"

	"github.com/influxdata/influxdb/tsdb"
)

func TestDocCachePutAllocs(t *testing.T) {
	c := NewTagValueSeriesIDCache(10)
	name, key, value := []byte("cpu"), []byte("host"), []byte("A")
	c.Put(name, key, value, tsdb.NewSeriesIDSet(1))
	t.Logf("hit allocs/op=%v", testing.AllocsPerRun(1000, func() {
		c.Put(name, key, value, nil)
	}))
}
```

**预期输出（按逃逸分析推导；下表是推导值，不是实测基准）**：

| 路径 | OLD `v1.8.10` | NEW `v1.12.3` | 依据 |
|---|---:|---:|---|
| `exists` 命中 | **0 allocs/op** | **3 allocs/op** | OLD 的 `string([]byte)` 只作 map 索引，编译器可零分配；NEW 在 `exists` 前先做 3 次 `string()`，并逃逸到 `seriesIDCacheElement`/map key（`cache.go:102-104`） |
| 插入（新 tuple） | **4–6 allocs/op** | **3 allocs/op** | NEW 共享 3 份字符串；OLD 元素字段 3 次，map key 按分支再 1–3 次（`v1.8.10:tsdb/index/tsi1/cache.go:112-140` vs `v1.12.3:tsdb/index/tsi1/cache.go:117-145`） |

**结论**：这是“插入省、命中亏”的交换，不要描述成“全面减少分配”。命令：`go test ./tsdb/index/tsi1 -run '^TestDocCachePutAllocs$' -count=1 -v`。**本文不填 ns/op**；tsi1 在本机因 `github.com/influxdata/flux` 的 `libflux.Options` 构建失败，所以上表只给推导值，不给实测值。

**B. `EqualFold`**：

- 输入：`set=[]string{"aBc"}`、`find="AbC"`（大小写混合，保证 `ToLower` 两侧都要分配）。
- harness（`pkg/slices` 包内测试，保存为 `strings_doc_test.go`；可独立编译运行）：

  ```go
  package slices

  import "testing"

  func TestDocExistsIgnoreCaseAllocs(t *testing.T) {
      set := []string{"aBc"}
      t.Logf("allocs/op=%v", testing.AllocsPerRun(1000, func() {
          ExistsIgnoreCase(set, "AbC")
      }))
  }
  ```
- 预期（推导）：OLD **2 allocs/op**（`ToLower(find)` 1 次 + 匹配项的 `ToLower(s)` 1 次）；NEW **0**（`EqualFold` 不分配）。依据 `pkg/slices/strings.go:33-40` 与 `v1.8.10:pkg/slices/strings.go:33-41`。
- 边界：`ExistsIgnoreCase` 只被同文件的 `Union`（导出函数）调用，`Union` 在 OSS 树内没有生产调用点（`grep -rn 'slices.Union' --include='*.go'` 无结果）；这是库函数清理，不是服务端热路径收益。

### 6.11 案例：purger 的锁等待（机制推演，非基准）

**输入**：100 个 fake `TSMFile`。仓库现有的 `mockPurgerTSMFile` 是**每个文件独立**的 `closeCh`/`removeCh`，`Close()` 不 sleep，也没有 `entered`（`tsdb/engine/tsm1/purger_test.go:16-53`）；下面的 `slowPurgerFile` 是在它基础上改的最小示意（**不是仓库现有 mock**）。`entered` 必须每个文件一个：若 100 个 `Close()` 共享同一个 channel，第二次 `close` 即 panic。全部 `InUse=false`；`newTestPurger()`。

```go
// tsdb/engine/tsm1/purger_doc_test.go — 示意，需自行加入 `_test.go` 才能运行。
// NEW：复用 purger_test.go 的 mockPurgerTSMFile / newTestPurger。
// OLD：v1.8.10 没有 mockPurgerTSMFile / newTestPurger；需等价 mock，
//      并用 &purger{files: map[string]TSMFile{}, logger: zap.NewNop()} 构造。
package tsm1

import (
	"fmt"
	"testing"
	"time"
)

type slowPurgerFile struct {
	*mockPurgerTSMFile
	entered chan struct{} // 每文件一个：共享同一个 channel 时第二次 close 会 panic
}

func newSlowPurgerFile(path string) *slowPurgerFile {
	return &slowPurgerFile{
		mockPurgerTSMFile: newMockPurgerTSMFile(path, false),
		entered:           make(chan struct{}),
	}
}

func (m *slowPurgerFile) Close() error {
	close(m.entered)
	time.Sleep(10 * time.Millisecond)
	return m.mockPurgerTSMFile.Close()
}

func TestDocPurgerAddLatency(t *testing.T) {
	p := newTestPurger() // OLD 用 &purger{files: map[string]TSMFile{}, logger: zap.NewNop()}
	files := make([]TSMFile, 100)
	for i := range files {
		files[i] = newSlowPurgerFile(fmt.Sprintf("a%03d.tsm", i))
	}
	go p.add(files)
	<-files[0].(*slowPurgerFile).entered // 第一个 Close() 已开始

	files2 := make([]TSMFile, 100)
	for i := range files2 {
		files2[i] = newSlowPurgerFile(fmt.Sprintf("b%03d.tsm", i))
	}
	start := time.Now()
	p.add(files2)
	t.Logf("second add elapsed=%v", time.Since(start))
	<-files2[0].(*slowPurgerFile).entered
}
```

**步骤**：

1. `files := make([]TSMFile, 100)`，每项 `newSlowPurgerFile(...)`；`go p.add(files)`。NEW 的 `add` 只入 map 并启动 purge goroutine、立即返回；OLD 的 `add` 会一直阻塞到这一批 purge 完成，所以必须放在 goroutine 里。
2. 等第一个 `<-files[0].(*slowPurgerFile).entered`，确认第一个 `Close()` 已开始，再计时第二批 `files2`（同样构造）的 `add`。计时片段**不能单独运行**：`files`/`files2`/`entered` 的完整定义见上面的 harness，核心只有 `start := time.Now(); p.add(files2); elapsed := time.Since(start)`。
3. 记录 `elapsed`；再等 purge 结束，确认两批文件都被 Close/Remove。

**预期输出（按锁语义推导；下表是推导下界，不是实测值）**：

| 版本 | 第二个 `add` 的阻塞 | 依据 |
|---|---|---|
| OLD | **≥ 99 × 10ms ≈ 0.99s** | `v1.8.10:tsdb/engine/tsm1/file_store.go:1385-1392` 的 `add` 拿 `p.mu`；`:1405-1432` 的 purge goroutine 在扫描/Close/Remove 全程持锁 |
| NEW | **不等待 purge**：只做 100 次 `sync.Map.Store`，`purge` 只短暂拿 `p.mu` 改 `running` | `v1.12.3:tsdb/engine/tsm1/file_store.go:1607-1620,1629-1637`（`gensyncmap.Map` 无全局锁；`hasFiles` 用“找到第一个就停”的 `Range`） |

**边界**：旧版的 0.99s 是 `N × d` 的下界（不含 `Remove` 与调度）；新版的具体微秒数取决于机器，本文不填。命令：`go test ./tsdb/engine/tsm1 -run '^TestDocPurgerAddLatency$' -count=1 -v`；旧版需在 `v1.8.10` worktree 放等价 mock。真实删除负载下可用 `go test -mutexprofile` 复核。

### 6.12 案例：备份 tar 的 `write` 次数（实测复现，非跨版本基准）

**输入**：一个 10 MiB 普通文件；调用 `pkg/tar.StreamRenameFile(fi, "x", "rel", path, tw, bufSize)`，`tw` 写到一个只计数、不落盘的 `io.Writer`。

**harness**（`pkg/tar` 包内临时测试）：

```go
type countWriter struct{ n int }
func (c *countWriter) Write(p []byte) (int, error) { c.n++; return len(p), nil }

for _, bs := range []uint64{0, 1 << 20} {
	f, _ := os.CreateTemp("", "src")
	defer os.Remove(f.Name())
	defer f.Close()
	f.Truncate(10 * 1024 * 1024)
	fi, _ := f.Stat()
	cw := &countWriter{}
	tw := tar.NewWriter(cw)
	err := StreamRenameFile(fi, "x", "rel", f.Name(), tw, bs)
	tw.Close()
	fmt.Printf("bufSize=%d writes=%d err=%v\n", bs, cw.n, err)
}
```

**预期输出（本机 Go 1.26.1 实测）**：

```text
bufSize=0     writes=326
bufSize=1MB   writes=326
```

326 = 320 次 32KiB payload + 6 次 header/trailer/padding。**原因**：`io.CopyBuffer` 在 `src` 实现 `io.WriterTo` 时不使用传入 buffer；`*os.File` 自 Go 1.22 起实现 `io.WriterTo`（Go API `go1.22.txt` 的 `pkg os, method (*File) WriteTo`），所以 1MB buffer 被忽略，回落到 32KB 拷贝。旧版 `io.CopyN` 包了 `LimitReader`（不实现 `WriterTo`），同样走 32KB。**结论**：#17 在本仓库的备份路径上不减少 `write` 次数；`StreamRenameFile` 仍为每个文件分配 1MB（`stream.go:73`）。本文不给跨版本 `strace` 数字，也不声称本机数字等于生产磁盘耗时。

### 6.13 案例：构建目标收窄的调用次数（源码计数，非耗时基准）

**输入**：OLD `v1.8.10` 的 `build.py` / `test.sh 0`；NEW `f3d5cb68ab` 的 `.circleci/config.yml`。

**计数**：

| 路径 | 组合数 | 每组合 target/command | `go build` 次数 | 归档/校验和 | 包 |
|---|---:|---:|---:|---|---:|
| OLD `test.sh 0` | 1（宿主 linux/amd64） | 5 target | **5** | 0 / 0（只打印 MD5） | 3（deb/rpm/tar.gz） |
| OLD 全矩阵 `--platform all --arch all` | 9 | 5 target | **45** | 每平台自写 tar/zip / 0 | **19**（linux 5×3 + static 2×1 + darwin 1 + windows 1） |
| NEW CI | 4 | 3 command | **12** | 4 tar.gz / 8 校验和 | 8（§6.2） |

**预期可核对输出**：

```bash
# NEW
find bins -type f | wc -l                 # 12
ls artifacts/*.tar.gz | wc -l             # 4
ls artifacts/*.md5 artifacts/*.sha256 | wc -l   # 8
# OLD test.sh 0
find build -maxdepth 1 -type f -executable | wc -l   # 5（influx/influxd/influx_inspect/influx_stress/influx_tools）
ls build/*.deb build/*.rpm build/*.tar.gz | wc -l    # 3（非 release 版本号含 ~<sha>）
```

**边界**：调用次数来自源码循环（`build.py:521-585,813-848`；`.circleci/config.yml:64-179`），是确定的；包数量依赖 packager 的格式映射（本文按平台惯例计 8/19），若 packager 改变映射以实际 `ls packages/` 为准。**不填构建耗时**：CircleCI wall time 不在仓库内，不能声称“cache v2 让构建快了 X%”。

### 6.14 案例：旧 `perf_test` 的一次运行（历史基线，不能直接与 1.12.3 比）

**输入**：

- AWS `r5.2xlarge`（8 vCPU / 64 GiB），100 GB EBS，Ubuntu 20.04，`/mnt/ramdisk` 为 32 GB tmpfs（`v1.8.10:scripts/ci/perf_test.sh:16,21,64-73`）。
- 包：`influxdb_*_amd64.deb` + `influx_tools`（`perf_test.sh:51-61`）。
- 环境变量：`TEST_FORMAT=http`、`TEST_RECORD_INGEST_RESULTS=true|false`、`INFLUXDB_VERSION=<branch>`、`TEST_COMMIT=<sha>`、`TEST_COMMIT_TIME=<unix>`、`DATASET_DIR=/mnt/ramdisk`、`NGINX_HOST=localhost`（`perf_test.sh:75-83`）。
- 固定参数：`scale_var=1000`、`queries=500`、`batch=5000`、`workers=4`、`duration=30s`（`run_perftest.sh:125-143`）。

**步骤**：

1. `perf_test.sh` 启动 EC2、装 deb、把 `flux-enabled` 改为 true、`index-version` 改为 `tsi1`、挂 tmpfs，然后 ssh 执行 `run_perftest.sh`。
2. `run_perftest.sh` 安装 `influxdb-comparisons` 的 4 个命令，生成查询文件（`run_perftest.sh:236-252`）。
3. 对 `iot`、`metaquery`、`multi-measurement` 三个数据集依次：`bulk_data_gen` → `bulk_load_influx` → `force_compaction` → `query_benchmarker_influxdb` → 重启 daemon → `DROP DATABASE benchmark_db`（`:254-315`）。
4. 末尾 `telegraf --debug --once` 上报结果（`:317-320`）；退出时 `cleanup` 上传日志。

**预期输出/计数**：

| 项 | 预期 | 依据 |
|---|---|---|
| 查询文件数 | **51** 个（6 个 use case × 7 + iot 5 + metaquery 3 + multi-measurement 1），每个 `queries=500` | `query_types()` `:189-208`、循环 `:236-252` |
| 生成查询总数 | 51 × 500 = **25,500** | 同上 |
| 数据集 | 3 个：iot / metaquery / multi-measurement | `:256` |
| 基数 | 脚本注释：`scale_var × scale_var` = **10^6**（以 `SHOW SERIES CARDINALITY` 为准） | `:130-131` |
| 每个数据集的结果文件 | `test-ingest-<usecase>.json`（仅 `TEST_RECORD_INGEST_RESULTS=true`） | `:271-277` |
| 每个查询的结果文件 | `test-query-<format>-<usecase>-<type>.json`，含 `use_case`/`query_type`/`branch`/`commit`/`time`/`query_format` | `:294-305` |
| 远端日志 | `s3://perftest-logs-influxdb/oss/<branch>/<commit>-<yyyymmddHHMMSS>.log` | `:17-19` |
| 工具指针 | `s3://perftest-binaries-influxdb/influx_tools/latest_<branch>.txt`，内容为 `influx_tools-<sha> <branch> <yyyymmdd>` | `perf_test.sh:54-58` |

**边界**：这是 **1.8 线的历史基线**，硬件、数据集、查询集和 `influx_tools` 都与 1.12.3 不同；不能把它的结果直接当 1.12.3 的对照。CI 没有阈值/回归门禁，结果只进 S3 与 Telegraf。**本文不填 TPS/QPS/延迟数字**：这些值每轮都变，且没有版本对照表。

**证据**：`v1.8.10:scripts/ci/perf_test.sh:16,21,51-83`；`v1.8.10:scripts/ci/run_perftest.sh:17-19,125-143,189-208,236-315`；`v1.8.10:.circleci/config.yml:111-134`。


---

**返回**：[主文档](07_build_perf_diff.md) · [附录 A：构建、CI、打包、发布与分发](07a_build_release_appendix.md)
