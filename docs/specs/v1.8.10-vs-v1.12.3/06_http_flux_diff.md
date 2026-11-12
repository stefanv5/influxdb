# 版本差异 6: HTTP 服务、TLS 与 Flux 入口（v1.8.10 → v1.12.3）

> **版本基线**
>
> - OLD：`v1.8.10` = commit `688e697c51`（2021-10-11）
> - NEW：`v1.12.3` = commit `f3d5cb68ab`（`docs/specs/` 这批文档的代码基准）
> - 区间：527 commits，602 files，+79,310 / −45,239
> - NEW 的当前工作树只额外包含若干 docs 提交（当前触及 `docs/specs/` 的为 8 个、全部 docs 提交为 10 个；数量随文档演进），代码与 `f3d5cb68ab` 相同。
>
> 引用格式：旧 `v1.8.10:path:line`，新 `v1.12.3:path:line`（即当前工作树行号）。
> 新版本行为的详细讲解见 [10 HTTP 服务层](../10_http_service_spec.md)、
> [09 Storage Reads](../09_storage_reads_spec.md)、[11 InfluxQL 执行](../11_influxql_execution_spec.md)。

> **拆分说明**：主文档保留标题、速读、结论、地图、§3 同编号摘要、§4/§5/§7/§8 与 3 个代表案例；§3 的完整原文（含全部表格、代码与证据）及 6.2/6.4/6.5 三个长案例见附录 [06a_details_appendix.md](06a_details_appendix.md)。

> **小白导读**
>
> 1. **前台大厅扩建了**：v1.8.10 的 HTTP 层只有 21 条路由；v1.12.3 变成 39 条，
>    多出来的 18 条几乎全是 InfluxDB 2.x 兼容面（`/api/v2/delete` 和 14 条
>    `/api/v2/buckets*`）以及 3 条 `OPTIONS` 预检。
> 2. **门禁换了新锁**：TLS 不再"启动时读一次证书"，而是引入
>    `TLSConfigManager` + `TLSCertLoader`，支持 SIGHUP 热重载、客户端证书、
>    证书过期巡检；代价是**默认开始检查证书/私钥文件权限**。
> 3. **大厅里多了几块仪表盘**：`/debug/vars` 从"system + cmdline + memstats + 统计"
>    扩展为额外输出 `build`、`crypto`、`config`（TSDB 配置）、`stats`
>    （压缩吞吐占用率）；`cq`（连续查询计数）本已存在，改为扁平输出。
> 4. **写查询的人和查查询的人对上了号**：`SHOW QUERIES` 多一列 `user`，
>    并且非管理员只能看到自己有读权限的库上的查询（修掉一个越权泄漏）。
> 5. **Flux 从"能跑"变成"有预算"**：旧版 Flux 控制器是 `flux/control`
>    里的薄壳，内存不设限；新版换成 `query/control`，有并发配额、单查询内存配额、
>    全局内存池和排队队列，读路径也搬家到 `storage/flux`。
> 6. **不会自动变好的地方**：`/api/v2/write` 仍然不经过写限流；
>    `/debug/vars`、`/debug/requests` 在特定开关组合下仍然无认证。

> **模块边界（本篇 owner）**
> - 本篇负责：HTTP 路由/中间件/认证矩阵、TLS 证书生命周期、可观测性（`/debug/vars`、`/metrics`、`SHOW STATS/DIAGNOSTICS`、pprof）、Prometheus 端点、Flux 的 **HTTP 入口与资源治理**、V2 delete/buckets。
> - Flux 控制器状态机/内存管理器（`query/control`）、`storage/flux` 读路径、window aggregate 下推：owner 为 [03](03_query_engine_diff.md) §3.1/§3.7/§3.10-§3.11 与 [09](../09_storage_reads_spec.md)。本篇 §3.16-3.18 只保留 HTTP 影响摘要和链接，不重复分析。
> - meta/shard/CQ/snapshotter/subscriber：owner 为 [05](05_meta_background_diff.md)；compaction/purger：owner 为 [04](04_storage_compaction_diff.md)。
> - 监控的 `cq` 扁平化以 §3.12 为准；05 只保留“形状变化”的摘要（05 §1 #17/§5.1）并链接回本节，不重复 JSON 样例与案例。


---

## 0. 五分钟速读

- **路由**：21→39 条，新增 `/api/v2/delete`、14 条 `/api/v2/buckets*` 与 OPTIONS；v2 delete 按 bucket 删除会跨所有 RP。
- **中间件**：`authenticate→gzip→writeThrottler→log/recovery` 的顺序；写限流 503（queue full/timeout，默认 30s）先于认证。
- **TLS**：`TLSConfigManager`/`CertLoader`；SIGHUP 只重载 HTTP/OpenTSDB 证书，其余配置不生效；key 权限 0600。
- **Flux**：关闭时认证开启且已有管理员返回 401，认证关闭/引导模式返回 403；`storage/flux` 读路径。
- **可观测性**：`/debug/vars` 的 config/stats/cq（cq 扁平化）；pprof 认证矩阵；`200+Flush` 后执行期错误只进 body。

> **事实归属**：HTTP 路由/中间件细节见 [10](../10_http_service_spec.md)；Flux 读见 [09](../09_storage_reads_spec.md)；InfluxQL 执行见 [11](../11_influxql_execution_spec.md)。

## 1. 结论速览

| # | 变更点 | v1.8.10 | v1.12.3 | 影响 | 证据 |
|---|--------|---------|---------|------|------|
| 1 | 路由总数 | 21 条（14 基础 + 6 pprof + 1 flux） | 39 条（31 基础 + 6 pprof + 2 flux） | 兼容面扩大，攻击面同步扩大 | `v1.8.10:services/httpd/handler.go:186` / `v1.12.3:services/httpd/handler.go:193` |
| 2 | `/api/v2/delete` | 不存在 | `serveDeleteV2`，POST，tombstone 删除 | 新增 V2 删除能力 | `v1.12.3:services/httpd/handler.go:214`、`:975` |
| 3 | `/api/v2/buckets*` | 不存在 | 14 条（CRUD + labels/members/owners 空实现） | V2 bucket 语义映射到 db/rp | `v1.12.3:services/httpd/handler.go:218`-`:273` |
| 4 | `OPTIONS` 预检 | 仅 `/query`、`/write` | 追加 `/api/v2/write`、`/api/v2/query`、`/health` | 浏览器 CORS 预检可用 | `v1.12.3:services/httpd/handler.go:278`、`:310`、`:371` |
| 5 | Flux 关闭时的行为 | 匿名 handler 直接 403，**绕过认证** | `serveFluxQueryDisabled`，走 `authenticate` | 认证开启时未带凭据变 401 | `v1.8.10:services/httpd/handler.go:292` / `v1.12.3:services/httpd/handler.go:376`、`:2263` |
| 6 | `/query` 时间格式 | 只有 `epoch` 参数 | 新增 `time_format=rfc3339\|epoch`，默认 `epoch` | 可读时间戳；非法值 400 | `v1.8.10:services/httpd/handler.go:534` / `v1.12.3:services/httpd/handler.go:635`-`:646` |
| 7 | 用户维度查询字节 | 无 | `userquerybytes` 统计（`userQueryRespBytes`，tag `user`），开关 `user-query-bytes-enabled` | 可按用户计费/排障；默认关闭 | `v1.12.3:services/httpd/handler.go:483`-`:511`、`services/httpd/config.go:68` |
| 8 | `SHOW QUERIES` 列 | `qid,query,database,duration,status` | 追加 `user` 列 | 运维可定位提交人 | `v1.8.10:query/task_manager.go:152` / `v1.12.3:query/task_manager.go:34`、`:152` |
| 9 | `SHOW QUERIES` 越权 | 无按库过滤，非管理员可见他人库查询 | 按 `CoarseAuthorizer` 的 READ 权限过滤 | 关闭信息泄漏 | `v1.8.10:query/task_manager.go:129` / `v1.12.3:query/task_manager.go:136`-`:147` |
| 10 | pprof 无用户时 | `user.ID()` 空指针 panic → 500 | 先判空，返回 403 | 行为修正 | `v1.8.10:services/httpd/handler.go:252` / `v1.12.3:services/httpd/handler.go:326`-`:334` |
| 11 | TLS 加载 | `tls.LoadX509KeyPair` 一次性装入 `tls.Config.Certificates` | `TLSConfigManager` + `TLSCertLoader`，`GetCertificate` 动态取 | 为热重载/巡检铺路 | `v1.8.10:services/httpd/service.go:115` / `v1.12.3:services/httpd/service.go:162` |
| 12 | 证书重载 | 不支持，SIGHUP 无处理 | SIGHUP → `ReloadConfig` → `PrepareCertificateLoad` | 不重启换证 | `v1.8.10:cmd/influxd/main.go:85` / `v1.12.3:cmd/influxd/main.go:87`、`services/httpd/service.go:296` |
| 13 | 客户端证书 | 不支持 | `GetClientCertificate` + `SetupTLSConfig` | 双向 TLS 可用 | `v1.12.3:pkg/tlsconfig/certconfig.go:318`、`:344` |
| 14 | `https-insecure-certificate` | 无此配置；不检查文件权限 | 存在；语义=**忽略证书/私钥文件权限检查** | 权限过宽会启动失败，需显式放行 | `v1.12.3:services/httpd/config.go:53`、`services/httpd/service.go:162`-`:164` |
| 15 | 证书文件权限检查 | 无 | `VerifyFilePermissivenessF`：证书 ≤0644、私钥 ≤0600 | **升级可能启动失败** | `v1.12.3:pkg/tlsconfig/certconfig.go:120`-`:125`、`pkg/file/file.go:58` |
| 16 | `/debug/vars` 内容 | system/cmdline/memstats/各服务统计 | 新增 `build`/`crypto`/`config`/`stats`；`cq` 旧版已存在，改为扁平输出（`{name,tags,values}` → `values`） | 诊断能力增强，也扩大泄露面 | `v1.8.10:services/httpd/handler.go:1479` / `v1.12.3:services/httpd/handler.go:2291` |
| 17 | 压缩吞吐诊断 | 无 | `monitor/stats.go` 注册 `stats` 诊断 | 可观测 compaction 限流占用 | `v1.12.3:monitor/service.go:156`、`monitor/stats.go:12-34` |
| 18 | TSDB 配置诊断 | 无 | `monitor` 注册 `config`（`tsdb.Config`） | `/debug/vars` 可见 3 个新 TSDB 字段 | `v1.12.3:monitor/service.go:164`、`tsdb/config.go:304-306` |
| 19 | CQ 诊断 | 无 | CQ 服务新增空实现的 `Diagnostics()`（注册名 `cq`，当前不返回诊断值，仅满足 Monitor 接口） | `/debug/vars` 的 `cq` 两版都有，新版只是扁平化 | `v1.12.3:services/continuous_querier/service.go:175`、`cmd/influxd/run/server.go:426` |
| 20 | Prometheus 协议类型 | 自带 `prometheus/remote`（gogo/protobuf） | 上游 `prompb`（仍由 protoc-gen-gogo 生成；influxdb 改用生成方法 `req.Unmarshal`/`resp.Marshal`，不再直接 import gogo 包） | 线格式不变，字段名 `TimestampMs`→`Timestamp` | `v1.8.10:services/httpd/handler.go:1192` / `v1.12.3:services/httpd/handler.go:1974` |
| 21 | Flux 控制器 | `flux/control`，无内存/并发限制 | `query/control`，并发配额/内存配额/队列/指标 | 资源可控，错误码变化；实现细节 owner 03 §3.1 | `v1.8.10:flux/control/controller.go:73` / `v1.12.3:query/control/controller.go:175` |
| 22 | Flux 读路径 | `storage/reads/flux_reader.go`，无 window aggregate | `storage/flux/*`，新增 `ReadWindowAggregate` | 下推能力增强；实现 owner 03 §3.7/§3.10-§3.11、09 | `v1.8.10:storage/reads/flux_reader.go:30` / `v1.12.3:storage/flux/reader.go:61`、`:85` |
| 23 | `Store` 接口 | 只有 `ReadFilter` | 增加 `Delete`、`DeleteRetentionPolicy` | V2 删除/桶删除落地 | `v1.8.10:services/httpd/handler.go:2025` / `v1.12.3:services/httpd/handler.go:3088`-`:3092` |
| 24 | 写入统计 `valuesWrittenOK` | 存在（`WritePointsWithContext` + context 计数） | 移除（连同 `WritePointsWithContext` 路径） | `SHOW STATS` 少 1 个（valuesWrittenOK）、多 1 个（fluxQueryRespBytes），promReadReq 从恒 0 变真实计数 | `v1.8.10:services/httpd/handler.go:989`-`:1010` / `v1.12.3:services/httpd/handler.go:1772` |
| 25 | CSV 输出转义 | `HashKey()`（转义 tag） | `HashKey(false)`（不转义） | CSV tag 列不再被二次转义 | `v1.8.10:services/httpd/response_writer.go:233` / `v1.12.3:services/httpd/response_writer.go:228` |
| 26 | JWT 库 | `dgrijalva/jwt-go/v4` | `golang-jwt/jwt/v4` | API 兼容，依赖换血 | `v1.8.10:services/httpd/handler.go:26` / `v1.12.3:services/httpd/handler.go:24` |
| 27 | Service 关闭/重开 | `Close()` 直接关 listener；重复调用会重复关闭 | `sync.OnceValue(doClose)` 幂等；`Open` 在 `closed` 后报错 | 生命周期更安全 | `v1.8.10:services/httpd/service.go:199` / `v1.12.3:services/httpd/service.go:137`、`:155`、`:239` |
| 28 | 可观测性/调试面 | 分散在 `/debug/vars`、`SHOW STATS`、pprof | §3.12 统一三个面；pprof/`/debug/requests` 并入；§3.3.1 唯一认证矩阵 | 明确 owner 与默认匿名面；`/metrics` 默认无认证 | `v1.12.3:services/httpd/handler.go:2291-2524`、`monitor/service.go:146-166`、`:315-317` |

---

## 2. 变更地图

### 2.1 请求进入与分发（新版本真实符号）

```mermaid
flowchart TD
    C["Client"] --> SH["Handler.ServeHTTP<br/>handler.go:555"]
    SH --> PB{"AuthEnabled && PprofEnabled && PprofAuthEnabled?"}
    PB -->|"是"| MUX["pat.PatternServeMux.ServeHTTP"]
    PB -->|"否"| PP{"HasPrefix /debug/pprof<br/>&& PprofEnabled?"}
    PP -->|"是"| HP["h.handleProfiles<br/>pprof.go:22"]
    PP -->|"否"| DV{"HasPrefix /debug/vars?"}
    DV -->|"是"| EV["h.serveExpvar<br/>handler.go:2291"]
    DV -->|"否"| DR{"HasPrefix /debug/requests?"}
    DR -->|"是"| DQ["h.serveDebugRequests<br/>handler.go:2527"]
    DR -->|"否"| MUX

    MUX -->|"pat 命中已注册的 recovery handler"| MW["已注册 wrapper 链<br/>（NewHandler/AddRoutes 构建期 handler.go:514）"]
    MW --> REC["recovery"]
    REC --> LOG["logging（LogEnabled 且该路由 LoggingEnabled）"]
    LOG --> RID["requestID"]
    RID --> CORS["cors"]
    CORS --> OPT{"Method == OPTIONS?"}
    OPT -->|"是（仅已注册 OPTIONS 的路由）"| OPTR["直接 return（隐式 200；CORS 头仅当带 Origin，另有版本/请求 ID 头；serveOptions 不可达）"]
    MUX -.->|"未注册 OPTIONS 的路径（如 /api/v2/buckets*、/api/v2/delete）"| E405["pat：方法未命中 → 405 Method Not Allowed"]
    OPT -->|"否"| HDR["SetHeadersHandler"]
    HDR --> GZ["gzipFilter（Gzipped=true 时）"]
    GZ --> RW["responseWriter"]
    RW --> TH{"Method==POST 且 Pattern 为 /write 或 /api/v1/prom/write?"}
    TH -->|"是"| THR["writeThrottler.Handler"]
    TH -->|"否"| AUTH
    THR --> AUTH["authenticate（handler 签名带 meta.User 时）"]
    AUTH --> H1["serveQuery / serveWriteV1 / serveWriteV2<br/>serveDeleteV2 / servePostCreateBucketV2<br/>serveDeleteBucketV2 / serveRetrieveBucketV2<br/>serveListBucketsV2 / serveUpdateBucketV2<br/>serveLabelsNotAllowedV2 / serveBucketMembersNotAllowedV2<br/>serveBucketOwnersNotAllowedV2（labels/members/owners → 405）"]
    AUTH --> H2["servePromWrite / servePromRead"]
    AUTH --> H3["serveFluxQuery / serveFluxQueryDisabled"]
    AUTH -->|"仅 AuthEnabled && PingAuthEnabled（authWrapper 转 3 参）"| H4["servePing / serveHealth / serveStatus<br/>/metrics（promhttp）"]
    MUX -.->|"!(AuthEnabled && PingAuthEnabled) 时（默认 PingAuthEnabled=false）ping/status/health/metrics 为 2 参直达，不经 authenticate"| H4
    AUTH --> DP["handleProfiles / serveExpvar / serveDebugRequests<br/>（三开关全开时经 MUX 注册、带 authWrapper）"]
    H1 --> RESP["Response / httpError"]
    H2 --> RESP
    H3 --> RESP
    H4 --> RESP
    DP --> RESP
```

### 2.2 TLS 证书生命周期（旧 vs 新）

```mermaid
flowchart LR
    subgraph OLD["v1.8.10"]
        O1["Service.Open"] --> O2["tls.LoadX509KeyPair(cert,key)"]
        O2 --> O3["tlsConfig.Certificates = [cert]"]
        O3 --> O4["tls.Listen(addr, tlsConfig)"]
        O1 -.->|"https=false"| O4E["net.Listen(addr)"]
        O5["SIGHUP"] --> O6["未注册 handler<br/>默认动作=终止进程"]
    end

    subgraph NEW["v1.12.3"]
        N1["Service.Open"] --> N2["tlsconfig.NewTLSConfigManager"]
        N2 -->|"useTLS 且 cert/key 非空"| N3["NewTLSCertLoader(cert,key)"]
        N3 --> N4["LoadCertificate：权限检查 + X509KeyPair + 解析 leaf"]
        N4 --> N5["SetupTLSConfig：GetCertificate / GetClientCertificate"]
        N4 -.-> N4E["Service.Open 返回错误<br/>（权限/路径/证书解析失败）"]
        N5 --> N6["TLSConfigManager.Listen → tls.Listen"]
        N7["SIGHUP"] --> N8["Main.Run reloadCh"]
        N8 --> N9["Command.ReloadConfig"]
        N9 --> N10["Server.ApplyReloadedConfig"]
        N10 --> N11["Service.PrepareReloadConfig"]
        N11 --> N12["TLSConfigManager.PrepareCertificateLoad"]
        N12 --> N13["TLSCertLoader.PrepareLoad（预校验）"]
        N13 --> N14["apply: SetCertificate（原子替换）"]
        N14 --> N16["GetCertificate：下次握手读取新 cl.cert"]
        N3 --> N15["monitorCert：每小时巡检过期"]
        N6 -.-> N6E["useTLS=false → net.Listen（无 certLoader）<br/>PrepareReloadConfig 在 https=false 时直接返回"]
    end
```

### 2.3 模块地图与 owner

| 模块 | 角色 | 本篇小节 | owner / 外部链接 |
|------|------|----------|------------------|
| HTTP 路由/中间件/限流 | 请求进入、包装顺序、状态码 | 3.1、3.2、3.20 | 本篇 |
| 认证/授权 | 凭据解析、JWT、各路由授权钩子 | 3.3（唯一矩阵）、3.7、3.19 | 本篇；语句级授权见 [11](../11_influxql_execution_spec.md) |
| TLS | 证书加载、SIGHUP 热重载、客户端证书、巡检 | 3.8-3.11、6.3 | 本篇；主 [10](../10_http_service_spec.md) |
| 可观测性 | `/debug/vars`、`/metrics`、`SHOW STATS/DIAGNOSTICS`、pprof | 3.12、3.13、6.4 | 本篇；CQ 统计来源 [05](05_meta_background_diff.md) §3.15 |
| Prometheus 端点 | remote write/read 协议与授权 | 3.14 | 本篇；转换细节主 [10](../10_http_service_spec.md) §9 |
| Flux | HTTP 入口、错误映射、资源配额配置 | 3.15、3.16-3.18 摘要、6.5 | 入口本篇；controller/read path [03](03_query_engine_diff.md) §3.1/§3.7/§3.10-§3.11、[09](../09_storage_reads_spec.md) |
| V2 delete/buckets | 语义映射、授权、body 边界 | 3.19、6.6 | 本篇；主 [10](../10_http_service_spec.md) §4.5 |

---

## 3. 逐项差异

> 本节只保留每个小节的核心结论；完整代码、表格、图与失败矩阵见 [06a 详细附录](06a_details_appendix.md)。标题编号与附录一致，跨文档的 `06 §3.x` 引用仍然有效。H4 细分小节（§3.3.1、§3.12a-e、§3.14a-e、§3.15a-d）的完整正文也在附录。

### 3.1 路由表逐条对照

旧版 21 条路由（14 基础 + 6 pprof + 1 flux）集中在三段 `AddRoutes`；新版 39 条（31 基础 + 6 pprof + 2 flux），新增全部是 V2 兼容面（`/api/v2/delete`、14 条 `/api/v2/buckets*`、3 条 OPTIONS），未删除任何旧路由。三条新 OPTIONS 被外层 `cors` 在 OPTIONS 直接 return，实际是隐式 200；`:dbrp` 是 pat 风格参数，`db/rp` 必须写成 `db%2Frp`；labels/members/owners 九条占位路由返回 405。

> 完整细节见 [06a 附录 §3.1](06a_details_appendix.md#s3-1)

### 3.2 中间件链与限流

包装顺序两版一致（`authenticate→gzip→SetHeaders→cors→requestID→logging→recovery`）；`writeThrottler` 只包 `POST /write` 与 `/api/v1/prom/write`，位于 `authenticate` 外层，队列满或等待超过 30s 返回 `text/plain` 503 且无 `X-InfluxDB-Error`，`/api/v2/write` 仍不受写限流。新增 V2 路由 gzip 均为 false；CORS 方法加 `PATCH`、头加 `User-Agent`，但 bucket/delete 未注册 OPTIONS，浏览器预检仍是 405。

> 完整细节见 [06a 附录 §3.2](06a_details_appendix.md#s3-2)

### 3.3 认证、JWT 与 pprof 授权

认证主链两版一致（`u/p`、`Bearer`、`Token`、`Basic` → `MetaClient`/JWT → 引导模式跳过）；三处差异是 JWT 库由 `dgrijalva/jwt-go/v4` 换成 `golang-jwt/jwt/v4`（已有 token 仍可用）、pprof 未授权由 `user.ID()` nil panic（500）改为先判空返回 403、`parseToken` 改用 `strings.Cut`。

> 完整细节见 [06a 附录 §3.3](06a_details_appendix.md#s3-3)

#### 3.3.1 路由 × 凭据 × 开关 × 授权矩阵（唯一矩阵）

06 的唯一认证矩阵：`authenticate` 仅在 `AuthEnabled` 且管理员存在时校验凭据（缺失/错误 401）；引导模式与“已认证但无权限”返回 403；写限流 503 先于认证；`/metrics`、ping/status/health 默认匿名。完整矩阵表见附录。

> 完整细节见 [06a 附录 §3.3.1](06a_details_appendix.md#s3-3-1)

### 3.4 访问日志与 status filters

`logging` 中间件与 CLF 行格式无功能变化；唯一差异是 `NewConfig` 显式初始化 `AccessLogStatusFilters` 空切片（`a2e029fa7f`），使 `influxd config` 输出 `access-log-status-filters = []`。运行时 nil 与空切片等价（`StatusFilters.Match` 对 `len==0` 返回 true）。

> 完整细节见 [06a 附录 §3.4](06a_details_appendix.md#s3-4)

### 3.5 用户维度查询字节（#27188）

新增 `user-query-bytes-enabled` 开关（默认 false）与按用户累计的 `userquerybytes` 统计（tag `user`，匿名记 `(anonymous)`），覆盖 `/query` chunked/非 chunked、Flux、Prom read 四条路径。`/debug/vars` 顶层键按 `bind` 拼装（如 `userquerybytes::8086`，冲突追加 `:N`）；该 map 只增不减（风险 9）。

> 完整细节见 [06a 附录 §3.5](06a_details_appendix.md#s3-5)

### 3.6 `/query` 的 `time_format`（#26596）

新增 `time_format=rfc3339|epoch` 白名单（默认 `epoch`），非法值在写响应头之前返回 400；`rfc3339` 时忽略 `epoch`，时间列输出 RFC3339Nano 字符串。`convertToTimeFormat` 失败发生在 `writeHeader(200)+Flush` 之后，400 无法生效（已知缺陷，与 10 §5.5 一致）。

> 完整细节见 [06a 附录 §3.6](06a_details_appendix.md#s3-6)

### 3.7 `SHOW QUERIES`：user 列与越权修复（#26981、#27196）

`SHOW QUERIES` 由 5 列变 6 列（新增 `user`，来源 `ExecutionOptions.UserID`），并按 `CoarseAuthorizer.AuthorizeDatabase(ReadPrivilege, db)` 过滤：非管理员只能看到自己有 READ 权限的库上的查询（修复越权泄漏），`database` 为空的查询不受影响。按列位置解析的客户端会错位（破坏兼容）。

> 完整细节见 [06a 附录 §3.7](06a_details_appendix.md#s3-7)

### 3.8 TLS 体系重构：`TLSConfigManager` + `TLSCertLoader`

TLS 拆为 `TLSConfigManager`（策略/监听）→ `TLSCertLoader`（加载/热替换/巡检）→ `LoadCertificate`（单次加载 + 权限校验），握手经 `GetCertificate` 动态取证书，换证只需原子替换 loader 指针。`monitorCert` 每小时巡检、只告警不自动重载；`Close` 只发退出信号、不 join 巡检 goroutine（可能多一条 Warn）。

> 完整细节见 [06a 附录 §3.8](06a_details_appendix.md#s3-8)

### 3.9 SIGHUP 证书热重载（#27057）

SIGHUP 从旧版“默认终止进程”变为触发 `Command.ReloadConfig`；两阶段执行（先全量验证，任一失败则整次不 apply；apply 阶段失败保留旧证书）。作用域只有 HTTPD 与 OpenTSDB 证书：`https-insecure-certificate`、`auth-enabled`、`max-body-size`、`[flux-controller]` 等不生效也不报错，`https-enabled` 运行中变更会整次拒绝；已有 keep-alive 连接仍用旧证书，reload 同步占住信号循环（慢盘可阻塞 SIGTERM）。

> 完整细节见 [06a 附录 §3.9](06a_details_appendix.md#s3-9)

### 3.10 客户端证书支持（#27090）

新增 `GetClientCertificate` 并把 `SetupTLSConfig` 作为统一挂载点，可装配双向 TLS；服务端不接受客户端证书时返回空证书 + nil error 以贴合 Go 原生行为（两端看到 `certificate required`，而非 `bad certificate`）。HTTP 服务未暴露 `WithClientAuth` 配置项，需企业版/自定义 `Service` 装配。

> 完整细节见 [06a 附录 §3.10](06a_details_appendix.md#s3-10)

### 3.11 `https-insecure-certificate` 的真实语义（#27219）

该配置的语义是**忽略证书/私钥文件权限检查**，不是 `InsecureSkipVerify`（#27216 曾误用 `WithAllowInsecure` 而完全无效，#27219 改为 `WithIgnoreFilePermissions`）。默认 false 时证书 ≤0644、私钥 ≤0600：历史 0644 私钥会使 `Service.Open` 启动失败；运行中 SIGHUP 换 0644 key 则整次 reload 不 apply，旧证书继续服务。

> 完整细节见 [06a 附录 §3.11](06a_details_appendix.md#s3-11)

### 3.12 可观测性：`/debug/vars`、`/metrics` 与 `SHOW STATS/DIAGNOSTICS`

可观测性统一为三个面：`/debug/vars`（JSON，新增 `build`/`config`/`stats`/`crypto`，`cq` 由 `{name,tags,values}` 改为扁平 `values`，重复键追加 `:N`）、`/metrics`（Prometheus，默认匿名）、`SHOW STATS/DIAGNOSTICS`（httpd 少 `valuesWrittenOK`、新增 `fluxQueryRespBytes`、`promReadReq` 变真实计数）。`/debug/vars` 流式写出，进入正文后的 500 只能追加 body；`cq` 空 values 会返回 200 截断 JSON（风险 17）。

> 完整细节见 [06a 附录 §3.12](06a_details_appendix.md#s3-12)

### 3.13 调试面：pprof 与 `/debug/requests`（可观测性模块的一部分）

pprof 与 `/debug/requests` 并入可观测性模块，共用 `ServeHTTP` 前缀分支和 `PprofAuthEnabled`：三开关全开才注册路由并做管理员检查，否则匿名可达。pprof 引导模式由 nil panic 500 修为 403；`/debug/pprof/all` 返回未压缩 tar（6 个 profile + SHOW SHARDS/STATS/DIAGNOSTICS），`?cpu`/`?trace` 可追加 profile/trace。

> 完整细节见 [06a 附录 §3.13](06a_details_appendix.md#s3-13)

### 3.14 Prometheus remote write/read：协议栈迁移与端点行为

协议类型从自带 `prometheus/remote`（gogo/protobuf）迁到上游 `prompb`（`Sample.TimestampMs`→`Timestamp`），线格式不变。写路径容忍 `DroppedValuesError`（NaN/±Inf 静默丢弃仍返回 204）；read 只支持 float 游标、`len(Queries)!=1` 返回 400；`StartTimestampMs=0` 旧版被扩为全时间范围、新版保持 0 得到空结果（200）。

> 完整细节见 [06a 附录 §3.14](06a_details_appendix.md#s3-14)

### 3.15 Flux 模块：HTTP 入口与资源治理

Flux 入口负责请求解码、错误映射与 `[flux-controller]` 配额：控制器编译/队列/内存错误一律映射 **500**（`ResourceExhausted` 不区分，风险 8）；编码中途失败且已写字节则返回 200 + 截断 CSV。配额默认全 0 = 不限制；关闭分支改走 `authenticate`，已有管理员时缺凭据返回 401（旧版直接 403 文本）。

> 完整细节见 [06a 附录 §3.15](06a_details_appendix.md#s3-15)

### 3.16 `query/control`：控制器（摘要，owner 03 §3.1）

控制器状态机/并发配额/内存池/指标 owner 为 [03](03_query_engine_diff.md) §3.1，本篇只保留 HTTP 影响：`ResourceExhausted` 仍映射 500；默认全 0 时每查询一个 goroutine、无全局池，进程级 OOM 风险与旧版相同。

> 完整细节见 [06a 附录 §3.16](06a_details_appendix.md#s3-16)

### 3.17 `storage/flux` 读路径（摘要，owner 03 §3.7/§3.10/§3.11、09）

`storage/flux` 读路径与 window aggregate 下推 owner 为 [03](03_query_engine_diff.md) §3.7/§3.10-§3.11、[09](../09_storage_reads_spec.md)；HTTP 响应格式不变，唯一相关边界是 `decodeQueryRequest` 不做 `MaxBodySize`/`ContentLength` 预检。

> 完整细节见 [06a 附录 §3.17](06a_details_appendix.md#s3-17)

### 3.18 `services/storage`（摘要，owner 03 §3.7、09）

读侧 limit/降序游标/`GetReadSource` nil 防护 owner 为 [03](03_query_engine_diff.md) §3.7、[09](../09_storage_reads_spec.md)；本篇只保留 V2 删除依赖的 `Store.Delete`/`DeleteRetentionPolicy`，`Handler.Store` 接口由 1 个方法扩到 3 个。

> 完整细节见 [06a 附录 §3.18](06a_details_appendix.md#s3-18)

### 3.19 V2 删除与 bucket 端点语义

`/api/v2/delete` 的 `bucket=db/rp` 只做 404 校验，measurement 不带 RP，shard 过滤仅 `byDatabase(db)`，因此会删除该 db 下**所有 RP** 的匹配数据；无 `_measurement` 时删除整个 db 在时间区间内的所有 measurement。bucket 端点族与授权分支见附录；单桶 `?id=`/`?name=` 只检查 `user != nil` 而不授权（已知缺口，风险 18）；新增端点只做 `ContentLength` 预检，chunked body 无上限。

> 完整细节见 [06a 附录 §3.19](06a_details_appendix.md#s3-19)

### 3.20 其它 HTTP 层差异

`valuesWrittenOK` 随 `WritePointsWithContext` 移除（values 统计下移 tsdb）、新增 `fluxQueryRespBytes`、`promReadReq` 变为真实计数；V1/V2 非法 precision 补 `return`（旧版先写 400 仍可能落库）；部分写错误文本追加 database/RP；Service 生命周期改 `sync.OnceValue` 幂等、Close 后 Open 报错；CSV tag 改用 `HashKey(false)` 不再二次转义；删除 `WriteError`。

> 完整细节见 [06a 附录 §3.20](06a_details_appendix.md#s3-20)


## 4. 优化与收益

| 维度 | 变化 | 机制 | 证据 |
|------|------|------|------|
| 可用性 | 证书热重载 | SIGHUP → 预校验 → 原子替换 loader 指针；失败保留旧证书 | `certconfig.go:412-424`、`services/httpd/service.go:296-322` |
| 安全 | 私钥/证书文件权限强制 | 启动时 `f.Stat()` 比较权限位；`https-insecure-certificate` 显式放行 | `certconfig.go:120-125`、`file/file.go:74-88` |
| 安全 | `SHOW QUERIES` 越权修复 | 按 `CoarseAuthorizer.AuthorizeDatabase(ReadPrivilege, db)` 过滤 | `query/task_manager.go:144-146` |
| 安全 | pprof 未授权不再 500 | 先判 `user != nil` 再取 ID | `services/httpd/handler.go:326-334` |
| 可观测 | 按用户查询字节 | `gensyncmap.Map[string,*atomic.Int64]` + 可选开关 | `handler.go:153`、`:483-511` |
| 可观测 | `/debug/vars` 诊断扩展 | `Monitor.RegisterDiagnosticsClient` 注册 `config`/`stats`/`cq` | `monitor/service.go:156-166` |
| 可观测 | 查询归属人 | `ExecutionOptions.UserID` → `Task.userID` → `SHOW QUERIES`/慢查询日志 | `query/executor.go:205-206`、`task_manager.go:216`、`:233` |
| 资源控制 | Flux 并发/内存/队列 | `query/control` 的 `memoryManager` + `queryQueue` + `wg` 配额 | `query/control/controller.go:175-227`、`memory.go:73-156` |
| 资源控制 | 窗口聚合下推 | `ReadWindowAggregate` → `Store.WindowAggregate` → `reads.NewWindowAggregateResultSet` | `storage/flux/reader.go:85-93`、`services/storage/store.go:234-277` |
| 正确性 | 降序游标 | `IsAscendingGroupAggregate`/`IsAscendingWindowAggregate` 决定 shard 读取方向 | `services/storage/store.go:195`、`:252` |
| 正确性 | 空 `ReadSource` 的 nil 判定集中到 `GetReadSource` | 旧版各入口逐个返回 `missing read source`；新版把 nil `Any` 判定集中到 `GetReadSource` | `services/storage/store.go:28-31` |
| 正确性 | V2 写入精度错误不再"边报错边写" | 补 `return` | `handler.go:1636-1640` |
| 正确性 | CSV 双重转义 | `HashKey(false)` | `response_writer.go:228` |
| 正确性 | monitor 全局 tag 竞态 | 用 `mu sync.RWMutex` 包裹 map | `monitor/service.go:37-68` |
| 生命周期 | HTTP 服务幂等关闭 | `sync.OnceValue` + `tcpServerStarted` 归属判定 | `services/httpd/service.go:137`、`:246-285` |
| 兼容性 | V2 bucket/delete 面 | 新增 18 条路由，复用 db/rp 语义 | `handler.go:214-273` |

没有基准数据的地方不写数字。上述收益都是**机制层面**的：
例如 TLS 热重载只改变"证书从哪里取"，不改变握手成本；
Flux 内存配额不改变单查询的算法复杂度，但配置配额后每个查询都要经过
`queryMemoryManager.RequestMemory`（CAS 全局池）与 `memory.ResourceAllocator` 计账，
开销未做基准，不能宣称吞吐不变（03 §3.1）。默认全 0 时也会安装 allocator，只是
`Limit=math.MaxInt64`（`query/control/memory.go:45-55`、`controller.go:112-131`）。

---

## 5. 行为变化与兼容性

### 5.1 配置项变化（`[http]`）

| 配置项 | v1.8.10 | v1.12.3 | 默认值 | 升级影响 |
|--------|---------|---------|--------|----------|
| `flux-testing` | 无 | 有 | `false` | 无（不开就不用） |
| `https-insecure-certificate` | 无 | 有 | `false` | **私钥权限 >0600 会启动失败** |
| `user-query-bytes-enabled` | 无 | 有 | `false` | 无；开启后多出 `userquerybytes` 统计 |
| `access-log-status-filters` | 有（TOML） | 有（且进入默认输出） | `[]` | 无运行时变化 |
| `debug-pprof-enabled` | 有 | 有 | `false` | 不变 |
| `prom-read-auth-enabled` | 有 | 有 | `false` | 不变 |
| `[flux-controller] query-concurrency` 等 | 无（控制器无配置） | 有 | 全 0=不限制 | 不配置时行为等同旧版 |

`[http]` 在 `v1.12.3:services/httpd/config.go:34-70`；
`[flux-controller]` 在 `v1.12.3:cmd/influxd/run/config.go:45` 与
`v1.12.3:query/control/controller.go:71-110`。

> `[data]` 另新增 `ingress-metric-by-measurement-enabled` / `ingress-metric-by-login-enabled`（默认均 `false`）。
> 开启后 `SHOW STATS` 的写入统计按 measurement/login 拆分，tag 基数上升；默认关闭时行为与旧版一致
> （`v1.12.3:tsdb/config.go:138-139`、`v1.12.3:etc/config.sample.toml:72-73`）。

> `[flux-controller]` 有互斥/正值校验，违反时 `New` 失败、启动即报错（`query/control/controller.go:112-165`）：
> `query-concurrency > 0` 时 `query-queue-size` 必须 > 0；`query-concurrency == 0`（不限制）时
> `query-queue-size` 与 `total-max-memory-bytes` 必须为 0。0 值不是错误：`query-max-memory-bytes=0`
> → `math.MaxInt64`，`query-initial-memory-bytes=0` → 继承 max；只有负数报错（`controller.go:112-165`）。
> `total-max-memory-bytes` 非 0 时要求 ≥ `query-concurrency × query-initial-memory-bytes`，但如果
> initial 被填成 `math.MaxInt64`，`int64(concurrency)*initial` 会溢出为负数、该校验被绕过
> （03 §7.5）。生产配置应显式给非 0 的 `query-initial-memory-bytes`，并让不等式在数学上成立。

### 5.2 状态码与错误体变化

| 场景 | v1.8.10 | v1.12.3 | 是否破坏兼容 |
|------|---------|---------|--------------|
| Flux 关闭 + 无凭据 + auth 开启 | 403（不认证） | 401（已存在管理员时）/ 403（引导模式） | **是**（状态码可能变化） |
| pprof 在引导模式（无管理员用户）下访问 | 500（panic 被 recovery 捕获） | 403 | 否（更正确） |
| `/api/v2/write` 非法 precision | 400 + 可能仍写入 | 400 且不写入 | 否（更正确） |
| 部分写解析错误 body | `partial write: ... dropped=N` | 追加 `for database: ... for retention policy: ...` | 客户端若做精确字符串匹配会受影响 |
| `/api/v2/query` 队列满/内存超限 | 不适用（无限制） | **500** + flux 错误文本 | 新增失败模式 |
| 写限流（`/write`、`/api/v1/prom/write`）队列满/排队超时 | 503（两版相同） | 503（两版相同） | 否；但限流在认证外层，`text/plain` 且无 `X-InfluxDB-Error`，不能当业务写失败处理 |
| `SHOW QUERIES` 列数 | 5 | 6（多 `user`） | **是**（按列位置解析的客户端会错位） |
| `SHOW STATS` httpd | 含 `valuesWrittenOK`、`promReadReq`（恒 0） | 不含 `valuesWrittenOK`；新增 `fluxQueryRespBytes`；`promReadReq` 真实计数 | **是**（面板缺列/多列/旧指标失真） |
| `/api/v1/prom/read` 跨库读取 | 无库级授权 | 仅当 `prom-read-auth-enabled=true` 时检查 READ | 默认不变；开启后可能 403 |
| `/debug/vars` | `system`/`cmdline`/`memstats` + 各服务统计（含 `cq`，形状 `{name,tags,values}`） | 新增 `config`/`stats`/`crypto` 等；httpd 少 `valuesWrittenOK`；`cq` 变扁平 | 是（有增有减） |

**请求级对照（可直接复现）**：以下请求默认 `auth-enabled=false`，除注明者外不带 `Accept`；除 **pat 层**的未注册路径 404（`http.NotFound`，`text/plain`，body `404 page not found\n`）、方法不匹配 405（`text/plain`）与写限流 503（`text/plain`）外，已注册 handler 的错误体统一 JSON，`X-InfluxDB-Error` 只对非 401 的 4xx/5xx 设置（`handler.go:2737-2760`、`bmizerany/pat mux.go:141,146`）。

| # | 请求 | 关键头/body | 状态 | 响应头/body |
|---|------|-------------|------|-------------|
| 1 | `OPTIONS /api/v2/write` | `Origin: http://x` | 200 | 空 body；`Access-Control-Allow-Origin: http://x`、`Access-Control-Allow-Methods/Headers/Expose-Headers`、`X-Influxdb-Version`、`X-Influxdb-Build`；**无** `Content-Type`（cors 在 responseWriter 之前直接 return，`handler.go:2964-2968`） |
| 2 | `OPTIONS /api/v2/buckets` | 任意 `Origin` | 405 | `Allow: GET, POST`（顺序由 pat 的 map 迭代决定）、`Content-Type: text/plain; charset=utf-8`、`X-Content-Type-Options: nosniff`，body `Method Not Allowed\n`（未注册 OPTIONS，`pat mux.go:111-149`） |
| 2b | `OPTIONS /query` | `Origin: http://x`；`auth-enabled=true`、`ping-auth-enabled=true` | 200（两版相同） | 空 body；`Access-Control-Allow-Origin: http://x`；`Access-Control-Allow-Methods: DELETE, GET, OPTIONS, POST, PUT`（旧）/ `DELETE, GET, OPTIONS, POST, PUT, PATCH`（新）；`X-Influxdb-Version`、`X-Influxdb-Build`；**无** `Content-Type`；**不经过认证**（`cors` 在外层对任何 OPTIONS 直接 return，`handler.go:2933-2969`） |
| 3 | `GET /query?q=SHOW+DATABASES&time_format=foo` | — | 400 | `{"error":"Time format must be one of the following: rfc3339,epoch"}` + 同名 `X-InfluxDB-Error` |
| 4 | `POST /write?db=telegraf&precision=x` | body `cpu value=1` | 400 | `{"error":"invalid precision \"x\" (use n, u, ms, s, m or h)"}` + `X-InfluxDB-Error` |
| 5 | `POST /api/v2/write?bucket=telegraf/autogen&precision=x` | body `cpu value=1` | 400 | `{"error":"invalid precision \"x\" (use ns, us, ms or s)"}` + `X-InfluxDB-Error` |
| 6 | `POST /write?db=telegraf&rp=autogen` | 一个超出 7d 的点 | 400 | `partial write: ... dropped=1 for database: telegraf for retention policy: autogen`（精确 reason 见附录 §6.2 与 `tsdb/shard.go:131-140`） |
| 7 | `POST /api/v2/delete`（无 `bucket`） | `{}` | 404 | `{"error":"delete - bucket: bucket name \"\" is missing a slash; not in \"database/retention-policy\" format"}` |
| 8 | `POST /api/v2/delete?bucket=telegraf/autogen` | `{"stop":"2026-10-02T00:00:00Z"}` | 400 | `{"error":"delete - start field in RFC3339Nano format required"}` |
| 9 | 同上，合法 `start`/`stop`/`predicate` | — | 200 | **空 body**；`Content-Type: application/json` |
| 10 | `POST /api/v2/buckets` | `{"name":"telegraf/autogen"}`，库不存在 | 201 | `{"description":"","name":"telegraf/autogen","retentionRules":[{"type":"","everySeconds":0,"shardGroupDurationSeconds":604800}],"orgId":"","schemaType":"implicit","id":"telegraf/autogen","type":"","rp":"autogen","createdAt":"0001-01-01T00:00:00Z","updatedAt":"0001-01-01T00:00:00Z"}` |
| 11 | `POST /api/v2/query`，`flux-enabled=true`，并发/队列满 | `Content-Type: application/vnd.flux`，flux body | 500 | `Content-Type: application/json`、`X-InfluxDB-Error: queue length exceeded`，body `{"error":"queue length exceeded"}`（附录 §6.5） |
| 12 | `POST /api/v1/prom/read?db=telegraf&rp=autogen` | body `x`（非 snappy） | 400 | `{"error":"snappy: corrupt input"}` + `X-InfluxDB-Error` |
| 13 | `GET /metrics`，`auth-enabled=true && ping-auth-enabled=true`，已有管理员 | 无 `Authorization` | 401 | `WWW-Authenticate: Basic realm="InfluxDB"`、`Content-Type: application/json`，body `{"error":"unable to parse authentication credentials"}`；**无** `X-InfluxDB-Error` |
| 14 | `GET /debug/pprof/cmdline`，`auth-enabled=true && pprof-enabled=true && pprof-auth-enabled=true`，无管理员（引导模式） | 无 `Authorization` | 403 | `{"error":"error authorizing admin access"}` + `X-InfluxDB-Error`（`handler.go:326-334`） |
| 15 | `GET /debug/vars`，`pprof-auth-enabled=false` | 无 `Authorization` | 200 | `Content-Type: application/json`，JSON 诊断面；不经过 `authenticate` |
| 16 | `POST /write?db=telegraf`，`max-concurrent-write-limit>0`、`max-enqueued-write-limit>0` 且队列已满 | body `cpu value=1`；不提供凭据 | 503 | `Content-Type: text/plain; charset=utf-8`、`X-Content-Type-Options: nosniff`，body `request throttled, queue full\n`（`http.Error` 追加换行）；**无** `X-InfluxDB-Error`；认证尚未执行（`handler.go:3194-3198`） |

> `/api/v2/delete`、`/api/v2/buckets`、Flux 的 `POST` 都不走 `writeThrottler`（`handler.go:528-535` 只匹配 `/write` 与 `/api/v1/prom/write`），因此上表的 413 只来自 `MaxBodySize` 预检。

### 5.3 资源与边界

| 边界 | v1.8.10 | v1.12.3 | 说明 |
|------|---------|---------|------|
| `/write` body 上限 | `max-body-size`（默认 25MB） | 同 | 但 `Content-Encoding: gzip` 分支仍会**丢弃** `truncateReader`（`handler.go:1701-1715`），chunked gzip 无上限 |
| `/api/v2/delete` body | 不适用 | 仅 `ContentLength` 预检 | chunked 无上限（`handler.go:1004-1021`） |
| `/api/v2/buckets` body | 不适用 | 仅 `ContentLength` 预检 | 同上（`handler.go:1160-1173`） |
| `/api/v1/prom/read` body | 无限制（`io.ReadAll`） | 无限制 | 未改善（`handler.go:2033`） |
| `/api/v2/query` body | 无限制（`io.ReadAll`） | 无限制 | Flux 与 JSON 都不经过 `MaxBodySize`/`truncateReader`，也不做 `ContentLength` 预检（`handler.go:2186`、`flux.go:25-54`） |
| 写并发限流 | `/write`、`/api/v1/prom/write` | 同 | `/api/v2/write` 仍未覆盖；拥塞/`enqueued-write-timeout`（默认 30s）超时返回 **503**，先于认证且无 `X-InfluxDB-Error` |
| `/query` 响应写出错误 | 忽略（`n, _ := rw.WriteResponse`） | 同 | chunked 与非 chunked 都不检查错误、不记日志；客户端可能收到 **200 + 截断 body**，计数器只按已写 `n` 累加（`handler.go:817-822`、`:911-913`） |
| 连接数上限 | `max-connection-limit` | 同 | 超出直接 `Close`，不排队 |
| 查询取消 | `CloseNotifier` → `closing`（仅 `/query`） | `/query` 同。Flux 分两种情况：①**执行中**查询由 `createQuery` 派生的 `context.WithCancel(ctx)`（`controller.go:292`）随 `r.Context()` 取消，`pump` 观察到 `ctx.Done()` 后调 `exec.Cancel()`（`:481`、`:896-930`）；②**排队中**查询不会因客户端断开离开 `queryQueue`——`enqueueQuery` 的 `select` 只有 `case c.queryQueue <- q` 与 `default`，没有 `ctx.Done()` 分支（`:386-424`），worker 出队后 `tryExec` 才看到已取消的 parent ctx，直接转 `Canceled` 并关闭结果（`:439-471`）。Prom read 用 `context.Background()`（不随客户端断开） | `/query` 仍是 deprecated API（`handler.go:2192/2097`） |
| HTTP server 超时 | 无 Read/Write/IdleTimeout | 无 | 两版都只设置 `Handler`（`v1.12.3:services/httpd/service.go:132-134`） |

### 5.4 升级检查清单

1. **证书权限**：确认 `https-private-key` 权限 ≤0600、证书 ≤0644；
   否则要么修权限，要么显式 `https-insecure-certificate = true`。
2. **脚本 / 监控兼容**：完整对照见 §5.2。至少检查：`SHOW QUERIES` 多 `user` 列（按列位置解析的脚本会错位）；
   `SHOW STATS` 少 `valuesWrittenOK`、新增 `fluxQueryRespBytes`、`promReadReq` 从恒 0 变真实计数；
   `/debug/vars` 的 `cq` 从 `{name,tags,values}` 变为扁平 values，并新增 `config`/`stats`/`crypto` 板块
   （按 key 采集的监控面板需补/删 key，整段落盘的采集端需评估体积）。Flux 关闭分支的 401/403 变化见第 3 条。
3. **Flux 客户端**：如果依赖"Flux 关闭时返回 403"，需要适配 401。
4. **V2 客户端**：现在可以使用 `/api/v2/delete`、`/api/v2/buckets*`；
   但 labels/members/owners 一律 405。**从 1.8.10 升级后首次使用 delete 前必须确认删除范围**：
   `bucket=db/rp` 只用于 404 校验，`_measurement` 构造的 source 不带 RP、shard 过滤只有
   `byDatabase(db)`，因此会删除该 db 下**所有 RP** 的匹配数据；predicate 中没有
   `_measurement` 时删除整个 db 在时间区间内的所有 measurement（§3.19）。
5. **SIGHUP 语义变化**：旧版没有注册 SIGHUP handler，Unix 上的默认动作是**终止进程**
   （且不走优雅关闭）；新版会触发**配置重载**，但**作用域只有 HTTP 与 OpenTSDB 的 TLS 证书**：
   `ApplyReloadedConfig` 仅收集这两类 apply 闭包（`server.go:583-627`），其余配置项
   （`auth-enabled`、`max-body-size`、`[flux-controller]`、`[coordinator]` 等）**不会生效，也不报错**。
   即使证书重载也是两阶段：`LoadConfig`/`PrepareReloadConfig` 验证失败则完全不 apply；
   apply 阶段单个失败仍继续并 `errors.Join`（失败者保留旧证书）。**改证书以外的配置必须重启**。
6. **SIGTERM 查询转储**：需要排障时打开 `[coordinator] termination-query-log=true`，
   进程收到 SIGTERM 会把在跑的查询写进日志（`fd1d51690d`）。
7. **Flux 资源**：如启用 Flux 且希望限制资源，配置
   `[flux-controller]` 的 `query-concurrency`/`query-queue-size`/
   `query-max-memory-bytes`；不配置则保持旧的"不限制"行为。注意 5.1 的互斥约束，例如
   `query-concurrency > 0` 必须同时给 `query-queue-size > 0`，否则启动失败。

---

## 6. 具体案例

> 本节完整保留 6.1、6.3、6.6 三个代表案例；6.2、6.4、6.5 保留同编号摘要，完整请求/响应与 Flux 案例见 [06a 详细附录](06a_details_appendix.md)。

### 6.1 `/query` 请求的新旧对照

**请求**：

```
POST /query?db=telegraf&epoch=ms&time_format=rfc3339&chunked=true&chunk_size=1000
Authorization: Basic Z3JhZmFuYTpncmFmYW5h
Content-Type: application/x-www-form-urlencoded

q=SELECT mean(usage_user) FROM cpu WHERE time > now()-1h GROUP BY time(1m),host
```

**v1.8.10 处理**：

1. `serveQuery`（`v1.8.10:services/httpd/handler.go:496`）读 `q`、`epoch=ms`。
   `time_format` 参数**完全被忽略**。
2. 解析、授权、`ExecutionOptions{ReadOnly:false}`。
3. 写 200 + Flush（`:675-678`）。
4. 每个结果进入 `if epoch != "" { convertToEpoch(r, "ms") }`（`:689-691`），
   时间列变成**毫秒整数**。
5. chunked 分支逐块写出（`:694-700`），累加全局
   `QueryRequestBytesTransmitted`。
6. 客户端看到 `"values":[[1704067200000, 42.1]]`。

**v1.12.3 处理**：

1. `serveQuery`（`v1.12.3:services/httpd/handler.go:595`）先校验
   `time_format`（`:635-646`）：`rfc3339` 合法。
2. `userName` 先声明为空，`user != nil` 时 `userName = user.ID()`，写入 `ExecutionOptions.UserID`（`:724-737`）。
3. 写 200 + Flush（`:793-796`）。
4. 因为 `timeFormat != "epoch"`，走
   `convertToTimeFormat(r, time.RFC3339Nano)`（`:809-813`），
   `epoch=ms` **被忽略**。
5. chunked 写出后，除了全局计数器，还调用
   `h.addQueryBytesForUser(userName, n)`（`:820-821`）。
6. 客户端看到 `"values":[["2024-01-01T00:00:00Z", 42.1]]`。
7. 若把 `time_format` 写成 `RFC3339`（大写），直接 400
   `Time format must be one of the following: rfc3339,epoch`。

**结论**：同一条 URL 在两版返回**不同的时间表示**。这是升级时必须检查的
客户端兼容点。

### 6.2 `/write` 请求的新旧对照

V1 正常写入的 `Content-Length` 是 gzip **压缩后**长度，gzip 分支会用解压流覆盖 `truncateReader`；V2 `precision=xyz` 在旧版先写 400 后仍继续解析并写入，新版补 `return` 后干净返回 400。

> 完整案例见 [06a 附录 §6.2](06a_details_appendix.md#s6-2)

### 6.3 TLS 证书热重载案例

**初始状态**：`https-enabled=true`，`https-certificate=/etc/ssl/influxdb.pem`，
`https-private-key=/etc/ssl/influxdb.key`（key 权限 0600）。

**v1.8.10**：

```
$ kill -HUP <pid>        # 旧版未注册 SIGHUP handler，Unix 默认动作=终止进程（非优雅关闭）
$ # 换证书只能重启进程
$ systemctl restart influxdb
```

`Service.Open` 在启动时执行一次 `tls.LoadX509KeyPair`
（`v1.8.10:services/httpd/service.go:115`），结果固化进
`tls.Config.Certificates`；进程生命周期内无法更换。

**v1.12.3**：

```
# 1) 替换文件，保持权限正确
$ install -m 0600 new.key  /etc/ssl/influxdb.key
$ install -m 0644 new.pem  /etc/ssl/influxdb.pem

# 2) 发送 SIGHUP
$ kill -HUP <pid>
```

内部时序：

```mermaid
sequenceDiagram
    participant K as SIGHUP
    participant M as Main.Run
    participant C as Command.ReloadConfig
    participant S as Server.ApplyReloadedConfig
    participant H as httpd.Service
    participant TM as TLSConfigManager
    participant CL as TLSCertLoader

    K->>M: SIGHUP on reloadCh
    M->>C: ReloadConfig(args...)
    C->>C: LoadConfig → reloadedConfig
    alt LoadConfig 失败（解析/校验）
        C-->>M: log "error reloading config"；不调用 ApplyReloadedConfig，旧证书继续服务
    else 成功
        C->>S: ApplyReloadedConfig(reloadedConfig, log)
        S->>H: PrepareReloadConfig(config.HTTPD)
        alt https-enabled 变化
            H-->>S: error（"can not change https-enabled on a running server"）；verify 阶段 return，本次不 apply
        else https 未变
            H->>TM: PrepareCertificateLoad(cert, key)
            TM->>CL: PrepareLoad(cert, key)
            CL->>CL: loadCertificate：权限检查 + X509KeyPair + 解析 leaf
            alt 权限/证书校验失败（loadCertificate 返回错误，PrepareLoad 不返回 apply 闭包）
                CL-->>TM: error
                TM-->>H: 错误
                H-->>S: 错误
                Note over S,CL: ApplyReloadedConfig 立即 return；不执行任何 apply，旧证书继续服务
            else 校验通过
                CL-->>TM: apply 闭包（尚未替换）
                TM-->>H: apply
                H-->>S: apply
                S->>S: 全部服务验证通过
                S->>CL: apply()（闭包捕获 CL）→ SetCertificate(loadedCert)
                CL->>CL: 持锁替换 cert/leaf，checkCurrentCert 打过期告警
                Note over CL: 下一次 TLS 握手<br/>GetCertificate 返回新证书
            end
        end
    end
```

失败路径（例如新 key 权限 0644）：

1. `LoadCertificate` 的 `VerifyFilePermissivenessF` 返回
   `ErrPermissionsTooOpen`（`certconfig.go:120-125`）。
2. `PrepareLoad` 返回错误 → `PrepareReloadConfig` 包装为
   `"httpd: error loading certificate at (...)"`。
3. `ApplyReloadedConfig` **立即 return，不执行任何 apply**（`server.go:595-598`）。
4. 旧证书继续服务，日志里出现一条 ERROR。
5. 进程不退出，也不需要回滚。

**行为要点**：已有 keep-alive 连接不会被踢掉；只有新握手拿新证书。
想强制全部换证，仍然需要重启或等待连接自然断开。

### 6.4 `/debug/vars` 输出对照

旧版顶层为 `system`/`cmdline`/`memstats` + 各服务统计（`cq` 带 `{name,tags,values}`）；新版新增 `build`/`config`/`stats`/`crypto`，`cq` 改为扁平 `values`，并对照 `/metrics` 与 `/debug/vars` 的认证开关与内容。

> 完整案例见 [06a 附录 §6.4](06a_details_appendix.md#s6-4)

### 6.5 Flux：并发 4 个查询时的排队与 500

`query-concurrency=2`、`query-queue-size=1` 下并发 4 个 Flux 请求：2 个执行、1 个排队、1 个返回 **500** `queue length exceeded`；同时对照 Flux 关闭分支的 401/403 与 `qc_*` 指标。

> 完整案例见 [06a 附录 §6.5](06a_details_appendix.md#s6-5)

### 6.6 V2 bucket：create 的 400/201 与 `?id=` 授权缺口

**请求 A（body RP 冲突）**：

```http
POST /api/v2/buckets
Authorization: Token admin:secret
Content-Type: application/json

{"name":"telegraf/autogen","rp":"other"}
```

`bucket2dbrp("telegraf/autogen")` → `(telegraf, autogen)`；`brd.Rp="other"` 与 `rp` 不同，命中 `!((brd.Rp == rp) || (brd.Rp == ""))` → **400**，body `buckets - two retention policies specified: "autogen" differs from "other"`（`handler.go:1191-1193`）。

**请求 B（成功创建）**：

```http
POST /api/v2/buckets
Authorization: Token admin:secret
Content-Type: application/json

{"name":"telegraf/autogen"}
```

目标库 `telegraf` 不存在 → `AuthorizeCreateDatabase` 通过 → `CreateDatabaseWithRetentionPolicy(db, &spec)`，`spec.Name="autogen"`、`Duration=DefaultRetentionPolicyDuration`、`ReplicaN=1`；成功 **201** + `makeBucket(rpi, db)` 的 JSON（含 `id:"telegraf/autogen"`、`name`、`retentionRules`）。若库已存在，走 `CreateRetentionPolicy`，RP 已存在时 **400**。

**请求 C（单桶读取的授权缺口）**：

```http
GET /api/v2/buckets?id=telegraf/autogen
Authorization: Token lowpriv:secret
```

`serveListBucketsV2` 在 `AuthEnabled` 时只检查 `user != nil`（`handler.go:1429-1434`）；`id`/`name` 分支直接 `oneBucket(w, name)`，而 `oneBucket` 只做 `bucket2dbrp` + 存在性检查，**不调用 `AuthorizeDatabase`**（`handler.go:1439-1452`、`:1529-1543`）。结果：已认证但无 `telegraf` 读权限的用户得到 **200** + bucket 元数据。普通 list（无 `id`/`name`）才逐库 `AuthorizeDatabase(Read)` 并跳过无权限库（`:1512-1518`）。

**body 边界**：create/delete/update 都只做 `ContentLength > MaxBodySize` 预检；chunked（`ContentLength=-1`）时 `buf.ReadFrom(r.Body)` 无截断（`handler.go:1160-1173`、`:1004-1021`），风险 5。

---


## 7. 仍存在的风险

1. **`/api/v2/write` 不受写限流**。`AddRoutes` 的 switch 仍只匹配
   `/write` 与 `/api/v1/prom/write`（`v1.12.3:services/httpd/handler.go:528-535`）。
   V2 写入与 V1 共用 `serveWrite`，因此这是纯粹的保护缺口。
2. **`/debug/vars`、`/debug/requests` 的认证取决于开关组合**。
   当 `AuthEnabled=false`、`PprofAuthEnabled=false` 或 `PprofEnabled=false` 时
   （`/debug/vars`、`/debug/requests` 前缀分支不检查 `PprofEnabled`），
   `ServeHTTP` 的前缀分支直接分发（`handler.go:567-577`），
   新增的 `config`/`crypto` 板块也随之暴露。
3. **`debug-pprof-enabled` 是第二个暴露面**。它会在 `localhost:6060`
   另起 `DefaultServeMux` 服务（`cmd/influxd/run/command.go` 启动前逻辑），
   不受 `AuthEnabled`/`PprofEnabled` 约束。
4. **chunked gzip 写入绕过 `max-body-size`**。`truncateReader` 被
   `gzip.NewReader(r.Body)` 覆盖（`handler.go:1701-1715`），
   而 `ContentLength == -1` 时预检也被跳过。
5. **新增 V2 端点没有 body 截断**。`/api/v2/delete`、`/api/v2/buckets`
   只做 `ContentLength` 预检（`handler.go:1004-1021`、`:1160-1173`）。
6. **`prom-read-auth-enabled` 默认关闭**：即使 `auth-enabled=true`，
   Prometheus remote read 仍只做认证、不做库级 READ 授权
   （`handler.go:2055` 的 `&& h.Config.PromReadAuthEnabled`）。
   多租户部署应显式打开。
7. **`servePromRead` 无资源边界**：`io.ReadAll` + `context.Background()`
   （`handler.go:2033`、`:2097`），既不限大小也不随客户端取消。
8. **Flux 资源错误映射为 500**。`query/control` 用
   `flux.Error{Code: ResourceExhausted}` 表示队列满，但
   `serveFluxQuery` 一律 `http.StatusInternalServerError`
   （`handler.go:2216-2220`），运维难以区分"服务端 bug"与"该扩容了"。
9. **`queryBytesPerUser` 只增不减**。按用户名做 key，
   用户被删除后计数仍留在 map 中（`handler.go:153`），
   属于可控但无界的增长。
10. **TLS 权限检查是破坏性变更**。默认 `https-insecure-certificate=false`
   （`config.go:88`），大量历史部署的 key 文件是 0644，
   升级后会直接 `Service.Open` 失败。
11. **`Close` 与 Prometheus collector**。`Handler.Close` 不再
    `prom.Unregister`（旧版在 `v1.8.10:services/httpd/handler.go:343-348`），
    collector 只在 `server.go:346` 启动时注册一次；测试或多次
    `NewService`/`Open` 场景下需要注意重复注册。
12. **`prepareReloadConfig` 的互斥粒度**。它持有 `Service.mu` 做证书文件 IO
    （`service.go:297-318`），大文件或慢磁盘会短暂阻塞 `Addr()`/`Close()`。
13. **`https-enabled` 运行中不可改**。验证阶段返回 `httpd: can not change https-enabled on a running server`
    （`services/httpd/service.go:300-303`），`ApplyReloadedConfig` 打印
    `error reloading httpd service config, no new configuration applied` 后直接返回
    （`cmd/influxd/run/server.go:592-600`），**整次 reload 一个配置都不 apply**；command 层再打
    `error applying reloaded config; some config values may have been applied`
    （`cmd/influxd/run/command.go:201-206`）。进程不退出、旧配置继续服务；由于只是 Error 级日志、
    没有非 0 退出码，自动化里容易漏掉。
14. **`/api/v2/query` 请求体不受 `max-body-size` 限制**。`decodeQueryRequest`
    对 Flux 直接 `io.ReadAll`、对 JSON 直接 `json.Decoder`（`handler.go:2186`、
    `flux.go:25-54`），`MaxBodySize` 只在 `/write`、`/api/v1/prom/write` 与
    V2 delete/buckets 使用（`handler.go:1006/1162/1330/1702/1931`）。
    Flux 端点应在外层加 `http.MaxBytesReader`。
15. **Flux 控制器的关停未接线，且排队查询不随客户端断开取消**。`query/control.Controller.Shutdown`
    （`controller.go:516-560`）只被测试调用，`Server.Close` 不调用它；`Shutdown` 本身会
    `q.Cancel()` 所有查询，但生产路径永远不会走到。三段可复核的行为：
    ①`createQuery` 用 `context.WithCancel(ctx)` 派生查询 ctx（`:292`），执行中的查询由
    `q.pump(exec, ctx.Done())` 观察取消并 `exec.Cancel()`（`:481`、`:896-930`）；
    ②`enqueueQuery` 只做 `select { case c.queryQueue <- q: default: ResourceExhausted }`，
    **没有 `ctx.Done()` 分支**（`:386-424`），所以客户端断开的查询仍占队列槽；
    ③worker 取到它（`processQueryQueue`，`:427-437`）后，`tryExec` 发现 parent ctx 已取消，
    直接转 `Canceled`、关闭结果，handler 阻塞的 `encoder.Encode`/`q.Done()` 此时才返回。
    案例：`query-concurrency=1`、`query-queue-size=1`，Q2 排队时断开 → `qc_queueing_active`
    仍为 1，直到 Q1 结束、Q2 出队后才归零，handler goroutine 也一直挂着。
    “依赖进程退出”只适用于未出队的查询，不适用于执行中的查询。
16. **`/metrics` 默认无认证**。`ping-auth-enabled` 默认 false（`services/httpd/config.go:84`），
    `authWrapper` 对 `/metrics` 退化为 2 参直达（`handler.go:182-191`、`:315-317`）；
    Flux 开启时 `qc_*` 指标（队列/内存/请求量）对任何能访问 8086 的人可见。
    若启用 `ping-auth-enabled=true`，`/metrics` 会随 ping 一起要求认证，但权限仍是“任意已认证用户”，没有管理员限制。
17. **`/debug/vars` 的 `cq` 空 values 返回 200 截断 JSON**。`parseCQStatistics`
    在 `len(Values)==0` 时返回 error（`handler.go:2728-2734`），而 `serveExpvar`
    已经写出 `{` 与前置板块，`httpError(500)` 无法改状态码（`handler.go:2452-2458`）。
    OSS 的 CQ 总会填 `queryOk`/`queryFail`，企业扩展返回空 values 时才可达；监控端需要按 body 完整性判断，不能只看 200。
18. **V2 单桶读取没有授权**。`GET /api/v2/buckets?id=<db/rp>` 只检查 `user != nil`，
    `oneBucket` 不调用 `AuthorizeDatabase`（`handler.go:1429-1452`、`:1529-1543`）；
    已认证低权用户可读任意 bucket 元数据。普通 list 有逐库 READ 过滤（`:1512-1518`），
    两条路径的授权语义不一致。
19. **Prom write 静默丢弃 NaN/±Inf**。`WriteRequestToPoints` 返回 `DroppedValuesError`
    时 `servePromWrite` 只记录日志/继续写有效点，最终返回 **204**（`handler.go:1990-2000`、
    `prometheus/converters.go:60-97`）。客户端无法从状态码知道部分样本被丢；应监控
    `prom_metric` 写入量与发送量差异，或在上游过滤非有限值。

---

## 8. 证据索引

### 8.1 services/httpd

| 主题 | v1.8.10 | v1.12.3 |
|------|---------|---------|
| 路由注册（基础） | `services/httpd/handler.go:186-243` | `services/httpd/handler.go:193-318` |
| 路由注册（pprof） | `services/httpd/handler.go:247-285` | `services/httpd/handler.go:322-365` |
| 路由注册（Flux） | `services/httpd/handler.go:287-299` | `services/httpd/handler.go:367-381` |
| `AddRoutes` | `services/httpd/handler.go:415-453` | `services/httpd/handler.go:514-552` |
| `ServeHTTP` 分发 | `services/httpd/handler.go:456-482` | `services/httpd/handler.go:555-580` |
| `Statistics` 结构 | `services/httpd/handler.go:352-378` | `services/httpd/handler.go:422-448` |
| `Statistics()` | `services/httpd/handler.go:381-412` | `services/httpd/handler.go:451-501` |
| 用户字节统计 | 无 | `services/httpd/handler.go:483-511` |
| `serveQuery` | `services/httpd/handler.go:496-791` | `services/httpd/handler.go:595-915` |
| `time_format` | 无（仅 `epoch`，`:534`） | `services/httpd/handler.go:635-646`、`:806-813` |
| `serveWriteV2` | `services/httpd/handler.go:846-866` | `services/httpd/handler.go:1627-1648` |
| `serveWrite` | `services/httpd/handler.go:887-1041` | `services/httpd/handler.go:1670-1806` |
| `valuesWrittenOK` 路径 | `services/httpd/handler.go:989-1010` | 已删除（`:1772`） |
| `serveDeleteV2` | 无 | `services/httpd/handler.go:975-1111` |
| bucket 端点族 | 无 | `services/httpd/handler.go:1159-1624` |
| `servePromWrite` | `services/httpd/handler.go:1115-1244` | `services/httpd/handler.go:1897-2026` |
| `servePromRead` | `services/httpd/handler.go:1248-1372` | `services/httpd/handler.go:2030-2173` |
| `serveFluxQuery` | `services/httpd/handler.go:1374-1454` | `services/httpd/handler.go:2175-2261` |
| `serveFluxQueryDisabled` | 无（匿名函数，`:292-295`） | `services/httpd/handler.go:2263-2266` |
| `serveExpvar` | `services/httpd/handler.go:1479-1572` | `services/httpd/handler.go:2291-2524` |
| `parseConfigDiagnostics` | 无 | `services/httpd/handler.go:2703-2726` |
| `parseCQStatistics` | 无 | `services/httpd/handler.go:2728-2734` |
| `ossCryptoDiagnostics` | 无 | `services/httpd/handler.go:2654-2661` |
| `httpError` | `services/httpd/handler.go:1675-1698` | `services/httpd/handler.go:2737-2760` |
| `parseToken` | `services/httpd/handler.go:1709-1715` | `services/httpd/handler.go:2771-2776` |
| `authenticate` | `services/httpd/handler.go:1776-1870` | `services/httpd/handler.go:2837-2931` |
| pprof 授权 nil 修复 | `services/httpd/handler.go:251-252` | `services/httpd/handler.go:326-334` |
| `Store` 接口 | `services/httpd/handler.go:2025-2027` | `services/httpd/handler.go:3088-3092` |
| `logging`/`recovery` | `services/httpd/handler.go:1960`、`:1998` | `services/httpd/handler.go:3023`、`:3061` |
| `Config` | `services/httpd/config.go:34-67` | `services/httpd/config.go:34-70` |
| `NewConfig` | `services/httpd/config.go:70-91` | `services/httpd/config.go:72-98` |
| `Config.Diagnostics` | `services/httpd/config.go:94-109` | `services/httpd/config.go:100-118` |
| `StatusFilters.Match` | `services/httpd/config.go:189-200` | `services/httpd/config.go:198-206` |
| `Service` 结构 | `services/httpd/service.go:51-72` | `services/httpd/service.go:60-114` |
| `Service.Open` | `services/httpd/service.go:108-196` | `services/httpd/service.go:149-236` |
| `Service.Close` | `services/httpd/service.go:198-220` | `services/httpd/service.go:239-285` |
| TLS 管理器装配 | `services/httpd/service.go:114-128`（LoadX509KeyPair） | `services/httpd/service.go:162-174` |
| `PrepareReloadConfig` | 无 | `services/httpd/service.go:296-322` |
| `decodeQueryRequest` | `services/httpd/flux.go:25-53` | `services/httpd/flux.go:25-53` |
| CSV tag 列 | `services/httpd/response_writer.go:233` | `services/httpd/response_writer.go:228` |
| `WriteError` | `services/httpd/response_writer.go:75-78` | 已删除 |
| `handleProfiles` | `services/httpd/pprof.go:22-35` | `services/httpd/pprof.go:22-35` |

| 认证主链 `authenticate` | `services/httpd/handler.go:1776-1870` | `services/httpd/handler.go:2837-2931` |
| `parseCredentials` | `services/httpd/handler.go:1709-1769` | `services/httpd/handler.go:2785-2830` |
| `authWrapper`（ping/metrics） | 无（直接 handler） | `services/httpd/handler.go:182-191` |
| pprof 管理员检查 | `services/httpd/handler.go:251-252` | `services/httpd/handler.go:326-334` |
| Prom read 授权 | 无（配置存在但未使用） | `services/httpd/handler.go:2055-2064` |
| V2 单桶读取（无授权） | 无 | `services/httpd/handler.go:1429-1452`、`:1529-1543` |
| `/debug/vars` 输出顺序 | `services/httpd/handler.go:1479-1572` | `services/httpd/handler.go:2291-2524` |
| `/debug/vars` `uniqueKeys` | 无 | `services/httpd/handler.go:2509-2514` |
| monitor 诊断注册 | `monitor/service.go:105-113` | `monitor/service.go:146-166` |
| 压缩吞吐诊断 | 无 | `monitor/stats.go:12-34` |
| `/metrics` 路由 | `services/httpd/handler.go:229` | `services/httpd/handler.go:315-317` |
| Flux HTTP 入口 | `services/httpd/handler.go:1374-1454` | `services/httpd/handler.go:2175-2261` |
| Flux 关闭分支 | `services/httpd/handler.go:292-295`（匿名） | `services/httpd/handler.go:2263-2266`（3 参） |
| Flux 入口错误映射 | 无控制器配额 | `services/httpd/handler.go:2216-2220` |
| Flux 队列/内存配额 | 无 | `query/control/controller.go:175-227`、`:386-424`、`memory.go:73-116` |
| V2 bucket `bucket2dbrp` | 无 | `services/httpd/handler.go:942-968` |
| V2 bucket create 授权 | 无 | `services/httpd/handler.go:1159-1265` |
| TLS 巡检常量 | 无 | `pkg/tlsconfig/certconfig.go:28-29` |
| TLS 巡检 goroutine | 无 | `pkg/tlsconfig/certconfig.go:247-274`、`:512-533` |
| Prom write/read 全文 | `services/httpd/handler.go:1115-1372` | `services/httpd/handler.go:1897-2173` |
| Prom 转换字段 | `prometheus/converters.go:49-97` | `prometheus/converters.go:49-129` |

### 8.2 pkg/tlsconfig 与 pkg/file

| 主题 | v1.8.10 | v1.12.3 |
|------|---------|---------|
| 包内容 | 仅 `tls_config.go` | `tls_config.go`、`configmanager.go`、`certconfig.go` |
| 证书加载 | 无（httpd 直接 `tls.LoadX509KeyPair`） | `pkg/tlsconfig/certconfig.go:83-170` |
| 权限检查 | 无 | `pkg/tlsconfig/certconfig.go:107-131` |
| `TLSCertLoader` | 无 | `pkg/tlsconfig/certconfig.go:174-210` |
| `NewTLSCertLoader` | 无 | `pkg/tlsconfig/certconfig.go:247-274` |
| `SetupTLSConfig` | 无 | `pkg/tlsconfig/certconfig.go:318-324` |
| `GetCertificate` | 无 | `pkg/tlsconfig/certconfig.go:329-339` |
| `GetClientCertificate` | 无 | `pkg/tlsconfig/certconfig.go:344-362` |
| `Load` / `PrepareLoad` | 无 | `pkg/tlsconfig/certconfig.go:378-407`、`:412-424` |
| 过期巡检 | 无 | `pkg/tlsconfig/certconfig.go:512-533` |
| `TLSConfigManager` | 无 | `pkg/tlsconfig/configmanager.go:24-27` |
| `NewTLSConfigManager` | 无 | `pkg/tlsconfig/configmanager.go:286-296` |
| `Listen` | 无 | `pkg/tlsconfig/configmanager.go:340-346` |
| `PrepareCertificateLoad` | 无 | `pkg/tlsconfig/configmanager.go:368-378` |
| 权限比较 | 无 | `pkg/file/file.go:58-88` |
| `tls_config.go.Parse` | `pkg/tlsconfig/tls_config.go:25-65` | `pkg/tlsconfig/tls_config.go:25-65` |

### 8.3 cmd / monitor / query / storage / prometheus

| 主题 | v1.8.10 | v1.12.3 |
|------|---------|---------|
| 信号处理 | `cmd/influxd/main.go:85-104`（无 SIGHUP） | `cmd/influxd/main.go:85-104`（新增 `reloadCh`） |
| `ReloadConfig` | 无 | `cmd/influxd/run/command.go:194-208` |
| `ApplyReloadedConfig` | 无 | `cmd/influxd/run/server.go:583-627` |
| `Monitor.New` | `monitor/service.go:74` | `monitor/service.go:114` |
| 诊断注册 | `monitor/service.go:105-113` | `monitor/service.go:146-166` |
| 压缩吞吐诊断 | 无 | `monitor/stats.go:12-34` |
| 全局 tag 并发 | 裸 map（`monitor/service.go:46`、`:189`） | `tags` 结构（`monitor/service.go:37-68`、`:251`） |
| `SHOW QUERIES` 列 | `query/task_manager.go:152` | `query/task_manager.go:34`、`:152` |
| `SHOW QUERIES` 越权过滤 | 无（`:129`） | `query/task_manager.go:136-147` |
| `ExecutionOptions.UserID` | 无 | `query/executor.go:205-206` |
| Flux Controller | `flux/control/controller.go:73-105` | `query/control/controller.go:175-227` |
| Flux 内存管理 | 无 | `query/control/memory.go:11-156` |
| Flux 指标 | 无 | `query/control/metrics.go:6-95` |
| Flux 读入口 | `storage/reads/flux_reader.go:30-94` | `storage/flux/reader.go:61-117` |
| WindowAggregate 读 | 无 | `storage/flux/reader.go:85-93`、`:609-838` |
| Flux 表实现 | `storage/reads/flux_table.go`、`flux_table.gen.go` | `storage/flux/table.go:17-240`、`table.gen.go` |
| `Store.ReadFilter` | `services/storage/store.go:109-144` | `services/storage/store.go:126-174` |
| `Store.ReadGroup` | `services/storage/store.go:147-188` | `services/storage/store.go:177-231` |
| `Store.WindowAggregate` | 无 | `services/storage/store.go:234-277` |
| `Store.Delete` | 无 | `services/storage/store.go:500-502` |
| `Store.DeleteRetentionPolicy` | 无 | `services/storage/store.go:504-506` |
| `GetReadSource` | `services/storage/store.go:28-34` | `services/storage/store.go:28-37` |
| `validateArgs` 时间归一化 | `services/storage/store.go:85-108` | `services/storage/store.go:88-110` |
| Prom 转换 | `prometheus/converters.go:49-97` | `prometheus/converters.go:49-97` |
| Prom 协议类型 | `prometheus/remote/*`（已删除） | 上游 `github.com/prometheus/prometheus/prompb` |
| `PartialWriteError` | `tsdb/shard.go:113-123` | `tsdb/shard.go:121-140` |
| CQ 诊断接口 | 无 | `services/continuous_querier/service.go:173-177` |
| CQ 统计写失败日志 | 无（`:415`） | `services/continuous_querier/service.go:418-428` |
| Flux 指标去向 | 无 | 经 `Server.Statistics` + `PrometheusToStatistics` 进入 `SHOW STATS`/`/debug/vars`（`cmd/influxd/run/server.go:270-273`） |

### 8.4 关键 commit

| commit | PR | 内容 |
|--------|----|------|
| `2fe52d8edc` | #23247 | 实现 `/api/v2/delete` |
| `ce40acb298` | #23284 | `/api/v2/buckets` 部分支持 |
| `be9a3d4a07` | #23898 | `:db/:rp` → `:dbrp`，修 create/update bucket |
| `b0e44a4659` | #23879 | listBuckets 返回包装结构 |
| `f365bb7e3a` | #24145 | V2 delete 使用 nil Sources |
| `c31e99574f` | #24141 | V2 delete 不设置 measurement 的 db/rp |
| `e3e9232de5` | #25407 | V2 delete 剥离 measurement 双引号 |
| `2dc3dcb3d1` | #24311 | CSV 输出不转义 |
| `db523227a2` | #25669 | `/debug/vars` 增加 `fluxQueryRespBytes` |
| `6795ec6c01` | — | 移除 context-value 写法，仍由 httpd 累加 `valuesWrittenOK` |
| `117341fb0f` | — | 将 values 统计从 httpd 下移到 tsdb store |
| `32d8b002d8` | #26981 | `SHOW QUERIES`/日志显示用户 |
| `f75be56eee` | #26874 | `/debug/vars` 的 `cq` 改为扁平输出 |
| `269f1b6281` | #26624 | `/debug/vars` 增加 `config`（TSDB 配置） |
| `b9a0155ca9` | #26596 | 新增 `time_format` |
| `a2e029fa7f` | #26917 | 默认配置输出包含 `access-log-status-filters` |
| `f8635640cc` | #26292 | JWT 换 `golang-jwt/jwt/v4` |
| `aa1775bfc2` | #27057 | SIGHUP 证书重载 + 文件权限检查 |
| `e3b058616c` | #27090 | `TLSCertLoader` 客户端证书 |
| `049b1f7e00` | #27216 | 引入 `TLSConfigManager` |
| `ba705ec658` | #27219 | 修 `https-insecure-certificate` 语义 |
| `4dcc8776c1` | #27188 | 用户维度查询字节；pprof nil 用户修复 |
| `2b7164440f` | #27196 | `SHOW QUERIES` 越权过滤 |
| `a6a3bdd0f0` | #25308 | `Config.Diagnostics` 增加 `flux-enabled` |
| `0e656d6dd5` | #26754 | compact-throughput 统计测量 |
| `997698f891` | #21746 | V1/V2 非法 precision 补 `return` |
| `8843a01ebf` | — | `/debug/vars` 重复键追加 `:N` 后缀 |
| `e8f7b78d68` | — | CORS Allow-Methods 增加 `PATCH` |
| `0e458e7175` | — | CORS Allow-Headers 增加 `User-Agent` |
| `57ea78e984` | #18429 | Prometheus remote read 认证与计数 |
| `fd1d51690d` | #22638 | SIGTERM 时可选转储当前查询 |
| `c4bfaa8954` | #25146 | `Store.validateArgs` 不再覆盖显式 start/end |

---

### 交叉引用

- [10 HTTP 服务层](../10_http_service_spec.md)：路由表、中间件、认证、限流、
  `/api/v2/delete`、Prometheus 端点、pprof 开关矩阵。
- [09 Storage Reads](../09_storage_reads_spec.md)：`Store` 接口、ResultSet、
  Predicate、多 shard 游标、window aggregate 下推。
- [11 InfluxQL 执行](../11_influxql_execution_spec.md)：`serveQuery` 之后的
  解析/编译/执行/取消链、`SHOW QUERIES` 的语句分发。
- [04 Background Tasks](../04_background_tasks_spec.md)：CQ 统计与
  `/debug/vars` 中 `cq` 板块的关系。
- [05 元数据与后台](05_meta_background_diff.md)：CQ 的 `lastRuns`/lease/写窗口后果（§3.15c）、snapshotter/subscriber。
- [03 查询引擎](03_query_engine_diff.md)：Flux 控制器状态机/内存配额（§3.1）、`storage/reads` 契约（§3.7）、`table.gen.go`（§3.11）与 window aggregate 案例（§6.3）；计划下推规则见 §3.10。
- **本篇不重复分析**：`query/control` 内部状态机、`storage/flux` 读路径、window aggregate 算法、04 的 purger/compaction；这些内容以 03/04/09/16 为准。
