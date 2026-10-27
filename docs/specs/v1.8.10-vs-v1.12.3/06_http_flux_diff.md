# 版本差异 6: HTTP 服务、TLS 与 Flux 读路径（v1.8.10 → v1.12.3）

> **版本基线**
>
> - OLD：`v1.8.10` = commit `688e697c51`（2021-10-11）
> - NEW：`v1.12.3` = commit `f3d5cb68ab`（`docs/specs/` 这批文档的代码基准）
> - 区间：527 commits，602 files，+79,310 / −45,239
> - NEW 的当前工作树只额外包含 4 个 docs 提交，代码与 `f3d5cb68ab` 相同。
>
> 引用格式：旧 `v1.8.10:path:line`，新 `v1.12.3:path:line`（即当前工作树行号）。
> 新版本行为的详细讲解见 [10 HTTP 服务层](../10_http_service_spec.md)、
> [09 Storage Reads](../09_storage_reads_spec.md)、[11 InfluxQL 执行](../11_influxql_execution_spec.md)。

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
>    （压缩吞吐占用率）、`cq`（连续查询计数）。
> 4. **写查询的人和查查询的人对上了号**：`SHOW QUERIES` 多一列 `user`，
>    并且非管理员只能看到自己有读权限的库上的查询（修掉一个越权泄漏）。
> 5. **Flux 从"能跑"变成"有预算"**：旧版 Flux 控制器是 `flux/control`
>    里的薄壳，内存不设限；新版换成 `query/control`，有并发配额、单查询内存配额、
>    全局内存池和排队队列，读路径也搬家到 `storage/flux`。
> 6. **不会自动变好的地方**：`/api/v2/write` 仍然不经过写限流；
>    `/debug/vars`、`/debug/requests` 在特定开关组合下仍然无认证。

---

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
| 17 | 压缩吞吐诊断 | 无 | `monitor/stats.go` 注册 `stats` 诊断 | 可观测 compaction 限流占用 | `v1.12.3:monitor/service.go:156`、`monitor/stats.go:9` |
| 18 | TSDB 配置诊断 | 无 | `monitor` 注册 `config`（`tsdb.Config`） | `/debug/vars` 可见 3 个新 TSDB 字段 | `v1.12.3:monitor/service.go:164`、`tsdb/config.go:304-306` |
| 19 | CQ 诊断 | 无 | CQ 服务新增空实现的 `Diagnostics()`（注册名 `cq`，当前不返回诊断值，仅满足 Monitor 接口） | `/debug/vars` 的 `cq` 两版都有，新版只是扁平化 | `v1.12.3:services/continuous_querier/service.go:175`、`cmd/influxd/run/server.go:426` |
| 20 | Prometheus 协议类型 | 自带 `prometheus/remote`（gogo/protobuf） | 上游 `prompb`（google.golang.org/protobuf） | 线格式不变，字段名 `TimestampMs`→`Timestamp` | `v1.8.10:services/httpd/handler.go:1192` / `v1.12.3:services/httpd/handler.go:1974` |
| 21 | Flux 控制器 | `flux/control`，无内存/并发限制 | `query/control`，并发配额/内存配额/队列/指标 | 资源可控，错误码变化 | `v1.8.10:flux/control/controller.go:73` / `v1.12.3:query/control/controller.go:175` |
| 22 | Flux 读路径 | `storage/reads/flux_reader.go`，无 window aggregate | `storage/flux/*`，新增 `ReadWindowAggregate` | 下推能力增强 | `v1.8.10:storage/reads/flux_reader.go:30` / `v1.12.3:storage/flux/reader.go:61`、`:85` |
| 23 | `Store` 接口 | 只有 `ReadFilter` | 增加 `Delete`、`DeleteRetentionPolicy` | V2 删除/桶删除落地 | `v1.8.10:services/httpd/handler.go:2025` / `v1.12.3:services/httpd/handler.go:3088`-`:3092` |
| 24 | 写入统计 `valuesWrittenOK` | 存在（`WritePointsWithContext` + context 计数） | 移除（连同 `WritePointsWithContext` 路径） | `SHOW STATS` 少 1 个（valuesWrittenOK）、多 1 个（fluxQueryRespBytes），promReadReq 从恒 0 变真实计数 | `v1.8.10:services/httpd/handler.go:989`-`:1010` / `v1.12.3:services/httpd/handler.go:1772` |
| 25 | CSV 输出转义 | `HashKey()`（转义 tag） | `HashKey(false)`（不转义） | CSV tag 列不再被二次转义 | `v1.8.10:services/httpd/response_writer.go:233` / `v1.12.3:services/httpd/response_writer.go:228` |
| 26 | JWT 库 | `dgrijalva/jwt-go/v4` | `golang-jwt/jwt/v4` | API 兼容，依赖换血 | `v1.8.10:services/httpd/handler.go:26` / `v1.12.3:services/httpd/handler.go:24` |
| 27 | Service 关闭/重开 | `Close()` 直接关 listener；重复调用会重复关闭 | `sync.OnceValue(doClose)` 幂等；`Open` 在 `closed` 后报错 | 生命周期更安全 | `v1.8.10:services/httpd/service.go:199` / `v1.12.3:services/httpd/service.go:137`、`:155`、`:239` |

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

    MUX --> MW["AddRoutes 包装链<br/>handler.go:514"]
    MW --> REC["recovery"]
    REC --> LOG["logging"]
    LOG --> RID["requestID"]
    RID --> CORS["cors"]
    CORS --> HDR["SetHeadersHandler"]
    HDR --> GZ["gzipFilter（Gzipped=true 时）"]
    GZ --> RW["responseWriter"]
    RW --> TH{"Method==POST 且 Pattern 为 /write 或 /api/v1/prom/write?"}
    TH -->|"是"| THR["writeThrottler.Handler"]
    TH -->|"否"| AUTH
    THR --> AUTH["authenticate（handler 签名带 meta.User 时）"]
    AUTH --> H1["serveQuery / serveWriteV1 / serveWriteV2<br/>serveDeleteV2 / servePostCreateBucketV2"]
    AUTH --> H2["servePromWrite / servePromRead"]
    AUTH --> H3["serveFluxQuery / serveFluxQueryDisabled"]
    AUTH --> H4["servePing / serveHealth / serveStatus"]
    H1 --> RESP["Response / httpError"]
    H2 --> RESP
    H3 --> RESP
    H4 --> RESP
```

### 2.2 TLS 证书生命周期（旧 vs 新）

```mermaid
flowchart LR
    subgraph OLD["v1.8.10"]
        O1["Service.Open"] --> O2["tls.LoadX509KeyPair(cert,key)"]
        O2 --> O3["tlsConfig.Certificates = [cert]"]
        O3 --> O4["tls.Listen(addr, tlsConfig)"]
        O5["SIGHUP"] --> O6["未注册 handler<br/>默认动作=终止进程"]
    end

    subgraph NEW["v1.12.3"]
        N1["Service.Open"] --> N2["tlsconfig.NewTLSConfigManager"]
        N2 --> N3["NewTLSCertLoader(cert,key)"]
        N3 --> N4["LoadCertificate：权限检查 + X509KeyPair + 解析 leaf"]
        N4 --> N5["SetupTLSConfig：GetCertificate / GetClientCertificate"]
        N5 --> N6["TLSConfigManager.Listen → tls.Listen"]
        N7["SIGHUP"] --> N8["Main.Run reloadCh"]
        N8 --> N9["Command.ReloadConfig"]
        N9 --> N10["Server.ApplyReloadedConfig"]
        N10 --> N11["Service.PrepareReloadConfig"]
        N11 --> N12["TLSConfigManager.PrepareCertificateLoad"]
        N12 --> N13["TLSCertLoader.PrepareLoad（预校验）"]
        N13 --> N14["apply: SetCertificate（原子替换）"]
        N14 --> N5
        N3 --> N15["monitorCert：每小时巡检过期"]
    end
```

---

## 3. 逐项差异

### 3.1 路由表逐条对照

旧版路由集中在 `NewHandler` 的两段 `AddRoutes` 调用中：基础路由
`v1.8.10:services/httpd/handler.go:186-243`，pprof 条件路由 `:247-285`，
Flux 路由 `:287-299`。新版对应 `v1.12.3:services/httpd/handler.go:193-318`、
`:322-365`、`:367-381`。

| 方法 | 路径 | v1.8.10 | v1.12.3 | 差异 |
|------|------|---------|---------|------|
| OPTIONS | `/query` | ✅ `serveOptions` | ✅ | 不变 |
| GET/POST | `/query` | ✅ `serveQuery` | ✅ | 新增 `time_format`、用户字节统计 |
| OPTIONS | `/write` | ✅ `serveOptions` | ✅ | 不变 |
| POST | `/write` | ✅ `serveWriteV1` | ✅ | 移除 `valuesWrittenOK` 统计路径 |
| POST | `/api/v2/write` | ✅ `serveWriteV2` | ✅ | 修掉 precision 错误后未 `return` 的缺陷 |
| OPTIONS | `/api/v2/write` | ❌ | ✅ | **新增**（`handler.go:278-281`） |
| POST | `/api/v2/delete` | ❌ | ✅ `serveDeleteV2` | **新增**（`:214-217`） |
| POST | `/api/v2/buckets` | ❌ | ✅ `servePostCreateBucketV2` | **新增**（`:218-221`） |
| DELETE | `/api/v2/buckets/:dbrp` | ❌ | ✅ `serveDeleteBucketV2` | **新增**（`:222-225`） |
| GET | `/api/v2/buckets/:dbrp` | ❌ | ✅ `serveRetrieveBucketV2` | **新增**（`:226-229`） |
| GET | `/api/v2/buckets` | ❌ | ✅ `serveListBucketsV2` | **新增**（`:230-233`） |
| PATCH | `/api/v2/buckets/:dbrp` | ❌ | ✅ `serveUpdateBucketV2` | **新增**（`:234-237`） |
| GET/POST/DELETE | `/api/v2/buckets/:dbrp/labels[/:labelID]` | ❌ | ✅ `serveLabelsNotAllowedV2` | **新增**，统一 405（`:238-249`） |
| GET/POST/DELETE | `/api/v2/buckets/:dbrp/members[/:userID]` | ❌ | ✅ `serveBucketMembersNotAllowedV2` | **新增**，统一 405（`:250-261`） |
| GET/POST/DELETE | `/api/v2/buckets/:dbrp/owners[/:userID]` | ❌ | ✅ `serveBucketOwnersNotAllowedV2` | **新增**，统一 405（`:262-273`） |
| POST | `/api/v1/prom/write` | ✅ `servePromWrite` | ✅ | 协议类型换成 `prompb` |
| POST | `/api/v1/prom/read` | ✅ `servePromRead` | ✅ | 协议类型换成 `prompb` |
| GET/HEAD | `/ping` | ✅ `servePing` | ✅ | 不变 |
| GET/HEAD | `/status` | ✅ `serveStatus`（已废弃） | ✅ | 不变 |
| GET | `/health` | ✅ | ✅ | 路由 Name 由 `ping` 改为 `health`（影响访问日志 route 字段） |
| OPTIONS | `/health` | ❌ | ✅ `serveOptions` | **新增**（`:310-313`） |
| GET | `/metrics` | ✅ `promhttp` | ✅ | 不变 |
| POST | `/api/v2/query` | ✅（FluxEnabled 决定 handler） | ✅ | 关闭分支改走认证；新增字节统计 |
| OPTIONS | `/api/v2/query` | ❌ | ✅ `serveOptions` | **新增**（`:371-374`） |
| GET | `/debug/pprof/{cmdline,profile,symbol,all}` | ✅ 条件注册 | ✅ 条件注册 | 不变（认证分支修了 nil panic） |
| GET | `/debug/vars` | ✅ 条件注册 | ✅ 条件注册 | 内容大幅扩展 |
| GET | `/debug/requests` | ✅ 条件注册 | ✅ 条件注册 | 不变 |

关键点：

- 新增路由全部是**兼容面**，没有删除任何旧路由；因此 v1.8.10 的客户端脚本不需要改。
- `/api/v2/buckets*` 用 **pat** 风格参数 `:dbrp`，源码用
  `r.URL.Query().Get(":dbrp")` 读取（`v1.12.3:services/httpd/handler.go:1265-1277`、
  `:1280`、`:1313`、`:1390`），不是 Go 1.22 的 `r.PathValue`。
- `labels/members/owners` 九条路由是"占位拒绝"，返回 405
  `"not supported in this version"`（`handler.go:1609-1624`），用于让 Terraform /
  influx CLI 之类的 V2 客户端得到明确答案而不是 404。

### 3.2 中间件链与限流

`AddRoutes` 的包装顺序在 1.8.10 与 1.12.3 完全一致
（`v1.8.10:services/httpd/handler.go:415-453` vs
`v1.12.3:services/httpd/handler.go:514-552`）：

```go
// v1.12.3:services/httpd/handler.go:518 — AddRoutes（节选）
if hf, ok := r.HandlerFunc.(func(http.ResponseWriter, *http.Request, meta.User)); ok {
    handler = authenticate(hf, h, h.Config.AuthEnabled)
}
if hf, ok := r.HandlerFunc.(func(http.ResponseWriter, *http.Request)); ok {
    handler = http.HandlerFunc(hf)
}
if r.Method == http.MethodPost {
    switch r.Pattern {
    case "/write", "/api/v1/prom/write":
        handler = h.writeThrottler.Handler(handler)
    }
}
handler = h.responseWriter(handler)
if r.Gzipped { handler = gzipFilter(handler) }
handler = h.SetHeadersHandler(handler)
handler = cors(handler)
handler = requestID(handler)
if h.Config.LogEnabled && r.LoggingEnabled { handler = h.logging(handler, r.Name) }
handler = h.recovery(handler, r.Name)
```

```go
// v1.8.10:services/httpd/handler.go:429 — AddRoutes（节选）
if r.Method == http.MethodPost {
    switch r.Pattern {
    case "/write", "/api/v1/prom/write":
        handler = h.writeThrottler.Handler(handler)
    default:
    }
}
```

差异结论：

1. **限流范围没变**：仍然只覆盖 `POST /write` 和 `POST /api/v1/prom/write`。
   `POST /api/v2/write`、`POST /api/v2/delete`、`POST /api/v2/query` 都不经过
   `writeThrottler`（`handler.go:528-535`）。V2 写入与 V1 共用 `serveWrite`，
   所以这是一个**依旧存在**的绕过点。
2. **认证由 handler 签名决定**：`Route` 的两个 bool 只表示 gzip 与访问日志，
   不表示认证（见 [10 §2.2/§14.9](../10_http_service_spec.md)）。
3. **新增路由的 gzip 全部为 false**：`/api/v2/delete`、`/api/v2/buckets*` 的
   `Route.Gzipped` 都是 `false`（`handler.go:214-273`），响应不压缩。
4. **CORS 头扩张**：新版 `Access-Control-Allow-Methods` 增加 `PATCH`（`e8f7b78d68`），
   `Access-Control-Allow-Headers` 增加 `User-Agent`（`0e458e7175`）；这是
   `/api/v2/buckets` PATCH 与浏览器预检可工作的前提（旧 `v1.8.10:services/httpd/handler.go:1878-1895` /
   新 `v1.12.3:services/httpd/handler.go:2939-2955`）。

### 3.3 认证、JWT 与 pprof 授权

认证主体逻辑两版一致：`parseCredentials` 四种来源（`u/p` 查询参数、`Bearer`、
`Token`、`Basic`）→ `MetaClient.Authenticate` 或 JWT 校验 → 引导模式
（`AdminUserExists()==false` 时跳过认证）。
对照：`v1.8.10:services/httpd/handler.go:1776-1870` 与
`v1.12.3:services/httpd/handler.go:2837-2931`。

真实差异有三处：

**(a) JWT 依赖更换**。导入从 `github.com/dgrijalva/jwt-go/v4`
（`v1.8.10:services/httpd/handler.go:26`）换成
`github.com/golang-jwt/jwt/v4`（`v1.12.3:services/httpd/handler.go:24`，
`dec56f9c31` #21934 先换 `golang-jwt` v3，`f8635640cc` #26292 再升 v4）。`jwt.Parse` + `jwt.MapClaims` + `claims["exp"].(float64)`
的用法没变，因此**已有 token 仍可用**，只是签名校验实现换了维护分支。

**(b) pprof 未授权时不再 panic**：

```go
// v1.8.10:services/httpd/handler.go:251 — 旧：user==nil 仍调用 user.ID()
if user == nil || !user.AuthorizeUnrestricted() {
    h.Logger.Info("Unauthorized request", zap.String("user", user.ID()), zap.String("path", r.URL.Path))
    h.httpError(w, "error authorizing admin access", http.StatusForbidden)
    return
}
```

```go
// v1.12.3:services/httpd/handler.go:326 — 新：先判空再取 ID
if user == nil || !user.AuthorizeUnrestricted() {
    // Don't panic
    id := ""
    if user != nil {
        id = user.ID()
    }
    h.Logger.Info("Unauthorized request", zap.String("user", id), zap.String("path", r.URL.Path))
    h.httpError(w, "error authorizing admin access", http.StatusForbidden)
    return
}
```

旧版在引导模式（`AdminUserExists()` 为 false，此时 `authenticate` 跳过认证并以
`user == nil` 调用 `inner`）下访问 pprof，会直接 nil 解引用；
因为外层有 `recovery`，客户端看到的是 **500 + 一条 panic 日志**，而不是 403。
新版返回 403。这是一次真实的**可观测行为修正**。

**(c) `parseToken` 改用 `strings.Cut`**（`handler.go:2771-2776`），语义等价。

### 3.4 访问日志与 status filters

`logging` 中间件与 CLF 行格式没有任何功能变化
（`v1.8.10:services/httpd/handler.go:1960-1978` / `v1.12.3:...:3023-3041`），
`response_logger.go` 只有注释格式调整。

变化在**配置默认值输出**（commit `a2e029fa7f`，#26917）：

```go
// v1.12.3:services/httpd/config.go:96 — NewConfig 显式初始化空切片
AccessLogStatusFilters: make([]StatusFilter, 0),
```

```go
// v1.8.10:services/httpd/config.go:70-91 — 旧 NewConfig 不设置该字段
return Config{ ... MaxBodySize: DefaultMaxBodySize, EnqueuedWriteTimeout: DefaultEnqueuedWriteTimeout }
```

影响：`influxd config` 生成的示例配置现在会打印
`access-log-status-filters = []`（`etc/config.sample.toml:314` 也有对应注释）。
运行时行为不变——`StatusFilters.Match` 对 nil 与空切片都返回 `true`
（`v1.12.3:services/httpd/config.go:198-206`）。

### 3.5 用户维度查询字节（#27188）

旧版只统计全局 `QueryRequestBytesTransmitted` 与
`FluxQueryRequestBytesTransmitted`（后者还是 #25669 才加入），`Statistics` 结构中
没有任何按用户维度字段（`v1.8.10:services/httpd/handler.go:352-378`）。
新版新增按用户维度：

```go
// v1.12.3:services/httpd/handler.go:503 — addQueryBytesForUser
func (h *Handler) addQueryBytesForUser(user string, n int64) {
    if !h.Config.UserQueryBytesEnabled {
        return
    }
    counter, _ := h.queryBytesPerUser.LoadOrStore(user, &atomic.Int64{})
    counter.Add(n)
}
```

调用点覆盖四条查询路径：

| 路径 | 计数口径 | 证据 |
|------|----------|------|
| `/query` chunked | 每个 `WriteResponse` 的字节数 | `handler.go:820-821` |
| `/query` 非 chunked | 最终整包写入字节数 | `handler.go:911-913` |
| `/api/v2/query`（Flux） | `encoder.Encode` 返回值 | `handler.go:2258-2259` |
| `/api/v1/prom/read` | snappy 压缩后响应字节 | `handler.go:2093-2094` |

统计输出在 `Statistics()` 中以独立 measurement 名 `userquerybytes` 追加：

```go
// v1.12.3:services/httpd/handler.go:483-499
if h.Config.UserQueryBytesEnabled && !h.queryBytesPerUser.IsEmpty() {
    h.queryBytesPerUser.Range(func(user string, counter *atomic.Int64) bool {
        userTag := user
        if user == "" { userTag = StatAnonymousUser }
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

配套常量在 `v1.12.3:services/httpd/service.go:50-56`
（`userQueryRespBytes`、`user`、`(anonymous)`），配置项在
`v1.12.3:services/httpd/config.go:68`（`toml:"user-query-bytes-enabled"`，
也可用环境变量 `INFLUXDB_HTTP_USER_QUERY_BYTES_ENABLED` 打开）。
`gensyncmap.Map.IsEmpty` 是本特性新增的辅助方法
（`v1.12.3:pkg/data/gensyncmap/gensyncmap.go:49-56`）。

### 3.6 `/query` 的 `time_format`（#26596）

旧版只认 `epoch`，且**任何非空 epoch 值都会执行转换**，包括非法值（落到
`divisor=1`，等于纳秒）：

```go
// v1.8.10:services/httpd/handler.go:534
epoch := strings.TrimSpace(r.FormValue("epoch"))
...
// v1.8.10:services/httpd/handler.go:688-691
if epoch != "" {
    convertToEpoch(r, epoch)
}
```

新版引入 `time_format` 白名单并对 `epoch` 做门控：

```go
// v1.12.3:services/httpd/handler.go:635-646
timeFormats := []string{"rfc3339", "epoch"}
timeFormat := strings.TrimSpace(r.FormValue("time_format"))
if timeFormat == "" {
    timeFormat = "epoch"
} else if !slices.Contains(timeFormats, timeFormat) {
    h.httpError(rw, fmt.Sprintf("Time format must be one of the following: %s",
        strings.Join(timeFormats, ",")), http.StatusBadRequest)
    return
}
```

```go
// v1.12.3:services/httpd/handler.go:806-813
if epoch != "" && timeFormat == "epoch" {
    convertToEpoch(r, epoch)
} else if timeFormat == "rfc3339" {
    if err := convertToTimeFormat(r, time.RFC3339Nano); err != nil {
        h.httpError(rw, fmt.Sprintf("error converting time to RFC3339Nano: %s", err.Error()),
            http.StatusBadRequest)
    }
}
```

语义影响：

- `time_format=rfc3339` 时 `epoch` 参数被**忽略**，时间列变成
  `2024-01-01T00:00:00.123456789Z` 字符串；旧版做不到。
- 非法 `time_format` 在**写响应头之前**返回 400，可正常改状态码。
- `convertToTimeFormat` 失败发生在 `writeHeader(200)+Flush()` 之后
  （`handler.go:793`），此时 `httpError` 的 400 无法生效——这是已知缺陷，
  与 [10 §5.5](../10_http_service_spec.md) 描述一致。

### 3.7 `SHOW QUERIES`：user 列与越权修复（#26981、#27196）

**(a) 增加 user 列**（commit `32d8b002d8`）：

```go
// v1.8.10:query/task_manager.go:152 — 旧版 5 列是内联字面量；全文件没有 queryFieldNames
Columns: []string{"qid", "query", "database", "duration", "status"}
// v1.12.3:query/task_manager.go:34 — 新版抽成包级变量并增加 user
queryFieldNames []string = []string{"qid", "query", "database", "duration", "status", "user"}
```

用户来源是 HTTP 层注入的 `ExecutionOptions.UserID`
（`v1.12.3:services/httpd/handler.go:724-737`，字段定义
`v1.12.3:query/executor.go:205-206`）。同一个 `userID` 也进入慢查询日志、
超时日志与 `log-queries-after` 输出（`query/task_manager.go:233`、`:334`）。

**(b) 越权泄漏修复**（commit `2b7164440f`）：

```go
// v1.8.10:query/task_manager.go:129
func (t *TaskManager) executeShowQueriesStatement(q *influxql.ShowQueriesStatement) (models.Rows, error) {
```

```go
// v1.12.3:query/task_manager.go:136-147
func (t *TaskManager) executeShowQueriesStatement(q *influxql.ShowQueriesStatement, authorizer CoarseAuthorizer) (models.Rows, error) {
    ...
    for id, qi := range t.queries {
        if authorizer != nil && qi.database != "" && !authorizer.AuthorizeDatabase(influxql.ReadPrivilege, qi.database) {
            continue
        }
```

调用方把 `ctx.CoarseAuthorizer` 传进来（`v1.12.3:query/task_manager.go:106`）。
效果：非管理员执行 `SHOW QUERIES` 时只能看到自己有 READ 权限的库上的查询；
管理员（`OpenCoarseAuthorizer` 或全权用户）仍能看到全部。

### 3.8 TLS 体系重构：`TLSConfigManager` + `TLSCertLoader`

v1.8.10 的 `pkg/tlsconfig` 只有 `tls_config.go`（`Ciphers/MinVersion/MaxVersion`
解析），HTTP 层自己 `tls.LoadX509KeyPair`：

```go
// v1.8.10:services/httpd/service.go:114-128
if s.https {
    cert, err := tls.LoadX509KeyPair(s.cert, s.key)
    if err != nil { return err }
    tlsConfig := s.tlsConfig.Clone()
    tlsConfig.Certificates = []tls.Certificate{cert}
    listener, err := tls.Listen("tcp", s.addr, tlsConfig)
```

v1.12.3 分三层：`TLSConfigManager`（策略/监听/拨号）→ `TLSCertLoader`
（证书加载/热替换/过期巡检）→ `LoadCertificate`（单次加载与校验）。

```go
// v1.12.3:services/httpd/service.go:162-174
tm, err := tlsconfig.NewTLSConfigManager(s.https, s.tlsConfig, s.cert, s.key, false,
    tlsconfig.WithIgnoreFilePermissions(s.insecureCert),
    tlsconfig.WithLogger(s.Logger))
if err != nil { return fmt.Errorf("httpd: error creating TLS manager: %w", err) }
s.tlsManager = tm
if ln, err := s.tlsManager.Listen("tcp", s.addr); err != nil { ... } else { s.ln = ln }
```

```go
// v1.12.3:pkg/tlsconfig/certconfig.go:318-324 — 服务端与客户端证书都走动态回调
func (cl *TLSCertLoader) SetupTLSConfig(tlsConfig *tls.Config) {
    if tlsConfig == nil { return }
    tlsConfig.GetCertificate = cl.GetCertificate
    tlsConfig.GetClientCertificate = cl.GetClientCertificate
}
```

`TLSConfigManager.Listen` 在 TLS 关闭时退化为 `net.Listen`
（`v1.12.3:pkg/tlsconfig/configmanager.go:340-346`），因此 `Service.Open` 不再
需要 `if s.https { ... } else { ... }` 两条分支（旧版见
`v1.8.10:services/httpd/service.go:113-136`）。

**收益**：证书在握手时通过 `GetCertificate` 动态读取
（`certconfig.go:329-339`），替换证书只需替换 loader 内部指针，不必重建 listener。

### 3.9 SIGHUP 证书热重载（#27057）

旧版 `influxd` 只监听退出信号：

```go
// v1.8.10:cmd/influxd/main.go:85-104
signalCh := make(chan os.Signal, 1)
signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
...
<-signalCh
```

新版拆成两个 channel，并在主循环里处理 SIGHUP：

```go
// v1.12.3:cmd/influxd/main.go:85-104
shutdownCh := make(chan os.Signal, 1)
signal.Notify(shutdownCh, os.Interrupt, syscall.SIGTERM)
reloadCh := make(chan os.Signal, 1)
signal.Notify(reloadCh, syscall.SIGHUP)
...
for !shutdown {
    select {
    case sig := <-shutdownCh: ...
    case <-reloadCh:
        cmd.ReloadConfig(args...)
    }
}
```

重载链路：

```go
// v1.12.3:cmd/influxd/run/command.go:194-208
func (cmd *Command) ReloadConfig(args ...string) {
    _, reloadedConfig, err := cmd.LoadConfig(args...)
    if err != nil { log.Error("error reloading config", zap.Error(err)); return }
    if err := cmd.Server.ApplyReloadedConfig(reloadedConfig, log); err != nil {
        log.Error("error applying reloaded config; some config values may have been applied", zap.Error(err))
    }
}
```

```go
// v1.12.3:services/httpd/service.go:296-322 — 两阶段：先验证，再应用
func (s *Service) PrepareReloadConfig(c Config) (func() error, error) {
    s.mu.Lock(); defer s.mu.Unlock()
    if s.https != c.HTTPSEnabled {
        return nil, fmt.Errorf("httpd: can not change https-enabled on a running server")
    }
    if s.https {
        if s.tlsManager == nil { return nil, errors.New("httpd: no TLS manager available") }
        if apply, err := s.tlsManager.PrepareCertificateLoad(c.HTTPSCertificate, c.HTTPSPrivateKey); err == nil {
            return apply, nil
        } else {
            return nil, fmt.Errorf("httpd: error loading certificate at (%q, %q): %w",
                c.HTTPSCertificate, c.HTTPSPrivateKey, err)
        }
    }
    return nil, nil
}
```

```go
// v1.12.3:pkg/tlsconfig/certconfig.go:412-424 — 预校验后返回 apply 闭包
func (cl *TLSCertLoader) PrepareLoad(certPath, keyPath string) (func() error, error) {
    loadedCert, err := cl.loadCertificate(certPath, keyPath)
    if err != nil { return nil, err }
    return func() error {
        if err := cl.SetCertificate(loadedCert); err != nil { ... }
        return nil
    }, nil
}
```

调度侧（`cmd/influxd/run/server.go:583-627`）保证"任一服务验证失败则一个都不应用"：

```go
// v1.12.3:cmd/influxd/run/server.go:592-600
if s.HttpdService != nil {
    if af, err := s.HttpdService.PrepareReloadConfig(config.HTTPD); err == nil {
        applyFuncs = append(applyFuncs, af)
    } else {
        log.Error("error reloading httpd service config, no new configuration applied", zap.Error(err))
        return err
    }
}
```

语义与注意事项：

- **只对新建连接生效**：`GetCertificate` 在 TLS 握手时调用，已有 keep-alive
  连接继续用旧证书直到断开。
- **不支持改 `https-enabled`**：运行中切换会整体失败（返回错误）。
- **失败安全**：`TLSCertLoader.Load` 失败时保留旧证书并打日志
  （`certconfig.go:394-404`）；`PrepareLoad` 在验证阶段就失败，根本不会替换。
- **opentsdb 走另一条语义**：不能改路径，只重新加载"当前位置"的证书
  （`server.go:602-614`，`PrepareReloadTLSCertificates`）。
- **同一区间还新增 SIGTERM 查询转储**：收到 SIGTERM 时若 `[coordinator] termination-query-log=true`，
  会把当前查询写入日志（`fd1d51690d` #22638；`cmd/influxd/main.go:91-109`、`cmd/influxd/run/server.go:759`）。

### 3.10 客户端证书支持（#27090）

新增 `GetClientCertificate`，并把 `SetupTLSConfig` 作为统一挂载点：

```go
// v1.12.3:pkg/tlsconfig/certconfig.go:344-362
func (cl *TLSCertLoader) GetClientCertificate(cri *tls.CertificateRequestInfo) (*tls.Certificate, error) {
    if cri == nil {
        return new(tls.Certificate), fmt.Errorf("tls client: %w", ErrCertificateRequestInfoNil)
    }
    cert := cl.Certificate()
    if cert == nil { return new(tls.Certificate), fmt.Errorf("tls client: %w", ErrCertificateNil) }
    if err := cri.SupportsCertificate(cert); err == nil { return cert, nil }
    // 服务端不接受本证书时返回空证书而不是错误，贴合 Go 原生行为
    return new(tls.Certificate), nil
}
```

旧版既没有 `GetClientCertificate`，也没有 `SetupTLSConfig`：
`v1.8.10:pkg/tlsconfig/` 只有 `tls_config.go:10-14` 的 `Config`（ciphers/版本）。

`TLSConfigManager` 同时提供 `WithClientAuth`、`WithClientCAFiles`、
`WithClientCAIncludeSystem`（`configmanager.go:169-188`），服务端可要求双向 TLS。
HTTP 服务本身没有暴露"要求客户端证书"的配置项，需要走企业版/自定义装配。

### 3.11 `https-insecure-certificate` 的真实语义（#27219）

这个配置项是本区间新增的（`v1.12.3:services/httpd/config.go:53`）；
旧版 `[http]` 只有 `https-enabled`/`https-certificate`/`https-private-key` 三项
（`v1.8.10:services/httpd/config.go:49-51`），且不检查文件权限。它的含义
**不是** `InsecureSkipVerify`，而是"忽略证书/私钥的文件权限检查"。

引入过程中踩过一次坑：#27216 在 httpd 里写成
`WithAllowInsecure(s.insecureCert)`，而 `WithAllowInsecure` 最终设置的是
`tlsConfig.InsecureSkipVerify`（`configmanager.go:237`）——对**服务端监听**没有意义，
所以这个开关当时完全无效。#27219（`ba705ec658`）改成
`WithIgnoreFilePermissions(s.insecureCert)`：

```go
// v1.12.3:services/httpd/service.go:162-164 — 修复后的写法
tm, err := tlsconfig.NewTLSConfigManager(s.https, s.tlsConfig, s.cert, s.key, false,
    tlsconfig.WithIgnoreFilePermissions(s.insecureCert),
    tlsconfig.WithLogger(s.Logger))
```

权限检查本身在 `LoadCertificate` 里：

```go
// v1.12.3:pkg/tlsconfig/certconfig.go:120-125
if !config.ignoreFilePermissions {
    if err := file.VerifyFilePermissivenessF(f, maxPerms); err != nil {
        return nil, fmt.Errorf("LoadCertificate: %w", err)
    }
}
```

`CertMaxPermissions = 0644`、`KeyMaxPermissions = 0600`
（`certconfig.go:18-23`），比较逻辑见 `pkg/file/file.go:74-88`
（多出的权限位即报错）。

### 3.12 `/debug/vars` 的诊断扩展（#26624、#26874、#23948、#26754）

旧版 `serveExpvar` 只输出 `system`、`cmdline`、`memstats` 和各服务统计：

```go
// v1.8.10:services/httpd/handler.go:1479-1572（结构）
{"system": {...}, "cmdline": ..., "memstats": ..., "<service>": {...}}
```

新版新增 4 个板块（`build`/`config`/`stats`/`crypto`），并把 `cq` 的输出形状重塑为扁平 values（`cq` 本身旧版已输出；`v1.12.3:services/httpd/handler.go:2291-2524`）：

| 板块 | 来源 | 实现 | 证据 |
|------|------|------|------|
| `build` | `Monitor.Diagnostics()["build"]` | `parseBuildInfo` | `handler.go:2329-2346`、`:2628` |
| `config` | `Monitor.Diagnostics()["config"]` = `*tsdb.Config` | `parseConfigDiagnostics`（处理 `toml.Size`/`toml.Duration`） | `handler.go:2356-2382`、`:2703-2726` |
| `stats` | `Monitor.Diagnostics()["stats"]` | 单行 map 摊平（压缩吞吐占用率） | `handler.go:2384-2406`、`monitor/stats.go:12-34` |
| `crypto` | `Monitor.Diagnostics()["crypto"]`，OSS 无此诊断时用 `ossCryptoDiagnostics()` | `parseCryptoDiagnostics` | `handler.go:2408-2439`、`:2654-2699` |
| `cq` | `Monitor.Statistics()` 中 `Name=="cq"` 的项 | `parseCQStatistics`（旧版经通用 `json.Marshal(models.Statistic)` 输出 `{name,tags,values}`） | `handler.go:2451-2478`、`:2728-2734` |

`config` 板块的内容来自 `tsdb.Config.Diagnostics()`，本区间还新增了 3 个字段：

```go
// v1.12.3:tsdb/config.go:304-306（尾部）
"aggressive-points-per-block":            c.AggressivePointsPerBlock,
"compact-throughput":                     c.CompactThroughput,
"compact-throughput-burst":               c.CompactThroughputBurst,
```

注册侧：

```go
// v1.12.3:monitor/service.go:156-166
if m.Limiter != nil {
    m.RegisterDiagnosticsClient("stats", &stats{comp: compactThroughputStats{limiter: m.Limiter}})
}
if m.TSDBConfig != nil {
    m.RegisterDiagnosticsClient("config", m.TSDBConfig)
}
```

```go
// v1.12.3:cmd/influxd/run/server.go:205
s.Monitor = monitor.New(s, c.Monitor, &c.Data)
// v1.12.3:cmd/influxd/run/server.go:426
s.Monitor.RegisterDiagnosticsClient("cq", srv)
// v1.12.3:cmd/influxd/run/server.go:525
s.Monitor.WithCompactThroughputLimiter(s.TSDBStore.EngineOptions.CompactionThroughputLimiter)
```

CQ 服务为满足 `Monitor` 的诊断接口补了一个空实现
（`v1.12.3:services/continuous_querier/service.go:173-177`），同时把统计写入
失败从"静默"改成打日志（`:418-428`）。`monitor.New` 的签名从 2 参数变 3 参数
（`v1.8.10:monitor/service.go:74` / `v1.12.3:monitor/service.go:114`），
`globalTags` 也从裸 map 换成带锁的 `tags` 结构（`v1.12.3:monitor/service.go:37-68`），
修掉了 `SetGlobalTag` 与 `Statistics` 并发访问 map 的竞态。

同一区间还修了 `/debug/vars` 的重复键：旧版同名 statistic 会产生重复 JSON key；新版用
`uniqueKeys` 计数并追加 `:1`、`:2` 后缀（`userquerybytes`、`userquerybytes:1`…；`v1.12.3:services/httpd/handler.go:2501-2506`，`8843a01ebf`）。这与 §3.5 的 per-user
`userquerybytes` 直接相关。另外 `[httpd]` 的 `SHOW DIAGNOSTICS` 新增 `flux-enabled`、
`https-insecure-certificate`（`v1.12.3:services/httpd/config.go:110-118`）。

### 3.13 pprof 路由与 `/debug/requests`

`pprof.go` 只有注释格式变化。`/debug/requests` 的代码逐行等价
（`v1.8.10:services/httpd/handler.go:1575-1630` /
`v1.12.3:services/httpd/handler.go:2527-2582`）。

开关矩阵与 [10 §2.3](../10_http_service_spec.md) 的四行矩阵一致；两版逻辑逐行相同（旧 `handler.go:468-478` / 新 `handler.go:567-577`）：

| AuthEnabled | PprofEnabled | PprofAuthEnabled | `/debug/pprof*` | `/debug/vars`、`/debug/requests` |
|---|---|---|---|---|
| true | true | true | 注册路由 + `AuthorizeUnrestricted()`，非管理员 403 | 走注册路由，需管理员 |
| true | true | false | `handleProfiles`，**无认证** | 前缀分支，**无认证** |
| false | true | true/false | `handleProfiles`，**无认证**（`PprofAuthEnabled` 被忽略） | 前缀分支，**无认证** |
| 任意 | false | 任意 | 不匹配 pprof 分支，交给 mux（通常 404） | 前缀分支，**无认证** |

这是 v1.8.10 就存在的问题，v1.12.3 没有收紧。

### 3.14 Prometheus remote write/read：协议栈迁移

旧版自带一套 protobuf 定义 `prometheus/remote`（`remote.proto`/`remote.pb.go`，
本区间已删除），用 gogo/protobuf：

```go
// v1.8.10:services/httpd/handler.go:1191-1196
var req remote.WriteRequest
if err := proto.Unmarshal(reqBuf, &req); err != nil { ... }
```

```go
// v1.12.3:services/httpd/handler.go:1973-1978
var req prompb.WriteRequest
if err := req.Unmarshal(reqBuf); err != nil { ... }
```

转换函数的类型也跟着换，并且时间戳字段名变了：

```go
// v1.8.10:prometheus/converters.go:82
t := time.Unix(0, s.TimestampMs*int64(time.Millisecond))
// v1.12.3:prometheus/converters.go:83
t := time.Unix(0, s.Timestamp*int64(time.Millisecond))
```

线格式（snappy + protobuf 字段号）与上游 Prometheus 兼容，**客户端无感**。
另外 `ReadFilterRequest.Range` 由值类型 `datatypes.TimestampRange` 改成指针
（`v1.12.3:prometheus/converters.go:112-117`、`storage/reads/datatypes` 重新生成），
`GetReadSource` 从 `types.Any` 改成 `*anypb.Any` 并显式处理 nil
（`v1.12.3:services/storage/store.go:28-37`）。

**(a) `servePromRead` 首次真正执行库级授权**：

```go
// v1.12.3:services/httpd/handler.go:2055-2064 — 新增的授权检查
if h.Config.AuthEnabled && h.Config.PromReadAuthEnabled {
    if user == nil {
        h.httpError(w, fmt.Sprintf("user is required to read from database %q", db), http.StatusForbidden)
        return
    }
    if h.QueryAuthorizer.AuthorizeDatabase(user, influxql.ReadPrivilege, db) != nil {
        h.httpError(w, fmt.Sprintf("user %q is not authorized to read from database %q", user.ID(), db), http.StatusForbidden)
        return
    }
}
```

旧版 `servePromRead`（`v1.8.10:services/httpd/handler.go:1248-1372`）的配置里虽然
有 `prom-read-auth-enabled`，但**函数体完全没有使用它**：只要通过 `authenticate`
（`AuthEnabled=false` 时连认证都没有），调用者就能读任意 `db`。新版补上了这道
READ 授权，但仍受开关约束——`prom-read-auth-enabled` 默认 `false`
（`v1.12.3:services/httpd/config.go:85`），所以**默认仍不做库级授权**，只做认证。

**(b) 资源边界**：
`servePromRead` 的资源边界**没有改善**：仍然 `io.ReadAll(r.Body)`
（`handler.go:2033`）且用 `context.Background()`（`:2097`），既不受
`max-body-size` 限制，也不随客户端断开取消。

**(c) 可观测性**：旧版 `servePromRead` 无计数、也不进 `requestTracker`
（`v1.8.10:services/httpd/handler.go:1248-1252`）；新版在入口 `PromReadRequests++`
并 `requestTracker.Add`（`v1.12.3:services/httpd/handler.go:2031-2032`）。
因此旧版 `promReadReq` 恒 0，`/debug/requests` 也看不到 Prom read。

### 3.15 Flux 查询端点

端点主体两版几乎逐行一致（`v1.8.10:services/httpd/handler.go:1374-1454` /
`v1.12.3:services/httpd/handler.go:2175-2261`），真实差异如下：

1. **响应字节统计**：新版在编码后累加
   `FluxQueryRequestBytesTransmitted` 与用户维度字节
   （`handler.go:2258-2259`）。该计数器本身是 #25669 引入的。
2. **错误分支加 `return`**：旧版 `if err != nil { if n == 0 { h.httpError(...) } }`
   之后没有 `return`，新版在 `n == 0` 分支 `return`（`handler.go:2253-2257`）。
   行为等价（后面也没代码了），但更清晰。
3. **Flux 关闭分支的签名变化**（见 §3.1 第 5 行）：
   旧版匿名 `func(w,r)` 使该路由**完全跳过认证**；新版
   `serveFluxQueryDisabled(w,r,user)` 会被 `authenticate` 包装，并额外打一条
   `Warn` 日志（`handler.go:2263-2266`）。
4. **新增 `OPTIONS /api/v2/query`**（`handler.go:371-374`）。

`decodeQueryRequest` 的两种 Content-Type 处理不变
（`v1.8.10:services/httpd/flux.go:25-53` / `v1.12.3:services/httpd/flux.go:25-53`），
`ioutil.ReadAll` → `io.ReadAll` 只是 API 现代化。

### 3.16 `query/control`：Flux 控制器从"薄壳"到"资源管理器"

旧版：

```go
// v1.8.10:flux/control/controller.go:73-105
func NewController(mc MetaClient, reader influxdb.Reader, auth Authorizer, authEnabled bool, logger *zap.Logger) *Controller {
    ...
    return &Controller{deps: []flux.Dependency{storageDeps}, logger: logger}
}
func (c *Controller) Query(ctx context.Context, compiler flux.Compiler) (flux.Query, error) {
    ...
    alloc := &memory.Allocator{}   // 无上限
    return p.Start(ctx, alloc)
}
```

新版迁到 `query/control`，并引入完整的配额模型：

```go
// v1.12.3:query/control/controller.go:71-110 — Config
type Config struct {
    ConcurrencyQuota                int32 `toml:"query-concurrency"`
    InitialMemoryBytesQuotaPerQuery int64 `toml:"query-initial-memory-bytes"`
    MemoryBytesQuotaPerQuery        int64 `toml:"query-max-memory-bytes"`
    MaxMemoryBytes                  int64 `toml:"total-max-memory-bytes"`
    QueueSize                       int32 `toml:"query-queue-size"`
}
```

```go
// v1.12.3:query/control/controller.go:175-227 — New
// ConcurrencyQuota==0 表示不限制：queryQueue=nil，每个查询一个 goroutine
// 否则启动 ConcurrencyQuota 个 processQueryQueue worker，队列满返回 ResourceExhausted
```

```go
// v1.12.3:query/control/controller.go:246-264 — query 三段式
q, err := c.createQuery(ctx, compiler, deps)
if err := c.compileQuery(q, compiler); err != nil { ... }
if err := c.enqueueQuery(q); err != nil { ... }   // 队列满 → flux.Error{Code: ResourceExhausted}
```

```go
// v1.12.3:query/control/memory.go:73-112 — 每查询内存申请
if q.limit+want > q.m.memoryBytesQuotaPerQuery {
    return 0, errors.New("query hit hard limit")
}
unused := q.m.getUnusedMemoryBytes()
if unused < want { return 0, errors.New("not enough capacity") }
... CAS 扣减 unusedMemoryBytes ...
```

```go
// v1.12.3:query/control/controller.go:337-343 — 请求结果分类指标
c.metrics.requests.WithLabelValues(lvs...).Inc()  // label: success/compile-error/queue-error/...
```

对 HTTP 层的影响：

- `serveFluxQuery` 的 `h.Controller.Query(ctx, pr.Compiler)` 失败仍统一映射为
  **500**（`handler.go:2216-2220`）。也就是说：**队列满/内存不足在 HTTP 上仍是 500**，
  不是 429/503，客户端只能从 body 文本区分。
- 控制器由 `cmd/influxd/run/server.go:338-346` 构造，Prometheus collector 在
  服务端启动时注册（不在 `Handler.Open` 里）。

### 3.17 `storage/flux`：Flux 读路径重写

旧版读实现在 `storage/reads/flux_reader.go`（`NewReader(s Store) influxdb.Reader`），
新版迁移到独立包 `storage/flux`（`NewReader(s storage.Store) query.Reader`）：

```go
// v1.12.3:storage/flux/reader.go:60-63
func NewReader(s storage.Store) query.Reader {
    return &storeReader{s: s}
}
```

新增能力对照：

| 能力 | v1.8.10 | v1.12.3 | 证据 |
|------|---------|---------|------|
| `ReadFilter` | ✅ | ✅ | `v1.8.10:storage/reads/flux_reader.go:34` / `v1.12.3:storage/flux/reader.go:65` |
| `ReadGroup` | ✅ | ✅ | `:44` / `:75` |
| `ReadWindowAggregate` | ❌ | ✅ | `v1.12.3:storage/flux/reader.go:85-93` |
| `windowAggregateIterator` | ❌ | ✅ | `v1.12.3:storage/flux/reader.go:609-843` |
| `ReadTagKeys/Values` | ✅ | ✅ | `:54`、`:74` / `:95`、`:106` |
| `GroupCursorError`（类型冲突显式报错） | ❌ | ✅ | `v1.12.3:storage/flux/reader.go:23-47` |
| 列存实现 | `arrow/array` | `flux/array`（`colReader`） | `v1.12.3:storage/flux/table.go:139-192` |
| 谓词类型 | `influxdb.ReadFilterSpec` | `query.ReadFilterSpec`（`flux/stdlib/influxdata/influxdb`） | `v1.12.3:storage/flux/reader.go:14` |
| protobuf | gogo `types.Any` | `anypb.New` | `v1.12.3:storage/flux/reader.go:627` |

`windowAggregateIterator.Do` 会把窗口、offset 与聚合类型编码成
`datatypes.ReadWindowAggregateRequest`，下推到存储层：

```go
// v1.12.3:storage/flux/reader.go:632-663
var req datatypes.ReadWindowAggregateRequest
req.ReadSource = any
req.Predicate  = wai.spec.Predicate
req.Range      = &datatypes.TimestampRange{Start: ..., End: ...}
req.Window     = &datatypes.Window{Every: ..., Offset: ...}
req.Aggregate  = make([]*datatypes.Aggregate, len(wai.spec.Aggregates))
...
rs, err := wai.s.WindowAggregate(wai.ctx, &req)
```

`handleRead` 里还区分 selector（`first/last/min/max`）与非 selector，
selector 走 `createEmpty`/降序游标优化（`reader.go:689-720`，`isSelector` 在
`:684-687`）。这就是 `services/storage` 里 `descending := !reads.IsAscendingWindowAggregate(req)`
的来由（见 §3.18）。

`table.gen.go` 是代码生成产物（5 种列类型 × 5 种表变体 = 25 个生成类型），
由 `storage/flux/table.go:3` 的 `go:generate` + `storage/reads/types.tmpldata` 驱动；
模板 `table.gen.go.tmpl` 新增了 `colReader` 的 `flux/array` 适配。

### 3.18 `services/storage`：读请求限 shard、窗口聚合、删除

旧版 `Store` 只有 `ReadFilter`/`ReadGroup`/`TagKeys`/`TagValues`/`MeasurementNames`：

```go
// v1.8.10:services/storage/store.go:109 — ReadFilter
// v1.8.10:services/storage/store.go:147 — ReadGroup
```

新版统一成 `XxxLimit(ctx, req, limitShardIDs)` 形式（供企业版限制 shard 集合）：

```go
// v1.12.3:services/storage/store.go:126 — ReadFilterLimit
// v1.12.3:services/storage/store.go:174 — ReadFilter  = ReadFilterLimit(ctx, req, nil)
// v1.12.3:services/storage/store.go:177 — ReadGroupLimit
// v1.12.3:services/storage/store.go:234 — WindowAggregateLimit
// v1.12.3:services/storage/store.go:277 — WindowAggregate = WindowAggregateLimit(ctx, req, nil)
```

四个语义变化：

1. **降序游标判定**：`ReadGroupLimit` / `WindowAggregateLimit` 调用
   `reads.IsAscendingGroupAggregate` / `IsAscendingWindowAggregate` 决定
   `findShardIDs(..., descending, ...)`（`store.go:195`、`:252`）。这影响
   `last()` 类函数的正确性。
2. **时间范围下界判定修正**：

   ```go
   // v1.8.10:services/storage/store.go:100-105
   if start <= 0 { start = models.MinNanoTime }
   if end <= 0 { end = models.MaxNanoTime }
   // v1.12.3:services/storage/store.go:103-108
   if start <= models.MinNanoTime { start = models.MinNanoTime }
   if end >= models.MaxNanoTime { end = models.MaxNanoTime }
   ```

   旧版把 `end<=0` 当作"未指定"；新版只把 `end>=MaxNanoTime` 归一化，
   `end=0`（1970-01-01）会被当成真实上界。对一个显式传 `stop=0` 的请求，
   旧版返回全量、新版返回空——**这是一处真实的行为变化**。省略 `Range`（nil）同样受影响：
   `GetStart/GetEnd` 返回 0，`[0,0]` 被当作真实区间；旧版值类型零值会被 `end<=0` 兜底为全量。
   当前生产调用方都显式设置 `Range`，影响面主要是自定义/企业调用方。
3. **`GetReadSource` 空指针防护**：`*anypb.Any == nil` 返回 `ErrMissingReadSource`
   （`v1.12.3:services/storage/store.go:28-31`）。旧版各入口各自判 `req.ReadSource == nil`
   并返回 `"missing read source"`（`v1.8.10:services/storage/store.go:110/148/192/268/355`），
   不会 panic；新版只是把检查集中到一个函数。
4. **删除能力**：

   ```go
   // v1.12.3:services/storage/store.go:500-506
   func (s *Store) Delete(database string, sources []influxql.Source, condition influxql.Expr) error {
       return s.TSDBStore.DeleteSeries(database, sources, condition)
   }
   func (s *Store) DeleteRetentionPolicy(database, name string) error {
       return s.TSDBStore.DeleteRetentionPolicy(database, name)
   }
   ```

   对应 `Handler.Store` 接口从 1 个方法扩到 3 个
   （`v1.8.10:services/httpd/handler.go:2025-2027` /
   `v1.12.3:services/httpd/handler.go:3088-3092`）。

### 3.19 V2 删除与 bucket 端点语义

`POST /api/v2/delete`（`handler.go:975-1111`）流程：

1. `bucket2dbrp(bucket)`，失败 → **404**（注意不是 400）。
2. 库/RP 不存在 → 404。
3. `AuthEnabled` 时：`user==nil` → 403；`AuthorizeWrite(user.ID(), db)` 失败 → 403
   （复用写权限，**不是**单独 delete 权限）。
4. `ContentLength > MaxBodySize` → 413；读 body / JSON 失败 → 400。
5. `start`/`stop` 必填且必须是 RFC3339Nano → 否则 400。
6. 拼接 `predicate AND time >= 'start' AND time < 'stop'`，`influxql.ParseExpr`。
7. `PartitionExpr` 抽出 `_measurement = 'x'` 作为 `sources`，同时**拒绝**
   `!=`、`=~`、`!~`、`OR` → 400。
8. `fixLiterals` 把双引号值转成字符串字面量（由 `2fe52d8edc` #23247 引入；#25407 的 `e3e9232de5` 只增加 measurement 名去引号，`handler.go:1085`），
   `srcs` 初始化为 nil（#24145，避免空 WHERE 破坏企业版远程执行）。
9. `Store.Delete(db, srcs, remainingExpr)`，失败 → 400。

历史修复都在新版里：`f365bb7e3a`（nil Sources）、`c31e99574f`（不在 V2 delete 里
设置 measurement 的 db/rp）、`e3e9232de5`（剥离 measurement 名双引号）。

bucket 端点族（`handler.go:1159-1624`）语义：

| 操作 | 授权方法（`AuthEnabled` 时） | 行为 | 状态码 |
|------|------|------|--------|
| create | `AuthorizeCreateDatabase`（库不存在）/ `AuthorizeCreateRetentionPolicy`（库存在，`handler.go:1228/1233`） | `bucket2dbrp(name)` 必须含 RP；body RP 冲突 → 400；`meta.ValidName` 校验；库不存在用 `CreateDatabaseWithRetentionPolicy`，存在用 `CreateRetentionPolicy` | 201 |
| delete | `AuthorizeDeleteRetentionPolicy`（`:1290`） | `Store.DeleteRetentionPolicy` → `MetaClient.DropRetentionPolicy` | 200 空体 |
| update | `AuthorizeCreateDatabase`（`:1324`，源码有意复用 create 权限） | `UpdateRetentionPolicy` | 200 + JSON |
| retrieve | `AuthorizeDatabase(ReadPrivilege, db)`（`:1402`） | `oneBucket` | 200 / 404 |
| list | 逐库 `AuthorizeDatabase(ReadPrivilege, db)`（`:1512`） | 默认 `limit=100`，`limit<=0` 视为 `MaxInt`；`after` 与 `offset` 互斥 | 200 |
| labels/members/owners | — | 统一 405，文案分别为 `bucket labels` / `bucket members` / `bucket owners` + `not supported in this version`（`handler.go:1610/1615/1620`） | 405 |

**注意 create 的 body 读取没有 `truncateReader`**，只有
`ContentLength > MaxBodySize` 预检（`handler.go:1160-1173`）；
chunked 请求（`ContentLength == -1`）可以无限读。`serveDeleteV2` 同理
（`handler.go:1004-1021`）。这是新增端点带来的新边界。

### 3.20 其它 HTTP 层差异

**(a) `valuesWrittenOK` 统计消失**。旧版通过类型断言使用
`WritePointsWithContext` 并从 context 读回 values 计数：

```go
// v1.8.10:services/httpd/handler.go:989-1010
type pointsWriterWithContext interface {
    WritePointsWithContext(context.Context, ...) error
}
...
ctx = context.WithValue(ctx, coordinator.StatValuesWritten, &nvalues)
err := pw.WritePointsWithContext(ctx, database, retentionPolicy, consistency, user, points)
atomic.AddInt64(&h.stats.ValuesWrittenOK, nvalues)
```

```go
// v1.12.3:services/httpd/handler.go:1772-1773
if err := h.PointsWriter.WritePoints(database, retentionPolicy, consistency, user, points); influxdb.IsClientError(err) {
```

commit `6795ec6c01` 移除了 context-value 反模式。后果：`Statistics()` 不再输出
`valuesWrittenOK`（旧版在 `v1.8.10:services/httpd/handler.go:394`，新版
`v1.12.3:services/httpd/handler.go:455-479` 没有该项）。常量
`statValuesWrittenOK` 仍留在 `service.go:34`，但已无引用。

同一区间还有两处 httpd 统计变化（§8.4 已列 commit）：① 新增 `fluxQueryRespBytes`（`v1.12.3:services/httpd/service.go:50`、`handler.go:447/479`，#25669）；② `promReadReq` 旧版只读不自增（`v1.8.10:services/httpd/handler.go:375/407`、`servePromRead:1248-1252`），新版在入口 +1 并 `requestTracker.Add`（`v1.12.3:services/httpd/handler.go:2031-2032`，#18429）。此外 Flux 开启时 `Server.Statistics` 会并入 Prometheus Gather 的 `qc_*` 指标（`cmd/influxd/run/server.go:270-273`），`SHOW STATS` 条目会增多。

**(b) `serveWriteV1` / `serveWriteV2` 精度错误补 `return`**：

V1 与 V2 的 `default` 分支在旧版都缺 `return`（V1 `v1.8.10:services/httpd/handler.go:862-865`、V2 `:855-858`），新版两处都补上（V1 `:1656-1660`、V2 `:1636-1640`），同一 commit `997698f891` (#21746)。

```go
// v1.8.10:services/httpd/handler.go:855-858
default:
    err := fmt.Sprintf("invalid precision %q (use ns, us, ms or s)", precision)
    h.httpError(w, err, http.StatusBadRequest)
// ← 没有 return，继续执行 bucket2dbrp + serveWrite
```

```go
// v1.12.3:services/httpd/handler.go:1636-1640
default:
    err := fmt.Sprintf("invalid precision %q (use ns, us, ms or s)", precision)
    h.httpError(w, err, http.StatusBadRequest)
    return
```

旧版在 `precision=xyz` 时会先写 400，然后仍然尝试解析并写入，最后再
`writeHeader(204)`（标准库报 superfluous，客户端看到 400 但数据可能已落库）。
新版干净返回 400。

**(c) 部分写错误文本变化**。`PartialWriteError` 新增 `Database`/`RetentionPolicy`
字段（commit `07c261a21a`），`Error()` 会拼接
`" ... for database: db for retention policy: rp"`（`v1.12.3:tsdb/shard.go:131-140`；
旧版只有 `partial write: %s dropped=%d`，`v1.8.10:tsdb/shard.go:121-123`）。
解析失败分支在 `v1.12.3:services/httpd/handler.go:1792-1801` 填了这两个字段。

**(d) `Service` 生命周期**：

| 项 | v1.8.10 | v1.12.3 |
|----|---------|---------|
| `err` channel | 无缓冲（`service.go:84`） | 缓冲 2（`service.go:127`） |
| `Close` | 直接执行，重复调用会重复关 listener（`:198-220`） | `sync.OnceValue(doClose)`（`:137`、`:239-241`） |
| `Open` 后 `Close` 再 `Open` | 可能成功但状态错乱 | 明确报错 `"attempt to Open http service after Close"`（`:155-157`） |
| listener 归属 | `Close` 总是 `s.ln.Close()` | `tcpServerStarted` 后交给 `httpServer.Shutdown`（`:260-275`） |
| `BoundHTTPAddr` | 直接 `s.ln.Addr()`（可能 nil panic） | 加 RLock + nil 判断（`:345-353`） |
| 证书 loader 关闭 | 无（一次性 LoadX509KeyPair） | `tlsManager.Close()` 停巡检 goroutine（`:277-282`） |

**(e) CSV 输出不转义**（#24311）：CSV formatter 生成 tag 列时改用
`HashKey(false)`（`v1.12.3:services/httpd/response_writer.go:228`，
旧 `v1.8.10:...:233` 是 `HashKey()`）。CSV 中的 tag 值不再被 line-protocol
规则二次转义。

**(f) `WriteError` 辅助函数删除**（`v1.8.10:services/httpd/response_writer.go:75-78`），
包内无引用。

---

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
| 正确性 | 空 `ReadSource` 不再 panic | `any == nil` 提前返回 `ErrMissingReadSource` | `services/storage/store.go:28-31` |
| 正确性 | V2 写入精度错误不再"边报错边写" | 补 `return` | `handler.go:1636-1640` |
| 正确性 | CSV 双重转义 | `HashKey(false)` | `response_writer.go:228` |
| 正确性 | monitor 全局 tag 竞态 | 用 `mu sync.RWMutex` 包裹 map | `monitor/service.go:37-68` |
| 生命周期 | HTTP 服务幂等关闭 | `sync.OnceValue` + `tcpServerStarted` 归属判定 | `services/httpd/service.go:137`、`:246-285` |
| 兼容性 | V2 bucket/delete 面 | 新增 18 条路由，复用 db/rp 语义 | `handler.go:214-273` |

没有基准数据的地方不写数字。上述收益都是**机制层面**的：
例如 TLS 热重载只改变"证书从哪里取"，不改变握手成本；
Flux 内存配额只改变 OOM 的触发方式（从进程崩溃变成查询失败 + 500），
不改变单查询吞吐。

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

### 5.2 状态码与错误体变化

| 场景 | v1.8.10 | v1.12.3 | 是否破坏兼容 |
|------|---------|---------|--------------|
| Flux 关闭 + 无凭据 + auth 开启 | 403（不认证） | 401（已存在管理员时）/ 403（引导模式） | **是**（状态码可能变化） |
| pprof 在引导模式（无管理员用户）下访问 | 500（panic 被 recovery 捕获） | 403 | 否（更正确） |
| `/api/v2/write` 非法 precision | 400 + 可能仍写入 | 400 且不写入 | 否（更正确） |
| 部分写解析错误 body | `partial write: ... dropped=N` | 追加 `for database: ... for retention policy: ...` | 客户端若做精确字符串匹配会受影响 |
| `/api/v2/query` 队列满/内存超限 | 不适用（无限制） | **500** + flux 错误文本 | 新增失败模式 |
| `SHOW QUERIES` 列数 | 5 | 6（多 `user`） | **是**（按列位置解析的客户端会错位） |
| `SHOW STATS` httpd | 含 `valuesWrittenOK`、`promReadReq`（恒 0） | 不含 `valuesWrittenOK`；新增 `fluxQueryRespBytes`；`promReadReq` 真实计数 | **是**（面板缺列/多列/旧指标失真） |
| `/api/v1/prom/read` 跨库读取 | 无库级授权 | 仅当 `prom-read-auth-enabled=true` 时检查 READ | 默认不变；开启后可能 403 |
| `/debug/vars` | `system`/`cmdline`/`memstats` + 各服务统计（含 `cq`，形状 `{name,tags,values}`） | 新增 `config`/`stats`/`crypto` 等；httpd 少 `valuesWrittenOK`；`cq` 变扁平 | 是（有增有减） |

### 5.3 资源与边界

| 边界 | v1.8.10 | v1.12.3 | 说明 |
|------|---------|---------|------|
| `/write` body 上限 | `max-body-size`（默认 25MB） | 同 | 但 `Content-Encoding: gzip` 分支仍会**丢弃** `truncateReader`（`handler.go:1701-1715`），chunked gzip 无上限 |
| `/api/v2/delete` body | 不适用 | 仅 `ContentLength` 预检 | chunked 无上限（`handler.go:1004-1021`） |
| `/api/v2/buckets` body | 不适用 | 仅 `ContentLength` 预检 | 同上（`handler.go:1160-1173`） |
| `/api/v1/prom/read` body | 无限制（`io.ReadAll`） | 无限制 | 未改善（`handler.go:2033`） |
| 写并发限流 | `/write`、`/api/v1/prom/write` | 同 | `/api/v2/write` 仍未覆盖 |
| 连接数上限 | `max-connection-limit` | 同 | 超出直接 `Close`，不排队 |
| 查询取消 | `CloseNotifier` → `closing` | 同 | 仍是 deprecated API |
| HTTP server 超时 | 无 Read/Write/IdleTimeout | 无 | 两版都只设置 `Handler`（`v1.12.3:services/httpd/service.go:132-134`） |

### 5.4 升级检查清单

1. **证书权限**：确认 `https-private-key` 权限 ≤0600、证书 ≤0644；
   否则要么修权限，要么显式 `https-insecure-certificate = true`。
2. **脚本兼容**：`SHOW QUERIES` 多一列；`SHOW STATS` 少 `valuesWrittenOK`。
3. **Flux 客户端**：如果依赖"Flux 关闭时返回 403"，需要适配 401。
4. **V2 客户端**：现在可以使用 `/api/v2/delete`、`/api/v2/buckets*`；
   但 labels/members/owners 一律 405。
5. **SIGHUP 语义变化**：旧版没有注册 SIGHUP handler，Unix 上的默认动作是**终止进程**
   （且不走优雅关闭）；新版会触发配置重载，**配置写错只会让重载失败并打错误日志**，
   进程继续用旧配置服务。
6. **SIGTERM 查询转储**：需要排障时打开 `[coordinator] termination-query-log=true`，
   进程收到 SIGTERM 会把在跑的查询写进日志（`fd1d51690d`）。
7. **Flux 资源**：如启用 Flux 且希望限制资源，配置
   `[flux-controller]` 的 `query-concurrency`/`query-queue-size`/
   `query-max-memory-bytes`；不配置则保持旧的"不限制"行为。

---

## 6. 具体案例

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
2. `userName := user.ID()`，写入 `ExecutionOptions.UserID`（`:724-737`）。
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

**请求 A（正常 V1 写入）**：

```
POST /write?db=telegraf&rp=autogen&precision=s
Authorization: Token admin:secret
Content-Encoding: gzip
Content-Length: 120

<gzip: cpu,host=web01 usage=42.1 1704067200>
```

两版共同路径：

1. `serveWriteV1` 校验 precision（`v1.12.3:services/httpd/handler.go:1651-1666`）。
2. `serveWrite` 校验 `database != ""`、库存在、`AuthorizeWrite`。
3. `body := truncateReader(r.Body, MaxBodySize)`（`:1701-1704`）。
4. **gzip 分支覆盖 `body`**（`:1707-1715`）：截断器被丢弃。
   由于 `ContentLength=120` 且小于 25MB，预检通过；解压后如果膨胀到 1GB，
   两版都会照读不误。
5. `ParsePointsWithPrecision`，`PointsWriter.WritePoints`，成功 204。

差异只在统计：旧版会额外走 `WritePointsWithContext` 并累加
`ValuesWrittenOK`（`v1.8.10:services/httpd/handler.go:989-1010`），新版没有这一步
（`v1.12.3:services/httpd/handler.go:1772`），因此 `SHOW STATS` 的 httpd 节点少一个字段。

**请求 B（V2 写入 + 非法 precision，展示 bug 修复）**：

```
POST /api/v2/write?bucket=telegraf/autogen&precision=xyz
```

```go
// v1.8.10:services/httpd/handler.go:855-865
default:
    err := fmt.Sprintf("invalid precision %q (use ns, us, ms or s)", precision)
    h.httpError(w, err, http.StatusBadRequest)      // 先写 400
}
db, rp, err := bucket2dbrp(r.URL.Query().Get("bucket"))
if err != nil { ... }
h.serveWrite(db, rp, precision, w, r, user)         // ← 仍然继续，precision 仍是 "xyz"
```

同一 commit 也修了 V1 的 `/write`：`precision=xyz` 时旧版同样先写 400 再继续解析写入。

`w` 已经被 `h.responseWriter` 包装为 `ResponseWriter`；`httpError` 已经
`writeHeader(400)` 并写了 body。随后 `serveWrite` 若解析成功，会再次
`writeHeader(204)`（标准库记录 superfluous），客户端最终看到 400，
但**数据可能已经写入**。

```go
// v1.12.3:services/httpd/handler.go:1636-1647
default:
    err := fmt.Sprintf("invalid precision %q (use ns, us, ms or s)", precision)
    h.httpError(w, err, http.StatusBadRequest)
    return                                          // ← 修复
}
db, rp, err := bucket2dbrp(...)
h.serveWrite(db, rp, precision, w, r, user)
```

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
    C->>S: ApplyReloadedConfig(reloadedConfig, log)
    S->>H: PrepareReloadConfig(config.HTTPD)
    H->>H: https 未变？是
    H->>TM: PrepareCertificateLoad(cert, key)
    TM->>CL: PrepareLoad(cert, key)
    CL->>CL: loadCertificate：权限检查 + X509KeyPair + 解析 leaf
    CL-->>TM: apply 闭包（尚未替换）
    TM-->>H: apply
    H-->>S: apply
    S->>S: 全部服务验证通过
    S->>H: apply()
    H->>CL: SetCertificate(loadedCert)
    CL->>CL: 持锁替换 cert/leaf，checkCurrentCert 打过期告警
    Note over CL: 下一次 TLS 握手<br/>GetCertificate 返回新证书
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

旧版（v1.8.10）顶层键：`system`、`cmdline`、`memstats`、以及
`httpd`/`runtime`/`network`… 等统计对象。

新版（v1.12.3）额外：

```json
{
  "system":  {"currentTime": "...", "started": "...", "uptime": 123},
  "build":   {"Version": "...", "Commit": "...", "Branch": "...", "Build Time": "..."},
  "cmdline": [],
  "config":  {"aggressive-points-per-block": 10000,
              "compact-throughput": 50331648,
              "compact-throughput-burst": 50331648,
              "max-concurrent-compactions": 0},
  "stats":   {"compact-throughput-usage-percentage": 37.5},
  "crypto":  {"ensureFIPS": false, "FIPS": false, "implementation": "Go", "passwordHash": "bcrypt"},
  "memstats": {},
  "cq":      {"queryOk": 12, "queryFail": 0},
  "httpd":   {"name": "httpd", "tags": {}, "values": {"req": 100, "...": "..."}}
}
```

注意：

- `config` 实际输出 `tsdb.Config.Diagnostics()` 的全部字段，示例只列 4 个，是节选。
- 除 `cq` 外，服务统计仍走 `json.Marshal(models.Statistic)`，所以带 `{"name","tags","values"}` 包装。
- `build` 的时间键是 `Build Time`（`monitor/build_info.go:14-19`），不是 `Time`。
- `config` 里的 `compact-throughput` 是 `toml.Size`，被
  `parseConfigDiagnostics` 转成 `uint64`；`toml.Duration` 转成字符串
  （`handler.go:2715-2722`）。
- `stats` 只在压缩限流器存在时出现（`monitor/service.go:156`）。
- `crypto` 在 OSS 且无 `crypto` 诊断时使用默认值（`handler.go:2424-2426`）。
- 这些输出**没有认证保护**（当 `pprof-auth-enabled=false` 时），
  等于把 TSDB 调优参数暴露给任何能访问 8086 的人。

---

## 7. 仍存在的风险

1. **`/api/v2/write` 不受写限流**。`AddRoutes` 的 switch 仍只匹配
   `/write` 与 `/api/v1/prom/write`（`v1.12.3:services/httpd/handler.go:528-535`）。
   V2 写入与 V1 共用 `serveWrite`，因此这是纯粹的保护缺口。
2. **`/debug/vars`、`/debug/requests` 的认证取决于开关组合**。
   当 `AuthEnabled=false`，或 `PprofAuthEnabled=false` 时，
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
13. **`https-enabled` 运行中不可改**。这是有意为之，但错误信息只在
    重载日志里出现，容易被忽略。

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
| WindowAggregate 读 | 无 | `storage/flux/reader.go:85-93`、`:609-843` |
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
| `6795ec6c01` | — | 移除 context-value 写法，`valuesWrittenOK` 消失 |
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
