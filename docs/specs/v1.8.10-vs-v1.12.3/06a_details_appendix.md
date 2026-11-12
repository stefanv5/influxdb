# 06 附录：HTTP 服务、TLS 与 Flux 入口 —— 逐项差异全文与详细案例

> 本文件是 [06_http_flux_diff.md](06_http_flux_diff.md) 的详细附录：主文档保留结论与同编号标题，完整代码、表格、失败矩阵与案例集中在这里。

## 基线

- OLD（旧）: `v1.8.10` = commit `688e697c51`（2021-10-11）。
- NEW（新）: `v1.12.3` = commit `f3d5cb68ab`（本仓库 `docs/specs/` 的代码基准）。
- 区间: 527 commits / 602 files / +79,310 −45,239；当前工作树只额外包含若干 docs 提交（数量随文档演进），代码相同。
- 旧版本证据统一来自 `git show v1.8.10:<path>`；新版本证据来自当前工作树。

## 范围

本附录收录：

- 主文档 §3「逐项差异」的全部原文：§3.1–§3.20（含 §3.3.1、§3.12a-e、§3.14a-e、§3.15a-d）；小节编号与标题不变。
- 主文档移出的 §6.2「`/write` 请求的新旧对照」、§6.4「`/debug/vars` 输出对照」、§6.5「Flux：并发 4 个查询时的排队与 500」全文。
- 原文中的 Go/HTTP/JSON/TOML 代码块、表格、失败矩阵与版本化证据行（`v1.8.10:path:line` / `v1.12.3:path:line`）均原样保留；§3 无 Mermaid，唯一的 §6.3 时序图保留在主文档。

本附录**不**重复主文档仍完整保留的内容：§0–§2、§4、§5、§6.1、§6.3、§6.6、§7、§8。

## 与主文档的关系

- 编号一一对应：主文档 `### 3.x` / `#### 3.x` 与本附录同名标题的编号相同；跨文档引用 `06 §3.x` 仍指向主文档的同编号标题，本附录只做补充，不新增或改动编号。
- 主文档每个小节给出 1–2 句结论，并链接到本附录对应锚点；本附录承载该小节的完整正文与证据。
- §6.2、§6.4、§6.5 在主文档保留同编号摘要与链接，完整推演见本附录同编号小节；§6.1、§6.3、§6.6 的全文只在主文档。
- 原文中的“本篇/本文”均指 06 主文档。

## 编号与锚点

- §3 锚点：`#s3-1`、`#s3-2` … `#s3-20`；§3.3.1 为 `#s3-3-1`。
- §6 锚点：`#s6-2`、`#s6-4`、`#s6-5`。
- 锚点使用显式 HTML 的 `a id` 标签，不依赖平台对中文标题的自动 slug 规则，保证主文档链接可达。

## 3. 逐项差异

> 编号与主文档 §3 完全一致；正文、表格、代码与证据均为主文档拆分前的原文。


<a id="s3-1"></a>

### 3.1 路由表逐条对照

旧版路由集中在 `NewHandler` 的三段 `AddRoutes` 调用中：基础路由
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
| GET/POST | `/api/v2/buckets/:dbrp/labels` | ❌ | ✅ `serveLabelsNotAllowedV2` | **新增**，405（`:238-245`） |
| DELETE | `/api/v2/buckets/:dbrp/labels/:labelID` | ❌ | ✅ `serveLabelsNotAllowedV2` | **新增**，405（`:246-249`） |
| GET/POST | `/api/v2/buckets/:dbrp/members` | ❌ | ✅ `serveBucketMembersNotAllowedV2` | **新增**，405（`:250-257`） |
| DELETE | `/api/v2/buckets/:dbrp/members/:userID` | ❌ | ✅ `serveBucketMembersNotAllowedV2` | **新增**，405（`:258-261`） |
| GET/POST | `/api/v2/buckets/:dbrp/owners` | ❌ | ✅ `serveBucketOwnersNotAllowedV2` | **新增**，405（`:262-269`） |
| DELETE | `/api/v2/buckets/:dbrp/owners/:userID` | ❌ | ✅ `serveBucketOwnersNotAllowedV2` | **新增**，405（`:270-273`） |
| POST | `/api/v1/prom/write` | ✅ `servePromWrite` | ✅ | 协议类型换成 `prompb` |
| POST | `/api/v1/prom/read` | ✅ `servePromRead` | ✅ | 协议类型换成 `prompb` |
| GET/HEAD | `/ping` | ✅ `servePing` | ✅ | 不变 |
| GET/HEAD | `/status` | ✅ `serveStatus`（已废弃） | ✅ | 不变 |
| GET | `/health` | ✅ | ✅ | 路由 Name 由 `ping` 改为 `health`（仅元数据；`logging`/`recovery` 的 `name` 形参未使用，不进入访问日志） |
| OPTIONS | `/health` | ❌ | ✅ `serveOptions` | **新增**（`:310-313`） |
| GET | `/metrics` | ✅ `promhttp` | ✅ | 不变 |
| POST | `/api/v2/query` | ✅（FluxEnabled 决定 handler） | ✅ | 关闭分支改走认证；新增字节统计 |
| OPTIONS | `/api/v2/query` | ❌ | ✅ `serveOptions` | **新增**（`:371-374`） |
| GET | `/debug/pprof/{cmdline,profile,symbol,all}` | ✅ 条件注册 | ✅ 条件注册 | 不变（认证分支修了 nil panic） |
| GET | `/debug/vars` | ✅ 条件注册 | ✅ 条件注册 | 内容大幅扩展 |
| GET | `/debug/requests` | ✅ 条件注册 | ✅ 条件注册 | 不变 |

关键点：

- 新增路由全部是**兼容面**，没有删除任何旧路由；因此 v1.8.10 的客户端脚本不需要改。三条新增 OPTIONS 路由虽然注册了 `serveOptions`，但每条路由都被 `cors` 包裹，`cors` 在 `r.Method == "OPTIONS"` 时直接 `return`（`v1.12.3:services/httpd/handler.go:2966-2968`），所以实际响应是**隐式 200**（body 为空）：CORS 头（仅当请求带 `Origin`，`handler.go:2937-2955`）+ 外层 `ServeHTTP` 的 `X-Influxdb-Version`/`X-Influxdb-Build`（`:562-563`）+ `requestID` 的 `X-Request-Id`/`Request-Id`（`:2974-3000`）；路由里的 `serveOptions`（204）不可达。
- `/api/v2/buckets*` 用 **pat** 风格参数 `:dbrp`，源码用
  `r.URL.Query().Get(":dbrp")` 读取（`v1.12.3:services/httpd/handler.go:1265-1277`、
  `:1280`、`:1313`、`:1390`），不是 Go 1.22 的 `r.PathValue`。值必须 URL 编码：`pat` 的
  `matchPart` 遇 `/` 即断段（`bmizerany/pat mux.go:254-286`），`db/rp` 要写成 `db%2Frp`；
  `try` 随后 `url.QueryUnescape`，handler 拿到的是解码后的值。写原始 `/` 会让路由不匹配（404/405）。
- `labels/members/owners` 九条路由是"占位拒绝"，返回 405（文案分别为
  `bucket labels/members/owners not supported in this version`，
  `handler.go:1610/1615/1620`），让 Terraform / influx CLI 等 V2 客户端得到明确答案。

<a id="s3-2"></a>

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
    default:
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

**写限流（`/write`、`/api/v1/prom/write`）的失败语义**：`writeThrottler` 在 `authenticate` **之后**包装（`:514-532`），所以它位于认证外层——请求先过限流，再解析凭据。`max-concurrent-write-limit>0` 时启用限流（`cap(t.current)==0` 则直接返回原 handler，`handler.go:3176-3179`；配置接线见 `:171-172`、`config.go:65-67`）：

- `max-enqueued-write-limit>0` 且入队 channel（容量 `concurrent + enqueued`）已满：立即 `http.Error(w, "request throttled, queue full", 503)`（`:3194-3198`；`http.Error` 用 `fmt.Fprintln` 追加换行，实际 body 为 `request throttled, queue full\n`，下一条超时变体同理）；
- 未立即拿到并发槽、等待超过 `enqueued-write-timeout`（默认 **30s**，`config.go:29-30,95`）：`http.Error(w, "request throttled, exceeds timeout", 503)`（`:3203-3214`）；timeout 为 0 时 `timerCh` 为 nil，会无限等待而不是超时（`:3174-3190`）。

两条 503 都由 `http.Error` 产生：`Content-Type: text/plain; charset=utf-8`、`X-Content-Type-Options: nosniff`，**不带 `X-InfluxDB-Error`**，也不是 JSON body。`http.Error` 会以 `fmt.Fprintln` 追加 `\n`，所以两种 body 分别是 `request throttled, queue full\n` 与 `request throttled, exceeds timeout\n`（与 405/404 的处理一致）。因为限流先于认证，拥塞时即使未提供/提供了错误凭据也先得到 503，而不是 401/403；503 表示“尚未尝试写入”，客户端应退避重试而不是当作业务写入失败。`POST /api/v2/write` 不经过该中间件。

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
   `Access-Control-Allow-Headers` 增加 `User-Agent`（`0e458e7175`）（旧 `v1.8.10:services/httpd/handler.go:1878-1895` /
   新 `v1.12.3:services/httpd/handler.go:2939-2955`）。但 `/api/v2/buckets*` 的 14 条路由
   （`handler.go:218-273`）与 `POST /api/v2/delete`（`handler.go:214-217`）**都没有 OPTIONS 路由**；`cors` 是路由级包装，`pat` 在 `handlers["OPTIONS"]`
   未命中后按命中的其他 method 写 `Allow` + **405**（`mux.go:111-138`），请求根本进不到
   `cors`。因此加 `PATCH` 只让实际响应可带 CORS 头，**浏览器预检仍不可用**；需要为 bucket
   路由补注册 OPTIONS handler（参照 `/api/v2/write`、`/health`、`/api/v2/query`）。

<a id="s3-3"></a>

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

<a id="s3-3-1"></a>

#### 3.3.1 路由 × 凭据 × 开关 × 授权矩阵（唯一矩阵）

**读法**：`authenticate` 只在 `AuthEnabled=true` 且 `AdminUserExists()==true` 时校验凭据；缺凭据/凭据错误返回 **401**。引导模式（无管理员用户）下 `authenticate` 直接以 `user=nil` 调 handler，各 handler 的 `if AuthEnabled && user==nil` 分支返回 **403**。`403` 也用于“已认证但授权失败”。本矩阵是 06 的唯一认证表，其余小节只写本路由的差异。

| 路由（方法） | 凭据来源 | 认证包装 | 授权钩子 | 默认值/默认行为 | 失败状态码 |
|--------------|----------|----------|----------|------------------|------------|
| `OPTIONS /query`、`/write`、`/api/v2/write`、`/api/v2/query`、`/health` | 无 | 2 参 `serveOptions`，不经 `authenticate` | 无 | 无认证；`cors` 在 OPTIONS 直接 return，实际隐式 200 | 200 |
| `GET/POST /query` | `u/p`、`Token`、`Basic`、`Bearer` | `authenticate`（`AuthEnabled`） | 语句级 `QueryAuthorizer`（11） | AuthEnabled=false；开启后无凭据 401 | 401；执行期 400/403 |
| `POST /write`、`/api/v2/write` | 同上 | `authenticate`；`/write` 另有 `writeThrottler` 在外层 | `WriteAuthorizer.AuthorizeWrite` | AuthEnabled=false；开启后无凭据 401；`/api/v2/write` 不经写限流 | 401；授权失败 403；`/write` 队列满/排队超时 **503**（先于认证，见 §3.2） |
| `POST /api/v2/delete` | 同上 | `authenticate` | `AuthorizeWrite(user, db)`（复用写权限） | AuthEnabled=false | 401；`user==nil`/授权失败 403 |
| `POST /api/v2/buckets` | 同上 | `authenticate` | 库不存在 `AuthorizeCreateDatabase`；库存在 `AuthorizeCreateRetentionPolicy` | AuthEnabled=false | 401；403 |
| `DELETE/PATCH /api/v2/buckets/:dbrp`、`GET /api/v2/buckets/:dbrp` | 同上 | `authenticate` | Delete→`AuthorizeDeleteRetentionPolicy`；Update→`AuthorizeCreateDatabase`；Retrieve→`AuthorizeDatabase(Read)` | AuthEnabled=false | 401；403 |
| `GET /api/v2/buckets`（普通遍历） | 同上 | `authenticate` | 逐库 `AuthorizeDatabase(Read)` | AuthEnabled=false | 401；无权限库被跳过（200） |
| `GET /api/v2/buckets`（`?id=` 或 `?name=` 单桶） | 同上 | `authenticate` | **无**（`oneBucket` 不检查权限，`:1429-1452`） | AuthEnabled=false | 401；已认证低权用户 200（已知缺口） |
| `POST /api/v1/prom/write` | 同上 | `authenticate`；`writeThrottler` 在外层 | `AuthorizeWrite(user, db)` | AuthEnabled=false | 401；403；队列满/排队超时 **503**（先于认证，见 §3.2） |
| `POST /api/v1/prom/read` | 同上 | `authenticate` | **仅当** `AuthEnabled && PromReadAuthEnabled` 时 `AuthorizeDatabase(Read)` | `PromReadAuthEnabled=false` → 只认证、不授权 | 401；403 |
| `POST /api/v2/query`（Flux） | 同上 | `authenticate`（关闭分支 `serveFluxQueryDisabled` 同样被包装） | 控制器/查询依赖里的 authorizer（owner 03 §3.1） | `FluxEnabled=false`；关闭时 403 | 401；关闭 403；队列/内存错误 500 |
| `GET /debug/pprof/*` | 同上 | 仅 `AuthEnabled && PprofEnabled && PprofAuthEnabled` 时注册路由 + `authenticate` + `AuthorizeUnrestricted` | 管理员权限 | `PprofAuthEnabled=false` → 前缀分支**无认证** | 全部开启：401/403；`PprofAuthEnabled=false`：200；`PprofEnabled=false`：404 |
| `GET /debug/vars`、`/debug/requests` | 同上 | 与 pprof 同一条件；`PprofEnabled=false` 时前缀分支仍会命中（`handler.go:567-577`） | 管理员权限（仅注册路由时） | 默认 `PprofAuthEnabled=false` → 无认证 | 全部开启：401/403；否则 200 |
| `GET /metrics`、`/ping`、`/status`、`/health` | 同上 | `authWrapper` 仅在 `AuthEnabled && PingAuthEnabled` 时转 3 参并 `authenticate` | 无（`servePing`/`serveStatus`/`serveHealth` 不检查权限） | `PingAuthEnabled=false`（默认）→ 2 参直达、无认证 | 开启后无凭据 401；授权无 403 分支 |

**备注**：

- `/metrics` 的 `promhttp.Handler()` 与 ping/status/health 共用 `authWrapper`；默认 `PingAuthEnabled=false` 时匿名可读（风险 16）。它只暴露默认 Prometheus gatherer（Go/进程指标 + Flux 开启时注册的 `qc_*`），不包含 HTTP 请求计数；HTTP 统计要看 `/debug/vars` 或 `SHOW STATS`。
- `pprof`/`debug` 的“全部开启”指 `AuthEnabled=true && PprofEnabled=true && PprofAuthEnabled=true`；缺任意一个，`ServeHTTP` 会走无认证前缀分支（`PprofEnabled=false` 时 pprof 不匹配，但 `/debug/vars`、`/debug/requests` 仍匹配）。
- 认证来源的解析顺序是 `u/p` → `Bearer` → `Token` → `Basic`（`parseCredentials`，`handler.go:2785-2830`）；JWT 只校验 HMAC、`exp`、`username`，不校验库权限。
- 写限流 503 只覆盖 `/write` 与 `/api/v1/prom/write`，且位于 `authenticate` 外层：拥塞时未认证请求同样先得到 `text/plain` 的 503，**没有** `X-InfluxDB-Error`（详见 §3.2）。

<a id="s3-4"></a>

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

**可复现对照**：

```bash
# v1.12.3
$ influxd config | grep access-log-status-filters
access-log-status-filters = []

# v1.8.10：无输出（NewConfig 未初始化该字段，配置输出省略空值）
$ influxd config | grep access-log-status-filters
$
```

运行时请求级对照（默认配置，`log-enabled=true`）：

| 请求 | 状态 | 默认 `[]`（与旧版 nil 等价） | `access-log-status-filters = ["4xx"]` |
|------|------|------------------------------|----------------------------------------|
| `GET /ping` | 204 | 写一行 CLF | 不写 |
| `GET /nope` | 404 | 写一行 CLF | 写一行 CLF |

CLF 行形如 `127.0.0.1 - - [10/Nov/2026:12:00:00 +0000] "GET /nope HTTP/1.1" 404 19 "" "curl/8.5.0"`（`buildLogLine`，`services/httpd/response_logger.go:83-...`；404 body `404 page not found\n` 为 19 字节）。`StatusFilters.Match` 对 `len==0` 返回 true（`services/httpd/config.go:198-206`），因此新版的空切片与旧版 nil 在运行时完全一致。

<a id="s3-5"></a>

### 3.5 用户维度查询字节（#27188）

旧版只统计全局 `QueryRequestBytesTransmitted`
（`FluxQueryRequestBytesTransmitted` 是 #25669 在本区间才加入的），`Statistics` 结构中
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

**请求/输出案例**（`auth-enabled=true`、`user-query-bytes-enabled=true`，`alice` 执行一次非 chunked `SHOW DATABASES`）：

响应体为 `{"results":[{"statement_id":0,"series":[{"name":"databases","columns":["name"],"values":[["db0"]]}]}]}` 加一个 `\n`，共 **103 字节**；`handler.go:911-913` 执行 `QueryRequestBytesTransmitted += 103` 与 `addQueryBytesForUser("alice", 103)`。

随后 `GET /debug/vars`：

```json
"userquerybytes::8086": {"name":"userquerybytes","tags":{"bind":":8086","user":"alice"},"values":{"userQueryRespBytes":103}}
```

`bob` 再执行同一条查询后，输出两个独立顶层键（`uniqueKeys` 对同名统计追加序号，`handler.go:2509-2514`）：

```json
"userquerybytes::8086":   {"name":"userquerybytes","tags":{"bind":":8086","user":"alice"},"values":{"userQueryRespBytes":103}},
"userquerybytes::8086:1": {"name":"userquerybytes","tags":{"bind":":8086","user":"bob"},"values":{"userQueryRespBytes":103}}
```

默认 `bind-address=":8086"` 时真实顶层键是 `userquerybytes::8086` / `userquerybytes::8086:1`：`httpd.Service.Statistics` 先把 `{"bind": s.addr}` 合并进 tags（`services/httpd/service.go:339-341`、`services/httpd/config.go:17-18,79`），`serveExpvar` 拼 key 时 `bind` 优先于 `database`（`handler.go:2484-2495`）。按旧示例执行 `jq '.userquerybytes'` 会得到 `null`。未认证用户（`auth-enabled=false`）的 `user` 为空，标签值是 `(anonymous)`（`StatAnonymousUser`，`services/httpd/service.go:54-56`）。关闭 `user-query-bytes-enabled` 时该键完全不出现（`handler.go:483-485`）。

<a id="s3-6"></a>

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

<a id="s3-7"></a>

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

**案例（`SHOW QUERIES` 的过滤与列，`auth-enabled=true`）**：用户 alice 只对 `db0` 有 READ 权限，对 `db1` 无权限。

| 请求/状态 | 结果 |
|-----------|------|
| bob 在 `db1` 执行 `qid=1` 的 `SELECT * FROM cpu`；carol 在 `db0` 执行 `qid=2` 的 `SELECT * FROM mem`（`qid` 从 1 起分配，`query/task_manager.go:93-99`） | `ExecutionOptions.UserID` 分别为 `bob`/`carol`（`services/httpd/handler.go:724-737`），随后写入 `qi.userID`（`query/task_manager.go:214-217`） |
| alice（非管理员）执行 `SHOW QUERIES` | `ctx.CoarseAuthorizer` 是 `userQueryAuthorizer`（`handler.go:739-747`）；对 `qid=2` 的 `database=db0` 授权通过，对 `qid=1` 的 `database=db1` 返回 false 被 `continue` 跳过（`query/task_manager.go:143-147`） |
| 输出 | 列 `["qid","query","database","duration","status","user"]`（`query/task_manager.go:30-35`），只剩 `[2,"SELECT * FROM mem","db0","1s","running","carol"]`；`duration` 经 `prettyTime` 截断到秒/毫秒（`:161-171`） |
| admin 执行 | `OpenCoarseAuthorizer`/全权用户看到 1 和 2 两行 |
| v1.8.10 同场景 | 没有 authorizer 参数、不过滤（`v1.8.10:query/task_manager.go:129-156`），alice 看到 1+2；列只有 5 个（`qid,query,database,duration,status`），按列位置解析的客户端会把 `user` 错位 |

`database` 为空的查询（如 `SHOW DATABASES`）不受该过滤影响（条件带 `qi.database != ""`）。

<a id="s3-8"></a>

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
（`certconfig.go:329-339`），替换证书只需替换 loader 内部指针，不必重建 listener。注意 `Certificate()`/`GetCertificate` 用同一个 `sync.Mutex`（排他），每次 TLS 握手都串行取一次指针（`certconfig.go:306-311,329-339`）。

**证书过期巡检（`monitorCert`）**：`NewTLSCertLoader` 在首次 `Load` 成功后启动一个 goroutine（`pkg/tlsconfig/certconfig.go:247-274`），每 `DefaultCertificateCheckTime=1h`（`:28-29`）调用 `checkCurrentCert`：用当前 leaf 证书判断“已过期 / 尚未生效 / 5 天内过期”，只打 `Warn`，**不会自动重载或替换证书**（`:456-485`、`:512-533`）。`SetCertificate` 成功后也会立即 `checkCurrentCert`（`:278-289`）；`Close` 只 `close(closeCh)` 发退出信号、**不 join**（`:429-435`）：`monitorCert` 的 select 在 `closeCh` 与 ticker 同时 ready 时可能再执行一次 `checkCurrentCert`（`:512-533`），所以 `Close` 返回后仍可能多一条 `Warn`。因此巡检与 3.9 的两阶段热重载是两件事：巡检只告警，替换只发生在 SIGHUP 的 apply 阶段。

**案例（巡检只告警，不换证）**：证书 `NotAfter=2026-11-13T00:00:00Z`，当前 `2026-11-10T12:00:00Z`，剩余 60 小时（小于 5 天窗口）。

| 时刻/动作 | 输出 |
|-----------|------|
| 启动时 `NewTLSCertLoader` → `Load` | `Load` 成功后调用 `logX509CertIssues(log, loadedCert.Leaf)`（`certconfig.go:392`），日志 `Certificate will expire soon`，字段 `NotAfter=2026-11-13T00:00:00.000000Z`、`untilExpires=216000000.000ms`（`zap.Duration` 在本仓库 logger 中按毫秒串编码，`logger/logger.go:73-76`；判断见 `certconfig.go:453-454/474`） |
| 之后每个 `1h` tick（`certconfig.go:28-29`） | 重复同一条 `Warn`；**不会**重新读文件、不会替换 `cl.cert`；TLS 握手继续用当前证书 |
| `2026-11-13T00:00:01Z` 后的第一个 tick | `checkCurrentCert` → `isCertExpired` 优先命中，日志变为 `Certificate is expired`（`:469-470`）；仍是同一张证书，除非 SIGHUP |
| SIGHUP + 校验通过 | `PrepareLoad` → apply → `SetCertificate`（`:278-289`）→ `checkCurrentCert`；新证书生效，后续握手用新证书 |
| SIGHUP + 新 key 0644 | `PrepareLoad` 返回权限错误；`ApplyReloadedConfig` 不执行 apply，旧证书继续服务（§3.11/§6.3） |
| `Service.Close()` | `tlsManager.Close()` → `certLoader.Close()` 只关闭 `closeCh`（`configmanager.go:381-385`、`certconfig.go:429-435`），**不等待**巡检 goroutine；它随后打 `Closing TLS certificate monitor` 退出，但 Close 返回前可能因 select 竞争再执行一次 tick、多一条 Warn |

<a id="s3-9"></a>

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
- **作用域只有 TLS 证书**：`ApplyReloadedConfig` 只收集 `HttpdService.PrepareReloadConfig` 与 `OpenTSDBServices.PrepareReloadTLSCertificates`（`server.go:583-627`）。实际 apply 的只有两条：①HTTPD 的 `https-certificate`/`https-private-key`（`services/httpd/service.go:296-322`，且仅当启动时 `https-enabled=true`；为 false 时 `PrepareReloadConfig` 返回 `(nil,nil)`，什么也不做）；②OpenTSDB 当前配置路径的证书（`services/opentsdb/service.go:209-212`）。以下全部**不 apply 且不报错**：`https-insecure-certificate`（只在 `Open` 时装配 `tlsconfig.WithIgnoreFilePermissions`，`services/httpd/service.go:162-164`）、`auth-enabled`、`max-body-size`、`max-concurrent-write-limit`、`[flux-controller]`、`[coordinator]`、retention/precreator 等。`https-enabled` 单独处理：值变化时验证阶段直接返回 `httpd: can not change https-enabled on a running server`（`service.go:300-303`），整次 reload 一个配置都不 apply。失败时进程不退出、旧配置/旧证书继续服务；日志固定为 HTTPD 的 `error reloading httpd service config, no new configuration applied`（`cmd/influxd/run/server.go:596`）或 command 的 `error applying reloaded config; some config values may have been applied`（`cmd/influxd/run/command.go:206`）。改证书以外的配置必须重启。
- **同一区间还新增 SIGTERM 查询转储**：收到 SIGTERM 时若 `[coordinator] termination-query-log=true`，
  会把当前查询写入日志（`fd1d51690d` #22638；`cmd/influxd/main.go:91-109`、`cmd/influxd/run/server.go:759`）。
- **生命周期窗口**：`ReloadConfig` 在信号循环的同一 goroutine 内同步执行（`cmd/influxd/main.go:85-104`）；`PrepareReloadConfig` 持 `Service.mu` 做证书文件 IO（`services/httpd/service.go:296-322`，慢盘/NFS 可达秒级），期间 `shutdownCh`（cap 1）中的 SIGTERM 不被处理，systemd stop 只能等 reload 完成或超时。建议把 reload 放到独立 goroutine 或限制证书 IO 时长。

<a id="s3-10"></a>

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

**案例：双向 TLS 的三条握手分支**（`pkg/tlsconfig/configmanager_test.go:1090-1193`，同一套 API 可直接复现）：

```go
serverManager, _ := NewTLSConfigManager(true, nil, serverCert, serverKey, false,
    WithClientCAFiles(caPath),
    WithClientAuth(tls.RequireAndVerifyClientCert))
ln, _ := serverManager.Listen("tcp", "127.0.0.1:0")
// tls.Config.ClientAuth=RequireAndVerifyClientCert，ClientCAs=caPath
```

| 客户端 | 握手结果 | 客户端错误/输出 | 服务端错误/输出 |
|--------|----------|------------------|------------------|
| 用 `clientManager`（`GetClientCertificate` 返回 CA 签发的证书） | 成功 | `Dial` 返回 nil，读写回显 `hello`（HTTP 场景即 200） | 无错误 |
| 用另一 CA 签发的客户端证书 | 失败 | `remote error: tls: certificate required`（`configmanager_test.go:1187`） | `tls: client didn't provide a certificate`（`:1192`）——`GetClientCertificate` 的 `cri.SupportsCertificate(cert)` 失败后返回**空证书 + nil error**（`certconfig.go:355-361`），TLS 栈因此不发送证书 |
| 完全不带客户端证书（裸 `tls.Dial`） | 失败 | 同上 `remote error: tls: certificate required` | 同上 `tls: client didn't provide a certificate` |

第二条与第三条在服务端看来相同：`GetClientCertificate` 的“服务端不接受时返回空证书”是为了贴合 Go 原生行为并让两端都得到可读错误；客户端不会看到 `bad certificate`（那是“服务端证书不被客户端信任”的另一条测试，`configmanager_test.go:916-958`）。HTTP 服务没有暴露 `WithClientAuth` 的配置项，以上装配仅在企业版/自定义 `Service` 中可达。

<a id="s3-11"></a>

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

**案例 A（升级后启动失败，key 为 0644）**：

1. 配置：`https-enabled=true`、`https-certificate=/etc/ssl/influxdb.pem`（0644）、`https-private-key=/etc/ssl/influxdb.key`（**0644**）、`https-insecure-certificate=false`（默认，`services/httpd/config.go:88`）。
2. 启动链：`Service.Open`（`services/httpd/service.go:149`）→ `NewTLSConfigManager`（`:162`）→ `newTLSConfigManager`（`pkg/tlsconfig/configmanager.go:220`）→ `NewTLSCertLoader`（`certconfig.go:247`）→ `Load` → `LoadCertificate`（`:83`）→ `loadFile(keyPath, KeyMaxPermissions=0600)` → `file.VerifyFilePermissivenessF`（`pkg/file/file.go:74-88`）。
3. `0644 & ^0600 = 0044 != 0`，返回 `ErrPermissionsTooOpen`；`Service.Open` 包装为：
   ```
   httpd: error creating TLS manager: LoadCertificate: file permissions are too open: for "/etc/ssl/influxdb.key", maximum is 0600 (-rw-------) but found 0644 (-rw-r--r--); extra permissions: 0044 (----r--r--)
   ```
   进程**启动失败**，HTTP 监听未建立。证书本身 0644 不报错（上限就是 0644），只有私钥 0644 触发。
4. 修复：`chmod 600 /etc/ssl/influxdb.key`，或显式设置 `https-insecure-certificate=true`（`WithIgnoreFilePermissions(true)` 跳过该项检查，**不是**跳过 TLS 校验）；重启后 `Starting HTTP service` + `Listening on HTTP` 正常输出。

**案例 B（运行中 SIGHUP，新 key 为 0644）**：

1. 服务已用 0600 的 key 正常启动；把 `/etc/ssl/influxdb.key` 换成新文件并保持 0644，`kill -HUP <pid>`。
2. `main` 的 `reloadCh` 收到 SIGHUP（`cmd/influxd/main.go:85-104`）→ `Command.ReloadConfig` → `Server.ApplyReloadedConfig` → `HttpdService.PrepareReloadConfig`（`services/httpd/service.go:296`）→ `PrepareCertificateLoad` → `PrepareLoad` → `loadCertificate` → 同一权限错误。
3. `PrepareReloadConfig` 包装为：
   ```
   httpd: error loading certificate at ("/etc/ssl/influxdb.pem", "/etc/ssl/influxdb.key"): LoadCertificate: file permissions are too open: for "/etc/ssl/influxdb.key", maximum is 0600 (-rw-------) but found 0644 (-rw-r--r--); extra permissions: 0044 (----r--r--)
   ```
4. `ApplyReloadedConfig` 在验证阶段直接 `return err`，**不执行任何 apply**（`cmd/influxd/run/server.go:592-600`）；日志依次为 `error reloading httpd service config, no new configuration applied`（server）与 `error applying reloaded config; some config values may have been applied`（command，措辞是通用模板）。
5. 现象：进程不退出，旧证书继续服务新握手；`https-enabled` 未变，无需回滚。修复仍是 `chmod 600` 后再次 SIGHUP。

<a id="s3-12"></a>

### 3.12 可观测性：`/debug/vars`、`/metrics` 与 `SHOW STATS/DIAGNOSTICS`

#### 3.12a 角色与三个面

**角色**：HTTP 层提供三个互补的可观测面。它们都不是数据库查询接口，但共享“是否认证/是否暴露配置”的开关。

| 面 | 端点/语句 | 内容 | 数据来源 | 默认开关 |
|----|-----------|------|----------|----------|
| JSON 诊断 | `GET /debug/vars` | system/build/cmdline/config/stats/crypto/memstats + 各服务统计（含 `cq`） | `monitor.Monitor.Statistics` + `Diagnostics` | `pprof-enabled=true`、`pprof-auth-enabled=false`；实际匿名可读 |
| Prometheus | `GET /metrics` | 默认 gatherer：Go/进程指标；Flux 开启时 + `qc_*` | `promhttp.Handler()` | `ping-auth-enabled=false`；默认匿名可读 |
| SQL 统计 | `SHOW STATS` / `SHOW DIAGNOSTICS` | 各服务 `Statistics()` / `Diagnostics()` 行 | `monitor.Monitor` | 无独立开关 |

#### 3.12b 结构

| 板块/字段 | 来源 | 处理 | 证据 |
|-----------|------|------|------|
| `system` | `Diagnostics()["system"]` | `parseSystemDiagnostics` | `handler.go:2586`（函数）、`:2323-2332`（调用） |
| `build` | `Diagnostics()["build"]` | `parseBuildInfo`；时间键是 `Build Time` | `handler.go:2329-2346`、`:2628` |
| `cmdline` | `expvar.Get("cmdline")` | 原样输出 | `handler.go:2348-2354` |
| `config` | `Diagnostics()["config"]` = `*tsdb.Config` | `parseConfigDiagnostics`（`toml.Size`/`toml.Duration`） | `handler.go:2356-2382`、`:2703-2726` |
| `stats` | `Diagnostics()["stats"]` | 单行 map 摊平（压缩吞吐占用率） | `handler.go:2384-2406`、`monitor/stats.go:12-34` |
| `crypto` | `Diagnostics()["crypto"]`，OSS 无则 `ossCryptoDiagnostics()` | `parseCryptoDiagnostics` | `handler.go:2408-2439`、`:2654-2699` |
| `memstats` | `expvar.Get("memstats")` | 原样输出 | `handler.go:2441-2446` |
| `cq` | `Statistics()` 中 `Name=="cq"` | `parseCQStatistics` 只输出 `Values`，`continue` 跳过通用包装 | `handler.go:2451-2478`、`:2728-2734` |
| 其他服务统计 | `Statistics()` | `json.Marshal(models.Statistic)` + `uniqueKeys` 去重 | `handler.go:2470-2524` |

注册侧：`monitor.Open` 注册 `build`/`runtime`/`network`/`system`；`Limiter != nil` 时注册 `stats`；`TSDBConfig != nil` 时注册 `config`（`v1.12.3:monitor/service.go:146-166`）。`cmd/influxd/run/server.go:426` 注册 `cq`，`:525` 注入 compact-throughput limiter。

#### 3.12c 流程

1. `serveExpvar` 先取 `Monitor.Statistics(nil)` 与 `Monitor.Diagnostics()`；任一失败直接 500（此时尚未写 body）。
2. 设置 `Content-Type: application/json`，写出 `{`，按固定顺序拼 `system`、`build`、`cmdline`、`config`、`stats`、`crypto`、`memstats`；每个板块之间按 `first` 标记补逗号。
3. 进入 `stats` 循环：`cq` 走特判（`parseCQStatistics` → 只写 `"cq": {values}`）；其他统计 `json.Marshal` 后用 `name[:path][:id]` 等规则拼 key，`uniqueKeys` 冲突时追加 `:N`。
4. 最后写 `}` 结束。**响应是流式写出的**：一旦进入第 2 步，状态码 200 已隐式发送；后续任何 `httpError(500)` 都只能追加 body，不能改状态码。
5. `/metrics` 由 `promhttp.Handler()` 独立处理，不经过 `serveExpvar`；它不读 `Monitor.Statistics`，也不做 `uniqueKeys` 去重。

#### 3.12d 案例

见 §6.4：同一时刻分别请求 `/metrics` 与 `/debug/vars`，对照 `cq`、`config`、`userquerybytes` 与认证开关；并给出 `SHOW STATS` 中 `valuesWrittenOK` 消失、`fluxQueryRespBytes`/`promReadReq` 变化的预期。

#### 3.12e 风险

- **认证取决于开关组合**：`PprofAuthEnabled=false`（默认）时 `/debug/vars`、`/debug/requests` 走无认证前缀分支；`PprofEnabled=false` 也不影响这两个前缀。开启 `config`/`crypto` 后，TSDB 调优参数和加密配置对任何能访问 8086 的人可见（风险 2）。
- **`cq` 空 values 导致 200 截断 JSON**：`parseCQStatistics` 返回 error 后 `httpError(500)` 已无法改状态码（`handler.go:2452-2458`、`:2728-2734`）；OSS 的 CQ 总会填 `queryOk`/`queryFail`，企业扩展返回空 values 时可达（05 §7.9）。
- **`userquerybytes` 无界增长**：按用户名的 `gensyncmap.Map[string,*atomic.Int64]` 只增不减（`handler.go:153`、`:483-511`）；用户删除后条目仍在（风险 9）。
- **`/metrics` 默认匿名**：`PingAuthEnabled=false` 时不认证；Flux 关闭时只有 Go/进程指标，开启后 `qc_*` 也会暴露（风险 16）。

**差异明细**（v1.8.10 → v1.12.3）：

| 板块 | 旧版 | 新版 | 证据 |
|------|------|------|------|
| `build` | 无 | 新增 | `handler.go:2329-2346`、`:2628` |
| `config` | 无 | 新增（TSDB 配置；本区间还新增 `aggressive-points-per-block`、`compact-throughput`、`compact-throughput-burst` 三字段） | `handler.go:2356-2382`、`tsdb/config.go:304-306` |
| `stats` | 无 | 新增（压缩吞吐占用率） | `handler.go:2384-2406`、`monitor/stats.go:12-34` |
| `crypto` | 无 | 新增（OSS 默认值） | `handler.go:2408-2439`、`:2654-2699` |
| `cq` | 通用 `json.Marshal(models.Statistic)`，输出 `{name,tags,values}` | 特判为扁平 `values` | `handler.go:2452-2478`、`:2728-2734` |
| 重复 key | 只按已知 tag 拼 key，`userquerybytes` 多用户可能撞键 | `uniqueKeys` + `:N` 兜底 | `handler.go:2509-2514`（`8843a01ebf`） |
| `SHOW DIAGNOSTICS`（httpd） | 无 `flux-enabled`/`https-insecure-certificate` | 两者加入 | `services/httpd/config.go:100-118`（`a6a3bdd0f0` #25308） |
| `SHOW STATS`（httpd） | 含 `valuesWrittenOK`；`promReadReq` 恒 0 | 不含 `valuesWrittenOK`；新增 `fluxQueryRespBytes`；`promReadReq` 真实计数 | `handler.go:451-501`、`:2031-2032`；`117341fb0f`、`57ea78e984` |
| monitor 全局 tag | 裸 map（并发访问） | `tags` + `RWMutex` | `monitor/service.go:37-68` |

> `config` 实际输出 `tsdb.Config.Diagnostics()` 的全部字段，示例只列节选；`crypto` 的 `crypto` 板块在 OSS 且无诊断时使用默认值（`handler.go:2424-2426`）。`/debug/vars` 的服务统计除 `cq` 外仍带 `{"name","tags","values"}` 包装。

<a id="s3-13"></a>

### 3.13 调试面：pprof 与 `/debug/requests`（可观测性模块的一部分）

**角色**：pprof（`/debug/pprof/{cmdline,profile,symbol,all}`）与 `/debug/requests` 是排障面，不是独立的一级模块；它们与 3.12 的 `/debug/vars` 共用 `ServeHTTP` 的前缀分支和 `PprofAuthEnabled` 开关，因此放在可观测性模块内说明。

**结构**：

| 组件 | 作用 | 证据 |
|------|------|------|
| `pprof.go` | `handleProfiles` 按路径分派 `httppprof.Cmdline/Profile/Symbol` 或 `archiveProfilesAndQueries` | `v1.12.3:services/httpd/pprof.go:22-35` |
| 注册路由 | 仅 `AuthEnabled && PprofEnabled && PprofAuthEnabled` 时注册 6 条（pprof ×4 + vars + requests） | `handler.go:322-365` |
| 前缀分支 | 否则 `/debug/pprof`（仅 `PprofEnabled`）、`/debug/vars`、`/debug/requests` 直接由 `ServeHTTP` 分派 | `handler.go:567-577` |
| 授权 | 注册路由时 `user==nil || !user.AuthorizeUnrestricted()` → 403（先判空再取 ID） | `handler.go:326-334` |

**流程**：

1. `ServeHTTP` 先看三开关是否全开；全开则 `h.mux.ServeHTTP`，pprof/debug 走注册路由 + `authenticate` + 管理员检查。
2. 否则按前缀匹配：`/debug/pprof` 且 `PprofEnabled` → `handleProfiles`（**无认证**）；`/debug/vars` → `serveExpvar`（**无认证**）；`/debug/requests` → `serveDebugRequests`（**无认证**）。
3. `/debug/requests` 解析 `seconds`（默认 `DefaultDebugRequestsInterval=10s`，上限 `MaxDebugRequestsInterval=6h`，`handler.go:59-61`），然后**阻塞采集**：`TrackRequests()` 把 profile 挂到 tracker 后，timer 等满整个区间（`handler.go:2527-2575`）；期间 handler goroutine 与 `RequestProfile` 一直存在，此后**每个 HTTP 请求**的 `RequestTracker.Add` 都要在 RLock 下遍历所有活跃 profile（`services/httpd/requests.go:99-135`）。代码两版逐行等价（旧 `handler.go:1575-1630` / 新 `:2527-2582`）。长区间会放大每个请求的 Add 成本，应限制并发/时限。

**开关矩阵**（与主 10 §2.3 一致）：

| AuthEnabled | PprofEnabled | PprofAuthEnabled | `/debug/pprof*` | `/debug/vars`、`/debug/requests` |
|---|---|---|---|---|
| true | true | true | 注册路由 + `AuthorizeUnrestricted()`，非管理员 403 | 走注册路由，需管理员 |
| true | true | false | `handleProfiles`，**无认证** | 前缀分支，**无认证** |
| false | true | true/false | `handleProfiles`，**无认证**（`PprofAuthEnabled` 被忽略） | 前缀分支，**无认证** |
| 任意 | false | 任意 | 不匹配 pprof 分支（`ServeHTTP` 的前缀分支要求 `PprofEnabled`），交给 mux；pprof 路由只在三开关全开时注册（`handler.go:322-365`），因此 pat 必然返回 **404**：`Content-Type: text/plain; charset=utf-8`、`X-Content-Type-Options: nosniff`，body `404 page not found\n`（`pat mux.go:137-139` → `http.NotFound`） | 前缀分支，**无认证** |

**代码（nil 修复）**：

```go
// v1.8.10:services/httpd/handler.go:251 — 旧：user==nil 仍调用 user.ID()
if user == nil || !user.AuthorizeUnrestricted() {
    h.Logger.Info("Unauthorized request", zap.String("user", user.ID()), zap.String("path", r.URL.Path))
    h.httpError(w, "error authorizing admin access", http.StatusForbidden)
    return
}

// v1.12.3:services/httpd/handler.go:326 — 新：先判空再取 ID
if user == nil || !user.AuthorizeUnrestricted() {
    // Don't panic
    id := ""
    if user != nil { id = user.ID() }
    h.Logger.Info("Unauthorized request", zap.String("user", id), zap.String("path", r.URL.Path))
    h.httpError(w, "error authorizing admin access", http.StatusForbidden)
    return
}
```

**案例（引导模式）**：配置 `auth-enabled=true`、`pprof-enabled=true`、`pprof-auth-enabled=true`，但 meta 里还没有任何管理员用户。请求 `GET /debug/pprof/goroutine?debug=1`：
- v1.8.10：路由已注册 → `authenticate` 因 `AdminUserExists()==false` 放行并以 `user=nil` 调用 handler → `user.ID()` nil 解引用 panic → 外层 `recovery` 捕获 → **500** + panic 日志。
- v1.12.3：同一路径先判空 → **403** `error authorizing admin access`，无 panic。

**风险**：`PprofAuthEnabled=false`（默认）时，即使 `auth-enabled=true`，pprof 与 `/debug/vars`/`/debug/requests` 仍无认证；`debug-pprof-enabled=true` 还会在 `localhost:6060` 另起一个不受 `AuthEnabled`/`PprofEnabled` 约束的 `DefaultServeMux`（风险 3）；`/debug/pprof/all` 返回**未压缩 tar**（`Content-Type: application/x-tar`、`Content-Disposition: attachment; filename=profiles.tar`，`services/httpd/pprof.go:335-338`），固定包含 goroutine/block/mutex/heap/allocs/threadcreate 六个 profile 与 `SHOW SHARDS`/`SHOW STATS`/`SHOW DIAGNOSTICS` 三份文本结果；profile 用 `prof.WriteTo(buf, 0)`（protobuf 二进制，条目名 `profiles/<name>.pb.gz`；函数注释写的 debug=1 与代码不符，以代码为准，`:37-72`、`:234`）。`?cpu=true`（值非空即可）追加 CPU profile，`seconds` 默认 30s；`?trace=<duration>` 追加 trace，上限 45s（`:90-169`）。因此泄露面是“运行时画像 + shard/统计/诊断”，不是普通单 profile。本模块除 nil 修复/注释外代码基本未变。

<a id="s3-14"></a>

### 3.14 Prometheus remote write/read：协议栈迁移与端点行为

> 本节的认证/授权行（write 的 `AuthorizeWrite`、read 的 `PromReadAuthEnabled`）以 3.3.1 矩阵为准；这里只写协议与错误路径。

#### 3.14a 角色与结构

**角色**：`/api/v1/prom/write` 与 `/api/v1/prom/read` 是 Prometheus remote write/read 的兼容入口。它们把 Prometheus 的 snappy+protobuf 帧翻译成 Influx 的 points / storage 读请求，本身不实现存储。

**结构**：

| 层 | 旧（v1.8.10） | 新（v1.12.3） | 证据 |
|----|----------------|----------------|------|
| 协议类型 | 仓库自带 `prometheus/remote`（gogo/protobuf，`remote.proto`/`remote.pb.go`） | 上游 `github.com/prometheus/prometheus/prompb` | `v1.8.10:services/httpd/handler.go:1191-1196` / `v1.12.3:services/httpd/handler.go:1973-1978` |
| 解码 | `proto.Unmarshal` | 生成方法 `req.Unmarshal(reqBuf)` | 同上 |
| 写入转换 | `prometheus.WriteRequestToPoints` | 同左，但 `Sample.TimestampMs` → `Sample.Timestamp` | `v1.8.10:prometheus/converters.go:82` / `v1.12.3:prometheus/converters.go:83` |
| 读取转换 | `prometheus.ReadRequestToInfluxStorageRequest` | 同左；`Range` 变指针、`ReadSource` 变 `*anypb.Any` | `v1.12.3:prometheus/converters.go:100-129`、`services/storage/store.go:28-37` |

**字段映射（写）**：

| Prometheus | Influx | 规则 |
|------------|--------|------|
| `TimeSeries.Labels["__name__"]` | measurement | 缺失时用 `prom_metric_not_specified`（`converters.go:19`） |
| 其他 labels | tags | 原样 `map[string]string` |
| `Sample.Value` | field `value` | 固定字段名（`converters.go:20`） |
| `Sample.Timestamp`（ms） | point time | `time.Unix(0, ms*int64(time.Millisecond))` |
| `NaN`/`+Inf`/`-Inf` | 丢弃 | 计数后返回 `DroppedValuesError`（`converters.go:60-97`） |

**字段映射（读）**：

| Prometheus 请求 | storage 请求 | 规则 |
|------------------|--------------|------|
| `Queries[0].StartTimestampMs` / `EndTimestampMs` | `Range.Start/End`（ns） | `time.Unix(0, ms*1e6).UnixNano()` |
| `Queries[0].Matchers` | `Predicate` | `predicateFromMatchers`；`_measurement` 不是 source |
| `db`/`rp` query 参数 | `ReadSource` | `anypb.New(&storage.ReadSource{Database, RetentionPolicy})` |
| 响应 series tags | `LabelPair` | 去掉 `_measurement`/`_field`（`RemoveInfluxSystemTags`） |
| cursor timestamp（ns） | `Sample.Timestamp`（ms） | `ts / int64(time.Millisecond)` |

#### 3.14b 写流程与错误路径

| 步骤 | 代码/条件 | 失败状态码 |
|------|-----------|------------|
| 0. 写限流 | `writeThrottler` 在 `authenticate` 外层；队列入队 channel 满立即拒绝，等并发槽超过 `enqueued-write-timeout`（默认 30s） | **503**（`text/plain`，无 `X-InfluxDB-Error`） |
| 1. `db` 参数 | `servePromWrite` 入口 | 缺失 400；库不存在 404 |
| 2. 认证/授权 | `AuthEnabled`：`user==nil` → 403；`AuthorizeWrite` → 403（无凭据在 `authenticate` 阶段 401） | 401/403 |
| 3. body 大小 | `MaxBodySize>0` 时 `truncateReader`；`ContentLength>Max` 或读到 `errTruncated` | 413 |
| 4. snappy 解码 | `snappy.Decode(nil, buf.Bytes())` | 400 |
| 5. protobuf 解码 | `prompb.WriteRequest.Unmarshal` | 400 |
| 6. 转换 | `WriteRequestToPoints`；`DroppedValuesError` 被容忍（有效点继续写，NaN/Inf 丢弃） | 其他错误 400；丢弃不报错 |
| 7. consistency | `models.ParseConsistencyLevel` | 400 |
| 8. 写 points | `PointsWriter.WritePoints` | client error 400；authorization 403；`PartialWriteError` 400；其他 500；成功 **204** |

```go
// v1.12.3:services/httpd/handler.go:1967-1978（解码链）
reqBuf, err := snappy.Decode(nil, buf.Bytes())
if err != nil { h.httpError(w, err.Error(), http.StatusBadRequest); return }
var req prompb.WriteRequest
if err := req.Unmarshal(reqBuf); err != nil {
    h.httpError(w, err.Error(), http.StatusBadRequest); return
}
```

**案例（remote write 端到端）**：

请求：

```http
POST /api/v1/prom/write?db=telegraf&rp=autogen
Content-Type: application/x-protobuf
Content-Encoding: snappy

<snappy(protobuf WriteRequest)>
```

解码后的 `WriteRequest`：`Timeseries[0].Labels=[{Name:"__name__",Value:"cpu"},{Name:"host",Value:"web01"}]`，`Samples=[{Timestamp:1704067200000, Value:42.1}]`（`1704067200000ms = 2024-01-01T00:00:00Z`）。

| 变体 | 处理 | 响应 |
|------|------|------|
| 正常 | `WriteRequestToPoints` → `cpu,host=web01 value=42.1 1704067200000000000`；`PointsWriter.WritePoints` 成功 | **204**，空 body |
| 同一批再加 `{Timestamp:1704067201000, Value:NaN}` | `WriteRequestToPoints` 返回 1 个有效点 + `DroppedValuesError{nan:1,...}`；`servePromWrite` 判定类型后继续写有效点（`handler.go:1985-2000`） | 仍是 **204**；有效点可见，NaN 静默丢弃；只有 `write-tracing=true` 时日志出现 `Prom write handler` + `dropped unsupported Prometheus values: [NaN = 1, +Inf = 0, -Inf = 0]`（`prometheus/converters.go:43-45`）；`promWriteReq` 只 +1，没有专门的 NaN 丢弃计数 |
| body 不是合法 snappy | `snappy.Decode` 返回 `snappy: corrupt input` | **400**，`Content-Type: application/json`，`X-InfluxDB-Error: snappy: corrupt input`，body `{"error":"snappy: corrupt input"}` |
| `Content-Length > max-body-size` | 预检直接返回 | **413**，`X-InfluxDB-Error: Request Entity Too Large`，body `{"error":"Request Entity Too Large"}` |
| `db` 缺失 / 库不存在 | 入口检查（`handler.go:1907-1917`） | 400 `{"error":"database is required"}` / 404 `{"error":"database not found: \"telegraf\""}` |

#### 3.14c 读流程与错误路径

| 步骤 | 代码/条件 | 失败状态码 |
|------|-----------|------------|
| 1. 计数/tracker | `PromReadRequests++`、`requestTracker.Add` | — |
| 2. 读 body | `io.ReadAll(r.Body)`（无大小限制） | 500 |
| 3. snappy/protobuf | `snappy.Decode` → `prompb.ReadRequest.Unmarshal` | 400 |
| 4. 授权 | 仅 `AuthEnabled && PromReadAuthEnabled`：`user==nil` → 403；`AuthorizeDatabase(Read)` → 403 | 401/403 |
| 5. 转换 | `ReadRequestToInfluxStorageRequest`：`len(Queries)!=1` → 400；`anypb.New`/predicate 失败 → 400 | 400 |
| 6. 读存储 | `Store.ReadFilter(context.Background(), req)` | 400 |
| 7a. 序列化响应 | `resp.Marshal()` 失败（发生在写任何响应字节之前） | 500 JSON |
| 7b. 写出响应 | 先设 `Content-Type: application/x-protobuf`、`Content-Encoding: snappy`，再 `w.Write(compressed)`；状态码由这次写入隐式定为 200。`w.Write` 返回错误时：若首字节已写出 → `httpError(500)` 的 `WriteHeader(500)` 是 superfluous（服务端日志 `superfluous response.WriteHeader call`），`X-InfluxDB-Error` 头已无法送达，客户端得到 200 + 截断的 snappy body（连接被 RST 时可能一无所获）；若错误发生在首字节前、响应尚未提交 → 500 JSON | 200（已提交）或 500（未提交） |
| 8. 成功 | `Content-Type: application/x-protobuf`、`Content-Encoding: snappy` | **200** |

7b 的“客户端实际收到什么”取决于连接状态与内核缓冲，**待实测**：构造大 body 的 remote read，用原始 TCP 客户端读 1 字节后 `close()`，记录客户端收到的状态行/字节数，并检查服务端是否出现 `superfluous response.WriteHeader call`。

**游标限制**：只把 `tsdb.FloatArrayCursor` 转成 `prompb.TimeSeries`；`int64`/`uint`/`bool`/`string` 游标只打 `Prometheus can't read cursor` 日志并跳过，响应仍是 200（缺 series，不是错误）。`respond` 内还会累加 `QueryRequestBytesTransmitted` 与 `addQueryBytesForUser`（`handler.go:2093-2094`）。

#### 3.14d 案例：remote read 的 `StartTimestampMs=0`

**请求**：

```http
POST /api/v1/prom/read?db=telegraf&rp=autogen
Content-Type: application/x-protobuf
Content-Encoding: snappy

<snappy(protobuf ReadRequest)>
```

解码后：`Queries` 长度为 1，`Queries[0]={StartTimestampMs:0, EndTimestampMs:0, Matchers:[{Type:EQ, Name:"__name__", Value:"cpu"}]}`。`ReadRequestToInfluxStorageRequest` 得到 `Range{Start:0, End:0}`（`prometheus/converters.go:100-129`）。

- v1.8.10：`validateArgs` 把 `start <= 0` 改成 `models.MinNanoTime`、`end <= 0` 改成 `models.MaxNanoTime`（`v1.8.10:services/storage/store.go:100-105`），扫描全时间范围，返回 `telegraf.autogen.cpu` 的全部 samples。响应 `200`、`Content-Type: application/x-protobuf`、`Content-Encoding: snappy`；解码后 `results[0].timeseries` 非空。
- v1.12.3：`validateArgs` 只归一化 `start <= MinNanoTime` / `end >= MaxNanoTime`（`services/storage/store.go:103-108`），`0` 保持 1970-01-01T00:00:00Z，区间 `[0,0)` 为空。返回 **200**、同样的两个头，body 的 protobuf 为 `ReadResponse{Results:[QueryResult{}]}`（wire 为 `0a 00`，snappy 块为 `02 04 0a 00`）；**`timeseries` 长度为 0**。

注意：`{"results":[{}]}` 只是 protojson 视图，**不是** HTTP body；HTTP body 始终是 snappy 压缩的 protobuf。
#### 3.14e 风险

- **`prom-read-auth-enabled` 默认 false**（`services/httpd/config.go:85`）：即使 `auth-enabled=true`，remote read 也只认证、不做库级 READ 授权；多租户部署必须显式打开（风险 6）。
- **无 body 边界**：`io.ReadAll(r.Body)` 不受 `max-body-size` 限制；`context.Background()` 不随客户端断开取消（`handler.go:2033/2097`），慢客户端/大 body 可长期占用资源（风险 7）。
- **写路径静默丢弃 NaN/±Inf**：`DroppedValuesError` 被容忍，有效点照样写入并返回 204；客户端只从日志/统计才能发现部分样本丢失（`converters.go:60-97`、`handler.go:1990-2000`）。
- **只支持 float**：int/uint/bool/string 游标被跳过，remote read 返回缺 series 但状态码 200；依赖非 float 字段的 Prom 客户端会看到“成功但没数据”。
- **单查询限制**：`len(req.Queries)!=1` 直接 400；Prometheus 某些并行读取会发多 query，需要客户端侧拆分。

<a id="s3-15"></a>

### 3.15 Flux 模块：HTTP 入口与资源治理

> **owner 划分**：本篇只负责 Flux 的 HTTP 入口（`serveFluxQuery`/OPTIONS/关闭分支）、错误码映射与 `[flux-controller]` 配置的资源治理。`query/control` 的状态机、并发/内存配额实现、`storage/flux` 读路径与 window aggregate 下推，owner 是 03 §3.1/§3.7/§3.10-§3.11 与 09；本篇 §3.16-3.18 只保留摘要和链接，不重复分析。

#### 3.15a HTTP 入口

**结构**：

| 组件 | 作用 | 证据 |
|------|------|------|
| 路由 | `POST /api/v2/query`，Flux 关闭时 handler 为 `serveFluxQueryDisabled` | `v1.12.3:services/httpd/handler.go:367-381` |
| OPTIONS | 新增 `OPTIONS /api/v2/query`；handler 为 `serveOptions`，但 `cors` 在 OPTIONS 直接 return，实际隐式 200 | `handler.go:371-374`、`:2966-2968` |
| 请求解码 | `decodeQueryRequest`：`application/vnd.flux` → `io.ReadAll`；JSON → `json.Decoder` | `services/httpd/flux.go:25-53` |
| 认证 | 3 参 handler，`AuthEnabled` 时被 `authenticate` 包装；关闭分支同样被包装 | `handler.go:2175`、`:2263` |
| 执行 | `h.Controller.Query(ctx, pr.Compiler)` | `handler.go:2216` |
| 统计 | `FluxQueryRequests`、`FluxQueryRequestDuration`、`FluxQueryRequestBytesTransmitted`、`userquerybytes` | `handler.go:2176-2181`、`:2258-2259` |

**流程**（成功路径）：

1. `serveFluxQuery` 计数 + 计时；从 `user` 取 `userName`（匿名空串）。
2. `decodeQueryRequest` 解出 `ProxyRequest`；`node_id` 查询参数可注入 `ReadOptions.NodeID`（`handler.go:2192-2197`）。非法 `node_id` 被**静默忽略**：`strconv.ParseUint` 失败不返回 400、不记日志，查询按默认节点执行（同一处 `err == nil` 分支无 else）。
3. `AuthEnabled` 时把 `user` 放进 ctx（`meta.NewContextWithUser`），供控制器/存储授权使用。
4. `h.Controller.Query` 返回 `flux.Query`；失败统一映射为 **500**（`:2216-2220`）。
5. 按 `Accept` 选 dialect（默认 CSV）；`encoder.Encode` 写响应并返回字节数 `n`。
6. `FluxQueryRequestBytesTransmitted += n`；`addQueryBytesForUser(userName, n)`（`:2258-2259`）。

```go
// v1.12.3:services/httpd/handler.go:2216-2220 — 资源/编译/队列错误全部 500
q, err := h.Controller.Query(ctx, pr.Compiler)
if err != nil {
    h.httpError(w, err.Error(), http.StatusInternalServerError)
    return
}
```

#### 3.15b 错误映射与状态码

| 场景 | v1.8.10 | v1.12.3 | 证据 |
|------|---------|---------|------|
| Flux 关闭 + 无凭据 + auth 开启 | 匿名 handler 直接 403，**绕过认证** | `serveFluxQueryDisabled` 走 `authenticate`；已有管理员时缺凭据 **401**，引导模式 403 | `v1.8.10:services/httpd/handler.go:292-295` / `v1.12.3:services/httpd/handler.go:2263-2266` |
| 请求体解码失败 | 400 | 400 | `handler.go:2186-2190` |
| 控制器编译/队列/内存错误 | 不适用（无配额） | **500** + flux 错误文本（如 `queue length exceeded`） | `handler.go:2216-2220`；`query/control/controller.go:386-424` |
| 编码失败且 `n == 0` | 500 | 500（`err != nil && n == 0` → `httpError`） | `handler.go:2252-2257` |
| 编码失败且 `n > 0` | 200 + 已写出的部分 CSV，无 `X-InfluxDB-Error`、无 `{"error":...}` | 同左（`hd.SetHeaders` 只设头不写状态码，状态码由 encoder 第一次 `Write` 隐式提交） | `handler.go:2241-2259` |

**中途编码失败案例**（`encoder.Encode(w, results)`，`handler.go:2241-2259`）：

输入：Flux 查询返回 1 张表、2 行；`Accept: text/csv`（默认分支）；用一个 `io.Writer` 包装真实 `ResponseWriter`，在第 48 字节后返回 `errors.New("write: broken pipe")`。CSV encoder 先写 header + 第一行（`n>0`），再写第二行时失败。

| 阶段 | `n` | `err` | handler 分支 | 客户端可见 |
|------|-----|-------|--------------|------------|
| 首字节前失败（writer 立即报错） | 0 | 非 nil | `err != nil && n == 0` → `httpError(500)` | 500 JSON `{"error":"write: broken pipe"}` + `X-InfluxDB-Error` |
| 已写 header/首行后失败 | >0（例中约 48） | 非 nil | 条件不成立，跳过 `httpError`；状态码已由 encoder 的第一次 `Write` 隐式定为 200 | **200** + 已写出的部分 CSV（截断），无 `X-InfluxDB-Error`、无错误 JSON |

**统计是否累加**：`n > 0` 时执行 `atomic.AddInt64(&h.stats.FluxQueryRequestBytesTransmitted, n)` 与 `addQueryBytesForUser(userName, n)`（`handler.go:2258-2259`），即**只把失败前实际写出的字节计入**；`n == 0` 的 500 分支在累加之前 return，不增加任何字节计数（`FluxQueryRequests` 已在入口 +1，`handler.go:2176`）。因此 `fluxQueryRespBytes` 会小于客户端期望的完整响应大小，可作为“截断”信号之一，但无法区分客户端断开与服务端编码错误。

**关键点**：队列满/内存超限在控制器里是 `codes.ResourceExhausted`，但 `serveFluxQuery` 一律用 `http.StatusInternalServerError`；客户端无法从状态码区分“该扩容”和“服务端 bug”，只能匹配 body 文本（风险 8）。

#### 3.15c 资源治理（摘要；实现 owner 03 §3.1）

| 配置（`[flux-controller]`） | 默认 | 行为 | 证据 |
|-----------------------------|------|------|------|
| `query-concurrency` | 0 | 0=不限制（每查询一个 goroutine）；>0 时启动固定 worker + 有界队列 | `query/control/controller.go:71-110/175-227` |
| `query-queue-size` | 0 | `query-concurrency>0` 时必须 >0；队列满返回 `ResourceExhausted` | `controller.go:386-424` |
| `query-initial-memory-bytes` / `query-max-memory-bytes` | 0 | 单查询初始/硬上限；0 继承/不限 | `controller.go:117-165`、`memory.go:73-116` |
| `total-max-memory-bytes` | 0 | 全局内存池；非 0 时必须 ≥ `query-concurrency × query-initial-memory-bytes` | `controller.go:134-165` |

**HTTP 影响**：配置非 0 后，Flux 查询可能排队或被拒；被拒时 HTTP 仍是 500（3.15b）。不配置时行为等同旧版“来一个跑一个”，但每个查询仍安装内存计账 allocator（03 §3.1）。`Controller.Shutdown` 在 OSS 生产代码无调用方（仅测试调用，`controller.go:516-560`），所以排队/执行中的查询不会因关停而取消；具体取消语义与“排队查询不随客户端断开出队”见 §5.3 与风险 15。

#### 3.15d 案例

见 §6.5：`query-concurrency=2`、`query-queue-size=1` 下并发 4 个 Flux 请求，两个执行、一个排队、一个 500 `queue length exceeded`；随后对比 Flux 关闭分支的 401/403。

<a id="s3-16"></a>

### 3.16 `query/control`：控制器（摘要，owner 03 §3.1）

本节不再重复控制器代码。需要知道的只有与 HTTP 相关的结论：

| 机制 | 对 HTTP 的影响 | 详见 |
|------|----------------|------|
| 查询状态机 `Created→Compiling→Queueing→Executing→Finished/Errored/Canceled` | 无直接状态码；`ResourceExhausted` 仍映射 500 | 03 §3.1 |
| 并发配额 + 有界队列 | 队列满 → `flux.Error{Code: ResourceExhausted, Msg:"queue length exceeded"}` → 500 | 03 §3.1/§7.5 |
| 单查询/全局内存配额 | 分三种条件：①只配 `query-max-memory-bytes>0` 时，单查询超过硬上限返回 `query hit hard limit`；②配了 `query-concurrency>0` 且 `total-max-memory-bytes>0` 时，全局池不足返回 `not enough capacity`；两者都经 `flux.Error` → HTTP 500。③默认全 0：`complete()` 把 `query-max-memory-bytes` 填成 `math.MaxInt64`、`memoryManager.unlimited=true`、`queryQueue=nil`（每查询一个 goroutine），没有全局池也没有并发上限，进程级 OOM 风险与旧版相同 | 03 §3.1/§7.5、`query/control/controller.go:112-165/191-203`、`query/control/memory.go:73-116` |
| 编译/执行 panic recover | 错误经 `handleFluxError` 转成 `flux.Error`，仍由 500 出口 | 03 §3.1/§7.8 |
| Prometheus 指标 `qc_*` | 只在 Flux 开启时注册到默认 gatherer，经 `/metrics` 暴露 | 03 §3.1；`cmd/influxd/run/server.go:346` |

**结论**：06 只负责把控制器错误翻译成 HTTP 状态码；控制器的状态机、配额算法、指标定义不在本篇维护。

<a id="s3-17"></a>

### 3.17 `storage/flux` 读路径（摘要，owner 03 §3.7/§3.10/§3.11、09）

本节不再重复 `ReadWindowAggregate`/`windowAggregateIterator`/`table.gen.go` 的实现。与 HTTP 相关的结论：

- `storage/flux.NewReader` 实现了 `query.Reader`；`ReadFilter`/`ReadGroup`/`ReadWindowAggregate` 的差异属于 03 §3.7。
- window aggregate 下推减少读放大。以 1 个 series、原始精度 1s、范围 1h 为例：3600 个原始点 → 60 个 1m mean 值（每 series；N 个 series 则 ×N）；若原始精度是 10s 或 1m，同样 1h 范围分别是 360 或 60 个原始点，收益比例随之变化。HTTP 响应格式不变（03 §6.3、`storage/flux/reader.go:84-93`）。
- `table.gen.go` 的 25 个生成类型、`flux/array` 适配、`GroupCursorError` 类型冲突属于 03 §3.11。
- 唯一与 HTTP 有关的边界：`decodeQueryRequest` 不做 `MaxBodySize`/`ContentLength` 预检（3.15a），与读路径无关。

<a id="s3-18"></a>

### 3.18 `services/storage`（摘要，owner 03 §3.7、09）

读侧的 `ReadFilterLimit`/`ReadGroupLimit`/`WindowAggregateLimit`、降序游标、`GetReadSource` 的 nil 防护、时间范围 `[0,0]` 变化，owner 是 03 §3.7、09；本节只保留 V2 删除直接依赖的两个方法：

```go
// v1.12.3:services/storage/store.go:500-506
func (s *Store) Delete(database string, sources []influxql.Source, condition influxql.Expr) error {
    return s.TSDBStore.DeleteSeries(database, sources, condition)
}
func (s *Store) DeleteRetentionPolicy(database, name string) error {
    return s.TSDBStore.DeleteRetentionPolicy(database, name)
}
```

`Handler.Store` 接口因此从 1 个方法扩到 3 个（旧 `handler.go:2025-2027` / 新 `:3088-3092`）；`/api/v2/delete` 用 `Delete`，`DELETE /api/v2/buckets/:dbrp` 用 `DeleteRetentionPolicy`（3.19）。时间边界 `StartTimestampMs=0` 的行为见 3.14d。

<a id="s3-19"></a>

### 3.19 V2 删除与 bucket 端点语义

> 本节的授权分支（create/delete/update/retrieve/list）只是 3.3.1 矩阵的展开；认证开关、401/403 的判定以 3.3.1 为准。

`POST /api/v2/delete`（`handler.go:975-1111`）流程：

1. `bucket2dbrp(bucket)`，失败 → **404**（注意不是 400）。
2. 库/RP 不存在 → 404。**RP 只做存在性校验，不参与删除谓词**（`handler.go:986-988`）。
3. `AuthEnabled` 时：`user==nil` → 403；`AuthorizeWrite(user.ID(), db)` 失败 → 403
   （复用写权限，**不是**单独 delete 权限）。正常认证开启且已有管理员时，无凭据请求在
   `authenticate` 就先得到 **401**（`handler.go:2847-2853`）；这里的 `user==nil` 403 只在引导模式
   （`AdminUserExists()==false`，认证放行 nil 用户）可达。
4. `ContentLength > MaxBodySize` → 413；读 body / JSON 失败 → 400。
5. `start`/`stop` 必填且必须是 RFC3339Nano → 否则 400。
6. 拼接 `predicate AND time >= 'start' AND time < 'stop'`，`influxql.ParseExpr`。
7. `PartitionExpr` 抽出 `_measurement = 'x'` 构造 `&influxql.Measurement{Name: ...}`，**不带 `RetentionPolicy`**（`c31e99574f`，`handler.go:1085`），同时**拒绝**
   `!=`、`=~`、`!~`、`OR` → 400。
8. `fixLiterals` 把双引号值转成字符串字面量（由 `2fe52d8edc` #23247 引入；#25407 的 `e3e9232de5` 只增加 measurement 名去引号，`handler.go:1085`），
   `srcs` 初始化为 nil（#24145，避免空 WHERE 破坏企业版远程执行）；**无 `_measurement` 时 `srcs=nil`**。
9. `Store.Delete(db, srcs, remainingExpr)`，失败 → 400。

**删除范围（务必注意）**：`services/storage/store.go:500-502` 直接转 `TSDBStore.DeleteSeries(db, srcs, condition)`；`tsdb/store.go:1697-1714` 的 shard 过滤只固定 `byDatabase(db)`，仅当某个 `Measurement.RetentionPolicy != ""` 才追加 `byRetentionPolicy(rp)`。本端点从不设置该字段，因此：

- `bucket=db/rp` + `_measurement='cpu'` 会删除 **`db` 下所有 RP** 的 `cpu` 数据，RP 只决定 404；
- predicate 里没有 `_measurement` → `srcs=nil`，`tsdb/store.go:1738-1748` 遍历 shard 内**所有 measurement**，删除整个 `db` 在时间区间内的数据。

若要 bucket 级语义，需把 RP 写进 `Measurement.RetentionPolicy`（并评估 `c31e99574f` 的企业版兼容性）。

历史修复都在新版里：`f365bb7e3a`（nil Sources）、`c31e99574f`（不在 V2 delete 里
设置 measurement 的 db/rp）、`e3e9232de5`（剥离 measurement 名双引号）。

bucket 端点族（`handler.go:1159-1624`）语义：

| 操作 | 授权方法（`AuthEnabled` 时） | 行为 | 状态码 |
|------|------|------|--------|
| create | `AuthorizeCreateDatabase`（库不存在）/ `AuthorizeCreateRetentionPolicy`（库存在，`handler.go:1228/1233`） | `bucket2dbrp(name)` 必须含 RP；body RP 冲突 → 400；`meta.ValidName` 校验；库不存在用 `CreateDatabaseWithRetentionPolicy`，存在用 `CreateRetentionPolicy` | 201 |
| delete | `AuthorizeDeleteRetentionPolicy`（`:1290`） | `checkDbRp` 解析失败 → 400；库/RP 不存在 → 404；`Store.DeleteRetentionPolicy` / `MetaClient.DropRetentionPolicy` 失败 → 400 | 200 空体 / 400 / 404 |
| update | `AuthorizeCreateDatabase`（`:1324`，源码有意复用 create 权限） | `UpdateRetentionPolicy` | 200 + JSON |
| retrieve | `AuthorizeDatabase(ReadPrivilege, db)`（`:1402`） | `checkDbRp` 解析失败 → 400；`oneBucket` | 200 / 400 / 404 |
| list（普通遍历） | 逐库 `AuthorizeDatabase(ReadPrivilege, db)`（`:1512`） | 默认 `limit=100`，`limit<=0` 视为 `MaxInt`；`after` 与 `offset` 互斥 | 200 |
| list（`?id=` / `?name=` 单桶） | **仅 `user != nil`**（`:1429-1434`）；`oneBucket` 内无任何授权（`:1439-1452/1529-1543`） | 已认证低权用户可读任意 bucket 元数据 | 200 / 404（已知缺口） |
| labels/members/owners | — | 统一 405，文案分别为 `bucket labels` / `bucket members` / `bucket owners` + `not supported in this version`（`handler.go:1610/1615/1620`） | 405 |

> `POST /api/v2/write?bucket=...` 的 bucket 解析失败（缺 `/`、`bucket2dbrp` 报错）返回 **404**（`handler.go:1642-1645`），不是 400；与 `/api/v2/delete` 的 404 一致。

**注意 create 的 body 读取没有 `truncateReader`**，只有
`ContentLength > MaxBodySize` 预检（`handler.go:1160-1173`）；
chunked 请求（`ContentLength == -1`）可以无限读。`serveDeleteV2` 同理
（`handler.go:1004-1021`）。这是新增端点带来的新边界。

**存储层失败统一 400**：create 的 `CreateDatabaseWithRetentionPolicy` / `CreateRetentionPolicy` 失败（`handler.go:1242/1250`）、delete 的 `Store.DeleteRetentionPolicy` / `MetaClient.DropRetentionPolicy` 失败（`:1303/1306`）、update 的 `UpdateRetentionPolicy` 失败（`:1369`）都返回 **400**，文本前缀分别为 `buckets - cannot create bucket` / `delete bucket` / `update bucket`；`makeBucket` 后的 `json.Marshal` 失败也返回 400（`:1257`、`:1381`）。这些不是 500/503，客户端不应按可重试的服务端错误处理。

**代码节选：create bucket 的 dbrp 解析与授权分支**（`v1.12.3:services/httpd/handler.go:1184-1261`）：

```go
db, rp, err := bucket2dbrp(brd.Name)
if err != nil {
    h.httpError(w, fmt.Sprintf("buckets - illegal bucket name: %s", err.Error()), http.StatusBadRequest)
    return
} else if rp == "" {
    h.httpError(w, fmt.Sprintf("buckets - illegal bucket name: %q", brd.Name), http.StatusBadRequest)
    return
} else if !((brd.Rp == rp) || (brd.Rp == "")) {
    h.httpError(w, fmt.Sprintf("buckets - two retention policies specified: %q differs from %q", rp, brd.Rp), http.StatusBadRequest)
    return
}
...
dbi := h.MetaClient.Database(db)
if h.Config.AuthEnabled {
    if user == nil {
        h.httpError(w, fmt.Sprintf("buckets - user is required to to create bucket %q", brd.Name), http.StatusForbidden)
        return
    }
    if dbi == nil {
        if err := h.QueryAuthorizer.AuthorizeCreateDatabase(user); err != nil { /* 403 */ }
    } else {
        if err := h.QueryAuthorizer.AuthorizeCreateRetentionPolicy(user, db); err != nil { /* 403 */ }
    }
}
var rpi *meta.RetentionPolicyInfo
if dbi == nil {
    dbi, err = h.MetaClient.CreateDatabaseWithRetentionPolicy(db, &spec)
} else {
    rpi, err = h.MetaClient.CreateRetentionPolicy(db, &spec, false)
}
...
w.WriteHeader(http.StatusCreated)
```

`bucket2dbrp` 只在**第一个** `/` 处分割（`handler.go:942-968`）：`telegraf/autogen` → `(telegraf, autogen)`；没有 `/` 时 rp 为空，create 会拒绝；`db/rp` 作为 URL path 参数时必须写成 `db%2Frp`（3.1）。

**案例**（`AuthEnabled=true` 且已有管理员）：

| 请求 | body/凭据 | 处理 | 预期 |
|------|-----------|------|------|
| `POST /api/v2/buckets` | `{"name":"telegraf/autogen","rp":"other"}` + 有 create 权限 | `bucket2dbrp` → `(telegraf,autogen)`；`brd.Rp` 冲突 | **400** `two retention policies specified` |
| 同上 | `{"name":"telegraf/autogen"}`，库不存在 | `AuthorizeCreateDatabase` 通过 → `CreateDatabaseWithRetentionPolicy` | **201** + bucket JSON |
| 同上 | `{"name":"telegraf/autogen"}`，库存在但 RP 已存在 | `CreateRetentionPolicy` 失败 | **400** |
| `GET /api/v2/buckets?id=telegraf/autogen` | 已认证低权用户 | `serveListBucketsV2` 只检查 `user!=nil`，`oneBucket` 不再授权 | **200** 元数据（已知缺口） |
| `POST /api/v2/buckets` | chunked（`ContentLength=-1`） | 只有 `ContentLength>Max` 预检，`buf.ReadFrom` 无截断 | body 可无限大（风险 5） |

**案例：`/api/v2/delete` 的请求、响应与删除范围**（`auth-enabled=false`）：

```http
POST /api/v2/delete?bucket=telegraf/autogen
Content-Type: application/json

{"start":"2026-10-01T00:00:00Z","stop":"2026-10-02T00:00:00Z","predicate":"_measurement=\"cpu\" AND host=\"web01\""}
```

- `bucket2dbrp` → `(telegraf, autogen)`；库/RP 都存在；`srcs=[Measurement{Name:"cpu"}]`（**不带 `RetentionPolicy`**，`handler.go:1085`）；剩余谓词为 `host = 'web01' AND time >= '2026-10-01T00:00:00Z' AND time < '2026-10-02T00:00:00Z'`。
- `Store.Delete` → `DeleteSeries("telegraf", [cpu], cond)`；shard 过滤只有 `byDatabase("telegraf")`（`tsdb/store.go:1697-1714`），因此 **`telegraf` 下所有 RP 的 `cpu` 在该时间段的点都会被删**，`rp=autogen` 只用于 404 校验。
- 成功响应：**200**、`Content-Type: application/json`、**空 body**（handler 成功路径不写 body，也不显式 `WriteHeader`；`Content-Type` 由 `NewResponseWriter` 统一加）。
- 验证：`SELECT * FROM telegraf.autogen.cpu` 与 `SELECT * FROM telegraf.other.cpu` 都查不到 2026-10-01 整天的 `cpu`；`host="web01"` 之外的点保留。

错误分支（同一 URL）：

| 请求/body | 状态 | body |
|-----------|------|------|
| `POST /api/v2/delete`（无 `bucket`）+ 任意 body | 404 | `{"error":"delete - bucket: bucket name \"\" is missing a slash; not in \"database/retention-policy\" format"}` |
| `bucket=telegraf/autogen`，body 缺 `start` | 400 | `{"error":"delete - start field in RFC3339Nano format required"}` |
| `bucket=telegraf/autogen`，body `start="2026-10-01"` | 400 | `{"error":"delete - invalid format for start field \"2026-10-01\", please use RFC3339Nano: ..."}` |
| `predicate` 含 `!=`、`=~`、`!~`、`OR` | 400 | `{"error":"delete - predicate only supports equality operators and conjunctions. database: \"telegraf\", retention policy: \"autogen\", start: \"...\", stop: \"...\", predicate: \"_measurement=\\\"cpu\\\" AND host != \\\"web01\\\"\""}` |
| `ContentLength > max-body-size` | 413 | `{"error":"Request Entity Too Large"}` |

<a id="s3-20"></a>

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

commit `6795ec6c01` 先移除 context-value 反模式，改为显式 tracker，但仍在 httpd
累加 `ValuesWrittenOK`；随后 `117341fb0f` 把 values 统计下移到 tsdb store，
`Statistics()` 才不再输出该项（旧版在 `v1.8.10:services/httpd/handler.go:394`，新版
`v1.12.3:services/httpd/handler.go:455-479` 没有该项）。常量
`statValuesWrittenOK` 仍留在 `service.go:34`，但已无引用。

同一区间还有两处 httpd 统计变化（§8.4 已列 commit）：① 新增 `fluxQueryRespBytes`（`v1.12.3:services/httpd/service.go:50`、`handler.go:447/479`，#25669）；② `promReadReq` 旧版只读不自增（`v1.8.10:services/httpd/handler.go:375/407`、`servePromRead:1248-1252`），新版在入口 +1 并 `requestTracker.Add`（`v1.12.3:services/httpd/handler.go:2031-2032`，#18429）。此外 Flux 开启时 `Server.Statistics` 会并入 Prometheus Gather 的 `qc_*` 指标（`cmd/influxd/run/server.go:270-273`），`SHOW STATS` 条目会增多。

**(b) `serveWriteV1` / `serveWriteV2` 精度错误补 `return`**：

V1 与 V2 的 `default` 分支在旧版都缺 `return`（V1 `v1.8.10:services/httpd/handler.go:874-877`、V2 `:855-858`），新版两处都补上（V1 `:1656-1660`、V2 `:1636-1640`），同一 commit `997698f891` (#21746)。

```go
// v1.8.10:services/httpd/handler.go:855-858（V2；V1 同形但文案为 `use n, u, ms, s, m or h`）
default:
    err := fmt.Sprintf("invalid precision %q (use ns, us, ms or s)", precision)
    h.httpError(w, err, http.StatusBadRequest)
    // ← 旧版没有 return，继续执行 bucket2dbrp + serveWrite

// v1.12.3:services/httpd/handler.go:1636-1640（V2；V1 同形，文案为 use n, u, ms, s, m or h）
default:
    err := fmt.Sprintf("invalid precision %q (use ns, us, ms or s)", precision)
    h.httpError(w, err, http.StatusBadRequest)
    return
}
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
| 证书 loader 关闭 | 无（一次性 LoadX509KeyPair） | `tlsManager.Close()` 停巡检 goroutine（`:277-282`）；但 `Shutdown`/listener `Close` 出错会提前 return 跳过此步，`OnceValue` 不会重试 |
| `Open` 失败清理 | 不清 access log，无巡检 goroutine | `Handler.Open` 已开 access log（`:159`），`NewTLSConfigManager` 已启动证书巡检 goroutine（`pkg/tlsconfig/certconfig.go:247-274`）；随后 `Listen`/unix socket 设置失败直接 return，未 Close `tlsManager`/listener/access log（`:149-236`），需重启进程 |

> `err` channel 的并发后果：旧版 `serve()` 在 `http.Server.Serve` 返回非 "closed" 错误时向**无缓冲** `s.err` 发送（`v1.8.10:services/httpd/service.go:261-266`），但 OSS 生产代码没有消费者（`HttpdService.Err()` 未被调用，`Server.Err()` 的 `s.err` 也没有写入者，`cmd/influxd/run/server.go:431`、`command.go:210-219`），该 serve goroutine 会永久阻塞。新版缓冲 2 对应 TCP + unix 两个 serve 调用（`services/httpd/service.go:127`），goroutine 可以退出。

**生命周期案例**（同一 `Service` 实例）：

| 调用序列 | v1.8.10 | v1.12.3 |
|----------|---------|---------|
| `Open()` | nil | nil |
| `Close()`（第一次） | nil | nil |
| `Close()`（第二次） | `close tcp <addr>: use of closed network connection`（`s.ln.Close()` 再次关闭） | nil（`sync.OnceValue` 返回第一次的结果，`service.go:137`、`:239-241`） |
| `Close()` 后 `Open()` | 重新 `net.Listen` 成功并返回 nil，但同一 `Service` 出现第二套 listener/goroutine，`Handler` 与已 `Shutdown` 的 `httpServer` 状态不一致 | 返回 `attempt to Open http service after Close`（`service.go:155-157`），不再监听 |

`BoundHTTPAddr`：旧版直接 `s.ln.Addr()`（`Open` 在 Listen 前失败、`s.ln==nil` 时 panic）；新版 `RLock` + nil 判断，返回 `N/A`（`service.go:345-353`）。

**关闭顺序风险（两版同形）**：`doClose()` 在 `httpServer.Shutdown` **之前**调用 `Handler.Close()`（`services/httpd/service.go:246-260`）；后者关闭 accessLog 并把 `h.accessLogFilters = nil`（`handler.go:409-419`）。在飞请求的 `logging` 中间件无锁读 `h.accessLogFilters` 并向仍指向已关闭文件的 `h.CLFLogger` 写日志（`handler.go:3023-3031`），构成 slice 头读写数据竞争 + 写已关闭文件（错误被忽略）。建议先 `Shutdown` 等请求结束再关 access log，或对 filters/logger 加同步。

**(e) CSV 输出不转义**（#24311）：CSV formatter 生成 tag 列时改用
`HashKey(false)`（`v1.12.3:services/httpd/response_writer.go:228`，
旧 `v1.8.10:...:233` 是 `HashKey()`）。CSV 中的 tag 值不再被 line-protocol
规则二次转义。

**(f) `WriteError` 辅助函数删除**（`v1.8.10:services/httpd/response_writer.go:75-78`），
包内无引用。

---


---

## 6. 具体案例（补充）

> 主文档完整保留 §6.1、§6.3、§6.6；§6.2、§6.4、§6.5 因包含完整请求/响应、JSON 输出与 Flux 排队推演，全文移至本附录，编号不变。

<a id="s6-2"></a>

### 6.2 `/write` 请求的新旧对照

**请求 A（正常 V1 写入）**：

```
POST /write?db=telegraf&rp=autogen&precision=s
Authorization: Token admin:secret
Content-Encoding: gzip
Content-Length: 57

<gzip bytes of "cpu,host=web01 usage=42.1 1704067200\n">
```

这里的 `Content-Length: 57` 是 gzip **压缩后**的长度（未压缩行协议 body 为 37 字节）。构造请求 A 的可运行命令（`Content-Length` 由 `--data-binary` 自动按压缩后大小写出）：

```bash
printf 'cpu,host=web01 usage=42.1 1704067200\n' | gzip -c > /tmp/body.gz
wc -c < /tmp/body.gz   # 57（本机 gzip 实测；换压缩级别/实现会略有差异）
curl -s -o /dev/null -w '%{http_code}\n' \
  -H 'Authorization: Token admin:secret' -H 'Content-Encoding: gzip' \
  --data-binary @/tmp/body.gz \
  'http://localhost:8086/write?db=telegraf&rp=autogen&precision=s'
# 预期 204
```

两版共同路径：

1. `serveWriteV1` 校验 precision（`v1.12.3:services/httpd/handler.go:1651-1666`）。
2. `serveWrite` 校验 `database != ""`、库存在、`AuthorizeWrite`。
3. `body := r.Body`；仅当 `h.Config.MaxBodySize > 0` 时 `body = truncateReader(body, int64(h.Config.MaxBodySize))`（`:1701-1704`）。
4. **gzip 分支覆盖 `body`**（`:1707-1715`）：截断器被丢弃。
   由于压缩后的 `ContentLength`（本例 57）小于 25MB，预检通过；解压后如果膨胀到 1GB，
   两版都会照读不误。
5. `ParsePointsWithPrecision`，`PointsWriter.WritePoints`，成功 204。

差异只在统计：旧版会额外走 `WritePointsWithContext` 并累加
`ValuesWrittenOK`（`v1.8.10:services/httpd/handler.go:989-1010`），新版没有这一步
（`v1.12.3:services/httpd/handler.go:1772`），因此 `SHOW STATS` 的 httpd 节点少一个字段。

**请求 B（V2 写入 + 非法 precision，展示 bug 修复）**：

```
POST /api/v2/write?bucket=telegraf/autogen&precision=xyz
Content-Type: text/plain

cpu,host=web01 usage=42.1 1704067200000000000
```

必须给 body：空 body 会走 `parseError == "EOF"` 分支（`v1.12.3:services/httpd/handler.go:1750-1755`），只再 `writeHeader(200)` 后返回、不写数据（客户端仍看到先写出的 400）；有上面的 body 才会在 400 已写出后继续解析并写入。旧版把 `precision=xyz` 原样传给 `serveWrite`，`GetPrecisionMultiplier("xyz")` 退化为纳秒（`v1.12.3:models/points.go:471-487`），点按 ns 写入。

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
db, rp, err := bucket2dbrp(r.URL.Query().Get("bucket"))
h.serveWrite(db, rp, precision, w, r, user)
```


<a id="s6-4"></a>

### 6.4 `/debug/vars` 输出对照

旧版（v1.8.10）顶层键：`system`、`cmdline`、`memstats`、以及
`httpd`/`runtime`/`network`… 等统计对象。旧版没有 `build`/`config`/`stats`/`crypto`，且 `cq` 与其他服务统计一样带 `{name,tags,values}` 包装：

```json
{
"system":  {"currentTime": "...", "started": "...", "uptime": 700},
"cmdline": ["/usr/bin/influxd", "-config", "/etc/influxdb/influxdb.conf"],
"memstats": {"Alloc": 12345678, "...": "..."},
"cq":      {"name": "cq", "tags": null, "values": {"queryFail": 0, "queryOk": 12}},
"httpd":   {"name": "httpd", "tags": {"bind": ":8086"}, "values": {"req": 100, "...": "..."}},
"runtime": {"name": "runtime", "tags": {}, "values": {"...": "..."}},
"...": "..."
}
```

`cq` 的 `{name,tags,values}` 形状来自旧版 `serveExpvar` 的通用分支：它直接 `json.Marshal(models.Statistic)`，key 只用 `s.Name` 与 `path`/`bind`/`database` 标签拼装（`v1.8.10:services/httpd/handler.go:1532-1587`）。注意：`serveExpvar` 调 `Monitor.Statistics(nil)`（新版 `handler.go:2292`），CQ 的 `Statistics` 把传入 tags 原样放入，所以旧版 `cq.tags` 是 `null` 而不是 `{}`；`httpd` 则在 `Service.Statistics` 内合并 `bind`。`Values` 是 map，`json.Marshal` 按 key 升序，故 `queryFail` 在 `queryOk` 前。新版把 `Name=="cq"` 特判为只写 `Values`（`handler.go:2452-2478`、`:2728-2734`），所以同一个 `cq` 从对象变成 `{"queryFail":0,"queryOk":12}`（map key 升序）。

**请求/响应头逐项**：

| 项 | v1.8.10 | v1.12.3 |
|----|---------|---------|
| 请求 | `GET /debug/vars HTTP/1.1` + `Host: <addr>`；`pprof-auth-enabled=false` 时不带 `Authorization` | 同左 |
| 状态行 | `HTTP/1.1 200 OK` | `HTTP/1.1 200 OK` |
| `Content-Type` | `application/json; charset=utf-8`（`v1.8.10:services/httpd/handler.go:1494`） | `application/json; charset=utf-8`（`handler.go:2306`） |
| `X-Influxdb-Version` / `X-Influxdb-Build` | 有（`ServeHTTP` 对所有请求添加，`v1.8.10:services/httpd/handler.go:462-464`） | 有（`handler.go:561-563`） |
| `Content-Length` | 无显式设置；`serveExpvar` 流式写出，超过 4KB 时 chunked | 同左 |
| 认证开关 | `pprof-auth-enabled=false`（默认）时匿名 200；三开关全开且无凭据 → 401 + `WWW-Authenticate: Basic realm="InfluxDB"`、body `{"error":"unable to parse authentication credentials"}` | 同左 |

新版（v1.12.3）额外：

```json
{
  "system":  {"currentTime": "...", "started": "...", "uptime": 123},
  "build":   {"Version": "...", "Commit": "...", "Branch": "...", "Build Time": "..."},
  "cmdline": ["/usr/bin/influxd", "-config", "/etc/influxdb/influxdb.conf"],
  "config":  {"aggressive-points-per-block": 10000,
              "compact-throughput": 50331648,
              "compact-throughput-burst": 50331648,
              "max-concurrent-compactions": 0},
  "stats":   {"compact-throughput-usage-percentage": 37.5},
  "crypto":  {"ensureFIPS": false, "FIPS": false, "implementation": "Go", "passwordHash": "bcrypt"},
  "memstats": {},
  "cq":      {"queryFail": 0, "queryOk": 12},
  "httpd":   {"name": "httpd", "tags": {"bind": ":8086"}, "values": {"req": 100, "...": "..."}}
}
```

注意：

- `config` 实际输出 `tsdb.Config.Diagnostics()` 的全部字段，示例只列 4 个，是节选。
- `httpd`/`userquerybytes` 的 tags 至少含 `"bind":":8086"`（默认 `bind-address`）；示例省略会导致按 key 解析失败（见 §3.5）。
- 除 `cq` 外，服务统计仍走 `json.Marshal(models.Statistic)`，所以带 `{"name","tags","values"}` 包装。
- `build` 的时间键是 `Build Time`（`monitor/build_info.go:14-19`），不是 `Time`。
- `config` 里的 `compact-throughput` 是 `toml.Size`，被
  `parseConfigDiagnostics` 转成 `uint64`；`toml.Duration` 转成字符串
  （`handler.go:2715-2722`）。
- `stats` 只在压缩限流器存在时出现（`monitor/service.go:156`）。
- `crypto` 在 OSS 且无 `crypto` 诊断时使用默认值（`handler.go:2424-2426`）。
- 这些输出**没有认证保护**（当 `pprof-auth-enabled=false` 时），
  等于把 TSDB 调优参数暴露给任何能访问 8086 的人。

**`/metrics` 与 `/debug/vars` 对照**（默认配置：`auth-enabled=false`、`ping-auth-enabled=false`、`pprof-auth-enabled=false`）：

| 请求 | 认证 | 响应 | 关键内容 |
|------|------|------|----------|
| `GET /metrics` | 无（`PingAuthEnabled=false`） | 200 `text/plain; version=0.0.4` | 默认 gatherer 的 Go/进程指标；Flux 开启时 `qc_*`；**没有** `httpd` 请求计数 |
| `GET /metrics`（`auth-enabled=true && ping-auth-enabled=true`，无凭据） | 401 | `authenticate` 拒绝 | — |
| `GET /debug/vars` | 无（`PprofAuthEnabled=false`） | 200 JSON | `cq` 是扁平 `{"queryFail":0,"queryOk":12}`（map key 升序）；含 `config`/`stats`/`crypto` |
| `GET /debug/vars`（三开关全开，无凭据） | 401；有凭据非管理员 403 | — | — |
| `SHOW STATS` | SQL 授权 | 行 | httpd 行不含 `valuesWrittenOK`；含 `fluxQueryRespBytes`；`promReadReq` 为真实计数 |
| `SHOW DIAGNOSTICS`（httpd） | SQL 授权 | 行 | 新增 `flux-enabled`、`https-insecure-certificate` |

> `/metrics` 不是 `/debug/vars` 的替代品：前者是 Prometheus 指标面，后者是 JSON 诊断面。排查 HTTP 请求/错误/用户字节应看 `/debug/vars` 或 `SHOW STATS`；排查 Flux 控制器队列/内存应看 `/metrics` 的 `qc_*`。


<a id="s6-5"></a>

### 6.5 Flux：并发 4 个查询时的排队与 500

**配置**：

```toml
[http]
  flux-enabled = true
  auth-enabled = false

[flux-controller]
  query-concurrency = 2
  query-queue-size = 1
  query-initial-memory-bytes = 104857600
  query-max-memory-bytes = 104857600
  total-max-memory-bytes = 209715200
```

**请求**：同时发出 4 个 `POST /api/v2/query`，`Content-Type: application/vnd.flux`，body 相同：

```flux
from(bucket: "telegraf/autogen")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "cpu")
  |> count()
```

**预期时序**：

| 请求 | 控制器状态 | HTTP 结果 |
|------|------------|-----------|
| 1、2 | `Executing`（2 个 worker） | 200 + CSV 结果 |
| 3 | `Queueing`（队列容量 1） | 挂起，等待 worker 空闲 |
| 4 | `enqueueQuery` 的 `select default` 命中 | **500**；`Content-Type: application/json`、`X-InfluxDB-Error: queue length exceeded`；body `{"error":"queue length exceeded"}` |
| 1 或 2 完成后 | worker 取 3 | 3 返回 200 |

**观测**：请求 4 的完整响应为：

```http
HTTP/1.1 500 Internal Server Error
Content-Type: application/json
X-Influxdb-Version: 1.12.3
X-Influxdb-Build: OSS
X-InfluxDB-Error: queue length exceeded

{"error":"queue length exceeded"}
```

错误来源是 `enqueueQuery` 的 `select default` 返回 `*flux.Error{Code: ResourceExhausted, Msg: "queue length exceeded"}`（`query/control/controller.go:414-419`）；`Query.Err()` 经 `handleFluxError` 映射为 `errors2.Error{Msg: "queue length exceeded"}`（`:1088-1108`），`serveFluxQuery` 用 `h.httpError(..., 500)` 输出（`handler.go:2216-2220`）。`/metrics` 的计数按 `result` 标签区分：请求 1/2 执行成功 → `qc_requests_total{org="",result="success"}` 各 +1；请求 4 队列满 → `qc_requests_total{org="",result="queue_error"}` +1（`labelQueueError`，`controller.go:80-85/261`）；请求 3 出队执行完成、handler 调用 `Done()` 后才计入 `result="success"`（`:716-718`）。`qc_queueing_active` 在 3 挂起期间为 1，`qc_executing_active` 最多 2，`qc_all_active` 包含 Queueing+Executing 等全部状态；请求 3 在 worker 释放后以 200 + CSV 完成。队列满的请求只计 `queue_error`，不会同时计 `success`。若把 `query-concurrency` 设为 0（默认），4 个请求都会立即执行，不会出现 500；若把 `query-queue-size` 设为 0 而 concurrency 仍 >0，`Config.validate` 会让进程启动失败（03 §3.1/§5.1）。

**成功路径的字节统计**：请求 1 的 `encoder.Encode` 返回 `n`（例如 137 字节），则 `FluxQueryRequestBytesTransmitted += 137`，且 `userquerybytes{user="alice"}` 加 137（`handler.go:2258-2259`）。`n` 是编码器实际写出的字节数，不是查询结果行数。

**关闭分支对照**：请求统一为 `POST /api/v2/query`、`Content-Type: application/vnd.flux`、body 为任意 Flux 文本（关闭分支不解析 body）。

| 配置/凭据 | v1.8.10 | v1.12.3 |
|-----------|---------|---------|
| `flux-enabled=false`，`auth-enabled=false` | 403 `text/plain; charset=utf-8`、`X-Content-Type-Options: nosniff`，body `Flux query service disabled. Verify flux-enabled=true in the [http] section of the InfluxDB config.\n`（匿名 `http.Error`，`v1.8.10:services/httpd/handler.go:292-295`） | 403 JSON，body `{"error":"Flux query service disabled. Verify flux-enabled=true in the [http] section of the InfluxDB config."}` + 同名 `X-InfluxDB-Error`（`handler.go:2263-2266`） |
| `auth-enabled=true`，已有管理员，无 `Authorization` | 同左（旧版不认证） | **401** JSON，body `{"error":"unable to parse authentication credentials"}` + `WWW-Authenticate: Basic realm="InfluxDB"`；**无** `X-InfluxDB-Error`（`handler.go:2847-2853/2739-2741`） |
| `auth-enabled=true`，已有管理员，凭据有效 | 403 text/plain | 403 JSON（`serveFluxQueryDisabled` 被 `authenticate` 包装后执行） |
| `auth-enabled=true`，引导模式（无管理员） | 403 text/plain | 403 JSON（`authenticate` 放行 `user=nil`，handler 不检查用户） |
| `auth-enabled=true`，凭据错误/用户不存在 | 403 text/plain | 401 JSON `{"error":"authorization failed"}` + `WWW-Authenticate` |
| `OPTIONS /api/v2/query` | 200 空 body（CORS） | 200 空 body（CORS，`handler.go:2964-2968`） |

新旧差异的核心是：旧版关闭分支是**2 参匿名 handler**，任何请求都直接 403 文本；新版是**3 参 handler**，先过 `authenticate`，所以“关闭”之前可能先得到 401，且错误体统一为 JSON。依赖“Flux 关闭时返回 403”的客户端需要适配（§5.2 第 1 行）。

