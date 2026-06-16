# Module 10: HTTP 服务层 (路由 + 中间件 + 认证 + 限流) - 深度审计报告

> **小白导读**: HTTP 服务层是 InfluxDB 的"前台接待大厅"。
>
> 所有外部请求（写入、查询、健康检查）都必须经过这个大厅。
> 就像一家银行：
> - **路由 (Route)** = 窗口分类（存钱窗口、取钱窗口、咨询窗口）
> - **中间件 (Middleware)** = 安检流程（身份验证、金属探测、登记访客ID）
> - **认证 (Authentication)** = 身份证核查（密码、JWT令牌、Basic Auth）
> - **限流 (Throttler)** = 排队叫号系统（超过容量就等候，等候超时就请回）
> - **Handler** = 柜员（处理具体业务：写入数据、执行查询）
>
> 请求的生命周期：**进入大厅 → 安检(中间件) → 取号(限流) → 验身份证(认证) → 到窗口办理(Handler) → 拿回执(响应)**

## 1. 核心结构体

### 1.1 Handler — HTTP 请求处理中枢

```go
// services/httpd/handler.go:104 — Handler
type Handler struct {
    mux       *pat.PatternServeMux   // 路由分发器 (bmizerany/pat 库)
    Version   string                 // InfluxDB 版本号
    BuildType string                 // 构建类型 (OSS/Enterprise)

    MetaClient interface {           // 元数据客户端 (来自 Module 07)
        Database(name string) *meta.DatabaseInfo
        Databases() []meta.DatabaseInfo
        Authenticate(username, password string) (ui meta.User, err error)
        User(username string) (meta.User, error)
        AdminUserExists() bool
        CreateDatabaseWithRetentionPolicy(name string, spec *meta.RetentionPolicySpec) (*meta.DatabaseInfo, error)
        DropRetentionPolicy(database, name string) error
        CreateRetentionPolicy(database string, spec *meta.RetentionPolicySpec, makeDefault bool) (*meta.RetentionPolicyInfo, error)
        UpdateRetentionPolicy(database, name string, rpu *meta.RetentionPolicyUpdate, makeDefault bool) error
    }

    QueryAuthorizer  QueryAuthorizer    // 查询权限检查
    WriteAuthorizer  interface {        // 写入权限检查
        AuthorizeWrite(username, database string) error
    }

    QueryExecutor  *query.Executor      // InfluxQL 查询执行器 (Module 03)
    Monitor        interface {          // 监控统计
        Statistics(tags map[string]string) ([]*monitor.Statistic, error)
        Diagnostics() (map[string]*diagnostics.Diagnostics, error)
    }
    PointsWriter   interface {          // 数据写入器 (Module 01)
        WritePoints(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, user meta.User, points []models.Point) error
    }
    Store          Store                // 存储层接口 (Module 09)

    // Flux 查询相关
    Controller       Controller         // Flux 控制器
    CompilerMappings flux.CompilerMappings

    Config           *Config            // HTTP 配置
    Logger           *zap.Logger        // 结构化日志
    CLFLogger        *log.Logger        // CLF 格式访问日志
    accessLog        *os.File           // 访问日志文件句柄
    accessLogFilters StatusFilters      // 访问日志状态码过滤器
    stats            *Statistics        // 统计计数器 (原子操作)
    queryBytesPerUser gensyncmap.Map[string, *atomic.Int64] // 用户维度查询字节数

    requestTracker *RequestTracker      // 请求追踪器 (debug 用)
    writeThrottler *Throttler           // 写入限流器
}
```

> **小白解释**: Handler 是整个 HTTP 层的"大脑"，它持有了所有外部依赖的接口引用。
> 注意这些依赖全部是**接口类型**而非具体结构体——这是 Go 的依赖注入模式，
> 方便测试时替换为 mock 实现。

### 1.2 Service — HTTP 服务生命周期管理

```go
// services/httpd/service.go:60 — Service
type Service struct {
    ln        net.Listener     // TCP 监听器 (受 mu 保护)
    addr      string           // 绑定地址 (如 ":8086")
    https     bool             // 是否启用 HTTPS
    cert      string           // TLS 证书路径 (初始证书，reload 时可能换)
    key       string           // TLS 私钥路径
    insecureCert bool          // 是否忽略证书文件权限检查 (service.go:81)
    limit     int              // 最大连接数限制
    tlsConfig *tls.Config      // TLS 配置
    err       chan error        // 致命错误通道
    closeFunc func() error     // 幂等的 Close 实现 (sync.OnceValue 包装, service.go:87)

    httpServer http.Server     // 标准库 HTTP 服务器

    unixSocket         bool    // 是否启用 Unix Socket
    unixSocketPerm     uint32  // Socket 文件权限
    unixSocketGroup    int     // Socket 文件所属组
    bindSocket         string  // Socket 文件路径
    unixSocketListener net.Listener

    Handler *Handler           // 请求处理器
    Logger  *zap.Logger
}
```

> **审计校准** (service.go:60-114):
> - 新增 `insecureCert bool` (line 81) 和 `closeFunc func() error` (line 87)。
>   `closeFunc` 在 `NewService` 中由 `sync.OnceValue(s.doClose)` 初始化，保证 Close 幂等。
> - 结构体引用从 51 → **60**。
> - 真实结构体还包含 `mu sync.RWMutex`、`tlsManager`、`tcpServerStarted`、
>   `unixServerStarted`、`closed` 等并发控制字段 (这里省略以保持文档简洁)。

**生命周期**:

```mermaid
sequenceDiagram
    participant Main as influxd 主进程
    participant Svc as Service
    participant H as Handler
    participant TCP as TCP Listener
    participant UX as Unix Socket

    Main->>Svc: NewService(config)
    Svc->>H: NewHandler(config)
    H->>H: 注册路由 (AddRoutes)
    H->>H: 创建 Throttler

    Main->>Svc: Open()
    Svc->>H: Handler.Open()
    H->>H: 打开访问日志文件
    H->>H: 检查 Auth + SharedSecret
    H->>H: 注册 Prometheus Collector (Flux)

    Svc->>TCP: tls.Listen("tcp", ":8086") 或 net.Listen
    alt Unix Socket 启用
        Svc->>UX: net.Listen("unix", "/var/run/influxdb.sock")
        Svc->>UX: go serveUnixSocket()
    end
    Svc->>TCP: go serveTCP()

    Note over Svc: 服务运行中...<br>接收 HTTP 请求

    Main->>Svc: Close()
    Svc->>H: Handler.Close() (关闭访问日志, 取消注册)
    Svc->>Svc: httpServer.Shutdown(10s 超时)
    alt TCP serve 未启动
        Svc->>TCP: ln.Close()
    else TCP serve 已启动
        Note over Svc,TCP: httpServer 已接管 listener，Shutdown 负责关闭
    end
    alt Unix serve 未启动且 listener 存在
        Svc->>UX: unixSocketListener.Close()
    end
```

### 1.3 Config — HTTP 配置

```go
// services/httpd/config.go:34 — Config
type Config struct {
    Enabled                 bool              // 是否启用 HTTP 服务
    BindAddress             string            // 绑定地址 (默认 ":8086")
    AuthEnabled             bool              // 是否启用认证
    LogEnabled              bool              // 是否启用访问日志
    SuppressWriteLog        bool              // 是否抑制写入日志
    WriteTracing            bool              // 写入追踪 (debug)
    FluxEnabled             bool              // 是否启用 Flux 查询
    FluxLogEnabled          bool              // Flux 查询日志
    FluxTesting             bool              // Flux 测试开关
    PprofEnabled            bool              // 是否启用 pprof
    PprofAuthEnabled        bool              // pprof 是否需要认证
    DebugPprofEnabled       bool              // debug pprof
    PingAuthEnabled         bool              // /ping 是否需要认证
    PromReadAuthEnabled     bool              // Prometheus 读取是否需要认证
    HTTPHeaders             map[string]string // 用户自定义 HTTP 响应头
    HTTPSEnabled            bool              // 是否启用 HTTPS
    HTTPSCertificate        string            // TLS 证书路径
    HTTPSPrivateKey         string            // TLS 私钥路径
    HTTPSInsecureCertificate bool             // 是否允许不安全/自签名证书
    MaxRowLimit             int               // 非分块查询最大行数 (0=不限)
    MaxConnectionLimit      int               // 最大连接数
    SharedSecret            string            // JWT 共享密钥
    Realm                   string            // Basic Auth 域名 (默认 "InfluxDB")
    UnixSocketEnabled       bool              // Unix Socket 启用
    UnixSocketGroup         *toml.Group       // Socket 文件所属组
    UnixSocketPermissions   toml.FileMode     // Socket 文件权限 (默认 0777)
    BindSocket              string            // Socket 路径 (默认 "/var/run/influxdb.sock")
    MaxBodySize             int               // 请求体最大大小 (默认 25MB)
    AccessLogPath           string            // 访问日志文件路径
    AccessLogStatusFilters  []StatusFilter    // 访问日志状态码过滤器
    MaxConcurrentWriteLimit int               // 最大并发写入数
    MaxEnqueuedWriteLimit   int               // 最大排队写入数
    EnqueuedWriteTimeout    time.Duration     // 排队超时 (默认 30s)
    UserQueryBytesEnabled   bool              // 是否按用户统计查询响应字节数
    TLS                     *tls.Config       // TLS 配置 (运行时)
}
```

**默认值**:

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `BindAddress` | `:8086` | 监听所有网卡的 8086 端口 |
| `MaxBodySize` | `25MB` (25e6) | 防止超大请求体导致 OOM |
| `EnqueuedWriteTimeout` | `30s` | 写入请求最大排队时间 |
| `Realm` | `InfluxDB` | Basic Auth 认证域 |
| `BindSocket` | `/var/run/influxdb.sock` | Unix Socket 默认路径 |
| `UnixSocketPermissions` | `0777` | Socket 文件权限 |

## 2. 完整路由表

### 2.1 路由注册

路由在 `NewHandler()` 中通过 `h.AddRoutes()` 批量注册：

```go
// services/httpd/handler.go:186 — NewHandler 中的路由注册
h.AddRoutes([]Route{
    // 查询相关
    Route{"query-options", "OPTIONS", "/query",        false, true, h.serveOptions},           // CORS 预检
    Route{"query",         "GET",     "/query",        true,  true, h.serveQuery},             // InfluxQL 查询 (GET)
    Route{"query",         "POST",    "/query",        true,  true, h.serveQuery},             // InfluxQL 查询 (POST)

    // 写入相关
    Route{"write-options", "OPTIONS", "/write",        false, true, h.serveOptions},           // CORS 预检
    Route{"write",         "POST",    "/write",        true,  writeLogEnabled, h.serveWriteV1},// V1 写入
    Route{"write",         "POST",    "/api/v2/write", true,  writeLogEnabled, h.serveWriteV2},// V2 写入 (bucket→db/rp)

    // Prometheus 兼容
    Route{"prometheus-write", "POST", "/api/v1/prom/write", false, true, h.servePromWrite},   // Prometheus Remote Write
    Route{"prometheus-read",  "POST", "/api/v1/prom/read",  true,  true, h.servePromRead},    // Prometheus Remote Read

    // 健康检查
    Route{"ping",       "GET",  "/ping",    false, true, authWrapper(h.servePing)},    // 健康探针
    Route{"ping-head",  "HEAD", "/ping",    false, true, authWrapper(h.servePing)},    // 健康探针 (HEAD)
    Route{"status",     "GET",  "/status",  false, true, authWrapper(h.serveStatus)},  // 已废弃
    Route{"status-head","HEAD", "/status",  false, true, authWrapper(h.serveStatus)},  // 已废弃
    Route{"ping",       "GET",  "/health",  false, true, authWrapper(h.serveHealth)},  // V2 健康端点

    // Prometheus 指标
    Route{"prometheus-metrics", "GET", "/metrics", false, true, authWrapper(promhttp.Handler().ServeHTTP)},

    // Flux 查询 (条件注册)
    Route{"flux-read", "POST", "/api/v2/query", true, true, fluxHandler},

    // V2 兼容删除
    Route{"delete", "POST", "/api/v2/delete", false, true, h.serveDeleteV2},
}...)
```

### 2.2 路由总览表

| 方法 | 路径 | Handler | Route.Gzipped | Route.LoggingEnabled | 认证来源/条件 |
|------|------|---------|---------------|----------------------|----------------|
| `OPTIONS` | `/query` | `serveOptions` | 否 | 是 | CORS 预检 |
| `GET` | `/query` | `serveQuery` | 是 | 是 | handler/auth 配置 |
| `POST` | `/query` | `serveQuery` | 是 | 是 | handler/auth 配置 |
| `OPTIONS` | `/write` | `serveOptions` | 否 | 是 | CORS 预检 |
| `POST` | `/write` | `serveWriteV1` | 是 | 可配置 | handler/auth 配置 + writeThrottler |
| `OPTIONS` | `/api/v2/write` | `serveOptions` | 否 | 是 | CORS 预检 |
| `POST` | `/api/v2/write` | `serveWriteV2` | 是 | 可配置 | handler/auth 配置，不经过 writeThrottler |
| `POST` | `/api/v1/prom/write` | `servePromWrite` | 否 | 是 | Prom write auth 配置 + writeThrottler |
| `POST` | `/api/v1/prom/read` | `servePromRead` | 是 | 是 | `PromReadAuthEnabled` |
| `GET` | `/ping` | `servePing` | 否 | 是 | `PingAuthEnabled` |
| `HEAD` | `/ping` | `servePing` | 否 | 是 | `PingAuthEnabled` |
| `GET` | `/status` | `serveStatus` | 否 | 是 | authWrapper 条件 |
| `HEAD` | `/status` | `serveStatus` | 否 | 是 | authWrapper 条件 |
| `GET` | `/health` | `serveHealth` | 否 | 是 | authWrapper 条件 |
| `GET` | `/metrics` | `promhttp.Handler` | 否 | 是 | authWrapper 条件 |
| `POST` | `/api/v2/query` | `serveFluxQuery` | 是 | 是 | handler/auth 配置 |
| `OPTIONS` | `/api/v2/query` | `serveOptions` | 否 | 是 | CORS 预检 |
| `POST` | `/api/v2/delete` | `serveDeleteV2` | 否 | 是 | handler/auth 配置 |
| 多方法 | `/api/v2/buckets*` | bucket/RP 兼容处理器 | 视路由而定 | 是 | handler/auth 配置 |

### 2.2a 补充路由表 — `/api/v2/buckets*` 家族与 `OPTIONS /health`

下表补充 2.2 总览表省略的 V2 bucket 兼容路由族（共 14 条，handler.go:218-273）
以及 `OPTIONS /health` CORS 预检（handler.go:310-313）。所有路由的
`Gzipped=false`、`LoggingEnabled=true`，认证由 handler 签名 / 全局 auth 配置决定，
与 `AddRoutes` 包装逻辑 (handler.go:514-551) 一致。

| 方法 | 路径 | Handler | Route.Gzipped | Route.LoggingEnabled | 说明 |
|------|------|---------|---------------|----------------------|------|
| `POST` | `/api/v2/buckets` | `servePostCreateBucketV2` | 否 | 是 | 创建 bucket (映射到 db/rp) |
| `DELETE` | `/api/v2/buckets/:dbrp` | `serveDeleteBucketV2` | 否 | 是 | 删除 bucket |
| `GET` | `/api/v2/buckets/:dbrp` | `serveRetrieveBucketV2` | 否 | 是 | 检索单个 bucket |
| `GET` | `/api/v2/buckets` | `serveListBucketsV2` | 否 | 是 | 列出所有 bucket |
| `PATCH` | `/api/v2/buckets/:dbrp` | `serveUpdateBucketV2` | 否 | 是 | 更新 bucket |
| `GET` | `/api/v2/buckets/:dbrp/labels` | `serveLabelsNotAllowedV2` | 否 | 是 | labels 不支持 (V2 兼容空实现) |
| `POST` | `/api/v2/buckets/:dbrp/labels` | `serveLabelsNotAllowedV2` | 否 | 是 | labels 不支持 |
| `DELETE` | `/api/v2/buckets/:dbrp/labels/:labelID` | `serveLabelsNotAllowedV2` | 否 | 是 | labels 不支持 |
| `GET` | `/api/v2/buckets/:dbrp/members` | `serveBucketMembersNotAllowedV2` | 否 | 是 | members 不支持 (OSS) |
| `POST` | `/api/v2/buckets/:dbrp/members` | `serveBucketMembersNotAllowedV2` | 否 | 是 | members 不支持 |
| `DELETE` | `/api/v2/buckets/:dbrp/members/:userID` | `serveBucketMembersNotAllowedV2` | 否 | 是 | members 不支持 |
| `GET` | `/api/v2/buckets/:dbrp/owners` | `serveBucketOwnersNotAllowedV2` | 否 | 是 | owners 不支持 (OSS) |
| `POST` | `/api/v2/buckets/:dbrp/owners` | `serveBucketOwnersNotAllowedV2` | 否 | 是 | owners 不支持 |
| `DELETE` | `/api/v2/buckets/:dbrp/owners/:userID` | `serveBucketOwnersNotAllowedV2` | 否 | 是 | owners 不支持 |
| `OPTIONS` | `/health` | `serveOptions` | 否 | 是 | CORS 预检 (handler.go:310-313) |

> **注**: bucket 路由族用 `:dbrp` 路径参数承载 `database/retentionPolicy` 映射；
> labels/members/owners 系列在 OSS 单机版统一返回 "not allowed"，仅用于 V2 API 形态兼容。
> `OPTIONS /health` 与 `OPTIONS /query`、`OPTIONS /write` 一样走 `serveOptions`，
> 不经过 `authWrapper`，由 CORS 中间件直接短路返回。

> `Route` 的两个布尔字段只控制 gzip 和访问日志。认证不能从这两个布尔值推断，
> 而是由 handler 签名、全局 auth 配置、endpoint-specific auth 配置和包装器共同决定。

**条件认证说明**:
- `/ping`, `/status`, `/health`, `/metrics`: 当 `AuthEnabled && PingAuthEnabled` 时需要认证
- pprof 路由: 当 `AuthEnabled && PprofEnabled && PprofAuthEnabled` 时需要认证，且要求 `AuthorizeUnrestricted()`（管理员权限）

### 2.3 pprof 路由（条件注册）

当 `AuthEnabled && PprofEnabled && PprofAuthEnabled` 时，额外注册：

| 方法 | 路径 | Handler | 说明 |
|------|------|---------|------|
| `GET` | `/debug/pprof/cmdline` | `httppprof.Cmdline` | 命令行参数 |
| `GET` | `/debug/pprof/profile` | `httppprof.Profile` | CPU profile |
| `GET` | `/debug/pprof/symbol` | `httppprof.Symbol` | 符号表 |
| `GET` | `/debug/pprof/all` | `archiveProfilesAndQueries` | 全量 profile 归档 |
| `GET` | `/debug/vars` | `serveExpvar` | expvar 指标 |
| `GET` | `/debug/requests` | `serveDebugRequests` | 请求追踪 |

当 `PprofAuthEnabled=false` 时，pprof 路由绕过中间件链，直接由 `h.handleProfiles` 分发到
`net/http/pprof` 处理器（handler.go:569-570 → pprof.go:22-35）。`handleProfiles` 按
`r.URL.Path` switch 到 `httppprof.Cmdline`/`Profile`/`Symbol`/`archiveProfilesAndQueries`/`Index`，
**不涉及** `http.DefaultServeMux`。

## 3. 中间件链

### 3.1 中间件注册顺序

中间件在 `AddRoutes()` 中按**从外到内**的顺序包装（handler.go:514-553）：

```go
// services/httpd/handler.go:514 — AddRoutes
func (h *Handler) AddRoutes(routes ...Route) {
    for _, r := range routes {
        var handler http.Handler

        // 1. 认证包装 (最内层)
        if hf, ok := r.HandlerFunc.(func(http.ResponseWriter, *http.Request, meta.User)); ok {
            handler = authenticate(hf, h, h.Config.AuthEnabled)
        }
        if hf, ok := r.HandlerFunc.(func(http.ResponseWriter, *http.Request)); ok {
            handler = http.HandlerFunc(hf)
        }

        // 2. 写入限流 (仅 /write 和 /api/v1/prom/write)
        if r.Method == http.MethodPost {
            switch r.Pattern {
            case "/write", "/api/v1/prom/write":
                handler = h.writeThrottler.Handler(handler)
            }
        }

        // 3. ResponseWriter 包装 (内容协商)
        handler = h.responseWriter(handler)

        // 4. Gzip 压缩 (仅响应体)
        if r.Gzipped {
            handler = gzipFilter(handler)
        }

        // 5. 自定义响应头
        handler = h.SetHeadersHandler(handler)

        // 6. CORS
        handler = cors(handler)

        // 7. 请求 ID
        handler = requestID(handler)

        // 8. 访问日志
        if h.Config.LogEnabled && r.LoggingEnabled {
            handler = h.logging(handler, r.Name)
        }

        // 9. Panic 恢复 (最外层)
        handler = h.recovery(handler, r.Name)

        h.mux.Add(r.Method, r.Pattern, handler)
    }
}
```

### 3.2 中间件执行流程图

```mermaid
flowchart TD
    REQ["HTTP 请求进入"] --> R["⑨ recovery<br>(最外层, panic 兜底)"]
    R --> L["⑧ logging<br>(记录访问日志)"]
    L --> RID["⑦ requestID<br>(生成/传递 X-Request-Id)"]
    RID --> C["⑥ cors<br>(跨域资源共享)"]
    C --> SH["⑤ SetHeaders<br>(用户自定义响应头)"]
    SH --> GZ["④ gzipFilter<br>(Accept-Encoding: gzip)"]
    GZ --> RW["③ responseWriter<br>(内容协商: JSON/CSV/msgpack)"]
    RW --> TH{"② writeThrottler<br>(仅写入端点)"}
    TH -->|写入端点| AUTH["① authenticate<br>(认证 + 授权)"]
    TH -->|非写入端点| AUTH
    AUTH --> HANDLER["⑩ 路由 Handler<br>(serveWrite / serveQuery / ...)"]
    HANDLER --> RESP["HTTP 响应返回"]

    style R fill:#f96,stroke:#333
    style AUTH fill:#6cf,stroke:#333
    style TH fill:#fc6,stroke:#333
```

> **小白解释**: 中间件像洋葱一样层层包裹。请求从外到内穿过每一层，响应从内到外穿过每一层。
> 每一层都可以在请求到达 Handler 之前做预处理，或在响应返回客户端之前做后处理。
>
> 例如 `recovery` 在最外层，意味着如果 Handler 或任何内层中间件发生 panic，
> recovery 都能捕获并返回 500，而不是让整个进程崩溃。

### 3.3 各中间件详解

#### 3.3.1 recovery — Panic 恢复 (handler.go:3061)

```go
// services/httpd/handler.go:3061 — recovery
func (h *Handler) recovery(inner http.Handler, name string) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        start := time.Now()
        l := &responseLogger{w: w}

        defer func() {
            if err := recover(); err != nil {
                logLine := buildLogLine(l, r, start)
                logLine = fmt.Sprintf("%s [panic:%s] %s", logLine, err, debug.Stack())
                h.CLFLogger.Println(logLine)
                http.Error(w, http.StatusText(http.StatusInternalServerError), 500)
                atomic.AddInt64(&h.stats.RecoveredPanics, 1)

                if willCrash {
                    // INFLUXDB_PANIC_CRASH=true 时，记录所有 goroutine 后退出
                    h.CLFLogger.Println("\n\n=====\nAll goroutines now follow:")
                    buf := debug.Stack()
                    h.CLFLogger.Printf("%s\n", buf)
                    os.Exit(1)
                }
            }
        }()

        inner.ServeHTTP(l, r)
    })
}
```

**关键设计**:
- `willCrash` 由环境变量 `INFLUXDB_PANIC_CRASH` 控制（handler.go:3052 `willCrash` 声明；
  handler.go:3056 `init()` 读 `os.Getenv(query.PanicCrashEnv)`；
  常量定义在 `query/executor.go:54` `PanicCrashEnv = "INFLUXDB_PANIC_CRASH"`）。
  **注意是 `INFLUXDB_PANIC_CRASH`，不是 `INFLUXD_PANIC_CRASH`。**
- 默认 `willCrash=false`：panic 被捕获，返回 500，服务继续运行
- `willCrash=true`：panic 后打印所有 goroutine 栈并 `os.Exit(1)`，用于调试
- `debug.Stack()` 记录完整调用栈，便于事后分析

#### 3.3.2 logging — 访问日志 (handler.go:3023)

```go
// services/httpd/handler.go:3023 — logging
func (h *Handler) logging(inner http.Handler, name string) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        start := time.Now()
        l := &responseLogger{w: w}       // 包装 ResponseWriter，捕获状态码
        inner.ServeHTTP(l, r)

        if h.accessLogFilters.Match(l.Status()) {
            h.CLFLogger.Println(buildLogLine(l, r, start))  // CLF 格式
        }

        // 5xx 错误额外记录到结构化日志
        if l.Status()/100 == 5 {
            errStr := l.Header().Get("X-InfluxDB-Error")
            if errStr != "" {
                h.Logger.Error(fmt.Sprintf("[%d] - %q", l.Status(), errStr))
            }
        }
    })
}
```

**CLF (Common Log Format)** 示例:
```
127.0.0.1 - - [01/Jan/2024:00:00:00 +0000] "POST /write?db=mydb HTTP/1.1" 204 0 "-" "curl/7.68.0" <request-id> <duration>
```

**状态码过滤器** (`accessLogStatusFilters`):
- 支持通配符：`2XX` 匹配所有 200-299，`5XX` 匹配所有 500-599
- 未配置时默认记录所有状态码

#### 3.3.3 requestID — 请求 ID 生成 (handler.go:2974)

```go
// services/httpd/handler.go:2974 — requestID
func requestID(inner http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        // 优先级: X-Request-Id > Request-Id > 自动生成
        rid := r.Header.Get("X-Request-Id")
        if rid == "" {
            rid = r.Header.Get("Request-Id")
        }
        if rid == "" {
            rid = uuid.TimeUUID().String()  // v1 UUID (基于时间)
        }

        r.Header.Set("Request-Id", rid)     // 内部传递
        w.Header().Set("X-Request-Id", rid) // 响应头 (标准名)
        w.Header().Set("Request-Id", rid)   // 响应头 (兼容旧版)

        inner.ServeHTTP(w, r)
    })
}
```

> **小白解释**: 每个请求都有一个唯一 ID，就像快递单号。
> 如果客户端没传，服务器自动生成一个。这个 ID 会贯穿整个请求链路，
> 方便在日志中追踪一个请求的完整处理过程。

#### 3.3.4 cors — 跨域资源共享 (handler.go:2935)

```go
// services/httpd/handler.go:2935 — cors
func cors(inner http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        if origin := r.Header.Get("Origin"); origin != "" {
            w.Header().Set("Access-Control-Allow-Origin", origin)
            w.Header().Set("Access-Control-Allow-Methods", strings.Join([]string{
                "DELETE", "GET", "OPTIONS", "POST", "PUT", "PATCH",   // 含 PATCH
            }, ", "))
            w.Header().Set("Access-Control-Allow-Headers", strings.Join([]string{
                "Accept", "Accept-Encoding", "Authorization",
                "Content-Length", "Content-Type",
                "User-Agent",                            // 含 User-Agent
                "X-CSRF-Token", "X-HTTP-Method-Override",
            }, ", "))
            w.Header().Set("Access-Control-Expose-Headers", strings.Join([]string{
                "Date", "X-InfluxDB-Version", "X-InfluxDB-Build",
            }, ", "))
        }

        if r.Method == "OPTIONS" {
            return  // 预检请求直接返回，不调用 inner handler
        }

        inner.ServeHTTP(w, r)
    })
}
```

> **审计校准** (handler.go:2935-2972):
> - `Access-Control-Allow-Methods` 含 `PATCH` (共 6 个方法)。
> - `Access-Control-Allow-Headers` 含 `User-Agent` (共 8 个头)。
> - 实际用 `strings.Join([]string{...}, ", ")` 拼接，不是单个字符串字面量。

**关键设计**:
- `Access-Control-Allow-Origin` 设为请求的 `Origin` 值（即允许任意来源）
- OPTIONS 请求直接返回，不进入后续中间件和 Handler
- 只暴露 `Date`、`X-InfluxDB-Version`、`X-InfluxDB-Build`。错误详情和 request id
  可能存在于响应头中，但当前 CORS `Access-Control-Expose-Headers` 不暴露它们。
- 只有请求带 `Origin` 头时才设置 CORS 头；`Access-Control-Allow-Origin`
  回显请求 Origin，不写固定 `*`，但效果上允许任意 Origin。

#### 3.3.5 SetHeaders — 用户自定义响应头 (handler.go:3005)

```go
// services/httpd/handler.go:3005 — SetHeadersHandler
func (h *Handler) SetHeadersHandler(handler http.Handler) http.Handler {
    return http.HandlerFunc(h.SetHeadersWrapper(handler.ServeHTTP))
}

func (h *Handler) SetHeadersWrapper(f func(http.ResponseWriter, *http.Request)) func(http.ResponseWriter, *http.Request) {
    if len(h.Config.HTTPHeaders) == 0 {
        return f  // 无自定义头，直接透传
    }

    return func(w http.ResponseWriter, r *http.Request) {
        for header, value := range h.Config.HTTPHeaders {
            w.Header().Add(header, value)
        }
        f(w, r)
    }
}
```

配置示例 (`influxdb.conf`):
```toml
[http]
  [http.headers]
    X-Content-Type-Options = "nosniff"
    X-Frame-Options = "DENY"
```

#### 3.3.6 gzipFilter — 响应压缩 (gzip.go:20)

```go
// services/httpd/gzip.go:20 — gzipFilter
func gzipFilter(inner http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
            inner.ServeHTTP(w, r)  // 客户端不支持 gzip，直接透传
            return
        }

        gw := &lazyGzipResponseWriter{ResponseWriter: w, Writer: w}
        // ... 附加 Flusher, CloseNotifier
        defer gw.Close()
        inner.ServeHTTP(gw, r)
    })
}
```

**懒初始化设计**:

```go
// services/httpd/gzip.go:43 — WriteHeader
func (w *lazyGzipResponseWriter) WriteHeader(code int) {
    if w.wroteHeader { return }
    w.wroteHeader = true

    if code == http.StatusOK {
        w.Header().Set("Content-Encoding", "gzip")
        if _, ok := w.Writer.(*gzip.Writer); !ok {
            w.Writer = getGzipWriter(w.Writer)  // 从 sync.Pool 获取
        }
    }
    w.ResponseWriter.WriteHeader(code)
}
```

**关键设计**:
- **懒初始化**: 只在第一次 WriteHeader(200) 时才创建 gzip.Writer
- **sync.Pool 复用**: `gzipWriterPool` 避免频繁分配 gzip.Writer
- **非 200 不压缩**: 错误响应不进行 gzip 压缩
- **资源回收**: `defer gw.Close()` 将 gzip.Writer 放回 Pool

```go
// services/httpd/gzip.go:89 — gzipWriterPool
var gzipWriterPool = sync.Pool{
    New: func() interface{} {
        return gzip.NewWriter(nil)
    },
}

func getGzipWriter(w io.Writer) *gzip.Writer {
    gz := gzipWriterPool.Get().(*gzip.Writer)
    gz.Reset(w)
    return gz
}

func putGzipWriter(gz *gzip.Writer) {
    gz.Close()
    gzipWriterPool.Put(gz)
}
```

#### 3.3.7 responseWriter — 内容协商 (handler.go:3043)

```go
// services/httpd/handler.go:3043 — responseWriter
func (h *Handler) responseWriter(inner http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        w = NewResponseWriter(w, r)
        inner.ServeHTTP(w, r)
    })
}
```

**内容协商逻辑** (response_writer.go:51):

```go
// services/httpd/response_writer.go:51 — NewResponseWriter
func NewResponseWriter(w http.ResponseWriter, r *http.Request) ResponseWriter {
    pretty := r.URL.Query().Get("pretty") == "true"
    rw := &responseWriter{ResponseWriter: w}

    acceptHeaders := parseAccept(r.Header["Accept"])
    for _, accept := range acceptHeaders {
        for _, ct := range contentTypes {
            if match(accept, ct) {
                w.Header().Add("Content-Type", ct.full)
                rw.formatter = ct.formatter(pretty)
                return rw
            }
        }
    }
    // 默认 JSON
    w.Header().Add("Content-Type", defaultContentType.full)
    rw.formatter = defaultContentType.formatter(pretty)
    return rw
}
```

**支持的内容类型**:

| Accept Header | Content-Type | 格式化器 | 说明 |
|---------------|-------------|----------|------|
| `application/json` | `application/json` | `jsonFormatter` | 默认格式 |
| `application/csv` | `application/csv` | `csvFormatter` | CSV 格式 |
| `text/csv` | `text/csv` | `csvFormatter` | CSV 格式 |
| `application/x-msgpack` | `application/x-msgpack` | `msgpackFormatter` | MessagePack 二进制格式 |

#### 3.3.8 writeThrottler — 写入限流 (handler.go:2086)

（详见第 7 节）

#### 3.3.9 authenticate — 认证中间件 (handler.go:1776)

（详见第 6 节）

## 4. 写入请求全链路

### 4.1 V1 写入: POST /write

```mermaid
sequenceDiagram
    participant Client as 写入客户端
    participant V1 as serveWriteV1
    participant W as serveWrite
    participant MC as MetaClient
    participant WA as WriteAuthorizer
    participant TR as truncateReader
    participant GZ as gzip.Reader
    participant PP as ParsePointsWithPrecision
    participant PW as PointsWriter

    Client->>V1: POST /write?db=mydb&rp=autogen&precision=n
    V1->>V1: 解析 precision 参数 (n/u/ms/s/m/h)
    V1->>V1: 获取 db, rp 查询参数
    V1->>W: serveWrite(db, rp, precision, w, r, user)

    W->>W: ① atomic.AddInt64(&stats.WriteRequests, 1)
    W->>W: ② 检查 database != ""

    W->>MC: ③ Database(database)
    alt 数据库不存在
        MC-->>W: nil
        W-->>Client: 404 "database not found"
    end

    alt AuthEnabled
        W->>W: ④ 检查 user != nil
        W->>WA: ⑤ AuthorizeWrite(user.ID(), database)
        alt 未授权
            WA-->>W: error
            W-->>Client: 403 Forbidden
        end
    end

    W->>TR: ⑥ truncateReader(body, MaxBodySize)
    Note over TR: 超过 25MB 返回 errTruncated

    alt Content-Encoding: gzip
        W->>GZ: ⑦ gzip.NewReader(body)
    end

    W->>W: ⑧ buf.ReadFrom(body)
    alt errTruncated
        W-->>Client: 413 Request Entity Too Large
    end

    W->>W: atomic.AddInt64(&stats.WriteRequestBytesReceived, buf.Len())

    W->>PP: ⑨ models.ParsePointsWithPrecision(bytes, now, precision)
    alt 全部解析失败
        PP-->>W: error, empty points
        W-->>Client: 400 Bad Request
    end

    W->>W: ⑩ 解析 consistency 参数 (默认: one)

    W->>PW: ⑪ WritePoints(db, rp, consistency, user, points)

    alt IsClientError
        PW-->>W: client error
        W-->>Client: 400 Bad Request
    else IsAuthorizationError
        PW-->>W: auth error
        W-->>Client: 403 Forbidden
    else PartialWriteError
        PW-->>W: partial error (dropped=N)
        W-->>Client: 400 Partial Write
    else 其他错误
        PW-->>W: internal error
        W-->>Client: 500 Internal Server Error
    else 成功
        PW-->>W: nil
        W-->>Client: 204 No Content
    end
```

### 4.2 V2 写入适配: POST /api/v2/write

```go
// services/httpd/handler.go:1627 — serveWriteV2
func (h *Handler) serveWriteV2(w http.ResponseWriter, r *http.Request, user meta.User) {
    // 1. 转换精度参数: ns→n, us→u, ms/s 保持不变
    precision := r.URL.Query().Get("precision")
    switch precision {
    case "ns": precision = "n"
    case "us": precision = "u"
    case "ms", "s", "": // 保持不变
    default:
        h.httpError(w, fmt.Sprintf("invalid precision %q", precision), http.StatusBadRequest)
    }

    // 2. bucket → db/rp 转换
    db, rp, err := bucket2dbrp(r.URL.Query().Get("bucket"))
    if err != nil {
        h.httpError(w, err.Error(), http.StatusNotFound)
        return
    }

    // 3. 复用 V1 写入逻辑
    h.serveWrite(db, rp, precision, w, r, user)
}
```

**bucket2dbrp 转换规则** (handler.go:942):

| bucket 格式 | 解析结果 | 说明 |
|-------------|---------|------|
| `"mydb/myrp"` | db="mydb", rp="myrp" | 标准格式 |
| `"mydb"` | db="mydb", rp="" | 仅指定数据库 |
| `""` | error | 空 bucket 报错 |
| `"/myrp"` | error | 空数据库报错 |

### 4.3 写入错误处理链

```go
// services/httpd/handler.go:1773-1805 — 错误处理级联
if err := writePoints(); influxdb.IsClientError(err) {
    // 客户端错误: 数据格式问题、类型冲突等
    atomic.AddInt64(&h.stats.PointsWrittenFail, int64(len(points)))
    h.httpError(w, err.Error(), http.StatusBadRequest)       // 400
} else if influxdb.IsAuthorizationError(err) {
    // 授权错误: 用户无权写入该数据库
    atomic.AddInt64(&h.stats.PointsWrittenFail, int64(len(points)))
    h.httpError(w, err.Error(), http.StatusForbidden)        // 403
} else if werr, ok := err.(tsdb.PartialWriteError); ok {
    // 部分写入错误: 部分点成功，部分失败
    atomic.AddInt64(&h.stats.PointsWrittenOK, int64(len(points)-werr.Dropped))
    atomic.AddInt64(&h.stats.PointsWrittenDropped, int64(werr.Dropped))
    h.httpError(w, werr.Error(), http.StatusBadRequest)     // 400
} else if err != nil {
    // 其他内部错误
    atomic.AddInt64(&h.stats.PointsWrittenFail, int64(len(points)))
    h.httpError(w, err.Error(), http.StatusInternalServerError) // 500
} else if parseError != nil {
    // 部分解析失败: 有些点写入成功，有些解析失败
    atomic.AddInt64(&h.stats.PointsWrittenOK, int64(len(points)))
    h.httpError(w, tsdb.PartialWriteError{Reason: parseError.Error()}.Error(), http.StatusBadRequest) // 400
} else {
    // 完全成功
    atomic.AddInt64(&h.stats.PointsWrittenOK, int64(len(points)))
    h.writeHeader(w, http.StatusNoContent)                   // 204
}
```

> **具体案例**: 写入一批数据，其中包含一个格式错误的行
>
> ```
> curl -X POST 'http://localhost:8086/write?db=mydb' \
>   -d 'cpu,host=web01 value=87.3 1704067200000000000
> cpu,host=web01 value=invalid 1704067200000000001
> cpu,host=web01 value=92.1 1704067200000000002'
> ```
>
> 处理过程：
> 1. `ParsePointsWithPrecision` 解析 3 行，第 2 行解析失败
> 2. 返回 2 个有效 point + parseError
> 3. `writePoints()` 成功写入 2 个 point
> 4. 由于 `parseError != nil`，返回 400 + PartialWriteError 信息
> 5. 客户端知道有部分数据未被写入

### 4.4 truncateReader — 请求体大小限制

```go
// services/httpd/io.go:14 — truncateReader
func truncateReader(r io.Reader, n int64) io.ReadCloser {
    tr := &truncatedReader{r: &io.LimitedReader{R: r, N: n + 1}}
    if rc, ok := r.(io.Closer); ok {
        tr.Closer = rc
    }
    return tr
}

type truncatedReader struct {
    r *io.LimitedReader
    io.Closer
}

func (r *truncatedReader) Read(p []byte) (n int, err error) {
    n, err = r.r.Read(p)
    if r.r.N <= 0 {
        return n, errTruncated  // 超过限制，返回截断错误
    }
    return n, err
}
```

> **小白解释**: `truncateReader` 就像一个计数器——每读一个字节就减一。
> 当读取的字节数超过 `MaxBodySize` (默认 25MB) 时，立即返回 `errTruncated`，
> 后续代码捕获这个错误返回 413 (Request Entity Too Large)。
>
> 注意使用 `N+1` 而非 `N`：只有当读取**超过** N 字节时才触发截断，
> 恰好 N 字节时仍视为正常。

## 5. 查询请求全链路

### 5.1 serveQuery 完整流程

```mermaid
sequenceDiagram
    participant Client as 查询客户端
    participant SQ as serveQuery
    participant P as influxql.Parser
    participant QA as QueryAuthorizer
    participant QE as QueryExecutor
    participant RW as ResponseWriter

    Client->>SQ: GET/POST /query?db=mydb&q=SELECT+*+FROM+cpu

    SQ->>SQ: ① atomic.AddInt64(&stats.QueryRequests, 1)

    SQ->>SQ: ② 读取查询语句
    alt 从 'q' 表单参数
        SQ->>SQ: qr = strings.NewReader(r.FormValue("q"))
    else 从 multipart 文件上传
        SQ->>SQ: qr = multipartForm.File["q"][0].Open()
    end

    SQ->>SQ: ③ sanitize(r) — 清理请求参数（防止密码泄露到日志）

    SQ->>P: ④ influxql.NewParser(qr).ParseQuery()
    alt 解析失败
        P-->>SQ: error
        SQ-->>Client: 400 "error parsing query"
    end

    SQ->>QA: ⑤ AuthorizeQuery(user, query, database)
    alt 授权失败
        QA-->>SQ: error
        SQ-->>Client: 403 Forbidden
    else 授权成功
        QA-->>SQ: fineAuthorizer
    end

    SQ->>SQ: ⑥ 解析 chunked, chunk_size, async 参数

    SQ->>SQ: ⑦ 构建 ExecutionOptions{Database, ChunkSize, ReadOnly, Authorizer, ...}

    SQ->>SQ: ⑧ 设置 closing 通道 (客户端断开时取消查询)

    SQ->>QE: ⑨ ExecuteQuery(query, opts, closing) → chan *QueryResult

    alt async=true
        SQ->>SQ: go async(q, results) — 后台消费
        SQ-->>Client: 204 No Content
    else chunked=true
        loop 每个结果
            QE-->>SQ: result
            SQ->>RW: WriteResponse({Results: [result]})
            SQ->>RW: Flush()
        end
    else 非分块
        QE-->>SQ: result
        SQ->>SQ: 合并同 statement 结果
        SQ->>SQ: 应用 MaxRowLimit 裁剪
        SQ->>RW: WriteResponse({Results: allResults})
    end
```

### 5.2 查询参数详解

| 参数 | 类型 | 说明 |
|------|------|------|
| `q` | string | InfluxQL 查询语句 (必需) |
| `db` | string | 目标数据库 |
| `rp` | string | 目标保留策略 |
| `epoch` | string | 时间戳精度: `h`, `m`, `s`, `ms`, `u`, `ns` |
| `chunked` | bool | 是否分块返回 |
| `chunk_size` | int | 每块最大行数 (默认 10000) |
| `async` | bool | 是否异步执行 |
| `node_id` | uint64 | 指定执行节点 (集群模式) |
| `pretty` | bool | JSON 格式化输出 |
| `params` | string | JSON 格式的查询参数 |

### 5.3 分块 vs 非分块查询

```go
// 分块模式: 流式返回每个结果
if chunked {
    n, _ := rw.WriteResponse(Response{Results: []*query.Result{r}})
    atomic.AddInt64(&h.stats.QueryRequestBytesTransmitted, int64(n))
    w.(http.Flusher).Flush()
    continue
}

// 非分块模式: 缓冲所有结果后一次性返回
// 合并同一 StatementID 的结果
if resp.Results[l-1].StatementID == r.StatementID {
    // 合并 series: 如果是同一个 series，追加 values
    lastSeries.Values = append(lastSeries.Values, row.Values...)
}
```

**MaxRowLimit 保护** (handler.go:832-853):

```go
if h.Config.MaxRowLimit > 0 {
    for i, series := range r.Series {
        n := h.Config.MaxRowLimit - rows
        if n < len(series.Values) {
            series.Values = series.Values[:n]  // 截断
            series.Partial = true              // 标记为部分结果
        }
        rows += len(series.Values)
        if rows >= h.Config.MaxRowLimit {
            r.Series = r.Series[:i+1]  // 丢弃后续 series
            break
        }
    }
}
```

> **小白解释**: 非分块查询就像把所有货物装进一个大箱子再寄出——
> 如果货物太多（超过 MaxRowLimit），就只装一部分，剩下的丢弃。
> 分块查询则像分批寄送——每批 chunk_size 件，分多次寄出。
> 分块模式对大结果集更友好，不会撑爆内存。

### 5.4 客户端断开检测

```go
// services/httpd/handler.go:749-774 — closing 通道
var closing chan struct{}
if !async {
    closing = make(chan struct{})
    if notifier, ok := w.(http.CloseNotifier); ok {
        done := make(chan struct{})
        defer close(done)

        notify := notifier.CloseNotify()
        go func() {
            select {
            case <-done:    // 请求正常完成
            case <-notify:  // 客户端断开
                close(closing)  // 通知 QueryExecutor 取消查询
            }
        }()
        opts.AbortCh = done
    } else {
        defer close(closing)
    }
}
```

> **具体案例**: 客户端发起一个大查询后立即断开连接
>
> 1. 客户端发送 `SELECT * FROM cpu` (可能返回百万行)
> 2. QueryExecutor 开始执行，通过 results channel 返回数据
> 3. 客户端在第 1000 行时断开连接
> 4. `CloseNotify()` 触发，`closing` 通道被关闭
> 5. QueryExecutor 检测到 `closing` 通道关闭，停止执行
> 6. 资源被释放，不再继续扫描数据

## 6. 认证机制

### 6.1 parseCredentials — 凭据解析 (handler.go:2785)

```go
// services/httpd/handler.go:2785 — parseCredentials
func parseCredentials(r *http.Request) (*credentials, error) {
    q := r.URL.Query()

    // 来源 1: URL 参数 u/p
    if u, p := q.Get("u"), q.Get("p"); u != "" && p != "" {
        return &credentials{Method: UserAuthentication, Username: u, Password: p}, nil
    }

    // 来源 2-4: Authorization 头
    if s := r.Header.Get("Authorization"); s != "" {
        strs := strings.Split(s, " ")
        if len(strs) == 2 {
            switch strs[0] {
            case "Bearer":
                // 来源 2: Bearer JWT Token
                return &credentials{Method: BearerAuthentication, Token: strs[1]}, nil
            case "Token":
                // 来源 3: Token username:password
                if u, p, ok := parseToken(strs[1]); ok {
                    return &credentials{Method: UserAuthentication, Username: u, Password: p}, nil
                }
            }
        }

        // 来源 4: Basic Auth (base64)
        if u, p, ok := r.BasicAuth(); ok {
            return &credentials{Method: UserAuthentication, Username: u, Password: p}, nil
        }
    }

    return nil, fmt.Errorf("unable to parse authentication credentials")
}
```

**四种凭据来源及优先级**:

| 优先级 | 来源 | 格式 | 认证方式 |
|--------|------|------|---------|
| 1 | URL 参数 | `?u=admin&p=secret` | UserAuthentication |
| 2 | Bearer Token | `Authorization: Bearer <JWT>` | BearerAuthentication |
| 3 | Token 头 | `Authorization: Token admin:secret` | UserAuthentication |
| 4 | Basic Auth | `Authorization: Basic <base64>` | UserAuthentication |

### 6.2 authenticate 中间件 (handler.go:2837)

```mermaid
flowchart TD
    REQ["请求进入 authenticate"] --> CHECK{"requireAuthentication?"}
    CHECK -->|false| PASSNIL["inner(w, r, nil)<br>跳过认证，传递 nil user"]
    CHECK -->|true| ADMIN{"AdminUserExists()?"}

    ADMIN -->|false| PASSNIL2["inner(w, r, nil)<br>引导模式: 无管理员用户"]
    ADMIN -->|true| PARSE["parseCredentials(r)"]

    PARSE -->|error| UNAUTH["401 Unauthorized<br>atomic.AddInt64(stats.AuthenticationFailures)"]

    PARSE -->|成功| METHOD{"creds.Method?"}

    METHOD -->|UserAuthentication| USER{"Username 非空?"}
    USER -->|空| UNAUTH2["401 'username required'"]
    USER -->|非空| AUTH_MC["MetaClient.Authenticate(username, password)"]
    AUTH_MC -->|error| UNAUTH3["401 'authorization failed'"]
    AUTH_MC -->|成功| USER_OK["user = authenticated user"]

    METHOD -->|BearerAuthentication| SECRET{"SharedSecret 非空?"}
    SECRET -->|空| UNAUTH4["401 'bearer auth disabled'"]
    SECRET -->|非空| JWT["jwt.Parse(token, keyLookupFn)"]
    JWT -->|error| UNAUTH5["401 JWT 验证失败"]
    JWT -->|成功| VALID{"token.Valid?"}
    VALID -->|false| UNAUTH6["401 'invalid token'"]
    VALID -->|true| CLAIMS["提取 claims"]
    CLAIMS --> EXP{"exp > 0?"}
    EXP -->|否| UNAUTH7["401 'token expiration required'"]
    EXP -->|是| USERNAME{"username 非空字符串?"}
    USERNAME -->|否| UNAUTH8["401 'token must contain a username'"]
    USERNAME -->|是| LOOKUP["MetaClient.User(username)"]
    LOOKUP -->|error/nil| UNAUTH9["401 ErrUserNotFound"]
    LOOKUP -->|成功| USER_OK2["user = looked up user"]

    USER_OK --> INNER["inner(w, r, user)"]
    USER_OK2 --> INNER
    PASSNIL --> INNER
    PASSNIL2 --> INNER
```

### 6.3 JWT 认证详解

```go
// services/httpd/handler.go:2869 — BearerAuthentication 分支
case BearerAuthentication:
    if h.Config.SharedSecret == "" {
        // SharedSecret 未配置，拒绝所有 JWT
        atomic.AddInt64(&h.stats.AuthenticationFailures, 1)
        h.httpError(w, "bearer auth disabled", http.StatusUnauthorized)
        return
    }

    // HMAC 签名验证
    keyLookupFn := func(token *jwt.Token) (interface{}, error) {
        if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
            return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
        }
        return []byte(h.Config.SharedSecret), nil
    }

    // 解析并验证 JWT
    token, err := jwt.Parse(creds.Token, keyLookupFn)
    // ... 验证 token.Valid, claims["exp"], claims["username"]

    // 从元数据存储查找用户
    if user, err = h.MetaClient.User(username); err != nil { ... }
```

**JWT Claims 要求**:

| Claim | 类型 | 必需 | 说明 |
|-------|------|------|------|
| `username` | string | 是 | 用户名，必须存在于 MetaClient 中 |
| `exp` | float64 | 是 | 过期时间戳，必须 > 0 |

### 6.4 引导模式 (Bootstrap)

```go
// services/httpd/handler.go:2847 — authenticate
if requireAuthentication && h.MetaClient.AdminUserExists() {
    // 只有当管理员用户存在时才执行认证
    // ...
}
// 如果 AdminUserExists() == false，跳过认证，user = nil
```

> **小白解释**: 首次启动 InfluxDB 时，系统中没有任何用户。
> 此时即使 `auth-enabled=true`，认证也会被跳过——
> 这允许管理员第一次连接时创建用户。一旦第一个管理员用户被创建，
> `AdminUserExists()` 返回 true，后续所有请求都需要认证。

## 7. 限流器 (Throttler)

### 7.1 Throttler 结构

```go
// services/httpd/handler.go:3151 — Throttler
type Throttler struct {
    current  chan struct{}    // 并发处理槽 (如: 缓冲区大小 = MaxConcurrentWriteLimit)
    enqueued chan struct{}    // 排队槽 (缓冲区大小 = concurrent + maxEnqueue)

    EnqueueTimeout time.Duration  // 排队超时时间
    Logger         *zap.Logger
}

func NewThrottler(concurrentN, maxEnqueueN int) *Throttler {
    return &Throttler{
        current:  make(chan struct{}, concurrentN),             // 如: 10
        enqueued: make(chan struct{}, concurrentN+maxEnqueueN), // 如: 10+100=110
        Logger:   zap.NewNop(),
    }
}
```

### 7.2 限流执行流程

```mermaid
flowchart TD
    REQ["写入请求进入"] --> CAP{"current 容量 == 0?"}
    CAP -->|是| PASS["直接放行<br>(无限流)"]
    CAP -->|否| ENQUEUE_TRY{"尝试进入排队槽<br>select { enqueued <- struct{}{} }"}

    ENQUEUE_TRY -->|成功| CURRENT_TRY{"尝试立即获取处理槽<br>select { current <- struct{}{} }"}
    ENQUEUE_TRY -->|满| REJECT_QUEUE["503 'request throttled, queue full'"]

    CURRENT_TRY -->|成功| EXEC["执行 Handler<br>defer: &lt;-current, &lt;-enqueued"]
    CURRENT_TRY -->|满| WAIT["等待处理槽...<br>select { current; timerCh }"]

    WAIT -->|获取到处理槽| EXEC
    WAIT -->|超时| REJECT_TIMEOUT["503 'request throttled, exceeds timeout'"]

    EXEC --> RESP["返回响应"]

    style REJECT_QUEUE fill:#f66,stroke:#333
    style REJECT_TIMEOUT fill:#f66,stroke:#333
    style EXEC fill:#6f6,stroke:#333
```

### 7.3 限流器实现代码

```go
// services/httpd/handler.go:3173 — Handler
func (t *Throttler) Handler(h http.Handler) http.Handler {
    timeout := t.EnqueueTimeout

    // 并发数为 0 时不限流
    if cap(t.current) == 0 {
        return h
    }

    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        // 启动超时定时器
        var timerCh <-chan time.Time
        if timeout > 0 {
            timer := time.NewTimer(timeout)
            defer timer.Stop()
            timerCh = timer.C
        }

        // 第一关: 排队槽 (enqueued = current + 排队)
        if cap(t.enqueued) > cap(t.current) {
            select {
            case t.enqueued <- struct{}{}:
                defer func() { <-t.enqueued }()
            default:
                // 排队槽已满，立即拒绝
                t.Logger.Warn("request throttled, queue full", zap.Duration("d", timeout))
                http.Error(w, "request throttled, queue full", http.StatusServiceUnavailable)
                return
            }
        }

        // 第二关: 处理槽 (current = 实际并发处理)
        select {
        case t.current <- struct{}{}:
            // 立即获取到处理槽
        default:
            // 需要等待
            select {
            case t.current <- struct{}{}:
                // 等待成功
            case <-timerCh:
                // 等待超时
                t.Logger.Warn("request throttled, exceeds timeout", zap.Duration("d", timeout))
                http.Error(w, "request throttled, exceeds timeout", http.StatusServiceUnavailable)
                return
            }
        }
        defer func() { <-t.current }()

        h.ServeHTTP(w, r)
    })
}
```

> **具体案例**: 配置 `max-concurrent-write-limit=10`, `max-enqueued-write-limit=100`, `enqueued-write-timeout=30s`
>
> ```
> 场景: 突然涌入 150 个并发写入请求
>
> 请求 1-10:   直接获取 current 槽，立即执行
> 请求 11-110: 进入 enqueued 队列等待，等前面的请求完成后获取 current 槽
> 请求 111-120: enqueued 队列已满 (10+100=110)，立即返回 503 "queue full"
> 请求 11-110 中等待超过 30s 的: 返回 503 "exceeds timeout"
>
> 两层保护:
> - enqueued 限制: 防止队列无限增长导致内存溢出
> - timeout 限制: 防止请求在队列中等待太久导致客户端超时
> ```

### 7.4 限流器应用范围

```go
// services/httpd/handler.go:429 — 仅对写入端点限流
if r.Method == http.MethodPost {
    switch r.Pattern {
    case "/write", "/api/v1/prom/write":
        handler = h.writeThrottler.Handler(handler)
    }
}
```

**限流端点**:
- `POST /write` — V1 写入
- `POST /api/v1/prom/write` — Prometheus Remote Write

**不限流端点**:
- `POST /api/v2/write` — V2 写入（未被限流）
- `POST /query`, `GET /query` — 查询
- `POST /api/v2/query` — Flux 查询
- 其他 GET/HEAD 端点

> **注意**: `/api/v2/write` 未被限流，这可能是一个遗漏。
> V2 写入路径最终调用 `serveWrite`，与 V1 走相同的写入逻辑，
> 但绕过了 Throttler 中间件。

## 8. Flux 查询端点

### 8.1 serveFluxQuery 流程

```go
// services/httpd/handler.go:2175 — serveFluxQuery
func (h *Handler) serveFluxQuery(w http.ResponseWriter, r *http.Request, user meta.User) {
    atomic.AddInt64(&h.stats.FluxQueryRequests, 1)

    // 1. 解码请求 (JSON 或 Flux 脚本)
    req, err := decodeQueryRequest(r)

    // 2. 设置上下文 (node_id, user)
    ctx := r.Context()
    if val := r.FormValue("node_id"); val != "" {
        if nodeID, err := strconv.ParseUint(val, 10, 64); err == nil {
            ctx = storage.NewContextWithReadOptions(ctx, &storage.ReadOptions{NodeID: nodeID})
        }
    }
    if h.Config.AuthEnabled {
        ctx = meta.NewContextWithUser(ctx, user)
    }

    // 3. 获取代理请求 (包含 compiler 和 dialect)
    pr := req.ProxyRequest()

    // 4. 执行 Flux 查询
    q, err := h.Controller.Query(ctx, pr.Compiler)
    // 注意: 单个 defer 包裹一个匿名函数 (handler.go:2221-2224)
    defer func() {
        q.Cancel()  // 先执行 (闭包内自上而下)
        q.Done()    // 后执行 (闭包内自上而下)
    }()

    // 5. 根据 Accept 头编码响应
    switch r.Header.Get("Accept") {
    case "text/csv":
        fallthrough
    default:
        encoder := pr.Dialect.Encoder()
        results := flux.NewResultIteratorFromQuery(q)
        n, err = encoder.Encode(w, results)
    }
}
```

> **审计校准** (handler.go:2175, 2221-2224):
> - 函数起始行是 2175，不是 1374。
> - 源码中是**单个 `defer` 包裹一个匿名函数** (handler.go:2221-2224)，不是两个独立的
>   `defer` 语句，也不是单行 `defer q.Cancel(); defer q.Done()`。源码形态:
>   ```go
>   defer func() {
>       q.Cancel()
>       q.Done()
>   }()
>   ```
> - 闭包体内语句**自上而下顺序执行** (非 LIFO): 函数返回时先跑 `q.Cancel()`，
>   再跑 `q.Done()`。即先取消查询，再标记查询完成。最终执行顺序与旧版 LIFO 推理一致，
>   但原因从"两个 defer 的后进先出"改为"单个 defer 内闭包的顺序执行"。

### 8.2 decodeQueryRequest — 请求解码

```go
// services/httpd/flux.go:25 — decodeQueryRequest
func decodeQueryRequest(r *http.Request) (*client.QueryRequest, error) {
    ct := r.Header.Get("Content-Type")
    mt, _, err := mime.ParseMediaType(ct)
    if err != nil {
        return nil, err  // Content-Type 解析错误
    }

    var req client.QueryRequest
    switch mt {
    case "application/vnd.flux":
        // 纯 Flux 脚本: Body 就是查询语句
        d, err := io.ReadAll(r.Body)
        if err != nil {
            return nil, err  // body 读取错误
        }
        req.Query = string(d)
    default:
        // JSON 格式: 包含 query, dialect, extern 等
        if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
            return nil, err  // JSON decode 错误
        }
    }

    req = req.WithDefaults()
    if err := req.Validate(); err != nil {
        return nil, err  // validate 错误
    }
    return &req, nil
}
```

**两种请求格式**:

| Content-Type | 格式 | 示例 |
|-------------|------|------|
| `application/vnd.flux` | 纯 Flux 脚本 | `from(bucket:"mydb") \|> range(start:-1h)` |
| `application/json` (默认) | JSON QueryRequest | `{"query":"from(bucket:\"mydb\")...", "type":"flux"}` |

## 9. Prometheus Remote Write/Read 协议

### 9.1 servePromWrite — Prometheus 写入适配

> **小白解释**: Prometheus Remote Write 就像一个**翻译官**——
> Prometheus 发送的是 Protobuf 格式的时序数据，InfluxDB 需要把它翻译成自己的 Point 格式再写入。
> 关键区别：这个端点**绕过了 Line Protocol 解析器**，直接从 Protobuf 构建 Point。

```mermaid
sequenceDiagram
    participant Prom as Prometheus
    participant HTTP as servePromWrite
    participant TR as truncateReader
    participant PB as Protobuf 解码
    participant Conv as prometheus.WriteRequestToPoints
    participant PW as PointsWriter

    Prom->>HTTP: POST /api/v1/prom/write<br/>Content-Type: application/x-protobuf
    HTTP->>HTTP: 检查 Auth (PromWriteAuthEnabled)
    HTTP->>TR: truncateReader(body, MaxBodySize)
    TR->>PB: buf.ReadFrom(body) → bytes.Buffer
    PB->>PB: snappy.Decode(nil, buf.Bytes())
    PB->>PB: req.Unmarshal(reqBuf) — prompb.WriteRequest

    loop 每个 TimeSeries
        PB->>Conv: 提取 Labels → measurement + tags
        Note over Conv: __name__ label → measurement 名称<br/>其他 label → tag key/value
        PB->>Conv: 提取 Samples → (timestamp, value) 对
        Note over Conv: 每个 Sample 构建一个 models.Point<br/>timestamp: s.Timestamp (毫秒)→纳秒转换 (converters.go:83)
        Conv->>PW: WritePoints(database, rp, points)
    end

    PW-->>HTTP: 成功/错误
    HTTP-->>Prom: 204 / 400 / 500
```

**协议转换细节**:

> **审计校准** (handler.go:1897-2026 + prometheus/converters.go:49-97):
> - 请求体读取走 `truncateReader(body, MaxBodySize)` + `buf.ReadFrom(body)`，
>   **不是** `ioutil.ReadAll`/`io.ReadAll`。
> - Snappy 解压后用 `req.Unmarshal(reqBuf)` (prompb 生成方法，内部等价 proto.Unmarshal)，**不是**裸 `proto.Unmarshal`。
> - Point 转换委托给 `prometheus.WriteRequestToPoints(&req)` (prometheus/converters.go:49)，
>   handler 自身不做 label→tag 循环。
> - 时间戳字段是 `s.Timestamp` (converters.go:83)，**不是** `s.TimestampMs`；
>   转换公式 `time.Unix(0, s.Timestamp*int64(time.Millisecond))`。
> - measurement 默认常量 `measurementName`，只有当 label name == `prometheusNameTag`
>   时才用该 label value 覆盖 measurement。
> - 字段名用常量 `fieldName`。
> - NaN/+Inf/-Inf 会被静默丢弃，仅累计计数后包装成 `DroppedValuesError` 返回。

```go
// services/httpd/handler.go:1897 — servePromWrite (校准版)
func (h *Handler) servePromWrite(w http.ResponseWriter, r *http.Request, user meta.User) {
    // ... (db 校验、auth 校验略)

    body := r.Body
    if h.Config.MaxBodySize > 0 {
        body = truncateReader(body, int64(h.Config.MaxBodySize))   // 请求体截断
    }
    var bs []byte
    if r.ContentLength > 0 {
        if h.Config.MaxBodySize > 0 && r.ContentLength > int64(h.Config.MaxBodySize) {
            h.httpError(w, http.StatusText(413), 413); return
        }
        bs = make([]byte, 0, r.ContentLength)   // 预分配 hint
    }
    buf := bytes.NewBuffer(bs)
    _, err := buf.ReadFrom(body)                // 非 ioutil.ReadAll
    if err == errTruncated { /* 413 */ return }

    reqBuf, err := snappy.Decode(nil, buf.Bytes())

    var req prompb.WriteRequest
    if err := req.Unmarshal(reqBuf); err != nil { /* 400 */ return }  // 非 proto.Unmarshal

    points, err := prometheus.WriteRequestToPoints(&req)              // converters.go:49
    if _, ok := err.(prometheus.DroppedValuesError); !ok && err != nil { /* 400 */ return }

    // ... (consistency 解析略)
    h.PointsWriter.WritePoints(database, rp, consistency, user, points)
}

// prometheus/converters.go:49 — WriteRequestToPoints (关键片段)
func WriteRequestToPoints(req *prompb.WriteRequest) ([]models.Point, error) {
    for _, ts := range req.Timeseries {
        measurement := measurementName
        tags := make(map[string]string, len(ts.Labels))
        for _, l := range ts.Labels {
            tags[l.Name] = l.Value
            if l.Name == prometheusNameTag {   // "__name__" → measurement
                measurement = l.Value
            }
        }
        for _, s := range ts.Samples {
            // NaN/Inf 丢弃并计数...
            t := time.Unix(0, s.Timestamp*int64(time.Millisecond))   // s.Timestamp, 非 s.TimestampMs
            fields := map[string]interface{}{fieldName: s.Value}
            p, _ := models.NewPoint(measurement, models.NewTags(tags), fields, t)
            points = append(points, p)
        }
    }
    // 若有丢弃值，返回 points + DroppedValuesError
}
```

**关键差异**:

| 维度 | Line Protocol 写入 | Prometheus Remote Write |
|------|-------------------|------------------------|
| 数据格式 | 文本 (`cpu,host=web value=3.14`) | Protobuf 二进制 |
| 压缩 | 可选 gzip | Snappy |
| 请求体读取 | `buf.ReadFrom(body)` + `truncateReader` | 同左 (handler 复用同一截断/缓冲路径) |
| 解析器 | `models.ParsePointsWithPrecision` | `req.Unmarshal` → `prometheus.WriteRequestToPoints` |
| measurement 来源 | Line Protocol 中的第一段 | `__name__` label (无则用默认常量) |
| 时间戳精度 | 可配置 (n/u/ms/s/m/h) | 毫秒 (固定)，字段名 `s.Timestamp` |
| 字段名 | Line Protocol 中指定 | 固定常量 `fieldName` |
| 非法值处理 | 解析失败即报错 | NaN/+Inf/-Inf 静默丢弃，返回 `DroppedValuesError` |

### 9.2 servePromRead — Prometheus 读取适配

```mermaid
sequenceDiagram
    participant Prom as Prometheus
    participant HTTP as servePromRead
    participant PB as Protobuf 解码
    participant Store as Store.ReadFilter
    participant RS as ResultSet

    Prom->>HTTP: POST /api/v1/prom/read<br/>Content-Type: application/x-protobuf
    HTTP->>PB: Snappy 解压 → prompb.ReadRequest
    PB->>PB: 解析 Query (时间范围 + matcher)

    PB->>Store: ReadFilter(ctx, ReadFilterRequest)
    Note over Store: 转换 Prometheus matcher 为 influxql.Expr<br/>执行标准查询路径 (Module 09)

    Store->>RS: 遍历 ResultSet
    loop 每个 series
        RS->>PB: 构建 prompb.TimeSeries
        Note over PB: Labels: measurement→__name__<br/>tags → Prometheus labels<br/>Samples: (timestamp_ms, value)
    end

    PB->>HTTP: proto.Marshal(prompb.ReadResponse)
    HTTP->>HTTP: Snappy 压缩
    HTTP-->>Prom: Protobuf 响应
```

**Prometheus 查询转换**:

| Prometheus 概念 | InfluxDB 等价物 |
|-----------------|----------------|
| `matchers` (label 匹配) | `WHERE tag = 'value'` |
| `start` / `end` (时间范围) | `WHERE time >= start AND time < end` |
| `__name__` matcher | `FROM measurement` |
| `=~` (正则匹配) | `tag =~ /regex/` |
| `!=` (不等于) | `tag != 'value'` |

## 10. ServeHTTP 入口与路由分发

### 9.1 请求入口

```go
// services/httpd/handler.go:555 — ServeHTTP
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    atomic.AddInt64(&h.stats.Requests, 1)
    atomic.AddInt64(&h.stats.ActiveRequests, 1)
    defer atomic.AddInt64(&h.stats.ActiveRequests, -1)
    start := time.Now()

    // 添加版本头
    w.Header().Add("X-Influxdb-Version", h.Version)
    w.Header().Add("X-Influxdb-Build", h.BuildType)

    // 路由分发
    if h.Config.AuthEnabled && h.Config.PprofEnabled && h.Config.PprofAuthEnabled {
        // pprof 走统一的中间件链 (带认证)
        h.mux.ServeHTTP(w, r)
    } else if strings.HasPrefix(r.URL.Path, "/debug/pprof") && h.Config.PprofEnabled {
        // pprof 绕过中间件链 (无认证)
        h.handleProfiles(w, r)
    } else if strings.HasPrefix(r.URL.Path, "/debug/vars") {
        h.serveExpvar(w, r)
    } else if strings.HasPrefix(r.URL.Path, "/debug/requests") {
        h.serveDebugRequests(w, r)
    } else {
        h.mux.ServeHTTP(w, r)
    }

    atomic.AddInt64(&h.stats.RequestDuration, time.Since(start).Nanoseconds())
}
```

> **小白解释**: `ServeHTTP` 是所有 HTTP 请求的总入口。
> 它先做一些全局统计，然后根据 URL 路径决定走哪条路由。
> 注意 pprof 有两条路径：如果启用了 pprof 认证，走中间件链；
> 否则直接调用 Go 标准库的 pprof handler，绕过所有中间件。

## 10. 统计计数器

### 10.1 Statistics 结构

```go
// services/httpd/handler.go:421-448 — Statistics (25 fields)
type Statistics struct {
    Requests                         int64  // 总请求数
    CQRequests                       int64  // 连续查询请求数 (handler.go:424)
    QueryRequests                    int64  // 查询请求数
    WriteRequests                    int64  // 写入请求数
    PingRequests                     int64  // Ping 请求数
    StatusRequests                   int64  // Status 请求数 (已废弃)
    WriteRequestBytesReceived        int64  // 写入请求接收字节数
    QueryRequestBytesTransmitted     int64  // 查询响应发送字节数
    PointsWrittenOK                  int64  // 成功写入点数
    PointsWrittenDropped             int64  // 被丢弃的点数
    PointsWrittenFail                int64  // 写入失败的点数
    AuthenticationFailures           int64  // 认证失败次数
    RequestDuration                  int64  // 总请求耗时 (纳秒)
    QueryRequestDuration             int64  // 查询请求耗时 (纳秒)
    WriteRequestDuration             int64  // 写入请求耗时 (纳秒)
    ActiveRequests                   int64  // 当前活跃请求数
    ActiveWriteRequests              int64  // 当前活跃写入请求数
    ClientErrors                     int64  // 4xx 错误数
    ServerErrors                     int64  // 5xx 错误数
    RecoveredPanics                  int64  // 捕获的 panic 数
    PromWriteRequests                int64  // Prometheus 写入请求数
    PromReadRequests                 int64  // Prometheus 读取请求数
    FluxQueryRequests                int64  // Flux 查询请求数
    FluxQueryRequestDuration         int64  // Flux 查询耗时 (纳秒)
    FluxQueryRequestBytesTransmitted int64  // Flux 查询响应字节数 (handler.go:447)
}
```

> **审计校准**:
> - **没有** `ValuesWrittenOK` 字段——该字段在整个 httpd 包内不存在。
> - 新增 `CQRequests` (line 424) 和 `FluxQueryRequestBytesTransmitted` (line 447)。
> - 字段总数 24 → **25**。

所有计数器使用 `atomic.AddInt64` / `atomic.LoadInt64` 操作，保证并发安全。

## 11. 具体案例：完整请求生命周期

### 11.1 写入请求案例

> **场景**: Telegraf 代理每 10 秒向 InfluxDB 写入一次系统指标

```
# Telegraf 配置的输出:
curl -X POST 'http://localhost:8086/write?db=telegraf&rp=autogen&precision=s' \
  -H 'Authorization: Token admin:mysecretpassword' \
  -H 'Content-Type: text/plain' \
  -d 'cpu,host=web01,cpu=cpu0 usage_user=42.1,usage_system=12.3,usage_idle=45.6 1704067200
cpu,host=web01,cpu=cpu1 usage_user=38.7,usage_system=15.1,usage_idle=46.2 1704067200
mem,host=web01 used_percent=72.4,used=61847527424 1704067200'
```

**完整处理链路**:

```
① ServeHTTP 入口
   - stats.Requests++ (原子操作)
   - stats.ActiveRequests++
   - 添加 X-Influxdb-Version, X-Influxdb-Build 头

② recovery 中间件 (最外层)
   - 设置 defer recover() 捕获 panic

③ logging 中间件
   - 记录请求开始时间
   - 包装 responseLogger 捕获状态码

④ requestID 中间件
   - 客户端未传 X-Request-Id
   - 生成 uuid.TimeUUID() → "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
   - 设置 Request-Id 和 X-Request-Id 响应头

⑤ cors 中间件
   - 检测 Origin 头 (Telegraf 无 Origin，跳过)

⑥ SetHeaders 中间件
   - 添加用户自定义响应头 (如有)

⑦ responseWriter 中间件
   - Accept 头为空，使用默认 JSON 格式化器

⑧ writeThrottler 中间件
   - 尝试进入 enqueued 槽: 成功 (当前队列 5/110)
   - 尝试获取 current 槽: 成功 (当前并发 3/10)

⑨ authenticate 中间件
   - AuthEnabled=true, AdminUserExists()=true
   - parseCredentials: Authorization: Token admin:mysecretpassword
   - 解析为 UserAuthentication(username="admin", password="mysecretpassword")
   - MetaClient.Authenticate("admin", "mysecretpassword")
     → bcrypt.CompareHashAndPassword (或 salted SHA-256 缓存命中)
   - 认证成功，user = admin

⑩ serveWriteV1 Handler
   - 解析 precision="s" (秒级)
   - db="telegraf", rp="autogen"

⑪ serveWrite 核心逻辑
   - stats.WriteRequests++
   - stats.ActiveWriteRequests++
   - 检查 database "telegraf" 存在 → MetaClient.Database("telegraf") ✓
   - 检查写入权限 → WriteAuthorizer.AuthorizeWrite("admin", "telegraf") ✓
   - body = truncateReader(r.Body, 25MB)
   - buf.ReadFrom(body) → 3 行 Line Protocol
   - stats.WriteRequestBytesReceived += buf.Len()
   - ParsePointsWithPrecision → 3 个 models.Point
   - consistency = "one" (默认)
   - PointsWriter.WritePoints("telegraf", "autogen", one, user, points)
     → 协调器转发到正确的 shard group
     → TSM1 Engine 写入 (Module 01)
   - 成功 → stats.PointsWrittenOK += 3
   - 返回 204 No Content

⑫ 各中间件后处理 (从内到外)
   - writeThrottler: 释放 current 和 enqueued 槽
   - logging: 记录 CLF 日志行
   - recovery: 无 panic，正常返回

⑬ ServeHTTP 退出
   - stats.ActiveRequests--
   - stats.RequestDuration += time.Since(start)
```

### 11.2 查询请求案例

> **场景**: Grafana 查询最近 1 小时的 CPU 使用率

```
POST /query?db=telegraf&chunked=true&chunk_size=1000&q=SELECT+mean(usage_user)+FROM+cpu+WHERE+time+>+now()-1h+GROUP+BY+time(1m),host
Authorization: Basic Z3JhZmFuYTpncmFmYW5h (grafana:grafana)
```

**处理链路**:

```
① 中间件链 (同写入，但 writeThrottler 跳过非 POST /write)

② authenticate
   - parseCredentials → Basic Auth → (grafana, grafana)
   - MetaClient.Authenticate → 成功

③ serveQuery
   - stats.QueryRequests++
   - qr = "SELECT mean(usage_user) FROM cpu WHERE time > now()-1h GROUP BY time(1m), host"
   - sanitize(r) — 清理参数防止日志泄露
   - influxql.NewParser(qr).ParseQuery() → *influxql.Query
   - QueryAuthorizer.AuthorizeQuery(grafana_user, query, "telegraf")
     → 检查 grafana_user 对 telegraf 数据库的 READ 权限
     → 返回 fineAuthorizer
   - chunked=true, chunk_size=1000
   - ExecutionOptions{Database: "telegraf", ChunkSize: 1000, ReadOnly: true}
   - 设置 closing 通道 (检测客户端断开)
   - QueryExecutor.ExecuteQuery(query, opts, closing) → results channel

④ 结果流式返回 (chunked)
   - 第 1 个 result: series(host=web01) 的 mean(usage_user) 按 1 分钟聚合
     → WriteResponse → Flush → 客户端收到第一批数据
   - 第 2 个 result: series(host=web02) 的 mean(usage_user)
     → WriteResponse → Flush
   - ... 直到 results channel 关闭
```

## 12. 架构设计意图

### 12.1 为什么使用 bmizerany/pat 路由库

| 维度 | pat | 标准库 http.ServeMux | gorilla/mux |
|------|-----|---------------------|-------------|
| **路径匹配** | 方法+模式 | 仅前缀匹配 | 正则表达式 |
| **依赖** | 单文件, 极轻量 | 标准库 | 较重 |
| **中间件** | 不内置 (自行包装) | 不内置 | 内置 |
| **Go 版本** | 兼容旧版 | 随 Go 版本演进 | 需要较新版本 |

InfluxDB 选择 pat 的原因：**轻量、简单、无额外依赖**。
路由表在编译时已知，不需要运行时动态路由能力。

### 12.2 为什么中间件用函数嵌套而非接口链

```go
// 当前实现: 函数嵌套 (装饰器模式)
handler = h.recovery(handler, name)       // 最外层
handler = h.logging(handler, name)
handler = requestID(handler)
handler = cors(handler)
handler = authenticate(hf, h, authEnabled) // 最内层
```

**设计原因**:
- **类型安全**: Go 的 `http.Handler` 接口是强类型的，每层包装都保证类型正确
- **零分配**: 闭包捕获变量，不需要额外的结构体分配
- **简单直观**: 执行顺序与代码阅读顺序一致（从下到上）
- **可选择性组合**: 不同路由可以有不同的中间件组合（如写入端点额外加限流器）

### 12.3 为什么认证在限流器之后

```
请求 → recovery → logging → ... → writeThrottler → authenticate → handler
```

**原因**:
1. **节省认证开销**: 如果请求被限流器拒绝 (503)，不需要执行认证（涉及 bcrypt 或 JWT 验证）
2. **保护认证系统**: 高并发写入时，限流器先过滤掉溢出请求，避免认证系统过载
3. **快速失败**: 限流器的 channel 操作是 O(1) 的，比认证的 bcrypt 快几个数量级

### 12.4 为什么 gzip 是懒初始化

```go
func (w *lazyGzipResponseWriter) WriteHeader(code int) {
    if code == http.StatusOK {
        w.Writer = getGzipWriter(w.Writer)  // 仅 200 时创建
    }
}
```

**原因**:
- 大量请求返回 204 (写入成功)、400 (错误)、401 (未认证) 等非 200 状态码
- 对这些响应进行 gzip 压缩没有意义（响应体很小或为空）
- 懒初始化避免了无谓的 gzip.Writer 分配和 sync.Pool 操作

### 12.5 为什么 Throttler 使用双层 channel

```
enqueued channel (容量 = concurrent + maxEnqueue)
    ↓
current channel (容量 = concurrent)
```

**设计意图**:
- **enqueued**: 防止无限排队，超出容量立即返回 503
- **current**: 控制实际并发处理数，保护下游资源（CPU、内存、磁盘 IO）
- **分离关注点**: 排队容量和并发容量可以独立配置
- **超时机制**: 即使进入了 enqueued 队列，等待太久也会被拒绝

## 13. 架构收益

| 维度 | 收益 | 说明 |
|------|------|------|
| **可扩展性** | 中间件可自由组合 | 不同端点可以有不同的中间件栈 |
| **可测试性** | 依赖注入 (接口) | Handler 的所有依赖都是接口，可 mock |
| **安全性** | 多层防护 | 限流 → 认证 → 授权，层层过滤 |
| **可观测性** | 原子计数器 + CLF 日志 | 24 个统计指标 + 访问日志 + 请求追踪 |
| **兼容性** | V1/V2 API 共存 | bucket2dbrp 适配层，同一写入逻辑 |
| **性能** | sync.Pool gzip 复用 | 减少 gzip.Writer 分配开销 |
| **容错性** | recovery 中间件 | panic 不会导致进程崩溃 |
| **资源保护** | MaxBodySize + Throttler | 防止 OOM 和过载 |
| **协议兼容** | Prometheus Remote Write/Read | 支持 Prometheus 生态 |
| **调试能力** | pprof + expvar + 请求追踪 | 生产环境问题诊断 |

## 14. 潜在隐患与瓶颈

### 14.1 /api/v2/write 未被限流

```go
// handler.go:429 — 仅对 /write 和 /api/v1/prom/write 限流
if r.Method == http.MethodPost {
    switch r.Pattern {
    case "/write", "/api/v1/prom/write":
        handler = h.writeThrottler.Handler(handler)
    }
}
```

`POST /api/v2/write` 走的是 `serveWriteV2` → `serveWrite`，与 V1 走相同的写入逻辑，
但未经过 `writeThrottler`。如果 V2 写入被大量使用，限流保护将失效。

**建议**: 在 switch 中增加 `"/api/v2/write"` case。

### 14.2 认证缓存与 bcrypt 的性能影响

```go
// MetaClient.Authenticate 内部 (来自 Module 07)
// bcrypt.CompareHashAndPassword: ~100ms/op (默认 cost=10)
// salted SHA-256 缓存: ~1μs/op (缓存命中)
```

- **缓存未命中**: bcrypt 验证是 CPU 密集型操作，高并发时可能成为瓶颈
- **缓存命中**: 使用 salted SHA-256 快速路径，性能好 10 万倍
- **风险**: 如果缓存被清除（如密码变更），第一个请求会慢 100ms

### 14.3 CORS 允许任意来源

```go
w.Header().Set("Access-Control-Allow-Origin", origin)  // 直接回显 Origin
```

当前实现将请求的 `Origin` 值直接设为 `Access-Control-Allow-Origin`，
等同于允许任意来源跨域访问。在生产环境中，如果 InfluxDB 暴露在公网，
这可能带来安全风险。

**建议**: 添加可配置的 CORS 白名单。

### 14.4 pprof 端点的默认暴露

```go
// 默认配置
PprofEnabled:     true   // 启用 pprof
PprofAuthEnabled: false  // 不需要认证
```

默认情况下，`/debug/pprof/*` 端点无需认证即可访问。
这些端点暴露了进程的内部状态（goroutine 栈、内存分配、CPU profile），
攻击者可以利用这些信息进行情报收集。

**建议**: 生产环境应设置 `pprof-auth-enabled = true` 或通过防火墙限制 `/debug/*` 路径。

### 14.5 请求体读取到内存

```go
// handler.go:945 — 将整个请求体读入内存
buf := bytes.NewBuffer(bs)
_, err := buf.ReadFrom(body)
```

即使有 `MaxBodySize` (25MB) 限制，每个写入请求仍会分配最多 25MB 的内存。
在高并发场景下（如 100 个并发请求），理论最大内存占用为 2.5GB。

**缓解措施**: Throttler 限制了并发数，但内存占用仍然可观。

### 14.6 CloseNotifier 已废弃

```go
// handler.go:635 — 使用 http.CloseNotifier
if notifier, ok := w.(http.CloseNotifier); ok {
    notify := notifier.CloseNotify()
```

`http.CloseNotifier` 在 Go 1.11+ 中已被标记为 deprecated，
推荐使用 `r.Context().Done()` 替代。当前代码仍使用旧接口，
未来 Go 版本可能移除该接口。

### 14.7 非分块查询的内存放大

```go
// handler.go:747 — 合并同 statement 的结果
lastSeries.Values = append(lastSeries.Values, row.Values...)
```

非分块查询会将所有结果缓冲在内存中。对于大查询（如 `SELECT * FROM cpu`），
这可能导致显著的内存放大。虽然 `MaxRowLimit` 提供了保护，
但默认值为 0（不限制）。

**建议**: 设置合理的 `max-row-limit` 值（如 10000）。

### 14.8 Flux 查询的 Content-Type 处理

```go
// flux.go:34 — 仅识别 "application/vnd.flux"
case "application/vnd.flux":
    d, err := io.ReadAll(r.Body)
    if err != nil {
        return nil, err
    }
    req.Query = string(d)
default:
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        return nil, err
    }
```

如果客户端发送了非标准 Content-Type（如 `text/plain`），会走 default 分支尝试 JSON 解码，
可能返回令人困惑的错误信息。

### 14.9 Route 字段、V2 兼容路由与 Debug 暴露面校准

当前源码中的 `Route` 两个布尔字段不是“是否认证”：

```go
// services/httpd/handler.go
type Route struct {
    Name           string
    Method         string
    Pattern        string
    Gzipped        bool
    LoggingEnabled bool
    HandlerFunc    interface{}
}
```

认证由 handler 签名、`Config.AuthEnabled`、以及 `authWrapper`/特定端点包装决定。
因此路由表应把第 4/5 列理解为 `Gzipped` 和 `LoggingEnabled`，不能直接从布尔值推出认证状态。

```mermaid
flowchart TD
    A["AddRoutes(Route)"] --> B{"Route.Gzipped?"}
    B -->|"是"| C["gzipFilter 包装"]
    B -->|"否"| D["原 handler"]
    C --> E{"LoggingEnabled?"}
    D --> E
    E -->|"是"| F["logging 中间件记录"]
    E -->|"否"| G["跳过访问日志"]
    F --> H["认证由 handler/authWrapper 决定"]
    G --> H
```

源码还注册了一批 V2 兼容路由，不能只列 `/api/v2/write` 和 `/api/v2/query`：

| 路由族 | 示例 | 处理器 |
|---|---|---|
| Delete | `POST /api/v2/delete` | `serveDeleteV2` |
| Buckets | `GET/POST /api/v2/buckets` | bucket/RP 兼容处理器 |
| Bucket by DBRP | `/api/v2/buckets/:dbrp` | bucket/RP 映射处理器 |
| Bucket labels/members/owners | `/api/v2/buckets/:dbrp/labels` 等 | V2 兼容空实现/映射 |
| OPTIONS | `/api/v2/write`, `/api/v2/query` | CORS 预检 |

Debug 路由也要区分：`/debug/vars` 与 `/debug/requests` 由 `ServeHTTP`
按路径前缀直接分发；它们不是普通 pprof 路由，也不完全受 `PprofEnabled`
分支约束。开启 `PprofAuthEnabled` 时会注册认证包装的 debug/pprof 路由。

**案例**: 如果只关闭 pprof 但暴露 HTTP 服务，仍需要单独评估
`/debug/vars` 和 `/debug/requests` 的访问控制，否则可能泄露 expvar 指标和请求追踪信息。

### 14.10 Prometheus Remote Read 边界

`servePromRead` 与普通写入/查询端点的资源边界不同：

```go
// services/httpd/handler.go — servePromRead 关键行为
body, err := io.ReadAll(r.Body)        // 不走 truncateReader/MaxBodySize
ctx := context.Background()            // 不继承客户端取消
resp, err := h.Store.ReadFilter(ctx, req)
```

```mermaid
sequenceDiagram
    participant Client
    participant HTTP as servePromRead
    participant Store

    Client->>HTTP: snappy remote_read body
    HTTP->>HTTP: io.ReadAll(r.Body)
    HTTP->>HTTP: snappy.Decode + proto.Unmarshal
    HTTP->>Store: ReadFilter(context.Background(), req)
    Store-->>HTTP: float cursor results
    HTTP-->>Client: snappy protobuf response
```

**案例**: 客户端上传一个很大的 remote read body 后立刻断开。该路径没有
`MaxBodySize` 截断，也没有把 `r.Context()` 传给 storage read；storage 读取不会因为客户端断开自动取消。

## 15. 关键文件索引

| 文件 | 行数 | 职责 |
|------|------|------|
| `services/httpd/handler.go` | ~3222 | Handler 结构体、路由注册、所有端点处理函数、中间件、认证、限流 |
| `services/httpd/service.go` | 268 | Service 生命周期: NewService, Open, Close, serveTCP, serveUnixSocket |
| `services/httpd/config.go` | 201 | Config 结构体、默认值、StatusFilter、Diagnostics |
| `services/httpd/response_writer.go` | 385 | ResponseWriter 接口、JSON/CSV/msgpack 格式化器、内容协商 |
| `services/httpd/gzip.go` | 105 | gzipFilter 中间件、lazyGzipResponseWriter、sync.Pool 复用 |
| `services/httpd/io.go` | 45 | truncateReader: 请求体大小限制 |
| `services/httpd/flux.go` | 53 | Controller 接口、decodeQueryRequest (Flux 请求解码) |
| `services/httpd/requests.go` | ~150 | RequestTracker: 请求追踪 (debug) |
| `services/httpd/response_logger.go` | ~80 | responseLogger: 捕获响应状态码和字节数 |
| `services/httpd/handler_test.go` | ~2000+ | Handler 单元测试 |

## 16. 与其他模块的关系

```mermaid
flowchart LR
    subgraph "HTTP 服务层 (本模块)"
        H["Handler"]
        S["Service"]
        TH["Throttler"]
        AUTH["authenticate"]
    end

    subgraph "元数据 (Module 07)"
        MC["MetaClient"]
        META["Meta Store"]
    end

    subgraph "查询引擎 (Module 03)"
        QE["QueryExecutor"]
        COMP["Compiler"]
    end

    subgraph "写入引擎 (Module 01)"
        PW["PointsWriter"]
        COORD["Coordinator"]
    end

    subgraph "存储读取 (Module 09)"
        STORE["Store"]
    end

    subgraph "Flux 引擎"
        CTRL["Controller"]
    end

    H --> MC
    H --> QE
    H --> PW
    H --> STORE
    H --> CTRL
    AUTH --> MC
    QE --> COMP
    PW --> COORD
    MC --> META
```

**依赖方向**: HTTP 服务层是系统的**最外层**，依赖所有内部模块，但内部模块不依赖 HTTP 层。
这符合**依赖倒置原则**——HTTP 层通过接口调用内部模块，内部模块不需要知道 HTTP 的存在。
