# 附录 A：构建、CI、打包、发布与分发（v1.8.10 → v1.12.3）

> 本文件是 [`07_build_perf_diff.md`](07_build_perf_diff.md) 的附录 A，保留 §3.1–§3.8 与 §6.1–§6.9 的全文。
> **基线**
> - OLD（旧）: `v1.8.10` = commit `688e697c51`（2021-10-11）
> - NEW（新）: `v1.12.3` = commit `f3d5cb68ab`（本仓库 `docs/specs/` 的代码基准）
> - 区间: `git rev-list --count v1.8.10..f3d5cb68ab` = **527 commits**；`git diff --shortstat` = **602 files changed, 79,310 insertions(+), 45,239 deletions(-)**。当前工作树只比 `f3d5cb68ab` 多若干 docs 提交（数量随文档演进），代码相同。
> - 本文覆盖: `go.mod` / `go.sum`、`.circleci/*`、`scripts/ci/*`、`releng/*`、`Dockerfile`、`Makefile`、`kit/*`、`pkg/*`、`monitor/*`、`tests/*`、`stress/*`，以及**构建执行 / CI 编排 / 打包 / 发布 / 分发安装**、**测试基础设施（含性能回归缺口）**和**跨子系统性能优化**（compaction、TSI 缓存、purger、query control、array cursor、TSM 文件名缓存等）。各子系统的深度细节请看对应主文档，本文只给机制与 commit。
> - 旧版本证据统一来自 `git show v1.8.10:<path>`；新版本证据来自当前工作树（代码等价于 `f3d5cb68ab`）。

> **范围与关系**
> - **收录**：§3.1–§3.8（Go/toolchain、依赖、Rust/CGO、构建执行、CI、打包、发布、分发安装）与 §6.1–§6.9（构建、流水线、安装生命周期、测试基础设施与工具链的长案例）**全文**。
> - **与主文档**：主文档保留同编号标题与 1–2 句结论摘要；本附录保留原始代码块、表格与全部证据，编号与主文档一致。
> - **跨文档引用**：`07 §3.x` 仍指向主文档的同编号标题；需要完整证据时从主文档对应小节或本附录顶部进入。§4.6 build/CI 的逐条推导按拆分约定放在 [附录 B](07b_perf_appendix.md)。
> - **基线**：OLD = `v1.8.10`（`688e697c51`，2021-10-11），NEW = `v1.12.3`（`f3d5cb68ab`）；旧版证据来自 `git show v1.8.10:<path>`，新版来自当前工作树（代码等价于 `f3d5cb68ab`）。
> - **返回**：[主文档](07_build_perf_diff.md) · [附录 B：性能、可观测性与测试](07b_perf_appendix.md)

## 3. 逐项差异

### 3.1 Go 版本与 toolchain

旧版 `go.mod` 只有一行版本声明，且停在 Go 1.13：

```go
// v1.8.10:go.mod:1-5 — 模块声明
module github.com/influxdata/influxdb

go 1.13

require (
```

新版同时使用 `go` 与 `toolchain` 两条指令：

```go
// v1.12.3:go.mod:1-7 — 模块声明
module github.com/influxdata/influxdb

go 1.24.0

toolchain go1.24.13

require (
```

**为什么要分开写**：`go` 指令决定**语言语义**（例如 Go 1.22 的循环变量作用域、1.24 的泛型别名），`toolchain` 指令决定**实际执行构建的工具链**。把 `go` 固定在 `1.24.0` 而 `toolchain` 指向 `go1.24.13`，意味着：语言特性按 1.24.0 判定，但补丁级安全修复会自动跟进到 1.24.13，避免每个补丁版本都改一次语言版本。

演进路径（均可从 `git show <commit>:go.mod | sed -n '3p'` 复核）：

| 时间 | commit | `go` 指令 | CI 容器 tag |
|------|--------|-----------|-------------|
| 2021-10-11 | `v1.8.10` | `go 1.13` | 无（machine executor + Dockerfile） |
| 2021-11-24 | `08a4a710eb` | `go 1.17` | — |
| 2022-03-31 | `0574163566` | `go 1.18` | `go1.18-0b1bcef254c3...`（带 commit 后缀） |
| 2022-11-28 | `e484c4d871` | `go 1.19` | `go1.19.3-7bc83382e2fd...` |
| 2024-09-05 | `0582cf4148` | `go 1.21` | `go1.21.12-latest` |
| 2024-09-18 | `599e4f7be3` | `go 1.22` | `go1.22.7-latest` |
| 2025-01 | `edf5ff20f6` | `go 1.23` | `go1.23.5-latest` |
| 2025-04-22 | `f8635640cc` | `go 1.23.0` + `toolchain go1.23.8` | `go1.23.8-latest` |
| 2025-09-18 | `0a4c903b47` | `go 1.23.0` + `toolchain go1.23.8` | `go1.23.12-latest` |
| 2025-10-06 | `55b7fb5e1b` | `go 1.23.0` + `toolchain go1.23.12` | `go1.23.12-latest` |
| 2025-10-31 | `542ce7f684` | `go 1.24.0` + `toolchain go1.24.9` | `go1.24.9-latest` |
| 2026-01-06 | `56a3f970e4` | 未改 go.mod（仅换 CI 镜像，仍是 `toolchain go1.24.9`） | `go1.24.11-latest` |
| 2026-01-07 | `1a6880b283` | `go 1.24.0` + `toolchain go1.24.11` | `go1.24.11-latest` |
| 2026-02-09 | `d65673b1f5` | `go 1.24.0` + `toolchain go1.24.13` | `go1.24.13-latest` |

注意两点容易踩的细节：

1. **`f8635640cc` 是第一次引入 `toolchain` 指令**，同一提交里还把 `go 1.23` 改成了 `go 1.23.0`（补零）。这是提交作者的选择：`go 1.23` 与 `go 1.23.0` 都是合法写法，工具链不会自动补零。
2. **CI 镜像 tag 与 `go.mod` 的 `toolchain` 不是严格同步的，多数情况下“CI 先动、go.mod 随后补齐”**：`0a4c903b47` 先把 CI 拉到 1.23.12，18 天后 `55b7fb5e1b` 把 go.mod 的 toolchain 从 1.23.8 补到 1.23.12；`56a3f970e4`（"update go to 1.24.11"）只改 `.circleci/config.yml`、没有动 `go.mod`，次日 `1a6880b283` 才把 go.mod 从 1.24.9 补到 1.24.11；`542ce7f684`、`d65673b1f5` 则在同一提交里把 CI 与 go.mod 一起升级。也就是说存在过“CI 用 1.24.11 编译、开发者本地仍是 1.24.9”的窗口。

### 3.2 依赖规模与关键依赖

```bash
# 复核命令
git show v1.8.10:go.mod | grep -cE '^\s+[a-zA-Z0-9]'        # 51
grep -cE '^\s+[a-zA-Z0-9]' go.mod                            # 181
git show v1.8.10:go.mod | grep -E '^\s+\S' | grep -vc '// indirect'  # 39 直接
grep -E '^\s+\S' go.mod | grep -vc '// indirect'             # 50 直接
git show v1.8.10:go.sum | wc -l                              # 493
wc -l go.sum                                                 # 1684
```

| 依赖 | v1.8.10 | v1.12.3 | 备注 |
|------|---------|---------|------|
| `github.com/influxdata/flux` | `v0.65.1` | `v0.196.1` | 提交 `03b6ed2bed`；一次提交改动 `go.mod` 146 行、`go.sum` 376 行 |
| `github.com/influxdata/influxql` | `v1.1.1-0.20200828144457-65d3ef77d385` | `v1.4.1` | 路径上经过 `v1.2.0`（`8648298012`）→ `v1.4.0` → `v1.4.1`（`77d6f20894`） |
| `github.com/golang-jwt/jwt/v4` | 无（用 `dgrijalva/jwt-go/v4 v4.0.0-preview1`） | `v4.5.2` | 提交 `f8635640cc`；同提交把 `golang.org/x/net` 拉到 `v0.38.0`（该提交时；最终 v1.12.3 为 `v0.46.0`，`go.mod:181`） |
| `github.com/cespare/xxhash/v2` | `cespare/xxhash v1.1.0` | `v2.3.0` | 提交 `45a8227ad6` |
| `github.com/prometheus/client_golang` | `v1.0.0` | `v1.19.1` | 配套 `client_model v0.6.1` / `common v0.53.0` / `procfs v0.15.0` |
| `go.uber.org/zap` | `v1.9.1` | `v1.16.0` | 见 §3.9.5 的动态日志级别 |
| `github.com/stretchr/testify` | `v1.7.0` | `v1.9.0` | 配套 `go-cmp v0.4.0 → v0.6.0` |
| `github.com/apache/arrow/go/arrow` | `v0.0.0-20191024131854-af6fa24be0db` | 旧路径升到 `v0.0.0-20211112161151-bc219186db40`（`go.mod:10`）；另间接引入 `arrow/go/v7 v7.0.1`、`arrow/go/v15 v15.0.2` | **同一份依赖图里同时存在三代 arrow** |
| `golang.org/x/oauth2` | 间接伪版本（`go.sum` 里有 6 行） | `v0.27.0`（间接） | 随 Flux 的云存储后端引入；新 `go.mod:182` |
| `github.com/klauspost/compress` | `v1.4.0`（间接） | `v1.17.8`（间接） | 压缩库跨 13 个小版本 |
| `google.golang.org/protobuf` | 无（用 `github.com/gogo/protobuf v1.3.2` + `protoc-gen-gogo/gogofaster`） | `v1.34.1`（直接，`go.mod:57`）；gogo 降为 `go.mod:126` 的 `// indirect` | 生成器改为官方 `protoc-gen-go`，13 个 `.pb.go` 重新生成（+9148/−15082）；wire 兼容，见 §3.11.1 |

**依赖膨胀的来源可以直接归因到两个上游**：

- Flux `v0.65.1 → v0.196.1` 把一整套 SQL/对象存储连接器带了进来：`cloud.google.com/go/bigquery`、`github.com/Azure/azure-sdk-for-go/sdk/storage/azblob`、`github.com/aws/aws-sdk-go-v2/service/s3`、`github.com/influxdata/gosnowflake`、`github.com/SAP/go-hdb`、`github.com/vertica/vertica-sql-go`、`github.com/segmentio/kafka-go`。这些在 `go.mod` 里全是 `// indirect`，但是否被链接进 `influxd` 取决于是否被引用（Go 的 linker 会裁掉未使用的包），而 `go.sum` 与构建缓存仍需完整下载。
- 云 SDK 又各自拖来 `google.golang.org/api v0.183.0`、`google.golang.org/grpc v1.64.1`、`go.opentelemetry.io/otel v1.27.0` 等。这就是 12 个间接依赖 → 131 个间接依赖的主因。

#### 3.2.1 Flux 组件从 `arrow/array` 迁到 `flux/array`

这是 readme §3.5 指向 07、但旧版 07 没有展开的一条依赖变化。v1.8.10 的 Flux 读取侧直接使用 Apache Arrow 的数组类型；v1.12.3 改为 Flux 自己封装的 `flux/array`：

```go
// v1.8.10:storage/reads/flux_table.go:7
	"github.com/apache/arrow/go/arrow/array"
// v1.8.10:storage/reads/flux_tags_cache.go:7
	"github.com/apache/arrow/go/arrow/array"
```

```go
// v1.12.3:storage/flux/table.go:10、tags_cache.go:8、window.go:10、table.gen.go:15
	"github.com/influxdata/flux/array"
```

迁移提交 `8fae484a66`（"feat: update flux components to use flux/array instead of arrow/array (#21979)"，2021-07-30，v1.10.0 进入）改了 11 个文件：`storage/flux/table.go`、`table.gen.go(.tmpl)`、`tags_cache.go`、`tags_cache_test.go`、`window.go`、`storage/reads/types.tmpldata`、`flux/stdlib/influxdata/influxdb/to.go`、`cmd/influx/cli/cli.go`、`go.mod`/`go.sum`。它是一次**类型接口迁移**，不是性能优化：`flux/array` 包装 Arrow 数组，同时给 `*array.String` 等类型留出内部优化空间；`go.mod` 仍保留旧路径 `github.com/apache/arrow/go/arrow v0.0.0-20211112161151-bc219186db40`（Flux 的其他包还在用），另引入 `arrow/go/v7`、`arrow/go/v15`——同一份依赖图里三代 arrow 并存（§1 #6、§3.2 表）。v1.8.10 的对应文件在 `storage/reads/` 下（`flux_table.go`、`flux_table.gen.go`、`flux_tags_cache.go`），v1.12.3 已迁到 `storage/flux/` 并改名（`table.go`、`table.gen.go`、`tags_cache.go`）。影响面是 Flux 查询的 table/tag 缓存与 window 读取；InfluxQL 不经过这些文件。

### 3.3 Rust、CGO 与交叉编译

新版 Linux 产物是 **static-pie + external linkmode + Rust 静态库**，因此构建必须能找到 Rust 的 `libunwind`：

```yaml
# v1.12.3:.circleci/config.yml:90-110 — rustlib 与 linker flags
                # rustlib
                if [[ "${GOOS}" == linux ]] ; then
                  case "${GOARCH}" in
                    amd64)
                      rustlib="$(find_rustlib_path x86_64-unknown-linux-musl)"
                      ;;
                    arm64)
                      rustlib="$(find_rustlib_path aarch64-unknown-linux-musl)"
                      ;;
                  esac
                fi

                # linker flags
                case "${GOOS}" in
                  darwin|windows)
                    ldflags='-s -w'
                    ;;
                  linux)
                    ldflags='-s -w -linkmode=external -extld='"${CC}"' -extldflags "-fno-PIC -static-pie -Wl,-z,stack-size=8388608 -L'"${rustlib}"' -Wl,-lunwind"'
                    ;;
                esac
```

这段的 rustlib 与缓存键来自提交 `6a3b02cac4`（"fix: use new cache, add rustlib path (#27234) (#27236)"），同提交把缓存键从 `influxdb-cache-v1-*` 换成 `influxdb-cache-v2-*`；但 `-static-pie` 来自更早的 `a52893479e`（#23642），`-Wl,-z,stack-size=8388608` 来自 `004438e91f`（#23043），`6a3b02cac4` 只加了 rustlib 路径与缓存键。`xcc` 和 `find_rustlib_path` 都是 cross-builder 镜像注入的 shell 函数，**仓库内没有定义**（`grep -rn "find_rustlib_path" .circleci/config.yml` 只命中 `:94`、`:97` 两处调用）。

Rust 工具链版本不是硬编码，而是从 Flux 模块的 `Dockerfile_build` 里反推：

```bash
# v1.12.3:scripts/ci/install-rust.sh:5-7,16-29
flux_dir=$(go list -m -f '{{.Dir}}' github.com/influxdata/flux)
FLUX_RUST_VERSION=$(cat ${flux_dir}/Dockerfile_build | grep 'FROM rust:' | cut -d ' ' -f2 | cut -d ':' -f2)
RUST_LATEST_VERSION=${FLUX_RUST_VERSION:-1.53}
...
RUSTUP_LATEST_VERSION=1.24.2
OUR_RUSTUP_SHA="40229562d4fa60e102646644e473575bae22ff56c3a706898a47d7241c9c031e"
...
echo "${OUR_RUSTUP_SHA} rustup-init.sh" | sha256sum --check -- \
    || { echo "Checksum problem!"; exit 1; }
```

**但 CI 不调用这个脚本**：仓库内没有 `install-rust` 调用点，`.circleci/config.yml` 的 `build_binaries` 直接用 cross-builder 镜像，Rust 版本由镜像 tag 决定。因此“Flux 版本间接钉住 Rust”**仅当重建 cross-builder 镜像或手动执行 `install-rust.sh` 时成立**：此时 `grep 'FROM rust:'` 的结果决定 Rust 版本，升级 Flux 会改变该版本；脚本里 rustup 的 SHA256 是人工审计后硬编码的，两者需要人工同步；CI 正常路径由镜像 tag 决定，不受 Flux 版本影响。`RUST_LATEST_VERSION` 的兜底值是 `1.53`（2021 年 6 月的版本），执行脚本时如果 Flux 的 `Dockerfile_build` 格式变化导致 grep 失败，会静默回落到一个极旧的 Rust。

对比旧版：`v1.8.10` 的构建容器是 `Dockerfile_build_ubuntu64`（`FROM ubuntu:focal` + `ruby` + `fpm` + `python2.7`（`build.py` 运行时）+ `python3`），链路上**没有 Rust 相关步骤**，`scripts/ci/` 下也没有 `install-rust.sh`——`git ls-tree v1.8.10 -- scripts/ci/install-rust.sh` 无输出，`git cat-file -e` 对 `v1.8.10` 中的 `scripts/ci/install-rust.sh` 返回非 0（该版本无此文件），与引文一致。

### 3.4 构建执行：`build()` / `archive()`

> 本节回答“源码怎样变成四个平台的归档”。它不跑测试、不签名、不发布；CI 编排见 §3.5，打包见 §3.6，发布见 §3.7，安装见 §3.8。六要素：角色 / 结构 / 流程 / 代码 / 案例 / 风险。

**角色**：在 `build_binaries` job 内完成版本注入、man page/源码包生成、4 个目标 × 3 条命令的编译与归档，并把 Go/Rust/构建缓存保存给后续 job。

**结构**：

- 入口 job：`build_binaries`（`.circleci/config.yml:17-191`），docker executor 直接使用 `quay.io/influxdb/cross-builder:<< pipeline.parameters.cross-container-tag >>`（默认 `go1.24.13-latest`，`:10`）。
- 版本注入：`PREFIX=1.x .circleci/scripts/get-version`（`:29-32`）导出 `VERSION/MAJOR/MINOR/PATCH/RELEASE`；`go build -ldflags` 写入 `main.version`、`main.branch`、`main.commit`（`:120-127`）。
- 源码包：写 `.metadata.json`（version/branch/sha），预生成 man page（`(cd man && make build && gzip -9 ./*.1)`），再 `tar` 成 `tarball/influxdb-src-${CIRCLE_SHA1}.tar.gz`（`:33-56`）。
- 缓存：`restore_cache` 只匹配精确键 `influxdb-cache-v2-{{ checksum "go.mod" }}`（`:24-28`）；`save_cache` 保存 `/go/pkg/mod`、`/root/.cargo`、`/root/.cache/go-build`（`:186-191`）。提交 `6a3b02cac4` 把键从 v1 换成 v2。
- 目标矩阵：darwin/amd64、windows/amd64、linux/amd64、linux/arm64（`:149-179`）。旧版是 linux amd64/i386/armhf/arm64/armel/static_i386/static_amd64 + darwin/amd64 + windows/amd64（`v1.8.10:build.py:97-101`）。
- `build()` 对 `influx`、`influx_inspect`、`influxd` 三条命令循环（`:64-130`），按 `GOOS` 选 tags/buildmode/ldflags；linux 额外取 `rustlib` 并加 `-linkmode=external -static-pie -Wl,-lunwind`（`:90-110`，细节见 §3.3）。
- `archive()` 把三条命令打成 `artifacts/influxdb_bin_${GOOS}_${GOARCH}-${CIRCLE_SHA1}.tar.gz`，同时生成 `.md5`、`.sha256`（`:132-147`）；`persist_to_workspace` 传 `artifacts` 与 `tarball`。

**流程**：

1. `checkout` → `restore_cache`（精确命中的 v2 缓存；没有就全量下载）。
2. `get-version`：HEAD 上有 semver tag 时 `VERSION=<tag 去掉 v>`；否则 `VERSION=1.x-<short sha>`（`.circleci/scripts/get-version:25-47`）。
3. 生成 `.metadata.json`、man page 与源码 tarball。
4. 依次设置 4 个 `(GOOS, GOARCH, CC)` 组合，`CGO_ENABLED=1`，对 3 条命令执行 `go build`。
5. `archive()` 归档并写校验和；`persist_to_workspace` + `save_cache`。

**代码**：

```yaml
# v1.12.3:.circleci/config.yml:64-69,132-147（节选）
            function build() {
              for command in \
                influx \
                influx_inspect \
                influxd
              do
...
            archive() {
              target="artifacts/influxdb_bin_${GOOS}_${GOARCH}-${CIRCLE_SHA1}.tar.gz"
              if [[ "${GOOS}" = windows ]] ; then
                tar -czf "${target}" -C "bins/${GOOS}-${GOARCH}" \
                  influx.exe influx_inspect.exe influxd.exe
              else
                tar -czf "${target}" -C "bins/${GOOS}-${GOARCH}" \
                  influx influx_inspect influxd
              fi
              md5sum    "${target}" > "${target}.md5"
              sha256sum "${target}" > "${target}.sha256"
            }
```

**案例**：§6.1 用同一个 `influxd` 对比两版从源码到归档的完整路径。

**风险**：

1. `cross-builder:...-latest` 是可变 tag（`:10`），构建环境不可复现；只有 packager 镜像按 digest 固定（§3.6）。
2. `xcc`、`find_rustlib_path` 不在仓库内，完全由镜像注入；脱离镜像自建 CI 要复刻 musl/Rust 工具链（§3.3、§7 风险 2）。
3. 产物收窄不可逆：i386/armhf/armel/static_* 不再产出。
4. 根 `Dockerfile` 与 `appveyor.yml` 仍是死代码，不能作为 1.12.3 的构建入口（§3.11.3）。

#### 3.4.1 产物清单：5 个命令 → 3 个命令

| 产物 | v1.8.10 | v1.12.3 | 证据/影响 |
|---|---|---|---|
| `influx` | 构建 | 构建 | `v1.8.10:build.py:89-95`；`.circleci/config.yml:64-69` |
| `influxd` | 构建 | 构建 | 同上 |
| `influx_inspect` | 构建 | 构建 | 同上 |
| `influx_tools` | 构建并入包/workspace | **不再产出**；源码仍在 `cmd/influx_tools/*` | `v1.8.10:build.py:94`；旧 CI persists `packages/influx_tools`（`v1.8.10:.circleci/config.yml:55-59`）；新版 `build()` 只列 3 条；旧 perf harness 用它 `compact-shard`（`v1.8.10:scripts/ci/run_perftest.sh:155`） |
| `influx_stress` | 构建 | **源码与产物均删除** | `v1.8.10:build.py:92`；`stress/` 63 文件 → 0（提交 `6b8ec8cfe0`，§3.11.3） |
| man page | 9 个（含 `influx_stress.1`） | 8 个 | `v1.8.10:man/Makefile:5-14`；`.circleci/packages/config.yaml:51-73` |
| deb/rpm/tar/zip | fpm 自建 | `packager` 生成 | 见 §3.6 |

下游影响：旧 `perf_test` 依赖 `influx_tools` 制品（`scripts/ci/perf_test.sh:54-59` 上传到 `s3://perftest-binaries-influxdb`）；新版既不出该制品，也不再跑该性能 job（§3.10.5）。依赖 `influx_tools`/`influx_stress` 的外部流程必须自行从源码构建或改用其他工具。

#### 3.4.2 构建性能相关变化（归入 §4.6）

- 缓存键从无到 `influxdb-cache-v2-{{ checksum "go.mod" }}`，保存 module/cargo/go-build 三层缓存（`.circleci/config.yml:24-28,186-191`）。
- 去掉旧版的嵌套 Docker：`v1.8.10:test.sh:87-93` 每个测试容器先 `docker build -f Dockerfile_build_*`，再在容器内跑 `build.py`；新版直接跑在 cross-builder 容器里（`.circleci/config.yml:17-19`）。
- 目标矩阵从 9 个（7 linux + darwin + windows）收到 4 个（`:149-179`）。
- man page 在 `build_binaries` 与 `build_packages` 各生成一次（`:44-46`、`:203-206`），共 2 次 `make build` + `gzip`，是可见的重复工作；相对编译的 wall time 占比未测，CI 不记录分段耗时（§4.6）；当前只能确认重复 1 次。
- **没有构建耗时基准**：CI 配置里没有记录 wall time，仓库里也没有构建基准脚本；§4.6 只能给机制，不能给“快了多少”。

### 3.5 CI 编排

**角色**：决定 push / tag / 定时三类流水线各跑哪些 job、谁依赖谁、谁能阻断发布。

**结构**：

- 3 条 workflow：`release`、`on_push`、`nightly`（`.circleci/config.yml:456-567`）。
- 常驻 job：`build_binaries`、`build_packages`、`sign_packages`、`test_pkgs_64bit`、`changelog`、`publish_changelog`、`publish_packages`、`slack`、`static_code_checks`、`fluxtest`、`unit_test`（`inmem`/`tsi1`/`race` 三个实例）。
- 执行器：cross-builder docker（构建/静态检查/单测/Flux）、`ubuntu-2204` machine（安装测试）、`rsign`（签名）、`changelogger`（changelog）、`cimg/python:3.6`（S3 上传）、`ci-slack`（通知）。
- 第二套 CI：`.github/workflows/semantic.yml`（PR 标题/提交语义检查），见 §3.5.2。

**流程**：

| workflow | 触发 | job 依赖与门禁 | 产物去向 |
|---|---|---|---|
| `release` | tag，且匹配 `release_filter` 的 `^v(\d+)(?:\.(\d+))?(?:\.(\d+))?$`（`:445-454`） | `build_binaries → build_packages → sign_packages → publish_packages → slack`；`test_pkgs_64bit` 与 `sign_packages` 都只 `requires build_packages`，彼此并行；`changelog → publish_changelog`；`static_code_checks`/`fluxtest`/`unit_test*` 与主链并行且不 gate 发布 | 二进制 tar、deb/rpm/tar/zip、签名、digest、S3 `releases/<tag>/`、`CHANGELOG.<tag>.md` |
| `on_push` | 分支 push（无 tag 过滤） | `build_binaries → build_packages → test_pkgs_64bit`；`static_code_checks`/`fluxtest`/`unit_test*` 并行；**无 sign/publish** | 只上传 workspace/artifacts，不发布 |
| `nightly` | `scheduled_pipeline` 且 `pipeline.parameters.workflow=nightly`（`:529-540`） | `changelog`/`static_code_checks`/`fluxtest`/`unit_test*` 先跑；`build_binaries` requires 这 6 个全部通过；再 `build_packages → test_pkgs_64bit`；**无 sign/publish** | 只产出 workspace/artifacts |

关键门禁差异（源码事实，不是建议）：

- `publish_packages` 只 `requires sign_packages`（`:483-486`）；`test_pkgs_64bit` 是并行 sibling（`:472-475`）。**安装测试失败不会阻断发布**。
- `static_code_checks`/`fluxtest`/`unit_test*` 在 release workflow 里没有作为 `publish_packages` 的前置（`:491-506`）；它们失败时 tag 流水线整体会红，但 S3 上传不依赖它们。
- `nightly` 的 `build_binaries` 反而把它们作为前置（`:554-561`），这是 nightly 与 release 门禁语义不一致的地方。
- `publish_changelog` job 内部定义了 `workflow=nightly` 的 S3 分支（`:404-413`），但 `nightly` workflow 的 jobs 列表没有实例化 `publish_changelog`（`:529-567`）；所以 nightly 实际不会上传 CHANGELOG。

**代码**：

```yaml
# v1.12.3:.circleci/config.yml:458-490（节选）
  release:
    when:
      equal: [ << pipeline.parameters.workflow >>, build ]
    jobs:
      - build_binaries:
          <<: *release_filter
      - build_packages:
          <<: *release_filter
          requires:
            - build_binaries
      - sign_packages:
          <<: *release_filter
          requires:
            - build_packages
      - test_pkgs_64bit:
          <<: *release_filter
          requires:
            - build_packages
      - changelog:
          <<: *release_filter
      - publish_changelog:
          <<: *release_filter
          workflow: release
          requires:
            - changelog
      - publish_packages:
          <<: *release_filter
          requires:
            - sign_packages
      - slack:
          <<: *release_filter
          requires:
            - publish_packages
```

```yaml
# v1.12.3:.circleci/config.yml:554-567（nightly 的依赖链）
      - build_binaries:
          requires:
            - changelog
            - static_code_checks
            - fluxtest
            - unit_test_inmem
            - unit_test_race
            - unit_test_tsi1
      - build_packages:
          requires:
            - build_binaries
      - test_pkgs_64bit:
          requires:
            - build_packages
```

**旧版对照**：v1.8.10 只有 `on_push` + 两个 AWS 清理 workflow（`aws_destroy_daily`/`aws_destroy_callback`，`v1.8.10:.circleci/config.yml:187-225`），没有 release/nightly/changelog/semantic。旧 `build_and_test` 是 `machine` executor + 4 个并行容器 + `./test.sh <idx>`（`:24-40`），测试与打包都在同一个 job 里；`pkg_run_test` 和 `perf_test` 是另外两个 job（`:61-134`），后者见 §3.10.5。

**案例**：§6.2 用一次 tag、一次 branch push、一次 nightly 演示三条流水线各跑到哪里。

**风险**：见 §7 模块 × 风险矩阵（CI 行）与风险 5/11/14/15。

#### 3.5.1 静态检查与代码生成闸门

`static_code_checks` 三件套（`.circleci/config.yml:281-290`）：

```bash
./checkfmt.sh     # goimports -l -d，跳过 // Code generated 文件（checkfmt.sh:3-24）
./generate.sh     # go generate ./...，前后各查一次 git status（generate.sh:8-21）
go vet ./...
```

旧版在 `build.py` 的 amd64 测试路径上做同样的事：`go fmt ./...` + `./generate.sh` + `go vet ./...`（`v1.8.10:build.py:190-209`，`run_gen = args.arch == "amd64"`）。新版拆成独立 job，并把格式检查从 `go fmt` 换成 `goimports`。这条机制抓的是“改了 `.tmpl` 忘了重新生成 `.gen.go`”：`tsdb/engine/tsm1/*.gen.go`、`storage/flux/table.gen.go` 都靠它。

#### 3.5.2 第二套 CI：GitHub Actions `semantic.yml`

v1.8.10 没有这个文件（`git cat-file -e v1.8.10:.github/workflows/semantic.yml` 失败）；新版在 PR 上做标题/提交语义检查：

```yaml
# .github/workflows/semantic.yml:1-10
name: "Semantic PR and Commit Messages"
on:
  pull_request:
    types: [opened, reopened, synchronize, edited]
jobs:
  semantic:
    uses: influxdata/validate-semantic-github-messages/.github/workflows/semantic.yml@main
```

引入提交 `8f78c4bb02`（"chore: backport semantic PR and commit message checks (#23306)"，2022-04-27）。角色是**提交信息规范**，不属于 CircleCI 的构建/发布门禁；风险是复用 workflow 指向 `@main` 可变分支，上游改动会在无本仓库提交的情况下改变检查行为（§7 风险 11）。

### 3.6 打包

**角色**：把 §3.4 的 4 个二进制归档加上 man page、示例配置、systemd unit 与 control scripts，打成 deb/rpm/tar.gz/zip。

**结构**：

- job `build_packages`（`.circleci/config.yml:193-212`），镜像按 digest 固定：`ci-packager-next@sha256:c92ea43b5529b9c4f93478c26d128b3a5aa811559e1fa634cdb476241f8d7620`（`:195`）。
- 步骤：`( cd man ; make build ; gzip -9 ./*.1 )` → `packager .circleci/packages/config.yaml`（`:203-206`）。
- 清单 `.circleci/packages/config.yaml`：
  - `version`：tag 匹配 `^v[0-9]+.[0-9]+.[0-9]+` 时取 `CIRCLE_TAG[1:]`，否则 `1.x-{{env.CIRCLE_SHA1[:8]}}`（`:1-6`）。
  - `sources`：windows/amd64、darwin/amd64、linux/amd64、linux/arm64 四个 tar.gz（`:8-27`）。
  - `packages.influxdb.binaries`：`influx`、`influx_inspect`、`influxd`（+ Windows `.exe`）（`:38-46`）。
  - `extras`：`etc/config.sample.toml` → `etc/influxdb/influxdb.conf`，8 个 man page（`:47-73`）。
  - `perm_overrides`：`init.sh`/`influxd-systemd-start.sh` 0755，unit 0644（`:74-88`）。
  - `rpm_attributes`：`/var/log/influxdb`、`/var/lib/influxdb` 归 `influxdb:influxdb` 750（`:89-91`）。
  - `deb/rpm.recommends`：`influxdata-archive-keyring`（`:93-98`）。
- 包内文件树：`.circleci/packages/influxdb/fs/`（unit、init.sh、start 脚本、config、keepdir）与 `control/`（pre-install、post-install、post-uninstall）。

**流程**：`build_packages` 只 `requires build_binaries`；产出 `packages/` 后 `persist_to_workspace`。release workflow 随后并行进入 `sign_packages` 与 `test_pkgs_64bit`；`publish_packages` 只等签名。

**代码**：

```yaml
# v1.12.3:.circleci/packages/config.yaml:1-27,47-51,89-98（节选）
version:
  - match: '^v[0-9]+.[0-9]+.[0-9]+'
    value: '{{env.CIRCLE_TAG[1:]}}'
  - match: '.*'
    value: '1.x-{{env.CIRCLE_SHA1[:8]}}'
sources:
  - binary: /tmp/workspace/artifacts/influxdb_bin_linux_amd64-*.tar.gz
    target: packages/
    arch:   amd64
    plat:   linux
...
    extras:
      - source: etc/config.sample.toml
        target: etc/influxdb/influxdb.conf
      - source: man/influx.1.gz
        target: usr/share/man/man1/influx.1.gz
...
    rpm_attributes:
      - 750,influxdb,influxdb:/var/log/influxdb
      - 750,influxdb,influxdb:/var/lib/influxdb
    source: .circleci/packages/influxdb
```

**旧版对照**：v1.8.10 用 Python 2.7 `build.py` + fpm（`#!/usr/bin/python2.7 -u`，`optional_prereqs = ['fpm','rpmbuild','gpg']`，`v1.8.10:build.py:1,55-58`），包内文件树由 `create_package_fs()`/`package_scripts()`/`package_man_files()` 手工拼；支持 7 种 linux 架构与 static 变体（`build.py:97-101,645-662`）。新版改成一份 YAML 清单 + 独立镜像。

**案例**：§6.2 的 release 案例会列出 `packages/` 里的实际产物类型与去向。

**风险**：packager 镜像 digest 固定是优点；但 `sign_packages`/`slack` 仍用 `rsign:latest`/`ci-slack:latest`（§3.7）。包格式与权限变化影响安装（§3.8）。

### 3.7 发布

**角色**：给包签名、生成 digest、上传 S3、生成/发布 changelog、通知 Slack，并决定发布门禁。

**结构**：

- 版本号：`.circleci/scripts/get-version`（CI job 内导出 `VERSION`）与 `config.yaml` 的 `version` 规则（包版本）；两套规则都对 tag 取 `v` 后版本，对非 tag 分支分别生成 `1.x-<short sha>` / `1.x-<sha8>`。
- `sign_packages`（`.circleci/config.yml:214-256`）：`rsign:latest`，`circleci_ip_ranges: true`，对 `*.deb|*.rpm|*.tar.gz|*.zip` 执行 `rsign`，并写 `influxdb.${CIRCLE_TAG}.digests`、`<target>.md5`、`<target>.sha256`。
- `publish_packages`（`:415-427`）：`aws-s3/sync --acl public-read` 到 `s3://dl.influxdata.com/influxdb/releases/<< pipeline.git.tag >>`。
- `changelog`（`:369-383`）：`changelogger` 镜像（digest 固定 `d7093c40...`），`PRODUCT=OSS changelogger`，产出 `changelog_artifacts/`。
- `publish_changelog`（`:385-413`）：release 分支上传到 `releases/<tag>/CHANGELOG.<tag>.md`；nightly 分支上传到 `nightlies/<branch>/CHANGELOG.md`。**注意**：当前 nightly workflow 没有实例化这个 job（§3.5）。
- `slack`（`:428-443`）：`ci-slack:latest`，`SLACK_ARTIFACT_URL=https://dl.influxdata.com/influxdb/releases`，排除 `race` 目录。
- 凭据：`RELEASE_AWS_REGION`、`RELEASE_AWS_ACCESS_KEY_ID`、`RELEASE_AWS_SECRET_ACCESS_KEY`、`QUAY_RSIGN_USERNAME`/`PASSWORD`、`CISUPPORT_GCS_AUTHORIZATION`（packager/slack 镜像拉取）。

**流程**：

| 触发 | 版本 | 签名 | S3 产物 | changelog | Slack |
|---|---|---|---|---|---|
| tag `vX.Y.Z` | `X.Y.Z` | `sign_packages` | `releases/vX.Y.Z/`（包 + digest + md5/sha256） | `releases/vX.Y.Z/CHANGELOG.vX.Y.Z.md` | 有 |
| 分支 push | `1.x-<sha8>` | 无 | 无 | 无 | 无 |
| nightly | `1.x-<sha8>` | 无 | 无 | `changelog` 生成，但 `publish_changelog` 未被 workflow 实例化 → 实际不上传 | 无 |

**代码**：

```yaml
# v1.12.3:.circleci/config.yml:227-250（节选）
          for target in /tmp/workspace/packages/*
          do
            case "${target}" in
              *.deb|*.rpm|*.tar.gz|*.zip) rsign "${target}" ;;
            esac
            if [ -f "${target}" ]; then
              sha256sum "${target}" >> "/tmp/workspace/packages/influxdb.${CIRCLE_TAG}.digests"
              md5sum    "${target}" >"${target}.md5"
              sha256sum "${target}" >"${target}.sha256"
            fi
          done
```

```yaml
# v1.12.3:.circleci/config.yml:421-427
      - aws-s3/sync:
          arguments:             --acl public-read
          aws-region:            RELEASE_AWS_REGION
          aws-access-key-id:     RELEASE_AWS_ACCESS_KEY_ID
          aws-secret-access-key: RELEASE_AWS_SECRET_ACCESS_KEY
          from:                  /tmp/workspace/packages
          to:                    s3://dl.influxdata.com/influxdb/releases/<< pipeline.git.tag >>
```

**旧版对照**：v1.8.10 的 CI 没有 tag workflow、没有 S3 上传；`build.py` 虽提供 `--sign`、`--upload`、`--bucket`（默认 `dl.influxdata.com/influxdb/artifacts`，`v1.8.10:build.py:40-41,872-876,972-982`），但 `test.sh 0` 只传 `--test --junit-report --package`（`v1.8.10:test.sh:105`），所以签名/上传都在 CircleCI 之外完成。旧版签名是 `gpg --armor --detach-sign`（`build.py:596-608`），新版是远程 `rsign`。

**案例**：§6.2。

**风险**：见 §7 风险 5/12；另注意 `add_ssh_keys` 的键名在源码里拼成 `fingerpints`（`.circleci/config.yml:223`），CircleCI 不识别该键，SSH key 实际未注入；当前签名链路仍能工作，说明 rsign 不依赖它，但这是一个应修的配置错误。

#### 3.7.1 版本号生成：两套脚本，一套在用

| 脚本 | 使用点 | 规则 | 状态 |
|---|---|---|---|
| `.circleci/scripts/get-version` | `build_binaries` 的 `PREFIX=1.x .circleci/scripts/get-version`（`.circleci/config.yml:29-32`） | 取 `git tag --points-at HEAD` 中匹配 `vX.Y.Z` 的最新 tag；无 tag 时 `PREFIX-<short sha>`；导出 `VERSION/MAJOR/MINOR/PATCH/RELEASE` | 在用 |
| `releng/get-version` | 仓库内无调用点（`grep -rn 'releng/get-version' --include='*.yml' --include='*.yaml' --include='*.sh' .` 无输出） | 支持 `-p PREFIX`、`-n` nightly；tag 正则更宽（允许 rc/`~`/`+`） | **未被 CI 调用**，属于可删除/需接线的脚本 |
| `config.yaml` 的 `version` | `build_packages` 的 `packager` | `CIRCLE_TAG[1:]` 或 `1.x-CIRCLE_SHA1[:8]` | 在用 |

旧版没有这两个脚本；`build.py` 自己用 `git describe --always --tags --abbrev=0` 取版本（`v1.8.10:build.py:308-315`）。两套新脚本的 tag 规则不同（`.circleci` 只认三段 semver，`releng` 允许 rc），若将来把 `releng/get-version` 接进流水线，需要先统一规则。

### 3.8 分发与安装

**角色**：包内 unit/init/control scripts 决定安装、升级、卸载、启动的行为。它不负责构建和发布。

**结构**：

| 文件 | v1.8.10 | v1.12.3 | 作用 |
|---|---|---|---|
| systemd unit | `scripts/influxdb.service`（21 行） | `.circleci/packages/influxdb/fs/lib/systemd/system/influxdb.service`（21 行，内容逐字相同） | `Type=forking`、`Restart=on-failure`、`LimitNOFILE=65536`、`ExecStart=/usr/lib/influxdb/scripts/influxd-systemd-start.sh` |
| systemd 启动脚本 | 直接 `influxd &` 后探活 | 先 `influxd config` 解析 data/wal 目录，两个目录都存在就跑 `influx_inspect buildtsi -compact-series-file` | 启动预热 TSI |
| SysV `init.sh` | 无 buildtsi | 增加 `setpriv --reuid influxdb ... buildtsi`（`:154-165`） | SysV 启动也预热 |
| `pre-install` | `scripts/pre-install.sh` | `.circleci/packages/influxdb/control/pre-install` | 建 `influxdb` 用户；迁移 `/etc/opt/influxdb` → `/etc/influxdb` 并备份配置 |
| `post-install` | 复制 unit 到 `/lib/systemd/system/`，再 `enable` | 只 `daemon-reload` + `enable`（unit 由包直接落盘） | 注册服务 |
| `post-uninstall` | `systemctl disable` + `rm -f /lib/systemd/system/influxdb.service` | 只 `systemctl disable`（文件由包管理器删除） | 注销服务 |
| `logrotate` | `scripts/logrotate` 打包到 `/etc/logrotate.d/influxdb`（daily/rotate 7/copytruncate/compress） | **仓库内无对应文件** | 日志轮转 |
| 权限 | fpm `--rpm-attr` | `config.yaml:74-91` 的 `perm_overrides`/`rpm_attributes` | unit 0644、脚本 0755、目录 750 |

**流程**（安装 → 启动 → 升级 → 卸载）：

1. `pre-install`：若无 `influxdb` 用户则 `useradd --system -U -M ... -s /bin/false`；若检测到 `/etc/opt/influxdb` 且 `/etc/influxdb` 不存在，`mv -vn` 迁移并备份 `influxdb.conf`。
2. 包管理器落盘：unit、`init.sh`、start 脚本、config sample、8 个 man page。
3. `post-install`：RHEL/Debian/Amazon Linux 分别走 systemd 或 SysV；systemd 只 `daemon-reload` + `enable`。
4. `systemctl start influxdb`：`influxd-systemd-start.sh` 先 `buildtsi`（两个目录都存在时），再启动 `influxd` 并探活 `/health`。
5. 升级：包管理器替换文件；`pre-install` 的配置迁移只在 `/etc/opt/influxdb` 存在时触发；`post-install` 重新 enable。
6. 卸载：`post-uninstall` 删除 `/etc/default/influxdb`、`systemctl disable`；unit 文件由包管理器删除；旧版脚本自己 `rm -f` unit。

**代码**：

```bash
# v1.12.3:.circleci/packages/influxdb/fs/usr/lib/influxdb/scripts/influxd-systemd-start.sh:40-52
DATA_DIR="$( influxd_config data dir     )"
WAL_DIR="$(  influxd_config data wal-dir )"
if [[ ( -d "${DATA_DIR}" ) && ( -d "${WAL_DIR}" ) ]]
then
    yes | /usr/bin/influx_inspect buildtsi -compact-series-file \
        -datadir "${DATA_DIR}" \
        -waldir  "${WAL_DIR}"
fi
/usr/bin/influxd -config "${CONFIG}" ${INFLUXD_OPTS} &
```

```bash
# v1.12.3:.circleci/packages/influxdb/control/pre-install:5-22（节选）
if ! id influxdb &>/dev/null; then
    useradd --system -U -M influxdb -s /bin/false -d /var/lib/influxdb
fi
if [[ -d /etc/opt/influxdb ]]; then
    if [[ ! -d /etc/influxdb ]]; then
        mv -vn /etc/opt/influxdb /etc/influxdb
        if [[ -f /etc/influxdb/influxdb.conf ]]; then
            backup_name="influxdb.conf.$(date +%s).backup"
            cp -a /etc/influxdb/influxdb.conf /etc/influxdb/$backup_name
        fi
    fi
fi
```

```bash
# v1.12.3:.circleci/packages/influxdb/control/post-install:14-18
function install_systemd {
    # Service file is now installed directly by RPM - just enable it
    systemctl daemon-reload
    systemctl enable influxdb
}
```

```bash
# v1.8.10:scripts/post-uninstall.sh:3-6（与新版的差异）
function disable_systemd {
    systemctl disable influxdb
    rm -f /lib/systemd/system/influxdb.service
}
```

**案例**：§6.3。

**风险**：

1. 启动期 `buildtsi` 失败会让服务起不来：`influxd-systemd-start.sh` 是 `#!/bin/bash -e`（`:1`），`yes | influx_inspect ...` 不在条件位也没有 `|| true`；没有 `pipefail` 时管道取最后一条命令的退出码，失败触发 `set -e`，unit 报启动失败并按 `on-failure` 重试（§7 风险 9）。
2. 启动时间机制上随 series 数增长（每次 start 跑一次 `buildtsi`），耗时未实测；判定式为 `buildtsi wall + influxd start > TimeoutStartUSec`（unit 未设 `TimeoutStartSec`，默认 90s），测量方法见 §5.2.2。**口径**：索引格式兼容，升级流程不需要手工 `buildtsi`（02 §5.1）；这里是包安装的启动脚本自动预热，不是升级前置条件。
3. `logrotate` 删除后，包安装不再配置日志轮转；依赖 `/etc/logrotate.d/influxdb` 的部署需要自建。
4. `post-uninstall` 不再删 unit；正常包卸载由包管理器删文件，但手工执行 `post-uninstall` 的流程会留下 unit。
5. `pre-install` 的配置迁移只在 `/etc/opt/influxdb` 存在时触发；从更老版本升级时要确认迁移分支。
6. `https-enabled=true` 且 `https-certificate` 指向的文件不存在：脚本先在 `:52-54` fork `influxd ... &` 并写 PID，再在探活前执行 `echo "${HTTPS_CERT} not found! Exiting..."; exit 1`（`influxd-systemd-start.sh:60-67`），unit 因脚本非 0 立即失败；证书路径写错时不要先怀疑 `buildtsi`，也不要据此判断 influxd 未启动。
7. `/health` 探活循环 `set +e` 且**没有次数上限**（`:73-84`）：curl 失败、端口未监听或 influxd 已崩溃时 `result` 为 `000`（或空），循环每秒重试、永不退出，只能等 systemd 的 `TimeoutStartUSec`（默认 90s）杀掉并重启；表现为“超时—重启”链，而不是明确错误。
8. `influxd config` 解析失败时，`influxd_config` 把 stderr 丢弃（`:37`）并返回空 `DATA_DIR`/`WAL_DIR`，`-d` 判断为假，`buildtsi` 被**静默跳过**，随后才由 `influxd` 自己报配置错误退出（`:40-50`）。

## 6. 具体案例

### 6.1 案例：在干净 Linux 上构建同一个 `influxd`，两版差在哪

#### v1.8.10 的路径与可核对产物

**输入**：同一棵源码树；OLD `v1.8.10`，NEW `f3d5cb68ab`。OLD 用 `Dockerfile_build_ubuntu64`（ubuntu:focal + ruby/fpm + python2.7 + python3 + gcc），NEW 用 `quay.io/influxdb/cross-builder:go1.24.13-latest`。两个镜像都没有在仓库内固定 digest，本文不填。

```bash
# 旧 CI 入口（不是唯一入口；本地也可直接跑 python build.py）
./test.sh 0
#    ├─ run_test_docker Dockerfile_build_ubuntu64 test_64bit --test --junit-report --package
#    ├─ docker build -f Dockerfile_build_ubuntu64 ...   (ubuntu:focal + ruby + fpm + python2.7（build.py）+ python3 + gcc)
#    └─ 容器内执行：
#         python build.py --test --junit-report --package
#           ├─ go mod download → go fmt ./... → ./generate.sh（仅 amd64）→ go vet ./...
#           ├─ go test ./...，再经 go-junit-report 汇总
#           ├─ 对 supported_builds 里当前 (platform, arch) 执行 go build
#           │    （test.sh 0 不传 --arch/--platform，默认取 get_system_platform()/get_system_arch()，
#           │      即宿主 linux/amd64；不是 supported_builds 的 9 个组合）
#           └─ fpm -s dir -t deb / -t rpm ...
```

关键配置：

```python
# v1.8.10:build.py:89-107
targets = {
    'influx' : './cmd/influx',
    'influxd' : './cmd/influxd',
    'influx_stress' : './cmd/influx_stress',
    'influx_inspect' : './cmd/influx_inspect',
    'influx_tools': './cmd/influx_tools',
}
supported_builds = {
    'darwin': [ "amd64" ],
    'windows': [ "amd64" ],
    'linux': [ "amd64", "i386", "armhf", "arm64", "armel", "static_i386", "static_amd64" ]
}
supported_packages = {
    "darwin": [ "tar" ],
    "linux": [ "deb", "rpm", "tar" ],
    "windows": [ "zip" ],
}
```

**`test.sh 0` 的预期产物（宿主 linux/amd64）**：

| 产物 | 数量/路径 | 依据 |
|---|---|---|
| 二进制 | **5 个**：`build/influx`、`build/influxd`、`build/influx_inspect`、`build/influx_stress`、`build/influx_tools`（`targets` 5 项；`single_build=True` 时 `outdir` 不追加 `platform/arch`） | `v1.8.10:build.py:89-95,817-848` |
| 包 | **3 个**：linux/amd64 的 deb、rpm、tar.gz；非 release 包版本为 `<version>~<short sha>`、iteration 0；tar 名形如 `influxdb-<version>~<sha>_linux_amd64.tar.gz` | `v1.8.10:build.py:103-107,664-761,677-681` |
| 版本注入 | `-X main.version=<version> -X main.branch=<branch> -X main.commit=<commit>`；非 static 分支**没有** `-s/-w` | `v1.8.10:build.py:558-577` |
| 校验和 | 只在日志里打印 MD5（`Packages created: ... (MD5=...)`），**不写** `.md5/.sha256` 文件 | `v1.8.10:build.py:878-881` |

**全矩阵行为**：只有 `python build.py --platform all --arch all` 才走 9 个 `(platform, arch)` 组合（linux 7 + darwin 1 + windows 1），每个组合循环 5 个 target，即 **45 次 `go build`**。不要把它和 `test.sh 0` 的 5 次混在一起。

#### v1.12.3 的路径：先修版本注入，再谈产物

```bash
# 1) 直接用 cross-builder 镜像，不再有嵌套 Docker
docker run --rm -it -v "$PWD":/work -w /work \
  quay.io/influxdb/cross-builder:go1.24.13-latest bash

# 2) 版本号：get-version 把 export 追加到 BASH_ENV，不会写进当前 shell
export BASH_ENV="$(mktemp)"
PREFIX=1.x .circleci/scripts/get-version
# shellcheck disable=SC1090
source "$BASH_ENV"
echo "$VERSION"

# 3) man page 预生成（release 与 on_push 都跑）
(cd man && make build && gzip -9 ./*.1)

# 4) 四种目标各自 build + archive（仅示意循环骨架，真实分支见 .circleci/config.yml:70-179）
export CGO_ENABLED=1
for combo in "darwin amd64 $(xcc darwin)" \
             "windows amd64 $(xcc windows)" \
             "linux amd64 $(xcc linux x86_64)" \
             "linux arm64 $(xcc linux aarch64)"; do
  set -- $combo; export GOOS=$1 GOARCH=$2 CC=$3
  case "$GOOS" in
    darwin)  tags="netgo,osusergo";              buildmode=pie; ldflags="-s -w" ;;
    windows) tags="netgo,osusergo,static_build"; buildmode=exe; ldflags="-s -w" ;;
    linux)   tags="netgo,osusergo,static_build"; buildmode=pie
             [ "$GOARCH" = amd64 ] && rustlib=$(find_rustlib_path x86_64-unknown-linux-musl) \
                                   || rustlib=$(find_rustlib_path aarch64-unknown-linux-musl)
             ldflags="-s -w -linkmode=external -extld=$CC -extldflags \"-fno-PIC -static-pie -Wl,-z,stack-size=8388608 -L$rustlib -Wl,-lunwind\"" ;;
  esac
  # 三条命令都 build；windows 需 .exe 后缀
  for cmd in influx influx_inspect influxd; do
    binary="bins/$GOOS-$GOARCH/$cmd"; [ "$GOOS" = windows ] && binary="$binary.exe"
    go build -tags="$tags" -buildmode="$buildmode" -ldflags="$ldflags \
      -X \"main.version=$VERSION\" -X \"main.branch=${CIRCLE_BRANCH}\" -X \"main.commit=${CIRCLE_SHA1}\"" \
      -o "$binary" "github.com/influxdata/influxdb/cmd/$cmd"
  done
done

# 5) 打包（独立的 job + 独立镜像）
packager .circleci/packages/config.yaml
```

**版本注入的失败路径（可复现）**：

```console
$ env -u BASH_ENV PREFIX=1.x bash .circleci/scripts/get-version
.circleci/scripts/get-version: line 41: BASH_ENV: unbound variable
$ echo $?
1
```

当前 HEAD 没有 tag，所以命中 else 分支的 `:41`；如果 HEAD 有语义化 tag，失败点是 `:33`。CircleCI 里由 runner 注入 `BASH_ENV`，而且 `get-version` 是独立 `run` step，所以 CI 自身不受影响（`.circleci/config.yml:29-32`、`.circleci/scripts/get-version:31-47`）。即使手动设置了 `BASH_ENV`，不 `source` 它也不会得到 `VERSION`——脚本只做追加，不导出到父 shell。

**两种预期版本**：

| HEAD 状态 | `source "$BASH_ENV"` 后 `$VERSION` | 依据 |
|---|---|---|
| 有 `v1.12.3` tag | `1.12.3` | `get-version:31-39` |
| 无 tag | `1.x-$(git rev-parse --short HEAD)` | `get-version:40-47` |

**预期产物清单（4 组合 × 3 command）**：

| 产物 | 数量/路径 | 依据 |
|---|---|---|
| 二进制 | **12 个**：`bins/darwin-amd64/{influx,influx_inspect,influxd}`、`bins/windows-amd64/{...}.exe`、`bins/linux-amd64/{...}`、`bins/linux-arm64/{...}` | `.circleci/config.yml:60-69,112-129,149-179` |
| 归档 | **4 个**：`artifacts/influxdb_bin_darwin_amd64-<sha>.tar.gz`、`..._windows_amd64-...`、`..._linux_amd64-...`、`..._linux_arm64-...`；每个只含 3 个二进制 | `:132-147` |
| 校验和 | **8 个**：每个归档的 `.md5` 与 `.sha256` | `:145-146` |
| 源码包 | **1 个**：`tarball/influxdb-src-<sha>.tar.gz`（含预生成 man page） | `:33-56` |
| 包 | `packages/`，由独立 `build_packages` job 用 packager 镜像产出，见 §6.2 的产物表 | `:193-212`、`.circleci/packages/config.yaml` |

**可核对的二进制属性**：

```bash
file bins/linux-amd64/influxd
# 稳定判据：ELF 64-bit LSB pie executable；readelf -d 无 NEEDED libc（或 ldd 报 not a dynamic executable）
file bins/darwin-amd64/influxd
# 稳定判据：Mach-O 64-bit executable x86_64
file bins/windows-amd64/influx.exe
# 稳定判据：PE32+ executable (console) x86-64
bins/linux-amd64/influxd version
# 预期：InfluxDB 1.12.3 (git: <branch> <commit>)；无 tag 时为 1.x-<short sha>（`git rev-parse --short HEAD`，长度随仓库而定）
go version -m bins/linux-arm64/influxd | grep -E 'main\.(version|branch|commit)'
# 预期：-ldflags 行里含 main.version=1.12.3（或无 tag 的 1.x-<short sha>）、main.branch、main.commit
```

`file` 的具体 `static-pie linked` 字样随 binutils 版本变化，所以只固定格式与“无动态 libc 依赖”这一可验证属性；**本文不填二进制字节数/构建耗时**（§5.2.1 明确待实测）。

**构建次数与产物数量对照**：

| 路径 | `go build` 次数 | 归档 | 校验和 |
|---|---:|---:|---:|
| OLD `test.sh 0`（宿主 linux/amd64） | 1 组合 × 5 target = **5** | 无 tar 归档；3 个包 | 0（只打印 MD5） |
| OLD `build.py --platform all --arch all`（全矩阵） | 9 组合 × 5 target = **45** | 每平台自写 tar/zip | 0 |
| NEW CI（4 组合） | 4 组合 × 3 command = **12** | 4 个 tar.gz | 8 个 |

#### 差异对照

| 步骤 | v1.8.10 | v1.12.3 |
|------|---------|---------|
| 触发 | `./test.sh <idx>`（或直接 `python build.py`） | `.circleci/config.yml` 的 job |
| 隔离层 | 宿主机 → Docker → 容器内再跑脚本 | 直接跑在 job 容器里 |
| 构建语言 | Python 2.7 | Bash |
| Go 版本来源 | 构建镜像 `Dockerfile_build_ubuntu64` 里装 | `cross-container-tag: go1.24.13-latest` + `go.mod` 的 `toolchain` |
| 目标平台 | 7 种 linux 架构 + darwin + windows（全矩阵 9 组合；`test.sh 0` 只构建宿主） | linux amd64/arm64 + darwin amd64 + windows amd64（4 组合） |
| 二进制数 | 5 个/组合（含 `influx_stress`、`influx_tools`） | 3 个/组合（`influx`、`influx_inspect`、`influxd`） |
| CGO | 32bit 环境需要 `-arch=i386` | 全部 `CGO_ENABLED=1`，linux 需要 Rust `libunwind` |
| 链接方式 | 默认 internal linkmode | linux 强制 `-linkmode=external` + `-static-pie` |
| 符号 | 非 static 无 `-s`；static 分支有 `-s` | 全部 `-s -w` |
| 打包 | fpm（`deb`/`rpm`），自写 tar/zip | `packager` 一份 YAML |
| 签名 | build.py 内 `gpg` | 独立 `sign_packages` job + `rsign` |
| 包安装测试 | AWS EC2（terraform + ssh + `apt-get install` deb） | `test_pkgs_64bit`：起 `ubuntu-2204` machine，跑 `releng/packages/spec/clean_install/run.bash -D/-R`（deb/rpm） |

### 6.2 案例：同一个 commit 的 tag push / branch push / nightly，分别走到哪

**输入**：

- 仓库在 `f3d5cb68ab`，打 tag `v1.12.3` 并 push。
- 同一个 commit 在分支 `master-1.x` 上 push。
- 定时 pipeline 触发，参数 `workflow=nightly`。

**tag push（release workflow）**：

1. `release_filter` 只匹配 `^v(\d+)(?:\.(\d+))?(?:\.(\d+))?$`（`.circleci/config.yml:445-454`），`v1.12.3` 命中；`release` workflow 的全部 job 都带这个 filter。
2. `build_binaries` 执行 `PREFIX=1.x .circleci/scripts/get-version`；HEAD 上有 `v1.12.3` tag，`VERSION=1.12.3`（`.circleci/scripts/get-version:25-39`）。脚本把 export 追加到 runner 注入的 `BASH_ENV`，后续 step 由 CircleCI 自动 source；本地复现方式与失败路径见 §6.1。
3. `build()` 产出 **4 个**归档，组合是 `darwin_amd64`、`windows_amd64`、`linux_amd64`、`linux_arm64`（不是 6 或 8 个）：`artifacts/influxdb_bin_darwin_amd64-<sha>.tar.gz`、`artifacts/influxdb_bin_windows_amd64-<sha>.tar.gz`、`artifacts/influxdb_bin_linux_amd64-<sha>.tar.gz`、`artifacts/influxdb_bin_linux_arm64-<sha>.tar.gz`；每个归档带 `.md5` 与 `.sha256` 各一个文件，共 8 个校验和文件。
4. `build_packages` 的 `config.yaml` 匹配 `CIRCLE_TAG`，包版本 `1.12.3`；产出 `packages/`，产物类型与内容见下表。
5. `sign_packages` 对包签名，写 `packages/influxdb.v1.12.3.digests`（`CIRCLE_TAG` 保留 `v`）和每个包的 `.md5`/`.sha256`。
6. `test_pkgs_64bit` 与 `sign_packages` 并行；它失败**不会**阻止 `publish_packages`。
7. `changelog` 产出 `CHANGELOG.md`；`publish_changelog` 上传到 `s3://dl.influxdata.com/influxdb/releases/v1.12.3/CHANGELOG.v1.12.3.md`。
8. `publish_packages` 上传到 `s3://dl.influxdata.com/influxdb/releases/v1.12.3/`；`slack` 在 publish 成功后发通知。

**`packages/` 产物清单（tag 与 branch 共用）**：

| source 归档（`.circleci/packages/config.yaml:8-27`） | 平台/架构 | 预期包格式 | 预期数量 |
|---|---|---|---:|
| `influxdb_bin_windows_amd64-*.tar.gz` | windows/amd64 | zip | 1 |
| `influxdb_bin_darwin_amd64-*.tar.gz` | darwin/amd64 | tar.gz | 1 |
| `influxdb_bin_linux_amd64-*.tar.gz` | linux/amd64 | deb + rpm + tar.gz | 3 |
| `influxdb_bin_linux_arm64-*.tar.gz` | linux/arm64 | deb + rpm + tar.gz | 3 |
| **合计** | | | **8** |

包内容（`config.yaml:29-98` 声明，packager 负责落盘）：

- `binaries`：`influx`、`influx_inspect`、`influxd`；Windows 为对应的 3 个 `.exe`。
- `extras`：`etc/config.sample.toml → etc/influxdb/influxdb.conf`；8 个 man page：`influx.1.gz`、`influx_inspect.1.gz`、`influxd.1.gz`、`influxd-backup.1.gz`、`influxd-config.1.gz`、`influxd-restore.1.gz`、`influxd-run.1.gz`、`influxd-version.1.gz`。
- `perm_overrides`：`init.sh`、`influxd-systemd-start.sh` 为 `root:root 0755`；`influxdb.service` 为 `root:root 0644`。
- `rpm_attributes`：`/var/log/influxdb`、`/var/lib/influxdb` 为 `750,influxdb,influxdb`。
- deb/rpm 都 `recommends: influxdata-archive-keyring`（不是 requires）。

**边界**：仓库内只有 source 与包内容声明，没有 packager 实现；`config.yaml` 未声明格式映射，上表的 deb/rpm/tar.gz/zip 是按平台惯例与 §3.6 的产物描述给出的预期。若 packager 镜像改变映射，包名与格式以实际 `ls packages/` 为准；本文不填具体包文件名字节。

**branch push（on_push workflow）**：

1. 没有 tag，`get-version` 走 fallback：`VERSION=1.x-<short sha>`；包版本是 `1.x-<sha8>`（`config.yaml:1-6`）。
2. 只跑 `build_binaries → build_packages → test_pkgs_64bit`，外加并行的 `static_code_checks`/`fluxtest`/`unit_test*`。
3. 没有 `sign_packages`/`publish_packages`/`changelog`/`slack`；不会写 S3。

**nightly（scheduled pipeline）**：

1. 触发条件是 `pipeline.trigger_source=scheduled_pipeline` 且 `workflow=nightly`（`.circleci/config.yml:529-540`）。
2. 先跑 `changelog`、`static_code_checks`、`fluxtest`、`unit_test_inmem/tsi1/race`；`build_binaries` 要求这 6 个全部通过（`:554-561`），再 `build_packages → test_pkgs_64bit`。
3. 不签名、不发布。`publish_changelog` 虽然在 job 里定义了 nightly 分支（`:404-413`），但 nightly workflow 没有实例化该 job，所以 nightly 的 `CHANGELOG.md` **不会**上传到 `nightlies/<branch>/`。

**预期失败语义**：

| 失败点 | tag 发布是否继续 | 依据 |
|---|---|---|
| `test_pkgs_64bit` 失败 | **继续**（只红流水线） | `publish_packages` 只 `requires sign_packages`（`:483-486`） |
| `sign_packages` 失败 | 停止；`publish_packages` 不跑 | `:468-471,483-486` |
| `changelog` 失败 | 包发布继续；`publish_changelog` 不跑 | `publish_changelog` 只 requires changelog（`:478-482`），与 publish_packages 无依赖 |
| `static_code_checks`/`fluxtest`/`unit_test*` 失败 | **继续**（不 gate S3） | release workflow 中没有作为 publish 前置（`:491-506`） |

**证据**：`.circleci/config.yml:445-506,529-567`；`.circleci/packages/config.yaml:1-6`；`.circleci/scripts/get-version:25-47`。

### 6.3 案例：一次安装 → 启动 → 卸载的生命周期

**输入**：Ubuntu 22.04；`influxdb_1.12.3_amd64.deb`；`/var/lib/influxdb/data` 与 `/var/lib/influxdb/wal` 已存在；series 数记为 N（用 `influx_inspect report-db -db-path /var/lib/influxdb/data/<db> -rollup t -exact` 或 `SHOW SERIES CARDINALITY` 得到）。

**步骤与预期**：

1. `dpkg -i influxdb_1.12.3_amd64.deb`：
   - `pre-install` 若 `id influxdb` 失败则 `useradd --system -U -M ...`；若存在 `/etc/opt/influxdb` 且 `/etc/influxdb` 不存在，迁移并打印备份路径（`pre-install:5-22`）。
   - 包落盘：3 个二进制、8 个 man page、`/etc/influxdb/influxdb.conf`、`/lib/systemd/system/influxdb.service`、`/usr/lib/influxdb/scripts/{init.sh,influxd-systemd-start.sh}`。
   - `post-install` 执行 `systemctl daemon-reload` + `systemctl enable influxdb`（`post-install:14-18`）。
2. `systemctl start influxdb`：
   - `influxd-systemd-start.sh` 用 `influxd config` 解析 `data dir`/`wal-dir`；两个目录都存在时先跑 `yes | influx_inspect buildtsi -compact-series-file`（`:40-52`）。
   - 若 `buildtsi` 退出非 0，`#!/bin/bash -e` 会让脚本失败，unit 报启动失败；`systemctl show -p TimeoutStartUSec` 默认 `1min 30s`，N 很大时超时会形成“重启—超时—再重启”（§5.2.2、§7 风险 9）。
   - `influxd ... &` 之后立即 `PID=$!` 并写 `/var/lib/influxdb/influxd.pid`（`:52-54`）；写 PID 只表示后台进程已 fork，不表示启动成功。
- 探活循环请求 `$PROTOCOL://$HOST:$PORT/health`，接受任意 `20x` 或 `40x` 状态码（`:73-83`）；`serveHealth` 固定写 200 + JSON body（`v1.12.3:services/httpd/handler.go:1827-1839`），所以**无认证时是 200**、开启认证（`auth-enabled && ping-auth-enabled`）时是 401；**204 是 `/ping` 的行为，不是 `/health`**（`:1813-1825`）。TLS 自签证书需要 `curl -k`。
   - 若 `https-enabled=true` 且 `https-certificate` 指向的文件不存在，脚本在 `:52` 已 fork `influxd ... &` 之后、探活之前 `exit 1`（`:60-67`），unit 因脚本非 0 立即失败；influxd 已被拉起过（缺证书时它自身也会启动失败），不要据此认为它没启动。
   - 探活循环 `set +e` 且**没有次数上限**（`:73-84`）：influxd 崩溃、端口未监听或 curl 失败时脚本不会退出，只能等 systemd 的 `TimeoutStartUSec`（默认 90s）杀掉，日志中的 `InfluxDB API unavailable after N attempts` 会持续增长。
   - `influxd config` 解析失败时 `DATA_DIR`/`WAL_DIR` 为空，`-d` 判断为假，`buildtsi` 被**静默跳过**（`:37,40-50`），随后 `influxd` 才以自身错误退出（§3.8 风险 6-8）。
3. `systemctl status influxdb`：`active (running)`；`curl -k https://<bind-address>/health`（若开 TLS）无认证返回 **200 + JSON**、开启认证返回 401；探活脚本只看首两位（`20`/`40`），不区分具体码。
4. `dpkg -r influxdb`：
   - `post-uninstall` 删除 `/etc/default/influxdb` 并 `systemctl disable influxdb`（`post-uninstall:30-41`）；unit 文件由 dpkg 删除，不再由脚本 `rm -f`（对比 `v1.8.10:scripts/post-uninstall.sh:3-6`）。
   - `influx`/`influxd`/`influx_inspect` 从 `$PATH` 消失（clean_install 用例断言这一点，`_install_uninstall.bash:28-33,82-84`）。
5. 检查日志轮转：`test -f /etc/logrotate.d/influxdb`。v1.8.10 的 `scripts/logrotate` 会打包到该路径；v1.12.3 仓库内没有这个文件，预期为“不存在”，依赖旧路径的部署要自建（§7 风险 16）。

**可复核的固定值**：3 个二进制、8 个 man page、`TimeoutStartUSec` 默认 90s、`PRE-INSTALL` 迁移源 `/etc/opt/influxdb`、目标 `/etc/influxdb`。**没有固定时间值**：启动/`buildtsi` 耗时必须按 §5.2.2 在目标数据规模上实测。

**N 的档位**：本文不填启动时间，也不把 N 写成一个笼统的“很大”。可复现的输入档位是 N=10^4 / 10^5 / 10^6 series（用上面的 `report-db -rollup t -exact` 或 `SHOW SERIES CARDINALITY` 确认），每档记录 `buildtsi wall`、`systemctl start wall`、是否超过 `TimeoutStartUSec=90s`。仓库没有 series–启动时间基线，三档的耗时列保持“待测”，只固定判定关系：`buildtsi wall` 超过 90s 时 unit 启动失败并按 `Restart=on-failure` 重试。

### 6.4 案例：相邻性 property 如何抓到一个非法 compaction plan

**输入**（`compact_property_test.go:32-39` 的示例）：

- 文件：`01-01.tsm`、`02-02.tsm`、`03-03.tsm`。
- 分组：`Group 1 = [01-01.tsm, 03-03.tsm]`，`Group 2 = [02-02.tsm]`。

**步骤**：

1. `validateFileAdjacency(allFiles, groups)` 先对每个文件 `tsm1.DefaultParseFileName`，得到 generation/sequence。
2. 按 `(generation, sequence)` 排序，三个文件的 index 为 0、1、2。
3. Group 1：`lastIndex=0`（01）；下一个 03 的 index=2；`lastIndex+1 != 2` → 返回错误。
4. `ValidateCompactionProperties` 把错误包成 `Adjacency Rule violation: ...`。

**预期输出**：

```text
Adjacency Rule violation: file 03-03.tsm in compaction group 1 violates adjacency policy
```

（错误文本来自 `compact_property_test.go:80-82` 的 `file %s in compaction group %d violates adjacency policy`，`groupIndex+1` 使第 0 组显示为 group 1；外层包装来自 `:104-107`。）

**合法对照**：`Group 1 = [01-01.tsm, 02-02.tsm]`、`Group 2 = [03-03.tsm]` 时，每组内 index 连续，返回 `nil`。**运行方式**：validator 不在 `compact_property_test.go` 里由 `func Test` 直接调用，而是由 `ValidateTestCase`（同文件 `:122-144`）在 `compact_test.go:5411` 的 `TestEnginePlanCompactions` 内调用。跑现有覆盖：

```bash
go test ./tsdb/engine/tsm1 -run '^TestEnginePlanCompactions$' -count=1
# 或单个子测试（名称示例来自 compact_test.go:4600）：
go test ./tsdb/engine/tsm1 -run '^TestEnginePlanCompactions$/nested_4_5_2_4_5$' -count=1
```

要单跑上面的 3 文件例子，新增一个同包（`tsm1_test`）最小测试：

```go
func TestDocAdjacencyExample(t *testing.T) {
	err := validateFileAdjacency(
		[]string{"01-01.tsm", "02-02.tsm", "03-03.tsm"},
		[]tsm1.CompactionGroup{{"01-01.tsm", "03-03.tsm"}, {"02-02.tsm"}},
	)
	if err == nil || err.Error() != "file 03-03.tsm in compaction group 1 violates adjacency policy" {
		t.Fatalf("unexpected: %v", err)
	}
}
```

这个最小测试直接拿到内层错误；若经 `ValidateCompactionProperties`，外层会再包一层 `Adjacency Rule violation: `（`:104-107`）。

### 6.5 案例：一个 `planner tick` 的文件名解析与首块解码

**输入**：一个 shard，100 个 generation，每个 generation 1 个文件，全部 level 4，无 tombstone，文件集在一个 tick 内不变。旧版 `Engine.compact` 的 tick 调 `PlanLevel(1)`、`PlanLevel(2)`、`PlanLevel(3)`、`Plan`（`v1.8.10:tsdb/engine/tsm1/engine.go:2111-2120`）。

**旧版调用点计数（下界）**：

`tsmGeneration.level()` 每次执行一次 `parseFileName(t.files[0].Path)`（`v1.8.10:tsdb/engine/tsm1/compact.go:177-188`）；`tsmGenerations.level()` 会遍历组内每个世代，对每个成员再调一次 `tsmGeneration.level()`（同文件 `:2089-2098`）。因此单个 `PlanLevel`：

| 调用点 | 次数 | 说明 |
|---|---:|---|
| `cur.level()`（`:260`，`i=0..98`） | 99 | `i=99` 时 `i < len-1` 为假，跳过这一支 |
| `generations[i+1].level()`（`:260`，`i=0..98`） | 99 | 每次比较还会调用右侧；此前漏计 |
| `currentGen.level()`（`:266`，`i=1..99`） | Σi = **4950** | `currentGen` 每轮增加 1 个世代；`tsmGenerations.level()` 对组内每个成员调用 `g.level()`，所以是 1+2+…+99，不是 99 |
| `cur.level()`（`:266`，`i=1..99`） | 99 | `==` 比较的右侧；`i=0` 因 `len(currentGen)==0` 在 `||` 处短路 |
| 组过滤 `cur.level()`（`:283`） | 100 | 100 个同 level 世代合成 1 个 group；`cur` 是 `tsmGenerations`，其 `level()` 会遍历组内 100 个成员；多 group 时按成员总数增加 |
| **小计** | **5347** | |

3 个 `PlanLevel` = **16041** 次 `level()`。`Plan`：

| 调用点 | 次数 | 说明 |
|---|---:|---|
| 结束点扫描 `g.level()`（`:494`） | 100 | 全部 level 4，不触发 `<=3` 的 break |
| 分组 `gen.level()`（`:549`） | 100 | 内层 `j` 循环覆盖 100 个 generation |
| **小计** | **200** | |

合计 **≥16241 次 `level()` / 文件名解析**（旧版每次 `level()` 都解析）。若 `Plan` 无结果而调用 `PlanOptimize`，再加 **5347**（同构：`:361` 的 `cur.level()` / `generations[i+1].level()` 各 99 次、`:367` 的 `currentGen.level()` Σi=4950 与 `cur.level()` 99 次、`:385` 的组过滤 100 次）。此前文档写的 797 只算了“每层 99+99+1”，漏了 `tsmGenerations.level()` 对组内成员的遍历；正确量级是 16241。

- **首块**：若这 100 个 generation 中 10 个 >2GB 且满块，`Plan` 的结束点扫描（`:515`）与分组（`:559`）都会对扫描到的大世代调 `FileStore.BlockCount(...,1)`；每个大世代在每个阶段各最多一次，因此每 tick **10–20 次** `BlockIterator` + `Read()` + `BlockCount(block)` 解码（`v1.8.10:tsdb/engine/tsm1/file_store.go:927-947`）。

**新版**：

- 打开时每个文件解析 **2 次**（`FileStore.Open()` 的 `f.ParseFileName(fn)` 与 `NewTSMReader()` 的 `parseAndCacheFileName`），重命名再 1 次；100 个文件共 **200 次一次性解析**（`v1.12.3:tsdb/engine/tsm1/file_store.go:614-638,334-338`；`reader.go:245-276`）。`findGenerations` 从 `ExtFileStat.Generation` 分组，`level()` 读 `files[0].Sequence`，**每 tick 0 次文件名解析**（`compact.go:219-229,699-724`）。注意 `tsmGenerations.level()` 仍会遍历组内成员，新版每 tick 的 `level()` 调用次数并未降到 0：3 次 `PlanLevel` 各 5149 次（`groupAdjacentGenerations` 5049 + 组过滤 100），`Plan` 常规路径再 1 次（反向扫描在最后一个 level-4 世代即 `break`）；若 `Plan` 走 cold/full 分支则为 99 次（`generations[i+1].level()`），两处互斥，因此合计 **15448**（常规）或 **15546**（cold/full），再走 `PlanOptimize` 各加 5149。每次调用只是一次字段读取，不再解析文件名。
- 首块：`FileStore.Stats()` 对 `f.files` 中**每个**文件调 `ExtStats()`，后者无条件调 `firstBlockCount()`；100 个文件共 **100 次一次性首块解码**（不限 >2GB 的 10 个），之后缓存，**每 tick 0 次**（`v1.12.3:tsdb/engine/tsm1/file_store.go:881-887`；`reader.go:685-697`）。

**预期**：旧版每 tick 的解析次数按 generation 数 n 为 `1.5n² + 12.5n - 9`（单次 `PlanLevel` 为 `0.5n² + 3.5n - 3`：`:260` 的 `cur.level()`/`generations[i+1].level()` 各 `n-1`、`:266` 的 `currentGen.level()` 为 `n(n-1)/2`、`:266` 的 `cur.level()` 为 `n-1`、组过滤为 `n`；3 次 `PlanLevel` 为 `1.5n² + 10.5n - 9`，再加 `Plan` 的 `2n`；仅适用于旧版调用点，n=100 → 16241；n=1000 → **1,512,491**），是二次增长，不是线性；新版在文件集稳定时每 tick 0 次解析。**这是按源码调用点推导的下界，不是实测耗时**；墙钟收益取决于文件数、文件大小、磁盘与 CPU，必须按 §4.8 的方法测。

**计数 harness（示意，不可直接编译运行；`open100Generations` 未实现，OLD 的计数还依赖 planner 的方法值）**：

```go
// 伪代码：tsm1 包外测试（`open100Generations` 为夹具占位名，需自行补齐；本代码块不能直接运行）。
package tsm1_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// countingFS 统计 planner 经 fileStore.ParseFileName 的调用。
// OLD 的 tsmGeneration.level() 使用 planner 的 ParseFileName 方法值，因此会被计数；
// NEW 的 level() 只读 FileStat.Sequence，预期计数为 0。
type countingFS struct {
	*tsm1.FileStore
	n int64
}

func (c *countingFS) ParseFileName(p string) (int, int, error) {
	atomic.AddInt64(&c.n, 1)
	return tsm1.DefaultParseFileName(p)
}

func TestDocPlannerParseCount(t *testing.T) {
	fs := &countingFS{FileStore: open100Generations(t)} // 夹具：100 个 level-4 文件；不推进 mtime 时得到“mtime 未变”场景
	p := tsm1.NewDefaultPlanner(fs, 0)

	// 预热：第一次 findGenerations 的解析不计入稳态
	p.PlanLevel(1)
	p.PlanLevel(2)
	p.PlanLevel(3)
	p.Plan(time.Time{})
	atomic.StoreInt64(&fs.n, 0)

	p.PlanLevel(1)
	p.PlanLevel(2)
	p.PlanLevel(3)
	p.Plan(time.Time{})
	t.Logf("steady-state ParseFileName calls=%d", atomic.LoadInt64(&fs.n))
}
```

预期输出：

| 场景 | 稳态每 tick `ParseFileName` 调用 | 稳态每 tick `level()` 调用 |
|---|---:|---:|
| OLD，mtime 推进（`findGenerations` 重新解析，`Plan` 正常执行） | **16341**（16041 次 `level()` 各解析 1 次 + `findGenerations` 100 次 + `Plan` 200 次） | **16241**（16041 + `Plan` 200） |
| OLD，mtime 未变（`findGenerations` 命中缓存，`Plan` 因 `lastPlanCheck` 早退） | **16041** | **16041** |
| NEW，任一场景 | **0** | 15448（常规）/15546（cold/full 分支）；读 `FileStat.Sequence`，不解析文件名 |

> `level()` 列是按调用点推导的，**不是 harness 实测值**：`tsmGenerations.level()`/`tsmGeneration.level()` 都不导出，包外测试无法直接计数，harness 只统计 `ParseFileName`。OLD 的 `level()` 每次都解析文件名，所以 OLD 两列之差正好是本 tick `findGenerations` 的解析次数（mtime 推进时 100，未变时 0）；NEW 的 `level()` 只读字段，两列不再相关。

`open100Generations` 是测试夹具的占位名（harness 为示意代码，未补齐夹具）；也可以复用 `compact_test.go` 的 `TestEnginePlanCompactionsRunner` 构造文件集。`Plan` 在 `lastPlanCheck.After(FileStore.LastModified())` 时会直接返回，复现时需让夹具在两个 tick 之间推进文件 mtime，否则 `Plan` 的贡献为 0、`findGenerations` 也不重新解析（只剩 3×5347=16041）。除了计数 harness，`go test -run '^$' -bench` 与 pprof 只能给相对耗时；**没有跨版本墙钟基准**。

### 6.6 案例：同一台机器上 `/debug/vars` 的输出差异

**输入**：在 `etc/config.sample.toml` 基础上设置 `[data] compact-throughput = "48m"`、`compact-throughput-burst = "48m"`；建 1 个 CQ，并让它成功执行若干次（下面的 `queryOk: 42` 中 42 是**示意值**，实际值等于成功执行次数）。

**示意值标注**：下文的 `42`、`12.34` 只用于展示 JSON 形状，不是本机实测；可复现的是键名、嵌套形状与下面的推导关系。

**v1.8.10** 的顶层键（`services/httpd/handler.go:1479-1572`）：

```json
{
  "system":  { ... },
  "cmdline": [ ... ],
  "memstats": { ... },
  "httpd::8086": { ... },
  "cq":      { "name": "cq", "tags": null, "values": { "queryFail": 0, "queryOk": 42 } },
  "runtime": { ... }
}
```

**v1.12.3** 的顶层键（`services/httpd/handler.go:2291-2523`）：

```json
{
  "system":  { ... },
  "build":   { ... },
  "cmdline": [ ... ],
  "config":  {
    ...,
    "compact-throughput": 50331648,
    "compact-throughput-burst": 50331648,
    "aggressive-points-per-block": 10000
  },
  "stats":   { "compact-throughput-usage-percentage": 12.34 },
  "crypto":  { ... },
  "memstats": { ... },
  "httpd::8086": { ... },
  "cq":      { "queryFail": 0, "queryOk": 42 },
  "userquerybytes::8086": { ... },
  "runtime": { ... }
}
```

要点：`"config"` 是 `tsdb.Config.Diagnostics()` 的 17 个键（`tsdb/config.go:288-308`，示例省略其余 14 个），只包含 `[data]` 段（`monitor/service.go:164-166` 注册的是 `m.TSDBConfig`），但已含 `dir`/`wal-dir` 绝对路径；`"stats"` 里的百分比是**令牌桶瞬时占用**，`"cq"` 旧版是带 `{name,tags,values}` 包装的 Statistic、新版只输出 `values`（键名 `queryOk`/`queryFail`），`userquerybytes` 需要显式开启才会出现。`httpd`/`userquerybytes` 的真实顶层键带 `bind` 后缀（默认 `httpd::8086`、`userquerybytes::8086`；`httpd.Service.Statistics` 在 tags 中合并 `bind`，`serveExpvar` 拼 key 时 `bind` 优先，见 06 §3.5）；下文示例已按默认 `bind` 写出（`httpd::8086`、`userquerybytes::8086`），自定义端口时后缀随之变化。

**`12.34` 的推导**：公式是 `usage = 100 × (1 - Tokens / Limit)`（`monitor/stats.go:21-24`）。该公式的分母是速率上限 `Limit()`，不是 burst 容量；下表三行只在默认 `burst == limit`（均为 48 MiB）时成立。若 `burst > limit`，满桶时 `usage = 100×(1 - burst/limit)` 可为负值。`NewRate(48MiB/s, 48MiB)` 在创建时调用 `AllowN(now, burstLimit)`，把初始 burst 一次性花掉（`pkg/limiter/writer.go:25-29`），所以：

| 时点 | `Tokens()` | `CompactThroughputUsage()` |
|---|---:|---:|
| `NewRate` 返回后立即 | 0 | **100%** |
| 1s 后且无 compaction 消耗 | 48 MiB（cap） | **0%** |
| 0.5s 后且无 compaction 消耗 | 24 MiB | **50%** |

因此启动瞬间接近 100% 不是“吞吐打满”，而是 burst 被预支；用它做告警必须在启动后加宽限期（§7 风险 7）。

### 6.7 案例：依赖规模的结构差异与测量方法（动态值待实测）

**输入**：两个干净的 checkout（`git checkout v1.8.10` / `git checkout v1.12.3`），各自使用 `go.mod` 指定的工具链；不预置模块缓存。这个案例的“可确定输出”只来自文件本身，`go list`/`du` 的结果依赖网络、Go 版本与平台，必须由读者实测，本文不填。

**可提前确认的结构输出**：

| 指标 | OLD `v1.8.10` | NEW `v1.12.3` | 来源 |
|---|---:|---:|---|
| `go.mod` require 行 | **51**（39 直接 + 12 间接） | **181**（50 直接 + 131 间接） | `go.mod`；直接数 = 总数 − `// indirect` 数 |
| `go.sum` 行数 | **493** | **1684** | `go.sum` |
| Flux 版本 | `v0.65.1` | `v0.196.1` | `go.mod` |
| InfluxQL 版本 | `v1.1.1-0.20200828144457` | `v1.4.1` | `go.mod` |
| `// indirect` 行 | 12 | 131 | Go 1.13 与 Go 1.17+ 模块图裁剪的差异 |
| SBOM/SPDX/CycloneDX 文件 | 无 | 无 | `find . -maxdepth 3 \( -iname "*sbom*" -o -iname "*.spdx*" -o -iname "*cyclonedx*" \)` 无结果 |

**测量命令与预期形状**：

```bash
git checkout v1.8.10
go list -m all | wc -l          # 待实测：输出一个正整数；本文不填
go mod download && du -sh "$(go env GOMODCACHE)"   # 待实测
git checkout v1.12.3
go list -m all | wc -l          # 待实测；预期大于旧版（模块图裁剪不等于模块数变少）
go mod download && du -sh "$(go env GOMODCACHE)"   # 待实测
```

**本案例的可断言输出只有上表的结构计数**；下面的 `go list -m all | wc -l` 与 `du -sh` 是测量方法，在固定网络/Go 版本/平台之前不构成回归断言。

**为什么不能把 181 当直接依赖**：`go.mod` 的 181 条里有 131 条 `// indirect`，直接依赖是 50；旧版 51 条里 12 条间接，直接依赖 39。模块图裁剪只改变“哪些模块需要写进 `go.mod`”，不改变编译时实际加载的包数。**可复核的动态量**（`go list -m all` 行数、`GOMODCACHE` 体积）标注为“待实测”，不写进结论。

### 6.8 案例：脱离 cross-builder 构建 linux/amd64 会在哪里失败

**输入**：一台普通 Linux，装 Go 1.24.13 与 gcc，但没有 Rust musl 工具链，也没有 `xcc`/`find_rustlib_path`；不设置 `GOOS/GOARCH/CC`。**取证边界**：本文没有 cross-builder 的反例环境，下面的 stderr 是 ld/gcc 的标准失败形态，不是一次实际执行的完整转录；回归时不要把它当固定断言，按步骤 4 的命令自行留证。

**步骤与预期**：

1. `command -v xcc find_rustlib_path`：两者都无输出（命令不存在）。仓库内也没有定义；`grep -rn "find_rustlib_path" .circleci/config.yml` 只命中 `:94`、`:97` 两处调用。
2. `find_rustlib_path x86_64-unknown-linux-musl`：shell 报 `find_rustlib_path: command not found`，命令替换得到空串。
3. 按 `.circleci/config.yml:102-110` 展开 linux 的 flags。`rustlib` 为空时：
   - `-ldflags` 的值是 `-s -w -linkmode=external -extld=<CC> -extldflags "-fno-PIC -static-pie -Wl,-z,stack-size=8388608 -L -Wl,-lunwind"`；
   - `-extldflags` 内部的片段展开为 `-fno-PIC -static-pie -Wl,-z,stack-size=8388608 -L -Wl,-lunwind`（注意 `-L` 后面没有路径，紧跟着 `-Wl,-lunwind`）。
   - 外部链接阶段会失败。可能的报错有两类：gcc 把 `-Wl,-lunwind` 当成 `-L` 的目录，随后报 `_Unwind_*` 未定义；或 `-lunwind` 仍被传给 ld 时先报 `cannot find -lunwind`。**本文不把其中任何一条写成“实测 stderr”**。
4. 要留证，在容器里执行同一条命令并保存完整输出与退出码：

```bash
docker run --rm -it -v "$PWD":/work -w /work golang:1.24.13-bookworm bash -lc '
  set -x
  export CGO_ENABLED=1 GOOS=linux GOARCH=amd64 CC=gcc
  go build -tags="netgo,osusergo,static_build" -buildmode=pie \
    -ldflags="-s -w -linkmode=external -extld=gcc -extldflags \"-fno-PIC -static-pie -Wl,-z,stack-size=8388608 -L -Wl,-lunwind\"" \
    -o /tmp/influxd ./cmd/influxd 2>/tmp/build.err; rc=$?
  cat /tmp/build.err; echo "exit=$rc"
'
```

预期 `rc != 0`，`/tmp/build.err` 含 `cannot find -lunwind` 或 `_Unwind_*` 未定义之一；把这两行与 `go version`、`gcc --version` 一起记入回归证据。**唯一可断言的预期是退出码非 0**；stderr 文本随 gcc/ld 版本变化，只作形态参考。

5. 如果只跑 `go build ./cmd/influxd` 而不带这些 flags：能编出动态链接的二进制（`ldd` 会列出 libc），但与发布产物不一致，不能据此判断发布包能否运行。

**结论**：v1.12.3 的 linux 构建**不是**“装个 Go 就能编”，必须使用 `quay.io/influxdb/cross-builder` 或完整复刻 `xcc`、`find_rustlib_path`、musl/Rust/静态 libc。证据：`.circleci/config.yml:90-110`；`scripts/ci/install-rust.sh` 在 CI 无调用点（§3.3、§7 风险 2）。

### 6.9 案例：旧 Go 或离线工具链构建 1.12.3

**输入**：同一棵 `f3d5cb68ab` 工作树；本机分别装 Go 1.21.x / 1.22.x / 1.23.x / 1.24.0 / 1.24.13；`go.mod:3` 是 `go 1.24.0`，`go.mod:5` 是 `toolchain go1.24.13`。命令固定为 `go build ./cmd/influxd`（不带 CI 的 CGO/Rust flags）。

**步骤与预期**：

| # | 本机 Go | `GOTOOLCHAIN` | 预期首行 stderr / 结果 | 退出码 | 说明 |
|---|---|---|---|---|---|
| 1 | 1.21.x | `local` | `go: go.mod requires go >= 1.24.0 (running go 1.21.x; GOTOOLCHAIN=local)` | 1 | 构建不开始；版本号随本机小版本变化 |
| 2 | 1.22.x | `local` | 同上，`running go 1.22.x` | 1 | 同上 |
| 3 | 1.23.x | `local` | 同上，`running go 1.23.x` | 1 | 同上 |
| 4 | 1.21.x–1.23.x | `auto`（默认） | `go: downloading go1.24.13 (linux/amd64)`，随后成功切换 | 0（有网） | 无网/代理不允许时下载失败；失败文本随网络栈变化，本文不填 |
| 5 | 1.24.0 | `local` | 无错误；使用 1.24.0 编译 | 0 | `toolchain go1.24.13` 在 local 策略下不强制；需 gcc/CGO 环境 |
| 6 | 1.24.13 | `local` | 无错误；使用 1.24.13 编译 | 0 | 与 CI 工具链一致；仍需 §6.8 的 Rust/CGO 才能复刻发布产物 |
| 7 | 1.24.13 | `auto` | 无错误；无需下载 | 0 | 本机版本已满足 |

上表的错误首行是 Go 1.21+ 的标准格式；`running go <version>` 的具体版本字符串由本机决定，不要把它写成固定值。**离线场景**：`GOTOOLCHAIN=auto` 在无法访问 `proxy.golang.org` 或工具链下载源时失败，具体错误包含 `go: downloading go1.24.13` 与网络错误；本文不伪造完整文本。

**结论**：升级评估必须同时确认“本机 Go 版本、`GOTOOLCHAIN` 策略、是否能访问 `proxy.golang.org`/工具链下载源”。离线环境应预置 `go1.24.13`，或把 `GOTOOLCHAIN=local` 与本机 1.24.13 一起纳入镜像；只满足 `go 1.24.0` 的 1.24.0 工具链能编，但不是 CI 使用的版本。证据：`go.mod:3,5`；`.circleci/config.yml:10`。


---

**返回**：[主文档](07_build_perf_diff.md) · [附录 B：性能、可观测性与测试](07b_perf_appendix.md)
