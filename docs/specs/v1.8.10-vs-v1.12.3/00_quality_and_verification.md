# 质量基线与验证说明

> 本页说明这组版本对比文档的**证据基线、单一事实来源、阅读路径、自动校验方法与已知边界**。  
> 基线：OLD `v1.8.10`=`688e697c51`；NEW `v1.12.3`=`f3d5cb68ab`（当前工作树代码与 NEW 相同）。

## 1. 文档地图

| 文档 | 主题 | 建议读者 |
|---|---|---|
| [readme](readme.md) | 版本身份、全局变更图、Top10、升级影响 | 所有人，先读 |
| [01 写入链路](01_write_path_diff.md) | WriteContext、fields.idx/idxl、WAL/Cache、snapshot | 写入/存储 |
| [02 索引与 Series](02_index_series_diff.md) | TSI、SeriesFile、SIDX/SSEG、删除与压缩 | 索引/存储 |
| [03 查询引擎](03_query_engine_diff.md) | query/control、storage reads、cursor、Flux 下推 | 查询 |
| [04 存储与 Compaction](04_storage_compaction_diff.md) | 4→5 级、planner/scheduler、Replace、purger | 存储/运维 |
| [05 元数据与后台](05_meta_background_diff.md) | meta commit、retention、CQ、snapshotter、lease | 元数据/运维 |
| [06 HTTP 与 Flux](06_http_flux_diff.md) | 路由、中间件、TLS/SIGHUP、Flux、限流 | API/运维 |
| [07 构建与性能](07_build_perf_diff.md) | Go/依赖、CI/打包/发布、性能优化分组 | 构建/发布 |
| [05a 元数据/后台细节](05a_details_appendix.md) | 05 的完整 §3 细节与长案例 | 需要逐行核对时 |
| [06a HTTP/Flux 细节](06a_details_appendix.md) | 06 的完整 §3 细节与请求/响应案例 | 需要逐行核对时 |
| [07a 构建/发布细节](07a_build_release_appendix.md) | 构建、CI、打包、发布、分发完整细节 | 构建/发布深入 |
| [07b 性能细节](07b_perf_appendix.md) | 18 项优化、计数推导与性能案例 | 性能深入 |

> **阅读约定**：05/06/07 的主文档保留全部 §3 标题编号与 1–2 句结论；完整代码、表格、证据和长案例在对应附录。跨文档引用 `05 §3.x` 等仍然有效，读者从主文档标题即可跳到附录。

## 2. 单一事实来源（Single Source of Truth）

同一事实只在一篇展开；其他篇应**链接而不是复述**。修改时先改 owner，再检查引用方。

| 事实 | Owner | 其他篇应如何引用 |
|---|---|---|
| line protocol 解析、tag 排序、field 类型推断 | 13 | 01/02 只引用，不复述状态机 |
| series key / SeriesFile / SSEG / SIDX / RHH | 14 | 02 讲 TSI 侧；01/13 引用 |
| TSI LogFile/IndexFile/Partition、TSI compaction | 02 | 14 讲 SeriesFile；04 引用 |
| TSM 四段格式、block/index/footer、BitReader、Digest | 17 | 01/04/16 只引用格式细节 |
| FileStore、KeyCursor、Replace、purger | 16 | 04 讲 compaction 策略；01 引用 |
| 4→5 级 compaction、planner、scheduler、L5 limiter/holdoff | 04 | 01/07/readme 只引用 |
| WAL/Cache/snapshot、fields.idx/idxl | 01 | 02/04/15 引用 |
| meta clone-modify-commit、snapshot、lease、认证 | 07 | 05/12 引用 |
| retention、precreator、shard 生命周期 | 12 | 05 讲差异；06/07 引用 |
| CQ 调度/写回 | 05 | 07/12 引用 |
| HTTP 路由/中间件/状态码 | 10 | 06 讲版本差异；11/03 引用 |
| InfluxQL 编译/执行/Emitter | 11 | 03 讲 iterator；06/09 引用 |
| storage reads / cursor / WindowAggregate | 09 | 03/08 讲 cursor 实现；06 引用 |
| ArrayCursor 逐点实现 | 08 | 03/09 引用 |
| 构建/CI/打包/发布/性能 | 07 | 其他篇不重复构建细节 |

> 如果发现两篇对同一事实说法不同：先回源码确认，再改 owner，最后在引用方改一句话并指向 owner。

## 3. 自动校验

在仓库根运行：

```bash
python docs/specs/v1.8.10-vs-v1.12.3/check_docs.py
```

检查项：

- 8 篇文档的 UTF-8/无 BOM、CRLF、代码围栏成对；
- `v1.8.10:` / `v1.12.3:` 引用的文件存在、行号在范围内；
- 相对 Markdown 链接可达；
- 01–07 的 8 个必需章节齐全；
- H2 标题不重复；
- Mermaid 块数量与空块检查（语法级校验需可选安装 `mermaid`/`jsdom`，见 `verify/`）。

可选的 Mermaid 语法校验（需要 Node 依赖）：

```bash
cd verify && node validate_dir.mjs ../docs/specs/v1.8.10-vs-v1.12.3
```

## 4. 当前质量指标

- 31 篇文档（17 主 + 8 对比主文档 + 4 附录 + 质量页 + 主 readme），约 2.28MB；
- 306 张 Mermaid、1,444 个代码块、1,942 条版本引用；
- 自动校验：版本引用 0 坏引用、链接 0 死链、编码/围栏全通过。

## 5. 已知边界（必须如实告知读者）

1. **语义审查不是自动化测试**：自动脚本只能查结构、引用、编码；代码/流程是否讲对，仍依赖人工或 LLM 复核。
2. **部分案例是推导而非可执行基准**：尤其 07 的 planner 计数、TSI allocs、purger 延迟；文中已标注“推导/待实测”的，不能当作 benchmark。
3. **行号会漂移**：引用以当前基线为准；代码更新后请先跑 `check_docs.py`，再按变化文件重审。
4. **跨文档一致性靠约定**：第 2 节的 owner 表是约定，不是强制检查；发现冲突时以源码为准。
5. **版本边界**：本组文档基于 v1.12.3，不是 v1.8.10；v1.8.10 只作为对比基线。
6. **主文档是摘要层**：05/06/07 主文档的 §3 是“1–2 句结论 + 附录链接”，逐行代码/表格/长案例在附录；不要把摘要当作完整细节。

## 6. 代码更新后的维护流程

1. 更新基线 commit（readme 与各篇头部）；
2. 运行 `check_docs.py`，修掉失效路径/行号/链接；
3. 对发生变化的源码目录，重跑对应篇的“代码/流程/案例”审查；
4. 若修改了共享事实，先改 owner 文档，再改引用方；
5. 需要时运行 Mermaid 语法校验。
