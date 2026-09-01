# TSM Tombstone 延迟应用设计

## 1. 背景

当前 `NewTSMReader` 在构造 Reader 时同步调用 `applyTombstones()`。`FileStore.Open()` 会为 shard 下的所有 TSM 文件创建 Reader，因此当以下条件同时存在时，shard Open 会被明显拉长：

- TSM 文件数量多；
- 多个 TSM 都带有 tombstone 文件；
- tombstone 文件大或记录数量多；
- `applyTombstones()` 需要解析大量记录并更新 indirect index。

本版本只使用 TSI，不考虑 inmem index。目标是把历史 tombstone replay 移出正常 shard Open 关键路径，同时保证查询、compaction 和在线 DELETE 的正确性。

## 2. 已确认的代码事实

### 2.1 apply 的实际作用

`applyTombstones()` 不修改 TSM block，而是把磁盘 tombstone 物化到 Reader 的 indirect index：

- full tombstone：从 `index.offsets` 移除整个 composite key；
- partial tombstone：将删除时间范围写入 `index.tombstones`；
- 查询 cursor 和 compaction iterator 再根据这些结构跳过 block 或过滤点。

因此，“完成 Ensure”只表示 Reader 已经知道哪些 key 和时间范围被删除，不表示所有读取 API 会自动过滤 partial tombstone。

### 2.2 V4 已有增量游标

V4 tombstone 使用 `lastAppliedOffset`：

- 第一次 `Walk()` 从 header 后开始读取；
- 后续 `Walk()` 从 `lastAppliedOffset` 继续；
- 在线 DELETE Commit 后只 replay 新追加的 gzip member。

它不是持久化游标。Reader 重建后：

- `lastAppliedOffset` 从 0 开始；
- indirect index 也是重新从 TSM index 创建；
- 所以历史 tombstone 必须重新 replay。

这解释了为什么启动时是“重头读”，而同一个 Reader 生命周期内的多次 DELETE 可以增量读。

### 2.3 `ApplyTombstonesBulk` 不是第一优先级

批量更新可以减少 offsets Pack、排序或 map 更新次数，但不能消除 shard Open 上的 tombstone 文件读取、解压和遍历。第一阶段应先移动执行时机，再根据 profile 判断是否需要 Bulk 优化。

## 3. 目标与非目标

### 3.1 目标

1. 正常 TSI shard Open 不 replay 历史 tombstone。
2. 查询不返回 full 或 partial tombstone 已删除的数据。
3. compaction 不把已删除数据写回新 TSM。
4. 在线 DELETE 保持 V4 增量应用语义。
5. 同一 Reader 的查询、compaction 和 DELETE 并发时，apply 状态与 `lastAppliedOffset` 正确。
6. tombstone apply 失败后 fail closed，不允许读取未过滤数据。

### 3.2 非目标

- 不支持 inmem index。
- 不改变 V4 tombstone 磁盘格式。
- 不持久化 indirect index 或 `lastAppliedOffset`。
- 第一阶段不实现后台全 shard 预热。
- 第一阶段不实现 `ApplyTombstonesBulk`。
- 不把 export、`influx_inspect`、digest 等独立工具改成 deferred。

## 4. 方案选择

### 4.1 选择：Open deferred，查询入口按 shard Ensure

第一阶段采用：

```text
FileStore.Open        -> 不 apply
首次实际查询          -> Ensure 当前 shard 的全部 Reader
实际 compaction       -> Ensure 当前 compaction group
在线 DELETE Commit    -> 完成首次或增量 apply
```

选择查询入口全量 Ensure，而不是直接在 `KeyCursor.locations()` 内 Ensure，原因是：

- `locations()` 当前在 `FileStore.mu.RLock` 内执行；
- 大 tombstone replay 不能在持有 FileStore 锁时进行；
- `KeyCursor()` 当前不能返回初始化错误；
- 部分现有 cursor 代码会忽略后续 `Read*Block()` 错误；
- `CreateIterator()` 和 `CreateCursorIterator()` 可以立即、可靠地返回 Ensure 错误。

代价是第一次查询某个 shard 时会应用该 shard 的全部 Reader，而不是只应用目标 key 所在 Reader。这个代价符合“Open 快、首次使用付费、后续正常”的目标，并显著减少第一阶段改动面。

### 4.2 未选择：在 locations 内直接 Ensure

直接修改 `locations()` 会形成：

```text
FileStore.mu.RLock
  -> Reader.deleteMu.Lock
     -> Tombstoner.Walk 大文件
```

它未必立即死锁，但会在整个 replay 期间占用 FileStore 读锁，阻塞 `Replace`、文件切换和其他需要 `FileStore.mu.Lock` 的操作，因此不采用。

### 4.3 后续可选：候选 Reader 精细化

如果首次查询全 shard apply 仍然过慢，可在第二阶段改为：

```text
FileStore.mu.RLock
  -> 使用 raw index 筛选候选 Reader
  -> Ref
FileStore.mu.RUnlock

候选 Reader Ensure

基于同一批已 Ref Reader 创建 locations
```

raw index 是 apply 后 index 的超集，因此不会漏掉 full tombstone 尚未物化的 key。

该方案必须同时处理：

- `KeyCursor` 初始化错误传播；
- generated cursor 中被忽略的读取错误；
- Reader Ref/Unref 所有权；
- FileStore 文件集合快照语义。

第一阶段不引入这组改造。

## 5. Reader 构造策略

### 5.1 默认保持 eager

为 `NewTSMReader` 增加 option：

```go
WithDeferredTombstoneApply(true)
```

行为：

- `NewTSMReader(file)`：保持当前同步 apply，构造成功后状态为 `Applied`；
- `FileStore.Open()`：显式传入 deferred option，构造成功后状态为 `Unapplied`；
- export、`influx_inspect`、digest 等直接调用者继续默认 eager；
- compaction、snapshot 或 cache flush 新产生且没有历史墓碑的 Reader 为 `Applied`。

只允许 `FileStore.Open()` 创建 deferred Reader。运行期由 `Replace` 安装的新 Reader 必须是 `Applied`，这是“查询 Ensure 与随后 KeyCursor 之间允许文件集合变化”的重要不变量。

`TSMFile` 内部接口需要增加 `EnsureTombstonesApplied() error`，使 FileStore 级快照可以统一调用；测试 mock 同步实现该方法。不要依赖具体类型断言静默跳过 Ensure。

### 5.2 不全局改默认值

不能把 `NewTSMReader` 全局改成默认 deferred。大量工具和内部流程直接使用 `KeyCount()`、`KeyAt()`、`Entries()`、`BlockIterator()`，它们没有统一的错误返回和 Ensure 门禁。

## 6. Reader 状态机

### 6.1 状态定义

```go
type tombstoneApplyState uint32

const (
    tombstoneUnapplied tombstoneApplyState = iota
    tombstoneApplied
    tombstoneFailed
)
```

推荐字段：

```go
type TSMReader struct {
    // Existing fields...

    tombstoneState atomic.Uint32
    tombstoneErr   error
}
```

状态转换：

```text
deferred constructor -> Unapplied
eager constructor    -> Applied

Unapplied --apply success--> Applied
Unapplied --apply failure--> Failed
Applied   --DELETE success-> Applied
Applied   --DELETE failure-> Failed
Failed    -----------------> Failed
```

`Failed` 在第一阶段是 Reader 生命周期内终态。修复磁盘文件后需要重新打开 shard，不自动重试。

### 6.2 为什么不能只靠锁

纯 `deleteMu` 加普通 bool 可以保证互斥，但每次查询都要竞争 mutex。原子状态提供：

- apply 后查询的无锁快速判断；
- `Unapplied`、`Applied`、`Failed` 的明确语义；
- 没有 tombstone 文件时也能正确记为 `Applied`。

不能用 `lastAppliedOffset == 0` 代替状态，因为以下情况 offset 都可能为 0：

- 尚未 apply；
- tombstone 文件不存在；
- 空 tombstone；
- 非 V4 格式。

### 6.3 为什么不用 `sync.Once`

首次 apply 可能由查询、compaction 或在线 DELETE 触发；首次完成后，DELETE 仍需继续 apply 新追加的 V4 member。`sync.Once` 无法同时表达“初始只执行一次”和“后续允许增量执行”。

### 6.4 Ensure 方法

```go
func (r *TSMReader) EnsureTombstonesApplied() error {
    switch tombstoneApplyState(r.tombstoneState.Load()) {
    case tombstoneApplied:
        return nil
    case tombstoneFailed:
        return r.tombstoneErr
    }

    r.deleteMu.Lock()
    defer r.deleteMu.Unlock()

    switch tombstoneApplyState(r.tombstoneState.Load()) {
    case tombstoneApplied:
        return nil
    case tombstoneFailed:
        return r.tombstoneErr
    }

    return r.applyTombstonesLocked()
}
```

锁内二次检查保证多个查询、查询与 compaction、查询与 DELETE 同时发现 `Unapplied` 时只有一个流程执行首次 replay。

### 6.5 锁内 apply

```go
func (r *TSMReader) applyTombstonesLocked() error {
    // Caller holds r.deleteMu.
    if err := r.applyTombstones(); err != nil {
        r.tombstoneErr = err
        r.tombstoneState.Store(uint32(tombstoneFailed))
        return err
    }

    r.tombstoneErr = nil
    r.tombstoneState.Store(uint32(tombstoneApplied))
    return nil
}
```

只有完整 `Walk()` 和 index 更新成功后才能设置 `Applied`。失败后必须保存错误并设置 `Failed`，不能让查询绕过损坏的 tombstone。

## 7. 三把锁的职责

之前把 `deleteMu` 描述为“保护 `index.offsets` 和 `index.tombstones`”并不准确。正确职责是：

| 锁 | 负责内容 |
|---|---|
| `indirectIndex.mu` | `offsets`、`tombstones` map 等 indirect index 内部读写安全 |
| `Tombstoner.mu` | tombstone 文件、pending writer、内存 tombstone 和 `lastAppliedOffset` |
| `TSMReader.deleteMu` | DELETE、首次 apply、后续增量 apply 和 Reader 状态转换的流程串行化 |

一次 apply 是跨组件操作：

```text
读取 tombstone
  -> 推进 lastAppliedOffset
  -> 修改 indirect index
  -> 更新 Reader apply 状态
```

`indirectIndex.mu` 只能保证 index 内部没有数据竞争，不能保证上面整个复合流程的逻辑一致性。因此查询 Ensure、compaction Ensure 和在线 DELETE Commit 仍需复用 `deleteMu`。

apply 路径的锁顺序为：

```text
TSMReader.deleteMu
  -> Tombstoner.mu
     -> indirectIndex.mu
```

禁止在持有 `FileStore.mu` 时执行长时间 tombstone apply。

## 8. 查询流程

### 8.1 实际查询门禁

两个能够返回错误的查询入口都必须先调用 FileStore 级 Ensure：

- `Engine.CreateIterator()`；
- `Engine.CreateCursorIterator()`。

推荐流程：

```text
Engine query entry
  |
  +-- FileStore.EnsureTombstonesApplied
  |     +-- f.mu.RLock
  |     +-- 检查 newReadersBlocked
  |     +-- 对当前 Readers Ref
  |     +-- f.mu.RUnlock
  |     +-- 逐个 Reader.EnsureTombstonesApplied
  |     `-- 对 Readers Unref
  |
  `-- 现有 KeyCursor 创建流程
        +-- f.mu.RLock
        +-- locations
        +-- location Readers Ref
        `-- f.mu.RUnlock
```

FileStore Ensure 不应在持有 `f.mu` 时等待 Reader `deleteMu`。Reader Ref 用于防止 apply 过程中文件被 Unmap 或关闭。

查询入口每次可以遍历 Readers，但已完成的 Reader 只做一次原子状态读取，不再打开 tombstone 文件，也不竞争 `deleteMu`。第一阶段不增加 FileStore 全局 ready cache，避免在线 DELETE apply 失败后 aggregate 状态与 Reader 状态不一致。

### 8.2 KeyCursor 的 full 与 partial 处理

Ensure 完成后：

- full tombstone key 已从 offsets 移除，`locations()` 不会为它建立 block location；
- 完全被 partial tombstone 覆盖的 block 在 `locations()` 中被跳过；
- 部分覆盖的 block 仍需 Decode，随后由 `KeyCursor.Read*Block()` 的 `excludeTombstones*()` 过滤点。

因此不能把“Ensure 已完成”等同于“所有 Read API 都已经过滤 partial tombstone”。

### 8.3 不在 Reader getter 内隐式 Ensure

不要在以下方法内部自动 Ensure：

- `KeyCount()`；
- `KeyAt()`；
- `Seek()`；
- `ReadEntries()`；
- `BlockIterator()`；
- `Stats()` / `ExtStats()`。

原因：

- 部分接口不能返回错误；
- compaction planner 会间接调用这些接口；
- 隐式 Ensure 会让 planner 或其他枚举流程批量触发 apply；
- 调用方可能已经持有 `FileStore.mu`，导致长锁链。

门禁必须显式放在能够处理错误的上层流程。

## 9. `FileStore.Read` 的特殊处理

### 9.1 已确认的问题

`FileStore.Read()` 调用 `TSMReader.Read()`，最终进入 `mmapAccessor.read()`：

```text
index.Entry(key, timestamp)
  -> readBlock
  -> DecodeBlock
  -> 直接返回整个 block
```

当前路径没有读取 `TombstoneRange()`，因此即使 Ensure 已完成，partial tombstone 区间内的点仍会返回。这是现有 eager 模式下已经存在的行为缺口。

当前仓库内正常查询不使用 `FileStore.Read()`，而是使用会过滤 partial tombstone 的 KeyCursor；但既然 `FileStore.Read()` 是可调用 API，就应独立修复。

### 9.2 修正方案

`FileStore.Read()` 先在 `f.mu` 外调用 FileStore Ensure：

```text
FileStore.Read
  -> FileStore.EnsureTombstonesApplied
  -> f.mu.RLock
  -> Contains / TSMReader.Read
  -> f.mu.RUnlock
```

同时让 `mmapAccessor.read()` 与 `readAll()` 保持一致：

```go
values, err := m.readBlock(entry, nil)
if err != nil {
    return nil, err
}
for _, tr := range m.index.TombstoneRange(key) {
    values = Values(values).Exclude(tr.Min, tr.Max)
}
return values, nil
```

这样：

- full tombstone 通过 `Entry == nil` 过滤；
- partial tombstone 在 Decode 后按时间范围过滤；
- direct eager Reader 的 `Read()` 也获得正确语义。

## 10. Compaction 流程

### 10.1 Planner 不 apply

Compaction planner 只需要：

- 文件大小；
- generation / sequence；
- TSM 时间范围；
- tombstone 文件是否存在；
- compaction level 与策略统计。

Planner 不应触发 Ensure。否则 scheduler 启动后可能立即物化大量 Reader，抵消 deferred Open 的收益。

### 10.2 实际 compaction 必须 apply

实际 compaction 在创建 `NewTSMBatchKeyIterator` 前 Ensure 当前 compaction group：

```text
Compactor.compact
  |
  +-- FileStore.TSMReader(path)
  |     +-- f.mu.RLock
  |     +-- Reader.Ref
  |     `-- f.mu.RUnlock
  |
  +-- Reader.EnsureTombstonesApplied
  |
  +-- NewTSMBatchKeyIterator
  |     +-- full tombstone key 已从 offsets 移除
  |     `-- partial tombstone points 被过滤
  |
  +-- 写入新 TSM
  `-- FileStore.Replace
```

如果 compaction 不 apply：

- iterator 会读取 raw offsets；
- 已删除的 key 或 point 会被写入新 TSM；
- Replace 会删除旧 TSM 和旧 tombstone；
- 删除数据将永久复活。

所以实际 compaction 门禁是 P0 正确性要求。apply 失败必须中止 compaction，并保留旧 TSM 和 tombstone。

## 11. 在线 DELETE

### 11.1 不在 DELETE 扫描前 Ensure

现有 BatchDelete 生命周期已经持有 `deleteMu`：

```text
BatchDelete
  +-- deleteMu.Lock
  +-- KeyCount / Seek / KeyAt
  +-- Tombstoner.AddRange
  `-- Commit
        +-- Flush
        +-- applyTombstones
        `-- deleteMu.Unlock
```

不需要在扫描 raw index 前额外 Ensure：

- raw index 是 apply 后 index 的超集；
- 可能多扫描已经删除的 key，最多产生冗余墓碑；
- 不会漏掉仍存在且需要删除的 key；
- Commit 会 replay 历史墓碑和本次新增墓碑。

### 11.2 Commit 复用锁内方法

`batchDelete.Commit()` 已经持有 `deleteMu`，不能再次调用会加锁的 `EnsureTombstonesApplied()`，否则会自锁。应直接调用 `applyTombstonesLocked()`：

```go
func (b *batchDelete) Commit() error {
    defer b.r.deleteMu.Unlock()

    if err := b.r.tombstoner.Flush(); err != nil {
        return err
    }
    return b.r.applyTombstonesLocked()
}
```

状态语义：

- `Unapplied`：Commit 从初始 offset replay 历史和新增墓碑；
- `Applied`：Commit 根据 V4 `lastAppliedOffset` 只 replay 新增 member；
- Commit 成功：状态为 `Applied`；
- apply 失败：状态为 `Failed`。

### 11.3 Failed Reader 拒绝继续删除

进入 `BatchDelete()` 并持有 `deleteMu` 后，必须先检查 Reader 状态。如果已经是 `Failed`，在 `Tombstoner.AddRange()` 前返回保存错误，不能继续追加 tombstone。

否则会出现“删除已落盘，但当前 Reader 永远无法应用”的新状态，破坏 fail-closed 语义。

未被查询或 compact 的 Reader 第一次在线 DELETE 可能承担历史 tombstone replay 延迟，这是把成本移出 shard Open 后的预期行为。

## 12. TSI 下的 WalkKeys

本版本不使用 inmem index，因此 `WalkKeys` 不属于正常 shard Open 的 P0 门禁。

TSI 且 `fields.idx` 正常、非空时，`LoadMetadataIndex()` 会直接返回，不扫描 TSM。

仍有两个低优先级兜底场景：

1. `fields.idx` 丢失、损坏或为空时，即使使用 TSI，也会通过 `WalkKeys()` 重建字段和 series metadata；
2. `cleanupMeasurement()` 会通过 `WalkKeys()` 判断 measurement 是否仍有数据。

如果这些流程在 deferred Reader 上执行，未 apply 的 full tombstone key 可能被枚举：

- metadata rebuild 可能恢复已删除的字段或 series metadata；
- cleanup 可能误判 measurement 仍有数据，从而留下陈旧 metadata。

因此 `WalkKeys` 降级为 P2 兼容性门禁：正常 TSI 加载不触发；只有异常恢复或 metadata cleanup 真正调用它时才 Ensure。

## 13. 独立工具与其他流程

### 13.1 Export 和 influx_inspect

它们是独立工具，优先级较低，并继续使用默认 eager `NewTSMReader`。第一阶段不修改工具调用链。

### 13.2 Snapshot 与普通 backup

直接 hardlink 或复制 TSM 与 tombstone 文件不依赖 indirect index，无需 Ensure。

### 13.3 Digest、dump 和验证工具

继续默认 eager，保证直接调用 Reader getter 时看到 tombstone 已物化后的 index。

## 14. 最终门禁矩阵

| 优先级 | 流程 | 是否 Ensure/apply | 门禁位置 |
|---|---|---:|---|
| P0 | `FileStore.Open` | 否 | 仅创建 deferred Reader |
| P0 | 普通查询 | 是 | `Engine.CreateIterator()`，按 shard 全量 Ensure |
| P0 | Cursor 查询 | 是 | `Engine.CreateCursorIterator()`，按 shard 全量 Ensure |
| P0 | 实际 compaction | 是 | 创建 batch iterator 前，只 Ensure 当前 group |
| P0 | 在线 DELETE | 是 | 由 `batchDelete.Commit()` 完成首次或增量 apply |
| P1 | `FileStore.Read` | 是 | 获取 `f.mu` 前 Ensure，并修复 partial 过滤 |
| P2 | TSI metadata rebuild | 是 | 真正执行 `WalkKeys()` 前 |
| P2 | `cleanupMeasurement` | 是 | 真正执行 `WalkKeys()` 前 |
| 不触发 | compaction planner | 否 | 只使用 raw stats 与 tombstone existence |
| 不触发 | Cost 估算 | 否 | 允许使用 raw index 近似值 |
| 不触发 | snapshot / 普通 backup | 否 | 复制或链接文件 |
| 保持 eager | export / inspect / digest | 构造时 apply | 独立工具保持现状 |

## 15. 并发场景

### 15.1 两个查询同时首次进入 shard

两个查询都会在 FileStore 锁外调用 Reader Ensure：

- 第一个查询获得某个 Reader 的 `deleteMu` 并 replay；
- 第二个查询等待相同 Reader；
- 第二个获得锁后通过二次状态检查直接返回；
- 不重复读取 tombstone。

### 15.2 查询与 compaction 并发

对同一 Reader：

- 都调用 Ensure；
- 由 Reader `deleteMu` 串行；
- 只有一个执行首次 replay。

对不同 Reader 可以并行 apply。第一阶段不增加全局 limiter；如果并发首次查询造成 I/O 峰值，再根据指标增加限制。

### 15.3 查询与在线 DELETE 并发

对同一 Reader：

- 查询 Ensure 与 BatchDelete 共用 `deleteMu`；
- DELETE 先执行时，Commit 完成历史和新增 replay，查询随后看到 `Applied`；
- 查询先执行时，DELETE 随后只 replay 新增 V4 member；
- `lastAppliedOffset` 与 Reader 状态不会交错更新。

### 15.4 FileStore Replace 与查询 Ensure

FileStore Ensure 在 `f.mu` 内只做快照和 Ref，然后释放锁。Ref 保证旧 Reader 在 apply 期间不会被关闭。

运行期 Replace 安装的新 Reader 必须已经是 `Applied`。因此即使 Replace 发生在查询 Ensure 与 KeyCursor 创建之间，也不会引入新的 `Unapplied` Reader。

## 16. 错误处理

### 16.1 查询

- 任一 Reader apply 失败，查询入口立即返回错误；
- Reader 状态变为 `Failed`；
- 后续查询返回保存的相同错误；
- 不允许退化为读取 raw TSM 数据。

### 16.2 Compaction

- 中止当前 compaction；
- 不安装新 TSM；
- 保留旧 TSM 与 tombstone；
- 释放 Reader Ref 和 compaction plan。

### 16.3 DELETE

- Flush 失败：返回错误并释放 `deleteMu`；
- Flush 成功但 apply 失败：磁盘 tombstone 保留，Reader 进入 `Failed`；
- Reader 已为 `Failed`：在 AddRange 前拒绝新 DELETE；
- 不允许把部分成功的 apply 标记为 `Applied`。

## 17. 可观测性

建议记录：

- Reader 初始模式：eager / deferred；
- 首次 apply 触发来源：query / compaction / delete / walk_keys；
- tombstone 压缩字节数和记录数；
- apply 耗时；
- 等待同一 Reader apply 的时间；
- apply 成功与失败计数；
- shard Open 跳过 apply 的 Reader 数量；
- 首次查询 Ensure 的 Reader 数量和总耗时。

日志至少包含 shard、TSM path、触发来源和耗时，不为每条 tombstone 打日志。

## 18. 测试设计

### 18.1 Reader 状态

- eager Reader 构造时 apply，初始状态为 `Applied`；
- deferred Reader 构造时不 Walk，初始状态为 `Unapplied`；
- 第一次 Ensure 应用 full 和 partial tombstone；
- 并发 Ensure 只执行一次全量 replay；
- 无 tombstone 文件也进入 `Applied`；
- apply 失败进入终态 `Failed`；
- 热路径不重新打开 tombstone 文件。

### 18.2 查询入口与锁

- `CreateIterator()` 在创建 KeyCursor 前 Ensure；
- `CreateCursorIterator()` 在返回 iterator 前 Ensure；
- Ensure 时不持有 `FileStore.mu`；
- Apply 期间 Replace 可以等待 Reader Ref，但不被 FileStore 读锁长期阻塞；
- Ensure 错误从查询入口返回；
- 后续查询只走 Reader 原子状态快路径。

### 18.3 KeyCursor 正确性

- full tombstone key 不生成 locations；
- 完全覆盖的 block 被跳过；
- partial tombstone points 在 Decode 后被过滤；
- asc/desc cursor 均正确；
- float、integer、unsigned、string、boolean 和 array cursor 均覆盖。

### 18.4 FileStore.Read

- full tombstone 返回空；
- partial tombstone 删除区间内的点不返回；
- 同一个 block 内未删除点仍返回；
- Ensure 在获取 `f.mu.RLock` 前完成；
- tombstone apply 失败返回错误。

### 18.5 Compaction

- planner 不触发 apply；
- 实际 compaction 只 Ensure 当前 group；
- full tombstone key 不写入新 TSM；
- partial tombstone points 不写入新 TSM；
- apply 失败中止 compaction；
- Replace 后删除数据不复活。

### 18.6 DELETE

- `Unapplied` Reader 首次 Commit replay 历史和新增墓碑；
- `Applied` Reader Commit 只 replay 新增 V4 member；
- Query Ensure 与 DELETE Commit 并发时不重复 apply、不发生 offset 错位；
- Commit 不会重复获取 `deleteMu`；
- `Failed` Reader 在 AddRange 前拒绝 DELETE，tombstone 文件不继续增长。

### 18.7 TSI WalkKeys 兜底

- 正常 TSI 且有效 `fields.idx` 时 shard Open 不触发 WalkKeys apply；
- `fields.idx` 重建前能够正确 Ensure；
- metadata rebuild 不恢复 full tombstone key；
- cleanupMeasurement 不被已删除 key 阻止。

### 18.8 性能验证

覆盖：

- 无 tombstone；
- 少量 tombstone；
- 大量 full tombstone；
- 大量 partial tombstone；
- 大量 TSM 且每个文件都有 tombstone；
- shard Open 前后对比；
- 首次查询延迟；
- 后续查询延迟；
- 首次 compaction prepare 延迟。

主要验收指标：

- 大 tombstone 不再显著增加正常 `FileStore.Open()` 时间；
- apply 后查询没有 tombstone 文件 I/O；
- 查询不返回 partial tombstone 点；
- compaction 后删除数据不复活；
- 在线 DELETE 保持 V4 增量读取。

## 19. 分阶段实施

### Phase 1：Reader 状态与 Open

1. 增加 deferred Reader option；
2. eager/deferred 初始化正确状态；
3. 增加 `EnsureTombstonesApplied()`；
4. 拆分 `applyTombstonesLocked()`；
5. 复用 `deleteMu` 串行首次和增量 apply；
6. `FileStore.Open()` 单独启用 deferred。

### Phase 2：P0 门禁

1. 增加 FileStore 级 Ensure 快照与 Ref 流程；
2. `CreateIterator()` 增加查询门禁；
3. `CreateCursorIterator()` 增加查询门禁；
4. 实际 compaction 在 iterator 创建前 Ensure group；
5. `batchDelete.Commit()` 使用锁内 apply；
6. Failed Reader 在 AddRange 前拒绝 DELETE。

### Phase 3：Read 与 TSI 兜底

1. `FileStore.Read()` 在 FileStore 锁外 Ensure；
2. `mmapAccessor.read()` 增加 partial tombstone 过滤；
3. TSI metadata rebuild 的 WalkKeys 前 Ensure；
4. cleanupMeasurement 的 WalkKeys 前 Ensure。

### Phase 4：观测与可选优化

1. 增加 apply 来源、耗时和等待指标；
2. 增加 shard Open 与首次查询 benchmark；
3. 根据 profile 决定是否实现 `ApplyTombstonesBulk`；
4. 根据首次查询延迟决定是否实现候选 Reader 精细化；
5. 必要时增加跨 Reader apply 并发 limiter；
6. 长期可考虑持久化 tombstone 物化状态或 sidecar index。

## 20. 结论

最终方案不是简单地“从 `NewTSMReader` 删除 apply，然后在 `locations()` 里补一次”。正确边界是：

```text
正常 TSI shard Open
  -> 创建 deferred Reader
  -> 不读取历史 tombstone

查询入口
  -> FileStore 锁内快照并 Ref
  -> FileStore 锁外 Ensure 全部 Reader
  -> 现有 KeyCursor 创建与 partial 过滤

实际 compaction
  -> Ref 当前 group
  -> Ensure group
  -> 创建 batch iterator

在线 DELETE
  -> deleteMu 下 AddRange / Flush
  -> Commit 完成首次或增量 apply
```

`indirectIndex.mu` 负责 index 内存结构，`Tombstoner.mu` 负责 tombstone 文件与 `lastAppliedOffset`，`deleteMu` 负责跨组件 apply/delete 流程串行化。

`FileStore.Read()` 必须单独补充 partial tombstone 过滤；`WalkKeys` 在纯 TSI 部署中只作为异常恢复和 metadata cleanup 的低优先级门禁。这样可以在不影响正常 shard Open 的前提下，保证查询、compaction 和在线 DELETE 的正确性。
