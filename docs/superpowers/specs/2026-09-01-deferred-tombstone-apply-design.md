# TSM Tombstone 延迟应用设计

## 1. 背景

`NewTSMReader` 当前在 mmap TSM 索引后同步调用 `applyTombstones()`。当一个 shard 包含大量 TSM 文件，且对应 tombstone 文件较大时，`FileStore.Open()` 会同时承担：

- tombstone 文件打开与 gzip 解压；
- 墓碑记录遍历；
- `indirectIndex.DeleteRange()` 扫描；
- 全量删除 key 后的 offsets `Pack()`；
- partial tombstone range 的排序和内存构建。

这些工作位于 shard 加载关键路径中，导致 shard Open 时间随 TSM 数量和 tombstone 规模增长。

当前部署只使用 TSI，不涉及 in-memory index。正常情况下 `fields.idx` 有效，`LoadMetadataIndex()` 不需要在 shard Open 阶段全量执行 `WalkKeys()`。

## 2. 目标

1. 将历史 tombstone 的解析和索引物化移出 shard Open 关键路径。
2. 保证查询不会返回已删除数据。
3. 保证 compaction 不会把已删除数据写回新 TSM。
4. 保持在线 DELETE 的增量 tombstone 应用语义。
5. 保持 export、`influx_inspect`、digest 等独立工具的现有行为。
6. 同一个 TSMReader 的首次墓碑应用只能执行一次，并正确处理查询、compaction 和 DELETE 并发。

## 3. 非目标

第一阶段不包含：

- 修改 V4 tombstone 磁盘格式；
- 持久化 `lastAppliedOffset`；
- 实现 `.tombstone.state` 或 V5 物化索引；
- 优化 `prepareV4()` 每次提交时复制旧 tombstone 文件的写放大；
- 实现 `ApplyTombstonesBulk`；
- 修改独立 export 和 `influx_inspect` 工具为 lazy 模式；
- 增加后台 tombstone 预热任务。

## 4. 核心方案

采用“默认 eager、FileStore Open 显式 deferred、首次数据访问时物化”的设计。

```text
Shard Open
  |
  +-- mmap TSM index
  +-- 创建 Tombstoner
  +-- stat tombstone 文件
  +-- Reader 状态 = Unapplied
  `-- 不执行 applyTombstones

Reader 首次被使用
  |
  +-- 查询创建 locations
  +-- 实际 compaction 创建 iterator
  +-- 在线 DELETE Commit
  `-- WalkKeys 全量枚举
          |
          `-- applyTombstones -> Applied
```

### 4.1 NewTSMReader 保持默认 eager

为 `NewTSMReader` 增加 option：

```go
WithDeferredTombstoneApply(true)
```

行为：

- `NewTSMReader(file)`：保持当前同步 apply 行为，构造成功后状态初始化为 `Applied`；
- `FileStore.Open()`：显式传入 deferred option，构造成功后状态初始化为 `Unapplied`；
- export、`influx_inspect`、digest 等直接调用者继续使用默认 eager；
- compaction 或 snapshot 新产生且没有墓碑的 TSMReader 不会产生额外成本。

不能将 `NewTSMReader` 全局改为默认 deferred，否则所有直接使用 `KeyCount()`、`KeyAt()`、`Entries()` 和 `BlockIterator()` 的工具都需要同步改造。

## 5. Reader 状态与锁

### 5.1 状态定义

Reader 维护历史 tombstone 是否已经物化到内存索引：

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

状态描述的是“启动时已有墓碑是否已经物化到 `index.offsets` 和 `index.tombstones`”，不是磁盘 tombstone 文件以后是否还会新增记录。

### 5.2 复用 deleteMu

不新增独立 apply mutex。查询、compaction 和在线 DELETE 复用现有 Reader 级 `deleteMu`，共同保护：

- tombstone 初始全量应用；
- tombstone 后续增量应用；
- `lastAppliedOffset` 对应的索引状态；
- `index.offsets` 的删除与 Pack；
- `index.tombstones` 的构建；
- Reader apply 状态转换。

`Tombstoner.mu` 继续保护 tombstone 文件内部状态和 `lastAppliedOffset`。`deleteMu` 负责更高层的“文件读取 + index 更新 + Reader 状态”的串行化。

### 5.3 EnsureTombstonesApplied

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

    // Double check after acquiring the lock.
    switch tombstoneApplyState(r.tombstoneState.Load()) {
    case tombstoneApplied:
        return nil
    case tombstoneFailed:
        return r.tombstoneErr
    }

    return r.applyTombstonesLocked()
}
```

原子状态读取提供查询热路径。首次 apply 完成后，查询和 compaction 不需要每次竞争 `deleteMu` 或重新打开 tombstone 文件。

锁内二次校验用于处理多个查询或查询与 compaction 同时发现 `Unapplied` 的情况。

### 5.4 锁内应用方法

```go
func (r *TSMReader) applyTombstonesLocked() error {
    // Caller must hold r.deleteMu.
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

只有整个 `Walk()` 和 index 更新成功后才能设置 `Applied`。发生错误时设置 `Failed`，后续查询和 compaction 返回同一个错误，不能绕过墓碑读取未过滤数据。

这里不使用 `sync.Once`。首次应用既可能由查询或 compaction 的 Ensure 触发，也可能由在线 DELETE 的 Commit 触发；而首次应用之后，DELETE 仍需持续应用新增的 V4 gzip member。`sync.Once` 无法同时表达“初始只执行一次”和“后续允许增量执行”这两种语义。

### 5.5 门禁必须显式放在上层流程

不要在 `KeyCount()`、`KeyAt()`、`Seek()`、`ReadEntries()`、`BlockIterator()`、`Stats()` 等 Reader getter 内部隐式调用 Ensure：

- 这些接口中的一部分当前不能返回 apply 错误；
- compaction planner 会通过 `Stats()`、`ExtStats()` 或首 block 统计间接使用这些 getter；
- 一旦 getter 隐式 Ensure，planner 就可能在 shard 加载后批量触发 apply，抵消 deferred Open 的收益。

Ensure 只放在能够明确处理错误、且真正要求墓碑已生效的上层门禁：查询创建 locations、实际 compaction 创建 iterator、`WalkKeys` 枚举，以及在线 DELETE 的 Commit。

## 6. 各流程门禁

### 6.1 最终门禁矩阵

| 优先级 | 流程 | 是否 apply | 门禁位置 |
|---|---|---:|---|
| P0 | `FileStore.Open` | 否 | 创建 deferred Reader |
| P0 | 实际查询 | 是 | 创建 locations 之前 |
| P0 | 实际 compaction | 是 | 创建 `NewTSMBatchKeyIterator` 之前 |
| P0 | 在线 DELETE | 是 | 由 `batchDelete.Commit()` 自然触发 |
| P1 | `WalkKeys` / metadata 重建 | 是 | 枚举所有 Reader 之前 |
| P1 | 状态、日志和指标 | 不适用 | 记录首次 apply 行为 |
| P2 | 查询 Cost 估算 | 可不 apply | 允许使用 raw index 近似值 |
| 暂不修改 | export / `influx_inspect` / digest | 默认 eager | 独立工具保持现状 |
| 不需要 | snapshot / 普通 backup | 否 | 直接 hardlink 或复制文件 |
| 不需要 | compaction planner | 否 | 只读取 Stats 和 HasTombstone |

### 6.2 查询

查询依赖 apply 后的：

- `index.offsets`：全量删除的 key 必须不可见；
- `index.tombstones`：partial tombstone 必须在 block 解码后过滤；
- `ReadEntries()`：不能返回已全量删除 key 的 block。

必须在创建 `locations` 之前完成 apply。不能先缓存 `IndexEntry` 再 apply，因为全量删除只会从 offsets 移除 key，不一定在 `TombstoneRange()` 中保留范围。

推荐流程：

```text
FileStore.KeyCursor(key)
  |
  +-- 在 FileStore 锁内筛选候选 Reader 并 Ref
  +-- 释放 FileStore 锁
  +-- 对候选 Reader 执行 EnsureTombstonesApplied
  +-- 基于 apply 后的 index 创建 locations
  `-- KeyCursor.Close 时 Unref
```

候选 Reader 应使用不依赖墓碑状态的 raw index 精确 key 查询，避免仅根据宽泛的 key range 对大量无关 TSM 执行 apply。

不能持有 `FileStore.mu` 等待 `deleteMu`，否则大 tombstone 的首次查询可能长期阻塞 Replace、snapshot 和文件切换。

`FileStore.Read()` 也直接依赖 `Contains()` 和 `Read()`，需要使用相同门禁。

查询路径必须能够传播 `EnsureTombstonesApplied()` 错误，不能忽略损坏墓碑，也不能退化为读取未过滤数据。

### 6.3 Compaction

Compaction 分为 planner 和实际执行两个阶段。

Planner 只需要：

- 文件大小；
- generation / sequence；
- TSM 时间范围；
- tombstone 文件是否存在。

Planner 不应触发 apply，否则 compaction scheduler 启动后会立即物化全部 Reader，抵消 deferred Open 的收益。

实际执行流程：

```text
Compactor.compact
  |
  +-- FileStore.TSMReader(path)
  |     +-- Ref Reader
  |     +-- 释放 FileStore 锁
  |     `-- EnsureTombstonesApplied
  |
  +-- NewTSMBatchKeyIterator
  |     +-- BlockIterator 跳过 full tombstone key
  |     `-- TombstoneRange 过滤 partial tombstone
  |
  +-- 写入新 TSM
  `-- FileStore.Replace 删除旧 TSM 和 tombstone
```

如果实际 compaction 未 apply：

- full tombstone key 会重新写入新 TSM；
- partial tombstone 范围内的数据会重新写入；
- Replace 删除旧 tombstone 后，删除数据会永久复活。

因此 compaction 门禁是最高优先级的正确性要求。apply 失败时必须中止 compaction，保留旧 TSM 和 tombstone。

### 6.4 在线 DELETE

当前 `BatchDelete()` 在扫描 index 前已经持有 `deleteMu`：

```text
BatchDelete
  +-- deleteMu.Lock
  +-- KeyCount / Seek / KeyAt
  +-- AddRange
  `-- Commit
        +-- Flush
        +-- applyTombstones
        `-- deleteMu.Unlock
```

因此不需要在 index 扫描前额外调用 `EnsureTombstonesApplied()`：

- raw index 是 apply 后 index 的超集；
- 扫描可能多看到已删除 key，最多写入冗余墓碑；
- 不会漏掉仍存在且需要删除的 key；
- Commit 会应用全部历史墓碑和本次新增墓碑。

但进入 `BatchDelete()` 并拿到 `deleteMu` 后，必须先检查 Reader 状态。如果状态已经是 `Failed`，应在调用 `Tombstoner.AddRange()` 之前返回保存的错误，不能继续向 tombstone 文件追加记录。这样 `Failed` 才能保持 Reader 生命周期内的终态，也避免出现“删除已落盘，但当前 Reader 永远无法应用”的新状态。

`batchDelete.Commit()` 已持有 `deleteMu`，不能调用会再次加锁的 `EnsureTombstonesApplied()`。它应直接调用 `applyTombstonesLocked()`：

```go
func (b *batchDelete) Commit() error {
    defer b.r.deleteMu.Unlock()

    if err := b.r.tombstoner.Flush(); err != nil {
        return err
    }

    return b.r.applyTombstonesLocked()
}
```

语义：

- Reader 为 `Unapplied`：`lastAppliedOffset == 0`，Commit 应用历史墓碑和本次新增墓碑；
- Reader 为 `Applied`：Commit 根据 V4 `lastAppliedOffset` 只应用新增 gzip member；
- Reader 为 `Failed`：在 AddRange 前拒绝本次 DELETE，不进入 Commit；
- Commit 成功后状态保持或变为 `Applied`。

这意味着未被查询或 compact 的 Reader，第一次在线 DELETE 可能承担历史墓碑 replay 延迟。这是将成本移出 shard Open 后的预期行为。

### 6.5 WalkKeys 与 metadata

以下操作依赖可见 key 集合，必须在枚举前 Ensure：

- `fields.idx` 丢失或损坏后的 metadata 重建；
- `FileStore.Keys()`；
- measurement cleanup；
- schema / field 类型扫描；
- 导入后的 metadata 重建。

正常 TSI 且 `fields.idx` 有效时，shard Open 会跳过全量 `WalkKeys()`，因此不会抵消 deferred Open 的收益。

如果 `fields.idx` 需要重建，则必须应用所有相关 Reader；该次恢复启动仍然较慢，但这是正确性要求。

### 6.6 Export 与独立工具

export、`influx_inspect`、digest 等是独立工具，优先级较低，第一阶段不修改。

这些流程通过默认 eager 的 `NewTSMReader(file)` 保持当前语义。需要做回归测试，确认 deferred option 只由服务端 `FileStore.Open()` 使用，没有改变直接调用者行为。

## 7. V4 lastAppliedOffset 语义

V4 tombstone 在同一个 Reader 生命周期内已经使用逻辑游标：

```text
首次 apply: 读取 [header, N]，保存 lastAppliedOffset=N
第一次 DELETE: 新 gzip member [N, M]，只读取 [N, M]
第二次 DELETE: 新 gzip member [M, P]，只读取 [M, P]
```

`Walk()` 每次重新打开文件是因为 tombstone 提交通过临时文件和 rename 原子替换，长期持有的文件描述符会指向旧 inode。代码使用 byte offset 而不是长期文件句柄作为逻辑游标。

重启后不能只恢复 offset，因为 `index.offsets` 和 `index.tombstones` 都是内存状态。只恢复 offset 会跳过历史墓碑并暴露已删除数据。

Deferred 方案下：

- shard Open：`lastAppliedOffset == 0`；
- 首次查询、实际 compaction 或在线 DELETE：全量 replay；
- 后续在线 DELETE：只应用新增 member。

## 8. 锁顺序与并发约束

锁顺序必须固定：

1. 在 `FileStore.mu` 下选择并 Ref Reader；
2. 释放 `FileStore.mu`；
3. 获取 Reader `deleteMu`；
4. `Tombstoner.Walk()` 内部获取 `Tombstoner.mu`；
5. 更新 indirect index；
6. 更新 Reader apply 状态；
7. 释放 `deleteMu`。

禁止在持有 `FileStore.mu` 时等待 `deleteMu`。

同一 Reader：

- 查询、compaction 和 DELETE 串行执行首次/增量 apply；
- 多个并发查询只有第一个执行 apply；
- 等待者在获取锁后通过二次状态检查直接返回。

不同 Reader：

- 可以并发 apply；
- 如首次查询引发过高并发，可后续增加全局 limiter；
- 第一阶段不增加后台全 shard 预热，避免大量 shard Open 后形成 I/O 风暴。

## 9. 错误处理

### 9.1 查询

墓碑读取或解压失败时：

- Reader 状态设置为 `Failed`；
- 查询返回错误；
- 不允许读取未过滤的 TSM 数据；
- 后续查询返回保存的相同错误。

### 9.2 Compaction

墓碑应用失败时：

- 中止当前 compaction；
- 不生成或安装新 TSM；
- 保留旧 TSM 和 tombstone；
- 释放 Reader 引用和 compaction plan。

### 9.3 DELETE

- Flush 失败：保持当前错误语义并释放 `deleteMu`；
- Flush 成功但 apply 失败：磁盘墓碑仍然存在，Reader 标记 `Failed`；
- Reader 已为 `Failed`：在 AddRange 前返回已保存错误，不再追加新墓碑；
- 不能在 apply 失败后把 Reader 标记为 `Applied`。

第一阶段将 `Failed` 定义为 Reader 生命周期内终态，需要修复磁盘文件并重新打开 shard。自动重试可作为后续增强，但必须单独定义重试与错误恢复语义。

## 10. 可观测性

建议增加：

- Reader 初始状态：eager / deferred；
- 首次 apply 触发来源：query / compaction / delete / walk_keys；
- tombstone 文件压缩字节数；
- 墓碑记录数；
- apply 耗时；
- 等待同一 Reader apply 的 goroutine 数或等待时间；
- apply 成功/失败计数；
- shard Open 阶段跳过 apply 的 Reader 数量。

日志中应包含 shard、TSM path、触发来源和耗时，但避免为每条墓碑打印日志。

## 11. 测试设计

### 11.1 Reader 单元测试

- deferred Reader 构造时不调用 `Walk()`；
- eager Reader 保持构造时 apply；
- 第一次 Ensure 应用 full 和 partial tombstone；
- 并发 Ensure 只执行一次全量 replay；
- apply 成功后热路径不获取 tombstone 文件；
- apply 失败进入 `Failed` 并返回稳定错误；
- 没有 tombstone 文件时也能进入 `Applied`，不能用 `lastAppliedOffset == 0` 判断状态。

### 11.2 查询测试

- full tombstone key 不出现在 locations；
- partial tombstone points 不返回；
- locations 必须在 apply 后构建；
- 查询只触发包含目标 composite key 的 Reader；
- 并发查询同一个 Reader 只应用一次；
- apply 错误向查询层传播；
- `FileStore.Read()` 走相同门禁。

### 11.3 Compaction 测试

- full tombstone key 不写入新 TSM；
- partial tombstone points 不写入新 TSM；
- planner 不触发 apply；
- 实际 compaction 只 Ensure 当前 group；
- apply 失败时 compaction 中止，旧文件和 tombstone 保留；
- Replace 后删除数据不会复活。

### 11.4 DELETE 测试

- `Unapplied` Reader 的首次 Commit 应用历史和新增墓碑；
- `Applied` Reader 的 Commit 只读取新增 V4 member；
- `Failed` Reader 在 AddRange 前拒绝 DELETE，tombstone 文件不再增长；
- Commit 成功后状态为 `Applied`；
- 查询 Ensure 与 DELETE Commit 并发时不存在重复 apply 或 offset 错位；
- Commit 已持有 `deleteMu`，不会因重复加锁死锁。

### 11.5 WalkKeys 与工具回归

- `WalkKeys` 不返回 full tombstone key；
- 有效 TSI `fields.idx` 时 shard Open 不触发 WalkKeys apply；
- metadata 重建时正确应用所有 Reader；
- export、`influx_inspect` 和 digest 仍使用默认 eager Reader。

### 11.6 性能验证

至少覆盖：

- 无 tombstone；
- 少量 tombstone；
- 大型 full tombstone；
- 大型 partial tombstone；
- 大量 TSM 文件且每个文件有 tombstone；
- 首次查询延迟；
- 后续查询延迟；
- 首次 compaction prepare 延迟；
- shard `FileStore.Open()` 前后对比。

主要验收指标：

- 大 tombstone 不再显著增加 `FileStore.Open()` 时间；
- apply 完成后的查询没有额外锁竞争和 tombstone 文件 I/O；
- compaction 后删除数据不复活；
- 在线 DELETE 保持 V4 增量读取。

## 12. 分阶段实施

### Phase 1：正确性门禁

1. 增加 deferred reader option；
2. 增加 Reader apply 状态和 `EnsureTombstonesApplied()`；
3. 复用 `deleteMu` 串行首次和增量 apply；
4. 修改 `batchDelete.Commit()` 更新 apply 状态；
5. 查询在创建 locations 前 Ensure；
6. 实际 compaction 在创建 iterator 前 Ensure；
7. `WalkKeys` 增加全量枚举门禁；
8. 保持独立工具默认 eager。

### Phase 2：可观测性与性能

1. 增加 apply 来源、耗时和等待指标；
2. 增加候选 Reader raw key 精确过滤；
3. 增加首次查询和 shard Open benchmark；
4. 根据实测决定是否增加全局 apply limiter。

### Phase 3：可选优化

根据 profile 决定是否实现：

- `ApplyTombstonesBulk`；
- partial range 合并；
- offsets 只 Pack 一次；
- `DeleteRange()` 从 `searchOffset(keys[0])` 开始扫描；
- Store 全部 shard Open 后的低优先级后台预热；
- V5 tombstone 物化状态或持久化 sidecar index。

## 13. 设计结论

第一阶段的本质是：

```text
Open 不 apply

查询          -> Ensure once
实际 compaction -> Ensure once
在线 DELETE   -> Commit 自然完成首次或增量 apply
WalkKeys      -> Ensure all relevant Readers
```

同一 Reader 复用现有 `deleteMu`，不新增 apply mutex。原子状态用于记录历史墓碑是否已经物化，并为 apply 后的查询提供无锁快速路径。`lastAppliedOffset` 只负责 V4 文件内的增量读取位置，不能代替 Reader apply 状态。
