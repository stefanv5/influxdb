## Context

`stream_iterator.go` 是 TSM 引擎中新实现的流式合并迭代器，用于 full compaction 场景。它由三个核心组件构成：

- `BlockValueIterator`: 单文件级别的块迭代器，逐值解码并过滤墓碑
- `KeyAwareMergingIterator`: 多文件合并迭代器，使用堆按 (timestamp, fileIdx) 归并，保证每个输出块只含一个 key
- `streamingKeyIterator`: 实现 `KeyIterator` 接口的适配器

代码审查发现了 8 个问题，其中 1 个 CRITICAL（数据丢失）、3 个 HIGH（资源泄漏/不可取消/错误吞没）、4 个 MEDIUM（buffer 复用/类型安全/错误处理）。

当前代码位于 `stefan_stream_compact` 分支，尚未合入 main。

## Goals / Non-Goals

**Goals:**
- 修复 `advanceAndPush` 的数据丢失 bug（CRITICAL）
- 实现正确的资源释放（`Close()`）
- 添加 compaction 取消支持（`interrupt` channel）
- 加固错误处理和类型安全
- 为所有修复添加回归测试

**Non-Goals:**
- 不重构整体架构（三个组件的分层设计保持不变）
- 不修改 `KeyIterator` 接口定义
- 不改变 `NewTSMBatchKeyIterator` 的行为
- 不优化性能（本次只关注正确性）

## Decisions

### D1: `advanceAndPush` 改为循环遍历块

**选择**: 将 `advanceAndPush` 中的单次 `NextBlock()` 调用改为 `for` 循环，直到找到有有效值的块或 key 发生变化。

**替代方案**: 在 `activateIteratorForBlock` 中处理——但该方法只在初始化时调用，不适合运行时路径。

**理由**: 循环方式最小化改动，且语义清晰：跳过空块是 advance 的内在职责。

### D2: `Close()` 关闭所有底层 `BlockValueIterator`

**选择**: `KeyAwareMergingIterator.Close()` 遍历所有 `BlockValueIterator`，释放其持有的 `TSMReader` 引用。

**替代方案**: 依赖调用方的 `defer tr.Unref()`——但这违反 `KeyIterator` 接口契约。

**理由**: 接口契约要求 `Close()` 释放资源，调用方不应了解内部实现细节。

### D3: 通过函数参数传递 `interrupt` channel

**选择**: `NewStreamingKeyIterator` 新增 `interrupt chan struct{}` 参数，在 `KeyAwareMergingIterator.Next()` 和 `BlockValueIterator.NextBlock()` 中检查。

**替代方案**: 通过 `context.Context` 传递——更通用但改动更大，且现有 `NewTSMBatchKeyIterator` 使用的是 channel。

**理由**: 保持与现有 `NewTSMBatchKeyIterator` 的一致性，改动最小。

### D4: 移除 `encodeBufPool`，使用局部变量

**选择**: 删除 `encodeBufPool`，在 `encodeValues` 中使用局部 `make([]byte, 0)` 分配 buffer。

**替代方案**: 修复池化逻辑（返回副本而非共享引用）——增加复杂度，收益有限。

**理由**: 编码操作不是热路径（每次 compaction 调用一次），1MB 的池化 buffer 在并发场景下有数据损坏风险，得不偿失。

### D5: `encodeValues` 使用 comma-ok 类型断言

**选择**: 将 `v.(FloatValue)` 改为 comma-ok 形式，类型不匹配时返回 `fmt.Errorf`。

**理由**: compaction 路径可能处理损坏数据，panic 不可接受。

### D6: `currentType` 从实际产生数据的块获取

**选择**: 将 `currentType` 的设置从 `activateIteratorsForKey` 移到首次成功从堆中弹出值时设置。

**理由**: 第一个匹配迭代器的块可能被完全墓碑化，不产生任何值，从它获取类型没有意义。

## Risks / Trade-offs

- **[风险] 循环跳过块可能掩盖数据问题** → 在循环中记录 debug 日志，便于排查异常情况
- **[风险] `interrupt` channel 检查增加每次迭代开销** → 开销极小（channel select 无阻塞），可忽略
- **[风险] 移除 `encodeBufPool` 增加内存分配** → 编码操作频率低（每次 compaction 一次），1MB buffer 的分配开销可接受
- **[权衡] 不修改 `KeyIterator` 接口** → `Close()` 语义由实现方保证，接口层面无法强制，但这是 Go 的惯用模式
