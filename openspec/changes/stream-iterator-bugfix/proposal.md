## Why

在对 `tsdb/engine/tsm1/stream_iterator.go` 进行代码审查后，发现了多个影响正确性和健壮性的问题。其中最严重的是 `advanceAndPush` 在遇到全墓碑化的中间块时会跳过后续所有块，导致 compaction 过程中数据静默丢失。此外还存在资源泄漏、无法取消、错误吞没等问题。这些问题必须在流式合并迭代器投入使用前修复，否则会影响数据库的数据完整性。

## What Changes

- 修复 `advanceAndPush` 方法：当遇到全墓碑化块时，循环尝试后续块而非直接放弃（**CRITICAL — 数据丢失**）
- 实现 `Close()` 方法：正确释放底层 `TSMReader` 资源
- 添加中断/取消支持：`NewStreamingKeyIterator` 接受 `interrupt chan struct{}` 参数，在迭代过程中检查取消信号
- 检查 `ActivatePending()` 返回值：在 `activateIteratorsForKey` 中处理解码失败
- 修复 `encodeBufPool` buffer 复用问题：避免并发 compaction 下的数据损坏风险
- 改进 `currentType` 设置逻辑：从实际产生数据的块获取类型，而非第一个匹配迭代器
- 改进 `initIterators` 错误处理：正确传播迭代器初始化错误
- 在 `encodeValues` 中使用安全类型断言：避免因数据类型不匹配导致 panic

## Capabilities

### New Capabilities
- `stream-iterator-robustness`: 修复流式合并迭代器的正确性、资源管理和错误处理问题

### Modified Capabilities

（无已有 spec 需要修改）

## Impact

- **受影响代码**: `tsdb/engine/tsm1/stream_iterator.go`, `tsdb/engine/tsm1/compact.go`
- **受影响接口**: `NewStreamingKeyIterator` 函数签名变更（新增 `interrupt` 参数）
- **受影响测试**: `tsdb/engine/tsm1/stream_iterator_test.go` 需要新增测试覆盖修复的场景
- **依赖**: 无新增外部依赖
- **风险**: 修改核心 compaction 路径，需要充分测试确保不引入回归
