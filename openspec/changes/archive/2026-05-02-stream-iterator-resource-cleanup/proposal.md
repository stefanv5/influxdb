## Why

`BlockValueIterator` 持有解码后的 `decoded []Value`（每个 block 最多 1000 个值，约 50KB）和预取的 `nextRaw []byte`（原始块数据），但在流式迭代器关闭时没有显式清空这些资源。`KeyAwareMergingIterator.Close()` 也没有遍历释放底层迭代器持有的内存。在高配置环境、大压力写入场景下，compaction 可能频繁触发，这些大 slice 无法及时被 GC 回收，导致内存压力升高。

## What Changes

- 为 `BlockValueIterator` 添加 `Close()` 方法，清空 `decoded`、`nextRaw`、`tombstones` 等 slice，帮助 GC 及时回收
- 修改 `KeyAwareMergingIterator.Close()`，遍历所有 `BlockValueIterator` 调用 `Close()`，并将 `m.iterators` 置 nil
- 为 `Close()` 的资源释放行为添加回归测试

## Capabilities

### New Capabilities

- `iterator-resource-cleanup`: 流式合并迭代器的资源释放机制，确保 BlockValueIterator 和 KeyAwareMergingIterator 在关闭时显式清空持有的大 slice 内存

### Modified Capabilities

(none)

## Impact

- **受影响代码**: `tsdb/engine/tsm1/stream_iterator.go` — `BlockValueIterator` 新增 `Close()` 方法，`KeyAwareMergingIterator.Close()` 增强
- **受影响测试**: `tsdb/engine/tsm1/stream_iterator_test.go` — 新增资源释放测试
- **无接口变更**: `KeyIterator` 接口不变，`Close()` 语义增强（更积极释放内存）
- **性能影响**: `Close()` 增加少量 nil 赋值操作，开销可忽略；收益是高频 compaction 场景下内存更及时释放
