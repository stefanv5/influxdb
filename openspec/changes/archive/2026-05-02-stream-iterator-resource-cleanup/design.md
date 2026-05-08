## Context

`stream_iterator.go` 中的 `BlockValueIterator` 和 `KeyAwareMergingIterator` 是 TSM 流式 compaction 的核心组件。`BlockValueIterator` 持有解码后的 `decoded []Value`（最多 1000 个值，约 50KB）和预取的 `nextRaw []byte`（原始块数据）。`KeyAwareMergingIterator` 持有 `[]*BlockValueIterator` 切片。

当前 `KeyAwareMergingIterator.Close()` 只清空了 `buf` 和 `heap.entries`，没有遍历释放底层 `BlockValueIterator` 的内存。`BlockValueIterator` 本身也没有 `Close()` 方法。

在高配置环境、大压力写入场景下，compaction 可能频繁触发。每次 compaction 创建一组新的迭代器，如果关闭时不能及时释放大 slice，会导致 GC 压力升高。

**关键文件:**
- `tsdb/engine/tsm1/stream_iterator.go` — `BlockValueIterator`、`KeyAwareMergingIterator`
- `tsdb/engine/tsm1/stream_iterator_test.go` — 单元测试

**资源释放链路:**
```
compact()
  → FileStore.TSMReader(file) → r.Ref()        // 引用 +1
  → NewStreamingKeyIterator(trs, ...)            // 创建迭代器
  → writeNewFiles(tsm)                           // 迭代器被消费
  → defer tr.Unref()                             // 引用 -1
  → FileStore 之后调用 file.Close()              // refsWG.Wait() 后释放文件
```

`TSMReader` 的生命周期由 `Ref()/Unref()` 引用计数 + `FileStore.Close()` 管理，不在本次修改范围内。本次只关注迭代器持有的 Go slice 内存。

## Goals / Non-Goals

**Goals:**
- `BlockValueIterator.Close()` 清空所有持有的 slice（`decoded`、`nextRaw`、`tombstones`、`currentKey`、`nextKey`）
- `KeyAwareMergingIterator.Close()` 遍历调用所有底层 `BlockValueIterator.Close()`，并清空 `m.iterators`
- 确保 `Close()` 幂等，多次调用不 panic
- 为资源释放行为添加回归测试

**Non-Goals:**
- 不修改 `TSMReader` 的引用计数机制
- 不修改 `KeyIterator` 接口定义
- 不修改 `compact.go` 中的 `defer tr.Unref()` 逻辑
- 不添加 `sync.Pool` 等对象池化（已在 `stream-iterator-bugfix` 中决定移除）

## Decisions

### D1: BlockValueIterator.Close() 清空所有 slice 字段

**选择**: `Close()` 将 `decoded`、`nextRaw`、`tombstones`、`currentKey`、`nextKey` 置为 nil，`pos` 置 0，设置 `exhausted = true`。

**替代方案**: 只清空 `decoded` 和 `nextRaw`（大 slice）——遗漏其他 slice，收益不完整。

**理由**: 全部清空语义清晰，且 nil 赋值开销极低。`currentKey` 和 `nextKey` 虽然通常很小，但保持一致性。

### D2: KeyAwareMergingIterator.Close() 遍历调用子迭代器 Close()

**选择**: `Close()` 先遍历 `m.iterators` 调用每个 `BlockValueIterator.Close()`，再将 `m.iterators` 置 nil，最后清空 `buf` 和 `heap.entries`。

**替代方案**: 不遍历，依赖 GC 回收——在高频 compaction 场景下 GC 压力大。

**理由**: 显式清空让大 slice 在 `Close()` 调用后立即可被 GC 回收，不需等待整个 `KeyAwareMergingIterator` 对象被回收。

### D3: Close() 使用 closed 标志保证幂等

**选择**: `BlockValueIterator.Close()` 检查 `exhausted` 标志（复用已有字段），已关闭则直接返回。`KeyAwareMergingIterator.Close()` 检查已有的 `closed` 标志。

**替代方案**: 为 `BlockValueIterator` 新增 `closed` 字段——增加状态字段，与 `exhausted` 语义重叠。

**理由**: `exhausted = true` 已表示迭代器不再产出值，语义上等同于"已关闭"。复用该字段避免状态冗余。

## Risks / Trade-offs

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| Close() 后仍有代码访问迭代器 | panic | `exhausted` 和 `closed` 标志阻止 `Next()` 执行，`Read()` 不在 Close 后调用是调用方契约 |
| nil 赋值增加 Close() 耗时 | 可忽略 | 5-6 次 nil 赋值，纳秒级开销 |
| 与现有 Close() 行为不一致 | 兼容性 | 现有 Close() 已设置 closed/exhausted，本次只是增加清空操作，行为向后兼容 |
