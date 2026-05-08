## 1. BlockValueIterator.Close() 实现

- [x] 1.1 在 `stream_iterator.go` 中为 `BlockValueIterator` 添加 `Close()` 方法：清空 `decoded`、`nextRaw`、`tombstones`、`currentKey`、`nextKey` 为 nil，`pos` 置 0，设置 `exhausted = true`
- [x] 1.2 确保 `Close()` 幂等：检查 `exhausted` 标志，已关闭则直接返回

## 2. KeyAwareMergingIterator.Close() 增强

- [x] 2.1 修改 `KeyAwareMergingIterator.Close()`：遍历 `m.iterators` 调用每个 `BlockValueIterator.Close()`
- [x] 2.2 在遍历后将 `m.iterators` 置为 nil
- [x] 2.3 确保已有的 `closed` 标志检查保持不变（幂等性）

## 3. 测试

- [x] 3.1 添加 `TestBlockValueIterator_Close`：验证 Close() 后 decoded/nextRaw 为 nil，exhausted 为 true
- [x] 3.2 添加 `TestBlockValueIterator_CloseIdempotent`：验证多次调用 Close() 不 panic
- [x] 3.3 添加 `TestKeyAwareMergingIterator_CloseReleasesIterators`：验证 Close() 后每个子迭代器的 Close() 被调用，m.iterators 为 nil
- [x] 3.4 添加 `TestKeyAwareMergingIterator_CloseIdempotent`：验证多次调用 Close() 不 panic
- [x] 3.5 运行 `go test -count=1 -run "TestBlockValueIterator|TestKeyAwareMergingIterator" ./tsdb/engine/tsm1/` 确认全部通过
