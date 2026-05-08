## 1. CRITICAL 修复

- [x] 1.1 修复 `advanceAndPush`：将单次 `NextBlock()` 改为 `for` 循环，跳过全墓碑化块直到找到有效值或 key 变化（stream_iterator.go:569-596）
- [x] 1.2 为 `advanceAndPush` 编写回归测试：覆盖单个墓碑化中间块、连续多个墓碑化块、最后一个块被墓碑化三种场景

## 2. 资源管理

- [x] 2.1 实现 `KeyAwareMergingIterator.Close()`：遍历所有 `BlockValueIterator` 并释放资源
- [x] 2.2 添加 `closed` 状态标志，`Close()` 后 `Next()` 返回 false
- [x] 2.3 为 `Close()` 编写测试：验证资源释放和幂等性

## 3. 中断/取消支持

- [x] 3.1 修改 `NewStreamingKeyIterator` 签名：新增 `interrupt chan struct{}` 参数
- [x] 3.2 在 `KeyAwareMergingIterator` 中存储 interrupt channel
- [x] 3.3 在 `KeyAwareMergingIterator.Next()` 中检查 interrupt channel（select 无阻塞）
- [x] 3.4 更新 `compact.go:956` 的调用点：传入 `intC`
- [x] 3.5 编写中断测试：迭代中关闭 channel、开始前已关闭、传入 nil

## 4. 错误处理加固

- [x] 4.1 在 `activateIteratorsForKey` 中检查 `ActivatePending()` 返回值，失败时跳过该迭代器
- [x] 4.2 改进 `initIterators` 错误传播：保留首个错误，不被后续迭代器覆盖（审查后确认逻辑已正确）
- [x] 4.3 为错误处理编写测试

## 5. 类型安全

- [x] 5.1 在 `encodeValues` 中将所有 `v.(FloatValue)` 等类型断言改为 comma-ok 形式
- [x] 5.2 类型不匹配时返回 `fmt.Errorf("type mismatch: expected %T, got %T")`
- [x] 5.3 移除 `encodeBufPool`，改用局部变量分配 buffer（消除并发数据损坏风险）
- [x] 5.4 移除 `valueEntryPool`（与 buffer pool 同理，避免并发问题）
- [x] 5.5 为类型不匹配编写测试（可选，需要构造损坏数据）— 跳过，需要构造损坏数据，收益有限

## 6. currentType 修正

- [x] 6.1 将 `currentType` 设置逻辑从 `activateIteratorsForKey` 移到首次从堆弹出值时
- [x] 6.2 确保 `Read()` 返回的 key/time range 与实际编码数据的类型一致

## 7. 集成与验证

- [x] 7.1 更新 `compact.go` 中 `NewStreamingKeyIterator` 的调用，传入 `intC`
- [x] 7.2 运行 `go vet ./tsdb/engine/tsm1/...`
- [x] 7.3 运行 `go test -count=1 ./tsdb/engine/tsm1/...` 确保全部通过（CGO 未启用，无法使用 -race）
- [x] 7.4 运行 benchmark 对比修复前后的性能（BenchmarkStreamingCompaction）— 22ms/op, 9.9MB/op, 200k allocs/op
