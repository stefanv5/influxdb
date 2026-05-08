## ADDED Requirements

### Requirement: advanceAndPush SHALL skip fully-tombstoned blocks

当 `advanceAndPush` 推进到下一个块时，如果该块的所有值都被墓碑标记（`Next()` 返回 false），系统 SHALL 继续尝试同一 key 的后续块，直到找到有有效值的块或 key 发生变化。

#### Scenario: Single tombstoned block between valid blocks
- **WHEN** key A 有 Block 1（ts=1-1000）、Block 2（ts=1001-2000，全部被墓碑标记）、Block 3（ts=2001-3000）
- **THEN** 消耗完 Block 1 的最后一个值后，`advanceAndPush` SHALL 跳过 Block 2 并将 Block 3 的第一个有效值推入堆中

#### Scenario: Consecutive tombstoned blocks
- **WHEN** key A 有 Block 1（有效）、Block 2（全墓碑）、Block 3（全墓碑）、Block 4（有效）
- **THEN** `advanceAndPush` SHALL 跳过 Block 2 和 Block 3，将 Block 4 的第一个有效值推入堆中

#### Scenario: Last block is tombstoned
- **WHEN** key A 有 Block 1（有效）、Block 2（全墓碑），且 Block 2 是该 key 的最后一个块
- **THEN** `advanceAndPush` SHALL 检测到块耗尽，不推入任何值，等待 `moveToNextKey` 处理

---

### Requirement: Close SHALL release underlying resources

`KeyAwareMergingIterator.Close()` SHALL 关闭所有底层 `BlockValueIterator`，释放其持有的资源引用。

#### Scenario: Close releases all iterators
- **WHEN** 调用 `Close()` 方法
- **THEN** 所有底层 `BlockValueIterator` 持有的资源 SHALL 被释放，后续调用 `Next()` SHALL 返回 false

#### Scenario: Close is idempotent
- **WHEN** 多次调用 `Close()` 方法
- **THEN** 不 SHALL panic 或返回错误

---

### Requirement: Streaming iterator SHALL support interruption

`NewStreamingKeyIterator` SHALL 接受一个 `interrupt chan struct{}` 参数。当该 channel 被关闭时，迭代器 SHALL 在最近的 `Next()` 调用中返回 false，并设置 `Err()` 返回中断错误。

#### Scenario: Interrupt during iteration
- **WHEN** 迭代过程中关闭 interrupt channel
- **THEN** 下一次 `Next()` 调用 SHALL 返回 false，且 `Err()` SHALL 返回非 nil 错误

#### Scenario: Interrupt before iteration starts
- **WHEN** interrupt channel 在调用 `Next()` 之前已被关闭
- **THEN** 第一次 `Next()` 调用 SHALL 返回 false

#### Scenario: No interrupt channel provided
- **WHEN** 传入 nil 作为 interrupt 参数
- **THEN** 迭代器 SHALL 正常工作，不检查取消信号

---

### Requirement: ActivatePending errors SHALL be checked

在 `activateIteratorsForKey` 中调用 `ActivatePending()` 后，系统 SHALL 检查返回值。如果返回 false，系统 SHALL 记录错误并跳过该迭代器。

#### Scenario: ActivatePending fails due to decode error
- **WHEN** `ActivatePending()` 因解码错误返回 false
- **THEN** 该迭代器 SHALL 被跳过，其他迭代器继续正常工作

#### Scenario: ActivatePending succeeds
- **WHEN** `ActivatePending()` 返回 true
- **THEN** 迭代器 SHALL 正常参与后续合并

---

### Requirement: encodeValues SHALL use safe type assertions

`encodeValues` 方法中的类型断言 SHALL 使用 comma-ok 形式。如果值类型与预期的块类型不匹配，系统 SHALL 返回描述性错误而非 panic。

#### Scenario: Type mismatch during encoding
- **WHEN** 块类型标记为 `BlockFloat64` 但值的实际类型不是 `FloatValue`
- **THEN** `encodeValues` SHALL 返回 `fmt.Errorf` 描述类型不匹配，不 SHALL panic

#### Scenario: Correct type assertion
- **WHEN** 块类型与值类型匹配
- **THEN** 编码 SHALL 正常完成并返回编码后的数据

---

### Requirement: initIterators SHALL propagate errors correctly

`initIterators` SHALL 在遇到迭代器初始化错误时正确设置 `m.err` 并返回 false。如果部分迭代器成功、部分失败，错误 SHALL 被保留而非覆盖。

#### Scenario: Third iterator fails after two succeed
- **WHEN** 迭代器 1 和 2 初始化成功，迭代器 3 初始化失败并返回错误
- **THEN** `initIterators` SHALL 返回 false，且 `m.err` SHALL 包含迭代器 3 的错误

#### Scenario: All iterators succeed
- **WHEN** 所有迭代器初始化成功
- **THEN** `initIterators` SHALL 返回 true，`m.err` SHALL 为 nil
