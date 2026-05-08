## ADDED Requirements

### Requirement: BlockValueIterator SHALL release resources on Close

`BlockValueIterator` SHALL 提供 `Close()` 方法，将 `decoded`、`nextRaw`、`tombstones`、`currentKey`、`nextKey` 置为 nil，`pos` 置 0，设置 `exhausted = true`。调用 `Close()` 后，`Next()` SHALL 返回 false。

#### Scenario: Close clears large slices
- **WHEN** 调用 `BlockValueIterator.Close()`
- **THEN** `decoded`、`nextRaw` 字段 SHALL 为 nil，`exhausted` SHALL 为 true

#### Scenario: Close is idempotent
- **WHEN** 多次调用 `BlockValueIterator.Close()`
- **THEN** 不 SHALL panic 或返回错误

#### Scenario: Next returns false after Close
- **WHEN** 调用 `Close()` 后调用 `Next()`
- **THEN** `Next()` SHALL 返回 false

---

### Requirement: KeyAwareMergingIterator.Close SHALL release all underlying iterators

`KeyAwareMergingIterator.Close()` SHALL 遍历所有 `BlockValueIterator` 并调用其 `Close()` 方法，然后将 `m.iterators` 置为 nil。

#### Scenario: Close releases all child iterators
- **WHEN** 调用 `KeyAwareMergingIterator.Close()`
- **THEN** 每个 `BlockValueIterator` 的 `Close()` SHALL 被调用，`m.iterators` SHALL 为 nil

#### Scenario: Close releases buffer and heap
- **WHEN** 调用 `KeyAwareMergingIterator.Close()`
- **THEN** `m.buf` 和 `m.heap.entries` SHALL 为 nil

#### Scenario: Close is idempotent
- **WHEN** 多次调用 `KeyAwareMergingIterator.Close()`
- **THEN** 不 SHALL panic 或返回错误

#### Scenario: Next returns false after Close
- **WHEN** 调用 `Close()` 后调用 `Next()`
- **THEN** `Next()` SHALL 返回 false
