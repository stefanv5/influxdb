## Why

TSM1 流式 compact 路径（`stream_iterator.go`）没有任何内存池化或复用机制。从现网火焰图来看，`BlockValueIterator.NextBlock` 中 `DecodeBlock` 的内存开销很高，尤其是 string 类型 field 场景：每个 block 的 decode 都分配全新的 `[]Value` 切片和独立的 Go string 值（通过 `string(e.b[lower:upper])`），encode 阶段又分配全新的 `StringArray`。这导致大量短生命周期对象的分配和 GC 压力，在高写入场景下成为性能瓶颈。

## What Changes

- `BlockValueIterator` 持有 decoded `[]Value` 切片，`DecodeBlock` 传入已有切片复用容量，避免每次 decode 分配新切片
- `KeyAwareMergingIterator` 持有 reusable 的 `StringArray`（`Timestamps` + `Values`），`encodeValues` 复用底层数组而非每次 `NewStringArrayLen(0)`
- `Read()` 编码完成后立即 nil 化 `m.buf` 中的元素，断开对 `StringValue` 等引用类型值的引用，使 GC 能及时回收 string 数据
- 所有改动限定在 `tsdb/engine/tsm1/stream_iterator.go` 内，不修改共享的 encoding/decoding 层代码

## Capabilities

### New Capabilities
- `value-slice-reuse`: `BlockValueIterator` 复用 `[]Value` 切片容量，减少 decode 阶段的内存分配
- `string-array-pooling`: `KeyAwareMergingIterator` 池化 `StringArray`，减少 encode 阶段的内存分配
- `value-lifecycle-management`: `Read()` 后 nil 化 buffer 元素，加速 string 数据的 GC 回收

### Modified Capabilities

（无已有 spec 需要修改）

## Impact

- **Affected code**: `tsdb/engine/tsm1/stream_iterator.go` — `BlockValueIterator`、`KeyAwareMergingIterator`、`encodeValues`
- **API**: 无外部 API 变更，仅内部实现优化
- **Dependencies**: 无新依赖，使用已有的 `DecodeBlock` 和 `EncodeStringArrayBlock` 接口
- **Risk**: 低 — 改动限定在单文件内，不影响其他子系统（cache、query）使用的共享 encoding 代码
