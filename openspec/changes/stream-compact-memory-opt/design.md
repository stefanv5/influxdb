## Context

TSM1 流式 compact 路径（`stream_iterator.go`）在 `BlockValueIterator` 和 `KeyAwareMergingIterator` 中没有任何内存池化或复用机制。从现网火焰图来看，每个 block 的 decode/encode 周期都分配全新内存：

- `DecodeBlock(buf, nil)` 传 `nil`，每次分配新 `[]Value` 切片
- `DecodeStringBlock` 内部 `snappy.Decode(nil, ...)` 和 `string(e.b[lower:upper])` 每个 string 值独立分配
- `encodeValues` 每次 `tsdb.NewStringArrayLen(0)` 从零 grow
- `Read()` 每次 `make([]byte, len(key))` 拷贝 key

当前代码已有的池化：`encoding.go` 中的 encoder/decoder pool（`timeDecoderPool`、`stringDecoderPool` 等），`pools.go` 中的 `bufPool`。但这些只在 decode 内部使用，streaming 路径的上层没有复用机制。

约束：所有改动限定在 `stream_iterator.go` 内，不修改共享的 `DecodeBlock` / `EncodeStringArrayBlock` 接口。

## Goals / Non-Goals

**Goals:**
- 减少 streaming compact 路径的内存分配次数和峰值
- 使 string 类型解码数据在编码写出后能被 GC 及时回收
- 改动限定在 `stream_iterator.go` 单文件内，不影响其他子系统

**Non-Goals:**
- 不修改 `DecodeBlock` / `DecodeStringBlock` / `EncodeStringArrayBlock` 等共享编码层
- 不引入 arena 分配器或自定义内存管理器
- 不改变 compact 的功能行为或输出结果
- 不优化非 string 类型的特殊处理（虽然 `[]Value` 复用对所有类型有效）

## Decisions

### Decision 1: `[]Value` 切片复用策略

**选择**: 在 `BlockValueIterator` 上持久持有 `decoded []Value` 字段，`DecodeBlock` 传入 `it.decoded[:0]` 复用容量。

**替代方案**:
- 使用 `sync.Pool` 池化 `[]Value` 切片 — 增加了 get/put 的复杂度，且 `BlockValueIterator` 本身已经是 per-file 的，切片生命周期与迭代器一致，直接持有更简单
- 每次传 `nil`（当前行为）— 每次分配新切片

**理由**: `DecodeBlock` 内部通过 `append(a[:i], ...)` 追加值，传入已有容量的切片可以直接复用底层数组，避免分配。`BlockValueIterator` 的生命周期覆盖整个 compact 过程，切片的容量会逐渐 grow 到稳定状态后不再分配。

### Decision 2: `StringArray` 复用策略

**选择**: 在 `KeyAwareMergingIterator` 上持久持有 reusable 的 `StringArray`（以及 `FloatArray`、`IntegerArray` 等），`encodeValues` 复用底层数组。

**替代方案**:
- 使用 `sync.Pool` — 增加复杂度，且 `KeyAwareMergingIterator` 是单次使用的，池化收益不大
- 每次 `NewStringArrayLen(0)`（当前行为）— 每次分配

**理由**: `Timestamps` 和 `Values` 切片在每次 encode 后 reset 为 `[:0]`，容量会 grow 到 `bufSize`（1000）后稳定，后续不再分配。

### Decision 3: 生命周期管理 — nil 化 buffer 元素

**选择**: 在 `Read()` 完成编码后，遍历 `m.buf` 将每个元素设为零值 `Value{}`，断开对 `StringValue` 等引用类型值的引用。

**替代方案**:
- 不做 nil 化，依赖 `m.buf = m.buf[:0]` — 切片缩短后底层数组仍在，`StringValue.value`（string）的引用不会被断开，GC 无法回收
- 在 `Next()` 开头做 nil 化 — 时机太晚，编码完成到下次 `Next()` 之间有一段时间 string 仍被引用

**理由**: 编码完成后，`m.buf` 中的 `StringValue` 已经被消费（编码到 output block），立即 nil 化可以让 GC 在下一个周期回收这些 string 数据。这对于 string 类型尤其重要，因为 string 值可能很大。

### Decision 4: Key copy 复用

**选择**: 在 `KeyAwareMergingIterator` 和 `streamingKeyIterator` 上持有 reusable 的 `keyBuf []byte`，`Read()` 时复用而非每次 `make`。

**替代方案**:
- 每次 `make([]byte, len(key))`（当前行为）— 简单但产生分配

**理由**: key 的长度在同一 key 的多个 block 间是稳定的，复用 buffer 可以避免每次 `Read()` 的分配。

## Risks / Trade-offs

- **[Risk] 切片复用导致内存峰值**: 复用的 `[]Value` 切片容量 grow 后不会 shrink，即使后续 block 较小也持有较大容量。→ **Mitigation**: 这是可接受的 trade-off，因为 compact 过程中 block size 通常是稳定的（`bufSize=1000`），且迭代器生命周期结束后整个切片会被 GC 回收。

- **[Risk] nil 化遗漏**: 如果漏掉某个引用类型的 nil 化，string 数据无法回收。→ **Mitigation**: 对所有 `m.buf` 元素统一做零值化，不区分类型，确保无遗漏。

- **[Risk] 复用的 Array 切片在 type mismatch 时状态不一致**: 如果 `encodeValues` 中途遇到 type mismatch，Array 的 Timestamps/Values 可能部分填充。→ **Mitigation**: 每次 encode 前 reset 为 `[:0]`，type mismatch 时跳过单个值继续处理，不影响正确性。
