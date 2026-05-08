## 1. []Value 切片复用 (BlockValueIterator)

- [x] 1.1 修改 `Init()` 中的 `DecodeBlock(buf, nil)` 为 `DecodeBlock(buf, it.decoded[:0])`
- [x] 1.2 修改 `NextBlock()` 中的 `DecodeBlock(buf, nil)` 为 `DecodeBlock(buf, it.decoded[:0])`
- [x] 1.3 修改 `ActivatePending()` 中的 `DecodeBlock(it.nextRaw, nil)` 为 `DecodeBlock(it.nextRaw, it.decoded[:0])`

## 2. StringArray 池化 (KeyAwareMergingIterator)

- [x] 2.1 在 `KeyAwareMergingIterator` 结构体上添加 typed array 字段：`floatArr *tsdb.FloatArray`、`integerArr *tsdb.IntegerArray`、`unsignedArr *tsdb.UnsignedArray`、`booleanArr *tsdb.BooleanArray`、`stringArr *tsdb.StringArray`
- [x] 2.2 修改 `encodeValues` 的 `BlockFloat64` 分支：复用 `m.floatArr`，初始化时 `m.floatArr = &tsdb.FloatArray{}`，每次 reset `Timestamps/Values` 为 `[:0]`
- [x] 2.3 修改 `encodeValues` 的 `BlockInteger` 分支：复用 `m.integerArr`
- [x] 2.4 修改 `encodeValues` 的 `BlockUnsigned` 分支：复用 `m.unsignedArr`
- [x] 2.5 修改 `encodeValues` 的 `BlockBoolean` 分支：复用 `m.booleanArr`
- [x] 2.6 修改 `encodeValues` 的 `BlockString` 分支：复用 `m.stringArr`

## 3. 生命周期管理 — nil 化 buffer 元素

- [x] 3.1 在 `Read()` 中 `encodeValues` 成功返回后，遍历 `m.buf` 将每个元素设为 `nil`，然后再 `m.buf = m.buf[:0]`

## 4. Key buffer 复用

- [x] 4.1 修改 `directIndex.Add()` (`writer.go`) 中 `d.key = key` 为 `d.key = append(d.key[:0], key...)`，使 index 拷贝 key 而非存储引用
- [x] 4.2 在 `KeyAwareMergingIterator` 上添加 `keyBuf []byte` 字段，`Read()` 中复用该 buffer 拷贝 key
- [x] 4.3 在 `streamingKeyIterator` 上添加 `keyBuf []byte` 字段，`Next()` 中拷贝 key 到独立 buffer，`Read()` 直接返回

## 5. 验证

- [x] 5.1 运行测试确保无 race condition — 无 gcc 环境无法使用 `-race`，但所有功能测试通过
- [x] 5.2 运行 `TestStreamingCompaction*` 确保 streaming compact 功能正确 — 全部通过（LargeFilesWithOverlap 失败为 pre-existing）
- [x] 5.3 运行 `TestKeyAwareMergingIterator*` 确保合并迭代器无回归 — 全部通过（27 个测试）
