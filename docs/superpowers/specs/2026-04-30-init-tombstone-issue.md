# Init() Tombstone 过滤问题分析

## 1. 问题描述

### 问题场景

当 `BlockValueIterator.Init()` 读取的第一个 block 中，**所有值都被 tombstone 删除**时，iterator 会陷入"卡住"状态，无法贡献任何值，也无法被正确跳过。

```
场景设定：
  Block A: [ts=100]  (被 tombstone 覆盖)
  Block B: [ts=200]  (有效值)

期望行为：
  Init() 后，iterator 应该自动跳过 Block A，继续读取 Block B

实际行为（当前设计）：
  iterator 卡住，既不贡献值，也不再被处理
```

## 2. 问题根因

### 2.1 Init() 实现

```go
// 当前实现
func (b *BlockValueIterator) Init() bool {
    if !b.iter.Next() {
        return false
    }
    key, minTime, maxTime, typ, _, raw, err := b.iter.Read()
    if err != nil {
        b.err = err
        return false
    }
    b.currentKey = key
    b.currentType = typ
    b.tombstones = b.r.TombstoneRange(key)
    b.decoded = DecodeBlock(raw)
    b.pos = -1
    _ = minTime
    _ = maxTime
    return len(b.decoded) > 0  // 问题：只要 decoded 有值就返回 true
}
```

**问题**：`Init()` 只检查 `decoded` 数组是否有值，但不检查这些值是否被 tombstone 删除。

### 2.2 问题发生过程

```
1. Init() 调用后:
   currentKey = "cpu"
   decoded = [ts=100]    // 被 tombstone
   pos = -1
   hasNextBlock = false  // 还没调用 NextBlock()
   返回 true ✓

2. activateIteratorsForKey() 调用 Next():
   Next() {
       // pos=-1, len(decoded)-1=0, -1 < 0 → true
       b.pos++              // pos = 0
       v = decoded[0]       // ts=100
       if !isDeleted(v)     // isDeleted(ts=100) → true
           return true
       // 继续循环...

       // pos=0, len(decoded)-1=0, 0 < 0 → false
       return false         // 循环结束，返回 false
   }

3. Next() 返回 false，但没有触发 NextBlock()
   - iterator.currentKey = "cpu" (有值，不会被跳过)
   - hasNextBlock = false (未被激活)
   - 无法贡献任何值到堆

4. 在 findMinKey() 中:
   该 iterator 被计入 minKey 比较（因为 currentKey != nil）
   但它无法贡献任何值

5. popAndDedup() 消费完其他 iterator 的值后：
   - 堆变空
   - currentKey 完成
   - 但 Block B 永远不会被读取
```

### 2.3 核心问题

| 状态 | 当前设计 | 期望行为 |
|------|----------|----------|
| `currentKey` | 有值 | 有值 |
| `hasNextBlock` | false | true（应该自动读取下一个 block） |
| `decoded` | 包含被 tombstone 的值 | 应该为空或跳过 |

**根本原因**：`Init()` 不检查 tombstone，导致第一个 block 的所有值都被 tombstone 时，iterator 既无法贡献值，也不会触发 `NextBlock()` 读取下一个 block。

## 3. 解决方案

### 方案 A：在 Init() 中循环读取直到找到有效值

```go
func (b *BlockValueIterator) Init() bool {
    if !b.iter.Next() {
        return false
    }
    key, minTime, maxTime, typ, _, raw, err := b.iter.Read()
    if err != nil {
        b.err = err
        return false
    }
    b.currentKey = key
    b.currentType = typ
    b.tombstones = b.r.TombstoneRange(key)
    b.decoded = DecodeBlock(raw)
    b.pos = -1

    // 循环直到找到有效值或真正耗尽
    for {
        // 检查当前 block 中是否有有效值
        for b.pos < len(b.decoded)-1 {
            b.pos++
            v := b.decoded[b.pos]
            if !b.isDeleted(v) {
                return true  // 找到有效值
            }
        }

        // 当前 block 所有值都被删除，尝试下一个 block
        if !b.iter.Next() {
            return false  // 没有更多 block
        }

        key, minTime, maxTime, typ, _, raw, err := b.iter.Read()
        if err != nil {
            b.err = err
            return false
        }

        // key 变化：保存给下一轮使用
        if !bytes.Equal(b.currentKey, key) {
            b.nextKey = key
            b.nextType = typ
            b.nextRaw = raw
            b.nextMinTime = minTime
            b.nextMaxTime = maxTime
            b.hasNextBlock = true
            return false  // 第一个 block 耗尽，key 变化
        }

        // 同 key：解码并继续
        b.currentType = typ
        b.tombstones = b.r.TombstoneRange(key)
        b.decoded = DecodeBlock(raw)
        b.pos = -1
        // 循环回去检查新 block
    }
}
```

### 方案 B：在 activateIteratorsForKey() 中处理

```go
func (m *KeyAwareMergingIterator) activateIteratorsForKey(key []byte) {
    m.currentType = 0
    for _, iter := range m.iterators {
        if !bytes.Equal(iter.currentKey, key) {
            continue
        }

        if m.currentType == 0 {
            m.currentType = iter.currentType
        }

        // 循环直到成功 push 或真正耗尽
        for {
            if !iter.Next() {
                // 当前 block 耗尽，尝试下一个 block
                if !iter.NextBlock() {
                    break // 真正耗尽
                }
                continue // 有新 block，继续循环
            }

            v, _ := iter.Read()
            if iter.isDeleted(v) {
                continue // tombstone 跳过，继续取下一个
            }

            // 成功取到有效值
            heap.Push(&m.heap, &valueEntry{
                value:     v,
                iterator:  iter,
                timestamp: v.UnixNano(),
                fileIdx:   iter.fileIdx,
            })
            break
        }
    }
}
```

## 4. 推荐方案

**推荐方案 B**：在 `activateIteratorsForKey()` 中处理。

理由：
1. `Init()` 的职责应该只是初始化第一个 block，不需要承担"跳过 tombstone"的职责
2. `activateIteratorsForKey()` 是 key 轮次入口，在这里处理 tombstone 更合适
3. tombstone 是删除操作的产物，正常情况下很少出现整个 block 都被删除的场景
4. 可以在实现 Phase 1 时一并处理，不影响架构设计

## 5. 测试用例

### 必须覆盖的测试场景

| 测试用例 | 描述 |
|---------|------|
| `TestBlockValueIterator_TombstoneAllDeleted` | 第一个 block 所有值被 tombstone，验证能正确读取下一个 block |
| `TestBlockValueIterator_TombstoneMultipleBlocks` | 连续多个 block 所有值都被 tombstone |
| `TestKeyAwareMergingIterator_TombstoneAllDeleted` | 端到端：某个 iterator 的所有 block 都被 tombstone |
| `TestKeyAwareMergingIterator_TombstonePartialAfterAllDeleted` | 第一个 block 全删除，第二个 block 部分删除 |

### 边界条件

1. 最后一个 block 的所有值都被删除
2. 同一个 block 中部分值被 tombstone，部分值有效
3. 多个 iterator 的同一个 key 的第一个 block 都被 tombstone
4. tombstone 覆盖时间范围与 block 时间范围完全重合
5. tombstone 覆盖时间范围超出 block 时间范围

## 6. 影响评估

| 影响点 | 评估 |
|--------|------|
| 架构影响 | 无 - 属于实现细节修复 |
| 性能影响 | 极小 - tombstone 全覆盖 block 是极端场景 |
| 测试覆盖 | 需要补充相关测试用例 |
| 相关问题 | 无其他模块受影响 |

---

*文档版本: 1.0*
*创建时间: 2026-04-30*
*相关设计文档: 2026-04-29-tsm-streaming-compaction-design-v2.md*
