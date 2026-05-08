package tsm1

import (
	"bytes"
	"container/heap"
	"fmt"
	"sort"

	"github.com/influxdata/influxdb/tsdb"
)

// blockTypeName returns a human-readable name for a block type byte.
func blockTypeName(typ byte) string {
	switch typ {
	case BlockFloat64:
		return "float"
	case BlockInteger:
		return "integer"
	case BlockUnsigned:
		return "unsigned"
	case BlockBoolean:
		return "boolean"
	case BlockString:
		return "string"
	default:
		return fmt.Sprintf("unknown(%d)", typ)
	}
}

// blockTypeForValue returns the block type byte for a given Value.
func blockTypeForValue(v Value) byte {
	switch v.(type) {
	case FloatValue:
		return BlockFloat64
	case IntegerValue:
		return BlockInteger
	case UnsignedValue:
		return BlockUnsigned
	case BooleanValue:
		return BlockBoolean
	case StringValue:
		return BlockString
	default:
		return 0
	}
}

// BlockValueIterator iterates over values within a single TSM block.
// It holds at most one decoded block in memory at any time.
// When the current block is exhausted, Next() returns false without
// automatically advancing to the next block. The upper layer
// (KeyAwareMergingIterator) controls block advancement via NextBlock().
type BlockValueIterator struct {
	r       *TSMReader
	iter    *BlockIterator
	fileIdx int

	// Current block state
	currentKey  []byte
	currentType byte
	tombstones  []TimeRange
	decoded     []Value
	pos         int

	// Pending state from NextBlock when key changes
	nextRaw  []byte
	nextKey  []byte
	nextType byte
	nextMin  int64
	nextMax  int64

	hasNextBlock bool
	initialized  bool   // true if Init() has been called
	exhausted    bool   // true when iterator has no more blocks
	err          error
}

// NewBlockValueIterator creates a new BlockValueIterator for the given TSM reader.
func NewBlockValueIterator(r *TSMReader, fileIdx int) *BlockValueIterator {
	return &BlockValueIterator{
		r:       r,
		iter:    r.BlockIterator(),
		fileIdx: fileIdx,
	}
}

// Init reads the first block and prepares the iterator for iteration.
// Returns true if there is at least one block to iterate.
func (it *BlockValueIterator) Init() bool {
	if !it.iter.Next() {
		if it.iter.Err() != nil {
			it.err = it.iter.Err()
		}
		return false
	}

	key, minTime, maxTime, typ, _, buf, err := it.iter.Read()
	if err != nil {
		it.err = err
		return false
	}

	it.currentKey = append([]byte(nil), key...)
	it.currentType = typ
	it.tombstones = it.r.TombstoneRange(key)

	decoded, err := DecodeBlock(buf, nil)
	if err != nil {
		it.err = fmt.Errorf("decode error: unable to decompress block type %s for key '%s': %v", blockTypeName(typ), key, err)
		return false
	}
	it.decoded = decoded
	it.pos = 0
	it.initialized = true

	// Cache block metadata for NextBlock detection
	it.nextMin = minTime
	it.nextMax = maxTime

	return true
}

// Next advances to the next value within the current block.
// Returns false when the current block is exhausted or all remaining values
// are tombstoned. Does NOT advance to the next block automatically.
func (it *BlockValueIterator) Next() bool {
	for it.pos < len(it.decoded) {
		v := it.decoded[it.pos]
		if !it.isDeleted(v) {
			it.pos++
			return true
		}
		it.pos++
	}
	return false
}

// NextBlock reads the next block from the underlying TSM iterator.
// If the key changes, the new key is cached in nextKey/nextType and the
// method returns false. The caller should use ActivatePending() to
// start iterating the new key's block.
// If the key is the same, the new block replaces the current one and
// returns true.
func (it *BlockValueIterator) NextBlock() bool {
	if !it.iter.Next() {
		if it.iter.Err() != nil {
			it.err = it.iter.Err()
		}
		return false
	}

	key, minTime, maxTime, typ, _, buf, err := it.iter.Read()
	if err != nil {
		it.err = err
		return false
	}

	// Key changed - cache the new block info
	if !bytes.Equal(key, it.currentKey) {
		it.nextKey = append([]byte(nil), key...)
		it.nextType = typ
		it.nextRaw = buf
		it.nextMin = minTime
		it.nextMax = maxTime
		it.hasNextBlock = true
		return false
	}

	// Same key - decode and replace current block
	it.tombstones = it.r.TombstoneRange(key)
	decoded, err := DecodeBlock(buf, nil)
	if err != nil {
		it.err = fmt.Errorf("decode error: unable to decompress block type %s for key '%s': %v", blockTypeName(typ), key, err)
		return false
	}
	it.decoded = decoded
	it.pos = 0
	it.nextMin = minTime
	it.nextMax = maxTime
	return true
}

// ActivatePending activates the pending block that was cached by NextBlock
// when the key changed. After calling this, the iterator is positioned at
// the new key's first block.
func (it *BlockValueIterator) ActivatePending() bool {
	if !it.hasNextBlock {
		return false
	}

	it.currentKey = it.nextKey
	it.currentType = it.nextType
	it.tombstones = it.r.TombstoneRange(it.currentKey)

	decoded, err := DecodeBlock(it.nextRaw, nil)
	if err != nil {
		it.err = fmt.Errorf("decode error: unable to decompress block type %s for key '%s': %v", blockTypeName(it.nextType), it.nextKey, err)
		return false
	}
	it.decoded = decoded
	it.pos = 0

	// Clear pending state
	it.nextKey = nil
	it.nextRaw = nil
	it.hasNextBlock = false

	return true
}

// isDeleted checks if a value's timestamp falls within any tombstone range.
func (it *BlockValueIterator) isDeleted(v Value) bool {
	ts := v.UnixNano()
	for _, tr := range it.tombstones {
		if tr.Min <= ts && tr.Max >= ts {
			return true
		}
	}
	return false
}

// Read returns the current value. Should be called after Next() returns true.
func (it *BlockValueIterator) Read() Value {
	return it.decoded[it.pos-1]
}

// Key returns the current key being iterated.
// Returns nil if Init() has not been called yet.
func (it *BlockValueIterator) Key() []byte {
	if !it.initialized {
		return nil
	}
	return it.currentKey
}

// Type returns the current block type.
func (it *BlockValueIterator) Type() byte {
	return it.currentType
}

// Err returns any error encountered during iteration.
func (it *BlockValueIterator) Err() error {
	return it.err
}

// FileIdx returns the file index of the TSM reader this iterator is bound to.
func (it *BlockValueIterator) FileIdx() int {
	return it.fileIdx
}

// Close releases all slice resources held by this iterator.
// After Close, Next() returns false. Close is idempotent.
func (it *BlockValueIterator) Close() {
	if it.exhausted {
		return
	}
	it.exhausted = true
	it.decoded = nil
	it.nextRaw = nil
	it.tombstones = nil
	it.currentKey = nil
	it.nextKey = nil
	it.pos = 0
}

// blockTypeUnset is a sentinel value for currentType that does not collide
// with any valid block type (BlockFloat64=0 through BlockUnsigned=4).
const blockTypeUnset = byte(0xFF)

// valueEntry represents an entry in the value heap.
type valueEntry struct {
	value     Value
	iterator  *BlockValueIterator
	key       []byte // snapshot of the key at entry creation time
	timestamp int64
	fileIdx   int
}

// valueHeap implements heap.Interface and sorts entries by (timestamp, fileIdx) ascending.
type valueHeap struct {
	entries []*valueEntry
}

func (h valueHeap) Len() int { return len(h.entries) }

func (h valueHeap) Less(i, j int) bool {
	if h.entries[i].timestamp != h.entries[j].timestamp {
		return h.entries[i].timestamp < h.entries[j].timestamp
	}
	// For same timestamp, newer file (higher fileIdx) should be "less" to win the heap top
	// Since heap.Pop gets the smallest, we need to invert here so higher fileIdx wins
	return h.entries[i].fileIdx > h.entries[j].fileIdx
}

func (h valueHeap) Swap(i, j int) {
	h.entries[i], h.entries[j] = h.entries[j], h.entries[i]
}

func (h *valueHeap) Push(x interface{}) {
	h.entries = append(h.entries, x.(*valueEntry))
}

func (h *valueHeap) Pop() interface{} {
	old := h.entries
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	h.entries = old[:n-1]
	return item
}

func (h valueHeap) Peek() *valueEntry {
	return h.entries[0]
}

// KeyAwareMergingIterator merges multiple BlockValueIterators into a single
// stream of blocks, ensuring each output block contains values from exactly
// one key. It uses a heap to merge values by (timestamp, fileIdx) and
// deduplicates same-timestamp values keeping the newest file's value.
type KeyAwareMergingIterator struct {
	iterators []*BlockValueIterator
	heap      valueHeap
	currentKey []byte
	currentType byte
	blockKey    []byte  // key for the current output block

	// Output buffer
	buf     []Value
	bufSize int

	// State
	initialized bool
	exhausted   bool
	closed      bool
	err         error

	// Interrupt channel for cancellation
	interrupt chan struct{}

	// For tracking estimated index size
	estimatedIndexSize int
}

// NewKeyAwareMergingIterator creates a new merging iterator from the given
// BlockValueIterators. bufSize controls the maximum number of values per
// output block (typically DefaultMaxPointsPerBlock = 1000).
// interrupt is an optional channel for cancellation; closing it will cause
// Next() to return false with an error. Pass nil to disable interruption.
func NewKeyAwareMergingIterator(iters []*BlockValueIterator, bufSize int, interrupt chan struct{}) *KeyAwareMergingIterator {
	return &KeyAwareMergingIterator{
		iterators: iters,
		buf:       make([]Value, 0, bufSize),
		bufSize:   bufSize,
		interrupt: interrupt,
	}
}

// Next advances to the next output block. Returns true if there are more blocks.
func (m *KeyAwareMergingIterator) Next() bool {
	if m.exhausted || m.closed {
		return false
	}

	// Check for cancellation
	if m.interrupted() {
		m.err = fmt.Errorf("compaction interrupted")
		m.exhausted = true
		return false
	}

	// Initialize on first call
	if !m.initialized {
		m.initialized = true
		if !m.initIterators() {
			m.exhausted = true
			return false
		}
	}

	// Clear output buffer
	m.buf = m.buf[:0]

	// Safety limit to prevent infinite loops
	iterations := 0
	maxIterations := 10000

	// Track the key for this output block
	currentKeyForBlock := m.currentKey

	// Fill output buffer from heap
	for len(m.buf) < m.bufSize && iterations < maxIterations {
		iterations++
		// Check for cancellation on each iteration
		if m.interrupted() {
			m.err = fmt.Errorf("compaction interrupted")
			m.exhausted = true
			break
		}

		if m.heap.Len() == 0 {
			// Current key exhausted, find next key
			if len(m.buf) > 0 {
				// We have values from previous key, return them
				break
			}
			if !m.moveToNextKey() {
				m.exhausted = true
				break
			}
			// Update currentKeyForBlock to the new key
			currentKeyForBlock = m.currentKey
			// Check if we got any values in the heap
			if m.heap.Len() == 0 {
				// No values for the new key, continue to find next key
				continue
			}
		}

		entry := m.popAndDedup()
		if entry != nil {
			// Set currentType for first value
			if m.currentType == blockTypeUnset {
				m.currentType = blockTypeForValue(entry.value)
			}
			m.buf = append(m.buf, entry.value)
		}
	}

	// Save the block key for Read()
	if len(m.buf) > 0 {
		m.blockKey = currentKeyForBlock
	} else {
		m.blockKey = nil
	}

	return len(m.buf) > 0
}

// interrupted returns true if the interrupt channel has been closed.
func (m *KeyAwareMergingIterator) interrupted() bool {
	if m.interrupt == nil {
		return false
	}
	select {
	case <-m.interrupt:
		return true
	default:
		return false
	}
}

// Read returns the current block's key, time range, and encoded data.
func (m *KeyAwareMergingIterator) Read() (key []byte, minTime int64, maxTime int64, data []byte, err error) {
	if len(m.buf) == 0 {
		return nil, 0, 0, nil, fmt.Errorf("no values in buffer")
	}

	// Return the key that was set when this block was started
	// (m.blockKey tracks the key for the current block being returned)
	keyCopy := make([]byte, len(m.blockKey))
	copy(keyCopy, m.blockKey)
	key = keyCopy

	// Sort buffer by timestamp before encoding. The heap merge can produce
	// unsorted output when an iterator advances across block boundaries
	// (NextBlock) and pushes a value with an earlier timestamp than values
	// already popped from other iterators.
	sort.Slice(m.buf, func(i, j int) bool {
		return m.buf[i].UnixNano() < m.buf[j].UnixNano()
	})

	minTime = m.buf[0].UnixNano()
	maxTime = m.buf[len(m.buf)-1].UnixNano()

	data, err = m.encodeValues(m.currentType, m.buf)
	if err != nil {
		return nil, 0, 0, nil, err
	}

	// If all values were skipped due to type mismatch, data is nil.
	// Return nil data without error — the caller should skip this block.
	if data == nil {
		return nil, 0, 0, nil, nil
	}

	return key, minTime, maxTime, data, nil
}

// Close closes all underlying iterators and releases resources.
func (m *KeyAwareMergingIterator) Close() error {
	if m.closed {
		return nil
	}
	m.closed = true
	m.exhausted = true
	for _, iter := range m.iterators {
		iter.Close()
	}
	m.iterators = nil
	m.buf = nil
	m.heap.entries = nil
	return nil
}

// Err returns any error encountered during iteration.
func (m *KeyAwareMergingIterator) Err() error {
	return m.err
}

// EstimatedIndexSize returns the estimated index size.
func (m *KeyAwareMergingIterator) EstimatedIndexSize() int {
	return m.estimatedIndexSize
}

// initIterators initializes all iterators and finds the first key.
func (m *KeyAwareMergingIterator) initIterators() bool {
	// Initialize all iterators that haven't been initialized yet
	for _, iter := range m.iterators {
		// Skip if already initialized
		if iter.initialized {
			continue
		}
		if !iter.Init() {
			if iter.Err() != nil {
				m.err = iter.Err()
				return false
			}
			// Iterator has no blocks, skip it
		}
	}

	// Find minimum key and activate matching iterators
	minKey := m.findMinKey()
	if minKey == nil {
		return false
	}

	m.currentKey = minKey
	// Initialize blockKey for first block
	m.blockKey = minKey
	m.currentType = blockTypeUnset
	m.activateIteratorsForKey(minKey)
	return true
}

// findMinKey scans all iterators and returns the lexicographically smallest key.
// It considers both the current key and any pending (next) key from NextBlock.
// Exhausted iterators are skipped.
func (m *KeyAwareMergingIterator) findMinKey() []byte {
	var minKey []byte
	for _, iter := range m.iterators {
		if iter.Err() != nil {
			continue
		}
		// Skip exhausted iterators
		if iter.exhausted {
			continue
		}
		key := iter.Key()
		if key == nil {
			continue
		}
		if minKey == nil {
			minKey = append([]byte(nil), key...)
		} else if bytes.Compare(key, minKey) < 0 {
			minKey = append([]byte(nil), key...)
		}
	}
	return minKey
}

// activateIteratorsForKey activates all iterators whose current key matches
// the given key. For each matching iterator, it skips tombstoned blocks until
// a valid value is found, then pushes it to the heap.
func (m *KeyAwareMergingIterator) activateIteratorsForKey(key []byte) {
	for _, iter := range m.iterators {
		if iter.Err() != nil {
			continue
		}

		// Check if iterator has pending block for target key
		if iter.hasNextBlock && bytes.Equal(iter.nextKey, key) {
			if !iter.ActivatePending() {
				if iter.Err() != nil {
					m.err = iter.Err()
				}
				continue
			}
		}

		// Check if iterator is already on target key
		if bytes.Equal(iter.Key(), key) {
			m.activateIteratorForBlock(iter, key)
			continue
		}

		// Iterator is on a different key - try to advance to next block
		if !iter.NextBlock() {
			if iter.hasNextBlock && bytes.Equal(iter.nextKey, key) {
				if !iter.ActivatePending() {
					if iter.Err() != nil {
						m.err = iter.Err()
					}
				} else {
					m.activateIteratorForBlock(iter, key)
				}
			}
			continue
		}

		// Now check if we reached target key
		if bytes.Equal(iter.Key(), key) {
			m.activateIteratorForBlock(iter, key)
		}
	}
}

// activateIteratorForBlock pushes the first valid value from the iterator's
// current block into the heap. If the block is fully tombstoned, it advances
// to the next block for the same key.
func (m *KeyAwareMergingIterator) activateIteratorForBlock(iter *BlockValueIterator, targetKey []byte) {
	for {
		if iter.Next() {
			v := iter.Read()
			entry := &valueEntry{
				value:     v,
				iterator:  iter,
				key:       append([]byte(nil), iter.currentKey...),
				timestamp: v.UnixNano(),
				fileIdx:   iter.FileIdx(),
			}
			heap.Push(&m.heap, entry)
			return
		}

		// Current block exhausted, try next block
		if !iter.NextBlock() {
			// If there's a pending block for target key, activate it
			if iter.hasNextBlock && bytes.Equal(iter.nextKey, targetKey) {
				if !iter.ActivatePending() {
					if iter.Err() != nil {
						m.err = iter.Err()
					}
					return
				}
				// Now on target key, continue to push value
				continue
			}
			return
		}

		// Check if key changed
		if !bytes.Equal(iter.Key(), targetKey) {
			return
		}
		// Same key, new block - continue loop to call Next() again
	}
}

// popAndDedup pops the top entry and consumes all same-timestamp entries
// for the SAME key, keeping the value from the highest fileIdx.
// The winner's iterator is advanced after dedup completes to prevent
// cross-key value mixing in the heap during the dedup loop.
func (m *KeyAwareMergingIterator) popAndDedup() *valueEntry {
	if m.heap.Len() == 0 {
		return nil
	}

	top := heap.Pop(&m.heap).(*valueEntry)

	// Collect losers for deferred advancement
	var losers []*valueEntry

	// Consume all same-timestamp entries for the same key
	for m.heap.Len() > 0 {
		next := m.heap.Peek()
		if next.timestamp != top.timestamp {
			break
		}
		if !bytes.Equal(next.key, top.key) {
			break
		}
		popped := heap.Pop(&m.heap).(*valueEntry)

		// Newer file wins (higher fileIdx)
		if popped.fileIdx > top.fileIdx {
			losers = append(losers, top)
			top = popped
		} else {
			losers = append(losers, popped)
		}
	}

	// Advance losers' iterators and push their next values if still on the same key
	for _, loser := range losers {
		m.advanceAndPush(loser.iterator)
	}

	// Advance winner's iterator last
	m.advanceAndPush(top.iterator)

	return top
}

// advanceAndPush advances the iterator and pushes the next valid value to the heap.
// It loops through blocks for the same key until a non-tombstoned value is found,
// the key changes, or the iterator is exhausted.
func (m *KeyAwareMergingIterator) advanceAndPush(iter *BlockValueIterator) {
	if iter.Next() {
		v := iter.Read()
		entry := &valueEntry{
			value:     v,
			iterator:  iter,
			key:       append([]byte(nil), iter.currentKey...),
			timestamp: v.UnixNano(),
			fileIdx:   iter.FileIdx(),
		}
		heap.Push(&m.heap, entry)
		return
	}

	// Current block exhausted - call NextBlock() to pre-fetch next key's block
	// This populates hasNextBlock/nextKey for the next key transition
	if iter.NextBlock() {
		// Same key, new block - get next value
		if iter.Next() {
			v := iter.Read()
			entry := &valueEntry{
				value:     v,
				iterator:  iter,
				key:       append([]byte(nil), iter.currentKey...),
				timestamp: v.UnixNano(),
				fileIdx:   iter.FileIdx(),
			}
			heap.Push(&m.heap, entry)
			return
		}
		// Fall through - block exhausted or all tombstoned
	}
	// Either key changed (hasNextBlock now set) or iterator exhausted
	iter.exhausted = true
}

// moveToNextKey moves to the next key after the current one is exhausted.
func (m *KeyAwareMergingIterator) moveToNextKey() bool {
	// First, activate ALL pending blocks for all iterators.
	// This ensures all iterators are at their next keys before we determine
	// the minimum key to process.
	for _, iter := range m.iterators {
		if iter.Err() != nil {
			continue
		}
		if iter.hasNextBlock && len(iter.nextKey) > 0 {
			if !iter.ActivatePending() {
				if iter.Err() != nil {
					m.err = iter.Err()
				}
			} else {
				// Successfully activated a pending block - iterator is no longer exhausted
				iter.exhausted = false
			}
		}
	}

	// Now find minimum key among all iterators
	minKey := m.findMinKey()
	if minKey == nil {
		return false
	}

	// Set currentKey and activate iterators for minKey
	m.currentKey = minKey
	m.currentType = blockTypeUnset
	m.activateIteratorsForKey(minKey)
	return true
}

// encodeValues encodes a slice of values into a TSM block.
// On type mismatch, the mismatched value is skipped and the error is recorded
// in m.err. This prevents a single corrupted value from aborting compaction.
func (m *KeyAwareMergingIterator) encodeValues(typ byte, values []Value) ([]byte, error) {
	switch typ {
	case BlockFloat64:
		arr := tsdb.NewFloatArrayLen(0)
		for _, v := range values {
			fv, ok := v.(FloatValue)
			if !ok {
				m.err = fmt.Errorf("type mismatch: expected FloatValue, got %T for key %s", v, m.blockKey)
				continue
			}
			arr.Timestamps = append(arr.Timestamps, v.UnixNano())
			arr.Values = append(arr.Values, fv.value)
		}
		if arr.Len() == 0 {
			return nil, nil
		}
		return EncodeFloatArrayBlock(arr, nil)

	case BlockInteger:
		arr := tsdb.NewIntegerArrayLen(0)
		for _, v := range values {
			iv, ok := v.(IntegerValue)
			if !ok {
				m.err = fmt.Errorf("type mismatch: expected IntegerValue, got %T for key %s", v, m.blockKey)
				continue
			}
			arr.Timestamps = append(arr.Timestamps, v.UnixNano())
			arr.Values = append(arr.Values, iv.value)
		}
		if arr.Len() == 0 {
			return nil, nil
		}
		return EncodeIntegerArrayBlock(arr, nil)

	case BlockUnsigned:
		arr := tsdb.NewUnsignedArrayLen(0)
		for _, v := range values {
			uv, ok := v.(UnsignedValue)
			if !ok {
				m.err = fmt.Errorf("type mismatch: expected UnsignedValue, got %T for key %s", v, m.blockKey)
				continue
			}
			arr.Timestamps = append(arr.Timestamps, v.UnixNano())
			arr.Values = append(arr.Values, uv.value)
		}
		if arr.Len() == 0 {
			return nil, nil
		}
		return EncodeUnsignedArrayBlock(arr, nil)

	case BlockBoolean:
		arr := tsdb.NewBooleanArrayLen(0)
		for _, v := range values {
			bv, ok := v.(BooleanValue)
			if !ok {
				m.err = fmt.Errorf("type mismatch: expected BooleanValue, got %T for key %s", v, m.blockKey)
				continue
			}
			arr.Timestamps = append(arr.Timestamps, v.UnixNano())
			arr.Values = append(arr.Values, bv.value)
		}
		if arr.Len() == 0 {
			return nil, nil
		}
		return EncodeBooleanArrayBlock(arr, nil)

	case BlockString:
		arr := tsdb.NewStringArrayLen(0)
		for _, v := range values {
			sv, ok := v.(StringValue)
			if !ok {
				m.err = fmt.Errorf("type mismatch: expected StringValue, got %T for key %s", v, m.blockKey)
				continue
			}
			arr.Timestamps = append(arr.Timestamps, v.UnixNano())
			arr.Values = append(arr.Values, sv.value)
		}
		if arr.Len() == 0 {
			return nil, nil
		}
		return EncodeStringArrayBlock(arr, nil)

	default:
		return nil, fmt.Errorf("unknown block type: %d", typ)
	}
}

// NewStreamingKeyIterator creates a new KeyIterator that uses the streaming
// merge approach for full compaction. This is the entry point for integrating
// with the Compactor. interrupt is an optional channel for cancellation;
// closing it will cause iteration to stop with an error. Pass nil to disable.
func NewStreamingKeyIterator(tsmFiles []string, readers []*TSMReader, size int, interrupt chan struct{}) KeyIterator {
	iters := make([]*BlockValueIterator, len(readers))
	for i, r := range readers {
		iters[i] = NewBlockValueIterator(r, i)
	}

	mergeIter := NewKeyAwareMergingIterator(iters, size, interrupt)

	return &streamingKeyIterator{
		mergeIter: mergeIter,
		tsmFiles:  tsmFiles,
	}
}

// streamingKeyIterator implements KeyIterator using the streaming merge approach.
type streamingKeyIterator struct {
	mergeIter *KeyAwareMergingIterator
	tsmFiles  []string
	key       []byte
	minTime   int64
	maxTime   int64
	data      []byte
	err       error
}

func (s *streamingKeyIterator) Next() bool {
	for {
		if !s.mergeIter.Next() {
			if s.mergeIter.Err() != nil {
				s.err = s.mergeIter.Err()
			}
			return false
		}

		key, minTime, maxTime, data, err := s.mergeIter.Read()
		if err != nil {
			s.err = err
			return false
		}

		// Skip blocks where all values were skipped due to type mismatch
		if data == nil {
			continue
		}

		s.key = key
		s.minTime = minTime
		s.maxTime = maxTime
		s.data = data
		return true
	}
}

func (s *streamingKeyIterator) Read() (key []byte, minTime int64, maxTime int64, data []byte, err error) {
	keyCopy := make([]byte, len(s.key))
	copy(keyCopy, s.key)
	return keyCopy, s.minTime, s.maxTime, s.data, nil
}

func (s *streamingKeyIterator) Close() error {
	return s.mergeIter.Close()
}

func (s *streamingKeyIterator) Err() error {
	return s.err
}

func (s *streamingKeyIterator) EstimatedIndexSize() int {
	return s.mergeIter.EstimatedIndexSize()
}
