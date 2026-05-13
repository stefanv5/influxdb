package tsm1

import (
	"bytes"
	"fmt"
	"math"
	"sort"

	"github.com/influxdata/influxdb/tsdb"
)

// block represents a single block of data for a key. It holds the raw encoded
// bytes (typically a mmap slice) along with metadata needed for merge decisions.
// This is the same struct used by tsmBatchKeyIterator in compact.go.
type streamBlock struct {
	key                    []byte
	minTime, maxTime       int64
	typ                    byte
	b                      []byte
	tombstones             []TimeRange
	readMin, readMax       int64
}

func (bl *streamBlock) overlapsTimeRange(min, max int64) bool {
	return bl.minTime <= max && bl.maxTime >= min
}

func (bl *streamBlock) read() bool {
	return bl.readMin <= bl.minTime && bl.readMax >= bl.maxTime
}

func (bl *streamBlock) markRead(min, max int64) {
	if min < bl.readMin {
		bl.readMin = min
	}
	if max > bl.readMax {
		bl.readMax = max
	}
}

// blockTypeUnset is a sentinel value for currentType that does not collide
// with any valid block type (BlockFloat64=0 through BlockUnsigned=4).
const blockTypeUnset = byte(0xFF)

// BlockValueIterator iterates over raw blocks within a single TSM file.
type BlockValueIterator struct {
	r       *TSMReader
	iter    *BlockIterator
	fileIdx int

	// Current block state (raw)
	currentKey  []byte
	currentType byte
	rawBlock    []byte
	rawMinTime  int64
	rawMaxTime  int64

	// Pending state from NextBlock when key changes
	nextKey     []byte
	nextType    byte
	nextRaw     []byte
	nextMinTime int64
	nextMaxTime int64
	hasNext     bool

	initialized bool
	exhausted   bool
	err         error
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
func (it *BlockValueIterator) Init() bool {
	if !it.iter.Next() {
		if it.iter.Err() != nil {
			it.err = it.iter.Err()
		}
		it.exhausted = true
		return false
	}

	key, minTime, maxTime, typ, _, buf, err := it.iter.Read()
	if err != nil {
		it.err = err
		return false
	}

	it.currentKey = append([]byte(nil), key...)
	it.currentType = typ
	it.rawBlock = buf
	it.rawMinTime = minTime
	it.rawMaxTime = maxTime
	it.initialized = true
	return true
}

// NextBlock reads the next block from the underlying TSM iterator.
// If the key changes, the new key is cached and the method returns false.
// If the key is the same, the new block replaces the current one and returns true.
func (it *BlockValueIterator) NextBlock() bool {
	if !it.iter.Next() {
		if it.iter.Err() != nil {
			it.err = it.iter.Err()
		}
		it.exhausted = true
		return false
	}

	key, minTime, maxTime, typ, _, buf, err := it.iter.Read()
	if err != nil {
		it.err = err
		return false
	}

	if !bytes.Equal(key, it.currentKey) {
		it.nextKey = append(it.nextKey[:0], key...)
		it.nextType = typ
		it.nextRaw = buf
		it.nextMinTime = minTime
		it.nextMaxTime = maxTime
		it.hasNext = true
		return false
	}

	it.rawBlock = buf
	it.rawMinTime = minTime
	it.rawMaxTime = maxTime
	return true
}

// ActivatePending activates the pending block cached by NextBlock.
func (it *BlockValueIterator) ActivatePending() bool {
	if !it.hasNext {
		return false
	}

	it.currentKey = append(it.currentKey[:0], it.nextKey...)
	it.currentType = it.nextType
	it.rawBlock = it.nextRaw
	it.rawMinTime = it.nextMinTime
	it.rawMaxTime = it.nextMaxTime

	it.nextKey = nil
	it.nextRaw = nil
	it.hasNext = false
	return true
}

// Key returns the current key being iterated.
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

// RawBlock returns the raw encoded block bytes (mmap slice).
func (it *BlockValueIterator) RawBlock() []byte {
	return it.rawBlock
}

// MinTime returns the minimum time of the current block.
func (it *BlockValueIterator) MinTime() int64 {
	return it.rawMinTime
}

// MaxTime returns the maximum time of the current block.
func (it *BlockValueIterator) MaxTime() int64 {
	return it.rawMaxTime
}

// Err returns any error encountered during iteration.
func (it *BlockValueIterator) Err() error {
	return it.err
}

// FileIdx returns the file index of the TSM reader this iterator is bound to.
func (it *BlockValueIterator) FileIdx() int {
	return it.fileIdx
}

// TombstoneRange returns the tombstone ranges for the given key.
func (it *BlockValueIterator) TombstoneRange(key []byte) []TimeRange {
	return it.r.TombstoneRange(key)
}

// Reader returns the underlying TSMReader.
func (it *BlockValueIterator) Reader() *TSMReader {
	return it.r
}

// Close releases resources held by this iterator.
func (it *BlockValueIterator) Close() {
	if it.exhausted {
		return
	}
	it.exhausted = true
	it.rawBlock = nil
	it.nextRaw = nil
	it.currentKey = nil
	it.nextKey = nil
}

// streamBlocks is a sortable slice of streamBlock pointers.
type streamBlocks []*streamBlock

func (a streamBlocks) Len() int           { return len(a) }
func (a streamBlocks) Less(i, j int) bool { return a[i].minTime < a[j].minTime }
func (a streamBlocks) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// KeyAwareMergingIterator merges multiple BlockValueIterators into a single
// stream of blocks using overlap-aware key-level merge. For each key:
//   - Non-overlapping blocks pass through as raw mmap data (zero decode/re-encode)
//   - Overlapping blocks are decoded into typed arrays and merged
type KeyAwareMergingIterator struct {
	iterators  []*BlockValueIterator
	currentKey []byte
	currentTyp byte
	blockKey   []byte

	// Per-file block buffers: buf[i] holds unread blocks from iterators[i]
	buf [][]*streamBlock

	// Collected blocks for current key (across all files)
	blocks streamBlocks

	// Output chunks for current key (raw pass-through or encoded)
	chunks   streamBlocks
	chunkIdx int

	// Merged typed arrays (for overlapping blocks, reused across keys)
	mergedFloat    tsdb.FloatArray
	mergedInteger  tsdb.IntegerArray
	mergedUnsigned tsdb.UnsignedArray
	mergedBoolean  tsdb.BooleanArray
	mergedString   tsdb.StringArray

	// Reusable key buffer for Read()
	keyBuf []byte

	// State
	initialized bool
	exhausted   bool
	closed      bool
	err         error

	// Interrupt channel for cancellation
	interrupt chan struct{}

	bufSize int
}

// NewKeyAwareMergingIterator creates a new overlap-aware merge iterator.
func NewKeyAwareMergingIterator(iters []*BlockValueIterator, bufSize int, interrupt chan struct{}) *KeyAwareMergingIterator {
	return &KeyAwareMergingIterator{
		iterators: iters,
		bufSize:   bufSize,
		interrupt: interrupt,
		buf:       make([][]*streamBlock, len(iters)),
	}
}

// Next advances to the next output block. Returns true if there are more blocks.
func (m *KeyAwareMergingIterator) Next() bool {
	if m.exhausted || m.closed {
		return false
	}

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

	// If we have buffered chunks from the current key, yield the next one
	if m.chunkIdx < len(m.chunks) {
		return true
	}

	// Current key fully consumed, move to next key
	m.chunks = nil
	m.chunkIdx = 0

	if !m.moveToNextKey() {
		m.exhausted = true
		return false
	}

	// Process the new key: collect blocks, detect overlap, merge or pass-through
	m.processCurrentKey()

	if len(m.chunks) == 0 {
		return m.Next()
	}

	return true
}

// Read returns the current block's key, time range, and encoded data.
func (m *KeyAwareMergingIterator) Read() (key []byte, minTime int64, maxTime int64, data []byte, err error) {
	if m.chunkIdx >= len(m.chunks) {
		return nil, 0, 0, nil, fmt.Errorf("no values in buffer")
	}

	if cap(m.keyBuf) < len(m.blockKey) {
		m.keyBuf = make([]byte, len(m.blockKey))
	} else {
		m.keyBuf = m.keyBuf[:len(m.blockKey)]
	}
	copy(m.keyBuf, m.blockKey)
	key = m.keyBuf

	entry := m.chunks[m.chunkIdx]
	m.chunkIdx++

	return key, entry.minTime, entry.maxTime, entry.b, nil
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
	m.blocks = nil
	m.chunks = nil
	m.mergedFloat = tsdb.FloatArray{}
	m.mergedInteger = tsdb.IntegerArray{}
	m.mergedUnsigned = tsdb.UnsignedArray{}
	m.mergedBoolean = tsdb.BooleanArray{}
	m.mergedString = tsdb.StringArray{}
	return nil
}

// Err returns any error encountered during iteration.
func (m *KeyAwareMergingIterator) Err() error {
	return m.err
}

// EstimatedIndexSize returns the estimated index size based on the input readers.
func (m *KeyAwareMergingIterator) EstimatedIndexSize() int {
	var size uint32
	for _, iter := range m.iterators {
		size += iter.Reader().IndexSize()
	}
	return int(size) / len(m.iterators)
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

// initIterators initializes all iterators, reads one block per file, and finds
// the first (minimum) key.
func (m *KeyAwareMergingIterator) initIterators() bool {
	for _, iter := range m.iterators {
		if iter.initialized {
			continue
		}
		if !iter.Init() {
			if iter.Err() != nil {
				m.err = iter.Err()
				return false
			}
		}
	}

	minKey := m.findMinKey()
	if minKey == nil {
		return false
	}

	m.currentKey = append(m.currentKey[:0], minKey...)
	m.blockKey = append(m.blockKey[:0], minKey...)
	m.currentTyp = blockTypeUnset
	return true
}

// findMinKey scans all iterators and returns the lexicographically smallest key.
func (m *KeyAwareMergingIterator) findMinKey() []byte {
	var minKey []byte
	for _, iter := range m.iterators {
		if iter.Err() != nil || iter.exhausted {
			continue
		}
		key := iter.Key()
		if key == nil {
			continue
		}
		if minKey == nil || bytes.Compare(key, minKey) < 0 {
			minKey = append(minKey[:0], key...)
		}
	}
	return minKey
}

// moveToNextKey advances past the current key and finds the next minimum key.
func (m *KeyAwareMergingIterator) moveToNextKey() bool {
	// Activate all pending blocks (from iterators that had key changes)
	for _, iter := range m.iterators {
		if iter.Err() != nil {
			continue
		}
		if iter.hasNext {
			if !iter.ActivatePending() {
				if iter.Err() != nil {
					m.err = iter.Err()
				}
			} else {
				iter.exhausted = false
			}
		}
	}

	minKey := m.findMinKey()
	if minKey == nil {
		return false
	}

	m.currentKey = append(m.currentKey[:0], minKey...)
	m.blockKey = append(m.blockKey[:0], minKey...)
	m.currentTyp = blockTypeUnset
	return true
}

// readBlocksForKey reads all blocks for the current key from each file into
// m.blocks. For each file, it reads from the per-file buffer first, then from
// the iterator until the key changes (similar to tsmBatchKeyIterator.Next inner loop).
func (m *KeyAwareMergingIterator) readBlocksForKey() {
	m.blocks = m.blocks[:0]

	for i, iter := range m.iterators {
		if iter.Err() != nil || iter.exhausted {
			continue
		}

		// Drain per-file buffer for blocks matching current key
		for len(m.buf[i]) > 0 && bytes.Equal(m.buf[i][0].key, m.currentKey) {
			m.blocks = append(m.blocks, m.buf[i][0])
			m.buf[i] = m.buf[i][1:]
		}

		// Check if iterator is on the target key
		if !bytes.Equal(iter.Key(), m.currentKey) {
			if iter.hasNext && bytes.Equal(iter.nextKey, m.currentKey) {
				if !iter.ActivatePending() {
					if iter.Err() != nil {
						m.err = iter.Err()
					}
					continue
				}
			} else {
				continue
			}
		}

		// Read all blocks for this key from this iterator (like batch inner loop)
		blockKey := iter.Key()
		for {
			tombstones := iter.TombstoneRange(m.currentKey)
			m.blocks = append(m.blocks, &streamBlock{
				key:        blockKey,
				minTime:    iter.MinTime(),
				maxTime:    iter.MaxTime(),
				typ:        iter.Type(),
				b:          iter.RawBlock(),
				tombstones: tombstones,
				readMin:    math.MaxInt64,
				readMax:    math.MinInt64,
			})

			if !iter.NextBlock() {
				break
			}
		}
	}
}

// needsDedup checks whether the collected blocks require deduplication.
// Returns true if blocks overlap in time or have tombstones.
func (m *KeyAwareMergingIterator) needsDedup() bool {
	if len(m.blocks) <= 1 {
		// Single block: only needs dedup if it has tombstones
		return len(m.blocks) == 1 && len(m.blocks[0].tombstones) > 0
	}

	sort.Stable(m.blocks)

	for i := 0; i < len(m.blocks); i++ {
		if len(m.blocks[i].tombstones) > 0 {
			return true
		}
		if i > 0 && m.blocks[i].overlapsTimeRange(m.blocks[i-1].minTime, m.blocks[i-1].maxTime) {
			return true
		}
	}
	return false
}

// processCurrentKey collects blocks for the current key, detects overlaps,
// and either passes non-overlapping blocks through or merges overlapping ones.
func (m *KeyAwareMergingIterator) processCurrentKey() {
	m.chunks = nil

	// Phase 1: Collect all blocks for the current key from all files
	m.readBlocksForKey()

	if len(m.blocks) == 0 {
		return
	}

	// Phase 2: Set currentTyp from first block
	if m.currentTyp == blockTypeUnset {
		m.currentTyp = m.blocks[0].typ
	}

	// Phase 3: Check overlap and dispatch
	if m.needsDedup() {
		// Overlapping: decode, merge, chunk
		m.decodeAndMerge()
		if m.err != nil {
			return
		}
		m.chunkMergedArray()
	} else {
		// Non-overlapping: pass through as raw data
		m.chunks = make(streamBlocks, len(m.blocks))
		copy(m.chunks, m.blocks)
		for _, blk := range m.chunks {
			blk.markRead(blk.minTime, blk.maxTime)
		}
	}
}

// decodeAndMerge decodes overlapping blocks into typed arrays, applies
// tombstones, and merges them (same algorithm as tsmBatchKeyIterator).
func (m *KeyAwareMergingIterator) decodeAndMerge() {
	m.mergedFloat = tsdb.FloatArray{}
	m.mergedInteger = tsdb.IntegerArray{}
	m.mergedUnsigned = tsdb.UnsignedArray{}
	m.mergedBoolean = tsdb.BooleanArray{}
	m.mergedString = tsdb.StringArray{}

	for _, blk := range m.blocks {
		if blk.read() {
			continue
		}

		switch blk.typ {
		case BlockFloat64:
			m.decodeAndMergeFloat(blk)
		case BlockInteger:
			m.decodeAndMergeInteger(blk)
		case BlockUnsigned:
			m.decodeAndMergeUnsigned(blk)
		case BlockBoolean:
			m.decodeAndMergeBoolean(blk)
		case BlockString:
			m.decodeAndMergeString(blk)
		default:
			m.err = fmt.Errorf("unknown block type: %d for key %s", blk.typ, m.currentKey)
			return
		}
		if m.err != nil {
			return
		}
	}
}

func (m *KeyAwareMergingIterator) decodeAndMergeFloat(blk *streamBlock) {
	var v tsdb.FloatArray
	if err := DecodeFloatArrayBlock(blk.b, &v); err != nil {
		m.err = fmt.Errorf("decode error: float key=%s: %v", m.currentKey, err)
		return
	}
	for _, ts := range blk.tombstones {
		v.Exclude(ts.Min, ts.Max)
	}
	if v.Len() > 0 {
		blk.markRead(v.MinTime(), v.MaxTime())
		if m.mergedFloat.Len() == 0 {
			m.mergedFloat.Timestamps = append(m.mergedFloat.Timestamps[:0], v.Timestamps...)
			m.mergedFloat.Values = append(m.mergedFloat.Values[:0], v.Values...)
		} else {
			m.mergedFloat.Merge(&v)
		}
	}
}

func (m *KeyAwareMergingIterator) decodeAndMergeInteger(blk *streamBlock) {
	var v tsdb.IntegerArray
	if err := DecodeIntegerArrayBlock(blk.b, &v); err != nil {
		m.err = fmt.Errorf("decode error: integer key=%s: %v", m.currentKey, err)
		return
	}
	for _, ts := range blk.tombstones {
		v.Exclude(ts.Min, ts.Max)
	}
	if v.Len() > 0 {
		blk.markRead(v.MinTime(), v.MaxTime())
		if m.mergedInteger.Len() == 0 {
			m.mergedInteger.Timestamps = append(m.mergedInteger.Timestamps[:0], v.Timestamps...)
			m.mergedInteger.Values = append(m.mergedInteger.Values[:0], v.Values...)
		} else {
			m.mergedInteger.Merge(&v)
		}
	}
}

func (m *KeyAwareMergingIterator) decodeAndMergeUnsigned(blk *streamBlock) {
	var v tsdb.UnsignedArray
	if err := DecodeUnsignedArrayBlock(blk.b, &v); err != nil {
		m.err = fmt.Errorf("decode error: unsigned key=%s: %v", m.currentKey, err)
		return
	}
	for _, ts := range blk.tombstones {
		v.Exclude(ts.Min, ts.Max)
	}
	if v.Len() > 0 {
		blk.markRead(v.MinTime(), v.MaxTime())
		if m.mergedUnsigned.Len() == 0 {
			m.mergedUnsigned.Timestamps = append(m.mergedUnsigned.Timestamps[:0], v.Timestamps...)
			m.mergedUnsigned.Values = append(m.mergedUnsigned.Values[:0], v.Values...)
		} else {
			m.mergedUnsigned.Merge(&v)
		}
	}
}

func (m *KeyAwareMergingIterator) decodeAndMergeBoolean(blk *streamBlock) {
	var v tsdb.BooleanArray
	if err := DecodeBooleanArrayBlock(blk.b, &v); err != nil {
		m.err = fmt.Errorf("decode error: boolean key=%s: %v", m.currentKey, err)
		return
	}
	for _, ts := range blk.tombstones {
		v.Exclude(ts.Min, ts.Max)
	}
	if v.Len() > 0 {
		blk.markRead(v.MinTime(), v.MaxTime())
		if m.mergedBoolean.Len() == 0 {
			m.mergedBoolean.Timestamps = append(m.mergedBoolean.Timestamps[:0], v.Timestamps...)
			m.mergedBoolean.Values = append(m.mergedBoolean.Values[:0], v.Values...)
		} else {
			m.mergedBoolean.Merge(&v)
		}
	}
}

func (m *KeyAwareMergingIterator) decodeAndMergeString(blk *streamBlock) {
	var v tsdb.StringArray
	if err := DecodeStringArrayBlock(blk.b, &v); err != nil {
		m.err = fmt.Errorf("decode error: string key=%s: %v", m.currentKey, err)
		return
	}
	for _, ts := range blk.tombstones {
		v.Exclude(ts.Min, ts.Max)
	}
	if v.Len() > 0 {
		blk.markRead(v.MinTime(), v.MaxTime())
		if m.mergedString.Len() == 0 {
			m.mergedString.Timestamps = append(m.mergedString.Timestamps[:0], v.Timestamps...)
			m.mergedString.Values = append(m.mergedString.Values[:0], v.Values...)
		} else {
			m.mergedString.Merge(&v)
		}
	}
}

// chunkMergedArray splits the merged typed array into encoded blocks.
func (m *KeyAwareMergingIterator) chunkMergedArray() {
	switch m.currentTyp {
	case BlockFloat64:
		m.chunkFloat()
	case BlockInteger:
		m.chunkInteger()
	case BlockUnsigned:
		m.chunkUnsigned()
	case BlockBoolean:
		m.chunkBoolean()
	case BlockString:
		m.chunkString()
	}
}

func (m *KeyAwareMergingIterator) chunkFloat() {
	for m.mergedFloat.Len() > 0 {
		if m.mergedFloat.Len() > m.bufSize {
			var values tsdb.FloatArray
			values.Timestamps = m.mergedFloat.Timestamps[:m.bufSize]
			minTime, maxTime := values.Timestamps[0], values.Timestamps[len(values.Timestamps)-1]
			values.Values = m.mergedFloat.Values[:m.bufSize]

			cb, err := EncodeFloatArrayBlock(&values, nil)
			if err != nil {
				m.err = fmt.Errorf("encode error: float key=%s: %v", m.currentKey, err)
				return
			}
			m.chunks = append(m.chunks, &streamBlock{
				key:     m.blockKey,
				minTime: minTime,
				maxTime: maxTime,
				b:       cb,
			})
			m.mergedFloat.Timestamps = m.mergedFloat.Timestamps[m.bufSize:]
			m.mergedFloat.Values = m.mergedFloat.Values[m.bufSize:]
			continue
		}

		minTime := m.mergedFloat.Timestamps[0]
		maxTime := m.mergedFloat.Timestamps[m.mergedFloat.Len()-1]
		cb, err := EncodeFloatArrayBlock(&m.mergedFloat, nil)
		if err != nil {
			m.err = fmt.Errorf("encode error: float key=%s: %v", m.currentKey, err)
			return
		}
		m.chunks = append(m.chunks, &streamBlock{
			key:     m.blockKey,
			minTime: minTime,
			maxTime: maxTime,
			b:       cb,
		})
		m.mergedFloat = tsdb.FloatArray{}
	}
}

func (m *KeyAwareMergingIterator) chunkInteger() {
	for m.mergedInteger.Len() > 0 {
		if m.mergedInteger.Len() > m.bufSize {
			var values tsdb.IntegerArray
			values.Timestamps = m.mergedInteger.Timestamps[:m.bufSize]
			minTime, maxTime := values.Timestamps[0], values.Timestamps[len(values.Timestamps)-1]
			values.Values = m.mergedInteger.Values[:m.bufSize]

			cb, err := EncodeIntegerArrayBlock(&values, nil)
			if err != nil {
				m.err = fmt.Errorf("encode error: integer key=%s: %v", m.currentKey, err)
				return
			}
			m.chunks = append(m.chunks, &streamBlock{
				key:     m.blockKey,
				minTime: minTime,
				maxTime: maxTime,
				b:       cb,
			})
			m.mergedInteger.Timestamps = m.mergedInteger.Timestamps[m.bufSize:]
			m.mergedInteger.Values = m.mergedInteger.Values[m.bufSize:]
			continue
		}

		minTime := m.mergedInteger.Timestamps[0]
		maxTime := m.mergedInteger.Timestamps[m.mergedInteger.Len()-1]
		cb, err := EncodeIntegerArrayBlock(&m.mergedInteger, nil)
		if err != nil {
			m.err = fmt.Errorf("encode error: integer key=%s: %v", m.currentKey, err)
			return
		}
		m.chunks = append(m.chunks, &streamBlock{
			key:     m.blockKey,
			minTime: minTime,
			maxTime: maxTime,
			b:       cb,
		})
		m.mergedInteger = tsdb.IntegerArray{}
	}
}

func (m *KeyAwareMergingIterator) chunkUnsigned() {
	for m.mergedUnsigned.Len() > 0 {
		if m.mergedUnsigned.Len() > m.bufSize {
			var values tsdb.UnsignedArray
			values.Timestamps = m.mergedUnsigned.Timestamps[:m.bufSize]
			minTime, maxTime := values.Timestamps[0], values.Timestamps[len(values.Timestamps)-1]
			values.Values = m.mergedUnsigned.Values[:m.bufSize]

			cb, err := EncodeUnsignedArrayBlock(&values, nil)
			if err != nil {
				m.err = fmt.Errorf("encode error: unsigned key=%s: %v", m.currentKey, err)
				return
			}
			m.chunks = append(m.chunks, &streamBlock{
				key:     m.blockKey,
				minTime: minTime,
				maxTime: maxTime,
				b:       cb,
			})
			m.mergedUnsigned.Timestamps = m.mergedUnsigned.Timestamps[m.bufSize:]
			m.mergedUnsigned.Values = m.mergedUnsigned.Values[m.bufSize:]
			continue
		}

		minTime := m.mergedUnsigned.Timestamps[0]
		maxTime := m.mergedUnsigned.Timestamps[m.mergedUnsigned.Len()-1]
		cb, err := EncodeUnsignedArrayBlock(&m.mergedUnsigned, nil)
		if err != nil {
			m.err = fmt.Errorf("encode error: unsigned key=%s: %v", m.currentKey, err)
			return
		}
		m.chunks = append(m.chunks, &streamBlock{
			key:     m.blockKey,
			minTime: minTime,
			maxTime: maxTime,
			b:       cb,
		})
		m.mergedUnsigned = tsdb.UnsignedArray{}
	}
}

func (m *KeyAwareMergingIterator) chunkBoolean() {
	for m.mergedBoolean.Len() > 0 {
		if m.mergedBoolean.Len() > m.bufSize {
			var values tsdb.BooleanArray
			values.Timestamps = m.mergedBoolean.Timestamps[:m.bufSize]
			minTime, maxTime := values.Timestamps[0], values.Timestamps[len(values.Timestamps)-1]
			values.Values = m.mergedBoolean.Values[:m.bufSize]

			cb, err := EncodeBooleanArrayBlock(&values, nil)
			if err != nil {
				m.err = fmt.Errorf("encode error: boolean key=%s: %v", m.currentKey, err)
				return
			}
			m.chunks = append(m.chunks, &streamBlock{
				key:     m.blockKey,
				minTime: minTime,
				maxTime: maxTime,
				b:       cb,
			})
			m.mergedBoolean.Timestamps = m.mergedBoolean.Timestamps[m.bufSize:]
			m.mergedBoolean.Values = m.mergedBoolean.Values[m.bufSize:]
			continue
		}

		minTime := m.mergedBoolean.Timestamps[0]
		maxTime := m.mergedBoolean.Timestamps[m.mergedBoolean.Len()-1]
		cb, err := EncodeBooleanArrayBlock(&m.mergedBoolean, nil)
		if err != nil {
			m.err = fmt.Errorf("encode error: boolean key=%s: %v", m.currentKey, err)
			return
		}
		m.chunks = append(m.chunks, &streamBlock{
			key:     m.blockKey,
			minTime: minTime,
			maxTime: maxTime,
			b:       cb,
		})
		m.mergedBoolean = tsdb.BooleanArray{}
	}
}

func (m *KeyAwareMergingIterator) chunkString() {
	for m.mergedString.Len() > 0 {
		if m.mergedString.Len() > m.bufSize {
			var values tsdb.StringArray
			values.Timestamps = m.mergedString.Timestamps[:m.bufSize]
			minTime, maxTime := values.Timestamps[0], values.Timestamps[len(values.Timestamps)-1]
			values.Values = m.mergedString.Values[:m.bufSize]

			cb, err := EncodeStringArrayBlock(&values, nil)
			if err != nil {
				m.err = fmt.Errorf("encode error: string key=%s: %v", m.currentKey, err)
				return
			}
			m.chunks = append(m.chunks, &streamBlock{
				key:     m.blockKey,
				minTime: minTime,
				maxTime: maxTime,
				b:       cb,
			})
			m.mergedString.Timestamps = m.mergedString.Timestamps[m.bufSize:]
			m.mergedString.Values = m.mergedString.Values[m.bufSize:]
			continue
		}

		minTime := m.mergedString.Timestamps[0]
		maxTime := m.mergedString.Timestamps[m.mergedString.Len()-1]
		cb, err := EncodeStringArrayBlock(&m.mergedString, nil)
		if err != nil {
			m.err = fmt.Errorf("encode error: string key=%s: %v", m.currentKey, err)
			return
		}
		m.chunks = append(m.chunks, &streamBlock{
			key:     m.blockKey,
			minTime: minTime,
			maxTime: maxTime,
			b:       cb,
		})
		m.mergedString = tsdb.StringArray{}
	}
}

// NewStreamingKeyIterator creates a new KeyIterator that uses the streaming
// merge approach for full compaction.
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
	keyBuf    []byte
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

		if data == nil {
			continue
		}

		if cap(s.keyBuf) < len(key) {
			s.keyBuf = make([]byte, len(key))
		} else {
			s.keyBuf = s.keyBuf[:len(key)]
		}
		copy(s.keyBuf, key)
		s.key = s.keyBuf
		s.minTime = minTime
		s.maxTime = maxTime
		s.data = data
		return true
	}
}

func (s *streamingKeyIterator) Read() (key []byte, minTime int64, maxTime int64, data []byte, err error) {
	return s.key, s.minTime, s.maxTime, s.data, nil
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
