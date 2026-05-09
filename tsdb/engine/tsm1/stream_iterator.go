package tsm1

import (
	"bytes"
	"fmt"

	"github.com/influxdata/influxdb/tsdb"
)

// blockTypeUnset is a sentinel value for currentType that does not collide
// with any valid block type (BlockFloat64=0 through BlockUnsigned=4).
const blockTypeUnset = byte(0xFF)

// BlockValueIterator iterates over raw blocks within a single TSM file.
// It yields raw (encoded) blocks rather than decoded values, allowing the
// upper layer to perform batch decode/merge without per-value interface boxing.
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

	// Same key - replace current block
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

// RawBlock returns the raw encoded block bytes.
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

// rawBlockEntry holds a raw block's data along with its tombstone ranges.
type rawBlockEntry struct {
	rawBlock   []byte
	typ        byte
	tombstones []TimeRange
}

// chunkEntry holds an encoded output block along with its time range.
type chunkEntry struct {
	data    []byte
	minTime int64
	maxTime int64
}

// KeyAwareMergingIterator merges multiple BlockValueIterators into a single
// stream of blocks using batch-style key-level merge. For each key, it collects
// all blocks from all readers, decodes into typed arrays, applies tombstones,
// merges sorted arrays, and chunks the output. This avoids per-value interface
// boxing and per-value heap allocations.
type KeyAwareMergingIterator struct {
	iterators []*BlockValueIterator
	currentKey []byte
	currentType byte
	blockKey    []byte

	// Batch merge state: collected blocks for current key
	keyBlocks []rawBlockEntry

	// Merged typed arrays (one per type, reused across keys)
	mergedFloat    tsdb.FloatArray
	mergedInteger  tsdb.IntegerArray
	mergedUnsigned tsdb.UnsignedArray
	mergedBoolean  tsdb.BooleanArray
	mergedString   tsdb.StringArray

	// Output chunks from the merged array
	chunks    []chunkEntry
	chunkIdx  int

	// Reusable key buffer for Read()
	keyBuf []byte

	// State
	initialized bool
	exhausted   bool
	closed      bool
	err         error

	// Interrupt channel for cancellation
	interrupt chan struct{}

	// For tracking estimated index size
	estimatedIndexSize int

	bufSize int
}

// NewKeyAwareMergingIterator creates a new batch-merge iterator.
func NewKeyAwareMergingIterator(iters []*BlockValueIterator, bufSize int, interrupt chan struct{}) *KeyAwareMergingIterator {
	return &KeyAwareMergingIterator{
		iterators: iters,
		bufSize:   bufSize,
		interrupt: interrupt,
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

	// Process the new key: collect blocks, merge, chunk
	m.processCurrentKey()

	if len(m.chunks) == 0 {
		// All values tombstoned or empty, try next key
		return m.Next()
	}

	return true
}

// Read returns the current block's key, time range, and encoded data.
func (m *KeyAwareMergingIterator) Read() (key []byte, minTime int64, maxTime int64, data []byte, err error) {
	if m.chunkIdx >= len(m.chunks) {
		return nil, 0, 0, nil, fmt.Errorf("no values in buffer")
	}

	// Copy key into reusable buffer
	if cap(m.keyBuf) < len(m.blockKey) {
		m.keyBuf = make([]byte, len(m.blockKey))
	} else {
		m.keyBuf = m.keyBuf[:len(m.blockKey)]
	}
	copy(m.keyBuf, m.blockKey)
	key = m.keyBuf

	entry := &m.chunks[m.chunkIdx]
	m.chunkIdx++

	return key, entry.minTime, entry.maxTime, entry.data, nil
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
	m.chunks = nil
	m.keyBlocks = nil
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

// EstimatedIndexSize returns the estimated index size.
func (m *KeyAwareMergingIterator) EstimatedIndexSize() int {
	return m.estimatedIndexSize
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

// initIterators initializes all iterators and finds the first key.
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
	m.currentType = blockTypeUnset
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

// moveToNextKey moves to the next key after the current one is exhausted.
func (m *KeyAwareMergingIterator) moveToNextKey() bool {
	// Activate all pending blocks first
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
	m.currentType = blockTypeUnset
	return true
}

// processCurrentKey collects all blocks for the current key from all readers,
// decodes them into typed arrays, applies tombstones, merges, and chunks.
func (m *KeyAwareMergingIterator) processCurrentKey() {
	m.keyBlocks = m.keyBlocks[:0]

	// Phase 1: Collect all blocks for the current key from all readers
	for _, iter := range m.iterators {
		if iter.Err() != nil || iter.exhausted {
			continue
		}

		// Check if iterator is on the target key
		if !bytes.Equal(iter.Key(), m.currentKey) {
			// Try pending block
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

		// Collect all blocks for this key from this iterator
		for {
			tombstones := iter.TombstoneRange(m.currentKey)
			raw := iter.RawBlock()
			rawCopy := make([]byte, len(raw))
			copy(rawCopy, raw)
			m.keyBlocks = append(m.keyBlocks, rawBlockEntry{
				rawBlock:   rawCopy,
				typ:        iter.Type(),
				tombstones: tombstones,
			})

			if !iter.NextBlock() {
				// Key changed or exhausted - hasNext is set if key changed
				break
			}
			// Same key, more blocks - continue collecting
		}
	}

	if len(m.keyBlocks) == 0 {
		return
	}

	// Phase 2: Set currentType from first block
	if m.currentType == blockTypeUnset {
		m.currentType = m.keyBlocks[0].typ
	}

	// Phase 3: Decode, apply tombstones, and merge
	m.decodeAndMerge()
	if m.err != nil {
		return
	}

	// Phase 4: Chunk the merged array into encoded blocks
	m.chunkMergedArray()
}

// decodeAndMerge decodes all collected blocks into typed arrays, applies
// tombstones, and merges them.
func (m *KeyAwareMergingIterator) decodeAndMerge() {
	// Reset merged arrays
	m.mergedFloat = tsdb.FloatArray{}
	m.mergedInteger = tsdb.IntegerArray{}
	m.mergedUnsigned = tsdb.UnsignedArray{}
	m.mergedBoolean = tsdb.BooleanArray{}
	m.mergedString = tsdb.StringArray{}

	for _, entry := range m.keyBlocks {
		raw := entry.rawBlock
		typ := entry.typ

		switch typ {
		case BlockFloat64:
			v := &tsdb.FloatArray{}
			if err := DecodeFloatArrayBlock(raw, v); err != nil {
				m.err = fmt.Errorf("decode error: float key=%s: %v", m.currentKey, err)
				return
			}
			for _, ts := range entry.tombstones {
				v.Exclude(ts.Min, ts.Max)
			}
			if v.Len() > 0 {
				if m.mergedFloat.Len() == 0 {
					m.mergedFloat.Timestamps = append(m.mergedFloat.Timestamps[:0], v.Timestamps...)
					m.mergedFloat.Values = append(m.mergedFloat.Values[:0], v.Values...)
				} else {
					m.mergedFloat.Merge(v)
				}
			}

		case BlockInteger:
			v := &tsdb.IntegerArray{}
			if err := DecodeIntegerArrayBlock(raw, v); err != nil {
				m.err = fmt.Errorf("decode error: integer key=%s: %v", m.currentKey, err)
				return
			}
			for _, ts := range entry.tombstones {
				v.Exclude(ts.Min, ts.Max)
			}
			if v.Len() > 0 {
				if m.mergedInteger.Len() == 0 {
					m.mergedInteger.Timestamps = append(m.mergedInteger.Timestamps[:0], v.Timestamps...)
					m.mergedInteger.Values = append(m.mergedInteger.Values[:0], v.Values...)
				} else {
					m.mergedInteger.Merge(v)
				}
			}

		case BlockUnsigned:
			v := &tsdb.UnsignedArray{}
			if err := DecodeUnsignedArrayBlock(raw, v); err != nil {
				m.err = fmt.Errorf("decode error: unsigned key=%s: %v", m.currentKey, err)
				return
			}
			for _, ts := range entry.tombstones {
				v.Exclude(ts.Min, ts.Max)
			}
			if v.Len() > 0 {
				if m.mergedUnsigned.Len() == 0 {
					m.mergedUnsigned.Timestamps = append(m.mergedUnsigned.Timestamps[:0], v.Timestamps...)
					m.mergedUnsigned.Values = append(m.mergedUnsigned.Values[:0], v.Values...)
				} else {
					m.mergedUnsigned.Merge(v)
				}
			}

		case BlockBoolean:
			v := &tsdb.BooleanArray{}
			if err := DecodeBooleanArrayBlock(raw, v); err != nil {
				m.err = fmt.Errorf("decode error: boolean key=%s: %v", m.currentKey, err)
				return
			}
			for _, ts := range entry.tombstones {
				v.Exclude(ts.Min, ts.Max)
			}
			if v.Len() > 0 {
				if m.mergedBoolean.Len() == 0 {
					m.mergedBoolean.Timestamps = append(m.mergedBoolean.Timestamps[:0], v.Timestamps...)
					m.mergedBoolean.Values = append(m.mergedBoolean.Values[:0], v.Values...)
				} else {
					m.mergedBoolean.Merge(v)
				}
			}

		case BlockString:
			v := &tsdb.StringArray{}
			if err := DecodeStringArrayBlock(raw, v); err != nil {
				m.err = fmt.Errorf("decode error: string key=%s: %v", m.currentKey, err)
				return
			}
			for _, ts := range entry.tombstones {
				v.Exclude(ts.Min, ts.Max)
			}
			if v.Len() > 0 {
				if m.mergedString.Len() == 0 {
					m.mergedString.Timestamps = append(m.mergedString.Timestamps[:0], v.Timestamps...)
					m.mergedString.Values = append(m.mergedString.Values[:0], v.Values...)
				} else {
					m.mergedString.Merge(v)
				}
			}

		default:
			m.err = fmt.Errorf("unknown block type: %d for key %s", typ, m.currentKey)
			return
		}
	}
}

// chunkMergedArray splits the merged typed array into encoded blocks of at most
// bufSize values each.
func (m *KeyAwareMergingIterator) chunkMergedArray() {
	m.chunks = nil

	switch m.currentType {
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
	arr := &m.mergedFloat
	for arr.Len() > 0 {
		sz := m.bufSize
		if sz > arr.Len() {
			sz = arr.Len()
		}

		// Capture time range before encoding — EncodeFloatArrayBlock mutates
		// timestamps in-place (converts to deltas).
		minTime := arr.Timestamps[0]
		maxTime := arr.Timestamps[sz-1]

		enc := tsdb.FloatArray{
			Timestamps: arr.Timestamps[:sz],
			Values:     arr.Values[:sz],
		}

		data, err := EncodeFloatArrayBlock(&enc, nil)
		if err != nil {
			m.err = fmt.Errorf("encode error: float key=%s: %v", m.currentKey, err)
			return
		}
		if data != nil {
			m.chunks = append(m.chunks, chunkEntry{
				data:    data,
				minTime: minTime,
				maxTime: maxTime,
			})
		}

		arr.Timestamps = arr.Timestamps[sz:]
		arr.Values = arr.Values[sz:]
	}
	// Reset merged array for GC
	m.mergedFloat = tsdb.FloatArray{}
}

func (m *KeyAwareMergingIterator) chunkInteger() {
	arr := &m.mergedInteger
	for arr.Len() > 0 {
		sz := m.bufSize
		if sz > arr.Len() {
			sz = arr.Len()
		}

		minTime := arr.Timestamps[0]
		maxTime := arr.Timestamps[sz-1]

		enc := tsdb.IntegerArray{
			Timestamps: arr.Timestamps[:sz],
			Values:     arr.Values[:sz],
		}

		data, err := EncodeIntegerArrayBlock(&enc, nil)
		if err != nil {
			m.err = fmt.Errorf("encode error: integer key=%s: %v", m.currentKey, err)
			return
		}
		if data != nil {
			m.chunks = append(m.chunks, chunkEntry{
				data:    data,
				minTime: minTime,
				maxTime: maxTime,
			})
		}

		arr.Timestamps = arr.Timestamps[sz:]
		arr.Values = arr.Values[sz:]
	}
	m.mergedInteger = tsdb.IntegerArray{}
}

func (m *KeyAwareMergingIterator) chunkUnsigned() {
	arr := &m.mergedUnsigned
	for arr.Len() > 0 {
		sz := m.bufSize
		if sz > arr.Len() {
			sz = arr.Len()
		}

		minTime := arr.Timestamps[0]
		maxTime := arr.Timestamps[sz-1]

		enc := tsdb.UnsignedArray{
			Timestamps: arr.Timestamps[:sz],
			Values:     arr.Values[:sz],
		}

		data, err := EncodeUnsignedArrayBlock(&enc, nil)
		if err != nil {
			m.err = fmt.Errorf("encode error: unsigned key=%s: %v", m.currentKey, err)
			return
		}
		if data != nil {
			m.chunks = append(m.chunks, chunkEntry{
				data:    data,
				minTime: minTime,
				maxTime: maxTime,
			})
		}

		arr.Timestamps = arr.Timestamps[sz:]
		arr.Values = arr.Values[sz:]
	}
	m.mergedUnsigned = tsdb.UnsignedArray{}
}

func (m *KeyAwareMergingIterator) chunkBoolean() {
	arr := &m.mergedBoolean
	for arr.Len() > 0 {
		sz := m.bufSize
		if sz > arr.Len() {
			sz = arr.Len()
		}

		minTime := arr.Timestamps[0]
		maxTime := arr.Timestamps[sz-1]

		enc := tsdb.BooleanArray{
			Timestamps: arr.Timestamps[:sz],
			Values:     arr.Values[:sz],
		}

		data, err := EncodeBooleanArrayBlock(&enc, nil)
		if err != nil {
			m.err = fmt.Errorf("encode error: boolean key=%s: %v", m.currentKey, err)
			return
		}
		if data != nil {
			m.chunks = append(m.chunks, chunkEntry{
				data:    data,
				minTime: minTime,
				maxTime: maxTime,
			})
		}

		arr.Timestamps = arr.Timestamps[sz:]
		arr.Values = arr.Values[sz:]
	}
	m.mergedBoolean = tsdb.BooleanArray{}
}

func (m *KeyAwareMergingIterator) chunkString() {
	arr := &m.mergedString
	for arr.Len() > 0 {
		sz := m.bufSize
		if sz > arr.Len() {
			sz = arr.Len()
		}

		minTime := arr.Timestamps[0]
		maxTime := arr.Timestamps[sz-1]

		enc := tsdb.StringArray{
			Timestamps: arr.Timestamps[:sz],
			Values:     arr.Values[:sz],
		}

		data, err := EncodeStringArrayBlock(&enc, nil)
		if err != nil {
			m.err = fmt.Errorf("encode error: string key=%s: %v", m.currentKey, err)
			return
		}
		if data != nil {
			m.chunks = append(m.chunks, chunkEntry{
				data:    data,
				minTime: minTime,
				maxTime: maxTime,
			})
		}

		arr.Timestamps = arr.Timestamps[sz:]
		arr.Values = arr.Values[sz:]
	}
	m.mergedString = tsdb.StringArray{}
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
