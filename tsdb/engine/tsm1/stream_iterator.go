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
	fileIdx                int // source file index, -1 = not poolable
}

// maxPoolPerFile caps the per-file block pool to prevent unbounded growth.
// Matches the batch path's k.buf[i] which holds ~1-5 blocks per file per key.
const maxPoolPerFile = 8

// outputBlock is a lightweight struct for the output pipeline.
// It holds only what the TSM writer needs, decoupling the streamBlock
// lifecycle from the chunks slice so streamBlocks can be pooled immediately.
type outputBlock struct {
	key              []byte
	minTime, maxTime int64
	b                []byte
}

type outputBlocks []outputBlock

func (a outputBlocks) Len() int           { return len(a) }
func (a outputBlocks) Less(i, j int) bool { return a[i].minTime < a[j].minTime }
func (a outputBlocks) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

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

func (bl *streamBlock) overlapsTimeRange(min, max int64) bool {
	return bl.minTime <= max && bl.maxTime >= min
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

	// Per-file reusable block pool (capped at maxPoolPerFile)
	blockPool [][]*streamBlock

	// Collected blocks for current key (across all files)
	blocks streamBlocks

	// Pending blocks that may overlap with each other
	pending streamBlocks

	// Raw non-overlapping blocks from readBlocksIncrementally (before conversion to outputBlock)
	rawChunks streamBlocks

	// Reusable buffer for processGroups (avoids per-key allocation)
	allBlocks streamBlocks

	// Output chunks for current key (lightweight, decoupled from streamBlock lifecycle)
	chunks   []outputBlock
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

// getBlock returns a streamBlock from the per-file pool or allocates a new one.
func (m *KeyAwareMergingIterator) getBlock(fileIdx int, typ byte) *streamBlock {
	pool := m.blockPool[fileIdx]
	if n := len(pool); n > 0 {
		blk := pool[n-1]
		m.blockPool[fileIdx] = pool[:n-1]
		blk.typ = typ
		blk.fileIdx = fileIdx
		blk.readMin = math.MaxInt64
		blk.readMax = math.MinInt64
		blk.tombstones = blk.tombstones[:0]
		return blk
	}
	return &streamBlock{
		typ:     typ,
		fileIdx: fileIdx,
		readMin: math.MaxInt64,
		readMax: math.MinInt64,
	}
}

// returnBlock returns a single block to its per-file pool if under cap.
func (m *KeyAwareMergingIterator) returnBlock(blk *streamBlock) {
	fi := blk.fileIdx
	if fi < 0 || fi >= len(m.blockPool) {
		return
	}
	if len(m.blockPool[fi]) >= maxPoolPerFile {
		return
	}
	blk.key = nil
	blk.b = nil
	m.blockPool[fi] = append(m.blockPool[fi], blk)
}

// returnBlocks returns all poolable blocks in the slice to their per-file pools.
func (m *KeyAwareMergingIterator) returnBlocks(blocks streamBlocks) {
	for _, blk := range blocks {
		if blk != nil {
			m.returnBlock(blk)
		}
	}
}

// NewKeyAwareMergingIterator creates a new overlap-aware merge iterator.
func NewKeyAwareMergingIterator(iters []*BlockValueIterator, bufSize int, interrupt chan struct{}) *KeyAwareMergingIterator {
	return &KeyAwareMergingIterator{
		iterators: iters,
		bufSize:   bufSize,
		interrupt: interrupt,
		buf:       make([][]*streamBlock, len(iters)),
		blockPool: make([][]*streamBlock, len(iters)),
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

	// Current key fully consumed, outputBlocks are lightweight (no pool needed).
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
	m.blockPool = nil
	m.blocks = nil
	m.pending = nil
	m.rawChunks = nil
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

// readBlocksIncrementally reads blocks for the current key one at a time
// round-robin across files. Each pass reads at most one block per file,
// ensuring incremental progress across all files. It tracks maxTimeSeen
// to classify each block:
//   - Non-overlapping (minTime > maxTimeSeen) → rawChunks (output immediately)
//   - Possibly overlapping → pending (buffered for decode/merge)
func (m *KeyAwareMergingIterator) readBlocksIncrementally() {
	m.chunks = m.chunks[:0]
	m.pending = m.pending[:0]

	// Ensure per-file buffers are allocated
	for i := range m.buf {
		if m.buf[i] == nil {
			m.buf[i] = make([]*streamBlock, 0, 4)
		}
	}

	var maxTimeSeen int64 = math.MinInt64

	// Round-robin: each outer pass reads at most one block per file.
	// This ensures we see blocks from all files early, enabling
	// incremental overlap detection and early output of non-overlapping blocks.
	for {
		readAny := false

		for i, iter := range m.iterators {
			if iter.Err() != nil {
				continue
			}

			// Drain one buffered block per file per pass (true round-robin).
			// buf[i] holds blocks with keys > currentKey from prior key changes.
			if len(m.buf[i]) > 0 {
				blk := m.buf[i][0]
				if bytes.Equal(blk.key, m.currentKey) {
					m.buf[i] = m.buf[i][1:]
					if blk.minTime > maxTimeSeen {
						m.rawChunks = append(m.rawChunks, blk)
					} else {
						m.pending = append(m.pending, blk)
					}
					if blk.maxTime > maxTimeSeen {
						maxTimeSeen = blk.maxTime
					}
					readAny = true
					continue
				}
				// Buffer has a block with key != currentKey; skip this file.
				continue
			}

			// No buffered block; try reading from the iterator.
			if iter.exhausted {
				continue
			}

			// If the iterator cached a different key in NextBlock, skip.
			if iter.hasNext && !bytes.Equal(iter.nextKey, m.currentKey) {
				continue
			}

			// Try to position the iterator at the current key.
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

			// Read exactly one block from this file.
			tombstones := iter.TombstoneRange(m.currentKey)
			blk := m.getBlock(i, iter.Type())
			blk.key = iter.Key()
			blk.minTime = iter.MinTime()
			blk.maxTime = iter.MaxTime()
			blk.b = iter.RawBlock()
			blk.tombstones = tombstones

			if blk.minTime > maxTimeSeen {
				m.rawChunks = append(m.rawChunks, blk)
			} else {
				m.pending = append(m.pending, blk)
			}
			if blk.maxTime > maxTimeSeen {
				maxTimeSeen = blk.maxTime
			}
			readAny = true

			// Advance the iterator. If the next block has a different key,
			// NextBlock caches it in iter.hasNext/nextKey and returns false.
			iter.NextBlock()
		}

		if !readAny {
			break
		}
	}

	// Clear any remaining buffered blocks (they have keys > currentKey,
	// will be re-read by moveToNextKey → ActivatePending)
	for i := range m.buf {
		m.buf[i] = m.buf[i][:0]
	}
}

// processCurrentKey reads blocks incrementally for the current key, detects
// overlaps, and either passes non-overlapping blocks through or merges
// overlapping ones. Non-overlapping blocks are classified during reading via
// maxTimeSeen tracking; only potentially overlapping blocks are buffered.
func (m *KeyAwareMergingIterator) processCurrentKey() {
	// Phase 1: Read blocks incrementally (round-robin across files).
	// This populates m.rawChunks (non-overlapping streamBlocks) and m.pending (possibly overlapping).
	m.readBlocksIncrementally()

	// Set currentTyp from first available block
	if m.currentTyp == blockTypeUnset {
		if len(m.rawChunks) > 0 {
			m.currentTyp = m.rawChunks[0].typ
		} else if len(m.pending) > 0 {
			m.currentTyp = m.pending[0].typ
		}
	}

	// Phase 2: Fast path for single block with no tombstones — pass through
	// without decode/re-encode. Multiple blocks always go through processGroups
	// to ensure deduplication (different files may have duplicate timestamps).
	if len(m.pending) == 0 && len(m.rawChunks) == 1 && len(m.rawChunks[0].tombstones) == 0 {
		blk := m.rawChunks[0]
		m.chunks = append(m.chunks[:0], outputBlock{key: blk.key, minTime: blk.minTime, maxTime: blk.maxTime, b: blk.b})
		m.returnBlock(blk)
		m.rawChunks = m.rawChunks[:0]
		return
	}

	// Phase 3: Group-based processing.
	// Merge rawChunks + pending into one sorted list and partition into
	// independent overlap groups. Each group is either pass-through (single
	// block, no tombstones, non-overlapping) or decode+merge (overlapping or
	// tombstoned). This avoids decoding non-overlapping blocks that were
	// misclassified into rawChunks by the incremental maxTimeSeen tracking.
	m.processGroups()
	m.rawChunks = m.rawChunks[:0]
	m.pending = m.pending[:0]
}

// processGroups merges rawChunks and pending into a sorted list, partitions
// them into independent overlap groups, and processes each group individually.
// Non-overlapping singletons pass through as raw mmap data; overlapping groups
// or blocks with tombstones are decoded, merged, and re-encoded.
//
// Correctness: if two blocks have non-overlapping time ranges, their timestamp
// sets are strictly disjoint (∀ ta ∈ A, tb ∈ B: ta ≤ A.maxTime < B.minTime ≤ tb).
// Therefore a singleton group with no tombstones can safely pass through without
// decode/re-encode — there are no duplicate timestamps to deduplicate.
func (m *KeyAwareMergingIterator) processGroups() {
	m.allBlocks = append(m.allBlocks[:0], m.rawChunks...)
	m.allBlocks = append(m.allBlocks, m.pending...)

	if len(m.allBlocks) == 0 {
		return
	}

	sort.Stable(m.allBlocks)

	m.chunks = m.chunks[:0]
	groupStart := 0

	for i := 1; i <= len(m.allBlocks); i++ {
		// Determine if allBlocks[i] starts a new group.
		// It starts a new group if it does NOT overlap any block in the current group.
		startNewGroup := i == len(m.allBlocks)
		if !startNewGroup {
			overlaps := false
			for j := groupStart; j < i && !overlaps; j++ {
				if m.allBlocks[i].overlapsTimeRange(m.allBlocks[j].minTime, m.allBlocks[j].maxTime) {
					overlaps = true
				}
			}
			startNewGroup = !overlaps
		}

		if !startNewGroup {
			continue
		}

		// Process group [groupStart, i).
		group := m.allBlocks[groupStart:i]
		groupHasTombstones := false
		for _, blk := range group {
			if len(blk.tombstones) > 0 {
				groupHasTombstones = true
				break
			}
		}
		groupHasOverlap := i-groupStart > 1

		if groupHasOverlap || groupHasTombstones {
			m.blocks = append(m.blocks[:0], group...)
			m.decodeAndMerge()
			if m.err != nil {
				return
			}
			m.chunkMergedArray()
			m.returnBlocks(m.blocks)
			m.blocks = m.blocks[:0]
		} else {
			// Singleton, no tombstones, non-overlapping: pass through as raw mmap data.
			blk := group[0]
			m.chunks = append(m.chunks, outputBlock{
				key: blk.key, minTime: blk.minTime, maxTime: blk.maxTime, b: blk.b,
			})
			m.returnBlock(blk)
		}

		groupStart = i
	}

	// Sort chunks by minTime (merged groups and pass-through blocks may be interleaved).
	if len(m.chunks) > 1 {
		sort.Stable(outputBlocks(m.chunks))
	}
}


// decodeAndMerge decodes overlapping blocks into typed arrays, applies
// tombstones, and merges them (same algorithm as tsmBatchKeyIterator).
func (m *KeyAwareMergingIterator) decodeAndMerge() {
	// Reset merged arrays while keeping backing capacity.
	m.mergedFloat.Timestamps = m.mergedFloat.Timestamps[:0]
	m.mergedFloat.Values = m.mergedFloat.Values[:0]
	m.mergedInteger.Timestamps = m.mergedInteger.Timestamps[:0]
	m.mergedInteger.Values = m.mergedInteger.Values[:0]
	m.mergedUnsigned.Timestamps = m.mergedUnsigned.Timestamps[:0]
	m.mergedUnsigned.Values = m.mergedUnsigned.Values[:0]
	m.mergedBoolean.Timestamps = m.mergedBoolean.Timestamps[:0]
	m.mergedBoolean.Values = m.mergedBoolean.Values[:0]
	m.mergedString.Timestamps = m.mergedString.Timestamps[:0]
	m.mergedString.Values = m.mergedString.Values[:0]

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
			m.chunks = append(m.chunks, outputBlock{
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
		m.chunks = append(m.chunks, outputBlock{
			key:     m.blockKey,
			minTime: minTime,
			maxTime: maxTime,
			b:       cb,
		})
		m.mergedFloat.Timestamps = m.mergedFloat.Timestamps[:0]
		m.mergedFloat.Values = m.mergedFloat.Values[:0]
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
			m.chunks = append(m.chunks, outputBlock{
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
		m.chunks = append(m.chunks, outputBlock{
			key:     m.blockKey,
			minTime: minTime,
			maxTime: maxTime,
			b:       cb,
		})
		m.mergedInteger.Timestamps = m.mergedInteger.Timestamps[:0]
		m.mergedInteger.Values = m.mergedInteger.Values[:0]
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
			m.chunks = append(m.chunks, outputBlock{
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
		m.chunks = append(m.chunks, outputBlock{
			key:     m.blockKey,
			minTime: minTime,
			maxTime: maxTime,
			b:       cb,
		})
		m.mergedUnsigned.Timestamps = m.mergedUnsigned.Timestamps[:0]
		m.mergedUnsigned.Values = m.mergedUnsigned.Values[:0]
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
			m.chunks = append(m.chunks, outputBlock{
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
		m.chunks = append(m.chunks, outputBlock{
			key:     m.blockKey,
			minTime: minTime,
			maxTime: maxTime,
			b:       cb,
		})
		m.mergedBoolean.Timestamps = m.mergedBoolean.Timestamps[:0]
		m.mergedBoolean.Values = m.mergedBoolean.Values[:0]
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
			m.chunks = append(m.chunks, outputBlock{
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
		m.chunks = append(m.chunks, outputBlock{
			key:     m.blockKey,
			minTime: minTime,
			maxTime: maxTime,
			b:       cb,
		})
		m.mergedString.Timestamps = m.mergedString.Timestamps[:0]
		m.mergedString.Values = m.mergedString.Values[:0]
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
