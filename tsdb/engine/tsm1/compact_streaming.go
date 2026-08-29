package tsm1

// streamingBatchKeyIterator is a KeyIterator that produces byte-identical
// output to tsmBatchKeyIterator while releasing consumed blocks as the merge
// window advances; the per-key gather peak is unchanged from legacy — see
// Memory characteristics below.
//
// It embeds *tsmBatchKeyIterator to reuse the generated merge/combine/chunk
// logic (mergeFloat/combineFloat/chunkFloat and the Integer/Unsigned/Boolean/
// String twins in compact.gen.go) verbatim. Those methods operate on the
// embedded iterator's blocks/merged/mergedFloatValues/size/key/typ fields via
// the *tsmBatchKeyIterator receiver, so Go method promotion gives the streaming
// iterator access to them directly.
//
// Responsibility split:
//   - streamingBatchKeyIterator fills embedded.blocks one block at a time per
//     reader via a min-heap ordered purely by fileIndex (see blockHeap), which
//     reproduces the collection order that tsmBatchKeyIterator establishes by
//     appending k.buf in reader order (compact.go:1819-1827). mergeFloat then
//     runs sort.Stable(k.blocks) with blocks.Less (a partial order: strictly
//     non-overlapping blocks compare by minTime; overlapping blocks are
//     unordered and stable-sort preserves their input/file-index order). This
//     is the load-bearing invariant for byte-identical dedup: for overlapping
//     blocks the file-index order decides which block FloatArray.Merge sees
//     later and thus wins the duplicate-timestamp conflict (b wins).
//   - The embedded generated methods consume embedded.blocks, decode/dedup/
//     apply tombstones/chunk/encode, and produce embedded.merged.
//   - evictBeforeWindow drops head blocks once fully read (read()==true) whose
//     maxTime < winMin, clearing their block.b slice header. This is safe
//     because combineFloat skips read() blocks (compact.gen.go:1067,1092,1139)
//     — they are neither decoded nor passed through.
//
// Memory characteristics: per-key peak equals legacy — selectKey drains ALL of
// a key's blocks across the K open readers into embedded.blocks before the
// first merge(), same as tsmBatchKeyIterator. evictBeforeWindow reduces steady-
// state (not peak) by releasing consumed blocks as the merge window advances.
// block.b is a mmap alias (reader.go:1483); setting it nil drops only the Go
// slice header — the mmap stays resident until TSMReader.Close (munmap) at
// compact() return. The real RSS bound comes from Phase B, which caps the
// number of simultaneously open readers at K (default 0, rolling disabled)
// instead of N.
//
// The worst key is surfaced at runtime, not bounded: after each gather,
// selectKey reports the key's gathered byte total to hotKeyWarnFn when it
// exceeds hotKeyGatherWarnBytes, identifying the key by a truncated SHA-256
// hash rather than the raw key (see checkHotKeyGather below). This is
// diagnostics only — the per-key peak itself is unchanged.

import (
	"bytes"
	"container/heap"
	"crypto/sha256"
	"encoding/hex"
	"math"
	"os"
	"strings"

	"go.uber.org/zap"

	"github.com/influxdata/influxdb/tsdb"
)

// Hot-key gather observability.
//
// Peak compaction heap is dominated by the largest single-key gather: before
// the first merge, BOTH key iterators (streamingBatchKeyIterator.selectKey
// below and the legacy tsmBatchKeyIterator.Next in compact.go) hold every
// compressed block of a key across all readers in the compaction group at
// once — the sum of len(b.b) over the key's blocks. In pread mode each of
// those blocks is a fresh heap allocation (reader.go ReadBytes); in mmap mode
// each is an alias into the mapped file that keeps the mapping rooted. Either
// way the full gather is resident, so:
//
//	peak compaction heap ≈ max over keys of Σ(compressed block bytes of that key across the group)
//
// hotKeyGatherWarnBytes is the gather size above which a key is reported and
// hotKeyWarnFn is the (replaceable) reporting hook; checkHotKeyGather is the
// shared accounting both call sites use. The check is O(len(blocks)) at
// gather time, allocation-free, and fires at most once per key per
// compaction. It is diagnostics only — it does not reduce the peak. Bounding
// the peak requires windowed incremental refill/merge/evict with bounded
// per-reader lookahead, which is deliberately not attempted here: a naive
// k-way merge is byte-broken by the tombstone pre-Merge Exclude fallback
// semantics in compact.gen.go (a tombstoned higher-index block loses to a
// lower-index non-tombstoned value at the same timestamp, which no
// per-timestamp winner rule reproduces), and a lazy refill that preserves the
// exact sort.Stable input order needs its own design cycle.
const hotKeyGatherWarnBytes = 256 << 20 // 256MiB

// hotKeyWarnFn reports a hot-key gather: lg is the logger to report through
// (nil suppresses reporting), key the series key and total the summed
// compressed byte size (Σ len(b.b)) of the key's gathered blocks. It is a
// package-level var so it can be replaced (e.g. by tests, or by an embedding
// application routing into a real logger); setting it to nil disables
// reporting. The default implementation reports through lg.
var hotKeyWarnFn = defaultHotKeyWarn

// hotKeyWarnDisabled is resolved once at process start: hot-key gather
// warnings are on by default and can be silenced by setting
// INFLUXDB_HOT_KEY_GATHER_WARN=0 (false/off/no are also accepted,
// case-insensitively). Replacement hotKeyWarnFn hooks are not env-gated.
var hotKeyWarnDisabled = hotKeyWarnEnvDisabled()

func hotKeyWarnEnvDisabled() bool {
	switch strings.ToLower(os.Getenv("INFLUXDB_HOT_KEY_GATHER_WARN")) {
	case "0", "false", "off", "no":
		return true
	}
	return false
}

// hotKeyWarnRawKey is resolved once at process start: the default report
// identifies the key only by a truncated SHA-256 hash (series keys can contain
// sensitive tag and field names), and the raw (truncated) key is emitted only
// when INFLUXDB_HOT_KEY_GATHER_WARN_RAW_KEY is set to 1/true/on/yes
// (case-insensitively). Tests and embedding applications may also set the var
// directly; replacement hotKeyWarnFn hooks are not gated on it.
var hotKeyWarnRawKey = hotKeyWarnRawKeyEnabled()

func hotKeyWarnRawKeyEnabled() bool {
	switch strings.ToLower(os.Getenv("INFLUXDB_HOT_KEY_GATHER_WARN_RAW_KEY")) {
	case "1", "true", "on", "yes":
		return true
	}
	return false
}

// defaultHotKeyWarn reports one structured warning through lg (a nil logger is
// a no-op). The key is identified by the first 8 hex characters of its SHA-256
// hash; the raw (truncated) key is added only when hotKeyWarnRawKey is set.
func defaultHotKeyWarn(lg *zap.Logger, key []byte, total uint64) {
	if hotKeyWarnDisabled || lg == nil {
		return
	}
	sum := sha256.Sum256(key)
	fields := []zap.Field{
		zap.Uint64("total_bytes", total),
		zap.Uint64("threshold_bytes", hotKeyGatherWarnBytes),
		zap.String("key_hash", hex.EncodeToString(sum[:4])),
	}
	if hotKeyWarnRawKey {
		const maxKeyLen = 128
		raw := key
		if len(raw) > maxKeyLen {
			raw = raw[:maxKeyLen]
		}
		fields = append(fields, zap.String("key", string(raw)))
	}
	lg.Warn("Hot key gather", fields...)
}

// checkHotKeyGather is the shared per-key gather accounting for both key
// iterators: streamingBatchKeyIterator.selectKey and the legacy
// tsmBatchKeyIterator.Next (compact.go) call it immediately after gathering a
// key's blocks, passing exactly the blocks gathered for that key. It sums the
// compressed byte sizes and invokes hotKeyWarnFn once if the total exceeds
// hotKeyGatherWarnBytes, reporting through lg (nil lg suppresses reporting).
// Every key is gathered exactly once per iterator, so the hook runs at most
// once per key per compaction. The total is accumulated in uint64 — on 32-bit
// platforms a gather past math.MaxInt32 would overflow an int and could wrap
// negative, skipping the warning. The sum is O(len(blks)) with no allocations;
// the nil check keeps the hook optional.
func checkHotKeyGather(lg *zap.Logger, key []byte, blks blocks) {
	if hotKeyWarnFn == nil {
		return
	}
	var total uint64
	for _, b := range blks {
		total += uint64(len(b.b))
	}
	if total > hotKeyGatherWarnBytes {
		hotKeyWarnFn(lg, key, total)
	}
}

// streamingBatchKeyIterator implements KeyIterator by streaming blocks.
type streamingBatchKeyIterator struct {
	// Embedded to reuse mergeFloat/combineFloat/chunkFloat/merged*/size/key/typ
	// and the merge()/AppendError()/handleDecodeError()/handleEncodeError()
	// methods. Its Next()/Read()/Close() are NOT promoted because we shadow
	// them below; only the merge helpers are invoked via the embedded pointer.
	*tsmBatchKeyIterator

	// pending[i] holds reader i's current block for the active key, or a
	// lookahead block for a different key (see pendingKey). At most one per
	// reader at any time.
	pending []*block
	// pendingKey[i] is non-nil when pending[i] is a lookahead block whose key
	// differs from the currently-processed key. It seeds the next selectKey.
	pendingKey [][]byte

	// winMin/winMax track the current merge window for eviction. Updated after
	// each merge from the embedded iterator's remaining blocks/mergedValues.
	winMin int64
	winMax int64

	// heap orders pending blocks for the current key by fileIndex (see blockHeap).
	hp blockHeap

	// keyDone is true when all blocks for the current key have been drained
	// from the heap and fed through merge.
	keyDone bool
}

// NewStreamingBatchKeyIterator returns a streaming KeyIterator over readers.
// size is the maximum number of values to encode in a single block.
func NewStreamingBatchKeyIterator(size int, fast bool, interrupt chan struct{}, tsmFiles []string, readers ...*TSMReader) (KeyIterator, error) {
	var iters []*BlockIterator
	for _, r := range readers {
		iters = append(iters, r.BlockIterator())
	}

	// Build the embedded tsmBatchKeyIterator with the same field layout the
	// generated merge methods expect. We never call its Next()/Read(); we only
	// drive its merge*()/combine*()/chunk*() helpers by populating blocks.
	base := &tsmBatchKeyIterator{
		readers:              readers,
		values:               map[string][]Value{},
		pos:                  make([]int, len(readers)),
		size:                 size,
		iterators:            iters,
		fast:                 fast,
		tsmFiles:             tsmFiles,
		blocks:               nil, // streaming fills this incrementally
		buf:                  nil, // unused by streaming (no per-reader gather)
		mergedFloatValues:    &tsdb.FloatArray{},
		mergedIntegerValues:  &tsdb.IntegerArray{},
		mergedUnsignedValues: &tsdb.UnsignedArray{},
		mergedBooleanValues:  &tsdb.BooleanArray{},
		mergedStringValues:   &tsdb.StringArray{},
		interrupt:            interrupt,
	}

	s := &streamingBatchKeyIterator{
		tsmBatchKeyIterator: base,
		pending:             make([]*block, len(readers)),
		pendingKey:          make([][]byte, len(readers)),
	}
	s.hp = make(blockHeap, 0, len(readers))
	return s, nil
}

// blockHeapItem pairs a block with the reader index that produced it.
type blockHeapItem struct {
	blk       *block
	fileIndex int
}

// blockHeap is a min-heap of *blockHeapItem ordered by fileIndex.
//
// The legacy tsmBatchKeyIterator (compact.go) collects blocks for a key across
// readers by iterating k.buf in reader/file-index order and appending each
// reader's same-key blocks (which are minTime-ascending within the reader, since
// BlockIterator yields IndexEntry order and indexEntries is sorted by MinTime at
// write time). The resulting k.blocks is therefore:
//
//	[reader0 blocks (minTime asc), reader1 blocks (minTime asc), ...]
//
// mergeFloat then does sort.Stable(k.blocks) using blocks.Less, whose same-key
// predicate is the partial order `a.minTime < b.minTime && a.maxTime < b.minTime`
// (strictly non-overlapping). Non-overlapping blocks are totally ordered by
// minTime regardless of input order; overlapping blocks compare false in both
// directions and so sort.Stable preserves their relative input order. The final
// ordering therefore depends on the input order ONLY for overlapping blocks,
// and that input order is the file-index order.
//
// combineFloat walks k.blocks in this order and calls FloatArray.Merge (b-wins),
// so the file-index order determines the dedup winner for overlapping blocks.
//
// To be byte-for-byte identical to legacy, the streaming iterator must fill
// k.blocks in the same file-index order. Using a (minTime, fileIndex) heap
// reorders overlapping blocks whose minTime order differs from file-index order,
// flipping the dedup winner (CRITICAL bug). Ordering the heap purely by
// fileIndex makes selectKey drain each reader's same-key blocks contiguously in
// reader order, reproducing the legacy collection order exactly.
type blockHeap []*blockHeapItem

func (h blockHeap) Len() int { return len(h) }
func (h blockHeap) Less(i, j int) bool {
	// Pure fileIndex order: matches legacy tsmBatchKeyIterator's k.buf append
	// order. Do NOT add a minTime tiebreak — that reintroduces the bug whenever
	// overlapping blocks have minTime order reversed relative to fileIndex.
	return h[i].fileIndex < h[j].fileIndex
}
func (h blockHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *blockHeap) Push(x interface{}) { *h = append(*h, x.(*blockHeapItem)) }
func (h *blockHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// fillPending loads reader i's next block if it matches curKey.
// If the reader's next block is for a different key, it is stashed in
// pending[i]/pendingKey[i] as a lookahead for the next selectKey, and
// fillPending returns false. Returns true when pending[i] holds a block for
// curKey (either freshly loaded or already present).
func (k *streamingBatchKeyIterator) fillPending(i int, curKey []byte) bool {
	if k.pending[i] != nil && k.pendingKey[i] == nil {
		return true // already holds a block for curKey
	}
	if k.pending[i] != nil && k.pendingKey[i] != nil {
		// Lookahead from a prior key. It belongs to curKey only if it matches.
		if bytes.Equal(k.pendingKey[i], curKey) {
			k.pendingKey[i] = nil
			return true
		}
		return false // lookahead is for a different key
	}

	iter := k.iterators[i]
	if !iter.Next() {
		if iter.Err() != nil {
			k.currentTsm = k.tsmFiles[i]
			k.AppendError(errBlockRead{k.currentTsm, iter.Err()})
		}
		return false
	}
	key, minTime, maxTime, typ, _, b, err := iter.Read()
	if err != nil {
		k.currentTsm = k.tsmFiles[i]
		k.AppendError(errBlockRead{k.currentTsm, err})
	}
	blk := &block{
		key:        key,
		minTime:    minTime,
		maxTime:    maxTime,
		typ:        typ,
		b:          b,
		tombstones: k.readers[i].TombstoneRange(key),
		readMin:    math.MaxInt64,
		readMax:    math.MinInt64,
	}
	k.pending[i] = blk
	if !bytes.Equal(key, curKey) {
		k.pendingKey[i] = key
		return false
	}
	return true
}

// selectKey advances to the next smallest key across all readers, seeds the
// heap with one block per reader that matches that key, and drains the heap
// into embedded.blocks in fileIndex order (matching the legacy
// tsmBatchKeyIterator collection order), refilling each reader one block at a
// time. Returns false when no readers have any blocks left.
func (k *streamingBatchKeyIterator) selectKey() bool {
	// 1. Ensure each reader has a pending block or has reached its lookahead/EOF.
	var minKey []byte
	for i := range k.readers {
		if k.pending[i] == nil {
			// Try to load the next block without a key constraint; we just need
			// to know this reader's current key. Use nil curKey to load any.
			k.loadAny(i)
		}
		if k.pending[i] == nil {
			continue // reader exhausted
		}
		key := k.pending[i].key
		if k.pendingKey[i] != nil {
			key = k.pendingKey[i]
		}
		if minKey == nil || bytes.Compare(key, minKey) < 0 {
			minKey = key
		}
	}
	if minKey == nil {
		return false
	}

	// 2. Set the embedded key/type from the first reader that matches minKey.
	k.key = minKey
	for i := range k.readers {
		if k.pending[i] == nil {
			continue
		}
		key := k.pending[i].key
		if k.pendingKey[i] != nil {
			key = k.pendingKey[i]
		}
		if bytes.Equal(key, minKey) {
			k.typ = k.pending[i].typ
			break
		}
	}

	// 3. Reset window state for the new key.
	k.hp = k.hp[:0]
	k.winMin = math.MaxInt64
	k.winMax = math.MinInt64
	k.keyDone = false
	k.blocks = k.blocks[:0]

	// 4. Seed the heap: for each reader whose pending block matches minKey,
	//    push it; clear pending[i]/pendingKey[i] so fillPending can reload.
	for i := range k.readers {
		if k.pending[i] == nil {
			continue
		}
		matches := k.pendingKey[i] == nil || bytes.Equal(k.pendingKey[i], minKey)
		if !matches {
			continue
		}
		heap.Push(&k.hp, &blockHeapItem{blk: k.pending[i], fileIndex: i})
		k.pending[i] = nil
		k.pendingKey[i] = nil
	}

	// 5. Drain the heap into embedded.blocks, refilling each reader one block
	//    at a time when its block is consumed and the next matches minKey.
	for k.hp.Len() > 0 {
		item := heap.Pop(&k.hp).(*blockHeapItem)
		k.blocks = append(k.blocks, item.blk)
		// Reload reader item.fileIndex's next block for minKey, if any.
		if k.fillPending(item.fileIndex, minKey) {
			heap.Push(&k.hp, &blockHeapItem{blk: k.pending[item.fileIndex], fileIndex: item.fileIndex})
			k.pending[item.fileIndex] = nil
		}
	}

	// 6. Hot-key gather accounting: embedded.blocks now holds every compressed
	//    block for this key across all readers before the first merge — the
	//    per-key peak described in the file header (fresh heap copies in pread
	//    mode, mmap aliases otherwise). Report it when it exceeds
	//    hotKeyGatherWarnBytes, at most once per key (see checkHotKeyGather).
	checkHotKeyGather(k.logger, k.key, k.blocks)

	return len(k.blocks) > 0
}

// loadAny loads reader i's next block into pending[i] without a key filter,
// recording the key in pendingKey[i] so selectKey can read it. Used to
// initialize/advance a reader that has no pending block.
func (k *streamingBatchKeyIterator) loadAny(i int) {
	iter := k.iterators[i]
	if !iter.Next() {
		if iter.Err() != nil {
			k.currentTsm = k.tsmFiles[i]
			k.AppendError(errBlockRead{k.currentTsm, iter.Err()})
		}
		return
	}
	key, minTime, maxTime, typ, _, b, err := iter.Read()
	if err != nil {
		k.currentTsm = k.tsmFiles[i]
		k.AppendError(errBlockRead{k.currentTsm, err})
	}
	blk := &block{
		key:        key,
		minTime:    minTime,
		maxTime:    maxTime,
		typ:        typ,
		b:          b,
		tombstones: k.readers[i].TombstoneRange(key),
		readMin:    math.MaxInt64,
		readMax:    math.MinInt64,
	}
	k.pending[i] = blk
	k.pendingKey[i] = key
}

// evictBeforeWindow drops head blocks from embedded.blocks that can no longer
// overlap the current or any future merge window for this key. A block is
// evictable when it is fully read (read() == true) and its maxTime precedes
// the window min. Setting b.b = nil releases the mmap alias reference.
func (k *streamingBatchKeyIterator) evictBeforeWindow() {
	i := 0
	for i < len(k.blocks) {
		b := k.blocks[i]
		if b.read() && b.maxTime < k.winMin {
			b.b = nil
			i++
			continue
		}
		break
	}
	if i > 0 {
		k.blocks = k.blocks[i:]
	}
}

// Next returns true if there are any values remaining in the iterator.
// Mirrors tsmBatchKeyIterator.Next (compact.go:1696) but uses selectKey +
// streaming heap fill instead of the PeekNext gather loop, and drives the
// embedded merge helpers.
func (k *streamingBatchKeyIterator) Next() bool {
RETRY:
	// Any merged blocks pending? Pop the head (Read returned it previously).
	if len(k.merged) > 0 {
		k.merged = k.merged[1:]
		if len(k.merged) > 0 {
			return true
		}
	}

	// Any merged values pending (carry-over from a prior chunk)?
	if k.hasMergedValues() {
		k.merge()
		k.evictBeforeWindow()
		if len(k.merged) > 0 || k.hasMergedValues() {
			return true
		}
	}

	// If we still have blocks for the current key, keep merging.
	if len(k.blocks) > 0 {
		k.merge()
		k.evictBeforeWindow()
		if len(k.merged) > 0 || k.hasMergedValues() {
			return true
		}
	}

	// Move to the next key.
	if !k.selectKey() {
		return false
	}
	k.merge()
	k.evictBeforeWindow()

	// After merging all values for this key, we might have none (e.g. all
	// tombstoned). Move on to the next key.
	if len(k.merged) == 0 {
		goto RETRY
	}
	return len(k.merged) > 0
}

// Read returns the next merged block. Mirrors tsmBatchKeyIterator.Read.
func (k *streamingBatchKeyIterator) Read() ([]byte, int64, int64, []byte, error) {
	select {
	case <-k.interrupt:
		return nil, 0, 0, nil, errCompactionAborted{}
	default:
	}
	if len(k.merged) == 0 {
		return nil, 0, 0, nil, k.Err()
	}
	block := k.merged[0]
	return block.key, block.minTime, block.maxTime, block.b, k.Err()
}

// EstimatedIndexSize returns the sum of input readers' index sizes (not the
// average) so the disk-buffer heuristic does not underestimate when many small
// files merge into one large output. Accumulates in uint64 — a uint32 wraps
// past 4GiB and could fall below the threshold. (Phase C fix.)
func (k *streamingBatchKeyIterator) EstimatedIndexSize() int {
	var size uint64
	for _, r := range k.readers {
		size += uint64(r.IndexSize())
	}
	// Saturate at MaxInt so 32-bit builds (where int is 32-bit) cannot wrap
	// negative on sums above ~2GiB and misfire the disk-buffer threshold.
	if size > uint64(math.MaxInt32) {
		size = uint64(math.MaxInt32)
	}
	return int(size)
}

// Close releases per-reader resources. The embedded tsmBatchKeyIterator.Close
// closes all readers; we reuse that behavior.
func (k *streamingBatchKeyIterator) Close() error {
	return k.tsmBatchKeyIterator.Close()
}

// Err returns any accumulated errors.
func (k *streamingBatchKeyIterator) Err() error {
	return k.tsmBatchKeyIterator.Err()
}

// merge forwards to the embedded merge() so evictBeforeWindow runs alongside.
// (merge() is promoted from *tsmBatchKeyIterator; this wrapper updates the
// eviction window after each merge call.)
func (k *streamingBatchKeyIterator) merge() {
	k.tsmBatchKeyIterator.merge()
	// Update the window from the remaining blocks so eviction is accurate.
	if len(k.blocks) > 0 {
		// The window's min is the earliest remaining block minTime; max is
		// tracked implicitly by what combineFloat decoded. For eviction we
		// only need winMin: once mergedFloatValues carry-over is flushed, the
		// next window starts at the first remaining block's minTime.
		k.winMin = k.blocks[0].minTime
		if k.hasMergedValues() {
			// Carry-over values are < next block's minTime; keep winMin at
			// the carry-over's floor so we don't evict blocks still needed.
			k.winMin = minInt64(k.winMin, k.mergedFloatValuesFloor())
		}
	}
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// mergedFloatValuesFloor returns the smallest timestamp currently held across
// all merged-value arrays, or MaxInt64 if empty.
func (k *streamingBatchKeyIterator) mergedFloatValuesFloor() int64 {
	if k.mergedFloatValues.Len() > 0 {
		return k.mergedFloatValues.Timestamps[0]
	}
	if k.mergedIntegerValues.Len() > 0 {
		return k.mergedIntegerValues.Timestamps[0]
	}
	if k.mergedUnsignedValues.Len() > 0 {
		return k.mergedUnsignedValues.Timestamps[0]
	}
	if k.mergedBooleanValues.Len() > 0 {
		return k.mergedBooleanValues.Timestamps[0]
	}
	if k.mergedStringValues.Len() > 0 {
		return k.mergedStringValues.Timestamps[0]
	}
	return math.MaxInt64
}
