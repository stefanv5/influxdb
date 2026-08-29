package tsm1_test

import (
	"bytes"
	"math"
	"os"
	"sort"
	"testing"

	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// drainKeyIterator reads all blocks from a tsm1.KeyIterator and returns a deterministic
// byte blob (length-prefixed key/minTime/maxTime/blockData) so that byte comparison
// is order-sensitive and complete. This is the golden-master comparator for the
// A/B byte-identical test.
func drainKeyIterator(t *testing.T, iter tsm1.KeyIterator) []byte {
	t.Helper()
	var buf bytes.Buffer
	for iter.Next() {
		key, minTime, maxTime, data, err := iter.Read()
		if err != nil {
			t.Fatalf("iter.Read: %v", err)
		}
		// length-prefixed key
		buf.WriteByte(byte(len(key) >> 8))
		buf.WriteByte(byte(len(key)))
		buf.Write(key)
		// minTime / maxTime (8 bytes each, big-endian via append)
		var tb [8]byte
		putInt64BE(tb[:], minTime)
		buf.Write(tb[:])
		putInt64BE(tb[:], maxTime)
		buf.Write(tb[:])
		// length-prefixed block data
		putInt32BE(tb[:4], len(data))
		buf.Write(tb[:4])
		buf.Write(data)
	}
	if err := iter.Err(); err != nil {
		t.Fatalf("iter.Err: %v", err)
	}
	if err := iter.Close(); err != nil {
		t.Fatalf("iter.Close: %v", err)
	}
	return buf.Bytes()
}

func putInt64BE(b []byte, v int64) {
	u := uint64(v)
	b[0] = byte(u >> 56)
	b[1] = byte(u >> 48)
	b[2] = byte(u >> 40)
	b[3] = byte(u >> 32)
	b[4] = byte(u >> 24)
	b[5] = byte(u >> 16)
	b[6] = byte(u >> 8)
	b[7] = byte(u)
}

func putInt32BE(b []byte, v int) {
	u := uint32(v)
	b[0] = byte(u >> 24)
	b[1] = byte(u >> 16)
	b[2] = byte(u >> 8)
	b[3] = byte(u)
}

// openReadersForFiles opens a fresh TSMReader per file path. A/B testing needs two
// independent reader sets because BlockIterator mutates reader state and Close
// closes the reader.
func openReadersForFiles(t *testing.T, files []string) []*tsm1.TSMReader {
	t.Helper()
	readers := make([]*tsm1.TSMReader, len(files))
	for i, f := range files {
		readers[i] = MustOpenTSMReader(f)
	}
	return readers
}

// buildTSMFiles writes one TSM file per (gen, values) entry and returns the file
// paths in the same order. Each file is written with keys sorted lexicographically
// (MustWriteTSM already sorts).
func buildTSMFiles(t *testing.T, dir string, gens []int, perFileValues []map[string][]tsm1.Value) []string {
	t.Helper()
	if len(gens) != len(perFileValues) {
		t.Fatalf("gens and perFileValues length mismatch: %d vs %d", len(gens), len(perFileValues))
	}
	files := make([]string, len(gens))
	for i := range gens {
		files[i] = MustWriteTSM(dir, gens[i], perFileValues[i])
	}
	return files
}

// TestStreamingKeyIterator_ByteIdentical_AB is the golden-master test: for each
// input scenario it runs BOTH the legacy tsm1.NewTSMBatchKeyIterator and the new
// tsm1.NewStreamingBatchKeyIterator on independent reader sets built from the same
// files, then asserts the drained output bytes are identical. This subsumes
// dedup, ordering, tombstone, carry-over and type-variant correctness.
func TestStreamingKeyIterator_ByteIdentical_AB(t *testing.T) {
	cases := []struct {
		name string
		// gens and per-file values describe the input TSM files
		gens           []int
		perFileValues  []map[string][]tsm1.Value
		fast           bool
		maxPointsPerBlk int
	}{
		{
			name:            "single_key_single_file",
			gens:            []int{1},
			perFileValues:   []map[string][]tsm1.Value{
				{"cpu,host=A#!~#value": {tsm1.NewValue(1, float64(1.0)), tsm1.NewValue(2, float64(2.0))}},
			},
			fast:            false,
			maxPointsPerBlk: 1000,
		},
		{
			name: "single_key_multi_file_no_overlap",
			gens: []int{1, 2},
			perFileValues: []map[string][]tsm1.Value{
				{"cpu,host=A#!~#value": {tsm1.NewValue(1, float64(1.0)), tsm1.NewValue(2, float64(2.0))}},
				{"cpu,host=A#!~#value": {tsm1.NewValue(3, float64(3.0)), tsm1.NewValue(4, float64(4.0))}},
			},
			fast:            false,
			maxPointsPerBlk: 1000,
		},
		{
			// The load-bearing dedup case: two files, same key, same timestamp,
			// different values. The later-file-index value must win (b-wins).
			name: "single_key_multi_file_overlap_dup_timestamps",
			gens: []int{1, 2},
			perFileValues: []map[string][]tsm1.Value{
				{"cpu,host=A#!~#value": {tsm1.NewValue(1, float64(1.0)), tsm1.NewValue(2, float64(2.0))}},
				{"cpu,host=A#!~#value": {tsm1.NewValue(1, float64(99.0)), tsm1.NewValue(2, float64(88.0))}},
			},
			fast:            false,
			maxPointsPerBlk: 1000,
		},
		{
			name: "multi_key_interleaved",
			gens: []int{1, 2},
			perFileValues: []map[string][]tsm1.Value{
				{
					"cpu,host=A#!~#value": {tsm1.NewValue(1, float64(1.0))},
					"cpu,host=B#!~#value": {tsm1.NewValue(1, float64(2.0))},
				},
				{
					"cpu,host=A#!~#value": {tsm1.NewValue(2, float64(3.0))},
					"cpu,host=B#!~#value": {tsm1.NewValue(2, float64(4.0))},
				},
			},
			fast:            false,
			maxPointsPerBlk: 1000,
		},
		{
			name: "fast_path_full_blocks",
			gens: []int{1, 2},
			perFileValues: []map[string][]tsm1.Value{
				{"cpu,host=A#!~#value": makeFloatValues(1, 1000)},
				{"cpu,host=A#!~#value": makeFloatValues(1001, 1000)},
			},
			fast:            true,
			maxPointsPerBlk: 1000,
		},
		{
			name: "fast_path_small_tail",
			gens: []int{1, 2},
			perFileValues: []map[string][]tsm1.Value{
				{"cpu,host=A#!~#value": makeFloatValues(1, 1000)},
				{"cpu,host=A#!~#value": {tsm1.NewValue(1001, float64(7.0)), tsm1.NewValue(1002, float64(8.0))}},
			},
			fast:            true,
			maxPointsPerBlk: 1000,
		},
		{
			name: "integer_type",
			gens: []int{1, 2},
			perFileValues: []map[string][]tsm1.Value{
				{"mem,host=A#!~#value": {tsm1.NewValue(1, int64(10)), tsm1.NewValue(2, int64(20))}},
				{"mem,host=A#!~#value": {tsm1.NewValue(2, int64(99)), tsm1.NewValue(3, int64(30))}},
			},
			fast:            false,
			maxPointsPerBlk: 1000,
		},
		{
			name: "unsigned_type",
			gens: []int{1, 2},
			perFileValues: []map[string][]tsm1.Value{
				{"cnt,host=A#!~#value": {tsm1.NewValue(1, uint64(10)), tsm1.NewValue(2, uint64(20))}},
				{"cnt,host=A#!~#value": {tsm1.NewValue(2, uint64(99)), tsm1.NewValue(3, uint64(30))}},
			},
			fast:            false,
			maxPointsPerBlk: 1000,
		},
		{
			name: "string_type",
			gens: []int{1, 2},
			perFileValues: []map[string][]tsm1.Value{
				{"msg,host=A#!~#value": {tsm1.NewValue(1, "a"), tsm1.NewValue(2, "b")}},
				{"msg,host=A#!~#value": {tsm1.NewValue(2, "z"), tsm1.NewValue(3, "c")}},
			},
			fast:            false,
			maxPointsPerBlk: 1000,
		},
		{
			name: "boolean_type",
			gens: []int{1, 2},
			perFileValues: []map[string][]tsm1.Value{
				{"flg,host=A#!~#value": {tsm1.NewValue(1, true), tsm1.NewValue(2, false)}},
				{"flg,host=A#!~#value": {tsm1.NewValue(2, true), tsm1.NewValue(3, false)}},
			},
			fast:            false,
			maxPointsPerBlk: 1000,
		},
		{
			// Multi-chunk, no overlap: 3 files × 1000 points = 3000 points → 3
			// output chunks. Exercises carry-over of mergedFloatValues across
			// chunk boundaries and evictBeforeWindow as the window advances.
			name: "multi_chunk_no_overlap",
			gens: []int{1, 2, 3},
			perFileValues: []map[string][]tsm1.Value{
				{"cpu,host=A#!~#value": makeFloatValues(1, 1000)},
				{"cpu,host=A#!~#value": makeFloatValues(1001, 1000)},
				{"cpu,host=A#!~#value": makeFloatValues(2001, 1000)},
			},
			fast:            false,
			maxPointsPerBlk: 1000,
		},
		{
			// Multi-chunk with cross-file overlap and duplicate timestamps:
			// exercises the dedup path across multiple chunks plus carry-over.
			name: "multi_chunk_overlap_dup",
			gens: []int{1, 2},
			perFileValues: []map[string][]tsm1.Value{
				{"cpu,host=A#!~#value": makeFloatValues(1, 1500)},
				{"cpu,host=A#!~#value": makeFloatValues(501, 1500)}, // overlaps t=501..1500
			},
			fast:            false,
			maxPointsPerBlk: 1000,
		},
		{
			// Small block size forces multi-chunk on a small input, exercising
			// carry-over and eviction with many chunk boundaries.
			name: "small_block_size_multi_chunk",
			gens: []int{1, 2},
			perFileValues: []map[string][]tsm1.Value{
				{"cpu,host=A#!~#value": makeFloatValues(1, 50)},
				{"cpu,host=A#!~#value": makeFloatValues(51, 50)},
			},
			fast:            false,
			maxPointsPerBlk: 10,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			dir := MustTempDir()
			defer os.RemoveAll(dir)

			files := buildTSMFiles(t, dir, tc.gens, tc.perFileValues)
			sort.Strings(files) // match production ordering (compact.go sorts tsmFiles)

			// Two independent reader sets: BlockIterator mutates reader state.
			readersA := openReadersForFiles(t, files)
			readersB := openReadersForFiles(t, files)

			iterA, err := tsm1.NewTSMBatchKeyIterator(tc.maxPointsPerBlk, tc.fast, nil, files, readersA...)
			if err != nil {
				t.Fatalf("tsm1.NewTSMBatchKeyIterator: %v", err)
			}
			iterB, err := tsm1.NewStreamingBatchKeyIterator(tc.maxPointsPerBlk, tc.fast, nil, files, readersB...)
			if err != nil {
				t.Fatalf("tsm1.NewStreamingBatchKeyIterator: %v", err)
			}

			outA := drainKeyIterator(t, iterA)
			outB := drainKeyIterator(t, iterB)
			if len(outA) == 0 {
				t.Fatalf("legacy iterator produced no output (case %s) — test is vacuous", tc.name)
			}
			if !bytes.Equal(outA, outB) {
				t.Fatalf("output diverges: legacy=%d bytes, streaming=%d bytes\n--- legacy ---\n%x\n--- streaming ---\n%x",
					len(outA), len(outB), outA, outB)
			}
		})
	}
}

// TestStreamingKeyIterator_OverlapMinTimeReversed is the load-bearing regression
// test for the CRITICAL blockHeap ordering bug. It constructs the exact scenario
// where the legacy (fileIndex) collection order and the streaming (minTime, fileIndex)
// heap order diverge: two files, same key, overlapping blocks, and the
// later-file-index file has the SMALLER minTime. Under the old blockHeap.Less
// (minTime primary), streaming reorders the blocks and flips the b-wins dedup
// winner. With the fix (pure fileIndex order), streaming must match legacy
// byte-for-byte and the later-file-index value (88.0 at t=15) must win.
func TestStreamingKeyIterator_OverlapMinTimeReversed(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// file0 (gen=1): t=15->1.0, t=20->2.0  (min=15, max=20)
	// file1 (gen=2): t=10->99.0, t=15->88.0 (min=10, max=15)
	// fileIndex order (gen ascending): file0, file1.
	// minTime order: file1(10) < file0(15)  -- REVERSED vs fileIndex.
	// Blocks overlap at t=15. Legacy keeps [file0, file1]; b-wins => file1's 88.0 wins at t=15.
	gens := []int{1, 2}
	perFileValues := []map[string][]tsm1.Value{
		{"cpu,host=A#!~#value": {tsm1.NewValue(15, float64(1.0)), tsm1.NewValue(20, float64(2.0))}},
		{"cpu,host=A#!~#value": {tsm1.NewValue(10, float64(99.0)), tsm1.NewValue(15, float64(88.0))}},
	}

	files := buildTSMFiles(t, dir, gens, perFileValues)
	sort.Strings(files)

	readersA := openReadersForFiles(t, files)
	readersB := openReadersForFiles(t, files)

	iterA, err := tsm1.NewTSMBatchKeyIterator(1000, false, nil, files, readersA...)
	if err != nil {
		t.Fatalf("NewTSMBatchKeyIterator: %v", err)
	}
	iterB, err := tsm1.NewStreamingBatchKeyIterator(1000, false, nil, files, readersB...)
	if err != nil {
		t.Fatalf("NewStreamingBatchKeyIterator: %v", err)
	}

	outA := drainKeyIterator(t, iterA)
	outB := drainKeyIterator(t, iterB)
	if !bytes.Equal(outA, outB) {
		t.Fatalf("streaming diverges from legacy on overlap+minTime-reversed input:\n--- legacy ---\n%x\n--- streaming ---\n%x",
			outA, outB)
	}

	// Confirm the dedup winner is the later-file-index value (88.0 at t=15),
	// not the earlier-file-index value (1.0). Re-run a fresh streaming iterator
	// and inspect decoded values directly via a float block decode.
	readersC := openReadersForFiles(t, files)
	iterC, err := tsm1.NewStreamingBatchKeyIterator(1000, false, nil, files, readersC...)
	if err != nil {
		t.Fatalf("NewStreamingBatchKeyIterator (probe): %v", err)
	}
	for iterC.Next() {
		key, minT, maxT, data, err := iterC.Read()
		if err != nil {
			t.Fatalf("iterC.Read: %v", err)
		}
		if string(key) != "cpu,host=A#!~#value" {
			continue
		}
		var v tsdb.FloatArray
		if err := tsm1.DecodeFloatArrayBlock(data, &v); err != nil {
			t.Fatalf("DecodeFloatArrayBlock: %v", err)
		}
		// Expected merged series: t=10->99.0, t=15->88.0, t=20->2.0
		wantTS := []int64{10, 15, 20}
		wantVal := []float64{99.0, 88.0, 2.0}
		if v.Len() != len(wantTS) {
			t.Fatalf("merged block has %d values, want %d (minT=%d maxT=%d)", v.Len(), len(wantTS), minT, maxT)
		}
		for i := 0; i < len(wantTS); i++ {
			if v.Timestamps[i] != wantTS[i] {
				t.Fatalf("merged[%d].ts = %d, want %d", i, v.Timestamps[i], wantTS[i])
			}
			if v.Values[i] != wantVal[i] {
				t.Fatalf("merged[%d].val = %v, want %v (t=%d): later-file-index value must win on overlap",
					i, v.Values[i], wantVal[i], v.Timestamps[i])
			}
		}
	}
	iterC.Close()
}

// TestStreamingKeyIterator_ThreeFileOverlapChain exercises a 3-file overlapping
// block chain where pairwise adjacent blocks overlap but the endpoints do not.
// This stresses sort.Stable + blocks.Less partial-order behavior.
func TestStreamingKeyIterator_ThreeFileOverlapChain(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// file0 (gen=1): t=1..5   (min=1,  max=5)
	// file1 (gen=2): t=3..8   (min=3,  max=8)  overlaps file0 and file2
	// file2 (gen=3): t=6..10  (min=6,  max=10)
	// fileIndex order == minTime order here, so this case passes even pre-fix;
	// it guards the fix against regressions in chain handling.
	gens := []int{1, 2, 3}
	perFileValues := []map[string][]tsm1.Value{
		{"cpu,host=A#!~#value": makeFloatValues(1, 5)},
		{"cpu,host=A#!~#value": makeFloatValues(3, 6)},
		{"cpu,host=A#!~#value": makeFloatValues(6, 5)},
	}

	files := buildTSMFiles(t, dir, gens, perFileValues)
	sort.Strings(files)

	readersA := openReadersForFiles(t, files)
	readersB := openReadersForFiles(t, files)

	iterA, err := tsm1.NewTSMBatchKeyIterator(1000, false, nil, files, readersA...)
	if err != nil {
		t.Fatalf("NewTSMBatchKeyIterator: %v", err)
	}
	iterB, err := tsm1.NewStreamingBatchKeyIterator(1000, false, nil, files, readersB...)
	if err != nil {
		t.Fatalf("NewStreamingBatchKeyIterator: %v", err)
	}

	outA := drainKeyIterator(t, iterA)
	outB := drainKeyIterator(t, iterB)
	if !bytes.Equal(outA, outB) {
		t.Fatalf("3-file overlap chain diverges:\n--- legacy ---\n%x\n--- streaming ---\n%x", outA, outB)
	}
}

// TestStreamingKeyIterator_FileIndexOrderNotMinTimeOrder constructs a case where
// sort.Strings(files) (production file ordering) yields a fileIndex order that
// does NOT match the blocks' minTime order. This is the general class of inputs
// that distinguishes the old minTime-primary heap from the fixed fileIndex heap.
func TestStreamingKeyIterator_FileIndexOrderNotMinTimeOrder(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// gen determines filename => sort.Strings orders by gen ascending.
	// We assign gens so that fileIndex ascending is: high-minTime, low-minTime, mid-minTime.
	//   fileIndex 0 = gen=1 -> min=100 (high)
	//   fileIndex 1 = gen=2 -> min=10  (low)
	//   fileIndex 2 = gen=3 -> min=50  (mid)
	// Each file has a distinct, non-overlapping time band, so sort.Stable will
	// reorder by minTime for both legacy and streaming; the point is to confirm
	// the collection order (fileIndex) is respected as the stable-sort input
	// everywhere and no divergence is introduced.
	gens := []int{1, 2, 3}
	perFileValues := []map[string][]tsm1.Value{
		{"cpu,host=A#!~#value": makeFloatValues(100, 5)}, // t=100..104
		{"cpu,host=A#!~#value": makeFloatValues(10, 5)},  // t=10..14
		{"cpu,host=A#!~#value": makeFloatValues(50, 5)},  // t=50..54
	}

	files := buildTSMFiles(t, dir, gens, perFileValues)
	sort.Strings(files)

	readersA := openReadersForFiles(t, files)
	readersB := openReadersForFiles(t, files)

	iterA, err := tsm1.NewTSMBatchKeyIterator(1000, false, nil, files, readersA...)
	if err != nil {
		t.Fatalf("NewTSMBatchKeyIterator: %v", err)
	}
	iterB, err := tsm1.NewStreamingBatchKeyIterator(1000, false, nil, files, readersB...)
	if err != nil {
		t.Fatalf("NewStreamingBatchKeyIterator: %v", err)
	}

	outA := drainKeyIterator(t, iterA)
	outB := drainKeyIterator(t, iterB)
	if !bytes.Equal(outA, outB) {
		t.Fatalf("fileIndex!=minTime order diverges:\n--- legacy ---\n%x\n--- streaming ---\n%x", outA, outB)
	}
}

// TestStreamingKeyIterator_ThreeFileReversedOverlapDistinctWinners is the
// strongest dedup-winner regression: three files whose fileIndex order
// (gen ascending) is the REVERSE of their minTime order, with all three
// overlapping on a single timestamp t=30 and distinct values per file. The
// highest-file-index file (file2, gen=3) must win at t=30 under both legacy
// and streaming. Pre-fix, the (minTime, fileIndex) heap would order blocks by
// minTime ascending ([file2, file1, file0]), flipping the winner to file0.
func TestStreamingKeyIterator_ThreeFileReversedOverlapDistinctWinners(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// file0(gen=1): t=30->100.0, t=31->101.0  (min=30)
	// file1(gen=2): t=29->200.0, t=30->201.0  (min=29)
	// file2(gen=3): t=28->300.0, t=30->301.0  (min=28)
	// fileIndex order: file0, file1, file2.
	// minTime order: file2(28) < file1(29) < file0(30) -- fully reversed.
	// All overlap at t=30. Legacy keeps fileIndex order [file0,file1,file2];
	// b-wins => file2's 301.0 wins at t=30.
	gens := []int{1, 2, 3}
	perFileValues := []map[string][]tsm1.Value{
		{"cpu,host=A#!~#value": {tsm1.NewValue(30, float64(100.0)), tsm1.NewValue(31, float64(101.0))}},
		{"cpu,host=A#!~#value": {tsm1.NewValue(29, float64(200.0)), tsm1.NewValue(30, float64(201.0))}},
		{"cpu,host=A#!~#value": {tsm1.NewValue(28, float64(300.0)), tsm1.NewValue(30, float64(301.0))}},
	}

	files := buildTSMFiles(t, dir, gens, perFileValues)
	sort.Strings(files)

	readersA := openReadersForFiles(t, files)
	readersB := openReadersForFiles(t, files)

	iterA, err := tsm1.NewTSMBatchKeyIterator(1000, false, nil, files, readersA...)
	if err != nil {
		t.Fatalf("NewTSMBatchKeyIterator: %v", err)
	}
	iterB, err := tsm1.NewStreamingBatchKeyIterator(1000, false, nil, files, readersB...)
	if err != nil {
		t.Fatalf("NewStreamingBatchKeyIterator: %v", err)
	}

	outA := drainKeyIterator(t, iterA)
	outB := drainKeyIterator(t, iterB)
	if !bytes.Equal(outA, outB) {
		t.Fatalf("3-file reversed overlap distinct winners diverges:\n--- legacy ---\n%x\n--- streaming ---\n%x", outA, outB)
	}

	// Probe the dedup winner: file2 (highest fileIndex) must win at t=30 => 301.0.
	readersC := openReadersForFiles(t, files)
	iterC, err := tsm1.NewStreamingBatchKeyIterator(1000, false, nil, files, readersC...)
	if err != nil {
		t.Fatalf("NewStreamingBatchKeyIterator (probe): %v", err)
	}
	for iterC.Next() {
		key, _, _, data, err := iterC.Read()
		if err != nil {
			t.Fatalf("iterC.Read: %v", err)
		}
		if string(key) != "cpu,host=A#!~#value" {
			continue
		}
		var v tsdb.FloatArray
		if err := tsm1.DecodeFloatArrayBlock(data, &v); err != nil {
			t.Fatalf("DecodeFloatArrayBlock: %v", err)
		}
		// Expected merged series: t=28->300.0, t=29->200.0, t=30->301.0, t=31->101.0
		wantTS := []int64{28, 29, 30, 31}
		wantVal := []float64{300.0, 200.0, 301.0, 101.0}
		if v.Len() != len(wantTS) {
			t.Fatalf("merged block has %d values, want %d", v.Len(), len(wantTS))
		}
		for i := 0; i < len(wantTS); i++ {
			if v.Timestamps[i] != wantTS[i] {
				t.Fatalf("merged[%d].ts = %d, want %d", i, v.Timestamps[i], wantTS[i])
			}
			if v.Values[i] != wantVal[i] {
				t.Fatalf("merged[%d].val = %v, want %v (t=%d): highest-file-index value must win on overlap",
					i, v.Values[i], wantVal[i], v.Timestamps[i])
			}
		}
	}
	iterC.Close()
}

// makeFloatValues returns n float64 Values with timestamps start, start+1, ...
func makeFloatValues(start int64, n int) []tsm1.Value {
	values := make([]tsm1.Value, n)
	for i := 0; i < n; i++ {
		values[i] = tsm1.NewValue(start+int64(i), float64(start+int64(i)))
	}
	return values
}

// applyTombstonesToFile opens a reader on file, applies the given DeleteRange
// calls (writing the .tombstone sidecar to disk), and closes the reader. A
// subsequently opened reader on the same path will load the tombstones via
// applyTombstones at open time, so both A and B readers see them.
func applyTombstonesToFile(t *testing.T, file string, ranges []struct {
	key []byte
	min int64
	max int64
}) {
	t.Helper()
	r := MustOpenTSMReader(file)
	defer r.Close()
	for _, tr := range ranges {
		if err := r.DeleteRange([][]byte{tr.key}, tr.min, tr.max); err != nil {
			t.Fatalf("DeleteRange: %v", err)
		}
	}
}

// TestStreamingKeyIterator_Interrupt verifies that closing the interrupt
// channel causes Read to return errCompactionAborted, mirroring
// tsmBatchKeyIterator.Read (compact.go:1872). Mirrors TestTSMKeyIterator_Abort.
func TestStreamingKeyIterator_Interrupt(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	r := MustTSMReader(dir, 1, map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {tsm1.NewValue(1, 1.1)},
	})

	intC := make(chan struct{})
	iter, err := tsm1.NewStreamingBatchKeyIterator(1, false, intC, []string{""}, r)
	if err != nil {
		t.Fatalf("NewStreamingBatchKeyIterator: %v", err)
	}

	var aborted bool
	for iter.Next() {
		close(intC)
		_, _, _, _, err := iter.Read()
		if err == nil {
			t.Fatalf("expected abort error, got nil")
		}
		aborted = err != nil
	}
	if !aborted {
		t.Fatalf("iteration not aborted")
	}
}


// CompactFull with UseStreamingIterator=true produces a valid, readable TSM
// file whose point set matches the expected dedup outcome. It validates the
// full compact → writeNewFiles → TSMWriter path with the streaming iterator,
// complementing the isolated iterator A/B test.
func TestCompactor_CompactFull_StreamingEnabled(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// Three files with overlapping timestamps for key A (dedup) and distinct keys.
	f1 := MustWriteTSM(dir, 1, map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {tsm1.NewValue(1, float64(1.1))},
	})
	f2 := MustWriteTSM(dir, 2, map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {tsm1.NewValue(1, float64(1.2)), tsm1.NewValue(2, float64(2.2))}, // t=1 dup, later file wins (1.2)
		"cpu,host=B#!~#value": {tsm1.NewValue(1, float64(2.1))},
	})
	f3 := MustWriteTSM(dir, 3, map[string][]tsm1.Value{
		"cpu,host=C#!~#value": {tsm1.NewValue(1, float64(3.1))},
	})

	fs := &fakeFileStore{}
	defer fs.Close()
	compactor := tsm1.NewCompactor()
	compactor.Dir = dir
	compactor.FileStore = fs
	compactor.UseStreamingIterator = true
	compactor.Open()

	files, err := compactor.CompactFull([]string{f1, f2, f3})
	if err != nil {
		t.Fatalf("CompactFull (streaming): %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("expected 1 output file, got %d: %v", len(files), files)
	}

	// Read the output back and verify the point set.
	r := MustOpenTSMReader(files[0])
	defer r.Close()

	want := map[string]map[int64]interface{}{
		"cpu,host=A#!~#value": {1: float64(1.2), 2: float64(2.2)}, // 1.1 lost to 1.2 (b-wins)
		"cpu,host=B#!~#value": {1: float64(2.1)},
		"cpu,host=C#!~#value": {1: float64(3.1)},
	}

	got := map[string]map[int64]interface{}{}
	iter := r.BlockIterator()
	for iter.Next() {
		k, _, _, _, _, blockBytes, err := iter.Read()
		if err != nil {
			t.Fatalf("BlockIterator.Read: %v", err)
		}
		vals, err := tsm1.DecodeBlock(blockBytes, nil)
		if err != nil {
			t.Fatalf("DecodeBlock: %v", err)
		}
		ks := string(k)
		if got[ks] == nil {
			got[ks] = map[int64]interface{}{}
		}
		for _, v := range vals {
			got[ks][v.UnixNano()] = v.Value()
		}
	}

	if len(got) != len(want) {
		t.Fatalf("expected %d keys, got %d (%v)", len(want), len(got), got)
	}
	for ks, wantVals := range want {
		gotVals, ok := got[ks]
		if !ok {
			t.Fatalf("missing key %q in output", ks)
		}
		if len(gotVals) != len(wantVals) {
			t.Fatalf("key %q: expected %d points, got %d (%v)", ks, len(wantVals), len(gotVals), gotVals)
		}
		for t_, wantV := range wantVals {
			gotV, ok := gotVals[t_]
			if !ok {
				t.Fatalf("key %q: missing timestamp %d", ks, t_)
			}
			if gotV != wantV {
				t.Fatalf("key %q t=%d: got %v, want %v", ks, t_, gotV, wantV)
			}
		}
	}
}


// (compact.gen.go dedup trigger len(tombstones)>0, partiallyRead, and the
// full-key-tombstoned RETRY skip) against the legacy iterator.
func TestStreamingKeyIterator_ByteIdentical_Tombstones(t *testing.T) {
	cases := []struct {
		name    string
		gens    []int
		perFile []map[string][]tsm1.Value
		// tombRanges[fileIdx] lists DeleteRange calls applied to that file.
		tombRanges [][]struct {
			key []byte
			min int64
			max int64
		}
	}{
		{
			// Partial tombstone on one file: triggers dedup path for that key.
			name: "partial_tombstone_single_file",
			gens: []int{1},
			perFile: []map[string][]tsm1.Value{
				{"cpu,host=A#!~#value": {tsm1.NewValue(1, float64(1.0)), tsm1.NewValue(2, float64(2.0)), tsm1.NewValue(3, float64(3.0))}},
			},
			tombRanges: [][]struct {
				key []byte
				min int64
				max int64
			}{
				{{[]byte("cpu,host=A#!~#value"), 2, 2}},
			},
		},
		{
			// Full-key tombstone: the entire key is deleted → Next() RETRY-skips it.
			name: "full_tombstone_key_skipped",
			gens: []int{1, 2},
			perFile: []map[string][]tsm1.Value{
				{"cpu,host=A#!~#value": {tsm1.NewValue(1, float64(1.0))}},
				{"cpu,host=B#!~#value": {tsm1.NewValue(1, float64(2.0))}},
			},
			tombRanges: [][]struct {
				key []byte
				min int64
				max int64
			}{
				{{[]byte("cpu,host=A#!~#value"), math.MinInt64, math.MaxInt64}},
				{},
			},
		},
		{
			// Tombstone on file 1 deletes a timestamp duplicated across files 1 and 2.
			name: "tombstone_overlapping_dup",
			gens: []int{1, 2},
			perFile: []map[string][]tsm1.Value{
				{"cpu,host=A#!~#value": {tsm1.NewValue(1, float64(1.0)), tsm1.NewValue(2, float64(2.0))}},
				{"cpu,host=A#!~#value": {tsm1.NewValue(1, float64(99.0)), tsm1.NewValue(2, float64(88.0))}},
			},
			tombRanges: [][]struct {
				key []byte
				min int64
				max int64
			}{
				{{[]byte("cpu,host=A#!~#value"), 1, 1}},
				{},
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			dir := MustTempDir()
			defer os.RemoveAll(dir)

			files := buildTSMFiles(t, dir, tc.gens, tc.perFile)
			sort.Strings(files)

			// Re-map tombRanges to sorted file order. buildTSMFiles returns files
			// in gens order; sort.Strings may reorder when gen numbers don't match
			// lexicographic order. To keep this test simple, gens are chosen so
			// that sort.Strings preserves order (1, 2, ... → "000001-001.tsm" < ...).
			for i, trs := range tc.tombRanges {
				if len(trs) > 0 {
					applyTombstonesToFile(t, files[i], trs)
				}
			}

			readersA := openReadersForFiles(t, files)
			readersB := openReadersForFiles(t, files)

			iterA, err := tsm1.NewTSMBatchKeyIterator(1000, false, nil, files, readersA...)
			if err != nil {
				t.Fatalf("NewTSMBatchKeyIterator: %v", err)
			}
			iterB, err := tsm1.NewStreamingBatchKeyIterator(1000, false, nil, files, readersB...)
			if err != nil {
				t.Fatalf("NewStreamingBatchKeyIterator: %v", err)
			}

			outA := drainKeyIterator(t, iterA)
			outB := drainKeyIterator(t, iterB)
			if !bytes.Equal(outA, outB) {
				t.Fatalf("tombstone output diverges: legacy=%d bytes, streaming=%d bytes\n--- legacy ---\n%x\n--- streaming ---\n%x",
					len(outA), len(outB), outA, outB)
			}
		})
	}
}

// TestStreamingKeyIterator_EstimatedIndexSize_Sum verifies that
// EstimatedIndexSize returns the SUM of input readers' index sizes (not the
// average), so the disk-buffer heuristic in Compactor.write does not
// under-estimate when many small files merge into one large output. Both the
// legacy and streaming iterators must report the sum.
func TestStreamingKeyIterator_EstimatedIndexSize_Sum(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	files := buildTSMFiles(t, dir, []int{1, 2, 3}, []map[string][]tsm1.Value{
		{"cpu,host=A#!~#value": {tsm1.NewValue(1, float64(1.0))}},
		{"cpu,host=A#!~#value": {tsm1.NewValue(2, float64(2.0))}},
		{"cpu,host=A#!~#value": {tsm1.NewValue(3, float64(3.0))}},
	})
	sort.Strings(files)

	readersA := openReadersForFiles(t, files)
	readersB := openReadersForFiles(t, files)

	var sumIndex uint32
	for _, r := range readersA {
		sumIndex += r.IndexSize()
	}
	if sumIndex == 0 {
		t.Fatalf("readers report zero index size; test is vacuous")
	}

	iterA, err := tsm1.NewTSMBatchKeyIterator(1000, false, nil, files, readersA...)
	if err != nil {
		t.Fatalf("NewTSMBatchKeyIterator: %v", err)
	}
	iterB, err := tsm1.NewStreamingBatchKeyIterator(1000, false, nil, files, readersB...)
	if err != nil {
		t.Fatalf("NewStreamingBatchKeyIterator: %v", err)
	}

	if got := iterA.EstimatedIndexSize(); got != int(sumIndex) {
		t.Fatalf("legacy EstimatedIndexSize: got %d, want sum %d (average would be %d)", got, sumIndex, int(sumIndex)/len(files))
	}
	if got := iterB.EstimatedIndexSize(); got != int(sumIndex) {
		t.Fatalf("streaming EstimatedIndexSize: got %d, want sum %d", got, sumIndex)
	}

	iterA.Close()
	iterB.Close()
}
