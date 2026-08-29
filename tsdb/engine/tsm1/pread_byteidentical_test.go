package tsm1_test

import (
	"bytes"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// MustOpenTSMReaderMmap opens a TSM reader in mmap mode (default, no pread option).
func MustOpenTSMReaderMmap(name string) *tsm1.TSMReader {
	f, err := os.Open(name)
	if err != nil {
		panic(err)
	}
	r, err := tsm1.NewTSMReader(f)
	if err != nil {
		panic(err)
	}
	return r
}

// MustOpenTSMReaderPread opens a TSM reader in pread mode (WithMaxMmapFileSize(0) =
// always pread, never mmap block data). The option return type is unexported
// (tsmReaderOption), so we pass it inline without naming the type.
func MustOpenTSMReaderPread(name string) *tsm1.TSMReader {
	f, err := os.Open(name)
	if err != nil {
		panic(err)
	}
	r, err := tsm1.NewTSMReader(f, tsm1.WithMaxMmapFileSize(0))
	if err != nil {
		panic(err)
	}
	return r
}

// drainReaderBlocks reads all blocks from a TSMReader's BlockIterator and returns
// a deterministic byte blob (key+minT+maxT+data per block) for comparison.
func drainReaderBlocks(t *testing.T, r *tsm1.TSMReader) []byte {
	t.Helper()
	var buf bytes.Buffer
	iter := r.BlockIterator()
	for iter.Next() {
		key, minTime, maxTime, _, _, data, err := iter.Read()
		if err != nil {
			t.Fatalf("BlockIterator.Read: %v", err)
		}
		// length-prefixed key
		buf.WriteByte(byte(len(key) >> 8))
		buf.WriteByte(byte(len(key)))
		buf.Write(key)
		// minTime/maxTime as 8 bytes each
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
	return buf.Bytes()
}

// TestPread_ByteIdentical_MmapVsPread verifies that opening the same TSM file
// in mmap mode (default, no WithMaxMmapFileSize) vs pread mode
// (WithMaxMmapFileSize(0) = always pread) produces byte-identical block reads.
// This proves the pread path reads the same bytes as mmap — the foundation for
// byte-identical compaction output.
func TestPread_ByteIdentical_MmapVsPread(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// Write a TSM file with multiple keys and types, including overlapping timestamps.
	vals := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {tsm1.NewValue(1, float64(1.0)), tsm1.NewValue(2, float64(2.0))},
		"cpu,host=B#!~#value": {tsm1.NewValue(1, float64(3.0))},
		"mem,host=A#!~#value": {tsm1.NewValue(1, int64(10)), tsm1.NewValue(2, int64(20))},
		"msg,host=A#!~#value": {tsm1.NewValue(1, "hello"), tsm1.NewValue(2, "world")},
		"flg,host=A#!~#value": {tsm1.NewValue(1, true), tsm1.NewValue(2, false)},
	}
	file := MustWriteTSM(dir, 1, vals)

	// Open in mmap mode (default — no WithMaxMmapFileSize option)
	rMmap := MustOpenTSMReaderMmap(file)
	defer rMmap.Close()

	// Open in pread mode (WithMaxMmapFileSize(0) = always pread)
	rPread := MustOpenTSMReaderPread(file)
	defer rPread.Close()

	outMmap := drainReaderBlocks(t, rMmap)
	outPread := drainReaderBlocks(t, rPread)

	if len(outMmap) == 0 {
		t.Fatalf("mmap reader produced no blocks — test is vacuous")
	}
	if !bytes.Equal(outMmap, outPread) {
		t.Fatalf("mmap vs pread block bytes diverge:\n--- mmap ---\n%x\n--- pread ---\n%x",
			outMmap, outPread)
	}
	t.Logf("mmap and pread produced identical %d bytes across all blocks", len(outMmap))
}

// TestPread_ReadAll_ByteIdentical verifies that readAll (the query-path method)
// returns identical values in mmap and pread modes.
func TestPread_ReadAll_ByteIdentical(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	vals := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {tsm1.NewValue(1, float64(1.0)), tsm1.NewValue(2, float64(2.0)), tsm1.NewValue(3, float64(3.0))},
		"msg,host=A#!~#value": {tsm1.NewValue(1, "hello"), tsm1.NewValue(2, "world")},
	}
	file := MustWriteTSM(dir, 1, vals)

	rMmap := MustOpenTSMReaderMmap(file)
	defer rMmap.Close()
	rPread := MustOpenTSMReaderPread(file)
	defer rPread.Close()

	keys := []string{"cpu,host=A#!~#value", "msg,host=A#!~#value"}
	for _, key := range keys {
		mmapVals, err := rMmap.ReadAll([]byte(key))
		if err != nil {
			t.Fatalf("mmap ReadAll(%s): %v", key, err)
		}
		preadVals, err := rPread.ReadAll([]byte(key))
		if err != nil {
			t.Fatalf("pread ReadAll(%s): %v", key, err)
		}
		if len(mmapVals) != len(preadVals) {
			t.Fatalf("key %s: mmap has %d values, pread has %d", key, len(mmapVals), len(preadVals))
		}
		for i, mv := range mmapVals {
			if mv.UnixNano() != preadVals[i].UnixNano() {
				t.Fatalf("key %s value %d: time mismatch mmap=%d pread=%d", key, i, mv.UnixNano(), preadVals[i].UnixNano())
			}
			if mv.Value() != preadVals[i].Value() {
				t.Fatalf("key %s value %d: value mismatch mmap=%v pread=%v", key, i, mv.Value(), preadVals[i].Value())
			}
		}
	}
	t.Logf("readAll: mmap and pread produced identical values for all keys")
}

// TestPread_CloseReopen verifies that a pread-mode reader can be closed and a
// new reader opened on the same file (no fd leak — the fix for the close()
// early-return bug where m.b==nil caused m.f to never be closed).
func TestPread_CloseReopen(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	file := MustWriteTSM(dir, 1, map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {tsm1.NewValue(1, float64(1.0))},
	})

	// Open, read, close — repeat several times. If close() leaks the fd
	// (m.b==nil early-return bug), eventually os.Open fails with "too many open files".
	for i := 0; i < 50; i++ {
		r := MustOpenTSMReaderPread(file)
		// Read a block to ensure the file handle is used.
		iter := r.BlockIterator()
		if !iter.Next() {
			t.Fatalf("iteration %d: no blocks", i)
		}
		if _, _, _, _, _, _, err := iter.Read(); err != nil {
			t.Fatalf("iteration %d: Read: %v", i, err)
		}
		if err := r.Close(); err != nil {
			t.Fatalf("iteration %d: Close: %v", i, err)
		}
	}
	t.Logf("pread: 50 open/read/close cycles succeeded (no fd leak)")
}

// TestPread_CompactFull_MmapVsPread runs CompactFull on the same input files
// with a mmap-mode compactor and a pread-mode compactor, and asserts the output
// TSM file bytes are identical.
func TestPread_CompactFull_MmapVsPread(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// Write 3 TSM files with overlapping data across files (dedup path).
	v1 := tsm1.NewValue(1, float64(1.0))
	v2 := tsm1.NewValue(2, float64(2.0))
	v3 := tsm1.NewValue(1, float64(99.0)) // duplicate ts=1, higher gen wins
	f1 := MustWriteTSM(dir, 1, map[string][]tsm1.Value{"cpu,host=A#!~#value": {v1, v2}})
	f2 := MustWriteTSM(dir, 2, map[string][]tsm1.Value{"cpu,host=A#!~#value": {v3}})
	f3 := MustWriteTSM(dir, 3, map[string][]tsm1.Value{"cpu,host=B#!~#value": {tsm1.NewValue(1, float64(3.0))}})
	files := []string{f1, f2, f3}
	sort.Strings(files)

	// Use separate output dirs to avoid filename collision between the two compactors.
	dirMmap := MustTempDir()
	defer os.RemoveAll(dirMmap)
	dirPread := MustTempDir()
	defer os.RemoveAll(dirPread)

	// mmap-mode compactor (default — fakeFileStore opens readers without WithMaxMmapFileSize)
	fsMmap := &fakeFileStore{}
	defer fsMmap.Close()
	cMmap := tsm1.NewCompactor()
	cMmap.Dir = dirMmap
	cMmap.FileStore = fsMmap
	cMmap.Open()
	defer cMmap.DisableCompactions()
	outMmap, err := cMmap.CompactFull(files)
	if err != nil {
		t.Fatalf("mmap CompactFull: %v", err)
	}

	// pread-mode compactor — need a fakeFileStore that opens readers with WithMaxMmapFileSize(0).
	fsPread := &preadFileStore{}
	defer fsPread.Close()
	cPread := tsm1.NewCompactor()
	cPread.Dir = dirPread
	cPread.FileStore = fsPread
	cPread.Open()
	defer cPread.DisableCompactions()
	outPread, err := cPread.CompactFull(files)
	if err != nil {
		t.Fatalf("pread CompactFull: %v", err)
	}

	// Compare output file bytes.
	if len(outMmap) != len(outPread) {
		t.Fatalf("output file count mismatch: mmap=%d pread=%d", len(outMmap), len(outPread))
	}
	sort.Strings(outMmap)
	sort.Strings(outPread)
	for i := range outMmap {
		mmapBytes, err := os.ReadFile(outMmap[i])
		if err != nil {
			t.Fatalf("read mmap output %d: %v", i, err)
		}
		preadBytes, err := os.ReadFile(outPread[i])
		if err != nil {
			t.Fatalf("read pread output %d: %v", i, err)
		}
		if !bytes.Equal(mmapBytes, preadBytes) {
			t.Fatalf("output file %d bytes diverge: mmap=%d bytes, pread=%d bytes",
				i, len(mmapBytes), len(preadBytes))
		}
	}
	t.Logf("CompactFull: mmap and pread produced %d identical output file(s)", len(outMmap))
}

// preadFileStore is a fakeFileStore that opens TSM readers in pread mode
// (WithMaxMmapFileSize(0) = always pread, never mmap block data).
type preadFileStore struct {
	PathsFn    func() []tsm1.FileStat
	lastModified time.Time
	blockCount   int
	readers      []*tsm1.TSMReader
}

func (w *preadFileStore) Stats() []tsm1.FileStat           { return w.PathsFn() }
func (w *preadFileStore) NextGeneration() int              { return 1 }
func (w *preadFileStore) LastModified() time.Time          { return w.lastModified }
func (w *preadFileStore) BlockCount(path string, idx int) int { return w.blockCount }
func (w *preadFileStore) TSMReader(path string) *tsm1.TSMReader {
	r := MustOpenTSMReaderPread(path)
	w.readers = append(w.readers, r)
	r.Ref()
	return r
}
func (w *preadFileStore) ParseFileName(path string) (int, int, error) {
	return tsm1.DefaultParseFileName(path)
}
func (w *preadFileStore) Close() {
	for _, r := range w.readers {
		r.Close()
	}
	w.readers = nil
}
