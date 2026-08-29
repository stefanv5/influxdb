package tsm1

import (
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// writeSimpleTSMFile writes a small, valid TSM file with one block to dir and
// returns its path.
func writeSimpleTSMFile(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "simple.tsm")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	w, err := NewTSMWriter(f)
	if err != nil {
		t.Fatal(err)
	}
	values := make(Values, 2)
	values[0] = NewValue(1, float64(1.0))
	values[1] = NewValue(2, float64(2.0))
	if err := w.Write([]byte("cpu,host=A#!~#value"), values); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteIndex(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestMMAccessor_ValidEntry_OverflowAndNegative is the regression test for
// round-3 Critical 3: a corrupted index entry must never bypass bounds checking.
// The old addition-form check (Offset + int64(Size) <= fileSize) wrapped negative
// when Offset was near MaxInt64 and Size near 0xFFFFFFFF, letting a ~4GiB
// allocation through; the mmap branches had no Offset>=0/Size>=4 checks and
// panicked on negative-offset slices. validEntry now uses subtraction form and
// is shared by every pread and mmap slice path. It also bounds entries to the
// data section (after the 5-byte header, before the index region): real blocks
// always start at offset 5, so header- and index-region offsets are rejected.
func TestMMAccessor_ValidEntry_OverflowAndNegative(t *testing.T) {
	dir := t.TempDir()
	path := writeSimpleTSMFile(t, dir)

	// Open a fresh read handle for the accessor.
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	acc := &mmapAccessor{f: f, maxMmapSize: -1}
	if _, err := acc.init(); err != nil {
		t.Fatal(err)
	}
	defer acc.close()

	bound := int64(len(acc.b))
	if acc.preadMode {
		bound = acc.fileSize
	}

	cases := []struct {
		name  string
		entry IndexEntry
	}{
		{"overflow_offset_near_maxint64", IndexEntry{Offset: math.MaxInt64 - 10, Size: 0xffffffff}},
		{"negative_offset", IndexEntry{Offset: -5, Size: 64}},
		{"offset_in_header", IndexEntry{Offset: 0, Size: 64}},
		{"offset_at_header_end", IndexEntry{Offset: tsmHeaderSize - 1, Size: 64}},
		{"offset_in_index_region", IndexEntry{Offset: acc.dataEnd, Size: 64}},
		{"size_below_crc", IndexEntry{Offset: tsmHeaderSize, Size: 3}},
		{"zero_size", IndexEntry{Offset: tsmHeaderSize, Size: 0}},
		{"end_beyond_bound", IndexEntry{Offset: bound - 2, Size: 100}},
	}

	for _, mode := range []struct {
		name      string
		preadMode bool
	}{
		{"mmap", false},
		{"pread", true},
	} {
		for _, tc := range cases {
			t.Run(mode.name+"/"+tc.name, func(t *testing.T) {
				acc.mu.RLock()
				acc.preadMode = mode.preadMode
				defer func() {
					acc.preadMode = false
					acc.mu.RUnlock()
				}()

				// blockBytes must reject the entry — never panic, never allocate.
				b, err := acc.blockBytes(&tc.entry)
				if err == nil {
					t.Fatalf("corrupt entry accepted: got %d bytes, want error", len(b))
				}

				// readBytes must reject too.
				_, _, err = acc.readBytes(&tc.entry, nil)
				if err == nil {
					t.Fatal("readBytes accepted corrupt entry, want error")
				}
			})
		}
	}
}

// TestMMAccessor_ValidEntry_AbsoluteBlockCap is the regression test for round-4
// Important 6: validEntry's backing-file bound alone still let Size=0xffffffff
// through when the (sparse/corrupt) file itself was larger than 4 GiB, driving
// a ~4 GiB make([]byte, entry.Size). The writer-aligned maxTSMBlockSize budget
// (256 MiB) must reject such entries before any allocation; the index region is
// bounded separately at 2^31 (see TestPread_Init_RejectsTwoGiBIndex). No 4 GiB
// file is materialized: the accessor's cached fileSize and dataEnd are inflated
// in memory to simulate a huge backing store whose data section extends to
// 8 GiB.
func TestMMAccessor_ValidEntry_AbsoluteBlockCap(t *testing.T) {
	dir := t.TempDir()
	path := writeSimpleTSMFile(t, dir)

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// maxMmapSize=0 forces pread mode for any non-empty file.
	acc := &mmapAccessor{f: f, maxMmapSize: 0}
	if _, err := acc.init(); err != nil {
		t.Fatal(err)
	}
	defer acc.close()

	if !acc.preadMode {
		t.Fatal("expected pread mode with maxMmapSize=0")
	}

	// Simulate a sparse 8 GiB backing file whose data section extends to 8 GiB:
	// the file-bound check would happily accept entries up to 8 GiB.
	acc.mu.Lock()
	acc.fileSize = 8 << 30
	acc.dataEnd = 8 << 30
	acc.mu.Unlock()

	bigBound := int64(8 << 30)

	// The effective cap is min(maxTSMBlockSize, maxAllocLen): 256 MiB on every
	// supported platform, since maxAllocLen is at least MaxInt32. The exact
	// boundary is legal, one byte over is not — validEntry is a pure predicate
	// so these cases allocate nothing. Legal entries use Offset=tsmHeaderSize:
	// real blocks start after the 5-byte header.
	blockCap := maxTSMBlockSize
	if maxAllocLen < blockCap {
		blockCap = maxAllocLen
	}
	cases := []struct {
		name  string
		entry IndexEntry
		bound int64
		want  bool
	}{
		{"size_at_cap_allowed", IndexEntry{Offset: tsmHeaderSize, Size: uint32(blockCap)}, bigBound, true},
		{"size_one_over_cap_rejected", IndexEntry{Offset: tsmHeaderSize, Size: uint32(blockCap) + 1}, bigBound, false},
		{"size_0xffffffff_rejected_despite_bound", IndexEntry{Offset: tsmHeaderSize, Size: 0xffffffff}, bigBound, false},
		{"size_maxint32_rejected", IndexEntry{Offset: tsmHeaderSize, Size: math.MaxInt32}, bigBound, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			acc.mu.RLock()
			got := acc.validEntry(&tc.entry, tc.bound)
			acc.mu.RUnlock()
			if got != tc.want {
				t.Fatalf("validEntry(Offset=%d Size=%d bound=%d) = %v, want %v", tc.entry.Offset, tc.entry.Size, tc.bound, got, tc.want)
			}
		})
	}

	// blockBytes must reject a bound-legal but over-cap entry BEFORE the
	// make([]byte, entry.Size) allocation — assert the cumulative allocation
	// delta stays microscopic (a 4 GiB allocation would blow both up).
	entry := IndexEntry{Offset: tsmHeaderSize, Size: 0xffffffff}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	acc.mu.RLock()
	_, err = acc.blockBytes(&entry)
	acc.mu.RUnlock()
	runtime.ReadMemStats(&after)
	if err == nil {
		t.Fatal("blockBytes accepted over-cap entry, want error")
	}
	if delta := after.TotalAlloc - before.TotalAlloc; delta > 1<<20 {
		t.Fatalf("blockBytes allocated %d bytes before rejecting over-cap entry, want ~0", delta)
	}

	// readBytes must reject it the same way.
	acc.mu.RLock()
	_, _, err = acc.readBytes(&entry, nil)
	acc.mu.RUnlock()
	if err == nil {
		t.Fatal("readBytes accepted over-cap entry, want error")
	}
}

// TestPread_Init_RejectsEmptyIndexFooter is the regression test for round-4
// "pread/mmap empty-footer consistency": pread mode accepted a footer whose
// indexStart == fileSize-8 (an empty index region) while mmap mode rejected
// indexStart >= indexOfsPos. The same file must be rejected in both modes.
func TestPread_Init_RejectsEmptyIndexFooter(t *testing.T) {
	dir := t.TempDir()
	path := writeSimpleTSMFile(t, dir)

	// Overwrite the footer so it claims indexStart == fileSize-8, i.e. an
	// empty index region.
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		t.Fatal(err)
	}
	footer := make([]byte, 8)
	binary.BigEndian.PutUint64(footer, uint64(stat.Size()-8))
	if _, err := f.WriteAt(footer, stat.Size()-8); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	for _, mode := range []struct {
		name        string
		maxMmapSize int64
	}{
		{"pread", 0}, // pread always
		{"mmap", -1}, // mmap always
	} {
		t.Run(mode.name, func(t *testing.T) {
			fd, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer fd.Close()

			acc := &mmapAccessor{f: fd, maxMmapSize: mode.maxMmapSize}
			if _, err := acc.init(); err == nil {
				acc.close()
				t.Fatal("init accepted a footer describing an empty index, want error")
			}
			// init can fail after it has already created the whole-file mmap
			// (mmap mode); release it explicitly — an active mapping keeps the
			// file locked on Windows and would break t.TempDir cleanup.
			// close() is a no-op when nothing was mapped.
			acc.close()
		})
	}
}

// TestTSMReader_PreadClose_ReleasesIndexBytes is the regression test for
// round-4 "pread Close heap-index retention": closing a pread-mode reader only
// nil'd mmapAccessor.idxBuf, while indirectIndex.b (and its minKey/maxKey
// sub-slices) still referenced the same array, keeping the full heap index
// resident for as long as any caller held the closed reader.
func TestTSMReader_PreadClose_ReleasesIndexBytes(t *testing.T) {
	dir := t.TempDir()
	path := writeSimpleTSMFile(t, dir)

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}

	r, err := NewTSMReader(f, WithMaxMmapFileSize(0))
	if err != nil {
		f.Close()
		t.Fatal(err)
	}

	acc, ok := r.accessor.(*mmapAccessor)
	if !ok {
		r.Close()
		t.Fatalf("accessor is %T, want *mmapAccessor", r.accessor)
	}
	if !acc.preadMode {
		r.Close()
		t.Fatal("expected pread mode with maxMmapSize=0")
	}
	if len(acc.idxBuf) == 0 {
		r.Close()
		t.Fatal("expected non-empty pread index buffer")
	}
	if len(acc.index.b) == 0 {
		r.Close()
		t.Fatal("expected indirectIndex to reference the index bytes")
	}

	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	// Both the accessor buffer and the index's reference to it must be gone,
	// otherwise the heap index stays reachable through the closed reader.
	if len(acc.idxBuf) != 0 {
		t.Fatal("accessor idxBuf not released on Close")
	}
	if acc.index.b != nil {
		t.Fatal("indirectIndex.b not released on Close")
	}
	if acc.index.minKey != nil || acc.index.maxKey != nil {
		t.Fatal("indirectIndex minKey/maxKey (sub-slices of the index bytes) not released on Close")
	}
}

// TestPread_Init_RejectsTwoGiBIndex is the regression test for round-4
// Critical 3b: the pread index gate reused maxTSMBlockSize, and mmap mode had
// no index-length gate at all. indirectIndex.UnmarshalBinary assumes the index
// fits in an int32 — int32(len(b)) of exactly 2^31 wraps negative — so an index
// region of exactly 2^31 bytes must be rejected in both modes. The 2 GiB file
// is hole-backed: only the 5-byte header and the 8-byte footer are written and
// everything in between reads as zeros, so no 2 GiB is materialized on disk
// (verified sparse on NTFS and ext4).
func TestPread_Init_RejectsTwoGiBIndex(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "huge_index.tsm")

	// A file of exactly 2^31+8 bytes: valid header at 0 and a footer claiming
	// indexStart=0 at 2^31, so indexLen = fileSize-8-indexStart = 2^31.
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0x16, 0xD1, 0x16, 0xD1, 0x01}, 0); err != nil {
		f.Close()
		t.Fatal(err)
	}
	footer := make([]byte, 8)
	binary.BigEndian.PutUint64(footer, 0)
	if _, err := f.WriteAt(footer, int64(1)<<31); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	for _, mode := range []struct {
		name        string
		maxMmapSize int64
	}{
		{"pread", 0}, // pread always
		{"mmap", -1}, // mmap always
	} {
		t.Run(mode.name, func(t *testing.T) {
			if mode.maxMmapSize < 0 {
				switch runtime.GOARCH {
				case "386", "arm":
					t.Skip("mmap of a 2 GiB file is not possible on 32-bit platforms")
				}
			}

			fd, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer fd.Close()

			acc := &mmapAccessor{f: fd, maxMmapSize: mode.maxMmapSize}
			if _, err := acc.init(); err == nil {
				acc.close()
				t.Fatal("init accepted an index region of exactly 2^31 bytes, want error")
			}
			// mmap mode maps the file before the footer is read; init must
			// have released its own mapping, and close is a no-op then.
			acc.close()
		})
	}
}

// TestIndirectIndex_UnmarshalBinary_Corrupt is the regression test for round-4
// Critical 3c: a zero-filled index drove the per-key cursor backwards (a count
// of zero makes the step i += (count-1)*indexEntrySize negative) and a key
// length larger than the remaining bytes sliced past the end of the buffer —
// both panicked before count and key-length validation was added. A corrupted
// index must be rejected with an error instead.
func TestIndirectIndex_UnmarshalBinary_Corrupt(t *testing.T) {
	cases := []struct {
		name string
		b    []byte
	}{
		// Zero-filled index: count == 0 for the first key, the backward cursor
		// step moved i before the start of b.
		{"all_zero_22", make([]byte, 22)},
		{"all_zero_64", make([]byte, 64)},
		// count == 0 behind an otherwise well-formed key header.
		{"valid_key_header_zero_count", []byte{0x00, 0x03, 'c', 'p', 'u', 0x00, 0x00, 0x00, 0x00}},
		// Key length of 0xffff with only 5 bytes of index: reading the key
		// would slice past the end of b.
		{"keylen_overrun", []byte{0xff, 0xff, 0x00, 0x01, 0x02}},
		{"keylen_overrun_exact", []byte{0x00, 0x14, 0x00, 0x00, 0x00}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			idx := NewIndirectIndex()
			if err := idx.UnmarshalBinary(tc.b); err == nil {
				t.Fatal("UnmarshalBinary accepted corrupt index bytes, want error")
			}
		})
	}
}

// TestTSMReader_OpenCorruptIndex_NoPanic opens a file whose index region is
// zero-filled (behind a valid header) through NewTSMReader in both modes: the
// footer reads as indexStart=0, so UnmarshalBinary sees a zero-filled index
// region, which must be rejected as corrupt — never panic.
func TestTSMReader_OpenCorruptIndex_NoPanic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "corrupt_index.tsm")

	// Valid 5-byte header followed by 22 zero bytes.
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(append([]byte{0x16, 0xD1, 0x16, 0xD1, 0x01}, make([]byte, 22)...)); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	for _, mode := range []struct {
		name        string
		maxMmapSize int64
	}{
		{"pread", 0}, // pread always
		{"mmap", -1}, // mmap always
	} {
		t.Run(mode.name, func(t *testing.T) {
			fd, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer fd.Close()

			// init fails inside UnmarshalBinary, so the file descriptor was
			// never handed over to the reader and stays ours to close.
			r, err := NewTSMReader(fd, WithMaxMmapFileSize(mode.maxMmapSize))
			if err == nil {
				r.Close()
				t.Fatal("NewTSMReader accepted a zero-filled index, want error")
			}
		})
	}
}

// TestNewTSMReader_InitFailure_ReleasesMapping is the regression test for
// round-4 Important 3: a failed NewTSMReader left the accessor's resources
// behind — an mmap-mode init that failed after mmap kept the file mapped. On
// Windows an active mapping locks the file, so the mapping being released is
// asserted indirectly: the file must stay openable and be removable after the
// failed open.
func TestNewTSMReader_InitFailure_ReleasesMapping(t *testing.T) {
	dir := t.TempDir()
	path := writeSimpleTSMFile(t, dir)

	// Corrupt the footer: an indexStart at the footer position is invalid in
	// both modes. mmap mode only reaches that check after the file has been
	// mapped, so init must clean up its own mapping.
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		t.Fatal(err)
	}
	footer := make([]byte, 8)
	binary.BigEndian.PutUint64(footer, uint64(stat.Size()))
	if _, err := f.WriteAt(footer, stat.Size()-8); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	for _, mode := range []struct {
		name        string
		maxMmapSize int64
	}{
		{"pread", 0}, // pread always
		{"mmap", -1}, // mmap always
	} {
		t.Run(mode.name, func(t *testing.T) {
			fd, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}

			r, err := NewTSMReader(fd, WithMaxMmapFileSize(mode.maxMmapSize))
			if err == nil {
				r.Close()
				fd.Close()
				t.Fatal("NewTSMReader accepted an indexStart at the footer position, want error")
			}
			// NewTSMReader failed, so the descriptor was never handed over and
			// stays ours to close.
			if err := fd.Close(); err != nil {
				t.Fatal(err)
			}

			// A mapping left behind by the failed init keeps the file locked:
			// a fresh open must succeed.
			fd2, err := os.Open(path)
			if err != nil {
				t.Fatalf("file not re-openable after failed NewTSMReader: %v", err)
			}
			if err := fd2.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}

	// Nothing may be mapped anymore: removal succeeds. An active mapping would
	// lock the file on Windows and fail the t.TempDir cleanup too.
	if err := os.Remove(path); err != nil {
		t.Fatalf("file still locked after failed NewTSMReader: %v", err)
	}
}
