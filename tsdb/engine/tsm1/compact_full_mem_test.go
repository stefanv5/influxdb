package tsm1_test

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// readProcStatus reads VmRSS, VmData, VmSize from /proc/self/status (Linux only).
// Returns (vmRSS, vmData, vmSize, ok). On non-Linux, ok=false.
//
// RSS is LOGGED ONLY. mmap pages pinned by open TSM readers are not attributable
// to the compaction's Go-heap behavior and the kernel's reclaim of file-backed
// pages is OS-dependent, so a hard RSS assertion would be flaky across
// platforms (notably Windows dev boxes, where /proc does not exist at all).
func readProcStatus() (vmRSS, vmData, vmSize uint64, ok bool) {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0, 0, 0, false
	}
	for _, line := range strings.Split(string(data), "\n") {
		var v uint64
		if _, err := fmt.Sscanf(line, "VmRSS: %d kB", &v); err == nil {
			vmRSS = v * 1024 // kB → bytes
			ok = true
		}
		if _, err := fmt.Sscanf(line, "VmData: %d kB", &v); err == nil {
			vmData = v * 1024
		}
		if _, err := fmt.Sscanf(line, "VmSize: %d kB", &v); err == nil {
			vmSize = v * 1024
		}
	}
	return
}

// measurePeakHeapInuse runs fn with the Go GC disabled so HeapInuse grows
// monotonically to the true peak; a single post-run MemStats read then IS the
// peak — no transient allocation can be collected away mid-run, which is the
// failure mode of sampling after CompactFull returns (or of periodic sampling
// that misses sub-millisecond spikes). GC is restored on return, including via
// runtime.Goexit if fn (or the caller's t.Fatal path) unwinds the goroutine.
//
// The returned value is the peak above a GC'd baseline. With GC disabled this
// is the cumulative allocation high-water mark of the run: every byte the
// compaction touches — pread block buffers, decoded values, encode buffers —
// counts once, so the bound below has to accommodate one full gather pass plus
// one full decode pass (measured ~2.5× on-disk input), and fails anything that
// allocates multiples of that (double-buffered decode, unbounded merged
// accumulation, reader-count blowups).
func measurePeakHeapInuse(t *testing.T, fn func() error) uint64 {
	t.Helper()
	runtime.GC()
	var base runtime.MemStats
	runtime.ReadMemStats(&base)

	old := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(old)

	if err := fn(); err != nil {
		t.Fatal(err)
	}

	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	if after.HeapInuse > base.HeapInuse {
		return after.HeapInuse - base.HeapInuse
	}
	return 0
}

// compactFileStore is the subset of FileStore behavior the Compactor needs.
type compactFileStore interface {
	NextGeneration() int
	TSMReader(path string) *tsm1.TSMReader
}

// preadMemFileStore is a fakeFileStore variant for memory measurement: it opens
// every TSM reader in PREAD mode (WithMaxMmapFileSize(0) = always pread, never
// mmap block data). In pread mode ReadBytes allocates a fresh heap buffer per
// block read (mmapAccessor.readBytes), so the compaction's block traffic is
// visible to runtime.MemStats. The default fakeFileStore opens mmap readers
// whose block.b aliases the mapped file — invisible to the Go heap — which is
// exactly why the earlier round of memory tests measured nothing.
type preadMemFileStore struct {
	PathsFn      func() []tsm1.FileStat
	lastModified time.Time
	blockCount   int
	readers      []*tsm1.TSMReader
}

func (w *preadMemFileStore) Stats() []tsm1.FileStat { return w.PathsFn() }

func (w *preadMemFileStore) NextGeneration() int { return 1 }

func (w *preadMemFileStore) LastModified() time.Time { return w.lastModified }

func (w *preadMemFileStore) BlockCount(path string, idx int) int { return w.blockCount }

func (w *preadMemFileStore) TSMReader(path string) *tsm1.TSMReader {
	f, err := os.Open(path)
	if err != nil {
		panic(fmt.Sprintf("open tsm file: %v", err))
	}
	// WithMaxMmapFileSize(0): the option's return type is unexported
	// (tsmReaderOption) but the exported var is callable from outside the
	// package — the same pattern pread_byteidentical_test.go uses.
	r, err := tsm1.NewTSMReader(f, tsm1.WithMaxMmapFileSize(0))
	if err != nil {
		panic(fmt.Sprintf("new pread tsm reader: %v", err))
	}
	w.readers = append(w.readers, r)
	r.Ref()
	return r
}

func (w *preadMemFileStore) Close() {
	for _, r := range w.readers {
		r.Close()
	}
	w.readers = nil
}

// hotKeyString is the single hot key every file in buildHotKeyStringFiles
// carries: one key spanning the whole compaction group is the worst case for
// per-key block gathering (review Critical 2).
const hotKeyString = "log,host=hot#!~#message"

// buildHotKeyStringFiles builds N TSM files (gens 1..N) that each hold ONE hot
// key with P points of V-byte INCOMPRESSIBLE (random) string values, written as
// ≤1000-point chunks — one TSMWriter.Write per chunk, mirroring how
// cacheKeyIterator.encode chunks a key's cache values — so every file contains
// ceil(P/1000) separate blocks rather than one giant block.
//
// All files use the SAME timestamp range, so every block overlaps across files
// and the dedup merge path must decode all of a window's blocks.
//
// Random payloads keep the on-disk size equal to the decoded volume; if the
// strings were compressible the on-disk size would shrink and the
// peak-vs-on-disk bound in the tests below would silently tighten.
// Returns the sorted file paths and the total on-disk size.
func buildHotKeyStringFiles(t *testing.T, dir string, N, P, V int) ([]string, int64) {
	t.Helper()
	chunk := tsdb.DefaultMaxPointsPerBlock

	files := make([]string, N)
	var totalSize int64
	for i := 0; i < N; i++ {
		gen := i + 1
		// Distinct random content per file: same timestamps, different values,
		// so dedup has real work and the highest generation wins each timestamp.
		// A per-generation seeded source keeps the data reproducible.
		pool := make([]byte, P*V)
		if _, err := rand.New(rand.NewSource(int64(gen) + 1)).Read(pool); err != nil {
			t.Fatalf("generate random payload: %v", err)
		}
		vals := make([]tsm1.Value, P)
		for p := 0; p < P; p++ {
			vals[p] = tsm1.NewValue(int64(p), string(pool[p*V:(p+1)*V]))
		}
		files[i] = mustWriteTSMChunked(t, dir, gen, hotKeyString, vals, chunk)
		if fi, err := os.Stat(files[i]); err == nil {
			totalSize += fi.Size()
		}
	}
	sort.Strings(files)
	return files, totalSize
}

// mustWriteTSMChunked writes vals for a single key to a new TSM file of the
// given generation, one block per ≤chunk-point slice (NOT one giant block).
func mustWriteTSMChunked(t *testing.T, dir string, gen int, key string, vals []tsm1.Value, chunk int) string {
	t.Helper()
	w, name := MustTSMWriter(dir, gen)
	for start := 0; start < len(vals); start += chunk {
		end := start + chunk
		if end > len(vals) {
			end = len(vals)
		}
		if err := w.Write([]byte(key), vals[start:end]); err != nil {
			t.Fatalf("write chunk [%d,%d) to %s: %v", start, end, name, err)
		}
	}
	if err := w.WriteIndex(); err != nil {
		t.Fatalf("write index to %s: %v", name, err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer for %s: %v", name, err)
	}
	return name
}

// measuredCompactFull runs one streaming CompactFull over files inside a
// GC-disabled measurement window (see measurePeakHeapInuse) and returns the
// peak HeapInuse delta above a GC'd baseline plus the compaction's output file
// paths (in this harness CompactFull returns the freshly written .tsm.tmp
// paths; FileStore.Replace is what renames them in production). fs decides the
// reader mode: fakeFileStore opens mmap readers, preadMemFileStore opens pread
// readers.
//
// /proc VmRSS/VmData deltas are logged on Linux for observation only — no RSS
// assertion (see readProcStatus).
func measuredCompactFull(t *testing.T, outDir string, files []string, fs compactFileStore, label string) (uint64, []string) {
	t.Helper()
	compactor := tsm1.NewCompactor()
	compactor.Dir = outDir
	compactor.FileStore = fs
	compactor.UseStreamingIterator = true
	compactor.Open()
	defer compactor.DisableCompactions()

	rssBefore, dataBefore, _, isLinux := readProcStatus()

	var out []string
	peak := measurePeakHeapInuse(t, func() error {
		var err error
		out, err = compactor.CompactFull(files)
		return err
	})

	rssAfter, dataAfter, vmSize, _ := readProcStatus()
	t.Logf("%s: peak Go heap above GC'd baseline: %d bytes (%.1f MB)", label, peak, float64(peak)/1e6)
	if isLinux {
		t.Logf("%s: /proc VmRSS %.1f→%.1f MB (delta %.1f MB), VmData delta %.1f MB, VmSize %.1f MB (logged, not asserted)",
			label,
			float64(rssBefore)/1e6, float64(rssAfter)/1e6,
			float64(int64(rssAfter)-int64(rssBefore))/1e6,
			float64(int64(dataAfter)-int64(dataBefore))/1e6,
			float64(vmSize)/1e6)
	}
	return peak, out
}

// countHotKeyPoints opens the given output TSM files and returns the total
// number of points stored for the hot key, checking each value's length.
func countHotKeyPoints(t *testing.T, outputs []string, valLen int) int {
	t.Helper()
	if len(outputs) == 0 {
		t.Fatalf("compaction produced no output files — measurement would be vacuous")
	}
	total := 0
	for _, f := range outputs {
		r := MustOpenTSMReader(f)
		vals, err := r.ReadAll([]byte(hotKeyString))
		if err != nil {
			r.Close()
			t.Fatalf("ReadAll(%s): %v", f, err)
		}
		for _, v := range vals {
			s, ok := v.Value().(string)
			if !ok || len(s) != valLen {
				r.Close()
				t.Fatalf("output value at ts %d has wrong shape (len %d, ok %v)", v.UnixNano(), len(s), ok)
			}
		}
		total += len(vals)
		if err := r.Close(); err != nil {
			t.Fatalf("close output reader %s: %v", f, err)
		}
	}
	return total
}

// TestCompaction_Full_PreadHotKey_PeakHeapBounded measures the true Go-heap
// peak of a full compaction whose group is a SINGLE hot key spread across N
// files of LARGE string values in multiple ≤1000-pt blocks, with readers forced
// into pread mode and the GC disabled for the measurement.
//
// This is the hard test the review demanded (Critical 2 / Important 7):
//   - pread readers (WithMaxMmapFileSize(0)): every block read allocates a heap
//     buffer, so the per-key block gather across the group is VISIBLE to
//     runtime.MemStats. The earlier version of this test used default mmap
//     readers, whose block.b aliases the mapped file and never shows up.
//   - GC disabled during the run: HeapInuse grows monotonically to the true
//     peak; sampling once after CompactFull returns can miss a transient peak
//     the GC already reclaimed.
//   - single hot key, many blocks, V=4KB strings: exercises the worst-case
//     per-key gather + dedup decode amplification, not 500 floats.
//   - HARD bound: peak < 4× the on-disk input. Measured behavior (one gather
//     pass + one decode pass) sits near 2.5×, so a runaway that holds the whole
//     group's data more than once over — or that scales the gather beyond the
//     group — fails.
//   - pread-engagement proof: the pread peak must exceed the mmap peak for the
//     same data; if it does not, the readers are not really in pread mode and
//     the bound above would pass vacuously.
//
// The test is deterministic on Windows too: the measurement is pure Go-heap and
// pread opens no mmap handles (the source of the old Windows flakiness), so
// there is deliberately no OS skip here.
func TestCompaction_Full_PreadHotKey_PeakHeapBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("memory bound test is slow; run without -short")
	}

	const (
		numFiles  = 8    // compaction group: the hot key spans 8 files
		pointsPer = 2000 // points per file → 2 blocks per file at 1000-pt chunks
		valSize   = 4096 // bytes per string value — the large hot-key payload
	)
	// Hard ceiling on the peak heap relative to the on-disk input size.
	const maxPeakPerInputByte = 4

	dir := MustTempDir()
	defer os.RemoveAll(dir)

	files, totalOnDisk := buildHotKeyStringFiles(t, dir, numFiles, pointsPer, valSize)
	blocksPerFile := (pointsPer + tsdb.DefaultMaxPointsPerBlock - 1) / tsdb.DefaultMaxPointsPerBlock

	// Incompressibility guard: the bound below treats on-disk bytes as a proxy
	// for both the pread block-gather volume and the decoded volume. If the
	// payload ever became compressible, the on-disk size would shrink and the
	// bound would silently tighten into flakiness.
	uncompressed := uint64(numFiles) * uint64(pointsPer) * uint64(valSize)
	if uint64(totalOnDisk) < uncompressed*3/4 {
		t.Fatalf("test data unexpectedly compressible: on-disk %d bytes < 3/4 of uncompressed %d bytes — the peak/on-disk bound would be distorted",
			totalOnDisk, uncompressed)
	}
	if blocksPerFile < 2 {
		t.Fatalf("test data has %d block(s) per file; need ≥2 so the hot key spans multiple blocks", blocksPerFile)
	}

	// Run 1 — mmap readers (zero-copy block reads): contrast baseline. Block
	// data never enters the Go heap, so the peak is decode + encode only.
	outMmap := MustTempDir()
	defer os.RemoveAll(outMmap)
	fsMmap := &fakeFileStore{}
	defer fsMmap.Close()
	peakMmap, _ := measuredCompactFull(t, outMmap, files, fsMmap, "mmap-streaming")

	// Run 2 — pread readers: the production mode for network storage, and the
	// mode the review's Critical 2 is about. Every block read allocates.
	outPread := MustTempDir()
	defer os.RemoveAll(outPread)
	fsPread := &preadMemFileStore{}
	defer fsPread.Close()
	peakPread, outputsPread := measuredCompactFull(t, outPread, files, fsPread, "pread-streaming")

	t.Logf("input: N=%d files × P=%d pts × V=%dB strings (%d blocks/file), on-disk total %d bytes (%.1f MB)",
		numFiles, pointsPer, valSize, blocksPerFile, totalOnDisk, float64(totalOnDisk)/1e6)
	t.Logf("peak ratio pread/on-disk = %.2f×, mmap/on-disk = %.2f× (ceiling %d×)",
		float64(peakPread)/float64(totalOnDisk), float64(peakMmap)/float64(totalOnDisk), maxPeakPerInputByte)

	// Pread-engagement proof: pread reads every block into a heap buffer, so
	// its peak must exceed the mmap peak for the identical input. If this ever
	// fails the pread readers are not engaged and the hard bound is vacuous.
	if peakPread <= peakMmap {
		t.Fatalf("pread peak (%d bytes) is not greater than the mmap peak (%d bytes) — pread mode is not engaged, the memory bound would be measured vacuously",
			peakPread, peakMmap)
	}

	// HARD bound on the pread compaction: the whole-group gather + decode must
	// stay a small multiple of the input. A runaway that retains every decoded
	// value on top of the gather (or lets the gather grow with something other
	// than the group's data) blows past this ceiling.
	limit := uint64(maxPeakPerInputByte) * uint64(totalOnDisk)
	if peakPread > limit {
		t.Fatalf("memory unbounded: pread compaction peak %d bytes = %.2f× the %d-byte on-disk input (ceiling %d×) — the hot-key block gather or decode pass is retaining the whole group",
			peakPread, float64(peakPread)/float64(totalOnDisk), totalOnDisk, maxPeakPerInputByte)
	}
	// The mmap run gets the same data-relative ceiling (it should be far under
	// it — block reads are zero-copy there).
	if peakMmap > limit {
		t.Fatalf("memory unbounded: mmap compaction peak %d bytes = %.2f× the %d-byte on-disk input (ceiling %d×)",
			peakMmap, float64(peakMmap)/float64(totalOnDisk), totalOnDisk, maxPeakPerInputByte)
	}

	// Correctness guard so the measurement is not vacuous: the group's P unique
	// timestamps must all survive the dedup, each value still V bytes.
	if got := countHotKeyPoints(t, outputsPread, valSize); got != pointsPer {
		t.Fatalf("output point count for the hot key: got %d, want %d", got, pointsPer)
	}
}
