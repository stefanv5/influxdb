package tsm1_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

func maxU64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// preadRollingFileStore is a mutable FileStore for rolling compaction rounds
// that opens every TSM reader in PREAD mode (WithMaxMmapFileSize(0)) so that
// block reads allocate heap buffers and are visible to runtime.MemStats — see
// preadMemFileStore. It mirrors the rollingFileStore harness used by the e2e
// test but is deliberately self-contained so this test's measurement harness
// does not depend on that file. Stats() reflects the current on-disk file set;
// the test mutates files after each round so the planner re-plans against the
// post-merge state. Single-threaded test usage — no locking.
type preadRollingFileStore struct {
	dir     string
	files   []string
	lastMod time.Time
	maxGen  int
	opened  []*tsm1.TSMReader
}

func newPreadRollingFileStore(dir string, initial []string) *preadRollingFileStore {
	maxGen := 0
	for _, f := range initial {
		if g, _, err := tsm1.DefaultParseFileName(filepath.Base(f)); err == nil && g > maxGen {
			maxGen = g
		}
	}
	return &preadRollingFileStore{
		dir:     dir,
		files:   append([]string(nil), initial...),
		lastMod: time.Now(),
		maxGen:  maxGen,
	}
}

func (r *preadRollingFileStore) Stats() []tsm1.FileStat {
	stats := make([]tsm1.FileStat, len(r.files))
	for i, f := range r.files {
		fi, _ := os.Stat(f)
		var sz uint32
		if fi != nil {
			sz = uint32(fi.Size())
		}
		stats[i] = tsm1.FileStat{Path: f, Size: sz, LastModified: r.lastMod.UnixNano()}
	}
	return stats
}

func (r *preadRollingFileStore) LastModified() time.Time { return r.lastMod }

func (r *preadRollingFileStore) BlockCount(path string, idx int) int {
	return tsdb.DefaultMaxPointsPerBlock
}

func (r *preadRollingFileStore) ParseFileName(path string) (int, int, error) {
	return tsm1.DefaultParseFileName(path)
}

func (r *preadRollingFileStore) NextGeneration() int { return r.maxGen + 1 }

func (r *preadRollingFileStore) TSMReader(path string) *tsm1.TSMReader {
	f, err := os.Open(path)
	if err != nil {
		panic(fmt.Sprintf("open tsm file: %v", err))
	}
	// WithMaxMmapFileSize(0) = always pread, never mmap block data.
	reader, err := tsm1.NewTSMReader(f, tsm1.WithMaxMmapFileSize(0))
	if err != nil {
		panic(fmt.Sprintf("new pread tsm reader: %v", err))
	}
	r.opened = append(r.opened, reader)
	reader.Ref()
	return reader
}

// replace removes the consumed input files from the store and adds the newly
// produced output files, bumping lastMod so findGenerations' cache is invalidated.
func (r *preadRollingFileStore) replace(consumed []string, produced []string) {
	consumedSet := make(map[string]struct{}, len(consumed))
	for _, f := range consumed {
		consumedSet[f] = struct{}{}
	}
	var next []string
	for _, f := range r.files {
		if _, ok := consumedSet[f]; !ok {
			next = append(next, f)
		}
	}
	next = append(next, produced...)
	sort.Strings(next)
	r.files = next
	for _, f := range produced {
		if g, _, err := tsm1.DefaultParseFileName(filepath.Base(f)); err == nil && g > r.maxGen {
			r.maxGen = g
		}
	}
	r.lastMod = time.Now()
}

func (r *preadRollingFileStore) closeAll() {
	for _, rd := range r.opened {
		rd.Close()
	}
	r.opened = nil
}

// runRollingHotKeyCompaction drives a real streaming Compactor through rolling
// full-compaction rounds over N files (cap K) that all carry ONE hot key of
// large string values, measuring each individual compaction's peak Go heap in
// its own GC-disabled window (see measurePeakHeapInuse — with the GC disabled
// HeapInuse grows monotonically, so a single post-run read is the true peak;
// no transient allocation can be collected away before it is observed).
//
// Every reader is opened in pread mode, so each round's gather of the hot
// key's blocks across the group's K readers is fully visible to MemStats.
//
// Per compaction it asserts:
//   - structurally, the planned group holds ≤K files (the K-pin contract) and
//     more than one file (progress);
//   - the peak heap stays under 4× that group's on-disk input bytes, i.e. the
//     gather is bounded by the group, not by N.
//
// It returns the per-compaction peaks and group input sizes for the K-pin
// scaling assertion in the caller.
func runRollingHotKeyCompaction(t *testing.T, N, K int) (peaks []uint64, inputs []int64) {
	t.Helper()
	const (
		pointsPer = 1000 // one 1000-pt block per file (V=4KB → 4MB blocks)
		valSize   = 4096
	)

	dir := MustTempDir()
	t.Cleanup(func() { os.RemoveAll(dir) })

	initial, _ := buildHotKeyStringFiles(t, dir, N, pointsPer, valSize)

	fs := newPreadRollingFileStore(dir, initial)
	t.Cleanup(fs.closeAll)

	cp := tsm1.NewDefaultPlanner(fs, time.Nanosecond) // cold gate always satisfied
	cp.SetMaxFullCompactionFiles(K)

	compactor := tsm1.NewCompactor()
	compactor.Dir = dir
	compactor.FileStore = fs
	compactor.UseStreamingIterator = true
	compactor.Open()
	t.Cleanup(compactor.DisableCompactions)

	compactions := 0
	for compactions < 30 { // safety bound; ceil((N-1)/(K-1)) rounds expected
		plan := cp.Plan(time.Now().Add(-time.Second))
		if len(plan) == 0 {
			break // converged
		}
		for _, group := range plan {
			compactions++
			if len(group) > K {
				t.Fatalf("compaction %d: planned group of %d files exceeds the K=%d cap: %v", compactions, len(group), K, group)
			}
			if len(group) <= 1 {
				t.Fatalf("compaction %d: planned group of %d file(s) makes no progress: %v", compactions, len(group), group)
			}

			var inputBytes int64
			for _, f := range group {
				if fi, err := os.Stat(f); err == nil {
					inputBytes += fi.Size()
				}
			}

			consumed := append([]string(nil), group...)
			var out []string
			peak := measurePeakHeapInuse(t, func() error {
				var err error
				out, err = compactor.CompactFull(consumed)
				return err
			})
			peaks = append(peaks, peak)
			inputs = append(inputs, inputBytes)

			// Per-group hard bound: the gather + decode must stay a small
			// multiple of the group's own data. A runaway that opens more than
			// the group's readers, or retains more than one pass over the data,
			// blows past it.
			const maxPeakPerInputByte = 4
			if peak > uint64(maxPeakPerInputByte)*uint64(inputBytes) {
				t.Fatalf("compaction %d: peak heap %d bytes = %.2f× the group's %d-byte on-disk input (ceiling %d×) — the hot-key gather is not bounded by the group",
					compactions, peak, float64(peak)/float64(inputBytes), inputBytes, maxPeakPerInputByte)
			}

			for _, f := range consumed {
				os.Remove(f)
			}
			fs.replace(consumed, out)
		}
		cp.Release(plan)
	}
	if compactions == 0 {
		t.Fatalf("no compactions ran; planner returned empty immediately")
	}

	// Convergence: the shard must end as a single TSM file holding the deduped
	// point set (pointsPer unique timestamps for the hot key), so the measured
	// rounds did real work and the measurement is not vacuous.
	if len(fs.files) != 1 {
		t.Fatalf("expected 1 file after convergence, got %d: %v", len(fs.files), fs.files)
	}
	if got := readKeyPointsFromFile(t, fs.files[0], valSize); got != pointsPer {
		t.Fatalf("final file point count for the hot key: got %d, want %d", got, pointsPer)
	}

	t.Logf("N=%d K=%d: %d compaction(s), per-compaction peaks %v bytes over group inputs %v bytes",
		N, K, compactions, peaks, inputs)
	return peaks, inputs
}

// readKeyPointsFromFile opens one TSM file and returns the number of points
// stored for the hot key, checking each value's length.
func readKeyPointsFromFile(t *testing.T, path string, valLen int) int {
	t.Helper()
	r := MustOpenTSMReader(path)
	defer r.Close()
	vals, err := r.ReadAll([]byte(hotKeyString))
	if err != nil {
		t.Fatalf("ReadAll(%s): %v", path, err)
	}
	for _, v := range vals {
		s, ok := v.Value().(string)
		if !ok || len(s) != valLen {
			t.Fatalf("value at ts %d in %s has wrong shape (len %d, ok %v)", v.UnixNano(), path, len(s), ok)
		}
	}
	return len(vals)
}

// TestStreamingCompaction_MemoryBoundedByK verifies the K-pin property of the
// rolling full compaction on a single hot key of large string values: the peak
// Go heap of every rolling round is bounded by the round's K-file group and
// does NOT grow with N, the total input file count.
//
// Methodology (the review's Important 7 / Critical 2 test demands):
//   - pread readers everywhere (block reads allocate heap buffers, so the
//     gather is measurable — the previous version measured mmap aliases);
//   - GC disabled per compaction, so the single post-run MemStats read IS the
//     true peak (no transient peak GC'd away before the one sample);
//   - single hot key across every file, 4KB incompressible strings, 1000-pt
//     blocks (the per-key gather worst case, not 500 floats);
//   - HARD assertions: per-group peak < 4× the group's on-disk input (in the
//     runner), and the N=4K rolling run's worst round within 2× of the N=K
//     reference — a round that opened all 4K readers instead of K measures
//     ~3× the reference (calibrated) and fails.
//
// There is deliberately no Windows skip: the measurement is pure Go-heap and
// pread readers hold no mmap handles (the mmap handle/release timing was the
// source of the old Windows flakiness), so both runs are deterministic here.
func TestStreamingCompaction_MemoryBoundedByK(t *testing.T) {
	if testing.Short() {
		t.Skip("memory bound test is slow; run without -short")
	}
	const K = 4

	// N=K: a single compaction of exactly K files — the per-group peak reference.
	smallPeaks, _ := runRollingHotKeyCompaction(t, K, K)

	// N=4K: multiple rolling rounds; every round must stay pinned at K readers.
	largePeaks, _ := runRollingHotKeyCompaction(t, 4*K, K)

	var maxSmall, maxLarge uint64
	for _, p := range smallPeaks {
		maxSmall = maxU64(maxSmall, p)
	}
	for _, p := range largePeaks {
		maxLarge = maxU64(maxLarge, p)
	}

	// K-pin scaling assertion: the worst N=4K round handles a K-file group of
	// the same shape as the N=K run's single group, so its peak must be within
	// a bounded factor of the reference. The legitimate ratio is ~1.0 (measured
	// 1.00x, deterministic across rounds to <0.1%); a round that opened all 4K
	// readers instead of K measures ~3× the reference (calibrated), so a 2×
	// bound catches it while leaving a 2× safety factor over normal behavior.
	// (The planner returning >K files in one group is caught structurally in
	// the runner — this assertion covers heap-scale regressions.)
	const maxKPinRatio = 2.0
	if float64(maxLarge) > maxKPinRatio*float64(maxU64(maxSmall, 1)) {
		t.Fatalf("K-pin broken: worst N=%d round peak %d bytes exceeds %.1f× the N=%d reference peak %d bytes",
			4*K, maxLarge, maxKPinRatio, K, maxSmall)
	}

	t.Logf("K=%d: N=%d peak=%d bytes, N=%d worst-round peak=%d bytes (ratio %.2fx, bound %.1fx)",
		K, K, maxSmall, 4*K, maxLarge,
		float64(maxLarge)/float64(maxU64(maxSmall, 1)), maxKPinRatio)
}
