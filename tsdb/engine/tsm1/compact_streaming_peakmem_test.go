package tsm1_test

import (
	"os"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"testing"

	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// samplePeakHeapInuse disables GC, runs fn, then returns the peak HeapInuse
// observed above a GC'd baseline. With GC disabled, HeapInuse grows monotonically
// to the true peak (no transient allocations get reclaimed mid-run), so a single
// post-run read captures the peak reliably — far more robust than periodic
// sampling, which misses sub-millisecond peaks on fast drains. GC is restored on
// return.
func samplePeakHeapInuse(b *testing.B, fn func()) uint64 {
	b.Helper()
	runtime.GC()
	var base runtime.MemStats
	runtime.ReadMemStats(&base)

	// Disable GC for the duration of fn so HeapInuse grows monotonically to peak.
	old := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(old)

	fn()

	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	if after.HeapInuse > base.HeapInuse {
		return after.HeapInuse - base.HeapInuse
	}
	return 0
}

// drainAndMeasurePeak opens fresh readers, builds the iterator, and drains it,
// all inside the peak-measurement window so reader-open heap growth (index
// decode, offsets tables) is captured alongside the merge. Returns the peak
// HeapInuse delta above a GC'd baseline.
func drainAndMeasurePeak(b *testing.B, files []string, streaming bool) uint64 {
	b.Helper()
	return samplePeakHeapInuse(b, func() {
		readers := openReadersB(b, files)
		var iter tsm1.KeyIterator
		var err error
		if streaming {
			iter, err = tsm1.NewStreamingBatchKeyIterator(1000, false, nil, files, readers...)
		} else {
			iter, err = tsm1.NewTSMBatchKeyIterator(1000, false, nil, files, readers...)
		}
		if err != nil {
			b.Fatal(err)
		}
		drainIterB(b, iter)
	})
}

// BenchmarkKeyIterator_PeakHeap_LegacyVsStreaming measures the peak Go-heap
// HeapInuse (above a GC'd baseline) during a single full merge of N files,
// comparing legacy tsmBatchKeyIterator vs streamingBatchKeyIterator. Both
// gather all of a key's blocks into k.blocks before the first merge (per the
// review's finding that per-key peak is equivalent), so the peak Go-heap
// should be comparable — this benchmark confirms that empirically and checks
// that neither iterator's peak grows disproportionately with N.
//
// NOTE: this measures Go-heap only. mmap RSS (the larger resident-memory
// component, pinned by open TSMReaders) is NOT visible to runtime.MemStats;
// the RSS difference between a single-group (N readers) and rolling (K readers)
// compaction is bounded by Phase B's planner, not the iterator, and is covered
// by TestStreamingCompaction_MemoryBoundedByK.
func BenchmarkKeyIterator_PeakHeap_LegacyVsStreaming(b *testing.B) {
	cases := []struct {
		name string
		N, M, P int
	}{
		{"N4_M10_P1000", 4, 10, 1000},
		{"N16_M10_P1000", 16, 10, 1000},
		{"N32_M10_P1000", 32, 10, 1000},
	}

	for _, tc := range cases {
		tc := tc
		dir := MustTempDir()
		files := buildBenchmarkFiles(b, dir, tc.N, tc.M, tc.P)

		var legacyPeak, streamingPeak uint64
		b.Run(tc.name, func(b *testing.B) {
			// b.N iterations, but we only need one representative measurement per
			// iterator (peak is not cumulative). Use b.N=1 semantics by running
			// once and reporting the peak via ReportMetric.
			b.ResetTimer()
			legacyPeak = drainAndMeasurePeak(b, files, false)
			b.StopTimer()
			b.ReportMetric(float64(legacyPeak), "legacyPeakBytes")
			b.StartTimer()
		})
		b.Run(tc.name+"_streaming", func(b *testing.B) {
			b.ResetTimer()
			streamingPeak = drainAndMeasurePeak(b, files, true)
			b.StopTimer()
			b.ReportMetric(float64(streamingPeak), "streamingPeakBytes")
			b.StartTimer()
		})
		b.Logf("%s: legacy peak=%d bytes, streaming peak=%d bytes (ratio %.2fx)",
			tc.name, legacyPeak, streamingPeak,
			float64(streamingPeak)/float64(maxU64(legacyPeak, 1)))
		os.RemoveAll(dir)
	}
}

// buildSingleKeyManyBlockFiles constructs N TSM files each holding ONE key with
// P points in a NON-overlapping time range (file i covers ts [i*P, (i+1)*P)).
// With DefaultMaxPointsPerBlock=1000, file i contains ceil(P/1000) blocks, so
// the merge gathers N*ceil(P/1000) blocks for that single key — the per-key
// peak-amplification scenario. Non-overlapping timestamps exercise the
// fast/combine path (no dedup decode of every value), isolating the block-gather
// and chunk cost.
func buildSingleKeyManyBlockFiles(tb testing.TB, dir string, N, P int) []string {
	tb.Helper()
	const key = "cpu,host=big#!~#value"
	files := make([]string, N)
	for i := 0; i < N; i++ {
		points := make([]tsm1.Value, P)
		base := int64(i) * int64(P)
		for p := 0; p < P; p++ {
			points[p] = tsm1.NewValue(base+int64(p), float64(base+int64(p)))
		}
		files[i] = MustWriteTSM(dir, i+1, map[string][]tsm1.Value{key: points})
	}
	sort.Strings(files)
	return files
}

// BenchmarkKeyIterator_SingleKeyManyBlocks measures both throughput (ns/op) and
// peak HeapInuse for merging a SINGLE key whose blocks span many files — the
// worst case for per-key block gathering. Reports legacyPeakBytes /
// streamingPeakBytes via ReportMetric and logs the ratio.
//
// Scenarios scale P (points per file → blocks per file) at fixed N=16, and N
// (files) at fixed P=50000, to show how peak scales with block count per key.
func BenchmarkKeyIterator_SingleKeyManyBlocks(b *testing.B) {
	scenarios := []struct {
		name       string
		N, P       int
	}{
		// Scale points-per-file (blocks per file) at N=16:
		//   P=10000 → 10 blocks/file → 160 blocks for the key
		//   P=50000 → 50 blocks/file → 800 blocks for the key
		//   P=200000 → 200 blocks/file → 3200 blocks for the key
		{"N16_P10000", 16, 10000},
		{"N16_P50000", 16, 50000},
		{"N16_P200000", 16, 200000},
		// Scale file count at P=50000 (50 blocks/file):
		//   N=4 → 200 blocks, N=32 → 1600 blocks, N=64 → 3200 blocks
		{"N4_P50000", 4, 50000},
		{"N32_P50000", 32, 50000},
		{"N64_P50000", 64, 50000},
	}

	for _, sc := range scenarios {
		sc := sc
		dir := MustTempDir()
		files := buildSingleKeyManyBlockFiles(b, dir, sc.N, sc.P)

		var legacyPeak, streamingPeak uint64
		b.Run(sc.name+"/legacy", func(b *testing.B) {
			b.ReportAllocs()
			var peak uint64
			for i := 0; i < b.N; i++ {
				if i == 0 {
					peak = drainAndMeasurePeak(b, files, false)
				} else {
					readers := openReadersB(b, files)
					iter, _ := tsm1.NewTSMBatchKeyIterator(1000, false, nil, files, readers...)
					drainIterB(b, iter)
				}
			}
			legacyPeak = peak
			b.ReportMetric(float64(peak), "legacyPeakBytes")
		})
		b.Run(sc.name+"/streaming", func(b *testing.B) {
			b.ReportAllocs()
			var peak uint64
			for i := 0; i < b.N; i++ {
				if i == 0 {
					peak = drainAndMeasurePeak(b, files, true)
				} else {
					readers := openReadersB(b, files)
					iter, _ := tsm1.NewStreamingBatchKeyIterator(1000, false, nil, files, readers...)
					drainIterB(b, iter)
				}
			}
			streamingPeak = peak
			b.ReportMetric(float64(peak), "streamingPeakBytes")
		})
		b.Logf("%s: legacy peak=%d bytes, streaming peak=%d bytes (ratio %.2fx)",
			sc.name, legacyPeak, streamingPeak,
			float64(streamingPeak)/float64(maxU64(legacyPeak, 1)))
		os.RemoveAll(dir)
	}
}

// buildSingleKeyStringBlocks constructs N TSM files each holding ONE string-typed
// key with P points, each point a string of V bytes. Timestamps OVERLAP across
// files (same ts range in every file) so the dedup merge path is taken — every
// block is decoded. String values are heap-allocated on decode (snappy
// decompression yields fresh strings), so this stresses the merge buffer far
// harder than float/int: a 1000-point block of 1KB strings is ~1MB of live
// decoded string data, and dedup decodes all of them before merging.
func buildSingleKeyStringBlocks(tb testing.TB, dir string, N, P, V int) []string {
	tb.Helper()
	const key = "log,host=big#!~#message"
	// Pre-build a single V-byte string; each point reuses it (Write encodes, decode
	// allocates fresh). Distinct content per point would inflate the file; reuse
	// keeps the on-disk size small while decode still allocates V bytes per point.
	suffix := strings.Repeat("x", V)
	files := make([]string, N)
	for i := 0; i < N; i++ {
		points := make([]tsm1.Value, P)
		for p := 0; p < P; p++ {
			// Same ts range in every file → overlapsTimeRange → dedup path decodes
			// all blocks.
			points[p] = tsm1.NewValue(int64(p), suffix)
		}
		files[i] = MustWriteTSM(dir, i+1, map[string][]tsm1.Value{key: points})
	}
	sort.Strings(files)
	return files
}

// BenchmarkKeyIterator_SingleKeyStringBlocks measures peak HeapInuse and
// throughput for merging a single STRING-typed key whose blocks span many
// files with large string values. String decode allocates fresh heap per value,
// so this is the heaviest merge-buffer scenario.
//
// Scenarios scale V (bytes per string) at fixed N=16,P=2000, and P at fixed
// N=16,V=1024, to show how peak scales with decoded string volume.
func BenchmarkKeyIterator_SingleKeyStringBlocks(b *testing.B) {
	scenarios := []struct {
		name   string
		N, P, V int
	}{
		// Scale string size at N=16, P=2000 (2 blocks/file → 32 blocks/key):
		//   V=128   → 1000-pt block = 128KB decoded
		//   V=1024  → 1000-pt block = 1MB decoded
		//   V=8192  → 1000-pt block = 8MB decoded
		{"N16_P2000_V128", 16, 2000, 128},
		{"N16_P2000_V1024", 16, 2000, 1024},
		{"N16_P2000_V8192", 16, 2000, 8192},
		// Scale points at N=16, V=1024 (1KB strings):
		//   P=1000 → 1 block/file → 16 blocks/key
		//   P=10000 → 10 blocks/file → 160 blocks/key
		{"N16_P1000_V1024", 16, 1000, 1024},
		{"N16_P10000_V1024", 16, 10000, 1024},
	}

	for _, sc := range scenarios {
		sc := sc
		dir := MustTempDir()
		files := buildSingleKeyStringBlocks(b, dir, sc.N, sc.P, sc.V)

		var legacyPeak, streamingPeak uint64
		b.Run(sc.name+"/legacy", func(b *testing.B) {
			b.ReportAllocs()
			var peak uint64
			for i := 0; i < b.N; i++ {
				if i == 0 {
					peak = drainAndMeasurePeak(b, files, false)
				} else {
					readers := openReadersB(b, files)
					iter, _ := tsm1.NewTSMBatchKeyIterator(1000, false, nil, files, readers...)
					drainIterB(b, iter)
				}
			}
			legacyPeak = peak
			b.ReportMetric(float64(peak), "legacyPeakBytes")
		})
		b.Run(sc.name+"/streaming", func(b *testing.B) {
			b.ReportAllocs()
			var peak uint64
			for i := 0; i < b.N; i++ {
				if i == 0 {
					peak = drainAndMeasurePeak(b, files, true)
				} else {
					readers := openReadersB(b, files)
					iter, _ := tsm1.NewStreamingBatchKeyIterator(1000, false, nil, files, readers...)
					drainIterB(b, iter)
				}
			}
			streamingPeak = peak
			b.ReportMetric(float64(peak), "streamingPeakBytes")
		})
		b.Logf("%s: legacy peak=%d bytes, streaming peak=%d bytes (ratio %.2fx)",
			sc.name, legacyPeak, streamingPeak,
			float64(streamingPeak)/float64(maxU64(legacyPeak, 1)))
		os.RemoveAll(dir)
	}
}
