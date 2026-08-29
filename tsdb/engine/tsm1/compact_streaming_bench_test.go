package tsm1_test

import (
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// buildBenchmarkFiles constructs N TSM files suitable for compaction benchmarks:
// each file has M keys, each key has P points. Files overlap on keys so dedup is
// exercised. Returns the file paths (sorted).
func buildBenchmarkFiles(b testing.TB, dir string, N, M, P int) []string {
	b.Helper()
	files := make([]string, N)
	for i := 0; i < N; i++ {
		vals := make(map[string][]tsm1.Value, M)
		for k := 0; k < M; k++ {
			// Key appears in every file with overlapping timestamps so cross-file
			// dedup is exercised. Offset by i so files are not identical.
			key := fmt.Sprintf("cpu,host=h%04d#!~#value", k)
			points := make([]tsm1.Value, P)
			for p := 0; p < P; p++ {
				ts := int64(p) // same ts across files → dedup
				points[p] = tsm1.NewValue(ts, float64(i*1000+p))
			}
			vals[key] = points
		}
		files[i] = MustWriteTSM(dir, i+1, vals)
	}
	sort.Strings(files)
	return files
}

// openReadersB opens a fresh TSMReader per file (benchmark variant using testing.TB).
func openReadersB(tb testing.TB, files []string) []*tsm1.TSMReader {
	tb.Helper()
	readers := make([]*tsm1.TSMReader, len(files))
	for i, f := range files {
		readers[i] = MustOpenTSMReader(f)
	}
	return readers
}

// drainIterB drains a KeyIterator to completion, discarding output. Used by
// benchmarks to measure merge throughput without writeNewFiles I/O.
func drainIterB(b *testing.B, iter tsm1.KeyIterator) {
	b.Helper()
	for iter.Next() {
		if _, _, _, _, err := iter.Read(); err != nil {
			b.Fatal(err)
		}
	}
	if err := iter.Err(); err != nil {
		b.Fatal(err)
	}
	if err := iter.Close(); err != nil {
		b.Fatal(err)
	}
}

// BenchmarkKeyIterator_LegacyVsStreaming compares the legacy tsmBatchKeyIterator
// against the new streamingBatchKeyIterator on identical inputs across several
// file counts (N) and key/point dimensions. It reports ns/op, allocs/op, and
// B/op so both throughput and allocation differences are visible.
//
// Each op drains one full merge of N files. Readers are opened fresh per op
// (BlockIterator mutates reader state and Close closes the reader), so the
// per-op cost includes reader open — this is constant across both iterators,
// so the *difference* between legacy and streaming is the merge cost.
func BenchmarkKeyIterator_LegacyVsStreaming(b *testing.B) {
	cases := []struct {
		name string
		N, M, P int
	}{
		{"N4_M10_P1000", 4, 10, 1000},
		{"N16_M10_P1000", 16, 10, 1000},
		{"N4_M100_P1000", 4, 100, 1000},
		{"N16_M100_P1000", 16, 100, 1000},
		{"N8_M10_P10000", 8, 10, 10000},
	}

	for _, tc := range cases {
		tc := tc
		dir := MustTempDir()
		// Note: files intentionally NOT cleaned per-case (deferred to sub-bench
		// teardown) — but MustTempDir leaks across cases in a benchmark. Use a
		// single dir for all cases and clean at the end.
		files := buildBenchmarkFiles(b, dir, tc.N, tc.M, tc.P)

		b.Run(tc.name+"/legacy", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				readers := openReadersB(b, files)
				iter, err := tsm1.NewTSMBatchKeyIterator(1000, false, nil, files, readers...)
				if err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				drainIterB(b, iter)
			}
		})

		b.Run(tc.name+"/streaming", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				readers := openReadersB(b, files)
				iter, err := tsm1.NewStreamingBatchKeyIterator(1000, false, nil, files, readers...)
				if err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				drainIterB(b, iter)
			}
		})

		os.RemoveAll(dir)
	}
}

// BenchmarkKeyIterator_FastPath compares the fast=true path (blocks passed
// through without decode when non-overlapping) between legacy and streaming.
// Fast path is the common level-3 compaction mode.
func BenchmarkKeyIterator_FastPath(b *testing.B) {
	// Non-overlapping files so fast path is taken (no dedup decode).
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	const N, M, P = 8, 50, 1000
	files := make([]string, N)
	for i := 0; i < N; i++ {
		vals := make(map[string][]tsm1.Value, M)
		for k := 0; k < M; k++ {
			key := fmt.Sprintf("cpu,host=h%04d#!~#value", k)
			points := make([]tsm1.Value, P)
			for p := 0; p < P; p++ {
				// Distinct ts ranges per file → non-overlapping → fast path.
				points[p] = tsm1.NewValue(int64(i)*int64(P)+int64(p), float64(i*1000+p))
			}
			vals[key] = points
		}
		files[i] = MustWriteTSM(dir, i+1, vals)
	}
	sort.Strings(files)

	b.Run("legacy", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			readers := openReadersB(b, files)
			iter, _ := tsm1.NewTSMBatchKeyIterator(1000, true, nil, files, readers...)
			drainIterB(b, iter)
		}
	})
	b.Run("streaming", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			readers := openReadersB(b, files)
			iter, _ := tsm1.NewStreamingBatchKeyIterator(1000, true, nil, files, readers...)
			drainIterB(b, iter)
		}
	})
}
