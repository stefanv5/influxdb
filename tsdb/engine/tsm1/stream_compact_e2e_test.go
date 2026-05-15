package tsm1_test

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// TestStreamingCompaction_LargeFiles tests streaming compaction with large TSM files.
func TestStreamingCompaction_LargeFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large file test in short mode")
	}

	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// Generate ~500MB of data per file (3 files = ~1.5GB total)
	pointsPerFile := 50_000_000
	files := 3

	t.Logf("Generating %d files with ~%d points each", files, pointsPerFile)

	tsmFiles := make([]string, files)
	for i := 0; i < files; i++ {
		f, err := os.Create(filepath.Join(dir, tsm1.DefaultFormatFileName(i+1, 1)+".tsm"))
		if err != nil {
			t.Fatalf("failed to create file: %v", err)
		}

		w, err := tsm1.NewTSMWriter(f)
		if err != nil {
			t.Fatalf("failed to create TSM writer: %v", err)
		}

		pointsPerKey := pointsPerFile / 10
		for keyIdx := 0; keyIdx < 10; keyIdx++ {
			key := fmt.Sprintf("cpu,host=%02d#!~#value", keyIdx)
			startTime := int64(i) * int64(pointsPerFile)

			values := make([]tsm1.Value, 0, pointsPerKey)
			for j := 0; j < pointsPerKey; j++ {
				ts := startTime + int64(j)
				values = append(values, tsm1.NewValue(ts, float64(j%1000)))
			}

			if err := w.Write([]byte(key), values); err != nil {
				t.Fatalf("failed to write values: %v", err)
			}
		}

		if err := w.WriteIndex(); err != nil {
			t.Fatalf("failed to write index: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("failed to close writer: %v", err)
		}

		tsmFiles[i] = f.Name()
		stat, _ := os.Stat(f.Name())
		t.Logf("File %d: %s (%.2f MB)", i+1, f.Name(), float64(stat.Size())/1024/1024)
	}

	// Run full compaction (triggers streaming path)
	compactor := tsm1.NewCompactor()
	compactor.Dir = dir
	fs := &fakeFileStore{}
	compactor.FileStore = fs
	defer fs.Close()
	compactor.Open()

	start := time.Now()
	result, err := compactor.CompactFull(tsmFiles)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("compact failed: %v", err)
	}

	t.Logf("Compaction completed in %v, produced %d files", elapsed, len(result))

	if len(result) == 0 {
		t.Fatal("expected at least one output file")
	}

	// Verify
	r := MustOpenTSMReader(result[0])
	defer r.Close()

	// Count total points across all keys
	totalPoints := 0
	keyCount := r.KeyCount()
	for i := 0; i < keyCount; i++ {
		key, _ := r.KeyAt(i)
		values, err := r.ReadAll(key)
		if err != nil {
			t.Fatalf("failed to read key: %v", err)
		}
		totalPoints += len(values)
	}

	expectedPoints := files * pointsPerFile
	if totalPoints != expectedPoints {
		t.Errorf("expected %d points, got %d", expectedPoints, totalPoints)
	}

	t.Logf("Total points in compacted file: %d", totalPoints)
}

// TestStreamingCompaction_LargeFilesWithOverlap tests streaming compaction with overlapping time ranges.
func TestStreamingCompaction_LargeFilesWithOverlap(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large file test in short mode")
	}

	dir := MustTempDir()
	defer os.RemoveAll(dir)

	overlapStart := 50_000_000
	rangeSize := 50_000_000

	tsmFiles := make([]string, 3)
	for i, start := range []int{0, overlapStart, overlapStart * 2} {
		f, err := os.Create(filepath.Join(dir, tsm1.DefaultFormatFileName(i+1, 1)+".tsm"))
		if err != nil {
			t.Fatalf("failed to create file: %v", err)
		}

		w, err := tsm1.NewTSMWriter(f)
		if err != nil {
			t.Fatalf("failed to create TSM writer: %v", err)
		}

		key := "cpu,host=A#!~#value"
		values := make([]tsm1.Value, 0, rangeSize)
		for j := 0; j < rangeSize; j++ {
			ts := int64(start + j)
			values = append(values, tsm1.NewValue(ts, float64(j)))
		}

		if err := w.Write([]byte(key), values); err != nil {
			t.Fatalf("failed to write values: %v", err)
		}

		if err := w.WriteIndex(); err != nil {
			t.Fatalf("failed to write index: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("failed to close writer: %v", err)
		}

		tsmFiles[i] = f.Name()
	}

	compactor := tsm1.NewCompactor()
	compactor.Dir = dir
	fs := &fakeFileStore{}
	compactor.FileStore = fs
	defer fs.Close()
	compactor.Open()

	start := time.Now()
	result, err := compactor.CompactFull(tsmFiles)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("compact failed: %v", err)
	}

	t.Logf("Compaction completed in %v", elapsed)

	var allValues []tsm1.Value
	for _, f := range result {
		r := MustOpenTSMReader(f)
		values, err := r.ReadAll([]byte("cpu,host=A#!~#value"))
		if err != nil {
			r.Close()
			t.Fatalf("failed to read values from %s: %v", f, err)
		}
		allValues = append(allValues, values...)
		r.Close()
	}

	expectedPoints := 150_000_000
	if len(allValues) != expectedPoints {
		t.Errorf("expected %d points after dedup, got %d", expectedPoints, len(allValues))
	}

	for i := 1; i < len(allValues); i++ {
		if allValues[i].UnixNano() <= allValues[i-1].UnixNano() {
			t.Errorf("values not properly deduped at index %d", i)
			break
		}
	}

	t.Logf("Unique points after compaction: %d", len(allValues))
}

// TestStreamingCompaction_ManyKeys tests with many unique keys.
func TestStreamingCompaction_ManyKeys(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large file test in short mode")
	}

	dir := MustTempDir()
	defer os.RemoveAll(dir)

	numKeys := 10000
	pointsPerKey := 1000

	f, err := os.Create(filepath.Join(dir, tsm1.DefaultFormatFileName(1, 1)+".tsm"))
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	w, err := tsm1.NewTSMWriter(f)
	if err != nil {
		t.Fatalf("failed to create TSM writer: %v", err)
	}

	for keyIdx := 0; keyIdx < numKeys; keyIdx++ {
		key := fmt.Sprintf("cpu,host=%05d#!~#value", keyIdx)
		values := make([]tsm1.Value, pointsPerKey)
		for j := 0; j < pointsPerKey; j++ {
			values[j] = tsm1.NewValue(int64(j), float64(j%100))
		}

		if err := w.Write([]byte(key), values); err != nil {
			t.Fatalf("failed to write values for key %s: %v", key, err)
		}
	}

	if err := w.WriteIndex(); err != nil {
		t.Fatalf("failed to write index: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	compactor := tsm1.NewCompactor()
	compactor.Dir = dir
	fs := &fakeFileStore{}
	compactor.FileStore = fs
	defer fs.Close()
	compactor.Open()

	start := time.Now()
	result, err := compactor.CompactFull([]string{f.Name()})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("compact failed: %v", err)
	}

	t.Logf("Compaction completed in %v", elapsed)

	r := MustOpenTSMReader(result[0])
	defer r.Close()

	if r.KeyCount() != numKeys {
		t.Errorf("expected %d keys, got %d", numKeys, r.KeyCount())
	}

	totalPoints := 0
	keyCount := r.KeyCount()
	for i := 0; i < keyCount; i++ {
		key, _ := r.KeyAt(i)
		values, err := r.ReadAll(key)
		if err != nil {
			t.Fatalf("failed to read key: %v", err)
		}
		totalPoints += len(values)
	}

	if totalPoints != numKeys*pointsPerKey {
		t.Errorf("expected %d points, got %d", numKeys*pointsPerKey, totalPoints)
	}

	t.Logf("Keys: %d, Points: %d", r.KeyCount(), totalPoints)
}

// TestStreamingCompaction_Tombstones tests compaction with tombstones.
func TestStreamingCompaction_Tombstones(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large file test in short mode")
	}

	dir := MustTempDir()
	defer os.RemoveAll(dir)

	tsmFiles := make([]string, 3)
	for i := 0; i < 3; i++ {
		f, err := os.Create(filepath.Join(dir, tsm1.DefaultFormatFileName(i+1, 1)+".tsm"))
		if err != nil {
			t.Fatalf("failed to create file: %v", err)
		}

		w, err := tsm1.NewTSMWriter(f)
		if err != nil {
			t.Fatalf("failed to create TSM writer: %v", err)
		}

		for keyIdx := 0; keyIdx < 3; keyIdx++ {
			key := fmt.Sprintf("cpu,host=%02d#!~#value", keyIdx)
			startTime := int64(i * 1000)

			values := make([]tsm1.Value, 1000)
			for j := 0; j < 1000; j++ {
				ts := startTime + int64(j)
				values[j] = tsm1.NewValue(ts, float64(j))
			}

			if err := w.Write([]byte(key), values); err != nil {
				t.Fatalf("failed to write values: %v", err)
			}
		}

		if err := w.WriteIndex(); err != nil {
			t.Fatalf("failed to write index: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("failed to close writer: %v", err)
		}

		tsmFiles[i] = f.Name()
	}

	// Add tombstone to the second file for key 1 (delete half of its data)
	ts := tsm1.NewTombstoner(tsmFiles[1], nil)
	ts.AddRange([][]byte{[]byte("cpu,host=01#!~#value")}, int64(1200), int64(1499))
	if err := ts.Flush(); err != nil {
		t.Fatalf("failed to write tombstones: %v", err)
	}

	compactor := tsm1.NewCompactor()
	compactor.Dir = dir
	fs := &fakeFileStore{}
	compactor.FileStore = fs
	defer fs.Close()
	compactor.Open()

	result, err := compactor.CompactFull(tsmFiles)
	if err != nil {
		t.Fatalf("compact failed: %v", err)
	}

	r := MustOpenTSMReader(result[0])
	defer r.Close()

	key1Values, err := r.ReadAll([]byte("cpu,host=01#!~#value"))
	if err != nil {
		t.Fatalf("failed to read key 1: %v", err)
	}

	// File 0: 0-999 (1000 points), File 1: 1000-1999 tombstoned 1200-1499 (700 remaining),
	// File 2: 2000-2999 (1000 points) = 2700 total
	expectedKey1Points := 2700
	if len(key1Values) != expectedKey1Points {
		t.Errorf("expected %d points for key 1, got %d", expectedKey1Points, len(key1Values))
	}

	t.Logf("Key 1 points after tombstone compaction: %d", len(key1Values))
}

// TestStreamingCompaction_AllValueTypes tests with all value types.
func TestStreamingCompaction_AllValueTypes(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	f, err := os.Create(filepath.Join(dir, tsm1.DefaultFormatFileName(1, 1)+".tsm"))
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	w, err := tsm1.NewTSMWriter(f)
	if err != nil {
		t.Fatalf("failed to create TSM writer: %v", err)
	}

	// Keys must be written in sorted order (lexicographic)
	// Each key gets unique timestamps to avoid type mixing issues
	if err := w.Write([]byte("bool#!~#value"), []tsm1.Value{
		tsm1.NewBooleanValue(1000, true),
		tsm1.NewBooleanValue(1001, false),
	}); err != nil {
		t.Fatalf("failed to write bool values: %v", err)
	}

	if err := w.Write([]byte("float#!~#value"), []tsm1.Value{
		tsm1.NewValue(2000, 1.1),
		tsm1.NewValue(2001, 2.2),
	}); err != nil {
		t.Fatalf("failed to write float values: %v", err)
	}

	if err := w.Write([]byte("int#!~#value"), []tsm1.Value{
		tsm1.NewIntegerValue(3000, 100),
		tsm1.NewIntegerValue(3001, 200),
	}); err != nil {
		t.Fatalf("failed to write int values: %v", err)
	}

	if err := w.Write([]byte("str#!~#value"), []tsm1.Value{
		tsm1.NewStringValue(4000, "hello"),
		tsm1.NewStringValue(4001, "world"),
	}); err != nil {
		t.Fatalf("failed to write string values: %v", err)
	}

	if err := w.Write([]byte("uint#!~#value"), []tsm1.Value{
		tsm1.NewUnsignedValue(5000, 100),
		tsm1.NewUnsignedValue(5001, 200),
	}); err != nil {
		t.Fatalf("failed to write uint values: %v", err)
	}

	if err := w.WriteIndex(); err != nil {
		t.Fatalf("failed to write index: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	compactor := tsm1.NewCompactor()
	compactor.Dir = dir
	fs := &fakeFileStore{}
	compactor.FileStore = fs
	defer fs.Close()
	compactor.Open()

	result, err := compactor.CompactFull([]string{f.Name()})
	if err != nil {
		t.Fatalf("compact failed: %v", err)
	}

	r := MustOpenTSMReader(result[0])
	defer r.Close()

	for _, tc := range []struct {
		key      string
		expected int
	}{
		{"bool#!~#value", 2},
		{"float#!~#value", 2},
		{"int#!~#value", 2},
		{"str#!~#value", 2},
		{"uint#!~#value", 2},
	} {
		values, err := r.ReadAll([]byte(tc.key))
		if err != nil {
			t.Fatalf("failed to read %s: %v", tc.key, err)
		}
		if len(values) != tc.expected {
			t.Errorf("%s: expected %d points, got %d", tc.key, tc.expected, len(values))
		}
	}
}

// TestStreamingCompaction_DedupByTimestamp tests dedup of same timestamps across files.
func TestStreamingCompaction_DedupByTimestamp(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	tsmFiles := make([]string, 3)
	for i := 0; i < 3; i++ {
		f, err := os.Create(filepath.Join(dir, tsm1.DefaultFormatFileName(int(i+1), 1)+".tsm"))
		if err != nil {
			t.Fatalf("failed to create file: %v", err)
		}

		w, err := tsm1.NewTSMWriter(f)
		if err != nil {
			t.Fatalf("failed to create TSM writer: %v", err)
		}

		for ts := int64(0); ts < 1000; ts++ {
			value := float64(i*1000 + int(ts))
			if err := w.Write([]byte("cpu,host=A#!~#value"), []tsm1.Value{tsm1.NewValue(ts, value)}); err != nil {
				t.Fatalf("failed to write value: %v", err)
			}
		}

		if err := w.WriteIndex(); err != nil {
			t.Fatalf("failed to write index: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("failed to close writer: %v", err)
		}

		tsmFiles[i] = f.Name()
	}

	compactor := tsm1.NewCompactor()
	compactor.Dir = dir
	fs := &fakeFileStore{}
	compactor.FileStore = fs
	defer fs.Close()
	compactor.Open()

	result, err := compactor.CompactFull(tsmFiles)
	if err != nil {
		t.Fatalf("compact failed: %v", err)
	}

	r := MustOpenTSMReader(result[0])
	defer r.Close()

	values, err := r.ReadAll([]byte("cpu,host=A#!~#value"))
	if err != nil {
		t.Fatalf("failed to read values: %v", err)
	}

	if len(values) != 1000 {
		t.Errorf("expected 1000 points after dedup, got %d", len(values))
	}

	// Last file's values win (highest sequence = newest data)
	for i, v := range values {
		expectedValue := float64(2000 + i)
		if v.Value().(float64) != expectedValue {
			t.Errorf("value at index %d: expected %.0f, got %.0f", i, expectedValue, v.Value())
		}
	}

	t.Logf("Dedup successful: 3000 points -> %d points", len(values))
}

// TestStreamingCompaction_UnsortedBlocks tests compaction of unsorted blocks.
func TestStreamingCompaction_UnsortedBlocks(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	f, err := os.Create(filepath.Join(dir, tsm1.DefaultFormatFileName(1, 1)+".tsm"))
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	w, err := tsm1.NewTSMWriter(f)
	if err != nil {
		t.Fatalf("failed to create TSM writer: %v", err)
	}

	for ts := int64(999); ts >= 0; ts-- {
		if err := w.Write([]byte("cpu,host=A#!~#value"), []tsm1.Value{tsm1.NewValue(ts, float64(ts))}); err != nil {
			t.Fatalf("failed to write value: %v", err)
		}
	}

	if err := w.WriteIndex(); err != nil {
		t.Fatalf("failed to write index: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	compactor := tsm1.NewCompactor()
	compactor.Dir = dir
	fs := &fakeFileStore{}
	compactor.FileStore = fs
	defer fs.Close()
	compactor.Open()

	result, err := compactor.CompactFull([]string{f.Name()})
	if err != nil {
		t.Fatalf("compact failed: %v", err)
	}

	r := MustOpenTSMReader(result[0])
	defer r.Close()

	values, err := r.ReadAll([]byte("cpu,host=A#!~#value"))
	if err != nil {
		t.Fatalf("failed to read values: %v", err)
	}

	if len(values) != 1000 {
		t.Errorf("expected 1000 points, got %d", len(values))
	}

	for i := 1; i < len(values); i++ {
		if values[i].UnixNano() <= values[i-1].UnixNano() {
			t.Errorf("values not sorted at index %d", i)
			break
		}
	}

	t.Logf("Unsorted input compacted to %d sorted points", len(values))
}

// TestStreamingCompaction_Interrupt tests interruptibility.
func TestStreamingCompaction_Interrupt(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	f, err := os.Create(filepath.Join(dir, tsm1.DefaultFormatFileName(1, 1)+".tsm"))
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	w, err := tsm1.NewTSMWriter(f)
	if err != nil {
		t.Fatalf("failed to create TSM writer: %v", err)
	}

	for keyIdx := 0; keyIdx < 1000; keyIdx++ {
		key := fmt.Sprintf("cpu,host=%04d#!~#value", keyIdx)
		values := make([]tsm1.Value, 10000)
		for j := 0; j < 10000; j++ {
			values[j] = tsm1.NewValue(int64(j), float64(j))
		}

		if err := w.Write([]byte(key), values); err != nil {
			t.Fatalf("failed to write values: %v", err)
		}
	}

	if err := w.WriteIndex(); err != nil {
		t.Fatalf("failed to write index: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	// Test that compaction works with single file
	compactor := tsm1.NewCompactor()
	compactor.Dir = dir
	fs := &fakeFileStore{}
	compactor.FileStore = fs
	defer fs.Close()
	compactor.Open()

	_, err = compactor.CompactFull([]string{f.Name()})
	if err != nil {
		t.Fatalf("compact failed: %v", err)
	}
}

// BenchmarkStreamingCompactionLarge benchmarks streaming compaction with large data.
func BenchmarkStreamingCompactionLarge(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark in short mode")
	}

	dir := MustTempDir()
	defer os.RemoveAll(dir)

	numFiles := 5
	pointsPerFile := 10_000_000
	pointsPerKey := 1000

	tsmFiles := make([]string, numFiles)
	totalPoints := 0

	b.Logf("Generating %d files with %d points each...", numFiles, pointsPerFile)

	for i := 0; i < numFiles; i++ {
		f, err := os.Create(filepath.Join(dir, tsm1.DefaultFormatFileName(i+1, 1)+".tsm"))
		if err != nil {
			b.Fatalf("failed to create file: %v", err)
		}

		w, err := tsm1.NewTSMWriter(f)
		if err != nil {
			b.Fatalf("failed to create TSM writer: %v", err)
		}

		numKeys := pointsPerFile / pointsPerKey
		for keyIdx := 0; keyIdx < numKeys; keyIdx++ {
			key := fmt.Sprintf("cpu,host=%05d#!~#value", keyIdx)
			startTime := int64(i) * int64(pointsPerFile)

			values := make([]tsm1.Value, pointsPerKey)
			for j := 0; j < pointsPerKey; j++ {
				ts := startTime + int64(j)
				values[j] = tsm1.NewValue(ts, float64(j))
			}

			if err := w.Write([]byte(key), values); err != nil {
				b.Fatalf("failed to write values: %v", err)
			}
		}

		if err := w.WriteIndex(); err != nil {
			b.Fatalf("failed to write index: %v", err)
		}
		if err := w.Close(); err != nil {
			b.Fatalf("failed to close writer: %v", err)
		}

		tsmFiles[i] = f.Name()
		totalPoints += pointsPerFile
	}

	compactor := tsm1.NewCompactor()
	compactor.Dir = dir
	fs := &fakeFileStore{}
	compactor.FileStore = fs
	defer fs.Close()

	b.ResetTimer()
	start := time.Now()

	result, err := compactor.CompactFull(tsmFiles)
	elapsed := time.Since(start)

	if err != nil {
		b.Fatalf("compact failed: %v", err)
	}

	b.ReportMetric(float64(totalPoints)/elapsed.Seconds(), "points/sec")
	b.ReportMetric(float64(len(tsmFiles)), "input_files")
	b.ReportMetric(float64(len(result)), "output_files")
	b.ReportMetric(elapsed.Seconds(), "total_seconds")

	b.Logf("Compacted %d points in %v (%.0f points/sec)",
		totalPoints, elapsed, float64(totalPoints)/elapsed.Seconds())
}

// TestStreamingCompaction_2GBLargeFile tests streaming compaction with TSM files
// totaling ~2GB. Each file has 10 keys with 4.5M points per key (45M total per file,
// 3 files = 135M points ≈ 2.01 GB).
func TestStreamingCompaction_2GBLargeFile(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping 2GB large file test in short mode")
	}

	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// 3 files × 45M points × 16 bytes/point ≈ 2.01 GB
	pointsPerFile := 45_000_000
	numFiles := 3
	numKeys := 10
	pointsPerKey := pointsPerFile / numKeys

	t.Logf("Generating %d files with %d points each (%d keys × %d points/key)",
		numFiles, pointsPerFile, numKeys, pointsPerKey)

	tsmFiles := make([]string, numFiles)
	for i := 0; i < numFiles; i++ {
		f, err := os.Create(filepath.Join(dir, tsm1.DefaultFormatFileName(i+1, 1)+".tsm"))
		if err != nil {
			t.Fatalf("failed to create file: %v", err)
		}

		w, err := tsm1.NewTSMWriter(f)
		if err != nil {
			t.Fatalf("failed to create TSM writer: %v", err)
		}

		for keyIdx := 0; keyIdx < numKeys; keyIdx++ {
			key := fmt.Sprintf("cpu,host=%02d#!~#value", keyIdx)
			startTime := int64(i) * int64(pointsPerFile)

			values := make([]tsm1.Value, 0, pointsPerKey)
			for j := 0; j < pointsPerKey; j++ {
				ts := startTime + int64(j)
				values = append(values, tsm1.NewValue(ts, float64(j%1000)))
			}

			if err := w.Write([]byte(key), values); err != nil {
				t.Fatalf("failed to write values for key %s: %v", key, err)
			}
		}

		if err := w.WriteIndex(); err != nil {
			t.Fatalf("failed to write index: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("failed to close writer: %v", err)
		}

		tsmFiles[i] = f.Name()
		stat, _ := os.Stat(f.Name())
		t.Logf("File %d: %s (%.2f MB)", i+1, filepath.Base(f.Name()), float64(stat.Size())/1024/1024)
	}

	// Run full compaction (triggers streaming path for non-fast mode)
	compactor := tsm1.NewCompactor()
	compactor.Dir = dir
	fs := &fakeFileStore{}
	compactor.FileStore = fs
	defer fs.Close()
	compactor.Open()

	t.Log("Starting streaming compaction of ~2GB data...")
	start := time.Now()
	result, err := compactor.CompactFull(tsmFiles)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("compact failed: %v", err)
	}

	t.Logf("Compaction completed in %v, produced %d file(s)", elapsed, len(result))

	if len(result) == 0 {
		t.Fatal("expected at least one output file")
	}

	// Verify output: read all keys and check total point count
	r := MustOpenTSMReader(result[0])
	defer r.Close()

	totalPoints := 0
	keyCount := r.KeyCount()
	if keyCount != numKeys {
		t.Errorf("expected %d keys, got %d", numKeys, keyCount)
	}

	for i := 0; i < keyCount; i++ {
		key, _ := r.KeyAt(i)
		values, err := r.ReadAll(key)
		if err != nil {
			t.Fatalf("failed to read key %s: %v", key, err)
		}
		totalPoints += len(values)

		// Verify timestamps are sorted within each key
		for j := 1; j < len(values); j++ {
			if values[j].UnixNano() <= values[j-1].UnixNano() {
				t.Errorf("key %s: values not sorted at index %d (ts=%d <= prev=%d)",
					key, j, values[j].UnixNano(), values[j-1].UnixNano())
				break
			}
		}
	}

	expectedPoints := numFiles * pointsPerFile
	if totalPoints != expectedPoints {
		t.Errorf("expected %d points, got %d (diff: %d)", expectedPoints, totalPoints, expectedPoints-totalPoints)
	}

	// Verify output file size is reasonable
	outStat, _ := os.Stat(result[0])
	t.Logf("Output file: %s (%.2f MB)", filepath.Base(result[0]), float64(outStat.Size())/1024/1024)
	t.Logf("Total points: %d, Keys: %d, Elapsed: %v", totalPoints, keyCount, elapsed)
}

// generateLargeTSMFile creates a TSM file with many keys and points.
// Keys are written in sorted order using zero-padded host IDs.
// Points are written in batches of 1000 (DefaultMaxPointsPerBlock) to avoid
// huge memory allocations.
func generateLargeTSMFile(t testing.TB, dir string, gen int, numKeys, pointsPerKey int) (string, int) {
	t.Helper()

	f, err := os.Create(filepath.Join(dir, tsm1.DefaultFormatFileName(gen, 1)+".tsm"))
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	w, err := tsm1.NewTSMWriter(f)
	if err != nil {
		t.Fatalf("failed to create TSM writer: %v", err)
	}

	totalPoints := 0
 batchSize := 1000 // DefaultMaxPointsPerBlock

	for keyIdx := 0; keyIdx < numKeys; keyIdx++ {
		key := fmt.Sprintf("cpu,host=%06d#!~#value", keyIdx)
		for offset := 0; offset < pointsPerKey; offset += batchSize {
			count := batchSize
			if offset+count > pointsPerKey {
				count = pointsPerKey - offset
			}
			values := make([]tsm1.Value, count)
			for j := 0; j < count; j++ {
				values[j] = tsm1.NewValue(int64(offset+j), float64((offset+j)%1000))
			}
			if err := w.Write([]byte(key), values); err != nil {
				t.Fatalf("failed to write values for key %s: %v", key, err)
			}
			totalPoints += count
		}
	}

	if err := w.WriteIndex(); err != nil {
		t.Fatalf("failed to write index: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	return f.Name(), totalPoints
}

// generateLargeTSMFileWithOffset creates a TSM file with a time offset per file,
// used for overlapping time range tests.
func generateLargeTSMFileWithOffset(t testing.TB, dir string, gen, numKeys, pointsPerKey, timeOffset int) (string, int) {
	t.Helper()

	f, err := os.Create(filepath.Join(dir, tsm1.DefaultFormatFileName(gen, 1)+".tsm"))
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	w, err := tsm1.NewTSMWriter(f)
	if err != nil {
		t.Fatalf("failed to create TSM writer: %v", err)
	}

	totalPoints := 0
	batchSize := 1000

	for keyIdx := 0; keyIdx < numKeys; keyIdx++ {
		key := fmt.Sprintf("cpu,host=%06d#!~#value", keyIdx)
		for offset := 0; offset < pointsPerKey; offset += batchSize {
			count := batchSize
			if offset+count > pointsPerKey {
				count = pointsPerKey - offset
			}
			values := make([]tsm1.Value, count)
			for j := 0; j < count; j++ {
				ts := int64(timeOffset + offset + j)
				values[j] = tsm1.NewValue(ts, float64((offset+j)%1000))
			}
			if err := w.Write([]byte(key), values); err != nil {
				t.Fatalf("failed to write values for key %s: %v", key, err)
			}
			totalPoints += count
		}
	}

	if err := w.WriteIndex(); err != nil {
		t.Fatalf("failed to write index: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	return f.Name(), totalPoints
}

// runCompactionWithMetrics runs a compaction and returns performance metrics.
// A background goroutine samples memory stats every 10ms to capture peak usage
// during compaction (GC alone can't show the true peak).
func runCompactionWithMetrics(t testing.TB, compactor *tsm1.Compactor, tsmFiles []string, mode string) {
	t.Helper()

	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	// Start background memory sampler to capture peak during compaction
	var peakHeapAlloc, peakHeapInuse, peakHeapSys uint64
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				if m.HeapAlloc > peakHeapAlloc {
					peakHeapAlloc = m.HeapAlloc
				}
				if m.HeapInuse > peakHeapInuse {
					peakHeapInuse = m.HeapInuse
				}
				if m.HeapSys > peakHeapSys {
					peakHeapSys = m.HeapSys
				}
			}
		}
	}()

	t.Logf("[%s] Starting compaction of %d files...", mode, len(tsmFiles))
	start := time.Now()

	var result []string
	var err error
	if mode == "streaming" {
		result, err = compactor.CompactFull(tsmFiles)
	} else {
		result, err = compactor.CompactFast(tsmFiles)
	}
	elapsed := time.Since(start)

	// Stop sampler and capture final peak
	close(done)
	time.Sleep(20 * time.Millisecond) // let sampler goroutine finish

	if err != nil {
		t.Fatalf("[%s] compact failed: %v", mode, err)
	}

	runtime.GC()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	// Count total points and verify correctness
	totalPoints := 0
	keyCount := 0
	if len(result) > 0 {
		r := MustOpenTSMReader(result[0])
		keyCount = r.KeyCount()
		for i := 0; i < keyCount; i++ {
			key, _ := r.KeyAt(i)
			values, err := r.ReadAll(key)
			if err != nil {
				t.Fatalf("[%s] failed to read key %s: %v", mode, key, err)
			}
			totalPoints += len(values)

			// Verify timestamps are sorted
			for j := 1; j < len(values); j++ {
				if values[j].UnixNano() <= values[j-1].UnixNano() {
					t.Errorf("[%s] key %s: values not sorted at index %d", mode, key, j)
					break
				}
			}
		}
		r.Close()
	}

	// Report output file sizes
	var totalOutputSize int64
	for _, f := range result {
		stat, _ := os.Stat(f)
		totalOutputSize += stat.Size()
		t.Logf("[%s] Output: %s (%.2f MB)", mode, filepath.Base(f), float64(stat.Size())/1024/1024)
	}

	memAllocDiff := int64(memAfter.TotalAlloc) - int64(memBefore.TotalAlloc)
	if memAllocDiff < 0 {
		memAllocDiff = 0
	}

	t.Logf("[%s] Elapsed: %v", mode, elapsed)
	t.Logf("[%s] Total points: %d, Keys: %d", mode, totalPoints, keyCount)
	t.Logf("[%s] Output size: %.2f MB", mode, float64(totalOutputSize)/1024/1024)
	t.Logf("[%s] Throughput: %.0f points/sec, %.2f MB/sec",
		mode,
		float64(totalPoints)/elapsed.Seconds(),
		float64(totalOutputSize)/1024/1024/elapsed.Seconds())
	t.Logf("[%s] Memory After GC (post-compaction):", mode)
	t.Logf("[%s]   HeapAlloc  = %.2f MB", mode, float64(memAfter.HeapAlloc)/1024/1024)
	t.Logf("[%s]   HeapInuse  = %.2f MB", mode, float64(memAfter.HeapInuse)/1024/1024)
	t.Logf("[%s]   HeapSys    = %.2f MB", mode, float64(memAfter.HeapSys)/1024/1024)
	t.Logf("[%s] Memory Peak During Compaction (sampled every 10ms):", mode)
	t.Logf("[%s]   PeakHeapAlloc = %.2f MB (live objects at peak)", mode, float64(peakHeapAlloc)/1024/1024)
	t.Logf("[%s]   PeakHeapInuse = %.2f MB (in-use spans at peak)", mode, float64(peakHeapInuse)/1024/1024)
	t.Logf("[%s]   PeakHeapSys   = %.2f MB (from OS at peak)", mode, float64(peakHeapSys)/1024/1024)
	t.Logf("[%s] Other:", mode)
	t.Logf("[%s]   StackInuse = %.2f MB", mode, float64(memAfter.StackInuse)/1024/1024)
	t.Logf("[%s]   Sys        = %.2f MB", mode, float64(memAfter.Sys)/1024/1024)
	t.Logf("[%s]   TotalAlloc = %.2f MB (cumulative)", mode, float64(memAfter.TotalAlloc)/1024/1024)
	t.Logf("[%s]   NumGC      = %d", mode, memAfter.NumGC-memBefore.NumGC)
}

// TestStreamingCompaction_100KKeys_2GBx4 tests streaming compaction with 100K keys,
// 5000 points per key, across 4 non-overlapping TSM files.
// Total: 100K keys × 5000 points/key × 4 files = 2B points.
// TSMWriter buffers compressed blocks in memory; estimated ~10GB peak on 16GB system.
func TestStreamingCompaction_100KKeys_2GBx4(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping 100K keys test in short mode")
	}

	dir := MustTempDir()
	defer os.RemoveAll(dir)

	numKeys := 100_000
	pointsPerKey := 5_000
	numFiles := 4

	t.Logf("Generating %d files: %d keys × %d points/key = %d points/file",
		numFiles, numKeys, pointsPerKey, numKeys*pointsPerKey)

	tsmFiles := make([]string, numFiles)
	totalGenerated := 0
	for i := 0; i < numFiles; i++ {
		fname, pts := generateLargeTSMFile(t, dir, i+1, numKeys, pointsPerKey)
		tsmFiles[i] = fname
		totalGenerated += pts
		stat, _ := os.Stat(fname)
		t.Logf("File %d: %s (%.2f MB, %d points)", i+1, filepath.Base(fname),
			float64(stat.Size())/1024/1024, pts)
	}

	compactor := tsm1.NewCompactor()
	compactor.Dir = dir
	fs := &fakeFileStore{}
	compactor.FileStore = fs
	defer fs.Close()
	compactor.Open()

	runCompactionWithMetrics(t, compactor, tsmFiles, "streaming")
}

// TestStreamingCompaction_100KKeys_2GBx4_Overlap tests streaming compaction with 100K keys,
// 5000 points per key, across 4 overlapping TSM files with 50% overlap.
func TestStreamingCompaction_100KKeys_2GBx4_Overlap(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping 100K keys overlap test in short mode")
	}

	dir := MustTempDir()
	defer os.RemoveAll(dir)

	numKeys := 100_000
	pointsPerKey := 5_000
	numFiles := 4
	overlapStep := pointsPerKey / 2 // 50% overlap between consecutive files

	t.Logf("Generating %d overlapping files: %d keys × %d points/key, overlap step=%d",
		numFiles, numKeys, pointsPerKey, overlapStep)

	tsmFiles := make([]string, numFiles)
	for i := 0; i < numFiles; i++ {
		timeOffset := i * overlapStep
		fname, pts := generateLargeTSMFileWithOffset(t, dir, i+1, numKeys, pointsPerKey, timeOffset)
		tsmFiles[i] = fname
		stat, _ := os.Stat(fname)
		t.Logf("File %d: %s (%.2f MB, %d points, offset=%d)", i+1, filepath.Base(fname),
			float64(stat.Size())/1024/1024, pts, timeOffset)
	}

	// Expected unique points: time range [0, numFiles*overlapStep + pointsPerKey - overlapStep)
	// = [0, 4*5000 + 10000 - 5000) = [0, 25000)
	expectedUniquePerKey := numFiles*overlapStep + pointsPerKey - overlapStep
	t.Logf("Expected unique points per key: %d (total: %d)", expectedUniquePerKey, expectedUniquePerKey*numKeys)

	compactor := tsm1.NewCompactor()
	compactor.Dir = dir
	fs := &fakeFileStore{}
	compactor.FileStore = fs
	defer fs.Close()
	compactor.Open()

	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	t.Logf("[streaming-overlap] Starting compaction of %d files...", numFiles)
	start := time.Now()
	result, err := compactor.CompactFull(tsmFiles)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("compact failed: %v", err)
	}

	runtime.GC()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	// Verify output
	r := MustOpenTSMReader(result[0])
	defer r.Close()

	totalPoints := 0
	keyCount := r.KeyCount()
	if keyCount != numKeys {
		t.Errorf("expected %d keys, got %d", numKeys, keyCount)
	}

	for i := 0; i < keyCount; i++ {
		key, _ := r.KeyAt(i)
		values, err := r.ReadAll(key)
		if err != nil {
			t.Fatalf("failed to read key %s: %v", key, err)
		}
		totalPoints += len(values)

		// Verify timestamps are sorted and unique (deduped)
		for j := 1; j < len(values); j++ {
			if values[j].UnixNano() <= values[j-1].UnixNano() {
				t.Errorf("key %s: values not sorted/deduped at index %d (ts=%d <= prev=%d)",
					key, j, values[j].UnixNano(), values[j-1].UnixNano())
				break
			}
		}
	}

	expectedTotal := expectedUniquePerKey * numKeys
	t.Logf("[streaming-overlap] Elapsed: %v", elapsed)
	t.Logf("[streaming-overlap] Total points: %d (expected: %d, diff: %d)",
		totalPoints, expectedTotal, totalPoints-expectedTotal)
	t.Logf("[streaming-overlap] Throughput: %.0f points/sec", float64(totalPoints)/elapsed.Seconds())

	memAllocDiff := int64(memAfter.TotalAlloc) - int64(memBefore.TotalAlloc)
	t.Logf("[streaming-overlap] Memory: AllocDiff=%.2f MB, Sys=%.2f MB",
		float64(memAllocDiff)/1024/1024, float64(memAfter.Sys)/1024/1024)

	if totalPoints != expectedTotal {
		t.Errorf("point count mismatch: got %d, want %d", totalPoints, expectedTotal)
	}
}

// TestCompaction_100KKeys_StreamingVsBatch compares streaming vs batch compaction
// performance on the same dataset (100K keys × 5000 points/key × 4 files = 2B points).
func TestCompaction_100KKeys_StreamingVsBatch(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping streaming vs batch comparison in short mode")
	}

	numKeys := 100_000
	pointsPerKey := 5_000
	numFiles := 4

	t.Logf("=== Streaming vs Batch Compaction Comparison ===")
	t.Logf("Config: %d keys × %d points/key × %d files = %d total points",
		numKeys, pointsPerKey, numFiles, numKeys*pointsPerKey*numFiles)

	// --- Streaming mode ---
	dirStream := MustTempDir()
	defer os.RemoveAll(dirStream)

	t.Log("--- Generating files for streaming test ---")
	tsmFilesStream := make([]string, numFiles)
	for i := 0; i < numFiles; i++ {
		fname, _ := generateLargeTSMFile(t, dirStream, i+1, numKeys, pointsPerKey)
		tsmFilesStream[i] = fname
	}

	compactorStream := tsm1.NewCompactor()
	compactorStream.Dir = dirStream
	fsStream := &fakeFileStore{}
	compactorStream.FileStore = fsStream
	defer fsStream.Close()
	compactorStream.Open()

	runCompactionWithMetrics(t, compactorStream, tsmFilesStream, "streaming")

	// --- Batch mode ---
	dirBatch := MustTempDir()
	defer os.RemoveAll(dirBatch)

	t.Log("--- Generating files for batch test ---")
	tsmFilesBatch := make([]string, numFiles)
	for i := 0; i < numFiles; i++ {
		fname, _ := generateLargeTSMFile(t, dirBatch, i+1, numKeys, pointsPerKey)
		tsmFilesBatch[i] = fname
	}

	compactorBatch := tsm1.NewCompactor()
	compactorBatch.Dir = dirBatch
	fsBatch := &fakeFileStore{}
	compactorBatch.FileStore = fsBatch
	defer fsBatch.Close()
	compactorBatch.Open()

	runCompactionWithMetrics(t, compactorBatch, tsmFilesBatch, "batch")

	t.Log("=== Comparison complete. See metrics above. ===")
}

// TestCompaction_Overlapping_StreamingVsBatch compares streaming vs batch compaction
// on overlapping data where all files cover the same time range for each key.
// This is the worst case for memory: every block from every file overlaps,
// forcing full decode+merge. The streaming path should buffer less memory
// because it can flush non-overlapping blocks incrementally.
func TestCompaction_Overlapping_StreamingVsBatch(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping overlapping comparison in short mode")
	}

	numKeys := 10_000
	pointsPerKey := 5_000
	numFiles := 4

	// All files cover the same time range [0, pointsPerKey) for each key.
	// This means every block from every file overlaps with every other block.
	// The streaming path must buffer ALL blocks for each key (same as batch),
	// but the incremental reading still provides different memory dynamics.
	t.Logf("=== Streaming vs Batch Compaction Comparison (OVERLAPPING) ===")
	t.Logf("Config: %d keys × %d points/key × %d files = %d total points (all overlapping)",
		numKeys, pointsPerKey, numFiles, numKeys*pointsPerKey*numFiles)

	// --- Streaming mode ---
	dirStream := MustTempDir()
	defer os.RemoveAll(dirStream)

	t.Log("--- Generating overlapping files for streaming test ---")
	tsmFilesStream := make([]string, numFiles)
	for i := 0; i < numFiles; i++ {
		fname, _ := generateLargeTSMFile(t, dirStream, i+1, numKeys, pointsPerKey)
		tsmFilesStream[i] = fname
	}

	compactorStream := tsm1.NewCompactor()
	compactorStream.Dir = dirStream
	fsStream := &fakeFileStore{}
	compactorStream.FileStore = fsStream
	defer fsStream.Close()
	compactorStream.Open()

	runCompactionWithMetrics(t, compactorStream, tsmFilesStream, "streaming-overlap")

	// --- Batch mode ---
	dirBatch := MustTempDir()
	defer os.RemoveAll(dirBatch)

	t.Log("--- Generating overlapping files for batch test ---")
	tsmFilesBatch := make([]string, numFiles)
	for i := 0; i < numFiles; i++ {
		fname, _ := generateLargeTSMFile(t, dirBatch, i+1, numKeys, pointsPerKey)
		tsmFilesBatch[i] = fname
	}

	compactorBatch := tsm1.NewCompactor()
	compactorBatch.Dir = dirBatch
	fsBatch := &fakeFileStore{}
	compactorBatch.FileStore = fsBatch
	defer fsBatch.Close()
	compactorBatch.Open()

	runCompactionWithMetrics(t, compactorBatch, tsmFilesBatch, "batch-overlap")

	t.Log("=== Overlapping comparison complete. See metrics above. ===")
}

// TestCompaction_PartialOverlap_StreamingVsBatch compares streaming vs batch
// on partially overlapping data. Each file covers a shifted time range so that
// adjacent files overlap by 50%. This tests the streaming path's key advantage:
// it can flush the non-overlapping prefix of each key immediately.
func TestCompaction_PartialOverlap_StreamingVsBatch(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping partial overlap comparison in short mode")
	}

	numKeys := 10_000
	pointsPerKey := 5_000
	numFiles := 4

	// Time ranges: [0,5000), [2500,7500), [5000,10000), [7500,12500)
	// Adjacent files overlap by 50%. Non-adjacent files may or may not overlap.
	// The streaming path should have lower peak memory because it can flush
	// the non-overlapping prefix of each file's blocks immediately.
	t.Logf("=== Streaming vs Batch Compaction Comparison (PARTIAL OVERLAP) ===")
	t.Logf("Config: %d keys × %d points/key × %d files, shifted by %d = %d total points",
		numKeys, pointsPerKey, numFiles, pointsPerKey/2, numKeys*pointsPerKey*numFiles)

	// --- Streaming mode ---
	dirStream := MustTempDir()
	defer os.RemoveAll(dirStream)

	t.Log("--- Generating partially overlapping files for streaming test ---")
	tsmFilesStream := make([]string, numFiles)
	for i := 0; i < numFiles; i++ {
		fname, _ := generateLargeTSMFileWithOffset(t, dirStream, i+1, numKeys, pointsPerKey, i*pointsPerKey/2)
		tsmFilesStream[i] = fname
	}

	compactorStream := tsm1.NewCompactor()
	compactorStream.Dir = dirStream
	fsStream := &fakeFileStore{}
	compactorStream.FileStore = fsStream
	defer fsStream.Close()
	compactorStream.Open()

	runCompactionWithMetrics(t, compactorStream, tsmFilesStream, "streaming-partial")

	// --- Batch mode ---
	dirBatch := MustTempDir()
	defer os.RemoveAll(dirBatch)

	t.Log("--- Generating partially overlapping files for batch test ---")
	tsmFilesBatch := make([]string, numFiles)
	for i := 0; i < numFiles; i++ {
		fname, _ := generateLargeTSMFileWithOffset(t, dirBatch, i+1, numKeys, pointsPerKey, i*pointsPerKey/2)
		tsmFilesBatch[i] = fname
	}

	compactorBatch := tsm1.NewCompactor()
	compactorBatch.Dir = dirBatch
	fsBatch := &fakeFileStore{}
	compactorBatch.FileStore = fsBatch
	defer fsBatch.Close()
	compactorBatch.Open()

	runCompactionWithMetrics(t, compactorBatch, tsmFilesBatch, "batch-partial")

	t.Log("=== Partial overlap comparison complete. See metrics above. ===")
}

// BenchmarkCompaction_100KKeys benchmarks streaming compaction with 100K keys × 5000 points.
func BenchmarkCompaction_100KKeys(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark in short mode")
	}

	numKeys := 100_000
	pointsPerKey := 5_000
	numFiles := 4

	dir := MustTempDir()
	defer os.RemoveAll(dir)

	b.Logf("Generating %d files with %d keys × %d points/key...", numFiles, numKeys, pointsPerKey)
	tsmFiles := make([]string, numFiles)
	for i := 0; i < numFiles; i++ {
		fname, _ := generateLargeTSMFile(b, dir, i+1, numKeys, pointsPerKey)
		tsmFiles[i] = fname
	}

	compactor := tsm1.NewCompactor()
	compactor.Dir = dir
	fs := &fakeFileStore{}
	compactor.FileStore = fs
	defer fs.Close()
	compactor.Open()

	b.ResetTimer()
	start := time.Now()

	result, err := compactor.CompactFull(tsmFiles)
	elapsed := time.Since(start)

	if err != nil {
		b.Fatalf("compact failed: %v", err)
	}

	totalPoints := int64(numKeys) * int64(pointsPerKey) * int64(numFiles)
	b.ReportMetric(float64(totalPoints)/elapsed.Seconds(), "points/sec")
	b.ReportMetric(elapsed.Seconds(), "total_seconds")
	b.ReportMetric(float64(len(tsmFiles)), "input_files")
	b.ReportMetric(float64(len(result)), "output_files")

	// Report output size
	var totalSize int64
	for _, f := range result {
		stat, _ := os.Stat(f)
		totalSize += stat.Size()
	}
	b.ReportMetric(float64(totalSize)/1024/1024, "output_MB")

	b.Logf("Compacted %d points in %v (%.0f points/sec), output: %.2f MB",
		totalPoints, elapsed, float64(totalPoints)/elapsed.Seconds(), float64(totalSize)/1024/1024)
}
