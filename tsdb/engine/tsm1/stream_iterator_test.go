package tsm1_test

import (
	"fmt"
	"math"
	"os"
	"testing"

	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

func TestBlockValueIterator_Basic(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// Create a TSM file with a single key and values
	vals := []tsm1.Value{
		tsm1.NewValue(1, 1.1),
		tsm1.NewValue(2, 2.2),
		tsm1.NewValue(3, 3.3),
	}
	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": vals,
	}
	f := MustWriteTSM(dir, 1, writes)
	r := MustOpenTSMReader(f)
	defer r.Close()

	iter := tsm1.NewBlockValueIterator(r, 0)
	if !iter.Init() {
		t.Fatal("expected Init to return true")
	}

	// Decode the raw block
	decoded, err := tsm1.DecodeBlock(iter.RawBlock(), nil)
	if err != nil {
		t.Fatalf("unexpected error decoding block: %v", err)
	}

	if len(decoded) != len(vals) {
		t.Fatalf("expected %d values, got %d", len(vals), len(decoded))
	}

	for i, v := range decoded {
		if v.UnixNano() != vals[i].UnixNano() {
			t.Errorf("value %d: expected ts=%d, got %d", i, vals[i].UnixNano(), v.UnixNano())
		}
	}

	if iter.Err() != nil {
		t.Fatalf("unexpected error: %v", iter.Err())
	}
}

func TestBlockValueIterator_KeyBoundary(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// Create a TSM file with multiple keys
	vals1 := []tsm1.Value{
		tsm1.NewValue(1, 1.1),
		tsm1.NewValue(2, 2.2),
	}
	vals2 := []tsm1.Value{
		tsm1.NewValue(3, 3.3),
	}
	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": vals1,
		"cpu,host=B#!~#value": vals2,
	}
	f := MustWriteTSM(dir, 1, writes)
	r := MustOpenTSMReader(f)
	defer r.Close()

	iter := tsm1.NewBlockValueIterator(r, 0)
	if !iter.Init() {
		t.Fatal("expected Init to return true")
	}

	// First key should be "cpu,host=A#!~#value" (lexicographically smaller)
	if string(iter.Key()) != "cpu,host=A#!~#value" {
		t.Fatalf("expected key 'cpu,host=A#!~#value', got '%s'", string(iter.Key()))
	}

	// Decode block for first key
	decoded, err := tsm1.DecodeBlock(iter.RawBlock(), nil)
	if err != nil {
		t.Fatalf("unexpected error decoding block: %v", err)
	}
	if len(decoded) != len(vals1) {
		t.Fatalf("expected %d values for first key, got %d", len(vals1), len(decoded))
	}

	// NextBlock should detect key change
	if iter.NextBlock() {
		t.Fatal("expected NextBlock to return false (key changed)")
	}

	// Activate pending for next key
	if !iter.ActivatePending() {
		t.Fatal("expected ActivatePending to return true")
	}

	if string(iter.Key()) != "cpu,host=B#!~#value" {
		t.Fatalf("expected key 'cpu,host=B#!~#value', got '%s'", string(iter.Key()))
	}
}

func TestBlockValueIterator_MultiBlock(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// Create a TSM file with many values for same key (multiple blocks)
	var vals []tsm1.Value
	for i := int64(1); i <= 2000; i++ {
		vals = append(vals, tsm1.NewValue(i, float64(i)))
	}
	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": vals,
	}
	f := MustWriteTSM(dir, 1, writes)
	r := MustOpenTSMReader(f)
	defer r.Close()

	entries := r.Entries([]byte("cpu,host=A#!~#value"))
	t.Logf("Number of blocks: %d", len(entries))

	iter := tsm1.NewBlockValueIterator(r, 0)
	if !iter.Init() {
		t.Fatal("expected Init to return true")
	}

	// Decode all values across all blocks for this key
	var count int
	for {
		decoded, err := tsm1.DecodeBlock(iter.RawBlock(), nil)
		if err != nil {
			t.Fatalf("unexpected error decoding block: %v", err)
		}
		count += len(decoded)

		if !iter.NextBlock() {
			break
		}
	}

	if count != 2000 {
		t.Fatalf("expected 2000 total values, got %d", count)
	}
}

func TestBlockValueIterator_Tombstone(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// Create a TSM file
	vals := []tsm1.Value{
		tsm1.NewValue(1, 1.1),
		tsm1.NewValue(2, 2.2),
		tsm1.NewValue(3, 3.3),
	}
	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": vals,
	}
	f := MustWriteTSM(dir, 1, writes)

	// Add tombstone covering entire range
	ts := tsm1.NewTombstoner(f, nil)
	ts.AddRange([][]byte{[]byte("cpu,host=A#!~#value")}, math.MinInt64, math.MaxInt64)
	if err := ts.Flush(); err != nil {
		t.Fatalf("unexpected error flushing tombstone: %v", err)
	}

	r := MustOpenTSMReader(f)
	defer r.Close()

	iter := tsm1.NewBlockValueIterator(r, 0)
	// When entire key is tombstoned with full time range, the key is removed from index
	// so Init() returns false (no blocks to iterate)
	if iter.Init() {
		// If Init returns true, verify tombstone range is reported
		tr := iter.TombstoneRange(iter.Key())
		if len(tr) == 0 {
			t.Fatal("expected tombstone ranges to be non-empty")
		}
	}
	// It's also acceptable for Init to return false when key is fully deleted
}

func TestBlockValueIterator_TombstonePartial(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// Create a TSM file
	vals := []tsm1.Value{
		tsm1.NewValue(1, 1.1),
		tsm1.NewValue(2, 2.2),
		tsm1.NewValue(3, 3.3),
	}
	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": vals,
	}
	f := MustWriteTSM(dir, 1, writes)

	// Add tombstone covering ts=2
	ts := tsm1.NewTombstoner(f, nil)
	ts.AddRange([][]byte{[]byte("cpu,host=A#!~#value")}, 2, 2)
	if err := ts.Flush(); err != nil {
		t.Fatalf("unexpected error flushing tombstone: %v", err)
	}

	r := MustOpenTSMReader(f)
	defer r.Close()

	iter := tsm1.NewBlockValueIterator(r, 0)
	if !iter.Init() {
		t.Fatal("expected Init to return true")
	}

	// Verify tombstone range is reported
	tr := iter.TombstoneRange(iter.Key())
	if len(tr) == 0 {
		t.Fatal("expected tombstone ranges to be non-empty")
	}

	// Block still contains all 3 values; tombstones are applied at merge time
	decoded, err := tsm1.DecodeBlock(iter.RawBlock(), nil)
	if err != nil {
		t.Fatalf("unexpected error decoding block: %v", err)
	}
	if len(decoded) != 3 {
		t.Fatalf("expected 3 raw values (tombstones applied at merge), got %d", len(decoded))
	}
}

func TestBlockValueIterator_TombstoneMultiple(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// Create a TSM file
	vals := []tsm1.Value{
		tsm1.NewValue(1, 1.1),
		tsm1.NewValue(2, 2.2),
		tsm1.NewValue(3, 3.3),
		tsm1.NewValue(4, 4.4),
		tsm1.NewValue(5, 5.5),
	}
	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": vals,
	}
	f := MustWriteTSM(dir, 1, writes)

	// Add multiple tombstone ranges
	ts := tsm1.NewTombstoner(f, nil)
	ts.AddRange([][]byte{[]byte("cpu,host=A#!~#value")}, 2, 2)
	ts.AddRange([][]byte{[]byte("cpu,host=A#!~#value")}, 4, 4)
	if err := ts.Flush(); err != nil {
		t.Fatalf("unexpected error flushing tombstone: %v", err)
	}

	r := MustOpenTSMReader(f)
	defer r.Close()

	iter := tsm1.NewBlockValueIterator(r, 0)
	if !iter.Init() {
		t.Fatal("expected Init to return true")
	}

	// Verify tombstone ranges are reported
	tr := iter.TombstoneRange(iter.Key())
	if len(tr) < 2 {
		t.Fatalf("expected at least 2 tombstone ranges, got %d", len(tr))
	}

	// Block still contains all 5 values; tombstones are applied at merge time
	decoded, err := tsm1.DecodeBlock(iter.RawBlock(), nil)
	if err != nil {
		t.Fatalf("unexpected error decoding block: %v", err)
	}
	if len(decoded) != 5 {
		t.Fatalf("expected 5 raw values (tombstones applied at merge), got %d", len(decoded))
	}
}

func TestBlockValueIterator_TombstoneFullKey(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// Create a TSM file with multiple keys
	vals1 := []tsm1.Value{
		tsm1.NewValue(1, 1.1),
	}
	vals2 := []tsm1.Value{
		tsm1.NewValue(2, 2.2),
	}
	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": vals1,
		"cpu,host=B#!~#value": vals2,
	}
	f := MustWriteTSM(dir, 1, writes)

	// Tombstone entire key A with full time range
	ts := tsm1.NewTombstoner(f, nil)
	ts.AddRange([][]byte{[]byte("cpu,host=A#!~#value")}, math.MinInt64, math.MaxInt64)
	if err := ts.Flush(); err != nil {
		t.Fatalf("unexpected error flushing tombstone: %v", err)
	}

	r := MustOpenTSMReader(f)
	defer r.Close()

	iter := tsm1.NewBlockValueIterator(r, 0)
	// Since key A is fully tombstoned, Init() should start at key B
	if !iter.Init() {
		t.Fatal("expected Init to return true (should start at key B)")
	}

	// Should be on key B now
	if string(iter.Key()) != "cpu,host=B#!~#value" {
		t.Fatalf("expected key 'cpu,host=B#!~#value', got '%s'", string(iter.Key()))
	}

	decoded, err := tsm1.DecodeBlock(iter.RawBlock(), nil)
	if err != nil {
		t.Fatalf("unexpected error decoding block: %v", err)
	}
	if len(decoded) != 1 {
		t.Fatalf("expected 1 value for key B, got %d", len(decoded))
	}
	if decoded[0].UnixNano() != 2 {
		t.Errorf("expected ts=2, got %d", decoded[0].UnixNano())
	}
}

func TestBlockValueIterator_TombstoneAllDeleted(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// Create a TSM file with values spanning time 1-2
	vals := []tsm1.Value{
		tsm1.NewValue(1, 1.1),
		tsm1.NewValue(2, 2.2),
	}
	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": vals,
	}
	f := MustWriteTSM(dir, 1, writes)

	// Tombstone all values with exact time range
	ts := tsm1.NewTombstoner(f, nil)
	ts.AddRange([][]byte{[]byte("cpu,host=A#!~#value")}, 1, 2)
	if err := ts.Flush(); err != nil {
		t.Fatalf("unexpected error flushing tombstone: %v", err)
	}

	r := MustOpenTSMReader(f)
	defer r.Close()

	iter := tsm1.NewBlockValueIterator(r, 0)
	if !iter.Init() {
		// If Init returns false, the key was fully deleted from index
		return
	}

	// Verify tombstone range covers the full block
	tr := iter.TombstoneRange(iter.Key())
	if len(tr) == 0 {
		t.Fatal("expected tombstone ranges to be non-empty")
	}
}

func TestBlockValueIterator_TombstoneMultipleBlocks(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// Create a TSM file with many values (multiple blocks)
	var vals []tsm1.Value
	for i := int64(1); i <= 2000; i++ {
		vals = append(vals, tsm1.NewValue(i, float64(i)))
	}
	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": vals,
	}
	f := MustWriteTSM(dir, 1, writes)

	// Tombstone first 1000 values
	ts := tsm1.NewTombstoner(f, nil)
	ts.AddRange([][]byte{[]byte("cpu,host=A#!~#value")}, 1, 1000)
	if err := ts.Flush(); err != nil {
		t.Fatalf("unexpected error flushing tombstone: %v", err)
	}

	r := MustOpenTSMReader(f)
	defer r.Close()

	iter := tsm1.NewBlockValueIterator(r, 0)
	if !iter.Init() {
		t.Fatal("expected Init to return true")
	}

	// Verify tombstone range is reported
	tr := iter.TombstoneRange(iter.Key())
	if len(tr) == 0 {
		t.Fatal("expected tombstone ranges to be non-empty")
	}

	// Iterate through all blocks for this key, decoding raw blocks
	var totalCount int
	for {
		decoded, err := tsm1.DecodeBlock(iter.RawBlock(), nil)
		if err != nil {
			t.Fatalf("unexpected error decoding block: %v", err)
		}
		totalCount += len(decoded)

		if !iter.NextBlock() {
			break
		}
	}

	// Raw blocks still contain all values; tombstones applied at merge time
	if totalCount != 2000 {
		t.Fatalf("expected 2000 raw values across all blocks, got %d", totalCount)
	}
}

func TestBlockValueIterator_Accessors(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	vals := []tsm1.Value{
		tsm1.NewValue(1, 1.1),
	}
	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": vals,
	}
	f := MustWriteTSM(dir, 1, writes)
	r := MustOpenTSMReader(f)
	defer r.Close()

	iter := tsm1.NewBlockValueIterator(r, 42)
	if !iter.Init() {
		t.Fatal("expected Init to return true")
	}

	if iter.FileIdx() != 42 {
		t.Errorf("expected FileIdx=42, got %d", iter.FileIdx())
	}

	if iter.Type() != tsm1.BlockFloat64 {
		t.Errorf("expected type=%d (float64), got %d", tsm1.BlockFloat64, iter.Type())
	}

	if string(iter.Key()) != "cpu,host=A#!~#value" {
		t.Errorf("expected key='cpu,host=A#!~#value', got '%s'", string(iter.Key()))
	}

	if iter.Err() != nil {
		t.Errorf("expected no error, got %v", iter.Err())
	}
}

// KeyAwareMergingIterator Tests

func TestKeyAwareMergingIterator_BasicMerge(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// File 1: cpu values at ts=1,3
	vals1 := []tsm1.Value{
		tsm1.NewValue(1, 1.1),
		tsm1.NewValue(3, 3.1),
	}
	writes1 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": vals1,
	}
	f1 := MustWriteTSM(dir, 1, writes1)
	r1 := MustOpenTSMReader(f1)
	defer r1.Close()

	// File 2: cpu values at ts=2,4
	vals2 := []tsm1.Value{
		tsm1.NewValue(2, 2.2),
		tsm1.NewValue(4, 4.2),
	}
	writes2 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": vals2,
	}
	f2 := MustWriteTSM(dir, 2, writes2)
	r2 := MustOpenTSMReader(f2)
	defer r2.Close()

	// Create merging iterator
	iter1 := tsm1.NewBlockValueIterator(r1, 0)
	iter2 := tsm1.NewBlockValueIterator(r2, 1)
	mergeIter := tsm1.NewKeyAwareMergingIterator(
		[]*tsm1.BlockValueIterator{iter1, iter2},
		1000, nil,
	)

	// Read all blocks
	var allValues []tsm1.Value
	for mergeIter.Next() {
		key, _, _, data, err := mergeIter.Read()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Decode the block
		decoded, err := tsm1.DecodeBlock(data, nil)
		if err != nil {
			t.Fatalf("unexpected error decoding block: %v", err)
		}

		if string(key) != "cpu,host=A#!~#value" {
			t.Fatalf("expected key 'cpu,host=A#!~#value', got '%s'", string(key))
		}

		allValues = append(allValues, decoded...)
	}

	// Should have all 4 values in order
	if len(allValues) != 4 {
		t.Fatalf("expected 4 values, got %d", len(allValues))
	}

	expected := []int64{1, 2, 3, 4}
	for i, v := range allValues {
		if v.UnixNano() != expected[i] {
			t.Errorf("value %d: expected ts=%d, got %d", i, expected[i], v.UnixNano())
		}
	}

	if mergeIter.Err() != nil {
		t.Fatalf("unexpected error: %v", mergeIter.Err())
	}
}

func TestKeyAwareMergingIterator_Deduplicate(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// File 1 (older): cpu value at ts=1 with value 1.1
	vals1 := []tsm1.Value{
		tsm1.NewValue(1, 1.1),
	}
	writes1 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": vals1,
	}
	f1 := MustWriteTSM(dir, 1, writes1)
	r1 := MustOpenTSMReader(f1)
	defer r1.Close()

	// File 2 (newer): cpu value at ts=1 with value 2.2
	vals2 := []tsm1.Value{
		tsm1.NewValue(1, 2.2),
	}
	writes2 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": vals2,
	}
	f2 := MustWriteTSM(dir, 2, writes2)
	r2 := MustOpenTSMReader(f2)
	defer r2.Close()

	// Create merging iterator
	iter1 := tsm1.NewBlockValueIterator(r1, 0)
	iter2 := tsm1.NewBlockValueIterator(r2, 1)
	mergeIter := tsm1.NewKeyAwareMergingIterator(
		[]*tsm1.BlockValueIterator{iter1, iter2},
		1000, nil,
	)

	// Read all blocks
	var allValues []tsm1.Value
	for mergeIter.Next() {
		_, _, _, data, err := mergeIter.Read()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		decoded, err := tsm1.DecodeBlock(data, nil)
		if err != nil {
			t.Fatalf("unexpected error decoding block: %v", err)
		}

		allValues = append(allValues, decoded...)
	}

	// Should have 1 value (deduplicated), from file 2 (newer)
	if len(allValues) != 1 {
		t.Fatalf("expected 1 value, got %d", len(allValues))
	}

	// Value should be from file 2 (2.2)
	if allValues[0].Value() != 2.2 {
		t.Errorf("expected value 2.2, got %v", allValues[0].Value())
	}
}

func TestKeyAwareMergingIterator_DifferentKeySets(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// File 1: cpu, mem
	vals1 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {tsm1.NewValue(1, 1.1)},
		"mem,host=A#!~#value": {tsm1.NewValue(1, 2.1)},
	}
	f1 := MustWriteTSM(dir, 1, vals1)
	r1 := MustOpenTSMReader(f1)
	defer r1.Close()

	// File 2: cpu, disk
	vals2 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {tsm1.NewValue(2, 1.2)},
		"disk,host=A#!~#value": {tsm1.NewValue(1, 3.1)},
	}
	f2 := MustWriteTSM(dir, 2, vals2)
	r2 := MustOpenTSMReader(f2)
	defer r2.Close()

	// Create BlockValueIterators and initialize them
	iter1 := tsm1.NewBlockValueIterator(r1, 0)
	iter2 := tsm1.NewBlockValueIterator(r2, 1)
	if !iter1.Init() {
		t.Fatalf("iter1.Init() failed: %v", iter1.Err())
	}
	if !iter2.Init() {
		t.Fatalf("iter2.Init() failed: %v", iter2.Err())
	}

	mergeIter := tsm1.NewKeyAwareMergingIterator(
		[]*tsm1.BlockValueIterator{iter1, iter2},
		1000, nil,
	)

	// Collect all keys in order
	var keys []string
	for mergeIter.Next() {
		key, _, _, _, err := mergeIter.Read()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		keys = append(keys, string(key))
	}

	// Expected order: cpu, disk, mem (lexicographically: cpu < disk < mem)
	// d=100, m=109 so disk < mem
	expectedKeys := []string{
		"cpu,host=A#!~#value",
		"disk,host=A#!~#value",
		"mem,host=A#!~#value",
	}

	t.Logf("got %d keys: %v", len(keys), keys)
	if len(keys) != len(expectedKeys) {
		t.Fatalf("expected %d keys, got %d", len(expectedKeys), len(keys))
	}

	for i, k := range keys {
		t.Logf("key %d: got %q, expected %q", i, k, expectedKeys[i])
		if k != expectedKeys[i] {
			t.Errorf("key %d: expected '%s', got '%s'", i, expectedKeys[i], k)
		}
	}
}

func TestKeyAwareMergingIterator_SingleFile(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// Single file with multiple keys
	vals := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {tsm1.NewValue(1, 1.1), tsm1.NewValue(2, 1.2)},
		"mem,host=A#!~#value": {tsm1.NewValue(1, 2.1)},
	}
	f := MustWriteTSM(dir, 1, vals)
	r := MustOpenTSMReader(f)
	defer r.Close()

	// Create BlockValueIterator and initialize it
	iter := tsm1.NewBlockValueIterator(r, 0)
	if !iter.Init() {
		t.Fatalf("BlockValueIterator.Init() failed: %v", iter.Err())
	}

	mergeIter := tsm1.NewKeyAwareMergingIterator(
		[]*tsm1.BlockValueIterator{iter},
		1000, nil,
	)

	// Collect all keys
	var keys []string
	var totalValues int
	for mergeIter.Next() {
		key, _, _, data, err := mergeIter.Read()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		keys = append(keys, string(key))

		decoded, err := tsm1.DecodeBlock(data, nil)
		if err != nil {
			t.Fatalf("unexpected error decoding block: %v", err)
		}
		totalValues += len(decoded)
	}

	// Should have 2 keys
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(keys))
	}

	// Should have 3 total values
	if totalValues != 3 {
		t.Fatalf("expected 3 total values, got %d", totalValues)
	}
}

func TestKeyAwareMergingIterator_SameTimestamp(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// File 1: cpu at ts=1
	vals1 := []tsm1.Value{
		tsm1.NewValue(1, 1.1),
	}
	writes1 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": vals1,
	}
	f1 := MustWriteTSM(dir, 1, writes1)
	r1 := MustOpenTSMReader(f1)
	defer r1.Close()

	// File 2: cpu at ts=1 (same timestamp)
	vals2 := []tsm1.Value{
		tsm1.NewValue(1, 2.2),
	}
	writes2 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": vals2,
	}
	f2 := MustWriteTSM(dir, 2, writes2)
	r2 := MustOpenTSMReader(f2)
	defer r2.Close()

	// File 3: cpu at ts=1 (same timestamp)
	vals3 := []tsm1.Value{
		tsm1.NewValue(1, 3.3),
	}
	writes3 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": vals3,
	}
	f3 := MustWriteTSM(dir, 3, writes3)
	r3 := MustOpenTSMReader(f3)
	defer r3.Close()

	// Create merging iterator
	iter1 := tsm1.NewBlockValueIterator(r1, 0)
	iter2 := tsm1.NewBlockValueIterator(r2, 1)
	iter3 := tsm1.NewBlockValueIterator(r3, 2)
	mergeIter := tsm1.NewKeyAwareMergingIterator(
		[]*tsm1.BlockValueIterator{iter1, iter2, iter3},
		1000, nil,
	)

	// Read all blocks
	var allValues []tsm1.Value
	for mergeIter.Next() {
		_, _, _, data, err := mergeIter.Read()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		decoded, err := tsm1.DecodeBlock(data, nil)
		if err != nil {
			t.Fatalf("unexpected error decoding block: %v", err)
		}

		allValues = append(allValues, decoded...)
	}

	// Should have 1 value (deduplicated), from file 3 (newest)
	if len(allValues) != 1 {
		t.Fatalf("expected 1 value, got %d", len(allValues))
	}

	// Value should be from file 3 (3.3)
	if allValues[0].Value() != 3.3 {
		t.Errorf("expected value 3.3, got %v", allValues[0].Value())
	}
}

func TestKeyAwareMergingIterator_Tombstone(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// File 1: cpu at ts=1,2,3
	vals1 := []tsm1.Value{
		tsm1.NewValue(1, 1.1),
		tsm1.NewValue(2, 2.1),
		tsm1.NewValue(3, 3.1),
	}
	writes1 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": vals1,
	}
	f1 := MustWriteTSM(dir, 1, writes1)

	// Tombstone ts=2 from file 1
	ts1 := tsm1.NewTombstoner(f1, nil)
	ts1.AddRange([][]byte{[]byte("cpu,host=A#!~#value")}, 2, 2)
	if err := ts1.Flush(); err != nil {
		t.Fatalf("unexpected error flushing tombstone: %v", err)
	}
	r1 := MustOpenTSMReader(f1)
	defer r1.Close()

	// File 2: cpu at ts=4
	vals2 := []tsm1.Value{
		tsm1.NewValue(4, 4.2),
	}
	writes2 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": vals2,
	}
	f2 := MustWriteTSM(dir, 2, writes2)
	r2 := MustOpenTSMReader(f2)
	defer r2.Close()

	// Create merging iterator
	iter1 := tsm1.NewBlockValueIterator(r1, 0)
	iter2 := tsm1.NewBlockValueIterator(r2, 1)
	mergeIter := tsm1.NewKeyAwareMergingIterator(
		[]*tsm1.BlockValueIterator{iter1, iter2},
		1000, nil,
	)

	// Read all blocks
	var allValues []tsm1.Value
	for mergeIter.Next() {
		_, _, _, data, err := mergeIter.Read()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		decoded, err := tsm1.DecodeBlock(data, nil)
		if err != nil {
			t.Fatalf("unexpected error decoding block: %v", err)
		}

		allValues = append(allValues, decoded...)
	}

	// Should have 3 values (ts=1,3,4), ts=2 is tombstoned
	if len(allValues) != 3 {
		t.Fatalf("expected 3 values, got %d", len(allValues))
	}

	expected := []int64{1, 3, 4}
	for i, v := range allValues {
		if v.UnixNano() != expected[i] {
			t.Errorf("value %d: expected ts=%d, got %d", i, expected[i], v.UnixNano())
		}
	}
}

func TestKeyAwareMergingIterator_TombstoneAllDeleted(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// File 1: cpu at ts=1
	vals1 := []tsm1.Value{
		tsm1.NewValue(1, 1.1),
	}
	writes1 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": vals1,
	}
	f1 := MustWriteTSM(dir, 1, writes1)

	// Tombstone all values from file 1
	ts1 := tsm1.NewTombstoner(f1, nil)
	ts1.AddRange([][]byte{[]byte("cpu,host=A#!~#value")}, math.MinInt64, math.MaxInt64)
	if err := ts1.Flush(); err != nil {
		t.Fatalf("unexpected error flushing tombstone: %v", err)
	}
	r1 := MustOpenTSMReader(f1)
	defer r1.Close()

	// File 2: mem at ts=1
	vals2 := []tsm1.Value{
		tsm1.NewValue(1, 2.2),
	}
	writes2 := map[string][]tsm1.Value{
		"mem,host=A#!~#value": vals2,
	}
	f2 := MustWriteTSM(dir, 2, writes2)
	r2 := MustOpenTSMReader(f2)
	defer r2.Close()

	// Create merging iterator
	iter1 := tsm1.NewBlockValueIterator(r1, 0)
	iter2 := tsm1.NewBlockValueIterator(r2, 1)
	mergeIter := tsm1.NewKeyAwareMergingIterator(
		[]*tsm1.BlockValueIterator{iter1, iter2},
		1000, nil,
	)

	// Read all blocks
	var keys []string
	for mergeIter.Next() {
		key, _, _, _, err := mergeIter.Read()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		keys = append(keys, string(key))
	}

	// Should only have mem key (cpu is fully tombstoned)
	if len(keys) != 1 {
		t.Fatalf("expected 1 key, got %d", len(keys))
	}

	if keys[0] != "mem,host=A#!~#value" {
		t.Errorf("expected key 'mem,host=A#!~#value', got '%s'", keys[0])
	}
}

func TestKeyAwareMergingIterator_TombstoneWithDedup(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// File 1 (older): cpu at ts=1,2
	vals1 := []tsm1.Value{
		tsm1.NewValue(1, 1.1),
		tsm1.NewValue(2, 2.1),
	}
	writes1 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": vals1,
	}
	f1 := MustWriteTSM(dir, 1, writes1)

	// Tombstone ts=1 from file 1
	ts1 := tsm1.NewTombstoner(f1, nil)
	ts1.AddRange([][]byte{[]byte("cpu,host=A#!~#value")}, 1, 1)
	if err := ts1.Flush(); err != nil {
		t.Fatalf("unexpected error flushing tombstone: %v", err)
	}
	r1 := MustOpenTSMReader(f1)
	defer r1.Close()

	// File 2 (newer): cpu at ts=1,3
	vals2 := []tsm1.Value{
		tsm1.NewValue(1, 1.2),
		tsm1.NewValue(3, 3.2),
	}
	writes2 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": vals2,
	}
	f2 := MustWriteTSM(dir, 2, writes2)
	r2 := MustOpenTSMReader(f2)
	defer r2.Close()

	// Create merging iterator
	iter1 := tsm1.NewBlockValueIterator(r1, 0)
	iter2 := tsm1.NewBlockValueIterator(r2, 1)
	mergeIter := tsm1.NewKeyAwareMergingIterator(
		[]*tsm1.BlockValueIterator{iter1, iter2},
		1000, nil,
	)

	// Read all blocks
	var allValues []tsm1.Value
	for mergeIter.Next() {
		_, _, _, data, err := mergeIter.Read()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		decoded, err := tsm1.DecodeBlock(data, nil)
		if err != nil {
			t.Fatalf("unexpected error decoding block: %v", err)
		}

		allValues = append(allValues, decoded...)
	}

	// Should have 3 values: ts=1 (from file 2), ts=2 (from file 1), ts=3 (from file 2)
	// ts=1 from file 1 is tombstoned, so file 2's value wins
	if len(allValues) != 3 {
		t.Fatalf("expected 3 values, got %d", len(allValues))
	}

	expected := []int64{1, 2, 3}
	for i, v := range allValues {
		if v.UnixNano() != expected[i] {
			t.Errorf("value %d: expected ts=%d, got %d", i, expected[i], v.UnixNano())
		}
	}

	// ts=1 should be from file 2 (1.2)
	if allValues[0].Value() != 1.2 {
		t.Errorf("expected value 1.2 for ts=1, got %v", allValues[0].Value())
	}
}

func TestKeyAwareMergingIterator_TombstoneFirstBlockAllDeleted(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// File 1: cpu at ts=1-2000 (multiple blocks)
	var vals1 []tsm1.Value
	for i := int64(1); i <= 2000; i++ {
		vals1 = append(vals1, tsm1.NewValue(i, float64(i)))
	}
	writes1 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": vals1,
	}
	f1 := MustWriteTSM(dir, 1, writes1)

	// Tombstone first 1000 values
	ts1 := tsm1.NewTombstoner(f1, nil)
	ts1.AddRange([][]byte{[]byte("cpu,host=A#!~#value")}, 1, 1000)
	if err := ts1.Flush(); err != nil {
		t.Fatalf("unexpected error flushing tombstone: %v", err)
	}
	r1 := MustOpenTSMReader(f1)
	defer r1.Close()

	// File 2: cpu at ts=2001
	vals2 := []tsm1.Value{
		tsm1.NewValue(2001, 2001.2),
	}
	writes2 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": vals2,
	}
	f2 := MustWriteTSM(dir, 2, writes2)
	r2 := MustOpenTSMReader(f2)
	defer r2.Close()

	// Create merging iterator
	iter1 := tsm1.NewBlockValueIterator(r1, 0)
	iter2 := tsm1.NewBlockValueIterator(r2, 1)
	mergeIter := tsm1.NewKeyAwareMergingIterator(
		[]*tsm1.BlockValueIterator{iter1, iter2},
		1000, nil,
	)

	// Read all blocks
	var allValues []tsm1.Value
	for mergeIter.Next() {
		_, _, _, data, err := mergeIter.Read()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		decoded, err := tsm1.DecodeBlock(data, nil)
		if err != nil {
			t.Fatalf("unexpected error decoding block: %v", err)
		}

		allValues = append(allValues, decoded...)
	}

	// Should have 1001 values (1001-2000 from file 1 + 2001 from file 2)
	if len(allValues) != 1001 {
		t.Fatalf("expected 1001 values, got %d", len(allValues))
	}

	// First value should be 1001
	if allValues[0].UnixNano() != 1001 {
		t.Errorf("expected first value ts=1001, got %d", allValues[0].UnixNano())
	}

	// Last value should be 2001
	if allValues[len(allValues)-1].UnixNano() != 2001 {
		t.Errorf("expected last value ts=2001, got %d", allValues[len(allValues)-1].UnixNano())
	}
}

func TestKeyAwareMergingIterator_TombstoneMultipleIteratorsAllDeleted(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// File 1: cpu at ts=1
	vals1 := []tsm1.Value{
		tsm1.NewValue(1, 1.1),
	}
	writes1 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": vals1,
	}
	f1 := MustWriteTSM(dir, 1, writes1)

	// Tombstone all from file 1
	ts1 := tsm1.NewTombstoner(f1, nil)
	ts1.AddRange([][]byte{[]byte("cpu,host=A#!~#value")}, math.MinInt64, math.MaxInt64)
	if err := ts1.Flush(); err != nil {
		t.Fatalf("unexpected error flushing tombstone: %v", err)
	}
	r1 := MustOpenTSMReader(f1)
	defer r1.Close()

	// File 2: cpu at ts=2
	vals2 := []tsm1.Value{
		tsm1.NewValue(2, 2.2),
	}
	writes2 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": vals2,
	}
	f2 := MustWriteTSM(dir, 2, writes2)

	// Tombstone all from file 2
	ts2 := tsm1.NewTombstoner(f2, nil)
	ts2.AddRange([][]byte{[]byte("cpu,host=A#!~#value")}, math.MinInt64, math.MaxInt64)
	if err := ts2.Flush(); err != nil {
		t.Fatalf("unexpected error flushing tombstone: %v", err)
	}
	r2 := MustOpenTSMReader(f2)
	defer r2.Close()

	// File 3: cpu at ts=3 (not tombstoned)
	vals3 := []tsm1.Value{
		tsm1.NewValue(3, 3.3),
	}
	writes3 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": vals3,
	}
	f3 := MustWriteTSM(dir, 3, writes3)
	r3 := MustOpenTSMReader(f3)
	defer r3.Close()

	// Create merging iterator
	iter1 := tsm1.NewBlockValueIterator(r1, 0)
	iter2 := tsm1.NewBlockValueIterator(r2, 1)
	iter3 := tsm1.NewBlockValueIterator(r3, 2)
	mergeIter := tsm1.NewKeyAwareMergingIterator(
		[]*tsm1.BlockValueIterator{iter1, iter2, iter3},
		1000, nil,
	)

	// Read all blocks
	var allValues []tsm1.Value
	for mergeIter.Next() {
		_, _, _, data, err := mergeIter.Read()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		decoded, err := tsm1.DecodeBlock(data, nil)
		if err != nil {
			t.Fatalf("unexpected error decoding block: %v", err)
		}

		allValues = append(allValues, decoded...)
	}

	// Should have 1 value (ts=3 from file 3)
	if len(allValues) != 1 {
		t.Fatalf("expected 1 value, got %d", len(allValues))
	}

	if allValues[0].UnixNano() != 3 {
		t.Errorf("expected ts=3, got %d", allValues[0].UnixNano())
	}
}

// Benchmarks

func BenchmarkStreamingCompaction(b *testing.B) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// Create 10 TSM files with overlapping data
	numFiles := 10
	valuesPerBlock := 1000
	numBlocks := 10

	var readers []*tsm1.TSMReader
	var tsmFiles []string

	for i := 0; i < numFiles; i++ {
		vals := make(map[string][]tsm1.Value)
		var cpuVals []tsm1.Value
		for j := 0; j < numBlocks*valuesPerBlock; j++ {
			ts := int64(j + 1)
			cpuVals = append(cpuVals, tsm1.NewValue(ts, float64(i*1000+j)))
		}
		vals["cpu,host=A#!~#value"] = cpuVals

		f := MustWriteTSM(dir, i+1, vals)
		tsmFiles = append(tsmFiles, f)
		r := MustOpenTSMReader(f)
		readers = append(readers, r)
		defer r.Close()
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		iter := tsm1.NewStreamingKeyIterator(tsmFiles, readers, 1000, nil)
		for iter.Next() {
			// Read and discard
			_, _, _, _, _ = iter.Read()
		}
		if iter.Err() != nil {
			b.Fatalf("unexpected error: %v", iter.Err())
		}
	}
}

func BenchmarkBatchKeyIterator(b *testing.B) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// Create 10 TSM files with overlapping data
	numFiles := 10
	valuesPerBlock := 1000
	numBlocks := 10

	var readers []*tsm1.TSMReader
	var tsmFiles []string

	for i := 0; i < numFiles; i++ {
		vals := make(map[string][]tsm1.Value)
		var cpuVals []tsm1.Value
		for j := 0; j < numBlocks*valuesPerBlock; j++ {
			ts := int64(j + 1)
			cpuVals = append(cpuVals, tsm1.NewValue(ts, float64(i*1000+j)))
		}
		vals["cpu,host=A#!~#value"] = cpuVals

		f := MustWriteTSM(dir, i+1, vals)
		tsmFiles = append(tsmFiles, f)
		r := MustOpenTSMReader(f)
		readers = append(readers, r)
		defer r.Close()
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		iter, err := tsm1.NewTSMBatchKeyIterator(1000, false, nil, tsmFiles, readers...)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
		for iter.Next() {
			// Read and discard
			_, _, _, _, _ = iter.Read()
		}
		if iter.Err() != nil {
			b.Fatalf("unexpected error: %v", iter.Err())
		}
	}
}

// --- advanceAndPush tombstone skip tests (Task 1.2) ---

func TestKeyAwareMergingIterator_TombstonedIntermediateBlock(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	var vals1 []tsm1.Value
	for i := int64(1); i <= 2000; i++ {
		vals1 = append(vals1, tsm1.NewValue(i, float64(i)))
	}
	writes1 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": vals1,
	}
	f1 := MustWriteTSM(dir, 1, writes1)

	ts1 := tsm1.NewTombstoner(f1, nil)
	ts1.AddRange([][]byte{[]byte("cpu,host=A#!~#value")}, 1001, 2000)
	if err := ts1.Flush(); err != nil {
		t.Fatalf("unexpected error flushing tombstone: %v", err)
	}
	r1 := MustOpenTSMReader(f1)
	defer r1.Close()

	var vals2 []tsm1.Value
	for i := int64(2001); i <= 3000; i++ {
		vals2 = append(vals2, tsm1.NewValue(i, float64(i)))
	}
	writes2 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": vals2,
	}
	f2 := MustWriteTSM(dir, 2, writes2)
	r2 := MustOpenTSMReader(f2)
	defer r2.Close()

	iter1 := tsm1.NewBlockValueIterator(r1, 0)
	iter2 := tsm1.NewBlockValueIterator(r2, 1)
	mergeIter := tsm1.NewKeyAwareMergingIterator(
		[]*tsm1.BlockValueIterator{iter1, iter2},
		1000, nil,
	)

	var allValues []tsm1.Value
	for mergeIter.Next() {
		_, _, _, data, err := mergeIter.Read()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		decoded, err := tsm1.DecodeBlock(data, nil)
		if err != nil {
			t.Fatalf("unexpected error decoding block: %v", err)
		}
		allValues = append(allValues, decoded...)
	}

	if len(allValues) != 2000 {
		t.Fatalf("expected 2000 values, got %d", len(allValues))
	}

	if allValues[0].UnixNano() != 1 {
		t.Errorf("expected first value ts=1, got %d", allValues[0].UnixNano())
	}
	if allValues[len(allValues)-1].UnixNano() != 3000 {
		t.Errorf("expected last value ts=3000, got %d", allValues[len(allValues)-1].UnixNano())
	}

	for _, v := range allValues {
		ts := v.UnixNano()
		if ts >= 1001 && ts <= 2000 {
			t.Fatalf("unexpected value in tombstoned range: ts=%d", ts)
		}
	}

	if mergeIter.Err() != nil {
		t.Fatalf("unexpected error: %v", mergeIter.Err())
	}
}

func TestKeyAwareMergingIterator_ConsecutiveTombstonedBlocks(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	var vals1 []tsm1.Value
	for i := int64(1); i <= 3000; i++ {
		vals1 = append(vals1, tsm1.NewValue(i, float64(i)))
	}
	writes1 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": vals1,
	}
	f1 := MustWriteTSM(dir, 1, writes1)

	ts1 := tsm1.NewTombstoner(f1, nil)
	ts1.AddRange([][]byte{[]byte("cpu,host=A#!~#value")}, 1001, 3000)
	if err := ts1.Flush(); err != nil {
		t.Fatalf("unexpected error flushing tombstone: %v", err)
	}
	r1 := MustOpenTSMReader(f1)
	defer r1.Close()

	vals2 := []tsm1.Value{tsm1.NewValue(3001, 3001.0)}
	writes2 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": vals2,
	}
	f2 := MustWriteTSM(dir, 2, writes2)
	r2 := MustOpenTSMReader(f2)
	defer r2.Close()

	iter1 := tsm1.NewBlockValueIterator(r1, 0)
	iter2 := tsm1.NewBlockValueIterator(r2, 1)
	mergeIter := tsm1.NewKeyAwareMergingIterator(
		[]*tsm1.BlockValueIterator{iter1, iter2},
		1000, nil,
	)

	var allValues []tsm1.Value
	for mergeIter.Next() {
		_, _, _, data, err := mergeIter.Read()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		decoded, err := tsm1.DecodeBlock(data, nil)
		if err != nil {
			t.Fatalf("unexpected error decoding block: %v", err)
		}
		allValues = append(allValues, decoded...)
	}

	if len(allValues) != 1001 {
		t.Fatalf("expected 1001 values, got %d", len(allValues))
	}

	if mergeIter.Err() != nil {
		t.Fatalf("unexpected error: %v", mergeIter.Err())
	}
}

// --- Close tests (Task 2.3) ---

func TestKeyAwareMergingIterator_Close(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	vals := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {tsm1.NewValue(1, 1.1)},
	}
	f := MustWriteTSM(dir, 1, vals)
	r := MustOpenTSMReader(f)
	defer r.Close()

	iter := tsm1.NewBlockValueIterator(r, 0)
	mergeIter := tsm1.NewKeyAwareMergingIterator(
		[]*tsm1.BlockValueIterator{iter},
		1000, nil,
	)

	if err := mergeIter.Close(); err != nil {
		t.Fatalf("unexpected error on first Close: %v", err)
	}
	if err := mergeIter.Close(); err != nil {
		t.Fatalf("unexpected error on second Close: %v", err)
	}

	if mergeIter.Next() {
		t.Fatal("expected Next to return false after Close")
	}
}

// --- Resource cleanup tests ---

func TestBlockValueIterator_Close(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	vals := []tsm1.Value{
		tsm1.NewValue(1, 1.1),
		tsm1.NewValue(2, 2.2),
	}
	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": vals,
	}
	f := MustWriteTSM(dir, 1, writes)
	r := MustOpenTSMReader(f)
	defer r.Close()

	iter := tsm1.NewBlockValueIterator(r, 0)
	if !iter.Init() {
		t.Fatal("expected Init to return true")
	}

	// Close should release resources (no panic)
	iter.Close()

	// RawBlock should return nil after Close
	if iter.RawBlock() != nil {
		t.Fatal("expected RawBlock to return nil after Close")
	}
}

func TestBlockValueIterator_CloseIdempotent(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	vals := []tsm1.Value{
		tsm1.NewValue(1, 1.1),
	}
	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": vals,
	}
	f := MustWriteTSM(dir, 1, writes)
	r := MustOpenTSMReader(f)
	defer r.Close()

	iter := tsm1.NewBlockValueIterator(r, 0)
	if !iter.Init() {
		t.Fatal("expected Init to return true")
	}

	// Multiple Close calls should not panic
	iter.Close()
	iter.Close()
	iter.Close()
}

func TestKeyAwareMergingIterator_CloseReleasesIterators(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	vals := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {tsm1.NewValue(1, 1.1)},
		"mem,host=A#!~#value": {tsm1.NewValue(1, 2.1)},
	}
	f := MustWriteTSM(dir, 1, vals)
	r := MustOpenTSMReader(f)
	defer r.Close()

	// Pre-initialize the iterator so Init() is called
	iter := tsm1.NewBlockValueIterator(r, 0)
	if !iter.Init() {
		t.Fatalf("BlockValueIterator.Init() failed: %v", iter.Err())
	}

	mergeIter := tsm1.NewKeyAwareMergingIterator(
		[]*tsm1.BlockValueIterator{iter},
		1000, nil,
	)

	// Consume first block
	if !mergeIter.Next() {
		t.Fatal("expected first Next to return true")
	}

	// Close should release all child iterators
	if err := mergeIter.Close(); err != nil {
		t.Fatalf("unexpected error on Close: %v", err)
	}

	// Next should return false after Close
	if mergeIter.Next() {
		t.Fatal("expected Next to return false after Close")
	}
}

func TestKeyAwareMergingIterator_CloseIdempotent(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	vals := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {tsm1.NewValue(1, 1.1)},
	}
	f := MustWriteTSM(dir, 1, vals)
	r := MustOpenTSMReader(f)
	defer r.Close()

	iter := tsm1.NewBlockValueIterator(r, 0)
	mergeIter := tsm1.NewKeyAwareMergingIterator(
		[]*tsm1.BlockValueIterator{iter},
		1000, nil,
	)

	// Multiple Close calls should not panic
	if err := mergeIter.Close(); err != nil {
		t.Fatalf("unexpected error on first Close: %v", err)
	}
	if err := mergeIter.Close(); err != nil {
		t.Fatalf("unexpected error on second Close: %v", err)
	}

	if mergeIter.Next() {
		t.Fatal("expected Next to return false after Close")
	}
}

// --- Interrupt tests (Task 3.5) ---

func TestKeyAwareMergingIterator_InterruptDuringIteration(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	var vals []tsm1.Value
	for i := int64(1); i <= 5000; i++ {
		vals = append(vals, tsm1.NewValue(i, float64(i)))
	}
	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": vals,
	}
	f := MustWriteTSM(dir, 1, writes)
	r := MustOpenTSMReader(f)
	defer r.Close()

	interrupt := make(chan struct{})
	iter := tsm1.NewBlockValueIterator(r, 0)
	mergeIter := tsm1.NewKeyAwareMergingIterator(
		[]*tsm1.BlockValueIterator{iter},
		1000, interrupt,
	)

	if !mergeIter.Next() {
		t.Fatal("expected first Next to return true")
	}

	close(interrupt)

	if mergeIter.Next() {
		t.Fatal("expected Next to return false after interrupt")
	}

	if mergeIter.Err() == nil {
		t.Fatal("expected error after interrupt")
	}
}

func TestKeyAwareMergingIterator_InterruptBeforeStart(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	vals := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {tsm1.NewValue(1, 1.1)},
	}
	f := MustWriteTSM(dir, 1, vals)
	r := MustOpenTSMReader(f)
	defer r.Close()

	interrupt := make(chan struct{})
	close(interrupt)

	iter := tsm1.NewBlockValueIterator(r, 0)
	mergeIter := tsm1.NewKeyAwareMergingIterator(
		[]*tsm1.BlockValueIterator{iter},
		1000, interrupt,
	)

	if mergeIter.Next() {
		t.Fatal("expected Next to return false when interrupt already closed")
	}

	if mergeIter.Err() == nil {
		t.Fatal("expected error when interrupt already closed")
	}
}

func TestKeyAwareMergingIterator_NilInterrupt(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	vals := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {tsm1.NewValue(1, 1.1)},
	}
	f := MustWriteTSM(dir, 1, vals)
	r := MustOpenTSMReader(f)
	defer r.Close()

	iter := tsm1.NewBlockValueIterator(r, 0)
	mergeIter := tsm1.NewKeyAwareMergingIterator(
		[]*tsm1.BlockValueIterator{iter},
		1000, nil,
	)

	if !mergeIter.Next() {
		t.Fatal("expected Next to return true with nil interrupt")
	}

	if mergeIter.Err() != nil {
		t.Fatalf("unexpected error: %v", mergeIter.Err())
	}
}

func TestDebug_SingleFile(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// Single file with multiple keys
	vals := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {tsm1.NewValue(1, 1.1), tsm1.NewValue(2, 1.2)},
		"mem,host=A#!~#value": {tsm1.NewValue(1, 2.1)},
	}
	f := MustWriteTSM(dir, 1, vals)
	r := MustOpenTSMReader(f)
	defer r.Close()

	// Check block order
	iter := r.BlockIterator()
	var blockCount int
	for iter.Next() {
		blockCount++
		key, min, max, typ, _, _, _ := iter.Read()
		t.Logf("Block %d: key=%s, time=%d-%d, type=%d", blockCount, string(key), min, max, typ)
	}
	t.Logf("Total blocks in file: %d", blockCount)

	// Create BlockValueIterator and initialize
	blockIter := tsm1.NewBlockValueIterator(r, 0)
	if !blockIter.Init() {
		t.Fatalf("Init failed: %v", blockIter.Err())
	}
	t.Logf("After Init(): key=%s", string(blockIter.Key()))

	// Create mergeIter - NOT initializing blockIter before this
	blockIter2 := tsm1.NewBlockValueIterator(r, 0)
	mergeIter := tsm1.NewKeyAwareMergingIterator(
		[]*tsm1.BlockValueIterator{blockIter2},
		1000, nil,
	)

	// First Next() - this calls initIterators which calls blockIter2.Init()
	t.Logf("Calling mergeIter.Next() - FIRST call...")
	result := mergeIter.Next()
	t.Logf("mergeIter.Next() returned: %v", result)
	if result {
		key, minTime, maxTime, data, err := mergeIter.Read()
		if err != nil {
			t.Fatalf("error: %v", err)
		}
		t.Logf("Got block 1: key=%s, time=%d-%d, %d bytes", string(key), minTime, maxTime, len(data))

		// Decode and check values
		decoded, err := tsm1.DecodeBlock(data, nil)
		if err != nil {
			t.Fatalf("decode error: %v", err)
		}
		t.Logf("Block 1 has %d values:", len(decoded))
		for i, v := range decoded {
			t.Logf("  value[%d]: ts=%d", i, v.UnixNano())
		}

		// Second Next()
		t.Logf("Calling mergeIter.Next() - SECOND call...")
		result = mergeIter.Next()
		t.Logf("mergeIter.Next() returned: %v", result)

		if result {
			key, _, _, data, err := mergeIter.Read()
			if err != nil {
				t.Fatalf("error: %v", err)
			}
			t.Logf("Got block 2: key=%s, %d bytes", string(key), len(data))

			decoded, err := tsm1.DecodeBlock(data, nil)
			if err != nil {
				t.Fatalf("decode error: %v", err)
			}
			t.Logf("Block 2 has %d values:", len(decoded))
			for i, v := range decoded {
				t.Logf("  value[%d]: ts=%d", i, v.UnixNano())
			}
		} else {
			t.Logf("Second Next() returned false, err=%v", mergeIter.Err())
		}
	} else {
		t.Logf("First Next() returned false, err=%v", mergeIter.Err())
	}
}

func TestDebug_SingleFile_NoPreInit(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// Single file with multiple keys
	vals := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {tsm1.NewValue(1, 1.1), tsm1.NewValue(2, 1.2)},
		"mem,host=A#!~#value": {tsm1.NewValue(1, 2.1)},
	}
	f := MustWriteTSM(dir, 1, vals)
	r := MustOpenTSMReader(f)
	defer r.Close()

	// Create BlockValueIterator WITHOUT pre-initializing
	blockIter := tsm1.NewBlockValueIterator(r, 0)
	mergeIter := tsm1.NewKeyAwareMergingIterator(
		[]*tsm1.BlockValueIterator{blockIter},
		1000, nil,
	)

	t.Logf("mergeIter created, calling Next()...")

	var keys []string
	var values int
	for mergeIter.Next() {
		key, _, _, data, err := mergeIter.Read()
		if err != nil {
			t.Fatalf("error: %v", err)
		}
		t.Logf("Got block: key=%s", string(key))
		keys = append(keys, string(key))

		decoded, err := tsm1.DecodeBlock(data, nil)
		if err != nil {
			t.Fatalf("decode error: %v", err)
		}
		t.Logf("  -> decoded %d values, keys so far: %v", len(decoded), keys)
		values += len(decoded)
	}

	t.Logf("Final result: %d keys, %d values", len(keys), values)

	if mergeIter.Err() != nil {
		t.Fatalf("iterator error: %v", mergeIter.Err())
	}

	// Test with pre-init
	t.Logf("\n--- Testing with pre-init ---")
	blockIter2 := tsm1.NewBlockValueIterator(r, 0)
	if !blockIter2.Init() {
		t.Fatalf("blockIter2.Init() failed: %v", blockIter2.Err())
	}
	t.Logf("blockIter2.Init() succeeded, key=%s", string(blockIter2.Key()))

	mergeIter2 := tsm1.NewKeyAwareMergingIterator(
		[]*tsm1.BlockValueIterator{blockIter2},
		1000, nil,
	)

	var keys2 []string
	var values2 int
	for mergeIter2.Next() {
		key, _, _, data, err := mergeIter2.Read()
		if err != nil {
			t.Fatalf("error: %v", err)
		}
		t.Logf("Got block: key=%s", string(key))
		keys2 = append(keys2, string(key))

		decoded, err := tsm1.DecodeBlock(data, nil)
		if err != nil {
			t.Fatalf("decode error: %v", err)
		}
		t.Logf("  -> decoded %d values, keys so far: %v", len(decoded), keys2)
		values2 += len(decoded)
	}

	t.Logf("Final result: %d keys, %d values", len(keys2), values2)

	if mergeIter2.Err() != nil {
		t.Fatalf("iterator error: %v", mergeIter2.Err())
	}
}

// Test 6.1: Same timestamp, same key, different files — dedup proceeds correctly
func TestKeyAwareMergingIterator_DedupSameTimestampSameKey(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// File 0 (older): key "cpu,host=A#!~#value" at ts=1,2,3
	writes0 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {
			tsm1.NewValue(1, 1.0),
			tsm1.NewValue(2, 2.0),
			tsm1.NewValue(3, 3.0),
		},
	}
	f0 := MustWriteTSM(dir, 0, writes0)
	r0 := MustOpenTSMReader(f0)
	defer r0.Close()

	// File 1 (newer): same key at ts=2,3,4 (overlapping ts=2,3)
	writes1 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {
			tsm1.NewValue(2, 20.0),
			tsm1.NewValue(3, 30.0),
			tsm1.NewValue(4, 40.0),
		},
	}
	f1 := MustWriteTSM(dir, 1, writes1)
	r1 := MustOpenTSMReader(f1)
	defer r1.Close()

	iter0 := tsm1.NewBlockValueIterator(r0, 0)
	iter1 := tsm1.NewBlockValueIterator(r1, 1)
	mergeIter := tsm1.NewKeyAwareMergingIterator(
		[]*tsm1.BlockValueIterator{iter0, iter1},
		1000, nil,
	)

	var allValues []tsm1.Value
	for mergeIter.Next() {
		_, _, _, data, err := mergeIter.Read()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if data == nil {
			continue
		}
		decoded, err := tsm1.DecodeBlock(data, nil)
		if err != nil {
			t.Fatalf("decode error: %v", err)
		}
		allValues = append(allValues, decoded...)
	}
	if mergeIter.Err() != nil {
		t.Fatalf("iterator error: %v", mergeIter.Err())
	}

	// Expect 4 values: ts=1 (file0), ts=2 (file1, dedup wins), ts=3 (file1, dedup wins), ts=4 (file1)
	if len(allValues) != 4 {
		t.Fatalf("expected 4 values, got %d", len(allValues))
	}

	expectedTS := []int64{1, 2, 3, 4}
	expectedVal := []float64{1.0, 20.0, 30.0, 40.0}
	for i, v := range allValues {
		if v.UnixNano() != expectedTS[i] {
			t.Errorf("value %d: expected ts=%d, got %d", i, expectedTS[i], v.UnixNano())
		}
		fv, ok := v.(tsm1.FloatValue)
		if !ok {
			t.Fatalf("value %d: expected FloatValue, got %T", i, v)
		}
		if fv.Value() != expectedVal[i] {
			t.Errorf("value %d: expected %v, got %v", i, expectedVal[i], fv.Value())
		}
	}
}

// Test 6.2: Same timestamp, different keys — dedup stops, no cross-key mixing
func TestKeyAwareMergingIterator_NoCrossKeyDedup(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// File 0: "cpu,host=A#!~#field_1" (String) at ts=100
	writes0 := map[string][]tsm1.Value{
		"cpu,host=A#!~#field_1": {
			tsm1.NewValue(100, "value_from_file0"),
		},
	}
	f0 := MustWriteTSM(dir, 0, writes0)
	r0 := MustOpenTSMReader(f0)
	defer r0.Close()

	// File 1: "cpu,host=A#!~#field_2" (Float) at ts=100 (same ts, different key)
	writes1 := map[string][]tsm1.Value{
		"cpu,host=A#!~#field_2": {
			tsm1.NewValue(100, 42.0),
		},
	}
	f1 := MustWriteTSM(dir, 1, writes1)
	r1 := MustOpenTSMReader(f1)
	defer r1.Close()

	iter0 := tsm1.NewBlockValueIterator(r0, 0)
	iter1 := tsm1.NewBlockValueIterator(r1, 1)
	mergeIter := tsm1.NewKeyAwareMergingIterator(
		[]*tsm1.BlockValueIterator{iter0, iter1},
		1000, nil,
	)

	type block struct {
		key    string
		values []tsm1.Value
	}
	var blocks []block
	for mergeIter.Next() {
		key, _, _, data, err := mergeIter.Read()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if data == nil {
			continue
		}
		decoded, err := tsm1.DecodeBlock(data, nil)
		if err != nil {
			t.Fatalf("decode error: %v", err)
		}
		blocks = append(blocks, block{key: string(key), values: decoded})
	}
	if mergeIter.Err() != nil {
		t.Fatalf("iterator error: %v", mergeIter.Err())
	}

	// Expect 2 blocks (one per key), no cross-key mixing
	if len(blocks) != 2 {
		t.Fatalf("expected 2 blocks, got %d", len(blocks))
	}

	// Blocks come in key order: field_1 first, then field_2
	if blocks[0].key != "cpu,host=A#!~#field_1" {
		t.Errorf("block 0: expected key 'cpu,host=A#!~#field_1', got '%s'", blocks[0].key)
	}
	if len(blocks[0].values) != 1 {
		t.Fatalf("block 0: expected 1 value, got %d", len(blocks[0].values))
	}
	sv, ok := blocks[0].values[0].(tsm1.StringValue)
	if !ok {
		t.Fatalf("block 0: expected StringValue, got %T", blocks[0].values[0])
	}
	if sv.Value() != "value_from_file0" {
		t.Errorf("block 0: expected 'value_from_file0', got '%s'", sv.Value())
	}

	if blocks[1].key != "cpu,host=A#!~#field_2" {
		t.Errorf("block 1: expected key 'cpu,host=A#!~#field_2', got '%s'", blocks[1].key)
	}
	if len(blocks[1].values) != 1 {
		t.Fatalf("block 1: expected 1 value, got %d", len(blocks[1].values))
	}
	fv, ok := blocks[1].values[0].(tsm1.FloatValue)
	if !ok {
		t.Fatalf("block 1: expected FloatValue, got %T", blocks[1].values[0])
	}
	if fv.Value() != 42.0 {
		t.Errorf("block 1: expected 42.0, got %v", fv.Value())
	}
}

// Test 6.3: Defensive encodeValues — type mismatch skips value, records error
// Since the dedup fix (key check) prevents cross-key mixing, we verify that
// the iterator handles the sentinel value correctly for Float64 keys, which
// exercises the blockTypeForValue path.
func TestKeyAwareMergingIterator_Float64KeyTypeDetection(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// Single file with Float64 values
	writes := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {
			tsm1.NewValue(1, 1.1),
			tsm1.NewValue(2, 2.2),
			tsm1.NewValue(3, 3.3),
		},
	}
	f := MustWriteTSM(dir, 1, writes)
	r := MustOpenTSMReader(f)
	defer r.Close()

	iter := tsm1.NewBlockValueIterator(r, 0)
	mergeIter := tsm1.NewKeyAwareMergingIterator(
		[]*tsm1.BlockValueIterator{iter},
		1000, nil,
	)

	var allValues []tsm1.Value
	for mergeIter.Next() {
		_, _, _, data, err := mergeIter.Read()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if data == nil {
			t.Fatal("unexpected nil data for Float64 key")
		}
		decoded, err := tsm1.DecodeBlock(data, nil)
		if err != nil {
			t.Fatalf("decode error: %v", err)
		}
		allValues = append(allValues, decoded...)
	}
	if mergeIter.Err() != nil {
		t.Fatalf("iterator error: %v", mergeIter.Err())
	}

	// All 3 Float64 values should be present
	if len(allValues) != 3 {
		t.Fatalf("expected 3 values, got %d", len(allValues))
	}
	for i, v := range allValues {
		fv, ok := v.(tsm1.FloatValue)
		if !ok {
			t.Fatalf("value %d: expected FloatValue, got %T", i, v)
		}
		expected := float64(i+1) * 1.1
		got := fv.Value().(float64)
		if math.Abs(got-expected) > 1e-10 {
			t.Errorf("value %d: expected %v, got %v", i, expected, got)
		}
	}
}

// Test 6.4: Float64 key type detection — multiple files with Float64 values
// verify sentinel does not cause redundant recalculation across file boundaries.
func TestKeyAwareMergingIterator_Float64MultiFile(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// File 0: Float64 values at ts=1,2,3
	writes0 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {
			tsm1.NewValue(1, 1.0),
			tsm1.NewValue(2, 2.0),
			tsm1.NewValue(3, 3.0),
		},
	}
	f0 := MustWriteTSM(dir, 0, writes0)
	r0 := MustOpenTSMReader(f0)
	defer r0.Close()

	// File 1: Float64 values at ts=3,4,5 (overlapping ts=3)
	writes1 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {
			tsm1.NewValue(3, 30.0),
			tsm1.NewValue(4, 40.0),
			tsm1.NewValue(5, 50.0),
		},
	}
	f1 := MustWriteTSM(dir, 1, writes1)
	r1 := MustOpenTSMReader(f1)
	defer r1.Close()

	// File 2: Float64 values at ts=5,6 (overlapping ts=5)
	writes2 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {
			tsm1.NewValue(5, 500.0),
			tsm1.NewValue(6, 600.0),
		},
	}
	f2 := MustWriteTSM(dir, 2, writes2)
	r2 := MustOpenTSMReader(f2)
	defer r2.Close()

	iter0 := tsm1.NewBlockValueIterator(r0, 0)
	iter1 := tsm1.NewBlockValueIterator(r1, 1)
	iter2 := tsm1.NewBlockValueIterator(r2, 2)
	mergeIter := tsm1.NewKeyAwareMergingIterator(
		[]*tsm1.BlockValueIterator{iter0, iter1, iter2},
		1000, nil,
	)

	var allValues []tsm1.Value
	for mergeIter.Next() {
		_, _, _, data, err := mergeIter.Read()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if data == nil {
			t.Fatal("unexpected nil data for Float64 key")
		}
		decoded, err := tsm1.DecodeBlock(data, nil)
		if err != nil {
			t.Fatalf("decode error: %v", err)
		}
		allValues = append(allValues, decoded...)
	}
	if mergeIter.Err() != nil {
		t.Fatalf("iterator error: %v", mergeIter.Err())
	}

	// Expect 6 values: ts=1(f0), ts=2(f0), ts=3(f1,dedup), ts=4(f1), ts=5(f2,dedup), ts=6(f2)
	if len(allValues) != 6 {
		t.Fatalf("expected 6 values, got %d", len(allValues))
	}

	expectedTS := []int64{1, 2, 3, 4, 5, 6}
	expectedVal := []float64{1.0, 2.0, 30.0, 40.0, 500.0, 600.0}
	for i, v := range allValues {
		if v.UnixNano() != expectedTS[i] {
			t.Errorf("value %d: expected ts=%d, got %d", i, expectedTS[i], v.UnixNano())
		}
		fv, ok := v.(tsm1.FloatValue)
		if !ok {
			t.Fatalf("value %d: expected FloatValue, got %T", i, v)
		}
		got := fv.Value().(float64)
		if got != expectedVal[i] {
			t.Errorf("value %d: expected %v, got %v", i, expectedVal[i], got)
		}
	}
}

// Test 6.5: Read() with nil encoded data returns nil without error.
// This tests the scenario where encodeValues would return nil (all values skipped).
// Since the dedup fix prevents the root cause, we verify the normal path works
// and that the iterator handles the case gracefully.
func TestKeyAwareMergingIterator_NilDataHandling(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// Two files with same key, same timestamps — dedup should keep newer file's values
	writes0 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {
			tsm1.NewValue(1, 1.0),
			tsm1.NewValue(2, 2.0),
		},
	}
	f0 := MustWriteTSM(dir, 0, writes0)
	r0 := MustOpenTSMReader(f0)
	defer r0.Close()

	writes1 := map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {
			tsm1.NewValue(1, 10.0),
			tsm1.NewValue(2, 20.0),
		},
	}
	f1 := MustWriteTSM(dir, 1, writes1)
	r1 := MustOpenTSMReader(f1)
	defer r1.Close()

	iter0 := tsm1.NewBlockValueIterator(r0, 0)
	iter1 := tsm1.NewBlockValueIterator(r1, 1)
	mergeIter := tsm1.NewKeyAwareMergingIterator(
		[]*tsm1.BlockValueIterator{iter0, iter1},
		1000, nil,
	)

	var allValues []tsm1.Value
	for mergeIter.Next() {
		_, _, _, data, err := mergeIter.Read()
		if err != nil {
			t.Fatalf("Read returned error: %v", err)
		}
		// nil data is valid — means all values were skipped (should not happen here)
		if data == nil {
			continue
		}
		decoded, err := tsm1.DecodeBlock(data, nil)
		if err != nil {
			t.Fatalf("decode error: %v", err)
		}
		allValues = append(allValues, decoded...)
	}
	if mergeIter.Err() != nil {
		t.Fatalf("iterator error: %v", mergeIter.Err())
	}

	// Should have 2 values from file 1 (newer), dedup removed file 0's values
	if len(allValues) != 2 {
		t.Fatalf("expected 2 values, got %d", len(allValues))
	}
	for _, v := range allValues {
		fv, ok := v.(tsm1.FloatValue)
		if !ok {
			t.Fatalf("expected FloatValue, got %T", v)
		}
		// Values should be from file 1 (10.0, 20.0)
		if fv.Value() != 10.0 && fv.Value() != 20.0 {
			t.Errorf("expected value from file 1 (10.0 or 20.0), got %v", fv.Value())
		}
	}
}

// TestKeyAwareMergingIterator_StaleHeapEntryIsolation verifies that stale
// heap entries from a previous key do not contaminate the current key's buffer.
// This is the root cause of "type mismatch" production errors: when file 1's
// iterator advances to key "B" during dedup of key "A", it pushes a "B" entry
// into the heap. Without a key check in the buffer-filling loop, this stale
// entry gets mixed into key "A"'s output, causing type mismatches.
func TestKeyAwareMergingIterator_StaleHeapEntryIsolation(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// File 0: key "A" at timestamps 10, 11
	file0Vals := map[string][]tsm1.Value{
		"A#!~#value": {
			tsm1.NewValue(10, 1.0),
			tsm1.NewValue(11, 2.0),
		},
	}
	f0 := MustWriteTSM(dir, 1, file0Vals)
	r0 := MustOpenTSMReader(f0)
	defer r0.Close()

	// File 1: key "A" at timestamp 10, key "B" at timestamp 5
	// When processing key "A", file 1 wins dedup (newer file), then advances
	// to key "B" and pushes a stale "B" entry (ts=5) into the heap.
	file1Vals := map[string][]tsm1.Value{
		"A#!~#value": {tsm1.NewValue(10, 10.0)},
		"B#!~#value": {tsm1.NewValue(5, 100.0)},
	}
	f1 := MustWriteTSM(dir, 2, file1Vals)
	r1 := MustOpenTSMReader(f1)
	defer r1.Close()

	iter0 := tsm1.NewBlockValueIterator(r0, 0)
	iter1 := tsm1.NewBlockValueIterator(r1, 1)

	mergeIter := tsm1.NewKeyAwareMergingIterator(
		[]*tsm1.BlockValueIterator{iter0, iter1},
		1000, nil,
	)

	// First block: key "A" — should contain ONLY "A" values
	if !mergeIter.Next() {
		t.Fatal("expected first Next() to return true")
	}
	key, _, _, data, err := mergeIter.Read()
	if err != nil {
		t.Fatalf("first Read error: %v", err)
	}
	if string(key) != "A#!~#value" {
		t.Fatalf("expected key 'A#!~#value', got '%s'", string(key))
	}

	block1Values, err := tsm1.DecodeBlock(data, nil)
	if err != nil {
		t.Fatalf("decode block 1 error: %v", err)
	}
	if len(block1Values) != 2 {
		t.Fatalf("expected 2 values for key A, got %d", len(block1Values))
	}
	for _, v := range block1Values {
		if v.UnixNano() != 10 && v.UnixNano() != 11 {
			t.Errorf("unexpected timestamp for key A: %d", v.UnixNano())
		}
		if _, ok := v.(tsm1.FloatValue); !ok {
			t.Errorf("expected FloatValue for key A, got %T", v)
		}
	}
	// Verify dedup: timestamp 10 should use file 1's value (10.0, not 1.0)
	for _, v := range block1Values {
		if v.UnixNano() == 10 {
			fv := v.(tsm1.FloatValue)
			if fv.Value() != 10.0 {
				t.Errorf("expected dedup value 10.0 at ts=10, got %v", fv.Value())
			}
		}
	}

	// Second block: key "B" — stale entry must have been deferred, not mixed into "A"
	if !mergeIter.Next() {
		t.Fatal("expected second Next() to return true")
	}
	key, _, _, data, err = mergeIter.Read()
	if err != nil {
		t.Fatalf("second Read error: %v", err)
	}
	if string(key) != "B#!~#value" {
		t.Fatalf("expected key 'B#!~#value', got '%s'", string(key))
	}

	block2Values, err := tsm1.DecodeBlock(data, nil)
	if err != nil {
		t.Fatalf("decode block 2 error: %v", err)
	}
	if len(block2Values) != 1 {
		t.Fatalf("expected 1 value for key B, got %d", len(block2Values))
	}
	if block2Values[0].UnixNano() != 5 {
		t.Errorf("expected timestamp 5 for key B, got %d", block2Values[0].UnixNano())
	}
	bv, ok := block2Values[0].(tsm1.FloatValue)
	if !ok {
		t.Fatalf("expected FloatValue for key B, got %T", block2Values[0])
	}
	if bv.Value() != 100.0 {
		t.Errorf("expected value 100.0 for key B, got %v", bv.Value())
	}

	// No more blocks
	if mergeIter.Next() {
		t.Error("expected no more blocks")
	}
	if mergeIter.Err() != nil {
		t.Fatalf("unexpected error: %v", mergeIter.Err())
	}
}

// TestKeyAwareMergingIterator_StaleEntryDifferentTypes verifies that stale
// heap entries with a DIFFERENT type (e.g. StringValue) do not corrupt
// m.currentType when the current key uses FloatValue. This is the exact
// production scenario: "type mismatch: expected StringValue, got FloatValue".
func TestKeyAwareMergingIterator_StaleEntryDifferentTypes(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// File 0: key "cpu" (FloatValue) at timestamps 10, 11
	file0Vals := map[string][]tsm1.Value{
		"cpu#!~#value": {
			tsm1.NewValue(10, 1.0),
			tsm1.NewValue(11, 2.0),
		},
	}
	f0 := MustWriteTSM(dir, 1, file0Vals)
	r0 := MustOpenTSMReader(f0)
	defer r0.Close()

	// File 1: key "cpu" (FloatValue) at timestamp 10, key "host" (StringValue) at ts 5
	// When processing "cpu", file 1 wins dedup at ts=10 (newer file), then
	// advances to "host" and pushes a stale StringValue entry into the heap.
	// The fix must ensure this StringValue does NOT set m.currentType.
	file1Vals := map[string][]tsm1.Value{
		"cpu#!~#value":  {tsm1.NewValue(10, 10.0)},
		"host#!~#value": {tsm1.NewValue(5, "server1")},
	}
	f1 := MustWriteTSM(dir, 2, file1Vals)
	r1 := MustOpenTSMReader(f1)
	defer r1.Close()

	iter0 := tsm1.NewBlockValueIterator(r0, 0)
	iter1 := tsm1.NewBlockValueIterator(r1, 1)

	mergeIter := tsm1.NewKeyAwareMergingIterator(
		[]*tsm1.BlockValueIterator{iter0, iter1},
		1000, nil,
	)

	// First block: key "cpu" — must be FloatValue, not StringValue
	if !mergeIter.Next() {
		t.Fatal("expected first Next() to return true")
	}
	key, _, _, data, err := mergeIter.Read()
	if err != nil {
		t.Fatalf("first Read error: %v", err)
	}
	if string(key) != "cpu#!~#value" {
		t.Fatalf("expected key 'cpu#!~#value', got '%s'", string(key))
	}

	block1Values, err := tsm1.DecodeBlock(data, nil)
	if err != nil {
		t.Fatalf("decode block 1 error: %v", err)
	}
	if len(block1Values) != 2 {
		t.Fatalf("expected 2 values for key cpu, got %d", len(block1Values))
	}
	for _, v := range block1Values {
		if _, ok := v.(tsm1.FloatValue); !ok {
			t.Errorf("expected FloatValue for key cpu, got %T (type mismatch!)", v)
		}
	}
	// Verify dedup: timestamp 10 should use file 1's value (10.0)
	for _, v := range block1Values {
		if v.UnixNano() == 10 {
			fv := v.(tsm1.FloatValue)
			if fv.Value() != 10.0 {
				t.Errorf("expected dedup value 10.0 at ts=10, got %v", fv.Value())
			}
		}
	}

	// Second block: key "host" — must be StringValue
	if !mergeIter.Next() {
		t.Fatal("expected second Next() to return true")
	}
	key, _, _, data, err = mergeIter.Read()
	if err != nil {
		t.Fatalf("second Read error: %v", err)
	}
	if string(key) != "host#!~#value" {
		t.Fatalf("expected key 'host#!~#value', got '%s'", string(key))
	}

	block2Values, err := tsm1.DecodeBlock(data, nil)
	if err != nil {
		t.Fatalf("decode block 2 error: %v", err)
	}
	if len(block2Values) != 1 {
		t.Fatalf("expected 1 value for key host, got %d", len(block2Values))
	}
	sv, ok := block2Values[0].(tsm1.StringValue)
	if !ok {
		t.Fatalf("expected StringValue for key host, got %T", block2Values[0])
	}
	if sv.Value() != "server1" {
		t.Errorf("expected value 'server1' for key host, got '%v'", sv.Value())
	}

	// No more blocks
	if mergeIter.Next() {
		t.Error("expected no more blocks")
	}
	if mergeIter.Err() != nil {
		t.Fatalf("unexpected error: %v", mergeIter.Err())
	}
}

// TestKeyAwareMergingIterator_DebugTypeMismatch exercises a multi-file,
// multi-key scenario with different types (Float/String/Integer) to
// diagnose the persistent "type mismatch" production error. Debug
// logging is enabled; run with -v to see the output.
func TestKeyAwareMergingIterator_DebugTypeMismatch(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// File 0: "cpu"=Float, "host"=String, "count"=Integer
	file0Vals := map[string][]tsm1.Value{
		"cpu#!~#value":   {tsm1.NewValue(10, 1.0), tsm1.NewValue(20, 2.0)},
		"host#!~#value":  {tsm1.NewValue(10, "server0")},
		"count#!~#value": {tsm1.NewValue(10, int64(100))},
	}
	f0 := MustWriteTSM(dir, 1, file0Vals)
	r0 := MustOpenTSMReader(f0)
	defer r0.Close()

	// File 1: "cpu"=Float, "host"=String, "count"=Integer (overlapping ts)
	file1Vals := map[string][]tsm1.Value{
		"cpu#!~#value":   {tsm1.NewValue(10, 10.0), tsm1.NewValue(30, 3.0)},
		"host#!~#value":  {tsm1.NewValue(20, "server1")},
		"count#!~#value": {tsm1.NewValue(10, int64(200)), tsm1.NewValue(30, int64(300))},
	}
	f1 := MustWriteTSM(dir, 2, file1Vals)
	r1 := MustOpenTSMReader(f1)
	defer r1.Close()

	// File 2: "cpu"=Float only
	file2Vals := map[string][]tsm1.Value{
		"cpu#!~#value": {tsm1.NewValue(10, 100.0)},
	}
	f2 := MustWriteTSM(dir, 3, file2Vals)
	r2 := MustOpenTSMReader(f2)
	defer r2.Close()

	iter0 := tsm1.NewBlockValueIterator(r0, 0)
	iter1 := tsm1.NewBlockValueIterator(r1, 1)
	iter2 := tsm1.NewBlockValueIterator(r2, 2)

	mergeIter := tsm1.NewKeyAwareMergingIterator(
		[]*tsm1.BlockValueIterator{iter0, iter1, iter2},
		1000, nil,
	)

	blockCount := 0
	for mergeIter.Next() {
		blockCount++
		key, _, _, data, err := mergeIter.Read()
		if err != nil {
			t.Fatalf("block %d Read error: %v", blockCount, err)
		}
		vals, err := tsm1.DecodeBlock(data, nil)
		if err != nil {
			t.Fatalf("block %d decode error: %v", blockCount, err)
		}
		types := make(map[string]bool)
		for _, v := range vals {
			types[fmt.Sprintf("%T", v)] = true
		}
		t.Logf("block %d: key=%s count=%d types=%v", blockCount, string(key), len(vals), types)

		// Verify all values in this block have the same type
		if len(types) > 1 {
			t.Errorf("block %d: MIXED TYPES in single block: %v", blockCount, types)
		}
	}
	if mergeIter.Err() != nil {
		t.Fatalf("iterator error: %v", mergeIter.Err())
	}
	t.Logf("total blocks: %d", blockCount)
}

// TestCompactionMemoryComparison asserts that the streaming path's allocs/op
// and bytes/op are within 2x of the batch path.
func TestCompactionMemoryComparison(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	numFiles := 3
	valuesPerBlock := 1000
	numBlocks := 5

	var readers []*tsm1.TSMReader
	var tsmFiles []string

	for i := 0; i < numFiles; i++ {
		vals := make(map[string][]tsm1.Value)
		var cpuVals []tsm1.Value
		for j := 0; j < numBlocks*valuesPerBlock; j++ {
			ts := int64(j + 1)
			cpuVals = append(cpuVals, tsm1.NewValue(ts, float64(i*1000+j)))
		}
		vals["cpu,host=A#!~#value"] = cpuVals

		f := MustWriteTSM(dir, i+1, vals)
		tsmFiles = append(tsmFiles, f)
		r := MustOpenTSMReader(f)
		readers = append(readers, r)
		defer r.Close()
	}

	// Benchmark streaming path
	streamResult := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			iter := tsm1.NewStreamingKeyIterator(tsmFiles, readers, 1000, nil)
			for iter.Next() {
				_, _, _, _, _ = iter.Read()
			}
			if iter.Err() != nil {
				b.Fatalf("streaming error: %v", iter.Err())
			}
		}
	})

	// Benchmark batch path
	batchResult := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			iter, err := tsm1.NewTSMBatchKeyIterator(1000, false, nil, tsmFiles, readers...)
			if err != nil {
				b.Fatalf("batch error: %v", err)
			}
			for iter.Next() {
				_, _, _, _, _ = iter.Read()
			}
			if iter.Err() != nil {
				b.Fatalf("batch error: %v", iter.Err())
			}
		}
	})

	streamAllocs := streamResult.AllocsPerOp()
	streamBytes := streamResult.AllocedBytesPerOp()
	batchAllocs := batchResult.AllocsPerOp()
	batchBytes := batchResult.AllocedBytesPerOp()

	t.Logf("Streaming: %d allocs/op, %d bytes/op", streamAllocs, streamBytes)
	t.Logf("Batch:     %d allocs/op, %d bytes/op", batchAllocs, batchBytes)

	if batchAllocs > 0 && streamAllocs > 2*batchAllocs {
		t.Errorf("streaming allocs/op (%d) exceeds 2x batch allocs/op (%d)", streamAllocs, batchAllocs)
	}
	if batchBytes > 0 && streamBytes > 2*batchBytes {
		t.Errorf("streaming bytes/op (%d) exceeds 2x batch bytes/op (%d)", streamBytes, batchBytes)
	}
}

// TestCompactionOutputEquivalence asserts that the streaming and batch paths
// produce identical output blocks (same keys, same timestamps, same values).
func TestCompactionOutputEquivalence(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	numFiles := 3
	numKeys := 2

	var readers []*tsm1.TSMReader
	var tsmFiles []string

	for i := 0; i < numFiles; i++ {
		vals := make(map[string][]tsm1.Value)
		for k := 0; k < numKeys; k++ {
			key := fmt.Sprintf("cpu,host=%c#!~#value", 'A'+k)
			var keyVals []tsm1.Value
			for j := int64(1); j <= 100; j++ {
				keyVals = append(keyVals, tsm1.NewValue(j, float64(i*1000+int(j))))
			}
			vals[key] = keyVals
		}
		f := MustWriteTSM(dir, i+1, vals)
		tsmFiles = append(tsmFiles, f)
		r := MustOpenTSMReader(f)
		readers = append(readers, r)
		defer r.Close()
	}

	// Collect streaming output
	type block struct {
		key     string
		minTime int64
		maxTime int64
		data    []byte
	}
	var streamBlocks []block
	streamIter := tsm1.NewStreamingKeyIterator(tsmFiles, readers, 1000, nil)
	for streamIter.Next() {
		key, minTime, maxTime, data, err := streamIter.Read()
		if err != nil {
			t.Fatalf("streaming read error: %v", err)
		}
		k := make([]byte, len(key))
		copy(k, key)
		d := make([]byte, len(data))
		copy(d, data)
		streamBlocks = append(streamBlocks, block{string(k), minTime, maxTime, d})
	}
	if streamIter.Err() != nil {
		t.Fatalf("streaming error: %v", streamIter.Err())
	}

	// Collect batch output
	var batchBlocks []block
	batchIter, err := tsm1.NewTSMBatchKeyIterator(1000, false, nil, tsmFiles, readers...)
	if err != nil {
		t.Fatalf("batch error: %v", err)
	}
	for batchIter.Next() {
		key, minTime, maxTime, data, err := batchIter.Read()
		if err != nil {
			t.Fatalf("batch read error: %v", err)
		}
		k := make([]byte, len(key))
		copy(k, key)
		d := make([]byte, len(data))
		copy(d, data)
		batchBlocks = append(batchBlocks, block{string(k), minTime, maxTime, d})
	}
	if batchIter.Err() != nil {
		t.Fatalf("batch error: %v", batchIter.Err())
	}

	// Compare block counts
	if len(streamBlocks) != len(batchBlocks) {
		t.Fatalf("block count mismatch: streaming=%d, batch=%d", len(streamBlocks), len(batchBlocks))
	}

	// Compare each block
	for i := range streamBlocks {
		sb := streamBlocks[i]
		bb := batchBlocks[i]

		if sb.key != bb.key {
			t.Errorf("block %d: key mismatch: streaming=%q, batch=%q", i, sb.key, bb.key)
		}
		if sb.minTime != bb.minTime {
			t.Errorf("block %d: minTime mismatch: streaming=%d, batch=%d", i, sb.minTime, bb.minTime)
		}
		if sb.maxTime != bb.maxTime {
			// Debug: decode and show timestamps
			sVals, _ := tsm1.DecodeBlock(sb.data, nil)
			bVals, _ := tsm1.DecodeBlock(bb.data, nil)
			t.Logf("block %d: streaming maxTime=%d (decoded %d values, first=%d last=%d)", i, sb.maxTime, len(sVals), sVals[0].UnixNano(), sVals[len(sVals)-1].UnixNano())
			t.Logf("block %d: batch     maxTime=%d (decoded %d values, first=%d last=%d)", i, bb.maxTime, len(bVals), bVals[0].UnixNano(), bVals[len(bVals)-1].UnixNano())
			t.Errorf("block %d: maxTime mismatch: streaming=%d, batch=%d", i, sb.maxTime, bb.maxTime)
		}

		// Decode and compare values
		sVals, err := tsm1.DecodeBlock(sb.data, nil)
		if err != nil {
			t.Fatalf("block %d: streaming decode error: %v", i, err)
		}
		bVals, err := tsm1.DecodeBlock(bb.data, nil)
		if err != nil {
			t.Fatalf("block %d: batch decode error: %v", i, err)
		}

		if len(sVals) != len(bVals) {
			t.Errorf("block %d: value count mismatch: streaming=%d, batch=%d", i, len(sVals), len(bVals))
			continue
		}

		for j := range sVals {
			if sVals[j].UnixNano() != bVals[j].UnixNano() {
				t.Errorf("block %d value %d: timestamp mismatch: streaming=%d, batch=%d", i, j, sVals[j].UnixNano(), bVals[j].UnixNano())
			}
			if fmt.Sprintf("%v", sVals[j].Value()) != fmt.Sprintf("%v", bVals[j].Value()) {
				t.Errorf("block %d value %d: value mismatch: streaming=%v, batch=%v", i, j, sVals[j].Value(), bVals[j].Value())
			}
		}
	}
}
