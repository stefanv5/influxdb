package tsm1_test

import (
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

	// Read all values
	var got []tsm1.Value
	for iter.Next() {
		got = append(got, iter.Read())
	}

	if len(got) != len(vals) {
		t.Fatalf("expected %d values, got %d", len(vals), len(got))
	}

	for i, v := range got {
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

	// Read all values for first key
	var got []tsm1.Value
	for iter.Next() {
		got = append(got, iter.Read())
	}

	if len(got) != len(vals1) {
		t.Fatalf("expected %d values for first key, got %d", len(vals1), len(got))
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
	// TSM writer may split into multiple blocks based on internal logic
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

	// Check how many blocks exist for this key
	entries := r.Entries([]byte("cpu,host=A#!~#value"))
	t.Logf("Number of blocks: %d", len(entries))

	iter := tsm1.NewBlockValueIterator(r, 0)
	if !iter.Init() {
		t.Fatal("expected Init to return true")
	}

	// Read all values across all blocks
	var count int
	for iter.Next() {
		count++
	}

	// If there are multiple blocks, try advancing
	if len(entries) > 1 {
		for iter.NextBlock() {
			for iter.Next() {
				count++
			}
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
	// When tombstone covers full time range (MinInt64, MaxInt64), the key is
	// completely removed from the index, so Init() returns false
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
		// If Init returns true, all values should be tombstoned
		if iter.Next() {
			t.Fatal("expected Next to return false (all tombstoned)")
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

	// Should get ts=1 and ts=3
	var got []tsm1.Value
	for iter.Next() {
		got = append(got, iter.Read())
	}

	if len(got) != 2 {
		t.Fatalf("expected 2 values, got %d", len(got))
	}

	if got[0].UnixNano() != 1 {
		t.Errorf("expected first value ts=1, got %d", got[0].UnixNano())
	}
	if got[1].UnixNano() != 3 {
		t.Errorf("expected second value ts=3, got %d", got[1].UnixNano())
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

	// Should get ts=1, 3, 5
	var got []tsm1.Value
	for iter.Next() {
		got = append(got, iter.Read())
	}

	if len(got) != 3 {
		t.Fatalf("expected 3 values, got %d", len(got))
	}

	expected := []int64{1, 3, 5}
	for i, v := range got {
		if v.UnixNano() != expected[i] {
			t.Errorf("value %d: expected ts=%d, got %d", i, expected[i], v.UnixNano())
		}
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
	// This removes key A from the index completely
	ts := tsm1.NewTombstoner(f, nil)
	ts.AddRange([][]byte{[]byte("cpu,host=A#!~#value")}, math.MinInt64, math.MaxInt64)
	if err := ts.Flush(); err != nil {
		t.Fatalf("unexpected error flushing tombstone: %v", err)
	}

	r := MustOpenTSMReader(f)
	defer r.Close()

	iter := tsm1.NewBlockValueIterator(r, 0)
	// Since key A is fully tombstoned, Init() should start at key B
	// (key A is removed from index)
	if !iter.Init() {
		t.Fatal("expected Init to return true (should start at key B)")
	}

	// Should be on key B now
	if string(iter.Key()) != "cpu,host=B#!~#value" {
		t.Fatalf("expected key 'cpu,host=B#!~#value', got '%s'", string(iter.Key()))
	}

	if !iter.Next() {
		t.Fatal("expected Next to return true for key B")
	}

	v := iter.Read()
	if v.UnixNano() != 2 {
		t.Errorf("expected ts=2, got %d", v.UnixNano())
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
		// This is expected when tombstone covers the entire time range of the key
		return
	}

	// All values should be filtered
	if iter.Next() {
		t.Fatal("expected Next to return false (all values tombstoned)")
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

	// Tombstone first 1000 values (first block)
	// This creates a tombstone range, not a full key deletion
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

	// First block values (1-1000) are tombstoned
	// But Init reads the first block, so we need to check
	// if there are any non-tombstoned values
	if iter.Next() {
		// If we get a value, it should be from the second block
		// (after first block is exhausted)
		v := iter.Read()
		if v.UnixNano() <= 1000 {
			t.Errorf("expected value > 1000, got %d", v.UnixNano())
		}
	} else {
		// First block is fully tombstoned, advance to second block
		if !iter.NextBlock() {
			t.Fatal("expected NextBlock to return true")
		}

		// Should get values from second block (1001-2000)
		var count int
		for iter.Next() {
			count++
		}

		if count != 1000 {
			t.Fatalf("expected 1000 values from second block, got %d", count)
		}
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

	// Close should release resources
	iter.Close()

	// Next should return false after Close
	if iter.Next() {
		t.Fatal("expected Next to return false after Close")
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

	if iter.Next() {
		t.Fatal("expected Next to return false after Close")
	}
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
