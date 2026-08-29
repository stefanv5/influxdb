package tsm1_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

func TestFileStore_Read(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"mem", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	values, err := fs.Read([]byte("cpu"), 1)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := data[1]
	if got, exp := len(values), len(exp.values); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp.values {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}
}

func TestFileStore_SeekToAsc_FromStart(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 3.0)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	// Search for an entry that exists in the second file
	values, err := c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := data[0]
	if got, exp := len(values), len(exp.values); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp.values {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}
}

func TestFileStore_SeekToAsc_Duplicate(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 3.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 4.0)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	// Search for an entry that exists in the second file
	values, err := c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[1].values[0],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	// Check that calling Next will dedupe points
	c.Next()
	values, err = c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[3].values[0],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatal(err)
	}

	exp = nil
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}
}

func TestFileStore_SeekToAsc_BeforeStart(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, 3.0)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	values, err := c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := data[0]
	if got, exp := len(values), len(exp.values); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp.values {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}
}

// Tests that seeking and reading all blocks that contain overlapping points does
// not skip any blocks.
func TestFileStore_SeekToAsc_BeforeStart_OverlapFloat(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 0.0), tsm1.NewValue(1, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, 3.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 4.0), tsm1.NewValue(2, 7.0)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	values, err := c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[3].values[0],
		data[0].values[1],
		data[3].values[1],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[2].values[0],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}
}

// Tests that seeking and reading all blocks that contain overlapping points does
// not skip any blocks.
func TestFileStore_SeekToAsc_BeforeStart_OverlapInteger(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, int64(0)), tsm1.NewValue(1, int64(1))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, int64(2))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, int64(3))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, int64(4)), tsm1.NewValue(2, int64(7))}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.IntegerValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	values, err := c.ReadIntegerBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[3].values[0],
		data[0].values[1],
		data[3].values[1],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadIntegerBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[2].values[0],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}
}

// Tests that seeking and reading all blocks that contain overlapping points does
// not skip any blocks.
func TestFileStore_SeekToAsc_BeforeStart_OverlapUnsigned(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, uint64(0)), tsm1.NewValue(1, uint64(1))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, uint64(2))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, uint64(3))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, uint64(4)), tsm1.NewValue(2, uint64(7))}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.UnsignedValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	values, err := c.ReadUnsignedBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[3].values[0],
		data[0].values[1],
		data[3].values[1],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadUnsignedBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[2].values[0],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}
}

// Tests that seeking and reading all blocks that contain overlapping points does
// not skip any blocks.
func TestFileStore_SeekToAsc_BeforeStart_OverlapBoolean(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, true), tsm1.NewValue(1, false)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, true)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, true)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, false), tsm1.NewValue(2, true)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.BooleanValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	values, err := c.ReadBooleanBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[3].values[0],
		data[0].values[1],
		data[3].values[1],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadBooleanBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[2].values[0],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}
}

// Tests that seeking and reading all blocks that contain overlapping points does
// not skip any blocks.
func TestFileStore_SeekToAsc_BeforeStart_OverlapString(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, "zero"), tsm1.NewValue(1, "one")}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, "two")}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, "three")}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, "four"), tsm1.NewValue(2, "seven")}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.StringValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	values, err := c.ReadStringBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[3].values[0],
		data[0].values[1],
		data[3].values[1],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadStringBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[2].values[0],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}
}

// Tests that blocks with a lower min time in later files are not returned
// more than once causing unsorted results
func TestFileStore_SeekToAsc_OverlapMinFloat(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 1.0), tsm1.NewValue(3, 3.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 2.0), tsm1.NewValue(4, 4.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 0.0), tsm1.NewValue(1, 1.1)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 2.2)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	// Search for an entry that exists in the second file
	values, err := c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[2].values[0],
		data[2].values[1],
		data[3].values[0],
		data[0].values[1],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	// Check that calling Next will dedupe points
	c.Next()
	values, err = c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[1].values[1],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatal(err)
	}

	exp = nil
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}
}

// Tests that blocks with a lower min time in later files are not returned
// more than once causing unsorted results
func TestFileStore_SeekToAsc_OverlapMinInteger(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, int64(1)), tsm1.NewValue(3, int64(3))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, int64(2)), tsm1.NewValue(4, int64(4))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, int64(0)), tsm1.NewValue(1, int64(10))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, int64(5))}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.IntegerValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	// Search for an entry that exists in the second file
	values, err := c.ReadIntegerBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[2].values[0],
		data[2].values[1],
		data[3].values[0],
		data[0].values[1],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	// Check that calling Next will dedupe points
	c.Next()
	values, err = c.ReadIntegerBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[1].values[1],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadIntegerBlock(&buf)
	if err != nil {
		t.Fatal(err)
	}

	exp = nil
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}
}

// Tests that blocks with a lower min time in later files are not returned
// more than once causing unsorted results
func TestFileStore_SeekToAsc_OverlapMinUnsigned(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, uint64(1)), tsm1.NewValue(3, uint64(3))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, uint64(2)), tsm1.NewValue(4, uint64(4))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, uint64(0)), tsm1.NewValue(1, uint64(10))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, uint64(5))}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.UnsignedValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	// Search for an entry that exists in the second file
	values, err := c.ReadUnsignedBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[2].values[0],
		data[2].values[1],
		data[3].values[0],
		data[0].values[1],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	// Check that calling Next will dedupe points
	c.Next()
	values, err = c.ReadUnsignedBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[1].values[1],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadUnsignedBlock(&buf)
	if err != nil {
		t.Fatal(err)
	}

	exp = nil
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}
}

// Tests that blocks with a lower min time in later files are not returned
// more than once causing unsorted results
func TestFileStore_SeekToAsc_OverlapMinBoolean(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, true), tsm1.NewValue(3, true)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, true), tsm1.NewValue(4, true)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, true), tsm1.NewValue(1, false)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, false)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.BooleanValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	// Search for an entry that exists in the second file
	values, err := c.ReadBooleanBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[2].values[0],
		data[2].values[1],
		data[3].values[0],
		data[0].values[1],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	// Check that calling Next will dedupe points
	c.Next()
	values, err = c.ReadBooleanBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[1].values[1],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadBooleanBlock(&buf)
	if err != nil {
		t.Fatal(err)
	}

	exp = nil
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}
}

// Tests that blocks with a lower min time in later files are not returned
// more than once causing unsorted results
func TestFileStore_SeekToAsc_OverlapMinString(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, "1.0"), tsm1.NewValue(3, "3.0")}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, "2.0"), tsm1.NewValue(4, "4.0")}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, "0.0"), tsm1.NewValue(1, "1.1")}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, "2.2")}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.StringValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	// Search for an entry that exists in the second file
	values, err := c.ReadStringBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[2].values[0],
		data[2].values[1],
		data[3].values[0],
		data[0].values[1],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	// Check that calling Next will dedupe points
	c.Next()
	values, err = c.ReadStringBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[1].values[1],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadStringBlock(&buf)
	if err != nil {
		t.Fatal(err)
	}

	exp = nil
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}
}

func TestFileStore_SeekToAsc_Middle(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 1.0),
			tsm1.NewValue(2, 2.0),
			tsm1.NewValue(3, 3.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(4, 4.0)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 3, true)
	values, err := c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{data[0].values[2]}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{data[1].values[0]}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

}

func TestFileStore_SeekToAsc_End(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 3.0)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 2, true)
	values, err := c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := data[2]
	if got, exp := len(values), len(exp.values); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp.values {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}
}

func TestFileStore_SeekToDesc_FromStart(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 3.0)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, false)
	values, err := c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}
	exp := data[0]
	if got, exp := len(values), len(exp.values); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp.values {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}
}

func TestFileStore_SeekToDesc_Duplicate(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 4.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 3.0)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 2, false)
	values, err := c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}
	exp := []tsm1.Value{
		data[3].values[0],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}
	exp = []tsm1.Value{
		data[1].values[0],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}
}

func TestFileStore_SeekToDesc_OverlapMaxFloat(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 1.0), tsm1.NewValue(3, 3.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 2.0), tsm1.NewValue(4, 4.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 0.0), tsm1.NewValue(1, 1.1)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 2.2)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 5, false)
	values, err := c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[3].values[0],
		data[0].values[1],
		data[1].values[1],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}
	exp = []tsm1.Value{

		data[2].values[0],
		data[2].values[1],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}
}

func TestFileStore_SeekToDesc_OverlapMaxInteger(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, int64(1)), tsm1.NewValue(3, int64(3))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, int64(2)), tsm1.NewValue(4, int64(4))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, int64(0)), tsm1.NewValue(1, int64(10))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, int64(5))}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.IntegerValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 5, false)
	values, err := c.ReadIntegerBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[3].values[0],
		data[0].values[1],
		data[1].values[1],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadIntegerBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}
	exp = []tsm1.Value{
		data[2].values[0],
		data[2].values[1],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}
}
func TestFileStore_SeekToDesc_OverlapMaxUnsigned(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, uint64(1)), tsm1.NewValue(3, uint64(3))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, uint64(2)), tsm1.NewValue(4, uint64(4))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, uint64(0)), tsm1.NewValue(1, uint64(10))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, uint64(5))}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.UnsignedValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 5, false)
	values, err := c.ReadUnsignedBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[3].values[0],
		data[0].values[1],
		data[1].values[1],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadUnsignedBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}
	exp = []tsm1.Value{
		data[2].values[0],
		data[2].values[1],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}
}

func TestFileStore_SeekToDesc_OverlapMaxBoolean(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, true), tsm1.NewValue(3, true)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, true), tsm1.NewValue(4, true)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, true), tsm1.NewValue(1, false)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, false)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.BooleanValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 5, false)
	values, err := c.ReadBooleanBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[3].values[0],
		data[0].values[1],
		data[1].values[1],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadBooleanBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}
	exp = []tsm1.Value{
		data[2].values[0],
		data[2].values[1],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}
}

func TestFileStore_SeekToDesc_OverlapMaxString(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, "1.0"), tsm1.NewValue(3, "3.0")}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, "2.0"), tsm1.NewValue(4, "4.0")}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, "0.0"), tsm1.NewValue(1, "1.1")}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, "2.2")}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.StringValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 5, false)
	values, err := c.ReadStringBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[3].values[0],
		data[0].values[1],
		data[1].values[1],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadStringBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}
	exp = []tsm1.Value{
		data[2].values[0],
		data[2].values[1],
	}
	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}
}

func TestFileStore_SeekToDesc_AfterEnd(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, 3.0)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 4, false)
	values, err := c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := data[2]
	if got, exp := len(values), len(exp.values); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp.values {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}
}

func TestFileStore_SeekToDesc_AfterEnd_OverlapFloat(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 4 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(8, 0.0), tsm1.NewValue(9, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, 3.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, 4.0), tsm1.NewValue(7, 7.0)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 10, false)
	values, err := c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[0].values[0],
		data[0].values[1],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadFloatBlock(&buf)

	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[3].values[0],
		data[3].values[1],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadFloatBlock(&buf)

	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[1].values[0],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadFloatBlock(&buf)

	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	if got, exp := len(values), 0; got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}
}

func TestFileStore_SeekToDesc_AfterEnd_OverlapInteger(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(8, int64(0)), tsm1.NewValue(9, int64(1))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, int64(2))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, int64(3))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, int64(4)), tsm1.NewValue(10, int64(7))}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.IntegerValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 11, false)
	values, err := c.ReadIntegerBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[3].values[0],
		data[0].values[0],
		data[0].values[1],
		data[3].values[1],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadIntegerBlock(&buf)

	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[1].values[0],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadIntegerBlock(&buf)

	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	if got, exp := len(values), 0; got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}
}

func TestFileStore_SeekToDesc_AfterEnd_OverlapUnsigned(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(8, uint64(0)), tsm1.NewValue(9, uint64(1))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, uint64(2))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, uint64(3))}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, uint64(4)), tsm1.NewValue(10, uint64(7))}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.UnsignedValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 11, false)
	values, err := c.ReadUnsignedBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[3].values[0],
		data[0].values[0],
		data[0].values[1],
		data[3].values[1],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadUnsignedBlock(&buf)

	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[1].values[0],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadUnsignedBlock(&buf)

	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	if got, exp := len(values), 0; got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}
}

func TestFileStore_SeekToDesc_AfterEnd_OverlapBoolean(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(8, true), tsm1.NewValue(9, true)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, true)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, false)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, true), tsm1.NewValue(7, false)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.BooleanValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 11, false)
	values, err := c.ReadBooleanBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[0].values[0],
		data[0].values[1],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadBooleanBlock(&buf)

	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[3].values[0],
		data[3].values[1],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadBooleanBlock(&buf)

	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[1].values[0],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadBooleanBlock(&buf)

	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	if got, exp := len(values), 0; got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}
}

func TestFileStore_SeekToDesc_AfterEnd_OverlapString(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(8, "eight"), tsm1.NewValue(9, "nine")}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, "two")}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, "three")}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(3, "four"), tsm1.NewValue(7, "seven")}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.StringValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 11, false)
	values, err := c.ReadStringBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[0].values[0],
		data[0].values[1],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadStringBlock(&buf)

	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[3].values[0],
		data[3].values[1],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadStringBlock(&buf)

	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[1].values[0],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadStringBlock(&buf)

	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	if got, exp := len(values), 0; got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}
}

func TestFileStore_SeekToDesc_Middle(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 1.0)}},
		keyValues{"cpu", []tsm1.Value{
			tsm1.NewValue(2, 2.0),
			tsm1.NewValue(3, 3.0),
			tsm1.NewValue(4, 4.0)},
		},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 3, false)
	values, err := c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := []tsm1.Value{
		data[1].values[0],
		data[1].values[1],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}
	c.Next()
	values, err = c.ReadFloatBlock(&buf)

	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp = []tsm1.Value{
		data[0].values[0],
	}

	if got, exp := len(values), len(exp); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}

	c.Next()
	values, err = c.ReadFloatBlock(&buf)

	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	if got, exp := len(values), 0; got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}
}

func TestFileStore_SeekToDesc_End(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 3.0)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 2, false)
	values, err := c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	exp := data[2]
	if got, exp := len(values), len(exp.values); got != exp {
		t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range exp.values {
		if got, exp := values[i].Value(), v.Value(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", i, got, exp)
		}
	}
}

func TestKeyCursor_TombstoneRange(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 3.0)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	if err := fs.DeleteRange([][]byte{[]byte("cpu")}, 1, 1); err != nil {
		t.Fatalf("unexpected error delete range: %v", err)
	}

	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	expValues := []int{0, 2}
	for _, v := range expValues {
		values, err := c.ReadFloatBlock(&buf)
		if err != nil {
			t.Fatalf("unexpected error reading values: %v", err)
		}

		exp := data[v]
		if got, exp := len(values), 1; got != exp {
			t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
		}

		if got, exp := values[0].String(), exp.values[0].String(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", 0, got, exp)
		}
		c.Next()
	}
}

func TestKeyCursor_TombstoneRange_PartialFirst(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 0.0), tsm1.NewValue(1, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 2.0)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	// Delete part of the block in the first file.
	r := MustOpenTSMReader(files[0])
	r.DeleteRange([][]byte{[]byte("cpu")}, 1, 3)

	fs.Replace(nil, files)

	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	expValues := []tsm1.Value{tsm1.NewValue(0, 0.0), tsm1.NewValue(2, 2.0)}

	for _, exp := range expValues {
		values, err := c.ReadFloatBlock(&buf)
		if err != nil {
			t.Fatalf("unexpected error reading values: %v", err)
		}

		if got, exp := len(values), 1; got != exp {
			t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
		}

		if got, exp := values[0].String(), exp.String(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", 0, got, exp)
		}
		c.Next()
	}
}

func TestKeyCursor_TombstoneRange_PartialFloat(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{
			tsm1.NewValue(0, 1.0),
			tsm1.NewValue(1, 2.0),
			tsm1.NewValue(2, 3.0)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	if err := fs.DeleteRange([][]byte{[]byte("cpu")}, 1, 1); err != nil {
		t.Fatalf("unexpected error delete range: %v", err)
	}

	buf := make([]tsm1.FloatValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	values, err := c.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	expValues := []tsm1.Value{data[0].values[0], data[0].values[2]}
	for i, v := range expValues {
		exp := v
		if got, exp := len(values), 2; got != exp {
			t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
		}

		if got, exp := values[i].String(), exp.String(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", 0, got, exp)
		}
	}
}

func TestKeyCursor_TombstoneRange_PartialInteger(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{
			tsm1.NewValue(0, int64(1)),
			tsm1.NewValue(1, int64(2)),
			tsm1.NewValue(2, int64(3))}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	if err := fs.DeleteRange([][]byte{[]byte("cpu")}, 1, 1); err != nil {
		t.Fatalf("unexpected error delete range: %v", err)
	}

	buf := make([]tsm1.IntegerValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	values, err := c.ReadIntegerBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	expValues := []tsm1.Value{data[0].values[0], data[0].values[2]}
	for i, v := range expValues {
		exp := v
		if got, exp := len(values), 2; got != exp {
			t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
		}

		if got, exp := values[i].String(), exp.String(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", 0, got, exp)
		}
	}
}

func TestKeyCursor_TombstoneRange_PartialUnsigned(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{
			tsm1.NewValue(0, uint64(1)),
			tsm1.NewValue(1, uint64(2)),
			tsm1.NewValue(2, uint64(3))}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	if err := fs.DeleteRange([][]byte{[]byte("cpu")}, 1, 1); err != nil {
		t.Fatalf("unexpected error delete range: %v", err)
	}

	buf := make([]tsm1.UnsignedValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	values, err := c.ReadUnsignedBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	expValues := []tsm1.Value{data[0].values[0], data[0].values[2]}
	for i, v := range expValues {
		exp := v
		if got, exp := len(values), 2; got != exp {
			t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
		}

		if got, exp := values[i].String(), exp.String(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", 0, got, exp)
		}
	}
}

func TestKeyCursor_TombstoneRange_PartialString(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{
			tsm1.NewValue(0, "1"),
			tsm1.NewValue(1, "2"),
			tsm1.NewValue(2, "3")}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	if err := fs.DeleteRange([][]byte{[]byte("cpu")}, 1, 1); err != nil {
		t.Fatalf("unexpected error delete range: %v", err)
	}

	buf := make([]tsm1.StringValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	values, err := c.ReadStringBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	expValues := []tsm1.Value{data[0].values[0], data[0].values[2]}
	for i, v := range expValues {
		exp := v
		if got, exp := len(values), 2; got != exp {
			t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
		}

		if got, exp := values[i].String(), exp.String(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", 0, got, exp)
		}
	}
}

func TestKeyCursor_TombstoneRange_PartialBoolean(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{
			tsm1.NewValue(0, true),
			tsm1.NewValue(1, false),
			tsm1.NewValue(2, true)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	if err := fs.DeleteRange([][]byte{[]byte("cpu")}, 1, 1); err != nil {
		t.Fatalf("unexpected error delete range: %v", err)
	}

	buf := make([]tsm1.BooleanValue, 1000)
	c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)
	values, err := c.ReadBooleanBlock(&buf)
	if err != nil {
		t.Fatalf("unexpected error reading values: %v", err)
	}

	expValues := []tsm1.Value{data[0].values[0], data[0].values[2]}
	for i, v := range expValues {
		exp := v
		if got, exp := len(values), 2; got != exp {
			t.Fatalf("value length mismatch: got %v, exp %v", got, exp)
		}

		if got, exp := values[i].String(), exp.String(); got != exp {
			t.Fatalf("read value mismatch(%d): got %v, exp %v", 0, got, exp)
		}
	}
}

func TestFileStore_Open(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// Create 3 TSM files...
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"mem", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
	}

	_, err := newFileDir(dir, data...)
	if err != nil {
		fatal(t, "creating test files", err)
	}

	fs := tsm1.NewFileStore(dir)
	if err := fs.Open(); err != nil {
		fatal(t, "opening file store", err)
	}
	defer fs.Close()

	if got, exp := fs.Count(), 3; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}

	if got, exp := fs.CurrentGeneration(), 4; got != exp {
		t.Fatalf("current ID mismatch: got %v, exp %v", got, exp)
	}
}

func TestFileStore_Remove(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// Create 3 TSM files...
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"mem", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
	}

	files, err := newFileDir(dir, data...)
	if err != nil {
		fatal(t, "creating test files", err)
	}

	fs := tsm1.NewFileStore(dir)
	if err := fs.Open(); err != nil {
		fatal(t, "opening file store", err)
	}
	defer fs.Close()

	if got, exp := fs.Count(), 3; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}

	if got, exp := fs.CurrentGeneration(), 4; got != exp {
		t.Fatalf("current ID mismatch: got %v, exp %v", got, exp)
	}

	fs.Replace(files[2:3], nil)

	if got, exp := fs.Count(), 2; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}

	if got, exp := fs.CurrentGeneration(), 4; got != exp {
		t.Fatalf("current ID mismatch: got %v, exp %v", got, exp)
	}
}

func TestFileStore_Replace(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// Create 3 TSM files...
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 3.0)}},
	}

	files, err := newFileDir(dir, data...)
	if err != nil {
		fatal(t, "creating test files", err)
	}

	// Replace requires assumes new files have a .tmp extension
	replacement := fmt.Sprintf("%s.%s", files[2], tsm1.TmpTSMFileExtension)
	os.Rename(files[2], replacement)

	fs := tsm1.NewFileStore(dir)
	if err := fs.Open(); err != nil {
		fatal(t, "opening file store", err)
	}
	defer fs.Close()

	if got, exp := fs.Count(), 2; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}

	// Should record references to the two existing TSM files
	cur := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)

	// Should move the existing files out of the way, but allow query to complete
	if err := fs.Replace(files[:2], []string{replacement}); err != nil {
		t.Fatalf("replace: %v", err)
	}

	if got, exp := fs.Count(), 1; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}

	// There should be two blocks (1 in each file)
	cur.Next()
	buf := make([]tsm1.FloatValue, 10)
	values, err := cur.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got, exp := len(values), 1; got != exp {
		t.Fatalf("value len mismatch: got %v, exp %v", got, exp)
	}

	cur.Next()
	values, err = cur.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got, exp := len(values), 1; got != exp {
		t.Fatalf("value len mismatch: got %v, exp %v", got, exp)
	}

	// No more blocks for this cursor
	cur.Next()
	values, err = cur.ReadFloatBlock(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got, exp := len(values), 0; got != exp {
		t.Fatalf("value len mismatch: got %v, exp %v", got, exp)
	}

	// Release the references (files should get evicted by purger shortly)
	cur.Close()

	time.Sleep(time.Second)
	// Make sure the two TSM files used by the cursor are gone
	if _, err := os.Stat(files[0]); !os.IsNotExist(err) {
		t.Fatalf("stat file: %v", err)
	}
	if _, err := os.Stat(files[1]); !os.IsNotExist(err) {
		t.Fatalf("stat file: %v", err)
	}

	// Make sure the new file exists
	if _, err := os.Stat(files[2]); err != nil {
		t.Fatalf("stat file: %v", err)
	}

}

// TestFileStore_Replace_RejectsPathAlias verifies that a newFiles path that
// aliases an oldFiles path through dot-segments is rejected as a same-path
// replacement. Without normalization the string checks would pass, the prune
// loop would delete the live pathname while the alias reader survived, and
// Replace would return nil with the file gone after restart.
func TestFileStore_Replace_RejectsPathAlias(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
	}
	files, err := newFileDir(dir, data...)
	if err != nil {
		fatal(t, "creating test files", err)
	}

	fs := tsm1.NewFileStore(dir)
	if err := fs.Open(); err != nil {
		fatal(t, "opening file store", err)
	}
	defer fs.Close()

	if got, exp := fs.Count(), 2; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}

	// Alias files[0] as dir/./<name>: the same file, a different string.
	alias := fmt.Sprintf("%s%c.%c%s", dir, os.PathSeparator, os.PathSeparator, filepath.Base(files[0]))
	err = fs.Replace([]string{files[0]}, []string{alias})
	if err == nil {
		t.Fatal("expected replace of an old file through a path alias to fail")
	}
	if !strings.Contains(err.Error(), "same-path replacement is not supported") {
		t.Fatalf("unexpected error: %v", err)
	}

	// The store and the file on disk must be unchanged.
	if got, exp := fs.Count(), 2; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}
	if _, err := os.Stat(files[0]); err != nil {
		t.Fatalf("stat file: %v", err)
	}
}

// TestFileStore_Replace_RejectsCaseAlias verifies that on Windows, where
// paths are case-insensitive, a case-differing alias of an oldFiles path is
// rejected as a same-path replacement instead of passing the string checks.
func TestFileStore_Replace_RejectsCaseAlias(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("case-insensitive path semantics are Windows-specific")
	}

	dir := MustTempDir()
	defer os.RemoveAll(dir)

	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
	}
	files, err := newFileDir(dir, data...)
	if err != nil {
		fatal(t, "creating test files", err)
	}

	fs := tsm1.NewFileStore(dir)
	if err := fs.Open(); err != nil {
		fatal(t, "opening file store", err)
	}
	defer fs.Close()

	alias := strings.ToUpper(files[0])
	err = fs.Replace([]string{files[0]}, []string{alias})
	if err == nil {
		t.Fatal("expected replace of an old file through a case alias to fail")
	}
	if !strings.Contains(err.Error(), "same-path replacement is not supported") {
		t.Fatalf("unexpected error: %v", err)
	}

	if got, exp := fs.Count(), 1; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}
	if _, err := os.Stat(files[0]); err != nil {
		t.Fatalf("stat file: %v", err)
	}
}

// TestFileStore_Replace_RejectsDuplicateAliasTarget verifies that two newFiles
// entries resolving to the same final target through a path alias are rejected
// in validation, before any rename or reader is created.
func TestFileStore_Replace_RejectsDuplicateAliasTarget(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
	}
	files, err := newFileDir(dir, data...)
	if err != nil {
		fatal(t, "creating test files", err)
	}

	// Stage the single output as .tsm.tmp like the compactor does.
	tmp := files[0] + "." + tsm1.TmpTSMFileExtension
	if err := os.Rename(files[0], tmp); err != nil {
		fatal(t, "staging tmp file", err)
	}

	fs := tsm1.NewFileStore(dir)
	if err := fs.Open(); err != nil {
		fatal(t, "opening file store", err)
	}
	defer fs.Close()

	if got, exp := fs.Count(), 0; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}

	// The same final target expressed twice: once as the .tsm.tmp staging
	// path, once as a dot-segment alias of the final .tsm path.
	alias := fmt.Sprintf("%s%c.%c%s", dir, os.PathSeparator, os.PathSeparator, filepath.Base(files[0]))
	err = fs.Replace(nil, []string{tmp, alias})
	if err == nil {
		t.Fatal("expected duplicate new file target via path alias to fail")
	}
	if !strings.Contains(err.Error(), "duplicate new file target") {
		t.Fatalf("unexpected error: %v", err)
	}

	// Validation must reject before any side effect: the staged .tmp file is
	// untouched and no formal .tsm was created.
	if _, err := os.Stat(tmp); err != nil {
		t.Fatalf("staged tmp file must be untouched: %v", err)
	}
	if _, err := os.Stat(files[0]); !os.IsNotExist(err) {
		t.Fatalf("no formal file may be created when validation rejects: %v", err)
	}
	if got, exp := fs.Count(), 0; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}
}

// TestFileStore_Replace_MultiOutputRollbackOnExistingTarget verifies that when
// the second output's formal target already exists, the first output that was
// already installed is rolled back: its formal file is renamed back to the
// .tmp staging name and the store stays empty. (The equivalent scenario with
// valid TSM content is covered by TestFileStore_Replace_MultiOutputRollback;
// this variant also asserts the no-clobber error is reported.)
func TestFileStore_Replace_MultiOutputRollbackOnExistingTarget(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// Two compaction outputs staged as .tsm.tmp.
	f1 := MustWriteTSM(dir, 1, map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {tsm1.NewValue(1, float64(1.0))},
	})
	f2 := MustWriteTSM(dir, 2, map[string][]tsm1.Value{
		"cpu,host=B#!~#value": {tsm1.NewValue(1, float64(2.0))},
	})

	tmp1, tmp2 := f1+".tmp", f2+".tmp"
	if err := os.Rename(f1, tmp1); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(f2, tmp2); err != nil {
		t.Fatal(err)
	}

	fs := tsm1.NewFileStore(dir)
	if err := fs.Open(); err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	if got, exp := fs.Count(), 0; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}

	// Pre-create the formal target of the second output AFTER Open (Open
	// validates every *.tsm in the dir, so an invalid stub must not exist
	// yet) so its install fails after the first output already succeeded.
	if err := os.WriteFile(f2, []byte("pre-existing target"), 0666); err != nil {
		t.Fatal(err)
	}

	err := fs.Replace(nil, []string{tmp1, tmp2})
	if err == nil {
		t.Fatal("expected replace with pre-existing second target to fail")
	}
	if !strings.Contains(err.Error(), "refusing to rename") {
		t.Fatalf("unexpected error: %v", err)
	}

	// The first output must be rolled back to its .tmp staging name.
	if _, err := os.Stat(tmp1); err != nil {
		t.Fatalf("first output must be rolled back to its tmp path: %v", err)
	}
	if _, err := os.Stat(f1); !os.IsNotExist(err) {
		t.Fatalf("first output formal file must be gone: %v", err)
	}
	if got, exp := fs.Count(), 0; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}
}

// TestFileStore_Replace_RollbackOnCorruptOutput verifies that a second output
// with a valid staging name but corrupt content (NewTSMReader fails after the
// rename already happened) rolls back the first output that was already
// installed: its reader is closed, its formal file is renamed back to the .tmp
// staging name and the corrupt output is renamed back too.
func TestFileStore_Replace_RollbackOnCorruptOutput(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// Two compaction outputs staged as .tsm.tmp.
	f1 := MustWriteTSM(dir, 1, map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {tsm1.NewValue(1, float64(1.0))},
	})
	f2 := MustWriteTSM(dir, 2, map[string][]tsm1.Value{
		"cpu,host=B#!~#value": {tsm1.NewValue(1, float64(2.0))},
	})

	tmp1, corruptTmp := f1+".tmp", f2+".tmp"
	if err := os.Rename(f1, tmp1); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(f2, corruptTmp); err != nil {
		t.Fatal(err)
	}

	fs := tsm1.NewFileStore(dir)
	if err := fs.Open(); err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	if got, exp := fs.Count(), 0; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}

	// Corrupt the second output: valid staging name, garbage content, so
	// NewTSMReader fails on it after it has already been renamed to its
	// formal name.
	if err := os.WriteFile(corruptTmp, []byte("not a tsm file"), 0666); err != nil {
		t.Fatal(err)
	}

	if err := fs.Replace(nil, []string{tmp1, corruptTmp}); err == nil {
		t.Fatal("expected replace with corrupt second output to fail")
	}

	// The first output must be rolled back to its .tmp staging name.
	if _, err := os.Stat(tmp1); err != nil {
		t.Fatalf("first output must be rolled back to its tmp path: %v", err)
	}
	if _, err := os.Stat(f1); !os.IsNotExist(err) {
		t.Fatalf("first output formal file must be gone: %v", err)
	}
	// The corrupt second output must be renamed back to its staging name too.
	if _, err := os.Stat(corruptTmp); err != nil {
		t.Fatalf("corrupt output must be renamed back to its tmp path: %v", err)
	}
	if _, err := os.Stat(f2); !os.IsNotExist(err) {
		t.Fatalf("corrupt output formal file must be gone: %v", err)
	}
	if got, exp := fs.Count(), 0; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}
}

// TestFileStore_Replace_RollbackOnObserverFailure verifies that a FileFinishing
// failure on the second output rolls back the first output that was already
// installed: its reader is closed and its formal file renamed back to the .tmp
// staging name.
func TestFileStore_Replace_RollbackOnObserverFailure(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"mem", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
	}
	files, err := newFileDir(dir, data...)
	if err != nil {
		fatal(t, "creating test files", err)
	}

	tmp1 := files[0] + "." + tsm1.TmpTSMFileExtension
	tmp2 := files[1] + "." + tsm1.TmpTSMFileExtension
	if err := os.Rename(files[0], tmp1); err != nil {
		fatal(t, "staging first tmp file", err)
	}
	if err := os.Rename(files[1], tmp2); err != nil {
		fatal(t, "staging second tmp file", err)
	}

	fs := tsm1.NewFileStore(dir)
	fs.WithObserver(mockObserver{
		fileFinishing: func(path string) error {
			if path == tmp2 {
				return fmt.Errorf("injected FileFinishing failure for %s", path)
			}
			return nil
		},
		fileUnlinking: func(path string) error { return nil },
	})
	if err := fs.Open(); err != nil {
		fatal(t, "opening file store", err)
	}
	defer fs.Close()

	if err := fs.Replace(nil, []string{tmp1, tmp2}); err == nil {
		t.Fatal("expected replace with failing observer to fail")
	}

	// The first output must be rolled back to its .tmp staging name.
	if _, err := os.Stat(tmp1); err != nil {
		t.Fatalf("first output must be rolled back to its tmp path: %v", err)
	}
	if _, err := os.Stat(files[0]); !os.IsNotExist(err) {
		t.Fatalf("first output formal file must be gone: %v", err)
	}
	// The second output was never renamed.
	if _, err := os.Stat(tmp2); err != nil {
		t.Fatalf("second output tmp file must be untouched: %v", err)
	}
	if got, exp := fs.Count(), 0; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}
}

// TestFileStore_Replace_PruneErrorKeepsStoreLoaded verifies that a
// FileUnlinking failure while pruning an old file does not abort mid-prune
// with a stale in-memory state: the failed-to-remove old file stays loaded and
// usable and the failure is reported as an aggregated error.
func TestFileStore_Replace_PruneErrorKeepsStoreLoaded(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
	}
	files, err := newFileDir(dir, data...)
	if err != nil {
		fatal(t, "creating test files", err)
	}

	fs := tsm1.NewFileStore(dir)
	fs.WithObserver(mockObserver{
		fileFinishing: func(path string) error { return nil },
		fileUnlinking: func(path string) error {
			if path == files[0] {
				return fmt.Errorf("injected FileUnlinking failure for %s", path)
			}
			return nil
		},
	})
	if err := fs.Open(); err != nil {
		fatal(t, "opening file store", err)
	}
	defer fs.Close()

	if got, exp := fs.Count(), 1; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}

	// Replace with no new files prunes the old ones; pruning files[0] fails
	// at the observer.
	err = fs.Replace(files[:1], nil)
	if err == nil {
		t.Fatal("expected replace with failing prune to report an error")
	}
	if !strings.Contains(err.Error(), "unable to prune file") {
		t.Fatalf("unexpected error: %v", err)
	}

	// The failed-to-remove old file must remain loaded and usable.
	if got, exp := fs.Count(), 1; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}
	values, err := fs.Read([]byte("cpu"), 0)
	if err != nil {
		fatal(t, "reading kept file", err)
	}
	if len(values) != 1 {
		t.Fatalf("value len mismatch: got %v, exp 1", len(values))
	}
	if _, err := os.Stat(files[0]); err != nil {
		t.Fatalf("stat file: %v", err)
	}
}

func TestFileStore_Open_Deleted(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// Create 3 TSM files...
	data := []keyValues{
		keyValues{"cpu,host=server2!~#!value", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu,host=server1!~#!value", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"mem,host=server1!~#!value", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
	}

	_, err := newFileDir(dir, data...)
	if err != nil {
		fatal(t, "creating test files", err)
	}

	fs := tsm1.NewFileStore(dir)
	if err := fs.Open(); err != nil {
		fatal(t, "opening file store", err)
	}
	defer fs.Close()

	if got, exp := len(fs.Keys()), 3; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}

	if err := fs.Delete([][]byte{[]byte("cpu,host=server2!~#!value")}); err != nil {
		fatal(t, "deleting", err)
	}

	fs2 := tsm1.NewFileStore(dir)
	if err := fs2.Open(); err != nil {
		fatal(t, "opening file store", err)
	}
	defer fs2.Close()

	if got, exp := len(fs2.Keys()), 2; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}
}

func TestFileStore_Delete(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu,host=server2!~#!value", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu,host=server1!~#!value", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"mem,host=server1!~#!value", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	keys := fs.Keys()
	if got, exp := len(keys), 3; got != exp {
		t.Fatalf("key length mismatch: got %v, exp %v", got, exp)
	}

	if err := fs.Delete([][]byte{[]byte("cpu,host=server2!~#!value")}); err != nil {
		fatal(t, "deleting", err)
	}

	keys = fs.Keys()
	if got, exp := len(keys), 2; got != exp {
		t.Fatalf("key length mismatch: got %v, exp %v", got, exp)
	}
}

func TestFileStore_Apply(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu,host=server2#!~#value", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu,host=server1#!~#value", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"mem,host=server1#!~#value", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	keys := fs.Keys()
	if got, exp := len(keys), 3; got != exp {
		t.Fatalf("key length mismatch: got %v, exp %v", got, exp)
	}

	var n int64
	if err := fs.Apply(func(r tsm1.TSMFile) error {
		atomic.AddInt64(&n, 1)
		return nil
	}); err != nil {
		t.Fatalf("unexpected error deleting: %v", err)
	}

	if got, exp := n, int64(3); got != exp {
		t.Fatalf("apply mismatch: got %v, exp %v", got, exp)
	}
}

func TestFileStore_Stats(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// Create 3 TSM files...
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"mem", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
	}

	files, err := newFileDir(dir, data...)
	if err != nil {
		fatal(t, "creating test files", err)
	}

	fs := tsm1.NewFileStore(dir)
	if err := fs.Open(); err != nil {
		fatal(t, "opening file store", err)
	}
	defer fs.Close()

	stats := fs.Stats()
	if got, exp := len(stats), 3; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}

	// Another call should result in the same stats being returned.
	if got, exp := fs.Stats(), stats; !reflect.DeepEqual(got, exp) {
		t.Fatalf("got %v, exp %v", got, exp)
	}

	// Removing one of the files should invalidate the cache.
	fs.Replace(files[0:1], nil)
	if got, exp := len(fs.Stats()), 2; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}

	// Write a new TSM file that that is not open
	newFile := MustWriteTSM(dir, 4, map[string][]tsm1.Value{
		"mem": []tsm1.Value{tsm1.NewValue(0, 1.0)},
	})

	replacement := fmt.Sprintf("%s.%s.%s", files[2], tsm1.TmpTSMFileExtension, tsm1.TSMFileExtension) // Assumes new files have a .tmp extension
	if err := os.Rename(newFile, replacement); err != nil {
		t.Fatalf("rename: %v", err)
	}
	// Replace 3 w/ 1
	if err := fs.Replace(files, []string{replacement}); err != nil {
		t.Fatalf("replace: %v", err)
	}

	var found bool
	stats = fs.Stats()
	for _, stat := range stats {
		if strings.HasSuffix(stat.Path, fmt.Sprintf("%s.%s.%s", tsm1.TSMFileExtension, tsm1.TmpTSMFileExtension, tsm1.TSMFileExtension)) {
			found = true
		}
	}

	if !found {
		t.Fatalf("Didn't find %s in stats: %v", "foo", stats)
	}

	newFile = MustWriteTSM(dir, 5, map[string][]tsm1.Value{
		"mem": []tsm1.Value{tsm1.NewValue(0, 1.0)},
	})

	// Adding some files should invalidate the cache.
	fs.Replace(nil, []string{newFile})
	if got, exp := len(fs.Stats()), 2; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}
}

func TestFileStore_CreateSnapshot(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(2, 3.0)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Create a tombstone
	if err := fs.DeleteRange([][]byte{[]byte("cpu")}, 1, 1); err != nil {
		t.Fatalf("unexpected error delete range: %v", err)
	}

	s, e := fs.CreateSnapshot()
	if e != nil {
		t.Fatal(e)
	}
	t.Logf("temp file for hard links: %q", s)

	tfs, e := ioutil.ReadDir(s)
	if e != nil {
		t.Fatal(e)
	}
	if len(tfs) == 0 {
		t.Fatal("no files found")
	}

	for _, f := range fs.Files() {
		p := filepath.Join(s, filepath.Base(f.Path()))
		t.Logf("checking for existence of hard link %q", p)
		if _, err := os.Stat(p); os.IsNotExist(err) {
			t.Fatalf("unable to find file %q", p)
		}
		if ts := f.TombstoneStats(); ts.TombstoneExists {
			p := filepath.Join(s, filepath.Base(ts.Path))
			t.Logf("checking for existence of hard link %q", p)
			if _, err := os.Stat(p); os.IsNotExist(err) {
				t.Fatalf("unable to find file %q", p)
			}
		}
	}
}

type mockObserver struct {
	fileFinishing func(path string) error
	fileUnlinking func(path string) error
}

func (m mockObserver) FileFinishing(path string) error {
	return m.fileFinishing(path)
}

func (m mockObserver) FileUnlinking(path string) error {
	return m.fileUnlinking(path)
}

func TestFileStore_Observer(t *testing.T) {
	var finishes, unlinks []string
	m := mockObserver{
		fileFinishing: func(path string) error {
			finishes = append(finishes, path)
			return nil
		},
		fileUnlinking: func(path string) error {
			unlinks = append(unlinks, path)
			return nil
		},
	}

	check := func(results []string, expect ...string) {
		t.Helper()
		if len(results) != len(expect) {
			t.Fatalf("wrong number of results: %d results != %d expected", len(results), len(expect))
		}
		for i, ex := range expect {
			if got := filepath.Base(results[i]); got != ex {
				t.Fatalf("unexpected result: got %q != expected %q", got, ex)
			}
		}
	}

	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := tsm1.NewFileStore(dir)
	fs.WithObserver(m)

	// Setup 3 files
	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0), tsm1.NewValue(1, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(10, 2.0)}},
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(20, 3.0)}},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	if err := fs.Replace(nil, files); err != nil {
		t.Fatalf("error replacing: %v", err)
	}

	// Create a tombstone
	if err := fs.DeleteRange([][]byte{[]byte("cpu")}, 10, 10); err != nil {
		t.Fatalf("unexpected error delete range: %v", err)
	}

	// Check that we observed finishes correctly
	check(finishes,
		"000000001-000000001.tsm",
		"000000002-000000001.tsm",
		"000000003-000000001.tsm",
		"000000002-000000001.tombstone.tmp",
	)
	check(unlinks)
	unlinks, finishes = nil, nil

	// remove files including a tombstone
	if err := fs.Replace(files[1:3], nil); err != nil {
		t.Fatal("error replacing")
	}

	// Check that we observed unlinks correctly
	check(finishes)
	check(unlinks,
		"000000002-000000001.tsm",
		"000000002-000000001.tombstone",
		"000000003-000000001.tsm",
	)
	unlinks, finishes = nil, nil

	// add a tombstone for the first file multiple times.
	if err := fs.DeleteRange([][]byte{[]byte("cpu")}, 0, 0); err != nil {
		t.Fatalf("unexpected error delete range: %v", err)
	}
	if err := fs.DeleteRange([][]byte{[]byte("cpu")}, 1, 1); err != nil {
		t.Fatalf("unexpected error delete range: %v", err)
	}

	check(finishes,
		"000000001-000000001.tombstone.tmp",
		"000000001-000000001.tombstone.tmp",
	)
	check(unlinks)
	unlinks, finishes = nil, nil
}

func newFileDir(dir string, values ...keyValues) ([]string, error) {
	var files []string

	id := 1
	for _, v := range values {
		f := MustTempFile(dir)
		w, err := tsm1.NewTSMWriter(f)
		if err != nil {
			return nil, err
		}

		if err := w.Write([]byte(v.key), v.values); err != nil {
			return nil, err
		}

		if err := w.WriteIndex(); err != nil {
			return nil, err
		}

		if err := w.Close(); err != nil {
			return nil, err
		}
		newName := filepath.Join(filepath.Dir(f.Name()), tsm1.DefaultFormatFileName(id, 1)+".tsm")
		if err := os.Rename(f.Name(), newName); err != nil {
			return nil, err
		}
		id++

		files = append(files, newName)
	}
	return files, nil

}

func newFiles(dir string, values ...keyValues) ([]string, error) {
	var files []string

	id := 1
	for _, v := range values {
		f := MustTempFile(dir)
		w, err := tsm1.NewTSMWriter(f)
		if err != nil {
			return nil, err
		}

		if err := w.Write([]byte(v.key), v.values); err != nil {
			return nil, err
		}

		if err := w.WriteIndex(); err != nil {
			return nil, err
		}

		if err := w.Close(); err != nil {
			return nil, err
		}

		newName := filepath.Join(filepath.Dir(f.Name()), tsm1.DefaultFormatFileName(id, 1)+".tsm")
		if err := os.Rename(f.Name(), newName); err != nil {
			return nil, err
		}
		id++

		files = append(files, newName)
	}
	return files, nil
}

type keyValues struct {
	key    string
	values []tsm1.Value
}

func MustTempDir() string {
	dir, err := ioutil.TempDir("", "tsm1-test")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp dir: %v", err))
	}
	return dir
}

func MustTempFile(dir string) *os.File {
	f, err := ioutil.TempFile(dir, "tsm1test")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp file: %v", err))
	}
	return f
}

func fatal(t *testing.T, msg string, err error) {
	t.Fatalf("unexpected error %v: %v", msg, err)
}

var fsResult []tsm1.FileStat

func BenchmarkFileStore_Stats(b *testing.B) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// Create some TSM files...
	data := make([]keyValues, 0, 1000)
	for i := 0; i < 1000; i++ {
		data = append(data, keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}})
	}

	_, err := newFileDir(dir, data...)
	if err != nil {
		b.Fatalf("creating benchmark files %v", err)
	}

	fs := tsm1.NewFileStore(dir)
	if testing.Verbose() {
		fs.WithLogger(logger.New(os.Stderr))
	}

	if err := fs.Open(); err != nil {
		b.Fatalf("opening file store %v", err)
	}
	defer fs.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fsResult = fs.Stats()
	}
}

// TestFileStore_Replace_SamePathAndDuplicates is the regression test for
// round-3 Critical 1: Replace must reject (a) newFiles whose normalized final
// path intersects oldFiles, (b) duplicate final targets within newFiles
// (including [.tsm.tmp, .tsm] pairs that normalize to the same path) — before
// any side effect, leaving the store and disk contents untouched.
func TestFileStore_Replace_SamePathAndDuplicates(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// A valid TSM file at path P, written BEFORE Open so the store loads it.
	p := MustWriteTSM(dir, 1, map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {tsm1.NewValue(1, float64(1.0))},
	})

	fs := tsm1.NewFileStore(dir)
	if err := fs.Open(); err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	// (a) final .tsm newFiles equal to an oldFiles path → rejected.
	if err := fs.Replace([]string{p}, []string{p}); err == nil {
		t.Fatal("Replace([P],[P]) must be rejected, got nil error")
	}

	// (b) duplicate final targets within newFiles → rejected.
	if err := fs.Replace(nil, []string{p, p}); err == nil {
		t.Fatal("Replace(nil,[P,P]) must be rejected, got nil error")
	}

	// (c) [.tsm.tmp, .tsm] pair normalizing to the same target → rejected.
	tmpP := p + ".tmp"
	if err := os.WriteFile(tmpP, []byte("placeholder"), 0666); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpP)
	if err := fs.Replace(nil, []string{tmpP, p}); err == nil {
		t.Fatal("Replace(nil,[P.tmp,P]) must be rejected, got nil error")
	}

	// Store must be unchanged: the original file is still readable, count == 1.
	if got := fs.Count(); got != 1 {
		t.Fatalf("store count changed after rejections: got %d, want 1", got)
	}
	if _, err := os.Stat(p); err != nil {
		t.Fatalf("original file was removed by rejected Replace: %v", err)
	}
}

// TestFileStore_Replace_MultiOutputRollback is the regression test for
// round-3 Important 5: when a later newFile fails (here: the second output's
// formal target already exists on disk, tripping the no-clobber guard), all
// completed earlier iterations must be rolled back in reverse order — readers
// closed, formal files renamed back to .tmp — and the original error returned.
// Before the fix, the first output remained as an orphan formal file (loaded
// again at restart, duplicating data) with a leaked reader holding an fd.
func TestFileStore_Replace_MultiOutputRollback(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	// Two valid TSM files that will become .tmp compaction outputs.
	f1 := MustWriteTSM(dir, 1, map[string][]tsm1.Value{
		"cpu,host=A#!~#value": {tsm1.NewValue(1, float64(1.0))},
	})
	f2 := MustWriteTSM(dir, 2, map[string][]tsm1.Value{
		"cpu,host=B#!~#value": {tsm1.NewValue(1, float64(2.0))},
	})

	// Rename both to .tmp as writeNewFiles would.
	tmp1, tmp2 := f1+".tmp", f2+".tmp"
	if err := os.Rename(f1, tmp1); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(f2, tmp2); err != nil {
		t.Fatal(err)
	}

	fs := tsm1.NewFileStore(dir)
	if err := fs.Open(); err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	// Pre-create the formal target of the SECOND output AFTER Open (Open
	// validates every *.tsm in the dir, so an invalid stub must not exist yet)
	// so it trips the no-clobber guard mid-loop, after the first output has
	// been renamed+opened.
	preExisting := filepath.Join(dir, tsm1.DefaultFormatFileName(2, 1)+".tsm")
	if err := os.WriteFile(preExisting, []byte("pre-existing"), 0666); err != nil {
		t.Fatal(err)
	}

	// The first output's formal path must not pre-exist.
	formal1 := filepath.Join(dir, tsm1.DefaultFormatFileName(1, 1)+".tsm")
	os.Remove(formal1)

	err := fs.Replace(nil, []string{tmp1, tmp2})
	if err == nil {
		t.Fatal("expected error from the second output's no-clobber guard")
	}

	// Rollback: the first output's formal file must NOT remain on disk — it must
	// be renamed back to its .tmp name.
	if _, statErr := os.Stat(formal1); statErr == nil {
		t.Fatalf("rollback failed: orphan formal file %s remains on disk", formal1)
	}
	if _, statErr := os.Stat(tmp1); statErr != nil {
		t.Fatalf("rollback failed: %s was not renamed back to .tmp: %v", tmp1, statErr)
	}
	if _, statErr := os.Stat(tmp2); statErr != nil {
		t.Fatalf("second output .tmp missing: %v", statErr)
	}
	if _, statErr := os.Stat(preExisting); statErr != nil {
		t.Fatalf("pre-existing file was clobbered: %v", statErr)
	}
	// Store must be empty (no readers leaked into the active set).
	if got := fs.Count(); got != 0 {
		t.Fatalf("store count after failed replace: got %d, want 0", got)
	}
}

// mustSymlink creates newname as a symlink to oldname, skipping the test when
// the host does not allow creating symlinks (Windows without developer mode or
// administrator privileges).
func mustSymlink(t *testing.T, oldname, newname string) {
	t.Helper()
	if err := os.Symlink(oldname, newname); err != nil {
		t.Skipf("symlinks unavailable on this host: %v", err)
	}
}

// TestFileStore_Replace_RejectsSymlinkSource verifies that a newFiles entry
// whose staged .tsm.tmp path is a symlink to an old file is rejected in
// validation, before any side effect: the install loop renames the source by
// pathname, so a symlink source would land as a formal .tsm symlink and the
// prune loop would delete the real old file while the alias reader survived
// it. The staging name targets a fresh formal path, so only the source
// symlink check can reject it.
func TestFileStore_Replace_RejectsSymlinkSource(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	data := []keyValues{
		keyValues{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
	}
	files, err := newFileDir(dir, data...)
	if err != nil {
		fatal(t, "creating test files", err)
	}

	fs := tsm1.NewFileStore(dir)
	if err := fs.Open(); err != nil {
		fatal(t, "opening file store", err)
	}
	defer fs.Close()

	if got, exp := fs.Count(), 1; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}

	// A .tsm.tmp staging path named after a fresh formal target, symlinked to
	// the old file.
	formal := filepath.Join(dir, tsm1.DefaultFormatFileName(2, 1)+".tsm")
	link := formal + "." + tsm1.TmpTSMFileExtension
	mustSymlink(t, files[0], link)

	err = fs.Replace([]string{files[0]}, []string{link})
	if err == nil {
		t.Fatal("expected replace with a symlink .tsm.tmp source to be rejected")
	}
	if !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("unexpected error: %v", err)
	}

	// Validation must reject before any side effect: the old file is intact,
	// nothing was installed at the formal path (checked with Lstat so a
	// symlink left behind cannot hide) and the store is unchanged.
	if _, err := os.Stat(files[0]); err != nil {
		t.Fatalf("old file must be untouched: %v", err)
	}
	if _, err := os.Lstat(formal); !os.IsNotExist(err) {
		t.Fatalf("no formal file may be created when validation rejects: %v", err)
	}
	if got, exp := fs.Count(), 1; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}
}

// TestFileStore_Replace_RejectsDanglingSymlinkSource verifies that a staged
// .tsm.tmp path that is a dangling symlink is rejected before any side effect
// and nothing remains at the formal path. Before the Lstat fixes, the install
// loop renamed such a symlink by pathname, os.Open on the dangling formal path
// failed, and the Stat-based rollback skipped the rename-back (os.Stat follows
// the link and reports ENOENT), leaving a dangling formal .tsm that made
// FileStore.Open fail at restart. Validation now rejects symlink sources
// outright; the rollback's switch to Lstat stays as defense-in-depth for the
// residual window between validation and install, which is not
// deterministically reachable through the public API.
//
// Note: the fallback stat-guarded rename (errNoReplaceUnsupported), which also
// switched to Lstat, is not exercised here: Windows always has MoveFileEx and
// typical Linux filesystems support renameat2, so reaching that branch would
// require a platform shim. There Lstat refuses a dangling-symlink target
// exactly as renameat2 RENAME_NOREPLACE would.
func TestFileStore_Replace_RejectsDanglingSymlinkSource(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	fs := tsm1.NewFileStore(dir)
	if err := fs.Open(); err != nil {
		fatal(t, "opening file store", err)
	}
	defer fs.Close()

	// A .tsm.tmp staging path that is a dangling symlink.
	formal := filepath.Join(dir, tsm1.DefaultFormatFileName(1, 1)+".tsm")
	link := formal + "." + tsm1.TmpTSMFileExtension
	mustSymlink(t, filepath.Join(dir, "does-not-exist.tsm"), link)

	err := fs.Replace(nil, []string{link})
	if err == nil {
		t.Fatal("expected replace with a dangling symlink .tsm.tmp source to be rejected")
	}
	if !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("unexpected error: %v", err)
	}

	// Nothing may remain at the formal path (Lstat so a symlink cannot hide),
	// the staging symlink itself is untouched, and a fresh store over the same
	// directory still opens — the restart-bricking orphan must not exist.
	if _, err := os.Lstat(formal); !os.IsNotExist(err) {
		t.Fatalf("no formal file may remain: %v", err)
	}
	if _, err := os.Lstat(link); err != nil {
		t.Fatalf("staging symlink must be untouched: %v", err)
	}
	fs2 := tsm1.NewFileStore(dir)
	if err := fs2.Open(); err != nil {
		fatal(t, "reopening file store", err)
	}
	defer fs2.Close()
	if got, exp := fs2.Count(), 0; got != exp {
		t.Fatalf("file count mismatch: got %v, exp %v", got, exp)
	}
}
