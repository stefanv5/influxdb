package tsdb_test

import (
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/influxdb/tsdb"
)

func TestConfig_Parse(t *testing.T) {
	// Parse configuration.
	c := tsdb.NewConfig()
	if _, err := toml.Decode(`
dir = "/var/lib/influxdb/data"
wal-dir = "/var/lib/influxdb/wal"
wal-fsync-delay = "10s"
tsm-use-madv-willneed = true
`, &c); err != nil {
		t.Fatal(err)
	}

	if err := c.Validate(); err != nil {
		t.Errorf("unexpected validate error: %s", err)
	}

	if got, exp := c.Dir, "/var/lib/influxdb/data"; got != exp {
		t.Errorf("unexpected dir:\n\nexp=%v\n\ngot=%v\n\n", exp, got)
	}
	if got, exp := c.WALDir, "/var/lib/influxdb/wal"; got != exp {
		t.Errorf("unexpected wal-dir:\n\nexp=%v\n\ngot=%v\n\n", exp, got)
	}
	if got, exp := c.WALFsyncDelay, time.Duration(10*time.Second); time.Duration(got).Nanoseconds() != exp.Nanoseconds() {
		t.Errorf("unexpected wal-fsync-delay:\n\nexp=%v\n\ngot=%v\n\n", exp, got)
	}
	if got, exp := c.TSMWillNeed, true; got != exp {
		t.Errorf("unexpected tsm-madv-willneed:\n\nexp=%v\n\ngot=%v\n\n", exp, got)
	}
}

func TestConfig_Validate_Error(t *testing.T) {
	c := tsdb.NewConfig()
	if err := c.Validate(); err == nil || err.Error() != "Data.Dir must be specified" {
		t.Errorf("unexpected error: %s", err)
	}

	c.Dir = "/var/lib/influxdb/data"
	if err := c.Validate(); err == nil || err.Error() != "Data.WALDir must be specified" {
		t.Errorf("unexpected error: %s", err)
	}

	c.WALDir = "/var/lib/influxdb/wal"
	c.Engine = "fake1"
	if err := c.Validate(); err == nil || err.Error() != "unrecognized engine fake1" {
		t.Errorf("unexpected error: %s", err)
	}

	c.Engine = "tsm1"
	c.Index = "foo"
	if err := c.Validate(); err == nil || err.Error() != "unrecognized index foo" {
		t.Errorf("unexpected error: %s", err)
	}

	c.Index = tsdb.InmemIndexName
	if err := c.Validate(); err != nil {
		t.Error(err)
	}

	c.Index = tsdb.TSI1IndexName
	if err := c.Validate(); err != nil {
		t.Error(err)
	}

	c.SeriesIDSetCacheSize = -1
	if err := c.Validate(); err == nil || err.Error() != "series-id-set-cache-size must be non-negative" {
		t.Errorf("unexpected error: %s", err)
	}
}

func TestConfig_CompactionBounds_Fields(t *testing.T) {
	// NewConfig defaults.
	c := tsdb.NewConfig()
	if got, exp := c.MaxFullCompactionFiles, tsdb.DefaultMaxFullCompactionFiles; got != exp {
		t.Errorf("unexpected default max-full-compaction-files: got=%d exp=%d", got, exp)
	}
	// The default must disable rolling (0 = legacy single-group full
	// compaction); rolling is opt-in.
	if got, exp := c.MaxFullCompactionFiles, 0; got != exp {
		t.Errorf("unexpected default max-full-compaction-files: got=%d exp=%d", got, exp)
	}
	if got, exp := c.MaxTSMFileSizeForMmap, tsdb.DefaultMaxTSMFileSizeForMmap; got != exp {
		t.Errorf("unexpected default max-tsm-file-size-for-mmap: got=%d exp=%d", got, exp)
	}

	// TOML parse.
	c = tsdb.NewConfig()
	if _, err := toml.Decode(`
dir = "/var/lib/influxdb/data"
wal-dir = "/var/lib/influxdb/wal"
max-full-compaction-files = 12
max-tsm-file-size-for-mmap = 1073741824
streaming-compaction-enabled = true
`, &c); err != nil {
		t.Fatal(err)
	}
	if got, exp := c.MaxFullCompactionFiles, 12; got != exp {
		t.Errorf("unexpected max-full-compaction-files: got=%d exp=%d", got, exp)
	}
	if got, exp := c.MaxTSMFileSizeForMmap, int64(1073741824); got != exp {
		t.Errorf("unexpected max-tsm-file-size-for-mmap: got=%d exp=%d", got, exp)
	}
	if got, exp := c.StreamingCompactionEnabled, true; got != exp {
		t.Errorf("unexpected streaming-compaction-enabled: got=%t exp=%t", got, exp)
	}
	if err := c.Validate(); err != nil {
		t.Errorf("unexpected validate error: %s", err)
	}

	// TOML round-trip of the default streaming value (false).
	c = tsdb.NewConfig()
	if _, err := toml.Decode(`
dir = "/var/lib/influxdb/data"
wal-dir = "/var/lib/influxdb/wal"
streaming-compaction-enabled = false
`, &c); err != nil {
		t.Fatal(err)
	}
	if got, exp := c.StreamingCompactionEnabled, false; got != exp {
		t.Errorf("unexpected streaming-compaction-enabled: got=%t exp=%t", got, exp)
	}
	// The unset default must also be disabled.
	if got, exp := tsdb.NewConfig().StreamingCompactionEnabled, false; got != exp {
		t.Errorf("unexpected default streaming-compaction-enabled: got=%t exp=%t", got, exp)
	}
}

func TestConfig_CompactionBounds_Validate(t *testing.T) {
	c := tsdb.NewConfig()
	c.Dir = "/var/lib/influxdb/data"
	c.WALDir = "/var/lib/influxdb/wal"

	// Negative MaxFullCompactionFiles rejected.
	c.MaxFullCompactionFiles = -1
	if err := c.Validate(); err == nil || err.Error() != "max-full-compaction-files must be non-negative" {
		t.Errorf("unexpected error: %s", err)
	}

	// Below-4 positive MaxFullCompactionFiles rejected.
	c.MaxFullCompactionFiles = 3
	if err := c.Validate(); err == nil || err.Error() != "max-full-compaction-files must be 0 (disabled) or >= 4" {
		t.Errorf("unexpected error: %s", err)
	}

	// Zero (disabled) and >=4 accepted.
	c.MaxFullCompactionFiles = 0
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	c.MaxFullCompactionFiles = 8
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}

	// The shipped default (0, rolling disabled) is a legal value.
	c.MaxFullCompactionFiles = tsdb.DefaultMaxFullCompactionFiles
	if err := c.Validate(); err != nil {
		t.Fatalf("default max-full-compaction-files (%d) must validate: %s", c.MaxFullCompactionFiles, err)
	}

	// MaxTSMFileSizeForMmap < -1 rejected.
	c.MaxTSMFileSizeForMmap = -2
	if err := c.Validate(); err == nil || err.Error() != "max-tsm-file-size-for-mmap must be -1 (always mmap), 0 (always pread), or a positive byte threshold" {
		t.Errorf("unexpected error: %s", err)
	}

	// -1 (legacy mmap), 0 (always pread) accepted.
	c.MaxTSMFileSizeForMmap = -1
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	c.MaxTSMFileSizeForMmap = 0
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestConfig_CompactionBounds_Diagnostics(t *testing.T) {
	c := tsdb.NewConfig()
	c.Dir = "/var/lib/influxdb/data"
	c.WALDir = "/var/lib/influxdb/wal"
	c.MaxFullCompactionFiles = 8
	c.MaxTSMFileSizeForMmap = 1073741824
	c.StreamingCompactionEnabled = true

	d, err := c.Diagnostics()
	if err != nil {
		t.Fatal(err)
	}

	row := map[string]interface{}{}
	for i, col := range d.Columns {
		row[col] = d.Rows[0][i]
	}

	exp := map[string]interface{}{
		"max-full-compaction-files":    8,
		"max-tsm-file-size-for-mmap":   int64(1073741824),
		"streaming-compaction-enabled": true,
	}
	for k, want := range exp {
		got, ok := row[k]
		if !ok {
			t.Errorf("diagnostics missing key %q", k)
			continue
		}
		if got != want {
			t.Errorf("unexpected diagnostics %q: got=%v (%T) exp=%v (%T)", k, got, got, want, want)
		}
	}
}

func TestConfig_ByteSizes(t *testing.T) {
	// Parse configuration.
	c := tsdb.NewConfig()
	if _, err := toml.Decode(`
dir = "/var/lib/influxdb/data"
wal-dir = "/var/lib/influxdb/wal"
wal-fsync-delay = "10s"
cache-max-memory-size = 5368709120
cache-snapshot-memory-size = 104857600
`, &c); err != nil {
		t.Fatal(err)
	}

	if err := c.Validate(); err != nil {
		t.Errorf("unexpected validate error: %s", err)
	}

	if got, exp := c.Dir, "/var/lib/influxdb/data"; got != exp {
		t.Errorf("unexpected dir:\n\nexp=%v\n\ngot=%v\n\n", exp, got)
	}
	if got, exp := c.WALDir, "/var/lib/influxdb/wal"; got != exp {
		t.Errorf("unexpected wal-dir:\n\nexp=%v\n\ngot=%v\n\n", exp, got)
	}
	if got, exp := c.WALFsyncDelay, time.Duration(10*time.Second); time.Duration(got).Nanoseconds() != exp.Nanoseconds() {
		t.Errorf("unexpected wal-fsync-delay:\n\nexp=%v\n\ngot=%v\n\n", exp, got)
	}
	if got, exp := c.CacheMaxMemorySize, uint64(5<<30); uint64(got) != exp {
		t.Errorf("unexpected cache-max-memory-size:\n\nexp=%v\n\ngot=%v\n\n", exp, got)
	}
	if got, exp := c.CacheSnapshotMemorySize, uint64(100<<20); uint64(got) != exp {
		t.Errorf("unexpected cache-snapshot-memory-size:\n\nexp=%v\n\ngot=%v\n\n", exp, got)
	}
}

func TestConfig_HumanReadableSizes(t *testing.T) {
	// Parse configuration.
	c := tsdb.NewConfig()
	if _, err := toml.Decode(`
dir = "/var/lib/influxdb/data"
wal-dir = "/var/lib/influxdb/wal"
wal-fsync-delay = "10s"
cache-max-memory-size = "5g"
cache-snapshot-memory-size = "100m"
`, &c); err != nil {
		t.Fatal(err)
	}

	if err := c.Validate(); err != nil {
		t.Errorf("unexpected validate error: %s", err)
	}

	if got, exp := c.Dir, "/var/lib/influxdb/data"; got != exp {
		t.Errorf("unexpected dir:\n\nexp=%v\n\ngot=%v\n\n", exp, got)
	}
	if got, exp := c.WALDir, "/var/lib/influxdb/wal"; got != exp {
		t.Errorf("unexpected wal-dir:\n\nexp=%v\n\ngot=%v\n\n", exp, got)
	}
	if got, exp := c.WALFsyncDelay, time.Duration(10*time.Second); time.Duration(got).Nanoseconds() != exp.Nanoseconds() {
		t.Errorf("unexpected wal-fsync-delay:\n\nexp=%v\n\ngot=%v\n\n", exp, got)
	}
	if got, exp := c.CacheMaxMemorySize, uint64(5<<30); uint64(got) != exp {
		t.Errorf("unexpected cache-max-memory-size:\n\nexp=%v\n\ngot=%v\n\n", exp, got)
	}
	if got, exp := c.CacheSnapshotMemorySize, uint64(100<<20); uint64(got) != exp {
		t.Errorf("unexpected cache-snapshot-memory-size:\n\nexp=%v\n\ngot=%v\n\n", exp, got)
	}
}
