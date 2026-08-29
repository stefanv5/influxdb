package tsm1_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/influxdata/influxdb/tsdb/index/inmem"
)

// newEngineWithConfig constructs a tsm1.Engine with a custom tsdb.Config, so
// production wiring (Config → NewEngine → Compactor) can be asserted. Mirrors
// the NewEngine(index) helper in engine_test.go but accepts a config mutator.
func newEngineWithConfig(t *testing.T, mutate func(*tsdb.Config)) *tsm1.Engine {
	t.Helper()
	root, err := os.MkdirTemp("", "tsm1-wiring-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })

	db := "db0"
	dbPath := filepath.Join(root, "data", db)
	if err := os.MkdirAll(dbPath, os.ModePerm); err != nil {
		t.Fatal(err)
	}

	sfile := tsdb.NewSeriesFile(filepath.Join(dbPath, tsdb.SeriesFileDirectory))
	sfile.Logger = logger.New(os.Stdout)
	if err := sfile.Open(); err != nil {
		t.Fatal(err)
	}

	opt := tsdb.NewEngineOptions()
	opt.InmemIndex = inmem.NewIndex(db, sfile)
	seriesIDs := tsdb.NewSeriesIDSet()
	opt.SeriesIDSets = seriesIDSets([]*tsdb.SeriesIDSet{seriesIDs})
	if mutate != nil {
		mutate(&opt.Config)
	}

	idxPath := filepath.Join(dbPath, "index")
	idx := tsdb.MustOpenIndex(1, db, idxPath, seriesIDs, sfile, opt)

	e := tsm1.NewEngine(1, idx, filepath.Join(root, "data"), filepath.Join(root, "wal"), sfile, opt).(*tsm1.Engine)
	return e
}

// TestEngine_StreamingCompactionWiring is the production-chain test for
// round-3 Important 6: tsdb.Config.StreamingCompactionEnabled must flow
// through NewEngine into Compactor.UseStreamingIterator, so the experimental
// streaming iterator is actually configurable in a running influxd (not
// test-only).
func TestEngine_StreamingCompactionWiring(t *testing.T) {
	// Default config: streaming disabled → legacy iterator.
	e := newEngineWithConfig(t, nil)
	if e.Compactor.UseStreamingIterator {
		t.Fatal("default config must leave UseStreamingIterator=false (legacy iterator)")
	}
	e.Close()

	// Enabled config: streaming iterator wired through.
	e2 := newEngineWithConfig(t, func(c *tsdb.Config) {
		c.StreamingCompactionEnabled = true
	})
	if !e2.Compactor.UseStreamingIterator {
		t.Fatal("StreamingCompactionEnabled=true must set UseStreamingIterator=true")
	}
	e2.Close()
}
