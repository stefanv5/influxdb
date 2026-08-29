package tsm1_test

import (
	"sort"
	"testing"
	"time"

	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// makeFullCompactionFileSet returns N level-4 TSM FileStats with distinct
// generations (000001-04.tsm ... 0000NN-04.tsm), each 256MB so the full
// planner's skip-over-max-size logic (compact.go:473) does not skip them.
func makeFullCompactionFileSet(n int) []tsm1.FileStat {
	stats := make([]tsm1.FileStat, n)
	for i := 0; i < n; i++ {
		stats[i] = tsm1.FileStat{
			Path: tsm1.DefaultFormatFileName(i+1, 4) + "." + tsm1.TSMFileExtension,
			Size: 256 * 1024 * 1024,
		}
	}
	return stats
}

// plannerFileStore is a minimal fileStore for planner tests: it serves a fixed
// set of FileStats and a configurable BlockCount. ParseFileName delegates to
// tsm1.DefaultParseFileName so generation/sequence parsing matches production.
type plannerFileStore struct {
	stats      []tsm1.FileStat
	lastMod    time.Time
	blockCount int
}

func (f *plannerFileStore) Stats() []tsm1.FileStat { return f.stats }
func (f *plannerFileStore) LastModified() time.Time { return f.lastMod }
func (f *plannerFileStore) BlockCount(path string, idx int) int { return f.blockCount }
func (f *plannerFileStore) ParseFileName(path string) (int, int, error) {
	return tsm1.DefaultParseFileName(path)
}

// TestDefaultPlanner_Plan_RollingFull verifies that when the eligible file
// count exceeds maxFullCompactionFiles, Plan returns only the oldest K files
// (one round), capped at K, oldest-generation-first. Convergence across rounds
// is exercised by TestDefaultPlanner_Plan_RollingFull_PointSetEqual with a
// real compactor; here we assert the single-round split invariant.
func TestDefaultPlanner_Plan_RollingFull(t *testing.T) {
	const N, K = 50, 8
	stats := makeFullCompactionFileSet(N)
	fs := &plannerFileStore{stats: stats, lastMod: time.Now(), blockCount: 1000}

	cp := tsm1.NewDefaultPlanner(fs, time.Nanosecond)
	cp.SetMaxFullCompactionFiles(K)

	plan := cp.Plan(time.Now().Add(-time.Second))
	if len(plan) != 1 {
		t.Fatalf("expected 1 group, got %d", len(plan))
	}
	if got, exp := len(plan[0]), K; got != exp {
		t.Fatalf("expected %d files (capped), got %d (%v)", exp, got, plan[0])
	}
	// Oldest-first: the K lowest generation numbers.
	files := append([]string(nil), plan[0]...)
	sort.Strings(files)
	for i, f := range files {
		g, _, err := tsm1.DefaultParseFileName(f)
		if err != nil {
			t.Fatal(err)
		}
		if g != i+1 {
			t.Fatalf("capped round file %d: expected gen %d (oldest-first), got %d", i, i+1, g)
		}
	}
	cp.Release(plan)
}

// TestDefaultPlanner_Plan_RollingAtomic verifies that once a rolling full is
// in flight, Plan continues to schedule rounds even when the cold-duration
// gate would NOT trigger (shard became hot again), until convergence.
func TestDefaultPlanner_Plan_RollingAtomic(t *testing.T) {
	const N, K = 30, 8
	stats := makeFullCompactionFileSet(N)
	fs := &plannerFileStore{stats: stats, lastMod: time.Now(), blockCount: 1000}

	cp := tsm1.NewDefaultPlanner(fs, time.Hour) // 1h cold gate
	cp.SetMaxFullCompactionFiles(K)

	// Cold shard: gate triggers, rolling starts.
	plan := cp.Plan(time.Now().Add(-2 * time.Hour))
	if len(plan) == 0 || len(plan[0]) != K {
		t.Fatalf("round 1: expected %d files, got %v", K, plan)
	}
	cp.Release(plan)

	// Shard becomes hot: lastWrite is now. Without rollingInProgress the gate
	// (1h) would NOT trigger. Assert Plan still returns a round (rolling
	// atomicity: the gate is skipped while rollingInProgress).
	plan = cp.Plan(time.Now())
	if len(plan) == 0 {
		t.Fatalf("round 2: rolling full aborted prematurely by hot-shard gate; expected continuation")
	}
	if len(plan[0]) > K {
		t.Fatalf("round 2: cap exceeded: %d > %d", len(plan[0]), K)
	}
	cp.Release(plan)
}

// TestDefaultPlanner_Plan_NoRollingWhenUnderCap verifies that when the file
// count is ≤ maxFullCompactionFiles, Plan behaves exactly as legacy: one group
// with all files, no rolling flag set.
func TestDefaultPlanner_Plan_NoRollingWhenUnderCap(t *testing.T) {
	const N, K = 5, 8
	stats := makeFullCompactionFileSet(N)
	fs := &plannerFileStore{stats: stats, lastMod: time.Now(), blockCount: 1000}

	cp := tsm1.NewDefaultPlanner(fs, time.Nanosecond)
	cp.SetMaxFullCompactionFiles(K)

	plan := cp.Plan(time.Now().Add(-time.Second))
	if len(plan) != 1 || len(plan[0]) != N {
		t.Fatalf("expected single group of %d files, got %v", N, plan)
	}
}

// TestDefaultPlanner_Plan_MaxKZeroDisabled verifies that maxFullCompactionFiles=0
// (rolling disabled) preserves the legacy single-group behavior: ALL eligible
// files are returned in one group even when N exceeds the default cap, and
// rolling is not engaged (a second Plan after Release returns the same full set
// rather than a K-file slice, which would indicate rollingInProgress was set).
func TestDefaultPlanner_Plan_MaxKZeroDisabled(t *testing.T) {
	const N = 50
	stats := makeFullCompactionFileSet(N)
	fs := &plannerFileStore{stats: stats, lastMod: time.Now(), blockCount: 1000}

	cp := tsm1.NewDefaultPlanner(fs, time.Nanosecond)
	cp.SetMaxFullCompactionFiles(0) // rolling disabled

	plan := cp.Plan(time.Now().Add(-time.Second))
	if len(plan) != 1 {
		t.Fatalf("expected 1 group, got %d", len(plan))
	}
	if got, exp := len(plan[0]), N; got != exp {
		t.Fatalf("maxK=0 should return all %d files (legacy), got %d", exp, got)
	}
	cp.Release(plan)

	// Second plan after release: rolling was never engaged, so it returns the
	// full set again (not a K-slice). This indirectly asserts rollingInProgress
	// was not set.
	plan2 := cp.Plan(time.Now().Add(-time.Second))
	if len(plan2) != 1 || len(plan2[0]) != N {
		t.Fatalf("maxK=0 second plan: expected full set of %d, got %v", N, plan2)
	}
	cp.Release(plan2)
}

// TestDefaultPlanner_Plan_RollingFull_NoMidGenerationSplit is the regression test
// for Issue 1 (Critical): rolling truncation must NOT cut mid-generation. If it did,
// compact() would derive maxSeq from only the truncated input → output filename
// collides with an excluded higher-seq file of the same generation → os.Rename
// silently overwrites it → permanent data loss.
//
// Layout: gen 1-7 each seq=4, gen 8 seq=4 AND seq=5 (two files same gen), gen 9 seq=4.
// maxK=8: the 8th sorted file is gen8-seq4, the 9th is gen8-seq5 (SAME GENERATION).
// Fix: Plan must back off to before gen8 (return gen1-7 only, 7 files).
func TestDefaultPlanner_Plan_RollingFull_NoMidGenerationSplit(t *testing.T) {
	// Construct FileStats with multi-sequence-per-generation.
	fileNames := []string{
		"000000001-000000004.tsm",
		"000000002-000000004.tsm",
		"000000003-000000004.tsm",
		"000000004-000000004.tsm",
		"000000005-000000004.tsm",
		"000000006-000000004.tsm",
		"000000007-000000004.tsm",
		"000000008-000000004.tsm", // 8th file (gen8, seq4)
		"000000008-000000005.tsm", // 9th file (gen8, seq5) — SAME GEN as 8th!
		"000000009-000000004.tsm", // 10th file (gen9, seq4)
	}
	stats := make([]tsm1.FileStat, len(fileNames))
	for i, f := range fileNames {
		stats[i] = tsm1.FileStat{Path: f, Size: 256 * 1024 * 1024}
	}

	fs := &plannerFileStore{stats: stats, lastMod: time.Now(), blockCount: 1000}
	cp := tsm1.NewDefaultPlanner(fs, time.Nanosecond)
	cp.SetMaxFullCompactionFiles(8)

	plan := cp.Plan(time.Now().Add(-time.Second))
	if len(plan) != 1 {
		t.Fatalf("expected 1 group, got %d", len(plan))
	}

	// MUST NOT include any gen8 file — including gen8-seq4 but excluding gen8-seq5
	// would split the generation and cause output filename collision + data loss.
	for _, f := range plan[0] {
		g, _, _ := tsm1.DefaultParseFileName(f)
		if g == 8 {
			t.Fatalf("Plan included gen8 file %s — generation was split! Data loss risk.", f)
		}
	}

	// Should return gen 1-7 (7 files), excluding gen8 entirely.
	if len(plan[0]) != 7 {
		t.Fatalf("expected 7 files (gen1-7, gen8 backed off), got %d: %v", len(plan[0]), plan[0])
	}
	cp.Release(plan)

	// Verify: the 7th file is gen7-seq4, NOT gen8-seq4.
	last, _, _ := tsm1.DefaultParseFileName(plan[0][len(plan[0])-1])
	if last != 7 {
		t.Fatalf("last file should be gen7, got gen %d", last)
	}
}

// TestDefaultPlanner_Plan_RollingFull_OversizedGeneration is the regression test
// for round-2 Issue 2: when the first generation alone has >= maxK sequence files,
// the old backoff logic hit cutIdx<=1 and returned ALL files, silently defeating
// the K cap (RSS surge). The fix selects whole generations: gen1's 9 files exceed
// maxK=8, so the round includes gen1 (9 files, capped overflow of one generation)
// plus the next generation (gen2) to guarantee convergence progress — NOT all files.
func TestDefaultPlanner_Plan_RollingFull_OversizedGeneration(t *testing.T) {
	// gen1: 9 sequence files; gen2, gen3, gen4: 1 file each (12 files total). maxK=8.
	// Expected: gen1 (9, first-gen overflow) + gen2 (extension for convergence)
	// = 10 files — bounded, NOT all 12 (the old cutIdx<=1 bypass returned everything).
	fileNames := []string{
		"000000001-000000001.tsm",
		"000000001-000000002.tsm",
		"000000001-000000003.tsm",
		"000000001-000000004.tsm",
		"000000001-000000005.tsm",
		"000000001-000000006.tsm",
		"000000001-000000007.tsm",
		"000000001-000000008.tsm",
		"000000001-000000009.tsm", // 9th gen1 file
		"000000002-000000001.tsm", // gen2
		"000000003-000000001.tsm", // gen3
		"000000004-000000001.tsm", // gen4
	}
	stats := make([]tsm1.FileStat, len(fileNames))
	for i, f := range fileNames {
		stats[i] = tsm1.FileStat{Path: f, Size: 256 * 1024 * 1024}
	}

	fs := &plannerFileStore{stats: stats, lastMod: time.Now(), blockCount: 1000}
	cp := tsm1.NewDefaultPlanner(fs, time.Nanosecond)
	cp.SetMaxFullCompactionFiles(8)

	plan := cp.Plan(time.Now().Add(-time.Second))
	if len(plan) != 1 {
		t.Fatalf("expected 1 group, got %d", len(plan))
	}
	// The K cap must not return ALL 12 files (the cutIdx<=1 bypass).
	if got := len(plan[0]); got >= len(fileNames) {
		t.Fatalf("K cap defeated: returned all %d files, expected gen1+gen2 (10)", got)
	}
	// Exactly gen1 (9 files) + gen2 (1 file): first-gen overflow + convergence extension.
	if got := len(plan[0]); got != 10 {
		t.Fatalf("expected 10 files (gen1 atomic 9 + gen2 1), got %d: %v", got, plan[0])
	}
	// Every returned file must be a whole generation: no gen1 file excluded while
	// a gen1 file is included (no mid-generation cut).
	gens := map[int]bool{}
	for _, f := range plan[0] {
		g, _, _ := tsm1.DefaultParseFileName(f)
		if g != 1 && g != 2 {
			t.Fatalf("unexpected gen %d in plan", g)
		}
		gens[g] = true
	}
	// If gen1 is selected, ALL gen1 files must be included (atomicity).
	if gens[1] {
		count := 0
		for _, f := range plan[0] {
			g, _, _ := tsm1.DefaultParseFileName(f)
			if g == 1 {
				count++
			}
		}
		if count != 9 {
			t.Fatalf("gen1 split: %d/9 files included — mid-generation cut!", count)
		}
	}
	cp.Release(plan)
}

// TestDefaultPlanner_Plan_RollingFull_SingleGenExtended is the regression test for
// round-2 Issue 3: when generation-atomic selection yields a single generation
// whose compacted output would keep the same shape (8 near-2GB files), rolling
// would repeat the identical plan forever. The fix extends the group to include
// the next whole generation so the round merges >= 2 generations and progresses.
func TestDefaultPlanner_Plan_RollingFull_SingleGenExtended(t *testing.T) {
	// gen10: 8 files (would be selected as one whole generation under maxK=8);
	// gen11: 1 file. The plan MUST include gen11 so the round has 2 generations.
	fileNames := []string{
		"000000010-000000001.tsm",
		"000000010-000000002.tsm",
		"000000010-000000003.tsm",
		"000000010-000000004.tsm",
		"000000010-000000005.tsm",
		"000000010-000000006.tsm",
		"000000010-000000007.tsm",
		"000000010-000000008.tsm",
		"000000011-000000001.tsm", // gen11
	}
	stats := make([]tsm1.FileStat, len(fileNames))
	for i, f := range fileNames {
		// Small sizes so the >2GB skip-large-file logic does not skip gen10.
		stats[i] = tsm1.FileStat{Path: f, Size: 256 * 1024 * 1024}
	}

	fs := &plannerFileStore{stats: stats, lastMod: time.Now(), blockCount: 1000}
	cp := tsm1.NewDefaultPlanner(fs, time.Nanosecond)
	cp.SetMaxFullCompactionFiles(8)

	plan := cp.Plan(time.Now().Add(-time.Second))
	if len(plan) != 1 {
		t.Fatalf("expected 1 group, got %d", len(plan))
	}
	hasGen10, hasGen11 := false, false
	for _, f := range plan[0] {
		g, _, _ := tsm1.DefaultParseFileName(f)
		switch g {
		case 10:
			hasGen10 = true
		case 11:
			hasGen11 = true
		}
	}
	if !hasGen10 || !hasGen11 {
		t.Fatalf("plan must include both gen10 and gen11 (single-gen round cannot converge): gen10=%v gen11=%v", hasGen10, hasGen11)
	}
	// gen10 must be complete (atomicity).
	gen10Count := 0
	for _, f := range plan[0] {
		g, _, _ := tsm1.DefaultParseFileName(f)
		if g == 10 {
			gen10Count++
		}
	}
	if gen10Count != 8 {
		t.Fatalf("gen10 split: %d/8 files included", gen10Count)
	}
	cp.Release(plan)
}

// Ensure tsdb import is used (DefaultMaxPointsPerBlock referenced in skip logic context).
var _ = tsdb.DefaultMaxPointsPerBlock
