package tsm1

import (
	"sync/atomic"
)

var defaultWeights = [4]float64{0.4, 0.3, 0.2, 0.1}

type scheduler struct {
	maxConcurrency int
	stats          *EngineStatistics

	// queues is the depth of work pending for each compaction level
	queues  [4]int
	weights [4]float64

	// boostLowPriority is set while a rolling full compaction is mid-flight so
	// level-4 rounds are not starved by sustained level-1 work. Accessed
	// atomically (set by setBoostLowPriority, read by next).
	boostLowPriority int32
}

func newScheduler(stats *EngineStatistics, maxConcurrency int) *scheduler {
	return &scheduler{
		stats:          stats,
		maxConcurrency: maxConcurrency,
		weights:        defaultWeights,
	}
}

func (s *scheduler) setDepth(level, depth int) {
	level = level - 1
	if level < 0 || level > len(s.queues) {
		return
	}

	s.queues[level] = depth
}

func (s *scheduler) next() (int, bool) {
	level1Running := int(atomic.LoadInt64(&s.stats.TSMCompactionsActive[0]))
	level2Running := int(atomic.LoadInt64(&s.stats.TSMCompactionsActive[1]))
	level3Running := int(atomic.LoadInt64(&s.stats.TSMCompactionsActive[2]))
	level4Running := int(atomic.LoadInt64(&s.stats.TSMFullCompactionsActive) + atomic.LoadInt64(&s.stats.TSMOptimizeCompactionsActive))

	if level1Running+level2Running+level3Running+level4Running >= s.maxConcurrency {
		return 0, false
	}

	var (
		level    int
		runnable bool
	)

	loLimit, _ := s.limits()

	end := len(s.queues)
	if level3Running+level4Running >= loLimit && s.maxConcurrency-(level1Running+level2Running) == 0 {
		end = 2
	}

	// While a rolling full compaction is mid-flight, boost level-4 to level-1's
	// weight so its rounds are not starved by sustained level-1 work (L4's
	// default weight is 0.1 vs L1's 0.4 — with one L1 group queued every tick,
	// an unboosted L4 would never be scheduled and the rolling would stall).
	boostL4 := atomic.LoadInt32(&s.boostLowPriority) == 1

	var weight float64
	for i := 0; i < end; i++ {
		w := s.weights[i]
		if boostL4 && i == 3 {
			w = s.weights[0]
		}
		score := float64(s.queues[i]) * w

		// The scan runs L1→L4, so with a plain strict > the earlier level wins
		// equal-score ties. While the boost is active, level 4 must instead win
		// ties: rolling compaction yields exactly ONE level-4 group per tick,
		// so at a boosted tie (L1 depth 1 vs L4 depth 1, both scoring 0.4 now
		// that the boost raises L4 to L1's weight) the strict > scan would pick
		// L1 every tick and the rolling would starve forever despite the
		// weight bump. An empty level-4 queue never wins the tie-break.
		prefer := score > weight
		if boostL4 && i == 3 && s.queues[i] > 0 {
			prefer = score >= weight
		}
		if prefer {
			level, runnable = i+1, true
			weight = score
		}
	}
	return level, runnable
}

// setBoostLowPriority enables the level-4 weight boost while a rolling full
// compaction is mid-flight (wired from DefaultPlanner.RollingInProgress by the
// engine's compact loop). Safe for concurrent use with next().
func (s *scheduler) setBoostLowPriority(on bool) {
	v := int32(0)
	if on {
		v = 1
	}
	atomic.StoreInt32(&s.boostLowPriority, v)
}

func (s *scheduler) limits() (int, int) {
	hiLimit := s.maxConcurrency * 4 / 5
	loLimit := (s.maxConcurrency / 5) + 1
	if hiLimit == 0 {
		hiLimit = 1
	}

	if loLimit == 0 {
		loLimit = 1
	}

	return loLimit, hiLimit
}
