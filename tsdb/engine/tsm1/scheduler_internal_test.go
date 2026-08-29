package tsm1

import (
	"testing"
)

// TestScheduler_BoostLowPriorityWhileRolling verifies the level-4 boost that
// keeps a rolling full compaction from starving under sustained level-1 work.
//
// The boost does two things: it raises L4's weight to L1's (0.4), and it makes
// a boosted L4 win equal-score ties. The tie-break is essential because
// next() scans L1→L4 and the rolling planner produces exactly ONE level-4
// group per tick: with a strict > scan, L1 depth 1 vs boosted L4 depth 1
// (both scoring 0.4) would send every tick to L1 by scan order and the
// rolling would never advance — the exact starvation this fixes (round-3
// Important 2, confirmed in round 4).
func TestScheduler_BoostLowPriorityWhileRolling(t *testing.T) {
	s := newScheduler(&EngineStatistics{}, 4)
	s.setDepth(1, 1)
	s.setDepth(4, 1)

	// Unboosted tie: L1 (0.4) beats L4 (0.1) by scan order.
	if level, ok := s.next(); !ok || level != 1 {
		t.Fatalf("unboosted tie: expected level 1, got %d (ok=%v)", level, ok)
	}

	// Unboosted, even a deeper L4 queue loses to L1 (2*0.1=0.2 < 0.4).
	s.setDepth(4, 2)
	if level, ok := s.next(); !ok || level != 1 {
		t.Fatalf("unboosted deeper L4: expected level 1, got %d (ok=%v)", level, ok)
	}

	// Boosted (rolling in flight): deeper L4 (2*0.4=0.8) beats L1 (0.4).
	s.setBoostLowPriority(true)
	if level, ok := s.next(); !ok || level != 4 {
		t.Fatalf("boosted deeper L4: expected level 4, got %d (ok=%v)", level, ok)
	}

	// Boosted tie at equal depth: L4 must WIN. This is the starvation
	// scenario — rolling yields one L4 group per tick, so a tie broken toward
	// L1 would keep L4 unscheduled forever.
	s.setDepth(4, 1)
	if level, ok := s.next(); !ok || level != 4 {
		t.Fatalf("boosted tie: expected level 4, got %d (ok=%v)", level, ok)
	}

	// Boosted with strictly deeper L1: L1 2*0.4=0.8 beats L4 0.4 — the
	// tie-break only promotes L4 on equal-or-better scores, never below L1.
	s.setDepth(1, 2)
	if level, ok := s.next(); !ok || level != 1 {
		t.Fatalf("boosted deeper L1: expected level 1, got %d (ok=%v)", level, ok)
	}

	// Boosted with an empty L4 queue: the >= tie-break must not phantom-pick
	// an idle level 4.
	s.setDepth(1, 1)
	s.setDepth(4, 0)
	if level, ok := s.next(); !ok || level != 1 {
		t.Fatalf("boosted empty L4: expected level 1, got %d (ok=%v)", level, ok)
	}

	// Boost cleared: the plain strict-> scan returns, L1 wins the tie again.
	s.setBoostLowPriority(false)
	s.setDepth(4, 1)
	if level, ok := s.next(); !ok || level != 1 {
		t.Fatalf("boost cleared: expected level 1, got %d (ok=%v)", level, ok)
	}
}
