package sim

import (
	"flag"
	"sync"
	"sync/atomic"
	"testing"
)

var kvSeeds = flag.Int("kvseeds", 100, "how many seeds TestLinearizable runs")

// TestLinearizable runs the key-value store under the same faults as TestSafety, with clients
// that retry through timeouts and leader changes, and checks every run's history with Porcupine.
func TestLinearizable(t *testing.T) {
	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) { testLinearizable(t, m.set) })
	}
}

func testLinearizable(t *testing.T, mode func(*Config)) {
	first, n := *from, uint64(*kvSeeds)
	if *only != 0 {
		first, n = *only, 1
	}
	if testing.Short() {
		n = min(n, 15)
	}
	var mu sync.Mutex
	var total Stats
	var inconclusive, failed atomic.Int32
	parallel(first, n, func(seed uint64) {
		cfg := KVDefaults(seed)
		mode(&cfg)
		r := Run(cfg)
		mu.Lock()
		defer mu.Unlock()
		add(&total, r.Stats)
		total.Ops += r.Stats.Ops
		total.Retries += r.Stats.Retries
		if r.Inconclusive {
			inconclusive.Add(1)
		}
		if r.Violation != "" && failed.Add(1) == 1 {
			t.Errorf("%s\nreproduce: go test ./sim -run %s -seed %d -v", r, t.Name(), seed)
		}
		if r.Stats.Ops < 100 {
			t.Errorf("seed %d: only %d operations completed", seed, r.Stats.Ops)
		}
	})
	t.Logf("%d seeds: ops=%d retries=%d leaders=%d crashes=%d partitions=%d msgs=%d inconclusive=%d",
		n, total.Ops, total.Retries, total.Leaders, total.Crashes, total.Partitions, total.Sent, inconclusive.Load())
	if inconclusive.Load() > 0 {
		t.Errorf("%d runs were too big for the checker to decide", inconclusive.Load())
	}
}

// Controls for the key-value layer.

func TestControlNoDedupIsCaught(t *testing.T) {
	requireCaughtKV(t, func(c *Config) { c.UnsafeNoDedup = true })
}

// A leader answering reads from local state is only wrong while it is a stale leader: cut off
// from the majority, which has elected someone else and moved on. CheckQuorum makes such a leader
// step down within one or two election timeouts, which shrinks that window so much that 200
// seeds did not hit it. It is still a bug (the window is never zero), so the control runs with
// CheckQuorum off, where the window is wide open.
func TestControlLeaderLocalReadsIsCaught(t *testing.T) {
	requireCaughtKV(t, func(c *Config) {
		classic(c)
		c.UnsafeLeaderLocalReads = true
	})
}

func requireCaughtKV(t *testing.T, mutate func(*Config)) {
	t.Helper()
	const budget = 200
	var found atomic.Uint64
	parallel(1, budget, func(seed uint64) {
		if found.Load() != 0 {
			return
		}
		cfg := KVDefaults(seed)
		mutate(&cfg)
		if r := Run(cfg); r.Violation != "" && found.CompareAndSwap(0, seed) {
			t.Logf("caught on seed %d: %s (%d ops)", seed, r.Violation, r.Stats.Ops)
		}
	})
	if found.Load() == 0 {
		t.Fatalf("the broken store survived %d seeds", budget)
	}
}
