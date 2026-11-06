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

// A leader answering a read without the heartbeat round is only wrong while it is a stale leader:
// cut off from the majority, which has elected someone else and moved on. CheckQuorum makes such
// a leader step down within one or two election timeouts, which shrinks that window so much that
// 200 seeds did not hit it. It is still a bug (the window is never zero), so the control runs
// with CheckQuorum off, where the window is wide open.
func TestControlReadWithoutQuorumIsCaught(t *testing.T) {
	requireCaughtKV(t, func(c *Config) {
		classic(c)
		c.Raft.Unsafe.ReadWithoutQuorum = true
	})
}

// A new leader's commit index can lag what the old leader already acknowledged to a client.
// Reading at that index before the new leader's no-op commits returns a value from before a
// completed write. The heartbeat round that confirms the read usually carries the no-op too, so
// the commit index catches up in the same step and the read comes out fresh: 200 seeds missed it
// in both modes. With one entry per message a lagging follower can confirm the read before it
// has the no-op, and the stale read shows up.
func TestControlReadBeforeTermCommitIsCaught(t *testing.T) {
	requireCaughtKV(t, func(c *Config) {
		classic(c)
		c.Raft.Unsafe.ReadBeforeTermCommit = true
		c.Raft.MaxEntriesPerMsg = 1
	})
}

// The setting that opens the window must be safe with the rule on, or the control proves nothing.
func TestOneEntryPerMessageIsLinearizable(t *testing.T) {
	n := uint64(100)
	if testing.Short() {
		n = 15
	}
	parallel(1, n, func(seed uint64) {
		cfg := KVDefaults(seed)
		classic(&cfg)
		cfg.Raft.MaxEntriesPerMsg = 1
		if r := Run(cfg); r.Violation != "" || r.Inconclusive {
			t.Errorf("seed %d: inconclusive=%v %s", seed, r.Inconclusive, r)
		}
	})
}

// TestReadIndexSavesLogWrites runs the same seeds with gets through the log and through
// ReadIndex. Both must be linearizable; ReadIndex should need far fewer log entries per operation.
func TestReadIndexSavesLogWrites(t *testing.T) {
	n := uint64(50)
	if testing.Short() {
		n = 10
	}
	run := func(throughLog bool) (ops, entries, reads int) {
		var mu sync.Mutex
		parallel(1, n, func(seed uint64) {
			cfg := KVDefaults(seed)
			cfg.ReadsThroughLog = throughLog
			r := Run(cfg)
			mu.Lock()
			defer mu.Unlock()
			if r.Violation != "" || r.Inconclusive {
				t.Errorf("throughLog=%v seed %d: inconclusive=%v %s", throughLog, seed, r.Inconclusive, r)
			}
			ops += r.Stats.Ops
			entries += int(r.Stats.Committed)
			reads += r.Stats.Reads
		})
		return
	}
	logOps, logEntries, _ := run(true)
	riOps, riEntries, riReads := run(false)
	before, after := float64(logEntries)/float64(logOps), float64(riEntries)/float64(riOps)
	t.Logf("%d seeds: log entries per op %.2f -> %.2f (%d of %d ops were ReadIndex reads)", n, before, after, riReads, riOps)
	if after >= before {
		t.Errorf("ReadIndex did not reduce log entries per operation: %.2f -> %.2f", before, after)
	}
}

func requireCaughtKV(t *testing.T, mutate func(*Config)) {
	t.Helper()
	const budget = 200
	seed, r := lowestCaught(budget, func(seed uint64) Result {
		cfg := KVDefaults(seed)
		mutate(&cfg)
		return Run(cfg)
	})
	if seed == 0 {
		t.Fatalf("the broken store survived %d seeds", budget)
	}
	t.Logf("caught on seed %d: %s (%d ops)", seed, r.Violation, r.Stats.Ops)
}
