package sim

import (
	"flag"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Kage7047/faultline/raft"
)

var (
	seeds = flag.Int("seeds", 200, "how many seeds TestSafety runs")
	from  = flag.Uint64("from", 1, "first seed")
	only  = flag.Uint64("seed", 0, "run just this seed (to reproduce a failure)")
)

// TestSafety runs many seeded simulations with crashes, partitions, loss, duplication and
// reordering, and fails on the first safety violation with a command that replays it.
func TestSafety(t *testing.T) {
	first, n := *from, uint64(*seeds)
	if *only != 0 {
		first, n = *only, 1
	}
	if testing.Short() {
		n = min(n, 30)
	}
	var failed atomic.Bool
	var mu sync.Mutex
	var total Stats
	parallel(first, n, func(seed uint64) {
		r := Run(Defaults(seed))
		mu.Lock()
		defer mu.Unlock()
		add(&total, r.Stats)
		if r.Violation != "" && failed.CompareAndSwap(false, true) {
			t.Errorf("%s\nreproduce: go test ./sim -run TestSafety -seed %d -v", r, seed)
		}
		// liveness: after the quiet tail every node has caught up with what was committed
		for id, c := range r.Stats.FinalCommit {
			if r.Violation == "" && c == 0 {
				t.Errorf("seed %d: node %d committed nothing", seed, id+1)
			}
		}
	})
	t.Logf("%d seeds: leaders=%d proposed=%d committed=%d crashes=%d partitions=%d msgs=%d dropped=%d",
		n, total.Leaders, total.Proposed, total.Committed, total.Crashes, total.Partitions, total.Sent, total.Dropped)
}

// TestLivenessWithoutFaults: with a healthy network the cluster elects one leader and commits
// almost everything that was proposed to it.
func TestLivenessWithoutFaults(t *testing.T) {
	cfg := Defaults(1)
	cfg.Faults, cfg.DropRate, cfg.DupRate = false, 0, 0
	r := Run(cfg)
	if r.Violation != "" {
		t.Fatal(r)
	}
	if r.Stats.Leaders != 1 {
		t.Errorf("expected exactly one election on a healthy network, got %d", r.Stats.Leaders)
	}
	if r.Stats.Committed < raft.Index(r.Stats.Proposed) {
		t.Errorf("committed %d of %d proposals", r.Stats.Committed, r.Stats.Proposed)
	}
	for id, c := range r.Stats.FinalCommit {
		if c != r.Stats.Committed {
			t.Errorf("node %d ended at commit %d, cluster at %d", id+1, c, r.Stats.Committed)
		}
	}
	t.Log(r)
}

// The controls: switch off one safety rule at a time and require the simulator to notice.
// If it did not, a passing TestSafety would mean nothing.
func TestControlGrantAnyVoteIsCaught(t *testing.T) {
	requireCaught(t, func(c *Config) { c.Raft.Unsafe.GrantAnyVote = true })
}

// Figure 8's bug needs an old-term entry to reach a majority before the new leader's no-op does.
// When both travel in one message that almost never happens, so this control sends one entry
// per message, which lets old entries run ahead of the no-op on a lagging follower.
func TestControlCommitOldTermsIsCaught(t *testing.T) {
	requireCaught(t, func(c *Config) {
		c.Raft.Unsafe.CommitOldTerms = true
		c.Raft.MaxEntriesPerMsg = 1
	})
}

// The same small messages with the rule intact must stay safe, or the control proves nothing.
func TestOneEntryPerMessageIsSafe(t *testing.T) {
	parallel(1, 200, func(seed uint64) {
		cfg := Defaults(seed)
		cfg.Raft.MaxEntriesPerMsg = 1
		if r := Run(cfg); r.Violation != "" {
			t.Errorf("%s", r)
		}
	})
}

func requireCaught(t *testing.T, mutate func(*Config)) {
	t.Helper()
	const budget = 2000
	var found atomic.Uint64
	parallel(1, budget, func(seed uint64) {
		if found.Load() != 0 {
			return
		}
		cfg := Defaults(seed)
		mutate(&cfg)
		if r := Run(cfg); r.Violation != "" {
			if found.CompareAndSwap(0, seed) {
				t.Logf("caught on seed %d: %s", seed, r.Violation)
			}
		}
	})
	if found.Load() == 0 {
		t.Fatalf("broken raft survived %d seeds: the simulator cannot see this bug", budget)
	}
}

func parallel(first, n uint64, f func(seed uint64)) {
	work := make(chan uint64)
	var wg sync.WaitGroup
	for range runtime.GOMAXPROCS(0) {
		wg.Go(func() {
			for s := range work {
				f(s)
			}
		})
	}
	for s := first; s < first+n; s++ {
		work <- s
	}
	close(work)
	wg.Wait()
}

func add(t *Stats, s Stats) {
	t.Leaders += s.Leaders
	t.Proposed += s.Proposed
	t.Committed += s.Committed
	t.Crashes += s.Crashes
	t.Partitions += s.Partitions
	t.Sent += s.Sent
	t.Dropped += s.Dropped
}

func TestRunIsDeterministic(t *testing.T) {
	a, b := Run(Defaults(42)), Run(Defaults(42))
	if fmt.Sprint(a.Stats) != fmt.Sprint(b.Stats) {
		t.Fatalf("same seed, different runs:\n%v\n%v", a.Stats, b.Stats)
	}
}
