package sim

import (
	"flag"
	"fmt"
	"runtime"
	"strings"
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

// modes are the configurations every safety test runs in. PreVote and CheckQuorum make elections
// rarer, which is good for a cluster and bad for a test: fewer elections means fewer chances to
// hit an election bug. Snapshots let a lagging follower skip the slow entry-by-entry catch-up
// that some bugs need. So besides everything on, the tests run with PreVote and CheckQuorum off
// (classic) and with snapshots off as well (bare).
var modes = []struct {
	name string
	set  func(*Config)
}{
	{"prevote", func(*Config) {}},
	{"classic", classic},
	{"bare", bare},
}

func classic(c *Config) { c.Raft.PreVote, c.Raft.CheckQuorum = false, false }

func bare(c *Config) {
	classic(c)
	c.SnapshotEvery = 0
}

// TestSafety runs many seeded simulations with crashes, partitions, loss, duplication and
// reordering, and fails on the first safety violation with a command that replays it.
func TestSafety(t *testing.T) {
	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) { testSafety(t, m.set) })
	}
}

func testSafety(t *testing.T, mode func(*Config)) {
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
		cfg := Defaults(seed)
		mode(&cfg)
		r := Run(cfg)
		mu.Lock()
		defer mu.Unlock()
		add(&total, r.Stats)
		if r.Violation != "" && failed.CompareAndSwap(false, true) {
			t.Errorf("%s\nreproduce: go test ./sim -run %s -seed %d -v", r, t.Name(), seed)
		}
		// liveness: after the quiet tail every node has caught up with what was committed
		for id, c := range r.Stats.FinalCommit {
			if r.Violation == "" && c == 0 {
				t.Errorf("seed %d: node %d committed nothing", seed, id+1)
			}
		}
	})
	t.Logf("%d seeds: leaders=%d proposed=%d committed=%d crashes=%d partitions=%d msgs=%d dropped=%d compactions=%d installs=%d",
		n, total.Leaders, total.Proposed, total.Committed, total.Crashes, total.Partitions, total.Sent, total.Dropped,
		total.Compactions, total.Installs)
	if total.Installs == 0 && n >= 30 && Defaults(1).SnapshotEvery > 0 && !strings.Contains(t.Name(), "bare") {
		t.Error("no follower ever installed a snapshot: the InstallSnapshot path went untested")
	}
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
// per message, which lets old entries run ahead of the no-op on a lagging follower. It also
// needs leader churn and a follower catching up one entry at a time, so it runs in bare mode:
// with PreVote and CheckQuorum on, or with snapshots on, 2,000 seeds did not open the window once.
func TestControlCommitOldTermsIsCaught(t *testing.T) {
	requireCaught(t, func(c *Config) {
		bare(c)
		c.Raft.Unsafe.CommitOldTerms = true
		c.Raft.MaxEntriesPerMsg = 1
	})
}

// A delayed or duplicated InstallSnapshot that is older than what the follower has committed must
// be ignored. Installing it anyway rolls the follower's state machine back.
func TestControlInstallStaleSnapshotIsCaught(t *testing.T) {
	requireCaught(t, func(c *Config) { c.Raft.Unsafe.InstallStaleSnapshot = true })
}

// The same small messages with the rule intact must stay safe, or the control proves nothing.
func TestOneEntryPerMessageIsSafe(t *testing.T) {
	parallel(1, 200, func(seed uint64) {
		cfg := Defaults(seed)
		bare(&cfg)
		cfg.Raft.MaxEntriesPerMsg = 1
		if r := Run(cfg); r.Violation != "" {
			t.Errorf("%s", r)
		}
	})
}

func requireCaught(t *testing.T, mutate func(*Config)) {
	t.Helper()
	const budget = 2000
	seed, r := lowestCaught(budget, func(seed uint64) Result {
		cfg := Defaults(seed)
		mutate(&cfg)
		return Run(cfg)
	})
	if seed == 0 {
		t.Fatalf("broken raft survived %d seeds: the simulator cannot see this bug", budget)
	}
	t.Logf("caught on seed %d: %s", seed, r.Violation)
}

// lowestCaught runs seeds 1..budget in parallel and returns the lowest one with a violation, so
// the reported seed does not depend on how many CPUs the machine has. Seeds above a known
// violation are skipped.
func lowestCaught(budget uint64, run func(uint64) Result) (uint64, Result) {
	var mu sync.Mutex
	var best uint64
	var bestResult Result
	parallel(1, budget, func(seed uint64) {
		mu.Lock()
		skip := best != 0 && seed > best
		mu.Unlock()
		if skip {
			return
		}
		if r := run(seed); r.Violation != "" {
			mu.Lock()
			if best == 0 || seed < best {
				best, bestResult = seed, r
			}
			mu.Unlock()
		}
	})
	return best, bestResult
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
	t.Compactions += s.Compactions
	t.Installs += s.Installs
}

func TestRunIsDeterministic(t *testing.T) {
	a, b := Run(Defaults(42)), Run(Defaults(42))
	if fmt.Sprint(a.Stats) != fmt.Sprint(b.Stats) {
		t.Fatalf("same seed, different runs:\n%v\n%v", a.Stats, b.Stats)
	}
}
