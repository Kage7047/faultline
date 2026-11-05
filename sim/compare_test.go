package sim

import (
	"sync"
	"testing"
)

// TestPreVoteReducesElections compares the same 200 hostile seeds with and without PreVote and
// CheckQuorum. Safety must hold either way; the protected cluster should need fewer elections.
func TestPreVoteReducesElections(t *testing.T) {
	n := uint64(200)
	if testing.Short() {
		n = 30
	}
	count := func(on bool) (leaders, maxTerm int, violations int) {
		var mu sync.Mutex
		parallel(1, n, func(seed uint64) {
			cfg := Defaults(seed)
			cfg.Raft.PreVote, cfg.Raft.CheckQuorum = on, on
			r := Run(cfg)
			mu.Lock()
			defer mu.Unlock()
			leaders += r.Stats.Leaders
			maxTerm += int(r.Stats.MaxTerm)
			if r.Violation != "" {
				violations++
				t.Errorf("preVote=%v: %s", on, r)
			}
		})
		return
	}
	offLeaders, offTerms, _ := count(false)
	onLeaders, onTerms, _ := count(true)
	t.Logf("%d seeds: elections %d -> %d, sum of final terms %d -> %d", n, offLeaders, onLeaders, offTerms, onTerms)
	if onLeaders >= offLeaders {
		t.Errorf("PreVote+CheckQuorum did not reduce elections: %d -> %d", offLeaders, onLeaders)
	}
}
