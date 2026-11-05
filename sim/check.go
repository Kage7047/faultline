package sim

import (
	"bytes"
	"fmt"

	"github.com/Kage7047/faultline/raft"
)

// checker watches a run for violations of the safety properties in Figure 3 of the Raft paper.
// It is fed from the outside (what each node reports) and never looks at a node's internals,
// so a bug in the node cannot hide itself from the checker.
type checker struct {
	leaders   map[raft.Term]raft.NodeID // Election Safety: one leader per term
	committed map[raft.Index]raft.Entry // what was applied at each index, by anyone
	applied   map[raft.NodeID]raft.Index
	maxIndex  raft.Index
}

func newChecker() *checker {
	return &checker{
		leaders:   map[raft.Term]raft.NodeID{},
		committed: map[raft.Index]raft.Entry{},
		applied:   map[raft.NodeID]raft.Index{},
	}
}

func (c *checker) restarted(id raft.NodeID) { c.applied[id] = 0 }

func (c *checker) maxCommitted() raft.Index { return c.maxIndex }

// leader is called when a node becomes leader of a term, with its log at that moment.
func (c *checker) leader(id raft.NodeID, term raft.Term, log []raft.Entry) string {
	if other, ok := c.leaders[term]; ok && other != id {
		return fmt.Sprintf("election safety: nodes %d and %d both lead term %d", other, id, term)
	}
	c.leaders[term] = id
	// Leader Completeness: a leader's log holds every entry committed in an earlier term.
	for idx, e := range c.committed {
		if int(idx) > len(log) || !sameEntry(log[idx-1], e) {
			return fmt.Sprintf("leader completeness: leader %d of term %d is missing committed entry %d (term %d, %q)",
				id, term, idx, e.Term, e.Data)
		}
	}
	return ""
}

// apply is called for every entry a node applies to its state machine.
func (c *checker) apply(id raft.NodeID, e raft.Entry) string {
	// Each node applies the log in order, without gaps or repeats.
	if want := c.applied[id] + 1; e.Index != want {
		return fmt.Sprintf("node %d applied index %d, expected %d", id, e.Index, want)
	}
	c.applied[id] = e.Index
	// State Machine Safety: no two nodes apply different entries at the same index.
	if prev, ok := c.committed[e.Index]; ok {
		if !sameEntry(prev, e) {
			return fmt.Sprintf("state machine safety: index %d applied as %q (term %d) and as %q (term %d, by node %d)",
				e.Index, prev.Data, prev.Term, e.Data, e.Term, id)
		}
		return ""
	}
	c.committed[e.Index] = e
	c.maxIndex = max(c.maxIndex, e.Index)
	return ""
}

// logMatching checks the Log Matching property across the final persisted logs: if two logs
// hold an entry with the same index and term, the logs are identical up to that index.
func logMatching(logs [][]raft.Entry) string {
	for a := range logs {
		for b := a + 1; b < len(logs); b++ {
			la, lb := logs[a], logs[b]
			n := min(len(la), len(lb))
			for i := n - 1; i >= 0; i-- {
				if la[i].Term != lb[i].Term {
					continue
				}
				for j := 0; j <= i; j++ {
					if !sameEntry(la[j], lb[j]) {
						return fmt.Sprintf("log matching: nodes %d and %d agree at index %d (term %d) but differ at index %d",
							a+1, b+1, i+1, la[i].Term, j+1)
					}
				}
				break
			}
		}
	}
	return ""
}

func sameEntry(a, b raft.Entry) bool {
	return a.Index == b.Index && a.Term == b.Term && bytes.Equal(a.Data, b.Data) && (a.Data == nil) == (b.Data == nil)
}
