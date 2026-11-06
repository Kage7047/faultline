package raft

import (
	"fmt"
	"testing"
)

// cluster wires nodes together with instant, lossless delivery. Tests cut links with drop.
type cluster struct {
	t       *testing.T
	nodes   map[NodeID]*Node
	applied map[NodeID][]Entry
	drop    func(Message) bool
}

func newCluster(t *testing.T, n int) *cluster {
	t.Helper()
	c := &cluster{t: t, nodes: map[NodeID]*Node{}, applied: map[NodeID][]Entry{}, drop: func(Message) bool { return false }}
	var peers []NodeID
	for i := 1; i <= n; i++ {
		peers = append(peers, NodeID(i))
	}
	for _, id := range peers {
		c.nodes[id] = NewNode(Config{ID: id, Peers: peers, Seed: 1}, HardState{}, nil, 0)
	}
	return c
}

// settle delivers messages until nothing is left to do.
func (c *cluster) settle() {
	for range 10_000 {
		var pending []Message
		for id := NodeID(1); int(id) <= len(c.nodes); id++ {
			n := c.nodes[id]
			for n.HasReady() {
				rd := n.Ready()
				pending = append(pending, rd.Messages...)
				c.applied[id] = append(c.applied[id], rd.Committed...)
				n.Advance(rd)
			}
		}
		if len(pending) == 0 {
			return
		}
		for _, m := range pending {
			if !c.drop(m) {
				c.nodes[m.To].Step(m)
			}
		}
	}
	c.t.Fatal("cluster did not settle")
}

func (c *cluster) elect(id NodeID) {
	c.t.Helper()
	n := c.nodes[id]
	for n.Role() != Leader {
		n.campaign()
		c.settle()
	}
}

func (c *cluster) propose(id NodeID, s string) Index {
	c.t.Helper()
	idx, _, err := c.nodes[id].Propose([]byte(s))
	if err != nil {
		c.t.Fatalf("propose on %d: %v", id, err)
	}
	c.settle()
	return idx
}

func TestSingleNodeCommitsAlone(t *testing.T) {
	c := newCluster(t, 1)
	c.elect(1)
	c.propose(1, "x")
	if got := c.nodes[1].Commit(); got != 2 { // the no-op, then x
		t.Fatalf("commit = %d, want 2", got)
	}
}

func TestElectionAndReplication(t *testing.T) {
	c := newCluster(t, 3)
	c.elect(1)
	for _, id := range []NodeID{2, 3} {
		if c.nodes[id].Leader() != 1 || c.nodes[id].Role() != Follower {
			t.Fatalf("node %d: role %v leader %d", id, c.nodes[id].Role(), c.nodes[id].Leader())
		}
	}
	idx := c.propose(1, "x")
	for id, n := range c.nodes {
		if n.Commit() < idx {
			t.Errorf("node %d commit %d < %d", id, n.Commit(), idx)
		}
		if got := c.applied[id]; len(got) != 2 || string(got[1].Data) != "x" {
			t.Errorf("node %d applied %v", id, got)
		}
	}
}

func TestProposeOnFollowerIsRejected(t *testing.T) {
	c := newCluster(t, 3)
	c.elect(1)
	if _, _, err := c.nodes[2].Propose([]byte("x")); err != ErrNotLeader {
		t.Fatalf("err = %v, want ErrNotLeader", err)
	}
}

func TestMinorityCannotCommit(t *testing.T) {
	c := newCluster(t, 3)
	c.elect(1)
	c.drop = func(m Message) bool { return m.From == 1 || m.To == 1 } // isolate the leader
	idx, _, _ := c.nodes[1].Propose([]byte("lost"))
	c.settle()
	if c.nodes[1].Commit() >= idx {
		t.Fatalf("an isolated leader committed index %d", idx)
	}
}

func TestStaleLeaderStepsDown(t *testing.T) {
	c := newCluster(t, 3)
	c.elect(1)
	c.drop = func(m Message) bool { return m.From == 1 || m.To == 1 }
	c.elect(2) // a new term while 1 is cut off
	c.drop = func(Message) bool { return false }
	c.nodes[1].Tick() // its heartbeat reaches the others, whose rejection carries the new term
	c.settle()
	if c.nodes[1].Role() != Follower || c.nodes[1].Term() != c.nodes[2].Term() {
		t.Fatalf("old leader: role %v term %d; new term %d", c.nodes[1].Role(), c.nodes[1].Term(), c.nodes[2].Term())
	}
}

func TestVoteRejectsCandidateWithOlderLog(t *testing.T) {
	n := NewNode(Config{ID: 1, Peers: []NodeID{1, 2, 3}}, HardState{Term: 2},
		[]Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}}, 0)
	cases := []struct {
		lastIndex Index
		lastTerm  Term
		want      bool
	}{
		{1, 1, false}, // older last term
		{5, 1, false}, // longer, but an older last term still loses
		{1, 2, false}, // same term, shorter
		{2, 2, true},  // same
		{1, 3, true},  // newer term wins even if shorter
	}
	for i, tc := range cases {
		n.vote = None
		n.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 3 + Term(i), LastIndex: tc.lastIndex, LastTerm: tc.lastTerm})
		rd := n.Ready()
		n.Advance(rd)
		if got := rd.Messages[0].Granted; got != tc.want {
			t.Errorf("candidate last=%d@t%d: granted=%v, want %v", tc.lastIndex, tc.lastTerm, got, tc.want)
		}
	}
}

func TestRestartedNodeDoesNotVoteTwice(t *testing.T) {
	n := NewNode(Config{ID: 1, Peers: []NodeID{1, 2, 3}}, HardState{}, nil, 0)
	n.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 5})
	rd := n.Ready()
	n.Advance(rd)
	// crash and restart from what was persisted
	n = NewNode(Config{ID: 1, Peers: []NodeID{1, 2, 3}}, rd.HardState, rd.Entries, 0)
	n.Step(Message{Type: MsgVote, From: 3, To: 1, Term: 5})
	if m := n.Ready().Messages[0]; m.Granted {
		t.Fatal("voted for 3 in term 5 after voting for 2 in term 5 before the restart")
	}
}

// Figure 7 of the Raft paper: followers whose logs miss entries or hold extra, conflicting ones
// all end up with exactly the leader's log.
func TestFollowersConvergeToLeaderLog(t *testing.T) {
	terms := func(ts ...Term) []Entry {
		es := make([]Entry, len(ts))
		for i, t := range ts {
			es[i] = Entry{Term: t, Index: Index(i + 1), Data: []byte(fmt.Sprint(i))}
		}
		return es
	}
	leaderLog := terms(1, 1, 1, 4, 4, 5, 5, 6, 6, 6)
	followers := [][]Entry{
		terms(1, 1, 1, 4, 4, 5, 5, 6, 6),          // (a) missing one
		terms(1, 1, 1, 4),                         // (b) missing many
		terms(1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6),    // (c) one extra
		terms(1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7), // (d) extra from a later term
		terms(1, 1, 1, 4, 4, 4, 4),                // (e) extra term-4 entries
		terms(1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3),    // (f) a whole divergent history
	}
	for i, fl := range followers {
		t.Run(string(rune('a'+i)), func(t *testing.T) {
			peers := []NodeID{1, 2}
			leader := NewNode(Config{ID: 1, Peers: peers}, HardState{Term: 7}, leaderLog, 0)
			follower := NewNode(Config{ID: 2, Peers: peers}, HardState{Term: 7}, fl, 0)
			leader.term = 8 // as if it just won term 8
			leader.vote = 1
			leader.becomeLeader()
			nodes := map[NodeID]*Node{1: leader, 2: follower}
			for range 100 {
				var msgs []Message
				for _, n := range nodes {
					for n.HasReady() {
						rd := n.Ready()
						msgs = append(msgs, rd.Messages...)
						n.Advance(rd)
					}
				}
				if len(msgs) == 0 {
					break
				}
				for _, m := range msgs {
					nodes[m.To].Step(m)
				}
			}
			got, want := follower.Entries(), leader.Entries()
			if len(got) != len(want) {
				t.Fatalf("follower has %d entries, leader %d", len(got), len(want))
			}
			for j := range want {
				if got[j].Term != want[j].Term || string(got[j].Data) != string(want[j].Data) {
					t.Fatalf("entry %d: follower t%d %q, leader t%d %q", j+1, got[j].Term, got[j].Data, want[j].Term, want[j].Data)
				}
			}
		})
	}
}

// A node cut off from the cluster keeps timing out. Without PreVote each timeout bumps its term,
// and when it comes back its higher term forces the healthy leader to step down: an election
// nobody needed. With PreVote (and CheckQuorum's leader stickiness) its term never moves.
func TestReturningNodeAndPreVote(t *testing.T) {
	for _, preVote := range []bool{false, true} {
		t.Run(map[bool]string{false: "without", true: "with"}[preVote], func(t *testing.T) {
			c := newCluster(t, 3)
			for id := range c.nodes {
				c.nodes[id] = NewNode(Config{ID: id, Peers: []NodeID{1, 2, 3}, Seed: 1, PreVote: preVote, CheckQuorum: preVote},
					HardState{}, nil, 0)
			}
			c.elect(1)
			term := c.nodes[1].Term()

			c.drop = func(m Message) bool { return m.From == 3 || m.To == 3 }
			for range 200 { // node 3 alone, timing out over and over
				c.nodes[3].Tick()
				c.settle()
			}
			c.drop = func(Message) bool { return false }
			c.nodes[1].Tick() // the leader's next heartbeat reaches node 3 again
			c.settle()

			disrupted := c.nodes[1].Role() != Leader || c.nodes[1].Term() != term
			if disrupted != !preVote {
				t.Fatalf("preVote=%v: leader 1 role=%v term %d -> %d, node 3 term %d", preVote, c.nodes[1].Role(), term,
					c.nodes[1].Term(), c.nodes[3].Term())
			}
			t.Logf("leader term %d -> %d, returning node at term %d", term, c.nodes[1].Term(), c.nodes[3].Term())
		})
	}
}

func TestCheckQuorumLeaderStepsDownWhenCutOff(t *testing.T) {
	c := newCluster(t, 3)
	for id := range c.nodes {
		c.nodes[id] = NewNode(Config{ID: id, Peers: []NodeID{1, 2, 3}, Seed: 1, PreVote: true, CheckQuorum: true}, HardState{}, nil, 0)
	}
	c.elect(1)
	c.drop = func(m Message) bool { return m.From == 1 || m.To == 1 }
	for range 2 * 10 { // two election timeouts of silence from the followers
		c.nodes[1].Tick()
		c.settle()
	}
	if c.nodes[1].Role() == Leader {
		t.Fatal("a leader cut off from every follower still believes it leads")
	}
}

// reads drains every node's Ready and returns the ReadStates node id produced.
func (c *cluster) reads(id NodeID) []ReadState {
	var out []ReadState
	for range 10_000 {
		var pending []Message
		for nid := NodeID(1); int(nid) <= len(c.nodes); nid++ {
			n := c.nodes[nid]
			for n.HasReady() {
				rd := n.Ready()
				pending = append(pending, rd.Messages...)
				if nid == id {
					out = append(out, rd.ReadStates...)
				}
				n.Advance(rd)
			}
		}
		if len(pending) == 0 {
			return out
		}
		for _, m := range pending {
			if !c.drop(m) {
				c.nodes[m.To].Step(m)
			}
		}
	}
	c.t.Fatal("cluster did not settle")
	return nil
}

func TestReadIndexConfirmsLeadership(t *testing.T) {
	c := newCluster(t, 3)
	c.elect(1)
	idx := c.propose(1, "x")
	lastBefore := c.nodes[1].LastIndex()
	if err := c.nodes[1].ReadIndex(7); err != nil {
		t.Fatal(err)
	}
	got := c.reads(1)
	if len(got) != 1 || got[0].Ctx != 7 || got[0].Index != idx {
		t.Fatalf("read states %v, want one with ctx 7 at index %d", got, idx)
	}
	if c.nodes[1].LastIndex() != lastBefore {
		t.Fatal("a ReadIndex read wrote to the log")
	}
	if err := c.nodes[2].ReadIndex(8); err != ErrNotLeader {
		t.Fatalf("follower: err = %v, want ErrNotLeader", err)
	}
}

func TestReadIndexNeedsMajority(t *testing.T) {
	c := newCluster(t, 3)
	c.elect(1)
	c.drop = func(m Message) bool { return m.From == 1 || m.To == 1 }
	if err := c.nodes[1].ReadIndex(1); err != nil {
		t.Fatal(err)
	}
	if got := c.reads(1); len(got) != 0 {
		t.Fatalf("a leader cut off from every follower answered a read: %v", got)
	}
	// one follower answering is a majority of three
	c.drop = func(m Message) bool { return m.From == 3 || m.To == 3 }
	c.nodes[1].Tick()
	if got := c.reads(1); len(got) != 1 || got[0].Ctx != 1 {
		t.Fatalf("read states %v after a majority answered", got)
	}
}

func TestReadIndexWaitsForTermCommit(t *testing.T) {
	c := newCluster(t, 3)
	c.drop = func(m Message) bool { return m.Type == MsgAppResp } // the no-op never commits
	c.elect(1)
	if err := c.nodes[1].ReadIndex(1); err != ErrReadNotReady {
		t.Fatalf("err = %v, want ErrReadNotReady", err)
	}
	c.drop = func(Message) bool { return false }
	c.nodes[1].Tick()
	c.settle()
	if err := c.nodes[1].ReadIndex(1); err != nil {
		t.Fatalf("after the no-op committed: %v", err)
	}
}

func TestReadsDroppedOnStepDown(t *testing.T) {
	c := newCluster(t, 3)
	c.elect(1)
	c.drop = func(m Message) bool { return m.From == 1 || m.To == 1 }
	if err := c.nodes[1].ReadIndex(1); err != nil {
		t.Fatal(err)
	}
	c.reads(1)
	c.elect(2)
	c.drop = func(Message) bool { return false }
	c.nodes[1].Tick() // hears the new term and steps down
	if got := c.reads(1); len(got) != 0 {
		t.Fatalf("a deposed leader answered a read: %v", got)
	}
}
