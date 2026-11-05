package raft

import (
	"errors"
	"math/rand/v2"
	"slices"
)

// ErrNotLeader is returned by Propose on a node that is not the leader. Leader() may name one.
var ErrNotLeader = errors.New("raft: not the leader")

// Node is one Raft peer. It is not safe for concurrent use; the caller serialises calls.
type Node struct {
	cfg   Config
	id    NodeID
	peers []NodeID // sorted, includes id; sorted so output order never depends on map order

	role Role
	term Term
	vote NodeID
	lead NodeID
	log  raftLog

	commit  Index
	applied Index

	electionElapsed  int
	heartbeatElapsed int
	electionTimeout  int // randomised per election
	rng              *rand.Rand

	votes  map[NodeID]bool  // (pre-)candidate: responses received
	active map[NodeID]bool  // leader: peers heard from since the last quorum check
	next   map[NodeID]Index // leader: next index to send
	match  map[NodeID]Index // leader: highest index known replicated

	msgs   []Message
	prevHS HardState
}

// NewNode starts a node from what it persisted before (zero values for a brand-new node).
// applied is the last index the state machine already contains; entries after it up to the
// persisted commit index are handed out again by Ready.
func NewNode(cfg Config, hs HardState, entries []Entry, applied Index) *Node {
	cfg.setDefaults()
	peers := slices.Clone(cfg.Peers)
	slices.Sort(peers)
	n := &Node{
		cfg:     cfg,
		id:      cfg.ID,
		peers:   peers,
		term:    hs.Term,
		vote:    hs.Vote,
		commit:  hs.Commit,
		applied: applied,
		rng:     rand.New(rand.NewPCG(cfg.Seed, uint64(cfg.ID))),
	}
	n.log.entries = cloneEntries(entries)
	n.log.stable = n.log.lastIndex()
	if n.commit > n.log.lastIndex() {
		n.commit = n.log.lastIndex()
	}
	n.prevHS = n.hardState()
	n.becomeFollower(n.term, None)
	return n
}

// ---- inputs -----------------------------------------------------------------------------------

// Tick advances the node's clock by one tick.
func (n *Node) Tick() {
	if n.role == Leader {
		n.electionElapsed++
		if n.electionElapsed >= n.cfg.ElectionTicks {
			n.electionElapsed = 0
			if n.cfg.CheckQuorum && !n.quorumActive() {
				n.becomeFollower(n.term, None) // cut off from the majority: stop claiming to lead
				return
			}
		}
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.cfg.HeartbeatTicks {
			n.heartbeatElapsed = 0
			n.broadcastAppend()
		}
		return
	}
	n.electionElapsed++
	if n.electionElapsed >= n.electionTimeout {
		if n.cfg.PreVote {
			n.preCampaign()
		} else {
			n.campaign()
		}
	}
}

// Propose appends data to the log if this node is the leader and returns its position. The entry
// is only durable once it shows up in Ready.Committed.
func (n *Node) Propose(data []byte) (Index, Term, error) {
	if n.role != Leader {
		return 0, 0, ErrNotLeader
	}
	if data == nil {
		data = []byte{} // nil is reserved for the leader's no-op
	}
	idx := n.appendEntry(data)
	n.broadcastAppend()
	return idx, n.term, nil
}

// Step processes one message from a peer.
func (n *Node) Step(m Message) {
	switch {
	case m.Term > n.term:
		if (m.Type == MsgVote || m.Type == MsgPreVote) && n.inLease() {
			return // we have a live leader: a would-be candidate is the one out of date
		}
		switch {
		case m.Type == MsgPreVote:
			// a pre-vote asks about a future term; it does not move ours
		case m.Type == MsgPreVoteResp && m.Granted:
			// a granted pre-vote echoes the term we would campaign for
		default:
			lead := None
			if m.Type == MsgApp {
				lead = m.From
			}
			n.becomeFollower(m.Term, lead)
		}
	case m.Term < n.term:
		// A stale leader or candidate learns the current term from the rejection and steps down.
		switch m.Type {
		case MsgApp:
			n.send(Message{Type: MsgAppResp, To: m.From, Success: false})
		case MsgVote:
			n.send(Message{Type: MsgVoteResp, To: m.From, Granted: false})
		case MsgPreVote:
			n.send(Message{Type: MsgPreVoteResp, To: m.From, Granted: false})
		}
		return
	}

	switch m.Type {
	case MsgPreVote:
		n.handlePreVote(m)
	case MsgPreVoteResp:
		n.handlePreVoteResp(m)
	case MsgVote:
		n.handleVote(m)
	case MsgVoteResp:
		n.handleVoteResp(m)
	case MsgApp:
		n.handleAppend(m)
	case MsgAppResp:
		n.handleAppendResp(m)
	}
}

// ---- outputs ----------------------------------------------------------------------------------

// HasReady reports whether Ready would return anything.
func (n *Node) HasReady() bool {
	return len(n.msgs) > 0 || n.log.stable < n.log.lastIndex() || n.hardState() != n.prevHS || n.commit > n.applied
}

// Ready collects the node's output. See the Ready type for the caller's obligations.
func (n *Node) Ready() Ready {
	hs := n.hardState()
	rd := Ready{
		SoftState: SoftState{Role: n.role, Lead: n.lead},
		HardState: hs,
		MustSync:  hs != n.prevHS,
		Entries:   n.log.unstable(),
		Messages:  n.msgs,
		Committed: n.log.slice(n.applied+1, n.commit),
	}
	n.msgs = nil
	return rd
}

// Advance tells the node the caller has done everything rd asked.
func (n *Node) Advance(rd Ready) {
	if k := len(rd.Entries); k > 0 {
		last := rd.Entries[k-1]
		if n.log.term(last.Index) == last.Term && last.Index > n.log.stable {
			n.log.stable = last.Index
		}
	}
	if k := len(rd.Committed); k > 0 {
		n.applied = rd.Committed[k-1].Index
	}
	n.prevHS = rd.HardState
}

// ---- introspection ----------------------------------------------------------------------------

func (n *Node) ID() NodeID       { return n.id }
func (n *Node) Role() Role       { return n.role }
func (n *Node) Term() Term       { return n.term }
func (n *Node) Leader() NodeID   { return n.lead }
func (n *Node) Commit() Index    { return n.commit }
func (n *Node) LastIndex() Index { return n.log.lastIndex() }
func (n *Node) Entries() []Entry { return n.log.slice(1, n.log.lastIndex()) }
func (n *Node) hardState() HardState {
	return HardState{Term: n.term, Vote: n.vote, Commit: n.commit}
}

// ---- roles ------------------------------------------------------------------------------------

func (n *Node) becomeFollower(term Term, lead NodeID) {
	if term > n.term {
		n.term = term
		n.vote = None
	}
	n.role = Follower
	n.lead = lead
	n.resetElectionTimer()
}

// preCampaign asks the cluster whether this node could win an election in the next term,
// without changing any state that a real election would.
func (n *Node) preCampaign() {
	n.role = PreCandidate
	n.lead = None
	n.votes = map[NodeID]bool{n.id: true}
	n.resetElectionTimer()
	if n.quorum(n.votes, true) {
		n.campaign()
		return
	}
	for _, p := range n.peers {
		if p != n.id {
			n.send(Message{Type: MsgPreVote, To: p, Term: n.term + 1, LastIndex: n.log.lastIndex(), LastTerm: n.log.lastTerm()})
		}
	}
}

func (n *Node) campaign() {
	n.role = Candidate
	n.term++
	n.vote = n.id
	n.lead = None
	n.votes = map[NodeID]bool{n.id: true}
	n.resetElectionTimer()
	if n.quorum(n.votes, true) {
		n.becomeLeader()
		return
	}
	for _, p := range n.peers {
		if p != n.id {
			n.send(Message{Type: MsgVote, To: p, LastIndex: n.log.lastIndex(), LastTerm: n.log.lastTerm()})
		}
	}
}

func (n *Node) becomeLeader() {
	n.role = Leader
	n.lead = n.id
	n.heartbeatElapsed = 0
	n.electionElapsed = 0
	n.active = map[NodeID]bool{}
	n.next = make(map[NodeID]Index, len(n.peers))
	n.match = make(map[NodeID]Index, len(n.peers))
	for _, p := range n.peers {
		n.next[p] = n.log.lastIndex() + 1
	}
	// A no-op in the new term lets the leader commit everything before it (§5.4.2, §8).
	n.appendEntry(nil)
	n.broadcastAppend()
}

func (n *Node) resetElectionTimer() {
	n.electionElapsed = 0
	n.electionTimeout = n.cfg.ElectionTicks + n.rng.IntN(n.cfg.ElectionTicks)
}

// ---- elections --------------------------------------------------------------------------------

func (n *Node) handleVote(m Message) {
	canVote := n.vote == None || n.vote == m.From
	upToDate := n.cfg.Unsafe.GrantAnyVote || n.log.isUpToDate(m.LastIndex, m.LastTerm)
	granted := canVote && upToDate && n.role != Leader
	if granted {
		n.vote = m.From
		n.resetElectionTimer() // granting a vote counts as hearing from a candidate (§5.2)
	}
	n.send(Message{Type: MsgVoteResp, To: m.From, Granted: granted})
}

// handlePreVote grants if the candidate could legitimately win: its proposed term is newer and
// its log is at least as up to date. Nothing is recorded; a pre-vote binds no one.
func (n *Node) handlePreVote(m Message) {
	upToDate := n.cfg.Unsafe.GrantAnyVote || n.log.isUpToDate(m.LastIndex, m.LastTerm)
	if m.Term > n.term && upToDate {
		n.send(Message{Type: MsgPreVoteResp, To: m.From, Term: m.Term, Granted: true})
		return
	}
	n.send(Message{Type: MsgPreVoteResp, To: m.From, Granted: false})
}

func (n *Node) handlePreVoteResp(m Message) {
	if n.role != PreCandidate {
		return
	}
	n.votes[m.From] = m.Granted
	switch {
	case n.quorum(n.votes, true):
		n.campaign()
	case n.quorum(n.votes, false):
		n.becomeFollower(n.term, None)
	}
}

// inLease reports whether this node heard from a leader within the minimum election timeout.
func (n *Node) inLease() bool {
	return n.cfg.CheckQuorum && n.lead != None && n.electionElapsed < n.cfg.ElectionTicks
}

// quorumActive reports whether a majority (counting this leader) was heard from since the last
// check, and starts a new round.
func (n *Node) quorumActive() bool {
	c := 1
	for _, p := range n.peers {
		if p != n.id && n.active[p] {
			c++
		}
	}
	n.active = map[NodeID]bool{}
	return c > len(n.peers)/2
}

func (n *Node) handleVoteResp(m Message) {
	if n.role != Candidate {
		return
	}
	n.votes[m.From] = m.Granted
	switch {
	case n.quorum(n.votes, true):
		n.becomeLeader()
	case n.quorum(n.votes, false):
		n.becomeFollower(n.term, None)
	}
}

// quorum reports whether a majority of the cluster answered v.
func (n *Node) quorum(votes map[NodeID]bool, v bool) bool {
	c := 0
	for _, got := range votes {
		if got == v {
			c++
		}
	}
	return c > len(n.peers)/2
}

// ---- replication ------------------------------------------------------------------------------

func (n *Node) appendEntry(data []byte) Index {
	idx := n.log.lastIndex() + 1
	n.log.append(Entry{Term: n.term, Index: idx, Data: data})
	n.match[n.id] = idx
	n.next[n.id] = idx + 1
	n.maybeCommit() // a one-node cluster commits on its own
	return idx
}

func (n *Node) broadcastAppend() {
	for _, p := range n.peers {
		if p != n.id {
			n.sendAppend(p)
		}
	}
}

func (n *Node) sendAppend(to NodeID) {
	next := n.next[to]
	prev := next - 1
	hi := min(n.log.lastIndex(), prev+Index(n.cfg.MaxEntriesPerMsg))
	n.send(Message{
		Type:      MsgApp,
		To:        to,
		PrevIndex: prev,
		PrevTerm:  n.log.term(prev),
		Entries:   n.log.slice(next, hi),
		Commit:    n.commit,
	})
}

func (n *Node) handleAppend(m Message) {
	// m.Term == n.term here, so m.From is this term's leader.
	n.role = Follower
	n.lead = m.From
	n.resetElectionTimer()

	if m.PrevIndex > n.log.lastIndex() {
		n.send(Message{Type: MsgAppResp, To: m.From, ConflictIndex: n.log.lastIndex() + 1})
		return
	}
	if t := n.log.term(m.PrevIndex); t != m.PrevTerm {
		n.send(Message{Type: MsgAppResp, To: m.From, ConflictTerm: t, ConflictIndex: n.log.firstIndexOfTerm(m.PrevIndex)})
		return
	}
	// Skip entries we already have; truncate at the first conflict and append the rest (§5.3).
	// Never truncate on a mere duplicate: a delayed, shorter append must not erase newer entries.
	for i, e := range m.Entries {
		if e.Index > n.log.lastIndex() || n.log.term(e.Index) != e.Term {
			n.log.truncateAndAppend(m.Entries[i:])
			break
		}
	}
	lastNew := m.PrevIndex + Index(len(m.Entries))
	if c := min(m.Commit, lastNew); c > n.commit {
		n.commit = c
	}
	n.send(Message{Type: MsgAppResp, To: m.From, Success: true, MatchIndex: lastNew})
}

func (n *Node) handleAppendResp(m Message) {
	if n.role != Leader {
		return
	}
	p := m.From
	n.active[p] = true
	if m.Success {
		if m.MatchIndex > n.match[p] {
			n.match[p] = m.MatchIndex
		}
		n.next[p] = max(n.next[p], n.match[p]+1)
		if n.maybeCommit() {
			n.broadcastAppend() // tell followers about the new commit index promptly
		} else if n.next[p] <= n.log.lastIndex() {
			n.sendAppend(p)
		}
		return
	}
	next := m.ConflictIndex
	if m.ConflictTerm != 0 {
		if last := n.log.lastIndexOfTerm(m.ConflictTerm); last != 0 {
			next = last + 1
		}
	}
	// A stale rejection must not move next below what the follower is known to hold.
	n.next[p] = max(min(next, n.log.lastIndex()+1), n.match[p]+1, 1)
	n.sendAppend(p)
}

// maybeCommit advances commit to the highest index replicated on a majority, but only to an
// entry from the current term (§5.4.2): earlier-term entries commit indirectly through it.
func (n *Node) maybeCommit() bool {
	matched := make([]Index, 0, len(n.peers))
	for _, p := range n.peers {
		matched = append(matched, n.match[p])
	}
	slices.Sort(matched)
	majority := matched[(len(matched)-1)/2] // the highest index at least a majority holds
	if majority <= n.commit {
		return false
	}
	if n.log.term(majority) != n.term && !n.cfg.Unsafe.CommitOldTerms {
		return false
	}
	n.commit = majority
	return true
}

func (n *Node) send(m Message) {
	m.From = n.id
	if m.Term == 0 { // pre-votes carry a future term; everything else carries ours
		m.Term = n.term
	}
	n.msgs = append(n.msgs, m)
}
