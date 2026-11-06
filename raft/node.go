package raft

import (
	"errors"
	"math/rand/v2"
	"slices"
)

// ErrNotLeader is returned by Propose and ReadIndex on a node that is not the leader. Leader()
// may name one.
var ErrNotLeader = errors.New("raft: not the leader")

// ErrReadNotReady is returned by ReadIndex on a new leader that has not yet committed an entry
// of its own term, so it does not know the cluster's commit index yet. Retry shortly.
var ErrReadNotReady = errors.New("raft: leader has not committed in its term yet")

// ErrCompacted is returned by Compact for an index the log no longer holds, or one not applied yet.
var ErrCompacted = errors.New("raft: cannot compact at that index")

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

	readSeq    uint64        // leader: last read sequence number handed out
	reads      []pendingRead // leader: reads waiting for a majority to confirm leadership
	readStates []ReadState

	snapshot        Snapshot  // the latest snapshot, sent to followers that need compacted entries
	pendingSnapshot *Snapshot // installed from the leader, not yet handed out by Ready

	msgs   []Message
	prevHS HardState
}

type pendingRead struct {
	seq   uint64
	ctx   uint64
	index Index
	acks  map[NodeID]bool
}

// NewNode starts a node from what it persisted before (zero values for a brand-new node).
// applied is the last index the state machine already contains; entries after it up to the
// persisted commit index are handed out again by Ready.
func NewNode(cfg Config, hs HardState, entries []Entry, applied Index) *Node {
	return NewNodeFromSnapshot(cfg, hs, Snapshot{}, entries, applied)
}

// NewNodeFromSnapshot is NewNode for a node that persisted a snapshot and the entries after it.
// The caller restores its state machine from snap first, so applied is at least snap.Index.
func NewNodeFromSnapshot(cfg Config, hs HardState, snap Snapshot, entries []Entry, applied Index) *Node {
	cfg.setDefaults()
	peers := slices.Clone(cfg.Peers)
	slices.Sort(peers)
	n := &Node{
		cfg:      cfg,
		id:       cfg.ID,
		peers:    peers,
		term:     hs.Term,
		vote:     hs.Vote,
		commit:   max(hs.Commit, snap.Index),
		applied:  max(applied, snap.Index),
		rng:      rand.New(rand.NewPCG(cfg.Seed, uint64(cfg.ID))),
		snapshot: snap,
	}
	n.log.restore(snap)
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

// ReadIndex starts a linearizable read without writing to the log (§6.4 of Ongaro's thesis).
// The leader records its commit index, then confirms with one heartbeat round that a majority
// still follows it: no newer leader can have committed anything it does not know about. The
// answer comes back in Ready.ReadStates with the same ctx; the caller reads its state machine
// once it has applied that index. If leadership is lost first, the read is dropped and the
// caller times out.
func (n *Node) ReadIndex(ctx uint64) error {
	if n.role != Leader {
		return ErrNotLeader
	}
	// Until an entry of this term commits, n.commit may be behind what the old leader committed.
	if n.log.term(n.commit) != n.term && !n.cfg.Unsafe.ReadBeforeTermCommit {
		return ErrReadNotReady
	}
	if len(n.peers) == 1 || n.cfg.Unsafe.ReadWithoutQuorum {
		n.readStates = append(n.readStates, ReadState{Index: n.commit, Ctx: ctx})
		return nil
	}
	n.readSeq++
	n.reads = append(n.reads, pendingRead{seq: n.readSeq, ctx: ctx, index: n.commit, acks: map[NodeID]bool{n.id: true}})
	n.broadcastAppend()
	return nil
}

// Compact tells the node the state machine's state as of index (already applied) is saved as
// data. The log drops every entry up to index, and data is what the leader will send to a
// follower that needs any of them. The caller persists the snapshot before dropping its stored
// entries.
func (n *Node) Compact(index Index, data []byte) error {
	if index <= n.log.snapIndex || index > n.applied {
		return ErrCompacted
	}
	n.snapshot = Snapshot{Index: index, Term: n.log.term(index), Data: data}
	n.log.compact(index)
	return nil
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
			if m.Type == MsgApp || m.Type == MsgSnap {
				lead = m.From
			}
			n.becomeFollower(m.Term, lead)
		}
	case m.Term < n.term:
		// A stale leader or candidate learns the current term from the rejection and steps down.
		switch m.Type {
		case MsgApp, MsgSnap:
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
	case MsgSnap:
		n.handleSnapshot(m)
	}
}

// ---- outputs ----------------------------------------------------------------------------------

// HasReady reports whether Ready would return anything.
func (n *Node) HasReady() bool {
	return len(n.msgs) > 0 || n.log.stable < n.log.lastIndex() || n.hardState() != n.prevHS || n.commit > n.applied ||
		len(n.readStates) > 0 || n.pendingSnapshot != nil
}

// Ready collects the node's output. See the Ready type for the caller's obligations.
func (n *Node) Ready() Ready {
	hs := n.hardState()
	rd := Ready{
		SoftState:  SoftState{Role: n.role, Lead: n.lead},
		HardState:  hs,
		MustSync:   hs != n.prevHS,
		Snapshot:   n.pendingSnapshot,
		Entries:    n.log.unstable(),
		Messages:   n.msgs,
		Committed:  n.log.slice(n.applied+1, n.commit),
		ReadStates: n.readStates,
	}
	n.msgs, n.readStates, n.pendingSnapshot = nil, nil, nil
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

// FirstIndex is the first index still in the log; everything before it is in the snapshot.
func (n *Node) FirstIndex() Index { return n.log.firstIndex() }

// Entries returns the log after the snapshot.
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
	n.reads = nil // unconfirmed reads die with the leadership; their callers time out
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
	if prev < n.log.snapIndex {
		// The follower needs entries we no longer have. Assume the snapshot arrives; if it does
		// not, the follower rejects the next append and we land here again.
		snap := n.snapshot
		n.send(Message{Type: MsgSnap, To: to, Snapshot: &snap, ReadSeq: n.readSeq})
		n.next[to] = snap.Index + 1
		return
	}
	hi := min(n.log.lastIndex(), prev+Index(n.cfg.MaxEntriesPerMsg))
	n.send(Message{
		Type:      MsgApp,
		To:        to,
		PrevIndex: prev,
		PrevTerm:  n.log.term(prev),
		Entries:   n.log.slice(next, hi),
		Commit:    n.commit,
		ReadSeq:   n.readSeq,
	})
}

func (n *Node) handleAppend(m Message) {
	// m.Term == n.term here, so m.From is this term's leader.
	n.role = Follower
	n.lead = m.From
	n.resetElectionTimer()

	// Entries up to our snapshot are committed, so they match the leader's: skip that part.
	if m.PrevIndex < n.log.snapIndex {
		skip := n.log.snapIndex - m.PrevIndex
		if Index(len(m.Entries)) <= skip {
			n.send(Message{Type: MsgAppResp, To: m.From, Success: true, MatchIndex: n.log.snapIndex, ReadSeq: m.ReadSeq})
			return
		}
		m.Entries = m.Entries[skip:]
		m.PrevIndex, m.PrevTerm = n.log.snapIndex, n.log.snapTerm
	}

	// Every answer echoes ReadSeq, rejections included: a rejection in this term still says
	// "you are my leader", which is all a read needs.
	if m.PrevIndex > n.log.lastIndex() {
		n.send(Message{Type: MsgAppResp, To: m.From, ConflictIndex: n.log.lastIndex() + 1, ReadSeq: m.ReadSeq})
		return
	}
	if t := n.log.term(m.PrevIndex); t != m.PrevTerm {
		n.send(Message{Type: MsgAppResp, To: m.From, ConflictTerm: t, ConflictIndex: n.log.firstIndexOfTerm(m.PrevIndex),
			ReadSeq: m.ReadSeq})
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
	n.send(Message{Type: MsgAppResp, To: m.From, Success: true, MatchIndex: lastNew, ReadSeq: m.ReadSeq})
}

// handleSnapshot is InstallSnapshot on the follower (Figure 13 of the Raft paper).
func (n *Node) handleSnapshot(m Message) {
	n.role = Follower
	n.lead = m.From
	n.resetElectionTimer()
	s := *m.Snapshot
	ok := func(match Index) {
		n.send(Message{Type: MsgAppResp, To: m.From, Success: true, MatchIndex: match, ReadSeq: m.ReadSeq})
	}
	if s.Index <= n.commit && !n.cfg.Unsafe.InstallStaleSnapshot {
		ok(n.commit) // late or duplicated: we are already past it, and committed entries match the leader's
		return
	}
	if s.Index > n.commit && n.log.term(s.Index) == s.Term {
		n.commit = s.Index // we hold that entry already: keep the log after it (Log Matching)
		ok(s.Index)
		return
	}
	n.log.restore(s)
	n.snapshot = s
	n.commit, n.applied = s.Index, s.Index
	n.pendingSnapshot = &s
	ok(s.Index)
}

func (n *Node) handleAppendResp(m Message) {
	if n.role != Leader {
		return
	}
	p := m.From
	n.active[p] = true
	if m.ReadSeq > 0 {
		n.ackReads(p, m.ReadSeq)
	}
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

// ackReads records that peer still followed this leader after read seq was asked, which covers
// every read asked before it too. Reads are released in order once a majority has answered.
func (n *Node) ackReads(peer NodeID, seq uint64) {
	done := 0
	for i := range n.reads {
		r := &n.reads[i]
		if r.seq > seq {
			break
		}
		r.acks[peer] = true
		if len(r.acks) > len(n.peers)/2 {
			done = i + 1 // acks only grow towards older reads, so all reads before i are done too
		}
	}
	for _, r := range n.reads[:done] {
		n.readStates = append(n.readStates, ReadState{Index: r.index, Ctx: r.ctx})
	}
	n.reads = n.reads[done:]
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
