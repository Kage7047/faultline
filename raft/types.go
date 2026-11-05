// Package raft is the Raft consensus algorithm as a deterministic state machine.
//
// A Node does no I/O and starts no goroutines. The caller feeds it time (Tick), messages (Step)
// and proposals (Propose), then collects what it produced with Ready: state and entries to
// persist, messages to send, and committed entries to apply. The same Node runs under a
// deterministic simulator in tests and over a real network in the server, which is what makes
// every bug the simulator finds replayable from its seed.
package raft

import "fmt"

// NodeID names a member of the cluster. 0 means "nobody".
type NodeID uint64

// None is the zero NodeID: no vote cast, no known leader.
const None NodeID = 0

// Term is a Raft term number. Terms only grow.
type Term uint64

// Index is a log position. The first entry has index 1; index 0 is the empty log.
type Index uint64

// Role is what a node currently believes it is.
type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return fmt.Sprintf("role(%d)", r)
}

// Entry is one slot of the replicated log. A nil Data is the no-op a new leader appends.
type Entry struct {
	Term  Term
	Index Index
	Data  []byte
}

// MsgType enumerates the messages nodes exchange.
type MsgType uint8

const (
	MsgVote MsgType = iota
	MsgVoteResp
	MsgApp
	MsgAppResp
)

func (t MsgType) String() string {
	return [...]string{"MsgVote", "MsgVoteResp", "MsgApp", "MsgAppResp"}[t]
}

// Message is every RPC and response in one struct. Fields not used by a type stay zero.
type Message struct {
	Type MsgType
	From NodeID
	To   NodeID
	Term Term

	// MsgVote: the candidate's last log position.
	LastIndex Index
	LastTerm  Term
	// MsgVoteResp
	Granted bool

	// MsgApp: entries follow PrevIndex, whose term must match PrevTerm on the follower.
	PrevIndex Index
	PrevTerm  Term
	Entries   []Entry
	Commit    Index

	// MsgAppResp. On success MatchIndex is the last index known to match the leader.
	// On rejection the follower sends a hint so the leader can skip a whole term at a time:
	// ConflictTerm is the term at PrevIndex (0 if the follower's log is shorter) and
	// ConflictIndex the first index the leader should try next.
	Success       bool
	MatchIndex    Index
	ConflictTerm  Term
	ConflictIndex Index
}

func (m Message) String() string {
	switch m.Type {
	case MsgVote:
		return fmt.Sprintf("%d->%d %s t%d last=%d@t%d", m.From, m.To, m.Type, m.Term, m.LastIndex, m.LastTerm)
	case MsgVoteResp:
		return fmt.Sprintf("%d->%d %s t%d granted=%v", m.From, m.To, m.Type, m.Term, m.Granted)
	case MsgApp:
		return fmt.Sprintf("%d->%d %s t%d prev=%d@t%d n=%d commit=%d", m.From, m.To, m.Type, m.Term, m.PrevIndex,
			m.PrevTerm, len(m.Entries), m.Commit)
	default:
		return fmt.Sprintf("%d->%d %s t%d ok=%v match=%d conflict=%d@t%d", m.From, m.To, m.Type, m.Term, m.Success,
			m.MatchIndex, m.ConflictIndex, m.ConflictTerm)
	}
}

// HardState is what must be on stable storage before a node sends any message that depends on it.
type HardState struct {
	Term   Term
	Vote   NodeID
	Commit Index
}

// SoftState is volatile and only reported for observability (and the simulator's checks).
type SoftState struct {
	Role Role
	Lead NodeID
}

// Ready is everything a Node produced since the last Advance. The caller must, in order:
//  1. persist HardState (if MustSync) and Entries (overwriting any stored entries at or after
//     Entries[0].Index, which may have been truncated),
//  2. send Messages,
//  3. apply Committed to the state machine,
//
// then call Advance.
type Ready struct {
	SoftState SoftState
	HardState HardState
	MustSync  bool
	Entries   []Entry
	Messages  []Message
	Committed []Entry
}

// Config sets a node's identity and timing. Timeouts are in ticks.
type Config struct {
	ID    NodeID
	Peers []NodeID // every member, including ID
	// ElectionTicks is the minimum election timeout; each node waits a random number of ticks in
	// [ElectionTicks, 2*ElectionTicks) so that split votes resolve.
	ElectionTicks  int
	HeartbeatTicks int
	// MaxEntriesPerMsg caps an append message so a far-behind follower catches up in steps.
	MaxEntriesPerMsg int
	// Seed makes election timeouts deterministic.
	Seed uint64

	// Unsafe switches turn off one safety rule each. They exist so tests can prove the
	// simulator catches the bugs these rules prevent. Never set them outside tests.
	Unsafe Unsafe
}

// Unsafe disables individual safety rules (see Config.Unsafe).
type Unsafe struct {
	// GrantAnyVote skips the "candidate's log is at least as up to date" check (§5.4.1).
	GrantAnyVote bool
	// CommitOldTerms lets a leader commit entries from earlier terms by counting replicas
	// (the bug in Figure 8 of the Raft paper).
	CommitOldTerms bool
}

func (c *Config) setDefaults() {
	if c.ElectionTicks == 0 {
		c.ElectionTicks = 10
	}
	if c.HeartbeatTicks == 0 {
		c.HeartbeatTicks = 1
	}
	if c.MaxEntriesPerMsg == 0 {
		c.MaxEntriesPerMsg = 64
	}
}
