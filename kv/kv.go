// Package kv is the replicated state machine: a string key-value store with client sessions.
//
// Writes go through the Raft log. Reads are served from local state after a ReadIndex round
// (see raft.Node.ReadIndex), or through the log too if the caller prefers; either way a read
// reflects every write that completed before it started (linearizability).
//
// Each command carries (client, seq). The store remembers the last seq and result per client,
// so a command retried after a timeout or a leader change is applied once and answered with the
// original result: exactly-once effects, the same idea as an idempotency key, kept inside the
// replicated state so every replica agrees on it.
package kv

import (
	"cmp"
	"encoding/json"
	"fmt"
	"slices"
)

// Op is what a command does.
type Op uint8

const (
	Get Op = iota
	Put
	Append
)

func (o Op) String() string { return [...]string{"get", "put", "append"}[o] }

// Command is one client request, as stored in the log.
type Command struct {
	Client uint64 `json:"c"`
	Seq    uint64 `json:"s"`
	Op     Op     `json:"o"`
	Key    string `json:"k"`
	Value  string `json:"v,omitempty"`
}

func (c Command) String() string {
	if c.Op == Get {
		return fmt.Sprintf("c%d#%d get(%s)", c.Client, c.Seq, c.Key)
	}
	return fmt.Sprintf("c%d#%d %s(%s, %q)", c.Client, c.Seq, c.Op, c.Key, c.Value)
}

// Encode turns a command into a log entry payload.
func (c Command) Encode() []byte {
	b, err := json.Marshal(c)
	if err != nil {
		panic(err) // a struct of strings and integers always marshals
	}
	return b
}

// Decode parses a log entry payload.
func Decode(b []byte) (Command, error) {
	var c Command
	err := json.Unmarshal(b, &c)
	return c, err
}

// Result is what a command returned. Get returns the value; Put and Append return the value after.
type Result struct {
	Value string
	// Duplicate is true when the command had already been applied and this is the remembered answer.
	Duplicate bool
}

type session struct {
	seq    uint64
	result Result
}

// Store is the state machine. Apply must be called with every committed command, in log order,
// on every replica; given the same log, every replica ends in the same state.
type Store struct {
	data     map[string]string
	sessions map[uint64]session

	// Unsafe disables deduplication, for the control test that proves duplicates are detected.
	UnsafeNoDedup bool
	// UnsafeSnapshotWithoutSessions leaves the session table out of snapshots, a classic bug:
	// a replica restored from one forgets which requests it already applied.
	UnsafeSnapshotWithoutSessions bool
}

// New returns an empty store.
func New() *Store {
	return &Store{data: map[string]string{}, sessions: map[uint64]session{}}
}

// Apply executes a command and returns its result.
func (s *Store) Apply(c Command) Result {
	if prev, ok := s.sessions[c.Client]; ok && c.Seq <= prev.seq && !s.UnsafeNoDedup {
		// A retry of something already applied. Clients have one request in flight at a time,
		// so only the latest result is ever asked for again.
		r := prev.result
		r.Duplicate = true
		return r
	}
	switch c.Op {
	case Put:
		s.data[c.Key] = c.Value
	case Append:
		s.data[c.Key] += c.Value
	}
	r := Result{Value: s.data[c.Key]}
	s.sessions[c.Client] = session{seq: c.Seq, result: r}
	return r
}

type snapshotSession struct {
	Seq    uint64 `json:"s"`
	Value  string `json:"v"`
	Client uint64 `json:"c"`
}

type snapshot struct {
	Data     map[string]string `json:"data"`
	Sessions []snapshotSession `json:"sessions,omitempty"`
}

// Snapshot encodes the whole state: the data and the session table. Without the sessions, a
// replica restored from the snapshot would apply a retried write a second time.
func (s *Store) Snapshot() []byte {
	snap := snapshot{Data: s.data}
	if !s.UnsafeSnapshotWithoutSessions {
		for c, sess := range s.sessions {
			snap.Sessions = append(snap.Sessions, snapshotSession{Client: c, Seq: sess.seq, Value: sess.result.Value})
		}
		slices.SortFunc(snap.Sessions, func(a, b snapshotSession) int { return cmp.Compare(a.Client, b.Client) })
	}
	b, err := json.Marshal(snap) // map keys are sorted, so equal states encode equally
	if err != nil {
		panic(err)
	}
	return b
}

// Restore replaces the state with a snapshot's.
func (s *Store) Restore(b []byte) error {
	var snap snapshot
	if err := json.Unmarshal(b, &snap); err != nil {
		return err
	}
	s.data = snap.Data
	if s.data == nil {
		s.data = map[string]string{}
	}
	s.sessions = map[uint64]session{}
	for _, sess := range snap.Sessions {
		s.sessions[sess.Client] = session{seq: sess.Seq, result: Result{Value: sess.Value}}
	}
	return nil
}

// Read returns a key's value from this replica's local state, without going through the log.
// It is linearizable only after a ReadIndex round, once the store has applied the read index.
func (s *Store) Read(key string) string { return s.data[key] }
