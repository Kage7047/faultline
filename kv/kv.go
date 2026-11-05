// Package kv is the replicated state machine: a string key-value store with client sessions.
//
// Every command goes through the Raft log, reads included, so a read reflects every write that
// completed before it started (linearizability). Each command carries (client, seq). The store
// remembers the last seq and result per client, so a command retried after a timeout or a leader
// change is applied once and answered with the original result: exactly-once effects, the same
// idea as an idempotency key, kept inside the replicated state so every replica agrees on it.
package kv

import (
	"encoding/json"
	"fmt"
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

// Read returns a key's value from this replica's local state, without going through the log.
// It is only linearizable if the caller knows this replica is up to date; the store uses it for
// the unsafe-local-reads control and for debugging.
func (s *Store) Read(key string) string { return s.data[key] }
