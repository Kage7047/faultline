package sim

import (
	"flag"
	"fmt"
	"strings"
	"testing"
)

var matrix = flag.Bool("matrix", false, "run every control in every mode (slow: ~15 min)")

// TestControlMatrix runs every control in every mode and prints the lowest seed that catches it.
// It shows why the safety tests run in three modes: no single mode catches all of them.
//
//	go test ./sim -run TestControlMatrix -matrix -v -timeout 60m
func TestControlMatrix(t *testing.T) {
	if !*matrix {
		t.Skip("slow: run with -matrix")
	}
	controls := []struct {
		name      string
		kv        bool
		snapshots bool // the bug lives in snapshot code, so a mode without snapshots cannot see it
		mutate    func(*Config)
	}{
		{"vote for any candidate", false, false, func(c *Config) { c.Raft.Unsafe.GrantAnyVote = true }},
		{"commit old-term entries (one entry per message)", false, false, func(c *Config) {
			c.Raft.Unsafe.CommitOldTerms, c.Raft.MaxEntriesPerMsg = true, 1
		}},
		{"install a stale snapshot", false, true, func(c *Config) { c.Raft.Unsafe.InstallStaleSnapshot = true }},
		{"no dedup of retried writes", true, false, func(c *Config) { c.UnsafeNoDedup = true }},
		{"snapshot without the dedup table", true, true, func(c *Config) { c.UnsafeSnapshotWithoutSessions = true }},
		{"read without the quorum round", true, false, func(c *Config) { c.Raft.Unsafe.ReadWithoutQuorum = true }},
		{"read before the term's first commit (one entry per message)", true, false, func(c *Config) {
			c.Raft.Unsafe.ReadBeforeTermCommit, c.Raft.MaxEntriesPerMsg = true, 1
		}},
	}
	var out strings.Builder
	fmt.Fprintf(&out, "| rule switched off | %s |\n|---|---|---|---|\n", strings.Join([]string{"prevote", "classic", "bare"}, " | "))
	for _, c := range controls {
		fmt.Fprintf(&out, "| %s |", c.name)
		for _, m := range modes {
			if c.snapshots && m.name == "bare" {
				out.WriteString(" n/a |")
				continue
			}
			budget := uint64(2000)
			if c.kv {
				budget = 200
			}
			seed, _ := lowestCaught(budget, func(seed uint64) Result {
				cfg := Defaults(seed)
				if c.kv {
					cfg = KVDefaults(seed)
				}
				m.set(&cfg)
				c.mutate(&cfg)
				return Run(cfg)
			})
			if seed == 0 {
				fmt.Fprintf(&out, " missed in %d |", budget)
			} else {
				fmt.Fprintf(&out, " seed %d |", seed)
			}
		}
		out.WriteString("\n")
	}
	t.Log("\n" + out.String())
}
