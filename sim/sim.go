// Package sim runs a Raft cluster inside one goroutine on a virtual clock. Every source of
// nondeterminism (message latency, loss, duplication, reordering, partitions, crashes, election
// timeouts) comes from one seeded generator, so a run is fully determined by its seed: a failure
// found on seed 4127 reproduces on seed 4127, every time, on any machine.
package sim

import (
	"container/heap"
	"fmt"
	"math/rand/v2"
	"strings"

	"github.com/Kage7047/faultline/kv"
	"github.com/Kage7047/faultline/raft"
)

// Config describes one simulated run. Times are virtual milliseconds.
type Config struct {
	Seed     uint64
	Nodes    int
	Duration int64

	TickMs     int64 // one raft tick
	MinLatency int64
	MaxLatency int64
	DropRate   float64
	DupRate    float64

	// Faults turns on crashes, restarts and partitions. QuietTail is how long before the end
	// faults stop and everything heals, so liveness can be checked.
	Faults    bool
	QuietTail int64

	ProposeEvery int64       // raw proposals to a random node (consensus-only runs)
	Raft         raft.Config // ID, Peers and Seed are filled in per node

	// Clients > 0 runs the key-value store with that many clients, and checks the history of
	// what they saw for linearizability at the end.
	Clients       int
	Keys          int
	ClientTimeout int64
	CheckTimeout  int64 // ms the linearizability checker may take before giving up (inconclusive)
	// Unsafe key-value switches for the controls.
	UnsafeNoDedup          bool   // apply retried writes again
	UnsafeLeaderLocalReads bool   // a leader answers reads from local state without confirming it still leads
	VisualizePath          string // write Porcupine's HTML view of a non-linearizable history here

	TraceLen      int  // how many recent events to keep for a failure report
	TraceMessages bool // also record every delivered message (verbose; for debugging a seed)
}

// Defaults returns a hostile but realistic configuration.
func Defaults(seed uint64) Config {
	return Config{
		Seed:         seed,
		Nodes:        5,
		Duration:     30_000,
		TickMs:       10,
		MinLatency:   1,
		MaxLatency:   30,
		DropRate:     0.05,
		DupRate:      0.02,
		Faults:       true,
		QuietTail:    5_000,
		ProposeEvery: 15,
		Raft:         raft.Config{ElectionTicks: 15, HeartbeatTicks: 3, PreVote: true, CheckQuorum: true},
		TraceLen:     60,
	}
}

// KVDefaults is Defaults with clients talking to the key-value store instead of raw proposals.
func KVDefaults(seed uint64) Config {
	c := Defaults(seed)
	c.ProposeEvery = 0
	c.Clients = 5
	c.Keys = 3
	c.ClientTimeout = 300
	c.CheckTimeout = 20_000
	return c
}

// Stats summarises a run.
type Stats struct {
	Leaders     int // distinct (term, leader) pairs
	MaxTerm     raft.Term
	Proposed    int
	Committed   raft.Index
	Crashes     int
	Partitions  int
	Sent        int
	Dropped     int
	Duplicated  int
	FinalCommit []raft.Index // per node, at the end

	Ops     int // client operations that completed
	Retries int // client resends after a timeout or a "not leader" answer
}

// Result is what a run found.
type Result struct {
	Stats     Stats
	Violation string // empty if every check passed
	Trace     []string
	// Inconclusive is set when the linearizability checker ran out of time.
	Inconclusive bool
}

func (r Result) String() string {
	var b strings.Builder
	s := r.Stats
	fmt.Fprintf(&b, "leaders=%d maxTerm=%d proposed=%d committed=%d crashes=%d partitions=%d msgs=%d dropped=%d dup=%d",
		s.Leaders, s.MaxTerm, s.Proposed, s.Committed, s.Crashes, s.Partitions, s.Sent, s.Dropped, s.Duplicated)
	if s.Ops > 0 {
		fmt.Fprintf(&b, " ops=%d retries=%d", s.Ops, s.Retries)
	}
	if r.Violation != "" {
		fmt.Fprintf(&b, "\nVIOLATION: %s\nlast events:\n  %s", r.Violation, strings.Join(r.Trace, "\n  "))
	}
	return b.String()
}

type disk struct {
	hs      raft.HardState
	entries []raft.Entry
}

type node struct {
	id        raft.NodeID
	r         *raft.Node // nil while crashed
	disk      disk
	nextTick  int64
	restartAt int64
	restarts  uint64
	group     int // partition group; nodes talk only within a group
	wasLeader raft.Term

	store   *kv.Store
	waiters map[raft.Index]waiter // client requests this node proposed, by log index
}

// Sim is one run in progress.
type Sim struct {
	cfg   Config
	rng   *rand.Rand
	now   int64
	seq   uint64
	nodes []*node
	queue envelopes
	check *checker
	stats Stats

	healAt    int64
	trace     []string
	violation string

	clients      []*client
	history      []historyOp
	inconclusive bool
}

// Run executes a whole simulation and reports what it found.
func Run(cfg Config) Result {
	s := New(cfg)
	for s.now < cfg.Duration && s.violation == "" {
		s.step()
	}
	if s.violation == "" {
		s.finalChecks()
	}
	if s.violation == "" && cfg.Clients > 0 {
		s.checkLinearizable()
	}
	for _, n := range s.nodes {
		c := raft.Index(0)
		if n.r != nil {
			c = n.r.Commit()
		}
		s.stats.FinalCommit = append(s.stats.FinalCommit, c)
	}
	s.stats.Committed = s.check.maxCommitted()
	return Result{Stats: s.stats, Violation: s.violation, Trace: s.trace, Inconclusive: s.inconclusive}
}

// New builds a cluster without running it.
func New(cfg Config) *Sim {
	s := &Sim{cfg: cfg, rng: rand.New(rand.NewPCG(cfg.Seed, 0x5eed)), check: newChecker()}
	for i := 1; i <= cfg.Nodes; i++ {
		n := &node{id: raft.NodeID(i)}
		n.nextTick = s.rng.Int64N(cfg.TickMs) // nodes' clocks are not aligned
		s.nodes = append(s.nodes, n)
		s.start(n)
	}
	for i := range cfg.Clients {
		s.clients = append(s.clients, &client{id: i, guess: raft.NodeID(1 + s.rng.IntN(cfg.Nodes))})
	}
	return s
}

// peers is the full membership, fixed for the whole run (no membership changes yet).
func (s *Sim) peers() []raft.NodeID {
	ids := make([]raft.NodeID, s.cfg.Nodes)
	for i := range ids {
		ids[i] = raft.NodeID(i + 1)
	}
	return ids
}

func (s *Sim) start(n *node) {
	c := s.cfg.Raft
	c.ID, c.Peers = n.id, s.peers()
	c.Seed = s.cfg.Seed*1_000_003 + uint64(n.id)*7919 + n.restarts
	// A restarted node has an empty state machine: it re-applies the committed log from index 1.
	n.r = raft.NewNode(c, n.disk.hs, n.disk.entries, 0)
	n.store = kv.New()
	n.store.UnsafeNoDedup = s.cfg.UnsafeNoDedup
	n.waiters = map[raft.Index]waiter{}
	s.check.restarted(n.id)
}

// step advances virtual time by one millisecond.
func (s *Sim) step() {
	for s.queue.Len() > 0 && s.queue[0].at <= s.now && s.violation == "" {
		e := heap.Pop(&s.queue).(envelope)
		switch e.kind {
		case toNode:
			s.deliver(e.m)
		case toServer:
			s.serve(e.req)
		case toClient:
			s.clientReceive(e.resp)
		}
	}
	s.runClients()
	for _, n := range s.nodes {
		if s.violation != "" {
			return
		}
		if n.r == nil {
			if s.now >= n.restartAt {
				s.logf("restart %d", n.id)
				n.restarts++
				s.start(n)
				s.process(n)
			}
			continue
		}
		if s.now >= n.nextTick {
			n.nextTick += s.cfg.TickMs
			n.r.Tick()
			s.process(n)
		}
	}
	if s.cfg.ProposeEvery > 0 && s.now%s.cfg.ProposeEvery == 0 {
		s.propose()
	}
	if s.cfg.Faults {
		s.faults()
	}
	s.now++
}

func (s *Sim) deliver(m raft.Message) {
	to := s.nodes[m.To-1]
	from := s.nodes[m.From-1]
	if to.r == nil || to.group != from.group {
		s.stats.Dropped++
		return
	}
	if s.cfg.TraceMessages {
		s.logf("deliver %s", m)
	}
	to.r.Step(m)
	s.process(to)
}

// process drains a node's Ready in the order Raft requires: persist, then send, then apply.
func (s *Sim) process(n *node) {
	for n.r != nil && n.r.HasReady() && s.violation == "" {
		rd := n.r.Ready()
		if rd.MustSync {
			n.disk.hs = rd.HardState
		}
		if len(rd.Entries) > 0 {
			first := rd.Entries[0].Index
			n.disk.entries = append(n.disk.entries[:first-1:first-1], rd.Entries...)
		}
		for _, m := range rd.Messages {
			s.send(m)
		}
		if rd.SoftState.Role == raft.Leader && n.wasLeader != rd.HardState.Term {
			n.wasLeader = rd.HardState.Term
			s.stats.Leaders++
			s.logf("leader %d term %d", n.id, rd.HardState.Term)
			s.fail(s.check.leader(n.id, rd.HardState.Term, n.r.Entries()))
		}
		s.stats.MaxTerm = max(s.stats.MaxTerm, rd.HardState.Term)
		for _, e := range rd.Committed {
			s.fail(s.check.apply(n.id, e))
			if s.cfg.Clients > 0 && e.Data != nil {
				s.applyCommand(n, e)
			}
		}
		n.r.Advance(rd)
	}
}

func (s *Sim) send(m raft.Message) {
	s.stats.Sent++
	if s.rng.Float64() < s.cfg.DropRate {
		s.stats.Dropped++
		return
	}
	copies := 1
	if s.rng.Float64() < s.cfg.DupRate {
		copies = 2
		s.stats.Duplicated++
	}
	for range copies {
		s.enqueue(envelope{kind: toNode, m: m})
	}
}

// enqueue schedules delivery after a random latency with a long tail: about 1% of messages
// arrive ten times later than usual, after newer ones.
func (s *Sim) enqueue(e envelope) {
	lat := s.cfg.MinLatency + s.rng.Int64N(s.cfg.MaxLatency-s.cfg.MinLatency+1)
	if s.rng.Float64() < 0.01 {
		lat *= 10
	}
	s.seq++
	e.at, e.seq = s.now+lat, s.seq
	heap.Push(&s.queue, e)
}

func (s *Sim) propose() {
	// Clients do not know who leads; they try a random live node, like a client with a stale
	// leader hint. Two nodes may both think they lead (in different terms); both may accept.
	n := s.nodes[s.rng.IntN(len(s.nodes))]
	if n.r == nil {
		return
	}
	data := fmt.Appendf(nil, "op-%d", s.stats.Proposed+1)
	if _, _, err := n.r.Propose(data); err == nil {
		s.stats.Proposed++
		s.process(n)
	}
}

func (s *Sim) quiet() bool { return s.now >= s.cfg.Duration-s.cfg.QuietTail }

func (s *Sim) faults() {
	if s.quiet() {
		s.heal()
		for _, n := range s.nodes {
			if n.r == nil {
				n.restartAt = s.now
			}
		}
		return
	}
	if s.healAt != 0 && s.now >= s.healAt {
		s.heal()
	}
	r := s.rng.Float64()
	switch {
	case r < 0.0006: // ~ every 1.7 s: crash a node
		n := s.nodes[s.rng.IntN(len(s.nodes))]
		if n.r != nil {
			s.logf("crash %d", n.id)
			n.r = nil
			n.restartAt = s.now + 50 + s.rng.Int64N(2_000)
			s.stats.Crashes++
		}
	case r < 0.0010 && s.healAt == 0: // ~ every 2.5 s: partition the cluster at random
		for _, n := range s.nodes {
			n.group = s.rng.IntN(2)
		}
		s.healAt = s.now + 200 + s.rng.Int64N(3_000)
		s.stats.Partitions++
		s.logf("partition %s until %d", s.groups(), s.healAt)
	}
}

func (s *Sim) heal() {
	if s.healAt == 0 {
		return
	}
	for _, n := range s.nodes {
		n.group = 0
	}
	s.healAt = 0
	s.logf("heal")
}

func (s *Sim) groups() string {
	var a, b []string
	for _, n := range s.nodes {
		if n.group == 0 {
			a = append(a, fmt.Sprint(n.id))
		} else {
			b = append(b, fmt.Sprint(n.id))
		}
	}
	return fmt.Sprintf("{%s}|{%s}", strings.Join(a, ","), strings.Join(b, ","))
}

func (s *Sim) finalChecks() {
	var logs [][]raft.Entry
	for _, n := range s.nodes {
		logs = append(logs, n.disk.entries)
	}
	s.fail(logMatching(logs))
}

func (s *Sim) fail(v string) {
	if v != "" && s.violation == "" {
		s.violation = fmt.Sprintf("t=%dms seed=%d: %s", s.now, s.cfg.Seed, v)
	}
}

func (s *Sim) logf(format string, args ...any) {
	if s.cfg.TraceLen == 0 {
		return
	}
	s.trace = append(s.trace, fmt.Sprintf("%6dms ", s.now)+fmt.Sprintf(format, args...))
	if len(s.trace) > s.cfg.TraceLen {
		s.trace = s.trace[1:]
	}
}

// ---- message queue: a min-heap on (delivery time, send order) --------------------------------

type envelopeKind uint8

const (
	toNode   envelopeKind = iota // a raft message between nodes
	toServer                     // a client request to a node
	toClient                     // a node's answer to a client
)

type envelope struct {
	at   int64
	seq  uint64
	kind envelopeKind
	m    raft.Message
	req  request
	resp response
}

type envelopes []envelope

func (q envelopes) Len() int { return len(q) }
func (q envelopes) Less(i, j int) bool {
	return q[i].at < q[j].at || (q[i].at == q[j].at && q[i].seq < q[j].seq)
}
func (q envelopes) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *envelopes) Push(x any)   { *q = append(*q, x.(envelope)) }
func (q *envelopes) Pop() any {
	old := *q
	e := old[len(old)-1]
	*q = old[:len(old)-1]
	return e
}
