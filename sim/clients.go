package sim

import (
	"fmt"
	"math"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/Kage7047/faultline/kv"
	"github.com/Kage7047/faultline/raft"
)

// A client has at most one request in flight. It does not know who leads: it starts with a guess,
// follows "not leader" hints, and resends the same (client, seq) after a timeout, to the same node
// or another one. The request and every answer cross the same lossy, reordering network as the
// raft messages, so answers can be lost (forcing a retry of something already applied) or arrive
// long after the client moved on.
type client struct {
	id     int
	seq    uint64
	guess  raft.NodeID
	cur    *call
	nextAt int64
}

type call struct {
	cmd      kv.Command
	start    int64
	deadline int64
}

type request struct {
	client int
	node   raft.NodeID
	cmd    kv.Command
}

type response struct {
	client int
	seq    uint64
	ok     bool
	value  string
	hint   raft.NodeID // on !ok: who the node thinks leads
}

// waiter remembers which client is waiting for the entry a node proposed at some index.
type waiter struct {
	client int
	seq    uint64
	term   raft.Term
}

// pendingGet is a get served by ReadIndex: first waiting for leadership to be confirmed, then for
// the store to catch up with the read index.
type pendingGet struct {
	client int
	seq    uint64
	key    string
	index  raft.Index
}

// historyOp is one completed (or, at the end, still pending) client operation.
type historyOp struct {
	client      int
	cmd         kv.Command
	call, ret   int64
	output      string
	outputKnown bool
}

func (s *Sim) runClients() {
	for _, c := range s.clients {
		switch {
		case c.cur == nil && s.now >= c.nextAt:
			c.seq++
			cmd := kv.Command{Client: uint64(c.id + 1), Seq: c.seq, Key: fmt.Sprintf("k%d", s.rng.IntN(s.cfg.Keys))}
			switch r := s.rng.Float64(); {
			case r < 0.4:
				cmd.Op = kv.Get
			case r < 0.7:
				cmd.Op, cmd.Value = kv.Put, fmt.Sprintf("%d.%d", c.id+1, c.seq)
			default:
				cmd.Op, cmd.Value = kv.Append, fmt.Sprintf("%d.%d;", c.id+1, c.seq)
			}
			c.cur = &call{cmd: cmd, start: s.now}
			s.clientSend(c)
		case c.cur != nil && s.now >= c.cur.deadline:
			s.stats.Retries++
			c.guess = raft.NodeID(1 + s.rng.IntN(s.cfg.Nodes)) // maybe the node is gone: try another
			s.clientSend(c)
		}
	}
}

func (s *Sim) clientSend(c *client) {
	c.cur.deadline = s.now + s.cfg.ClientTimeout
	s.stats.Sent++
	if s.rng.Float64() < s.cfg.DropRate {
		s.stats.Dropped++
		return
	}
	s.enqueue(envelope{kind: toServer, req: request{client: c.id, node: c.guess, cmd: c.cur.cmd}})
}

func (s *Sim) reply(r response) {
	s.stats.Sent++
	if s.rng.Float64() < s.cfg.DropRate {
		s.stats.Dropped++
		return
	}
	copies := 1
	if s.rng.Float64() < s.cfg.DupRate {
		copies = 2
	}
	for range copies {
		s.enqueue(envelope{kind: toClient, resp: r})
	}
}

// serve is the node side of a client request.
func (s *Sim) serve(req request) {
	n := s.nodes[req.node-1]
	if n.r == nil {
		return // crashed: the client times out
	}
	no := response{client: req.client, seq: req.cmd.Seq, hint: n.r.Leader()}
	if n.r.Role() != raft.Leader {
		s.reply(no)
		return
	}
	if req.cmd.Op == kv.Get && !s.cfg.ReadsThroughLog {
		n.readCtx++
		if err := n.r.ReadIndex(n.readCtx); err != nil {
			s.reply(no) // not leader, or a new leader not ready yet: the client backs off and retries
			return
		}
		n.reads[n.readCtx] = pendingGet{client: req.client, seq: req.cmd.Seq, key: req.cmd.Key}
		s.process(n)
		return
	}
	idx, term, err := n.r.Propose(req.cmd.Encode())
	if err != nil {
		s.reply(no)
		return
	}
	if old, ok := n.waiters[idx]; ok { // that index was ours in an earlier term and got overwritten
		s.reply(response{client: old.client, seq: old.seq})
	}
	n.waiters[idx] = waiter{client: req.client, seq: req.cmd.Seq, term: term}
	s.process(n)
}

// applyCommand runs a committed command on a node's store and answers whoever waits for it.
func (s *Sim) applyCommand(n *node, e raft.Entry) {
	cmd, err := kv.Decode(e.Data)
	if err != nil {
		s.fail(fmt.Sprintf("node %d: undecodable entry %d: %v", n.id, e.Index, err))
		return
	}
	res := n.store.Apply(cmd)
	w, ok := n.waiters[e.Index]
	if !ok {
		return
	}
	delete(n.waiters, e.Index)
	if w.term == e.Term && w.client == int(cmd.Client)-1 && w.seq == cmd.Seq {
		s.reply(response{client: w.client, seq: w.seq, ok: true, value: res.Value})
	} else {
		// Someone else's entry won this index: our proposal was lost with a leadership change.
		s.reply(response{client: w.client, seq: w.seq, hint: n.r.Leader()})
	}
}

// serveReads answers the gets whose read index the store has reached.
func (s *Sim) serveReads(n *node) {
	keep := n.ready[:0]
	for _, g := range n.ready {
		if g.index > n.applied {
			keep = append(keep, g)
			continue
		}
		s.stats.Reads++
		s.reply(response{client: g.client, seq: g.seq, ok: true, value: n.store.Read(g.key)})
	}
	n.ready = keep
}

func (s *Sim) clientReceive(r response) {
	c := s.clients[r.client]
	if c.cur == nil || r.seq != c.cur.cmd.Seq {
		return // a late or duplicated answer to something already finished
	}
	if !r.ok {
		if r.hint != raft.None && r.hint != c.guess {
			c.guess = r.hint
			s.stats.Retries++
			s.clientSend(c)
		} else {
			c.cur.deadline = min(c.cur.deadline, s.now+20) // no leader known yet: back off a little
		}
		return
	}
	s.history = append(s.history, historyOp{client: c.id, cmd: c.cur.cmd, call: c.cur.start, ret: s.now,
		output: r.value, outputKnown: true})
	s.stats.Ops++
	c.cur = nil
	c.nextAt = s.now + s.rng.Int64N(10)
}

// ---- linearizability --------------------------------------------------------------------------

func (s *Sim) checkLinearizable() {
	ops := make([]porcupine.Operation, 0, len(s.history)+len(s.clients))
	for _, h := range s.history {
		ops = append(ops, porcupine.Operation{ClientId: h.client, Call: h.call, Return: h.ret,
			Input: kv.Input{Op: h.cmd.Op, Key: h.cmd.Key, Value: h.cmd.Value}, Output: kv.Output{Value: h.output, Known: true}})
	}
	// A write still in flight may have been applied or not; it may take effect any time after it
	// started. A read in flight has no effect, so it is left out.
	for _, c := range s.clients {
		if c.cur != nil && c.cur.cmd.Op != kv.Get {
			cmd := c.cur.cmd
			ops = append(ops, porcupine.Operation{ClientId: c.id, Call: c.cur.start, Return: math.MaxInt64,
				Input: kv.Input{Op: cmd.Op, Key: cmd.Key, Value: cmd.Value}, Output: kv.Output{}})
		}
	}
	res, info := porcupine.CheckOperationsVerbose(kv.Model, ops, time.Duration(s.cfg.CheckTimeout)*time.Millisecond)
	switch res {
	case porcupine.Illegal:
		msg := fmt.Sprintf("not linearizable: %d operations by %d clients have no valid single-copy order", len(ops), len(s.clients))
		if s.cfg.VisualizePath != "" && porcupine.VisualizePath(kv.Model, info, s.cfg.VisualizePath) == nil {
			msg += " (see " + s.cfg.VisualizePath + ")"
		}
		s.fail(msg)
	case porcupine.Unknown:
		s.inconclusive = true
	}
}
