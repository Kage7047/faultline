// Package server runs one faultline node over a real network: the raft.Node and the key-value
// store, a write-ahead log on disk, an HTTP transport to the other nodes and an HTTP API for
// clients.
//
// One goroutine owns the node, the store and the log, and does everything in the order Raft
// requires: persist (with fsync), then send, then apply. HTTP handlers talk to it over channels.
// A storage error stops the server: a node that cannot persist must not keep voting.
package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"time"

	"github.com/Kage7047/faultline/kv"
	"github.com/Kage7047/faultline/raft"
	"github.com/Kage7047/faultline/wal"
)

// Config describes one node.
type Config struct {
	ID raft.NodeID
	// Cluster maps every node, this one included, to the host:port it serves raft and clients on.
	Cluster map[raft.NodeID]string
	DataDir string
	// Tick is one raft tick. Elections time out after 15 to 30 ticks, heartbeats go every 3.
	Tick time.Duration
	// SnapshotEvery: snapshot and compact after this many applied entries.
	SnapshotEvery int
	Log           *slog.Logger
	// Secret, if set, authenticates raft traffic: every batch carries an HMAC-SHA256 of its body
	// and batches without a valid one are refused. Every node needs the same secret.
	Secret []byte
	// Unsafe switches off raft safety rules. Only the chaos test's control sets it.
	Unsafe raft.Unsafe
}

// Server is one running node.
type Server struct {
	cfg   Config
	log   *slog.Logger
	wal   *wal.WAL
	node  *raft.Node
	store *kv.Store
	tr    *transport

	applied   raft.Index
	snapIndex raft.Index
	role      raft.Role
	lead      raft.NodeID

	inbox   chan raft.Message
	props   chan *proposal
	readc   chan *read
	statusc chan statusReq

	waiters map[raft.Index]*proposal
	readCtx uint64
	pending map[uint64]*read
	ready   []*read

	m counters
}

type result struct {
	value string
	err   error
}

// errNotLeader carries who this node thinks leads (raft.None if nobody).
type errNotLeader struct{ lead raft.NodeID }

func (e errNotLeader) Error() string { return fmt.Sprintf("not the leader (leader: %d)", e.lead) }

// errRetry means the outcome is unknown or not ready yet: the client should retry the same
// request, which the session table makes safe.
var errRetry = errors.New("retry")

type statusReq struct {
	hash bool // also hash the whole store (costly: only on request)
	resp chan Status
}

type proposal struct {
	cmd  kv.Command
	term raft.Term
	done chan result
}

type read struct {
	key   string
	index raft.Index
	done  chan result
}

// Status is what /status reports.
type Status struct {
	ID            raft.NodeID `json:"id"`
	Role          string      `json:"role"`
	Term          raft.Term   `json:"term"`
	Leader        raft.NodeID `json:"leader"`
	Commit        raft.Index  `json:"commit"`
	Applied       raft.Index  `json:"applied"`
	LastIndex     raft.Index  `json:"last_index"`
	SnapshotIndex raft.Index  `json:"snapshot_index"`
	// StateHash is a sha256 of the store's state, only computed for /status?hash=1: replicas
	// at the same Applied must agree on it.
	StateHash string   `json:"state_hash,omitempty"`
	Counters  counters `json:"counters"`
}

type counters struct {
	Elections   int   `json:"elections"` // times this node became leader since it started
	Proposals   int   `json:"proposals"`
	Reads       int   `json:"reads"`
	Compactions int   `json:"compactions"`
	Installs    int   `json:"snapshots_installed"`
	WALRepaired int64 `json:"wal_repaired_bytes"`
	Sent        int64 `json:"messages_sent"`
	Dropped     int64 `json:"messages_dropped"`
}

// New opens the node's storage and rebuilds its state: the snapshot first, then the committed
// entries after it are re-applied by the first Ready.
func New(cfg Config) (*Server, error) {
	if cfg.Tick == 0 {
		cfg.Tick = 10 * time.Millisecond
	}
	if cfg.SnapshotEvery == 0 {
		cfg.SnapshotEvery = 1000
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	if _, ok := cfg.Cluster[cfg.ID]; !ok {
		return nil, fmt.Errorf("node %d is not in the cluster", cfg.ID)
	}
	w, err := wal.Open(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	s := &Server{
		cfg:     cfg,
		log:     cfg.Log.With("node", cfg.ID),
		wal:     w,
		store:   kv.New(),
		inbox:   make(chan raft.Message, 4096),
		props:   make(chan *proposal),
		readc:   make(chan *read),
		statusc: make(chan statusReq),
		waiters: map[raft.Index]*proposal{},
		pending: map[uint64]*read{},
	}
	s.m.WALRepaired = w.Repaired
	if w.Repaired > 0 {
		s.log.Warn("wal: cut off a torn tail", "bytes", w.Repaired)
	}
	hs, snap, entries := w.State()
	if snap.Index > 0 {
		if err := s.store.Restore(snap.Data); err != nil {
			return nil, fmt.Errorf("restore snapshot at %d: %w", snap.Index, err)
		}
		s.applied, s.snapIndex = snap.Index, snap.Index
	}
	var peers []raft.NodeID
	for id := range cfg.Cluster {
		peers = append(peers, id)
	}
	s.node = raft.NewNodeFromSnapshot(raft.Config{
		ID:             cfg.ID,
		Peers:          peers,
		ElectionTicks:  15,
		HeartbeatTicks: 3,
		PreVote:        true,
		CheckQuorum:    true,
		Seed:           rand.Uint64(),
		Unsafe:         cfg.Unsafe,
	}, hs, snap, entries, s.applied)
	s.tr = newTransport(cfg.ID, cfg.Cluster, cfg.Secret, s.receive, s.log)
	s.log.Info("started", "term", hs.Term, "commit", hs.Commit, "snapshot", snap.Index, "entries", len(entries))
	return s, nil
}

// Run serves until ctx is cancelled or storage fails.
func (s *Server) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.cfg.Cluster[s.cfg.ID])
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: s.routes(), ReadHeaderTimeout: 5 * time.Second}
	go srv.Serve(ln)
	s.tr.start(ctx)
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		srv.Shutdown(shutdown)
		s.wal.Close()
	}()

	tick := time.NewTicker(s.cfg.Tick)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			s.failAll()
			return nil
		case <-tick.C:
			s.node.Tick()
		case m := <-s.inbox:
			s.node.Step(m)
			s.stepQueued(63)
		case p := <-s.props:
			s.propose(p)
		case r := <-s.readc:
			s.startRead(r)
		case req := <-s.statusc:
			req.resp <- s.status(req.hash)
		}
		if err := s.drain(); err != nil {
			s.log.Error("storage failed, stopping", "err", err)
			return err
		}
	}
}

// stepQueued feeds up to n more queued messages before the next Ready, so one fsync covers them.
func (s *Server) stepQueued(n int) {
	for range n {
		select {
		case m := <-s.inbox:
			s.node.Step(m)
		default:
			return
		}
	}
}

// receive is called by the transport for every inbound message. A full inbox drops the message,
// which Raft tolerates like any other loss.
func (s *Server) receive(m raft.Message) {
	select {
	case s.inbox <- m:
	default:
		s.tr.dropped.Add(1)
	}
}

// drain does what the node asked: persist, send, apply, answer.
func (s *Server) drain() error {
	for s.node.HasReady() {
		rd := s.node.Ready()
		if rd.Snapshot != nil {
			// Decode it before anything touches the disk: a snapshot that does not decode must
			// not be persisted, or every restart would fail on it.
			restored := kv.New()
			if err := restored.Restore(rd.Snapshot.Data); err != nil {
				return fmt.Errorf("snapshot at %d from the leader does not decode: %w", rd.Snapshot.Index, err)
			}
			if err := s.wal.InstallSnapshot(*rd.Snapshot); err != nil {
				return err
			}
			s.store = restored
			s.applied, s.snapIndex = rd.Snapshot.Index, rd.Snapshot.Index
			s.m.Installs++
			s.log.Info("installed snapshot", "index", rd.Snapshot.Index)
		}
		if err := s.wal.Save(rd.HardState, rd.MustSync, rd.Entries); err != nil {
			return err
		}
		s.tr.send(rd.Messages)
		for _, e := range rd.Committed {
			s.apply(e)
		}
		for _, rs := range rd.ReadStates {
			if r, ok := s.pending[rs.Ctx]; ok {
				delete(s.pending, rs.Ctx)
				r.index = rs.Index
				s.ready = append(s.ready, r)
			}
		}
		s.serveReads()
		s.observe(rd.SoftState)
		s.node.Advance(rd)
		if err := s.maybeCompact(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) observe(ss raft.SoftState) {
	if ss.Role == s.role && ss.Lead == s.lead {
		return
	}
	if s.role == raft.Leader && ss.Role != raft.Leader {
		s.failAll() // whatever we were waiting for may or may not commit under the next leader
	}
	if ss.Role == raft.Leader && s.role != raft.Leader {
		s.m.Elections++
		s.log.Info("became leader", "term", s.node.Term())
	}
	s.role, s.lead = ss.Role, ss.Lead
}

func (s *Server) propose(p *proposal) {
	idx, term, err := s.node.Propose(p.cmd.Encode())
	if err != nil {
		p.done <- result{err: errNotLeader{s.node.Leader()}}
		return
	}
	if old, ok := s.waiters[idx]; ok { // our earlier proposal at idx was overwritten
		old.done <- result{err: errRetry}
	}
	p.term = term
	s.waiters[idx] = p
	s.m.Proposals++
}

func (s *Server) startRead(r *read) {
	s.readCtx++
	switch err := s.node.ReadIndex(s.readCtx); {
	case errors.Is(err, raft.ErrNotLeader):
		r.done <- result{err: errNotLeader{s.node.Leader()}}
	case err != nil:
		r.done <- result{err: errRetry}
	default:
		s.pending[s.readCtx] = r
	}
}

func (s *Server) apply(e raft.Entry) {
	s.applied = e.Index
	w := s.waiters[e.Index]
	delete(s.waiters, e.Index)
	if e.Data == nil {
		if w != nil {
			w.done <- result{err: errRetry}
		}
		return
	}
	cmd, err := kv.Decode(e.Data)
	if err != nil {
		s.log.Error("undecodable entry", "index", e.Index, "err", err)
		return
	}
	res := s.store.Apply(cmd)
	if w == nil {
		return
	}
	if w.term == e.Term && w.cmd.Client == cmd.Client && w.cmd.Seq == cmd.Seq {
		w.done <- result{value: res.Value}
	} else {
		w.done <- result{err: errRetry}
	}
}

func (s *Server) serveReads() {
	keep := s.ready[:0]
	for _, r := range s.ready {
		if r.index > s.applied {
			keep = append(keep, r)
			continue
		}
		s.m.Reads++
		r.done <- result{value: s.store.Read(r.key)}
	}
	s.ready = keep
}

func (s *Server) failAll() {
	for idx, w := range s.waiters {
		w.done <- result{err: errRetry}
		delete(s.waiters, idx)
	}
	for ctx, r := range s.pending {
		r.done <- result{err: errRetry}
		delete(s.pending, ctx)
	}
	for _, r := range s.ready {
		r.done <- result{err: errRetry}
	}
	s.ready = nil
}

// maybeCompact snapshots the store once enough was applied. The snapshot is on disk before the
// node drops the entries it covers.
func (s *Server) maybeCompact() error {
	if s.applied < s.snapIndex+raft.Index(s.cfg.SnapshotEvery) {
		return nil
	}
	data := s.store.Snapshot()
	if err := s.wal.Compact(s.applied, data); err != nil {
		return err
	}
	if err := s.node.Compact(s.applied, data); err != nil {
		return err
	}
	s.snapIndex = s.applied
	s.m.Compactions++
	return nil
}

func (s *Server) status(hash bool) Status {
	c := s.m
	c.Sent, c.Dropped = s.tr.sent.Load(), s.tr.dropped.Load()
	st := Status{
		ID:            s.cfg.ID,
		Role:          s.node.Role().String(),
		Term:          s.node.Term(),
		Leader:        s.node.Leader(),
		Commit:        s.node.Commit(),
		Applied:       s.applied,
		LastIndex:     s.node.LastIndex(),
		SnapshotIndex: s.snapIndex,
		Counters:      c,
	}
	if hash {
		sum := sha256.Sum256(s.store.Snapshot())
		st.StateHash = hex.EncodeToString(sum[:8])
	}
	return st
}
