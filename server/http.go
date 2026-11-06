package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/Kage7047/faultline/kv"
	"github.com/Kage7047/faultline/raft"
)

// Headers a client sends for exactly-once writes. Without them a write is anonymous: it still
// works, but a retry of it may apply twice.
const (
	HeaderClient = "Faultline-Client"
	HeaderSeq    = "Faultline-Seq"
	HeaderLeader = "Faultline-Leader"
)

// how long a handler waits for the node before telling the client to retry
const requestTimeout = 3 * time.Second

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /raft", s.tr.handleRaft)
	mux.HandleFunc("GET /kv/{key}", s.handleGet)
	mux.HandleFunc("PUT /kv/{key}", s.handleWrite)
	mux.HandleFunc("POST /kv/{key}", s.handleWrite) // ?op=append
	mux.HandleFunc("GET /status", s.handleStatus)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	return mux
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	rd := &read{key: r.PathValue("key"), done: make(chan result, 1)}
	s.call(w, r, func(ctx context.Context) bool {
		select {
		case s.readc <- rd:
			return true
		case <-ctx.Done():
			return false
		}
	}, rd.done)
}

func (s *Server) handleWrite(w http.ResponseWriter, r *http.Request) {
	op := kv.Put
	if r.Method == http.MethodPost {
		if r.URL.Query().Get("op") != "append" {
			http.Error(w, "POST needs ?op=append", http.StatusBadRequest)
			return
		}
		op = kv.Append
	}
	value, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	cmd := kv.Command{Op: op, Key: r.PathValue("key"), Value: string(value)}
	if c := r.Header.Get(HeaderClient); c != "" {
		cmd.Client, err = strconv.ParseUint(c, 10, 64)
		if err == nil {
			cmd.Seq, err = strconv.ParseUint(r.Header.Get(HeaderSeq), 10, 64)
		}
		if err != nil || cmd.Client == 0 || cmd.Seq == 0 {
			http.Error(w, "bad "+HeaderClient+"/"+HeaderSeq, http.StatusBadRequest)
			return
		}
	}
	p := &proposal{cmd: cmd, done: make(chan result, 1)}
	s.call(w, r, func(ctx context.Context) bool {
		select {
		case s.props <- p:
			return true
		case <-ctx.Done():
			return false
		}
	}, p.done)
}

// call hands a request to the node goroutine and writes the answer: 200 with the value, a 307 to
// the leader, or a 503 meaning "retry, maybe elsewhere".
func (s *Server) call(w http.ResponseWriter, r *http.Request, submit func(context.Context) bool, done chan result) {
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	var res result
	if !submit(ctx) {
		res.err = errRetry
	} else {
		select {
		case res = <-done:
		case <-ctx.Done():
			res.err = errRetry // the node may still apply it; a retry with the same seq is safe
		}
	}
	var nl errNotLeader
	switch {
	case res.err == nil:
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		io.WriteString(w, res.value)
	case errors.As(res.err, &nl) && nl.lead != raft.None && nl.lead != s.cfg.ID:
		w.Header().Set(HeaderLeader, strconv.FormatUint(uint64(nl.lead), 10))
		target := *r.URL
		target.Scheme, target.Host = "http", s.cfg.Cluster[nl.lead]
		http.Redirect(w, r, target.String(), http.StatusTemporaryRedirect)
	default:
		w.Header().Set("Retry-After", "1")
		http.Error(w, res.err.Error(), http.StatusServiceUnavailable)
	}
}

func (s *Server) currentStatus(ctx context.Context, hash bool) (Status, bool) {
	c := make(chan Status, 1)
	select {
	case s.statusc <- statusReq{hash: hash, resp: c}:
	case <-ctx.Done():
		return Status{}, false
	}
	select {
	case st := <-c:
		return st, true
	case <-ctx.Done():
		return Status{}, false
	}
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	st, ok := s.currentStatus(r.Context(), r.URL.Query().Get("hash") == "1")
	if !ok {
		http.Error(w, "busy", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(st)
}

// handleMetrics serves the Prometheus text format, without pulling in a client library.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	st, ok := s.currentStatus(r.Context(), false)
	if !ok {
		http.Error(w, "busy", http.StatusServiceUnavailable)
		return
	}
	leader := 0
	if st.Role == raft.Leader.String() {
		leader = 1
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	for _, m := range []struct {
		name, kind, help string
		v                int64
	}{
		{"faultline_term", "gauge", "Current raft term.", int64(st.Term)},
		{"faultline_is_leader", "gauge", "1 if this node leads.", int64(leader)},
		{"faultline_commit_index", "gauge", "Highest index known committed.", int64(st.Commit)},
		{"faultline_applied_index", "gauge", "Highest index applied to the store.", int64(st.Applied)},
		{"faultline_snapshot_index", "gauge", "Index of the latest snapshot.", int64(st.SnapshotIndex)},
		{"faultline_elections_won_total", "counter", "Times this process became leader.", int64(st.Counters.Elections)},
		{"faultline_proposals_total", "counter", "Writes proposed by this node.", int64(st.Counters.Proposals)},
		{"faultline_reads_total", "counter", "ReadIndex reads served.", int64(st.Counters.Reads)},
		{"faultline_compactions_total", "counter", "Local snapshots taken.", int64(st.Counters.Compactions)},
		{"faultline_snapshots_installed_total", "counter", "Snapshots installed from a leader.", int64(st.Counters.Installs)},
		{"faultline_wal_repaired_bytes", "gauge", "Bytes of torn WAL tail cut off at startup.", st.Counters.WALRepaired},
		{"faultline_messages_sent_total", "counter", "Raft messages handed to the transport.", st.Counters.Sent},
		{"faultline_messages_dropped_total", "counter", "Raft messages dropped (queue full or peer down).", st.Counters.Dropped},
	} {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n%s %d\n", m.name, m.help, m.name, m.kind, m.name, m.v)
	}
}
