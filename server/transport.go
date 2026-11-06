package server

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/Kage7047/faultline/raft"
)

// transport sends raft messages to peers over HTTP. Each peer has a queue and one goroutine that
// posts batches. Sending never blocks the node: a full queue or a failed post drops messages,
// and Raft recovers from loss by retrying on the next heartbeat.
type transport struct {
	self    raft.NodeID
	peers   map[raft.NodeID]*peer
	recv    func(raft.Message)
	client  *http.Client
	log     *slog.Logger
	sent    atomic.Int64
	dropped atomic.Int64
}

type peer struct {
	id  raft.NodeID
	url string
	q   chan raft.Message
}

func newTransport(self raft.NodeID, cluster map[raft.NodeID]string, recv func(raft.Message), log *slog.Logger) *transport {
	t := &transport{
		self:   self,
		peers:  map[raft.NodeID]*peer{},
		recv:   recv,
		client: &http.Client{Timeout: 500 * time.Millisecond},
		log:    log,
	}
	for id, addr := range cluster {
		if id != self {
			t.peers[id] = &peer{id: id, url: "http://" + addr + "/raft", q: make(chan raft.Message, 1024)}
		}
	}
	return t
}

func (t *transport) start(ctx context.Context) {
	for _, p := range t.peers {
		go t.run(ctx, p)
	}
}

func (t *transport) send(msgs []raft.Message) {
	for _, m := range msgs {
		p, ok := t.peers[m.To]
		if !ok {
			continue
		}
		t.sent.Add(1)
		select {
		case p.q <- m:
		default:
			t.dropped.Add(1)
		}
	}
}

func (t *transport) run(ctx context.Context, p *peer) {
	for {
		var batch []raft.Message
		select {
		case <-ctx.Done():
			return
		case m := <-p.q:
			batch = append(batch, m)
		}
	more:
		for len(batch) < 128 {
			select {
			case m := <-p.q:
				batch = append(batch, m)
			default:
				break more
			}
		}
		if err := t.post(ctx, p, batch); err != nil {
			t.dropped.Add(int64(len(batch)))
			// The peer is probably down: wait a little instead of spinning on refused connections.
			select {
			case <-ctx.Done():
				return
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
}

func (t *transport) post(ctx context.Context, p *peer, batch []raft.Message) error {
	body, err := json.Marshal(batch)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.client.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return &httpError{resp.StatusCode}
	}
	return nil
}

type httpError struct{ code int }

func (e *httpError) Error() string { return http.StatusText(e.code) }

// handleRaft receives a batch from a peer.
func (t *transport) handleRaft(w http.ResponseWriter, r *http.Request) {
	var batch []raft.Message
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 256<<20)).Decode(&batch); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	for _, m := range batch {
		if m.To == t.self {
			t.recv(m)
		}
	}
	w.WriteHeader(http.StatusNoContent)
}
