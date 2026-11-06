package server

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Kage7047/faultline/raft"
)

func TestTransportAuthentication(t *testing.T) {
	cluster := map[raft.NodeID]string{1: "a:1", 2: "b:2", 3: "c:3"}
	var got []raft.Message
	tr := newTransport(1, cluster, []byte("s3cret"), func(m raft.Message) { got = append(got, m) }, slog.Default())
	body, _ := json.Marshal([]raft.Message{
		{Type: raft.MsgVote, From: 2, To: 1, Term: 5},
		{Type: raft.MsgVote, From: 9, To: 1, Term: 5}, // not a member
		{Type: raft.MsgVote, From: 3, To: 2, Term: 5}, // not for us
	})
	post := func(sig string) int {
		req := httptest.NewRequest(http.MethodPost, "/raft", strings.NewReader(string(body)))
		if sig != "" {
			req.Header.Set(HeaderAuth, sig)
		}
		rec := httptest.NewRecorder()
		tr.handleRaft(rec, req)
		return rec.Code
	}
	if code := post(""); code != http.StatusUnauthorized || len(got) != 0 {
		t.Fatalf("unsigned batch: %d, delivered %d", code, len(got))
	}
	forged := newTransport(2, cluster, []byte("guess"), nil, slog.Default()).sign(body)
	if code := post(forged); code != http.StatusUnauthorized || len(got) != 0 {
		t.Fatalf("batch signed with the wrong secret: %d, delivered %d", code, len(got))
	}
	if code := post(tr.sign(body)); code != http.StatusNoContent {
		t.Fatalf("signed batch: %d", code)
	}
	if len(got) != 1 || got[0].From != 2 {
		t.Fatalf("delivered %v, want only the message from member 2 to us", got)
	}
}
