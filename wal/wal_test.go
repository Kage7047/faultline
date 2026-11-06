package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/Kage7047/faultline/raft"
)

func ents(from raft.Index, term raft.Term, n int) []raft.Entry {
	var es []raft.Entry
	for i := range n {
		idx := from + raft.Index(i)
		es = append(es, raft.Entry{Index: idx, Term: term, Data: []byte(fmt.Sprintf("e%d@%d", idx, term))})
	}
	return es
}

func mustOpen(t *testing.T, dir string) *WAL {
	t.Helper()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	return w
}

func TestRoundTripAndOverwrite(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir)
	noop := raft.Entry{Index: 1, Term: 1} // nil data must stay nil
	if err := w.Save(raft.HardState{Term: 1, Vote: 2}, true, []raft.Entry{noop}); err != nil {
		t.Fatal(err)
	}
	w.Save(raft.HardState{}, false, ents(2, 1, 4))                           // 2..5
	w.Save(raft.HardState{Term: 2, Vote: 3, Commit: 3}, true, ents(4, 2, 2)) // overwrite 4, 5
	w.Close()

	hs, snap, got := mustOpen(t, dir).State()
	want := append([]raft.Entry{noop}, append(ents(2, 1, 2), ents(4, 2, 2)...)...)
	if hs != (raft.HardState{Term: 2, Vote: 3, Commit: 3}) || snap.Index != 0 || !reflect.DeepEqual(got, want) {
		t.Fatalf("reopened: hs %+v snap %d entries %v", hs, snap.Index, got)
	}
	if got[0].Data != nil {
		t.Fatal("a no-op came back with non-nil data")
	}
}

// A crash can cut the last write anywhere. Every cut must reopen to the state before that write.
func TestTornTailAtEveryOffset(t *testing.T) {
	src := t.TempDir()
	w := mustOpen(t, src)
	w.Save(raft.HardState{Term: 1}, true, ents(1, 1, 3))
	before, _ := os.Stat(filepath.Join(src, logName))
	w.Save(raft.HardState{Term: 1, Commit: 3}, true, ents(4, 1, 2))
	w.Close()
	full, _ := os.ReadFile(filepath.Join(src, logName))

	for cut := before.Size() + 1; cut < int64(len(full)); cut++ {
		dir := t.TempDir()
		os.WriteFile(filepath.Join(dir, logName), full[:cut], 0o644)
		r := mustOpen(t, dir)
		hs, _, es := r.State()
		// The last batch was 3 records; whole ones survive, the torn one is cut off.
		if len(es) < 3 || len(es) > 5 || hs.Term != 1 {
			t.Fatalf("cut at %d: %d entries, hs %+v", cut, len(es), hs)
		}
		if torn := !onBoundary(full, cut); (r.Repaired > 0) != torn {
			t.Fatalf("cut at %d: torn=%v but repaired %d bytes", cut, torn, r.Repaired)
		}
		// and it is writable again: the next batch lands after the repaired tail
		r.Save(raft.HardState{Term: 9}, true, nil)
		r.Close()
		if hs, _, _ := mustOpen(t, dir).State(); hs.Term != 9 {
			t.Fatalf("cut at %d: write after repair lost", cut)
		}
	}
}

// onBoundary reports whether cut falls exactly between two records.
func onBoundary(b []byte, cut int64) bool {
	var off int64
	for off < cut {
		off += header + int64(uint32(b[off])<<24|uint32(b[off+1])<<16|uint32(b[off+2])<<8|uint32(b[off+3]))
	}
	return off == cut
}

func TestCorruptRecordIsCutOff(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir)
	w.Save(raft.HardState{Term: 1}, true, ents(1, 1, 3))
	w.Close()
	p := filepath.Join(dir, logName)
	b, _ := os.ReadFile(p)
	b[len(b)-2] ^= 0xff // flip bits in the last entry's data
	os.WriteFile(p, b, 0o644)
	r := mustOpen(t, dir)
	if _, _, es := r.State(); len(es) != 2 || r.Repaired == 0 {
		t.Fatalf("%d entries, repaired %d bytes; want 2 entries and a repair", len(es), r.Repaired)
	}
}

func TestCompactAndReopen(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir)
	w.Save(raft.HardState{Term: 3, Commit: 8}, true, ents(1, 3, 10))
	if err := w.Compact(6, []byte("state@6")); err != nil {
		t.Fatal(err)
	}
	w.Save(raft.HardState{}, false, ents(11, 3, 1)) // appends keep working after the rewrite
	w.Close()
	hs, snap, es := mustOpen(t, dir).State()
	if hs.Commit != 8 || snap.Index != 6 || snap.Term != 3 || string(snap.Data) != "state@6" {
		t.Fatalf("hs %+v snap %+v", hs, snap)
	}
	if len(es) != 5 || es[0].Index != 7 || es[4].Index != 11 {
		t.Fatalf("entries after the snapshot: %v", es)
	}
}

// A crash after the new snapshot is in place but before the log is rewritten leaves an old log
// that still holds the entries the snapshot covers. They must be skipped.
func TestCrashBetweenSnapshotAndLogRewrite(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir)
	w.Save(raft.HardState{Term: 1, Commit: 5}, true, ents(1, 1, 8))
	w.Close()
	oldLog, _ := os.ReadFile(filepath.Join(dir, logName))
	w = mustOpen(t, dir)
	w.Compact(5, []byte("s"))
	w.Close()
	os.WriteFile(filepath.Join(dir, logName), oldLog, 0o644) // the rewrite never happened
	_, snap, es := mustOpen(t, dir).State()
	if snap.Index != 5 || len(es) != 3 || es[0].Index != 6 {
		t.Fatalf("snap %d entries %v", snap.Index, es)
	}
}

func TestInstallSnapshotReplacesEverything(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir)
	w.Save(raft.HardState{Term: 1}, true, ents(1, 1, 4))
	w.InstallSnapshot(raft.Snapshot{Index: 20, Term: 5, Data: []byte("leader's")})
	w.Save(raft.HardState{Term: 5, Commit: 21}, true, ents(21, 5, 1))
	w.Close()
	hs, snap, es := mustOpen(t, dir).State()
	if snap.Index != 20 || len(es) != 1 || es[0].Index != 21 || hs.Commit != 21 {
		t.Fatalf("hs %+v snap %d entries %v", hs, snap.Index, es)
	}
}
