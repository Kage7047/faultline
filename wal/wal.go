// Package wal is a node's durable storage: an append-only write-ahead log of hard state and
// entries, plus the latest snapshot in its own file.
//
// Each record is [length uint32][crc32c uint32][type byte][payload]. Save writes a batch with one
// write and one fsync before returning, so Raft's rule holds: nothing is sent that depends on
// state not yet on disk. A crash can tear the last record; Open finds the first record that is
// short or fails its checksum, truncates the file there and reports how many bytes it dropped.
// Records are only ever torn at the tail, because each batch is synced before the next starts.
//
// A snapshot is written to a temporary file, synced, renamed over the old one and the directory
// synced, so it is either the old snapshot or the new one, never half of each. The log is then
// rewritten the same way without the entries the snapshot covers. A crash between the two leaves
// a new snapshot and an old log, which Open handles by skipping entries the snapshot covers.
package wal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/Kage7047/faultline/raft"
)

const (
	recHardState byte = 1
	recEntry     byte = 2

	logName  = "wal"
	snapName = "snap"
	header   = 9 // length + crc + type
	// maxRecord guards against reading a garbage length as a huge allocation.
	maxRecord = 64 << 20
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// WAL is one node's storage. It is not safe for concurrent use.
type WAL struct {
	dir     string
	f       *os.File
	hs      raft.HardState
	snap    raft.Snapshot
	entries []raft.Entry // entries[0].Index == snap.Index+1
	// Repaired is how many bytes of a torn tail Open cut off (0 if the log was clean).
	Repaired int64
	// NoSync skips fsync (for tests only: it makes a crash lose acknowledged data).
	NoSync bool
}

// Open loads (or creates) the storage in dir.
func Open(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	w := &WAL{dir: dir}
	if err := w.loadSnapshot(); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, logName), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	w.f = f
	if err := w.replay(); err != nil {
		f.Close()
		return nil, err
	}
	return w, nil
}

// State returns what was persisted: hard state, snapshot, and the entries after the snapshot.
func (w *WAL) State() (raft.HardState, raft.Snapshot, []raft.Entry) {
	return w.hs, w.snap, append([]raft.Entry(nil), w.entries...)
}

// Save durably records a new hard state and entries. Entries overwrite any stored entries at or
// after entries[0].Index, as Raft's Ready requires.
func (w *WAL) Save(hs raft.HardState, mustSync bool, entries []raft.Entry) error {
	if !mustSync && len(entries) == 0 {
		return nil
	}
	var buf bytes.Buffer
	if mustSync {
		appendRecord(&buf, recHardState, encodeHardState(hs))
	}
	for _, e := range entries {
		appendRecord(&buf, recEntry, encodeEntry(e))
	}
	if _, err := w.f.Write(buf.Bytes()); err != nil {
		return err
	}
	if err := w.sync(w.f); err != nil {
		return err
	}
	if mustSync {
		w.hs = hs
	}
	for _, e := range entries {
		w.put(e)
	}
	return nil
}

// Compact saves a snapshot of the state machine as of index (which must be stored) and drops the
// entries it covers.
func (w *WAL) Compact(index raft.Index, data []byte) error {
	if index <= w.snap.Index || index > w.snap.Index+raft.Index(len(w.entries)) {
		return fmt.Errorf("wal: cannot compact at %d (have %d..%d)", index, w.snap.Index+1,
			w.snap.Index+raft.Index(len(w.entries)))
	}
	snap := raft.Snapshot{Index: index, Term: w.entries[index-w.snap.Index-1].Term, Data: data}
	return w.installSnapshot(snap, w.entries[index-w.snap.Index:])
}

// InstallSnapshot replaces everything with a snapshot received from the leader.
func (w *WAL) InstallSnapshot(snap raft.Snapshot) error { return w.installSnapshot(snap, nil) }

func (w *WAL) installSnapshot(snap raft.Snapshot, keep []raft.Entry) error {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, uint64(snap.Index))
	binary.Write(&buf, binary.BigEndian, uint64(snap.Term))
	buf.Write(snap.Data)
	var file bytes.Buffer
	appendRecord(&file, 0, buf.Bytes())
	if err := w.atomicWrite(snapName, file.Bytes()); err != nil {
		return err
	}
	w.snap = snap
	w.entries = append([]raft.Entry(nil), keep...)

	// Rewrite the log without the covered entries.
	var log bytes.Buffer
	appendRecord(&log, recHardState, encodeHardState(w.hs))
	for _, e := range w.entries {
		appendRecord(&log, recEntry, encodeEntry(e))
	}
	if err := w.atomicWrite(logName, log.Bytes()); err != nil {
		return err
	}
	w.f.Close()
	f, err := os.OpenFile(filepath.Join(w.dir, logName), os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	w.f = f
	return nil
}

// Close closes the log file.
func (w *WAL) Close() error { return w.f.Close() }

func (w *WAL) put(e raft.Entry) {
	if e.Index <= w.snap.Index {
		return // covered by the snapshot (an old log replayed after a newer snapshot)
	}
	off := int(e.Index - w.snap.Index - 1)
	if off > len(w.entries) {
		return // a gap can only come from a log older than the snapshot; the snapshot wins
	}
	w.entries = append(w.entries[:off], e)
}

func (w *WAL) replay() error {
	r := bufio.NewReader(w.f)
	var good int64
	for {
		typ, payload, n, err := readRecord(r)
		if err == io.EOF {
			break
		}
		if err != nil {
			// A torn or corrupt tail: everything from here on is lost, cut it off.
			end, _ := w.f.Seek(0, io.SeekEnd)
			w.Repaired = end - good
			if err := w.f.Truncate(good); err != nil {
				return err
			}
			if err := w.sync(w.f); err != nil {
				return err
			}
			break
		}
		good += n
		switch typ {
		case recHardState:
			hs, err := decodeHardState(payload)
			if err != nil {
				return err
			}
			w.hs = hs
		case recEntry:
			e, err := decodeEntry(payload)
			if err != nil {
				return err
			}
			w.put(e)
		default:
			return fmt.Errorf("wal: unknown record type %d", typ)
		}
	}
	_, err := w.f.Seek(good, io.SeekStart)
	return err
}

func (w *WAL) loadSnapshot() error {
	b, err := os.ReadFile(filepath.Join(w.dir, snapName))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	_, payload, _, err := readRecord(bufio.NewReader(bytes.NewReader(b)))
	if err != nil || len(payload) < 16 {
		// The snapshot is written atomically, so this is real corruption, not a torn write.
		return fmt.Errorf("wal: snapshot file is corrupt: %v", err)
	}
	w.snap = raft.Snapshot{
		Index: raft.Index(binary.BigEndian.Uint64(payload[0:])),
		Term:  raft.Term(binary.BigEndian.Uint64(payload[8:])),
		Data:  payload[16:],
	}
	return nil
}

// atomicWrite replaces dir/name with data: temp file, fsync, rename, fsync the directory.
func (w *WAL) atomicWrite(name string, data []byte) error {
	tmp := filepath.Join(w.dir, name+".tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := w.sync(f); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(w.dir, name)); err != nil {
		return err
	}
	d, err := os.Open(w.dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return w.sync(d)
}

func (w *WAL) sync(f *os.File) error {
	if w.NoSync {
		return nil
	}
	return f.Sync()
}

// ---- encoding ---------------------------------------------------------------------------------

func appendRecord(buf *bytes.Buffer, typ byte, payload []byte) {
	var h [header]byte
	binary.BigEndian.PutUint32(h[0:], uint32(len(payload)))
	h[8] = typ
	crc := crc32.Update(crc32.Checksum(h[8:9], crcTable), crcTable, payload)
	binary.BigEndian.PutUint32(h[4:], crc)
	buf.Write(h[:])
	buf.Write(payload)
}

var errTorn = errors.New("wal: torn or corrupt record")

func readRecord(r *bufio.Reader) (typ byte, payload []byte, n int64, err error) {
	var h [header]byte
	k, err := io.ReadFull(r, h[:])
	if err == io.EOF {
		return 0, nil, 0, io.EOF
	}
	if err != nil || k < header {
		return 0, nil, 0, errTorn
	}
	size := binary.BigEndian.Uint32(h[0:])
	if size > maxRecord {
		return 0, nil, 0, errTorn
	}
	payload = make([]byte, size)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, 0, errTorn
	}
	crc := crc32.Update(crc32.Checksum(h[8:9], crcTable), crcTable, payload)
	if crc != binary.BigEndian.Uint32(h[4:]) {
		return 0, nil, 0, errTorn
	}
	return h[8], payload, int64(header) + int64(size), nil
}

func encodeHardState(hs raft.HardState) []byte {
	b := make([]byte, 24)
	binary.BigEndian.PutUint64(b[0:], uint64(hs.Term))
	binary.BigEndian.PutUint64(b[8:], uint64(hs.Vote))
	binary.BigEndian.PutUint64(b[16:], uint64(hs.Commit))
	return b
}

func decodeHardState(b []byte) (raft.HardState, error) {
	if len(b) != 24 {
		return raft.HardState{}, fmt.Errorf("wal: hard state record of %d bytes", len(b))
	}
	return raft.HardState{
		Term:   raft.Term(binary.BigEndian.Uint64(b[0:])),
		Vote:   raft.NodeID(binary.BigEndian.Uint64(b[8:])),
		Commit: raft.Index(binary.BigEndian.Uint64(b[16:])),
	}, nil
}

// An entry is index, term, a no-op flag, then the data.
func encodeEntry(e raft.Entry) []byte {
	b := make([]byte, 17, 17+len(e.Data))
	binary.BigEndian.PutUint64(b[0:], uint64(e.Index))
	binary.BigEndian.PutUint64(b[8:], uint64(e.Term))
	if e.Data == nil {
		b[16] = 1
	}
	return append(b, e.Data...)
}

func decodeEntry(b []byte) (raft.Entry, error) {
	if len(b) < 17 {
		return raft.Entry{}, fmt.Errorf("wal: entry record of %d bytes", len(b))
	}
	e := raft.Entry{Index: raft.Index(binary.BigEndian.Uint64(b[0:])), Term: raft.Term(binary.BigEndian.Uint64(b[8:]))}
	if b[16] == 0 {
		e.Data = append([]byte{}, b[17:]...)
	}
	return e, nil
}
