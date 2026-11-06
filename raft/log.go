package raft

// raftLog is the in-memory log after the last snapshot. entries[i].Index == snapIndex+i+1.
// Everything at or before snapIndex lives only in the snapshot. Entries up to stable are on disk;
// the rest are handed out by Ready to be persisted.
type raftLog struct {
	snapIndex Index
	snapTerm  Term
	entries   []Entry
	stable    Index
}

func (l *raftLog) firstIndex() Index { return l.snapIndex + 1 }

func (l *raftLog) lastIndex() Index { return l.snapIndex + Index(len(l.entries)) }

func (l *raftLog) lastTerm() Term { return l.term(l.lastIndex()) }

// term returns the term at i: the snapshot's term at snapIndex, and 0 for index 0, for indexes
// past the end and for compacted indexes (callers check firstIndex before asking about those).
func (l *raftLog) term(i Index) Term {
	switch {
	case i == l.snapIndex:
		return l.snapTerm
	case i < l.snapIndex || i > l.lastIndex():
		return 0
	}
	return l.entries[i-l.snapIndex-1].Term
}

// slice returns copies of the entries in [lo, hi] that are still in the log.
func (l *raftLog) slice(lo, hi Index) []Entry {
	lo = max(lo, l.firstIndex())
	hi = min(hi, l.lastIndex())
	if lo > hi {
		return nil
	}
	return cloneEntries(l.entries[lo-l.snapIndex-1 : hi-l.snapIndex])
}

func (l *raftLog) append(e Entry) {
	l.entries = append(l.entries, cloneEntry(e))
}

// truncateAndAppend replaces everything from es[0].Index on with es. es[0].Index > snapIndex.
func (l *raftLog) truncateAndAppend(es []Entry) {
	if len(es) == 0 {
		return
	}
	first := es[0].Index
	l.entries = append(l.entries[:first-l.snapIndex-1], cloneEntries(es)...)
	if l.stable >= first {
		l.stable = first - 1
	}
}

func (l *raftLog) unstable() []Entry {
	return cloneEntries(l.entries[l.stable-l.snapIndex:])
}

// compact drops every entry up to and including i, which must be in the log.
func (l *raftLog) compact(i Index) {
	t := l.term(i)
	l.entries = append([]Entry(nil), l.entries[i-l.snapIndex:]...)
	l.snapIndex, l.snapTerm = i, t
	l.stable = max(l.stable, i)
}

// restore replaces the whole log with a snapshot's position.
func (l *raftLog) restore(s Snapshot) {
	*l = raftLog{snapIndex: s.Index, snapTerm: s.Term, stable: s.Index}
}

// isUpToDate is §5.4.1: a log is more up to date if its last term is higher, or the last terms
// are equal and it is at least as long.
func (l *raftLog) isUpToDate(lastIndex Index, lastTerm Term) bool {
	return lastTerm > l.lastTerm() || (lastTerm == l.lastTerm() && lastIndex >= l.lastIndex())
}

// firstIndexOfTerm walks back from i to the first index still in the log with the same term as i.
func (l *raftLog) firstIndexOfTerm(i Index) Index {
	t := l.term(i)
	for i > l.firstIndex() && l.term(i-1) == t {
		i--
	}
	return i
}

// lastIndexOfTerm is the last index holding term t, or 0 if the log (or its snapshot) has none.
func (l *raftLog) lastIndexOfTerm(t Term) Index {
	for i := l.lastIndex(); i >= l.snapIndex && i > 0; i-- {
		switch lt := l.term(i); {
		case lt == t:
			return i
		case lt < t:
			return 0
		}
	}
	return 0
}

func cloneEntry(e Entry) Entry {
	if e.Data != nil { // keep nil (a no-op) distinct from an empty proposal
		d := make([]byte, len(e.Data))
		copy(d, e.Data)
		e.Data = d
	}
	return e
}

func cloneEntries(es []Entry) []Entry {
	if len(es) == 0 {
		return nil
	}
	out := make([]Entry, len(es))
	for i, e := range es {
		out[i] = cloneEntry(e)
	}
	return out
}
