package raft

// raftLog is the in-memory log. entries[i].Index == i+1 (no compaction yet).
// Entries up to stable are on disk; the rest are handed out by Ready to be persisted.
type raftLog struct {
	entries []Entry
	stable  Index
}

func (l *raftLog) lastIndex() Index { return Index(len(l.entries)) }

func (l *raftLog) lastTerm() Term { return l.term(l.lastIndex()) }

// term returns the term at i, or 0 for index 0 and for indexes past the end.
func (l *raftLog) term(i Index) Term {
	if i == 0 || i > l.lastIndex() {
		return 0
	}
	return l.entries[i-1].Term
}

// slice returns copies of the entries in [lo, hi].
func (l *raftLog) slice(lo, hi Index) []Entry {
	if lo < 1 {
		lo = 1
	}
	if hi > l.lastIndex() {
		hi = l.lastIndex()
	}
	if lo > hi {
		return nil
	}
	return cloneEntries(l.entries[lo-1 : hi])
}

func (l *raftLog) append(e Entry) {
	l.entries = append(l.entries, cloneEntry(e))
}

// truncateAndAppend replaces everything from es[0].Index on with es.
func (l *raftLog) truncateAndAppend(es []Entry) {
	if len(es) == 0 {
		return
	}
	first := es[0].Index
	l.entries = append(l.entries[:first-1], cloneEntries(es)...)
	if l.stable >= first {
		l.stable = first - 1
	}
}

func (l *raftLog) unstable() []Entry {
	return cloneEntries(l.entries[l.stable:])
}

// isUpToDate is §5.4.1: a log is more up to date if its last term is higher, or the last terms
// are equal and it is at least as long.
func (l *raftLog) isUpToDate(lastIndex Index, lastTerm Term) bool {
	return lastTerm > l.lastTerm() || (lastTerm == l.lastTerm() && lastIndex >= l.lastIndex())
}

// firstIndexOfTerm walks back from i to the first index that has the same term as i.
func (l *raftLog) firstIndexOfTerm(i Index) Index {
	t := l.term(i)
	for i > 1 && l.term(i-1) == t {
		i--
	}
	return i
}

// lastIndexOfTerm is the last index holding term t, or 0 if the log has none.
func (l *raftLog) lastIndexOfTerm(t Term) Index {
	for i := l.lastIndex(); i > 0; i-- {
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
