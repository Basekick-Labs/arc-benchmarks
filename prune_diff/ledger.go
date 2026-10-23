package main

import (
	"sync"
	"time"
)

// row is one generated row. seq is globally unique, so count and sum(seq)
// over a set identify it exactly.
type row struct {
	seq  int64
	tsUs int64 // row timestamp, microseconds since the epoch
	host int
	// sentAt is when the write carrying the row was sent; ackedAt when it
	// returned 204 (zero while in flight or after a failure).
	sentAt  time.Time
	ackedAt time.Time
}

// ledger records every row the generator sent, per measurement. It is the
// truth the judge compares Arc against.
type ledger struct {
	mu    sync.RWMutex
	rows  map[string][]*row
	dirty map[string]string // measurement -> why its rows cannot be judged
}

func newLedger() *ledger {
	return &ledger{rows: map[string][]*row{}, dirty: map[string]string{}}
}

func (l *ledger) sent(m string, rs []*row) {
	l.mu.Lock()
	l.rows[m] = append(l.rows[m], rs...)
	l.mu.Unlock()
}

func (l *ledger) acked(rs []*row, at time.Time) {
	l.mu.Lock()
	for _, r := range rs {
		r.ackedAt = at
	}
	l.mu.Unlock()
}

// failed marks a measurement whose write failed: its rows may or may not be
// in Arc, so nothing about it is judged from then on.
func (l *ledger) failed(m, why string) {
	l.mu.Lock()
	if _, ok := l.dirty[m]; !ok {
		l.dirty[m] = why
	}
	l.mu.Unlock()
}

func (l *ledger) isDirty(m string) (string, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	why, ok := l.dirty[m]
	return why, ok
}

// match selects the rows a query's predicate selects.
type match func(r *row) bool

// tally is a row set's identity.
type tally struct {
	count int64
	sum   int64
}

func (t *tally) add(r *row) { t.count++; t.sum += r.seq }

// ackedBy reports rows acked at or before cutoff: certainly flushed by a
// request sent at cutoff + the visibility lag.
func (l *ledger) ackedBy(m string, w match, cutoff time.Time) tally {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var t tally
	for _, r := range l.rows[m] {
		if !r.ackedAt.IsZero() && !r.ackedAt.After(cutoff) && w(r) {
			t.add(r)
		}
	}
	return t
}

// sentBy reports every row sent at or before cutoff, acked or not: the most
// a query answered by then can hold.
func (l *ledger) sentBy(m string, w match, cutoff time.Time) tally {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var t tally
	for _, r := range l.rows[m] {
		if !r.sentAt.After(cutoff) && w(r) {
			t.add(r)
		}
	}
	return t
}

// all reports every acked row in w: the frozen-mode truth, once every write
// returned and Arc has flushed.
func (l *ledger) all(m string, w match) tally {
	return l.ackedBy(m, w, time.Now().Add(24*time.Hour))
}

// drop removes n acked rows from the ledger (the self-test: a run against
// a corrupted ledger must fail).
func (l *ledger) drop(n int) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	dropped := 0
	for m, rs := range l.rows {
		kept := rs[:0]
		for _, r := range rs {
			if dropped < n && !r.ackedAt.IsZero() {
				dropped++
				continue
			}
			kept = append(kept, r)
		}
		l.rows[m] = kept
	}
	return dropped
}

func (l *ledger) measurements() []string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	out := make([]string, 0, len(l.rows))
	for m := range l.rows {
		out = append(out, m)
	}
	return out
}

// rowsOf returns the acked rows of m that w selects (frozen mode's full
// results).
func (l *ledger) rowsOf(m string, w match) []*row {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var out []*row
	for _, r := range l.rows[m] {
		if !r.ackedAt.IsZero() && w(r) {
			out = append(out, r)
		}
	}
	return out
}
