package main

import (
	"fmt"
	"strconv"
	"time"
)

// record is one executed query and Arc's answer.
type record struct {
	q      query
	wire   wire
	header bool   // bare table with x-arc-database, or db.m without
	node   string // the reader that answered
	sql    string
	res    result
	send   time.Time
	recv   time.Time
	phase  string // "frozen" or "live"
	// cut is the instant a row must have been acked by to be certainly
	// visible to this query; settled[m] the newest seq of m acked by then.
	// The SQL's exact part selects seq <= settled[m] (see cs).
	cut     time.Time
	settled map[string]int64
	// metric deltas over this one query
	expanded, passes float64
}

// status of one judgement; FAIL outranks every other.
type status string

const (
	pass        status = "PASS"
	fail        status = "FAIL"
	errored     status = "ERROR"       // harness or transport problem, not a wrong answer
	skipped     status = "SKIP"        // not judgeable (dirty measurement)
	unavailable status = "UNAVAILABLE" // 503 Retry-After after the one retry
)

// verdict is one judgement.
type verdict struct {
	status  status
	class   string // frozen-exact, live-exact, frozen-full, live-full
	why     string
	exp     expectation
	mayMiss bool
}

// judge settles a record against a ledger. lag is how long after its ack a
// row is certainly visible; off is Arc's clock minus ours; tol is the
// uncertainty of off.
type judge struct {
	l   *ledger
	lag time.Duration
	off time.Duration
	tol time.Duration
	// margin > 0 applies file-time pruning's documented contract to the
	// whole answer's lower bound (see mayHide).
	margin time.Duration
}

// mayHide reports a row file-time pruning may leave out of r's answer
// (internal/pruning/file_time_pruning.go): the window starts in the hour
// that is live for Arc, the row is stamped in that hour more than the margin
// after its flush, and its file was flushed before start - margin. Only
// future-stamped rows qualify, and they are outside the exact part anyway;
// this only loosens the whole answer's lower bound.
func (j judge) mayHide(x *row, r record) bool {
	if j.margin <= 0 || r.q.start.IsZero() || !x.fut {
		return false
	}
	slack := time.Second + j.tol
	hour := r.q.start.UTC().Truncate(time.Hour)
	ts := time.UnixMicro(x.tsUs).UTC()
	flush := x.sentAt.Add(j.off)
	return hour.Equal(r.send.Add(j.off).UTC().Truncate(time.Hour)) &&
		hour.Equal(ts.Truncate(time.Hour)) &&
		flush.Before(r.q.start.Add(-j.margin).Add(slack)) &&
		ts.Sub(flush) > j.margin-slack
}

// expectation is everything a record's answer is judged against.
type expectation struct {
	exactLo, exactHi tally // the settled, not-future part: one value unless relative
	totLo, totHi     tally // the whole answer
	rows             [][]string
	mayMiss          bool
}

func (e expectation) equal(o expectation) bool {
	return e.exactLo == o.exactLo && e.exactHi == o.exactHi && e.totLo == o.totLo && e.totHi == o.totHi && sameRows(e.rows, o.rows)
}

// expect computes what r's answer must be, from ledger l.
func (j judge) expect(l *ledger, r record) expectation {
	// Arc evaluated now() somewhere in [nowLo, nowHi]: the earliest instant
	// selects the most rows of a relative window, the latest the fewest.
	nowLo := r.send.Add(j.off - j.tol).UnixMicro()
	nowHi := r.recv.Add(j.off + j.tol).UnixMicro()
	most, least := r.q.sel(nowLo), r.q.sel(nowHi)
	settled := func(m string) match {
		n := r.settled[m]
		return func(x *row) bool { return !x.fut && x.seq <= n }
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	part := func(m string, sel, set, cut match) tally {
		var t tally
		for _, x := range l.rows[m] {
			if sel(x) && set(x) && cut(x) {
				t.add(x)
			}
		}
		return t
	}
	all := func(*row) bool { return true }
	ackedByCut := func(x *row) bool { return !x.ackedAt.IsZero() && !x.ackedAt.After(r.cut) }
	sentByRecv := func(x *row) bool { return !x.sentAt.After(r.recv) }
	if r.phase == "frozen" {
		sentByRecv = func(x *row) bool { return !x.ackedAt.IsZero() }
	}

	var e expectation
	if r.q.kind != kindCountSum {
		var rs []*row
		set := settled(r.q.meas)
		for _, x := range l.rows[r.q.meas] {
			if least(x) && set(x) {
				rs = append(rs, x)
			}
		}
		e.rows = expectRows(r.q.kind, rs)
		return e
	}

	e.exactLo = part(r.q.meas, least, settled(r.q.meas), all)
	e.exactHi = part(r.q.meas, most, settled(r.q.meas), all)
	visible := func(x *row) bool { return !j.mayHide(x, r) }
	e.totLo = part(r.q.meas, least, visible, ackedByCut)
	e.totHi = part(r.q.meas, most, all, sentByRecv)
	e.mayMiss = e.totLo != part(r.q.meas, least, all, ackedByCut)
	if r.q.meas2 != "" {
		e.exactLo = plus(e.exactLo, part(r.q.meas2, r.q.sel2, settled(r.q.meas2), all))
		e.exactHi = plus(e.exactHi, part(r.q.meas2, r.q.sel2, settled(r.q.meas2), all))
		// sel2 starts two hours before the live hour: never file-pruned.
		e.totLo = plus(e.totLo, part(r.q.meas2, r.q.sel2, all, ackedByCut))
		e.totHi = plus(e.totHi, part(r.q.meas2, r.q.sel2, all, sentByRecv))
	}
	return e
}

func (j judge) settle(r record) verdict {
	if r.res.status == 503 && r.res.retryAfter {
		return verdict{status: unavailable, why: "503 Retry-After"}
	}
	if r.res.err != nil {
		return verdict{status: errored, why: r.res.err.Error()}
	}
	if r.res.marker != "" {
		return verdict{status: errored, why: "answer carried " + r.res.marker}
	}
	for _, m := range []string{r.q.meas, r.q.meas2} {
		if m == "" {
			continue
		}
		if why, dirty := j.l.isDirty(m); dirty {
			return verdict{status: skipped, why: m + ": " + why}
		}
	}
	return j.judgeWith(j.l, r)
}

// judgeWith judges r's answer against what ledger l says it must be.
func (j judge) judgeWith(l *ledger, r record) verdict {
	e := j.expect(l, r)
	v := verdict{exp: e, mayMiss: e.mayMiss}
	if r.q.kind != kindCountSum {
		v.class = r.phase + "-full"
		if !sameRows(e.rows, r.res.rows) {
			v.status, v.why = fail, fmt.Sprintf("rows differ: want %d rows %v…, got %d rows %v…", len(e.rows), head(e.rows), len(r.res.rows), head(r.res.rows))
			return v
		}
		v.status = pass
		return v
	}
	v.class = r.phase + "-exact"
	if len(r.res.rows) != 1 || len(r.res.rows[0]) != 4 {
		v.status, v.why = errored, fmt.Sprintf("want one row of four values, got %v", r.res.rows)
		return v
	}
	var n [4]int64
	for i := range n {
		x, err := strconv.ParseInt(r.res.rows[0][i], 10, 64)
		if err != nil {
			v.status, v.why = errored, fmt.Sprintf("non-integer answer %v", r.res.rows[0])
			return v
		}
		n[i] = x
	}
	tot, exact := tally{n[0], n[1]}, tally{n[2], n[3]}
	if !within(exact, e.exactLo, e.exactHi) {
		v.status, v.why = fail, fmt.Sprintf("settled rows: got count=%d sum=%d, want count in [%d,%d] sum in [%d,%d]", exact.count, exact.sum, e.exactLo.count, e.exactHi.count, e.exactLo.sum, e.exactHi.sum)
		return v
	}
	if !within(tot, e.totLo, e.totHi) {
		v.status, v.why = fail, fmt.Sprintf("all rows: got count=%d sum=%d, want count in [%d,%d] sum in [%d,%d]", tot.count, tot.sum, e.totLo.count, e.totHi.count, e.totLo.sum, e.totHi.sum)
		return v
	}
	v.status = pass
	return v
}

func within(t, lo, hi tally) bool {
	return t.count >= lo.count && t.count <= hi.count && t.sum >= lo.sum && t.sum <= hi.sum
}

func plus(a, b tally) tally { return tally{a.count + b.count, a.sum + b.sum} }

func sameRows(a, b [][]string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if len(a[i]) != len(b[i]) {
			return false
		}
		for k := range a[i] {
			if a[i][k] != b[i][k] {
				return false
			}
		}
	}
	return true
}

func head(rs [][]string) [][]string {
	if len(rs) > 3 {
		return rs[:3]
	}
	return rs
}

// The self-test ledgers (plan revision 21, review H1/H3): each changes what
// some answers must be; every passing judgement the change moves off its old
// values (cannotFollow) must then FAIL, or the judge is not checking that
// part.

// withHighPhantoms adds one row per minute over [from, to] to every
// measurement, with seqs above any real one, acked at from: before every
// query, so never in flight (and never future-stamped, so never excused).
func (l *ledger) withHighPhantoms(from, to time.Time) *ledger {
	return l.mutate(func(m string, rs []*row) []*row {
		seq := int64(1e15)
		i := 0
		for t := from.Truncate(time.Minute); !t.After(to); t = t.Add(time.Minute) {
			seq++
			ts := t.Add(17 * time.Second)
			rs = append(rs, &row{seq: seq, tsUs: ts.UnixMicro(), host: i % hosts, sentAt: from, ackedAt: from})
			i++
		}
		return rs
	})
}

// withLowPhantoms adds rows with seqs below every real one, acked at from
// (before every query): they fall inside the exact part of every window that
// holds them.
func (l *ledger) withLowPhantoms(from, to time.Time) *ledger {
	return l.mutate(func(m string, rs []*row) []*row {
		seq := int64(0)
		i := 0
		for t := from.Truncate(time.Minute); !t.After(to); t = t.Add(time.Minute) {
			seq--
			rs = append(rs, &row{seq: seq, tsUs: t.Add(23 * time.Second).UnixMicro(), host: i % hosts, sentAt: from, ackedAt: from})
			i++
		}
		return rs
	})
}

// withoutRows drops every seventh acked row: an answer holding them is now
// above the upper bound, and the exact part differs.
func (l *ledger) withoutRows() *ledger {
	return l.mutate(func(m string, rs []*row) []*row {
		var kept []*row
		for _, x := range rs {
			if !x.ackedAt.IsZero() && x.seq%7 == 3 {
				continue
			}
			kept = append(kept, x)
		}
		return kept
	})
}

func (l *ledger) mutate(f func(m string, rs []*row) []*row) *ledger {
	l.mu.RLock()
	defer l.mu.RUnlock()
	c := newLedger()
	for m, d := range l.dirty {
		c.dirty[m] = d
	}
	for m, rs := range l.rows {
		c.rows[m] = f(m, append([]*row(nil), rs...))
	}
	return c
}
