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
	header bool // bare table with x-arc-database, or db.m without
	sql    string
	res    result
	send   time.Time
	recv   time.Time
	phase  string // "frozen" or "live"
	// metric deltas over this one query
	expanded, passes float64
}

// status of one judgement; FAIL outranks every other.
type status string

const (
	pass        status = "PASS"
	fail        status = "FAIL"
	errored     status = "ERROR"       // harness or transport problem, not a wrong answer
	skipped     status = "SKIP"        // not judgeable (dirty measurement, live full result)
	unavailable status = "UNAVAILABLE" // 503 Retry-After after the one retry
)

// verdict is one judgement.
type verdict struct {
	status status
	class  string // frozen-exact, frozen-bracket, live-exact, live-bracket
	why    string
	lower  tally
	upper  tally
}

// judge settles a record against a ledger. lag is how long after its ack a
// row is certainly visible; off is Arc's clock minus ours; tol is the
// uncertainty of off.
type judge struct {
	l   *ledger
	lag time.Duration
	off time.Duration
	tol time.Duration
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
	// Arc evaluated now() somewhere in [nowLo, nowHi]: the earliest instant
	// selects the most rows of a relative window, the latest the fewest.
	nowLo := r.send.Add(j.off - j.tol).UnixMicro()
	nowHi := r.recv.Add(j.off + j.tol).UnixMicro()
	most, least := r.q.sel(nowLo), r.q.sel(nowHi)

	var lowerCut, upperCut time.Time
	if r.phase == "frozen" {
		// Ingest stopped and the barrier saw every acked row.
		far := time.Now().Add(24 * time.Hour)
		lowerCut, upperCut = far, far
	} else {
		lowerCut, upperCut = r.send.Add(-j.lag), r.recv
	}
	lower := j.l.ackedBy(r.q.meas, least, lowerCut)
	upper := j.l.sentBy(r.q.meas, most, upperCut)
	if r.phase == "frozen" {
		upper = j.l.ackedBy(r.q.meas, most, upperCut)
	}
	if r.q.meas2 != "" {
		lower = plus(lower, j.l.ackedBy(r.q.meas2, r.q.sel2, lowerCut))
		if r.phase == "frozen" {
			upper = plus(upper, j.l.ackedBy(r.q.meas2, r.q.sel2, upperCut))
		} else {
			upper = plus(upper, j.l.sentBy(r.q.meas2, r.q.sel2, upperCut))
		}
	}
	exact := lower == upper
	class := r.phase + "-bracket"
	if exact {
		class = r.phase + "-exact"
	}
	v := verdict{class: class, lower: lower, upper: upper}

	switch r.q.kind {
	case kindCountSum:
		if len(r.res.rows) != 1 || len(r.res.rows[0]) != 2 {
			v.status, v.why = errored, fmt.Sprintf("want one row of two values, got %v", r.res.rows)
			return v
		}
		c, err1 := strconv.ParseInt(r.res.rows[0][0], 10, 64)
		s, err2 := strconv.ParseInt(r.res.rows[0][1], 10, 64)
		if err1 != nil || err2 != nil {
			v.status, v.why = errored, fmt.Sprintf("non-integer answer %v", r.res.rows[0])
			return v
		}
		got := tally{c, s}
		if got.count < lower.count || got.count > upper.count || got.sum < lower.sum || got.sum > upper.sum {
			v.status, v.why = fail, fmt.Sprintf("got count=%d sum=%d, want count in [%d,%d] sum in [%d,%d]", c, s, lower.count, upper.count, lower.sum, upper.sum)
			return v
		}
		v.status = pass
		return v
	default:
		// A full result is judged only when its row set is settled: the
		// selection does not depend on now() and nothing in it is in flight.
		if !exact || r.q.relative {
			v.status, v.why = skipped, "full result with an unsettled row set"
			return v
		}
		want := expectRows(r.q.kind, j.l.rowsOf(r.q.meas, func(x *row) bool { return least(x) && !x.ackedAt.After(lowerCut) }))
		if !sameRows(want, r.res.rows) {
			v.status, v.why = fail, fmt.Sprintf("rows differ: want %d rows %v…, got %d rows %v…", len(want), head(want), len(r.res.rows), head(r.res.rows))
			return v
		}
		v.status = pass
		return v
	}
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

// withPhantoms returns a copy of l with one phantom row per minute over
// [from, to] in every measurement, acked at at. A judge that is really
// checking must FAIL every judgement whose window holds one (plan
// revision 21).
func (l *ledger) withPhantoms(from, to, at time.Time) (*ledger, int64) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	c := newLedger()
	for m, d := range l.dirty {
		c.dirty[m] = d
	}
	seq := int64(1e15)
	for m, rs := range l.rows {
		c.rows[m] = append([]*row(nil), rs...)
		i := 0
		for t := from.Truncate(time.Minute); !t.After(to); t = t.Add(time.Minute) {
			seq++
			c.rows[m] = append(c.rows[m], &row{seq: seq, tsUs: t.Add(17 * time.Second).UnixMicro(), host: i % hosts, sentAt: at, ackedAt: at})
			i++
		}
	}
	return c, seq - 1e15
}

// phantomIn reports whether the phantom ledger changes what r must answer:
// only those judgements must fail. A phantom's seq (>= 1e15) puts every
// changed count/sum window's lower bound above anything Arc can return, so
// those fail in either mode; a full result must fail when its expected rows
// change.
func phantomIn(real, ph *ledger, r record, j judge) bool {
	least := r.q.sel(r.recv.Add(j.off + j.tol).UnixMicro())
	cut := time.Now().Add(24 * time.Hour)
	if r.phase != "frozen" {
		cut = r.send.Add(-j.lag)
	}
	if r.q.kind != kindCountSum {
		sel := func(x *row) bool { return least(x) && !x.ackedAt.After(cut) }
		return !sameRows(expectRows(r.q.kind, real.rowsOf(r.q.meas, sel)), expectRows(r.q.kind, ph.rowsOf(r.q.meas, sel)))
	}
	if real.ackedBy(r.q.meas, least, cut) != ph.ackedBy(r.q.meas, least, cut) {
		return true
	}
	return r.q.meas2 != "" && real.ackedBy(r.q.meas2, r.q.sel2, cut) != ph.ackedBy(r.q.meas2, r.q.sel2, cut)
}
