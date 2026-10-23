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
	// mayMiss: the answer is allowed to lack future-stamped rows (mayHide).
	mayMiss bool
	lower   tally
	upper   tally
}

// judge settles a record against a ledger. lag is how long after its ack a
// row is certainly visible; off is Arc's clock minus ours; tol is the
// uncertainty of off.
type judge struct {
	l   *ledger
	lag time.Duration
	off time.Duration
	tol time.Duration
	// margin > 0 applies file-time pruning's documented contract: a row
	// stamped more than margin after its flush, in the flush's own hour, may
	// be invisible until that hour closes (internal/pruning/file_time_pruning.go).
	margin time.Duration
}

// mayHide reports a row file-time pruning may leave out of a query sent at
// send: future-stamped beyond the margin, in its flush's hour, which is still
// the live hour. Its absence is reported, not failed.
func (j judge) mayHide(r *row, send time.Time) bool {
	if j.margin <= 0 {
		return false
	}
	ts := time.UnixMicro(r.tsUs).UTC()
	return ts.Sub(r.sentAt) > j.margin &&
		ts.Truncate(time.Hour).Equal(r.sentAt.UTC().Truncate(time.Hour)) &&
		ts.Truncate(time.Hour).Equal(send.UTC().Truncate(time.Hour))
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
	strict := least
	least = func(x *row) bool { return strict(x) && !j.mayHide(x, r.send) }
	sel2 := r.q.sel2
	if sel2 != nil {
		strict2 := sel2
		sel2 = func(x *row) bool { return strict2(x) && !j.mayHide(x, r.send) }
	}

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
		lower = plus(lower, j.l.ackedBy(r.q.meas2, sel2, lowerCut))
		if r.phase == "frozen" {
			upper = plus(upper, j.l.ackedBy(r.q.meas2, r.q.sel2, upperCut))
		} else {
			upper = plus(upper, j.l.sentBy(r.q.meas2, r.q.sel2, upperCut))
		}
	}
	strictLower := j.l.ackedBy(r.q.meas, strict, lowerCut)
	if r.q.meas2 != "" {
		strictLower = plus(strictLower, j.l.ackedBy(r.q.meas2, r.q.sel2, lowerCut))
	}
	exact := strictLower == upper
	class := r.phase + "-bracket"
	if exact {
		class = r.phase + "-exact"
	}
	v := verdict{class: class, lower: lower, upper: upper, mayMiss: lower != strictLower}

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
		all := j.l.rowsOf(r.q.meas, func(x *row) bool { return strict(x) && !x.ackedAt.After(lowerCut) })
		want := expectRows(r.q.kind, all)
		ok := sameRows(want, r.res.rows)
		if !ok && v.mayMiss {
			ok = withinHidden(r.q.kind, all, func(x *row) bool { return !least(x) }, r.res.rows)
		}
		if !ok {
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
			ts := t.Add(17 * time.Second)
			// sentAt at the row's own stamp: never "future-stamped", so
			// never excused by mayHide.
			c.rows[m] = append(c.rows[m], &row{seq: seq, tsUs: ts.UnixMicro(), host: i % hosts, sentAt: ts, ackedAt: at})
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

// withinHidden reports whether got is a full result of rows minus some subset
// of the hideable ones (file-time pruning hides those of them in files flushed
// before the window, not the others).
func withinHidden(k kind, rows []*row, hideable match, got [][]string) bool {
	switch k {
	case kindTopN:
		// The hideable rows that came back are known by seq; the answer must be
		// the top 50 of the others plus those.
		present := map[string]bool{}
		for _, g := range got {
			present[g[0]] = true
		}
		var kept []*row
		for _, x := range rows {
			if !hideable(x) || present[strconv.FormatInt(x.seq, 10)] {
				kept = append(kept, x)
			}
		}
		return sameRows(expectRows(k, kept), got)
	case kindBuckets:
		// Per bucket, between the all-hidden and none-hidden answers.
		var visible []*row
		for _, x := range rows {
			if !hideable(x) {
				visible = append(visible, x)
			}
		}
		hi, lo := index(expectRows(k, rows)), index(expectRows(k, visible))
		for _, g := range got {
			h, okH := hi[g[0]]
			if !okH {
				return false
			}
			l, okL := lo[g[0]] // absent: every row of the bucket hideable
			for i := 1; i <= 2; i++ {
				v, _ := strconv.ParseInt(g[i], 10, 64)
				var lv int64
				if okL {
					lv, _ = strconv.ParseInt(l[i], 10, 64)
				}
				hv, _ := strconv.ParseInt(h[i], 10, 64)
				if v < lv || v > hv {
					return false
				}
			}
		}
		for b := range lo {
			if !containsBucket(got, b) {
				return false // a bucket with visible rows is missing
			}
		}
		return true
	}
	return false
}

func index(rs [][]string) map[string][]string {
	m := map[string][]string{}
	for _, r := range rs {
		m[r[0]] = r
	}
	return m
}

func containsBucket(rs [][]string, b string) bool {
	for _, r := range rs {
		if r[0] == b {
			return true
		}
	}
	return false
}
