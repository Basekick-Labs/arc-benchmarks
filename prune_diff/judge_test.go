package main

import (
	"strconv"
	"testing"
	"time"
)

var base = time.Date(2026, 10, 22, 10, 0, 0, 0, time.UTC)

// rowsAt makes n settled rows a minute apart from base, seq 1..n, acked at
// acked.
func rowsAt(n int, acked time.Time) []*row {
	var rs []*row
	for i := 0; i < n; i++ {
		rs = append(rs, &row{seq: int64(i + 1), tsUs: base.Add(time.Duration(i) * time.Minute).UnixMicro(), host: i % hosts, sentAt: acked, ackedAt: acked})
	}
	return rs
}

func answer(c, s, ec, es int64) result {
	f := func(x int64) string { return strconv.FormatInt(x, 10) }
	return result{status: 200, rows: [][]string{{f(c), f(s), f(ec), f(es)}}}
}

func geQuery(start time.Time) query {
	return query{shape: "ge", kind: kindCountSum, meas: "m0", sel: fixed(ge(start.UnixMicro())), start: start}
}

// rec is a record whose exact part is the rows settled by cut, as exec sets it.
func rec(l *ledger, q query, phase string, send time.Time, lag time.Duration, res result) record {
	cut := send.Add(-lag)
	if phase == "frozen" {
		cut = send.Add(24 * time.Hour)
	}
	return record{q: q, phase: phase, send: send, recv: send.Add(10 * time.Millisecond), cut: cut,
		settled: map[string]int64{"m0": l.maxSettled("m0", cut)}, res: res}
}

func TestJudgeFrozenExact(t *testing.T) {
	l := newLedger()
	l.sent("m0", rowsAt(10, base))
	j := judge{l: l, lag: 4 * time.Second}
	q := geQuery(base.Add(5 * time.Minute)) // rows 6..10: count 5, sum 40
	send := base.Add(time.Hour)

	if v := j.settle(rec(l, q, "frozen", send, 0, answer(5, 40, 5, 40))); v.status != pass || v.class != "frozen-exact" {
		t.Fatalf("right answer: %+v", v)
	}
	for _, wrong := range []result{answer(4, 30, 4, 30), answer(5, 39, 5, 39), answer(6, 45, 6, 45), answer(5, 40, 4, 30)} {
		if v := j.settle(rec(l, q, "frozen", send, 0, wrong)); v.status != fail {
			t.Fatalf("wrong answer %v passed: %+v", wrong.rows, v)
		}
	}
	marked := answer(5, 40, 5, 40)
	marked.marker = "truncated"
	if v := j.settle(rec(l, q, "frozen", send, 0, marked)); v.status != errored {
		t.Fatalf("a truncated answer is a harness error: %+v", v)
	}
}

// Review B1: future-stamped rows Arc may hide must not cover for a dropped
// ordinary row.
func TestJudgeHideableRowsCannotCoverADroppedRow(t *testing.T) {
	flush := base.Add(10 * time.Minute)
	l := newLedger()
	l.sent("m0", []*row{
		{seq: 10, tsUs: base.Add(20 * time.Minute).UnixMicro(), sentAt: base.Add(20 * time.Minute), ackedAt: base.Add(20 * time.Minute)},
		{seq: 20, tsUs: flush.Add(30 * time.Minute).UnixMicro(), sentAt: flush, ackedAt: flush, fut: true},
		{seq: 30, tsUs: flush.Add(31 * time.Minute).UnixMicro(), sentAt: flush, ackedAt: flush, fut: true},
	})
	j := judge{l: l, margin: 300 * time.Second}
	q := geQuery(base.Add(16 * time.Minute))
	send := base.Add(25 * time.Minute)
	// seq 10 dropped, both future rows returned: the whole answer (2, 50)
	// is within its bounds, but the exact part says (0, 0).
	if v := j.settle(rec(l, q, "frozen", send, 0, answer(2, 50, 0, 0))); v.status != fail {
		t.Fatalf("a dropped row covered by future rows passed: %+v", v)
	}
	// The future rows hidden (the documented contract), seq 10 present.
	if v := j.settle(rec(l, q, "frozen", send, 0, answer(1, 10, 1, 10))); v.status != pass || !v.mayMiss {
		t.Fatalf("contract answer: %+v", v)
	}
}

// Review B2: an in-flight row must not cover for a dropped settled row.
func TestJudgeInFlightRowCannotCoverADroppedRow(t *testing.T) {
	send := base.Add(time.Hour)
	l := newLedger()
	l.sent("m0", rowsAt(10, base)) // settled
	l.sent("m0", []*row{{seq: 100, tsUs: base.Add(30 * time.Minute).UnixMicro(), sentAt: send.Add(-time.Second), ackedAt: send.Add(-time.Second)}})
	j := judge{l: l, lag: 4 * time.Second}
	q := geQuery(base.Add(5 * time.Minute)) // settled rows 6..10
	if v := j.settle(rec(l, q, "live", send, j.lag, answer(5, 40, 5, 40))); v.status != pass {
		t.Fatalf("settled rows only: %+v", v)
	}
	if v := j.settle(rec(l, q, "live", send, j.lag, answer(6, 140, 5, 40))); v.status != pass {
		t.Fatalf("settled rows plus the in-flight one: %+v", v)
	}
	// seq 6 dropped, seq 100 present: the whole answer is within its
	// bracket, the settled part is not.
	if v := j.settle(rec(l, q, "live", send, j.lag, answer(5, 134, 4, 34))); v.status != fail {
		t.Fatalf("a dropped settled row covered by an in-flight one passed: %+v", v)
	}
}

// Relative windows: Arc's now() is somewhere in [send, recv].
func TestJudgeRelativeBracket(t *testing.T) {
	l := newLedger()
	l.sent("m0", rowsAt(10, base))
	j := judge{l: l, tol: time.Millisecond}
	q := query{kind: kindCountSum, meas: "m0", relative: true, sel: func(now int64) match { return gt(now - 5*60e6) }}
	r := rec(l, q, "frozen", base.Add(9*time.Minute+30*time.Second), 0, answer(5, 40, 5, 40))
	r.recv = base.Add(10*time.Minute + 30*time.Second) // start in [4m30s, 5m30s]: rows 6..10
	if v := j.settle(r); v.status != pass {
		t.Fatalf("%+v", v)
	}
	r.res = answer(6, 45, 6, 45)
	if v := j.settle(r); v.status != fail {
		t.Fatalf("a row before the earliest start passed: %+v", v)
	}
}

// File-time pruning's contract: a hideable row may be missing only while its
// hour is the live hour.
func TestJudgeFutureStampedContract(t *testing.T) {
	flush := base.Add(10 * time.Minute)
	l := newLedger()
	l.sent("m0", []*row{
		{seq: 1, tsUs: flush.Add(-time.Minute).UnixMicro(), sentAt: flush, ackedAt: flush},
		{seq: 2, tsUs: flush.Add(20 * time.Minute).UnixMicro(), sentAt: flush, ackedAt: flush, fut: true},
	})
	q := geQuery(flush.Add(6 * time.Minute)) // after the flush + margin, in the live hour
	j := judge{l: l, margin: 300 * time.Second}
	send := flush.Add(30 * time.Minute)
	if v := j.settle(rec(l, q, "frozen", send, 0, answer(0, 0, 0, 0))); v.status != pass || !v.mayMiss {
		t.Fatalf("hidden in its live hour: %+v", v)
	}
	if v := (judge{l: l}).settle(rec(l, q, "frozen", send, 0, answer(0, 0, 0, 0))); v.status != fail {
		t.Fatalf("without file pruning the row must be there: %+v", v)
	}
	if v := j.settle(rec(l, q, "frozen", flush.Add(time.Hour), 0, answer(0, 0, 0, 0))); v.status != fail {
		t.Fatalf("after the hour closes the row must be there: %+v", v)
	}
	if v := j.settle(rec(l, q, "frozen", send, 0, answer(1, 2, 0, 0))); v.status != pass {
		t.Fatalf("finding the row is fine too: %+v", v)
	}
}

// Every self-test flavour must fail each passing judgement it moves, and
// each must move at least one.
func TestSelfTestFlavours(t *testing.T) {
	l := newLedger()
	l.sent("m0", rowsAt(10, base))
	j := judge{l: l, lag: 4 * time.Second}
	q := geQuery(base.Add(2 * time.Minute)) // rows 3..10: count 8, sum 52
	send := base.Add(time.Hour)
	full := query{shape: "topn", kind: kindTopN, meas: "m0", sel: fixed(ge(base.Add(2 * time.Minute).UnixMicro()))}
	fr := rec(l, full, "frozen", send, 0, result{})
	fr.res = result{status: 200, rows: j.expect(l, fr).rows}
	recs := []record{
		rec(l, q, "frozen", send, 0, answer(8, 52, 8, 52)),
		rec(l, q, "live", send, j.lag, answer(8, 52, 8, 52)),
		fr,
	}
	for _, st := range selfTest(j, recs, base, base.Add(time.Hour)) {
		if !st.OK || st.Missed != 0 {
			t.Fatalf("%s: %+v", st.Flavour, st)
		}
	}
}

// A row in flight at the query only widens the bracket: a correct answer
// may hold it or not, so the self-test must not require a failure (the live
// runs' false misses). A settled row moves the answer off its old values and
// is required.
func TestSelfTestRequiresOnlyAMoveTheAnswerCannotFollow(t *testing.T) {
	l := newLedger()
	l.sent("m0", rowsAt(10, base))
	j := judge{l: l, lag: 4 * time.Second}
	q := geQuery(base.Add(2 * time.Minute))
	send := base.Add(time.Hour)
	r := rec(l, q, "live", send, j.lag, answer(8, 52, 8, 52))
	add := func(at time.Time) *ledger {
		return l.mutate(func(m string, rs []*row) []*row {
			return append(rs, &row{seq: 999, tsUs: base.Add(30 * time.Minute).UnixMicro(), sentAt: at, ackedAt: at})
		})
	}
	st := selfTestWith(j, []record{r}, []flavour{{name: "in-flight", l: add(send.Add(-time.Second)), covers: func(string) bool { return false }}})[0]
	if st.Required != 0 || st.Widened != 1 || st.Missed != 0 {
		t.Fatalf("an in-flight row is not required to fail: %+v", st)
	}
	st = selfTestWith(j, []record{r}, []flavour{{name: "settled", l: add(base), covers: func(string) bool { return true }}})[0]
	if !st.OK || st.Required != 1 || st.Missed != 0 {
		t.Fatalf("a settled row must be required and fail: %+v", st)
	}
	// A relative window: a row between its earliest and latest start only
	// widens the bracket.
	rq := query{kind: kindCountSum, meas: "m0", relative: true, sel: func(now int64) match { return gt(now - 5*60e6) }}
	rr := rec(l, rq, "frozen", base.Add(9*time.Minute+30*time.Second), 0, answer(5, 40, 5, 40))
	rr.recv = base.Add(10*time.Minute + 30*time.Second)
	straddle := l.mutate(func(m string, rs []*row) []*row {
		return append(rs, &row{seq: -1, tsUs: base.Add(5 * time.Minute).UnixMicro(), sentAt: base, ackedAt: base})
	})
	st = selfTestWith(j, []record{rr}, []flavour{{name: "straddle", l: straddle, covers: func(string) bool { return false }}})[0]
	if st.Required != 0 || st.Widened != 1 {
		t.Fatalf("a row on a relative window's moving edge is not required to fail: %+v", st)
	}
}

// A judge that passes what it must fail is reported as a miss.
func TestSelfTestReportsAMiss(t *testing.T) {
	l := newLedger()
	l.sent("m0", rowsAt(10, base))
	j := judge{l: l, lag: 4 * time.Second}
	r := rec(l, geQuery(base.Add(2*time.Minute)), "live", base.Add(time.Hour), j.lag, answer(8, 52, 8, 52))
	blind := func(*ledger, record) verdict { return verdict{status: pass} }
	st := selfTestWith(j, []record{r}, []flavour{{name: "phantoms", l: l.withHighPhantoms(base, base.Add(time.Hour)), covers: func(string) bool { return true }, judgeWith: blind}})[0]
	if st.OK || st.Missed != 1 || len(st.MissedFirst) != 1 || st.MissedFirst[0].At == "" {
		t.Fatalf("a judge that passes a required failure must be a miss: %+v", st)
	}
}

// A write's rows grouped by time and host judge exactly like the rows one
// by one (the heavy measurement's 600-row writes).
func TestGroupedRowsJudgeLikeSingleRows(t *testing.T) {
	var single []*row
	for i := 0; i < 12; i++ {
		single = append(single, &row{seq: int64(i + 1), tsUs: base.Add(time.Duration(i%3) * time.Minute).UnixMicro(), host: i % 2, sentAt: base, ackedAt: base})
	}
	var copied []*row
	for _, r := range single {
		c := *r
		copied = append(copied, &c)
	}
	grouped := group(copied)
	if len(grouped) != 6 {
		t.Fatalf("groups = %d, want 6", len(grouped))
	}
	ls, lg := newLedger(), newLedger()
	ls.sent("m0", single)
	lg.sent("m0", grouped)
	for _, k := range []kind{kindBuckets, kindTopN} {
		if !sameRows(expectRows(k, single), expectRows(k, grouped)) {
			t.Fatalf("kind %d: grouped %v, single %v", k, expectRows(k, grouped), expectRows(k, single))
		}
	}
	sel := ge(base.Add(time.Minute).UnixMicro())
	if ls.all("m0", sel) != lg.all("m0", sel) || ls.maxSettled("m0", base) != lg.maxSettled("m0", base) {
		t.Fatal("grouped tallies differ")
	}
}
