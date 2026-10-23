package main

import (
	"strconv"
	"testing"
	"time"
)

func ledgerOf(base time.Time, n int, acked time.Time) *ledger {
	l := newLedger()
	var rs []*row
	for i := 0; i < n; i++ {
		rs = append(rs, &row{seq: int64(i + 1), tsUs: base.Add(time.Duration(i) * time.Minute).UnixMicro(), host: i % hosts, sentAt: acked, ackedAt: acked})
	}
	l.sent("m0", rs)
	return l
}

func countAnswer(c, s int64) result {
	return result{status: 200, rows: [][]string{{strconv.FormatInt(c, 10), strconv.FormatInt(s, 10)}}}
}

func geQuery(start time.Time) query {
	return query{shape: "ge", kind: kindCountSum, meas: "m0", sel: fixed(ge(start.UnixMicro())), start: start}
}

func TestJudgeFrozenExact(t *testing.T) {
	base := time.Date(2026, 10, 22, 10, 0, 0, 0, time.UTC)
	l := ledgerOf(base, 10, base)
	j := judge{l: l, lag: 4 * time.Second}
	q := geQuery(base.Add(5 * time.Minute)) // rows 6..10: count 5, sum 40
	rec := record{q: q, phase: "frozen", send: base.Add(time.Hour), recv: base.Add(time.Hour)}

	rec.res = countAnswer(5, 40)
	if v := j.settle(rec); v.status != pass || v.class != "frozen-exact" {
		t.Fatalf("right answer: %+v", v)
	}
	for _, wrong := range []result{countAnswer(4, 30), countAnswer(5, 39), countAnswer(6, 45)} {
		rec.res = wrong
		if v := j.settle(rec); v.status != fail {
			t.Fatalf("wrong answer %v passed: %+v", wrong.rows, v)
		}
	}
	rec.res = result{status: 200, rows: [][]string{{"5", "40"}}, marker: "truncated"}
	if v := j.settle(rec); v.status != errored {
		t.Fatalf("a truncated answer is a harness error: %+v", v)
	}
}

// Live: rows acked within the lag before the request, or still in flight,
// may or may not be in the answer; everything older must be.
func TestJudgeLiveBracket(t *testing.T) {
	base := time.Date(2026, 10, 22, 10, 0, 0, 0, time.UTC)
	send := base.Add(time.Hour)
	l := ledgerOf(base, 10, base) // old: certainly visible
	fresh := &row{seq: 100, tsUs: base.Add(30 * time.Minute).UnixMicro(), sentAt: send.Add(-time.Second), ackedAt: send.Add(-time.Second)}
	l.sent("m0", []*row{fresh})
	j := judge{l: l, lag: 4 * time.Second}
	rec := record{q: geQuery(base.Add(5 * time.Minute)), phase: "live", send: send, recv: send.Add(10 * time.Millisecond)}

	for _, ok := range []result{countAnswer(5, 40), countAnswer(6, 140)} {
		rec.res = ok
		if v := j.settle(rec); v.status != pass || v.class != "live-bracket" {
			t.Fatalf("%v: %+v", ok.rows, v)
		}
	}
	rec.res = countAnswer(4, 30)
	if v := j.settle(rec); v.status != fail {
		t.Fatalf("an answer missing a settled row passed: %+v", v)
	}
}

// Relative windows: Arc's now() is somewhere in [send, recv]; the answer
// must lie between the latest start's rows and the earliest start's.
func TestJudgeRelativeBracket(t *testing.T) {
	base := time.Date(2026, 10, 22, 10, 0, 0, 0, time.UTC)
	l := ledgerOf(base, 10, base)
	j := judge{l: l, lag: 4 * time.Second, tol: time.Millisecond}
	q := query{kind: kindCountSum, meas: "m0", relative: true, sel: func(now int64) match { return gt(now - 5*60e6) }}
	// now ∈ [base+9m30s, base+10m30s]: start ∈ [4m30s, 5m30s] → rows 6..10 (and row 5 never).
	rec := record{q: q, phase: "frozen", send: base.Add(9*time.Minute + 30*time.Second), recv: base.Add(10*time.Minute + 30*time.Second)}
	rec.res = countAnswer(5, 40)
	if v := j.settle(rec); v.status != pass {
		t.Fatalf("%+v", v)
	}
	rec.res = countAnswer(6, 45)
	if v := j.settle(rec); v.status != fail {
		t.Fatalf("a row before the earliest start passed: %+v", v)
	}
}

// The self-test must fail every passing judgement whose window a phantom
// changes, in both phases.
func TestSelfTestFailsWhatPhantomsChange(t *testing.T) {
	base := time.Date(2026, 10, 22, 10, 0, 0, 0, time.UTC)
	l := ledgerOf(base, 10, base)
	j := judge{l: l, lag: 4 * time.Second}
	recs := []record{
		{q: geQuery(base.Add(5 * time.Minute)), phase: "frozen", send: base.Add(time.Hour), recv: base.Add(time.Hour), res: countAnswer(5, 40)},
		{q: geQuery(base.Add(5 * time.Minute)), phase: "live", send: base.Add(time.Hour), recv: base.Add(time.Hour), res: countAnswer(5, 40)},
	}
	st := selfTest(j, recs, base.Add(-time.Minute), base.Add(time.Hour))
	if !st.OK || st.Required != 2 || st.Missed != 0 {
		t.Fatalf("self-test %+v", st)
	}
	// A judge that ignores the answer would pass everything: the self-test
	// must notice.
	recs[0].q.kind = kindTopN
	recs[0].res = result{status: 200, rows: expectRows(kindTopN, l.rowsOf("m0", recs[0].q.sel(0)))}
	if st := selfTest(j, recs, base.Add(-time.Minute), base.Add(time.Hour)); !st.OK {
		t.Fatalf("top-n self-test %+v", st)
	}
}
