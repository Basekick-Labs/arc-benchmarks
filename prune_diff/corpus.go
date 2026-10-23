package main

import (
	"fmt"
	"sort"
	"strconv"
	"time"
)

// kind is what a query selects.
type kind int

const (
	kindCountSum kind = iota // one row: count(*), sum(seq)
	kindBuckets              // per-minute count and sum, ordered
	kindTopN                 // the 50 newest seqs
)

// query is one corpus entry for one measurement.
type query struct {
	shape  string // predicate shape, for the report
	offset string // start offset bucket, for the report
	kind   kind
	meas   string
	meas2  string // the second measurement of a two-measurement shape
	// from builds the SQL for a table reference (bare "m" or "db.m") and the
	// settled-row filter of each measurement (see cs).
	from func(t, ex func(m string) string) string
	// sel selects the rows the SQL selects; for a relative predicate it
	// takes the instant Arc evaluated now() at, which is only bracketed.
	sel func(nowUs int64) match
	// sel2 selects meas2's rows (two-measurement shapes).
	sel2 match
	// relative is set when the predicate depends on Arc's now().
	relative bool
	// start is a literal window's start (zero for a relative one).
	start time.Time
	// prunable: Arc's time-range scanner narrows this shape, so a live-hour
	// window reaches the file-time pass. False for shapes Arc must not prune
	// (OR) and for shapes it refuses and reads whole (NOT, arithmetic, a
	// subquery, a CTE, two tables, a bound only on end_time). Every answer
	// must be right either way.
	prunable bool
}

func lit(t time.Time) string { return t.UTC().Format("2006-01-02 15:04:05.000000") }

func rfc(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000000Z") }

// startOffsets put the window start inside the live hour, on its edge, and in
// older hours; the 14-15 min offsets straddle a compacted hour's edge and the
// negative ones start in the future (plan revision 2).
var startOffsets = []struct {
	name string
	d    time.Duration
}{
	{"5s", 5 * time.Second}, {"30s", 30 * time.Second}, {"2m", 2 * time.Minute}, {"10m", 10 * time.Minute},
	{"14m", 14 * time.Minute}, {"15m", 15 * time.Minute}, {"59m", 59 * time.Minute}, {"2h", 2 * time.Hour},
	{"3h", 3 * time.Hour}, {"5h", 5 * time.Hour}, {"26h", 26 * time.Hour},
	{"future+60s", -60 * time.Second}, {"future+305s", -305 * time.Second},
}

func ge(s int64) match { return func(r *row) bool { return r.tsUs >= s } }
func gt(s int64) match { return func(r *row) bool { return r.tsUs > s } }
func and(ms ...match) match {
	return func(r *row) bool {
		for _, m := range ms {
			if !m(r) {
				return false
			}
		}
		return true
	}
}
func or(a, b match) match { return func(r *row) bool { return a(r) || b(r) } }
func lt(e int64) match    { return func(r *row) bool { return r.tsUs < e } }
func le(e int64) match    { return func(r *row) bool { return r.tsUs <= e } }
func hostIn(h ...int) match {
	return func(r *row) bool {
		for _, x := range h {
			if r.host == x {
				return true
			}
		}
		return false
	}
}

// fixed wraps a literal selection as a sel that ignores now.
func fixed(m match) func(int64) match {
	return func(int64) match { return m }
}

const countSum = "count(*), coalesce(sum(seq),0)::BIGINT"

// cs is a count/sum select list: the whole answer, then the part over the
// rows ex selects. ex picks the settled, not-future-stamped rows (seq <= the
// newest seq certainly visible when the query was sent): that part has one
// right answer, so it is compared exactly, and an in-flight or future row
// cannot make up for a dropped one (review B1/B2). The WHERE clause is the
// shape's own, so pruning sees the same query.
//
// The exact part is written with count_if and CASE, not FILTER (WHERE ...):
// Arc's time-range scanner refuses a query with a second WHERE keyword, so a
// FILTER clause turned pruning off for every count/sum query and the harness
// tested almost none of it.
func cs(ex string) string {
	return fmt.Sprintf("count(*), coalesce(sum(seq),0)::BIGINT, count_if(%s), coalesce(sum(CASE WHEN %s THEN seq ELSE 0 END),0)::BIGINT", ex, ex)
}

// buildCorpus returns every query for measurement m (and m2 for the
// two-measurement shapes) around the reference instant ref.
func buildCorpus(m, m2 string, ref time.Time) []query {
	var qs []query
	add := func(shape, off string, prunable bool, where string, sel match) {
		qs = append(qs, query{shape: shape, offset: off, kind: kindCountSum, meas: m, prunable: prunable, sel: fixed(sel),
			from: func(t, ex func(string) string) string {
				return fmt.Sprintf("SELECT %s FROM %s WHERE %s", cs(ex(m)), t(m), where)
			}})
	}
	for _, o := range startOffsets {
		first := len(qs)
		S := ref.Add(-o.d).Truncate(time.Microsecond)

		E := S.Add(10 * time.Minute)
		s, e := S.UnixMicro(), E.UnixMicro()
		Sl, El := lit(S), lit(E)
		add("ge", o.name, true, fmt.Sprintf("time >= '%s'", Sl), ge(s))
		add("gt-lt", o.name, true, fmt.Sprintf("time > '%s' AND time < '%s'", Sl, El), and(gt(s), lt(e)))
		add("ge-le", o.name, true, fmt.Sprintf("time >= '%s' AND time <= '%s'", Sl, El), and(ge(s), le(e)))
		add("between", o.name, true, fmt.Sprintf("time BETWEEN '%s' AND '%s'", Sl, El), and(ge(s), le(e)))
		add("grafana", o.name, true, fmt.Sprintf("(time) >= '%s' AND (time) < '%s'", Sl, El), and(ge(s), lt(e)))
		add("reversed", o.name, true, fmt.Sprintf("'%s' <= time AND '%s' > time", Sl, El), and(ge(s), lt(e)))
		add("cast", o.name, true, fmt.Sprintf("time >= '%s'::timestamp AND time < '%s'::timestamp", Sl, El), and(ge(s), lt(e)))
		add("typed", o.name, true, fmt.Sprintf("time >= TIMESTAMP '%s' AND time < TIMESTAMP '%s'", Sl, El), and(ge(s), lt(e)))
		add("rfc3339", o.name, true, fmt.Sprintf("time >= '%s'", rfc(S)), ge(s))
		add("host-in", o.name, true, fmt.Sprintf("time >= '%s' AND host IN ('h1','h2')", Sl), and(ge(s), hostIn(1, 2)))
		add("host-or", o.name, true, fmt.Sprintf("time >= '%s' AND (host = 'h1' OR host = 'h2')", Sl), and(ge(s), hostIn(1, 2)))
		// Shapes Arc must not narrow by time.
		add("or-time", o.name, false, fmt.Sprintf("(time >= '%s' OR host = 'h3')", Sl), or(ge(s), hostIn(3)))
		add("not", o.name, false, fmt.Sprintf("NOT (time < '%s')", Sl), ge(s))
		hourBack := S.Add(-time.Hour).UnixMicro()
		add("arith", o.name, false, fmt.Sprintf("time >= '%s'::timestamp - INTERVAL '1 hour'", Sl), ge(hourBack))
		// An inclusive end exactly on the next hour's first instant (seeded).
		H := S.Truncate(time.Hour).Add(time.Hour)
		add("le-hour-edge", o.name, true, fmt.Sprintf("time >= '%s' AND time <= '%s'", Sl, lit(H)), and(ge(s), le(H.UnixMicro())))
		// A column whose name ends in "time" is not the time column.
		endS := S.Add(-2 * time.Hour).UnixMicro()
		add("end-time", o.name, false, fmt.Sprintf("end_time >= '%s'", Sl), ge(endS))
		add("like-interval", o.name, true, fmt.Sprintf("host NOT LIKE '%%INTERVAL 1 HOUR%%' AND time >= '%s'", Sl), ge(s))

		// Wrapped forms.
		where := fmt.Sprintf("time >= '%s'", Sl)
		qs = append(qs,
			query{shape: "subquery", offset: o.name, kind: kindCountSum, meas: m, prunable: false, sel: fixed(ge(s)),
				from: func(t, ex func(string) string) string {
					return fmt.Sprintf("SELECT %s FROM (SELECT * FROM %s WHERE %s) sub", cs(ex(m)), t(m), where)
				}},
			query{shape: "cte", offset: o.name, kind: kindCountSum, meas: m, prunable: false, sel: fixed(ge(s)),
				from: func(t, ex func(string) string) string {
					return fmt.Sprintf("WITH w AS (SELECT * FROM %s WHERE %s) SELECT %s FROM w", t(m), where, cs(ex(m)))
				}},
			query{shape: "buckets", offset: o.name, kind: kindBuckets, meas: m, prunable: true, sel: fixed(and(ge(s), lt(e))),
				from: func(t, ex func(string) string) string {
					// Full results over settled rows only, so they have one
					// answer while ingest runs; the time bound is unchanged.
					return fmt.Sprintf("SELECT epoch_us(time_bucket(INTERVAL '1 minute', time)) AS b, count(*), coalesce(sum(seq),0)::BIGINT FROM %s WHERE time >= '%s' AND time < '%s' AND %s GROUP BY b ORDER BY b", t(m), Sl, El, ex(m))
				}},
			query{shape: "topn", offset: o.name, kind: kindTopN, meas: m, prunable: true, sel: fixed(ge(s)),
				from: func(t, ex func(string) string) string {
					return fmt.Sprintf("SELECT seq FROM %s WHERE %s AND %s ORDER BY time DESC, seq DESC LIMIT 50", t(m), where, ex(m))
				}},
		)
		if m2 != "" {
			S2 := S.Add(-2 * time.Hour) // another hour: a leaked bound drops rows
			s2 := S2.UnixMicro()
			qs = append(qs, query{shape: "two-measurements", offset: o.name, kind: kindCountSum, meas: m, meas2: m2, prunable: false,
				sel: fixed(ge(s)), // the second measurement's window is s2; see twoSel
				from: func(t, ex func(string) string) string {
					inner := "count(*) c, coalesce(sum(seq),0)::BIGINT s, count_if(%[1]s) ec, coalesce(sum(CASE WHEN %[1]s THEN seq ELSE 0 END),0)::BIGINT es"
					return fmt.Sprintf("SELECT x.c + y.c, x.s + y.s, x.ec + y.ec, x.es + y.es FROM (SELECT %s FROM %s WHERE time >= '%s') x, (SELECT %s FROM %s WHERE time >= '%s') y",
						fmt.Sprintf(inner, ex(m)), t(m), Sl, fmt.Sprintf(inner, ex(m2)), t(m2), lit(S2))
				}})
			qs[len(qs)-1].sel2 = ge(s2)
		}
		for i := first; i < len(qs); i++ {
			qs[i].start = S
		}
	}

	// Relative predicates: the window starts at Arc's now() - n.
	for _, n := range []time.Duration{30 * time.Second, 10 * time.Minute, 59 * time.Minute, 2 * time.Hour} {
		secs := int64(n / time.Second)
		rel := func(nowUs int64) match { return gt(nowUs - secs*1e6) }
		name := strconv.FormatInt(secs, 10) + "s"
		for _, form := range []struct{ shape, where string }{
			{"now-quoted", fmt.Sprintf("time > now() - INTERVAL '%d seconds'", secs)},
			{"now-unquoted", fmt.Sprintf("time > now() - INTERVAL %d SECOND", secs)},
			{"current-timestamp", fmt.Sprintf("time > CURRENT_TIMESTAMP - INTERVAL '%d seconds'", secs)},
		} {
			where := form.where
			qs = append(qs, query{shape: form.shape, offset: name, kind: kindCountSum, meas: m, prunable: true, relative: true, sel: rel,
				from: func(t, ex func(string) string) string {
					return fmt.Sprintf("SELECT %s FROM %s WHERE %s", cs(ex(m)), t(m), where)
				}})
		}
	}
	return qs
}

// expectRows is the frozen-mode answer for a full-result query.
func expectRows(k kind, rs []*row) [][]string {
	switch k {
	case kindBuckets:
		type b struct{ c, s int64 }
		bs := map[int64]*b{}
		for _, r := range rs {
			k := r.tsUs - ((r.tsUs%60e6)+60e6)%60e6
			if bs[k] == nil {
				bs[k] = &b{}
			}
			bs[k].c += r.count()
			bs[k].s += r.seqSum()
		}
		keys := make([]int64, 0, len(bs))
		for k := range bs {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		out := make([][]string, len(keys))
		for i, k := range keys {
			out[i] = []string{strconv.FormatInt(k, 10), strconv.FormatInt(bs[k].c, 10), strconv.FormatInt(bs[k].s, 10)}
		}
		return out
	case kindTopN:
		type one struct{ ts, seq int64 }
		var all []one
		for _, r := range rs {
			for _, q := range r.allSeqs() {
				all = append(all, one{r.tsUs, q})
			}
		}
		sort.Slice(all, func(i, j int) bool {
			if all[i].ts != all[j].ts {
				return all[i].ts > all[j].ts
			}
			return all[i].seq > all[j].seq
		})
		if len(all) > 50 {
			all = all[:50]
		}
		out := make([][]string, len(all))
		for i, o := range all {
			out[i] = []string{strconv.FormatInt(o.seq, 10)}
		}
		return out
	}
	return nil
}
