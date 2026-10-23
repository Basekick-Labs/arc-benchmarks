package main

import (
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

type failure struct {
	SQL    string `json:"sql"`
	Wire   string `json:"wire"`
	Header bool   `json:"header_form"`
	Why    string `json:"why"`
	Class  string `json:"class"`
	At     string `json:"at"`
}

type bucket struct {
	Pass, Fail, Error, Skip, Unavailable int
	Failures                             []failure `json:"failures,omitempty"`
}

type selfTestResult struct {
	Flavour  string `json:"flavour"`
	OK       bool   `json:"ok"`
	Required int    `json:"required_failures"`
	Missed   int    `json:"missed"`
	// Widened: judgements the change moved without moving them off their
	// old values (see cannotFollow); not required to fail.
	Widened     int            `json:"widened_not_required"`
	MissedFirst []failure      `json:"missed_first,omitempty"`
	ByClass     map[string]int `json:"required_by_class"`
	Note        string         `json:"note,omitempty"`
}

type report struct {
	Verdict       string             `json:"verdict"`
	Reasons       []string           `json:"reasons,omitempty"`
	Mode          string             `json:"mode"`
	ArcConfig     string             `json:"arc_config_label"`
	Started       string             `json:"started"`
	Duration      string             `json:"duration"`
	Writes        int64              `json:"writes"`
	WriteFails    int64              `json:"write_failures"`
	Rows          int64              `json:"rows_sent"`
	Dirty         map[string]string  `json:"dirty_measurements,omitempty"`
	Totals        map[string]int     `json:"totals"`
	ByClass       map[string]int     `json:"by_class"`
	ByShape       map[string]*bucket `json:"by_shape_offset"`
	HourStart     *bucket            `json:"live_minutes_0_6,omitempty"`
	RawScanFails  []string           `json:"raw_scan_failures,omitempty"`
	Expansions    float64            `json:"file_pruning_expansions"`
	Passes        float64            `json:"file_pruning_passes"`
	Compacted     *float64           `json:"compaction_files_compacted,omitempty"`
	LiveHourExact int                `json:"exact_judgements_starting_in_live_hour"`
	MayMiss       int                `json:"judgements_allowed_to_miss_future_stamped_rows"`
	BracketZero   float64            `json:"bracket_lower_zero_fraction"`
	SelfTest      []*selfTestResult  `json:"self_test,omitempty"`
	crossedHour   bool
	failCount     int
	errCount      int
}

func buildReport(c config, j judge, rn *runner, g *generator, start time.Time, compacted float64, compKnown bool) *report {
	rep := &report{
		Mode: c.mode, ArcConfig: c.label, Started: start.UTC().Format(time.RFC3339),
		Duration: time.Since(start).Round(time.Second).String(),
		Writes:   g.writes.Load(), WriteFails: g.failures.Load(), Rows: g.seq.Load(),
		Totals: map[string]int{}, ByClass: map[string]int{}, ByShape: map[string]*bucket{},
		RawScanFails: rn.rawFail,
	}
	j.l.mu.RLock()
	if len(j.l.dirty) > 0 {
		rep.Dirty = map[string]string{}
		for m, w := range j.l.dirty {
			rep.Dirty[m] = w
		}
	}
	j.l.mu.RUnlock()
	if compKnown {
		rep.Compacted = &compacted
	}
	brackets, zero := 0, 0
	verdicts := settleAll(j, rn.records)
	for i, r := range rn.records {
		v := verdicts[i]
		rep.Totals[string(v.status)]++
		if v.class != "" {
			rep.ByClass[v.class+"/"+string(v.status)]++
		}
		rep.Expansions += r.expanded
		rep.Passes += r.passes
		key := r.q.shape + "/" + r.q.offset
		b := rep.ByShape[key]
		if b == nil {
			b = &bucket{}
			rep.ByShape[key] = b
		}
		tallyInto(b, r, v)
		if r.phase == "live" && r.send.Minute() < 7 {
			if rep.HourStart == nil {
				rep.HourStart = &bucket{}
			}
			tallyInto(rep.HourStart, r, v)
		}
		if v.class == "live-exact" {
			brackets++
			if v.exp.exactLo.count == 0 {
				zero++
			}
		}
		// Evidence for file pruning: an exact judgement over a window that
		// starts in the live hour and holds settled rows.
		if v.status == pass && strings.HasSuffix(v.class, "-exact") && v.exp.exactLo.count > 0 &&
			!r.q.start.IsZero() && !r.q.start.Before(r.send.Truncate(time.Hour)) {
			rep.LiveHourExact++
		}
		if r.phase == "live" && !r.send.Truncate(time.Hour).Equal(start.Truncate(time.Hour)) {
			rep.crossedHour = true
		}
		if v.mayMiss {
			rep.MayMiss++
		}
		if v.status == fail {
			rep.failCount++
		}
		if v.status == errored {
			rep.errCount++
		}
	}
	if brackets > 0 {
		rep.BracketZero = float64(zero) / float64(brackets)
	}
	return rep
}

func tallyInto(b *bucket, r record, v verdict) {
	switch v.status {
	case pass:
		b.Pass++
	case fail:
		b.Fail++
	case errored:
		b.Error++
	case skipped:
		b.Skip++
	case unavailable:
		b.Unavailable++
	}
	if (v.status == fail || v.status == errored) && len(b.Failures) < 3 {
		b.Failures = append(b.Failures, failure{SQL: r.sql, Wire: string(r.wire), Header: r.header, Why: v.why, Class: v.class, At: r.send.UTC().Format(time.RFC3339Nano)})
	}
}

// selfTest re-judges every passing answer against changed ledgers: rows
// above every real seq, rows below it (inside the exact part), and real rows
// removed (the upper bound). Each judgement whose expectation a change moves
// must now FAIL; one that still passes was not checking that part.
func selfTest(j judge, recs []record, start, end time.Time) []*selfTestResult {
	from, to := start.Add(-32*time.Hour), end.Add(20*time.Minute)
	// covers: the judgement classes a flavour must move. High phantoms have
	// seqs above every settled one, so only the whole count/sum answer sees
	// them; full results select settled rows only.
	countSum := func(c string) bool { return strings.HasSuffix(c, "-exact") }
	every := func(string) bool { return true }
	return selfTestWith(j, recs, []flavour{
		{name: "high-phantoms", l: j.l.withHighPhantoms(from, to), covers: countSum},
		{name: "low-phantoms", l: j.l.withLowPhantoms(from, to), covers: every},
		{name: "removed-rows", l: j.l.withoutRows(), covers: every},
	})
}

// flavour is one changed ledger and the classes it must move.
type flavour struct {
	name   string
	l      *ledger
	covers func(class string) bool
	// judgeWith replaces the judge under test (tests only; nil = j.judgeWith).
	judgeWith func(*ledger, record) verdict
}

// cannotFollow reports a change that moved an answer's acceptable values
// entirely off their old position: every answer that passed before is now
// out of range, so a correct judge must fail it. A change that only widens
// or narrows a bracket (a row in flight at the query, or between a relative
// window's earliest and latest start) is not one: the true answer may lie in
// both. This reads the two expectations only, never the answer, so it does
// not share code with the judgement it checks.
func cannotFollow(old, new expectation) bool {
	apart := func(oLo, oHi, nLo, nHi tally) bool {
		return nLo.count > oHi.count || nHi.count < oLo.count || nLo.sum > oHi.sum || nHi.sum < oLo.sum
	}
	return !sameRows(old.rows, new.rows) ||
		apart(old.exactLo, old.exactHi, new.exactLo, new.exactHi) ||
		apart(old.totLo, old.totHi, new.totLo, new.totHi)
}

func selfTestWith(j judge, recs []record, flavours []flavour) []*selfTestResult {
	var out []*selfTestResult
	verdicts := settleAll(j, recs)
	for _, f := range flavours {
		st := &selfTestResult{Flavour: f.name, ByClass: map[string]int{}}
		classes := map[string]bool{}
		judgeWith := f.judgeWith
		if judgeWith == nil {
			judgeWith = j.judgeWith
		}
		// required[i]: the change moved record i's acceptable answers off
		// their old position (cannotFollow); judged[i]: its answer against
		// the changed ledger. Computed in parallel.
		required := make([]bool, len(recs))
		widened := make([]bool, len(recs))
		judged := make([]verdict, len(recs))
		parallel(len(recs), func(i int) {
			if verdicts[i].status != pass {
				return
			}
			e := j.expect(f.l, recs[i])
			if verdicts[i].exp.equal(e) {
				return
			}
			if !cannotFollow(verdicts[i].exp, e) {
				widened[i] = true
				return
			}
			required[i] = true
			judged[i] = judgeWith(f.l, recs[i])
		})
		for i, r := range recs {
			v := verdicts[i]
			if v.status != pass {
				continue
			}
			classes[v.class] = true
			if widened[i] {
				st.Widened++
			}
			if !required[i] {
				continue
			}
			st.Required++
			st.ByClass[v.class]++
			if pv := judged[i]; pv.status != fail {
				st.Missed++
				if len(st.MissedFirst) < 5 {
					why := fmt.Sprintf("still %s: answer %v; before %s; after %s", pv.status, r.res.rows, brackets(v.exp), brackets(pv.exp))
					st.MissedFirst = append(st.MissedFirst, failure{SQL: r.sql, Wire: string(r.wire), Header: r.header, Why: why, Class: v.class, At: r.send.UTC().Format(time.RFC3339Nano)})
				}
			}
		}
		st.OK = st.Missed == 0 && st.Required > 0
		for c := range classes {
			if f.covers(c) && st.ByClass[c] == 0 {
				st.OK = false
				st.Note += fmt.Sprintf("class %s had no judgement this change moved; ", c)
			}
		}
		out = append(out, st)
	}
	return out
}

func brackets(e expectation) string {
	if e.rows != nil {
		return fmt.Sprintf("%d rows %v", len(e.rows), head(e.rows))
	}
	return fmt.Sprintf("exact [%d,%d]/[%d,%d] total [%d,%d]/[%d,%d]", e.exactLo.count, e.exactHi.count, e.exactLo.sum, e.exactHi.sum, e.totLo.count, e.totHi.count, e.totLo.sum, e.totHi.sum)
}

func finalVerdict(c config, rep *report) string {
	if rep.failCount > 0 || len(rep.RawScanFails) > 0 {
		rep.Reasons = append(rep.Reasons, fmt.Sprintf("%d wrong answers, %d raw-scan mismatches", rep.failCount, len(rep.RawScanFails)))
		return "FAIL"
	}
	for _, st := range rep.SelfTest {
		if !st.OK {
			rep.Reasons = append(rep.Reasons, "self-test "+st.Flavour+": the judge passed answers it should have failed")
			return "HARNESS-BROKEN"
		}
	}
	if rep.errCount > 0 {
		rep.Reasons = append(rep.Reasons, fmt.Sprintf("%d errors", rep.errCount))
		return "ERROR"
	}
	var why []string
	if len(rep.Dirty) > 0 {
		why = append(why, fmt.Sprintf("%d measurements had a failed write and were not judged", len(rep.Dirty)))
	}
	if total := rep.Totals[string(pass)] + rep.Totals[string(skipped)] + rep.Totals[string(unavailable)]; total > 0 {
		if f := float64(rep.Totals[string(skipped)]+rep.Totals[string(unavailable)]) / float64(total); f > 0.05 {
			why = append(why, fmt.Sprintf("%.0f%% of answers were skipped or unavailable", f*100))
		}
	}
	if c.mode == "live" && !rep.crossedHour {
		why = append(why, "the live run never crossed an hour boundary")
	}
	if c.expectFilePruning && rep.Expansions == 0 {
		why = append(why, "file-time pruning never expanded a glob")
	}
	if c.compaction && (rep.Compacted == nil || *rep.Compacted == 0) {
		why = append(why, "no compaction ran (or its metric is absent)")
	}
	if c.mode == "live" {
		if rep.LiveHourExact < 20 {
			why = append(why, fmt.Sprintf("only %d exact judgements started inside the live hour", rep.LiveHourExact))
		}
		if rep.BracketZero > 0.5 {
			why = append(why, fmt.Sprintf("%.0f%% of live judgements had no settled rows", rep.BracketZero*100))
		}
	}
	if len(why) > 0 {
		rep.Reasons = why
		return "INCONCLUSIVE"
	}
	return "PASS"
}

func printReport(rep *report) {
	fmt.Printf("\n== prune_diff %s: %s %v\n", rep.Mode, rep.Verdict, rep.Reasons)
	fmt.Printf("writes %d (failed %d), rows %d, totals %v\n", rep.Writes, rep.WriteFails, rep.Rows, rep.Totals)
	fmt.Printf("by class %v\n", rep.ByClass)
	fmt.Printf("file pruning: passes %.0f expansions %.0f; live-hour exact judgements %d; bracket lower=0 %.0f%%; allowed to miss future-stamped rows %d\n", rep.Passes, rep.Expansions, rep.LiveHourExact, rep.BracketZero*100, rep.MayMiss)
	if rep.Compacted != nil {
		fmt.Printf("compaction files compacted: %.0f\n", *rep.Compacted)
	}
	for _, st := range rep.SelfTest {
		fmt.Printf("self-test %s ok=%v required=%d missed=%d by class %v %s\n", st.Flavour, st.OK, st.Required, st.Missed, st.ByClass, st.Note)
		for _, m := range st.MissedFirst {
			fmt.Printf("  missed: [%s %s] %s\n", m.Wire, m.Class, m.SQL)
		}
	}

	keys := sortedKeys(rep.ByShape)
	sort.Strings(keys)
	for _, k := range keys {
		b := rep.ByShape[k]
		if b.Fail+b.Error == 0 {
			continue
		}
		fmt.Printf("  %-32s pass %d fail %d error %d\n", k, b.Pass, b.Fail, b.Error)
		for _, f := range b.Failures {
			fmt.Printf("     [%s header=%v %s] %s\n       %s\n", f.Wire, f.Header, f.Class, f.Why, f.SQL)
		}
	}
	for _, f := range rep.RawScanFails {
		fmt.Println("  raw:", f)
	}
}

// settleAll judges every record, in parallel: judging is linear in ledger
// entries per record, and a long live run has tens of thousands of each.
func settleAll(j judge, recs []record) []verdict {
	out := make([]verdict, len(recs))
	parallel(len(recs), func(i int) { out[i] = j.settle(recs[i]) })
	return out
}

func parallel(n int, f func(i int)) {
	var wg sync.WaitGroup
	next := make(chan int)
	for w := 0; w < runtime.NumCPU(); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range next {
				f(i)
			}
		}()
	}
	for i := 0; i < n; i++ {
		next <- i
	}
	close(next)
	wg.Wait()
}
