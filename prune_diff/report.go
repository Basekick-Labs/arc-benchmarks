package main

import (
	"fmt"
	"sort"
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
	OK          bool           `json:"ok"`
	Phantoms    int64          `json:"phantoms_per_measurement"`
	Required    int            `json:"required_failures"`
	Missed      int            `json:"missed"`
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
	SelfTest      *selfTestResult    `json:"self_test,omitempty"`
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
	for _, r := range rn.records {
		v := j.settle(r)
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
		if v.class == "live-bracket" {
			brackets++
			if v.lower.count == 0 {
				zero++
			}
		}
		if v.class == "live-exact" && v.status == pass && !r.q.start.IsZero() && !r.q.start.Before(r.send.Truncate(time.Hour)) {
			rep.LiveHourExact++
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

// selfTest re-judges every passing answer against the ledger plus phantoms.
// Each judgement whose expected answer the phantoms change must now FAIL; one
// that still passes is a judgement that was not checking anything.
func selfTest(j judge, recs []record, start, end time.Time) *selfTestResult {
	ph, n := j.l.withPhantoms(start.Add(-32*time.Hour), end.Add(20*time.Minute), start)
	pj := j
	pj.l = ph
	st := &selfTestResult{Phantoms: n, ByClass: map[string]int{}}
	classes := map[string]bool{}
	for _, r := range recs {
		v := j.settle(r)
		if v.status != pass {
			continue
		}
		classes[v.class] = true
		if !phantomIn(j.l, ph, r, j) {
			continue
		}
		st.Required++
		st.ByClass[v.class]++
		if pv := pj.settle(r); pv.status != fail {
			st.Missed++
			if len(st.MissedFirst) < 5 {
				st.MissedFirst = append(st.MissedFirst, failure{SQL: r.sql, Wire: string(r.wire), Header: r.header, Why: "still " + string(pv.status) + " with phantoms", Class: v.class})
			}
		}
	}
	st.OK = st.Missed == 0 && st.Required > 0
	for c := range classes {
		if st.ByClass[c] == 0 {
			st.OK = false
			st.Note += fmt.Sprintf("class %s had no judgement the phantoms changed; ", c)
		}
	}
	return st
}

func finalVerdict(c config, rep *report) string {
	if rep.failCount > 0 || len(rep.RawScanFails) > 0 {
		rep.Reasons = append(rep.Reasons, fmt.Sprintf("%d wrong answers, %d raw-scan mismatches", rep.failCount, len(rep.RawScanFails)))
		return "FAIL"
	}
	if rep.SelfTest != nil && !rep.SelfTest.OK {
		rep.Reasons = append(rep.Reasons, "self-test: the judge passed answers it should have failed")
		return "HARNESS-BROKEN"
	}
	if rep.errCount > 0 {
		rep.Reasons = append(rep.Reasons, fmt.Sprintf("%d errors", rep.errCount))
		return "ERROR"
	}
	var why []string
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
			why = append(why, fmt.Sprintf("%.0f%% of bracket judgements had a lower bound of 0", rep.BracketZero*100))
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
	if rep.SelfTest != nil {
		fmt.Printf("self-test ok=%v required=%d missed=%d by class %v %s\n", rep.SelfTest.OK, rep.SelfTest.Required, rep.SelfTest.Missed, rep.SelfTest.ByClass, rep.SelfTest.Note)
		for _, m := range rep.SelfTest.MissedFirst {
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
