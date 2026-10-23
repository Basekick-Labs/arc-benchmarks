// prune_diff proves Arc's time-range pruning (partition pruning and
// query.file_time_pruning) never changes a query's answer (#659).
//
// It writes rows with globally unique seq values, keeps a ledger of every row
// Arc acknowledged, and judges each query's count(*) and sum(seq) (and a few
// full results) against that ledger. Every count/sum query also selects its
// exact part: the rows with seq <= N, where N is the newest seq acked a
// visibility lag before the query was sent. That part must equal the ledger
// exactly; the whole answer lies between the rows certainly visible and the
// rows sent by the time the answer came back.
//
//   - frozen: ingest for --duration, stop, flush, then run the corpus: every
//     answer is exact (plus, with --data-root, an in-process DuckDB scan of
//     the measurement's files);
//   - live: the corpus runs while ingest continues and across an hour
//     boundary (--compaction adds a measurement that compacts meanwhile).
//
// With --expect-file-pruning, rows stamped more than the margin after their
// flush may be missing while their hour is live: file-time pruning's
// documented contract, counted in the report, never excused in the exact part.
//
// Afterwards every passing answer is judged again against three changed
// ledgers (phantom rows above and below every real seq, every seventh row
// removed). Each judgement the change moves off its old acceptable values
// must then FAIL, or the verdict is HARNESS-BROKEN (--self-test, on by
// default). Verdicts: FAIL > HARNESS-BROKEN > ERROR > INCONCLUSIVE > PASS.
//
//	go run . --arc http://localhost:8000 --mode frozen --duration 5m
//	go run . --arc http://localhost:8000 --mode live --duration 75m --expect-file-pruning
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"time"
)

type config struct {
	arcURL, token, compactorURL string
	db                          string
	mode                        string
	duration                    time.Duration
	interval                    time.Duration
	heavyBatch                  int
	lag                         time.Duration
	dataRoot                    string
	report                      string
	seed                        int64
	label                       string
	compaction                  bool
	compactionFiles             int
	expectFilePruning           bool
	margin                      time.Duration
	selfTest                    bool
	waitForHour                 bool
	// M5 (cluster): queries to these readers instead of --arc; writes and
	// flushes stay on --arc.
	queryURLs []string
	// Quiescent failover: at minute failoverAt of the first hour after
	// start+10m, pause writes, barrier, run failoverCmd, resume on arcAfter.
	failoverAt  int
	failoverCmd string
	arcAfter    string
	// DELETE-concurrent check on md, one DELETE every deleteEvery (0: off).
	deleteEvery time.Duration
}

func main() {
	var c config
	flag.StringVar(&c.arcURL, "arc", "http://localhost:8000", "Arc base URL (the node that receives writes and queries)")
	flag.StringVar(&c.token, "token", os.Getenv("ARC_TOKEN"), "API token (admin: the flush barrier needs it)")
	flag.StringVar(&c.compactorURL, "compactor-url", "", "node whose /metrics count compactions (default --arc)")
	flag.StringVar(&c.db, "db", "prune_diff", "database (must have no retention policy or continuous query)")
	flag.StringVar(&c.mode, "mode", "frozen", "frozen or live")
	flag.DurationVar(&c.duration, "duration", 10*time.Minute, "ingest duration (frozen) or run duration (live; must cross an hour)")
	flag.DurationVar(&c.interval, "interval", 300*time.Millisecond, "one write per measurement every interval")
	flag.IntVar(&c.heavyBatch, "heavy-batch", 600, "rows per write of the heavy measurement (>= ingest.max_buffer_size)")
	flag.DurationVar(&c.lag, "visibility-lag", 4*time.Second, "ingest.max_buffer_age_ms + 3 s: a row acked this long before a query is certainly visible")
	flag.StringVar(&c.dataRoot, "data-root", "", "Arc's local storage root (enables the raw-scan reference; compaction must be off)")
	flag.StringVar(&c.report, "report", "prune_diff_report.json", "JSON report path")
	flag.Int64Var(&c.seed, "seed", 1, "random seed")
	flag.StringVar(&c.label, "arc-config", "", "free-text label of the Arc config under test (self-reported, recorded in the report)")
	flag.BoolVar(&c.compaction, "compaction", false, "compaction-on run: seed the compaction measurement and require compactions")
	flag.IntVar(&c.compactionFiles, "compaction-files", 3, "files per compaction hour (>= compaction.hourly_min_files)")
	flag.DurationVar(&c.margin, "margin", 300*time.Second, "query.file_time_pruning_margin_seconds of the Arc under test")
	flag.BoolVar(&c.expectFilePruning, "expect-file-pruning", false, "Arc runs with query.file_time_pruning on: zero expansions is INCONCLUSIVE")
	flag.BoolVar(&c.selfTest, "self-test", true, "re-judge every answer against phantom rows; each must FAIL")
	flag.BoolVar(&c.waitForHour, "wait", true, "wait for a run shape that proves something instead of refusing")
	queryURL := flag.String("query-url", "", "comma-separated name=url readers to query (default: --arc); each answer is judged on its own")
	flag.IntVar(&c.failoverAt, "failover-at", -1, "live: minute of the hour to kill the writer (needs --failover-cmd and --arc-after)")
	flag.StringVar(&c.failoverCmd, "failover-cmd", "", "shell command that kills the writer")
	flag.StringVar(&c.arcAfter, "arc-after", "", "the writer to resume writes on after the failover")
	flag.DurationVar(&c.deleteEvery, "delete-every", 0, "live: DELETE one hour of measurement md every interval while querying its total on every reader")
	flag.Parse()
	for _, kv := range strings.Split(*queryURL, ",") {
		if kv = strings.TrimSpace(kv); kv != "" {
			c.queryURLs = append(c.queryURLs, kv)
		}
	}
	if c.compactorURL == "" {
		c.compactorURL = c.arcURL
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	os.Exit(run(ctx, c))
}

// foResult is the failover's outcome, set by its goroutine before bg.Wait.
var foResult *failoverResult

func run(ctx context.Context, c config) int {
	if c.compaction && c.dataRoot != "" {
		fmt.Println("refusing: the raw scan double counts while compaction runs (drop --data-root or --compaction)")
		return 2
	}
	if c.mode == "live" && c.duration < 70*time.Minute && c.compaction {
		fmt.Println("refusing: a compaction-on run must last more than 70 minutes after the seed (hourly compaction skips hours with files newer than 1 h)")
		return 2
	}
	if c.failoverAt >= 0 && (c.mode != "live" || c.failoverCmd == "" || c.arcAfter == "") {
		fmt.Println("refusing: --failover-at needs --mode live, --failover-cmd and --arc-after")
		return 2
	}
	arc := newArcClient(c.arcURL, c.token)
	l := newLedger()
	g := &generator{db: c.db, ledger: l}
	g.w.Store(arc)
	rn := &runner{cfg: c, writer: &g.w}
	if len(c.queryURLs) == 0 {
		rn.readers = []reader{{name: "arc", c: arc}}
	}
	for i, kv := range c.queryURLs {
		name, u, ok := strings.Cut(kv, "=")
		if !ok {
			name, u = fmt.Sprintf("r%d", i), kv
		}
		rn.readers = append(rn.readers, reader{name: name, c: newArcClient(u, c.token)})
	}
	if err := precheck(ctx, arc, c.db); err != nil {
		fmt.Println("refusing:", err)
		return 2
	}
	if c.dataRoot != "" {
		db, err := sql.Open("duckdb", "")
		if err != nil {
			fmt.Println("duckdb:", err)
			return 2
		}
		defer db.Close()
		if _, err := db.Exec("SET TimeZone='UTC'"); err != nil {
			fmt.Println("duckdb:", err)
			return 2
		}
		rn.raw = db
	}
	if err := waitForShape(ctx, c); err != nil {
		fmt.Println("refusing:", err)
		return 2
	}

	off, tol, err := rn.clockOffset(ctx)
	if err != nil {
		fmt.Println(err)
		return 2
	}
	fmt.Printf("clock offset %v ± %v\n", off, tol)

	rng := rand.New(rand.NewSource(c.seed))
	ms := []string{"m0", "m1", "m2"}
	heavy := "m0"
	start := time.Now()
	for _, m := range ms {
		g.seed(ctx, m, start, rng)
	}
	if c.compaction {
		if err := g.seedCompaction(ctx, "mc", start, c.compactionFiles, rng); err != nil {
			fmt.Println(err)
			return 2
		}
	}
	all := append([]string(nil), ms...)
	if c.compaction {
		all = append(all, "mc")
	}
	var dc *deleteCell
	if c.deleteEvery > 0 {
		dc = &deleteCell{}
		if err := dc.seed(ctx, g, start, rng); err != nil {
			fmt.Println(err)
			return 2
		}
	}
	compBefore, _ := newArcClient(c.compactorURL, c.token).metrics(ctx, "arc_compaction_files_compacted_total")

	pairs := map[string]string{"m0": "m1", "m1": "m2", "m2": "m0", "mc": ""}
	j := judge{l: l, lag: c.lag, off: off, tol: tol}
	if c.expectFilePruning {
		j.margin = c.margin
	}
	switch c.mode {
	case "frozen":
		ictx, cancel := context.WithTimeout(ctx, c.duration)
		g.run(ictx, ms, heavy, c.interval, c.heavyBatch, c.seed)
		cancel()
		if err := rn.barrier(ctx, l, all); err != nil {
			fmt.Println(err)
			return 2
		}
		ref := time.Now()
		for _, m := range all {
			for _, q := range buildCorpus(m, pairs[m], ref) {
				rn.exec(ctx, l, q, "frozen")
				rn.rawCheck(l, q)
			}
		}
	case "live":
		// The seed's rows on disk before the first query: a measurement
		// with no file yet answers count(*) with no row at all.
		seeded := all
		if dc != nil {
			seeded = append(append([]string(nil), all...), deleteMeasurement)
		}
		if err := rn.barrier(ctx, l, seeded); err != nil {
			fmt.Println(err)
			return 2
		}
		ictx, cancel := context.WithTimeout(ctx, c.duration)
		done := make(chan struct{})
		go func() { g.run(ictx, ms, heavy, c.interval, c.heavyBatch, c.seed); close(done) }()
		var bg sync.WaitGroup
		if c.failoverAt >= 0 {
			bg.Add(1)
			go func() { defer bg.Done(); foResult = failover(ictx, c, g, rn, l, all, start) }()
		}
		if dc != nil {
			bg.Add(1)
			go func() { defer bg.Done(); dc.run(ictx, g, c.db, c.deleteEvery) }()
		}
		for ictx.Err() == nil {
			ref := time.Now()
			for _, m := range all {
				for _, q := range buildCorpus(m, pairs[m], ref) {
					if ictx.Err() != nil {
						break
					}
					// Count/sum only while ingest runs: a full result is
					// judged only when its row set is settled.
					rn.exec(ctx, l, q, "live")
				}
				if dc != nil {
					dc.check(ctx, rn)
				}
			}
		}
		cancel()
		<-done
		bg.Wait()
	default:
		fmt.Println("unknown --mode", c.mode)
		return 2
	}
	compAfter, _ := newArcClient(c.compactorURL, c.token).metrics(ctx, "arc_compaction_files_compacted_total")
	compacted, compKnown := delta(compBefore, compAfter, "arc_compaction_files_compacted_total")

	rep := buildReport(c, j, rn, g, start, compacted, compKnown)
	rep.Failover = foResult
	if dc != nil {
		rep.Deletes = dc.judge(c.lag)
	}
	if c.selfTest {
		rep.SelfTest = selfTest(j, rn.records, start, time.Now())
	}
	rep.Verdict = finalVerdict(c, rep)
	b, _ := json.MarshalIndent(rep, "", "  ")
	_ = os.WriteFile(c.report, b, 0o644)
	printReport(rep)
	if rep.Verdict != "PASS" {
		return 1
	}
	return 0
}

func delta(before, after map[string]float64, name string) (float64, bool) {
	a, okA := after[name]
	b, okB := before[name]
	if !okA || !okB {
		return 0, false // absent, not zero
	}
	return a - b, true
}

// precheck refuses a database with a retention policy or continuous query:
// either would change its rows under the ledger.
func precheck(ctx context.Context, arc *arcClient, db string) error {
	for _, p := range []string{"/api/v1/retention", "/api/v1/continuous_queries"} {
		resp, err := arc.do(ctx, "GET", p, nil, nil)
		if err != nil {
			return err
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		body := string(raw)
		if resp.StatusCode == 200 && strings.Contains(body, `"`+db+`"`) {
			return fmt.Errorf("%s lists a policy or query for database %q", p, db)
		}
	}
	res := arc.query(ctx, wireJSON, fmt.Sprintf("SELECT count(*) FROM %s.m0", db), "", false)
	if res.err == nil && len(res.rows) == 1 && res.rows[0][0] != "0" {
		return fmt.Errorf("database %q already holds rows: use a fresh database or a fresh Arc", db)
	}
	return nil
}

// waitForShape makes the run prove what it is for (plan revision 17).
// Frozen: ingest must end at least 15 minutes before the hour turns, so the
// live hour is still live when the corpus runs. Live: the run must cross an
// hour boundary with time to spare on both sides.
func waitForShape(ctx context.Context, c config) error {
	for {
		now := time.Now()
		next := now.Truncate(time.Hour).Add(time.Hour)
		var ok bool
		switch c.mode {
		case "frozen":
			ok = now.Add(c.duration).Add(15 * time.Minute).Before(next)
		case "live":
			end := now.Add(c.duration)
			ok = end.After(next.Add(7*time.Minute)) && next.Sub(now) > 5*time.Minute
		default:
			return nil
		}
		if ok {
			return nil
		}
		if !c.waitForHour {
			return fmt.Errorf("a %s run of %v starting now does not have the shape it needs (see --help)", c.mode, c.duration)
		}
		wait := next.Add(time.Minute).Sub(now)
		if c.mode == "live" && c.duration < time.Hour+12*time.Minute {
			return fmt.Errorf("--duration %v cannot cross an hour with margin; use at least 72m", c.duration)
		}
		fmt.Printf("waiting %v for a run shape that proves something\n", wait.Round(time.Second))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
}

func sortedKeys[V any](m map[string]V) []string {
	k := make([]string, 0, len(m))
	for x := range m {
		k = append(k, x)
	}
	sort.Strings(k)
	return k
}
