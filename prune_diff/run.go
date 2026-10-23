package main

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

var pruneMetrics = []string{"arc_query_file_time_pruning_", "arc_ingest_future_stamped_rows_total"}

// runner executes queries one at a time and records them.
type runner struct {
	cfg     config
	arc     *arcClient
	records []record
	raw     *sql.DB // raw-scan reference, nil without --data-root
	rawFail []string
}

func (rn *runner) tableRef(header bool) func(string) string {
	if header {
		return func(m string) string { return m }
	}
	return func(m string) string { return rn.cfg.db + "." + m }
}

// exec runs q over every wire and both table forms.
func (rn *runner) exec(ctx context.Context, q query, phase string) {
	for _, header := range []bool{true, false} {
		sqlText := q.from(rn.tableRef(header))
		wires := []wire{wireJSON}
		if q.kind == kindCountSum {
			wires = append(wires, wireArrow, wireMsgPack)
		}
		for _, w := range wires {
			before, _ := rn.arc.metrics(ctx, pruneMetrics...)
			send := time.Now()
			res := rn.arc.query(ctx, w, sqlText, rn.cfg.db, header)
			if res.status == 503 && res.retryAfter {
				time.Sleep(time.Second)
				send = time.Now()
				res = rn.arc.query(ctx, w, sqlText, rn.cfg.db, header)
			}
			recv := time.Now()
			if w == wireMsgPack && res.status == 501 {
				continue // built without the msgpack endpoint
			}
			after, _ := rn.arc.metrics(ctx, pruneMetrics...)
			rn.records = append(rn.records, record{
				q: q, wire: w, header: header, sql: sqlText, res: res, send: send, recv: recv, phase: phase,
				expanded: after["arc_query_file_time_pruning_expanded_total"] - before["arc_query_file_time_pruning_expanded_total"],
				passes:   after["arc_query_file_time_pruning_passes_total"] - before["arc_query_file_time_pruning_passes_total"],
			})
		}
	}
}

// clockOffset measures Arc's clock against ours: Arc's now() minus the
// midpoint of the request, with half the round trip as its uncertainty.
func (rn *runner) clockOffset(ctx context.Context) (time.Duration, time.Duration, error) {
	best := time.Duration(1 << 62)
	var off time.Duration
	for i := 0; i < 5; i++ {
		send := time.Now()
		res := rn.arc.query(ctx, wireJSON, "SELECT epoch_us(now())", "", false)
		recv := time.Now()
		if res.err != nil || len(res.rows) != 1 {
			return 0, 0, fmt.Errorf("clock probe: %v %v", res.err, res.rows)
		}
		us, err := strconv.ParseInt(res.rows[0][0], 10, 64)
		if err != nil {
			return 0, 0, err
		}
		rtt := recv.Sub(send)
		if rtt < best {
			best = rtt
			off = time.UnixMicro(us).Sub(send.Add(rtt / 2))
		}
	}
	return off, best/2 + 5*time.Millisecond, nil
}

// barrier flushes Arc and waits until an unpruned count of every
// measurement equals the ledger (plan revision 5).
func (rn *runner) barrier(ctx context.Context, l *ledger, ms []string) error {
	deadline := time.Now().Add(2 * time.Minute)
	for _, m := range ms {
		want := l.all(m, func(*row) bool { return true })
		for {
			_ = rn.arc.flush(ctx)
			res := rn.arc.query(ctx, wireJSON, fmt.Sprintf("SELECT %s FROM %s.%s", countSum, rn.cfg.db, m), "", false)
			if res.err == nil && len(res.rows) == 1 && res.rows[0][0] == strconv.FormatInt(want.count, 10) && res.rows[0][1] == strconv.FormatInt(want.sum, 10) {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("barrier: %s never reached the ledger (count=%d sum=%d), last answer %v %v", m, want.count, want.sum, res.rows, res.err)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
	return nil
}

// rawCheck runs a frozen count/sum query over the measurement's files with an
// in-process DuckDB and compares it with the ledger: a third, pruning-free
// answer (plan revision 15).
func (rn *runner) rawCheck(l *ledger, q query) {
	if rn.raw == nil || q.kind != kindCountSum || q.relative {
		return
	}
	ref := func(m string) string {
		return fmt.Sprintf("read_parquet('%s/%s/%s/**/*.parquet', union_by_name=true)", strings.ReplaceAll(rn.cfg.dataRoot, "'", "''"), rn.cfg.db, m)
	}
	sqlText := q.from(ref)
	var c, s int64
	if err := rn.raw.QueryRow(sqlText).Scan(&c, &s); err != nil {
		rn.rawFail = append(rn.rawFail, fmt.Sprintf("%s/%s: raw scan error %v", q.shape, q.offset, err))
		return
	}
	want := l.all(q.meas, q.sel(0))
	if q.meas2 != "" {
		want = plus(want, l.all(q.meas2, q.sel2))
	}
	if (tally{c, s}) != want {
		rn.rawFail = append(rn.rawFail, fmt.Sprintf("%s/%s: raw scan count=%d sum=%d, ledger count=%d sum=%d: %s", q.shape, q.offset, c, s, want.count, want.sum, sqlText))
	}
}
