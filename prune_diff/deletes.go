package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// deleteCell is the DELETE-concurrent-with-queries check (plan L5): a
// measurement, md, kept out of the ledger judge. Its hours are DELETEd one
// at a time while every reader is asked for its total, and each answer must
// be the total before or after a DELETE that could be visible to it, or a
// retryable 503. An empty 200 or any other error is a FAIL.
type deleteCell struct {
	mu      sync.Mutex
	hours   []time.Time // seeded hours, oldest first
	perHour []tally
	deletes []deleteStep
	checks  []deleteCheck
}

type deleteStep struct {
	hour       time.Time
	sent, done time.Time // done is zero until the DELETE answered 200
}

type deleteCheck struct {
	node       string
	send, recv time.Time
	status     int
	count, sum int64
	empty      bool
	err        string
}

type deleteReport struct {
	Deletes     int      `json:"deletes"`
	Checks      int      `json:"checks"`
	Pass        int      `json:"pass"`
	Unavailable int      `json:"unavailable_503"`
	Fail        int      `json:"fail"`
	Failures    []string `json:"failures,omitempty"`
}

const deleteMeasurement = "md"

// seed writes 50 rows into each of hours 3..10 back and flushes.
func (d *deleteCell) seed(ctx context.Context, g *generator, now time.Time, rng *rand.Rand) error {
	for h := 10; h >= 3; h-- {
		at := now.Add(-time.Duration(h) * time.Hour).Truncate(time.Hour).Add(7 * time.Minute)
		i := 0
		rs, body := g.batch(deleteMeasurement, 50, func() time.Time { i++; return at.Add(time.Duration(i) * time.Second) }, rng)
		var t tally
		for _, r := range rs {
			t.add(r)
		}
		g.write(ctx, deleteMeasurement, rs, body)
		if _, dirty := g.ledger.isDirty(deleteMeasurement); dirty {
			return fmt.Errorf("seeding %s failed", deleteMeasurement)
		}
		d.hours = append(d.hours, at.Truncate(time.Hour))
		d.perHour = append(d.perHour, t)
	}
	return g.w.Load().flush(ctx)
}

// total is md's total after the first k deletes.
func (d *deleteCell) total(k int) tally {
	var t tally
	for i := k; i < len(d.perHour); i++ {
		t = plus(t, d.perHour[i])
	}
	return t
}

// run DELETEs the oldest hours, one every interval, keeping the newest two.
func (d *deleteCell) run(ctx context.Context, g *generator, db string, every time.Duration) {
	for k := 0; k < len(d.hours)-2; k++ {
		select {
		case <-ctx.Done():
			return
		case <-time.After(every):
		}
		h := d.hours[k]
		where := fmt.Sprintf("time >= '%s' AND time < '%s'", lit(h), lit(h.Add(time.Hour)))
		body, _ := json.Marshal(map[string]any{"database": db, "measurement": deleteMeasurement, "where": where, "confirm": true})
		d.mu.Lock()
		d.deletes = append(d.deletes, deleteStep{hour: h, sent: time.Now()})
		d.mu.Unlock()
		g.pause.RLock()
		resp, err := g.w.Load().do(ctx, "POST", "/api/v1/delete", body, map[string]string{"Content-Type": "application/json"})
		g.pause.RUnlock()
		if err != nil {
			fmt.Println("delete:", err)
			return // the step stays not-done: later answers may show either total
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			fmt.Println("delete: status", resp.StatusCode)
			return
		}
		d.mu.Lock()
		d.deletes[len(d.deletes)-1].done = time.Now()
		d.mu.Unlock()
	}
}

// check asks every reader for md's total.
func (d *deleteCell) check(ctx context.Context, rn *runner) {
	for _, rd := range rn.readers {
		send := time.Now()
		res := rd.c.query(ctx, wireJSON, fmt.Sprintf("SELECT count(*), coalesce(sum(seq),0)::BIGINT FROM %s.%s", rn.cfg.db, deleteMeasurement), "", false)
		ck := deleteCheck{node: rd.name, send: send, recv: time.Now(), status: res.status}
		switch {
		case res.err != nil && res.status != 503:
			ck.err = res.err.Error()
		case res.status == 200 && len(res.rows) == 0:
			ck.empty = true
		case res.status == 200:
			ck.count, _ = strconv.ParseInt(res.rows[0][0], 10, 64)
			ck.sum, _ = strconv.ParseInt(res.rows[0][1], 10, 64)
		}
		d.mu.Lock()
		d.checks = append(d.checks, ck)
		d.mu.Unlock()
	}
}

// judge: an answer sent at s and received at r may show the total after k
// deletes for k from the number done by s - lag to the number sent by r.
func (d *deleteCell) judge(lag time.Duration) *deleteReport {
	d.mu.Lock()
	defer d.mu.Unlock()
	rep := &deleteReport{Deletes: len(d.deletes), Checks: len(d.checks)}
	for _, ck := range d.checks {
		lo, hi := 0, 0
		for _, st := range d.deletes {
			if !st.done.IsZero() && !st.done.After(ck.send.Add(-lag)) {
				lo++
			}
			if !st.sent.After(ck.recv) {
				hi++
			}
		}
		ok := false
		if ck.status == 200 && !ck.empty && ck.err == "" {
			for k := lo; k <= hi; k++ {
				if t := d.total(k); t.count == ck.count && t.sum == ck.sum {
					ok = true
				}
			}
		}
		switch {
		case ok:
			rep.Pass++
		case ck.status == 503:
			rep.Unavailable++
		default:
			rep.Fail++
			if len(rep.Failures) < 10 {
				rep.Failures = append(rep.Failures, fmt.Sprintf("%s at %s: status %d empty=%v count=%d sum=%d err=%q; allowed deletes %d..%d (totals %v..%v)",
					ck.node, ck.send.UTC().Format(time.RFC3339Nano), ck.status, ck.empty, ck.count, ck.sum, ck.err, lo, hi, d.total(lo), d.total(hi)))
			}
		}
	}
	return rep
}
