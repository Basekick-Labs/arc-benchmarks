package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// scenario picks a row timestamp relative to now.
type scenario struct {
	name   string
	weight int
	ts     func(now time.Time, rng *rand.Rand) time.Time
}

// futureOffsets stay clear of the 300 s file-pruning margin on both sides of
// the edge a regression would sit on (plan revision 3).
var futureOffsets = []time.Duration{60 * time.Second, 299 * time.Second, 305 * time.Second, 330 * time.Second, 900 * time.Second}

var scenarios = []scenario{
	{"steady", 70, func(now time.Time, _ *rand.Rand) time.Time { return now }},
	{"late", 15, func(now time.Time, rng *rand.Rand) time.Time {
		return now.Add(-time.Duration(rng.Int63n(int64(50 * time.Minute))))
	}},
	{"backfill", 8, func(now time.Time, rng *rand.Rand) time.Time {
		h := 2 + rng.Intn(29)
		return now.Add(-time.Duration(h)*time.Hour - time.Duration(rng.Int63n(int64(time.Hour))))
	}},
	{"future", 7, func(now time.Time, rng *rand.Rand) time.Time {
		return now.Add(futureOffsets[rng.Intn(len(futureOffsets))])
	}},
}

func pickScenario(rng *rand.Rand) scenario {
	total := 0
	for _, s := range scenarios {
		total += s.weight
	}
	n := rng.Intn(total)
	for _, s := range scenarios {
		if n < s.weight {
			return s
		}
		n -= s.weight
	}
	return scenarios[0]
}

// generator writes rows to Arc and records them in the ledger.
type generator struct {
	arc      *arcClient
	db       string
	ledger   *ledger
	seq      atomic.Int64
	writes   atomic.Int64
	failures atomic.Int64
}

const hosts = 5

// batch builds n rows for measurement m, timestamps from ts.
func (g *generator) batch(m string, n int, ts func() time.Time, rng *rand.Rand) ([]*row, []byte) {
	rs := make([]*row, n)
	var buf bytes.Buffer
	now := time.Now()
	for i := range rs {
		t := ts().UTC().Truncate(time.Microsecond)
		r := &row{seq: g.seq.Add(1), tsUs: t.UnixMicro(), host: rng.Intn(hosts), fut: t.After(now)}
		rs[i] = r
		// end_time is two hours after the row's time: a column whose name
		// ends in "time" that an old time-range reader took for the bound.
		fut := 0
		if r.fut {
			fut = 1
		}
		fmt.Fprintf(&buf, "%s,host=h%d seq=%di,fut=%di,v=%s,end_time=\"%s\" %d\n", m, r.host, r.seq, fut, strconv.FormatFloat(rng.Float64()*100, 'f', 3, 64), lit(t.Add(2*time.Hour)), r.tsUs)
	}
	return rs, buf.Bytes()
}

// write sends one batch; the ledger learns it before the request goes out,
// so a reader never misses an in-flight row.
func (g *generator) write(ctx context.Context, m string, rs []*row, body []byte) {
	sent := time.Now()
	for _, r := range rs {
		r.sentAt = sent
	}
	g.ledger.sent(m, rs)
	code, msg, err := g.arc.writeLP(ctx, g.db, body)
	g.writes.Add(1)
	if err != nil || code != http.StatusNoContent {
		g.failures.Add(1)
		g.ledger.failed(m, fmt.Sprintf("write failed: code=%d err=%v %s", code, err, msg))
		return
	}
	g.ledger.acked(rs, time.Now())
}

// seed writes one row into every hour the scenarios target (now-31h to
// now+15m), so no query meets an hour whose absence Arc cached before the
// generator first wrote into it (plan revision 4).
func (g *generator) seed(ctx context.Context, m string, now time.Time, rng *rand.Rand) {
	var ts []time.Time
	for h := 31; h >= 0; h-- {
		at := now.Add(-time.Duration(h) * time.Hour)
		// The row inside the hour, and one exactly on its first instant
		// (an inclusive end on an hour edge must find it).
		ts = append(ts, at, at.Truncate(time.Hour))
	}
	for _, off := range futureOffsets {
		ts = append(ts, now.Add(off))
	}
	// The next hour too: a late future-stamped write lands there before it
	// is live, and that hour's absence must not have been cached.
	ts = append(ts, now.Truncate(time.Hour).Add(time.Hour+30*time.Second))
	i := 0
	rs, body := g.batch(m, len(ts), func() time.Time { t := ts[i]; i++; return t }, rng)
	g.write(ctx, m, rs, body)
}

// run writes until ctx is done: every normal measurement 2 rows per write,
// one write every interval; the heavy one heavyBatch rows, 3 writes a second,
// so its files come from the buffer-size trigger (plan revision 2).
func (g *generator) run(ctx context.Context, ms []string, heavy string, interval time.Duration, heavyBatch int, seed int64) {
	var wg sync.WaitGroup
	for i, m := range ms {
		wg.Add(1)
		go func(i int, m string) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed + int64(i)))
			n, every := 2, interval
			if m == heavy {
				n, every = heavyBatch, time.Second/3
			}
			tick := time.NewTicker(every)
			defer tick.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
				}
				now := time.Now()
				ts := func() time.Time { return pickScenario(rng).ts(now, rng) }
				if m == heavy {
					// Steady only: its job is to reach the file cap in the live
					// hour, and a batch this size spread over 30 hours writes a
					// file into each of them on every flush (tens of thousands
					// of files in minutes, none of them the cap's).
					ts = func() time.Time { return now }
				}
				rs, body := g.batch(m, n, ts, rng)
				g.write(ctx, m, rs, body)
			}
		}(i, m)
	}
	wg.Wait()
}

func drain(r io.Reader) string {
	b, _ := io.ReadAll(io.LimitReader(r, 512))
	return string(b)
}

// seedCompaction writes the compaction measurement's hours (3 to 6 hours
// back) in flushes separate writes, so each hour holds at least files files
// for hourly compaction to merge. Nothing writes those hours again.
func (g *generator) seedCompaction(ctx context.Context, m string, now time.Time, files int, rng *rand.Rand) error {
	for f := 0; f < files; f++ {
		for h := 3; h <= 6; h++ {
			base := now.Add(-time.Duration(h) * time.Hour).Truncate(time.Hour)
			rs, body := g.batch(m, 20, func() time.Time { return base.Add(time.Duration(rng.Int63n(int64(time.Hour)))) }, rng)
			g.write(ctx, m, rs, body)
		}
		if err := g.arc.flush(ctx); err != nil {
			return err
		}
		time.Sleep(1100 * time.Millisecond) // distinct flush file names
	}
	if why, dirty := g.ledger.isDirty(m); dirty {
		return fmt.Errorf("compaction seed: %s", why)
	}
	return nil
}
