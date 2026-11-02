package main

import (
	"context"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"time"
)

// failoverResult is what the report says about a failover.
type failoverResult struct {
	At       string `json:"at"`
	Cmd      string `json:"cmd"`
	Downtime string `json:"writes_paused"`
	Err      string `json:"error,omitempty"`
}

// failover kills the writer at minute c.failoverAt of the first hour after
// start + 10 minutes, quiescently: writes pause, every acked row is made
// visible on every reader, then c.failoverCmd runs, and writes resume on
// c.arcAfter once it accepts one. No write is in flight across the kill, so
// the ledger stays exact; what the cell tests is the reader's pruning across
// the file-set change a failover causes, not write durability (#1264's job).
func failover(ctx context.Context, c config, g *generator, rn *runner, l *ledger, ms []string, start time.Time) *failoverResult {
	at := start.Add(10 * time.Minute).Truncate(time.Hour).Add(time.Hour + time.Duration(c.failoverAt)*time.Minute)
	select {
	case <-ctx.Done():
		return nil
	case <-time.After(time.Until(at)):
	}
	res := &failoverResult{At: time.Now().UTC().Format(time.RFC3339), Cmd: c.failoverCmd}
	paused := time.Now()
	g.pause.Lock()
	defer g.pause.Unlock()
	if err := rn.barrier(ctx, l, ms); err != nil {
		res.Err = "barrier before the kill: " + err.Error()
		return res
	}
	if out, err := exec.CommandContext(ctx, "sh", "-c", c.failoverCmd).CombinedOutput(); err != nil {
		res.Err = fmt.Sprintf("failover command: %v: %s", err, out)
		return res
	}
	// A command that exits 0 without killing anything (a stale pid file)
	// would leave every writer up, and the cell would pass without a
	// failover. The old writer must stop answering.
	if !writerGone(ctx, c.arcURL, 30*time.Second) {
		res.Err = "the old writer " + c.arcURL + " still answers /health 30s after the failover command: nothing failed over"
		return res
	}
	after := newArcClient(c.arcAfter, c.token)
	probe := []byte("failover_probe,host=h0 v=1i\n")
	deadline := time.Now().Add(8 * time.Minute)
	for {
		code, _, err := after.writeLP(ctx, c.db, probe)
		if err == nil && code == http.StatusNoContent {
			break
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			res.Err = fmt.Sprintf("the new writer never accepted a write (last code %d, err %v)", code, err)
			return res
		}
		time.Sleep(500 * time.Millisecond)
	}
	g.w.Store(after)
	res.Downtime = time.Since(paused).Round(time.Second).String()
	fmt.Printf("%s failover done: writes resumed on %s after %s\n", time.Now().Format("15:04:05"), c.arcAfter, res.Downtime)
	return res
}

// writerGone reports whether base stops answering /health within wait.
func writerGone(ctx context.Context, base string, wait time.Duration) bool {
	hc := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(wait)
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(base, "/")+"/health", nil)
		if err != nil {
			return false
		}
		resp, err := hc.Do(req)
		if err != nil {
			return true
		}
		resp.Body.Close()
		if time.Now().After(deadline) || ctx.Err() != nil {
			return false
		}
		time.Sleep(500 * time.Millisecond)
	}
}
