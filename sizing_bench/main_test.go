package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Basekick-Labs/msgpack/v6"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestPacedQueryStopsAtMeasurementDeadline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"data":[[1]]}`)
	}))
	defer server.Close()
	dir := t.TempDir()
	queries := filepath.Join(dir, "queries.json")
	output := filepath.Join(dir, "result.json")
	if err := os.WriteFile(queries, []byte(`[{"name":"one","sql":"SELECT 1"}]`), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, filepath.Join(runtime.GOROOT(), "bin", "go"), "run", ".", "--mode=query", "--host="+strings.TrimPrefix(server.URL, "http://"), "--queries="+queries, "--output="+output, "--duration=1", "--warmup=0", "--workers=4", "--rate=1")
	if logs, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("client: %v\n%s", err, logs)
	}
	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	var result Result
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	// Four paced workers have a four-second individual period. The run must
	// stop at its one-second boundary, rather than waiting for those periods.
	if result.Seconds < 1 || result.Seconds > 1.75 || result.Requests != 1 || result.AllErrors != 0 {
		t.Fatalf("unexpected deadline result: seconds=%v requests=%v errors=%v", result.Seconds, result.Requests, result.AllErrors)
	}
}

func TestHistorySpansTheWholeDay(t *testing.T) {
	cfg := Config{Protocol: "msgpack", Batch: 32, Hosts: 10000, Anchor: 1791817200000000, Seed: 42, History: true, MaxRecords: 32000}
	var first, last struct {
		Columns struct {
			Time []int64 `msgpack:"time"`
		} `msgpack:"columns"`
	}
	if err := msgpack.Unmarshal(batch(cfg, 0), &first); err != nil {
		t.Fatal(err)
	}
	if err := msgpack.Unmarshal(batch(cfg, 999), &last); err != nil {
		t.Fatal(err)
	}
	if first.Columns.Time[0] != cfg.Anchor-24*3600*1e6 {
		t.Fatal("history does not begin 24 hours before anchor")
	}
	if last.Columns.Time[31] < cfg.Anchor-3*1e6 || last.Columns.Time[31] >= cfg.Anchor {
		t.Fatal("history does not reach the final three seconds of the window")
	}
}

func TestProtocolsEncodeSameSamples(t *testing.T) {
	cfg := Config{Protocol: "msgpack", Batch: 32, Hosts: 10000, Anchor: 1791817200000000, Seed: 42, History: true, MaxRecords: 1000000}
	var payload struct {
		Measurement string `msgpack:"m"`
		Columns     struct {
			Time  []int64   `msgpack:"time"`
			Host  []string  `msgpack:"host"`
			Value []float64 `msgpack:"value"`
			Idle  []float64 `msgpack:"cpu_idle"`
			User  []float64 `msgpack:"cpu_user"`
		} `msgpack:"columns"`
	}
	if err := msgpack.Unmarshal(batch(cfg, 7), &payload); err != nil {
		t.Fatal(err)
	}
	cfg.Protocol = "lineprotocol"
	lines := strings.Split(strings.TrimSpace(string(batch(cfg, 7))), "\n")
	if payload.Measurement != "cpu" || len(lines) != len(payload.Columns.Time) {
		t.Fatal("schema differs")
	}
	for i, line := range lines {
		expected := fmt.Sprintf("cpu,host=%s value=%.4f,cpu_idle=%.4f,cpu_user=%.4f %d", payload.Columns.Host[i], payload.Columns.Value[i], payload.Columns.Idle[i], payload.Columns.User[i], payload.Columns.Time[i]*1000)
		if line != expected {
			t.Fatalf("row %d differs\n%s\n%s", i, line, expected)
		}
		if payload.Columns.Time[i] < cfg.Anchor-24*3600*1e6 || payload.Columns.Time[i] >= cfg.Anchor {
			t.Fatal("outside historical range")
		}
	}
}
