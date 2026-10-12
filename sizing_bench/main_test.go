package main

import (
	"fmt"
	"github.com/Basekick-Labs/msgpack/v6"
	"strings"
	"testing"
)

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
