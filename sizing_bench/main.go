// Sizing load generator, derived from sustained_bench's pre-generated HTTP
// workers and columnar MessagePack schema. Both protocols encode identical rows.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/klauspost/compress/zstd"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	Mode, Host, Database, Protocol, Compression, QueryFile, Output string
	Workers, Batch, Pregenerate, Hosts, Duration, Warmup           int
	Rate                                                           float64
	Seed, MaxRecords, Anchor                                       int64
	History                                                        bool
}
type Sample struct {
	Name             string  `json:"name"`
	Latency          float64 `json:"latency_ms"`
	OK               bool    `json:"ok"`
	CompletedSeconds float64 `json:"completed_seconds"`
}
type WorkerStats struct {
	Samples                                                           []Sample
	Records, Bytes, Errors, Requests, Missed, Acknowledged, AllErrors int64
	ErrorExamples                                                     []string
}
type Query struct {
	Name string `json:"name"`
	SQL  string `json:"sql"`
}
type Result struct {
	Config              Config                        `json:"config"`
	Started             string                        `json:"started_utc"`
	Seconds             float64                       `json:"seconds"`
	AcknowledgedRecords int64                         `json:"acknowledged_records_including_warmup"`
	Records             int64                         `json:"records"`
	Bytes               int64                         `json:"bytes"`
	Requests            int64                         `json:"requests"`
	Errors              int64                         `json:"errors"`
	AllErrors           int64                         `json:"errors_including_warmup"`
	Missed              int64                         `json:"missed_schedule_slots"`
	RecordsPerSecond    float64                       `json:"records_per_second"`
	RequestsPerSecond   float64                       `json:"requests_per_second"`
	Latencies           map[string]map[string]float64 `json:"latencies"`
	Windows             []map[string]any              `json:"minute_windows"`
	ErrorExamples       []string                      `json:"error_examples"`
}

func percentile(v []float64, p float64) float64 {
	if len(v) == 0 {
		return 0
	}
	return v[min(len(v)-1, int(float64(len(v))*p))]
}

func batch(cfg Config, index int64) []byte {
	// Independent deterministic seed per batch, so parallel generation and both
	// wire formats produce the same host, timestamp, and field values.
	rng := rand.New(rand.NewSource(cfg.Seed + index))
	hosts := make([]string, cfg.Batch)
	times := make([]int64, cfg.Batch)
	a, b, c := make([]float64, cfg.Batch), make([]float64, cfg.Batch), make([]float64, cfg.Batch)
	for j := 0; j < cfg.Batch; j++ {
		row := index*int64(cfg.Batch) + int64(j)
		hosts[j] = fmt.Sprintf("server%06d", rng.Intn(cfg.Hosts))
		if cfg.History {
			times[j] = cfg.Anchor - 24*3600*1e6 + row*(24*3600*1e6)/max(1, cfg.MaxRecords)
		} else {
			times[j] = cfg.Anchor + row
		}
		a[j] = math.Round(rng.Float64()*1e6) / 1e4
		b[j] = math.Round(rng.Float64()*1e6) / 1e4
		c[j] = math.Round(rng.Float64()*1e6) / 1e4
	}
	var data []byte
	if cfg.Protocol == "lineprotocol" {
		var buf bytes.Buffer
		for j := range times {
			fmt.Fprintf(&buf, "cpu,host=%s value=%.4f,cpu_idle=%.4f,cpu_user=%.4f %d\n", hosts[j], a[j], b[j], c[j], times[j]*1000)
		}
		data = buf.Bytes()
	} else {
		var err error
		data, err = msgpack.Marshal(map[string]any{"m": "cpu", "columns": map[string]any{"time": times, "host": hosts, "value": a, "cpu_idle": b, "cpu_user": c}})
		if err != nil {
			panic(err)
		}
	}
	if cfg.Compression == "zstd" {
		enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
		if err != nil {
			panic(err)
		}
		defer enc.Close()
		data = enc.EncodeAll(data, nil)
	}
	return data
}

func main() {
	cfg := Config{}
	flag.StringVar(&cfg.Mode, "mode", "ingest", "ingest or query")
	flag.StringVar(&cfg.Host, "host", "localhost:8000", "Arc host:port")
	flag.StringVar(&cfg.Database, "database", "sizing", "Arc database")
	flag.StringVar(&cfg.Protocol, "protocol", "msgpack", "msgpack or lineprotocol")
	flag.StringVar(&cfg.Compression, "compress", "none", "none or zstd")
	flag.StringVar(&cfg.QueryFile, "queries", "", "JSON query definitions")
	flag.StringVar(&cfg.Output, "output", "", "JSON result file")
	flag.IntVar(&cfg.Workers, "workers", 4, "HTTP workers")
	flag.IntVar(&cfg.Batch, "batch-size", 5000, "rows per request")
	flag.IntVar(&cfg.Pregenerate, "pregenerate", 256, "replayed batches for throughput")
	flag.IntVar(&cfg.Hosts, "hosts", 10000, "distinct hosts")
	flag.IntVar(&cfg.Duration, "duration", 60, "measured seconds")
	flag.IntVar(&cfg.Warmup, "warmup", 10, "unmeasured seconds")
	flag.Float64Var(&cfg.Rate, "rate", 0, "aggregate rows/s or queries/s; 0 = closed loop")
	flag.Int64Var(&cfg.Seed, "seed", 42, "deterministic seed")
	flag.Int64Var(&cfg.MaxRecords, "max-records", 0, "exact row count, unique generated batches")
	flag.Int64Var(&cfg.Anchor, "anchor", 0, "timestamp anchor in microseconds")
	flag.BoolVar(&cfg.History, "history", false, "spread unique seed rows over preceding 24 hours")
	flag.Parse()
	if cfg.Workers < 1 || cfg.Batch < 1 || cfg.Pregenerate < 1 || cfg.Hosts < 1 || cfg.Duration < 1 || (cfg.Protocol != "msgpack" && cfg.Protocol != "lineprotocol") || (cfg.Compression != "none" && cfg.Compression != "zstd") || (cfg.Mode != "ingest" && cfg.Mode != "query") || cfg.MaxRecords < 0 || cfg.MaxRecords%int64(cfg.Batch) != 0 {
		fmt.Fprintln(os.Stderr, "invalid configuration (max-records must be a multiple of batch-size)")
		os.Exit(2)
	}
	if cfg.Anchor == 0 {
		cfg.Anchor = time.Now().UnixMicro()
	}
	if cfg.MaxRecords > 0 {
		cfg.Warmup = 0
	}
	var queries []Query
	var batches [][]byte
	if cfg.Mode == "query" {
		data, err := os.ReadFile(cfg.QueryFile)
		if err != nil {
			panic(err)
		}
		if err = json.Unmarshal(data, &queries); err != nil {
			panic(err)
		}
		if len(queries) == 0 {
			panic("no queries")
		}
	} else if cfg.MaxRecords == 0 {
		batches = make([][]byte, cfg.Pregenerate)
		var next atomic.Int64
		var wg sync.WaitGroup
		for i := 0; i < min(cfg.Workers, 8); i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					idx := next.Add(1) - 1
					if idx >= int64(len(batches)) {
						return
					}
					batches[idx] = batch(cfg, idx)
				}
			}()
		}
		wg.Wait()
	}
	transport := &http.Transport{MaxIdleConns: cfg.Workers + 10, MaxIdleConnsPerHost: cfg.Workers + 10, MaxConnsPerHost: cfg.Workers + 10, IdleConnTimeout: 90 * time.Second}
	client := &http.Client{Transport: transport, Timeout: 60 * time.Second}
	stats := make([]WorkerStats, cfg.Workers)
	var tickets, progress, errorCount atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()
	measureStart := start.Add(time.Duration(cfg.Warmup) * time.Second)
	end := measureStart.Add(time.Duration(cfg.Duration) * time.Second)
	var period time.Duration
	if cfg.Rate > 0 {
		perRequest := 1.
		if cfg.Mode == "ingest" {
			perRequest = float64(cfg.Batch)
		}
		period = time.Duration(float64(time.Second) * float64(cfg.Workers) * perRequest / cfg.Rate)
	}
	for id := 0; id < cfg.Workers; id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			s := &stats[id]
			next := start.Add(time.Duration(id) * period / time.Duration(cfg.Workers))
			idx := int64(id)
			for time.Now().Before(end) && errorCount.Load() < 100 {
				if period > 0 {
					if time.Now().Before(next) {
						wake := next
						if end.Before(wake) {
							wake = end
						}
						time.Sleep(time.Until(wake))
					}
					if !time.Now().Before(end) {
						break
					}
					if time.Since(next) > period {
						missed := int64(time.Since(next) / period)
						if time.Now().After(measureStart) {
							s.Missed += missed
						}
						next = next.Add(time.Duration(missed) * period)
					}
					next = next.Add(period)
				}
				var payload []byte
				name := "write"
				endpoint := "/api/v1/write/msgpack"
				ctype := "application/msgpack"
				if cfg.Mode == "query" {
					q := queries[idx%int64(len(queries))]
					name = q.Name
					payload, _ = json.Marshal(map[string]string{"sql": q.SQL})
					endpoint = "/api/v1/query"
					ctype = "application/json"
				} else {
					if cfg.MaxRecords > 0 {
						ticket := tickets.Add(1) - 1
						if ticket >= cfg.MaxRecords/int64(cfg.Batch) {
							break
						}
						payload = batch(cfg, ticket)
					} else {
						payload = batches[idx%int64(len(batches))]
					}
					if cfg.Protocol == "lineprotocol" {
						endpoint = "/api/v1/write/line-protocol"
						ctype = "text/plain"
					}
				}
				idx += int64(cfg.Workers)
				req, err := http.NewRequest("POST", "http://"+cfg.Host+endpoint, bytes.NewReader(payload))
				if err != nil {
					panic(err)
				}
				req.Header.Set("Content-Type", ctype)
				req.Header.Set("x-arc-database", cfg.Database)
				req.Header.Set("Authorization", "Bearer "+os.Getenv("ARC_TOKEN"))
				if cfg.Mode == "ingest" && cfg.Compression == "zstd" {
					req.Header.Set("Content-Encoding", "zstd")
				}
				reqStart := time.Now()
				resp, err := client.Do(req)
				ok := false
				message := ""
				var responseBytes int64
				if err != nil {
					message = err.Error()
				} else {
					responseBytes, err = io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
					ok = err == nil && ((cfg.Mode == "ingest" && resp.StatusCode == 204) || (cfg.Mode == "query" && resp.StatusCode == 200))
					if !ok {
						message = fmt.Sprintf("HTTP %d body_read=%v", resp.StatusCode, err)
					}
				}
				if ok && cfg.Mode == "ingest" {
					s.Acknowledged += int64(cfg.Batch)
				}
				if !ok {
					s.AllErrors++
					errorCount.Add(1)
					if len(s.ErrorExamples) < 3 {
						s.ErrorExamples = append(s.ErrorExamples, message)
					}
				}
				if reqStart.Before(measureStart) {
					continue
				}
				s.Requests++
				s.Samples = append(s.Samples, Sample{name, float64(time.Since(reqStart).Microseconds()) / 1000, ok, time.Since(measureStart).Seconds()})
				if ok {
					if cfg.Mode == "ingest" {
						s.Records += int64(cfg.Batch)
						s.Bytes += int64(len(payload))
						progress.Add(int64(cfg.Batch))
					} else {
						s.Bytes += responseBytes
					}
				} else {
					s.Errors++
				}
			}
		}(id)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	ticker := time.NewTicker(10 * time.Second)
	for finished := false; !finished; {
		select {
		case <-done:
			finished = true
		case <-ticker.C:
			fmt.Fprintf(os.Stderr, "progress elapsed=%.1fs records=%d\n", time.Since(measureStart).Seconds(), progress.Load())
		}
	}
	ticker.Stop()
	result := Result{Config: cfg, Started: measureStart.UTC().Format(time.RFC3339Nano), Seconds: time.Since(measureStart).Seconds(), Latencies: map[string]map[string]float64{}}
	groups := map[string][]float64{}
	counts := map[string]int{}
	failures := map[string]int{}
	for _, s := range stats {
		result.Records += s.Records
		result.AcknowledgedRecords += s.Acknowledged
		result.Bytes += s.Bytes
		result.Requests += s.Requests
		result.Errors += s.Errors
		result.AllErrors += s.AllErrors
		result.Missed += s.Missed
		result.ErrorExamples = append(result.ErrorExamples, s.ErrorExamples...)
		for _, sample := range s.Samples {
			counts[sample.Name]++
			if sample.OK {
				groups[sample.Name] = append(groups[sample.Name], sample.Latency)
			} else {
				failures[sample.Name]++
			}
		}
	}
	result.RecordsPerSecond = float64(result.Records) / result.Seconds
	result.RequestsPerSecond = float64(result.Requests-result.Errors) / result.Seconds
	for name, n := range counts {
		v := groups[name]
		sort.Float64s(v)
		result.Latencies[name] = map[string]float64{"p50_ms": percentile(v, .5), "p95_ms": percentile(v, .95), "p99_ms": percentile(v, .99), "requests": float64(n), "errors": float64(failures[name])}
	}
	for startWindow := 0.; startWindow < float64(cfg.Duration); startWindow += 60 {
		stopWindow := math.Min(startWindow+60, float64(cfg.Duration))
		successful, failed := 0, 0
		windowLatencies := map[string][]float64{}
		for _, worker := range stats {
			for _, sample := range worker.Samples {
				if sample.CompletedSeconds >= startWindow && sample.CompletedSeconds < stopWindow {
					if sample.OK {
						successful++
						windowLatencies[sample.Name] = append(windowLatencies[sample.Name], sample.Latency)
					} else {
						failed++
					}
				}
			}
		}
		latencySummary := map[string]map[string]float64{}
		for name, values := range windowLatencies {
			sort.Float64s(values)
			latencySummary[name] = map[string]float64{"p95_ms": percentile(values, .95), "p99_ms": percentile(values, .99), "requests": float64(len(values))}
		}
		rps := 0.
		if cfg.Mode == "ingest" {
			rps = float64(successful*cfg.Batch) / (stopWindow - startWindow)
		}
		result.Windows = append(result.Windows, map[string]any{"start_seconds": startWindow, "end_seconds": stopWindow, "requests_per_second": float64(successful) / (stopWindow - startWindow), "records_per_second": rps, "errors": failed, "latencies": latencySummary})
	}
	data, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(data))
	if cfg.Output != "" {
		if err := os.WriteFile(cfg.Output, append(data, '\n'), 0644); err != nil {
			panic(err)
		}
	}
	if cfg.MaxRecords > 0 && result.Records != cfg.MaxRecords {
		os.Exit(1)
	}
}
