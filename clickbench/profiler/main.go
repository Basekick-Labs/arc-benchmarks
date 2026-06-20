// ClickBench profiler: runs a representative subset of queries through DuckDB
// (same version Arc embeds) with full operator profiling enabled.
//
// Outputs per-query JSON profile trees + a human-readable summary table
// comparing operator timing breakdown.
//
// Usage:
//
//	go run benchmarks/clickbench/profiler/main.go \
//	  -parquet ./data/arc/clickbench/hits/hits.parquet \
//	  -arc-url http://localhost:8000 \
//	  -out benchmarks/clickbench/local/profiles
//
// The -arc-url flag is optional. When provided, each profiled query is also
// sent to Arc via x-arc-profile:true and the execution_time_ms is recorded
// for side-by-side comparison.
package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

// profiled queries — representative cross-section of ClickBench workloads
var profiledQueries = []struct {
	name    string // short label
	arcSQL  string // sent to Arc (bare table name, x-arc-database: clickbench)
	duckSQL string // sent to DuckDB directly (placeholder PARQUET replaced at runtime)
}{
	{
		name:    "Q01 COUNT(*) full scan",
		arcSQL:  "SELECT COUNT(*) FROM hits",
		duckSQL: "SELECT COUNT(*) FROM read_parquet('PARQUET')",
	},
	{
		name:    "Q05 COUNT DISTINCT",
		arcSQL:  "SELECT COUNT(DISTINCT UserID) FROM hits",
		duckSQL: "SELECT COUNT(DISTINCT UserID) FROM read_parquet('PARQUET')",
	},
	{
		name:    "Q16 GROUP BY UserID",
		arcSQL:  "SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10",
		duckSQL: "SELECT UserID, COUNT(*) FROM read_parquet('PARQUET') GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10",
	},
	{
		name:    "Q21 LIKE filter",
		arcSQL:  "SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%'",
		duckSQL: "SELECT COUNT(*) FROM read_parquet('PARQUET') WHERE URL LIKE '%google%'",
	},
	{
		name:    "Q29 REGEXP_REPLACE",
		arcSQL:  `SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(STRLEN(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25`,
		duckSQL: `SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(STRLEN(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM read_parquet('PARQUET') WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25`,
	},
	{
		name:    "Q33 WatchID+ClientIP all rows",
		arcSQL:  "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10",
		duckSQL: "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM read_parquet('PARQUET') GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10",
	},
	{
		name:    "Q43 CounterID time-range",
		arcSQL:  "SELECT (EventTime // 60) * 60 AS M, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= 15888 AND EventDate <= 15917 AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY (EventTime // 60) * 60 ORDER BY (EventTime // 60) * 60 LIMIT 10 OFFSET 1000",
		duckSQL: "SELECT (EventTime // 60) * 60 AS M, COUNT(*) AS PageViews FROM read_parquet('PARQUET') WHERE CounterID = 62 AND EventDate >= 15888 AND EventDate <= 15917 AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY (EventTime // 60) * 60 ORDER BY (EventTime // 60) * 60 LIMIT 10 OFFSET 1000",
	},
}

// DuckDB raw profile JSON (top-level fields we care about)
type duckProfile struct {
	Latency           float64       `json:"latency"`            // total wall time in seconds
	CPUTime           float64       `json:"cpu_time"`           // sum of all operator CPU time
	TotalBytesRead    int64         `json:"total_bytes_read"`   // bytes read from Parquet
	RowsReturned      int64         `json:"rows_returned"`
	CumRowsScanned    int64         `json:"cumulative_rows_scanned"`
	PeakBufferMemory  int64         `json:"system_peak_buffer_memory"`
	Children          []duckOp      `json:"children"`
}

type duckOp struct {
	Name          string             `json:"operator_name"`
	Type          string             `json:"operator_type"`
	Timing        float64            `json:"operator_timing"` // seconds
	Cardinality   int64              `json:"operator_cardinality"`
	RowsScanned   int64              `json:"operator_rows_scanned"`
	ExtraInfo     map[string]string  `json:"extra_info"`
	Children      []duckOp           `json:"children"`
}

// Arc query response
type arcResponse struct {
	Success       bool             `json:"success"`
	ExecutionMs   int              `json:"execution_time_ms"`
	Profile       *arcProfile      `json:"profile,omitempty"`
	Error         string           `json:"error,omitempty"`
}

type arcProfile struct {
	TotalMs     float64 `json:"total_ms"`
	PlannerMs   float64 `json:"planner_ms"`
	ExecutionMs float64 `json:"execution_ms"`
}

// Result for one query run
type queryResult struct {
	Name           string
	DuckWallMs     float64
	DuckCPUMs      float64
	DuckBytesRead  int64
	DuckRowsRead   int64
	DuckPeakMemMB  float64
	DuckProfile    *duckProfile
	ArcWallMs      float64
	ArcPlannerMs   float64
	ArcExecMs      float64
	ArcErr         string
}

func main() {
	parquet := flag.String("parquet", "./data/arc/clickbench/hits/hits.parquet", "Path to hits.parquet")
	arcURL := flag.String("arc-url", "", "Arc HTTP URL (e.g. http://localhost:8000), empty = skip Arc")
	outDir := flag.String("out", "benchmarks/clickbench/local/profiles", "Output directory for profile JSON files")
	tries := flag.Int("tries", 2, "Number of runs per query (first is cold, rest warm)")
	flag.Parse()

	if err := os.MkdirAll(*outDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "failed to create output dir: %v\n", err)
		os.Exit(1)
	}

	if _, err := os.Stat(*parquet); err != nil {
		fmt.Fprintf(os.Stderr, "parquet file not found: %s\n", *parquet)
		os.Exit(1)
	}

	absParquet, _ := filepath.Abs(*parquet)
	ncpu := runtime.NumCPU()
	memGB := estimateMemGB()

	fmt.Printf("================================================================\n")
	fmt.Printf("  CLICKBENCH PROFILER\n")
	fmt.Printf("  Parquet:  %s\n", absParquet)
	fmt.Printf("  DuckDB:   v1.05.01 (Arc-embedded)\n")
	fmt.Printf("  Threads:  %d  |  Memory: %dGB\n", ncpu, memGB)
	fmt.Printf("  Arc URL:  %s\n", func() string { if *arcURL == "" { return "(skipped)" }; return *arcURL }())
	fmt.Printf("  Tries:    %d  (run 1 = fresh metadata cache, run 2+ = warm)\n", *tries)
	fmt.Printf("  Queries:  %d representative queries\n", len(profiledQueries))
	fmt.Printf("================================================================\n\n")

	// Open DuckDB with Arc-equivalent settings
	db, err := sql.Open("duckdb", "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open duckdb: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()
	ctx := context.Background()
	db.ExecContext(ctx, fmt.Sprintf("SET threads=%d", ncpu))
	db.ExecContext(ctx, fmt.Sprintf("SET memory_limit='%dGB'", memGB))
	db.ExecContext(ctx, "SET parquet_metadata_cache=true")

	var results []queryResult

	for _, q := range profiledQueries {
		fmt.Printf("── %s\n", q.name)

		duckSQL := strings.ReplaceAll(q.duckSQL, "PARQUET", absParquet)

		var lastResult queryResult
		lastResult.Name = q.name

		for run := 1; run <= *tries; run++ {
			label := "cold"
			if run > 1 {
				label = "warm"
			}
			fmt.Printf("   run %d (%s): ", run, label)

			prof, wallMs, err := runDuckWithProfile(ctx, db, duckSQL, *outDir, q.name, run)
			if err != nil {
				fmt.Printf("ERR: %v\n", err)
				continue
			}

			cpuMs := prof.CPUTime * 1000
			fmt.Printf("wall=%.1fms  cpu=%.1fms  bytes_read=%s  rows_scanned=%s  peak_mem=%s\n",
				wallMs,
				cpuMs,
				humanBytes(prof.TotalBytesRead),
				humanCount(prof.CumRowsScanned),
				humanBytes(prof.PeakBufferMemory),
			)

			// Keep last run as the representative result
			lastResult.DuckWallMs = wallMs
			lastResult.DuckCPUMs = cpuMs
			lastResult.DuckBytesRead = prof.TotalBytesRead
			lastResult.DuckRowsRead = prof.CumRowsScanned
			lastResult.DuckPeakMemMB = float64(prof.PeakBufferMemory) / (1024 * 1024)
			lastResult.DuckProfile = prof
		}

		// Arc comparison (single warm run)
		if *arcURL != "" {
			arcMs, planMs, execMs, arcErr := runArcQuery(*arcURL, q.arcSQL)
			if arcErr != "" {
				fmt.Printf("   arc: ERR: %s\n", arcErr)
				lastResult.ArcErr = arcErr
			} else {
				fmt.Printf("   arc: wall=%dms  planner=%.0fms  exec=%.0fms\n", arcMs, planMs, execMs)
				lastResult.ArcWallMs = float64(arcMs)
				lastResult.ArcPlannerMs = planMs
				lastResult.ArcExecMs = execMs
			}
		}

		results = append(results, lastResult)
		fmt.Println()
	}

	// Print summary table
	printSummary(results, *arcURL != "")

	// Write summary JSON
	summaryPath := filepath.Join(*outDir, "profile_summary.json")
	summaryData, _ := json.MarshalIndent(results, "", "  ")
	os.WriteFile(summaryPath, summaryData, 0644)
	fmt.Printf("\nProfile JSONs and summary saved to: %s\n", *outDir)
}

func runDuckWithProfile(ctx context.Context, db *sql.DB, query, outDir, queryName string, run int) (*duckProfile, float64, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("conn: %w", err)
	}
	defer conn.Close()

	profFile := filepath.Join(outDir, fmt.Sprintf("profile_%s_run%d.json",
		strings.ReplaceAll(strings.ToLower(queryName[:min(20, len(queryName))]), " ", "_"), run))

	conn.ExecContext(ctx, "PRAGMA enable_profiling='json'")
	conn.ExecContext(ctx, fmt.Sprintf("PRAGMA profiling_output='%s'", profFile))

	start := time.Now()
	rows, err := conn.QueryContext(ctx, query)
	wallMs := float64(time.Since(start).Microseconds()) / 1000.0
	if err != nil {
		return nil, 0, fmt.Errorf("query: %w", err)
	}
	for rows.Next() {}
	rows.Close()

	conn.ExecContext(ctx, "PRAGMA disable_profiling")

	// Small wait for profile flush
	time.Sleep(50 * time.Millisecond)

	data, err := os.ReadFile(profFile)
	if err != nil || len(data) == 0 {
		return &duckProfile{Latency: wallMs / 1000}, wallMs, nil
	}

	var prof duckProfile
	if err := json.Unmarshal(data, &prof); err != nil {
		return &duckProfile{Latency: wallMs / 1000}, wallMs, nil
	}
	return &prof, wallMs, nil
}

func runArcQuery(baseURL, sql string) (wallMs int, plannerMs, execMs float64, errMsg string) {
	body, _ := json.Marshal(map[string]string{"sql": sql})
	req, err := http.NewRequest("POST", baseURL+"/api/v1/query", bytes.NewReader(body))
	if err != nil {
		return 0, 0, 0, err.Error()
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-arc-database", "clickbench")
	req.Header.Set("x-arc-profile", "true")

	client := &http.Client{Timeout: 300 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, 0, 0, err.Error()
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	var ar arcResponse
	if err := json.Unmarshal(respBody, &ar); err != nil {
		return 0, 0, 0, "parse error: " + err.Error()
	}
	if !ar.Success {
		return 0, 0, 0, ar.Error
	}
	if ar.Profile != nil {
		return ar.ExecutionMs, ar.Profile.PlannerMs, ar.Profile.ExecutionMs, ""
	}
	return ar.ExecutionMs, 0, 0, ""
}

func printSummary(results []queryResult, hasArc bool) {
	fmt.Printf("\n%s\n", strings.Repeat("=", 90))
	fmt.Printf("  PROFILE SUMMARY  (last run per query)\n")
	fmt.Printf("%s\n", strings.Repeat("=", 90))

	if hasArc {
		fmt.Printf("%-30s %10s %10s %12s %12s %10s %10s\n",
			"Query", "Duck wall", "Duck CPU", "Bytes read", "Rows scanned", "Arc wall", "D/A ratio")
		fmt.Printf("%s\n", strings.Repeat("-", 90))
		for _, r := range results {
			ratio := ""
			if r.ArcWallMs > 0 && r.DuckWallMs > 0 {
				ratio = fmt.Sprintf("%.2fx", r.DuckWallMs/r.ArcWallMs)
			}
			arcWall := fmt.Sprintf("%.0fms", r.ArcWallMs)
			if r.ArcErr != "" {
				arcWall = "ERR"
			}
			fmt.Printf("%-30s %9.0fms %9.0fms %12s %12s %10s %10s\n",
				truncate(r.Name, 30),
				r.DuckWallMs,
				r.DuckCPUMs,
				humanBytes(r.DuckBytesRead),
				humanCount(r.DuckRowsRead),
				arcWall,
				ratio,
			)
		}
	} else {
		fmt.Printf("%-30s %10s %10s %12s %12s %12s\n",
			"Query", "Duck wall", "Duck CPU", "Bytes read", "Rows scanned", "Peak mem")
		fmt.Printf("%s\n", strings.Repeat("-", 90))
		for _, r := range results {
			fmt.Printf("%-30s %9.0fms %9.0fms %12s %12s %11.0fMB\n",
				truncate(r.Name, 30),
				r.DuckWallMs,
				r.DuckCPUMs,
				humanBytes(r.DuckBytesRead),
				humanCount(r.DuckRowsRead),
				r.DuckPeakMemMB,
			)
		}
	}

	fmt.Printf("%s\n\n", strings.Repeat("-", 90))
	fmt.Printf("Key metrics:\n")
	fmt.Printf("  Duck wall  = total elapsed time including DuckDB overhead\n")
	fmt.Printf("  Duck CPU   = sum of all operator CPU time (can exceed wall on multi-core)\n")
	fmt.Printf("  Bytes read = bytes read from Parquet (0 = metadata cache hit on footer read)\n")
	fmt.Printf("  Rows scanned = cumulative rows processed across all operators\n")
	if hasArc {
		fmt.Printf("  D/A ratio  = Duck wall / Arc wall  (>1 = Arc faster, <1 = DuckDB faster)\n")
	}
}

// ── helpers ─────────────────────────────────────────────────────────────────

func humanBytes(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1fGB", float64(b)/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1fMB", float64(b)/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1fKB", float64(b)/(1<<10))
	default:
		return fmt.Sprintf("%dB", b)
	}
}

func humanCount(n int64) string {
	switch {
	case n >= 1_000_000_000:
		return fmt.Sprintf("%.1fB", float64(n)/1e9)
	case n >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1e6)
	case n >= 1_000:
		return fmt.Sprintf("%.1fK", float64(n)/1e3)
	default:
		return fmt.Sprintf("%d", n)
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func estimateMemGB() int {
	// Rough estimate: ~50% of system RAM, min 1GB, matching Arc's default heuristic
	// On macOS: sysctl hw.memsize; on Linux: /proc/meminfo
	// We just use a fixed reasonable default here
	mem := runtime.NumCPU() * 2 // 2GB per core estimate
	if mem < 1 {
		mem = 1
	}
	if mem > 32 {
		mem = 32
	}
	return mem
}
