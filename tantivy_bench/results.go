package main

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// IndexResult holds indexing benchmark metrics.
type IndexResult struct {
	Name       string
	Duration   time.Duration
	DocsPerSec float64
	SizeBytes  int64
}

// QueryResult holds search benchmark metrics for a single query.
type QueryResult struct {
	Name        string
	DuckDBCount int64
	DuckDBAvg   time.Duration
	SimpleCount uint64
	SimpleAvg   time.Duration
	NgramCount  uint64
	NgramAvg    time.Duration
	SimpleMatch bool
	NgramMatch  bool
}

// ConcurrentResult holds concurrent read/write metrics.
type ConcurrentResult struct {
	SearchLatencyIdle    time.Duration
	SearchLatencyBusy    time.Duration
	Degradation          float64
	IndexThroughputAlone float64
	IndexThroughputBusy  float64
}

func formatBytes(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1f GB", float64(b)/float64(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(b)/float64(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

func formatRate(docsPerSec float64) string {
	switch {
	case docsPerSec >= 1_000_000:
		return fmt.Sprintf("%.1fM/s", docsPerSec/1_000_000)
	case docsPerSec >= 1_000:
		return fmt.Sprintf("%.1fK/s", docsPerSec/1_000)
	default:
		return fmt.Sprintf("%.0f/s", docsPerSec)
	}
}

func avgDuration(durations []time.Duration) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	var total time.Duration
	for _, d := range durations {
		total += d
	}
	return total / time.Duration(len(durations))
}

func percentile(durations []time.Duration, p float64) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(durations))
	copy(sorted, durations)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(float64(len(sorted)) * p)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func printHeader(title string) {
	line := strings.Repeat("=", 80)
	fmt.Println(line)
	fmt.Println(title)
	fmt.Println(line)
	fmt.Println()
}

func printSubHeader(title string) {
	fmt.Printf("=== %s ===\n\n", title)
}

func printIndexResults(parquetSize int64, results []IndexResult) {
	printSubHeader("INDEXING RESULTS")

	fmt.Printf("| %-20s | %10s | %10s | %10s | %12s |\n",
		"Index Type", "Time", "Docs/sec", "Size", "vs Parquet")
	fmt.Printf("| %-20s | %10s | %10s | %10s | %12s |\n",
		strings.Repeat("-", 20), strings.Repeat("-", 10), strings.Repeat("-", 10),
		strings.Repeat("-", 10), strings.Repeat("-", 12))

	fmt.Printf("| %-20s | %10s | %10s | %10s | %12s |\n",
		"Parquet", "-", "-", formatBytes(parquetSize), "1.00x")

	for _, r := range results {
		ratio := float64(r.SizeBytes) / float64(parquetSize)
		fmt.Printf("| %-20s | %10v | %10s | %10s | %11.2fx |\n",
			r.Name, r.Duration.Round(time.Millisecond), formatRate(r.DocsPerSec),
			formatBytes(r.SizeBytes), ratio)
	}
	fmt.Println()
}

func printQueryResults(results []QueryResult) {
	printSubHeader("SEARCH RESULTS")

	fmt.Printf("| %-25s | %10s | %10s | %10s | %10s | %10s | %7s |\n",
		"Query", "DuckDB", "Simple", "Ngram", "DuckDB #", "Ngram #", "Match?")
	fmt.Printf("| %-25s | %10s | %10s | %10s | %10s | %10s | %7s |\n",
		strings.Repeat("-", 25), strings.Repeat("-", 10), strings.Repeat("-", 10),
		strings.Repeat("-", 10), strings.Repeat("-", 10), strings.Repeat("-", 10),
		strings.Repeat("-", 7))

	for _, r := range results {
		match := "YES"
		if !r.NgramMatch {
			match = "NO"
		}
		fmt.Printf("| %-25s | %10v | %10v | %10v | %10d | %10d | %7s |\n",
			r.Name,
			r.DuckDBAvg.Round(time.Microsecond),
			r.SimpleAvg.Round(time.Microsecond),
			r.NgramAvg.Round(time.Microsecond),
			r.DuckDBCount,
			r.NgramCount,
			match)
	}
	fmt.Println()
}

func printConcurrentResults(cr ConcurrentResult) {
	printSubHeader("CONCURRENT READ/WRITE")

	fmt.Printf("| %-35s | %20s |\n", "Metric", "Value")
	fmt.Printf("| %-35s | %20s |\n", strings.Repeat("-", 35), strings.Repeat("-", 20))
	fmt.Printf("| %-35s | %20v |\n", "Search latency (idle)", cr.SearchLatencyIdle.Round(time.Microsecond))
	fmt.Printf("| %-35s | %20v |\n", "Search latency (during indexing)", cr.SearchLatencyBusy.Round(time.Microsecond))
	fmt.Printf("| %-35s | %19.1f%% |\n", "Degradation", cr.Degradation*100)
	fmt.Printf("| %-35s | %20s |\n", "Index throughput (standalone)", formatRate(cr.IndexThroughputAlone))
	fmt.Printf("| %-35s | %20s |\n", "Index throughput (concurrent)", formatRate(cr.IndexThroughputBusy))
	fmt.Println()
}

func printGoNoGo(results []QueryResult, indexResults []IndexResult, parquetSize int64, cr ConcurrentResult, numRecords int) {
	printSubHeader("GO / NO-GO METRICS")

	// Average DuckDB vs Tantivy speedup
	var totalDuckDB, totalNgram time.Duration
	ngramMatchCount := 0
	for _, r := range results {
		totalDuckDB += r.DuckDBAvg
		totalNgram += r.NgramAvg
		if r.NgramMatch {
			ngramMatchCount++
		}
	}

	speedup := float64(totalDuckDB) / float64(totalNgram)
	if totalNgram == 0 {
		speedup = 0
	}

	// Find ngram index
	var ngramSize int64
	var ngramThroughput float64
	for _, ir := range indexResults {
		if ir.Name == "Tantivy Ngram" {
			ngramSize = ir.SizeBytes
			ngramThroughput = ir.DocsPerSec
		}
	}

	sizeRatio := float64(ngramSize) / float64(parquetSize)

	fmt.Printf("1. Search speedup vs DuckDB LIKE:  %.1fx (target: >= 10x)\n", speedup)
	fmt.Printf("2. Ngram semantic correctness:      %d/%d queries match (target: 100%%)\n", ngramMatchCount, len(results))
	fmt.Printf("3. Ngram index size:                %.2fx vs Parquet (target: < 3x)\n", sizeRatio)
	fmt.Printf("4. Indexing throughput:              %s (target: >= 200K/s)\n", formatRate(ngramThroughput))
	fmt.Printf("5. Concurrent search degradation:   %.0f%% (target: < 100%%)\n", cr.Degradation*100)
	fmt.Println()

	// Verdict
	pass := speedup >= 10 &&
		ngramMatchCount == len(results) &&
		sizeRatio < 3.0 &&
		ngramThroughput >= 200_000 &&
		cr.Degradation < 1.0

	if pass {
		fmt.Println("VERDICT: GO — All metrics meet targets")
	} else {
		fmt.Println("VERDICT: INVESTIGATE — One or more metrics below target")
		if speedup < 10 {
			fmt.Printf("  - Search speedup %.1fx < 10x target\n", speedup)
		}
		if ngramMatchCount < len(results) {
			fmt.Printf("  - Semantic mismatch: %d/%d queries\n", ngramMatchCount, len(results))
		}
		if sizeRatio >= 3.0 {
			fmt.Printf("  - Index size ratio %.2fx >= 3x target\n", sizeRatio)
		}
		if ngramThroughput < 200_000 {
			fmt.Printf("  - Indexing throughput %s < 200K/s target\n", formatRate(ngramThroughput))
		}
		if cr.Degradation >= 1.0 {
			fmt.Printf("  - Concurrent degradation %.0f%% >= 100%% target\n", cr.Degradation*100)
		}
	}
	fmt.Println()
}
