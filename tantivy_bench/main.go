// Tantivy Full-Text Search Benchmark for Arc
//
// Evaluates tantivy-go (CGo bindings for Rust's Tantivy) as a full-text search
// layer for log/event data in Arc. Compares search performance against DuckDB's
// LIKE '%pattern%' over Parquet files.
//
// Prerequisites: Rust toolchain (rustup) for CGo compilation of tantivy-go.
//
// Usage:
//
//	go run . --records 100000 --iterations 3      # Quick validation
//	go run . --records 5000000 --iterations 10    # Full benchmark
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

var (
	numRecords = flag.Int("records", 5_000_000, "Number of log records to generate")
	iterations = flag.Int("iterations", 10, "Search iterations per query")
	batchSize  = flag.Int("batch-size", 10000, "Batch size for Tantivy indexing")
	skipNgram  = flag.Bool("skip-ngram", false, "Skip ngram index test (faster)")
	verbose    = flag.Bool("verbose", false, "Verbose output")
)

// queryKind controls which Tantivy search method to use.
type queryKind int

const (
	kindSimple  queryKind = iota // single text query on message field
	kindBoolAnd                  // boolean AND across multiple terms
	kindBoolOr                   // boolean OR across multiple terms
	kindTagText                  // boolean AND: exact tag + text search
)

// searchQuery defines a search benchmark query.
type searchQuery struct {
	name      string
	duckdbSQL string
	tantivyQ  string   // for kindSimple
	terms     []string // for kindBoolAnd / kindBoolOr
	tagField  string   // for kindTagText
	tagValue  string   // for kindTagText
	kind      queryKind
}

var queries = []searchQuery{
	{
		name:      "Single keyword",
		duckdbSQL: "SELECT COUNT(*) FROM logs WHERE message LIKE '%timeout%'",
		tantivyQ:  "timeout",
		kind:      kindSimple,
	},
	{
		name:      "Rare keyword",
		duckdbSQL: "SELECT COUNT(*) FROM logs WHERE message LIKE '%corruption%'",
		tantivyQ:  "corruption",
		kind:      kindSimple,
	},
	{
		name:      "Common keyword",
		duckdbSQL: "SELECT COUNT(*) FROM logs WHERE message LIKE '%completed%'",
		tantivyQ:  "completed",
		kind:      kindSimple,
	},
	{
		name:      "Multi-keyword AND",
		duckdbSQL: "SELECT COUNT(*) FROM logs WHERE message LIKE '%connection%' AND message LIKE '%failed%'",
		terms:     []string{"connection", "failed"},
		kind:      kindBoolAnd,
	},
	{
		name:      "Multi-keyword OR",
		duckdbSQL: "SELECT COUNT(*) FROM logs WHERE message LIKE '%timeout%' OR message LIKE '%refused%'",
		terms:     []string{"timeout", "refused"},
		kind:      kindBoolOr,
	},
	{
		name:      "Phrase search",
		duckdbSQL: "SELECT COUNT(*) FROM logs WHERE message LIKE '%connection refused%'",
		tantivyQ:  "\"connection refused\"",
		kind:      kindSimple,
	},
	{
		name:      "Prefix search",
		duckdbSQL: "SELECT COUNT(*) FROM logs WHERE message LIKE '%auth%'",
		tantivyQ:  "auth",
		kind:      kindSimple,
	},
	{
		name:      "Tag + text (ERROR)",
		duckdbSQL: "SELECT COUNT(*) FROM logs WHERE level = 'ERROR' AND message LIKE '%timeout%'",
		tantivyQ:  "timeout",
		tagField:  "level",
		tagValue:  "ERROR",
		kind:      kindTagText,
	},
	{
		name:      "Tag + text (FATAL)",
		duckdbSQL: "SELECT COUNT(*) FROM logs WHERE level = 'FATAL' AND message LIKE '%corruption%'",
		tantivyQ:  "corruption",
		tagField:  "level",
		tagValue:  "FATAL",
		kind:      kindTagText,
	},
	{
		name:      "Substring (onnect)",
		duckdbSQL: "SELECT COUNT(*) FROM logs WHERE message LIKE '%onnect%'",
		tantivyQ:  "onnect",
		kind:      kindSimple,
	},
}

func main() {
	flag.Parse()

	printHeader("TANTIVY FULL-TEXT SEARCH BENCHMARK")
	fmt.Printf("Records:    %d\n", *numRecords)
	fmt.Printf("Iterations: %d\n", *iterations)
	fmt.Printf("Batch size: %d\n", *batchSize)
	fmt.Printf("Platform:   %s %s, %d cores\n", runtime.GOOS, runtime.GOARCH, runtime.NumCPU())
	fmt.Println()

	// Create temp directory for index and parquet files
	tmpDir, err := os.MkdirTemp("", "tantivy_bench_*")
	if err != nil {
		fmt.Printf("Failed to create temp dir: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(tmpDir)

	// Phase A: Generate data
	printSubHeader("PHASE A: DATA GENERATION")
	startTime := time.Now().UnixMicro()
	genStart := time.Now()
	records := generateRecords(*numRecords, startTime)
	genDur := time.Since(genStart)
	fmt.Printf("Generated %d records in %v (%.0f records/sec)\n", len(records), genDur.Round(time.Millisecond),
		float64(len(records))/genDur.Seconds())
	fmt.Println()

	// Phase A cont: Write to Parquet via DuckDB
	fmt.Println("Writing records to Parquet...")
	parquetStart := time.Now()
	baseline, err := NewDuckDBBaseline(records, tmpDir)
	if err != nil {
		fmt.Printf("Failed to create DuckDB baseline: %v\n", err)
		os.Exit(1)
	}
	defer baseline.Close()
	parquetDur := time.Since(parquetStart)
	parquetSize := baseline.ParquetSize()
	fmt.Printf("Parquet written in %v (%s)\n", parquetDur.Round(time.Millisecond), formatBytes(parquetSize))
	fmt.Println()

	// Phase B: Indexing
	printSubHeader("PHASE B: INDEXING")
	InitTantivy()

	var indexResults []IndexResult

	// Simple tokenizer index
	simpleIndexPath := filepath.Join(tmpDir, "index_simple")
	simpleIndex, err := NewTantivyIndex(simpleIndexPath, false)
	if err != nil {
		fmt.Printf("Failed to create simple index: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Indexing with Simple tokenizer...")
	simpleDur, err := simpleIndex.IndexRecords(records, *batchSize)
	if err != nil {
		fmt.Printf("Simple indexing failed: %v\n", err)
		os.Exit(1)
	}
	simpleSize := simpleIndex.IndexSize()
	indexResults = append(indexResults, IndexResult{
		Name:       "Tantivy Simple",
		Duration:   simpleDur,
		DocsPerSec: float64(len(records)) / simpleDur.Seconds(),
		SizeBytes:  simpleSize,
	})
	fmt.Printf("Simple index: %v (%s, %s)\n", simpleDur.Round(time.Millisecond),
		formatRate(float64(len(records))/simpleDur.Seconds()), formatBytes(simpleSize))

	// Ngram tokenizer index
	var ngramIndex *TantivyIndex
	if !*skipNgram {
		ngramIndexPath := filepath.Join(tmpDir, "index_ngram")
		ngramIndex, err = NewTantivyIndex(ngramIndexPath, true)
		if err != nil {
			fmt.Printf("Failed to create ngram index: %v\n", err)
			os.Exit(1)
		}

		fmt.Println("Indexing with Ngram tokenizer (min=3, max=5)...")
		ngramDur, err := ngramIndex.IndexRecords(records, *batchSize)
		if err != nil {
			fmt.Printf("Ngram indexing failed: %v\n", err)
			os.Exit(1)
		}
		ngramSize := ngramIndex.IndexSize()
		indexResults = append(indexResults, IndexResult{
			Name:       "Tantivy Ngram",
			Duration:   ngramDur,
			DocsPerSec: float64(len(records)) / ngramDur.Seconds(),
			SizeBytes:  ngramSize,
		})
		fmt.Printf("Ngram index:  %v (%s, %s)\n", ngramDur.Round(time.Millisecond),
			formatRate(float64(len(records))/ngramDur.Seconds()), formatBytes(ngramSize))
	}
	fmt.Println()

	printIndexResults(parquetSize, indexResults)

	// Phase C: Search benchmark
	printSubHeader("PHASE C: SEARCH BENCHMARK")

	var queryResults []QueryResult
	maxResults := *numRecords // return all matches for count comparison

	for _, q := range queries {
		if *verbose {
			fmt.Printf("Running: %s...\n", q.name)
		}

		qr := QueryResult{Name: q.name}

		// DuckDB baseline
		duckDurations := make([]time.Duration, *iterations)
		for i := 0; i < *iterations; i++ {
			count, dur, err := baseline.RunQuery(q.duckdbSQL)
			if err != nil {
				fmt.Printf("DuckDB query %s failed: %v\n", q.name, err)
				break
			}
			duckDurations[i] = dur
			qr.DuckDBCount = count
		}
		qr.DuckDBAvg = avgDuration(duckDurations)

		// Tantivy Simple
		simpleDurations := make([]time.Duration, *iterations)
		for i := 0; i < *iterations; i++ {
			count, dur, err := runTantivyQuery(simpleIndex, q, maxResults)
			if err != nil {
				fmt.Printf("Simple search %s failed: %v\n", q.name, err)
				break
			}
			simpleDurations[i] = dur
			qr.SimpleCount = count
		}
		qr.SimpleAvg = avgDuration(simpleDurations)
		qr.SimpleMatch = qr.SimpleCount == uint64(qr.DuckDBCount)

		// Tantivy Ngram
		if ngramIndex != nil {
			ngramDurations := make([]time.Duration, *iterations)
			for i := 0; i < *iterations; i++ {
				count, dur, err := runTantivyQuery(ngramIndex, q, maxResults)
				if err != nil {
					fmt.Printf("Ngram search %s failed: %v\n", q.name, err)
					break
				}
				ngramDurations[i] = dur
				qr.NgramCount = count
			}
			qr.NgramAvg = avgDuration(ngramDurations)
			qr.NgramMatch = qr.NgramCount == uint64(qr.DuckDBCount)
		}

		queryResults = append(queryResults, qr)
	}

	printQueryResults(queryResults)

	// Phase D: Concurrent read/write
	printSubHeader("PHASE D: CONCURRENT READ/WRITE")

	cr := benchmarkConcurrentReadWrite(records, tmpDir)
	printConcurrentResults(cr)

	// Phase E: Summary
	printHeader("SUMMARY")
	printGoNoGo(queryResults, indexResults, parquetSize, cr, *numRecords)

	// Cleanup
	simpleIndex.Close()
	if ngramIndex != nil {
		ngramIndex.Close()
	}
}

// runTantivyQuery dispatches to the correct search method based on query kind.
func runTantivyQuery(idx *TantivyIndex, q searchQuery, limit int) (uint64, time.Duration, error) {
	switch q.kind {
	case kindBoolAnd:
		return idx.SearchBoolQuery(q.terms, limit)
	case kindBoolOr:
		return idx.SearchBoolOr(q.terms, limit)
	case kindTagText:
		return idx.SearchWithTagFilter(q.tantivyQ, q.tagField, q.tagValue, limit)
	default:
		return idx.Search(q.tantivyQ, limit)
	}
}

func benchmarkConcurrentReadWrite(records []LogRecord, tmpDir string) ConcurrentResult {
	cr := ConcurrentResult{}

	splitPoint := len(records) * 80 / 100
	initialRecords := records[:splitPoint]
	remainingRecords := records[splitPoint:]

	// Create a fresh index for this test
	indexPath := filepath.Join(tmpDir, "index_concurrent")
	idx, err := NewTantivyIndex(indexPath, false)
	if err != nil {
		fmt.Printf("Concurrent test: failed to create index: %v\n", err)
		return cr
	}
	defer idx.Close()

	// Index initial 80%
	fmt.Printf("Indexing initial %d records (80%%)...\n", len(initialRecords))
	initDur, err := idx.IndexRecords(initialRecords, *batchSize)
	if err != nil {
		fmt.Printf("Concurrent test: initial indexing failed: %v\n", err)
		return cr
	}
	cr.IndexThroughputAlone = float64(len(initialRecords)) / initDur.Seconds()

	// Measure idle search latency
	fmt.Println("Measuring idle search latency...")
	var idleLatencies []time.Duration
	for i := 0; i < 100; i++ {
		_, dur, err := idx.Search("timeout", 100)
		if err != nil {
			continue
		}
		idleLatencies = append(idleLatencies, dur)
	}
	cr.SearchLatencyIdle = avgDuration(idleLatencies)

	// Concurrent: search while indexing remaining 20%
	fmt.Printf("Concurrent: searching while indexing remaining %d records (20%%)...\n", len(remainingRecords))

	var busyLatencies []time.Duration
	var busyMu sync.Mutex
	var wg sync.WaitGroup
	done := make(chan struct{})

	// Search goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
				_, dur, err := idx.Search("timeout", 100)
				if err != nil {
					continue
				}
				busyMu.Lock()
				busyLatencies = append(busyLatencies, dur)
				busyMu.Unlock()
			}
		}
	}()

	// Index goroutine — add remaining records to the same index
	indexStart := time.Now()
	wg.Add(1)
	go func() {
		defer wg.Done()
		idx.IndexRecords(remainingRecords, *batchSize)
		close(done)
	}()

	wg.Wait()
	concurrentIndexDur := time.Since(indexStart)
	cr.IndexThroughputBusy = float64(len(remainingRecords)) / concurrentIndexDur.Seconds()

	busyMu.Lock()
	cr.SearchLatencyBusy = avgDuration(busyLatencies)
	busyMu.Unlock()

	if cr.SearchLatencyIdle > 0 {
		cr.Degradation = float64(cr.SearchLatencyBusy)/float64(cr.SearchLatencyIdle) - 1.0
	}

	fmt.Println()
	return cr
}
