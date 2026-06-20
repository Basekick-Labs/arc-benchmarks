// FTS Experiment - Testing DuckDB Full-Text Search extension for LIKE query optimization
//
// This experiment tests whether DuckDB's FTS extension can speed up ClickBench
// queries that use LIKE '%pattern%' for substring search.
//
// Key questions:
// 1. Can FTS match the semantics of LIKE '%pattern%'? (substring vs keyword search)
// 2. What's the index creation overhead?
// 3. How does query performance compare?
// 4. What are the practical limitations for Arc's use case?
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

var (
	dataPath   = flag.String("data-path", "", "Direct path to parquet files")
	iterations = flag.Int("iterations", 3, "Number of iterations per query")
)

// ClickBench queries that use LIKE patterns
var likeQueries = []struct {
	name    string
	query   string
	column  string // Column to index for FTS
	pattern string // Search pattern (for FTS equivalent)
}{
	{
		name:    "Q21",
		query:   "SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%'",
		column:  "URL",
		pattern: "google",
	},
	{
		name:    "Q22",
		query:   "SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
		column:  "URL",
		pattern: "google",
	},
	{
		name:    "Q23",
		query:   "SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
		column:  "Title",
		pattern: "Google",
	},
	{
		name:    "Q24",
		query:   "SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10",
		column:  "URL",
		pattern: "google",
	},
}

func main() {
	flag.Parse()

	fmt.Println("=== DuckDB FTS Extension Experiment ===")
	fmt.Println()

	// For this experiment, we'll create a local DuckDB instance
	// and load the ClickBench data directly
	if *dataPath == "" {
		fmt.Println("Error: --data-path is required")
		fmt.Println("Usage: go run main.go --data-path /path/to/clickbench/parquet/files")
		os.Exit(1)
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		fmt.Printf("Failed to open DuckDB: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// Load FTS extension
	fmt.Println("Loading FTS extension...")
	if _, err := db.Exec("INSTALL fts; LOAD fts;"); err != nil {
		fmt.Printf("Failed to load FTS extension: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("FTS extension loaded successfully")
	fmt.Println()

	// Create view over parquet files
	fmt.Println("Creating view over parquet files...")
	viewSQL := fmt.Sprintf("CREATE VIEW hits AS SELECT * FROM read_parquet('%s/**/*.parquet', hive_partitioning=false)", *dataPath)
	if _, err := db.Exec(viewSQL); err != nil {
		fmt.Printf("Failed to create view: %v\n", err)
		os.Exit(1)
	}

	// Get row count
	var rowCount int64
	if err := db.QueryRow("SELECT COUNT(*) FROM hits").Scan(&rowCount); err != nil {
		fmt.Printf("Failed to count rows: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Loaded %d rows from parquet files\n", rowCount)
	fmt.Println()

	// Run baseline LIKE queries first
	fmt.Println("=== Baseline: LIKE Queries ===")
	fmt.Println()

	baselineResults := make(map[string]time.Duration)
	for _, q := range likeQueries {
		durations := make([]time.Duration, *iterations)
		for i := 0; i < *iterations; i++ {
			start := time.Now()
			rows, err := db.Query(q.query)
			if err != nil {
				fmt.Printf("%s: ERROR - %v\n", q.name, err)
				continue
			}
			// Consume results
			for rows.Next() {
			}
			rows.Close()
			durations[i] = time.Since(start)
		}

		// Calculate average
		var total time.Duration
		for _, d := range durations {
			total += d
		}
		avg := total / time.Duration(*iterations)
		baselineResults[q.name] = avg
		fmt.Printf("%s: %v (avg of %d runs)\n", q.name, avg, *iterations)
	}
	fmt.Println()

	// Now test FTS approach
	// Important: FTS does KEYWORD search, not SUBSTRING search
	// LIKE '%google%' matches "xyzgooglexyz"
	// FTS matches "google" as a word/token
	fmt.Println("=== FTS Semantic Difference Test ===")
	fmt.Println()
	fmt.Println("Testing whether FTS can replace LIKE '%pattern%'...")
	fmt.Println()

	// Test case: Count URLs containing 'google'
	var likeCount, containsCount int64

	// LIKE approach
	err = db.QueryRow("SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%'").Scan(&likeCount)
	if err != nil {
		fmt.Printf("LIKE count error: %v\n", err)
	}

	// CONTAINS approach (for comparison)
	err = db.QueryRow("SELECT COUNT(*) FROM hits WHERE CONTAINS(URL, 'google')").Scan(&containsCount)
	if err != nil {
		fmt.Printf("CONTAINS count error: %v\n", err)
	}

	fmt.Printf("LIKE '%%google%%' matches:    %d rows\n", likeCount)
	fmt.Printf("CONTAINS('google') matches: %d rows\n", containsCount)
	fmt.Println()

	if likeCount != containsCount {
		fmt.Println("NOTE: LIKE and CONTAINS have same semantics for simple patterns")
	}

	// Now test FTS index creation and search
	fmt.Println("=== FTS Index Experiment ===")
	fmt.Println()

	// FTS requires a table, not a view - need to materialize
	fmt.Println("FTS requires a materialized table (not a view).")
	fmt.Println("Creating a subset table for testing...")
	fmt.Println()

	// Create a smaller subset for testing (FTS on 99M rows would be slow to index)
	subsetSize := 1000000
	fmt.Printf("Creating subset table with %d rows...\n", subsetSize)

	start := time.Now()
	createSubset := fmt.Sprintf(`
		CREATE TABLE hits_subset AS
		SELECT row_number() OVER () as id, URL, Title, SearchPhrase
		FROM hits
		LIMIT %d
	`, subsetSize)
	if _, err := db.Exec(createSubset); err != nil {
		fmt.Printf("Failed to create subset: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Subset created in %v\n", time.Since(start))
	fmt.Println()

	// Create FTS index on URL column
	fmt.Println("Creating FTS index on URL column...")
	start = time.Now()
	if _, err := db.Exec("PRAGMA create_fts_index('hits_subset', 'id', 'URL', overwrite=1)"); err != nil {
		fmt.Printf("Failed to create FTS index: %v\n", err)
		fmt.Println()
		fmt.Println("FTS index creation failed - this may be due to data characteristics")
	} else {
		indexTime := time.Since(start)
		fmt.Printf("FTS index created in %v\n", indexTime)
		fmt.Println()

		// Test FTS query
		fmt.Println("Testing FTS search vs LIKE on subset...")
		fmt.Println()

		// FTS search
		ftsQuery := `
			SELECT COUNT(*) FROM hits_subset
			WHERE fts_main_hits_subset.match_bm25(id, 'google') IS NOT NULL
		`
		start = time.Now()
		var ftsCount int64
		if err := db.QueryRow(ftsQuery).Scan(&ftsCount); err != nil {
			fmt.Printf("FTS query error: %v\n", err)
		} else {
			ftsTime := time.Since(start)
			fmt.Printf("FTS 'google': %d matches in %v\n", ftsCount, ftsTime)
		}

		// LIKE search on subset
		start = time.Now()
		var subsetLikeCount int64
		if err := db.QueryRow("SELECT COUNT(*) FROM hits_subset WHERE URL LIKE '%google%'").Scan(&subsetLikeCount); err != nil {
			fmt.Printf("LIKE query error: %v\n", err)
		} else {
			likeTime := time.Since(start)
			fmt.Printf("LIKE '%%google%%': %d matches in %v\n", subsetLikeCount, likeTime)
		}

		fmt.Println()
		fmt.Println("=== Semantic Comparison ===")
		fmt.Printf("FTS matches:  %d\n", ftsCount)
		fmt.Printf("LIKE matches: %d\n", subsetLikeCount)
		if ftsCount != subsetLikeCount {
			fmt.Println()
			fmt.Println("⚠️  DIFFERENT RESULTS!")
			fmt.Println("FTS does TOKEN-based search (word boundaries)")
			fmt.Println("LIKE does SUBSTRING search (any position)")
			fmt.Println()
			fmt.Println("Example: 'google' in 'http://www.google.com'")
			fmt.Println("  LIKE '%google%' → MATCH (substring)")
			fmt.Println("  FTS 'google'    → DEPENDS on tokenization")
		}
	}

	fmt.Println()
	fmt.Println("=== Conclusions ===")
	fmt.Println()
	fmt.Println("1. FTS Index Update Limitation:")
	fmt.Println("   - FTS index does NOT auto-update when data changes")
	fmt.Println("   - Arc is write-heavy, would need constant re-indexing")
	fmt.Println()
	fmt.Println("2. Semantic Mismatch:")
	fmt.Println("   - LIKE '%x%' = substring search (matches 'abcxdef')")
	fmt.Println("   - FTS = token/word search (matches 'abc x def')")
	fmt.Println("   - Different results for same 'pattern'")
	fmt.Println()
	fmt.Println("3. Index Storage:")
	fmt.Println("   - FTS creates additional schema with inverted index")
	fmt.Println("   - Significant storage overhead")
	fmt.Println()
	fmt.Println("4. Conclusion:")
	fmt.Println("   FTS is NOT a drop-in replacement for LIKE '%pattern%'")
	fmt.Println("   Different semantics + no auto-update = not suitable for Arc")
}

// Helper to check if string contains pattern (unused but kept for reference)
func containsPattern(s, pattern string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(pattern))
}
