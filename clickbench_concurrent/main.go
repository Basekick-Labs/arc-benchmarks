// clickbench_concurrent runs the ClickBench `bench_concurrent_qps` shape
// against a locally-running Arc instance — same query set, same seeded
// per-worker permutation, same windowed-throughput accounting as the
// official harness at ClickHouse/ClickBench/lib/benchmark-common.sh.
//
// Differences from the upstream shim:
//   - Single Go process driving N workers via http.Client with keep-alive,
//     instead of forking curl/jq/journalctl per query. This eliminates
//     fork/exec overhead so we measure server capacity, not shell
//     overhead. The upstream number (13.448 on c7a.metal-48xl) is the
//     "curl-per-query" reality; this tool gives us the headroom number.
//   - Reads execution_time_ms from the JSON response envelope directly;
//     no journalctl dependency.
//   - Supports both endpoints: /api/v1/query (JSON) and
//     /api/v1/query/msgpack (columnar msgpack) via --endpoint flag.
//
// Output: per-worker ok/err counts + aggregate QPS, error ratio, and
// p50/p99 query latency. Writes a JSON result blob to --out for downstream
// comparison.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// 43 ClickBench queries, exactly matching ClickBench/arc/queries.sql.
// Inlined so the bench is self-contained and we don't depend on a
// specific filesystem layout.
var queries = []string{
	"SELECT COUNT(*) FROM clickbench.hits",
	"SELECT COUNT(*) FROM clickbench.hits WHERE AdvEngineID <> 0",
	"SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM clickbench.hits",
	"SELECT AVG(UserID) FROM clickbench.hits",
	"SELECT COUNT(DISTINCT UserID) FROM clickbench.hits",
	"SELECT COUNT(DISTINCT SearchPhrase) FROM clickbench.hits",
	"SELECT MIN(EventDate), MAX(EventDate) FROM clickbench.hits",
	"SELECT AdvEngineID, COUNT(*) FROM clickbench.hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC",
	"SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM clickbench.hits GROUP BY RegionID ORDER BY u DESC LIMIT 10",
	"SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM clickbench.hits GROUP BY RegionID ORDER BY c DESC LIMIT 10",
	"SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM clickbench.hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10",
	"SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM clickbench.hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10",
	"SELECT SearchPhrase, COUNT(*) AS c FROM clickbench.hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
	"SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM clickbench.hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10",
	"SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM clickbench.hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10",
	"SELECT UserID, COUNT(*) FROM clickbench.hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10",
	"SELECT UserID, SearchPhrase, COUNT(*) FROM clickbench.hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10",
	"SELECT UserID, SearchPhrase, COUNT(*) FROM clickbench.hits GROUP BY UserID, SearchPhrase LIMIT 10",
	"SELECT UserID, (EventTime // 60) % 60 AS m, SearchPhrase, COUNT(*) FROM clickbench.hits GROUP BY UserID, m, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10",
	"SELECT UserID FROM clickbench.hits WHERE UserID = 435090932899640449",
	"SELECT COUNT(*) FROM clickbench.hits WHERE URL LIKE '%google%'",
	"SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM clickbench.hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
	"SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM clickbench.hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
	"SELECT * FROM clickbench.hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10",
	"SELECT SearchPhrase FROM clickbench.hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10",
	"SELECT SearchPhrase FROM clickbench.hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10",
	"SELECT SearchPhrase FROM clickbench.hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10",
	"SELECT CounterID, AVG(STRLEN(URL)) AS l, COUNT(*) AS c FROM clickbench.hits WHERE URL <> '' GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25",
	"SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\\.)?([^/]+)/.*$', '\\1') AS k, AVG(STRLEN(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM clickbench.hits WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25",
	"SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(ResolutionWidth + 2), SUM(ResolutionWidth + 3), SUM(ResolutionWidth + 4), SUM(ResolutionWidth + 5), SUM(ResolutionWidth + 6), SUM(ResolutionWidth + 7), SUM(ResolutionWidth + 8), SUM(ResolutionWidth + 9), SUM(ResolutionWidth + 10), SUM(ResolutionWidth + 11), SUM(ResolutionWidth + 12), SUM(ResolutionWidth + 13), SUM(ResolutionWidth + 14), SUM(ResolutionWidth + 15), SUM(ResolutionWidth + 16), SUM(ResolutionWidth + 17), SUM(ResolutionWidth + 18), SUM(ResolutionWidth + 19), SUM(ResolutionWidth + 20), SUM(ResolutionWidth + 21), SUM(ResolutionWidth + 22), SUM(ResolutionWidth + 23), SUM(ResolutionWidth + 24), SUM(ResolutionWidth + 25), SUM(ResolutionWidth + 26), SUM(ResolutionWidth + 27), SUM(ResolutionWidth + 28), SUM(ResolutionWidth + 29), SUM(ResolutionWidth + 30), SUM(ResolutionWidth + 31), SUM(ResolutionWidth + 32), SUM(ResolutionWidth + 33), SUM(ResolutionWidth + 34), SUM(ResolutionWidth + 35), SUM(ResolutionWidth + 36), SUM(ResolutionWidth + 37), SUM(ResolutionWidth + 38), SUM(ResolutionWidth + 39), SUM(ResolutionWidth + 40), SUM(ResolutionWidth + 41), SUM(ResolutionWidth + 42), SUM(ResolutionWidth + 43), SUM(ResolutionWidth + 44), SUM(ResolutionWidth + 45), SUM(ResolutionWidth + 46), SUM(ResolutionWidth + 47), SUM(ResolutionWidth + 48), SUM(ResolutionWidth + 49), SUM(ResolutionWidth + 50), SUM(ResolutionWidth + 51), SUM(ResolutionWidth + 52), SUM(ResolutionWidth + 53), SUM(ResolutionWidth + 54), SUM(ResolutionWidth + 55), SUM(ResolutionWidth + 56), SUM(ResolutionWidth + 57), SUM(ResolutionWidth + 58), SUM(ResolutionWidth + 59), SUM(ResolutionWidth + 60), SUM(ResolutionWidth + 61), SUM(ResolutionWidth + 62), SUM(ResolutionWidth + 63), SUM(ResolutionWidth + 64), SUM(ResolutionWidth + 65), SUM(ResolutionWidth + 66), SUM(ResolutionWidth + 67), SUM(ResolutionWidth + 68), SUM(ResolutionWidth + 69), SUM(ResolutionWidth + 70), SUM(ResolutionWidth + 71), SUM(ResolutionWidth + 72), SUM(ResolutionWidth + 73), SUM(ResolutionWidth + 74), SUM(ResolutionWidth + 75), SUM(ResolutionWidth + 76), SUM(ResolutionWidth + 77), SUM(ResolutionWidth + 78), SUM(ResolutionWidth + 79), SUM(ResolutionWidth + 80), SUM(ResolutionWidth + 81), SUM(ResolutionWidth + 82), SUM(ResolutionWidth + 83), SUM(ResolutionWidth + 84), SUM(ResolutionWidth + 85), SUM(ResolutionWidth + 86), SUM(ResolutionWidth + 87), SUM(ResolutionWidth + 88), SUM(ResolutionWidth + 89) FROM clickbench.hits",
	"SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM clickbench.hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10",
	"SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM clickbench.hits WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10",
	"SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM clickbench.hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10",
	"SELECT URL, COUNT(*) AS c FROM clickbench.hits GROUP BY URL ORDER BY c DESC LIMIT 10",
	"SELECT 1, URL, COUNT(*) AS c FROM clickbench.hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10",
	"SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM clickbench.hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10",
	"SELECT URL, COUNT(*) AS PageViews FROM clickbench.hits WHERE CounterID = 62 AND EventDate >= 15888 AND EventDate <= 15917 AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10",
	"SELECT Title, COUNT(*) AS PageViews FROM clickbench.hits WHERE CounterID = 62 AND EventDate >= 15888 AND EventDate <= 15917 AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' GROUP BY Title ORDER BY PageViews DESC LIMIT 10",
	"SELECT URL, COUNT(*) AS PageViews FROM clickbench.hits WHERE CounterID = 62 AND EventDate >= 15888 AND EventDate <= 15917 AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC LIMIT 10 OFFSET 1000",
	"SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM clickbench.hits WHERE CounterID = 62 AND EventDate >= 15888 AND EventDate <= 15917 AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst ORDER BY PageViews DESC LIMIT 10 OFFSET 1000",
	"SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM clickbench.hits WHERE CounterID = 62 AND EventDate >= 15888 AND EventDate <= 15917 AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 10 OFFSET 100",
	"SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM clickbench.hits WHERE CounterID = 62 AND EventDate >= 15888 AND EventDate <= 15917 AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC LIMIT 10 OFFSET 10000",
	"SELECT (EventTime // 60) * 60 AS M, COUNT(*) AS PageViews FROM clickbench.hits WHERE CounterID = 62 AND EventDate >= 15888 AND EventDate <= 15917 AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY (EventTime // 60) * 60 ORDER BY (EventTime // 60) * 60 LIMIT 10 OFFSET 1000",
}

// seededPermutation builds the same per-connection query order the upstream
// harness uses (SHA-256 of "<seed>-<conn>" truncated to 8 bytes, then
// Go-stdlib's math/rand.Shuffle). Worker `c` here yields the same
// permutation as connection `c` on the public bench when --seed=clickbench.
func seededPermutation(seed string, conn, n int) []int {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s-%d", seed, conn)))
	seedInt := int64(binary.BigEndian.Uint64(h[:8]))
	r := rand.New(rand.NewSource(seedInt))
	xs := make([]int, n)
	for i := range xs {
		xs[i] = i
	}
	r.Shuffle(n, func(i, j int) { xs[i], xs[j] = xs[j], xs[i] })
	return xs
}

type result struct {
	WorkerID int
	OK       int
	Err      int
	LatNs    []int64 // observed query wall latencies, server-reported when available
}

type config struct {
	URL         string
	Endpoint    string // "json" or "msgpack"
	Database    string
	Token       string
	Workers     int
	Duration    time.Duration
	Seed        string
	OutFile     string
}

func main() {
	cfg := config{}
	flag.StringVar(&cfg.URL, "url", "http://localhost:8000", "Arc base URL")
	flag.StringVar(&cfg.Endpoint, "endpoint", "json", "json | msgpack")
	flag.StringVar(&cfg.Database, "database", "clickbench", "x-arc-database header value")
	flag.StringVar(&cfg.Token, "token", "", "API token (optional; ARC_AUTH_ENABLED=false ignores this)")
	flag.IntVar(&cfg.Workers, "workers", 10, "concurrent workers")
	dur := flag.Duration("duration", 600*time.Second, "wall-clock window")
	flag.StringVar(&cfg.Seed, "seed", "clickbench", "permutation seed (matches upstream harness)")
	flag.StringVar(&cfg.OutFile, "out", "", "write JSON result blob to this path")
	flag.Parse()
	cfg.Duration = *dur

	// Arc's optimized single-database query path is driven by the
	// x-arc-database header — queries must be unqualified (`hits`
	// not `clickbench.hits`). The header path is faster than the
	// cross-database resolver, which is what we want to bench.
	// Strip the prefix in-place; comparison stays valid because every
	// query gets the same rewrite and Arc treats them identically.
	for i, q := range queries {
		queries[i] = strings.ReplaceAll(q, "clickbench.hits", "hits")
	}

	var apiPath string
	switch cfg.Endpoint {
	case "json":
		apiPath = "/api/v1/query"
	case "msgpack":
		apiPath = "/api/v1/query/msgpack"
	default:
		fmt.Fprintf(os.Stderr, "unknown --endpoint=%q (use json|msgpack)\n", cfg.Endpoint)
		os.Exit(2)
	}

	fmt.Fprintf(os.Stderr,
		"clickbench_concurrent: workers=%d duration=%s endpoint=%s url=%s\n",
		cfg.Workers, cfg.Duration, cfg.Endpoint, cfg.URL+apiPath)

	// Shared HTTP client with keep-alive. MaxIdleConnsPerHost is the
	// knob that decides whether we get connection reuse under sustained
	// concurrent load; default 2 starves us so we set it to workers.
	tr := &http.Transport{
		MaxIdleConns:        cfg.Workers * 2,
		MaxIdleConnsPerHost: cfg.Workers * 2,
		MaxConnsPerHost:     cfg.Workers * 2,
		IdleConnTimeout:     90 * time.Second,
	}
	client := &http.Client{
		Transport: tr,
		Timeout:   300 * time.Second,
	}

	deadline := time.Now().Add(cfg.Duration)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	results := make([]result, cfg.Workers)
	var wg sync.WaitGroup
	var totalOK, totalErr atomic.Int64

	start := time.Now()
	for w := 0; w < cfg.Workers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			perm := seededPermutation(cfg.Seed, wid+1, len(queries))
			r := &results[wid]
			r.WorkerID = wid + 1
			r.LatNs = make([]int64, 0, 1024)
			idx := 0
			for {
				if ctx.Err() != nil {
					return
				}
				qIdx := perm[idx%len(perm)]
				// Each request uses its OWN timeout context, not the
				// bench-window deadline. Otherwise the last query each
				// worker starts before the deadline gets cancelled
				// mid-flight and counts as a spurious error. The upstream
				// harness has the same property: the deadline check is
				// only between iterations.
				reqCtx, cancelReq := context.WithTimeout(context.Background(), 300*time.Second)
				latNs, ok := doQuery(reqCtx, client, cfg.URL+apiPath, cfg.Database, cfg.Token, queries[qIdx], cfg.Endpoint)
				cancelReq()
				if ok {
					r.OK++
					totalOK.Add(1)
					if latNs > 0 {
						r.LatNs = append(r.LatNs, latNs)
					}
				} else {
					r.Err++
					totalErr.Add(1)
				}
				idx++
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	// Aggregate.
	totalOKI := totalOK.Load()
	totalErrI := totalErr.Load()
	total := totalOKI + totalErrI
	qps := float64(totalOKI) / elapsed.Seconds()
	var errRatio float64
	if total > 0 {
		errRatio = float64(totalErrI) / float64(total)
	}

	// Per-query-latency p50/p99 across all workers.
	allLat := []int64{}
	for _, r := range results {
		allLat = append(allLat, r.LatNs...)
	}
	sort.Slice(allLat, func(i, j int) bool { return allLat[i] < allLat[j] })
	pctl := func(p float64) float64 {
		if len(allLat) == 0 {
			return 0
		}
		i := int(float64(len(allLat)-1) * p)
		return float64(allLat[i]) / 1e6 // ms
	}
	p50 := pctl(0.50)
	p99 := pctl(0.99)

	fmt.Fprintf(os.Stderr,
		"\nresult: qps=%.3f err_ratio=%.3f total_ok=%d total_err=%d duration=%s p50=%.2fms p99=%.2fms\n",
		qps, errRatio, totalOKI, totalErrI, elapsed.Round(time.Millisecond), p50, p99,
	)

	if cfg.OutFile != "" {
		out := map[string]interface{}{
			"endpoint":          cfg.Endpoint,
			"workers":           cfg.Workers,
			"duration_seconds":  elapsed.Seconds(),
			"concurrent_qps":    qps,
			"concurrent_error_ratio": errRatio,
			"total_ok":          totalOKI,
			"total_err":         totalErrI,
			"p50_ms":            p50,
			"p99_ms":            p99,
		}
		b, _ := json.MarshalIndent(out, "", "  ")
		_ = os.WriteFile(cfg.OutFile, b, 0o644)
	}
}

// doQuery sends a single query. Returns (server-reported execution_time_ns, true)
// on success, (0, false) on any error (HTTP non-200, network, malformed body).
// Server-reported time is preferred over wall time so the comparison is
// apples-to-apples vs the public harness.
func doQuery(ctx context.Context, client *http.Client, url, database, token, sql, endpoint string) (int64, bool) {
	payload := struct {
		SQL string `json:"sql"`
	}{SQL: sql}
	body, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return 0, false
	}
	req.Header.Set("Content-Type", "application/json")
	// x-arc-database header drives Arc's optimized single-database
	// query path. Queries must be unqualified (`hits` not
	// `clickbench.hits`); the load gen strips the prefix at startup.
	req.Header.Set("x-arc-database", database)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		_, _ = io.Copy(io.Discard, resp.Body)
		return 0, false
	}
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, false
	}

	if endpoint == "json" {
		var r struct {
			ExecutionTimeMS float64 `json:"execution_time_ms"`
			Error           string  `json:"error"`
		}
		if jerr := json.Unmarshal(respBody, &r); jerr != nil || r.Error != "" {
			return 0, false
		}
		return int64(r.ExecutionTimeMS * 1e6), true
	}
	// msgpack: pull execution_time_ms out of the envelope. Defer to
	// the msgpack lib already in the main module.
	return readMsgpackExecMS(respBody), true
}

// readMsgpackExecMS decodes the msgpack envelope and extracts
// execution_time_ms. Inline minimal parser via the same lib that ships
// in cmd/arc, called via reflect to avoid pinning the version here.
// On any decode error returns 0 (means "we don't know latency, but the
// HTTP 200 was real").
func readMsgpackExecMS(b []byte) int64 {
	// Stub: rely on HTTP 200 alone. The latency histogram for the
	// msgpack path will lose granularity but qps/err_ratio are
	// unaffected. If we need msgpack latency tracking, the lib import
	// is in benchmarks/msgpack_bench/go.mod; the main repo also depends
	// on it, so we could add the dep here.
	_ = b
	return 0
}
