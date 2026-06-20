// TLE ingestion benchmark for Arc
// Generates synthetic TLE satellite data and measures ingestion throughput/latency.
//
// Usage:
//   go run benchmarks/tle_bench/main.go --duration 30
//   go run benchmarks/tle_bench/main.go --duration 60 --workers 50 --satellites 5000
//   go run benchmarks/tle_bench/main.go --compress gzip --workers 100

package main

import (
	"bytes"
	"compress/gzip"
	"flag"
	"fmt"
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
	Duration    int
	Workers     int
	Satellites  int    // Number of satellites per batch
	Pregenerate int    // Number of batches to pre-generate
	Compress    string // "none" or "gzip"
	Host        string
	Port        int
	Token       string
	Database    string
	Measurement string
}

type Stats struct {
	totalSent   atomic.Int64
	totalErrors atomic.Int64
	running     atomic.Bool
	// Per-worker latency slices to avoid mutex contention during test
	workerLatencies [][]float64
	latencyMu       sync.Mutex
}

func (s *Stats) initWorkers(n int) {
	s.workerLatencies = make([][]float64, n)
	for i := range s.workerLatencies {
		s.workerLatencies[i] = make([]float64, 0, 10000)
	}
}

func (s *Stats) addLatency(workerID int, ms float64) {
	s.workerLatencies[workerID] = append(s.workerLatencies[workerID], ms)
}

func (s *Stats) getPercentile(p float64) float64 {
	var total int
	for _, wl := range s.workerLatencies {
		total += len(wl)
	}
	if total == 0 {
		return 0
	}

	all := make([]float64, 0, total)
	for _, wl := range s.workerLatencies {
		all = append(all, wl...)
	}
	sort.Float64s(all)

	idx := int(float64(len(all)) * p)
	if idx >= len(all) {
		idx = len(all) - 1
	}
	return all[idx]
}

// Satellite archetypes for realistic TLE generation
var constellations = []struct {
	prefix      string
	inclination float64 // degrees
	altitude    float64 // km (approximate)
	count       int     // how many in constellation
}{
	{"STARLINK", 53.0, 550, 5000},
	{"ONEWEB", 87.9, 1200, 600},
	{"IRIDIUM", 86.4, 780, 66},
	{"GPS", 55.0, 20200, 31},
	{"GALILEO", 56.0, 23222, 30},
	{"BEIDOU", 55.0, 21528, 35},
	{"COSMOS", 82.5, 950, 200},
	{"NOAA", 99.2, 850, 20},
	{"GOES", 0.1, 35786, 5},
	{"METEOR", 98.7, 830, 15},
	{"SENTINEL", 98.6, 693, 10},
	{"LANDSAT", 98.2, 705, 8},
	{"PLANET", 97.4, 475, 200},
	{"SPIRE", 97.5, 500, 100},
}

// computeChecksum calculates the TLE line checksum (mod 10 of digit/minus sum)
func computeChecksum(line string) int {
	sum := 0
	for i := 0; i < len(line)-1; i++ {
		c := line[i]
		if c >= '0' && c <= '9' {
			sum += int(c - '0')
		} else if c == '-' {
			sum += 1
		}
	}
	return sum % 10
}

// generateTLEEntry creates a realistic synthetic TLE entry for a satellite
func generateTLEEntry(noradID int, name string, inclination, altitude float64, epochYear int, epochDay float64) string {
	// Derive mean motion from altitude using Kepler's third law
	mu := 398600.4418     // km³/s² (Earth GM)
	earthRadius := 6371.0 // km
	a := earthRadius + altitude
	period := 2 * math.Pi * math.Sqrt(a*a*a/mu) // seconds
	meanMotion := 86400.0 / period              // rev/day

	// Random orbital elements within realistic ranges
	eccentricity := rand.Float64() * 0.01 // LEO/MEO are near-circular
	if altitude > 30000 {
		eccentricity = rand.Float64() * 0.001 // GEO very circular
	}
	raan := rand.Float64() * 360
	argPerigee := rand.Float64() * 360
	meanAnomaly := rand.Float64() * 360
	revNumber := 10000 + rand.Intn(90000)

	// Mean motion derivative (small positive for LEO drag)
	mmDot := 0.00001 + rand.Float64()*0.0005
	if altitude > 1000 {
		mmDot = rand.Float64() * 0.000001
	}

	// BSTAR drag term
	bstarMant := rand.Intn(99999)
	bstarExp := -4
	if altitude > 1000 {
		bstarExp = -5
	}

	// International designator
	launchYear := 10 + rand.Intn(16) // 2010-2025
	launchNum := 1 + rand.Intn(200)
	piece := string(rune('A' + rand.Intn(26)))

	// Element set number
	elemSet := 100 + rand.Intn(9000)

	// Build line 1 (69 chars + checksum)
	line1 := fmt.Sprintf("1 %05dU %02d%03d%-3s %02d%012.8f  .%08.0f  00000-0  %05d%d 0 %4d",
		noradID,
		launchYear, launchNum, piece,
		epochYear%100, epochDay,
		mmDot*1e8,
		bstarMant, bstarExp+10, // encode exponent
		elemSet,
	)

	// Fix BSTAR formatting — use proper TLE modified exponential
	bstarStr := fmt.Sprintf(" %05d%+d", bstarMant, bstarExp)
	if len(bstarStr) > 8 {
		bstarStr = bstarStr[:8]
	}

	// Rebuild line 1 properly
	intDesig := fmt.Sprintf("%02d%03d%-3s", launchYear, launchNum, piece)
	line1 = fmt.Sprintf("1 %05dU %-8s %02d%012.8f  .%08.0f  00000+0 %8s 0 %4d",
		noradID,
		intDesig,
		epochYear%100, epochDay,
		mmDot*1e8,
		bstarStr,
		elemSet,
	)
	// Pad/trim to exactly 68 chars before checksum
	for len(line1) < 68 {
		line1 += " "
	}
	line1 = line1[:68]
	line1 += fmt.Sprintf("%d", computeChecksum(line1+"0"))

	// Build line 2
	line2 := fmt.Sprintf("2 %05d %8.4f %8.4f %07d %8.4f %8.4f %11.8f%05d",
		noradID,
		inclination,
		raan,
		int(eccentricity*1e7),
		argPerigee,
		meanAnomaly,
		meanMotion,
		revNumber,
	)
	for len(line2) < 68 {
		line2 += " "
	}
	line2 = line2[:68]
	line2 += fmt.Sprintf("%d", computeChecksum(line2+"0"))

	return fmt.Sprintf("%s\n%s\n%s\n", name, line1, line2)
}

func generateTLEBatches(count, satellites int, compress string) [][]byte {
	batches := make([][]byte, count)

	// Build a catalog of satellite templates
	type satTemplate struct {
		noradID     int
		name        string
		inclination float64
		altitude    float64
	}

	catalog := make([]satTemplate, 0, satellites)
	noradBase := 25544 // Start near ISS
	for len(catalog) < satellites {
		for _, c := range constellations {
			needed := int(float64(satellites) * float64(c.count) / 6400.0)
			if needed < 1 {
				needed = 1
			}
			for j := 0; j < needed && len(catalog) < satellites; j++ {
				catalog = append(catalog, satTemplate{
					noradID:     noradBase,
					name:        fmt.Sprintf("%s-%d", c.prefix, j+1),
					inclination: c.inclination + (rand.Float64()-0.5)*0.5,
					altitude:    c.altitude + (rand.Float64()-0.5)*50,
				})
				noradBase++
			}
		}
	}

	for i := 0; i < count; i++ {
		// Each batch has a slightly different epoch
		now := time.Now().UTC()
		dayOfYear := float64(now.YearDay()) + float64(now.Hour())/24.0 +
			float64(now.Minute())/1440.0 + float64(now.Second())/86400.0
		epochYear := now.Year()

		// Jitter epoch slightly per batch to simulate different update times
		dayOfYear += float64(i) * 0.001

		var buf bytes.Buffer
		for _, sat := range catalog {
			buf.WriteString(generateTLEEntry(
				sat.noradID, sat.name,
				sat.inclination, sat.altitude,
				epochYear, dayOfYear,
			))
		}

		data := buf.Bytes()

		if compress == "gzip" {
			var compBuf bytes.Buffer
			w, _ := gzip.NewWriterLevel(&compBuf, gzip.BestSpeed)
			w.Write(data)
			w.Close()
			data = compBuf.Bytes()
		}

		batches[i] = data

		if (i+1)%100 == 0 {
			fmt.Printf("  Progress: %d/%d\n", i+1, count)
		}
	}

	return batches
}

func worker(id int, cfg *Config, batches [][]byte, stats *Stats, client *http.Client) {
	url := fmt.Sprintf("http://%s:%d/api/v1/write/tle", cfg.Host, cfg.Port)

	headers := map[string]string{
		"Content-Type":   "text/plain",
		"x-arc-database": cfg.Database,
	}
	if cfg.Measurement != "" {
		headers["x-arc-measurement"] = cfg.Measurement
	}
	if cfg.Token != "" {
		headers["Authorization"] = "Bearer " + cfg.Token
	}
	if cfg.Compress == "gzip" {
		headers["Content-Encoding"] = "gzip"
	}

	batchIdx := 0
	for stats.running.Load() {
		batch := batches[batchIdx%len(batches)]
		batchIdx++

		start := time.Now()

		req, err := http.NewRequest("POST", url, bytes.NewReader(batch))
		if err != nil {
			stats.totalErrors.Add(1)
			continue
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}

		resp, err := client.Do(req)
		if err != nil {
			stats.totalErrors.Add(1)
			if stats.totalErrors.Load() <= 3 {
				fmt.Printf("Error: %v\n", err)
			}
			continue
		}

		latencyMs := float64(time.Since(start).Microseconds()) / 1000.0

		if resp.StatusCode == 204 {
			stats.totalSent.Add(int64(cfg.Satellites))
			stats.addLatency(id, latencyMs)
		} else {
			stats.totalErrors.Add(1)
			if stats.totalErrors.Load() <= 3 {
				body, _ := io.ReadAll(resp.Body)
				fmt.Printf("Error %d: %s\n", resp.StatusCode, string(body)[:min(100, len(body))])
			}
		}
		resp.Body.Close()
	}
}

func main() {
	cfg := Config{}

	flag.IntVar(&cfg.Duration, "duration", 30, "Test duration in seconds")
	flag.IntVar(&cfg.Workers, "workers", 10, "Number of concurrent workers")
	flag.IntVar(&cfg.Satellites, "satellites", 100, "Number of satellites per batch")
	flag.IntVar(&cfg.Pregenerate, "pregenerate", 500, "Number of batches to pre-generate")
	flag.StringVar(&cfg.Compress, "compress", "none", "Compression: none, gzip")
	flag.StringVar(&cfg.Host, "host", "localhost", "Server host")
	flag.IntVar(&cfg.Port, "port", 8000, "Server port")
	flag.StringVar(&cfg.Database, "database", "benchmarks", "Target database")
	flag.StringVar(&cfg.Measurement, "measurement", "", "Target measurement (default: satellite_tle)")
	flag.Parse()

	cfg.Token = os.Getenv("ARC_TOKEN")

	compressLabel := "No Compression"
	if cfg.Compress == "gzip" {
		compressLabel = "WITH GZIP COMPRESSION"
	}

	measurementLabel := "satellite_tle (default)"
	if cfg.Measurement != "" {
		measurementLabel = cfg.Measurement
	}

	fmt.Println("================================================================================")
	fmt.Printf("TLE INGESTION BENCHMARK (%s)\n", compressLabel)
	fmt.Println("================================================================================")
	fmt.Printf("Target: http://%s:%d/api/v1/write/tle\n", cfg.Host, cfg.Port)
	fmt.Printf("Database: %s\n", cfg.Database)
	fmt.Printf("Measurement: %s\n", measurementLabel)
	fmt.Printf("Duration: %ds\n", cfg.Duration)
	fmt.Printf("Satellites per batch: %d\n", cfg.Satellites)
	fmt.Printf("Workers: %d\n", cfg.Workers)
	fmt.Printf("Pre-generate: %d batches\n", cfg.Pregenerate)
	fmt.Println("================================================================================")
	fmt.Println()

	// Generate batches
	fmt.Printf("Pre-generating %d batches (%d satellites each)...\n", cfg.Pregenerate, cfg.Satellites)
	startGen := time.Now()

	batches := generateTLEBatches(cfg.Pregenerate, cfg.Satellites, cfg.Compress)

	genTime := time.Since(startGen)
	var totalSize int64
	for _, b := range batches {
		totalSize += int64(len(b))
	}
	avgSize := float64(totalSize) / float64(len(batches))

	sizeLabel := "uncompressed"
	if cfg.Compress != "none" {
		sizeLabel = cfg.Compress
	}
	fmt.Printf("Generated %d batches in %.1fs\n", cfg.Pregenerate, genTime.Seconds())
	fmt.Printf("  Avg batch size: %.1f KB (%s)\n", avgSize/1024, sizeLabel)
	fmt.Printf("  Avg per satellite: %.0f bytes\n", avgSize/float64(cfg.Satellites))
	fmt.Println()

	if cfg.Token != "" {
		fmt.Printf("Using auth token: %s...\n", cfg.Token[:min(8, len(cfg.Token))])
	} else {
		fmt.Println("No ARC_TOKEN set - running without authentication")
	}

	// Create HTTP client with connection pooling
	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        cfg.Workers + 50,
			MaxIdleConnsPerHost: cfg.Workers + 50,
			MaxConnsPerHost:     cfg.Workers + 50,
			IdleConnTimeout:     30 * time.Second,
			DisableKeepAlives:   false,
		},
		Timeout: 120 * time.Second,
	}

	// Run benchmark
	fmt.Println("Starting test...")
	fmt.Println()
	stats := &Stats{}
	stats.initWorkers(cfg.Workers)
	stats.running.Store(true)

	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			worker(id, &cfg, batches, stats, client)
		}(i)
	}

	// Progress reporting
	startTime := time.Now()
	lastSent := int64(0)
	ticker := time.NewTicker(5 * time.Second)

	go func() {
		for range ticker.C {
			if !stats.running.Load() {
				return
			}
			elapsed := time.Since(startTime).Seconds()
			currentSent := stats.totalSent.Load()
			intervalRPS := float64(currentSent-lastSent) / 5.0
			fmt.Printf("[%6.1fs] RPS: %10.0f | Total: %12d | Errors: %6d\n",
				elapsed, intervalRPS, currentSent, stats.totalErrors.Load())
			lastSent = currentSent
		}
	}()

	// Wait for duration
	time.Sleep(time.Duration(cfg.Duration) * time.Second)
	stats.running.Store(false)
	ticker.Stop()

	// Wait for workers to finish
	wg.Wait()

	elapsed := time.Since(startTime).Seconds()
	totalSent := stats.totalSent.Load()
	totalErrors := stats.totalErrors.Load()

	// Results
	fmt.Println()
	fmt.Println("================================================================================")
	fmt.Println("RESULTS")
	fmt.Println("================================================================================")
	fmt.Printf("Duration:        %.1fs\n", elapsed)
	fmt.Printf("Total sent:      %d satellite records\n", totalSent)
	fmt.Printf("Total errors:    %d\n", totalErrors)
	successRate := float64(totalSent) / float64(totalSent+max(totalErrors, 1)) * 100
	fmt.Printf("Success rate:    %.2f%%\n", successRate)
	fmt.Println()
	fmt.Printf("THROUGHPUT:   %d satellite records/sec\n", int64(float64(totalSent)/elapsed))
	fmt.Printf("              %d batches/sec\n", int64(float64(totalSent)/float64(cfg.Satellites)/elapsed))
	fmt.Println()
	fmt.Println("Latency percentiles (per batch):")
	fmt.Printf("  p50:  %.2f ms\n", stats.getPercentile(0.50))
	fmt.Printf("  p95:  %.2f ms\n", stats.getPercentile(0.95))
	fmt.Printf("  p99:  %.2f ms\n", stats.getPercentile(0.99))
	fmt.Printf("  p999: %.2f ms\n", stats.getPercentile(0.999))
	fmt.Println("================================================================================")
}
