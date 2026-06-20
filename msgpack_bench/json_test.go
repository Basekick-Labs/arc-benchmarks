package msgpack_bench

import (
	"encoding/json"
	"math/rand"
	"testing"
	"time"

	goccy "github.com/goccy/go-json"
	segmentio "github.com/segmentio/encoding/json"
)

// ---------------------------------------------------------------------------
// Query response fixture matching Arc's QueryResponse struct shape
// ---------------------------------------------------------------------------

type QueryResponse struct {
	Success         bool            `json:"success"`
	Columns         []string        `json:"columns"`
	Data            [][]interface{} `json:"data"`
	RowCount        int             `json:"row_count"`
	ExecutionTimeMs float64         `json:"execution_time_ms"`
	Timestamp       string          `json:"timestamp"`
}

// smallQueryResponse: 10 rows, 5 columns — typical quick query
func smallQueryResponse() QueryResponse {
	data := make([][]interface{}, 10)
	for i := range data {
		data[i] = []interface{}{
			time.Now().UTC().Format(time.RFC3339Nano),
			"server01",
			rand.Float64() * 100,
			rand.Float64() * 20,
			int64(16),
		}
	}
	return QueryResponse{
		Success:         true,
		Columns:         []string{"time", "host", "usage_idle", "usage_system", "n_cpus"},
		Data:            data,
		RowCount:        10,
		ExecutionTimeMs: 12.5,
		Timestamp:       time.Now().UTC().Format(time.RFC3339Nano),
	}
}

// largeQueryResponse: 1000 rows, 8 columns — heavier analytical query
func largeQueryResponse() QueryResponse {
	data := make([][]interface{}, 1000)
	for i := range data {
		data[i] = []interface{}{
			time.Now().UTC().Format(time.RFC3339Nano),
			"server01",
			"us-east-1",
			rand.Float64() * 100,
			rand.Float64() * 20,
			rand.Float64() * 80,
			rand.Float64() * 4,
			int64(16),
		}
	}
	return QueryResponse{
		Success:         true,
		Columns:         []string{"time", "host", "region", "usage_idle", "usage_system", "usage_user", "load1", "n_cpus"},
		Data:            data,
		RowCount:        1000,
		ExecutionTimeMs: 45.2,
		Timestamp:       time.Now().UTC().Format(time.RFC3339Nano),
	}
}

// ---------------------------------------------------------------------------
// Benchmarks: Small Query Response (10 rows)
// ---------------------------------------------------------------------------

func BenchmarkJSON_StdLib_Marshal_SmallQuery(b *testing.B) {
	resp := smallQueryResponse()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := json.Marshal(resp); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSON_Goccy_Marshal_SmallQuery(b *testing.B) {
	resp := smallQueryResponse()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := goccy.Marshal(resp); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSON_Segmentio_Marshal_SmallQuery(b *testing.B) {
	resp := smallQueryResponse()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := segmentio.Marshal(resp); err != nil {
			b.Fatal(err)
		}
	}
}

// ---------------------------------------------------------------------------
// Benchmarks: Large Query Response (1000 rows)
// ---------------------------------------------------------------------------

func BenchmarkJSON_StdLib_Marshal_LargeQuery(b *testing.B) {
	resp := largeQueryResponse()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := json.Marshal(resp); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSON_Goccy_Marshal_LargeQuery(b *testing.B) {
	resp := largeQueryResponse()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := goccy.Marshal(resp); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSON_Segmentio_Marshal_LargeQuery(b *testing.B) {
	resp := largeQueryResponse()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := segmentio.Marshal(resp); err != nil {
			b.Fatal(err)
		}
	}
}

// ---------------------------------------------------------------------------
// Benchmarks: Unmarshal (client-side decoding, for reference)
// ---------------------------------------------------------------------------

func BenchmarkJSON_StdLib_Unmarshal_LargeQuery(b *testing.B) {
	resp := largeQueryResponse()
	encoded, _ := json.Marshal(resp)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var out QueryResponse
		if err := json.Unmarshal(encoded, &out); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSON_Goccy_Unmarshal_LargeQuery(b *testing.B) {
	resp := largeQueryResponse()
	encoded, _ := json.Marshal(resp)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var out QueryResponse
		if err := goccy.Unmarshal(encoded, &out); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSON_Segmentio_Unmarshal_LargeQuery(b *testing.B) {
	resp := largeQueryResponse()
	encoded, _ := json.Marshal(resp)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var out QueryResponse
		if err := segmentio.Unmarshal(encoded, &out); err != nil {
			b.Fatal(err)
		}
	}
}
