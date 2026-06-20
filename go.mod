// Benchmarks are a separate Go module so their dependencies (ClickHouse,
// PostgreSQL/pgx, tantivy-go, etc.) do not appear in the product module's
// go.mod or SBOM artifacts. They are not part of the Arc release binary.
//
// To run a benchmark:
//   cd benchmarks && go run ./log_bench [flags]
//   cd benchmarks && go run ./sustained_bench [flags]
//
// To update deps:
//   cd benchmarks && go mod tidy

module github.com/basekick-labs/arc/benchmarks

go 1.26

require (
	github.com/Basekick-Labs/msgpack/v6 v6.1.0
	github.com/ClickHouse/clickhouse-go/v2 v2.43.0
	github.com/apache/arrow-go/v18 v18.5.2
	github.com/duckdb/duckdb-go/v2 v2.10501.0
	github.com/goccy/go-json v0.10.5
	github.com/google/uuid v1.6.0
	github.com/jackc/pgx/v5 v5.9.2
	github.com/klauspost/compress v1.18.4
)
