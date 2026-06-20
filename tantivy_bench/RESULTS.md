# Tantivy FTS Benchmark Results

**Date:** February 2026
**Platform:** darwin arm64, Apple M3 Pro, 14 cores
**Dataset:** 500,000 synthetic log records (deterministic seed)
**Tantivy:** anyproto/tantivy-go v1.0.6 (CGo bindings for Rust Tantivy)
**DuckDB:** v2.5.4 (read_parquet baseline)

## Indexing

| Index Type     | Time    | Docs/sec | Size    | vs Parquet |
|----------------|---------|----------|---------|------------|
| Parquet        | -       | -        | 11.7 MB | 1.00x      |
| Tantivy Simple | 11.4s   | 43.9K/s  | 30.8 MB | 2.64x      |
| Tantivy Ngram  | 20.6s   | 24.3K/s  | 75.6 MB | 6.47x      |

## Search (500K records, 5 iterations avg)

| Query                | DuckDB LIKE | Tantivy Simple | Tantivy Ngram | DuckDB # | Ngram # | Match? |
|----------------------|-------------|----------------|---------------|----------|---------|--------|
| Single keyword       | 11.9ms      | 21.2ms         | 23.2ms        | 2,136    | 2,136   | YES    |
| Rare keyword         | 13.1ms      | 14.4ms         | 14.2ms        | 2,059    | 2,059   | YES    |
| Common keyword (95%) | 8.8ms       | 473.0ms        | 695.2ms       | 475,029  | 475,029 | YES    |
| Multi-keyword AND    | 9.2ms       | 29.8ms         | 42.0ms        | 4,098    | 6,234   | NO     |
| Multi-keyword OR     | 13.3ms      | 20.8ms         | 22.5ms        | 4,204    | 4,204   | YES    |
| Phrase search        | 9.2ms       | 10.9ms         | 13.2ms        | 2,068    | 2,068   | YES    |
| Prefix search        | 9.9ms       | 118.4ms        | 151.3ms       | 94,206   | 95,900  | NO     |
| Tag + text (ERROR)   | 6.8ms       | 9.5ms          | 9.8ms         | 1,719    | 1,719   | YES    |
| Tag + text (FATAL)   | 5.7ms       | 2.6ms          | 3.5ms         | 391      | 391     | YES    |
| Substring (onnect)   | 9.4ms       | 0.4ms          | 30.8ms        | 6,234    | 6,234   | YES    |

## Concurrent Read/Write

| Metric                       | Value   |
|------------------------------|---------|
| Search latency (idle)        | 465µs   |
| Search latency (during index)| 459µs   |
| Degradation                  | -1.3%   |
| Index throughput (standalone) | 43.5K/s |
| Index throughput (concurrent) | 43.9K/s |

## Go/No-Go Metrics

| Metric                          | Result    | Target    | Pass? |
|---------------------------------|-----------|-----------|-------|
| Search speedup vs DuckDB LIKE   | 0.1x      | >= 10x    | NO    |
| Ngram semantic correctness       | 8/10      | 100%      | NO    |
| Ngram index size                 | 6.47x     | < 3x      | NO    |
| Indexing throughput              | 24.3K/s   | >= 200K/s | NO    |
| Concurrent search degradation   | -1%       | < 100%    | YES   |

**VERDICT: NO-GO for ngram-based LIKE replacement**

## Key Findings

1. **DuckDB's vectorized Parquet scanning outperforms Tantivy for most query patterns.** DuckDB processes columnar Parquet data with SIMD-optimized string matching, which is hard to beat with a separate inverted index.

2. **Ngram index storage is prohibitive.** At 6.47x Parquet size with only 500K records, this would be untenable at scale. Arc stores terabytes of Parquet data — a 6x overhead for search indices is not acceptable.

3. **Indexing throughput (24K docs/sec) cannot keep up with Arc's ingestion rate (18.6M records/sec).** Even with async background indexing, the 750x gap means indices would fall permanently behind.

4. **Semantic mismatch persists.** Ngram tokenization (min=3, max=5) matches 8/10 query types but fails on multi-keyword AND (ngram creates false-positive substring matches) and prefix search (boundary differences).

5. **Where Tantivy excels: low-selectivity queries.** For rare terms in large datasets (e.g., Tag + text FATAL: 391 of 500K), Tantivy is 1.6x faster. Simple tokenizer substring search is 21x faster for the "onnect" query — but with word tokenization semantics, not LIKE semantics.

6. **Concurrent performance is excellent.** Zero degradation during active indexing — Tantivy's segment-based architecture handles this well.

## Recommendations

1. **Do not integrate Tantivy as a LIKE replacement.** DuckDB is faster for this workload.

2. **Consider Tantivy for a dedicated search API** (not LIKE replacement) if Arc adds a log aggregation feature where keyword/phrase search with relevance ranking is needed. The Simple tokenizer is fast and concurrent-safe.

3. **Invest in DuckDB query optimization instead.** The existing `like_optimizer.go` predicate reordering approach is the right strategy. Additional wins may come from:
   - DuckDB's native `CONTAINS()` function (same semantics, sometimes faster)
   - Partition pruning to reduce scanned files
   - Bloom filters on string columns in Parquet metadata

## Build Prerequisites

```bash
# Rust toolchain required for tantivy-go CGo bindings
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Build the static library
cd /tmp && git clone https://github.com/anyproto/tantivy-go
cd tantivy-go/rust
MACOSX_DEPLOYMENT_TARGET=10.15 cargo build --release --target aarch64-apple-darwin

# Run the benchmark
cd benchmarks/tantivy_bench
CGO_LDFLAGS="-L/path/to/tantivy-go/rust/target/aarch64-apple-darwin/release" go run . --records 500000
```
