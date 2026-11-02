"""Parquet vs CSV for analytics: size and query-time benchmark.

Generates a synthetic IoT telemetry table, writes it as CSV, gzipped CSV,
Parquet (Snappy), Parquet (Zstandard), and a delta-encoded Zstandard Parquet
file (size only), then times the same Polars queries
against each file. Usage:

    python bench.py --rows 10000000 --out ./data
"""

import argparse
import gzip
import json
import os
import platform
import shutil
import statistics
import time
from datetime import datetime, timedelta, timezone

import numpy as np
import polars as pl
import pyarrow
import pyarrow.parquet as pq

RUNS = 5
# declare the columns that inference gets wrong, so CSV reads return correct data
CSV_TYPES = {"firmware": pl.String, "zip_code": pl.String}


def generate(rows: int) -> pl.DataFrame:
    rng = np.random.default_rng(42)
    start = datetime(2026, 10, 1, tzinfo=timezone.utc)
    # one reading per 100 ms, in time order, like a real ingest stream
    ts = np.arange(rows, dtype="int64") * 100_000  # microseconds
    devices = np.array([f"dev-{i:05d}" for i in range(5_000)])
    sites = np.array([f"site-{i:02d}" for i in range(20)])
    device_idx = rng.integers(0, len(devices), rows)
    return pl.DataFrame(
        {
            "time": pl.Series(ts + int(start.timestamp() * 1_000_000)).cast(pl.Datetime("us")),
            "device_id": devices[device_idx],
            "site": sites[device_idx % len(sites)],
            "firmware": rng.choice(["2.4.1", "2.4.2", "2.5.0"], rows),
            # zero-padded numeric code: a classic CSV type-inference trap
            "zip_code": np.char.zfill(rng.integers(0, 99_999, rows).astype(str), 5),
            "temperature": np.round(20 + 5 * rng.standard_normal(rows), 2),
            "humidity": np.round(rng.uniform(30, 70, rows), 1),
            "pressure": np.round(1013 + 3 * rng.standard_normal(rows), 1),
            "voltage": np.round(rng.uniform(3.2, 3.4, rows), 3),
            "error_code": rng.choice([0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3], rows).astype("int32"),
        }
    )


def timed(fn):
    samples = []
    for _ in range(RUNS):
        t0 = time.perf_counter()
        fn()
        samples.append(time.perf_counter() - t0)
    return statistics.median(samples)


def scan(path: str) -> pl.LazyFrame:
    if path.endswith(".parquet"):
        return pl.scan_parquet(path)
    # scan_csv handles .gz too; gzip has no index, so the whole stream is
    # decompressed before parsing starts
    return pl.scan_csv(path, try_parse_dates=True, schema_overrides=CSV_TYPES)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=10_000_000)
    ap.add_argument("--out", default="./data")
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)

    df = generate(args.rows)
    files = {
        "csv": os.path.join(args.out, "telemetry.csv"),
        "csv_gzip": os.path.join(args.out, "telemetry.csv.gz"),
        "parquet_snappy": os.path.join(args.out, "telemetry_snappy.parquet"),
        "parquet_zstd": os.path.join(args.out, "telemetry_zstd.parquet"),
    }

    write_s = {}
    t0 = time.perf_counter(); df.write_csv(files["csv"]); write_s["csv"] = time.perf_counter() - t0
    t0 = time.perf_counter()
    with open(files["csv"], "rb") as src, gzip.open(files["csv_gzip"], "wb", compresslevel=6) as dst:
        shutil.copyfileobj(src, dst)
    write_s["csv_gzip"] = write_s["csv"] + time.perf_counter() - t0
    for name, codec in (("parquet_snappy", "snappy"), ("parquet_zstd", "zstd")):
        t0 = time.perf_counter()
        df.write_parquet(files[name], compression=codec, row_group_size=1_000_000, statistics=True)
        write_s[name] = time.perf_counter() - t0

    # same table, PyArrow writer with delta encoding on the sorted timestamp
    tuned = os.path.join(args.out, "telemetry_zstd_delta.parquet")
    pq.write_table(
        df.to_arrow(), tuned, compression="zstd", row_group_size=1_000_000,
        use_dictionary=[c for c in df.columns if c != "time"],
        column_encoding={"time": "DELTA_BINARY_PACKED"},
    )

    t_mid = datetime(2026, 10, 1) + timedelta(microseconds=int(args.rows * 100_000 * 0.5))
    queries = {
        "full_read": lambda lf: lf.collect(),
        "avg_one_column": lambda lf: lf.select(pl.col("temperature").mean()).collect(),
        "group_by_site": lambda lf: lf.group_by("site").agg(
            pl.col("temperature").mean(), pl.col("pressure").max(), pl.len()
        ).collect(),
        "one_hour_window": lambda lf: lf.filter(
            (pl.col("time") >= t_mid) & (pl.col("time") < t_mid + timedelta(hours=1))
        ).select(pl.col("temperature").mean()).collect(),
    }

    results = {"rows": args.rows, "env": {
        "machine": platform.machine(), "python": platform.python_version(),
        "polars": pl.__version__, "pyarrow": pyarrow.__version__,
    }, "formats": {}}

    for name, path in files.items():
        entry = {"bytes": os.path.getsize(path), "write_s": round(write_s[name], 3), "queries_s": {}}
        for qname, q in queries.items():
            entry["queries_s"][qname] = round(timed(lambda: q(scan(path))), 4)
        results["formats"][name] = entry
        print(name, json.dumps(entry))

    # the type-inference trap: what does CSV hand back for zip_code?
    inferred = pl.read_csv(files["csv"], n_rows=1000, try_parse_dates=True).schema
    results["csv_inferred_schema"] = {k: str(v) for k, v in inferred.items()}
    results["parquet_schema"] = {k: str(v) for k, v in pl.read_parquet_schema(files["parquet_zstd"]).items()}
    meta = pq.ParquetFile(files["parquet_zstd"]).metadata
    results["parquet_row_groups"] = meta.num_row_groups
    rg0 = meta.row_group(0).column(0).statistics
    results["parquet_rg0_time_stats"] = [str(rg0.min), str(rg0.max)]

    results["parquet_zstd_delta_bytes"] = os.path.getsize(tuned)
    results["column_bytes_zstd"] = {}
    m = pq.ParquetFile(files["parquet_zstd"]).metadata
    for r in range(m.num_row_groups):
        for c in range(m.num_columns):
            col = m.row_group(r).column(c)
            results["column_bytes_zstd"][col.path_in_schema] = results["column_bytes_zstd"].get(col.path_in_schema, 0) + col.total_compressed_size
    with open(os.path.join(args.out, "results.json"), "w") as f:
        json.dump(results, f, indent=2)
    print(json.dumps({k: v for k, v in results.items() if k != "formats"}, indent=2))


if __name__ == "__main__":
    main()
