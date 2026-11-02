# Parquet vs CSV benchmark

Backs the blog post [Parquet vs CSV for Analytics: Size and Speed Benchmarked](https://basekick.net/blog/parquet-vs-csv) (2026-11-02).

Generates 10 million rows of synthetic IoT telemetry and writes it as CSV,
gzipped CSV, Parquet (Snappy), Parquet (ZSTD), and Parquet (ZSTD with delta
and dictionary encoding, size only). It then times the same Polars lazy
queries against each file: full read, single-column mean, group-by, and a
one-hour time window.

    python3 -m venv venv && ./venv/bin/pip install polars pyarrow numpy
    ./venv/bin/python bench.py --rows 10000000 --out ./data

`results.json` is the run quoted in the post: Apple M3 Max, 36 GB RAM,
Python 3.14.7, Polars 2.1.0, PyArrow 26.0.0. CSV reads declare `firmware`
and `zip_code` as strings so every format returns the same, correct data.
Files were in the page cache, and each query is the median of 5 runs.
The data files (about 1.4 GB) are not committed; rerun to regenerate them.
