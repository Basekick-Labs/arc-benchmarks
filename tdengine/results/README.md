# Results

Measured 2026-10-27/28 on an Apple M3 Max (14 cores, 36 GB), macOS, both
engines native.

- **Arc:** `Basekick-Labs/arc` main at `20fe85f6` (includes #1407 and #1408).
  `scaleN/arc`: as shipped, default `wal.ack_mode = "queued"` (5 runs at
  10,000 trucks, 3 at 1,000). `scaleN/arc_durable`: `wal.ack_mode =
  "durable"` (3 runs). `scaleN/arc_sorted` and `scaleN/arc_sorted_durable`:
  the same two modes with `ingest.sort_keys = ["readings:fleet,name",
  "diagnostics:fleet,name"]` (3 runs each).
- **TDengine:** TSDB-OSS 3.4.3.3 (`scaleN/tdengine`, 3 runs; `native/` holds
  the native TSBS runner output, whose `daily-activity` result is excluded
  because that client ran in the local timezone, not UTC).

`final.json` holds the medians the published tables use; `aggregate.py`
rebuilds it and `summary.txt` from the run directories. Query medians are warm
(the first, cold instance of each type is excluded) and pooled across runs.
Query figures are reported for `arc` and `arc_sorted`; the durable variants
share their data layout, and their query timings are in their run directories.
Storage is in decimal MB (`du -k` x 1024 / 1e6). Every run directory
has `load.json`, `storage_kb.txt`, `queries.jsonl` (one line per query
execution), `sysinfo.txt` and the loader output. Every query instance returned
the same number of rows on both engines.
