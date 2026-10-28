# Arc vs TDengine: TSBS IoT

The harness behind the Arc vs TDengine comparison on basekick.net. Both
databases load the same TSBS IoT dataset and answer the same nine query types;
every query instance is checked to return the same number of rows on both.

## What is measured

- **Workload:** [TSBS](https://github.com/timescale/tsbs) IoT use case (trucks
  with `readings` and `diagnostics`), seed 123, 24 h at a 10 s interval.
- **Scales:** 1,000 and 10,000 trucks. TDengine prices its All-in-One edition
  per tag, "a unique time-series signal, typically Asset/Device + Measurement
  Name" (tdengine.com/pricing, read 2026-10-27). A TSBS truck has 10 signals,
  so these are TDengine's 10,000-tag and 100,000-tag price points.
- **Ingest:** wall-clock time for each engine's TSBS loader with 12 workers,
  from start until the loader exits (acknowledged), and for Arc also until
  every row is queryable (Arc flushes its buffer to Parquet asynchronously).
- **Storage:** after `FLUSH` on TDengine (TDengine OSS refuses `COMPACT
  DATABASE`: "Operation not supported in community edition"), after a manual
  compaction cycle on Arc. TDengine is measured on its `vnode/*/tsdb`
  directories, excluding the WAL it retains for 3,600 s by default.
- **Queries:** nine TSBS IoT query types, 20 instances each, run one at a time
  over HTTP from the same Python client (Arc `/api/v1/query`, TDengine REST via
  taosAdapter). The first instance of each type after a restart is reported
  separately; the rest are warm. TDengine's own native TSBS runner
  (`tsbs_run_queries_tdengine`) is also run, and the published TDengine figure
  for each type is the faster of the two clients.
- **Repetitions:** three full runs per engine and scale, one engine at a time
  with the other stopped, alternating engines; medians are reported.

## Components and versions

| | |
|---|---|
| Arc | built from `Basekick-Labs/arc` at the commit stated with the results, `go build -tags=duckdb_arrow` |
| TDengine | TSDB-OSS 3.4.3.3, `tdengine-tsdb-oss-3.4.3.3-macos-arm64.pkg` (sha256 `91665a851698e7fcfb0f5a1bdb6029cb724f177f992b4d9620f783c0462a2494`) |
| TSBS for Arc | `Basekick-Labs/tsbs` branch `arc-vs-tdengine-2026-10` (commit `c62e1dc`) |
| TSBS for TDengine | `taosdata/tsbs` at `8ca9097` plus `patches/taosdata-tsbs-8ca9097.patch` |
| Client tools | `arcli`, `taos` CLI, Python 3 standard library |

The two TSBS forks generate identical IoT data for the same seed: their IoT
generators differ only in Go module paths. Both carry one identical change for
this comparison, `zeroTagChance = 0`: stock TSBS nulls one tag on about 1% of
points, which Arc stores per row but TDengine (one subtable per truck, tags per
subtable) cannot represent, so the engines would answer different questions.
The TDengine fork patch also adds `tsbs_dump_tdengine_queries`, which decodes
TDengine's generated query file so Arc runs the same query instances.

## Engine configuration

- **Arc:** the repo's shipped `arc.toml` (installed by the Docker image and the
  packages), with WAL on (`sync_mode = "fdatasync"`, a full fsync on macOS) and
  telemetry off. See `configs/make_arc_toml.sh`. Four Arc variants are
  reported, each with its own result directory:

  | Variant | `ingest.sort_keys` | `wal.ack_mode` | Script |
  |---|---|---|---|
  | `arc` | none (sorted by time) | `queued` (default) | `run_arc_main.sh` |
  | `arc_durable` | none | `durable` | `run_arc_main.sh` |
  | `arc_sorted` | `readings:fleet,name`, `diagnostics:fleet,name` | `queued` | `run_arc_sorted.sh` |
  | `arc_sorted_durable` | same | `durable` | `run_arc_sorted_durable.sh` |

  Sort keys sort each measurement by fleet and truck before time (Arc appends
  `time`), and compaction keeps that order.
- **TDengine:** default `taos.cfg` except data/log/temp paths and
  `timezone UTC`; taosAdapter's installed config with `debug = false`. See
  `configs/make_taos_config.sh`. Database parameters are the ones TDengine's own
  TSBS comparison script (`scripts/tsdbComp`) passes for the IoT case:
  `vgroups 12, buffer 256, pages 4096, stt_trigger 1, wal_level 2,
  wal_fsync_period 3000, keep 36500d`, 12 workers, batch size 10,000,
  `hash-workers`. `cachemodel` stays at its default (`none`); TDengine's
  `last_row` queries would be faster with a last-row cache.

## Queries

`queries/translate.py` turns each generated TDengine query into Arc SQL with
the same parameters (fleet, time range). TDengine-only constructs map to
standard SQL: `last_row(...) partition by k` to `DISTINCT ON (k) ... ORDER BY
k, time DESC` (for `last-loc`, a `MAX(time)` per key joined back, which was
faster on Arc with identical rows; see `queries/variants.py`), `partition by
... interval(10m)` to `GROUP BY ..., time_bucket(INTERVAL '10 minutes', time)`,
and `tbname` to `name`. `stationary-trucks` follows TDengine's definition (the
first aligned 10-minute window per truck), not stock TSBS.

## Running it

Layout: everything lives under one directory `$W` with `bin/`, `data/`,
`run/`, `queries/` and `results/`; the scripts expect it.

```bash
# Tools (TDengine installed first: the TDengine loaders link libtaos)
export CGO_CFLAGS=-I/usr/local/include CGO_LDFLAGS=-L/usr/local/lib
(cd tsbs-arc   && go build -o $W/bin/tsbs_generate_data_bk ./cmd/tsbs_generate_data \
                && go build -o $W/bin/tsbs_load_arc ./cmd/tsbs_load_arc)
(cd tsbs-taos  && go build -o $W/bin/tsbs_generate_data_td ./cmd/tsbs_generate_data \
                && go build -o $W/bin/tsbs_generate_queries_td ./cmd/tsbs_generate_queries \
                && go build -o $W/bin/tsbs_load_tdenginestmt2_td ./cmd/tsbs_load_tdenginestmt2 \
                && go build -o $W/bin/tsbs_run_queries_tdengine_td ./cmd/tsbs_run_queries_tdengine \
                && go build -o $W/bin/tsbs_dump_tdengine_queries ./cmd/tsbs_dump_tdengine_queries)
(cd arc && go build -tags=duckdb_arrow -o $W/bin/arc-main ./cmd/arc)

# Data (both formats, both scales)
for s in 1000 10000; do
  $W/bin/tsbs_generate_data_bk --use-case=iot --seed=123 --scale=$s \
    --timestamp-start=2016-01-01T00:00:00Z --timestamp-end=2016-01-02T00:00:00Z \
    --log-interval=10s --format=arc > $W/data/arc_iot_scale$s.dat
  $W/bin/tsbs_generate_data_td --use-case=iot --seed=123 --scale=$s \
    --timestamp-start=2016-01-01T00:00:00Z --timestamp-end=2016-01-02T00:00:00Z \
    --log-interval=10s --format=TDengineStmt2 > $W/data/td_iot_scale$s.dat
done

# Queries
for s in 1000 10000; do $W/queries/gen_queries.sh $s; done

# Configs and secrets
configs/make_arc_toml.sh arc/arc.toml > $W/run/arc/arc.toml
configs/make_taos_config.sh $W/run/taos
printf 'ARC_ADMIN_TOKEN=%s\n' "$(openssl rand -hex 24)" > $W/run/.env

# Everything: waits for an idle machine, then 3 runs per engine and scale
./run_all.sh
```

`run_bench.sh <scale> <engine> <run>` runs a single engine and scale; set
`ARC_BIN` to pick the Arc binary in `bin/` and `ARC_RESULT_NAME` to choose the
result directory. `run_arc_main.sh`, `run_arc_sorted.sh` and
`run_arc_sorted_durable.sh` produce the Arc variants above (each restores
`arc.toml` on exit). `python3 results/aggregate.py` rebuilds
`results/final.json` and `results/summary.txt` from the run directories.

## Machine

Apple M3 Max (14 cores), 36 GB, macOS, both databases running natively (no
containers), with background services quiet: Spotlight excluded from the
benchmark directory, no VMs running. Each result directory records
`sysinfo.txt` for its run. A laptop is not a server: treat absolute numbers as
relative, and note that macOS syncs with `F_FULLFSYNC`.

Results are added under `results/` with the Arc commit they were measured on.
