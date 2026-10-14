# Native main-build sizing campaign

This campaign reruns the single-node OSS sizing matrix after the WAL group-commit
fix, using a native build of
[`116a318f936c50a6a6dae7ec8132b6617aae2500`](https://github.com/Basekick-Labs/arc/commit/116a318f936c50a6a6dae7ec8132b6617aae2500).
The experiment starts on October 13, 2026. The previous
[26.09.3 campaign](METHODOLOGY.md) remains available separately.

## Native build and hardware

Arc is built from an archive of the pinned main commit on Ubuntu 24.04 amd64,
using Go 1.26.6, CGO, and the standard `duckdb_arrow` build tag. No Docker or
ArcX engine is involved. Build flags are `-s -w` and
`-X main.Version=main-116a318f936c`. The build verifies Go's download checksum
and Go modules and runs `go test -tags=duckdb_arrow ./internal/wal`.
Every Arc server receives the same binary and verifies its SHA256.

The Arc instances are m7i.xlarge (4 vCPU / 16 GiB), m7i.2xlarge (8 / 32), and
m7i.4xlarge (16 / 64) in us-east-1. All measured servers are on-demand and run
one at a time. The final generator is a dedicated on-demand c7i.16xlarge with
64 vCPUs and 25 Gbps sustained network capacity. The initial c7i.8xlarge
generator recorded outbound bandwidth allowance events during a 4.5M offered
sweep. That sweep was stopped, retained under `rejected-generator/`, and
restarted on the larger generator.

A later line-protocol confirmation also recorded an outbound allowance event
on the 25-Gbps host despite offering less than 1 Gbps on average. That case is
retained separately and repeated. Evidence transfers are limited to 10,000
KiB/sec, and a Linux transmit token bucket paces the generator at 16 Gbps with
a 256 KiB burst. This remains above the planned maximum offered payload rate
(about 9.4 Gbps for 24M MessagePack points/sec) while reducing microbursts.
Qualified earlier cases with zero allowance events remain valid; pacing is
recorded in the experiment parameters and raw generator evidence.

Each Arc instance has a 750 GiB data/root volume and a 250 GiB separate WAL
volume. Both are encrypted gp3, each provisioned for 6,000 IOPS and 500 MiB/s.
The larger capacity prevents space from limiting higher-rate qualification;
the data volume's IOPS and throughput match the previous campaign. Separate
volumes add independently provisioned bandwidth but still share the instance's
aggregate EBS limit. The smaller instance types also have burst bandwidth
above their sustained EBS and network baselines. Ten-minute results are not
proof of continuous throughput above those baselines.

## Workload and qualification

The same three-field schema, 5,000-point uncompressed batches, replayed
throughput data, unique 20M-row historical seed, and SQL mix are used as in
the [previous methodology](METHODOLOGY.md). The current main `arc.toml` is
installed; its 5M-point / 100ms buffer configuration and normal compaction
remain enabled. Backups use a separate path to satisfy main's startup
validation. All effective settings are archived.

Ingestion uses 32 HTTP workers. MessagePack offered rates start at 1M and
extend to 24M points/sec. Fine steps of 500k resolve boundaries between
larger steps. Line protocol uses smaller steps, with 125k refinement. A
candidate must pass three confirmations at the same rate. If it fails,
selection lowers by a fine step and restarts all three confirmations.

Each sustained run offers 90% of the confirmed candidate for ten minutes.
Failures reduce the offered rate by 10% and repeat. This differs from the
previous campaign's coarse halving and 70% soak selection; comparing selected
operating points does not isolate the software fix's causal contribution.
Neither campaign determines precise hardware maxima.

Qualification requires achieved rate at least 95% of offered rate, no HTTP
errors including warmup, exact reconciliation of all acknowledged rows,
drain under 15 seconds, and no increases in WAL drops/write or sync failures, flush
failures, deferred flushes, or storage errors. Generator ENA allowance
counter increases stop capacity selection rather than being attributed to Arc.
The native generator records CPU, memory, process statistics, network bytes,
and ENA counters throughout the campaign.

Server ENA allowance events remain visible as possible instance constraints.
An achieved operating point with server-side shaping is not an isolated CPU
scaling measurement; neither extra volume bandwidth nor a faster generator
removes the server's own network and EBS limits.

Paced workers stop sleeping at the measurement deadline, even when their next
individual scheduled request falls after it. A regression check verifies this
with four workers offering one query/sec for a one-second run. The earlier
initial sweeps are retained under `rejected-timing/`; published cases use the
corrected client. Other client checks verify protocol equivalence and the
historical time span.

Main query-only and mixed cases retain the previous 20M seed, 10k/100k-host
profiles, equal query mix, three 30-second query repetitions, and two-second
per-type p95 selection criterion. All three repetitions must qualify.
Both protocols offer 100k writes/sec in the ten-minute main combined scenario;
the higher query offer is 25% of query-only capacity. Lower-demand MessagePack
evaluations start at 10%, halving if the target cannot be sustained. Separate
100k-host combined sensitivity cases last three minutes.

## Separate WAL disk experiment

The extra experiment measures ingestion with WAL enabled on the second EBS
volume, at `/srv/arc/wal-disk/working/wal`, instead of sharing the data volume.
The functional pilot verifies that both protocols persist their acknowledged
rows and write WAL files on the dedicated mount. Device identity is checked
against the newly provisioned volume's NVMe serial before formatting.

MessagePack and line protocol receive the same selection, confirmation, and
ten-minute qualification as the shared-disk cases. A further MessagePack run
uses the shared-disk WAL-enabled offered rate when the selected rates differ,
supporting a resource comparison at equal offered load. Ingestion-only results
do not establish separate-WAL-disk query or combined capacity.

## Evidence and interpretation

The result bundle records native binary/build metadata, hardware, source and
client checksums, stage events, per-minute latency/rate windows, row checks,
and native host and generator samples. Disk summaries retain individual
NVMe devices as well as aggregate throughput; block-device serials and mount
records identify the dedicated WAL volume. RSS includes native DuckDB
and Arrow allocations. Approximately five-second samples can miss brief
peaks. Resource coverage is explicitly marked rather than filled in.

Storage estimates use unique historical seeds, not replayed ingestion.
Evaluator starting targets reserve 50% below validated WAL-enabled shared-disk
ingestion and query-only rates. They remain ballpark targets requiring
validation against representative schema, query, retention, and longer runs.

The [results directory](results/oss-main-116a318f-20261013) contains compact
summaries, metadata, checksums, and `raw-results.tar.gz`. Unpack the bundle and
run `python3 analyze.py --root results/oss-main-116a318f-20261013 --quiet` to
regenerate case analysis. Credentials and infrastructure launch secrets are
excluded. The campaign archives evidence before terminating its AWS resources.
