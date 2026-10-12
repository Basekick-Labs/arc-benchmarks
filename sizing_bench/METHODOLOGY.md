# Native Arc 26.09.3 sizing measurements

This report documents the October 12, 2026 single-node OSS campaign behind the
hardware sizing guide. See [the harness](README.md) for execution instructions
and [the results](results/oss-26.09.3-20261012) for case analysis and raw evidence.

## Environment and scope

Arc runs natively from the released DEB on Ubuntu 24.04 in AWS us-east-1.
Server sizes are m7i.xlarge (4 vCPU / 16 GiB), m7i.2xlarge (8 / 32), and
m7i.4xlarge (16 / 64). Each vCPU is a logical thread; these types have two
threads per physical core. Each server has a 200 GiB gp3 volume provisioned
for 6,000 IOPS and 500 MiB/s. Instance EBS baseline bandwidths are approximately
156, 313, and 625 MB/s. Smaller instances can burst above their baselines.

The campaign starts on Spot. Reclaimed servers are replaced with the same
instance types on-demand, preserving completed cases and rerunning interrupted
work. A separate on-demand generator replaces interrupted Spot generators.
Purchase history and burst-credit state are not controlled variables.
Interrupted and overlapping-controller cases are excluded from sizing figures
and retained separately in the evidence archive.

Ingestion measurements run for ten minutes. Query-only measurements use
20 million unique rows. Main combined cases grow from 20 million to 81.5 million
rows. These tests do not qualify days of continuous ingestion, billions of
retained rows, steady-state compaction, or restart recovery.

## Configuration and data

The released package uses a 5,000,000-point ingestion buffer and a 100 ms
maximum buffer age. Parquet ingestion and normal scheduled compaction remain
enabled. Parquet uses the released default Zstd compression.

One point is one timestamp, one host tag, and three numeric fields: `value`,
`cpu_idle`, and `cpu_user`. MessagePack and line protocol encode the same `cpu`
measurement, schema, and values in uncompressed 5,000-point HTTP batches.
Values are independently generated in 0–100 and rounded to four decimal places.
Multiply point throughput by three to obtain numeric field values per second.

The main workload contains 10,000 hosts; sensitivity cases contain 100,000.
Measurement plus host defines a host series. Including each field separately
gives 30,000 and 300,000 field series respectively.

Sustained ingestion replays 256 pre-generated batches (1.28 million points).
Arc appends repeated timestamps and values. Historical seeding generates
20 million unique points spread over the full 24-hour window. Replayed data
reduces generator overhead but is unsuitable for retention compression estimates.

WAL-enabled cases use the release's buffered WAL with periodic `fdatasync`.
They do not measure a per-HTTP-acknowledgement disk-sync barrier.
WAL-disabled cases permit loss of buffered writes after an abrupt failure.

## Rate selection and validation

The controller increases offered ingestion rates with eight workers, attempts
three confirmations, halves candidates on failure, and runs ten-minute soaks
at 70% of the remaining candidate. It halves again if a soak fails. The
historical `confirmed_rate` key can refer to a candidate without three passing
confirmations; publication uses reconciled soak results.

The 4-vCPU WAL-off MessagePack refinement adds three confirmations and a
ten-minute run at a fixed 4M offered points/sec after the initial coarse
selection. The original checkpoint is preserved separately. Selected rates are
validated operating points, not precise hardware maxima. Equal rates on
different sizes can reflect coarse selection rather than identical capacity.

Qualification requires zero HTTP errors, persisted SQL rows equal to acknowledged
rows including warmup, achieved throughput at least 95% of offered load, and
no increase in flush failures, WAL drops/write failures, or storage errors.
The analyzer also records drain time, buffer behavior, and resource coverage.
Derived ingestion-only and query-only starting targets reserve 50% headroom
and round down. They are not separately qualified continuous production rates.

## Queries and combined workloads

Queries use JSON requests and responses through the SQL endpoint. HTTP latency
includes reading the complete response. Normal warm SQL/file metadata and OS
caches apply; every request executes against stored data.

The equal query mix contains:

| Query | Range | Work |
| --- | --- | --- |
| Minute aggregates | 1 hour | Average two fields and maximize a third, grouped by minute |
| Fleet aggregates | 24 hours | Group by host, aggregate values and counts, return the top 100 hosts |
| Host aggregates | 24 hours | Counts, averages, minima, and maxima for one host |

Query-only candidates run for 30 seconds after five-second warmups, repeated
three times. Published figures select the highest median QPS for which all
three repeats have no errors and each query type's p95 is at most two seconds.
Reported p95 is the worst across types and repeats. WAL cases are seeded
separately and can have different file layouts; read-only QPS differences do
not isolate WAL overhead.

Each combined case restores its WAL/cardinality seed snapshot. Both ingestion
protocols offer 100,000 points/sec, capped at half the slower protocol's
validated rate, plus 25% of query-only QPS for ten minutes. Those higher-query
cases maintain writes but fail to achieve their query targets. They remain in
the evidence as observed load boundaries, not qualified combined guidance.

Separate MessagePack evaluation cases offer the same write rate and 10% of
query-only QPS for ten minutes, halving query demand if qualification fails.
All six final evaluation cases achieve their requested rates and reconcile
81.5M ending rows. Their query p95 is reported rather than imposing a separate
latency service level. Later minutes can slow as retained data grows.

The 100,000-host combined sensitivity cases run for three minutes and end at
approximately 39.5M rows. Query offers differ by hardware/cardinality. They are
not equivalent to the ten-minute cases or isolated cardinality comparisons.
Compare the first three minutes of the main run for comparable retained volume.

Each seed triggers compaction and waits ten seconds before snapshotting.
This wait does not prove steady-state compaction. Scheduled compaction can
overlap individual cases, and separately seeded layouts can affect query cost.

## Resources and storage

The WAL resource comparison uses the same 2.8M offered MessagePack points/sec
for ten minutes on each size. A separate 16-vCPU line-protocol comparison uses
1.4M offered points/sec. Native process RSS includes DuckDB and Arrow memory;
host CPU busy excludes I/O wait. Approximately five-second sampling can miss
brief peaks. Disk intervals do not establish sustained storage bandwidth.

Missing monitoring remains missing. The reclaimed 8-vCPU server's original
WAL-off 5.6M soak lacks a complete host trace. Later matched-rate replacement
runs provide complete resource windows. Use `complete_window` when interpreting
case resource summaries, rather than inferring resource values from other runs.

Storage estimates use `du -sb` on the persisted local data directory after
unique historical seeding and before manual compaction. This includes local
data metadata, not just Parquet payloads. The three-field schema measures about
26.2 bytes/point at 10,000 hosts and 27.6 at 100,000. Compression depends on
schema, values, cardinality, ordering, and compaction.

At a rounded 28 bytes/point, 10,000 points/sec for 30 days needs approximately
676 GiB of stored data; 100,000 needs about 6.6 TiB. Add WAL, compaction output,
spill, operational metadata, and free disk headroom. The test's 200 GiB volume
does not represent a retention recommendation.
