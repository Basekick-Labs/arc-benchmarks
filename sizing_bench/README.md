# Native Arc OSS hardware sizing campaign

This harness measures starting hardware sizes for Arc evaluators. It uses the
pre-generated HTTP workload approach from `sustained_bench`, with a single,
identical schema for MessagePack and line protocol. Arc runs natively from the
released DEB package through systemd.

See [the methodology report](METHODOLOGY.md) for the workload, environment,
qualification rules, and interpretation of the published sizing figures.

## Plan

1. Validate the released native package, protocol equivalence, exact seed row
   counts, the full historical time window, and each SQL query in a short pilot.
2. Run on AWS Spot in us-east-1 with 4/16, 8/32, and 16/64 vCPU/GiB server sizes.
   Use a separate native load generator. After repeated Spot reclamations, the
   campaign uses an on-demand generator so all three Arc Spot servers can run
   concurrently under the account's 32-vCPU Spot quota. Reclaimed 8- and
   16-vCPU servers are replaced with the same instance types on-demand. The
   4-vCPU Spot server completes its primary matrix and WAL-on evaluation before
   reclamation; an on-demand replacement finishes its WAL-off evaluation.
3. Compare WAL enabled (`fdatasync`, the release's default batching) and WAL
   disabled. Both wire protocols use 5,000-row uncompressed batches containing
   `cpu`, `host`, `time`, `value`, `cpu_idle`, and `cpu_user`.
   Values are independently generated in 0–100 and rounded to four decimal
   places; Parquet uses the released default Zstd compression.
4. Increase offered ingestion rates with eight workers, requiring zero WAL
   drops and flush failures. Attempt three confirmations, halving the candidate
   on failure, then run each protocol for ten minutes at 70% of the resulting
   candidate rate. Halve again if the soak fails. Check persisted
   SQL row counts, flush failures, buffer queues, memory, CPU, and storage.
5. Seed exactly 20 million unique rows over 24 hours. Measure a defined mix of
   minute aggregates, fleet aggregates, and single-host aggregates, including
   p50/p95/p99 latency. Use the normal SQL/path caches and warm operating-system caches. Every
   request executes a DuckDB query; these caches do not store query responses.
6. Restore the same seed snapshot before each ten-minute mixed workload.
   Request the same ingestion rate for both protocols: 100,000 points/sec,
   capped at 50% of the slower protocol's validated sustained rate. Request 25%
   of query-only QPS.
   Record achieved rates and latency rather than assuming the targets were met.
7. Repeat query and MessagePack mixed tests at 100,000 hosts, using a shorter
   three-minute combined test as a sensitivity check.
8. Run a separate ten-minute MessagePack evaluation scenario for both WAL
   settings at the same write rate and 10% of the corresponding query-only
   QPS. If that QPS is not achieved, halve query demand and repeat. This leaves
   more CPU headroom as the retained dataset grows; preserve the earlier
   25% offered-query scenario as an observed load boundary.
9. Analyze the validated runs, derive conservative starting ranges, prepare
   a docs sizing guide, preserve raw results locally, and remove all temporary
   AWS resources. Enterprise will use a subsequent campaign.

## Interpretation

- A point/row is one timestamp, one host, and three numeric fields. Field values
  per second are three times point throughput. This differs from an InfluxDB
  table expressed in individual field writes.
- Main cardinality is 10,000 host series in one measurement; the sensitivity
  case uses 100,000. Under measurement + host + field terminology, those become
  30,000 and 300,000 field series. Always state the definition with the table.
- Sustained throughput replays 256 pre-generated batches (1.28 million rows).
  Historical seeding creates every batch uniquely. Replayed timestamps and
  values reduce generator overhead and are a disclosed workload limitation.
- WAL enabled is the release's existing buffered WAL with periodic fdatasync.
  It is not a claim that each HTTP acknowledgement waits for a disk sync.
- A run is usable only when acknowledged rows reconcile with persisted rows,
  flush/WAL failures remain zero, resource use is stable, and requested rates
  are actually achieved. A high HTTP acceptance rate alone is insufficient.
- Query results apply to the stated query mix, data volume, cache state, and
  growing mixed workload. They are not a universal maximum QPS.
- Mixed tests define an evaluation workload at a common offered write rate;
  they do not determine the maximum simultaneous write and query capacity.
- The 100,000-host run measures cardinality sensitivity; its shorter duration
  must not be presented as equivalent evidence to the ten-minute main runs.
- EBS is 200 GiB gp3 per server, provisioned for 6,000 IOPS and 500 MiB/s.
  Instance EBS limits can be lower than the volume's provisioned throughput.
- The capacity checkpoint's historical `confirmed_rate` key is the candidate
  remaining after the confirmation loop. If a repeat fails, the lower candidate
  may not have three confirmations. Use the reconciled ten-minute `soak` result
  as the validated operating point. The 4-vCPU WAL-off MessagePack evaluation
  adds three confirmations and a ten-minute run at a fixed 4M offered rate to
  revisit a coarse halving fallback, without treating that rate as a maximum.
  The primary `wal-off-capacities.json` remains its original aggregate snapshot;
  final summaries use the latest individual `*-validated-capacity.json` files.
  The pre-refinement MessagePack checkpoint is retained as
  `wal-off-msgpack-initial-capacity.json`.
- Query-only measurements are three 30-second repeats after five-second
  warmups. Published tables require all three repeats to pass the two-second
  per-query-type p95 criterion. The controller's internal candidate calculation
  can use a subset, so derive published figures from the individual run files.
- Ten-minute tests do not qualify multi-day retention or compaction behavior.
  Instance EBS/network burst bandwidth can exceed continuous baselines. The
  smaller server's 1.562-Gbps network baseline and 156.25-MB/s EBS baseline must
  be considered when extrapolating synthetic throughput to long-term rates.

## Build and pilot

Use Go 1.26 and Python 3.9 or later. On a disposable Ubuntu 24.04 server,
install the released `arc_26.09.3_amd64.deb` and verify its published SHA256.
Install `sysstat` for the monitor, stop the package's `arc` service, create
`/srv/arc/results`, and copy `server.py`, `monitor.py`, and a private bootstrap
token to `/srv/arc`. Run `sudo python3 /srv/arc/server.py configure true` to
configure and start the native service, then run `monitor.py` as a background
process. Preserve `/etc/arc/arc.toml`: its released buffer settings are part of
this experiment.

On the separate generator, put the same token in `~/token`, a key authorized on
the disposable server in `~/ssh_key`, and the client/controller files in
`~/harness`. Restrict token and private-key permissions to their owner. Allow
SSH and port 8000 between these hosts; the controller expects the server's
`ubuntu` account to have passwordless sudo. Use separate result names for each
server and run only one controller or evaluation per server.

```sh
cd sizing_bench
go test -race .
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o sizing_bench .
python3 controller.py --host PRIVATE_SERVER_IP --name pilot \
  --seed-records 100000 --soak-seconds 2 --sensitivity-seconds 2 \
  --sweep-seconds 2 --query-seconds 2 --repeats 1
```

`controller.py` runs on the dedicated load generator. It expects `~/token`,
`~/ssh_key`, and `~/harness/sizing_bench`; the disposable server has native Arc,
`/srv/arc/server.py`, `/srv/arc/monitor.py`, and `/srv/arc/token`. The `server.py`
reset/restore operations delete only the dedicated `/srv/arc/working` directory.
Never point them at an existing production host.

After the primary campaign completes on a server, run the evaluation scenario
on the same generator with `python3 evaluation.py --host PRIVATE_SERVER_IP
--name SERVER_NAME`. It reads the primary campaign's capacity
reports, reseeds the unique 20-million-point dataset, and saves separate results.

## Artifacts

Each case produces JSON results, process logs, query definitions, a package
checksum, server specifications, and structured stage events. Host monitoring
records native Arc RSS, systemd memory and CPU accounting, Arc metrics, vmstat,
iostat, and disk capacity at approximately five-second intervals. The local
orchestrator also preserves driver monitoring and periodically downloads results.

The published guide presents measured operating points separately from derived
starting targets, attaches the Arc version and run date, provides storage estimation
from the unique historical seed data
only (replayed ingestion is unsuitable for compression estimates), and explain the need to retain CPU/RAM/IO headroom.

The October 12, 2026 native OSS experiment is preserved in
[`results/oss-26.09.3-20261012`](results/oss-26.09.3-20261012). JSON case files
include overload failures as well as qualified runs; do not aggregate them into
capacity figures. Earlier unlimited-load experiments are under
`rejected-overload/`. Host traces and logs use gzip to keep the archive small.
Credentials and infrastructure launch secrets are excluded.
`rejected-orchestration/` preserves evaluation cases invalidated by overlapping
supervisors during a backup transition. The affected 4-vCPU evaluation is
restarted from a fresh seed with a per-server process lock; these rejected cases
are excluded from derived capacity and documentation tables.
`rejected-spot/` preserves the interrupted 4-vCPU reference run; an interrupted
result is excluded even when its instantaneous rate looks healthy.

The full case and monitoring tree is bundled in `raw-results.tar.gz`; compact
analysis, sizing summaries, hardware/package metadata, and checksums remain
directly readable. Unpack the evidence and regenerate case analysis with Python
3.9 or later:

```sh
tar -xzf results/oss-26.09.3-20261012/raw-results.tar.gz \
  -C results/oss-26.09.3-20261012
python3 analyze.py --root results/oss-26.09.3-20261012 --quiet
```

The analyzer matches native host samples by UTC time, reconciles rows and error
counters, and marks incomplete or overloaded runs. It does not turn offered
rates into recommendations. Missing native samples remain missing; in
particular, a reclaimed 8-vCPU server's WAL-off 5.6M soak has no complete host
trace. A later WAL-off run on its replacement uses the same 2.8M offered rate
as WAL-on to support the resource comparison.
