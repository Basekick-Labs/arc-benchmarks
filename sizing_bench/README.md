# Native Arc OSS hardware sizing

This harness measures ballpark hardware sizes for Arc evaluators. Arc and the
load generator run natively. MessagePack and line protocol use identical
three-field points and 5,000-point batches.

Experiments:

- [Main-build methodology](MAIN_METHODOLOGY.md) and
  [results](results/oss-main-116a318f-20261013): pinned main commit with the
  WAL group-commit fix, sequential on-demand servers, and a separate WAL-disk
  experiment. Current scripts implement this campaign.
- [Arc 26.09.3 methodology](METHODOLOGY.md) and
  [archived evidence](results/oss-26.09.3-20261012): October 12 baseline.
  The original harness is pinned at commit
  `d8aaf8ccd69008467505e3745004a95106558b1a`.

## Native build

Use Go 1.26.6, a C/C++ toolchain, and Python 3.9 or later on Ubuntu 24.04 amd64.
Build Arc from the recorded source commit using the standard DuckDB engine:

```sh
git clone https://github.com/Basekick-Labs/arc.git arc-source
cd arc-source
git checkout 116a318f936c50a6a6dae7ec8132b6617aae2500
go mod download
go mod verify
go test -tags=duckdb_arrow ./internal/wal
CGO_ENABLED=1 go build -tags=duckdb_arrow \
  -ldflags='-s -w -X main.Version=main-116a318f936c' \
  -o arc-main ./cmd/arc
sha256sum arc-main
```

Copy the same binary to every disposable server and compare its checksum.
Install the source `arc.toml` at `/etc/arc/arc.toml`; the 5M-point / 100ms
buffer settings are part of this workload. Run the binary at `/usr/bin/arc`
through a systemd `arc` service as the `arc` user, with `LimitNOFILE=65535`.
Create `/srv/arc/results` and `/srv/arc/working`.
Record `sha256sum /usr/bin/arc` in `/srv/arc/results/binary.sha256`.

Put `server.py`, `monitor.py`, `netmon.py`, a private bootstrap token, and
`source.json` under `/srv/arc`. The source file records the commit and version.
Install `sysstat` and `ethtool`, configure with
`sudo python3 /srv/arc/server.py configure true`, and run both monitors as
background services. Mount the extra filesystem at `/srv/arc/wal-disk` for
the separate-WAL experiment. Its working subtree is owned by the campaign.

**Use disposable servers.** Reset/restore/layout operations remove
`/srv/arc/working` and `/srv/arc/wal-disk/working`. Never point these scripts
at a production installation or an existing data directory.

## Generator and execution

Use a separate generator with ample sustained bandwidth. Put the same token
in `~/token`, an authorized private SSH key in `~/ssh_key`, and these sources
in `~/harness`. Restrict token/key permissions to their owner. The controller
expects the server's `ubuntu` account to have passwordless sudo, and private
SSH/8000 connectivity between the machines.

Build and verify the generator:

```sh
cd ~/harness
go test -race .
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o sizing_bench .
python3 pilot.py --host PRIVATE_SERVER_IP
```

Start `driver_monitor.py` and `netmon.py` on the generator before testing.
Use a unique result name for each server, run one server at a time, and keep
only one controller or evaluation active per server:

```sh
python3 controller.py --host PRIVATE_SERVER_IP --name small
python3 split_wal.py --host PRIVATE_SERVER_IP --name small
python3 evaluation.py --host PRIVATE_SERVER_IP --name small
```

The controller runs ingestion-only selection and ten-minute soaks, unique
historical seeding, repeated query-only profiles, and mixed workloads for
WAL enabled and disabled. The separate-WAL script qualifies both ingestion
protocols and runs an equal-offered-rate resource comparison. Evaluation
qualifies lower query demand at 100k MessagePack points/sec. Preserve native
host traces alongside generator results before terminating the servers.

## Interpretation and artifacts

- A point is one timestamp, one host tag, and three numeric fields. Numeric
  field values/sec are three times points/sec.
- Throughput cases replay batches; historical seeds contain unique points
  over 24 hours. Use the unique seeds for retention compression estimates.
- Selected rates are validated operating points, not precise maxima. Starting
  targets reserve 50% headroom; validate representative longer workloads.
- WAL enabled uses buffered, periodic `fdatasync`, not a per-request disk-sync
  acknowledgement barrier. WAL disabled permits loss of buffered writes.
- Query capacity depends on the stated mix, retained volume, cache state,
  cardinality, and concurrency. Separate-disk measurements cover ingestion.
- Generator allowance events stop qualification. Server allowance counters
  remain recorded as possible instance limits; volume bandwidth does not
  bypass the instance's aggregate EBS limit.

Compact metadata, analysis, sizing summaries, and checksums are directly
readable in each experiment's results directory. Full case JSON, native host
and generator traces, and rejected cases are bundled in `raw-results.tar.gz`.
Unpack and regenerate analysis with Python 3.9 or later:

```sh
tar -xzf results/oss-main-116a318f-20261013/raw-results.tar.gz \
  -C results/oss-main-116a318f-20261013
python3 analyze.py --root results/oss-main-116a318f-20261013 --quiet
```

The analyzer matches UTC windows and reports row/error checks, per-minute
rates and latencies, native RSS/CPU/I/O, generator resources, and network
allowances. Missing coverage stays missing. Raw rejected cases are evidence
of unsuccessful load or orchestration, never inputs to capacity tables.
Credentials and infrastructure launch secrets are excluded from publication.
