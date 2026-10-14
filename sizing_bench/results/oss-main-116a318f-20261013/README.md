# Native Arc OSS sizing results

Arc was built from [main commit 116a318f](https://github.com/Basekick-Labs/arc/commit/116a318f936c50a6a6dae7ec8132b6617aae2500).
The three on-demand EC2 servers ran sequentially in us-east-1. Each ingestion
point contains one timestamp, one host tag, and three numeric fields.
Rates below are validated ten-minute operating points, with all acknowledged
rows reconciled and no HTTP, WAL write/sync, flush, or storage failures.
They are not precise hardware maxima or guarantees for continuous operation.

## Ingestion and WAL placement

| Instance | Protocol | WAL shared disk | WAL separate disk | Separate vs shared | WAL disabled |
| --- | --- | --- | --- | --- | --- |
| m7i.xlarge | msgpack | 4.02M | 4.44M | +10.5% | 4.92M |
| m7i.xlarge | lineprotocol | 561.4k | 561.3k | 0.0% | 895.7k |
| m7i.2xlarge | msgpack | 8.43M | 8.01M | -5.0% | 9.78M |
| m7i.2xlarge | lineprotocol | 1.01M | 1.01M | 0.0% | 1.57M |
| m7i.4xlarge | msgpack | 8.15M | 10.06M | +23.4% | 18.22M |
| m7i.4xlarge | lineprotocol | 1.90M | 1.90M | 0.0% | 3.00M |

Both data and WAL volumes are gp3 provisioned for 6,000 IOPS and 500 MiB/s
each. Separate volumes still share the instance's aggregate EBS allowance.
Selected operating rates can differ; this table alone does not isolate the
cause of a difference. Smaller instances can burst above their sustained
network and EBS baselines during short tests.

Higher-rate shared-disk MessagePack soaks on the 16-vCPU server reconciled
rows but increased WAL-drop counters and therefore failed qualification.
At offered rates of 11.25M and 10.125M points/sec, disk-write medians were
about 500 MiB/s while CPU-busy medians were approximately 37% and 33%.
These runs show the fixed volume-throughput ceiling; more CPU alone cannot
remove it. Failed cases remain in the raw evidence and case analysis.

At the same 10.125M offered points/sec, the shared-disk run achieved 10.07M
points/sec but dropped 208,332 WAL entries. The separate-disk run achieved
10.06M with zero WAL drops. Both reconciled every acknowledged row. The
separate volume kept the WAL-drop counter at zero at this offered load,
allowing a higher qualified WAL-enabled operating point.

## Equal-offered-rate MessagePack comparison

Each server uses its own validated shared-disk WAL-enabled offered rate for
all three configurations below. CPU busy and iowait are medians; CPU busy
excludes iowait. Disk writes show the p95 of sampled throughput summed across
EBS devices, since bursty data writes can have a zero median. Wait is the
median of each sample's highest device write-await value. Measurements cover
the full ten-minute window.

| Instance | WAL | Offered points/sec | Achieved points/sec | CPU busy % | Iowait % | Disk write p95 MiB/s | Write await ms |
| --- | --- | --- | --- | --- | --- | --- | --- |
| m7i.xlarge | Shared disk | 4.05M | 4.02M | 72.8 | 13.6 | 624.0 | 2.3 |
| m7i.xlarge | Separate disk | 4.05M | 4.02M | 73.6 | 13.2 | 556.8 | 2.4 |
| m7i.xlarge | Disabled | 4.05M | 4.03M | 59.8 | 0.1 | 493.5 | 0.0 |
| m7i.2xlarge | Shared disk | 8.55M | 8.43M | 79.8 | 6.5 | 687.9 | 12.0 |
| m7i.2xlarge | Separate disk | 8.55M | 8.39M | 81.3 | 6.3 | 1010.3 | 2.8 |
| m7i.2xlarge | Disabled | 8.55M | 8.49M | 66.1 | 0.1 | 603.9 | 0.0 |
| m7i.4xlarge | Shared disk | 8.20M | 8.15M | 32.1 | 6.9 | 942.7 | 2.7 |
| m7i.4xlarge | Separate disk | 8.20M | 8.15M | 32.4 | 6.3 | 909.8 | 2.7 |
| m7i.4xlarge | Disabled | 8.20M | 8.15M | 26.1 | 0.5 | 641.6 | 0.0 |

The [placement summary](wal-placement-summary.json) retains full per-disk
statistics, resource coverage, and server network allowance counters.
Server shaping events are instance constraints; these are not isolated CPU
scaling measurements. Separate-WAL measurements cover ingestion only.

## Queries, combined workloads, and evidence

The [sizing summary](sizing-summary.json) includes query-only profiles,
qualified combinations at 100k MessagePack points/sec, and unique historical
storage estimates. The [methodology](../../MAIN_METHODOLOGY.md) describes
selection, workload limits, native build, and generator checks.

The [case analysis](analysis.json), hardware metadata, checksums, and
`raw-results.tar.gz` preserve the underlying measurements. Every published
ingestion capacity has zero generator network allowance increases.
Unpack the bundle in this directory and run the published analyzer to
reproduce the case analysis. Infrastructure was archived and terminated
after completion.
