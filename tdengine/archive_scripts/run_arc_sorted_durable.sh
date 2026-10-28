#!/bin/bash
# Arc main with ingest.sort_keys = fleet,name AND wal.ack_mode = durable: 3 runs per scale.
W="$(cd "$(dirname "$0")" && pwd)"; L=$W/results/run_arc_sorted_durable.log
idle() { top -l 2 -s 2 -n 0 | grep "CPU usage" | tail -1 | sed -E 's/.* ([0-9.]+)% idle.*/\1/'; }
ok=0; for i in $(seq 1 120); do v=$(idle); if awk "BEGIN{exit !($v>=85)}"; then ok=$((ok+1)); else ok=0; fi; [ $ok -ge 3 ] && break; sleep 20; done
echo "[$(date -u +%T)] starting (consecutive idle samples=$ok)" >> $L
cp $W/run/arc/arc.toml $W/run/arc/arc.toml.bench
trap 'cp $W/run/arc/arc.toml.bench $W/run/arc/arc.toml' EXIT
sed -E -e 's/^sort_keys = \[\]/sort_keys = ["readings:fleet,name", "diagnostics:fleet,name"]/' \
       -e 's/^sync_mode = "fdatasync"(.*)$/sync_mode = "fdatasync"\1\nack_mode = "durable"/' $W/run/arc/arc.toml.bench > $W/run/arc/arc.toml
echo "[$(date -u +%T)] $(grep '^sort_keys' $W/run/arc/arc.toml) | $(grep '^ack_mode' $W/run/arc/arc.toml)" >> $L
for run in 1 2 3; do for s in 1000 10000; do
  exp=$(python3 -c "import json;print(json.load(open('$W/results/scale$s/tdengine/run$run/load.json'))['rows'])")
  ARC_BIN=arc-main-20fe85f ARC_RESULT_NAME=arc_sorted_durable EXPECTED_ROWS=$exp "$W/run_bench.sh" $s arc $run >> $L 2>&1 || echo "FAILED $s run $run" >> $L
done; done
echo ALL_DONE >> $L
