#!/bin/bash
# Arc on main (default queued ack mode, then wal.ack_mode = durable): 3 runs per scale each.
W="$(cd "$(dirname "$0")" && pwd)"; L=$W/results/run_arc_main.log
idle() { top -l 2 -s 2 -n 0 | grep "CPU usage" | tail -1 | sed -E 's/.* ([0-9.]+)% idle.*/\1/'; }
ok=0; for i in $(seq 1 120); do v=$(idle); echo "[$(date -u +%T)] idle=$v%" >> $L; if awk "BEGIN{exit !($v>=85)}"; then ok=$((ok+1)); else ok=0; fi; [ $ok -ge 3 ] && break; sleep 20; done
echo "[$(date -u +%T)] starting (consecutive idle samples=$ok)" >> $L
cp $W/run/arc/arc.toml $W/run/arc/arc.toml.bench
trap 'cp $W/run/arc/arc.toml.bench $W/run/arc/arc.toml' EXIT
for mode in queued durable; do
  name=arc; [ $mode = durable ] && name=arc_durable
  if [ $mode = durable ]; then
    sed -E 's/^sync_mode = "fdatasync"(.*)$/sync_mode = "fdatasync"\1\nack_mode = "durable"/' $W/run/arc/arc.toml.bench > $W/run/arc/arc.toml
  else
    cp $W/run/arc/arc.toml.bench $W/run/arc/arc.toml
  fi
  echo "[$(date -u +%T)] mode=$mode ack_mode line: $(grep -c '^ack_mode' $W/run/arc/arc.toml)" >> $L
  for run in 1 2 3; do for s in 1000 10000; do
    exp=$(python3 -c "import json;print(json.load(open('$W/results/scale$s/tdengine/run$run/load.json'))['rows'])" 2>/dev/null || echo 0)
    ARC_BIN=arc-main-20fe85f ARC_RESULT_NAME=$name EXPECTED_ROWS=$exp "$W/run_bench.sh" $s arc $run >> $L 2>&1 || echo "FAILED $name $s run $run" >> $L
  done; done
done
echo ALL_DONE >> $L
