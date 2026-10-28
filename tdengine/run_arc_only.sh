#!/bin/bash
# Re-run Arc with 10,000-row payloads (matches TDengine's 10,000-row batches).
W="$(cd "$(dirname "$0")" && pwd)"
L=$W/results/run_arc_only.log
idle() { top -l 2 -s 2 -n 0 | grep "CPU usage" | tail -1 | sed -E 's/.* ([0-9.]+)% idle.*/\1/'; }
ok=0; for i in $(seq 1 120); do v=$(idle); echo "[$(date -u +%T)] idle=$v%" >> $L; if awk "BEGIN{exit !($v>=85)}"; then ok=$((ok+1)); else ok=0; fi; [ $ok -ge 3 ] && break; sleep 20; done
echo "[$(date -u +%T)] starting (consecutive idle samples=$ok)" >> $L
for run in 1 2 3; do
  for s in 1000 10000; do
    exp=$(python3 -c "import json;print(json.load(open('$W/results/scale$s/tdengine/run$run/load.json'))['rows'])" 2>/dev/null || echo 0)
    EXPECTED_ROWS=$exp "$W/run_bench.sh" $s arc $run >> $L 2>&1 || echo "FAILED arc $s run $run" >> $L
  done
done
echo ALL_DONE >> $L
