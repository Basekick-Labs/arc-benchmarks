#!/bin/bash
# Arc vs TDengine OSS: TSBS IoT, native on macOS, one engine at a time.
#
#   ./run_bench.sh <scale> <engine> [run]   scale: 1000 | 10000   engine: arc | tdengine   run: 1..N
#
# Per run: wipe data dir, start the engine, timed load, row count, compaction,
# storage footprint, restart (cold), queries. Everything lands in results/.
set -euo pipefail
W="$(cd "$(dirname "$0")" && pwd)"
SCALE=$1; ENGINE=$2; RUN=${3:-1}
DB=iot
WORKERS=12
RNAME=$ENGINE; [ "$ENGINE" = arc ] && RNAME=${ARC_RESULT_NAME:-arc}
R=$W/results/scale$SCALE/$RNAME/run$RUN; mkdir -p "$R"
source "$W/run/.env"                       # ARC_ADMIN_TOKEN
TAOS_CFG=$W/run/taos/cfg
export HOME_ARCLI=$W/run/arcli-home
arc_q() { HOME=$HOME_ARCLI arcli query --endpoint http://localhost:8000 --token "$ARC_ADMIN_TOKEN" -o csv --no-header "$1"; }
taos_q() { taos -c "$TAOS_CFG" -s "$1" 2>&1; }
log() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$R/run.log"; }

sysinfo() {
  { sw_vers; sysctl -n machdep.cpu.brand_string hw.ncpu hw.memsize; uptime; top -l 1 -n 0 | grep -E "CPU usage|PhysMem|Load Avg"; } > "$R/sysinfo.txt"
}

stop_all() {
  pkill -f "$W/bin/arc-" 2>/dev/null || true
  pkill -x taosadapter 2>/dev/null || true
  pkill -x taosd 2>/dev/null || true
  for i in $(seq 1 60); do pgrep -f "$W/bin/arc-|taosd|taosadapter" >/dev/null || return 0; sleep 1; done
  echo "engines did not stop" >&2; exit 1
}

start_arc() {
  (cd "$W/run/arc" && ARC_AUTH_BOOTSTRAP_TOKEN=$ARC_ADMIN_TOKEN nohup "$W/bin/${ARC_BIN:-arc-main}" >> arc.log 2>&1 &)
  for i in $(seq 1 60); do curl -sf localhost:8000/health >/dev/null && return 0; sleep 1; done
  echo "arc did not start" >&2; exit 1
}

start_taos() {
  (cd "$W/run/taos" && nohup taosd -c "$TAOS_CFG" >> taosd.out 2>&1 &)
  for i in $(seq 1 60); do taos_q "select server_version();" | grep -q community && break; sleep 1; done
  (cd "$W/run/taos" && nohup taosadapter -c "$TAOS_CFG/taosadapter.toml" >> taosadapter.out 2>&1 &)
  for i in $(seq 1 60); do curl -sf -u root:taosdata -d "select 1" localhost:6041/rest/sql >/dev/null && return 0; sleep 1; done
  echo "taos did not start" >&2; exit 1
}

wait_rows() {  # wait until the row count stops changing (Arc flushes buffers asynchronously)
  local prev=-1 cur
  local expected=${1:-0}
  for i in $(seq 1 600); do
    cur=$(arc_q "SELECT (SELECT COUNT(*) FROM $DB.readings) + (SELECT COUNT(*) FROM $DB.diagnostics)" 2>/dev/null || echo -1)
    [ "$expected" -gt 0 ] && [ "$cur" = "$expected" ] && { echo "$cur"; return; }
    [ "$expected" -eq 0 ] && [ "$cur" = "$prev" ] && [ "$cur" != "-1" ] && { echo "$cur"; return; }
    prev=$cur; sleep 1
  done
  echo "$cur"
}

sysinfo
stop_all
QF=$W/queries/scale$SCALE.jsonl

if [ "$ENGINE" = arc ]; then
  rm -rf "$W/run/arc/data"
  start_arc
  log "arc load start"
  t0=$(python3 -c 'import time;print(time.time())')
  "$W/bin/tsbs_load_arc" --file "$W/data/arc_iot_scale$SCALE.dat" --api-key "$ARC_ADMIN_TOKEN" \
     --database $DB --workers $WORKERS --batch-size 1 > "$R/load.out" 2>&1
  t1=$(python3 -c 'import time;print(time.time())')
  rows=$(wait_rows "${EXPECTED_ROWS:-0}")
  t2=$(python3 -c 'import time;print(time.time())')
  log "arc load done: client $(python3 -c "print(round($t1-$t0,3))")s, until queryable $(python3 -c "print(round($t2-$t0,3))")s, rows=$rows"
  echo "{\"engine\":\"arc\",\"scale\":$SCALE,\"load_client_s\":$(python3 -c "print($t1-$t0)"),\"load_until_queryable_s\":$(python3 -c "print($t2-$t0)"),\"rows\":$rows}" > "$R/load.json"
  du -sk "$W/run/arc/data/arc/$DB" | awk '{print $1}' > "$R/storage_before_compaction_kb.txt"
  HOME=$HOME_ARCLI arcli compaction trigger --endpoint http://localhost:8000 --token "$ARC_ADMIN_TOKEN" --database $DB --wait > "$R/compaction.out" 2>&1 || true
  HOME=$HOME_ARCLI arcli compaction trigger --endpoint http://localhost:8000 --token "$ARC_ADMIN_TOKEN" --database $DB --tier daily --wait >> "$R/compaction.out" 2>&1 || true
  du -sk "$W/run/arc/data/arc/$DB" | awk '{print $1}' > "$R/storage_kb.txt"
  du -sk "$W/run/arc/data/wal" 2>/dev/null | awk '{print $1}' > "$R/wal_kb.txt" || true
  find "$W/run/arc/data/arc/$DB" -name '*.parquet' | wc -l | tr -d ' ' > "$R/files.txt"
  log "arc storage $(cat "$R/storage_kb.txt") KB (before compaction $(cat "$R/storage_before_compaction_kb.txt") KB), files $(cat "$R/files.txt")"
  stop_all; start_arc
  log "arc queries (restarted, first instance per type is cold)"
  python3 -I "$W/queries/run_queries.py" --engine arc --url http://localhost:8000 --token "$ARC_ADMIN_TOKEN" \
     --db $DB --queries "$QF" --out "$R/queries.jsonl" | tee "$R/queries_summary.txt"
  stop_all
else
  rm -rf "$W/run/taos/data" "$W/run/taos/log" "$W/run/taos/tmp"; mkdir -p "$W/run/taos/data" "$W/run/taos/log" "$W/run/taos/tmp"
  start_taos
  attempt=1
  while :; do
    log "tdengine load start (attempt $attempt)"
    t0=$(python3 -c 'import time;print(time.time())')
    set +e
    "$W/bin/tsbs_load_tdenginestmt2_td" --file "$W/data/td_iot_scale$SCALE.dat" --db-name $DB --host localhost \
       --workers $WORKERS --batch-size 10000 --vgroups 12 --buffer 256 --pages 4096 --hash-workers=true \
       --stt_trigger 1 --wal_level 2 --wal_fsync_period 3000 --db_parameters 'keep 36500d' > "$R/load.out" 2>&1
    rc=$?; set -e
    t1=$(python3 -c 'import time;print(time.time())')
    grep -q "loaded .* rows" "$R/load.out" && break
    log "tdengine loader crashed (rc=$rc): $(grep -m1 -E 'SIG|panic' "$R/load.out" || true)"
    cp "$R/load.out" "$R/load_crash_$attempt.out"
    [ $attempt -ge 3 ] && { log "giving up"; exit 1; }
    attempt=$((attempt+1)); taos_q "drop database if exists $DB;" > /dev/null
  done
  rows=$(taos_q "select (select count(*) from $DB.readings) + (select count(*) from $DB.diagnostics);" | awk -F'|' '/^ +[0-9]+ +\|/{gsub(/ /,"",$1);print $1}' | head -1)
  t2=$(python3 -c 'import time;print(time.time())')
  log "tdengine load done: client $(python3 -c "print(round($t1-$t0,3))")s, rows=$rows, attempts=$attempt"
  echo "{\"engine\":\"tdengine\",\"scale\":$SCALE,\"load_client_s\":$(python3 -c "print($t1-$t0)"),\"load_until_queryable_s\":$(python3 -c "print($t1-$t0)"),\"rows\":$rows,\"attempts\":$attempt}" > "$R/load.json"
  du -sk "$W/run/taos/data/vnode" | awk '{print $1}' > "$R/storage_before_compaction_kb.txt"
  taos_q "flush database $DB;" > "$R/compaction.out"
  taos_q "compact database $DB;" >> "$R/compaction.out"
  for i in $(seq 1 360); do taos_q "show compacts;" | grep -q "Query OK, 0 row" && break; sleep 5; done
  taos_q "show compacts;" >> "$R/compaction.out"
  # TSDB data files only (excludes the WAL), and the whole vnode tree including WAL
  find "$W/run/taos/data/vnode" -type d -name tsdb -exec du -sk {} + | awk '{s+=$1} END {print s}' > "$R/storage_kb.txt"
  find "$W/run/taos/data/vnode" -type d -name wal -exec du -sk {} + | awk '{s+=$1} END {print s}' > "$R/wal_kb.txt"
  du -sk "$W/run/taos/data/vnode" | awk '{print $1}' > "$R/storage_vnode_total_kb.txt"
  log "tdengine storage tsdb $(cat "$R/storage_kb.txt") KB, wal $(cat "$R/wal_kb.txt") KB, vnode total $(cat "$R/storage_vnode_total_kb.txt") KB"
  stop_all; start_taos
  log "tdengine queries over REST (restarted, first instance per type is cold)"
  python3 -I "$W/queries/run_queries.py" --engine tdengine --url http://localhost:6041 --db $DB \
     --queries "$QF" --out "$R/queries.jsonl" | tee "$R/queries_summary.txt"
  log "tdengine queries, native TSBS runner (warm)"
  mkdir -p "$R/native"
  for g in "$W/queries/scale${SCALE}_gob"/*.gob; do
    t=$(basename "$g" .gob)
    "$W/bin/tsbs_run_queries_tdengine_td" --file "$g" --db-name $DB --host localhost --workers 1 \
       --print-interval 0 --results-file "$R/native/$t.json" > "$R/native/$t.out" 2>&1 || true
  done
  stop_all
fi
log "done"
