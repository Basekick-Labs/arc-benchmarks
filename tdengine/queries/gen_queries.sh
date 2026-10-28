#!/bin/bash
# Generate the TSBS IoT query set for one scale: TDengine gob files (native
# runner) and the translated JSON lines both engines run over HTTP.
set -euo pipefail
W="$(cd "$(dirname "$0")/.." && pwd)"
SCALE=$1; N=${2:-20}
TYPES="last-loc low-fuel high-load stationary-trucks long-driving-sessions long-daily-sessions avg-vs-projected-fuel-consumption avg-load daily-activity"
G=$W/queries/scale${SCALE}_gob; mkdir -p "$G"; : > "$W/queries/scale$SCALE.jsonl"
for t in $TYPES; do
  "$W/bin/tsbs_generate_queries_td" --use-case=iot --seed=123 --scale=$SCALE \
     --timestamp-start=2016-01-01T00:00:00Z --timestamp-end=2016-01-02T00:00:01Z \
     --queries=$N --query-type=$t --format=TDengine > "$G/$t.gob" 2>/dev/null
  "$W/bin/tsbs_dump_tdengine_queries" < "$G/$t.gob" | python3 -I "$W/queries/translate.py" iot >> "$W/queries/scale$SCALE.jsonl"
done
jq -r .type "$W/queries/scale$SCALE.jsonl" | sort | uniq -c
