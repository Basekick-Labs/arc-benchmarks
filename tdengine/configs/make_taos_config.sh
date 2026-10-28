#!/bin/bash
# Write taos.cfg and taosadapter.toml for a TDengine instance whose data, logs
# and temp files live under RUN_DIR (so storage is measured in one place).
# taos.cfg ships empty on macOS, so every setting is TDengine's default except
# paths and timezone UTC (TSBS data is UTC; interval(1d) windows follow the
# server timezone). taosadapter.toml is the installed one with debug = false.
#   ./make_taos_config.sh /abs/path/run/taos
set -euo pipefail
T=$1; mkdir -p "$T/cfg" "$T/data" "$T/log" "$T/tmp"
cat > "$T/cfg/taos.cfg" <<CFG
firstEp   localhost:6030
fqdn      localhost
serverPort 6030
dataDir   $T/data
logDir    $T/log
tempDir   $T/tmp
timezone  UTC
CFG
sed -e 's#^debug = true#debug = false#' -e "s#^taosConfigDir = \"\"#taosConfigDir = \"$T/cfg\"#" \
    -e "s#^\[log\]#[log]\npath = \"$T/log\"#" /etc/taos/taosadapter.toml > "$T/cfg/taosadapter.toml"
