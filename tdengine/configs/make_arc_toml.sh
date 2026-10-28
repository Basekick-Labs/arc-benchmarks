#!/bin/bash
# Build the benchmark arc.toml from the Arc repo's shipped arc.toml (the file
# the Docker image and the .deb/.rpm packages install). Two changes only:
# WAL on with fdatasync (the production recommendation), telemetry off.
#   ./make_arc_toml.sh /path/to/arc/arc.toml > ../run/arc/arc.toml
set -euo pipefail
python3 - "$1" <<'PY'
import sys
s = open(sys.argv[1]).read()
s = s.replace('[telemetry]\nenabled = true', '[telemetry]\nenabled = false', 1)
s = s.replace('[wal]\nenabled = false\n# sync_mode = "fdatasync"', '[wal]\nenabled = true\nsync_mode = "fdatasync"', 1)
assert '[wal]\nenabled = true\nsync_mode = "fdatasync"' in s and '[telemetry]\nenabled = false' in s
sys.stdout.write(s)
PY
