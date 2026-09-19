#!/usr/bin/env bash
# Inside a capped container: /storage read-only; only /audit writable.
# Setup pins window.test, inputs.json, this script and the source snapshot.
set -euo pipefail
cd /audit
test ! -e run.exit
test ! -e result.json
finish() {
  result=$?
  if test -r /sys/fs/cgroup/memory.peak; then cp /sys/fs/cgroup/memory.peak memory-peak.txt; fi
  printf '%s\n' "$result" > run.exit
}
trap finish EXIT
sha256sum -c SETUP_SHA256SUMS
test -n "${ARANGO_PASSWORD:?configured password required}"
export LT_WINDOW_INPUTS=/audit/inputs.json
LT_WINDOW_INPUTS_SHA256=$(sha256sum inputs.json | cut -d ' ' -f 1)
export LT_WINDOW_INPUTS_SHA256
export LT_WINDOW_STORAGE_ROOT=/storage
export LT_WINDOW_ENDPOINT=${LT_WINDOW_ENDPOINT:?configured Arango endpoint required}
TIMEFORMAT='real_seconds=%R user_seconds=%U system_seconds=%S'
{ time LT_WINDOW_OUTPUT=/audit/result.json ./window.test -test.run '^TestWindowConnectionLiveGate$' -test.v > run.log 2>&1; } 2> timing.txt
printf '0\n' > gate.exit
LT_WINDOW_EXPECTED_GATE=$(sed -n 's/^  "gate_id": "\([a-f0-9]\{64\}\)",$/\1/p' result.json)
test "${#LT_WINDOW_EXPECTED_GATE}" = 64
export LT_WINDOW_EXPECTED_GATE
{ time LT_WINDOW_OUTPUT=/audit/replay.json ./window.test -test.run '^TestWindowConnectionLiveGate$' -test.v > replay.log 2>&1; } 2> replay-timing.txt
printf '0\n' > replay.exit
cmp result.json replay.json
sha256sum result.json replay.json run.log replay.log timing.txt replay-timing.txt gate.exit replay.exit SETUP_SHA256SUMS > FINAL_SHA256SUMS
sha256sum -c FINAL_SHA256SUMS
