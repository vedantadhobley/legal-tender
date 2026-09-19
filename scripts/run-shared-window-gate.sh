#!/usr/bin/env bash
# Capped one-shot container: source storage read-only; only /audit is writable.
set -euo pipefail
if (( $# != 4 )); then
  echo "usage: $0 TEST_BINARY SOURCE_ARCHIVE INPUT_SPEC NEW_JOB_DIR" >&2
  exit 2
fi
job=$4
test ! -e "$job"
test -n "${ARANGO_PASSWORD:?configured password required}"
export LT_WINDOW_ENDPOINT=${LT_WINDOW_ENDPOINT:?project endpoint required}
mkdir "$job"
finish() {
  result=$?
  trap - EXIT
  if test -r /sys/fs/cgroup/memory.peak; then cp /sys/fs/cgroup/memory.peak "$job/memory-peak.txt"; fi
  printf 'exit_code=%s\n' "$result" > "$job/exit-status.txt"
  exit "$result"
}
trap finish EXIT
cp "$1" "$job/window.test"
cp "$2" "$job/source.tar.gz"
cp "$3" "$job/inputs.json"
cp "$0" "$job/run-gate.sh"
cd "$job"
sha256sum window.test source.tar.gz inputs.json run-gate.sh > SETUP_SHA256SUMS
export LT_SHARED_WINDOW_INPUTS="$job/inputs.json"
LT_SHARED_WINDOW_INPUTS_SHA256=$(sha256sum inputs.json | cut -d ' ' -f 1)
export LT_SHARED_WINDOW_INPUTS_SHA256
export LT_WINDOW_STORAGE_ROOT=/storage
TIMEFORMAT='real_seconds=%R user_seconds=%U system_seconds=%S'
date -u +%FT%TZ > started.txt
{ time LT_WINDOW_OUTPUT="$job/result.json" ./window.test -test.run '^TestSharedWindowLiveGate$' -test.v > run.log 2>&1; } 2> timing.txt
printf '0\n' > gate.exit
date -u +%FT%TZ > verified.txt
LT_WINDOW_EXPECTED_GATE=$(sed -n 's/^  "gate_id": "\([a-f0-9]\{64\}\)",$/\1/p' result.json)
test "${#LT_WINDOW_EXPECTED_GATE}" = 64
export LT_WINDOW_EXPECTED_GATE
{ time LT_WINDOW_OUTPUT="$job/replay.json" ./window.test -test.run '^TestSharedWindowLiveGate$' -test.v > replay.log 2>&1; } 2> replay-timing.txt
cmp result.json replay.json
printf '0\n' > replay.exit
date -u +%FT%TZ > finished.txt
sha256sum -c SETUP_SHA256SUMS > setup-check.txt
sha256sum result.json replay.json run.log replay.log timing.txt replay-timing.txt gate.exit replay.exit > FINAL_SHA256SUMS
sha256sum -c FINAL_SHA256SUMS > checksum-check.txt
