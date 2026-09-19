#!/usr/bin/env bash
# Run inside a capped container. Source storage is read-only; only /audit writes.
set -euo pipefail
if (( $# < 4 )); then
  echo "usage: $0 BINARY SOURCE_ARCHIVE NEW_JOB_DIR QUERY_FLAGS..." >&2
  exit 2
fi
binary=$1
archive=$2
job=$3
shift 3
test ! -e "$job"
test -n "${ARANGO_PASSWORD:?configured password required}"
mkdir "$job"
finish() {
  result=$?
  trap - EXIT
  if test -r /sys/fs/cgroup/memory.peak; then cp /sys/fs/cgroup/memory.peak "$job/memory-peak.txt"; fi
  printf 'exit_code=%s\n' "$result" > "$job/exit-status.txt"
  exit "$result"
}
trap finish EXIT
cp "$binary" "$job/legal-tender"
cp "$archive" "$job/source.tar.gz"
cp "$0" "$job/run-gate.sh"
printf '%s\n' "$@" > "$job/arguments.txt"
# Pin every explicit manifest locator, in addition to identities checked by Go.
previous=''
for arg in "$@"; do
  case "$previous" in
    --generation|--base-generation|--graph-manifest|--participants|--conduits|--shared-graph-manifest|--shared-conduits)
      sha256sum "$arg" >> "$job/INPUT_SHA256SUMS"
      ;;
  esac
  previous=$arg
done
(
  cd "$job"
  sha256sum legal-tender source.tar.gz run-gate.sh arguments.txt INPUT_SHA256SUMS > SETUP_SHA256SUMS
)
TIMEFORMAT='real_seconds=%R user_seconds=%U system_seconds=%S'
date -u +%FT%TZ > "$job/started.txt"
{ time "$job/legal-tender" pipeline fec validate-shared-conduit-queries "$@" > "$job/result.json" 2> "$job/progress.log"; } 2> "$job/timing.txt"
printf '0\n' > "$job/gate.exit"
date -u +%FT%TZ > "$job/verified.txt"
expected=$(sed -n 's/^  "gate_id": "\([a-f0-9]\{64\}\)",$/\1/p' "$job/result.json")
test "${#expected}" = 64
{ time "$job/legal-tender" pipeline fec validate-shared-conduit-queries "$@" --expected-gate-id "$expected" > "$job/replay.json" 2> "$job/replay.log"; } 2> "$job/replay-timing.txt"
cmp "$job/result.json" "$job/replay.json"
printf '0\n' > "$job/replay.exit"
date -u +%FT%TZ > "$job/finished.txt"
(
  cd "$job"
  sha256sum -c INPUT_SHA256SUMS > input-check.txt
  sha256sum -c SETUP_SHA256SUMS > setup-check.txt
  sha256sum result.json replay.json progress.log replay.log timing.txt replay-timing.txt gate.exit replay.exit > FINAL_SHA256SUMS
  sha256sum -c FINAL_SHA256SUMS > checksum-check.txt
)
