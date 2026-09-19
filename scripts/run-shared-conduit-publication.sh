#!/bin/sh
# Offline publication. Inputs read-only; only the new job directory is writable.
set -eu
if [ "$#" -ne 8 ]; then
    echo "usage: $0 BINARY PARTICIPANTS PARTICIPANT_ID TOPOLOGY TOPOLOGY_ID BASELINE BASELINE_ID NEW_JOB_DIR" >&2
    exit 2
fi
job=$8
test ! -e "$job"
mkdir "$job"
trap 'code=$?; trap - EXIT; printf "exit_code=%s\n" "$code" > "$job/exit-status.txt"; exit "$code"' EXIT
cp "$1" "$job/legal-tender"
cp "$0" "$job/run-publication.sh"
cp "$2" "$job/participant-manifest.json"
cp "$4" "$job/topology-manifest.json"
cp "$6" "$job/baseline-manifest.json"
printf '%s\n' "$@" > "$job/arguments.txt"
date -u +%FT%TZ > "$job/started.txt"
"$job/legal-tender" pipeline fec publish-shared-receipt-conduit-associations \
    --participant-manifest "$2" --expected-participant-id "$3" \
    --topology-manifest "$4" --expected-topology-id "$5" \
    --baseline-manifest "$6" --expected-baseline-id "$7" \
    --output-dir "$job/calculation" --workers "${LT_GROUP_WORKERS:-8}" \
    --run-rows "${LT_GROUP_RUN_ROWS:-100000}" --merge-fan-in "${LT_GROUP_FAN_IN:-8}" \
    --max-workspace-bytes 8589934592 > "$job/result.json" 2> "$job/progress.log"
date -u +%FT%TZ > "$job/finished.txt"
test -s "$job/calculation/manifest.json"
(
    cd "$job"
    sha256sum legal-tender run-publication.sh participant-manifest.json \
        topology-manifest.json baseline-manifest.json arguments.txt result.json \
        calculation/manifest.json calculation/data/*.zst > SHA256SUMS
    sha256sum -c SHA256SUMS > checksum-check.txt
)
