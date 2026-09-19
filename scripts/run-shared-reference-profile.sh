#!/bin/sh
# Offline, one-shot analysis. Run inside a capped container; inputs are read-only.
set -eu
if [ "$#" -ne 6 ]; then
    echo "usage: $0 BINARY PARTICIPANT_MANIFEST PARTICIPANT_ID TOPOLOGY_MANIFEST TOPOLOGY_ID NEW_JOB_DIR" >&2
    exit 2
fi
job=$6
test ! -e "$job"
mkdir "$job"
trap 'code=$?; trap - EXIT; printf "exit_code=%s\n" "$code" > "$job/exit-status.txt"; exit "$code"' EXIT
cp "$1" "$job/legal-tender"
cp "$2" "$job/participant-manifest.json"
cp "$4" "$job/topology-manifest.json"
"$job/legal-tender" pipeline fec profile-shared-receipt-references \
    --participant-manifest "$2" --expected-participant-id "$3" \
    --topology-manifest "$4" --expected-topology-id "$5" \
    --output-dir "$job/calculation" \
    --workers "${LT_PROFILE_WORKERS:-8}" --run-rows 100000 --merge-fan-in 8 \
    --max-workspace-bytes 8589934592 > "$job/result.json" 2> "$job/progress.log"
test -s "$job/calculation/shared-reference-profile.json"
(
    cd "$job"
    sha256sum legal-tender participant-manifest.json topology-manifest.json \
        result.json calculation/manifest.json calculation/shared-reference-profile.json > SHA256SUMS
)
