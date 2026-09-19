#!/bin/sh
# Bounded original-source review, inside an offline capped container.
set -eu
if [ "$#" -ne 8 ]; then
    echo "usage: $0 BINARY STORAGE_ROOT FACT_MANIFEST PROFILE PROFILE_ID TOPOLOGY_MANIFEST REFERENCE_MANIFEST NEW_JOB_DIR" >&2
    exit 2
fi
job=$8
test ! -e "$job"
mkdir "$job"
trap 'code=$?; trap - EXIT; printf "exit_code=%s\n" "$code" > "$job/exit-status.txt"; exit "$code"' EXIT
cp "$1" "$job/legal-tender"
cp "$0" "$job/run-review.sh"
cp "$3" "$job/fact-manifest.json"
cp "$4" "$job/shared-reference-profile.json"
cp "$6" "$job/topology-manifest.json"
cp "$7" "$job/reference-manifest.json"
printf '%s\n' "$@" > "$job/arguments.txt"
date -u +%FT%TZ > "$job/started.txt"
"$job/legal-tender" pipeline fec review-shared-receipt-references \
    --storage-root "$2" --schedule-a-facts "$3" \
    --profile "$4" --expected-profile-id "$5" \
    --topology-manifest "$6" --reference-manifest "$7" \
    > "$job/result.json" 2> "$job/progress.log"
date -u +%FT%TZ > "$job/finished.txt"
test -s "$job/result.json"
(
    cd "$job"
    sha256sum legal-tender run-review.sh fact-manifest.json shared-reference-profile.json \
        topology-manifest.json reference-manifest.json arguments.txt result.json > SHA256SUMS
)
