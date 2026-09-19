#!/bin/sh
# New extension only. Base publications read-only; password supplied privately.
set -eu
if [ "$#" -ne 10 ]; then
    echo "usage: $0 BINARY SOURCE_ARCHIVE GENERATION GENERATION_SHA GRAPH PARTICIPANTS BASELINE SHARED SHARED_ID NEW_JOB_DIR" >&2
    exit 2
fi
job=${10}
test ! -e "$job"
test -n "${ARANGO_PASSWORD:?configured password required}"
test -n "${LT_SHARED_ENDPOINT:?project endpoint required}"
mkdir "$job"
trap 'code=$?; trap - EXIT; printf "exit_code=%s\n" "$code" > "$job/exit-status.txt"; exit "$code"' EXIT
cp "$1" "$job/legal-tender"
cp "$2" "$job/source.tar.gz"
cp "$0" "$job/run-generation.sh"
cp "$3" "$job/base-generation.json"
cp "$5" "$job/base-graph-manifest.json"
cp "$6" "$job/participant-manifest.json"
cp "$7" "$job/baseline-manifest.json"
cp "$8" "$job/shared-manifest.json"
printf '%s\n' "$@" > "$job/arguments.txt"
(
    cd "$job"
    sha256sum legal-tender source.tar.gz run-generation.sh base-generation.json \
        base-graph-manifest.json participant-manifest.json baseline-manifest.json \
        shared-manifest.json arguments.txt > SETUP_SHA256SUMS
)
set -- --generation "$3" --expected-generation-sha256 "$4" \
    --graph-manifest "$5" --participants "$6" --conduits "$7" \
    --shared-conduits "$8" --expected-shared-conduit-id "$9" \
    --storage-root /storage --endpoint "$LT_SHARED_ENDPOINT" \
    --username root --password-env ARANGO_PASSWORD --publication-dir /publication \
    --lock-dir /locks --arango-data-dir /arango-data \
    --reserve-free-bytes 274877906944 --max-filesystem-growth-bytes 17179869184 \
    --max-encoded-bytes 4294967296
date -u +%FT%TZ > "$job/started.txt"
"$job/legal-tender" pipeline fec publish-shared-conduit-generation "$@" \
    --workers 8 --batch-size 2000 > "$job/result.json" 2> "$job/progress.log"
printf '0\n' > "$job/publication.exit"
date -u +%FT%TZ > "$job/published.txt"
"$job/legal-tender" pipeline fec publish-shared-conduit-generation "$@" \
    --workers 4 --batch-size 1000 > "$job/replay.json" 2> "$job/replay.log"
cmp "$job/result.json" "$job/replay.json"
printf '0\n' > "$job/replay.exit"
date -u +%FT%TZ > "$job/finished.txt"
(
    cd "$job"
    sha256sum -c SETUP_SHA256SUMS > setup-check.txt
    sha256sum result.json replay.json progress.log replay.log publication.exit replay.exit > FINAL_SHA256SUMS
    sha256sum -c FINAL_SHA256SUMS > checksum-check.txt
)
