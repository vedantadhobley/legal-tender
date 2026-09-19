#!/usr/bin/env bash
# Compare a candidate stage record without replacing missing historical bytes.
set -euo pipefail
if [[ "${1:-}" == --worker ]]; then
  test "$#" = 1
  test ! -e /output/result.json
  test ! -e /output/replay.json
  test "$(sha256sum /app | cut -d ' ' -f 1)" = "$LT_REVIEW_BINARY_SHA"
  date -u +%FT%TZ > /output/started.txt
  TIMEFORMAT='real_seconds=%R user_seconds=%U system_seconds=%S'
  for phase in result replay; do
    code=0
    { time /app pipeline fec review-release-stage-evidence \
        --storage-root /storage \
        --release "$LT_REVIEW_RELEASE" --expected-release-sha256 "$LT_REVIEW_RELEASE_SHA" \
        --candidate-stage "$LT_REVIEW_STAGE" --expected-candidate-stage-sha256 "$LT_REVIEW_STAGE_SHA" \
        > "/output/$phase.json" 2> "/output/$phase.log"; } \
        2> "/output/$phase.timing" || code=$?
    printf '%s\n' "$code" > "/output/$phase.exit"
    test "$code" = 0
    test -s "/output/$phase.json"
  done
  cmp /output/result.json /output/replay.json
  printf '0\n' > /output/replay-check.exit
  cp /sys/fs/cgroup/memory.peak /output/memory-peak
  date -u +%FT%TZ > /output/finished.txt
  cd /output
  sha256sum result.json replay.json result.exit replay.exit result.log replay.log \
    result.timing replay.timing replay-check.exit memory-peak started.txt finished.txt > SHA256SUMS
  sha256sum -c SHA256SUMS > checksum-check.txt
  exit 0
fi
if test "$#" != 7; then
  echo 'usage: run-stage-evidence-review.sh STORAGE_ROOT ACCEPTED_BINARY RELEASE_PATH RELEASE_SHA CANDIDATE_STAGE_PATH CANDIDATE_STAGE_SHA NEW_OUTPUT_DIRECTORY' >&2
  exit 2
fi
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
source "$repo_dir/scripts/go-build.env"
LT_REVIEW_STORAGE=$(realpath -e -- "$1")
LT_REVIEW_BINARY=$(realpath -e -- "$2")
LT_REVIEW_BINARY_SHA=$(sha256sum "$LT_REVIEW_BINARY" | cut -d ' ' -f 1)
export LT_REVIEW_STORAGE LT_REVIEW_BINARY LT_REVIEW_BINARY_SHA
export LT_REVIEW_RELEASE=$3 LT_REVIEW_RELEASE_SHA=$4 LT_REVIEW_STAGE=$5 LT_REVIEW_STAGE_SHA=$6
test -d "$LT_REVIEW_STORAGE"
test -f "$LT_REVIEW_BINARY"
mkdir -- "$7"
LT_REVIEW_OUTPUT=$(realpath -e -- "$7")
export LT_REVIEW_OUTPUT
finish() {
  code=$?
  trap - EXIT
  printf '%s\n' "$code" > "$LT_REVIEW_OUTPUT/exit-status.txt"
  printf 'Metadata review result: %s (%s)\n' "$code" "$LT_REVIEW_OUTPUT"
  exit "$code"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
cp "$repo_dir/docker-compose.stage-review.yml" "$LT_REVIEW_OUTPUT/compose.yml"
cp "$repo_dir/scripts/run-stage-evidence-review.sh" "$LT_REVIEW_OUTPUT/runner.sh"
cp "$repo_dir/scripts/go-build.env" "$LT_REVIEW_OUTPUT/go-build.env"
printf '%s\n' "$LT_REVIEW_BINARY_SHA" > "$LT_REVIEW_OUTPUT/executable.sha256"
docker image inspect "$LT_GO_IMAGE" --format '{{.Id}} {{.Os}}/{{.Architecture}}' > "$LT_REVIEW_OUTPUT/runtime.txt"
docker compose --env-file "$repo_dir/scripts/go-build.env" \
  -p "lt-stage-review-$$" -f "$repo_dir/docker-compose.stage-review.yml" \
  run --rm --no-deps --pull never stage-review
