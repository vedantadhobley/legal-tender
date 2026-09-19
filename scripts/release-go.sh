#!/usr/bin/env bash
# Retain one exact source/dependency closure and prove two clean offline builds.
# Run this long job in the background; success requires exit-status.txt=0.
set -euo pipefail
if test "$#" != 1 || test -z "$1"; then
  echo 'usage: release-go.sh NEW_OUTPUT_DIRECTORY' >&2
  exit 2
fi
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
source "$repo_dir/scripts/go-build.env"
[[ "$LT_GO_IMAGE" =~ @sha256:[0-9a-f]{64}$ ]]
for file in Dockerfile Dockerfile.dev; do
  grep -Fxq "ARG GO_IMAGE=$LT_GO_IMAGE" "$repo_dir/$file"
done
# Require the pin locally; never substitute a tag or pull during offline proof.
docker image inspect "$LT_GO_IMAGE" --format '{{.Id}} {{.Os}}/{{.Architecture}}' >/dev/null
export LT_BUILD_SOURCE="$repo_dir"
export LT_BUILD_MODULE_SEED=${LT_GO_MODULE_CACHE:-${HOME}/.cache/legal-tender/gomodcache}
test -d "$LT_BUILD_MODULE_SEED/cache/download"
LT_BUILD_MODULE_SEED=$(realpath "$LT_BUILD_MODULE_SEED")
export LT_BUILD_UID=$(id -u) LT_BUILD_GID=$(id -g)
mkdir -- "$1"
LT_BUILD_OUTPUT=$(realpath "$1")
export LT_BUILD_OUTPUT
printf 'Build evidence: %s\n' "$LT_BUILD_OUTPUT"
finish() {
  result=$?
  trap - EXIT
  printf '%s\n' "$result" > "$LT_BUILD_OUTPUT/exit-status.txt"
  printf 'Build result: %s (%s)\n' "$result" "$LT_BUILD_OUTPUT"
  exit "$result"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
compose=(docker compose --env-file "$repo_dir/scripts/go-build.env"
  -p "lt-go-build-$$" -f "$repo_dir/docker-compose.go-build.yml")
docker image inspect "$LT_GO_IMAGE" --format '{{.Id}} {{.Os}}/{{.Architecture}}' > "$LT_BUILD_OUTPUT/builder.txt"
cp "$repo_dir/scripts/go-build.env" "$LT_BUILD_OUTPUT/go-build.env"
date -u +%FT%TZ > "$LT_BUILD_OUTPUT/started.txt"
TIMEFORMAT='real_seconds=%R user_seconds=%U system_seconds=%S'
for phase in prepare first second; do
  { time "${compose[@]}" run --rm --no-deps --pull never go-build "$phase" > "$LT_BUILD_OUTPUT/$phase.log" 2>&1; } 2> "$LT_BUILD_OUTPUT/$phase.timing"
done
cd "$LT_BUILD_OUTPUT"
cmp first/legal-tender second/legal-tender
sha256sum -c INPUTS.sha256 > input-check.txt
test "$(< tests.exit)" = 0
test "$(< negative-tests.exit)" = 0
test "$(< first/exit-status)" = 0
test "$(< second/exit-status)" = 0
date -u +%FT%TZ > finished.txt
sha256sum source.tar.gz modules.tar.gz first/legal-tender second/legal-tender \
  first/legal-tender.buildinfo second/legal-tender.buildinfo first/help.txt second/help.txt go-build.env builder.txt \
  INPUTS.sha256 input-check.txt prepare.log first.log second.log tests.log tests.exit negative-tests.log negative-tests.exit \
  first/exit-status second/exit-status prepare.timing first.timing second.timing \
  prepare.memory-peak first.memory-peak second.memory-peak \
  started.txt finished.txt > SHA256SUMS
sha256sum -c SHA256SUMS > checksum-check.txt
