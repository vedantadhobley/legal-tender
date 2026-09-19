#!/usr/bin/env bash
# Run in the background when used by an agent. Success requires tests.exit=0
# and run.exit=0; logs survive teardown in the printed temporary directory.
set -euo pipefail

repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
run_dir=$(mktemp -d "${TMPDIR:-/tmp}/legal-tender-window-test.XXXXXXXX")
project_suffix=${run_dir##*.}
project_name="lt-window-test-${project_suffix,,}"
export LT_GO_CACHE_DIR=${LT_GO_CACHE_DIR:-${HOME}/.cache/legal-tender}
compose=(docker compose -p "$project_name" -f "$repo_dir/docker-compose.window-test.yml")
printf 'Integration evidence: %s\n' "$run_dir"

# Never adopt a prior project when cleaning up a failed or interrupted run.
test -z "$("${compose[@]}" ps -aq)"
cleanup() {
  result=$?
  trap - EXIT
  "${compose[@]}" down --volumes > "$run_dir/cleanup.log" 2>&1 || result=1
  printf '%s\n' "$result" > "$run_dir/run.exit"
  printf 'Integration result: %s (logs: %s)\n' "$result" "$run_dir"
  exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

"${compose[@]}" up -d > "$run_dir/setup.log" 2>&1
runner=$("${compose[@]}" ps -aq window-test)
test -n "$runner"
status=$(docker wait "$runner")
docker logs "$runner" > "$run_dir/tests.log" 2>&1
docker inspect "$runner" --format 'exit={{.State.ExitCode}} oom={{.State.OOMKilled}}' > "$run_dir/container-state.txt"
printf '%s\n' "$status" > "$run_dir/tests.exit"
if test "$status" != 0; then
  tail -50 "$run_dir/tests.log"
  exit 1
fi
