#!/usr/bin/env bash
# Read-only inventory, planning or admitted file verification plus exact replay.
# Exit 1 is retained; successful file verification is not recovery acceptance.
set -euo pipefail
if [[ "${1:-}" == --worker ]]; then
  test "$#" = 1
  LT_RECOVERY_COMMAND=${LT_RECOVERY_COMMAND:-inspect-funding-recovery-inventory}
  case "${LT_RECOVERY_COMMAND:-inspect-funding-recovery-inventory}" in
    inspect-funding-recovery-inventory|plan-funding-recovery|verify-funding-recovery-files) ;;
    *) echo 'unsupported recovery command' >&2; exit 2 ;;
  esac
  test ! -e /output/result.json
  test ! -e /output/replay.json
  test "$(sha256sum /app | cut -d ' ' -f 1)" = "$LT_RECOVERY_BINARY_SHA256"
  test "$(sha256sum /inputs.json | cut -d ' ' -f 1)" = "$LT_RECOVERY_INPUT_SHA256"
  cp /inputs.json /output/inputs.json
  printf '%s\n' "${LT_RECOVERY_COMMAND:-inspect-funding-recovery-inventory}" > /output/command.txt
  date -u +%FT%TZ > /output/started.txt
  TIMEFORMAT='real_seconds=%R user_seconds=%U system_seconds=%S'
  extra=()
  printf 'metadata_only\n' > /output/admission.txt
  if [[ "${LT_RECOVERY_COMMAND}" == verify-funding-recovery-files ]]; then
    extra=(--max-bytes "${LT_RECOVERY_MAX_BYTES:?explicit byte ceiling required}" --workers "${LT_RECOVERY_WORKERS:?explicit worker count required}")
    printf 'max_bytes=%s\nworkers=%s\n' "$LT_RECOVERY_MAX_BYTES" "$LT_RECOVERY_WORKERS" > /output/admission.txt
  fi
  for phase in result replay; do
    code=0
    { time /app pipeline fec "${LT_RECOVERY_COMMAND:-inspect-funding-recovery-inventory}" \
        --storage-root /storage --inputs /inputs.json \
        --expected-inputs-sha256 "$LT_RECOVERY_INPUT_SHA256" \
        "${extra[@]}" \
        > "/output/$phase.json" 2> "/output/$phase.log"; } \
        2> "/output/$phase.timing" || code=$?
    printf '%s\n' "$code" > "/output/$phase.exit"
    test "$code" -le 1
    test -s "/output/$phase.json"
  done
  cmp /output/result.json /output/replay.json
  cmp /output/result.exit /output/replay.exit
  printf '0\n' > /output/replay-check.exit
  cp /sys/fs/cgroup/memory.peak /output/memory-peak
  date -u +%FT%TZ > /output/finished.txt
  cd /output
  sha256sum inputs.json command.txt admission.txt result.json replay.json result.exit replay.exit \
    result.log replay.log result.timing replay.timing replay-check.exit \
    memory-peak started.txt finished.txt > SHA256SUMS
  sha256sum -c SHA256SUMS > checksum-check.txt
  exit "$(< result.exit)"
fi
if test "$#" -lt 4 || test "$#" -gt 7; then
  echo 'usage: run-funding-recovery-inventory.sh STORAGE_ROOT ACCEPTED_BINARY INPUTS_JSON NEW_OUTPUT_DIRECTORY [inventory|plan|verify MAX_BYTES WORKERS]' >&2
  exit 2
fi
case "${5:-inventory}" in
  inventory) export LT_RECOVERY_COMMAND=inspect-funding-recovery-inventory ;;
  plan) export LT_RECOVERY_COMMAND=plan-funding-recovery ;;
  verify) export LT_RECOVERY_COMMAND=verify-funding-recovery-files ;;
  *) echo 'unsupported recovery mode' >&2; exit 2 ;;
esac
if [[ "$LT_RECOVERY_COMMAND" == verify-funding-recovery-files ]]; then
  test "$#" = 7
  [[ "$6" =~ ^[1-9][0-9]{0,18}$ ]]
  [[ "$7" =~ ^[1-8]$ ]]
  export LT_RECOVERY_MAX_BYTES="$6" LT_RECOVERY_WORKERS="$7"
else
  test "$#" -le 5
  export LT_RECOVERY_MAX_BYTES=0 LT_RECOVERY_WORKERS=0
fi
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
source "$repo_dir/scripts/go-build.env"
export LT_RECOVERY_STORAGE=$(realpath -e -- "$1")
export LT_RECOVERY_BINARY=$(realpath -e -- "$2")
export LT_RECOVERY_INPUTS=$(realpath -e -- "$3")
export LT_RECOVERY_INPUT_SHA256=$(sha256sum "$LT_RECOVERY_INPUTS" | cut -d ' ' -f 1)
export LT_RECOVERY_BINARY_SHA256=$(sha256sum "$LT_RECOVERY_BINARY" | cut -d ' ' -f 1)
test -d "$LT_RECOVERY_STORAGE"
test -f "$LT_RECOVERY_BINARY"
test -f "$LT_RECOVERY_INPUTS"
mkdir -- "$4"
export LT_RECOVERY_OUTPUT=$(realpath -e -- "$4")
finish() {
  code=$?
  trap - EXIT
  printf '%s\n' "$code" > "$LT_RECOVERY_OUTPUT/exit-status.txt"
  printf 'Inventory result: %s (%s)\n' "$code" "$LT_RECOVERY_OUTPUT"
  exit "$code"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
cp "$repo_dir/docker-compose.recovery.yml" "$LT_RECOVERY_OUTPUT/compose.yml"
cp "$repo_dir/scripts/run-funding-recovery-inventory.sh" "$LT_RECOVERY_OUTPUT/runner.sh"
cp "$repo_dir/scripts/go-build.env" "$LT_RECOVERY_OUTPUT/go-build.env"
printf '%s\n' "$LT_RECOVERY_BINARY_SHA256" > "$LT_RECOVERY_OUTPUT/executable.sha256"
docker image inspect "$LT_GO_IMAGE" --format '{{.Id}} {{.Os}}/{{.Architecture}}' > "$LT_RECOVERY_OUTPUT/runtime.txt"
docker compose --env-file "$repo_dir/scripts/go-build.env" \
  -p "lt-recovery-$$" -f "$repo_dir/docker-compose.recovery.yml" \
  run --rm --no-deps --pull never recovery-inventory
