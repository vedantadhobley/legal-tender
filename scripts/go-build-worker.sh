#!/usr/bin/env bash
# Invoked by the capped, network-disabled Compose tool, once per fresh phase.
set -euo pipefail
mode=${1:?prepare|first|second}
finish() {
  result=$?
  trap - EXIT
  if test -r /sys/fs/cgroup/memory.peak; then
    cp /sys/fs/cgroup/memory.peak "/job/$mode.memory-peak"
  fi
  exit "$result"
}
trap finish EXIT
export GOCACHE=/tmp/cache GOMODCACHE=/tmp/modules
cd /job
case "$mode" in
  prepare)
    test ! -e source.tar.gz
    # Explicit source allowlist: never archive the repo root, .git or .env.
    inputs=(go.mod go.sum cmd internal contracts tests/fixtures docs/audit/fixtures scripts/go-build.env
      scripts/build-go.sh scripts/go-build-worker.sh scripts/release-go.sh scripts/test-go-build.sh
      scripts/run-funding-recovery-inventory.sh docker-compose.recovery.yml
      scripts/run-stage-evidence-review.sh docker-compose.stage-review.yml
      docker-compose.go-build.yml Dockerfile Dockerfile.dev Makefile)
    cd /source
    test -z "$(find "${inputs[@]}" ! -type f ! -type d -print -quit)"
    tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner \
      --mode=u=rwX,go=rX -cf - "${inputs[@]}" | gzip -n > /job/source.tar.gz
    mkdir /tmp/source
    tar -xzf /job/source.tar.gz -C /tmp/source
    cmp /job/go-build.env /tmp/source/scripts/go-build.env
    LT_GO_MODULE_PROXY=file:///seed/cache/download bash /tmp/source/scripts/build-go.sh dependencies
    cd "$GOMODCACHE"
    tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner \
      --mode=u=rwX,go=rX -cf - cache/download | gzip -n > /job/modules.tar.gz
    cd /job
    sha256sum source.tar.gz modules.tar.gz > INPUTS.sha256
    ;;
  first|second)
    sha256sum -c INPUTS.sha256
    # Different source paths and independent container-local caches. The second
    # invocation also supplies hostile ambient Go settings to test isolation.
    source_dir="/tmp/$mode/different-source-path"
    mkdir -p "$source_dir" "$GOMODCACHE"
    tar -xzf source.tar.gz -C "$source_dir"
    cmp "$0" "$source_dir/scripts/go-build-worker.sh"
    cmp /job/go-build.env "$source_dir/scripts/go-build.env"
    tar -xzf modules.tar.gz -C "$GOMODCACHE"
    if test "$mode" = second; then
      export GOFLAGS=-race CGO_ENABLED=1 GOARCH=arm64 GOAMD64=v4
      export GOENV=/nonexistent/go-env GOWORK=/nonexistent/go.work GOTOOLCHAIN=invalid
    fi
    LT_GO_MODULE_PROXY=off bash "$source_dir/scripts/build-go.sh" dependencies
    mkdir "$mode"
    bash "$source_dir/scripts/build-go.sh" build "/job/$mode/legal-tender"
    "/job/$mode/legal-tender" --help > "/job/$mode/help.txt" 2>&1
    if test "$mode" = first; then
      # Storage-admission tests need a disk filesystem with the normal reserve;
      # do not weaken the domain guard to accommodate a compiler-cache tmpfs.
      mkdir /job/test-tmp
      LT_GO_TEST_TMPDIR=/job/test-tmp bash "$source_dir/scripts/build-go.sh" test > /job/tests.log 2>&1
      rmdir /job/test-tmp
      printf '0\n' > /job/tests.exit
      bash "$source_dir/scripts/test-go-build.sh" "$source_dir" "/job/$mode/legal-tender" > /job/negative-tests.log 2>&1
      printf '0\n' > /job/negative-tests.exit
    fi
    printf '0\n' > "$mode/exit-status"
    ;;
  *) echo "unsupported worker phase: $mode" >&2; exit 2 ;;
esac
