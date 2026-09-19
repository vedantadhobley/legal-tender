#!/usr/bin/env bash
# The same compiler contract is used by retained releases and Docker stages.
set -euo pipefail
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
source "$repo_dir/scripts/go-build.env"
cd "$repo_dir"
mode=${1:?usage: build-go.sh dependencies|build|test [NEW_BINARY]}

# Do not inherit GOFLAGS, workspace files, user Go configuration, experiments,
# architecture tuning, CGO defaults, automatic toolchain downloads or telemetry.
go_cmd=(env -i PATH=/usr/local/go/bin:/usr/bin:/bin
  GOENV=off GOWORK=off GOTOOLCHAIN=local GOTELEMETRY=off
  GOOS=linux GOARCH=amd64 GOAMD64=v1 CGO_ENABLED=0 GOEXPERIMENT= GOFIPS140=off
  GOCACHE="${GOCACHE:-/tmp/go-build-cache}" GOMODCACHE="${GOMODCACHE:-/tmp/go-modules}"
  GOPATH=/tmp/go-path GOMEMLIMIT=2GiB GOMAXPROCS=4
  GOPROXY=off GOSUMDB=off)
if test "$("${go_cmd[@]}" go env GOVERSION)" != "$LT_GO_VERSION"; then
  echo "release requires compiler $LT_GO_VERSION" >&2
  exit 1
fi
before=$(sha256sum go.mod go.sum)
case "$mode" in
  dependencies)
    # Release preparation uses a read-only local module proxy. Docker image
    # construction can use the normal public proxy; go.sum must stay unchanged.
    "${go_cmd[@]}" GOPROXY="${LT_GO_MODULE_PROXY:-https://proxy.golang.org}" go mod download
    "${go_cmd[@]}" go mod verify
    ;;
  build)
    test "$#" = 2
    for target in "$2" "$2.buildinfo"; do
      if test -e "$target" || test -L "$target"; then
        echo 'release output already exists' >&2
        exit 1
      fi
    done
    "${go_cmd[@]}" go mod verify
    "${go_cmd[@]}" go build -mod=readonly -trimpath -buildvcs=false -pgo=off -o "$2" ./cmd/legal-tender
    "${go_cmd[@]}" go version -m "$2" > "$2.buildinfo"
    # A release must not acquire a dynamic loader through a new dependency.
    program_headers=$(readelf -l "$2")
    if [[ "$program_headers" == *INTERP* ]]; then
      echo 'release unexpectedly requires a dynamic loader' >&2
      exit 1
    fi
    ;;
  test)
    # Source tests use runtime.Caller to locate fixtures. This test executable
    # is not the release artifact; keep its source paths available to the tests.
    "${go_cmd[@]}" TMPDIR="${LT_GO_TEST_TMPDIR:-/tmp}" go test -mod=readonly -buildvcs=false -pgo=off -count=1 ./...
    ;;
  *) echo "unsupported build action: $mode" >&2; exit 2 ;;
esac
test "$before" = "$(sha256sum go.mod go.sum)"
