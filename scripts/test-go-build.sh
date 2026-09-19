#!/usr/bin/env bash
# Negative integration checks inside the isolated builder, not host mutations.
set -euo pipefail
source_dir=${1:?source directory required}
binary=${2:?existing release binary required}
check_dir=$(mktemp -d /tmp/build-contract.XXXXXXXX)
expect_failure() {
  name=$1
  shift
  if "$@" > "$check_dir/$name.log" 2>&1; then
    echo "unexpected success: $name" >&2
    exit 1
  fi
  printf 'PASS: %s rejected\n' "$name"
}
before=$(sha256sum "$binary")
expect_failure existing-output bash "$source_dir/scripts/build-go.sh" build "$binary"
grep -Fq 'release output already exists' "$check_dir/existing-output.log"
test "$before" = "$(sha256sum "$binary")"
expect_failure missing-modules env GOMODCACHE="$check_dir/empty-modules" \
  bash "$source_dir/scripts/build-go.sh" build "$check_dir/missing-modules-binary"
grep -Fq 'module lookup disabled by GOPROXY=off' "$check_dir/missing-modules.log"
mkdir -p "$check_dir/wrong-version/scripts"
cp "$source_dir/scripts/build-go.sh" "$check_dir/wrong-version/scripts/"
cp "$source_dir/scripts/go-build.env" "$check_dir/wrong-version/scripts/"
sed -i 's/^LT_GO_VERSION=.*/LT_GO_VERSION=go0.0.0/' "$check_dir/wrong-version/scripts/go-build.env"
expect_failure wrong-version bash "$check_dir/wrong-version/scripts/build-go.sh" dependencies
grep -Fq 'release requires compiler go0.0.0' "$check_dir/wrong-version.log"
# A modified downloaded module must fail verification even if it still compiles.
module_file=$(find "$GOMODCACHE" -name '*.go' -type f -print -quit)
test -n "$module_file"
cp "$module_file" "$check_dir/module-original"
chmod u+w "$module_file"
printf '\n// modified dependency\n' >> "$module_file"
expect_failure changed-module bash "$source_dir/scripts/build-go.sh" build "$check_dir/changed-module-binary"
grep -Fq 'has been modified' "$check_dir/changed-module.log"
cp "$check_dir/module-original" "$module_file"
