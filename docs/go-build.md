# Reproducible Go release builds

## Active development versus release acceptance

As clarified on 2026-09-16, use `make build`, `make test` and `make check` for
ordinary development. These do not run the two-build/archive comparison below.
Existing dependency/compiler pins and source-integrity checks stay in place;
pinning records what was used, not a ban on reviewed dependency or design changes.

`release-build`, archival reconstruction and complete recovery acceptance are
opt-in operational work, not prerequisites for exploratory source analysis, schema
discussion or prototypes. Do not start a new exact-build campaign merely because
code changed. Revisit that work when a release/recovery task is explicitly scoped.
The [relationship exploration](./design/relationship-exploration.md#development-discipline-and-cleanup-status)
records the verified partial cleanup; the historical build tooling remains available.

## Retained release contract

The release contract is Linux/amd64, Go 1.26.5 and `CGO_ENABLED=0`. The compiler
image is pinned by digest in [go-build.env](../scripts/go-build.env).
[build-go.sh](../scripts/build-go.sh) owns the environment and flags; both
Dockerfiles call that script. This pins the Go executable, not the entire
Python/Dagster runtime image.

## Commands

Docker, Bash and GNU core utilities are required. Prepare the exact compiler
image and module cache once; these commands may use the network:

```bash
source scripts/go-build.env
docker pull "$LT_GO_IMAGE"
make release-deps
```

`release-deps` checks downloads against `go.sum`, verifies cached content and
requires unchanged module metadata. It mounts the source read-only. It does not
update dependency versions or accept new sums.

Choose a new output directory whose parent already exists:

```bash
make release-build BUILD_OUTPUT=/absolute/path/to/new-build
```

Use configured durable storage for retention, such as
`~/workspace/data/legal-tender/builds/go/`; temporary checks can use `/tmp`.
Run long builds in the background. Success requires `exit-status.txt` to contain
zero, not merely a stopped process. Existing output directories are rejected.

The [controller](../scripts/release-go.sh) uses a [one-shot Compose tool](../docker-compose.go-build.yml)
to snapshot an explicit source/contract/fixture/recipe allowlist, prepare verified
modules, and compile twice in fresh network-disabled containers. Source paths,
module expansion and compiler caches differ. The second build deliberately sets
conflicting ambient Go options; the compiler wrapper must ignore them.

The gate runs the complete CGO-disabled Go source tests, CLI help smoke checks,
negative build-contract checks, byte comparison and artifact checksum verification.
It rejects missing modules without a download or toolchain fallback. Set
`LT_GO_MODULE_CACHE` only to select a different local download cache; versions
and content still come from the locked module files. Its default is
`~/.cache/legal-tender/gomodcache`.

## Compiler and resource boundary

The wrapper clears inherited settings and fixes Linux/amd64/v1, CGO off, local
toolchain selection, workspace/user-config isolation and no compiler experiments.
Release flags are `-mod=readonly -trimpath -buildvcs=false -pgo=off`. No build
timestamp, checkout path, Git dirty state or machine-specific CPU tuning is
injected. Go module/build information is retained. An ELF dynamic loader is forbidden.

Source tests keep source paths because existing fixtures use `runtime.Caller`.
Those test executables are not release artifacts. Race tests remain a separate
CGO-enabled development gate. `make build` is a development compile check, not an
accepted release. Dockerfile defaults must match the image pin; the controller
rejects drift. Custom image overrides are not the canonical environment.

The tool has a 4 GiB memory/no-swap cap, 2 GiB Go heap limit, four CPUs and a
2 GiB temporary filesystem. Source tests use a disk-backed temporary directory
inside the new output so filesystem-reserve checks retain their normal meaning.
No database, production network, credentials or application data is mounted.
No standing service limit changes.

## Retained evidence and rebuild

The source allowlist excludes `.git`, `.env`, the Python application and raw
data. Symlinks and other non-regular source entries are rejected. Archive order,
ownership, modes and timestamps are normalized inside the pinned compiler image.
Go authenticates module downloads from the read-only local file proxy into a
fresh cache; builds do not trust the original expanded module/compiler caches.

A successful output retains source and module-download archives, `INPUTS.sha256`,
two identical executables, their build information, image identity, help output,
tests, timings, cgroup memory peaks, `SHA256SUMS` and explicit success markers.
Negative tests reject wrong compiler versions, existing outputs, missing offline
dependencies and modified modules. They check the failure reason, not just status.

To rebuild without the original checkout or module cache, verify `SHA256SUMS`
inside the accepted output. Extract `source.tar.gz` into a new temporary source
directory and `modules.tar.gz` into a separate empty module-cache directory.
From that restored source:

```bash
LT_GO_MODULE_CACHE=/absolute/path/to/restored-module-cache \
  bash scripts/release-go.sh /absolute/path/to/new-rebuild
cmp /absolute/path/to/accepted/first/legal-tender \
  /absolute/path/to/new-rebuild/first/legal-tender
```

The pinned compiler image must already exist locally. Pinning does not preserve
a registry image indefinitely; archive or mirror it during complete recovery
work. Checksums establish integrity relative to the retained inventory, not
authorship or a signed supply-chain attestation.

The [acceptance audit](./audit/go-build-2026-09-14.md) records measured results and
limits. Historical binaries keep their original identities; no existing graph
or calculation is republished. Python/OS packages, Arango images/state, extraction
tools, source-data dependencies and retention enforcement remain outside this
gate and inside the [recovery checkpoint](./design/pre-attribution-review.md).
