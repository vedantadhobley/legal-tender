# Committee-flow publication and readiness gate — 2026-09-08

The immutable Go reconciliation publisher and observation-only readiness
bundle pass for the complete 2024 inputs. Publication preserves the exact
[earlier audited result](./committee-flow-reconciliation-2026-09-08.md),
including every candidate component, source locator, policy, and separate
ledger amount. No economic-flow interpretation or graph was added.

## Measured operations

| Operation | Result | Seconds |
|---|---|---:|
| First immutable calculation publication, eight scan workers | Exact previous result bytes | 515.223 |
| Same-input publication replay, one worker | Exact result; no source-row scan | 15.738 |
| Readiness using prior committee-master facts | Rejected different archive ancestry | Not timed |
| Readiness using release-matched master facts | Ready | 15.980 |
| Readiness publication replay | Byte-identical bundle | 15.992 |
| Read-only bundle loader | Byte-identical bundle; all backing reverified | 15.974 |
| Independent evidence and bundle validation | Passed | 32.192 |

Go jobs ran with a 4 GiB container cap, four CPUs, `GOMEMLIMIT=2GiB`, and
`GOMAXPROCS=4`, without network access. A sampled calculation reading was
908 MiB; this is not a measured peak. No job reported OOM. The initial gate
exited nonzero at the intended master-ancestry check; the later readiness and
independent-validation jobs exited zero with explicit completion markers.

The first publication scans existing Parquet policy columns. It downloads
nothing and does not re-extract Schedule A/B. Reuse still hashes all source
shards and replays compact candidate evidence; “no scan” means no source-row
decoding or reselection, not no storage reads. This does not establish a
speedup for the first full source scan over the previous audit.

The unchanged calculation conserves 421,629,769 source facts, selects 320,731
A observations and 341,720 B observations, and preserves 308,488 candidate
components. A retains $4,672,820,179.49 and B retains $5,158,069,433.82 as
separate signed reported amounts. They are not a combined funding total.

## A real readiness rejection, not a special-case repair

The current 2024 committee master initially belonged to release v2. Release
v3 selected a different ZIP archive SHA, even though the selected `cm.txt`
compressed and uncompressed bytes were identical. The accepted source
contract includes archive identity in occurrence ancestry. Readiness rejected
that old occurrence/fact ancestry and published no ready pointer.

The existing classic occurrence and fact publishers then consumed the already
staged v3 member. They published new immutable source ancestry and advanced
the 2024 committee-master occurrence/fact pointers. All 20,938 registrations
were valid and unique; the source change set reports 20,938 unchanged, zero
added, zero changed, and zero absent records. No archive download, record
correction, matching-rule change, or check relaxation was needed.

Previous immutable committee facts and bundles remain available. Existing
Arango graphs were not modified; their original bundles still pin their
original inputs. The new bundle does not include historical identity facts.

## Exact identities

Calculation:

```text
calculation_id = 987626c6070c7e7f57db092ab108fb56d0ab97c6ac6a3f802dd277d9738d6ae8
result_sha256  = 070e2c67057ab4671d48f139924760c6e5c8bf5fae60ecf2e6e2a779b4da1882
```

Readiness:

```text
bundle_id     = 113c25c47c3d008dd79a470c9cd8e3482bbdcf82c1fc6a53f717561f571e9c3d
bundle_sha256 = d450dfdb2efca9b0ace973fcb3daf2218ad0775152183fd03cd93f34dbc9a62e
master_facts  = a9f3235c81ce1487c9322a899a586ec00992adf26b81bf4c76444c0c5f75d254
master_sha256 = 17ba070b8ac450d9d383f4d815250a8c5299f7ac5e936b9a0f7806d10981f088
```

The result is 154,245 bytes; the bundle is 2,258 bytes. The new calculation
tree measures 26,505,179 bytes and the bundle tree 16,804 bytes via `du -sb`,
including their pointers and directory overhead. Source facts and the prior
audit are retained separately.

The [publication contract](../design/committee-flow-publication.md) owns
canonical immutable/current paths and evidence-key resolution. The source
release and A/B identities remain those in the earlier reconciliation audit.

## Verification and retained evidence

The independent one-off validator checks every observation/assertion schema,
all compressed/uncompressed artifact hashes and sizes, source-decision and
selected-ordinal conservation, component IDs/states, and separate signed
amounts. It compares the new result with the previous audit byte-for-byte,
independently recomputes the bundle ID, validates the bundle schema, checks
current/immutable byte equality, and walks master occurrence and original/
coordinated source manifests to compare selected byte identities.

Code gates passed:

- `go vet ./...` and `go test ./...`.
- Race tests for reconciliation, CLI, and occurrence packages.
- All 21 calculation-contract Python tests; Python adds no pipeline behavior.
- Concurrent publication, interruption recovery, failed-update pointer
  preservation, corrupt evidence rejection, policy/input invalidation,
  immutable collision, path escape, and exact archive-ancestry tests.

Artifacts are retained under
`/storage/dumps/audits/fec/committee-flow-publication/2026-09-08/2024/`, including
result/replay JSON, master publications, readiness/replay/readback JSON,
validation output and script, progress logs, and explicit success markers.
The initial rejection log and failure marker are retained separately; they
are not the final readiness state. Compact evidence lives in the canonical
calculation publication rather than another audit copy.

## Remaining boundary

The [observation graph](../design/arango-committee-flow-evidence.md) and thin
Dagster wiring remain next. Readiness does not claim complete master coverage
or terminal-source eligibility. Other cycles need their own source/calculation
gates. V1 reuses exact calculation identity; reuse across a changed coordinated
release and historical identity automation remain explicit deferred work.
