# FEC storage accounting and staging review — 2026-09-09

Follow-up to the [metadata-only v4 plan](./fec-v4-refresh-plan-2026-09-09.md).
Go now counts regular-file logical bytes once per device/inode in both
acquisition and staging. Separate copies count separately; sparse files retain
their full logical size. Symlinks are not followed. This fixes hard-link double
counting without changing the 600 GiB cap, 500 GiB floor, or workspace reserve.

## Read-only review boundary

The Go `review-release-storage` command uses saved plan/prior-manifest inputs
and local file metadata. It creates no storage directories, reads no publisher
bodies, performs no network requests, and cannot acquire, stage, or publish.
Its [strict diagnostic contract](../../contracts/audits/fec/storage-review/v1/result.schema.json)
is separate from the unchanged acquisition and staging result schemas.

```sh
legal-tender pipeline fec review-release-storage \
  --plan <saved-update-plan> --current <that-plans-prior-manifest> \
  --storage-root /storage
```

Run against quiescent project storage on one filesystem. The review uses the
same acquisition preflight, including candidate-specific partial-byte credit.
It checks metadata for reused artifacts and staged outputs; it does not replace
the full content verification required during acquisition/staging. Current
filesystem free space is an observation, not a reservation against other jobs.

For every inventory-selected output, it reports one of:

- `unchanged_source`: zero new retained output bytes, with existing backing checked.
- `prior_size_new_content`: assume the prior compressed size, with no speculative
  content-addressed deduplication or checkpoint savings.
- `unknown`: no compatible prior output; the scenario is incomplete, not zero-cost.

The conservative scenario envelope reserves all assumed new outputs, remaining
downloads, the full existing workspace allowance, and the free-space floor.
Schedule A outputs also accumulate against its hot cap. B's archive-direct
relations and whole committee CSVs do not invent staged extracts. The selected
zstd temporary file becomes its finalized inode; it is not a second retained
copy. The full legacy workspace allowance remains on top of this envelope.

Exit 1 means invalid inputs, acquisition blocked, or a staging scenario that is
incomplete/over budget. Inspect the JSON and stderr to distinguish these cases.
Exit 0 means only that the scenario fits: prior sizes are **not upper bounds**
on new bytes, and this command is not an automatic acquisition authorization.
No Dagster sensor or runtime acquisition policy is changed by this diagnostic.

## Real result

The September 9 saved candidate was reviewed at 19:14:27 UTC with networking
disabled and project data mounted read-only. The active v3 pointer matched the
captured baseline before and after the command. No new metadata discovery,
source download, large content hashing, extraction, or graph write ran.

| Quantity | Bytes |
|---|---:|
| Schedule A hot files, unique logical size | 297,015,247,494 |
| Remaining candidate downloads, all sources | 129,566,279,592 |
| Acquisition A projection, including workspace | 620,395,768,336 |
| Assumed new A extracts, all four cycles | 57,968,298,375 |
| Assumed new staged outputs, all families | 58,005,781,337 |
| A retained-output envelope plus workspace | 678,364,066,711 |
| Excess over unchanged 600 GiB cap | 34,118,972,311 |
| Filesystem bytes required, including floor | 957,649,911,487 |
| Available filesystem bytes | 1,404,702,175,232 |

Acquisition passes; the complete prior-size staging scenario does not. Three
unchanged classic outputs receive reuse credit, explaining the small difference
from the previous audit's all-output illustrative total. Actual new extracts
could differ in size or match existing content; this run does not establish either.

## Cleanup review

Read-only metadata inspection found no large disposable selected-stage files:

- The A 89.88 GB snapshot/partial pair and B 39.31 GB snapshot/CAS pair are hard
  links. Removing an alias would not reclaim their data blocks.
- The four older A snapshot extracts total 58,987,591,718 bytes. They belong to
  the August 23 snapshot, not the newer accepted extract set. Their filenames,
  sizes, and provenance do not establish duplicate contents. Retain them.
- The old non-schedule partials total 6,254,238 bytes. They are source captures,
  not evidence-free scratch files. Removing them would not fix the A hot cap.
- Project cache uses about 12.46 MB of allocated space. Legacy parser dumps
  use about 13.75 GB and remain differential-validation evidence, not approved
  garbage. Neither is counted against the Schedule A hot lane.

Nothing was deleted, moved, or deduplicated physically. A cold-tier retention
decision remains necessary for indefinite refreshes; deleting historical
evidence or moving it outside a counted directory is not a retention solution.

## Verification and next gate

The complete Go suite, targeted release/CLI race tests, static analysis, and
build pass. Regression tests cover hard links, separate identical copies,
sparse files, external links, symlink loops, shared acquire/stage accounting,
partial credit, overflow, exact budget boundaries, unknown output sizes,
inventory-derived A/E staging, B/CSV exclusions, and read-only CLI behavior.
Independent schema/arithmetic/inode checks plus source/Dagster boundary tests
pass (45 passed; 9 unrelated opt-in corpus checks skipped).

Small evidence is retained under
`dumps/audits/fec/storage-review/2026-09-09/`: exact plan/prior inputs, Go review,
file metadata, before/after pointer hashes, cleanup sizes, test logs, and exit
markers. The expected review exit is 1; verification gates exit 0.

Follow-up: the [streaming storage gate](./fec-streaming-storage-2026-09-09.md)
now passes. This audit and its v1 report retain the original reserve model.

At this audit's conclusion, the next step was a bounded streaming-workspace budget covering cumulative
retained outputs, temporary zstd bytes, cancellation, and disk growth. The
current extractor streams COPY directly through zstd; it does not materialize
the reserved largest uncompressed relation. Replacing that conservative reserve
requires explicit measured validation and runtime enforcement, not merely a
smaller constant. Keep the existing cap/floor and retained evidence intact.
Only then freshly discover/review the v4 candidate before acquisition. Source
release staging is distinct from downstream fact, calculation, and graph costs.
