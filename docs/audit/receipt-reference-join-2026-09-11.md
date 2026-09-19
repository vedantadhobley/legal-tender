# Complete-cycle receipt reference join — 2026-09-11

Status: complete-cycle execution, source/artifact verification and layout-varied
replay passed. The [join contract](../design/receipt-reference-join.md) defines the
scope; contributor graph publication remains separate. The subsequent
[parallel implementation](./receipt-reference-parallelism-2026-09-11.md) has its
own passing real-corpus equivalence gate.

## Input and executable

```text
fact_set_id = 8f69a2ef40316f62d8844ecf732342a3b4535136b434f60677d0608cb04465df
executable  = 7e602f31f1bfbc9e8ecfa53edf60c91049020ae4c219ca5d85b310cc72d91eb1
source_tgz  = 8e09609c9b1d4eae0a4955a314621fada5f2c536bced1456e52a806e4419c190
```

The complete existing 2024 fact set contains 264,085,606 occurrences in 265
shards. No new source download or mutable current-pointer selection is involved.
The input retains its original ancestry; the current source release does not
silently replace it. A fresh build from the retained code produced the identical
executable digest. `source-final.tar.gz` also includes the subsequent brute-force
fixture test; production code did not change after the executable was built.

The first complete request scan found 51,600,308 reference-bearing occurrences.
The second scan admitted 75,668,888 transaction occurrences into candidate lookup
membership. This latter count includes filter false positives and is not a count
of valid references, donors, payments or resolved entities. Exact-key joins and
complete multiplicities determine reference states after sorting. Both runs
completed and produced the same calculation identity:

```text
94cc38ce9d509dbb4acad76a35c64183221bf2adb0c9a18de47f22ff5ae847e3
```

| Disposition | Source occurrences |
|---|---:|
| No report reference | 212,485,298 |
| Exact same-report reference | 50,454,211 |
| Missing reference schedule | 1,114,397 |
| Duplicate source transaction ID | 27,234 |
| Reference schedule mismatch | 1,893 |
| Incomplete reference | 1,573 |
| Target absent from the pinned cycle/report | 839 |
| Self-reference | 161 |

These states conserve all 264,085,606 occurrences. The exact-reference artifacts
contain 100,908,422 directional incidences and 68,864,623 participating occurrence
endpoints; those are not resolved people, payments or qualified conduits.

## Execution and resource boundary

Each execution uses a network-disabled container with four CPU quota, 4 GiB
memory limit, `GOMEMLIMIT=2GiB` and `GOMAXPROCS=4`. Source storage is read-only.
Each new output workspace has a 16 GiB live write cap, including partial files
and simultaneous merge inputs/outputs. Runs contain at most 100,000 records and
32 MiB of encoded data. The candidate filter occupies 64 MiB.

The first run used four source readers and merge fan-in eight. Replay used three
readers and fan-in sixteen with the same source and executable. Logical decisions,
all four artifact hashes and calculation identity matched despite different
physical execution order and filenames. Runtime and RSS are not logical fields.

| Run | Wall seconds | Peak process RSS bytes | Peak workspace bytes | Retained artifact bytes |
|---|---:|---:|---:|---:|
| First | 2,780.008 | 747,311,104 | 4,098,433,229 | 3,098,441,071 |
| Layout-varied replay | 2,569.362 | 724,049,920 | 4,098,798,946 | 3,098,441,071 |

These runs overlapped on a shared host; the timings are observations, not a
controlled comparison of merge fan-in.

An initial serial-reader attempt was deliberately stopped after measurement
showed one busy CPU during scanning. Its partial workspace and interruption note
remain separate; it has no completion manifest. The replacement bounds readers
and requires consumer acknowledgment before source-buffer reuse, including errors
and cancellation. No source evidence was removed or changed.

## Verification boundary

The complete Go suite, `go vet` and targeted race tests passed. Fixtures cover
duplicate source/target keys across runs, unreferenced duplicate targets, missing
targets, invalid scope, self-reference, schedule mismatch, reciprocal links,
fan-out, filter false positives, execution geometry, corruption, byte limits and
buffer ownership. The existing bounded report-review tests still pass after
extracting one shared pure reference classifier.
An additional shuffled-fixture oracle compares membership using direct nullable
field equality and neighbor counts using in-memory peer sets. It does not use the
production key framing, filter or sort implementation. The final full Go suite,
targeted race checks and vet pass after that addition.

The retained-corpus test verified every output stream's checksums, ordering and
counts, every sparse decision's identity/state, global incoming/outgoing totals,
and data-selected lookup multiplicity witnesses. A separate full-physical-schema
field map checked 35 source/target witnesses across seven reference states against
the retained Parquet facts, including exact report and transaction keys. The test
passed in 368.25 seconds. It uses the same pinned Parquet library, not an independent
decoder or an independent full-cycle algorithm.

Execution, corpus checks and logical replay comparison all succeeded with explicit
zero exit markers. Exact references alone do not establish conduit roles,
effective cash payments, person identity or terminal attribution. No existing
Arango graph, source pointer, financial calculation or Dagster asset changes.

## Retained evidence

The complete audit tree, including the interrupted attempt separately identified
in its command note, is retained under:

```text
/storage/dumps/audits/fec/receipt-reference-join/2026-09-11/attempt-01/
```

`parallel/manifest.json` and `replay/manifest.json` are successful completion
manifests. The process, corpus, comparison and overall verification exit markers
are zero. `corpus-check.log` and `replay-comparison.json` preserve the checks;
`SHA256SUMS`, `retention-check.log` and zero `retention-check.exit` verify every
copied file. The original temporary tree remains unchanged.

The source archive, executable, test executable, command note, results and checks
are preserved. The interrupted `cycle/` directory is not an accepted result. The
subsequent parallel work addresses the observed serial merge/classification cost
and adds per-worker/stage progress; its separate real equivalence check now passes.

Next: qualify supported conduit associations using complete reference evidence
and the reviewed role rules, then publish source-backed contributor connections.
This join is a prerequisite for that publication, not a completed funding graph.
