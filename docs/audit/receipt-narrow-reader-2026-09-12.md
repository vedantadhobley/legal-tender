# Narrow receipt-reference reader — 2026-09-12

Status: implemented and accepted; fixtures, complete Go tests, race checks, vet,
bounded real-source field/performance gates, the full 2024 calculation, exact
baseline comparison and separate corpus readback all pass. This follows
the [source profile](./receipt-source-scan-profile-2026-09-12.md),
not a change to reference, donor, conduit or financial semantics.

## Implementation and identities

The [reader](../../internal/calculation/fec/receiptreferences/reader.go) uses the
pinned library's converted column chunks with `NewRowGroupRowReader`, instead of
the conversion wrapper's source-width `Rows` implementation. The generic typed
reconstructor now sees only the ten selected columns. No custom Parquet decoder,
new dependency, field-position constant or retained-source rewrite is introduced.

The source must remain nonempty and flat. Every selected column must exist with
the exact type, optionality and definition level. The production scan still checks
the complete shard digest, byte size, 99-column schema, ordinal range, cycle,
normalization state and row conservation. All retained source fields survive.
The old reader is test-only comparison evidence, not a fallback on malformed input.

`--scan-workers` now permits 1–8 readers and defaults to eight, separately from
the unchanged eight processing workers. Existing buffer acknowledgments, shared
memory/workspace limits, classifier, filter and sort/merge behavior are unchanged.
Source batches still pass through one dispatcher; this does not claim all stages
use eight CPUs continuously.

```text
fact_set_id = 8f69a2ef40316f62d8844ecf732342a3b4535136b434f60677d0608cb04465df
manifest    = b664e752a3bae186508a45bb6d09289ba2fe06f0b4149ecbd72063e7ef79b829
application = ca1a17348c5e807d3e6031adef3e8188d61f997439c97acd4f744e5b533e97db
test_binary = 63df437b3c0423d895275d4e73228a24a3a1f05622813aad5b68fdc12fcabc54
source_tgz  = 7d0ea55313e63155d705ac1594ae5a66f97ebd1e6660ec093d5d283ef70fb9e1
```

## Bounded real gate

The fixed sample is the same eight complete shards used in the profile: indices
0, 33, 66, 99, 132, 165, 198 and 231. The new reader matched every selected value
against the previous generic reader: eight million rows and ten fields per row.
The comparison passed in 28.80 seconds. It uses the same Parquet library with a
different row-construction path, not an independent decoder.

All benchmark cases use fresh processes, an eight-CPU quota, 4 GiB container cap,
`GOMEMLIMIT=2GiB`, `GOMAXPROCS=8`, read-only source storage and no network.
The filter remains 64 MiB total, with eight report workers, 100k-record runs and
a shared 2 GiB sample scratch cap. These timings cover both ingestion passes,
including naturally flushed sort runs, but not final joins or graph publication.

| Reader | Source workers | First wall seconds | Repeat wall seconds | First CPU seconds | First peak RSS bytes |
|---|---:|---:|---:|---:|---:|
| Previous generic | 4 | 22.006 | 22.251 | 75.293 | 1,346,932,736 |
| Narrow | 4 | 9.337 | — | 20.117 | 966,905,856 |
| Previous generic | 8 | 14.892 | — | 86.296 | 1,564,758,016 |
| Narrow | 8 | 8.936 | 9.017 | 20.532 | 1,052,225,536 |

The combined change is about 2.46× faster on the sampled ingestion stages and
uses about 73% fewer CPU-seconds. Most of this gain comes from avoiding unnecessary
row work. After that change, eight versus four narrow readers adds only a small
gain in this sample; do not reuse the old generic-reader scaling factor.
Input was warm-cache, with at most 8192 bytes of charged read I/O per pass.
These runs do not establish cold-disk throughput or statistical confidence bounds.

Every case conserves eight million rows per pass, 1,141,542 reference requests and
1,629,730 filter-candidate members. Sample filter occupancy and report completeness
differ from the full corpus; no source-reference decisions are published from it.
Scratch compression and byte counts can differ with arrival order.

The separate narrow/eight-reader CPU profile measured 20.86 CPU-seconds over
9.02 seconds. Source-width conversion is no longer a dominant cost; filtering,
selected-row reconstruction and ingestion remain. The dispatcher spends almost
the entire membership pass inside its consumer call. That identifies a possible
later batch-overlap optimization, not permission to relax buffer ownership or
the reason to hold this accepted field comparison open.

## Verification and full-cycle boundary

Fixtures cover all nullable strings, null versus empty, whitespace, non-ASCII,
duplicates, 4096-byte fields, differing column positions, multiple page/row-group
boundaries and read batch sizes. Missing, differently typed, differently nullable,
nested and repeated source fields fail. Existing source size/hash/count/ordinal/
cycle guards, cancellation and consumer errors remain tested. Source-buffer
ownership tests now include eight readers. CLI tests reject zero/nine and accept
eight at the concurrency boundary.

The full run uses the same immutable 264,085,606-row input and the same eight-CPU,
4 GiB and 16 GiB workspace budgets as the accepted parallel baseline. Its new
output is isolated. The following must pass before accepting the new result:

- Explicit successful run and completion manifest.
- All four sorted artifact hashes/counts, source identity, state totals and
  reference policy equal the accepted eight-report-worker baseline. Only build
  and calculation identity, execution geometry, filenames and resource/timing
  measurements may differ.
- Complete output-stream readback plus the existing source/multiplicity witnesses.
- Measured full-cycle runtime, memory and disk use, with durable checksum retention.

Both input passes have completed: requests took 196.535 seconds and membership
took 209.948 seconds, versus 424.369 and 401.015 seconds in the previous run.
The combined source-pass time fell from 825.384 to 406.483 seconds (about 2.03×).
All downstream workers and final assembly have completed. A membership-stage
point sample used 203% CPU and 1.023 GiB container memory; it is not a peak.

| Complete calculation | Previous parallel reader | Narrow reader |
|---|---:|---:|
| Wall seconds | 1,338.845 | 916.890 |
| Peak process RSS bytes | 2,011,222,016 | 2,018,181,120 |
| Peak workspace bytes | 5,386,701,602 | 5,386,701,602 |
| Retained artifact bytes | 3,117,911,731 | 3,117,911,731 |

The measured end-to-end improvement is about 1.46×, saving 421.955 seconds
(31.5% of elapsed time). The older serial-join/four-reader run took 2,780.008
seconds; neither comparison is a controlled one-CPU/eight-CPU benchmark.
The separate corpus verification is additional time, not included in this table.

All 264,085,606 source rows and 51,600,308 reference decisions are conserved.
All four artifact hashes, byte/row counts, reference states, exact source identity
and filter-candidate membership match the eight-report-worker baseline. The new
build has the expected distinct calculation identity:
`1a66232f2d0afb11a0b4e38af2d7caa23c9ea34db9c909f0698ac00dd7514fab`.
The separate corpus test passed in 358.75 seconds. It verified all four streams,
all 51,600,308 reference decisions and state totals, complete global incidence
totals, selected lookup multiplicities and 35 full-source witnesses across seven
observed states. This uses the same Parquet library, not an independent decoder
or a second independent full-cycle reference algorithm.

`compare.sh` also passes after requiring the accepted baseline JSON digest to
match its durable copy. Every field in the required evidence comparison is equal;
the result is `true`. This is full cross-build equivalence, not a second
same-build full-corpus replay. Earlier worker-layout fixture and baseline replay
gates remain separate evidence.

## Retained evidence

```text
/storage/dumps/audits/fec/receipt-narrow-reader/2026-09-12/attempt-01/
```

`preflight.sh` records the field comparison and old/new benchmarks. `tests.sh`
records full tests, race checks and vet. Both have zero completion markers.
`cycle.sh` records the complete command and starts corpus checks only after a
successful run. `cycle.exit`, `corpus-check.exit` and `cycle-and-corpus.exit`
are the explicit full-run gates and are all zero. `baseline-comparison.exit=0`
records the complete evidence comparison. `SHA256SUMS`, `retention-check.log` and
zero `retention-check.exit` verify every copied file, including source archive,
binaries, profile, metrics and complete outputs. The original working directory
remains unchanged at `/tmp/legal-tender-narrow-reader.GPdf47/`.

No source pointer, graph, financial calculation or Dagster asset changes here.
Return to contributor/conduit publication. Batch-overlap optimization remains a
measured follow-up, not a blocker for that next graph milestone.
