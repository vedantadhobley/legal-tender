# Committee-summary immutable publication gate — 2026-09-08

Scope: implemented [Go source publication](../design/committee-summary-source.md)
and an opt-in [release v4 contract](../../contracts/releases/fec/v4/).
This gate does not publish a real coordinated v4 release, alter existing source
facts, change Arango graphs, or calculate committee financial aggregates.

## Result

The full retained CSV corpus passes occurrence/fact artifact construction,
every-record raw readback, deterministic rebuild, and independent stored-value
comparison. Coordinated release discovery/planning/acquisition/staging/publication
and summary manifest publication pass with synthetic transport fixtures only.
Those are separate claims; real CSVs were not assigned synthetic release ancestry.

| Cycle | Stored occurrences/facts | Compressed bytes | Uncompressed bytes |
|---|---:|---:|---:|
| 2020 | 13,554 | 8,772,666 | 121,989,896 |
| 2022 | 13,977 | 8,939,815 | 125,755,789 |
| 2024 | 14,065 | 8,970,793 | 126,549,246 |
| 2026 | 14,152 | 8,888,263 | 127,272,641 |

The 55,748 rows occupy 35,571,537 compressed artifact bytes. This small family
uses the existing zstd JSONL writer; large A/B Parquet representations are
unchanged. The Go four-cycle build/readback/rebuild gate took 24.25 seconds with
local retained files under a 4 GiB container cap, 2 GiB Go soft memory limit,
and four CPUs. This is a cached integration-test measurement, not a cold
production refresh benchmark or measured peak-memory claim.

The independent Python gate decoded the actual zstd bytes, checked both hashes
and counts, and compared every raw field, money/date/identity value, issue,
byte offset, raw hash, occurrence ID, unkeyed record version, and fact ID against
the pinned CSV. It did not calculate a financial total or run domain ingestion.

## Preservation and safety checks

- All source rows survive, including invalid typed values and candidate-reference
  fan-out. Synthetic identical duplicate rows retain distinct fact identities.
- Exact pinned headers, quoted CSV framing, source-cycle checks, and raw hashes
  remain enforced by the existing Go reader.
- Every artifact receives raw/fact readback before a manifest can become visible.
  Compressed and uncompressed identities and row counts are verified at EOF.
- Malformed tails do not finalize row artifacts. Raw corruption, changed release
  pointers, missing release membership, wrong cycles, stored-artifact corruption,
  altered manifest evidence, cancellation, and failed storage preflight reject
  publication. No failing first publication writes a fact manifest.
- Atomic create-if-absent publication gives concurrent writers the same verified
  manifest. Replay retains the first run/time and leaves no mutable summary pointer.
- V4 retains all v3 source specifications. Only the four new whole CSV sources
  are changed in the unchanged-publisher migration fixture; all 23 prior sources
  are reused. Missing source observations cannot authorize acquisition.
- Existing Schedule E publication and B/E Dagster dispatch accept v4 without
  changing record or monetary policy. Unknown versions remain rejected.

The full Go suite, module-diff and vet gates, focused race tests, and independent
schema/orchestration/source-value tests pass. The final relevant Python regression
run has 91 passing tests and ten unrelated opt-in corpus tests skipped; 60 existing
Dagster warnings remain. Test commands and assertions live
in [Go publication tests](../../internal/source/fec/summarypublication/publish_test.go),
the [full-corpus artifact test](../../internal/source/fec/summarypublication/corpus_test.go),
[release tests](../../internal/source/fec/release/committee_summary_test.go), and the
[independent comparison](../../tests/test_committee_summary_publication.py).

## Retained evidence and active state

Inputs remain the immutable research captures identified in the
[source review](./committee-summary-source-2026-09-08.md). Artifact-gate outputs
are retained under
`dumps/audits/fec/committee-summary-publication/2026-09-08/` in project storage.
Each cycle has `artifact-gate.json`, explicitly labeled
`unpublished_artifact_gate_not_a_coordinated_release`, and its content-addressed
artifact. `fixture/` contains synthetic release/publication control examples,
not live release manifests.

The active release identity was read back after the tests and remains
`fec-136903645ee7c7050d463a4f779a051a454ebb87308fd854faabc5ca47a20cdf`,
inventory v3. This turn did not run network discovery, fetch large schedules,
change any release pointer, or write source facts into the live fact tree.

## Next gate

Observe every required source for a real v4 plan and inspect its acquisition
and storage cost before a large refresh. Reuse only unchanged publisher
versions. Publish all four summary fact sets from the exact accepted release;
then switch default discovery and add the thin Dagster summary asset. Financial
assertion grouping, scope-qualified comparison, report/account/time coverage,
and terminal allocation remain separate work in the [queue](../todo.md).
