# Storage Layout

> **Status:** The Go rewrite source-release, classic FEC evidence/facts, first
> real 2024 Schedule A evidence, direct calculation-probe path, and complete
> Schedule A Parquet fact set, compact occurrence/change layer, and compact
> candidate-receipt calculation are current. The immutable four-input fact
> bundle is also current. Historical release-inventory v2 and lossless selected-
> cycle Schedule E occurrence/fact publication, effective calculation, and
> immutable projection-readiness bundle plus isolated 2024 outside-spending
> graph projection are current. The receiver-flow v1 graph and additive
> identity calculation, v2 readiness bundle, and isolated v2 graph are also
> current. Retained release v3 owns the earlier processed Schedule B facts;
> active v4 adds refreshed source artifacts and four release-bound committee-summary
> fact sets. Earlier A/B/E facts and graphs keep their original ancestry. The old
> occurrence/change, full-row fact, and dense
> decision JSON layouts are retained corpus evidence but rejected for future
> publication. Legacy dump, cache, recovery, and production-retention sections
> still need consolidation.

Persistent state lives at `~/workspace/data/legal-tender/` on the host, bind-mounted as `/storage` inside containers. Subdirectory split:

- `raw/` — FEC zips, headers/, legislators/. Re-downloadable from FEC. Source of truth for input data.
- `builds/go/<date>/<executable-sha256>/` — retained [Go build evidence](./go-build.md):
  source/module archives, matching clean/recovery binaries, tests and checksums.
  No compiler cache, raw data or compiler-image export is included.
- `dumps/audits/fec/funding-recovery-inventory/<date>/<attempt>/` —
  [typed recovery inventories](./design/funding-recovery-inventory.md), exact
  inputs, fresh replay, per-file verification states and explicit exit markers.
  An incomplete inventory remains nonzero even when its replay matches.
- `dumps/audits/fec/funding-recovery-plan/<date>/<attempt>/` —
  [fact-start dependency plans](./design/funding-recovery-checkpoint.md), exact
  inputs, replay, recipes and explicit execution blockers. The embedded full
  provenance inventory retains its own completeness state; planning is not recovery.
- `dumps/audits/fec/funding-recovery-files/<date>/<attempt>/` —
  [selected file checks](./audit/funding-recovery-files-2026-09-15.md), exact
  execution/comparison hashes, resource admission, fresh replay and companion
  plan/budget-rejection evidence. Checksums do not enforce permanent retention.
- `dumps/audits/fec/funding-recovery-retention/<date>/<attempt>/` —
  [retention-boundary fixtures](./audit/funding-recovery-retention-2026-09-15.md),
  kernel read-only checks, explicit teardown evidence and real-file regression.
  The [unused input-copying implementation](./design/funding-recovery-retention.md)
  was removed on 2026-09-15; these historical audits and its source archive remain.
  No real-generation store or production capture mount was deployed.
- `dumps/audits/fec/release-stage-evidence-review/<date>/<attempt>/` —
  [separate stage-record reviews](./audit/release-stage-evidence-review-2026-09-15.md),
  exact metadata inputs and descriptor-comparison replay. These records never
  replace a release's missing original stage bytes or clear its inventory gap.
- `projections/arango/receipt-participants/cycle-v1/<projection-id>/` —
  [full-cycle participant](./design/arango-receipt-participant-cycle.md) progress
  checkpoints and final immutable completion manifest. Graph documents live in
  their isolated Arango database; a checkpoint is not a publication pointer.
- `facts/fec/committee-summary/v1/` — release-bound immutable manifests and
  content-addressed zstd JSONL occurrence/fact artifacts. All four v4 cycles
  pass readback and replay; no mutable summary pointer or financial grouping.
- `dumps/audits/fec/v4-publication/<date>/<attempt>/` — exact publication inputs,
  source and summary manifests/replays, independent readback, logs, and exit markers.
- `dumps/audits/fec/schedule-b-semantics/<date>/<cycle>/` — immutable complete
  reporting/identity profile, progress log, result digest, and completion
  marker. Source Parquet files stay under their existing content-addressed
  fact paths.
- `dumps/audits/fec/schedule-b-reporting/<date>/<cycle>/` — manual deterministic
  reporting-calculation artifact, reviewed source examples, independent
  validation result, run/replay logs, digests, and completion marker. This is
  retained gate evidence, not an active calculation publication pointer.
- `audits/fec/committee-master-history/raw/cmYY.zip` — official historical
  cycle committee-master archives used as immutable forensic inputs. These
  files are not coordinated-release authority and never replace a cycle's
  selected committee-master fact set.
- `dumps/` — ArangoDB JSONL dumps for fast reload. Subdirs: `fec/{cycle}/`, `enriched/{cycle}/`, `aggregation/`, `graphs/`. Regeneratable from `raw/` but skip the parse cost.
- `cache/` — Regeneratable API caches: `congress_api/`, `wikidata/`, etc. Should NOT be in repo (currently `wikidata_cache.json` and `corporate_families.json` are at repo root — Phase 3 fix).
- `control/fec/release/discoveries/` — exact schema-validated discovery JSON
  emitted by Go and stored by SHA-256 for Dagster handoff.
- `control/fec/release/plans/` — exact schema-validated release-plan JSON
  emitted by Go and stored by SHA-256 for Dagster handoff.
- `control/fec/release/acquisitions/` — exact schema-validated acquisition
  results stored by SHA-256. Contract-valid blocked and failed results are
  retained even though their Dagster step fails.
- `control/fec/release/stages/` — exact schema-validated staged-release results
  stored by SHA-256. Contract-valid blocked and failed results are retained.
- `control/fec/release/manifests/` — exact schema-validated published manifests
  captured from Go for Dagster lineage.
- `control/fec/release/occurrences/<cycle>/` — exact schema-validated
  Schedule A occurrence-set manifests captured from Go for Dagster lineage.
- `control/fec/release/occurrences/schedule-e/<cycle>/` — exact
  schema-validated Schedule E occurrence-set manifests captured from Go.
- `control/fec/release/occurrences/classic/<dataset>/<cycle>/` — exact classic
  occurrence-set manifests captured from Go for Dagster lineage.
- `control/fec/release/facts/classic/<dataset>/<cycle>/` — exact classic
  normalized-fact manifests captured from Go for Dagster lineage.
- `control/fec/release/facts/schedule-a/<cycle>/` — exact Schedule A fact-set
  manifests captured from Go for Dagster lineage.
- `control/fec/release/facts/schedule-b/<cycle>/` — exact Schedule B columnar
  fact-set manifests captured from Go for Dagster lineage.
- `control/fec/release/facts/schedule-e/<cycle>/` — exact Schedule E fact-set
  manifests captured from Go for Dagster lineage.
- `control/fec/release/bundles/candidate-itemized-individual-receipts/<cycle>/`
  — exact schema-validated readiness bundles captured from Go for Dagster
  lineage.
- `control/fec/release/bundles/independent-expenditure-projection/<cycle>/` —
  exact schema-validated projection-readiness bundles captured from Go for
  Dagster lineage.
- `control/fec/release/calculations/candidate-itemized-individual-receipts/<cycle>/`
  — exact schema-validated calculation manifests captured from Go for Dagster
  lineage.
- `control/dagster-io/` — shared Dagster transport values for rewrite asset
  dependencies. Domain evidence does not live here.
- `raw/fec/staging/<candidate-release-id>/` — resumable partials for changed
  non-Schedule-A sources. A retry for the same candidate resumes these files.
- `raw/fec/artifacts/sha256/<prefix>/<sha256>` — immutable captured source
  bodies other than Schedule A.
- `raw/fec/schedule-a/staging/<candidate-release-id>/` — resumable processed
  Schedule A dump partials inside the storage-budgeted hot lane.
- `raw/fec/schedule-a/artifacts/sha256/<prefix>/<sha256>` — immutable processed
  Schedule A dump bodies inside the hot lane.
- `raw/fec/schedule-b/snapshots/<publisher-snapshot>/` — retained pre-v3 audit
  snapshot, checksum, object metadata, and exact classic comparison artifacts.
  It is evidence for the source gates, not the active release pointer.
- `raw/fec/schedule-b/staging/<candidate-release-id>/` — resumable processed
  Schedule B archive partials for changed v3 sources.
- `raw/fec/schedule-b/artifacts/sha256/<prefix>/<sha256>` — immutable processed
  Schedule B archive bodies owned by coordinated release v3 and later.
- `raw/fec/acquisitions/<candidate-release-id>/<run-id>.json` — successful
  acquisition state. Retrying the same plan and run returns this state without
  publisher requests.
- `raw/fec/selected/sha256/<prefix>/<sha256>.zst` — immutable zstd
  streams for the 20 exact selected classic ZIP members.
- `raw/fec/schedule-a/extracts/sha256/<prefix>/<sha256>.copy.zst` — immutable
  data-row-only COPY streams for the four exact selected relations.
- `raw/fec/schedule-e/artifacts/sha256/<prefix>/<sha256>` — immutable
  all-history processed Schedule E dump bodies.
- `raw/fec/schedule-e/extracts/sha256/<prefix>/<sha256>.copy.zst` — immutable
  data-row-only COPY stream for `disclosure.fec_fitem_sched_e`. This preserves
  direct custom-dump row order; restored-table heap order is not its identity.
- `dumps/audits/fec/schedule-b/<publisher-snapshot>/<cycle>/` — canonical
  Schedule B source/classic comparison result, digest, completion marker, and
  diagnostic log. The selected COPY stream and uniqueness shards are not
  retained.
- `dumps/audits/fec/schedule-ab/<publisher-snapshot>/<cycle>/<run>/` — immutable
  same-publisher-batch Schedule A/B candidate-alignment result, digest,
  completion marker, and optional progress log. It is diagnostic evidence, not
  a fact set or reconciliation publication.
- `raw/fec/stages/<candidate-release-id>/<run-id>.checkpoint.json` — per-output
  staging checkpoint. A retry validates and reuses each completed stream.
- `raw/fec/stages/<candidate-release-id>/<run-id>.json` — successful staged
  result. A same-run retry performs no extraction.
- `releases/fec/manifests/<release-id>.json` — immutable published source-
  release manifest.
- `releases/fec/current.json` — atomically replaced active source-release
  pointer and planner baseline. Acquisition and staging cannot replace it.
- `evidence/fec/schedule-a/occurrences/sha256/<prefix>/<sha256>.jsonl.zst` —
  immutable physical-row occurrences with exact raw locators.
- `evidence/fec/schedule-a/issues/sha256/<prefix>/<sha256>.jsonl.zst` —
  immutable parse, partition, identifier, and duplicate-key issues.
- `evidence/fec/schedule-a/natural-index/sha256/<prefix>/<sha256>.jsonl.zst` —
  immutable globally sorted `SUB_ID` state for one cycle and source snapshot.
- `evidence/fec/schedule-a/changes/sha256/<prefix>/<sha256>.jsonl.zst` —
  immutable added, changed, absent, and invalid semantic transitions.
- `evidence/fec/schedule-a/manifests/<occurrence-set-id>.json` — immutable
  occurrence-set publication manifest.
- `evidence/fec/schedule-a/current/<cycle>.json` — atomically replaced active
  occurrence-set pointer for one cycle. Source-release publication cannot
  replace it.
- `evidence/fec/schedule-a/staging/<cycle>/<run-id>/` — bounded hash shards and
  temporary artifacts removed after successful occurrence publication.
- `evidence/fec/schedule-a/compact/key-index/sha256/<prefix>/<sha256>.bin.zst`
  — immutable fixed-width partitioned `SUB_ID`, row-ordinal, and semantic-
  digest indexes.
- `evidence/fec/schedule-a/compact/{key-exceptions,row-exceptions,deltas}/sha256/<prefix>/<sha256>.jsonl.zst`
  — sparse invalid/duplicate evidence and actual inter-release transitions.
- `evidence/fec/schedule-a/compact/manifests/<occurrence-set-id>.json` —
  immutable dense-occurrence and compact-index publication manifest.
- `evidence/fec/schedule-a/compact/current/<cycle>.json` — atomically replaced
  active compact occurrence pointer.
- `evidence/fec/schedule-a/compact/staging/<occurrence-set-id>/` — temporary
  fixed-width partition staging removed after successful publication.
- `evidence/fec/classic/<dataset>/{occurrences,issues,natural-index,changes}/sha256/<prefix>/<sha256>.jsonl.zst`
  — immutable row evidence and semantic changes for `candidate-master`,
  `committee-master`, `candidate-committee-linkage`,
  `all-candidates-summary`, or `current-campaigns-summary`.
- `evidence/fec/classic/<dataset>/manifests/<occurrence-set-id>.json` —
  immutable classic occurrence publication.
- `evidence/fec/classic/<dataset>/current/<cycle>.json` — atomically replaced
  classic occurrence pointer for one dataset and cycle.
- `facts/fec/classic/<dataset>/facts/sha256/<prefix>/<sha256>.jsonl.zst` —
  immutable normalized source facts for unique valid publisher keys.
- `facts/fec/classic/<dataset>/manifests/<fact-set-id>.json` — immutable fact
  publication manifest.
- `facts/fec/classic/<dataset>/current/<cycle>.json` — atomically replaced
  fact-set pointer for one dataset and cycle.
- `facts/fec/schedule-a/columnar/shards/sha256/<prefix>/<sha256>.parquet` —
  immutable one-million-source-row receipt-fact shards. Each file preserves
  all 81 decoded source fields, exact source locators, and the narrow typed
  projection defined by `legal-tender.fec.schedule-a-parquet.v1`.
- `facts/fec/schedule-a/columnar/manifests/<fact-set-id>.json` — immutable
  columnar fact-set publication with exact release and occurrence ancestry,
  shard ranges and digests, conservation checks, and source replay digests.
- `facts/fec/schedule-a/columnar/current/<cycle>.json` — atomically replaced
  active columnar fact-set pointer for one cycle.
- `facts/fec/schedule-a/columnar/staging/<fact-set-id>/checkpoint.json` —
  resumable shard checkpoint. A retry replays the source to reprove semantic
  ranges but reuses unchanged, digest-verified completed shards.
- `facts/fec/schedule-b/columnar/shards/sha256/<prefix>/<sha256>.parquet` —
  immutable one-million-source-row disbursement-fact shards. Each preserves all
  81 decoded publisher lexemes, exact archive-relation row locators, and the
  policy-free typed projection defined by
  `legal-tender.fec.schedule-b-parquet.v1`.
- `facts/fec/schedule-b/columnar/manifests/<fact-set-id>.json` and
  `facts/fec/schedule-b/columnar/current/<cycle>.json` — immutable selected-
  cycle publication manifests and atomically replaced active pointers. Fact
  identity depends on the Schedule B artifact and relation, so unrelated
  coordinated-release changes reuse the same verified manifest.
- `facts/fec/schedule-b/columnar/staging/<fact-set-id>/checkpoint.json` —
  resumable shard checkpoint. A retry streams the release-owned relation again
  to prove semantic ranges while reusing verified completed shards.
- `facts/fec/schedule-a/facts/sha256/<prefix>/<sha256>.jsonl.zst` — immutable
  legacy fixture-scale lossless receipt facts for unique source-valid `SUB_ID`
  values. This full-row JSON representation is rejected for complete cycles.
- `facts/fec/schedule-a/manifests/<fact-set-id>.json` — immutable Schedule A
  fact publication manifest.
- `facts/fec/schedule-a/current/<cycle>.json` — atomically replaced fact-set
  pointer for one cycle. No real-cycle pointer exists; the current JSON fact
  representation failed its corpus cost gate and must not be published.
- `evidence/fec/schedule-e/occurrences/sha256/<prefix>/<sha256>.jsonl.zst` —
  immutable selected-cycle physical Schedule E occurrences with all-history
  row ordinals and byte coordinates.
- `evidence/fec/schedule-e/manifests/<occurrence-set-id>.json` and
  `evidence/fec/schedule-e/current/<cycle>.json` — immutable manifests and
  atomic cycle pointers.
- `facts/fec/schedule-e/facts/sha256/<prefix>/<sha256>.jsonl.zst` — immutable
  lossless 80-field Schedule E facts plus typed money and filing context.
- `facts/fec/schedule-e/manifests/<fact-set-id>.json` and
  `facts/fec/schedule-e/current/<cycle>.json` — immutable manifests and atomic
  cycle pointers. These facts do not encode effective-record policy.
- `calculations/fec/effective-independent-expenditures/exceptions/sha256/<prefix>/<sha256>.jsonl.zst`
  — sparse unresolved-amount or included-unattributed Schedule E evidence.
- `calculations/fec/effective-independent-expenditures/results/sha256/<prefix>/<sha256>.jsonl.zst`
  — immutable exact signed totals grouped by spender committee, candidate,
  support/oppose, and cycle.
- `calculations/fec/effective-independent-expenditures/manifests/<calculation-set-id>.json`
  and `calculations/fec/effective-independent-expenditures/current/<cycle>.json`
  — immutable lineage, predicate, conservation, and artifact manifest plus the
  atomically replaced active cycle pointer.
- `calculations/fec/independent-expenditure-candidate-resolution/decisions/sha256/<prefix>/<sha256>.jsonl.zst`
  — one immutable dense identity decision per effective Schedule E fact. Each
  decision preserves its resolution state, reported candidate identifier,
  resolved candidate identifier when projectable, method, and signed amount.
- `calculations/fec/independent-expenditure-candidate-resolution/manifests/<calculation-set-id>.json`
  and `calculations/fec/independent-expenditure-candidate-resolution/current/<cycle>.json`
  — immutable resolution lineage and conservation manifest plus the atomic
  active-cycle pointer.
- `calculations/fec/resolved-independent-expenditures/results/sha256/<prefix>/<sha256>.jsonl.zst`
  — immutable projectable totals grouped by spender committee, resolved
  candidate, stance, and cycle, with resolution-state counts and amounts.
- `calculations/fec/resolved-independent-expenditures/exceptions/sha256/<prefix>/<sha256>.jsonl.zst`
  — sparse ambiguous or unresolved identity decisions with their exact signed
  amount and original evidence references intact.
- `calculations/fec/resolved-independent-expenditures/manifests/<calculation-set-id>.json`
  and `calculations/fec/resolved-independent-expenditures/current/<cycle>.json`
  — immutable result/exception membership, identity ancestry, and exact-cent
  conservation manifest plus the atomic active-cycle pointer.
- `bundles/fec/independent-expenditure-projection/manifests/<bundle-id>.json`
  and `bundles/fec/independent-expenditure-projection/current/<cycle>.json` —
  historical reported-ID projection bundle. It selects the effective
  calculation plus candidate and committee master facts without copying them.
- `bundles/fec/resolved-independent-expenditure-projection/manifests/<bundle-id>.json`
  and `bundles/fec/resolved-independent-expenditure-projection/current/<cycle>.json`
  — active graph-readiness bundle. It pins the resolved aggregate, exact dense
  candidate-resolution ancestry, and candidate and committee master facts from
  one cycle and coordinated release. The bundle contains no copied result,
  exception, decision, or fact.
- ArangoDB `lt_ie_probe_<cycle>_<projection-prefix>` databases — rebuildable,
  content-addressed historical reported-ID outside-spending projections.
- ArangoDB `lt_ie_probe_resolved_<cycle>_<projection-prefix>` databases —
  rebuildable resolved-candidate v2 projections. They contain only referenced
  candidate/committee vertices, support/opposition edges with identity-quality
  coverage, and a last-written completion document. Immutable filesystem
  artifacts remain the evidence authority for both graph generations.
- `calculations/fec/receiver-flow-committee-identity-coverage/decisions/sha256/<prefix>/<sha256>.jsonl.zst`
  — one immutable exact-ID evidence decision per receiver-flow endpoint absent
  from the selected cycle committee master. Historical registration assertions
  remain at source grain; unresolved reported IDs have no invented master.
- `calculations/fec/receiver-flow-committee-identity-coverage/manifests/<calculation-set-id>.json`
  and `calculations/fec/receiver-flow-committee-identity-coverage/current/<cycle>.json`
  — immutable flow and registration-evidence lineage plus the active cycle
  pointer.
- `bundles/fec/receiver-reported-committee-flow-projection/manifests/<bundle-id>.json`
  and `bundles/fec/receiver-reported-committee-flow-projection/current/<cycle>.json`
  — shipped v1 flow calculation and selected committee-master readiness
  bundle.
- `bundles/fec/receiver-reported-committee-flow-projection/v2/manifests/<bundle-id>.json`
  and `bundles/fec/receiver-reported-committee-flow-projection/v2/current/<cycle>.json`
  — additive v2 bundle that pins one immutable v1 bundle and exact identity-
  coverage calculation.
- ArangoDB `lt_flow_probe_<cycle>_<projection-prefix>` databases — rebuildable
  v1 receiver-flow projections with generic missing-master placeholders.
- ArangoDB `lt_flow_probe_v2_<cycle>_<projection-prefix>` databases —
  rebuildable identity-aware receiver-flow projections with distinct current,
  historical, alternate-release, and unresolved vertex states. Filesystem
  calculations and bundles remain the evidence and replay authority.
- `bundles/fec/candidate-itemized-individual-receipts/manifests/<bundle-id>.json`
  — immutable same-cycle and same-release selection of the exact Schedule A,
  linkage, and two summary fact manifests required by the compact calculation.
- `bundles/fec/candidate-itemized-individual-receipts/current/<cycle>.json` —
  atomically replaced active readiness pointer. It contains no copied facts and
  advances only after every selected manifest and backing artifact verifies.
- `calculations/fec/candidate-itemized-individual-receipts/decisions/sha256/<prefix>/<sha256>.jsonl.zst`
  — one immutable calculation decision per Schedule A fact.
- `calculations/fec/candidate-itemized-individual-receipts/results/sha256/<prefix>/<sha256>.jsonl.zst`
  — immutable candidate components, committee scope and subtotals, preserved
  summary assertions, and reconciliation gaps.
- `calculations/fec/candidate-itemized-individual-receipts/manifests/<calculation-set-id>.json`
  — immutable exact-input and dual-artifact calculation manifest.
- `calculations/fec/candidate-itemized-individual-receipts/current/<cycle>.json`
  — atomically replaced active calculation pointer for one cycle. The
  per-receipt JSON decision form is paused with the rejected Schedule A fact
  representation.
- `calculations/fec/candidate-itemized-individual-receipts/compact/exceptions/sha256/<prefix>/<sha256>.jsonl.zst`
  — sparse unresolved or invalid membership evidence for the accepted compact
  calculation.
- `calculations/fec/candidate-itemized-individual-receipts/compact/results/sha256/<prefix>/<sha256>.jsonl.zst`
  — immutable candidate results using the accepted result schema.
- `calculations/fec/candidate-itemized-individual-receipts/compact/manifests/<calculation-set-id>.json`
  — immutable predicate, exact-input, artifact, count, reconciliation, and
  conservation manifest.
- `calculations/fec/candidate-itemized-individual-receipts/compact/current/<cycle>.json`
  — atomically replaced active compact calculation pointer.
- `probes/fec/candidate-itemized-individual-receipts/results/sha256/<prefix>/<sha256>.jsonl.zst`
  — compact candidate results from a manual direct staged-source probe.
- `probes/fec/candidate-itemized-individual-receipts/manifests/<probe-id>.json`
  — immutable non-production probe input, measurement, conservation, and
  result descriptor. Probes create no active pointer.
- `probes/fec-schedule-a-layout-<cycle>-<sample>/` — manually named,
  create-only physical-layout probe containing an equal-row zstd baseline,
  ordinal-range Parquet shards, and `benchmark.json`. These are measured
  evidence, not facts or active publications. The 2024 one-million-row v1/v2
  and ten-million-row v1 directories are retained pending the complete-corpus
  probe.

Env vars (set in compose):
- `LEGAL_TENDER_STORAGE` → `/storage`
- `LEGAL_TENDER_RAW_DIR` → `/storage/raw`
- `LEGAL_TENDER_DUMPS_DIR` → `/storage/dumps`
- `LEGAL_TENDER_CACHE_DIR` → `/storage/cache`
- `LEGAL_TENDER_CONTROL_ARTIFACTS` →
  `/storage/control/fec/release` by default
- `LEGAL_TENDER_DAGSTER_IO` → `/storage/control/dagster-io` by default
- `LEGAL_TENDER_FEC_CURRENT_RELEASE_MANIFEST` →
  `/storage/releases/fec/current.json` by default
- `LEGAL_TENDER_FEC_ACQUISITION_TIMEOUT_SECONDS` → `86400` by default
- `LEGAL_TENDER_FEC_STAGING_TIMEOUT_SECONDS` → `86400` by default
- `LEGAL_TENDER_FEC_PUBLICATION_TIMEOUT_SECONDS` → `86400` by default; final
  backing-object validation can replay the complete reused Schedule A set.
- `LEGAL_TENDER_FEC_OCCURRENCE_TIMEOUT_SECONDS` → `86400` by default

The acquisition preflight counts every regular file already under
`raw/fec/schedule-a/`, including retained immutable dumps and partials. For a
changed Schedule A source it projects:

```text
current Schedule A hot bytes
+ remaining Schedule A body bytes
+ 206,363,392,958 bytes for the largest selected extract
+ 25 GiB working margin
```

The command blocks before any `GET` if that projection exceeds 600 GiB or all
remaining downloads plus the largest extraction workspace and margin would
leave less than 500 GiB free. Other acquisitions retain the 25 GiB working
margin. It does not delete evidence to pass the gate.

Staging repeats the hot-cap and free-floor gate before extraction, before each
new selected output, and after all outputs are measured. The working gate
reserves the largest accepted extraction plus 25 GiB while leaving 500 GiB
free. The final gate requires actual Schedule A hot bytes at or below 600 GiB
and at least 500 GiB free. Reused published outputs and valid checkpoints avoid
re-reading the source dump through `pg_restore`.

Occurrence publication performs a separate preflight before opening the
selected relation. By default it reserves the relation's recorded
uncompressed byte count plus 25 GiB and requires at least 500 GiB free. It
rechecks the free-space floor before replacing the per-cycle pointer. The
publisher does not delete prior evidence or weaken the floor to force a run.
The legacy Schedule A occurrence publisher is paused after the 2024 corpus
expanded one 14.77 GB staged relation into 113.45 GB of JSON evidence. The
accepted compact publisher reserves 65 staging bytes plus 48 raw index bytes
per recorded source row, adds the 25 GiB working margin, and requires the
500 GiB free-space floor. Its complete 2024 output used 10,276,745,806 bytes,
including both manifests, and its temporary partition staging was removed.

Columnar Schedule A fact publication performs its own preflight. A new run
projects 1.5 times the selected relation's compressed size plus the working
margin and requires the 500 GiB free-space floor. A resumed run subtracts
already completed immutable shard bytes from the projection. Each shard is
written to a temporary file, reread through Parquet, checked against the exact
99-column schema and semantic digest, then moved into content-addressed
storage before its checkpoint is replaced. The manifest and cycle pointer are
written only after the complete source stream matches the release's row,
byte, and digest evidence and every fact is conserved.

The accepted 2024 fact set contains 264,085,606 rows in 265 Parquet files and
uses 16,759,429,988 bytes. The immutable source relation uses 14,765,199,882
compressed bytes. See the
[complete publication audit](./audit/schedule-a-columnar-publication-2026-08-31.md).

Helpers in `src/utils/storage.py`: `get_raw_dir()`, `get_dumps_dir()`, `get_cache_dir()`, `get_cycle_raw_dir(cycle)`, `get_fec_dumps_dir(cycle)`, `get_enriched_dumps_dir(cycle)`, `get_aggregation_dumps_dir()`, `get_graph_dumps_dir()`.

Phase 3 should fill in:
- The legacy Arango dump format and its relation to the new evidence artifacts
- The dump-vs-reparse decision logic in `arango_dump.py`
- The Arango runtime memory tuning (block cache, write buffers — see commit `fb34a44`)
- The bind-mount semantics for dev vs prod (same source, separate runtime named volumes)
