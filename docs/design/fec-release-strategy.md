# Coordinated FEC bulk-release strategy

> **Status:** Accepted acquisition and publication invariants. The initial
> production cadence is weekly on Monday morning. The historical 21-source v1
> 22-source v2, and 23-source v3 inventories, plus active 27-source v4 inventory,
> release schemas, metadata discovery, pure release planner, resumable body
> acquisition, selected-data staging, release checks, and atomic source-
> release publication are implemented. Immutable Schedule A occurrence,
> issue, natural-key, and change publication is also implemented per selected
> cycle. All five classic products publish occurrence evidence and lossless
> typed facts for 2024; candidate- and committee-master facts also publish for
> 2020, 2022, and 2026. The first coordinated 2024 release, columnar Schedule A
> fact set, coherent receipt fact bundle, and bundle-fed compact calculation
> are published. V4 and all four release-bound committee-summary fact sets now
> pass real publication/readback/replay. The manual summary Dagster handoff also
> passes four-cycle replay. Default discovery migration remains separate;
> weekly acquisition is not enabled.

## Decision

Legal Tender uses one authoritative acquisition path per FEC fact family. An
official bulk product is canonical when it supplies the required complete
dataset. APIs and overlapping bulk products may provide fixtures, diagnostics,
or comparison evidence, but they never patch or silently supplement the
canonical source.

The initial production FEC path uses bulk artifacts only. It publishes a
coordinated, immutable release over the rolling four-cycle view. The first
release cadence is every Monday at 04:00 `America/New_York`.

This is a FEC decision, not a universal ban on APIs. A non-FEC product without
an adequate official bulk source must either make its API the explicitly
accepted sole source for that fact family or remain out of scope. It cannot
enter through a hidden fallback.

## Source authority

One publisher can expose several fact families. “One source” applies to each
fact family, not to the FEC as a whole.

| Fact family | Canonical production source | Other representation |
|---|---|---|
| Itemized receipts | Processed Schedule A dump | Classic `indiv` and `oth`, processed API, and raw filings are comparison or deferred evidence only. |
| Candidates | Candidate-master cycle bulk file | API lookup is diagnostic only. |
| Committees | Committee-master cycle bulk file | API lookup is diagnostic only. |
| Candidate-committee linkage | Linkage cycle bulk file | No silent inference from names or API results. |
| Candidate summaries | Each accepted summary bulk product as its own publisher assertion | One summary never fills or replaces another. |
| Itemized disbursements | Processed Schedule B dump | Classic overlap products remain comparison evidence. |
| Independent expenditures | Processed Schedule E dump | Classic `pas2` and 24/48-hour files remain separate evidence; the effective-record calculation is independently gated. |
| Committee history | Processed committee-history dump after its source audit | Current committee masters remain cycle-specific assertions. |

Source authority and contract maturity are separate claims. Processed Schedule
A is the selected authority for the implemented receipt slice, but its source
contract remains `draft` until the broader historic memo/conduit coverage gate
passes. Schedule E passed its complete-corpus source and parser gates on
2026-08-31 and its source contract is `accepted`. Release-inventory v2,
selected-cycle occurrence and fact publication, effective calculation,
candidate resolution, resolved grouping, readiness bundles, and ready v2 graph
gates are implemented for all four cycles. Schedule B passed its physical,
classic-comparison, same-publisher-batch Schedule A alignment, release-v3, and
complete 2024 columnar publication/replay gates; its source contract is now
`accepted`. Committee history remains unselected pending its own corpus audit.

### Versioned release membership

The replayable v1 release contains exactly:

- one processed Schedule A dump, with the four selected two-year relations;
- `cn`, `cm`, `ccl`, `weball`, and `webl` for each selected cycle; and
- the independent source and coverage watermarks for all 21 artifacts.

Historical v2 retains those members and adds one processed Schedule E dump with
the single all-history `disclosure.fec_fitem_sched_e` relation selected. It has
22 artifacts and 25 staged outputs. Historical v3 adds the processed Schedule B
dump and selects its four two-year relations as `archive_direct`. It has 23
artifacts and still has 25 staged outputs: Schedule B fact publishers stream
one selected relation directly from the immutable archive instead of retaining
a second COPY extract that is about 115 GiB for 2024. Classic comparison files,
OpenFEC API observations, raw filings, and committee history are not optional
fallbacks. A promoted fact family becomes required only through another
versioned contract change.

The active source release's [v4 inventory](../../contracts/releases/fec/v4/) adds
the four committee-summary CSVs as whole artifacts. It has 27 artifacts and the
same 25 staged outputs. Strict CSV verification runs during acquisition; the
separate summary publisher creates release-bound immutable occurrences/facts.
The [real v4 gate](../audit/fec-v4-publication-2026-09-10.md) now passes
coordinated source publication, all summary publications, complete independent
raw/fact comparison, and replay. Source activation is complete; default discovery
still selects v3. The manual summary asset is wired without an automatic trigger;
discovery migration remains a separate operational gate. Research CSVs
cannot be appended to an old release under a same-release claim. Existing A/B/E
facts and graphs retain their original release ancestry until their own refresh
gates pass. Cold retention still blocks perpetual scheduled acquisition.

The exact 2020, 2022, 2024, and 2026 membership is immutable replay identity
for v1 through v4. Advancing the rolling hot view never edits those inventories.
It publishes a new inventory version with the new ordered periods, exact
Schedule A relations and classic members, and an explicit compatibility plan
for reusable artifacts. Dagster derives dynamic cycle partitions from the
published release manifest; it does not own a second active-cycle list.

## Monday schedule

All schedule times use `America/New_York` so daylight-saving changes do not
silently move the operational window.

### Discovery

Every Monday at 04:00, a lightweight operation observes metadata for every
required bulk object. It records publisher URL, final URL, version ID when
available, last-modified time, ETag, checksum metadata, content length, and
the per-source observation time inside the discovery start/completion window.
Discovery never downloads a large body and never advances a published release.

### Acquisition

Acquisition planning follows every Monday discovery. A release is
version-driven, not date-driven:

1. Select the newest stable observed version of every required source.
2. Require a source version newer than the version in the last accepted
   release when that publisher product changed.
3. If a weekend publication is late or unstable, wait and retry. Do not label
   the prior version as a new weekly release.
4. Freeze the selected versions in a candidate release manifest before
   downloading bodies.
5. Recheck source metadata after acquisition. A source that changed during
   capture invalidates or supersedes the candidate according to its source
   contract; it never produces a falsely atomic release.

An unchanged source version is reused without downloading its body. Every
required source is still observed and represented in the weekly release.

## Release manifest

One `fec.release.v1` manifest will identify a candidate and accepted FEC
release. It records at least:

- release ID, state, discovery window, scheduled cadence, and run ID;
- one exact observation and selected artifact version per required fact
  family;
- sources reused because their accepted content identity did not change;
- artifact byte count, SHA-256, container identity, and source-native
  partition or relation;
- independent publisher and coverage watermarks for every source;
- rolling cycle set and the exact selected relations or archive members;
- source-contract, parser, calculation, and code versions;
- staged outputs, checks, change-set references, and publication time; and
- prior accepted release ID.

The manifest does not claim that independently published FEC products share
one publisher transaction or timestamp. It makes their exact observed versions
reproducible as one Legal Tender release.

## Publication invariants

Every candidate release moves through:

```text
observed -> selected -> acquired -> staged -> checked -> published
                                      |
                                      +-> failed
```

- Every acquired artifact is immutable and content-addressed.
- Every source row becomes a preserved occurrence or explicit issue before
  semantic filtering.
- All required blocking checks pass before publication.
- Source publication atomically changes only the coordinated source baseline
  after source checks. Occurrence, fact, graph, calculation, and eventual
  user-facing product pointers advance separately only after their own
  required writes and checks commit.
- A missing, late, changed-during-capture, corrupt, or semantically invalid
  source fails the candidate release.
- Failure leaves the prior complete release readable.
- No API call, older overlapping file, summary value, or empty dataset patches
  a failed required source.
- Unchanged source bytes cause no parse or domain rewrite unless a versioned
  parser or calculation migration explicitly requests one.

## Rolling four-cycle view

The newest four two-year FEC transaction periods define the default hot
investigative view. The period set is selected in the release manifest rather
than hardcoded into facts.

Advancing the view does not erase source evidence. Transaction, receipt,
report, election, processing, source-observation, and Legal Tender publication
times remain independent. A source row does not become a different fact when
the default four-cycle window advances.

## Full source refresh and targeted recomputation

A coordinated refresh selects the latest accepted version of every required
fact family. It may need to download and scan a complete monolithic publisher
artifact. That cost does not authorize a complete graph rebuild.

Go compares natural source keys and semantic row digests with the prior
release. It emits added, changed, newly absent, invalid, and unchanged counts
plus immutable affected-key sets. Only affected periods, filings, committees,
candidates, relationships, and calculation outputs propagate. Dagster maps
those versioned results; Python does not recalculate them.

## Freshness guarantee

The weekly product guarantees the FEC's current processed view at the exact
accepted source versions. It does not guarantee capture of every intermediate
weekly processed state or complete as-filed amendment history.

Every API and UI response exposes the release ID plus applicable source and
coverage watermarks. There is no source-agnostic `updated_at` claim. Weekly
processed snapshots still do not constitute complete amendment history.

## Retention

- Keep the current and prior accepted release artifacts hot for promotion,
  rollback, and comparison.
- Keep manifests, digests, schemas, change sets, calculation versions, and
  publication records indefinitely.
- Move older accepted source artifacts only to verified content-addressed cold
  storage under an explicit retention policy.
- Never delete the only bytes needed to reproduce a published result.
- Block acquisition before storage budgets or free-space floors are breached;
  do not evict evidence to force a refresh through.

The exact cold-storage backend can be chosen later. Perpetual scheduled
acquisition cannot begin without a verified retention destination or a
deliberately narrower reproducibility decision.

## Deferred source representations

The checked-in processed API, electronic-filing, effective-report, and
processed/raw reconciliation contracts preserve completed research. They are
not dependencies of the initial production release.

A future as-filed product can promote raw `.fec` documents as a separate fact
family through a new accepted decision. It must retain separate facts,
watermarks, calculations, and UI labels and cannot overwrite the processed
Schedule A source.

## Still configurable

These choices do not need to be fixed before the first release implementation:

- retry interval after a late weekend publication;
- cold-storage backend; and
- whether a separately labeled as-filed product is later worth its API and
  document-acquisition complexity.

The Dagster schedule and sensor are now fixed for this contract. The Monday
job materializes discovery and planning only. An asset sensor starts the
separate acquisition job only from an `update_available` materialization.
`no_change`, `source_not_ready`, and `invalid` never start acquisition.

## Implemented source-release boundary

The [streaming storage contract](./fec-streaming-storage.md) now enforces actual
acquisition/staging growth and provides the read-only `review-release-storage`
v2 diagnostic. It counts unique retained inodes and derives cumulative output
and temporary peaks from inventory/prior inputs. Cap, floor, and margin stay
unchanged; the unmaterialized uncompressed-extract reserve is removed. The
[saved-plan gate](../audit/fec-streaming-storage-2026-09-09.md) fits. A fitting
scenario is not a bound on unseen output or permission to acquire. The diagnostic
remains manual; enforcement is in the Go commands already invoked by Dagster.

The implemented release boundary now provides:

1. The language-neutral release contracts under
   `contracts/releases/fec/v1/`, `v2/`, and `v3/`, including manifest,
   discovery, plan, acquisition, and stage schemas plus fixtures for
   `update_available`, `no_change`, `source_not_ready`, and invalid candidates.
2. Equivalent Go types and validators bound to every release-plan fixture and
   to the exact checked-in inventory.
3. `legal-tender pipeline fec discover`, which performs `HEAD` observations
   for every artifact in the selected inventory without reading response
   bodies.
4. `legal-tender pipeline fec plan-release`, which is a pure selection step
   over saved observations and an optional prior published manifest.
5. `legal-tender pipeline fec acquire`, which accepts only an exact saved
   `update_available` plan, hashes those exact plan bytes, and reuses validated
   unchanged artifacts from the prior published manifest.
6. Pre-request gates using the accepted 600 GiB Schedule A hot cap, 500 GiB
   filesystem-free floor, 25 GiB margin, current unique hot bytes, remaining
   downloads, and a full prior-size output scenario. Shared acquisition/staging
   exclusion and write-time budgets enforce actual compressed growth. There is
   no default full-uncompressed-extract reserve.
7. Candidate-specific partial files with exact HTTP range validation and
   conditional resume. A server that ignores a range request restarts that
   staging file safely; short bodies remain resumable evidence.
8. A second complete selected-inventory `HEAD` discovery after capture. Any unavailable,
   changed-version, or changed-length source fails before ZIP validation or
   `pg_restore --list`.
9. Full CRC reads of selected ZIP members, `PGDMP` plus selected-relation TOC
   validation for Schedule A, Schedule B, and Schedule E, and finalization
   into immutable SHA-256 paths.
   Candidate partials remain resumable until successful acquisition state is
   durable. The acquired result does not advance the published pointer.
10. A separate Dagster acquisition asset with an `update_available` sensor,
    three bounded exponential retries, a 24-hour process timeout, and preserved
    contract-valid failure results.
11. `stage-release`, which accepts exact plan and acquisition artifacts,
    rechecks storage, extracts 20 exact ZIP members, four exact Schedule A
    relations, and in v2 or v3 the one all-history Schedule E relation into
    verified zstd streams, and checkpoints each output. V3 Schedule B
    relations remain `archive_direct` and add no staged output.
12. Exact Schedule A extraction acceptance: one COPY section, process success,
    physical row count, compressed and uncompressed bytes and SHA-256 values,
    and a full decompression-digest replay. Every physical data row is
    preserved; the downstream occurrence publisher records field, period, and
    identifier problems instead of deleting or rejecting their source bytes.
13. `publish-release`, which revalidates the evidence chain and immutable
    objects under an exclusive lock, writes one immutable manifest, and only
    then atomically replaces the active pointer. Stale baselines and partial
    stages cannot publish.
14. Separate Dagster staging and publication assets and sensors. Each run
    receives exact upstream control-artifact paths; staging failure never
    repeats acquisition.
15. `publish-schedule-a-occurrences`, which streams one selected cycle,
    conserves every row, emits explicit parse and duplicate issues, produces a
    globally sorted natural-key index and semantic change set, and atomically
    publishes one immutable occurrence-set manifest per cycle.
16. A partitioned Dagster occurrence asset and release-publication sensor that
    add dynamic `fec_cycle` partitions and fan out the exact published source
    release without moving record logic into Python.
17. Strict parsers plus immutable occurrence, issue, natural-key, and change
    publication for the 20 exact `cn`, `cm`, `ccl`, `weball`, and `webl`
    dataset/cycle slices in the coordinated release.
18. Lossless normalized facts for the five classic products. Candidate and
    committee profiles remain source assertions, linkages remain explicit
    source relationships, the two summary populations remain separate, and
    every reported monetary text value retains a lossless signed-cent value
    when valid.
19. One Dagster multi-partition per classic slice with separate static
    `dataset` and dynamic `fec_cycle` dimensions. A sensor launches occurrence
    and fact publication from the exact coordinated release. Python maps paths
    and metadata only.
20. Lossless Schedule A receipt facts for every unique source-valid `SUB_ID`,
    retaining all 81 source fields and typed receipt, contributor, candidate,
    conduit, election, and filing groups without counting or amendment policy.
21. One partitioned Schedule A fact asset after occurrence publication. Its Go
    publisher streams every row directly when occurrence evidence proves the
    cycle wholly valid and unique. Otherwise it uses the occurrence natural
    index's source-row ordinals as a compact selection bitmap.

22. One immutable calculation-readiness bundle per cycle. Dagster maps the
    Schedule A cycle and exactly the linkage plus two summary classic datasets
    into a `bundle`/`cycle` multi-partition. Go verifies same-release ancestry,
    immutable manifest identity, and complete backing before readiness.
23. Eager automation materializes the compact calculation only from the
    bundle's immutable path. Go remains the independent enforcement boundary;
    Dagster never infers release coherence from timing.
24. Version-aware release decoding and v1-to-v2 migration planning. The first
    v2 release reuses compatible immutable v1 artifacts and changes only the
    newly required Schedule E source.
25. Cycle-partitioned Schedule E occurrence and fact assets. Go preserves all
    selected physical rows and all 80 source fields; Dagster passes exact
    manifests and exposes lineage without implementing policy.
26. Version-aware v2-to-v3 planning that adds the Schedule B archive while
    preserving all prior release contracts and reusing unchanged artifacts and
    staged outputs.
27. Lossless selected-cycle Schedule B Parquet facts streamed directly from
    the release-owned archive, with 81 source lexemes, exact physical
    locators, policy-free typed projections, deterministic shards, global
    `SUB_ID` uniqueness, full-schema readback, and resumable replay.
28. A v3-only Dagster sensor that maps source publication to the four Schedule
    B cycle assets while Go owns extraction, validation, storage, and identity.
29. Direct CLI publication preserves the exact plan, acquisition, and stage
    input bytes in immutable content-addressed control paths before the active
    pointer can advance, matching Dagster's evidence retention.

## Next implementation milestone

Define Schedule B effective-record and outgoing-flow-role calculations over
the preserved facts. Then publish fact-level Schedule A/B reconciliation and
only afterward a new graph version whose economic-flow hypotheses point to
both independent source assertions. Committee history remains a separate
source-boundary prerequisite for time-correct terminal identity. The ready
four-cycle Schedule E graphs remain a distinct outside-spending component.
