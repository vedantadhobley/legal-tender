# Operations Runbook

> **Status:** The Go rewrite release, Schedule A/classic occurrence, compact
> Schedule A evidence, normalized-fact, columnar Schedule A, columnar Schedule
> B, and compact
> candidate-receipt and receiver-flow calculation procedures are current.
> Coordinated fact-bundle readiness and calculation automation are active.
> Legacy recovery and full production-readiness procedures remain stubs.

The "how do I…" doc. Lives in heads / chat scrollback today; should live here.

Topics to cover (Phase 4):

## Common ops

- How to start/stop the dev stack
- How to start/stop the prod stack
- How to run a manual sync for one cycle (override `cycles` config)
- How to materialize a single asset with custom config
- How to inspect a stuck Dagster run
- How to query ArangoDB from the host vs. inside a container
- How to read Dagster logs / the compute_logs/ directory

## Go rewrite FEC release flow

The `legal_tender_rewrite` Dagster code location contains the implemented
release path:

```text
fec_release_discovery -> fec_release_candidate
                              |
                              +-- update_available sensor --> fec_release_acquisition
                                                              |
                                                              +--> fec_release_stage
                                                                        |
                                                                        +--> fec_release_publication
                                                                                  |
                                                                                  +--> fec_schedule_a_occurrences[cycle]
                                                                                  |        |
                                                                                  |        +--> fec_schedule_a_facts[cycle] -------+
                                                                                  |                    |                        |
                                                                                  |                    +--> fec_receiver_reported_committee_flows[cycle]
                                                                                  |                                             |
                                                                                  +--> fec_classic_occurrences[dataset,cycle]      |
                                                                                           |                                      |
                                                                                           +--> fec_classic_facts[dataset,cycle] --+
                                                                                                                                 |
                                                                                  fec_candidate_receipt_fact_bundle[bundle,cycle]
                                                                                                        |
                                                                                                        +--> fec_candidate_itemized_receipts[cycle]

                                                                                  +--> fec_schedule_e_occurrences[cycle]
                                                                                           |
                                                                                           +--> fec_schedule_e_facts[cycle]
                                                                                                    |
                                                                                                    +--> fec_effective_independent_expenditures[cycle] --+
                                                                                                                                                        |
                                                                                  fec_classic_facts[candidate-master,cycle] -----------------------------+
                                                                                  fec_classic_facts[committee-master,cycle] -----------------------------+
                                                                                                                                                        |
                                                                                  fec_independent_expenditure_projection_bundle[bundle,cycle]
                                                                                                    |
                                                                                                    +--> arango_independent_expenditures[cycle]

                                                                                  +--> fec_schedule_b_facts[cycle]
```

Materialize it manually inside the development webserver:

```bash
docker exec legal-tender-dev-webserver dagster asset materialize \
  --select 'fec_release_discovery+' \
  -m orchestration.definitions
```

The first two assets run every Monday at 04:00 `America/New_York` through
`monday_fec_release_planning`. Set `DAGSTER_SCHEDULES_ENABLED=0` before starting
the stack to disable target schedules and sensors alongside the legacy
schedule. Results are immutable control artifacts documented in the
[storage layout](./storage.md).

The `fec_release_acquisition_sensor` evaluates each candidate materialization.
It starts `fec_release_acquisition_job` only for `update_available`, passing the
exact content-addressed plan path in run config. Its run key includes the
candidate release ID and plan-artifact digest. This suppresses duplicate event
delivery while allowing a later Monday plan to retry the same candidate after
an operational block. `no_change` and `source_not_ready` are observable
successful planning results and produce sensor skips.

Acquisition and staging each retry three times with exponential backoff and a
24-hour subprocess timeout. Acquisition resumes candidate partials. Staging
resumes individual selected outputs. A storage block, transport failure,
publisher version race, extraction failure, container failure, nonzero exit,
or JSON-contract violation fails the asset.
When Go emits a contract-valid blocked or failed result, the adapter preserves
that exact JSON and attaches its path, digest, status, and issues to the
Dagster failure.

Direct `publish-release` use also preserves the exact plan, acquisition, and
stage bytes under content-addressed `control/fec/release/` paths before the
active pointer can advance. Use a unique convenience stdout path for each
manual attempt anyway; never direct concurrent commands to the same file.

The direct command is useful for controlled recovery and will perform real
downloads when its storage gate passes:

```bash
legal-tender pipeline fec acquire \
  --plan /storage/control/fec/release/plans/<sha256>.json \
  --current /storage/releases/fec/current.json \
  --storage-root /storage \
  --run-id <stable-run-id>
```

Omit `--current` only for the initial all-changed release. The command accepts
only `update_available`. It does not publish the candidate or replace
`current.json`. No production acquisition has been initiated from this slice.

After a successful acquisition, the staging sensor passes the exact plan and
acquisition control artifacts to:

```bash
legal-tender pipeline fec stage-release \
  --plan /storage/control/fec/release/plans/<sha256>.json \
  --acquisition /storage/control/fec/release/acquisitions/<sha256>.json \
  --current /storage/releases/fec/current.json \
  --storage-root /storage \
  --run-id <stable-run-id>
```

Omit `--current` for the bootstrap release. The command extracts every exact
selected ZIP member and Schedule A relation to immutable zstd storage. It
requires exactly one COPY section per relation, preserves every physical data
row, records row/byte/digest evidence, verifies decompression, and writes a
checkpoint after each output. Field, period, and identifier problems become
downstream occurrence issues. Staging cannot replace `current.json`.

The publication sensor passes the same immutable evidence chain to:

```bash
legal-tender pipeline fec publish-release \
  --plan /storage/control/fec/release/plans/<sha256>.json \
  --acquisition /storage/control/fec/release/acquisitions/<sha256>.json \
  --stage /storage/control/fec/release/stages/<sha256>.json \
  --current /storage/releases/fec/current.json \
  --storage-root /storage \
  --run-id <stable-run-id>
```

Publication revalidates all source and selected objects, locks the release
directory, requires the active release to match the plan baseline, writes one
immutable manifest, and then atomically replaces `current.json`. A retry of an
already active evidence chain returns the original manifest. This publishes
the coordinated source release only.

The occurrence sensor reads only the published release's selected periods,
adds those values to the dynamic `fec_cycle` partition set, and starts one
partitioned occurrence job per cycle. The equivalent direct command is:

```bash
legal-tender pipeline fec publish-schedule-a-occurrences \
  --release /storage/control/fec/release/manifests/<sha256>.json \
  --cycle 2026 \
  --storage-root /storage \
  --current /storage/evidence/fec/schedule-a/current/2026.json \
  --run-id <stable-run-id>
```

Omit `--current` only for a cycle's bootstrap occurrence set. Go validates
source-release ancestry, conserves every physical row, records malformed and
duplicate rows as issues, compares semantic values by `SUB_ID`, and atomically
replaces only that cycle's occurrence pointer.

The scheduled Schedule A job now publishes the accepted compact occurrences
and columnar facts. The equivalent direct commands are:

```bash
legal-tender pipeline fec publish-schedule-a-compact-occurrences \
  --release /storage/releases/fec/current.json \
  --cycle 2024 \
  --storage-root /storage \
  --current /storage/evidence/fec/schedule-a/compact/current/2024.json \
  --run-id <stable-run-id> \
  --partitions 512
```

This is the accepted complete-cycle occurrence publisher. It preserves dense
source-row membership, a fixed-width partitioned `SUB_ID` index, sparse
exceptions, and actual inter-release deltas. The complete 2024 publication
finished in 17m 3.66s and used 10.277 GB, 90.94% less than the rejected legacy
JSON evidence. A same-input run fully rehashes immutable backing before reuse.

Then publish or adopt columnar facts:

```bash
legal-tender pipeline fec publish-schedule-a-columnar-facts \
  --release /storage/releases/fec/current.json \
  --occurrences /storage/evidence/fec/schedule-a/compact/current/2024.json \
  --storage-root /storage \
  --current /storage/facts/fec/schedule-a/columnar/current/2024.json \
  --run-id <stable-run-id>
```

The default physical layout is one million source rows per zstd Parquet shard
and 128,000 rows per row group. The command preserves all 81 source fields,
adds exact source locators and policy-free typed values, and validates every
new shard by a full Parquet readback before checkpointing it. A same-input,
same-layout retry replays the source and reuses verified completed shards. It
publishes the immutable manifest and cycle pointer only after complete source
row, byte, digest, fact, and shard conservation succeeds.

When an identical fact set is already published, the command rehashes every
referenced shard before reuse and does not replay the source. The complete
2024 first publication took 1h 53m 53s; a hardened all-shard reuse check took
9.169 seconds. Serial full-shard readback remains a performance target before
weekly automation.

For a clean compact occurrence set over the same source membership, the
publisher adopts the verified existing Parquet shards under the new ancestry
without rewriting them. A dirty cycle reads the compact key-index ordinals to
construct the exact selection bitmap.

Do not change `--rows-per-shard` or `--rows-per-row-group` while resuming a
fact-set. A different layout has a different fact-set identity and staging
directory. Leave at least 500 GiB free. The publisher will block before work
when its storage projection fails.

Do not run the older `publish-schedule-a-facts` command or automate the
downstream per-row JSON calculation publisher. The first 2024 JSON fact
attempt wrote 33.68 GB before reaching half of the source rows. Its complete-
cycle representation is rejected.

Publish or revalidate the exact calculation input bundle directly with:

```bash
legal-tender pipeline fec publish-candidate-itemized-receipts-fact-bundle \
  --cycle 2024 \
  --storage-root /storage \
  --current /storage/bundles/fec/candidate-itemized-individual-receipts/current/2024.json \
  --run-id <stable-run-id>
```

The omitted fact paths default to the canonical current Schedule A columnar,
candidate-committee linkage, all-candidates summary, and current-campaigns
summary pointers for that cycle. Go requires all four fact sets to share one
cycle and coordinated release, matches every pointer to its immutable
manifest, and verifies all referenced backing before publishing the bundle.

Then publish the accepted compact receipt calculation from that frozen bundle:

```bash
legal-tender pipeline fec publish-candidate-itemized-receipts-compact \
  --fact-bundle /storage/bundles/fec/candidate-itemized-individual-receipts/current/2024.json \
  --storage-root /storage \
  --current /storage/calculations/fec/candidate-itemized-individual-receipts/compact/current/2024.json \
  --run-id <stable-run-id>
```

The command validates the bundle against its immutable domain copy, resolves
only the four immutable fact manifests it names, rehashes every Parquet shard,
projects only the nine predicate columns, classifies every source row, and
materializes only exceptional membership plus candidate results. An identical
run revalidates immutable backing and returns the existing calculation set.
The four direct fact flags remain available for controlled diagnostics, but
scheduled calculation runs use the bundle.

Publish receiver-reported committee flows directly from the exact Schedule A
columnar fact set:

```bash
legal-tender pipeline fec publish-receiver-committee-flows \
  --cycle 2024 \
  --schedule-a-facts /storage/facts/fec/schedule-a/columnar/current/2024.json \
  --storage-root /storage \
  --current /storage/calculations/fec/receiver-reported-committee-flows/current/2024.json \
  --run-id <stable-run-id>
```

The two manifest flags default to the canonical cycle pointers. Go verifies
the immutable Schedule A manifest and every Parquet shard, applies the ordered
exact-ID and receipt-role policy, and publishes sparse unresolved exceptions
plus grouped results. All row and signed-cent dispositions must conserve. An
unchanged run verifies the published artifacts and returns the existing
manifest. Dagster runs this command eagerly from `fec_schedule_a_facts`.

Freeze the exact receiver-flow graph inputs, then project them into an isolated
ArangoDB database:

```bash
legal-tender pipeline fec publish-receiver-committee-flow-projection-bundle \
  --cycle 2024 \
  --storage-root /storage \
  --calculation /storage/calculations/fec/receiver-reported-committee-flows/current/2024.json \
  --committee-facts /storage/facts/fec/classic/committee-master/current/2024.json \
  --current /storage/bundles/fec/receiver-reported-committee-flow-projection/current/2024.json \
  --run-id <stable-run-id>

legal-tender pipeline fec probe-arango-receiver-committee-flows \
  --cycle 2024 \
  --storage-root /storage \
  --projection-bundle /storage/bundles/fec/receiver-reported-committee-flow-projection/current/2024.json \
  --endpoint http://legal-tender-dev-arango:8529 \
  --run-id <stable-run-id> \
  --query-repetitions 10
```

The bundle command rejects mixed cycles or releases and pins immutable
calculation, Schedule A, and committee-master lineage. The graph command
accepts only the verified bundle. It imports referenced committee vertices and
grouped source-to-recipient edges, reads all counts and exact signed cents
back, measures connected components and directed cycles, and runs bounded
neighborhood, ranked-path, shortest-path, and cycle queries. The password is
read from `ARANGO_PASSWORD`. An unchanged retry reuses matching completion
metadata; it does not mutate a graph with different lineage.

Audit receiver-flow committee IDs that are absent from the bundle's exact
committee master before using them for identity or terminal classification:

```bash
receiver_history_args=()
for receiver_history_cycle in {1980..2018..2}; do
  printf -v receiver_history_suffix '%02d' "$((receiver_history_cycle % 100))"
  receiver_history_args+=(
    --historical-committee-archive
    "${receiver_history_cycle}=/storage/audits/fec/committee-master-history/raw/cm${receiver_history_suffix}.zip"
  )
done

legal-tender pipeline fec audit-receiver-flow-master-gaps \
  --cycle 2024 \
  --storage-root /storage \
  --projection-bundle /storage/bundles/fec/receiver-reported-committee-flow-projection/current/2024.json \
  --committee-comparison-facts <different-immutable-2024-committee-master-manifest> \
  --committee-history-facts <immutable-2020-committee-master-manifest> \
  --committee-history-facts <immutable-2022-committee-master-manifest> \
  --committee-history-facts <immutable-2026-committee-master-manifest> \
  "${receiver_history_args[@]}"
```

Linkage and both candidate-summary inputs default to the active same-cycle
pointers. The command verifies the exact graph bundle and calculation,
classifies every omitted ID against attached assertions, and emits JSON to
standard output. It never updates the selected master or graph.

Add `--trace-source-receipts` only for a forensic run. That mode rehashes and
scans the exact Schedule A Parquet fact set to attach source rows for IDs
absent from every audited master. The measured 2024 run took 1,067.8 seconds;
the fast master-only audit takes seconds. Do not put the deep mode on the
weekly Dagster path. Exact inputs and results are in the
[2024 master-gap audit](./audit/receiver-flow-master-gaps-2026-09-01.md).

Publish replayable identity states from the same fast evidence boundary, then
compose and materialize the additive v2 graph:

```bash
legal-tender pipeline fec publish-receiver-flow-committee-identities \
  --cycle 2024 \
  --storage-root /storage \
  --run-id <stable-run-id> \
  --projection-bundle /storage/bundles/fec/receiver-reported-committee-flow-projection/current/2024.json \
  --committee-comparison-facts <different-immutable-2024-committee-master-manifest> \
  --committee-history-facts <immutable-2020-committee-master-manifest> \
  --committee-history-facts <immutable-2022-committee-master-manifest> \
  --committee-history-facts <immutable-2026-committee-master-manifest> \
  "${receiver_history_args[@]}"

legal-tender pipeline fec publish-receiver-committee-flow-projection-bundle-v2 \
  --cycle 2024 \
  --storage-root /storage \
  --run-id <stable-run-id>

legal-tender pipeline fec probe-arango-receiver-committee-flows-v2 \
  --cycle 2024 \
  --storage-root /storage \
  --projection-bundle /storage/bundles/fec/receiver-reported-committee-flow-projection/v2/current/2024.json \
  --endpoint http://legal-tender-dev-arango:8529 \
  --run-id <stable-run-id> \
  --query-repetitions 10
```

The identity publisher never scans Schedule A source rows and never repairs a
reported ID by name. It writes decisions under
`/storage/calculations/fec/receiver-flow-committee-identity-coverage/`. The v2
bundle lives under the versioned `receiver-reported-committee-flow-projection/v2/`
directory. The graph command writes a new `lt_flow_probe_v2_*` database and
does not alter v1. Historical, alternate-release, and unresolved vertices are
all terminal-identity-ineligible. See the
[measured v2 gate](./audit/arango-receiver-flow-identity-coverage-2026-09-01.md).

Run the isolated ArangoDB physical-model probe after the candidate and
committee master facts plus the compact calculation exist:

```bash
legal-tender pipeline fec probe-arango-candidate-receipts \
  --cycle 2024 \
  --storage-root /storage \
  --endpoint http://legal-tender-dev-arango:8529 \
  --run-id <stable-run-id> \
  --query-repetitions 25
```

The omitted manifest paths default to the active receipt bundle, compact
calculation, candidate-master facts, and committee-master facts for the cycle.
The password is read from `ARANGO_PASSWORD`; there is no literal password
flag. The command only creates a content-addressed `lt_probe_*` database. It
imports deterministic query projections, checks exact collection counts,
publishes completion metadata last, and measures direct result plus inbound
neighborhood queries. A retry reuses matching completion metadata. It never
drops, truncates, or writes the legacy database.

The classic sensor creates one Dagster multi-partition with separate `dataset`
and `cycle` dimensions for each selected small FEC product. Each partition
runs occurrence publication followed by fact normalization. The eager
automation sensor maps only linkage and the two summary datasets, plus the
same Schedule A cycle, into the calculation's bundle partition. It starts the
calculation only after that bundle materializes:

```bash
legal-tender pipeline fec publish-classic-occurrences \
  --release /storage/control/fec/release/manifests/<sha256>.json \
  --dataset candidate-master \
  --cycle 2024 \
  --storage-root /storage \
  --current /storage/evidence/fec/classic/candidate-master/current/2024.json \
  --run-id <stable-run-id>

legal-tender pipeline fec publish-classic-facts \
  --release /storage/control/fec/release/manifests/<sha256>.json \
  --occurrences /storage/evidence/fec/classic/candidate-master/current/2024.json \
  --storage-root /storage \
  --current /storage/facts/fec/classic/candidate-master/current/2024.json \
  --run-id <stable-run-id>
```

The first command preserves every physical row and isolates malformed or
duplicate publisher keys. The second emits one fact per unique valid key and
never chooses a duplicate winner. Both commands validate immutable backing and
source-release ancestry.

To measure the accepted receipt calculation without publishing row facts or
decisions, run the manual direct probe:

```bash
legal-tender pipeline fec probe-candidate-itemized-receipts \
  --cycle 2024 \
  --release /storage/control/fec/release/manifests/<sha256>.json \
  --occurrences /storage/evidence/fec/schedule-a/current/2024.json \
  --linkage-facts /storage/facts/fec/classic/candidate-committee-linkage/current/2024.json \
  --all-candidates-summary-facts /storage/facts/fec/classic/all-candidates-summary/current/2024.json \
  --current-campaigns-summary-facts /storage/facts/fec/classic/current-campaigns-summary/current/2024.json \
  --storage-root /storage \
  --run-id <stable-probe-run-id>
```

Go requires all inputs to share one cycle and source-release ID, revalidates
and hashes the staged Schedule A relation, and routes source rows through the
same calculator used by normalized facts. It writes candidate results and a
probe manifest under `/storage/probes`; it writes no receipt facts, per-row
decisions, production calculation manifest, or active pointer. Inspect
`reconciliations` as diagnostic gaps against each summary source; never copy
those summary values into the detailed component.

To compare a bounded columnar candidate with the same rows in zstd COPY, use a
new output directory:

```bash
legal-tender pipeline fec benchmark-schedule-a-layout \
  --source /storage/raw/fec/schedule-a/extracts/sha256/<prefix>/<sha256>.copy.zst \
  --cycle 2024 \
  --rows 10000000 \
  --source-total-rows 264085606 \
  --source-sha256 <complete-compressed-sha256> \
  --output /storage/probes/<unique-layout-probe-name>
```

The command refuses an existing output directory. It writes equal-row zstd
and Parquet artifacts, round-trips every logical source field, compares the
five-column receipt predicate, and emits `benchmark.json`. A result below ten
million rows is provisional. `accepted_for_full_corpus_probe` authorizes only
the next measurement; it does not resume the Dagster fact asset or publish an
active pointer. The accepted bounded result and known v1 reporting correction
are in the
[physical-layout benchmark audit](./audit/schedule-a-layout-benchmark-2026-08-30.md).

To reproduce the non-mutating classic flow audit for one cycle:

```bash
legal-tender pipeline fec audit-classic-flows \
  --period 2024 \
  --pas2 /storage/raw/2024/pas2.zip \
  --oth /storage/raw/2024/oth.zip
```

The command reads both ZIPs completely, hashes their selected members, and
emits one versioned JSON result on standard output. It writes no database or
artifact state. Treat the named legacy cohorts, repeated keys, and possible
two-sided signatures as audit evidence only. The accepted interpretation is
in the [flow fact requirements](./design/fec-flow-fact-requirements.md).

To verify or reproduce the historical Schedule B source gate without publishing
facts or release state:

```bash
legal-tender pipeline fec verify-schedule-b \
  --dump /storage/raw/fec/schedule-b/snapshots/<snapshot>/fec_fitem_sched_b.dump \
  --period 2024 \
  --work-dir /storage/cache

legal-tender pipeline fec audit-schedule-b-overlap \
  --dump /storage/raw/fec/schedule-b/snapshots/<snapshot>/fec_fitem_sched_b.dump \
  --period 2024 \
  --pas2 /storage/raw/fec/schedule-b/snapshots/<snapshot>/classic/pas224.zip \
  --oth /storage/raw/fec/schedule-b/snapshots/<snapshot>/classic/oth24.zip \
  --work-dir /storage/cache
```

`--max-rows` is a smoke-only option. A complete run streams the exact selected
relation, validates and hashes every row, proves `SUB_ID` uniqueness with
automatically removed temporary shards, then compares classic membership,
endpoint orientation, and exact cents. Redirect standard output to a new
immutable audit result and hash it before use. See the
[complete 2024 source audit](./audit/schedule-b-source-and-classic-overlap-2026-09-01.md).

To reproduce the passing same-publisher-batch Schedule A/B gate:

```bash
legal-tender pipeline fec audit-schedule-ab-alignment \
  --storage-root /storage \
  --cycle 2024 \
  --schedule-a-facts /storage/facts/fec/schedule-a/columnar/current/2024.json \
  --schedule-a-release /storage/control/fec/release/manifests/<manifest-sha256>.json \
  --schedule-b-dump /storage/raw/fec/schedule-b/snapshots/<snapshot>/fec_fitem_sched_b.dump \
  --schedule-b-observation contracts/sources/fec/schedule-b/v1/fixtures/archive/dump-2026-08-30.json \
  --work-dir /storage/cache \
  --workers 16
```

Run this as a background container job. Redirect standard output to a new
immutable result, retain stderr as the progress log, hash the result, and
publish a completion marker only after exit zero. `--max-schedule-b-rows` is
smoke-only and cannot pass the complete-source gate. The command does not
publish facts or database state. The measured input identities and canonical
result are in the
[alignment audit](./audit/schedule-ab-alignment-2026-09-04.md).

To publish lossless Schedule B facts from an accepted v3 release:

```bash
legal-tender pipeline fec publish-schedule-b-columnar-facts \
  --release /storage/releases/fec/current.json \
  --cycle 2024 \
  --storage-root /storage \
  --run-id <stable-run-id> \
  --work-dir /storage/cache
```

The command requires v3 `archive_direct` membership for the selected cycle,
rehashes the immutable Schedule B archive, and streams only that relation via
`pg_restore`. It preserves all 81 source lexemes and exact row locators in the
98-column Parquet contract, validates each shard through independent readback,
proves global `SUB_ID` uniqueness, and advances the cycle pointer only after
complete source, fact, and byte conservation. It does not select effective
records, infer recipients, classify outgoing flows, reconcile Schedule A, or
write graph state. The defaults are one million source rows per shard and
128,000 rows per row group. A stable run ID resumes the per-shard checkpoint;
an unchanged current fact set returns idempotently after backing verification.
A descendant coordinated release that reuses the same Schedule B artifact also
returns that source-stable fact set without invoking `pg_restore`.

To profile record-selection and reporting-role evidence over those facts:

```bash
legal-tender pipeline fec audit-schedule-b-semantics \
  --storage-root /storage \
  --facts /storage/facts/fec/schedule-b/columnar/current/2024.json \
  --cycle 2024 \
  --workers 4
```

Run in a background container with four CPUs, a 4 GiB container limit,
`GOMEMLIMIT=2GiB`, and read-only source mounts. Retain stdout as a new audit
result, stderr as its progress log, and a SHA-256 plus completion marker only
after successful exit and schema validation. This reads existing Parquet and
does not extract the archive or change source, calculation, or graph pointers.
The [diagnostic contract](../contracts/audits/fec/schedule-b-semantics/v1/)
defines exactly what its categories and non-memo amounts mean.

To calculate the accepted reporting subtotals and form-specific roles:

```bash
legal-tender pipeline fec calculate-disbursement-reporting \
  --storage-root /storage \
  --facts /storage/facts/fec/schedule-b/columnar/current/2024.json \
  --cycle 2024 \
  --workers 8
```

Use a background container with eight CPUs, a 4 GiB cap,
`GOMEMLIMIT=2GiB`, and read-only source mounts. Write stdout and stderr to
distinct, run-specific files. Require successful exit, validate against the
[reporting result schema](../contracts/calculations/fec/processed-disbursement-reporting/v1/result.schema.json),
then retain the artifact digest and completion marker. The result is
deterministic across worker counts; runtime is logged on stderr.

The command has no active publication pointer or Dagster asset. It does not
cache results or change facts and graphs. `complete_with_unresolved` means
the scan conserved all source evidence but some rows remain outside reviewed
reporting categories. `graph_eligible=false` is mandatory, including when
there are no unresolved rows. These amounts are not total spending or an
economic-flow ledger.

To verify data-row-only COPY output from the processed Schedule E relation:

```bash
legal-tender pipeline fec verify-schedule-e \
  --input /storage/raw/fec/schedule-e/<extract>.copy \
  --expected-rows <rows> \
  --expected-bytes <bytes> \
  --expected-sha256 <sha256>
```

Use `--input -` to stream `COPY ... TO STDOUT` without an intermediate file.
Add `--cycle` only when the input was already selected by `election_cycle`.
The command writes no artifacts or database state. A passing source parse does
not authorize an outside-spending total; the effective-record calculation is a
separate gate.

To publish the lossless Schedule E layer from an accepted v2 or v3 release:

```bash
legal-tender pipeline fec publish-schedule-e-occurrences \
  --release /storage/releases/fec/current.json \
  --cycle 2026 \
  --storage-root /storage \
  --run-id <stable-run-id>

legal-tender pipeline fec publish-schedule-e-facts \
  --release /storage/releases/fec/current.json \
  --occurrences /storage/evidence/fec/schedule-e/current/2026.json \
  --storage-root /storage \
  --run-id <stable-run-id>
```

Both commands replay and verify the exact all-history staged relation. They
preserve all selected rows and do not calculate effective outside spending.

To publish effective independent expenditures for one cycle:

```bash
legal-tender pipeline fec publish-effective-independent-expenditures \
  --schedule-e-facts /storage/facts/fec/schedule-e/current/2026.json \
  --storage-root /storage \
  --run-id <stable-run-id>
```

The command verifies the complete backing fact artifact, rejects notice-like
rows, applies the accepted memo/amount/route predicate, conserves exact signed
cents, and advances the cycle pointer atomically. Replaying an unchanged input
and calculation version reuses the existing immutable publication.

Resolve every attributed fact against the same-release candidate master before
building candidate graph groups:

```bash
legal-tender pipeline fec publish-independent-expenditure-candidate-resolution \
  --cycle 2024 \
  --effective /storage/calculations/fec/effective-independent-expenditures/current/2024.json \
  --candidate-facts /storage/facts/fec/classic/candidate-master/current/2024.json \
  --storage-root /storage \
  --run-id <stable-run-id>
```

The two input paths default to their canonical cycle pointers when omitted.
Go resolves each pointer to its immutable manifest, requires one coordinated
release, replays effective membership, and writes one dense decision per
attributed fact. `confirmed`, `resolved`, and `unverified` decisions carry a
projectable candidate ID; `ambiguous` and `unresolved` decisions retain their
signed amount without creating an identity. An unchanged replay returns the
existing immutable calculation.

Group projectable decisions and preserve unprojectable coverage:

```bash
legal-tender pipeline fec publish-resolved-independent-expenditures \
  --cycle 2024 \
  --candidate-resolution /storage/calculations/fec/independent-expenditure-candidate-resolution/current/2024.json \
  --storage-root /storage \
  --run-id <stable-run-id>
```

The candidate-resolution path defaults to its canonical cycle pointer. The
command writes one group per spender, resolved candidate, and stance plus one
sparse exception per ambiguous or unresolved decision. It blocks unless every
decision and exact signed cent conserves through one route.

Publish or revalidate the active resolved graph-readiness bundle:

```bash
legal-tender pipeline fec publish-resolved-independent-expenditure-projection-bundle \
  --cycle 2024 \
  --storage-root /storage \
  --current /storage/bundles/fec/resolved-independent-expenditure-projection/current/2024.json \
  --run-id <stable-run-id>
```

Omitted inputs default to the current resolved calculation plus candidate- and
committee-master pointers. Go also verifies the exact dense
candidate-resolution ancestry and requires the candidate master to equal the
fact set used by that resolution.

Then run the isolated v2 outside-spending graph gate:

```bash
legal-tender pipeline fec probe-arango-resolved-independent-expenditures \
  --cycle 2024 \
  --storage-root /storage \
  --projection-bundle /storage/bundles/fec/resolved-independent-expenditure-projection/current/2024.json \
  --endpoint http://legal-tender-dev-arango:8529 \
  --run-id <stable-run-id>
```

Set `ARANGO_PASSWORD` in the process environment; never pass or log it as a
flag. The command creates only a new content-addressed
`lt_ie_probe_resolved_*` database, preserves resolution quality on edges and
unprojectable coverage in metadata, reads all counts and amounts back, and
reuses an identical completed database.

The earlier reported-ID bundle remains available for historical replay:

```bash
legal-tender pipeline fec publish-independent-expenditure-projection-bundle \
  --cycle 2024 \
  --storage-root /storage \
  --current /storage/bundles/fec/independent-expenditure-projection/current/2024.json \
  --run-id <stable-run-id>
```

The omitted input paths default to the current effective calculation plus
candidate- and committee-master fact pointers for that cycle. Go matches each
pointer to its immutable manifest, requires one coordinated release, verifies
all backing artifacts, and publishes only lineage and readiness evidence. An
unchanged input set returns the existing immutable bundle.

Its matching historical graph command is:

```bash
legal-tender pipeline fec probe-arango-independent-expenditures \
  --cycle 2024 \
  --storage-root /storage \
  --projection-bundle /storage/bundles/fec/independent-expenditure-projection/current/2024.json \
  --endpoint http://legal-tender-dev-arango:8529 \
  --run-id <stable-run-id>
```

Set `ARANGO_PASSWORD` in the process environment; never pass or log it as a
flag. The bundle flag may be omitted because the command defaults to the
canonical cycle pointer. The mutually exclusive direct calculation and master
flags remain for diagnostics. The command creates only a content-addressed
`lt_ie_probe_*` database, imports one effective calculation result per
support/opposition edge, reads every exact amount back, benchmarks named-graph
queries, and writes completion metadata last. An identical replay verifies
and reuses the database.

Dagster eagerly targets the resolved readiness bundle when the same-cycle
aggregate and both master fact partitions exist, then targets the v2 graph.
The v1 commands remain manual. Python does not choose manifests or implement
money or graph logic.

The coordinated v3 source release, all five 2024 classic fact sets, the
accepted 2024 Schedule A evidence/calculation layers, and lossless Schedule E
occurrence/fact sets for 2020 through 2026 have been published. Effective
independent-expenditure calculation pointers also exist for all four cycles.
The accepted lossless 2024 Schedule B fact set contains all 157,544,163 source
rows in 158 verified Parquet shards and reuses by source identity on unchanged
Schedule B bytes.
The isolated 2024 resolved readiness bundle and v2 ArangoDB outside-spending
projection pass. No production graph publication or application cutover exists.

## Recovery / disaster

- How to restore Arango from `dumps/` after a corruption
- How to recover from a partial pipeline run (which assets to re-materialize)
- How to recover from a corrupted `wikidata_cache.json`
- How to drain + redeploy without losing in-flight runs
- How to roll back to a previous prod image

## Troubleshooting

- Common errors and their meanings (e.g., the int/float `MetadataValue` issue in spent_on)
- What a Wikidata 429 storm looks like (and how to abort cleanly)
- ArangoDB OOM signs and tuning knobs (see commit `fb34a44`)
- Dagster's "stuck" states and how to identify them

## Schedules + automation

- How the weekly Sunday refresh works (once enabled in prod)
- How to pause/resume schedules
- How to inject a one-off run vs. amending the schedule
