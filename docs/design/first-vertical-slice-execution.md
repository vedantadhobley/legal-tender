# First vertical slice execution contract

> **Status:** Draft implementation contract. The source-release foundation—
> discovery, planning, acquisition, selected-data staging, checks, atomic
> publication, shared Go adapter, sensors, and Monday schedule—is implemented.
> Immutable Schedule A occurrence, issue, natural-key, and semantic-change
> publication is also implemented. The five classic FEC products now publish
> occurrences and normalized facts. The first full-row Schedule A and dense
> calculation publishers remain fixture-only because the 2024 corpus probe
> rejected their JSON representations. The accepted compact occurrence,
> lossless Parquet fact, immutable fact-bundle, and compact calculation path is
> wired into Dagster and passed its complete 2024 publication, equivalence,
> readiness, and replay gates. A manual, isolated ArangoDB physical-model probe
> is implemented and passed the complete 2024 count, query, and replay gates;
> its production asset and the API remain target behavior. This is the
> execution companion to the
> [`evidence-backed candidate-receipts slice`](./first-vertical-slice.md).

## Dagster asset graph

Every compute node below invokes the same Go adapter. Python declares only
dependencies, native source partitions, automation, and event mapping.

```text
Monday metadata discovery
    -> fec_release_candidate
    -> fec_release_acquisition
    -> fec_release_stage
    -> fec_release_publication
        -> fec_schedule_a_occurrences[cycle]
            -> fec_schedule_a_facts[cycle]
        -> fec_classic_occurrences[dataset,cycle]
            -> fec_classic_facts[dataset,cycle]

fec_schedule_a_facts[cycle]
    + fec_classic_facts[candidate-committee-linkage,cycle]
    + fec_classic_facts[all-candidates-summary,cycle]
    + fec_classic_facts[current-campaigns-summary,cycle]
    -> fec_candidate_receipt_fact_bundle[bundle,cycle]
    -> fec_candidate_itemized_receipts[cycle]
```

The source-release chain, Schedule A occurrence publisher, all five classic
occurrence/fact pairs, the fixture-scale Schedule A fact publisher, and the
candidate receipt calculation exist now. Their dense JSON variants remain
fixture-only evidence. The accepted compact occurrence, columnar fact,
immutable fact-bundle, and compact calculation nodes are wired into Dagster
and passed their complete 2024 gates. A manual direct-source probe remains
independent equivalence evidence. The manual content-addressed ArangoDB probe
also exists and passed its real 2024 gate; its Dagster asset and API remain
target behavior.

Snapshots and normalized datasets remain separate assets. A source snapshot
can be valid even when a new parser version fails. A parser can be replayed
against an older snapshot without downloading again.

`fec_candidate_itemized_receipts` is a derived projection, not a replacement
for `fec_schedule_a_facts`. It uses the FEC's processed row set,
`is_individual` classification, and memo-subtotal rule defined in the
[`calculation contract`](./calculation-contracts.md). The as-filed projection
is deferred from initial production. Its existing
[effective-filing research](./effective-efile-calculations.md) does not feed or
repair the processed projection.

The API is a Go service over published domain state. It is not a Dagster asset.

## Partition contract

Assets use the publisher's natural acquisition partition. The coordinated
release records how independently published source versions form one Legal
Tender publication.

| Asset class | Dagster partition |
|---|---|
| `cn`, `cm`, `ccl`, and summaries | Multi-partition with static `dataset` and dynamic `fec_cycle` dimensions. |
| Classic comparison ZIPs | Not in the initial production release. |
| Processed Schedule A dump discovery and acquisition | Observable data version; no cycle partition. |
| Processed dump extraction, occurrences, facts, and calculations | Dynamic `fec_cycle` selected from the accepted dump. |
| Candidate receipt fact bundle | Multi-partition with static `bundle` and dynamic `fec_cycle` dimensions. |

An immutable publisher artifact and `fec.release.v1` are domain versions, not
Dagster partitions. This prevents source-version-times-cycle cross-products.

Go maps snapshot-to-snapshot record changes to affected cycles, committees,
and candidate IDs. Dagster requests only those projections. There is no
candidate, committee, contributor, or source-snapshot partition in Dagster.

## Incremental transition contract

The scheduled flow is:

1. At 04:00 `America/New_York` every Monday, Python invokes
   `legal-tender pipeline fec discover`. Go performs metadata-only observations
   of every required bulk object and records exact validators and lengths.
2. Every Monday, Go freezes the newest stable compatible versions into one
   candidate `fec.release.v1`. An incomplete or late publisher set produces
   `source_not_ready`.
3. The snapshot operation captures each required body under its frozen
   validators, verifies its digest and container, and fails if the object
   changes during capture. No API or overlapping file fills a missing source.
4. If the complete selected source set matches the active release, Go returns
   `no_change` and no parsing or domain state is rewritten.
5. Changed Schedule A snapshots produce immutable occurrences and explicit added,
   changed, newly absent, duplicate, and invalid change sets. A processed-dump
   run validates the whole artifact and streams the selected two-year
   relations. This boundary is implemented per dynamic `fec_cycle` partition.
6. Relationship and calculation commands derive affected cycle, committee,
   and candidate keys. Only those projections rebuild.
7. Go atomically advances each layer's pointer only after that layer's checks
   pass. The coordinated source pointer is a planner and evidence baseline,
   not the user-facing product release. Occurrence, fact, graph, calculation,
   and final product pointers remain on their prior versions when their own
   work fails.
8. Dagster records the release ID, source versions, partitions, counts, checks,
   and change-set locations supplied by Go.

An unchanged source can still receive an observation in Dagster. It must not
cause a full parse or candidate rebuild. A code or calculation-version change
can explicitly reprocess an unchanged source snapshot. The coordinated release
retains each source's independent watermark; it does not manufacture one FEC
coverage timestamp.

### Publication states

Each data-plane output moves through these states:

```text
absent -> staged -> checked -> published
                    |
                    +-> failed
```

Domain writes target a staged output version. Publishing changes one version
pointer only after required writes and blocking checks complete. Failure leaves
the prior published version readable. Cleanup of failed staging state is
recoverable maintenance, not part of the successful transaction.

No operation reports success before its domain writes commit. Dagster run state
does not substitute for the Go-owned input watermark and publication record;
Go commands must remain safe when run directly.

## Go commands

One compiled `legal-tender` binary exposes independently testable operations.
The source-release, occurrence, fact, and first calculation commands below are
shipped. Domain projection and serving remain the target surface:

```text
legal-tender pipeline fec discover
legal-tender pipeline fec plan-release
legal-tender pipeline fec acquire
legal-tender pipeline fec stage-release
legal-tender pipeline fec publish-release
legal-tender pipeline fec publish-schedule-a-occurrences
legal-tender pipeline fec publish-schedule-a-compact-occurrences
legal-tender pipeline fec publish-schedule-a-facts
legal-tender pipeline fec publish-schedule-a-columnar-facts
legal-tender pipeline fec publish-classic-occurrences
legal-tender pipeline fec publish-classic-facts
legal-tender pipeline fec publish-candidate-itemized-receipts
legal-tender pipeline fec publish-candidate-itemized-receipts-fact-bundle
legal-tender pipeline fec publish-candidate-itemized-receipts-compact
legal-tender pipeline fec probe-arango-candidate-receipts
legal-tender pipeline fec probe-candidate-itemized-receipts
legal-tender serve
```

`discover` accepts only bounded HTTP-control flags and emits the complete
metadata observation to standard output. `plan-release` accepts paths to the
saved observation and optional prior manifest and emits the pure plan.
`acquire` captures the exact frozen bodies. `stage-release` produces verified
selected streams with per-output checkpoints. `publish-release` validates the
exact evidence chain and atomically advances only the coordinated source-
release manifest. These commands do not yet use the general operation envelope
below, and source-release publication does not claim that domain projections
exist.

`publish-schedule-a-occurrences` accepts the exact published release manifest
and one cycle. It emits a versioned occurrence-set manifest referencing
content-addressed occurrences, issues, natural-key index, and change-set
artifacts. The Schedule A and classic fact commands normalize only lossless
source assertions. `publish-candidate-itemized-receipts` consumes four exact
same-release fact sets and emits immutable receipt decisions, candidate
components, and independent summary reconciliations. Each uses its own
explicit schema rather than the general operation envelope. None writes
ArangoDB state.

The Schedule A JSON fact and per-decision publishers remain shipped for fixture
and contract evidence but must not run against another complete cycle.
`probe-candidate-itemized-receipts` is the manual non-production corpus gate:
it streams the staged relation into the shared calculator and writes candidate
results only. The accepted columnar replacement preserves the same source-
fact boundary. `publish-candidate-itemized-receipts-compact` rehashes the
columnar fact set, evaluates its declared nine-column predicate projection,
stores only exceptional membership, and emits the accepted candidate result.
The complete 2024 publication reproduced the probe's decision, route,
candidate, reconciliation, amount, and result-artifact identities.
`benchmark-schedule-a-layout` selected the physical representation. Its first
ten million 2024 rows passed lossless round-trip, projected-decision, storage,
throughput, scan, and memory checks. `publish-schedule-a-columnar-facts` is the
resumable complete-corpus publisher. It binds the exact release and occurrence
manifests, writes deterministic content-addressed Parquet shards, validates
each shard through a full readback, and atomically publishes only after source
and fact conservation. `publish-candidate-itemized-receipts-fact-bundle`
verifies and freezes the exact Schedule A plus three classic fact manifests
from one cycle and source release. Dagster's eager automation uses explicit
multi-partition mappings to publish that readiness boundary and then invoke
the compact calculation from its immutable path. The complete 2024 bundle,
idempotence, and calculation-replay gates passed.

`probe-arango-candidate-receipts` consumes that exact bundle and calculation
plus same-release candidate and committee master facts. It writes only an
isolated content-addressed `lt_probe_*` database, never the legacy database.
It projects query-bearing entities, candidate-committee relationships,
receipt components, and candidate results while leaving fine Schedule A facts
in Parquet. The complete 2024 probe and replay passed count and query gates;
the explicit master-fact gap keeps the result partial. The accepted boundary
is documented in the
[ArangoDB projection design](./arango-candidate-receipt-projection.md).

Later domain-processing commands accept an explicit JSON
request containing operation, partition, input data versions, calculation
version, and run ID. Secrets and deployment endpoints remain environment
configuration. Each command emits one versioned JSON result envelope on
standard output and diagnostics on standard error.

The result envelope contains:

```json
{
  "schema_version": "legal-tender.pipeline-result.v1",
  "operation": "fec.normalize",
  "status": "changed",
  "partition": {"cycle": "2024", "source": "processed_schedule_a"},
  "inputs": [],
  "outputs": [],
  "changes": {
    "added": 0,
    "changed": 0,
    "absent": 0,
    "invalid": 0,
    "affected_keys_uri": ""
  },
  "checks": [],
  "timing": {},
  "diagnostics": []
}
```

A processed Schedule A capture uses the dump data version as its source-native
identity and reports affected cycles in its referenced change set. The
envelope schema does not pretend all sources share one partition shape.

Input and output entries carry asset key, data version, snapshot or
calculation identity, record count, byte count where applicable, and watermark.
Large change sets and fact memberships are stored as immutable artifacts and
referenced by URI and digest rather than placed in the envelope.

The schema is shared as a language-neutral JSON Schema. Go owns result
construction. Python validates and maps the envelope; it does not reinterpret
domain fields.

## Minimum Python shape

The first slice may add only:

- Dagster definitions generated from a small static asset-spec table.
- The shared `fec_cycle` dynamic partition definition.
- Monday metadata-discovery and configured coordinated-release run requests.
- One subprocess adapter for the Go binary.
- Result-envelope validation and mapping to Dagster metadata and checks.
- Wiring tests.

Python must not import an ArangoDB client, HTTP source client, dataframe
library, or FEC model. The adapter receives source and partition values as
opaque command parameters. All check evaluation occurs in Go; Python only
reports the result.

Correctness cannot depend solely on a separate partitioned Dagster asset-check
feature. Go blocks publication on required checks, and Dagster mirrors those
results for observability.

## Required checks

Go evaluates at least:

- Download completeness, content hash, source-container integrity, and expected
  relation or member selection.
- Candidate release completeness, compatible source selection, stable
  validators, and body-after-metadata agreement.
- Atomic publication of exactly the frozen manifest, with no substituted or
  silently patched source.
- Source relation or header identity and schema compatibility.
- Total, parsed, invalid, and duplicate-occurrence counts.
- Required publisher identifiers without dropping invalid rows.
- Every normalized fact's source-record and snapshot lineage.
- Candidate and committee endpoint coverage for linkage facts.
- Authorized-committee projection conservation against its included linkage
  facts.
- Itemized projection conservation against its included and excluded fact
  manifests.
- Applicable summary coverage dates and partial-cycle state.
- Difference from published FEC summaries as a measured result, not an
  automatic failure or fallback.

Checks that indicate corrupt or incomplete processing block publication.
Coverage differences and unresolved source semantics remain published warnings
when the underlying evidence is intact.

## Acceptance scenarios

The slice is complete when:

1. A controlled fixture and the full 2024 cycle run through the same Go
   commands and Dagster assets.
2. A candidate receipt result expands to the reported contribution, linkage,
   summary, snapshot, source relation or member, and row evidence.
3. Every source row is represented as a valid fact or an explicit parse issue;
   no semantic filter runs during ingestion.
4. Candidate and committee assertion history remains cycle- and
   snapshot-specific.
5. Contributor name or employer equality does not merge people.
6. Published summary amounts and calculated itemized amounts remain separate
   and carry their own meaning and coverage.
7. Reprocessing identical source bytes performs no domain writes and no
   candidate projection rebuild.
8. Adding, changing, or removing one fixture record recomputes only its
   affected committee and candidate projections.
9. A changed record version and its prior version both remain reproducible.
10. A failed parse or projection leaves the prior published version readable.
11. The same published input and calculation versions produce the same result
    digest.
12. The Python package passes an import-boundary test and contains no domain
    calculation.
13. The 2026 partial-cycle run exposes source-specific freshness and coverage
    rather than appearing complete.
14. Classic `indiv.zip` is not required to build the target receipt result and
    is used only for an explicit legacy comparison.
15. A changed source set produces one immutable `fec.release.v1`, publishes all
    required fact families together, and rebuilds only affected projections.
16. A late, missing, changed-during-capture, or invalid source leaves the prior
    release active, while a `no_change` candidate release performs no body
    download or downstream rebuild.

Legacy aggregate parity is not an acceptance condition. Differences must be
explained as intended semantics, legacy defects, source ambiguity, or a target
bug; they are not forced back into equality.

## External references

- [Dagster partitioning](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets)
- [Dagster external pipelines](https://docs.dagster.io/integrations/external-pipelines)
