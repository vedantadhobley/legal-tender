# Publication-independent funding path readers

Status: implemented in Go. Full regression, targeted race, static checks and
the [retained 2024 gate and fresh replay](../audit/funding-window-2026-09-13.md) pass. This is the first
production consumer following the [composition experiment](./cycle-calculation-windows.md#bounded-composition-test),
not a complete cross-source application or a new physical graph.

## Boundary

`inspect-funding-window-paths` accepts an explicit set of immutable
[funding generations](./funding-evidence-generation.md). Each generation still
opens through the existing source, reference, graph-readback and completion
checks. The wrapper never disables a source-cycle check or rewrites an input.
The original command supports committee-to-committee paths in one selected
Schedule A or Schedule B ledger. The additive
[connection interface](#receipt-and-candidate-connections) extends this verified
loader to receipt entries, candidate authorization context and
[dated Schedule E source members](#source-grain-schedule-e-connections).
Person/corporation resolution and non-FEC sources remain open.

The application selects observations by their reported dates, independent of
the source partitions. Exact FEC committee IDs connect across inputs; historical
master documents remain separate, generation-qualified facets. A shared endpoint
does not merge source profiles or establish verified corporate/person identity.

One generation per source cycle is accepted. Duplicate generations, repeated
A/B fact sets and multiple versions of the same cycle are rejected before graph
opening. This prevents accidental snapshot/amendment overlap; it is not a rule
that analysis must stay within a cycle. Combining different snapshots of one
partition needs a separate reconciliation contract. Different source occurrences
remain different observations even when they may describe the same payment.

## Date and evidence rules

- Supply both `--start-date` and `--end-date` as inclusive `YYYY-MM-DD` dates,
  or neither. Omitting them selects all observations in the supplied inputs,
  including undated ones; it does not mean all history has been loaded.
- Bounded windows include only known reported dates within the range. Undated
  observations remain in source storage and receive an explicit excluded count;
  no cycle label, filing date or snapshot date fills their missing event date.
- Per-input counts conserve the complete selected ledger as included, before,
  after and unknown-date-excluded observations. Undated-included is a subset of
  included for an unfiltered query. These are population counts, not money totals
  or estimates of missing real-world activity.
- Filtering dates does not impose chronological order on paths. It does not
  infer fund availability, relationship validity, terminal identity or allocation.
  A cycle-specific master assertion does not establish day-level validity.
- Search bounds reuse the existing deterministic simple-path search. A cutoff or
  no-outgoing result is not evidence of a terminal donor. Search completeness is
  relative to the selected topology and declared hop/work limits.

Every returned link retains its original graph key and routes source drilldown
through its originating verified reader. Full observation/source evidence remains
available, including signed amounts and source dates. No path-money sum is added.
Every returned committee retains a facet from each input, including absence or
missing-master state. Dates on the path are checked separately from those facets.

Results bind all original generation metadata/checksums, the consumer executable,
query dates and search limits. Input/row order does not change the answer.
Returned metadata and query dates are owned copies, not mutable reader state.
Every input's completion boundary is rechecked before and after a query. Errors
return no successful result. This relies on immutable publications, not a
transaction across multiple databases.

## Interface and limits

The input file is a pinned request, not proof that its backing is valid:

```json
{
  "version": "legal-tender.funding-window-inputs.v1",
  "inputs": [{
    "generation": "/storage/.../generation.json",
    "generation_sha256": "<sha256>",
    "graph_manifest": "/storage/.../receipt-graph-manifest.json",
    "participants": "/storage/.../participant-manifest.json",
    "conduits": "/storage/.../conduit-manifest.json"
  }]
}
```

```text
legal-tender pipeline fec inspect-funding-window-paths
  --inputs <spec.json> --expected-inputs-sha256 <sha256>
  --storage-root /storage --endpoint <arango-url>
  --password-env ARANGO_PASSWORD
  --from-committee <fec-id> --target <fec-id> --ledger schedule_a
  --start-date 2023-07-01 --end-date 2024-06-30
  [--max-committee-hops 4 --max-paths 3 --max-expansions 10000]
  [--expected-result-id <sha256>]
```

No credentials belong in the input file. No cycle override, current-pointer
discovery, source substitution, graph import or terminal policy is accepted.

The first implementation holds existing verified models and builds one selected
ledger's adjacency index per query. It caps requests at 64 KiB, eight publications
and two million combined selected A/B observations, checked before opening.
These are conservative resource guards, not a latest-four-cycle rule or a measured
multi-cycle capacity claim. Real runs use a 4 GiB container cap and 2 GiB Go heap
target. Full opening, index construction and evidence readback are outside the
path expansion budget; latency and larger-input memory need separate measurement.

## Verification and next boundary

Fixtures exercise the production window selector and path consumer over two
source cycles, both ledgers, exact source routing, historical facets, boundary
dates, null dates, overlap/cap rejection, cancellation, final completion changes,
owned results and deterministic replay. Existing single-cycle guards still pass.
The earlier test-only selector remains a historical experiment, not runtime code.

The [two-publication integration fixture](../../internal/integration/fundingwindow/window_test.go)
also exercises the public loader and CLI through the complete dependency chain.
Run `make test-window-integration`. It builds tiny synthetic 2022/2024 facts,
calculations, readiness bundles and completed receipt/A/B/E graphs with the normal
publishers, then opens them through `OpenWindowReader`. No reader stub or bypass
of verification is used. The source acquisition metadata is synthetic, and the
external Schedule B `pg_restore` process is replaced by fixed COPY output; this
does not test fetching or archive extraction. ZIP/member/COPY, Parquet, source
lineage and database checks remain active downstream.

The fixture checks both ledgers, a route unavailable in either partition alone,
date selection, source routing, historical facets, reversed-input reopening and
CLI result/replay identity. Corruption probes change only disposable fixture
data: foreign receipt locators, damaged second-source ZIP bytes, changed second-
graph observations and invalid second completion metadata must fail. Restoring
that backing must allow a clean reopen. This closes the loader integration gap;
it is not acceptance of two real FEC corpora or production memory capacity.

The same full-chain fixture now exercises receipt-to-committee,
committee-to-candidate and receipt-to-candidate connection queries in both
ledgers. Identical receipt ordinals route to different pinned facts. Identical
authorization graph keys remain separate publication assertions. Date-excluded
entries retain source evidence; absent conduit links remain absent. The new CLI
matches the public reader and reproduces the expected identity. Unit tests also
cover undated receipt/conduit entries, malformed scope, duplicate contexts,
owned results, cancellation and completion changes. Full regression, targeted
race/static checks and the full-chain integration race test pass.

The [test Compose stack](../../docker-compose.window-test.yml) has no host ports,
proxy connection, development volumes or real credentials. Its isolated network
uses an unauthenticated fixture-only ArangoDB with a 2 GiB container cap and
1 GiB detected-memory override; the Go race-test runner has a 4 GiB cap and
2 GiB heap target. These are temporary test limits, not new standing services.
The [runner](../../scripts/test-window-integration.sh) uses a unique Compose
project, retains logs and explicit `tests.exit`/`run.exit` markers under its
printed temporary directory, and removes only that project's containers and
generated volumes. Both markers must be zero. Ordinary `go test ./...` runs
the synthetic source-contract check but skips this database integration test.

The opt-in `TestWindowLiveGate` opens exact retained inputs, selects dated
non-self source witnesses automatically, and checks unfiltered, witness-day and
following-day queries for both ledgers. Independent complete compact-artifact
decoding checks every date-population count. Returned dates must match the
source-verified graph documents. A fresh process must reproduce the exact result.

The committee-only 2024 gate passes. A second real cycle is needed before claiming
real cross-cycle integration; another bulk acquisition or full four-cycle rollout
is not a prerequisite. The connection interface's separate
[retained 2024 witness/replay gate](../audit/funding-window-connections-2026-09-13.md)
also passes. The Schedule E member extension passes the synthetic full-chain
gate and its [retained 2024 census/replay gate](../audit/funding-window-spending-2026-09-14.md). Identity
resolution and reporting-period semantics remain separate boundaries. Keep the required
[interpretation review](./pre-attribution-review.md) before terminal definitions
and dollar allocation.

`TestWindowConnectionLiveGate` opens explicit pinned inputs, independently decodes
the complete selected A/B compact artifacts for date counts, and selects receipt,
committee/candidate and conduit witnesses from published evidence. It checks date
inclusion/exclusion, hop bounds, exact source routing, authorization's unknown
validity and separate historical facets. A fresh process reproduces the exact
result. Failure fixtures reseal altered results so the checker must reject wrong
values independently of digest equality. The
[runner](../../scripts/run-window-connection-gate.sh) retains explicit completion
markers and artifact checksums. The audit distinguishes this Go-reader gate from
CLI fixture coverage and documents its limits; no production reader rule changed.

## Receipt and candidate connections

`inspect-funding-window-connections` calls `WindowReader.ConnectionPaths` under
`legal-tender.funding-window-connections.v1` and
`fec/dated-receipt-and-authorization-connections@1.0.0`. It reuses the exact
input specification, loader, date selector and bounded simple-path search.
The committee-only command and its result contract remain unchanged.

Start with either `--from-committee`, or all of `--receipt-generation`,
`--receipt-ordinal` and `--entry-family`. The generation is its exact ID from the
input set, not a cycle label or database name. Receipt ordinals are only unique
within their source population. The entry family is `reported_receipt` or
`conduit_association`; both reuse the existing source-backed receipt reader.
This selects one source appearance, not a resolved person or all of their receipts.

The nullable receipt date comes from the existing typed Parquet date column in
the fully verified source occurrence. No new raw-date parser or fallback is
introduced. Apply the same inclusive window to this entry's underlying receipt
and the chosen A/B observations. A conduit entry uses the receipt's date as an
observation filter; it does not acquire a validity date or another amount.
Excluded or unavailable entries return their source evidence and explicit state
without a path. Entry disposition is separate from the full selected-ledger census.

A committee target needs no ending. A candidate target requires
`--ending-family candidate_authorization_context`. Keep every matching authorized
assertion from the supplied publications, with its original linkage fact
membership. These assertions do not provide day-level validity. They remain
explicitly `source_publication_context_day_level_validity_unknown`, with no
invented date, and are not filtered using the source cycle. The result therefore
shows contextual connections, not proof of authorization during the date window.

Every returned link ID is qualified by generation. Original graph keys and
endpoints remain unchanged in `original_topology`; source reads use those original
keys in the originating verified graph. This matters because authorization keys
identify endpoint pairs and can repeat across publications. Different publication
assertions may produce separate evidence-path variants; they are not additional
payments. Entity facets retain separate receipt and committee-flow assertions from
each input. Source appearances remain source-qualified, with no identity merge.

```text
legal-tender pipeline fec inspect-funding-window-connections
  --inputs <spec.json> --expected-inputs-sha256 <sha256>
  --storage-root /storage --endpoint <arango-url>
  --password-env ARANGO_PASSWORD
  --receipt-generation <generation-id> --receipt-ordinal <ordinal>
  --entry-family reported_receipt --ledger schedule_a
  --target <candidate-id> --ending-family candidate_authorization_context
  --start-date 2023-07-01 --end-date 2024-06-30
  [--max-committee-hops 4 --max-paths 3 --max-expansions 10000]
  [--expected-result-id <sha256>]
```

Entry and candidate context do not consume committee hops. Candidate ending
checks do consume the existing search expansion budget. Selected A/B observations
retain the original combined cap; authorization scanning has a separate
two-million-context guard. These are resource guards, not demonstrated capacity.
Results bind the executable, exact generations, query, coverage, source evidence
and limits, with completion checks before and after each query. No graph writes,
amount aggregation, terminal definition or allocation is introduced.

Receipt/authorization queries retain the v1 result schema and policy. Spending
queries use the additive v2 contract below. The existing generation-bound
Schedule E aggregate interface remains unchanged.

## Shared-conduit generation inputs

The loader now accepts the [shared-conduit extension](./shared-conduit-generation.md)
without replacing its base generation or reimporting any graph. Use input-spec
version `legal-tender.funding-window-inputs.v2` whenever any input is extended:

```json
{
  "version": "legal-tender.funding-window-inputs.v2",
  "inputs": [{
    "generation": "/storage/.../extended-generation.json",
    "generation_sha256": "<outer-file-sha256>",
    "graph_manifest": "/storage/.../base-receipt-graph/manifest.json",
    "participants": "/storage/.../participants/manifest.json",
    "conduits": "/storage/.../original-conduits/manifest.json",
    "shared_conduits": {
      "base_generation": "/storage/.../original-generation.json",
      "graph_manifest": "/storage/.../shared-graph/manifest.json",
      "conduits": "/storage/.../shared-calculation/manifest.json"
    }
  }]
}
```

All three extension locators are required together. V1 specifications cannot
silently acquire extensions; v2 requires at least one. Different source cycles
may supply base or extended generations. Base plus extension of the same cycle,
duplicate inputs and overlapping A/B populations fail before graph opening.
The original resource guards remain unchanged and count the base A/B population
once. They do not establish capacity for several real extended inputs.

Metadata preflight shares the exact base/outer file and identity checks with the
path/neighborhood opener. Every accepted input then opens through that source and
graph verifier. The outer generation identifies receipt entries, links, contexts,
date-coverage rows and facets. The original base ID is not an alias. In returned
`inputs`, `generation` preserves the unchanged base metadata; the optional
`shared_generation` contains the complete outer envelope and physical extension
binding. The enclosing `generation_id` and checksum identify that outer envelope.
All returned nested metadata is owned, not a mutable view into reader state.

`inspect-funding-window-connections --entry-family shared_conduit_association`
uses the original receipt's typed nullable date through the existing date reader.
The related memo's date, acquisition cycle, filing timestamp and publication time
never fill it. Known dates use the unchanged inclusive range; undated entries are
retained in unbounded queries and excluded explicitly in bounded queries. Excluded
entries still receive full source/graph checks and retain evidence. They are not
an excuse to ignore a damaged shared edge.

The entry retains original/new decisions, complete-group evidence, full original
receipt and full related memo. A separate `shared_conduit_facet` preserves extension
committee context. The original `conduit_association` family keeps its old meaning.
Committee observations and authorized or dated spending endings use their original
readers, source dates and physical keys. No amount, identity, relationship-validity
or terminal rule changes. Existing base-only result shapes remain unchanged.

The [shared-window gate](../audit/shared-conduit-windows-2026-09-14.md) records real
acceptance. Full-chain two-publication fixtures exercise source/date routing,
null dates with dated memos, both committee ledgers, all candidate endings,
outer-qualified identities, overlap rejection, reversed input order, owned metadata,
CLI identity and corrupted excluded edges. The real gate independently decodes
complete selected A/B artifacts for the date census, then tests automatically
selected shared entries and a dated committee continuation when available.
It does not measure all receipt dates or establish real cross-cycle coverage.

## Source-grain Schedule E connections

`inspect-funding-window-connections` also accepts `--ending-family
independent_support` or `independent_opposition`, with a required
`--spending-date-field expenditure|dissemination`. This selects the normalized
source field `expenditure_on` or `disseminated_on`, respectively. It never fills
one field from the other, a cycle label, filing date or publication timestamp.
Bounded windows exclude unknown selected dates. Unbounded queries retain them.
Both original dates remain in the returned source evidence.

Spending queries emit `legal-tender.funding-window-connections.v2` and policy
`fec/dated-schedule-e-source-connections@1.0.0`. Their link families are
`independent_support_observation` and `independent_opposition_observation`.
Each link is keyed by one source fact ID and qualified by its generation. It is
a verified source-member relationship, not a new per-fact document in ArangoDB.
The existing aggregate remains the explicit parent with its original amount,
count, topology, resolution breakdown and input identities. That parent amount
is neither the member amount nor a window total.

The [candidate-resolution reader](../../internal/calculation/fec/candidateresolution/read_members.go)
replays the publisher's existing effective and resolution functions against the
exact retained Schedule E and candidate-master ancestry. It compares every
decision, in order, with the published decision artifact and verifies full
membership, counts and signed amounts. No new memo, amendment or identity rule
is introduced. Facts outside candidate resolution are also visited.

The [resolved graph member reader](../../internal/projection/arango/independentexpenditures/dated_members.go)
then checks every group's member count, positive/negative/zero counts, resolution
states and exact signed amounts against the pinned projection. Date selection
never bypasses these checks. The query retains compact descriptors, then reads full
source evidence for returned members in one additional batch scan per relevant
publication, not one scan per path. Each selected parent is read back from Arango.
Vertices retain outside-spending facets from every supplied publication alongside
their receipt and committee-flow facets.

`spending_source_coverage` accounts for every Schedule E fact in each input, not
just the requested candidate or stance. Buckets separate effective states,
amount/route exceptions, candidate-resolution states, stance, date disposition
and projectability. Known amount counts distinguish null amounts from zero;
signed minor-unit strings remain exact. These diagnostic sums are not candidate
receipt totals, path allocations or reconciled payments. The evidence boundaries
remain those in the [FEC flow contract](./fec-flow-fact-requirements.md).

Resource guards allow at most two million Schedule E facts per replay and across
the window's supplied inputs, separately from the existing A/B topology guard.
The source replay holds duplicate identities, not whole facts. Only returned
witnesses retain full source rows. Exceeding a guard fails the query; it never
truncates source validation. These are circuit breakers, not measured real-corpus
capacity. Opening, complete source scans and evidence readback remain outside
the path search's expansion budget.

The [full-chain fixture](../../internal/integration/fundingwindow/spending_test.go)
publishes synthetic 2022/2024 inputs through the real parsers, calculations,
bundles, graphs and generation loader. It checks multiple dated members in one
aggregate, separate stances, unknown dates, memo/null/route/candidate exceptions,
negative and zero amounts, resolved/unverified candidate decisions, qualified
receipt starts, reversed-input fresh open, CLI replay and corrupt excluded-source
rejection. `make test-window-integration` passes with race detection. The
[retained 2024 gate](../audit/funding-window-spending-2026-09-14.md) now passes
complete independent source/date census, automatic direct/upstream/receipt
witnesses and byte-identical fresh-process replay. It records runtime and memory
pressure without claiming low-latency serving or larger-window capacity. No new
real data has been loaded and no existing graph has been rewritten.

Verification on 2026-09-14: full `go test ./...`, targeted race checks for the
resolution/projection/window/CLI packages, and `go vet ./...` pass. Disposable
integration evidence is under `/tmp/legal-tender-window-test.tAuauuDr/`;
regression markers are under `/tmp/legal-tender-spending-regression.EyXJbQu5/`.
Both runs exited zero without OOM. These temporary test logs are not a retained
real-data acceptance publication.

The retained gate uses `TestWindowSpendingLiveGate` and
[`run-window-spending-gate.sh`](../../scripts/run-window-spending-gate.sh).
It independently decodes source facts, candidate decisions and aggregate parents;
compares complete coverage and exact returned source members; and reuses the
shared receipt/A/B path checks. Result-corruption fixtures recompute result IDs
and still fail on altered evidence. The executable, source snapshot, inputs,
results, measurements and success markers are retained together. See the audit
above for exact identifiers and reproduction requirements.
