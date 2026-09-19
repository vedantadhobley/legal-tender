# Legacy election-cycle semantics

> **Excavation date:** 2026-08-27  
> **Scope:** How the Python system assigns, partitions, calculates, aggregates,
> refreshes, and presents FEC election cycles.  
> **Target-design authority:** None. Desired cycle behavior is recorded in the
> product contract and question catalog.

## Purpose as implemented

The legacy system analyzes four FEC two-year election-cycle datasets:

```text
2020, 2022, 2024, 2026
```

It stores parsed FEC data in one ArangoDB database per cycle, builds graph edges
with a `cycle` property, calculates candidate funding for each cycle, and then
sums those results into an all-active-cycles aggregate.

The four-cycle scope was intended to support an approximately eight-year view
without making every query operate over all historical FEC data.

## Evidence state

- `CODE` — the active list is the tuple `ACTIVE_CYCLES` in `src/config.py`.
- `CODE` — most assets default their `cycles` configuration from that tuple.
- `CODE` — no Dagster asset has a partition definition. Assets loop over cycle
  strings inside their Python compute functions.
- `OBSERVED` — raw archives and parser dumps exist for all four configured
  cycles.
- `OBSERVED` — the 2026 source snapshot is incomplete as a finished election
  cycle. The 2026 `weball` records contain varied coverage end dates; most in
  the observed archive end on 2026-03-31.
- `UNCLEAR` — the exact aggregation database state cannot be observed because
  the Legal Tender containers are not running.

## Cycle concepts present in the sources

The legacy system receives several distinct time concepts:

| Concept | Example field or location | Meaning in the legacy data |
|---|---|---|
| Bulk-file cycle | `raw/2024/indiv.zip` | FEC source dataset selected by ending election-cycle year |
| Transaction date | `TRANSACTION_DT` | Date printed on an itemized transaction row |
| Candidate election year | `CAND_ELECTION_YR` | Election year reported for a candidate |
| FEC linkage year | `FEC_ELECTION_YR` | Active two-year period on a candidate-committee linkage |
| Summary coverage end | `CVG_END_DT` | Through-date represented by a published FEC summary row |
| Materialization time | `updated_at`, `computed_at` | When the Python pipeline wrote a document or projection |

`CODE` — the derived graph uses only the bulk-file cycle. It does not derive or
validate edge cycle from `TRANSACTION_DT`, `CAND_ELECTION_YR`,
`FEC_ELECTION_YR`, or `CVG_END_DT`. Those fields remain available in parsed
documents but are not part of graph-edge construction or candidate grouping.

The source-cycle label and transaction date usually align by construction of
the FEC bulk files. The implementation does not enforce that assumption or
surface exceptions.

## Active-window definition

`ACTIVE_CYCLES` is a static ordered tuple. It is not calculated from the
current date, available source releases, or a window length.

Consequences:

- “Last four cycles” is true only while a developer manually maintains the
  tuple as a four-item rolling window.
- Adding 2028 does not automatically remove 2020.
- Removing 2020 does not delete its raw data or all derived vertices.
- A five-cycle tuple would silently change every cross-cycle aggregate into a
  five-cycle aggregate.
- The active window has no identifier of its own and is not stored with most
  projections beyond the list of cycles that happened to produce data.

`CODE` — per-election donor limits are stored in a second dictionary keyed by
cycle. A missing key silently falls back to the 2024 limit of `$3,300` in the
donor asset.

## Source and parsed partitioning

### Filesystem and ArangoDB

Raw and parsed data have real cycle isolation:

```text
raw/<cycle>/<source>.zip
dumps/fec/<cycle>/<collection>.jsonl.gz
fec_<cycle>.<collection>
```

`CODE` — parsers can receive a configured subset of cycle strings. Each
selected per-cycle collection is truncated or restored independently. A parser
run for 2026 does not mutate `fec_2024`.

This is the strongest cycle boundary in the legacy system.

### Source assignment

`CODE` — every parsed row inherits its cycle from the source database selected
by the asset loop. The parsed transaction document itself does not receive an
added `cycle` field because its containing database supplies that context.

The graph assets later attach the loop's cycle string to derived edges.

## Shared vertex behavior

### Candidates and committees

`CODE` — `contributed_to` copies every selected cycle's `cn` and `cm` documents
into shared `aggregation.candidates` and `aggregation.committees` collections.
Identity is the FEC `CAND_ID` or `CMTE_ID`.

For an ID present in several cycles:

- One shared document survives.
- Its `cycles` array lists the selected cycles where the ID appeared.
- Common master fields are overwritten in configured cycle order.
- With the default ascending order, the latest selected cycle wins.
- Per-cycle master-field values are not retained on the shared vertex.

Thus a candidate's current party, election year, office metadata, name, and
principal committee can be displayed beside historical-cycle money even when
those values differed in the historical source file.

`CODE` — these shared collections are not truncated during rebuild. Documents
that exist only in a cycle removed from the active window are not deleted.
They can remain as stale or isolated vertices after the corresponding edges
are rebuilt without that cycle.

### Donors

`CODE` — donors are rebuilt from scratch across all configured cycles. One
vertex is keyed by normalized `NAME|EMPLOYER`. The vertex sums amounts and
counts from each cycle in which that identity qualifies, and stores a `cycles`
array without per-cycle amounts.

Cross-cycle upserts run concurrently. When spelling differs between cycles,
the first insert creates `canonical_name` and `canonical_employer`; later
updates change totals and cycles but not those labels. The surviving spelling
can therefore depend on concurrent execution order.

### Employers and corporate families

Employer totals, canonical employer groups, donor classifications, Wikidata
resolution, and corporate families are built from the shared donor collection.
They are active-window projections. They do not preserve a separate value or
resolution for each cycle.

## Cycle-dependent donor qualification

The donor asset correctly applies the configured contribution limit separately
inside each cycle. It selects donor identities whose amount to at least one
committee reaches that cycle's threshold.

The next stage changes the semantics:

1. All qualifying donor keys from every active cycle are merged into one
   shared donor collection.
2. `contributed_to` then rereads every active cycle.
3. It emits that donor's contributions in a cycle whenever the key exists in
   the shared donor collection.
4. It does not require the donor to have qualified in the cycle of the edge.

`BUG?` — a donor who qualifies in 2026 can therefore have a sub-threshold 2020
contribution treated as graph-traced whale money. If 2026 is removed from the
active window and the graph is rebuilt, the same 2020 contribution may cease
to be a whale edge. A supposedly per-cycle result depends on the other cycles
included in the active window.

`BUG?` — `candidate_funding` introduces another window-level condition by
loading donor names and employers only for shared donor vertices whose
cross-cycle total is at least `$10,000`. Lower-total qualifying donor edges keep
their amounts but degrade to opaque donor-key identities during attribution.

## Cycle-specific graph edges

The principal money and affiliation edges are cycle-specific:

| Edge | Cycle assignment | Key includes cycle |
|---|---|---|
| `contributed_to` | Source `indiv` database loop | Yes |
| `transferred_to` | Source `pas2` or `oth` database loop | Yes |
| `spent_on` | Source `pas2` database loop | Yes |
| `affiliated_with` | Source `ccl` database loop | Yes |

The graph named `political_money_flow` has no cycle constraint. A traversal
must explicitly filter edges by cycle if it wants a cycle-consistent result.
The graph definition itself permits paths that mix edge cycles.

`CODE` — `candidate_funding` partitions edge lookups by the edge's `cycle`
field before tracing, so its main per-cycle calculation avoids mixed-cycle
money paths.

`BUG?` — when a `transferred_to` or `contributed_to` edge has no cycle,
`candidate_funding` silently assigns it to 2024. Missing temporal data is not
rejected or marked unresolved.

## Committee receipts by cycle

`CODE` — `committee_receipts` calculates individual, transfer, whale,
grassroots, self-funding, earmark, and total-receipt fields independently for
each configured cycle. It writes those values under
`committees.receipts_by_cycle[cycle]` and then sums them into cross-cycle fields
on the shared committee vertex.

The calculation correctly uses cycle-matched source summaries and cycle-tagged
graph edges. Two cross-cycle leaks remain:

- `whale_donor_total` comes from `contributed_to`, whose donor inclusion set is
  window-dependent as described above.
- Classification later consumes the aggregate `earmarked_share` and aggregate
  receipts rather than a cycle-specific classification state.

`CODE` — a subset run is destructive for updated committees. The asset builds
a new `receipts_by_cycle` object only from the configured subset and replaces
the existing object through `mergeObjects: false`. Running it for only 2026 can
remove 2020-2024 receipt blocks from committees touched in 2026, while leaving
untouched committees with stale multi-cycle blocks.

## Terminal classification across cycles

`CODE` — a committee has one mutable `terminal_type`, not one classification
per cycle or validity interval.

The base classification reads the latest-cycle-wins `CMTE_TP`, `ORG_TP`, and
connected-organization fields on the shared committee. Later rules use
cross-cycle aggregate receipts and earmark share, current external resolution,
and name or connected-organization inheritance.

Candidate attribution then copies that one current `terminal_type` into every
cycle's committee lookup. Historical transfers are traced using the current
classification even if the committee's source fields or behavior differed in
the historical cycle.

This is a cycle leak at a calculation-controlling boundary: the same 2020
transfer can change terminal attribution after a later cycle changes the
committee's shared classification.

## Candidate funding calculations

### Per-cycle phase

`CODE` — `candidate_funding` uses the fixed module-level `CYCLES` list and has
no runtime cycles configuration. For each candidate and cycle it:

1. Finds campaign committee affiliations whose edge has that cycle.
2. Selects only committee types `H`, `S`, or `P` from the linkage edge.
3. Uses contribution, transfer, and independent-spending edges from the same
   cycle.
4. Uses receipt totals from the same `receipts_by_cycle` entry.
5. Applies global donor identity/corporate resolution and global committee
   terminal classification.
6. Stores the resulting channels under `funding_channels.by_cycle[cycle]`.

The monetary edge selection and denominators are cycle-specific. Identity and
classification inputs are not fully cycle-specific.

### Cross-cycle phase

`CODE` — `merge_funding_channels` creates the `aggregate` projection by summing
the available per-cycle monetary results. Percentages are recalculated against
the summed total. The aggregate is nominal dollars; it performs no inflation
adjustment or constant-dollar normalization.

The candidate output stores `cycles_available`, but it does not store:

- The configured active-window definition or requested query window.
- Whether a cycle was current and incomplete.
- Each source's coverage-through date.
- A snapshot ID binding the four cycle inputs.
- An inflation basis.

`BUG?` — per-cycle named lists are thresholded and truncated before the
cross-cycle merge. An organization below top-N in each individual cycle can be
absent from the aggregate list even when its four-cycle sum would make it a top
aggregate source. Totals still include the amount.

## Other summary projections

Cycle behavior is inconsistent outside `candidate_funding`:

- `committee_summaries` stores per-cycle receipt totals, but its top donors,
  top source committees, top recipients, and whale tiers sum edges across all
  cycles without a cycle label.
- `donor_summaries` stores per-cycle donation totals, but its top committees,
  top candidates, and political lean sum across all cycles. Candidate joins do
  not require the contribution edge and affiliation edge to share a cycle.
- `candidate_summaries` reads affiliations and receipts across all cycles into
  one cycle-blind recursive summary. Its configured `cycles` value does not
  constrain those queries.
- `member_fec_mapping` defaults to only 2024 and 2026 rather than the shared
  four-cycle constant, then deduplicates candidate and committee IDs across
  those cycles without retaining the cycle association.
- `validation_report.py` hardcodes the four cycle strings separately from
  `ACTIVE_CYCLES`.

These are separate legacy presentation projections. They do not share one
enforced time contract.

## Orchestration and recomputation

`CODE` — there are no Dagster partitions for election cycle, source release,
or candidate. The full weekly job selects every asset. Each asset implements
its own internal cycle loop.

Operational effects:

- One changed 2026 source triggers the same full asset graph as any other
  change.
- Unchanged parser collections can restore from dumps, but the shared donor,
  employer, edge, classification, receipt, and candidate projections rebuild
  globally.
- Every candidate is recomputed even when only a small set of committees or
  source records changed.
- Dagster cannot address a cycle as an asset partition for targeted retry,
  backfill, freshness, or lineage.
- Asset configuration can select a cycle subset, but shared aggregate assets
  truncate or replace global state and are not safe incremental partitions.

This over-processing is an orchestration property, not a requirement of the
four-cycle product view.

## Failure and retry behavior

Parsers catch many cycle-level failures and can complete with a subset of
cycles. Later assets skip absent databases or collections. There is no global
snapshot gate requiring all sources for all active cycles to be fresh and
successful before publication.

Candidate funding includes only cycles that produce a nonempty calculation.
The aggregate silently sums that available subset. `cycles_available` exposes
which cycle keys survived, but not why another active cycle is missing.

A retry of a shared aggregate asset is generally a complete rebuild. A retry
with a reduced cycle configuration can produce a different, partially
destructive state.

## Invariants actually enforced

The legacy system enforces:

- Separate parsed databases per configured bulk-file cycle.
- A cycle property on the principal graph edges.
- Cycle-matched money edges and receipts in `candidate_funding`.
- Per-cycle candidate funding output followed by a numeric sum.

It does not enforce:

- A dynamically rolling four-cycle window.
- Cycle derived from or checked against transaction and coverage dates.
- Cycle-scoped candidate and committee master attributes.
- Cycle-scoped terminal classification or corporate resolution.
- Cycle-local donor qualification throughout the graph.
- Cycle-consistent arbitrary graph traversal.
- Complete-cycle versus partial-current-cycle presentation.
- Safe cycle-partition retries or incremental recomputation.
- Eviction of vertices from a removed cycle.
- One shared time contract across all summary assets and scripts.

## Finding register

| ID | Label | Finding |
|---|---|---|
| `CYCLE-001` | `CODE` | The four-cycle window is a static tuple, not a rolling rule. |
| `CYCLE-002` | `CODE` | Parsed FEC databases are genuinely isolated by source cycle. |
| `CYCLE-003` | `CODE` | Graph cycle comes from the source partition, not transaction or coverage fields. |
| `CYCLE-004` | `CODE` | Principal graph edges retain cycle, but the named graph does not constrain it. |
| `CYCLE-005` | `CODE` | Shared candidate and committee vertices keep latest-cycle master fields. |
| `CYCLE-006` | `CODE` | Removing a cycle does not evict vertices unique to that cycle. |
| `CYCLE-007` | `BUG?` | Donor qualification in one active cycle affects graph inclusion in every active cycle. |
| `CYCLE-008` | `BUG?` | Donor identity detail uses a cross-window `$10,000` filter. |
| `CYCLE-009` | `CODE` | Receipt blocks are per-cycle, then summed on the committee vertex. |
| `CYCLE-010` | `CODE` | One current terminal classification controls all historical-cycle traces. |
| `CYCLE-011` | `CODE` | Candidate money calculations are per-cycle before aggregation. |
| `CYCLE-012` | `BUG?` | Per-cycle top-list truncation can omit a true cross-cycle top source. |
| `CYCLE-013` | `CODE` | Cross-cycle monetary aggregates use nominal dollars. |
| `CYCLE-014` | `BUG?` | Current-cycle partial coverage is not represented in the result contract. |
| `CYCLE-015` | `CODE` | Dagster has no cycle partitions; all partitioning is inside assets. |
| `CYCLE-016` | `BUG?` | Subset configuration is unsafe for shared aggregate assets. |
| `CYCLE-017` | `DRIFT` | Secondary summary assets use incompatible cycle scopes. |
| `CYCLE-018` | `BUG?` | Missing edge cycles silently default to 2024 during attribution. |
| `CYCLE-019` | `CODE` | The member mapping uses only 2024 and 2026 by default. |
| `CYCLE-020` | `CODE` | The validation script separately hardcodes the same four cycles. |

## Representative scenarios

### Donor qualifies only in a later cycle

A normalized donor gives `$500` in 2020 and reaches the configured threshold in
2026. With all four cycles active, both the 2020 and 2026 donations receive
`contributed_to` edges. With only 2020 active, that donor has no graph vertex
and no 2020 edge. The 2020 whale/grassroots split changes even though the 2020
source data did not.

### Committee changes source attributes

A committee appears in 2020 and 2026 with different master fields. The shared
committee vertex takes the 2026 values. Classification runs once on that shared
state. Candidate funding uses the resulting type for both 2020 and 2026
transfers.

### Current cycle is incomplete

The 2026 archive is refreshed during 2026 and its summary rows have different
coverage-through dates. Candidate `by_cycle["2026"]` and the four-cycle
aggregate do not expose those dates or label 2026 as partial.

### Rolling the window forward

Changing the tuple to 2022, 2024, 2026, and 2028 and rebuilding removes 2020
money edges, but candidate and committee vertices unique to 2020 can remain.
All donor, employer, terminal-attribution, and cross-cycle aggregates can
change because the active-window population changed.

## Primary evidence

- [Shared cycle configuration](../../../src/config.py)
- [Dagster jobs without partitions](../../../src/jobs/asset_jobs.py)
- [Per-cycle donor qualification](../../../src/assets/graph/donors.py)
- [Shared vertices and contribution edges](../../../src/assets/graph/contributed_to.py)
- [Cycle-specific transfer edges](../../../src/assets/graph/transferred_to.py)
- [Cycle-specific independent-spending edges](../../../src/assets/graph/spent_on.py)
- [Cycle-specific affiliations](../../../src/assets/graph/affiliated_with.py)
- [Per-cycle committee receipts](../../../src/assets/enrichment/committee_receipts.py)
- [Global terminal classification](../../../src/assets/enrichment/committee_classification.py)
- [Per-cycle candidate calculation and aggregate](../../../src/assets/aggregation/candidate_upstream.py)
- [Cycle-blind candidate summary](../../../src/assets/aggregation/candidate_summaries.py)
- [Partially cycle-scoped committee summary](../../../src/assets/aggregation/committee_summaries.py)
- [Partially cycle-scoped donor summary](../../../src/assets/aggregation/donor_summaries.py)
- [Two-cycle member mapping](../../../src/assets/mapping/member_fec_mapping.py)
