# Calculation contracts: authorized itemized receipts

> **Status:** Accepted machine contract and Go publisher. The direct probe
> closed the calculation-semantics and conservation gate but rejected the per-
> row JSON fact/decision representation. The columnar fact, compact occurrence,
> and compact calculation layouts then passed complete-corpus gates. The
> compact publisher reproduced the exact direct-probe result while storing
> only exceptional membership. The immutable four-input fact bundle and
> bundle-fed calculation replay also passed their real 2024 gates. This
> document defines the first
> calculation over the
> [`normalized evidence model`](./evidence-model.md). It does not define total
> candidate-controlled receipts or terminal-source attribution. It governs the
> processed view only. Raw-filing report-family, effective Schedule A, and
> reconciliation research is deferred from initial production. A later
> as-filed product requires a separate accepted source and calculation
> boundary.

## Question answered

For one candidate and one FEC two-year cycle:

> Which processed Schedule A rows did the FEC classify as itemized individual
> receipts for the candidate's authorized committees, what signed amount do
> those rows sum to, and how does that component compare with separately
> published candidate summaries?

The answer remains narrower than “how much money did the candidate control?”
Transfers, loans, candidate contributions, other receipts, offsets, refunds,
and non-Schedule-A activity need later contracts.

## Calculation principles

1. Preserve first; select later.
2. Use the FEC processed Schedule A snapshot as the publisher's then-current
   processed row set. Do not rebuild its amendment logic locally.
3. Make every inclusion and exclusion rule explicit and versioned.
4. Parse and sum source-reported signed cents losslessly. This is exact
   arithmetic over reported points, not a claim that the public record captures
   every economic event.
5. Keep source totals separate from computed totals.
6. Never fill missing detail with a summary amount.
7. Never call a residual “unitemized,” “unknown,” or “grassroots” unless the
   source or a later accepted calculation establishes that meaning.
8. Preserve included, excluded, and unresolved fact membership for drilldown.

## Contract A: processed receipt ledger

### Identity

```text
contract: fec.processed_schedule_a_ledger.v1
key: cycle + Schedule A snapshot ID
```

### Input

All `fec.schedule_a_receipt.v1` facts present in one published processed
Schedule A snapshot partition for the requested cycle.

### Output

The ledger contains every valid and invalid normalized fact with no semantic
filter. It reports:

- Total occurrences, unique facts, and duplicate publisher references.
- Valid, invalid, and unresolved fact counts by issue code.
- Positive, negative, zero, null, and invalid amount counts.
- Counts by action code, entity type, FEC `is_individual`, receipt type, form,
  line number, memo code, and report type.
- The source snapshot, schema, parser, and result versions.

This is the source-facing investigative ledger. It can publish even when a
downstream monetary projection is partial, provided every source occurrence is
accounted for.

### Blocking checks

The monetary projection cannot publish when:

- Two distinct current facts claim the same non-null Schedule A `SUB_ID`.
- The source cycle and `two_year_transaction_period` conflict.
- Occurrence-to-fact conservation fails.
- The source schema changed without an accepted mapping.

The ledger still retains the conflicting evidence and reports why the
projection is blocked.

## Contract B: FEC itemized-individual receipt projection

### Identity

```text
contract: fec.itemized_individual_receipts.v1
key: cycle + Schedule A snapshot ID + calculation version
```

### Publisher semantics accepted

The FEC's processed Schedule A table contains an `is_individual` field produced
by its published classification methodology. Legal Tender accepts that field
as a publisher classification for this projection and records the methodology
reference with the calculation version.

The openFEC model defines `memoed_subtotal` as `memo_code == "X"`. Legal Tender
reproduces that exact boolean. `X` rows remain in the ledger and receipt APIs,
but they do not enter the FEC-style subtotal.

These rules are source-specific. They are not a general claim that every
`ENTITY_TP=IND` row is a person, that every memo row is duplicate evidence, or
that the same rules apply to another publisher.

### Per-fact decision

Each Schedule A fact receives one calculation decision:

| State | Rule | Amount contribution |
|---|---|---:|
| `included` | `is_individual == true`, `memo_code != "X"`, amount valid | Signed `amount_cents` |
| `excluded_non_individual` | `is_individual == false` | 0 |
| `excluded_memo_subtotal` | `is_individual == true`, `memo_code == "X"` | 0 |
| `unresolved_individual_class` | `is_individual` is null or invalid | unknown |
| `unresolved_amount` | Otherwise included but amount is null or invalid | unknown |
| `blocked_duplicate_reference` | Duplicate current `SUB_ID` | calculation blocked |

Order matters. A valid non-individual fact is excluded even if its amount is
invalid, because it cannot enter this projection. An individual-classified
fact with an invalid amount remains unresolved rather than becoming zero.

The output records the decision, reason code, calculation version, and fact ID.
It does not write a counted flag onto the normalized fact.

All monetary outputs follow the shared
[money-measure contract](./money-measures.md). FEC Schedule A amounts in this
contract are `reported_point` observations unless a future source rule proves a
different measurement kind.

The broader [FEC money and time mapping](./fec-money-semantics.md) governs
itemization coverage, unitemized summary points, estimates, valuations, debts,
and source clocks that this first calculation does not yet consume.

### Amounts and counts

For the included fact set `I`:

```text
reported_itemized_individual_cents = sum(f.amount_cents for f in I)
included_record_count              = count(I)
```

The projection also reports positive, negative, and zero included counts and
amounts. Negative rows reduce the signed total. Zero rows remain included
records with a zero monetary contribution. No separate refund netting occurs
in this contract.

When every calculation-controlling row is resolved, the signed sum produces a
`point` result. If unresolved individual or amount rows exist, the resolved
subtotal may be published as a separate reported-point component with
`coverage_state = partial`; the API must not present it as the complete
itemized-individual amount. Because missing rows can include negative
adjustments, the resolved subtotal is not automatically a lower bound on the
complete signed result. A duplicate current `SUB_ID` blocks publication rather
than guessing which row to count.

### Amendment handling

The processed Schedule A snapshot is already the FEC's processed data product.
Contract B counts only facts present in that snapshot. It does not:

- Sum prior Legal Tender snapshots.
- Select `A` over `N` or `N` over `A` by amendment or action code.
- Deduplicate by transaction ID, contributor, date, amount, or report type.
- Treat `SUB_ID`, original `SUB_ID`, or a back reference as proof of one
  economic event.

All amendment and filing fields remain drillable evidence. Snapshot changes
show how the publisher's processed view changed over time. A future raw-filing
contract may reconstruct filing chains without changing this calculation.

The [summary-value use policy](./summary-value-use.md) keeps report/summary
qualification separate from this processed ledger. Unresolved summary or
metadata evidence is not an added dependency of its existing graph projections.

### Memo, earmark, and conduit handling

Memo code `X` controls only FEC-style subtotal inclusion. Memo text does not.
The first local million-row sample contains `X` rows with several meanings, so
Legal Tender never drops them from evidence or treats them all as one semantic
category.

An earmarked individual receipt with `is_individual=true` and no `X` memo code
is counted once as reported to the filing committee. Its conduit ID, conduit
name, contributor fields, memo text, other ID, and back references remain on
the receipt fact. The calculation does not:

- Replace the contributor with ActBlue, WinRed, or another conduit.
- Add a second receipt for the conduit.
- Use a maximum of “direct” and “earmarked” totals.
- Infer a transfer or terminal source from a name substring.

Later graph contracts can connect related disclosures and trace their paths.
They must conserve the receipt amount and expose the evidence used to avoid
double counting.

## Contract C: candidate authorized-committee scope

### Identity

```text
contract: fec.candidate_authorized_committees.v1
key: candidate ID + cycle + ccl snapshot ID + calculation version
```

### Inclusion

A committee belongs to the candidate's authorized set when a valid `ccl` fact
for the same cycle links the candidate and committee with designation `A` or
`P`. All supporting linkage facts enter the relationship manifest.

The calculation does not infer authorization from committee name, committee
type, connected organization, a Schedule A candidate field, or a relationship
observed in another cycle.

Repeated compatible linkage facts produce one candidate-committee member with
several supporting facts. Conflicting designation evidence produces
`relationship_state = unresolved`; affected receipts remain visible but do not
silently enter the candidate total.

### Candidate receipt calculation

For the authorized committee set `C` and included receipt facts `I`:

```text
candidate_itemized_individual_cents =
    sum(f.amount_cents for f in I where f.recipient_committee_id in C)
```

The calculation output records:

- Candidate ID and cycle.
- Authorized, unresolved, and absent committee IDs.
- Linkage and receipt fact-set manifests.
- Included, excluded, and unresolved decision counts and amounts.
- Per-committee subtotals that conserve to the candidate subtotal.
- Snapshot, schema, method, code, run, and publication versions.

A receipt for a committee that lacks a valid same-cycle authorization path is
not reassigned. It remains in the source-level receipt-decision artifact but
does not enter a candidate subtotal. A conflicting relationship is recorded as
`unresolved`, and receipts routed through it increment the candidate's
`authorization_unresolved` count.

## Contract D: candidate summary reconciliation

### Source totals remain facts

FEC candidate summaries combine the candidate's principal campaign committee
and other authorized committees and publish coverage-through dates. Where the
source exposes itemized and unitemized individual fields separately, Legal
Tender preserves and displays both. The House/Senate current-campaign summary
publishes `TTL_INDIV_CONTRIB`, which is total individual contributions rather
than itemized detail.

No summary amount enters the Schedule A fact set. No summary value fills an
unresolved detailed subtotal.

### Comparison classes

For a summary with an explicit itemized-individual field:

```text
itemized_difference_cents =
    summary_itemized_individual_cents
    - candidate_itemized_individual_cents
```

For a summary that exposes only total individual contributions:

```text
individual_detail_gap_cents =
    summary_total_individual_cents
    - candidate_itemized_individual_cents
```

The second result is not called “unitemized.” It can include actual unitemized
receipts, timing differences, late processing, source-method differences,
unresolved facts, and target bugs. When a summary explicitly publishes an
unitemized field, that field is shown as a source fact; it is not inferred from
the gap.

### Coverage state

Every comparison reports one state:

| State | Meaning |
|---|---|
| `source_aligned` | The source contracts establish compatible as-of and coverage boundaries. |
| `date_bounded` | Detailed rows were bounded to the summary coverage end date, but publisher processing cutoffs may differ. |
| `not_comparable` | Cycle, coverage, source, or required field is absent or incompatible. |

For a date-bounded comparison, the detailed fact set includes receipt dates
inside the cycle and on or before the summary coverage end date. Invalid or
missing dates become unresolved. A numeric difference is diagnostic evidence,
not an automatic failure and never a fallback trigger.

The two summary datasets remain separate calculations. Legal Tender does not
silently choose one through an ordered fallback. The API labels the candidate
summary and the current House/Senate campaign summary by source, coverage, and
precision.

## Publication shape

The original fixture-tested publisher implements two content-addressed zstd JSONL
artifacts:

- [`decision.schema.json`](../../contracts/calculations/fec/candidate-itemized-individual-receipts/v1/decision.schema.json)
  records one decision, calculation version, fact identity, recipient
  committee, date, and included signed amount for every Schedule A fact.
- [`result.schema.json`](../../contracts/calculations/fec/candidate-itemized-individual-receipts/v1/result.schema.json)
  records one candidate/cycle component with its common money measure,
  committee relationships and subtotals, decision counts, independent source
  summaries, and reconciliations.

The immutable
[`manifest schema`](../../contracts/calculations/fec/candidate-itemized-individual-receipts/v1/manifest.schema.json)
pins both artifacts and the exact four input fact sets. Finite monetary values
serialize as signed decimal strings in minor units. Formatting happens only at
the presentation boundary.

The 2024 corpus probe rejected this per-row physical representation, not the
decision states or result schema. The accepted compact publisher identifies
ordinary membership through a versioned ordered predicate, exact input fact-
set identity, and one-based source row ordinal. It reads the nine declared
Parquet columns and materializes only unresolved or invalid membership under
[`compact/v1`](../../contracts/calculations/fec/candidate-itemized-individual-receipts/compact/v1/).
Candidate results retain the original result schema.

The complete 2024 publication produced the same included, excluded,
unresolved, committee, candidate, reconciliation, amount, and byte-identical
result artifact as the direct probe. It materialized two exception rows and
8,175 candidate results. See the
[compact calculation publication audit](../audit/compact-receipt-calculation-publication-2026-08-31.md).

## Related receiver-reported committee-flow contract

The authorized-individual calculation above does not define PAC-to-PAC flow.
The separate accepted
[`fec/receiver-reported-committee-flows@1.0.0`](../../contracts/calculations/fec/receiver-reported-committee-flows/v1/contract.json)
contract reuses the immutable Schedule A columnar facts at a different grain.
It requires exact agreement between raw and cleaned committee IDs and routes
only exact inbound receipt roles. In-kind, transfers, contributions, and
received refunds remain distinct roles. Outbound, semantic-memo, earmarked,
noncommittee, and unknown roles never enter an edge implicitly.

The complete 2024 scan accepted 320,731 occurrences and 180,283 grouped edge
candidates while preserving unknown roles and identities separately. The Go
policy, publisher, loader, CLI, schemas, fixtures, and Dagster asset are
executable. The complete publication conserves all rows and signed cents, and
an unchanged replay returns the byte-identical manifest. The graph projection
remains unimplemented. See the
[cohort audit](../audit/receiver-reported-committee-flow-cohort-2026-08-31.md)
and [publication audit](../audit/receiver-reported-committee-flow-publication-2026-09-01.md).

## Incremental effect

A changed Schedule A fact affects:

1. Its cycle ledger.
2. Its receipt decision.
3. Its recipient committee subtotal.
4. Candidates linked to that committee in the same cycle.
5. Reconciliations for those candidates.

A changed `ccl` fact affects the two endpoint entities and the linked
candidate's authorized set and receipt projection in the same cycle. A changed
summary fact affects only its candidate reconciliation. A method-version change
can intentionally rebuild all projections using that method without fetching
new source data.

## Required fixtures

The executable contract must cover at least:

- Included positive, negative, and zero individual receipts.
- A non-individual receipt.
- Memo code `X` with ordinary memo text.
- An earmarked receipt with memo text but no `X` code.
- Null `is_individual` and invalid amount.
- Original, amendment, and terminated action codes that remain distinct facts.
- Reused transaction IDs across reports.
- Duplicate `SUB_ID` conflict.
- Authorized, principal, unauthorized, absent, and conflicting linkages.
- Candidate summary with explicit itemized and unitemized fields.
- House/Senate summary with total individual contributions only.
- Aligned, date-bounded, and non-comparable coverage.
- A one-record snapshot change that invalidates only the affected committee and
  candidate.

## Acceptance gates

1. Every source occurrence appears in the ledger or a structured parse issue.
2. Included, excluded, and unresolved memberships are reproducible from their
   manifests.
3. Included signed cents conserve through receipt, committee, and candidate
   levels.
4. No amendment, transaction-ID, contributor-name, employer, or earmark
   heuristic changes the fact set.
5. Pinned official openFEC model evidence and checked-in samples agree on
   `is_individual` and `memoed_subtotal` behavior.
6. Candidate itemized totals are compared with, never replaced by, FEC summary
   facts.
7. Reprocessing the same inputs and calculation version yields the same result
   digest.
8. Differential tests explain legacy differences; legacy equality is not a
   gate.
9. Point, interval, partial-coverage, invalid, and scenario semantics conform
   to the shared money-measure contract.

## Required companion and deferred calculation contracts

- A separately labeled raw electronic-filing product, effective Schedule A,
  and processed/raw reconciliation, only after a new accepted source decision.
- Candidate-controlled total receipts and transfer de-duplication.
- Loans, offsets, refunds, candidate contributions, and in-kind receipts.
- Historical paper and raw-filing backfill beyond captured electronic
  amendment families.
- Conduit and joint-fundraising relationship resolution.
- Terminal-source attribution over the accepted receiver-reported committee-
  flow calculation and later sender-side reconciliation.
- Donor identity and employer-to-organization resolution.
- Independent expenditures, lobbying context, and cross-cycle views.

## External references

- [FEC Schedule A and individual-contribution methodology](https://www.fec.gov/campaign-finance-data/about-campaign-finance-data/methodology/)
- [FEC candidate summary semantics](https://www.fec.gov/campaign-finance-data/candidate-summary-file-description/)
- [FEC current House/Senate campaign summary semantics](https://www.fec.gov/campaign-finance-data/current-campaigns-house-and-senate-file-description/)
- [openFEC itemized model and memo-subtotal rule](https://github.com/fecgov/openFEC/blob/develop/webservices/common/models/itemized.py)
- [FEC filing reliability and amendment study](https://www.fec.gov/about/reports-about-fec/agency-operations/e-filing-study-2016/recommendation-4-ensure-future-filing-reliability/)
