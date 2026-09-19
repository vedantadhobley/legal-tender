# Committee-flow source review

Observed 2026-09-08. The read-only Go `review-committee-flows` command profiles
the complete [2024 reconciliation evidence](./committee-flow-reconciliation-2026-09-08.md)
and traces deterministic examples back to immutable Parquet facts. The
[review contract](../../contracts/audits/fec/committee-flow-review/v1/) records
the method and output schema.

## Result and scope

The run completed in 21.778 seconds. It replayed all 308,488 candidate
components, profiled them into 166 disjoint type/cardinality shapes, and
reviewed 76 complete source rows from 32 selected components. All source
examples exactly reproduced their saved observation, including signed amount,
reported date, endpoints, type, and policy membership.

All 423 backing Parquet shards were hash-verified through the existing fact
loaders. Row decoding then sought directly to sampled ordinals. No complete
421-million-row calculation scan, archive extraction, or download occurred.
The container used a 4 GiB cap and `GOMEMLIMIT=2GiB`; no peak-memory claim is
made. The run exited zero without OOM.

Profiles cover the complete selected evidence. The source-row sample is
targeted, not statistically representative. It spans each amount/role-conflict
type combination, ambiguity cardinality and exact-signature presence, the
largest component per state, and the largest one-to-one date gap. Up to four
rows per side are decoded per component, with explicit truncation flags.
The largest component has 40 A and 35 B observations; only its first four per
side were inspected. Full component membership remains available.

## Findings

### A candidate component need not represent one payment

A ordinals **46** and **39036739** report $5,000 on 2023-01-25 and 2024-01-10.
B ordinals **1733** and **17468645** report $5,000 on 2023-01-17 and 2024-01-08
for the same directed committee pair. The sender descriptions identify the
respective contribution years. The matcher correctly preserves one ambiguous
component because amount-only alternatives span the cycle; treating the
component as one payment would collapse distinct reporting activity.

This is a limitation of candidate grouping, not evidence that four source
records describe four independent funding amounts. A date-based resolver
would be a separate versioned method with its own source and ambiguity gates.

### One disbursement can correspond to several receipt items

A ordinals **792** and **1051** report $4,514.43 and $26,399.54 from the same
committee on 2023-03-22. B ordinal **60293** reports $30,913.97 to the same
recipient on that date. The two A amounts sum exactly to B's amount. All three
describe net joint-fundraising transfers, and one A description distinguishes
previously disclosed donors.

This supports investigating split reporting. It does not authorize automatic
sum-based matching: other components contain repeated payments across years,
and equal totals alone do not establish common economic identity. Keep all
three facts and the unchanged ambiguous state.

### Generic transaction codes do not establish cash

A ordinal **319654** has type `15K`, $399.79, date 2023-09-28, and an explicit
in-kind catering memo. B ordinal **127223351** has type `24Z`, the same amount
and date, and an in-kind description. A ordinal **722318** is `15Z` with a
fundraising-services memo, while B ordinal **35283** is `24K` and describes
the same service category. A ordinal **212774590** combines `22Z` with a
contribution reporting line and an in-kind catering memo; its B candidate is
`24Z`.

The FEC [type dictionary](https://www.fec.gov/campaign-finance-data/transaction-type-code-descriptions/)
distinguishes registered-filer contributions, transfers, refunds, and explicit
in-kind codes. Its [in-kind reporting guidance](https://www.fec.gov/help-candidates-and-committees/filing-reports/in-kind-contributions/)
also shows that valuation and reporting entries are not equivalent to cash
movement. The reviewed rows demonstrate that policy-assigned code roles and
the rest of the reported evidence can differ. Do not convert generic codes
to cash or let a narrative substring silently rewrite a source role.

There are 1,238 one-to-one role-conflict components across 14 type-pair shapes.
These are classifier disagreements, not 1,238 proven contradictory filings.
For example, 582 compare A `18K` with B `24G`; 398 compare A `15K` with B
`24Z`. The full profile preserves every combination and its separate amounts.

### Amount and sign differences remain unresolved

A ordinal **464596** reports $1,000.00; B ordinal **92361414** reports $959.70
on the same date for the same endpoints. The sampled descriptions do not
establish a fee, deduction, correction, or common payment identity. Do not
invent the $40.30 difference's meaning.

A ordinal **73966282** reports a positive $5,000 refund receipt, while B
ordinal **17325284** reports negative $5,000 with the same date and refund
code. Preserve both signs. The sample does not establish whether the cause is
a correction, another reporting convention, or an unrelated candidate match.

The complete profile has 203 one-to-one amount-conflict components across six
type-pair shapes. Neither sum equality nor a difference between ledger sums is
an accepted economic-flow amount or uncertainty interval.

### Reported dates differ and can fall outside the source cycle

Of the 130,650 unique date-disagreement candidates, 127,166 have A later than
B and 3,484 have A earlier than B. Absolute gaps exceed 30 days for 14,657
candidates and 365 days for 32. These are descriptive checks, not an accepted
tolerance or match-quality score.

The largest gap is 1,099 days: A ordinal **102692372** reports $2,000 on
2024-05-24, while B ordinal **59760386** reports $2,000 on **2021-05-21** inside
the publisher's 2024 relation and report-year context. The raw date exactly
matches the typed Parquet date. This is not a Go date-conversion error.
It could be a source-date error or an unrelated candidate; this review cannot
choose between them. Do not change the reported date or move the fact to a
different source partition.

FEC [recordkeeping guidance](https://www.fec.gov/help-candidates-and-committees/keeping-records/recording-receipts/)
assigns a distinct meaning to receipt dates. That supports preserving separate
time fields, not demanding equality with disbursement dates or explaining
every observed gap as ordinary processing delay.

### Exact subpairs remain visible inside ambiguous components

Of 57,011 ambiguous components, 9,814 contain at least one shared exact
signature. Across those components there are 15,854 shared signatures;
15,192 have exactly one member per side for that signature. Alternative
amount/date connections still exist. These counts do not promote any subpair
to a resolved payment.

The evidence graph should expose these alternatives without forcing users to
choose between discarding useful exact evidence and pretending the entire
component has been resolved.

## Verification and evidence

The complete Go suite, static analysis, and focused race tests pass. Tests cover
deterministic profiling, exact alternatives inside ambiguous components,
signed dates, corruption, changed result totals, source-field equality,
wrong cycles, missing ordinals, and fail-without-result CLI behavior.

Independent validation checks the strict review schema, every profile count
and signed cent against the prior calculation summary, date-profile counts,
sample membership/truncation, all 99 A or 98 B fields, and each sampled raw
amount, date, code, and endpoint against the saved observation. Full review
replay with the final guards produces byte-identical JSON.

| Identity | SHA-256 |
|---|---|
| Original reconciliation result | `070e2c67057ab4671d48f139924760c6e5c8bf5fae60ecf2e6e2a779b4da1882` |
| Review result | `4135369cb80103119073c3cf5580976ff87a0ff38cf787156e601caed2c1d0d9` |

The review JSON is 492,241 bytes. Retain it, replay output, run logs,
independent validator/result, and explicit success markers under
`/storage/dumps/audits/fec/committee-flow-review/2026-09-08/2024/`.
Source facts and prior reconciliation artifacts remain unchanged.

This review inspects publisher-processed source fields, not filing images.
The original image URLs are preserved in each complete source example; the
browser could not open those image-query URLs during this review. The findings
do not claim image-level or raw-electronic-filing confirmation.

## Next implementation

The [accepted graph boundary](../design/arango-committee-flow-evidence.md)
keeps receipt and disbursement observations in separate edge collections and
reconciliation components in an evidence document collection. No candidate
component becomes a payment edge, cash classification, or terminal source.

Implement immutable reconciliation publication and exact readiness first,
then the isolated observation graph and its ledger-specific query gates.
Preserve unresolved economic meanings throughout. No graph or Dagster asset
was changed by this review.
