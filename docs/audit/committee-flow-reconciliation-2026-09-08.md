# Committee-flow reconciliation gate

Observed 2026-09-08. The Go [candidate reconciliation](../design/committee-flow-reconciliation.md)
now passes over complete accepted 2024 Schedule A and B Parquet facts. This
gate selects typed sender observations, preserves source-indexed candidate
evidence, and verifies separate ledger conservation. It does not publish an
economic-flow graph or infer original funding ownership.

## Corpus and execution

The eight-worker run read 264,085,606 A facts and 157,544,163 B facts, including
complete backing digest verification, in 298.312 seconds. It exited zero
without OOM. Sampled container memory reached 1.139 GiB under a 4 GiB cap and
`GOMEMLIMIT=2GiB`; this is an observation, not a measured peak. The source
storage was read-only. No archive download, extraction, source publication,
graph mutation, or Dagster change occurred.

The unchanged receiver policy selected 320,731 A observations and
$4,672,820,179.49, exactly reproducing its prior accepted cohort. The new
sender policy selected 341,720 B observations and $5,158,069,433.82. These are
different reported populations; never add their amounts as money moved.
All remaining facts stay in the immutable Parquet sets and disjoint selection
decision buckets. Source totals conserve every row and known signed cent,
including the two missing A amounts.

The result has 308,488 candidate components:

| State | Components | A facts | B facts | A signed amount | B signed amount |
|---|---:|---:|---:|---:|---:|
| Unique exact signature | 23,273 | 23,273 | 23,273 | $1,936,173,990.65 | $1,936,173,990.65 |
| Date disagreement | 130,650 | 130,650 | 130,650 | $518,386,511.38 | $518,386,511.38 |
| Amount conflict | 203 | 203 | 203 | $18,198,788.02 | $20,465,669.59 |
| Role conflict | 1,238 | 1,238 | 1,238 | $69,526,099.31 | $69,526,099.31 |
| Ambiguous candidates | 57,011 | 125,770 | 129,840 | $1,668,617,376.60 | $1,738,399,897.30 |
| Unmatched A | 39,597 | 39,597 | 0 | $461,917,413.53 | $0.00 |
| Unmatched B | 56,516 | 0 | 56,516 | $0.00 | $875,117,265.59 |

There were no unique missing-date candidates. Exact signature means agreement
within these selected cohorts, not proof of unique economic identity. All
alternative same-role/amount and same-role/date candidates participate before
classification; the method does not greedily reserve exact pairs. This differs
from the earlier [source alignment diagnostic](./schedule-ab-alignment-2026-09-04.md),
which did not implement this sender policy and component contract. Its
coverage figures are not interchangeable with this calculation.

## Selection limits

Missing transaction type leaves 151,594,517 B records outside typed flow
selection, with $5,366,436,501.33 of signed reported amount. This is explicit
uncertainty, not discarded evidence or an estimate of missing cash. Earlier
[record review](./schedule-b-reporting-calculation-2026-09-08.md) found earmarked
activity among null-type contribution-line records. Neither the reporting
line nor a valid committee endpoint establishes own-money funding.

Other B decisions include memo evidence, separate reporting scopes, absent
committee recipients, non-flow reporting roles, held earmark codes, self
recipients, conduit evidence, and unreviewed role/type pairs. The full result
retains their exact role/type grouping and totals. Ordered policy precedence
means these counts need not equal earlier independent shape flags.

Loans are a selected B role but are not part of the unchanged A receiver
cohort. They remain one-sided under this contract. Refund/repayment comparisons
also inherit the existing A role boundary; this gate does not broaden it.
Unmatched means no candidate in these selected cohorts and this cycle, not
proof of no opposite report. Cross-cycle dates and additional reporting
evidence remain future review inputs, not guessed matches.

## Verification and replay

The Go source scan validates full physical schemas, dense ordinals, cycle,
typed values, complete counts, and signed amounts. Each selected source fact
appears exactly once across component assertions. Every saved observation and
assertion is reopened and compared to the calculated record, with complete
count and compressed/uncompressed digest verification.

An independent validation script then checked the strict result schema and
every saved evidence-record schema, all artifact digests/sizes, sorted unique
ordinals, source decision conservation, separate ledger conservation, assertion
identities and states, exact summaries, and `graph_eligible=false`. It passed
in 31.861 seconds. The script is gate evidence, not Python data-plane logic.

Three complete Go candidate replays over the saved observations produced the
same assertion descriptor and summary in a combined 9.87-second test. They
did not rescan the full source corpus. One-worker/four-worker equivalence is
fixture-tested across multiple complete-schema shards; a second complete
corpus scan at another worker count was not run.

Go tests, static analysis, focused race tests, and machine-contract tests pass.
Failure fixtures cover inconsistent normalization, wrong schema/cycle/ordinal,
corrupt artifact reuse, missing/reused observations, amount overflow, missing
dates, signed corrections, competing exact candidates, and role conflicts.

## Immutable identities and retained evidence

| Identity | SHA-256 or ID |
|---|---|
| Coordinated release | `fec-136903645ee7c7050d463a4f779a051a454ebb87308fd854faabc5ca47a20cdf` |
| Release manifest | `a28d56c7a3be024ed7cd85e2bf0ead21c982c96d00b683ef364c0a37eb9bc3ec` |
| A fact set | `8f69a2ef40316f62d8844ecf732342a3b4535136b434f60677d0608cb04465df` |
| A fact manifest | `b664e752a3bae186508a45bb6d09289ba2fe06f0b4149ecbd72063e7ef79b829` |
| B fact set | `aa025a0d06c303562d8d3de7975cc203d897c77217d9b149ade7dc033369af4c` |
| B fact manifest | `d977930d4b85d623c797a6cf5a251ffd7f105d315d972b54cd3e07ad86f0c9fb` |
| Calculation | `987626c6070c7e7f57db092ab108fb56d0ab97c6ac6a3f802dd277d9738d6ae8` |
| Result JSON | `070e2c67057ab4671d48f139924760c6e5c8bf5fae60ecf2e6e2a779b4da1882` |

The result JSON is 154,245 bytes. Compressed A observations use 5,052,677
bytes, B observations 4,966,614 bytes, and component assertions 16,115,958
bytes. Together they use 26,289,494 bytes. These are selected projections and
relationships, not a second dense copy of all source records.

Retain the result, `gate/evidence/` artifacts, progress log, independent
validator and its result, and explicit success markers under
`/storage/dumps/audits/fec/committee-flow-reconciliation/2026-09-08/2024/`.
Paths inside the result are relative to that directory's `gate/` subdirectory.

A's original publication release differs from B's. Both are valid inputs
because the coordinated release selects the exact unchanged A archive and
selected-relation bytes alongside the exact B archive and cycle relation.
Both original release manifests were also checked against their fact ancestry.
No facts were relabeled or republished to manufacture same-release identity.

## Next boundary

Review representative amount/role conflicts and ambiguous components before
accepting economic-flow assertions. Define an additive graph contract that
exposes both source identities and disagreement without adding the ledgers.
Automated immutable result publication, readiness, persistent no-scan reuse,
and thin Dagster wiring remain unimplemented. Existing graphs remain intact.
