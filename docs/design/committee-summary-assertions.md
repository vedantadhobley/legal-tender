# Exact committee-summary evidence grouping

The Go `calculate-committee-summary-assertions` command groups the
[published summary facts](./committee-summary-source.md) without changing them.
It is a read-only calculation, not a report selector, cash ledger, graph update,
or terminal-source allocator. The [versioned policy](../../contracts/calculations/fec/committee-summary-assertions/v1/policy.json)
and [result schema](../../contracts/calculations/fec/committee-summary-assertions/v1/result.schema.json)
define the executable boundary.

The [four-cycle gate](../audit/summary-assertions-2026-09-10.md) passes complete
Go readback/replay and independent reconstruction from every raw CSV record.

## Grouping rule

Within one exact fact set and source cycle, group rows for the same valid
committee only when every source field except `CAND_ID` is exactly equal.
The other 91 fields include all money, dates, committee metadata, and raw codes.
Do not trim strings, normalize monetary spelling for equality, equate blanks
with zero, clip dates, or ignore differing contact metadata. This deliberately
conservative rule can preserve more variants than a later financial comparison
needs; it cannot silently erase a difference.

Every occurrence remains a member with its fact ID, occurrence ID, ordinal,
raw byte span/hash, and raw/typed candidate reference. Exact duplicate rows
also remain separate members. Invalid candidate IDs do not erase the financial
evidence. Missing or invalid committee/cycle identity remains a separate
unindexed occurrence, never one shared anonymous committee.

Each distinct 91-field signature is an assertion variant. One variant produces
`single_assertion`; multiple variants produce `conflicting_assertions` and an
explicit list of differing fields. All variants survive. No financial consumer
may select a first/last variant or sum variants to resolve that conflict.

The lowest source ordinal is the representative used for source drilldown and
diagnostics within an exactly equal variant. It is not an amendment winner.
All members point back to the unchanged full facts, including their own issues.
Representative issue metadata does not replace each member's source evidence.

## Arithmetic diagnostics

Three signed-cent expressions run independently for each assertion variant:

| Diagnostic | Left minus right |
|---|---|
| `cash` | `COH_BOP + TTL_RECEIPTS - TTL_DISB - COH_COP` |
| `individual` | `INDV_ITEM_CONTB + INDV_UNITEM_CONTB - INDV_CONTB` |
| `cash_federal_columns` | `COH_BOP + TTL_FED_RECEIPTS - TTL_FED_DISB - COH_COP` |

Results retain the exact operand fields, raw strings, typed states, and signed
difference. Arithmetic uses arbitrary-precision integers, including when the
difference exceeds the range of a single source value. An invalid operand yields
`invalid`; otherwise a blank yields `missing`. Both have a null difference.
Available operands yield `equal` or `different`, without rounding or tolerance.

The federal-column expression is a sensitivity diagnostic, not an alternative
cash identity to select when it balances. FEC Form 3X's cash summary uses total
receipts and disbursements; its separate federal totals exclude specified
nonfederal amounts. Swapping column families therefore changes the question.
See the [official form, pages 2–4](https://www.fec.gov/pdf/forms/fecfrm3x.pdf)
and [CSV dictionary](https://www.fec.gov/campaign-finance-data/committee-summary-file-description/).

A difference is not a fabricated receipt, missing donor, corruption finding,
unitemized contribution, or authorized repair. An equality does not prove
report completeness, account scope, or within-period cash availability.
Every result retains `financial_use_eligible=false` and
`terminal_attribution_eligible=false`. Future scope-qualified consumers require
their own accepted contract; these flags must not be flipped by orchestration.

## Input, identity, and conservation

```bash
legal-tender pipeline fec calculate-committee-summary-assertions \
  --storage-root /storage --summary-facts <published-fact-manifest> \
  --cycle <source-cycle>
```

Go first verifies the immutable summary manifest, source-release membership,
raw bytes, and every stored fact through the existing publisher loader. A second
bounded stored-fact scan builds groups and verifies EOF. The source boundary
limits summaries to 16 MiB raw and 100,000 records; no large schedule scan,
network request, new parser, or database connection is involved.

The result binds the fact-set ID, exact fact-manifest digest, source-release
ID/digest, and source-artifact digest. It conserves:

- Source rows = indexed members + unindexed occurrences.
- Indexed members = assertion variants + repeated evidence rows.
- Every indexed occurrence belongs to exactly one variant.

Use full source-field values for grouping equality, not hash equality alone.
The evidence digest hashes Go's compact JSON array of the non-candidate values
in pinned source order. Assertion identity hashes compact JSON of
`[policy, fact_set_id, encoded_non_candidate_values]`. The calculation ID hashes
the complete compact result with its calculation-ID field empty. Committees
and variants have deterministic ordering; members retain source order.
Run IDs, elapsed time, and local paths are not identity inputs.

The command emits JSON only after successful verification and conservation.
It does not publish a calculation pointer or register another Dagster asset.
The verbose diagnostic output is an audit artifact, not the accepted physical
format for a scheduled production calculation.

## Next boundary

The [bounded report investigation](../audit/summary-report-review-2026-09-10.md)
now explains three selected discrepancies without changing summary facts.
An individually balanced report can still break cash continuity; a publisher's
latest-version flag can select an attachment with no financial summary; a paper
transcription can disagree with its original image. These are audit witnesses,
not named runtime exceptions or an accepted correction algorithm.

The [summary/receipt readiness review](./summary-receipt-compatibility.md) now
preserves nine reported fields and exact recipient cohorts while exposing
source/scope/conflict blockers. Its five-case 2024 gate passes without calculating
numeric differences or promoting financial use. Next prove compatible ancestry
and report/form-line coverage for a narrow comparison. Do not join older A/B/E calculations
to v4 summaries under a same-release claim. The
[funding coverage contract](./funding-coverage-and-time.md) remains the gate
before any pooled or terminal-dollar allocation.
