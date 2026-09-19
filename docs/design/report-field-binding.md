# Report-period field binding

Status: implemented bounded Go reader and diagnostic. The
[retained-source gate](../audit/report-field-binding-2026-09-10.md) passes.
The [source layout](../../contracts/sources/fec/electronic-cover/v1/layout.json)
and [binding policy](../../contracts/calculations/fec/report-field-binding/v1/policy.json)
define the accepted boundary. This is not a recurring filing source.

## What a bound field means

`reported_value_bound` means that an exact electronic-cover amount and an exact
report-endpoint amount agree, and both belong to one verified **observed-chain
candidate**. It does not declare the filing financially correct or complete.

The [membership review](./report-period-membership.md) still owns chain candidates
and requested-window date coverage. The binding command re-runs it from original
capture evidence; saved review JSON and caller-selected candidate IDs are not inputs.
A bound report can coexist with a gap elsewhere in the requested window. A
window-level consumer must check both interval coverage and every required field.

## Qualified electronic source

The official FEC electronic-format archive contains the exact 8.4 workbook used
by the retained reports. The gate verifies its ZIP member identity, CRC, size,
and SHA-256. It does not apply the newer 8.5 workbook to an 8.4 document.
The [audit](../audit/report-field-binding-2026-09-10.md#schema-evidence) records
the public source URL and bounded capture.

The reader accepts `HDR / FEC / 8.4`, then F3 or F3X with suffix `N`, `A`, or `T`.
The optional final HDR comment may be physically absent: seven or eight header
fields are accepted and preserved without padding. F3 has exactly 93 fields;
F3X has exactly 123. Unsupported versions, wrong widths, and undecodable cover
bytes remain raw, unresolved evidence. Interpretation is ASCII-only for now.

Every body byte survives with its record ordinal, byte offset, byte length, and
hash. The shared [document framing checks](./report-scope-assessment.md) verify
body/header hashes, exact HTTP 200 or zero-origin 206 framing, declared lengths,
capture extent, and resource limits. The original paper reader is unchanged.

All cover fields survive, including both period and column-B values. Every
AMT-12 position is typed as blank, valid exact cents, or invalid. F3X position
75 is a year, not money. No field is filled from arithmetic or another column.
Invalid amounts stay local to their positions; another valid field can bind.

Seven period fields are mapped. Repeated source positions remain separate
assertions, not alternative values to choose between:

| Field | F3 8.4 positions | F3X 8.4 positions |
|---|---|---|
| Itemized individual contributions | 33 | 30 |
| Unitemized individual contributions | 34 | 31 |
| Total individual contributions | 35 | 32 |
| Total receipts | 46, 59 | 24, 45 |
| Total disbursements | 57, 61 | 26, 66 |
| Opening cash | 58 | 23 |
| Closing cash | 30, 62 | 27 |

These are versioned source-schema positions, not committee-specific exceptions.
F3X total federal receipts is a different field and is never a total-receipts
fallback. Column-B running totals never substitute for period amounts.

## Scope and field checks

The binding requires:

1. A complete supported electronic document, one supported cover, no additional
   form/header record, and valid cover identity and dates. A prefix may expose a
   complete typed cover but cannot establish this complete-document condition.
2. An exact file-ID observation in one fully traversed report-endpoint capture,
   selected as an observed-chain candidate. Superseded records are not selected
   because their amounts happen to agree.
3. Matching committee, form, literal report code, coverage dates, and amendment
   indicator. The whole report interval must fall inside the requested window;
   report amounts are never apportioned by days. Unqualified code aliases such
   as `MYE` versus `YE` remain disagreements, not automatic normalization.
4. An amended cover's HDR original-report ID must equal the first chain member.
   The amendment sequence must be a positive one-to-three-digit number; preserve
   it without ranking versions or assuming it counts the observed chain.
   Singleton chains require non-amendment covers and blank amendment identity.
5. Each mapped cover position and its metadata amount must be explicit, valid,
   and equal in exact cents. Repeated cover positions must agree. Preserve the
   signed metadata-minus-first-cover diagnostic even when a binding is blocked.

Metadata types follow the pinned endpoint contract: the three individual fields
permit numeric strings; the other four accept numbers or null. Null, blank,
invalid, zero, and mismatch remain distinct. There is no endpoint precedence.

`scope_bound` says the source-to-candidate scope checks passed. Each field has
its own blockers and `reported_value_bound`. Matching reported cash does not
establish cash continuity, a usable funding denominator, or donor composition.
`cycle_total_ready`, `cash_basis_ready`, and `terminal_attribution_eligible`
remain false. The nested membership and source guards remain unchanged.

## Command and next boundary

```bash
legal-tender pipeline fec review-report-field-binding \
  --capture <retained-report-endpoint-capture.json> \
  --start <YYYY-MM-DD> --end <YYYY-MM-DD> \
  --source-url <public-dcdev-posted-file-url> \
  --body <retained-body> --body-sha256 <expected-sha256> \
  --headers <retained-headers> --headers-sha256 <expected-sha256>
```

Exit zero means a valid diagnostic, including blocked bindings. There is no HTTP,
bulk scan, source pointer change, Arango write, Dagster asset, or Python runtime
calculation. The current bounded, verbose JSON is not a recurring storage model.

The additive [window calculation](./report-window.md) now aggregates candidate-bound
fields with field-specific missing/conflicting coverage, cash boundary stocks,
and separate subtotal/carry-forward diagnostics. This binding command stays unchanged.
Whole-cycle financial use, account scope, source history, paper corrections,
immutable publication, and recurring acquisition remain separate gates.
