# Receipt-family summary comparisons

Go's `compare-receipt-family-summary` adds per-assertion comparisons to the
[family-window calculation](./receipt-family-window.md). It compares the same
summary field separately with a reported window and a qualified detail window.
It does not establish a complete funding basis or terminal-dollar attribution.

The [versioned policy](../../contracts/calculations/fec/receipt-family-summary/v1/policy.json)
and [retained gate](../audit/receipt-family-summary-2026-09-11.md) define the
accepted subset and its evidence. Earlier commands and nested outputs stay unchanged.

## Inputs and output

The command uses the same eight flags as the family-window command:

```text
legal-tender pipeline fec compare-receipt-family-summary \
  --storage-root <root> --summary-facts <exact-manifest> \
  --capture <retained-report-capture> --documents <pinned-document-set> \
  --start <YYYY-MM-DD> --end <YYYY-MM-DD> \
  --profile <complete-v2-profile> --profile-sha256 <expected-digest>
```

Go revalidates the original documents, metadata, profile and published summary.
It then reads extra raw/typed summary scalars from the same immutable fact set,
checking its manifest digest, source ancestry and representative fact locators.
The small summary is verified again; the processed Schedule A body is not scanned.
This is a bounded diagnostic, not an efficient recurring per-committee pipeline.

`family_window` preserves the complete previous result. Each new assertion names
the existing assertion ID and representative fact ID; its fields point to a
zero-based family index. All original member locators and conflicting variants
remain available in the nested summary. A representative supplies one operand,
not a sum over repeated candidate references. Existing assertion/calculation IDs
and diagnostic equations do not change.

## Accepted reported-field meanings

The mapping combines the pinned [receipt-family source map](./receipt-families.md),
the [FEC summary dictionary](https://www.fec.gov/campaign-finance-data/committee-summary-file-description/),
and the form-specific period columns. It is a reviewed reported-field mapping,
not evidence that the independent source snapshots contain identical reports.

| Report family | F3 line | F3X line | Summary field |
|---|---|---|---|
| Party contributions | 11B | 11B | `PTY_CMTE_CONTB` |
| Other-committee contributions | 11C | 11C | `OTH_CMTE_CONTB` |
| Authorized / affiliated-or-party transfers | 12 | 12 | `TRANF_FROM_OTHER_AUTH_CMTE` |
| Candidate-made or guaranteed loans | 13A | Not applicable | `CAND_LOAN` |
| Other / non-affiliated loan receipts | 13B | 13 | `OTH_LOANS` |

Form and literal line identity remain distinct despite shared summary columns.
F3 line 13A includes candidate-guaranteed loans; `CAND_LOAN` is not proof that the
candidate supplied the principal. F3 authorized-committee transfers include
specified loans and repayments on line 12 rather than 13B. See the
[F3 instructions, printed page 4](https://www.fec.gov/pdf/forms/fecfrm3i.pdf).

F3X line 13 excludes affiliated/party committee loans reported as line 12
transfers. It has no candidate-loan split. This maps to the summary's other-loan
field, not a fallback to `TTL_LOANS`, a balance or a repayment field. See the
[F3X instructions, printed page 6](https://www.fec.gov/pdf/forms/fecfrm3xi.pdf).
These distinctions constrain the interpretation; zero-valued witnesses do not
prove positive loan/transfer behavior. The remaining initial shapes now have
[positive source witnesses](../audit/positive-receipt-families-2026-09-11.md),
without establishing population-wide correctness or metadata binding for them.

## Scope and arithmetic

Each side uses the existing v2 `reported_summary_coverage` rules: valid ordered
summary dates within the source cycle; exact requested start and end; compatible
committee type, designation and report form; no relevant scope/field conflict.
The family window must independently cover all requested days and members.
Date equality is necessary, not proof of financial report membership.

`versus_reported_window` uses the bound reported total.
`versus_qualified_detail_window` uses the separate family comparison total.
That second total retains its member bases: nonmemo occurrence subtotal or
qualified reported zero without occurrences. It is not always a sum of detail rows.
Reported readiness can qualify while detail readiness remains blocked.

Both comparisons preserve their operands even when blocked. A source blank stays
null, not zero. Differences are summary minus window in arbitrary-precision signed
integer cents and exist only for scoped, ready pairs. There is no proration,
cross-family total, residual-based gap fill or normalization of source variants.

Conflicting scope fields block all fields in every assertion. A conflicting amount
blocks that field in every variant. Unrelated contact differences and independent
cash-equation discrepancies remain explicit without erasing scoped scalar pairs.
The shared `reported_comparison_ready` flag means diagnostic subtraction only,
including when nested under the qualified-detail comparison.

## Limits and next gate

`source_alignment` remains `independent_snapshots`.
`same_report_membership_proven`, `financial_use_eligible` and
`terminal_attribution_eligible` remain false. No publication, ArangoDB write,
Dagster activation, source refresh or financial amendment selection occurs.

Next, extend report-level evidence comparisons to the remaining mapped receipt
categories, preserving thresholded versus complete-detail scope. Broader forms,
effective financial membership, account/cash continuity and terminal allocation
remain separate gates.
