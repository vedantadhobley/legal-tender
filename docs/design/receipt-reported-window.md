# Receipt detail versus the reported window

Status: implemented bounded Go diagnostic. The
[retained gate](../audit/receipt-reported-window-2026-09-10.md) checks one matching
window and three blocked alternatives. This compares physical occurrence
subtotals, not unique/effective financial transactions or terminal dollars.

## Inputs and command

```bash
legal-tender pipeline fec compare-receipt-report-window \
  --storage-root /storage --summary-facts <exact-summary-manifest.json> \
  --capture <report-endpoint-capture.json> --documents <document-set.json> \
  --start <YYYY-MM-DD> --end <YYYY-MM-DD> \
  --profile <complete-v2-profile.json> --profile-sha256 <expected-byte-digest>
```

The command reuses the [v2 reported-span comparison](./summary-reported-span.md).
It reverifies published summary backing, metadata pages, original documents,
observed-chain membership, and field-specific window coverage. Its `reported`
member retains that complete result unchanged, including all assertion variants,
cash disagreements, independent snapshots, and unknown outside activity.

The saved [v2 receipt profile](./receipt-report-profile-v2.md) is pinned derived
evidence. Go checks its byte digest, strict typed JSON, versions, policies,
closed guards, complete physical-verification claims, all form/report groups,
monetary/date conservation, sorted identity, and content hash. Its summary input
and calculation must match exactly. Its Schedule A relation must equal the one
in the verified immutable summary release manifest.

This revalidates the retained profile, **not the backing transaction body**.
The profile's accepted original scan remains the backing evidence. No raw source
is fetched, decompressed, rescanned, or promoted into a new fact publication.
The manual reader caps input at 384 MiB and rejects more than the producer's
500,000-group limit per group population. It is not a recurring storage design.

## Narrow field and report membership

The [policy](../../contracts/calculations/fec/receipt-reported-window/v1/policy.json)
selects the existing reviewed F3/F3X Schedule A `11AI` non-memo population for
each bound observed-chain candidate. It uses the exact committee/file pair and
checks filing form, report type, and report year against the bound metadata.

The matching summary field is `INDV_ITEM_CONTB`, not all receipts or all
individual contributions. The FEC defines itemized and unitemized contributions
as separate summary fields. [FEC summary dictionary](https://www.fec.gov/campaign-finance-data/committee-summary-file-description/).
Form-line/memo meaning comes from the existing
[reviewed line policy](./receipt-report-lines.md), not a new interpretation.

Every selected report group remains in the result. The publisher's individual
flag does not filter the reviewed line. Memo `X` groups and other lines remain
evidence outside its subtotal; unresolved reviewed-line memo codes or amounts
block that report's comparison. Receipt dates are retained, not clipped to
reconstruct report membership. Duplicate physical occurrences are not deduplicated.
Superseded reports and other committees are not added by an amount/date guess.

An empty profile report is not automatically a zero receipt value. The narrow
corroborated-empty case requires all of:

- No profile groups for the exact committee/file pair.
- A complete, qualified electronic original with no Schedule A record tags.
- An explicit zero itemized-period amount bound between cover and metadata.

This is an explicit evidence basis, not a synthetic row or a general rule for
other empty line populations. An incomplete original, positive/blank cover,
unbound field, or original containing Schedule A blocks this empty-case use.

## Outputs and limits

Each report exposes all groups, count-preserving measures, amount basis,
reported/detail operands, and an exact reported-minus-detail difference when
qualified. Missing operands remain null. A valid difference need not be zero.
Selected known occurrence measures survive even when the requested window cannot
qualify a total; they are not labeled a lower bound or complete window amount.

The window comparison requires all field/window checks and every selected
report's detail qualification. Summary comparisons additionally inherit the
exact reported-span, type, designation, and field-conflict rules. Each keeps its
assertion ID; duplicate/fan-out rows are never summed into additional money.
Other summary fields remain in `reported`, without fabricated detail counterparts.

`occurrence_comparison_ready` qualifies only this reported numeric diagnostic.
The following remain false:

- `source_body_rescanned`
- `unique_transaction_membership_proven`
- `financial_use_eligible`
- `terminal_attribution_eligible`

Group extrema cannot establish exact transaction IDs or original member identity.
The retained audit separately compares complete older per-report rows with their
originals and with the v4 grouped measures. It does not relabel those older facts
as v4 or prove v4 transaction uniqueness. Equality is not a financial accuracy
certificate and does not resolve an unrelated cash equation.

## Next boundary

The [receipt-family source map](./receipt-families.md) now distinguishes the
remaining F3/F3X categories, itemization scope and cover equations using existing
profile evidence and bounded originals. The additive
[Go family comparison](./receipt-family-comparison.md) now covers contribution,
transfer and loan fields per report without changing this `11AI` rule or
generalizing its empty-report zero. The separate
[family-window comparator](./receipt-family-window.md) now adds the reviewed
family/date sums without changing this command. Broader
report/form coverage, unique/effective membership, financial account acceptance,
cash continuity, and terminal allocation remain separate contracts. No graph
dependency, Dagster asset, recurring capture, or source correction is added here.
