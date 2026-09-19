# Schedule B reporting and record-selection audit

Observed 2026-09-08. This audit measures the complete accepted 2024 Parquet
fact set before an effective-disbursement or outgoing-flow calculation is
published. The source and existing ArangoDB graphs are unchanged.

## Result

Every one of the 157,544,163 facts belongs to one of 1,717 disjoint reporting
and recipient-identity shapes. The signed source amount is
$23,765,207,537.31, exactly matching the independent archive/parser audit.
There are no missing amounts, 211,035 negative rows, and 440 zero rows.

The first complete scan took 219.033 seconds with four workers and sampled
memory of 334.9 MiB. It read the existing Parquet facts without downloading or
extracting the raw archive. The final form-line map corrects Form 3X line 30B
to federal election activity; the initial diagnostic label was incorrect and
was never accepted as calculation or graph policy.

The final eight-worker pass took 120.928 seconds. Every raw shape, measure,
and lowest-ordinal example matched the first run exactly after removing that
derived role label from the comparison. The final result passed its strict
JSON Schema and independent row/cent summation checks; the container exited
zero without an OOM event. Go checks and the focused race suite also passed.

| Immutable identity | Value |
|---|---|
| Source fact set | `aa025a0d06c303562d8d3de7975cc203d897c77217d9b149ade7dc033369af4c` |
| Source fact manifest SHA-256 | `d977930d4b85d623c797a6cf5a251ffd7f105d315d972b54cd3e07ad86f0c9fb` |
| Source archive SHA-256 | `39669f3c6c19d6f5076648f734f6456e0f8852d6cf37f0133e283fbfce91ac72` |
| Final profile SHA-256 | `f8be74e771eaef9d0bc8d0a0b64f8211eb63f0184978a1f16c28aa24506ea8c9` |
| Final result SHA-256 | `ec1a64ee898a850f326658db0ed4cd31023ef8f85f6c962d92e8bc43ade43066` |
| Result bytes | 3,178,339 |

The validated result, progress log, digest sidecar, and completion marker are
retained under
`/storage/dumps/audits/fec/schedule-b-semantics/2026-09-08/2024/`.

The Go [diagnostic contract](../../contracts/audits/fec/schedule-b-semantics/v1/)
defines the exact group keys, money measures, example selection, and result
digest. The [calculation design](../design/schedule-b-calculations.md) records
the remaining interpretation gates.

## Record-selection evidence

| Action | Non-memo rows | Non-memo signed amount |
|---|---:|---:|
| A, including 41 memo-Y rows | 155,640,319 | $16,018,213,772.51 |
| C | 36,665 | $245,776,673.62 |
| N | 760,337 | $6,758,188,040.35 |
| All actions | 156,437,321 | $23,022,178,486.48 |

The 1,106,842 memo-X rows carry another $743,029,050.83 of signed source
amount. The FEC's
[BaseItemized model](https://github.com/fecgov/openFEC/blob/develop/webservices/common/models/itemized.py)
defines the memo-X flag for Schedule B. Memo-Y rows are not equivalent to X.

The non-memo sum is a measured selection hypothesis. It is not total committee
spending: reporting scope, informational detail, and economic meaning still
matter. Selecting only action A would remove $7,003,964,713.97 of non-memo
source amount without an accepted reason.

All rows carry filing and link references. Transaction IDs are present on
157,538,857 rows; back-reference transaction IDs on 921,281; back-reference
schedule IDs on 899,327. Only 265 rows carry an original submission ID, and
none equals its own submission ID. These are presence/equality measurements,
not cross-row amendment-family reconstruction. They do not justify local
transaction-ID deduplication or choosing the latest action code.

## Reporting roles and recipient evidence

Form 3X line 23 contains 152,231,529 rows with a non-memo signed amount of
$6,916,000,338.59. Within that category, 151,911,656 rows lack `disb_tp`;
their non-memo amount is $5,335,470,397.31. Transaction type alone cannot
recover this reporting category. Most of these rows also carry beneficiary
name evidence; their conduit and ultimate-source interpretation requires
specific row review.

The [Form 3X instructions](https://www.fec.gov/pdf/forms/fecfrm3xi.pdf)
describe line 23 as contributions that can include in-kind activity, while
line 22 covers affiliated or party transfers. Neither label proves the
beneficial source of money. The [Form 3P](https://www.fec.gov/pdf/forms/fecfrm3p.pdf)
uses line 23 for operating expenditures, so the form must always accompany the
line number.

Recipient identity has three observed states:

| State | Rows | Non-memo signed amount |
|---|---:|---:|
| Raw and cleaned committee IDs agree | 152,356,465 | $10,966,435,994.69 |
| No valid committee ID | 5,089,848 | $11,761,531,750.12 |
| Raw committee ID only | 97,850 | $294,210,741.67 |

Every raw-only recipient is the sender itself. The source retains these
assertions; the cleaned ID is absent. They cannot become ordinary transfers
merely by selecting the raw ID. Every row has a syntactically valid sender
committee ID; syntax does not prove registration or current identity.

## Reporting categories outside the initial map

There are 8,394 rows outside the reviewed F3/F3P/F3X line map. Of these, 4,876
are non-memo rows carrying $198,569,750.82. Most have recognizable source
families; they are not collectively invalid records.

| Source shape | Rows | Interpretation requiring a separate rule |
|---|---:|---|
| F4: 21A, 23A, 24A | 8,100 | Convention reporting; inspect the exact Form 4 line and activity. |
| F9: F93 | 78 | Electioneering-communication notices; preserve outside regular committee-flow totals. |
| F3X: SL4A, SL4C, SL4D, SL5 | 154 | Publisher-labeled Levin-fund reporting. |
| F3X: 21 | 49 | Publisher labels operating expenditure; review the line alias. |
| F3X: 24 | 7 | Publisher labels independent expenditure; compare schedule meaning before any IE linkage. |
| F3: 21B or 22; F3X: 17 | 6 | Form/line discrepancies requiring source review. |

The official [Form 4](https://www.fec.gov/pdf/forms/fecfrm4.pdf) and
[FEC forms index](https://www.fec.gov/help-candidates-and-committees/forms/)
confirm the convention and Form 9 reporting families. The preserved
`line_number_label` identifies the Levin-fund observations. No code remaps
these exceptions from labels or drops them from source facts.

## Next decisions

1. Accept a precisely named processed non-memo subtotal with explicit form
   scope, retaining action and transaction fields as evidence.
2. Review the form-line exceptions and representative line-23 beneficiary,
   earmark, and in-kind records before accepting sender-flow membership.
3. Separate contribution, transfer, refund, loan, and operating roles even when
   the counterparty is a committee.
4. Reconcile qualifying Schedule B observations with Schedule A at fact grain.
   Link both disclosures to one economic-flow hypothesis before graph use.

The rewrite has therefore proved more than physical parsing, but this audit
does not publish a spending calculation or a Schedule B graph projection.
