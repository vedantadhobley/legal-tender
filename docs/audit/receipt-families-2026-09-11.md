# Receipt-family source gate — 2026-09-11

Status: reviewed source map and bounded evidence checks pass. The
[design](../design/receipt-families.md) owns the accepted field distinctions;
no runtime monetary policy changes in this gate.

## Findings

The map covers every F3 period receipt field from workbook positions 33–46 and
every F3X period receipt field from positions 30–46, including nested totals.
Every position and publisher label matches the pinned 8.4 workbook. Each
total-receipts equation expands to its leaf categories exactly once. F3X's
federal-only total excludes its two nonfederal transfer leaves.

Contributions, candidate loans, other loans, transfers, repayments, refunds,
offsets and other receipts remain different families. Some require itemization
regardless of amount; others permit a cover amount larger than disclosed detail.
F3X 18(a)/(b) refer to H3/H5 account transfers, not additional Schedule A money.
Neither a receipt category nor a blank memo code establishes spendable cash.

## Complete saved-profile routing

The existing 266,269,624-byte v2 profile is pinned by SHA-256
`acce0a13f2d87abfe78beb66c6bd2ae60f82a53bf656b3745343966ae57f3647`.
The original [complete scan](./receipt-report-profile-v2-2026-09-10.md) remains
its backing evidence. This audit reads that saved profile, not the transaction
body. Every count and signed/positive/negative/unknown measure is conserved.

| Source-map state | Physical occurrences |
|---|---:|
| Exact F3/F3X Schedule A leaf family | 244,271,670 |
| Other forms, retained outside this map | 19,813,424 |
| F3X aggregate-line reference, not an additional leaf | 7 |
| Unmapped literal F3X references | 532 |
| Total | 264,085,633 |

Mapped means a source family was identified, **not** a counted, unique or
financially accepted transaction. Memo/individual axes and signed amounts stay
attached. No population is discarded or normalized into another key.

All seven aggregate references are literal F3X `11D`; they cannot use F3's
candidate-contribution meaning. The unmapped population is two `19A`, 396
`SL1A` and 134 `SL2` occurrences. Other-form evidence is predominantly F3P and
also includes F4 and F9. Those sources still exist; this map does not yet define
their cover/report-period interpretation.

## Original-file witnesses

All seven retained complete F3 originals were checked using their pinned
transaction IDs, memo field from the schema, exact amounts, cover positions,
and whole Schedule A membership. The 168 detailed rows regroup to the same v4
family-level counts and all monetary measures without relabeling the older
facts or comparing source ordinals across releases. The earlier
[line audit](./receipt-report-lines-2026-09-10.md) and
[window audit](./receipt-reported-window-2026-09-10.md) retain their separate
identity and financial limits.

All four receipt-subtotal equations match on each of these seven covers. This
does not repair or remove their separately documented cash discrepancies.

Two reports demonstrate why “all detail adds up to all receipts” is not the
right assumption:

- [1743911](https://docquery.fec.gov/dcdev/posted/1743911.fec) reports 100 cents
  on its other-receipts cover line with no corresponding Schedule A row.
- [1780310](https://docquery.fec.gov/dcdev/posted/1780310.fec) reports 37 cents
  on that line, also without corresponding detail.

These are explicit report values. The audit does not infer interest, a donor,
an unitemized classification, a missing transaction, or a zero detail value.
Positive unitemized-individual fields likewise remain their own cover evidence.
An absent detail operand has no emitted reported-minus-detail difference.

## Verification and retention

The [source pins](./fixtures/receipt-families-2026-09-11.sha256) retain official
[F3](https://www.fec.gov/pdf/forms/fecfrm3i.pdf) and
[F3X](https://www.fec.gov/pdf/forms/fecfrm3xi.pdf) instruction PDFs and response
headers. Their combined PDF size is 597,742 bytes. The workbook, original files,
complete profile and earlier results are reused from their existing audit roots.
No bulk file, API capture or new original filing was fetched.

The final source-map, original-file and prior-window checks pass, as do the
complete Go suite, vet, Ruff and changed-document link checks. Tests include
cross-form line collisions, exact literal matching, unknown keys, disjoint
subtotal expansion, cyclic-equation rejection, all workbook positions, full
profile conservation and preservation of missing detail operands.

The initial test run caught two errors in the new audit, not in FEC data: a
manual memo-field offset and a window total used as a per-report expectation.
The check now reads the memo position from the pinned schema and uses the
actual report-specific amount. The initial log is retained; source values and
existing runtime code were not changed.

Evidence is retained at
`/storage/dumps/audits/fec/receipt-families/2026-09-11/attempt-01/`, with the source
PDFs, `profile-family-review.json`, `report-family-review.json`, test logs,
completion markers, drivers, focused source snapshots and verified checksums.
The active source pointer remains
`b030668efca44fcf611b204665d95c00fa7eda9ce3ce31b5711ebbba6805e81c`.
Facts, graph edges, Dagster and all runtime financial guards are unchanged.
