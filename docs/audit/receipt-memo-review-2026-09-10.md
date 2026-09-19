# Receipt memo and amount source review — 2026-09-10

Status: bounded source review complete. All 62 unresolved reviewed-line `Y`
occurrences in the [complete v2 profile](./receipt-report-profile-v2-2026-09-10.md)
are present in ten official paper transcriptions. The two unresolved amounts are
null in processed data but each reports $0.01 in its original electronic filing.
No value, memo predicate, graph, source pointer, or eligibility guard changes.

## Exact scope and source comparison

Selection comes from **all** v2 report groups with `unresolved_memo_code` or
`unresolved_line_amount`, not named-committee selection or an amount threshold.
It selects eleven reports and all their 26 groups, covering 2,863 processed
occurrences. The selected profile and original artifacts are bound by the
[digest fixture](./fixtures/receipt-memo-review-2026-09-10.sha256).

The Go audit opens the older accepted inventory once and reads these eleven
reports through the existing bounded reader. It conserves all 2,863 rows. The
independent test matches every selected group's measures and date states to the
same-v4 profile. **This is grouped equivalence, not all-field v4 equality.** Old
fact and v4 source ancestry remain separate; their ordinals differ and are not
interchanged. No full raw source scan was repeated.

The complete ten paper transcriptions contain 79 Schedule A rows. A count-
preserving multiset comparison matches every row's form-line, committee, date,
amount, raw memo code, memo text, and image reference to the older facts. Paper
transaction IDs are absent; a tuple match does not invent an identifier or prove
economic uniqueness. No other processed fields are claimed equivalent to paper.

The complete electronic filing contains 2,784 Schedule A rows. Its unique
transaction IDs match the older report's complete membership. Form-line,
committee, date, memo code, and memo text match. All known receipt amounts match;
the two null amounts are the only differences in the checked amount field.

## `Y` is paper evidence, not a safe counting shortcut

All ten transcriptions have `HDR P3.4` and 24-column Schedule A rows. This is
not the electronic `HDR FEC 8.4` / 45-column format. The preserved `Y` values
therefore do not establish a violation of the electronic format's `X` convention.
The audit's paper offsets are exact-artifact witnesses, not a shipped parser.

Follow-up: the [report-metadata audit](./receipt-report-metadata-2026-09-10.md)
now pins the official P3.4 workbook. It documents `X = True` for the paper memo
field too, with no `Y` mapping. This adds paper-specific schema evidence; it does
not resolve the checked-box/cover disagreements or authorize a counting change.

These 62 reviewed-line rows total $18,560.20 in reported amounts. Fifty-nine
have `is_individual=true`, and three have `false`. They stay unresolved under
the additive reviewed-line policy. Existing individual calculations remain
unchanged; those include the 59 true-classified rows. These are overlapping
diagnostic views, not additional funding.

| File | `Y` rows | `Y` amount | All transcribed SA11AI | Transcribed period cover |
|---|---:|---:|---:|---:|
| [1879526](https://docquery.fec.gov/paper/posted/1879526.fec) | 9 | $560.00 | $560.00 | $560.00 |
| [1840593](https://docquery.fec.gov/paper/posted/1840593.fec) | 2 | $953.00 | $5,558.00 | $24,505.00 |
| [1723437](https://docquery.fec.gov/paper/posted/1723437.fec) | 3 | $1,160.25 | $1,160.25 | $1,160.25 |
| [1765938](https://docquery.fec.gov/paper/posted/1765938.fec) | 7 | $198.75 | $208.75 | $208.75 |
| [1821649](https://docquery.fec.gov/paper/posted/1821649.fec) | 5 | $38.64 | $78.64 | $78.64 |
| [1813852](https://docquery.fec.gov/paper/posted/1813852.fec) | 4 | $720.00 | $920.00 | $510.00 |
| [1882880](https://docquery.fec.gov/paper/posted/1882880.fec) | 1 | $200.00 | $200.00 | $200.00 |
| [1735670](https://docquery.fec.gov/paper/posted/1735670.fec) | 2 | $105.65 | $105.65 | $105.65 |
| [1767007](https://docquery.fec.gov/paper/posted/1767007.fec) | 16 | $13,968.91 | $14,668.91 | $2,551.81 |
| [1733875](https://docquery.fec.gov/paper/posted/1733875.fec) | 13 | $655.00 | $655.00 | $655.00 |

The table is reproduced from the hash-pinned complete transcriptions, not OCR.
Seven reports' all-row SA11AI sum equals their transcribed period subtotal;
three do not. Excluding every `Y` matches none of these ten cover subtotals.
Equality is evidence about those fields, not permission to choose a predicate
that makes a report balance.

Two direct image witnesses show why the semantic issue remains open:

- [Filing 1879526](https://docquery.fec.gov/pdf/250/202412120300493250/202412120300493250.pdf),
  PDF page 7: the three shown payroll-deduction entries have checked Memo Item
  boxes and are included in that page's subtotal. Page 3 reports $560.00 on
  the period itemized-individual line, matching all nine transcribed entries.
- [Filing 1765938](https://docquery.fec.gov/pdf/335/202402090300462335/202402090300462335.pdf),
  PDF page 6: two checked Memo Item entries describe in-kind shipping and web
  hosting. Their $148.75 page subtotal includes both. Page 3 reports $208.75
  itemized, matching all eight transcribed entries rather than blank-memo rows.

These four pages were visually inspected. All ten complete PDFs are retained
and hash-checked, but this is not a visual review of every entry or a claim of
universal paper-code semantics. Memo text, checked boxes, reported subtotals,
and cash-versus-in-kind meaning remain distinct evidence. Do not rewrite `Y`
to `X` or blank, or automatically count a checked memo as available cash.

## Two missing processed amounts have exact original witnesses

[Electronic filing 1730162](https://docquery.fec.gov/dcdev/posted/1730162.fec)
reports `0.01` for transactions `A-234660` (September 15, 2023) and `A-281132`
(September 30, 2023). Both are SA11AI entries with blank memo codes and earmark
text. The older processed facts contain null raw and typed receipt amounts;
the selected v4 groups retain the same two unknown-amount observations.

The original complete non-memo SA11AI sum is $611,364.96, exactly its
report-period cover subtotal. The processed known-amount sum is $611,364.94,
with the two unknowns still present. This isolates the two-cent discrepancy
in the audited report. It does not identify the publisher processing step that
produced the nulls or authorize a fallback for other records.

Preserve the original values as separate evidence. A future accepted correction
needs the exact processed occurrence, filing/transaction locator, both source
hashes, field, value, and policy version. An unknown amount is not zero, and
this audit does not relabel older receipt facts as v4 facts.

## Report and amendment coverage

All eleven captured covers are marked new (`F3N` or `F3XN`). Their cover periods
are now available for this sample. That does not establish that they were never
amended, were selected into the cycle summary, or cover a complete committee
account. No amendment chain or complete report listing was acquired here.

The 73,277 v2 committee/file references cover reports **represented by processed
Schedule A occurrences**, not every filed financial report. A zero-itemization
report can be absent. The earlier
[attachment-selection witness](./summary-report-review-2026-09-10.md) also shows
why a latest-file flag alone cannot identify a complete financial replacement.
The [report-coverage boundary](../design/receipt-report-coverage.md) specifies
the missing evidence without selecting a new recurring source.

## Reproduction and retained evidence

The [Go corpus test](../../internal/calculation/fec/fundingbasis/memo_review_corpus_test.go)
uses one verified reader and took 134.649 seconds. The
[independent tests](../../tests/test_receipt_memo_review.py) verify complete
selection from the retained v2 profile, grouped cross-release equivalence,
original membership, memo states, exact amounts, and unchanged eligibility.
Fourteen checks pass in 2.49 seconds; all source calculations use exact integer
cents. Ruff, the complete Go suite, vet, and the focused funding-basis race gate
pass. The opt-in corpus, final independent, and verification exit markers are
zero. The post-review active source digest remains
`b030668efca44fcf611b204665d95c00fa7eda9ce3ce31b5711ebbba6805e81c`.

Evidence is retained under
`dumps/audits/fec/receipt-memo-review/2026-09-10/attempt-01/` in project storage:
selected groups, complete public transcriptions/electronic filing, original PDFs,
HTTP headers, four rendered witness pages, bounded Go reports, test/code snapshots,
logs, and explicit success markers with verified tree checksums. The paper URL
for electronic filing 1730162 returned 404; that response is retained separately,
not treated as a missing filing. Its electronic route returned the complete file.

Each transcription/electronic download was capped at 2 MiB; each PDF at 4 MiB.
There were no new bulk downloads, API requests, services, or runtime Python.
Go readback used a 4 GiB cap, 2 GiB Go target, and four CPUs; independent audit
tests used 2 GiB and two CPUs. Source storage remained read-only during review.
No graph, Dagster, production calculation, or source/fact publication changed.
