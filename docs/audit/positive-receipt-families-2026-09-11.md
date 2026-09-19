# Positive receipt-family source witnesses — 2026-09-11

Result: six additional positive form/family observations match original cover
values and saved processed subtotals. Together with the earlier candidate-loan
and F3X witnesses, all nine initial [comparison-family shapes](../design/receipt-family-comparison.md)
now have positive original-source examples. This is a source-format gate, not
population-wide financial acceptance or new report-metadata binding.

## Bounded selection and capture

The inventory reuses the complete pinned v2 profile. It ranks positive nonmemo
report/line populations by total report Schedule A rows, then file number and
committee ID. The ranking is an audit cost choice, not financial report selection
or a representative political sample. Candidate/file names never enter runtime
dispatch or counting policy.

The existing complete filing 1730162 already supplies positive F3 authorized
transfers and other-committee contributions. Four missing loan/party examples
were the smallest profiled choices for their target forms and lines; each has
one Schedule A row. No fresh bulk scan or API metadata request was needed.

Four unauthenticated HTTPS requests captured these complete originals, with a
4 MiB per-body limit, 45-second timeout, no redirects and no retries. The bodies
total 12,809 bytes; response headers add 4,499 bytes. No existing source was
refreshed or overwritten. The [source fixture](./fixtures/positive-receipt-families-2026-09-11.json)
pins every body/header digest, the reused paths, target values and known exceptions.
It is test evidence, not a runtime configuration file.

## Exact positive observations

| Original filing | Form / line | Family | Nonmemo rows | Cover = original detail = processed known detail |
|---|---|---|---:|---:|
| [1730162](https://docquery.fec.gov/dcdev/posted/1730162.fec), reused | F3 / 11C | Other-committee contributions | 26 | $110,000.00 |
| Same original | F3 / 12 | Authorized transfers | 1 | $66,361.85 |
| [1754019](https://docquery.fec.gov/dcdev/posted/1754019.fec) | F3 / 13B | Other loans | 1 | $30,500.00 |
| [1709268](https://docquery.fec.gov/dcdev/posted/1709268.fec) | F3X / 13 | Loan receipts | 1 | $350.00 |
| [1699218](https://docquery.fec.gov/dcdev/posted/1699218.fec) | F3 / 11B | Party contributions | 1 | $250.00 |
| [1700721](https://docquery.fec.gov/dcdev/posted/1700721.fec) | F3X / 11B | Party contributions | 1 | $856.68 |

These are independent report observations, not a cross-family or cross-report
funding total. The remaining three positive shapes already passed the
[candidate-loan comparison](./receipt-family-comparison-2026-09-11.md) and
[complete F3X witness](./receipt-family-witnesses-2026-09-11.md).

The current Go cover reader, family projection, saved-profile verifier and line
census are used unchanged. New source tests compare original period fields and
nonmemo profile measures directly. They do not manufacture OpenFEC records,
`scope_bound` fields, an amendment chain or a family-window result. The F3 other-
loan original is explicitly `F3A`; parsing it does not select an effective report.

## Complete evidence and preserved exceptions

The five originals contain 3,046 records and 2,788 Schedule A rows. Independent
checks verify every original byte/record locator, all Schedule A widths and
filers, exact selected profile groups, money/sign counts, date states/extrema,
and positive cover fields. Only dimensions available in both representations
are regrouped; all raw profile groups remain stored separately. Transaction IDs
are unique in these originals, but the grouped profile cannot establish processed
transaction-ID uniqueness or one-to-one all-field occurrence identity.

Four new originals have complete accepted layout censuses. The reused original
has eleven `SD10` debt rows outside the current census contract. All remain exact
raw records with `unreviewed_record_family`; its `layout_census_complete` stays
false. No debt parser, absence qualification or money use is inferred from them.
Loan originals also retain their Schedule C records without adding balances or
guarantees as another receipt ledger. This follows the separate schedule roles
in the [F3](https://www.fec.gov/pdf/forms/fecfrm3i.pdf) and
[F3X](https://www.fec.gov/pdf/forms/fecfrm3xi.pdf) instructions.

The reused original's two [previously documented one-cent amounts](./receipt-memo-review-2026-09-10.md)
remain unknown in the processed nonmemo SA11AI group. Its original known sum
exceeds the processed known sum by two cents; unknown/sign counts stay explicit.
Other groups agree, including the new contribution and transfer witnesses.
No original value fills a processed null, and no mismatch is hidden to pass a gate.

Filing 1754019 ends with a complete text record without a newline. The existing
Go reader preserves it using verified HTTP completeness. The independent oracle
now also preserves that final record without appending a byte or calling the
response partial. FEC's file separator is not Python's general `splitlines` set.

Workbook checks validate the consumed Schedule A field labels and the exact
family cover positions against the pinned 8.4 workbook. Unused copied labels in
the draft Schedule A layout can differ in whitespace from the workbook (for
example, the first street-address label). They were not rewritten or used as
evidence of an amount/position mismatch; full label transcription remains deferred.

## Verification and retention

Full Go tests, vet, focused race checks, twice-replayed Go source gates,
independent Python checks, Ruff and changed-document link checks pass. The prior
summary-source tests also pass unchanged. The source/profile test reads the saved
profile once per run, not once per committee and not the processed source body.

The rebuilt application SHA-256 is identical to the prior accepted binary:
`a70991ce56a1ccf2baff5b2153bc8b5d8bba638fc9f106dada78a228a7898cd8`.
This change adds tests, fixtures, bounded source evidence and docs, not runtime
code or policy. Prior diagnostics did not need another expensive replay chain.

Evidence lives under
`/storage/dumps/audits/fec/positive-receipt-families/2026-09-11/attempt-01/`.
It contains the four tiny captures, inventory, source assessments, group comparisons,
drivers, logs and focused source snapshot. Existing originals, large profile and
compiled binary are referenced, not duplicated. `SHA256SUMS` and explicit exit
markers cover completion. The active source pointer remains
`b030668efca44fcf611b204665d95c00fa7eda9ce3ce31b5711ebbba6805e81c`.
There was no credential access, graph write, source/calculation publication,
Dagster change, service start or memory-budget change.

## Next boundary

Extend report-level evidence comparisons to the remaining mapped F3/F3X receipt
categories. Preserve the distinction between required itemization, thresholded
detail and explicit cover-only totals; an absent or smaller detail population
must not become zero or inferred unitemized money. Metadata binding for these
new witnesses, broader forms, effective financial membership, cash continuity
and terminal-dollar allocation remain separate acceptance work.
