# Receipt-family absent-population evidence

Go's `review-receipt-family-absence` adds a per-report **qualified reported zero**
to the [family comparator](./receipt-family-comparison.md). It leaves that
comparator's absent detail and difference null. It creates no receipt fact,
donor, cash balance, window total or financial membership decision.

The [versioned policy](../../contracts/calculations/fec/receipt-family-absence/v1/policy.json)
accepts only the comparator's existing F3/F3X contribution, transfer and loan
subset. These are reviewed all-required-itemized Schedule A families, not every
receipt category. The [source map](./receipt-families.md) pins their official
form instructions and workbook positions. This qualification rule is a project
evidence contract, not an FEC guarantee that a filing is complete or correct.

## Required evidence

All conditions must pass for one exact report/family:

1. The complete original and retained metadata agree on an explicit numeric zero
   in the bound period field. Form, filer, report, dates and observed chain
   candidate pass the existing binding contract.
2. The pinned complete processed profile passes content, group conservation and
   exact summary/release ancestry verification. No bulk body is rescanned.
3. Every original record passes the narrow layout census. All Schedule A line
   counts agree between that original and the complete report profile.
4. Neither inventory contains an occurrence on the exact family line. All memo
   states count as presence; zero-valued rows and offsetting amounts also count.

Presence on a different reviewed line does not prevent a family's absence from
qualifying. Conversely, counts that disagree on any Schedule A line block this
negative-evidence qualification for the whole report. Count equality does not
prove per-transaction identity or the correctness of the publisher's ledger.

Missing, blank, conflicting or nonzero fields never qualify. A complete profile
alone or an original alone is insufficient. Superseded, partial or wrong-scope
reports remain blocked. Known original line counts in an incomplete census are
diagnostics only; an unreadable record retains its ordinal and issue.

## Original record census

The source reader re-verifies the pinned original body and HTTP headers. It
accepts the existing electronic 8.4 F3/F3X cover contract and retains its limits:
4 MiB per original, 4,096 records, and the shared document-set budget. No new
download, API, runtime schema discovery or larger reader budget is introduced.

The census uses strict ASCII, literal `0x1c` fields and the existing LF/CRLF
framer. A final unterminated record can be complete only within a verified
complete response. It neither pads short rows nor discards extra fields. Every
post-cover record must carry the exact cover filer in field 2.

| Record family | Width | Accepted meaning in this census |
|---|---:|---|
| Schedule A | 45 | Exact `SA` plus a reviewed leaf line for this form |
| Schedule B | 44 | Structurally recognized non-SA record; tag retained |
| Schedule C | 38 | Structurally recognized non-SA record; tag retained |
| Text | 6 | `TEXT` record; body retained in the original |

The widths and field-2 meaning are checked against the retained official 8.4
workbook from the [FEC electronic filing specifications](https://efilingapps.fec.gov/registration/softwarelogs.htm).
Non-SA tag patterns identify record families only. They do not validate a
disbursement or loan's financial line, amount, liability or cash meaning. An
unknown Schedule A line, other record family, width mismatch, filer mismatch,
unreadable record or source-scope issue blocks absence. Other schedules and
format versions need an explicit reviewed extension; 8.4 is not the current
format for every filing.

## Command and output

```sh
legal-tender pipeline fec review-receipt-family-absence \
  --storage-root /storage --summary-facts /path/to/exact-summary-manifest.json \
  --capture /path/to/retained-report-capture.json \
  --documents /path/to/pinned-documents.json \
  --start YYYY-MM-DD --end YYYY-MM-DD \
  --profile /path/to/complete-profile-v2.json --profile-sha256 EXPECTED_SHA256
```

The output embeds the unchanged older result in `compared`. Each report adds
its original census, line-count comparison and family decisions. A qualified
family has `state: qualified_reported_zero` and
`qualified_reported_zero_minor_units: "0"`. A blocked family has a null value
and explicit blockers. No field replaces `detail_minor_units` in the old result.

The [retained gate](../audit/receipt-family-absence-2026-09-11.md) verifies the
implementation and exact old-command replay. This remains a bounded diagnostic,
not a scalable per-committee production execution strategy.

## Unchanged limits and next gate

Family-window comparison readiness, financial-use eligibility and terminal
eligibility remain false. Date gaps, missing covers and unknown cycle prefixes
remain visible in the embedded evidence; per-report zero qualification does not
make them covered. Thresholded/cover-only families, other schedules, unsupported
forms and loan-principal attribution remain outside this rule.

The separate [family-window comparator](./receipt-family-window.md) now aggregates
qualified nonempty comparisons and these reported-zero observations with exact
field/date coverage. This command's output and closed window guard remain
unchanged. The separate [family-summary comparator](./receipt-family-summary.md)
now adds scoped summary differences. Remaining receipt categories and wider
source populations stay separate gates; no gap becomes unitemized money.
