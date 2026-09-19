# Summary discrepancies traced to original reports — 2026-09-10

Status: bounded source investigation complete. Three selected 2024 discrepancies
have reproducible source-level explanations. None is an authorized correction,
a general report-selection rule, or proof of wrongdoing. The
[four-cycle arithmetic audit](./summary-assertions-2026-09-10.md) remains unchanged.

## Scope and result

Selection used the largest absolute 2024 cash residual, the next largest cash
residual, and the largest individual-subtotal residual in the verified profile.
This is a diagnostic sample, not an estimate of error prevalence.

| Witness | Exact discrepancy | Established failure mode |
|---|---:|---|
| NRCC, `C00075820` | $21,876,124.94 cash residual | Published amendment metadata selects a blank financial cover associated with a loan attachment instead of September's financial report. |
| SID FOR CONGRESSS, `C00843367` | $1,500,000.00 cash residual | Adjacent reports disagree on carried cash; the committee separately acknowledged reporting errors. |
| AAB*PAC, `C00249581` | $698,334.00 individual-subtotal residual | FEC's paper transcription disagrees with the original report image. |

The Go audit tests reconstruct the numeric relationships from pinned source
bytes and the earlier raw-CSV profile. They do not change source facts or choose
a replacement financial amount. Named committees occur only in this audit and
its opt-in regression test, not a production exception table.

## NRCC: an attachment is not a complete financial amendment

The captured public report listing contains 34 report records. Taking its 24
`is_amended=false` records reproduces the cycle CSV's receipts
($236,307,296.49) and disbursements ($219,752,099.68) exactly. That filter is a
reproduction experiment, **not** an accepted amendment-selection policy.

For September 1–30, 2024, the source exposes these versions:

| File | Captured representation | Published selection | Financial evidence |
|---|---|---|---|
| `1833804` | Electronic Form 3X, format 8.4 | Amended / not most recent | Receipts $18,848,596.96; disbursements $40,724,721.90. |
| `1876290` | Paper transcription, `HDR P3.4`, `F3XA` | Amended / not most recent | Financial slots blank in transcription, zero in processed report response. |
| `1882886` | Paper transcription, `HDR P3.4`, `F3XA` | Not amended / most recent | Financial slots blank in transcription, zero in processed report response. |

The [latest original image](https://docquery.fec.gov/pdf/634/202410240300487634/202410240300487634.pdf)
is four pages: a cover memo, Schedule C-1, and mailing evidence. It does not
contain a replacement Form 3X financial summary. The
[paper transcription](https://docquery.fec.gov/paper/posted/1882886.fec)
nevertheless includes a blank Form 3X amendment cover. Its `P3.4` header is **not**
an electronic format-8.4 header; the layouts are different.

The [original electronic report](https://docquery.fec.gov/dcdev/posted/1833804.fec)
reports opening cash $70,751,396.74 and closing cash $48,875,271.80. Its period
cash equation balances. Disbursements minus receipts are $21,876,124.94,
exactly the cycle residual. Adding that report's receipts/disbursements back
to the selected totals makes the cycle equation balance. This counterfactual
isolates the effect of the observed selection; it does not authorize treating
that version as legally effective or establish complete account coverage.

Only bytes 0–16,383 of this 19,020,241-byte electronic filing were requested.
HTTP 206 and `Content-Range: bytes 0-16383/19020241` are preserved. The complete
header and financial-cover record fit in that prefix. No full transaction-file
capture or schedule-completeness claim is made.

## SID: balanced reports can still disagree at their boundary

The five captured most-recent financial reports cover April 1, 2023 through
April 30, 2024. Their individual period cash equations all balance. Their period
receipts and disbursements sum exactly to the cycle CSV's $2,120,568.37 and
$598,964.00. Thus our parser did not introduce this discrepancy.

But [amended Q1 filing 1780310](https://docquery.fec.gov/dcdev/posted/1780310.fec)
ends March 31 with $1,821,604.37. The
[termination filing 1780346](https://docquery.fec.gov/dcdev/posted/1780346.fec)
begins April 1 with $321,604.37: a $1,500,000 gap. The termination report's
cumulative disbursements also exceed the sum of report-period disbursements
by $1,500,000, with the same difference in candidate-loan repayment columns.
Selecting the cumulative column merely because it balances would conceal this
contradiction rather than resolve it.

The FEC's [June 5 termination review letter](https://docquery.fec.gov/pdf/013/202406050300213013/202406050300213013.pdf)
explicitly identifies the cash carry-forward mismatch and requests corrections.
It also raises termination, residual-fund, and loan-forgiveness documentation
questions. The committee's [July 11 explanation, filing 1796046](https://docquery.fec.gov/dcdev/posted/1796046.fec)
attributes erroneous candidate-loan repayment and refund reporting to software
issues, and asserts no remaining cash and forgiveness of the remaining loan.
Those are **filer assertions**, not verified cash transfers or replacement facts.

This is stronger than an unexplained residual, but it does not supply an
accepted corrected ledger. Keep the original reports, FEC request, and filer
explanation distinct. Do not infer that $1.5 million was paid or forgiven solely
from the numerical gap.

## AAB*PAC: original image versus machine transcription

The [paper transcription of Q2 filing 1813890](https://docquery.fec.gov/paper/posted/1813890.fec)
contains $699,033.00 in its itemized-individual period slot, $0.00 unitemized,
and $699.00 total individual contributions. These reproduce the three cycle-CSV
columns exactly and account for the $698,334.00 residual.

Page 3 of the [original image](https://docquery.fec.gov/pdf/120/202408020300479120/202408020300479120.pdf)
visibly reports $699.00 itemized, $0.00 unitemized, and $699.00 total in both
columns. The raw paper transcription disagrees with the submitted image.
This supports a source-transcription discrepancy, not a scaling heuristic.
Do not divide suspicious amounts, replace a field from arithmetic, or overwrite
the captured publisher record. A future reviewed correction would be separate
evidence with image/page/field provenance and its own acceptance policy.

The visual check is manual. The Go test pins the PDF digest and verifies the
transcribed fields and arithmetic; it does not pretend to perform OCR.

## Capture, verification, and limits

The old public document indexes returned HTTP 403. Discovery therefore used
three bounded public OpenFEC committee-filings requests and one committee-reports
request, each for cycle 2024 with 100 records per page. Every response declares
one complete page. Requests used the public `DEMO_KEY`, not a private credential.
API discovery is **audit-only**; bulk ingestion and its release contract are
unchanged. Captured responses retain individual source URLs and version flags.

Full small originals, response headers, PDFs, one explicitly partial electronic
report, the prior profile, and the previously pinned FEC specification workbook
are retained under `dumps/audits/fec/summary-report-review/2026-09-10/attempt-01/`
in project storage. Report/PDF/API bodies total 1,507,096 bytes, excluding
headers, specification/profile copies, and rendered audit images. Each complete
download was capped at 10 MiB or less; the large report used a 16 KiB range.
No bulk source was downloaded or extracted.

The [digest fixture](./fixtures/summary-report-review-2026-09-10.sha256) binds
the evidence. The [opt-in Go test](../../internal/calculation/fec/summaryassertion/report_review_test.go)
uses exact rational-to-cent conversion, never floating point. It checks report
identity, the explicit prefix boundary, source/profile equality, period equations,
inter-report differences, and the observed amendment-selection counterfactual.
Electronic offsets were checked against the pinned FEC workbook. `P3.4` offsets
are scoped to exact audited artifacts and original images, not a shipped parser.

The focused corpus gate passes, including all three witnesses. The package's
race gate passes in 1.124 seconds; static analysis and the complete Go suite
also pass. `verify.exit=0` records success. Logs, verification timestamps, and
the test source are retained with the evidence. A read-only post-test check
confirmed the active v4 manifest digest remains
`b030668efca44fcf611b204665d95c00fa7eda9ce3ce31b5711ebbba6805e81c`.
The offline verification container used a 2 GiB cap, a 1 GiB Go target, two CPUs,
read-only project/source mounts, and only the audit directory/compiler caches
as writable paths. No Python code or new production runtime was added.

```bash
LT_SUMMARY_REPORT_AUDIT=<retained-audit-directory> \
  go test -count=1 -run TestSummaryReportReviewCorpus -v \
  ./internal/calculation/fec/summaryassertion
```

No source or fact was repaired. Arango, Dagster, calculation eligibility, active
release ancestry, and automation remain unchanged. This sample does not classify
all 4,572 cash or 610 subtotal discrepancies, and equality elsewhere does not
prove completeness or available cash.

## Next boundary

Define a field- and scope-qualified summary/receipt comparison: distinguish an
available **reported summary observation** from an accepted cash denominator.
Expose unknown account/report coverage, arithmetic conflicts, and source-version
mismatches as explicit blockers. Keep unitemized observations separate from
detail gaps. Do not require an invented correction for every historical anomaly
to preserve and compare reported evidence.

Any future effective-report selector must distinguish a financial replacement
from an attachment, preserve paper/electronic provenance and amendment evidence,
and test both report arithmetic and inter-report continuity. A highest file number
or `most_recent` flag alone is insufficient. This gate does not select a new
production source or implement a report-level ingestion pipeline.
