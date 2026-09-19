# Bounded report-scope assessment

Status: implemented manual Go reader with a passing
[retained-case gate](../audit/report-scope-2026-09-10.md). It describes captured
document representations. It does not select effective financial reports,
correct summaries, or change the processed ledgers. It implements the first
evidence step under the [summary-value use policy](./summary-value-use.md).

## Inputs and verification

`pipeline fec review-report-scope` takes one retained `.fec` body, its response
headers, expected SHA-256 pins, and the recorded public document URL. Only exact
HTTPS `docquery.fec.gov/{paper,dcdev}/posted/<positive-file-number>.fec` URLs
are accepted. The URL is operator-recorded capture provenance; matching hashes
do not independently authenticate a network acquisition. This command makes no
HTTP request and reads no credentials.

The reader enforces a 4 MiB body, 128 KiB headers, 64 KiB physical record, and
4,096-record limit. It requires a regular file, matching digest, one successful
response header block, exact content length, supported media type, and an HTTP
date. Encoded or transfer-framed bodies are not accepted by this bounded reader.

HTTP 200 establishes a complete *captured response*, not complete financial
history. HTTP 206 must describe an exact zero-origin prefix of a larger object.
A prefix's unterminated final record remains incomplete. Absence of later
schedules cannot be inferred from a prefix, even when its last retained record
ends at a newline. LF, CRLF, and a complete response's unterminated final record
are distinct retained byte sequences.

Every physical record retains its exact bytes, offset, length, ordinal, and
digest. Unknown layouts or decoding bytes remain accessible. There is no padding,
truncation, replacement decoding, or duplicate collapse. Exceeding a hard read
budget or failing integrity checks returns an error; original files stay intact.

## Qualified interpretation

The [versioned calculation policy](../../contracts/calculations/fec/report-scope/v1/policy.json)
pins the official paper P3.4 workbook. Runtime interpretation currently covers
its header, 125-field Form 3X cover, and the 47-field Schedule C-1 shape used in
the attachment test. ASCII interpretation is intentionally narrower than full
publisher text support. Other encodings and layouts remain unresolved.

The paper header has six logical fields; the retained dialect also emits one
empty trailing delimiter field. A nonempty seventh field is not ignored. The
paper header's field 6 is marked unused in the workbook; it never creates an
amendment edge. It is not the electronic original-report-ID field.

All cover fields survive, including form/amendment suffix, report code, raw
coverage dates, image fields, and blanks. The exact 100 money positions are
21–27, 29–72, and 74–122. Field 28 is a checkbox and field 73 is a year, not
money. Amounts use checked integer cents, with raw lexemes retained. Blank,
valid zero, valid nonzero, and invalid amounts are separate states.

| Disposition | Evidence established | Still unproven |
|---|---|---|
| `financial_cover_present` | One supported cover has a valid identity/period and populated, parseable amount fields. | Complete schedules, arithmetic agreement, image fidelity, effective financial status, or funding eligibility. |
| `supplemental_attachment_shape` | A complete paper response has one valid-scope cover with every amount blank, followed only by substantive same-filer Schedule C-1 records. | That the original image was faithfully transcribed or which financial report, if any, is effective. |
| `unresolved` | Exact retained evidence and reasons interpretation is incomplete or unsupported. | A financial or supplemental role beyond the reported observations. |

For the second disposition, the lender name and a valid loan-amount field must
be present. The reader does not fully normalize Schedule C-1 or treat its loan
amount as another receipt. A blank cover by itself, an extra/malformed schedule,
multiple covers, conflicting filer IDs, invalid dates, or a partial capture
cannot establish this supplemental shape. None of these rules names a committee
or filing ID.

Electronic 8.4 is not silently parsed with paper P3.4 or the retained electronic
8.5 workbook. The additive [electronic field reader and binding](./report-field-binding.md)
now has its own pinned 8.4 contract. This paper reader and its outputs stay unchanged.

## Metadata is a separate assertion source

Up to four local [metadata captures](./report-metadata-reader.md) can accompany
the document. Each is revalidated through the existing reader, including exact
body/header identity and closed endpoint shapes. Blocked or repeated captures
fail the request. Each input retains its capture digest, endpoint, pagination
state, and matching-record count, including captures with no matching file.

Every exact-file match retains its whole raw record and page/capture ancestry.
The assessment exposes literal differences in form, period, amendment flags,
previous-file assertions, and chains. Different JSON types remain different:
`null`, `false`, a negative reference, and a self-reference are not normalized
into an invented family. Date versus midnight timestamp differences remain in
the raw comparison; cover matching accepts those qualified date representations
but does not truncate non-midnight timestamps. Endpoint labels `F3X` and
`Form 3X` are checked against their respective fields, not treated as identical
raw strings.

Cover/metadata identity and scope disagreements are explicit issues. They do
not rewrite the cover or invalidate its independently observed physical shape.
Missing metadata does not erase document evidence. There is no endpoint
precedence, most-recent winner, or family inferred from equal reporting dates.

## Command and result boundary

```bash
legal-tender pipeline fec review-report-scope \
  --source-url https://docquery.fec.gov/paper/posted/1882886.fec \
  --body <retained-body> --body-sha256 <expected-sha256> \
  --headers <retained-headers> --headers-sha256 <expected-sha256> \
  --metadata-capture <retained-capture.json>
```

JSON goes to stdout. Exit zero means a valid assessment was emitted, including
an explicit `unresolved` result; it does not mean financial readiness. There is
no automatic publication or Dagster asset for this manual reviewer. Python is
used only by independent audit tests, not the runtime implementation.

`original_image_verified`, `history_complete`, and `financial_selection_ready`
are always false. Human image review belongs in separately cited audit evidence;
no caller-supplied trusted flag can promote this result. No existing source,
fact, summary value, Arango projection, or transaction membership is changed.

## Next boundary

The [same-report total-receipts comparison](./report-total-receipts-comparison.md)
now qualifies one precise field and interval for numeric reported-pair comparison.
Financial-component and cycle-comparison eligibility remain separate and false.
The retained paper counterexample includes a populated cover that disagrees
with its original image: presence alone does not qualify its amounts. Do not
choose another report because it makes the cash equation balance.

Broader electronic layouts, financial family selection, old-report refresh,
immutable report publication, and recurring activation remain separate gates.
This reader does not become a new prerequisite for the bulk observation graph.
