# Go report-metadata reader

Status: implemented local review. The user accepted a separate OpenFEC metadata
layer alongside bulk-only A/B/E ingestion. The separate
[HTTP fetcher](./report-metadata-capture.md) now exists. Immutable source publication,
history acquisition, and weekly activation are not implemented in this reader.
The acquisition contract remains draft; this reader does not claim a complete
source population or effective financial report selection.

The [source qualification](../audit/receipt-report-metadata-2026-09-10.md)
established the need and retained the original endpoint disagreements. This
boundary implements the first step in the
[report-coverage requirements](./receipt-report-coverage.md).

## Command and input

```bash
legal-tender pipeline fec review-report-metadata \
  --capture /path/to/retained/filings-capture.json
```

The command reads only local files. It emits versioned JSON to stdout and
diagnostics to stderr. Exit 0 means the observations passed the reader; exit 1
means a transport/contract error or blocking review issues; exit 2 means invalid
command arguments. Structurally parseable rows remain in blocked review output.
Malformed JSON or unverifiable artifacts fail without a result; original files
remain untouched for inspection.

The [capture schema](../../contracts/sources/fec/report-metadata/v1/capture.schema.json)
binds one endpoint, one credential-free query, one pinned publisher schema, and
contiguous page descriptors. Each page names exact body/header file paths,
sizes and SHA-256 digests, a timestamp, and its time basis. `http_date` means the
retained server Date header, not a fabricated client acquisition time.
`client_clock` is explicitly declared by a future capture producer.

Supported endpoint shapes are `/v1/filings/`, `/v1/reports/house-senate/`, and
`/v1/reports/pac-party/`. Scope is either an exact committee and even two-year
cycle, or up to 100 positive file numbers for filings only. There is no arbitrary
URL, credential field, date cutoff, or implicit latest/amended filter. No FEC
credential is read. The descriptor records declared query provenance; independent
source completeness is not inferred from matching returned rows.

The Go boundary enforces:

- One successful JSON response-header block per page, matching content length
  when present and exact server time when that time basis is selected.
- Confined artifact paths through `os.Root`, regular files, exact byte identity,
  and cumulative size limits. Symlinks cannot escape the capture directory.
- At most 16 pages, 100 rows per page, 4 MiB per body, 128 KiB per header file,
  16 MiB total source bytes, and a 256 KiB descriptor. These are explicit initial
  review budgets, not estimates of whole-history cost.

## Preserved observations

Every results-array element retains its one-based ordinal, exact original
object hash, and all raw field values and JSON types. The full original response
is the byte authority: JSON output can reformat whitespace or escape text, so
do not rehash its re-encoded `raw` object as if it were the original object bytes.
The result carries each page descriptor and the capture-manifest digest.

Only positive integral file numbers and syntactically valid committee IDs receive
normalized lookup strings. Null or invalid keys remain raw, with blocking
issues. No amount, date, amendment reference, memo flag, or financial scope is
normalized into a decision. In particular, negative and self predecessor
references, false/null flags, zero values, and numeric strings stay distinct.

The [record contract](../../contracts/sources/fec/report-metadata/v1/record.schema.json)
and compiled Go field maps describe complete endpoint shapes. A regression test
requires them to agree. Every contracted field must be present; present nulls
are retained independently of the publisher's incomplete nullable annotations.
Missing/unknown fields and unreviewed types block review without discarding rows.

The reviewed deviations are explicit: nine report financial fields permit the
observed number/string representations, and report amendment chains permit
number/string elements. This accepts representation evidence, not a numeric
conversion or financial interpretation. Integer slots use integral JSON lexemes;
no floating-point conversion occurs. Invalid UTF-8, unpaired Unicode surrogates,
duplicate JSON keys, trailing documents, and nesting above the reader's limit
reject parsing instead of silently replacing or overwriting data.

Repeated file numbers within one endpoint capture remain separate occurrences
with a blocking issue. The same file in different endpoints remains separate
assertions. The reader does not resolve cross-endpoint conflicts or select a
preferred flag.

## Pagination is not financial completeness

| State | Evidence |
|---|---|
| `partial` | More query pages may exist; an approximate count never closes traversal. |
| `exact_count_satisfied` | Retained rows equal a stable publisher-declared exact count. No terminating empty page is asserted. |
| `empty_page_observed` | The contiguous traversal ends with an actual empty response. This is not an atomic source snapshot. |

The reader rejects page gaps and changing counters, flags inconsistent exact
counts/page counts, and preserves missing requested file IDs without claiming
permanent absence. It does not stop at a publisher's approximate `pages` value.
No network pagination loop runs here.

Both `history_complete` and `financial_selection_ready` remain false in every
result. Exact-count satisfaction does not close amendment families, distinguish
attachments from replacements, establish report/account coverage, or authorize
a cycle-summary difference.

## Verification and next boundary

The [implementation gate](../audit/report-metadata-reader-2026-09-10.md) checks
all 51 observations in the three retained endpoint responses, preserves the
known disagreements, and verifies deterministic CLI replay and independent
all-field/object-hash readback. No transaction scan, API call, source/fact pointer,
graph, or Dagster asset changes.

The [bounded Go HTTP fetcher](./report-metadata-capture.md) now emits this descriptor
and retains failed attempts with header-only authentication, explicit budgets,
retries, and capture status. This reader accepts both curl's `HTTP/2` and Go's
`HTTP/2.0` header representation. The
[bounded report-scope assessment](./report-scope-assessment.md) now consumes these
captures alongside retained originals without selecting financial replacements.
The [membership review](./report-period-membership.md) now checks observed chains
and date coverage without financial selection. Financial family selection and
old-report refresh remain separate;
new-receipt polling alone cannot keep older status assertions current. Each
metadata capture's ancestry stays separate from the bulk release it may inform.
