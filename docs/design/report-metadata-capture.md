# Bounded Go report-metadata capture

Status: manual HTTP capture implemented. This extends the
[local reader](./report-metadata-reader.md), not the coordinated bulk release,
Dagster schedule, or financial report selector.

The [implementation audit](../audit/report-metadata-capture-2026-09-10.md) records
passing offline gates and live seven-record readback. The terminating live page
hit the public demo quota; complete live traversal is still unverified.

## Why metadata is separate

The downloaded Schedule A/B/E relations remain the detailed transaction sources.
They do not enumerate every report independently of transaction rows. The
[source qualification](../audit/receipt-report-metadata-2026-09-10.md) found a
zero-Schedule-A termination report, attachment/replacement ambiguity, and bulk
period-summary headers without exact filing IDs or amendment chains. FEC says
its [period summaries](https://www.fec.gov/campaign-finance-data/committee-report-summary-file-metadata/)
incorporate amendments; those summaries are not preserved report-version history.

OpenFEC supplies additional report assertions. It does **not** resolve the checked
endpoint disagreements automatically. We did not identify a suitable processed
report-history bulk export in the inspected catalog; that is not proof none
exists. The small HTTP gate verifies capture, not a new transaction source or
full historical coverage. Reuse retained pages for development and replay.

## Interface

```bash
legal-tender pipeline fec capture-report-metadata \
  --request /audit/request.json --output-dir /audit/new-capture \
  --api-key-env ELECTION_API_KEY
```

For a small manual test, `--demo-key` uses the public demo credential instead.
Never pass a private key as an argument or put it in request JSON. The command
does not load `.env`. The caller supplies the configured environment variable.

The closed [request schema](../../contracts/sources/fec/report-metadata/v1/request.schema.json)
requires contract/schema identity, one endpoint, explicit query, and all budgets:

```json
{
  "contract": "fec/report-metadata@1.0.0",
  "schema_sha256": "0cca7f2af73270b35d7276e53d99b56b2f8f9970b33cb5d36a85fdb8b773caed",
  "endpoint": "/v1/filings/",
  "query": {"committee_id": "C12345678", "cycle": 2024, "per_page": 100},
  "limits": {"pages": 3, "requests": 4, "attempts_per_page": 2, "source_bytes": 2097152}
}
```

The committee ID above is synthetic, not a runtime default. Alternatively,
`/v1/filings/` accepts up to 100 explicit positive, unique `file_numbers`.
The two report endpoints require a committee and cycle. There is no arbitrary
URL, full-history switch, latest-only filter, transaction endpoint, or resume.
Every run requires a new directory with an existing parent.

## Transport and budgets

The fixed HTTPS origin is `api.open.fec.gov`. Authentication uses the
[documented header](https://api.data.gov/docs/developer-manual/#api-key-usage).
The client disables implicit proxies, redirects, decompression, and connection
reuse. It has a 30-second request timeout and five-minute run deadline. DNS/TLS
use Go's normal verified transport; no insecure TLS mode exists.

Limits are 16 pages, 48 request attempts, three attempts per page, 100 rows per
page, 4 MiB per body, 128 KiB per decoded header block, and 16 MiB across body
reads plus serialized non-sensitive headers. Run records/checkpoints/reviews are
additional bounded files, not part of that source-byte counter. TLS framing and
discarded sensitive headers are not counted as source bytes. Header-limit stops
retain no response artifacts and report zero captured/read-accounted bytes.
An overflow probe consumes at most one additional body byte inside the run cap;
only the bounded prefix is retained and marked incomplete.

Requests run serially with at least a one-second wait between attempts. Transport
failures, truncated bodies, and HTTP 429/500/502/503/504 can retry within budgets.
`Retry-After` seconds and HTTP dates are honored; a delay over 30 seconds or an
unparseable value stops as `retry_deferred` instead of retrying early. Other HTTP
statuses, unsupported encodings, invalid JSON, schema drift, changed pagination,
or repeated report IDs stop without selecting reports.

Authentication/cookie response headers are omitted with their names recorded.
Response bodies retain exact bytes unless they exceed the read budget or echo
the credential. Plain, URL-encoded, and ordinary JSON-escaped echoes are checked;
matching responses are suppressed and block the capture. Raw transport error
messages are never retained. Header artifacts preserve Go-decoded values, not
original wire casing/order; this representation is explicit in the result.

## Evidence and completion

The run directory is private (`0700`); files are exclusive (`0600`) and synced:

- `request.json` records scope/budgets without authentication.
- `attempt-NNN-start.json` precedes each request. `attempt-NNN.json` records its
  outcome, status, times, byte accounting, omitted header names, and file digests.
- Complete or bounded rejected response bytes remain `.body`/`.headers` artifacts
  except for explicitly suppressed credential echoes or oversized headers.
- `checkpoint-NNN.json` binds accepted transport pages. The local reader checks
  every checkpoint; valid parsing produces `review-NNN.json` including any issues.
- Only a validated traversal ending in an actual empty response produces final
  `capture.json` and `review.json` references in `result.json`.

`result.json` follows the [outcome schema](../../contracts/sources/fec/report-metadata/v1/fetch-result.schema.json).
`captured` means this explicit query reached an observed empty page. `incomplete`
means a budget, deferred retry, or cancellation stopped traversal. `blocked`
means rejected transport/source evidence. `failed` covers a local write failure;
its final record may be absent or unreadable. CLI success requires `captured` and
exit zero; consumers must also verify referenced artifact digests. Process
disappearance, a checkpoint, or a partially written result is not success.

Exact-count satisfaction alone does not stop fetching. Empty-page observation
does not establish an atomic API snapshot or close amendment families.
`history_complete` and `financial_selection_ready` always remain false.
Client capture times use `client_clock`; server Date stays separate in headers.
API observations are never relabeled as members of the weekly bulk snapshot.

## Next boundary

Use the retained original/metadata cases to define report-family and attachment
selection rules, including explicit unresolved outcomes. Then test exact
detail-to-cover membership before cycle-summary comparison. Old-report refresh,
full-population coverage/cost, immutable metadata publication, and recurring
activation remain separate gates. No transaction scan or new service is needed
to continue this bounded selection work.
