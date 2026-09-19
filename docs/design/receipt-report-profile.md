# Same-release receipt report-occurrence profile

Status: implemented manual Go diagnostic with a passing
[complete 2024 gate](../audit/receipt-report-profile-2026-09-10.md). It extends the
[summary/receipt readiness investigation](./summary-receipt-compatibility.md),
not the accepted receipt fact set or financial calculation.
The [wire contract](../../contracts/audits/fec/receipt-report-profile/v1/)
requires explicit limits and prohibits comparison or terminal eligibility.

The explicit [v2 profile](./receipt-report-profile-v2.md) now extends this grain
to every report-line population with independent memo and individual axes.
V1 remains the default for compatibility; select v2 with `--profile-version 2`.

## Source boundary

The command accepts a published committee-summary manifest and cycle. It verifies
the summary's immutable backing, source bytes, stored facts, and exact assertion
grouping. It then selects Schedule A from **that exact release**, not from the
current pointer or a caller-supplied relation. The release digest and identity
must match the summary ancestry. A missing or ambiguous selected relation fails.

This path allows source-aligned investigation before a new large Parquet fact
publication. It does not relabel older facts, adopt changed shards, or establish
equivalence between source versions. A change in physical row count is a net
change, not proof of append-only changes or a complete changed-record inventory.

One pass decompresses the already-staged relation. The existing strict Schedule A
verifier validates every row and recomputes compressed/uncompressed byte counts
and SHA-256 digests. A synchronous observer sees only source-valid rows. Its
observations remain provisional until complete verification succeeds; errors,
cancellation, mismatched counts/hashes, and unsupported amounts produce no result.

The observer does **not** verify `SUB_ID` uniqueness. Each physical occurrence,
including an identical repeated row, remains an occurrence. This result cannot
replace the occurrence publisher, normalized fact set, or effective calculation.

## What is counted

The first table partitions all physical occurrences by exact filing form,
schedule type, line number, and existing individual-predicate decision. It retains
known and unknown amounts, positive/negative/zero counts, signed minor units,
and conduit-ID presence. Those are diagnostic source measures, not a cash total.

The second table partitions only the included individual predicate by exact
committee, file number, form, schedule, line, report type, and report year. It
retains the same measures and first/last source-row ordinal. Nulls and empty
strings remain distinct. An unexpected form/line or malformed committee identity
is not discarded. Group boundaries do not infer a report's amendment status.

This v1 profile cannot reconstruct complete per-report form-line membership:
excluded-predicate rows have no report groups, and `excluded_non_individual`
does not independently expose memo status. The
[bounded report-line reviewer](./receipt-report-lines.md) now separates these
axes and passes a small original-file gate. A future cycle-wide consumer must
preserve that grain explicitly; do not reinterpret existing excluded groups.

The two tables are overlapping views: **never add them together**. The form table
must conserve all source occurrences. Included form groups and the report table
must conserve the same individual-predicate occurrences and every amount measure.

Each included report group also retains receipt-date counts: missing, invalid,
before-cycle, in-cycle, and after-cycle, plus observed valid date minima/maxima.
These categories partition its occurrences. The cycle interval comes from the
requested even year, not a fixed four-cycle list. Dates are not clipped. Observed
receipt-date extrema are **not** report coverage dates or proof that a report
period is complete. Excluded-predicate dates remain in the source and are outside
this narrow date profile.

## Why form scope matters

The FEC's [individual-contribution methodology](https://www.fec.gov/campaign-finance-data/about-campaign-finance-data/methodology/)
describes a classification that can include several form lines, including transfer
lines. It is not simply a test for a single individual-contribution summary line.
The FEC's [receipt-data reference](https://www.fec.gov/campaign-finance-data/about-campaign-finance-data/about-receipts-data/)
separates those lines by form. The profile records the actual labels without
silently replacing the accepted predicate or claiming a reviewed form mapping.

Shared release ancestry establishes a common source selection, not synchronized
report/account coverage. Publisher-key uniqueness, effective-report selection,
summary report/account membership, report coverage, and accepted form-line
equivalence remain separate checks. Cash funding and terminal attribution are
also unestablished. This command calculates no summary-minus-detail difference.

## Runtime and identity

```bash
legal-tender pipeline fec profile-receipt-report-scope \
  --storage-root /storage \
  --summary-facts <published-committee-summary-manifest> \
  --cycle <source-cycle>
```

The reader uses a bounded zstd decoder. Each form table, report table, and date
cache has a 500,000-entry cap; reaching a cap fails instead of dropping groups.
Exact money outside the profile's signed-int64 range or a sum overflow also fails.
The real job runs offline with a container memory cap, read-only source storage,
and a writable diagnostic directory. No corpus-sized intermediate is written.

Output ordering compares exact cells, with null before present values. The profile
ID hashes compact Go JSON with `profile_id` empty. The pinned release's selected
source metadata remains part of that identity. Per-run elapsed time is logged to
stderr and normalized to zero in the embedded verifier result. JSON is emitted
only after complete input verification and conservation.

This is a manual audit format, not accepted recurring storage. No source download,
extraction, fact pointer, graph, API, or Dagster asset is changed. Python is limited
to independent test/schema checks.

## Next gate

Extend the profile with complete per-report line membership and independent
memo/individual axes established by the bounded reviewer. Establish its
unique/effective membership before treating an occurrence sum as a comparable
financial result. Preserve mismatches; do not repair summaries or infer unitemized
receipts from differences. Report-source ingestion and correction policy remain
separate contracts.
