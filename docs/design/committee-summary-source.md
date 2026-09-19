# Committee financial-summary source boundary

Status: implemented read-only Go reader/verifier with a passing
[four-cycle reader gate](../audit/committee-summary-reader-2026-09-08.md), following
the [source review](../audit/committee-summary-source-2026-09-08.md).
The Go immutable fact publisher and opt-in release-v4 membership now exist.
Their original [publication gate](../audit/committee-summary-publication-2026-09-08.md)
passes complete raw/artifact comparison and fixture release integration. The
[real v4 gate](../audit/fec-v4-publication-2026-09-10.md) now passes coordinated
source publication and complete four-cycle summary readback/replay. The source
contract is accepted for lossless preservation. V4 is the active source release;
the [manual Dagster handoff](#manual-dagster-handoff) is implemented while
default discovery remains v3. This is the next input for the
[funding coverage gate](./funding-coverage-and-time.md), not terminal allocation.

## Selection

Use the official cycle-specific `committee_summary_<cycle>.csv` product for
committee-cycle financial assertions. Keep all source columns and occurrences.
Do not promote it to a report-level cash ledger or merge it with Schedule A/B/E.

| Product | Useful scope | Missing requirement / disposition |
|---|---|---|
| Existing `weball` / `webl` | Candidate financial summaries | Not individual upstream committee statements; retain existing separate comparisons. |
| Classic `webk` | PAC/party summary, including opening and closing cash | No explicit unitemized-individual column, coverage start, or report identity; not the new authority. |
| Modern committee-summary CSV | Committee-cycle financial assertions, coverage start/end, explicit unitemized individual subtotal, opening/closing cash | Selected for this fact family; report/amendment/account identities remain absent. |
| Published processed database dumps | A, B, E, and committee history | The audited public dump prefix has no financial-report summary dump. No new all-history extraction is selected. |
| Raw filings or report API | Potential report-specific evidence | Separate source and revision contracts would be required; not added as a fallback here. |

The [official committee-summary description](https://www.fec.gov/campaign-finance-data/committee-summary-file-description/)
describes a nightly processed product combining reporting activity within the
two-year period. The [PAC-summary dictionary](https://www.fec.gov/campaign-finance-data/pac-and-party-summary-file-description/)
describes the narrower classic layout. Actual captured files, not an assumed
dictionary match, determine the proposed physical contract.

## Physical boundary

The [accepted source contract](../../contracts/sources/fec/committee-summary/v1/contract.json)
pins a plain UTF-8 CSV with an embedded 92-column header. All four reviewed
files have the same header and valid record widths. No ZIP, database restore,
or API pagination is required. The acquisition URL is cycle-scoped; it does not
require downloading pre-window history.

Use a strict CSV reader, not splitting on commas or assuming physical lines
always equal records. Preserve quoting, record byte spans, source strings, and
artifact identity. The reviewed files use LF and contain no embedded newlines;
that observation does not justify a line-splitting parser.

The source differs from its web dictionary:

| Position | Actual CSV header | Dictionary spelling |
|---|---|---|
| 51 | `OTHER_FED_OP_EXP` | `OTH_FED_OPE_EXP` |
| 77 | `SUBTTL_OTHER_REF_REB_RET` | `SUBTTL_OTHER_REF_REB_RETB` |
| 89 | `TTL_COMMUNICATION_COST` | `TTL_COMMUNICATION_COSTS` |

Both date columns use `YYYYMMDD`, not the dictionary's slash-separated form.
Pin these reviewed differences; do not add a permissive runtime alias table or
let future remote documentation alter parsing. The raw record schema keeps all
fields as strings. Typed validity is an independent result, not a reason to
erase the occurrence.

Normalize the 75 monetary fields using exact signed cents. Leading-decimal
lexemes such as `.32` and `-.01` are valid decimal representations. Blank,
zero, and negative values are distinct. Strict date validation must retain
`99999999` as invalid raw evidence; its publisher meaning is not established.
Reversed and out-of-cycle intervals remain explicit issues, not clipped dates.

## Identity and repeated financial assertions

`CMTE_ID` is not a unique row key. In each reviewed file, every repeated
committee group differs only in `CAND_ID`; all other fields are identical.
The observed `(CMTE_ID, FEC_ELECTION_YR, CAND_ID)` composite is unique in these
snapshots, but it is not a guaranteed publisher revision identity.

Preserve every occurrence using artifact digest, logical record ordinal, byte
span, and raw-record digest. Report both committee-key and composite-key
multiplicity. Never overwrite a row by committee ID or sum each candidate
reference as additional receipts.

The [exact assertion calculation](./committee-summary-assertions.md) now groups
only identical values across all 91 non-candidate fields, retaining every
membership and candidate reference. Conflicting variants remain explicit with
no first/last-wins financial selection. It adds arithmetic diagnostics but does
not establish a cash denominator or financial-use eligibility. `CAND_ID` also
does not replace the accepted candidate-authorization linkage source.

## What this source can and cannot establish

The explicit unitemized field removes the need to invent that component from a
detail/summary residual. Opening cash supplies a reported cycle-level balance,
not the identities of donors behind carry-in funds. Values can be blank and
coverage can differ by committee. A populated field is not proof of a complete
financial statement.

The file has no report number, amendment chain, account identifier, or complete
report sequence. It cannot establish recipient cash availability, bank-account
ownership, within-period ordering, or original donors of opening funds. Do not
label all receipt categories spendable cash or assume a single account.

The review compares two equations where all operands exist:

- Opening cash + total receipts − total disbursements = closing cash.
- Itemized individual subtotal + unitemized individual subtotal = individual total.

Differences occur in every reviewed cycle. These are diagnostics, not automatic
proof of incorrect disclosure: scope, processing, adjustments, or source issues
still need investigation. Preserve the values and difference; do not correct
one field to make an equation balance or derive an unitemized amount from it.

## Refresh and acceptance

Capture an immutable whole CSV per selected cycle with HTTP metadata, full
SHA-256, and retrieval time. Retain the object version when available. A newer
snapshot can revise an older coverage interval; maximum coverage date is not an
amendment-selection policy. Snapshot changes are not newly received dollars.

The opt-in [release v4](../../contracts/releases/fec/v4/) extends the existing
Monday discovery/acquisition shape without changing release v3. The September research
snapshots are not coherent members of the older receipt bundle. Do not join
them to current graph money under a same-release claim.

The [real refresh-plan audit](../audit/fec-v4-refresh-plan-2026-09-09.md) now
measures download cost and identifies hard-link accounting and staging-budget
issues. The [storage review](../audit/fec-storage-review-2026-09-09.md) fixes inode
accounting; its original full-reserve scenario exceeded the cap. The
[streaming storage gate](../audit/fec-streaming-storage-2026-09-09.md) now passes
with runtime growth guards and a fitting read-only saved-plan scenario. The
[fresh preflight](../audit/fec-v4-preflight-2026-09-09.md)
confirms the same fitting candidate. The approved
[acquisition](../audit/fec-v4-acquisition-2026-09-09.md) now passes its durable
completion and independent verification checks, with v3 unchanged. The
[verified stage](../audit/fec-v4-staging-2026-09-09.md) now passes all selected
output, readback, storage, and independent verification checks. Coordinated v4
publication and all same-release summaries now pass. Default discovery still
selects v3; changing that default is separate from the manual summary job.
No weekly automation was enabled by source publication or summary wiring.
Every source must be freshly observed; only unchanged publisher versions may
reuse prior artifacts. The initial summary implementation did not download
large schedules; the separately approved acquisition did. Committee-summary
assertion grouping is now implemented separately. Remaining arithmetic causes
and scope-qualified receipt reconciliation need further evidence; report-time
funding and pooled allocation remain separate.
Do not label research snapshots as same-release graph inputs.

## Implemented reader and verifier

```bash
legal-tender pipeline fec verify-committee-summary \
  --input <captured-csv> --cycle <source-cycle> \
  --expected-sha256 <capture-sha256> --expected-bytes <capture-byte-count>
```

The CLI accepts regular local files only. Go first verifies a bounded whole-file
copy against the supplied capture identity; it does not derive its expected hash
from the scan. Limits are 16 MiB per artifact, 1 MiB per accepted logical record,
and 100,000 data records. These fit the measured source; exceeding them fails
explicitly. There is no unbounded identity index or historical dump extraction.

The reader uses Go's strict CSV decoder, checks exact header bytes, rejects
blank records before the decoder can skip them, and retains logical record
ordinals, byte spans, and raw SHA-256. Quoted commas, doubled quotes, UTF-8, and
embedded LF survive. CR and missing final LF fail the current physical contract;
CR is not silently normalized to LF. A format extension requires source review.

Each returned record owns all raw fields, 75 typed money observations, both
typed dates, three typed identity fields, and issue references. Money/date/ID
states are `valid`, `source_blank`, or `invalid`. Nonblank money has a checked
signed-cent value only when valid; source scale is meaningful after successful
lexical parsing. Codes remain raw and explicitly uninterpreted. Invalid candidate
references are not rewritten as committee identities or dropped with their rows.

The [verification result](../../contracts/audits/fec/committee-summary-verification/v1/)
contains field coverage, multiplicity/conflict profiles, interval observations,
the two diagnostic equations, and complete issue counts with at most ten examples
per code. The reader exposes every issue per row; the diagnostic example cap
does not truncate source preservation. Equation arithmetic uses exact integers
without signed-overflow wraparound. There is no cross-committee money total.

Physical errors, corruption, read failures, wrong/invalid source-cycle values,
limits, and cancellation produce no successful JSON. Other typed issues remain
in a completed scan; `complete` means source-scan completeness, not valid money
attribution. Every result keeps `terminal_attribution_eligible=false`.

Result fingerprints bind every source field and every typed money/date/identity
value in row order. The independent corpus test reconstructs both hashes directly
from the CSV; all four cycles pass. Results have no timestamp, local path, or
duration field, and replay is byte-identical. This command performs no source
acquisition, fact publication, graph mutation, or Dagster work.

## Immutable publication

```bash
legal-tender pipeline fec publish-committee-summary \
  --storage-root /storage --release <published-v4-manifest> \
  --cycle <source-cycle> --run-id <run-id>
```

This command consumes only a published v4 manifest with byte-identical immutable
backing. It verifies the selected raw artifact's canonical storage path, size,
digest, and complete CSV framing. It does not accept a research CSV as a release
member, download bytes, or rescan unrelated large schedule artifacts.

One zstd JSONL artifact physically combines each occurrence and its normalized
fact. This small source reuses the existing artifact writer rather than adding
a Parquet mapping. No row is dropped or grouped. Each fact retains raw strings,
typed observations, every issue, the snapshot hash, and the exact raw byte span.
Duplicate raw rows have distinct occurrence/fact IDs. Record references are
explicitly `unkeyed:<occurrence-id>` because no stable revision key is accepted.

The [fact contract](../../contracts/facts/fec/committee-summary/v1/) binds the
exact release-manifest digest, selected source artifact, parser, normalization,
cycle, verification report, and both compressed/uncompressed artifact identities.
Before publication and on replay, a fresh CSV scan is compared with every stored
record and verified EOF. Raw corruption, altered fields, missing/extra rows,
malformed framing, wrong partitions, failed storage preflight, and cancellation
do not publish a manifest. Typed issues remain in otherwise complete output.

Immutable manifests use atomic create-if-absent publication under
`facts/fec/committee-summary/v1/manifests/<fact-set-id>.json`. There is no summary
`current` pointer to roll back during historical replay. Concurrent writers reuse
the verified winner. An orphaned content-addressed artifact from an interrupted
run is not a published fact set. New builds respect the existing 500 GiB FEC free
floor plus a 1 GiB working allowance; replay performs no new artifact build.

Fact-set identity includes the exact release digest. A later release with the
same CSV can reuse the identical content-addressed row artifact, but receives a
new release-bound manifest. This preserves truthful membership without rewriting
old lineage or treating snapshot change as a new financial event.

V4 adds four `committee_summary_csv` whole artifacts and no staged ZIP/COPY
outputs. Acquisition uses the strict Go verifier after capture and publisher
version recheck. Physical failures retain captured bytes without releasing them;
typed issues remain eligible for lossless preservation. V1–v3 membership remains
unchanged. Dagster's local schema resolver and existing B/E dispatch understand
v4. Default discovery remains v3; migration is separate from the manual asset.
The real v4 release is already accepted and active; do not rerun source
acquisition to complete the orchestration handoff.

## Manual Dagster handoff

The [isolated four-cycle gate](../audit/committee-summary-dagster-2026-09-10.md)
passes against the existing published release with source/fact storage read-only.

The [summary asset](../../orchestration/committee_summary.py),
`fec_committee_summary_facts`, uses the shared dynamic `fec_cycle` partitions.
Its only input is the actual `fec_release_publication` output, passed unchanged
to Go as `--release`. It does not consult a current pointer, acquire sources,
stage schedules, group financial assertions, or update graphs. Go verifies the
supplied release bytes against their immutable backing and selects the requested
cycle's CSV. Python only invokes Go, validates the local result schema and cycle,
and maps allowlisted metadata.

`fec_committee_summary_fact_job` selects only this asset. There is no automation
condition, schedule, or sensor targeting it, regardless of
`DAGSTER_SCHEDULES_ENABLED`. This does not change existing assets' automation
defaults. Discovery migration and weekly activation need separate acceptance.

The output is the canonical immutable fact-manifest path, with `fact_set_id`
as the Dagster data version. A partition-scoped blocking `go_verified` check
passes only after command success and schema/cycle validation. Success follows
the output so Dagster attaches the check to this materialization. Failed commands
or invalid envelopes emit a failed check and no asset output. Retained source
issues remain visible in metadata; a preservation pass is not financial approval.

The retry policy allows two retries with a 60-second exponential delay. Go reuse
rechecks the small CSV and stored facts, without reacquisition, extraction, or a
new fact publication. Immutable input identity, not Dagster run ID, governs reuse.

The manual job expects its upstream output to exist in the configured IO manager.
For a release published outside Dagster, the isolated gate seeds that exact
immutable path into its private IO manager; it does not rerun release publication
or populate the resident instance. The shared filesystem IO manager stores the
latest output per asset partition, not a cross-release transaction. Earlier A/B/E
facts retain their own ancestry; future consumers must verify same-release inputs
in Go before combining them.
