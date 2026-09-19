# Effective electronic-filing calculations

> **Status:** Deferred as-filed research; not an initial production dependency.
> The machine-readable
> [report-family](../../contracts/calculations/fec/efile-report-family/v1/contract.json)
> and
> [effective Schedule A](../../contracts/calculations/fec/effective-schedule-a/v1/contract.json)
> contracts implement this design against one exact original/amendment family.
> The [reconciliation contract](./schedule-a-reconciliation.md) now relates
> that family to one exact targeted processed observation. They do not yet
> authorize a user-facing as-filed total. Corpus expansion and complete-dump
> reconciliation remain open gates if this product is later promoted.

## Boundary

These calculations answer two separate questions:

1. Which complete electronic filing is effective for one report family at a
   closed observation boundary?
2. Which Schedule A rows are present in that filing, and how did they change
   from the preceding filing?

They do not decide whether a receipt counts, whether two disclosures describe
one economic event, who a terminal source is, or how a raw row relates to a
processed FEC row. Keeping selection separate from money meaning lets later
calculations use the complete source grain without rebuilding amendment logic.

The result is provisional electronic-filing evidence. It does not include
paper filings or later FEC processing.

## Stage 1: report-family assembly

### Identity

```text
family_id = "fec:efile-report:" + root file_number
result key = family_id + closed input-manifest digest + calculation version
```

The root file number is source identity, not a generated database key. A later
amendment changes the observed family result but not the family ID.

### Evidence priority

Family assembly uses structural publisher evidence:

| Evidence | Meaning | Authority in the calculation |
|---|---|---|
| Listing `amends_file` | Immediate predecessor asserted by the filing listing. | Primary edge. |
| Predecessor listing `amended_by` | Immediate successor asserted by the earlier listing. | Primary reverse edge. |
| HDR `original_report_id` | Root report asserted by the submitted document. | Primary family-root evidence; never treated as the immediate predecessor. |
| Listing amendment number and HDR amendment number | Position assertions. | Required to agree and form a continuous sequence. |
| `most_recent` and `most_recent_filing` | Publisher's projected current filing. | Must corroborate the computed leaf. |
| `amendment_chain` | Publisher convenience projection. | Corroborating only; preserved exactly and checked for anomalies. |

The exact fixture proves why `amendment_chain` cannot be the family identity.
The original listing exposes `[1997074]`; its amendment exposes
`[1997074, 2009982]`. Both records still agree through `amends_file`,
`amended_by`, HDR root, amendment number, and current-filing assertions.

### Assembly and selection

For one closed received-through observation, Go:

1. Pins every listing page, convergence result, referenced file number,
   document digest, and parser version.
2. Builds immediate edges only from `amends_file` and `amended_by`.
3. Adds HDR root assertions without converting them to immediate edges.
4. Forms connected components from explicit assertions.
5. Finds the unique node without a predecessor and derives the family ID.
6. Validates every node and the linear sequence.
7. Finds the unique node without a successor.
8. Requires publisher current-filing assertions to agree with that leaf.
9. Selects the leaf only if every blocking check passes.

Committee ID, base form, and coverage interval are compatibility checks. They
do not group records by similarity. If explicit amendment links connect
incompatible reports, the family becomes a conflict; the implementation does
not silently split it.

Filing disposition remains separate from base form. For the demonstrated
family, `F3XN` is the new filing and `F3XA` is the amendment while both share
base form `F3X`. A termination disposition ends that report family as filed;
it does not delete the committee or its other reports.

### Publication states

| State | Effect |
|---|---|
| `selected` | One complete supported leaf can feed record-family projections. |
| `blocked_incomplete` | Discovery did not converge or referenced evidence is absent. |
| `blocked_conflict` | Links branch, cycle, disagree, or connect incompatible reports. |
| `blocked_invalid_document` | Exact-byte, header, cover, or conservation checks failed. |
| `blocked_unsupported_format` | Bytes remain preserved but no accepted parser can project them. |

A new source observation creates a new immutable calculation result. It never
rewrites which filing was effective in an earlier observation.

## Stage 2: effective Schedule A projection

### Complete replacement rule

An electronic amendment is a complete report submission. The selected
document supplies the complete current Schedule A set. The calculation never
starts with the predecessor and applies additions or modifications as a
patch.

This distinction matters for removals. A predecessor transaction ID absent
from the selected filing is `removed` from the current as-filed projection,
even though its original occurrence remains immutable source evidence.

### Row identity

The official format requires a Schedule A transaction ID that is unique for
the life of the original report and its amendments. The calculation key is:

```text
row_key = family_id + ":SA:" + exact transaction_id
```

Form type belongs to row content, not row identity. A form or line-reference
change under the same transaction ID is `modified`.

A missing or same-document duplicate transaction ID blocks the complete
projection. Legal Tender does not fall back to contributor, date, amount,
ordinal, or a row hash. Those values are not safe report-life identity.

### Physical and logical identity

Each source occurrence keeps its document digest, byte range, ordinal,
terminator, physical field count, exact physical-row digest, canonical fields,
and extra fields. A separate logical digest answers whether the 45 Schedule A
fields changed.

`fec.schedule_a.logical_row.v1` is calculated as follows:

1. Initialize SHA-256 with UTF-8 `fec.schedule_a.logical_row.v1` and one zero
   byte.
2. Visit the 45 fields in the accepted layout order.
3. For an omitted logical field (`null`), append one zero byte.
4. For a present field, append one byte, its UTF-8 byte length as an unsigned
   four-byte big-endian integer, then the exact accepted field text as UTF-8.
5. Empty text is present with length zero. It is distinct from an omitted
   field.
6. Do not include document identity, offset, ordinal, framing, terminator, or
   extra physical fields.

The semantic row-set digest starts with UTF-8
`fec.schedule_a.logical_row_set.v1` and one zero byte. For each row sorted
bytewise by UTF-8 row key, it appends the four-byte big-endian key length, key
bytes, and raw 32-byte logical digest.

This split avoids two opposite errors:

- Equal row bytes in different filings remain distinct source occurrences.
- A framing-only change can update evidence lineage without forcing monetary,
  entity, and graph recomputation.

### Row states

| State | Rule | Downstream semantic invalidation |
|---|---|---|
| `added` | Key occurs only in the selected filing. | Yes. |
| `modified` | Key occurs in both; logical digests differ. | Yes. |
| `removed` | Key occurs only in the predecessor. | Yes. |
| `representation_changed` | Logical digests agree; physical digests differ. | No. |
| `carried_forward_identical` | Logical and physical digests agree. | No. |

Every current row points to its occurrence in the selected filing, including
carried-forward rows. Equal content may reuse a content-addressed logical fact
version. It never assigns the current assertion to the predecessor document.

### Exact fixture result

The fixture compares original file `1997074` with amendment `2009982` for
committee `C00392928`:

| State | Count |
|---|---:|
| Prior Schedule A rows | 23 |
| Current Schedule A rows | 25 |
| Added | 2 |
| Modified | 5 |
| Removed | 0 |
| Carried forward, logically and physically identical | 18 |
| Representation-only changes | 0 |
| Unresolved | 0 |

Added transaction IDs are `DA4945` and `IA21354IDTA4945`. Five existing
direct-contributor rows change accumulator values. Three also change
occupation text, and all five change a source field from `00` to empty at
`donor_candidate_district`. The calculation records all differing fields; it
does not discard changes because a later money subtotal may ignore them.

The identical memo row `IA21226IDTA4957` appears in both documents with the
same physical digest. The effective result points to the amendment occurrence
and retains the original as prior support.

The deterministic fixture lives in the
[effective Schedule A contract](../../contracts/calculations/fec/effective-schedule-a/v1/fixtures/first-amendment-change-set.json).

## Incremental propagation

Go owns record comparison and emits two independent signals:

```text
lineage_changed  = selected document or occurrence evidence changed
semantic_changed = semantic row-set digest changed
```

Every new amendment records lineage. When `semantic_changed` is false,
Dagster can record the new family and evidence versions without requesting
money, entity, reconciliation-content, or graph rebuilds.

When it is true, the change set derives committee IDs, contribution dates,
target two-year periods, and transaction IDs only from `added`, `modified`,
and `removed` rows. Dagster maps those domain keys to the affected cycle runs.
It does not use one new daily filing to rerun all four target periods.

Different downstream calculations can narrow the change set further. For
example, an occupation-only change need not rebuild a subtotal that depends
only on amount and memo state, but it must invalidate employer/occupation
resolution. That dependency belongs to the downstream calculation contract,
not this source projection.

## Invalid-chain behavior

Source evidence always remains inspectable. The current as-filed projection
does not publish when:

- the listing window is partial or changing;
- any referenced predecessor, listing, or document is missing;
- the graph has a cycle, branch, multiple roots, or multiple leaves;
- amendment position or current-filing assertions conflict;
- committee, base form, or coverage differs across explicit links;
- a required document is invalid or unsupported;
- a selected Schedule A row is invalid; or
- a selected Schedule A transaction ID is missing or duplicated.

The last good published version remains readable. A later complete input
manifest can produce a new result without deleting the failed attempt.

## Relationship to processed Schedule A

The processed dump and raw electronic filings are separate source lanes:

- The raw projection answers what the filer submitted electronically in the
  selected complete filing.
- The processed projection answers what the FEC's processed relation contains
  in a pinned snapshot, including later coding and paper coverage.

Neither overwrites the other. The draft
[reconciliation calculation](./schedule-a-reconciliation.md) now relates them
with explicit identity, field-comparison, revision-alignment, conservation,
and publication states.

Its first exact targeted observation matches all 23 original raw rows to 23
processed rows without omission or material difference. The processed source
still exposes original file `1997074`, while the raw family selects amendment
`2009982`. The result therefore corroborates 18 unchanged current rows through
identical prior content and marks five modified plus two added rows
`raw_only_pending_processing`. It does not mislabel the revision lag as a
processed change or conflict, and it cannot feed totals because the API query
is not a complete processed partition.

## Remaining acceptance gates

Before these contracts move from `draft` to `accepted`:

1. Expand fixtures across target-window form types, HDR versions, deeper
   amendment chains, terminations, CRLF framing, legacy decoding, and omitted
   trailing fields.
2. Add deliberately incomplete, branching, cyclic, incompatible, missing-key,
   and duplicate-key fixtures for every blocked state.
3. Run the family rules across a representative full received-date corpus and
   quantify every publisher inconsistency.
4. Prove deterministic Go replay and performance at the expected daily filing
   volume.
5. Prove the landed processed/raw reconciliation contract against an exact
   complete processed dump extract and add blocked-state fixtures.

## Official references

- [FEC electronic-filing format and vendor resources](https://efilingapps.fec.gov/registration/softwarelogs.htm)
- [FEC electronic-filings API](https://api.open.fec.gov/v1/efile/filings/)
- [FEC processed receipts methodology](https://www.fec.gov/campaign-finance-data/about-campaign-finance-data/about-receipts-data/)
