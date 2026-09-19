# Schedule A raw and processed reconciliation

> **Status:** Deferred raw/processed research; not an initial production
> dependency. The machine-readable
> [reconciliation contract](../../contracts/calculations/fec/schedule-a-reconciliation/v1/contract.json)
> and one exact targeted fixture have landed. They prove identity, field
> comparison, revision lag, and change propagation for one report family. A
> Additional corpus gates and a new accepted source decision are required
> before this result can support a separately labeled user view.

## Question

This calculation answers two related but distinct questions:

1. Which processed Schedule A occurrence corresponds to an exact raw
   electronic-filing occurrence, and what did the FEC representation preserve,
   enrich, omit, or change?
2. How does that processed evidence relate to the Schedule A rows in the
   currently effective raw filing revision?

The first question compares one filing revision to the same revision. The
second accounts for amendment timing. Combining them into one comparison would
turn normal processing lag into false conflicts.

Reconciliation does not decide whether a receipt counts, whether a memo row
duplicates another row economically, which organization a donor belongs to,
or how money reaches a terminal source. Those remain later calculations over
preserved facts and relationships.

## Evidence boundaries

The calculation accepts two processed evidence classes:

| Evidence | Coverage | Permitted result |
|---|---|---|
| Pinned Schedule A dump partition | Complete accepted two-year processed relation snapshot. | `complete_snapshot` after every gate passes. |
| Exact Schedule A API observation | Only the captured query boundary at its observation time. | `provisional_targeted_observation`; diagnostic only. |

The API can establish that a specific processed row existed and had specific
values at a specific time. A zero-result query can establish only that the row
was not exposed inside that exact query boundary then. It cannot prove a
permanent absence, a complete period, or a physical deletion.

Raw evidence comes from exact `.fec` document bytes and lossless physical-row
occurrences. The current raw view comes from the separate report-family and
effective Schedule A calculations. Neither source lane overwrites the other.

## Direct-source identity

A direct raw-to-processed match requires one unique group with all three exact
signals:

```text
committee_id + file_number + transaction_id
```

Form and line, back reference, contribution date, and amount corroborate that
identity. They cannot create it. Names and fuzzy text never participate in
identity.

The grouping model must retain zero, one, or several occurrences on either
side. A non-unique group becomes `unresolved`; it is not forced into a
one-to-one match. Processed `sub_id` remains processed identity and is never
invented for a raw row.

## Field comparison

[`field-map.json`](../../contracts/calculations/fec/schedule-a-reconciliation/v1/field-map.json)
maps all 45 raw Schedule A fields to the processed API and dump-relation
representations:

- 43 fields have accepted comparators;
- `election_other_description` and `account_system_code` have no accepted
  processed equivalent and remain explicitly `unmapped`;
- organization-name comparison is non-applicable for raw `IND` and `CAN`
  entity types; and
- mutable nested committee and contributor master expansions from the API are
  preserved as response evidence but excluded from row identity and content
  comparison.

Each mapped field receives exactly one state:

| State | Meaning |
|---|---|
| `equal_exact` | The accepted source representations are identical. |
| `equal_absent` | Both representations assert absence under the field rule. |
| `equal_representation` | Declared formatting changes preserve meaning, such as `YYYYMMDD` versus `YYYY-MM-DD`, decimal lexical form, case, or surrounding ASCII spaces. |
| `processed_enriched` | The raw source is absent and the processed source adds a value. |
| `processed_omitted` | The raw source has a value and the processed source omits it. |
| `different` | Both assert materially different content under the comparator. |
| `not_applicable` | The raw entity or form makes the comparison inapplicable. |

Unmapped fields are conserved separately. The seven comparable state counts
must sum to 43 for every direct match, and `unmapped` must equal two. Source
values remain unchanged; comparison values are derived evidence.

Publisher enrichment is not automatically a conflict. For example, the exact
fixture shows processed election coding and contributor committee IDs that are
empty in the raw rows. Later calculations can choose whether an enrichment is
authoritative for their question, but they cannot relabel it as filer-supplied.

## Direct relationship states

| State | Rule |
|---|---|
| `corroborated` | Strong identity is unique and all applicable raw content agrees after declared representation rules; labeled processed enrichment is allowed. |
| `representation_only_difference` | Identity is unique and every difference is accepted representation only. |
| `processed_changed` | Processed evidence aligned to the same filing revision differs materially. |
| `conflict` | Strong identity signals or mapped same-revision content contradict. |
| `unresolved` | Evidence is absent, incomplete, non-unique, or otherwise cannot satisfy the match contract. |

`processed_changed` is deliberately same-revision. An older processed row that
differs from a newer raw amendment is not evidence that the FEC changed the
new row; it is evidence that the processed source still reflects an earlier
revision.

## Effective-revision alignment

After direct comparison, every current effective raw row gets one processed
support state:

| Support | Meaning |
|---|---|
| `same_filing` | Processed evidence is from the selected effective file. |
| `prior_filing_identical_content` | Processed evidence is older, but the effective row is carried forward with identical logical content. |
| `prior_filing_different_content` | Processed evidence is older and the effective row changed. |
| `none` | No processed occurrence supports this row. |

The report-family revision alignment is recorded independently as `aligned`,
`processed_lags_raw`, `raw_lags_processed`, or `indeterminate`.

When processed evidence lags raw:

- identical carried-forward content can remain `corroborated`, with the prior
  filing named as its support;
- modified content becomes `raw_only_pending_processing`; and
- added content with no processed occurrence also becomes
  `raw_only_pending_processing`.

Pending is a time-bounded evidence state, not a promise that the publisher will
eventually expose the row. A later complete processed snapshot can move it to
corroborated, processed-changed, conflict, or another explicit state without
rewriting the earlier result.

## Exact targeted proof

On 2026-08-27, an exact Schedule A API observation used the official listing
image boundaries for committee `C00392928` in transaction period 2026:

| Raw file | Image range | Processed rows returned |
|---:|---|---:|
| Original `1997074` | `202607159885305174`–`202607159885305254` | 23 |
| Amendment `2009982` | `202608279903415525`–`202608279903415608` | 0 |

All 23 returned rows identify file `1997074`. Each has a unique `sub_id` and
transaction ID, and each matches exactly one raw original occurrence through
committee, file, and transaction identity.

Across 23 rows and 45 raw fields, the deterministic comparison conserves 1,035
field states:

| Field state | Count |
|---|---:|
| Exact equality | 208 |
| Equal absence | 559 |
| Representation equality | 197 |
| Processed enrichment | 15 |
| Processed omission | 0 |
| Material difference | 0 |
| Not applicable | 10 |
| Unmapped | 46 |

The 15 enrichments are nine processed election codes and six processed
contributor committee IDs. No mapped raw value is omitted or materially
different in the original-filing comparison.

The complete raw family selects amendment `2009982`. Its effective Schedule A
projection contains 25 rows:

| Effective state | Count | Reconciliation result |
|---|---:|---|
| Carried forward with identical content | 18 | Corroborated by identical prior-filing content. |
| Modified | 5 | `raw_only_pending_processing`. |
| Added | 2 | `raw_only_pending_processing`. |

Therefore the family alignment is `processed_lags_raw`, not conflict. The
seven pending row keys form the targeted committee and period change set.

The exact response also proves two source-contract cautions:

- all processed original rows expose `amendment_indicator=A` / `ADD` even
  though the raw cover is `F3XN`, so processed action is not raw cover
  disposition; and
- the pinned Swagger describes `original_sub_id` as non-null while the exact
  response contains null values, so observed values outrank generated client
  assumptions.

The complete fixture is
[`original-processed-effective-amendment-pending.json`](../../contracts/calculations/fec/schedule-a-reconciliation/v1/fixtures/original-processed-effective-amendment-pending.json).

## Conservation and publication

A result must account for every direct raw occurrence, every processed
occurrence inside its declared coverage, and every effective raw row exactly
once. Unmatched processed rows go to `processed_only`. Ambiguous groups remain
in `unresolved_groups`. Nothing disappears because it is inconvenient for a
subtotal.

Only `complete_snapshot` can become eligible for downstream totals, and only
after all blocking checks pass. A targeted API observation always sets:

```text
diagnostic_only = true
eligible_for_user_totals = false
```

This boundary protects the project from treating a useful exact sample as a
complete processed population.

## Change propagation

Go owns occurrence grouping, field comparison, revision alignment,
conservation, and change-set derivation. The result names exact affected raw
row keys, committee IDs, and two-year transaction periods.

Dagster maps those domain keys to assets and partitions. A new API observation
that adds history but changes no relationship state emits no downstream
calculation work. A newly processed amendment can invalidate only its report
family, committee, period, receipt calculations, entity links, and reachable
graph projections rather than all four target periods.

## Remaining acceptance gates

1. ~~Extract exact rows from a pinned complete processed Schedule A dump
   partition and validate every relation field, SQL null, ordinal, and source
   digest.~~ The complete 2025/2026 partition, eight paired fixtures, and
   independent row/digest replay passed on 2026-08-28. All 23 targeted original
   `sub_id` and transaction-ID values match the earlier API observation; the
   amendment remains absent from the older 2026-08-23 dump.
2. Re-run this family after amendment `2009982` appears in a pinned processed
   evidence boundary and prove the pending-state transitions.
3. Add one-to-zero, zero-to-one, one-to-many, many-to-one, duplicate-ID,
   identity-conflict, processed-change, processed-only, and incomplete-input
   fixtures.
4. Run the calculation across a representative target-window corpus and
   quantify unresolved groups, publisher enrichments, omissions, material
   changes, and processing lag.
5. Implement deterministic Go replay and prove field and row conservation at
   selected-partition scale.

## Official references

- [OpenFEC API documentation](https://api.open.fec.gov/developers/)
- [OpenFEC Schedule A endpoint](https://api.open.fec.gov/v1/schedules/schedule_a/)
- [FEC processed schedules dump README](https://www.fec.gov/files/bulk-downloads/data-dump/schedules/README.txt)
- [FEC receipts data description](https://www.fec.gov/campaign-finance-data/about-campaign-finance-data/about-receipts-data/)
