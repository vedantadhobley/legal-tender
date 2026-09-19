# Legacy data grain and lineage

> **Excavation date:** 2026-08-27  
> **Question:** Where does the Python system preserve source grain, aggregate
> early, overwrite state, or discard dimensions?  
> **Target-design authority:** None. This is an as-built map.

## Summary finding

The legacy system preserves most FEC transaction fields in the latest raw ZIP
and, except for `oth`, in the per-cycle parsed collections. It then creates a
separate graph optimized around thresholded people and cycle-level amount
summaries. That graph does not retain source-record references. The final
candidate projection compresses it further into top lists, five funding
channels, and residual totals.

The system can answer aggregate questions. It cannot return an auditable graph
path whose vertices and edges resolve to the exact FEC rows and source release
that justified the path.

## Grain levels

This document uses five grain levels:

1. **Source release** — the bytes supplied by an external publisher.
2. **Source record** — one row in a particular source release.
3. **Normalized fact** — a typed fact that preserves the source record and its
   important dimensions.
4. **Relationship projection** — a graph edge derived from one or more facts.
5. **Presentation projection** — totals, channels, rankings, or UI-shaped data.

The legacy implementation has levels 1, 2, 4, and 5 in partial form. It does
not have a durable normalized-fact layer with source-release lineage.

## End-to-end grain map

| Boundary | Grain entering | Grain leaving | Preserved | Collapsed or lost |
|---|---|---|---|---|
| Download | Remote source release | One active ZIP path | Current bytes, URL, size, download time | Prior releases, checksum, remote version |
| Parse: master files | Master row | One document per source ID | Official fields | Duplicate history, row location, source release |
| Parse: `indiv` | Contribution row | One document per `SUB_ID` | All mapped FEC fields | Invalid original numeric text, duplicate history, row location |
| Parse: `pas2` | Disbursement row | One document per `SUB_ID` | All mapped FEC fields | Invalid original numeric text, duplicate history, row location |
| Parse: `oth` | Other-receipt row | Selected document per `SUB_ID` | All mapped fields for four entity types | Every row with any other entity type |
| Donor projection | Individual rows | One person-like vertex per normalized name + employer | Cross-cycle amount/count, listed employer, cycles | Address, dates, transaction types, recipients, source IDs, per-cycle totals |
| Contribution projection | Individual rows | One donor→committee edge per cycle | Amount, count, endpoints, cycle | Dates, election designation, amendment state, memo, source IDs |
| Transfer projection | `pas2`/`oth` rows | One committee→committee edge per source and cycle | Amount, count, endpoints, cycle, source dataset | Dates, transaction codes, memos, filing IDs, source IDs |
| IE projection | Selected `pas2` rows | One committee→candidate edge per stance and cycle | Amount, count, endpoints, stance, cycle | Dates, descriptions, payees, transaction IDs, source IDs |
| Linkage projection | `ccl` rows | One committee→candidate edge per cycle | Linkage/type/designation, endpoints, cycle | Parsed document ID and source-release lineage |
| Employer projection | Donor vertices | Employer vertices and donor→employer edges | Normalized employer label and aggregates | Which contribution rows carried which employer value |
| Committee receipts | Parsed summaries and graph edges | Per-committee, per-cycle receipt block | Selected totals, counts, source-choice flag | Constituent facts and calculation evidence |
| Terminal classification | Committee fields, receipt shape, external resolution | One mutable `terminal_type` | Current class and some refinement flags | Rule version, full evidence set, prior classes |
| Candidate funding | Summary graph plus classifications | Per-cycle and cross-cycle funding channels | Totals, top sources, some coverage flags | Actual traversed paths, per-edge attribution, source facts, full rankings |

## Parsed transaction grain

### `indiv`

`CODE` — `fec_<cycle>.indiv` retains all mapped rows in the selected ZIP member.
It does not apply the graph's high-dollar threshold during parsing. Each
document keeps the 21 official fields, converts `TRANSACTION_AMT` to a number
or null, and uses `SUB_ID` as `_key` when present.

Preserved dimensions include filer committee, amendment indicator, report
type, transaction type, entity type, contributor name, city, state, ZIP,
employer, occupation, date, amount, other ID, transaction ID, file number,
memo code, memo text, and sub-ID.

Lost or mutable dimensions:

- Source archive version and hash.
- ZIP member and row number or byte offset.
- Original bytes and invalid numeric string.
- Multiple records with the same `SUB_ID`; the later import replaces the
  earlier document.
- Prior materializations; the collection is truncated before reload.

### `pas2`

`CODE` — `fec_<cycle>.pas2` follows the same latest-row contract for all mapped
22-field disbursement records. It does not restrict transaction or entity type
at parse time.

The later graph selects only small semantic subsets. The full parsed collection
therefore preserves more query flexibility than the graph.

### `oth`

`CODE` — filtering occurs before persistence. Only rows whose `ENTITY_TP` is
`PAC`, `COM`, `PTY`, or `ORG` enter `fec_<cycle>.oth`. Rows for individuals,
candidates, and every other entity code exist only in the replaceable raw ZIP.

This is the first destructive semantic filter in the pipeline. A threshold or
classification change cannot recover excluded rows from ArangoDB or its dump;
the parser must reread the still-current raw ZIP.

## Master and summary grain

`cn`, `cm`, and `ccl` preserve publisher-defined master/linkage rows for the
current source release. `weball`, `webl`, and `webk` preserve publisher-defined
summary rows, not constituent transactions.

`CODE` — candidate and committee documents from different cycles are later
merged into shared `aggregation.candidates` and `aggregation.committees`
documents. The `cycles` array records cycle membership, but later cycles
overwrite common master fields. The shared vertex does not preserve a complete
per-cycle copy of each master record.

That behavior is useful for a current label. It prevents the shared vertex
from answering how a candidate or committee master field differed by cycle.

## Donor vertex projection

### Qualification

`CODE` — a source row is considered only when `ENTITY_TP` is `IND` or `CAN`,
has a name, and has a numeric amount. Donor identity is the SHA-256-derived key
of normalized `NAME|EMPLOYER`.

For each cycle, the asset:

1. Reattributes some `EARMARKED FOR` records to a target committee parsed from
   memo text or resolved by exact committee-name lookup.
2. Groups rows by donor identity and effective committee.
3. Separates direct and earmark totals.
4. Uses `max(direct_total, earmark_total)` for each donor/committee pair.
5. Qualifies donors whose largest committee total meets that cycle's
   per-election limit.
6. Sums all retained committee totals into one donor total for the cycle.
7. Merges matching donor keys across cycles into one vertex.

### Preserved output

The donor vertex retains:

- One source spelling of name and employer from a grouped cycle result. When
  the key exists across cycles, parallel upsert order can determine which
  spelling survives.
- Amount and approximate transaction count summed across processed cycles.
- A unique list of cycles.
- Materialization time.

### Collapsed dimensions

The vertex loses:

- Per-cycle totals.
- Recipient committees and candidate targets.
- Every transaction date and source record ID.
- Amendment and memo details.
- Election designation.
- Addresses and occupation.
- Direct-versus-earmarked amounts.
- The qualification committee and threshold used.
- Confidence that two identical normalized name/employer pairs are the same
  person.

`CODE` — donor name plus employer is an identity heuristic, not an enforced
person identity. The same person can split when their employer changes; two
people can merge when their normalized names and employers collide.

`BUG?` — `candidate_funding` later loads donor details only when the donor's
cross-cycle total is at least $10,000, although donor vertices qualify at a
lower per-cycle threshold. Contribution amounts from excluded donor-detail
rows remain attributed, but their name and employer degrade to the opaque donor
hash during tracing.

## Relationship projections

### `contributed_to`

`CODE` — one edge is emitted for each qualified donor, effective recipient
committee, and cycle. The edge contains total amount and approximate row count.
The same direct-versus-earmark maximum used for donor qualification is used for
edge amount.

The edge does not contain the contributing `SUB_ID` values. It cannot be
expanded back to the rows that formed its amount.

### `transferred_to`

`CODE` — two parallel projections create separate edges for `pas2` and `oth`:

- `pas2`: transaction types `24K`, `24P`, and `24Z`; entity types `COM`, `PAC`,
  `PTY`, `CCM`, and `ORG`; non-memo rows; direction filer committee to
  `OTHER_ID`.
- `oth`: selected `11*`, `15*`, `18*`, and `22Z` codes; non-memo rows;
  direction `OTHER_ID` to filer committee.

Rows are grouped by source committee and destination committee within cycle
and source dataset. Negative or zero net groups and self-transfers are removed.
Endpoints absent from the current committee vertex set are also removed.

The edge keeps `total_amount`, `transaction_count`, `cycle`, and source dataset.
It discards the transaction-code mixture after selection, dates, report and
amendment fields, memo text, filing and transaction IDs, and all `SUB_ID`
lineage.

Because `pas2` and `oth` have distinct edge keys, the same disclosed transfer
can survive as two graph edges when represented in both datasets. The candidate
trace loads both amounts without a source-record reconciliation key.

### `spent_on`

`CODE` — only `pas2` transaction type `24E` becomes support and `24A` becomes
opposition. Rows group by filer committee, candidate, stance, and cycle. The
edge keeps amount and count.

The edge drops expenditure dates, payee, purpose or description, transaction
and filing identifiers, amendment state, and source-row lineage. It represents
spending, not a receipt controlled by the candidate.

### `affiliated_with`

`CODE` — a `ccl` row becomes a cycle-specific committee→candidate edge with
linkage ID, committee designation, and committee type. The derived edge omits a
reference to the parsed `ccl` document and source release.

### `employed_by`

`CODE` — one edge connects a donor vertex to the employer label embedded in
that already-aggregated donor. It has no cycle, date range, amount, or source
record. It can therefore mean only “this donor identity was grouped under this
employer,” not “this person worked here when this specific donation occurred.”

## Committee receipt projection

`CODE` — `committee_receipts` computes one mutable `receipts_by_cycle` block on
each committee plus cross-cycle sums. It combines:

- Itemized individual totals and counts from `indiv`.
- Whale totals from `contributed_to`.
- Committee receipt totals from `transferred_to`.
- FEC authoritative aggregate individual totals from `webk` or `weball`.
- Candidate contributions and loans from `weball`.

The block preserves cycle, selected amounts, counts, earmark share, and a flag
describing which aggregate source supplied total individual receipts. It does
not preserve the lists of rows or edges used to produce each field.

`CODE` — when `weball` is mapped to a principal committee, the implementation
chooses the linked principal with the most itemized rows, falling back to
`cn.CAND_PCC`. That choice is a heuristic and is stored only through the final
target committee values, not as a durable mapping decision with evidence.

`CODE` — `small_donor_total` is a residual: authoritative external individual
total minus graph whale total, clamped to zero. It is not a set of identified
small-donor facts.

## Terminal-source projection

`CODE` — `committee_classification` writes one current `terminal_type` directly
onto the shared committee vertex. The base rule uses `CMTE_TP` and `ORG_TP`.
Later phases can replace it using:

- Reconci/Wikidata type resolution for membership organizations.
- High `earmarked_share` plus receipt threshold for conduit behavior.
- Name-cluster inheritance.
- Connected-organization inheritance.

Some refinement flags and a Wikidata QID are retained. The complete ordered
rule trace, inputs, rule version, rejected alternatives, prior value, and
effective time are not. Rerunning classification mutates the current value.

`terminal_type` is therefore a derived stopping-rule projection. It is not a
source fact and does not identify a terminal source without the algorithm and
rule version that interpreted it.

## Candidate funding projection

`CODE` — candidate attribution is calculated separately per cycle and then
summed into a cross-cycle projection. It starts only from affiliated committees
whose committee type is `H`, `S`, or `P`.

The trace loads summary edges into memory, traverses upstream for at most eight
levels, stops at committee terminal types, and applies proportional
multipliers. Cycle breaking and caps bound the calculation.

The stored `candidates.funding_channels` object preserves:

- Channel totals and percentages by cycle and across cycles.
- Selected top organizations, PACs, companies, and people.
- Independent-spending support and opposition totals.
- Direct, grassroots, whale, employee-connected, self-funded, and residual
  breakdowns.
- A donor-detail coverage ratio and summary-source label.
- Materialization time.

It discards:

- The actual graph paths traversed.
- The multiplier and attributed amount at each hop.
- Which transfers were suppressed by cycle prevention or depth limits.
- Which source rows formed each graph edge.
- Organizations below the configured `$1,000` display threshold.
- Entries below top-N truncation, although their amounts remain in totals.
- Rule and algorithm versions.
- An explicit link to the source snapshot used.

The final projection can explain the model in prose, but it cannot supply an
evidence bundle for an individual result.

## Loss and overwrite register

| ID | Label | Boundary | Legacy behavior |
|---|---|---|---|
| `GRAIN-001` | `CODE` | Download | Replaces the prior source archive at the canonical path. |
| `GRAIN-002` | `CODE` | Parse | Truncates every per-cycle collection before reload. |
| `GRAIN-003` | `CODE` | Parse | Drops source-release, member, row, byte, and checksum lineage. |
| `GRAIN-004` | `CODE` | Parse | Silently ignores invalid UTF-8 bytes and extra fields. |
| `GRAIN-005` | `CODE` | `oth` parse | Deletes all entity types outside four allowed codes. |
| `GRAIN-006` | `CODE` | Master merge | Later cycles overwrite shared candidate and committee fields. |
| `GRAIN-007` | `CODE` | Donor identity | Merges normalized name + employer and drops row identity. |
| `GRAIN-008` | `CODE` | Graph edges | Replaces transaction facts with cycle-level amount/count edges. |
| `GRAIN-009` | `CODE` | Graph edges | Stores no membership list of source records. |
| `GRAIN-010` | `CODE` | Employer graph | Converts per-transaction employer text into a timeless donor→employer link. |
| `GRAIN-011` | `CODE` | Receipts | Stores residual and summary fields without calculation evidence. |
| `GRAIN-012` | `CODE` | Classification | Mutates one current terminal class without a full rule trace. |
| `GRAIN-013` | `CODE` | Candidate funding | Stores totals and truncated top lists, not traversed paths. |
| `GRAIN-014` | `BUG?` | Candidate funding | Lower-total qualified donor details degrade to opaque hash keys. |
| `GRAIN-015` | `UNCLEAR` | Transfer graph | Cross-source duplicate disclosures can be counted as separate edges. |
| `GRAIN-016` | `DRIFT` | Snapshot | Raw and dump dates differ by cycle; no global snapshot ID binds them. |

## What can be recomputed today

If the latest raw ZIP still contains a discarded fact, the legacy code can
reparse it after a code change. That is weaker than preservation:

- A prior source release cannot be reconstructed after overwrite.
- `oth` exclusions can be recovered only from the current archive.
- Graph-edge membership cannot be recovered without rerunning the exact old
  logic, whose version is not stored with the edge.
- Candidate results cannot be tied to a precise mixed set of source and cache
  versions.

The parsed dumps are performance caches. They do not close these lineage gaps.

## Tests and observable invariants

The test suite concentrates on name matching and Wikidata resolution. It does
not contain fixture-based parser, graph-edge membership, transaction-lineage,
or candidate-path tests.

The legacy validation process checks aggregate agreement and named candidates.
Those checks can detect amount drift. They do not prove that the same source
facts or graph paths produced the totals.

## Questions passed to disposition and target design

This excavation does not answer these questions:

- Which legacy filters are desired product semantics and which are accidental
  storage loss?
- Should transaction facts themselves be graph edges, or should graph edges
  reference fact sets?
- What constitutes one disclosure across amendment and cross-file duplicates?
- Which fields are part of stable entity identity versus assertions that vary
  by filing and time?
- How should an attribution result expose every path and source record without
  forcing presentation queries over raw transaction volume?

Those decisions belong in the disposition register, evidence model, graph
ontology, and calculation contracts.

## Primary evidence

- [FEC schema parsing](../../../src/utils/fec_schema.py)
- [Individual-contribution ingestion](../../../src/assets/fec/indiv.py)
- [Other-receipt filtering](../../../src/assets/fec/oth.py)
- [Donor projection](../../../src/assets/graph/donors.py)
- [Contribution edges](../../../src/assets/graph/contributed_to.py)
- [Transfer edges](../../../src/assets/graph/transferred_to.py)
- [Independent-spending edges](../../../src/assets/graph/spent_on.py)
- [Committee receipt projection](../../../src/assets/enrichment/committee_receipts.py)
- [Terminal classification](../../../src/assets/enrichment/committee_classification.py)
- [Candidate funding projection](../../../src/assets/aggregation/candidate_upstream.py)
- [Legacy funding-channel description](../../funding-channels.md)
