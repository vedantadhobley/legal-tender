# Receipt source-role review — 2026-09-08

Status: complete overlap source review; additive routing policy implemented.
The [policy contract](../design/receipt-source-evidence.md) defines its limits.
No source correction, terminal attribution, or production graph change occurred.

## Exact source population

The review consumes inventory
`e31d7e8248ad6594c6ce23f86a0fe0cd2ff0ac13a0e78d079c97562519416985`
and its exact 2024 Schedule A fact set
`8f69a2ef40316f62d8844ecf732342a3b4535136b434f60677d0608cb04465df`.
The preceding [inventory gate](./committee-funding-basis-2026-09-08.md) owns the
complete source identities and baseline counts.

All 738 overlap occurrences were read with their complete 99 physical fields,
not sampled. Every recipient/decision/role bucket's counts and signed,
positive, and negative cent subtotals match the inventory. The first complete
source review took 126.643 seconds. It used four CPUs, a 4 GiB container cap,
`GOMEMLIMIT=2GiB`, `GOMAXPROCS=4`, read-only source storage, and no network.
No full-cycle receipt inventory was recalculated and no source shard was rewritten.

The final annotated complete review took 127.144 seconds; bounded earmark
inspection took 8.456 seconds including inventory/backing verification. Full
Go checks and targeted race tests passed.

The population contains 319 inventory buckets across 316 recipient IDs: 720
affiliated transfers and 18 registered-filer contributions. Counts remain 619
positive, 66 negative, and 53 zero observations. Reported amounts and source
roles are preserved, not normalized into spendable cash.

## What explains the overlap

The FEC's [published methodology](https://www.fec.gov/campaign-finance-data/about-campaign-finance-data/methodology/)
adds an amount/report-line condition to its transaction-code list. Applying
only that published condition as a diagnostic gives:

| Source evidence | Rows |
|---|---:|
| At most $200, null memo, documented form/line combination | 697 |
| At most $200, null memo, Form 3X line 17 | 41 |
| **Complete overlap** | **738** |

The comparison uses signed cents, not absolute amounts: a negative adjustment
can satisfy an upper bound. This is consistent with the published condition;
it does not prove the exact deployed publisher function or explain its extra
line-17 population. No local replacement for `is_individual` was introduced.

Every occurrence has matching raw/clean committee IDs and an accepted committee
receipt role. The new decision retains it once as a reported committee
observation, not a second individual donor. Seven records retain a separate
`IND`/`CAN` entity-label conflict. Routing a reported ID does not certify that
the identity is correct or registered; all terminal eligibility remains false.

## Original-file check of earmark linkage

Three existing processed examples point to [official filing 1708331](https://docquery.fec.gov/dcdev/posted/1708331.fec).
The bounded research fetch retrieved 4,826,663 bytes; SHA-256:
`c9814592585ab534f038649424d59222f254970d3af57dafcea95e149edb1e1b`.
The document header declares format 8.4. No API token or runtime source change
was needed. This file is audit evidence, not a new authoritative receipt ledger.

The review used the existing pinned research Schedule A field layout by field
name and checked physical width. The three exact same-filer transactions
`40570326`, `40570327`, and `40570328` occur at physical lines 16,875–16,877.
Their reported amounts and memo descriptions match processed ordinals 535–537.
Each has an empty conduit-name field and empty back-reference fields in the
original filing. No Schedule A child in that same filing points back to those
transaction IDs. The processed nulls therefore did not originate in this
project's Parquet conversion for these witnesses.

The memo identifies earmarked activity through a named conduit; it does not
supply an accepted exact-ID relationship. Inspection now explicitly reports
`reported_earmarked_receipt`, `earmark_conduit_unresolved`, and
`no_report_reference`. The original contributor and any future verified conduit
remain separate. No name-to-ID constant or memo-text parser was introduced.

This is a three-record, one-filing check. It does not establish universal
absence of usable references. The [FEC guidance](https://www.fec.gov/help-candidates-and-committees/filing-pac-reports/earmarked-contributions/)
provides reporting context, not authority to fabricate absent IDs or collapse
memo records into another amount.

## Reproducible gates and artifacts

The complete review command produces sorted full source records and one
versioned decision per record. Inspection preserves the existing raw page
inside a new decision envelope. The independent audit compares the full source
records before and after annotation and verifies exact conservation and all
738 decisions, including the seven entity conflicts. It also compares the
earmark witnesses against the whole original-file digest and exact fields.

Unit tests cover signed/zero/large amounts without threshold-based routing,
one-sided and conflicting committee IDs, memo exclusions, all reviewed earmark
codes, invalid/missing conduit IDs, adversarial name/ID-like memo text,
incomplete report references, deterministic complete review, cancellation,
resource limits, unchanged raw pages, and malformed source values.

Retained audit path:
`/storage/dumps/audits/fec/receipt-source-evidence/2026-09-08/2024/`.
Artifacts: `overlaps.json` (initial verified source review),
`overlaps-reviewed.json` (final source review with decisions),
`earmarks-inspected.json`, `1708331.fec`, and command exit markers.
The opt-in [independent corpus test](../../tests/test_receipt_evidence_corpus.py)
uses `LT_RECEIPT_EVIDENCE_AUDIT` to locate them.

The existing source facts, receipt and flow calculations, inventory, Arango
graphs, Dagster definitions, and canonical current pointers are unchanged.
Positive conduit linkage remains unimplemented where no accepted exact evidence
supports it. Complete funding coverage and allocation remain separate work.
