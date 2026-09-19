# Python FEC pipeline and live ArangoDB state

> **Observed:** 2026-08-31  
> **Purpose:** Preserve the legacy source and runtime evidence needed to choose
> the next Go graph sources. This is not a target-design contract.

## Result

The Python pipeline proved that a useful candidate-funding product can be built
from the smaller cycle bulk products. It ingested `cn`, `cm`, `ccl`, `indiv`,
`oth`, `pas2`, `weball`, `webl`, and `webk`; it did not ingest the complete
processed Schedule B or Schedule E relations.

That prototype is valuable product and parity evidence. It is not evidence that
`oth` and `pas2` are complete or non-overlapping authorities for the narrower
receipt, disbursement, transfer, and independent-expenditure facts required by
the rewrite. The next source decision must compare those classic products with
the processed B/E relations for explicitly named fact families. It must not
assume that every processed FEC schedule belongs in production.

## Python source use

| Source | Legacy use |
|---|---|
| `cn`, `cm`, `ccl` | Candidate and committee vertices plus candidate-authorized-committee relationships. |
| `indiv` | Itemized individual receipts, max-out donor qualification, donor-to-committee edges, employer attribution, and itemized receipt totals. |
| `oth` | Parse-time-filtered committee and organization receipt evidence used for recipient-side committee-transfer edges. |
| `pas2` | Candidate-directed committee contributions and independent expenditures used for sender-side transfer and support/oppose edges. |
| `weball`, `webl`, `webk` | Candidate and committee summary assertions, individual-detail gaps, total context, and fallback totals. |

The graph then collapsed source rows into cycle-level edges:

- `transferred_to` selected `24K`, `24P`, and `24Z` from `pas2`, selected
  `11*`, `15*`, `18*`, and `22Z` receipt codes from `oth`, and retained one
  aggregate per source, destination, cycle, and classic dataset;
- `spent_on` summed `pas2` `24E` and `24A` rows by spender, candidate, stance,
  and cycle; and
- `contributed_to` retained one aggregate per qualified donor, recipient
  committee, and cycle.

Those projections discarded transaction dates, amendment/report identity,
memo and purpose detail, `SUB_ID` membership, and source-release identity. The
two transfer projections were not reconciled as two reports of a possible
single real-world transfer. The IE projection did not implement a separately
versioned effective-record rule.

## Live database state

The ArangoDB named volume still exists and the three-month-old
`legal-tender-dev-arango` container was healthy when inspected. The database
list was:

- `_system`;
- `fec_2020`, `fec_2022`, `fec_2024`, and `fec_2026`;
- `aggregation`; and
- `lt_probe_2024_e3530a3f82e329ee`.

The Go projection did not overwrite the Python database. It created the
content-addressed `lt_probe_*` database beside the existing databases. Its
observed counts remained 31,035 entities, 8,175 candidate results, 8,584
candidate-committee relationships, 2,116 receipt-component edges, and one
projection-metadata document.

The legacy `aggregation` database retained:

| State | Observed count |
|---|---:|
| Candidates | 19,004 |
| Candidates with stored `funding_channels` | 8,441 |
| Committees | 36,553 |
| Committees with `receipts_by_cycle` | 21,572 |
| Committees with `terminal_type` | 36,553 |
| Donors | 947,793 |
| `transferred_to` edges | 634,080 |
| `spent_on` edges | 21,764 |
| `contributed_to` edges | 0 |

The transfer edges comprised 142,598 `oth` projections and 491,482 `pas2`
projections. The spending edges comprised 15,523 support and 6,241 opposition
projections.

This is not a coherent accepted snapshot. Stored candidate calculations were
last written between 2026-05-26 and 2026-06-21; transfer and spending edges were
last written on 2026-06-21; donors were updated on 2026-06-28; and the
`contributed_to` collection is empty. The Python asset truncates that collection
before rebuilding it, so a later incomplete materialization can explain the
state without erasing already embedded candidate results.

The per-cycle parser dumps still exist for all nine legacy FEC products and
occupy about 13 GiB. The raw store occupies about 156 GiB. No separate
aggregation JSONL dumps were present under the documented dump directory.
Therefore the ArangoDB named volume is currently the only observed copy of the
legacy derived graph state and must not be removed as part of rewrite work.

## Source-decision consequence

Schedule A remains justified by the measured failure of the classic receipt
products to preserve the complete detailed receipt relation. Schedule B and E
are not automatically justified by that result.

Before expanding the coordinated release, define and measure these separate
questions:

1. Can `oth` and `pas2` preserve complete, correctly directed, reconcilable
   committee-to-committee contribution and transfer facts for the product?
2. Can `pas2` preserve complete candidate-directed contribution facts without
   importing unrelated Schedule B disbursements?
3. Can `pas2` and/or the 24/48-hour product support a correct effective
   independent-expenditure calculation, or does that require processed
   Schedule E?
4. Which required source fields and record identities disappear from each
   classic projection?
5. What are the acquisition, retained-storage, scan-time, and update costs of
   each acceptable source choice?

The answer can select a classic bulk file as canonical for a narrowly defined
fact family. It cannot combine overlapping products or promote a processed
schedule merely because that schedule is more complete in the abstract.
