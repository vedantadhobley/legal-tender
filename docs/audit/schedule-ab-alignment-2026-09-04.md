# Processed Schedule A/B alignment audit

> **Observed:** 2026-09-04 UTC  
> **Scope:** Accepted 2024 receiver-reported committee-flow cohort from
> processed Schedule A and the complete 2024 inherited relation from processed
> Schedule B, using immutable FEC objects published on 2026-08-30.  
> **Disposition:** The same-publisher-batch alignment gate passes. Schedule B
> may proceed into coordinated acquisition and lossless fact publication. It
> is not yet an effective-outflow calculation or graph money authority.

## Result

Processed Schedule B is usable as an independent sender-reported assertion
source. Every source-integrity and conservation gate passed:

- all 264,085,606 Schedule A facts were rehashed and scanned;
- all 157,544,163 Schedule B rows parsed, with zero invalid rows and zero
  duplicate `SUB_ID`s;
- the accepted 320,731-row Schedule A flow cohort was conserved exactly;
- every valid Schedule B row received one diagnostic disposition; and
- both amounts remained separate in every match state.

Of the Schedule A flow cohort, 183,929 rows representing
$3,832,864,002.86 have a unique exact or same-amount/different-date Schedule B
candidate. That is 57.35% of rows and 82.02% of signed Schedule A flow amount.
Only 323 Schedule A rows, 0.10% of the cohort, have a unique same-date but
different-amount candidate. Their signed aggregate differs by $179,212.55.

This is sufficient to close the source-alignment blocker. It is not sufficient
to combine the two ledgers. Date disagreement is common, ambiguous Schedule B
candidates remain material, and Schedule A retains one-sided receiver
assertions. Schedule B still needs lossless facts, an effective-record policy,
an outgoing-flow-role calculation, and fact-level reconciliation before it can
alter the graph.

## Exact inputs

| Input | Immutable identity | Last modified |
|---|---|---|
| Schedule A dump | version `oqsbbVO7XdA4efCql3X.dtmSlzs_kKuc`; SHA-256 `35974c29037cf502752c0d961361aa15e77e674e2fdeee53006a8509f62804fb` | 2026-08-30 18:54:29 UTC |
| Schedule A fact set | `8f69a2ef40316f62d8844ecf732342a3b4535136b434f60677d0608cb04465df`; manifest SHA-256 `b664e752a3bae186508a45bb6d09289ba2fe06f0b4149ecbd72063e7ef79b829` | Derived from the pinned dump |
| Schedule A release | `fec-a6e6f777c3c506b100a7709815bc89f2ae84749faf4343a4589f7ec1bac066a2`; manifest SHA-256 `b921fda742759747b7e581c8897e8022ea5152eb33b86538ec80dbb9f13b5cad` | Immutable release lineage |
| Schedule B dump | version `JARG_mL4APJoVPy8FUvwiTILidolHqfY`; SHA-256 `39669f3c6c19d6f5076648f734f6456e0f8852d6cf37f0133e283fbfce91ac72`; 39,310,353,867 bytes | 2026-08-30 15:21:54 UTC |
| Schedule B relation | `disclosure.fec_fitem_sched_b_2023_2024` | Selected without materializing COPY output |

The objects have distinct immutable version IDs and the same UTC publisher
date. A live HEAD observation on 2026-09-04 UTC confirmed that both official
endpoints still identified these versions. The audit binds the exact retained
bytes, not mutable URLs.

## Match method

The audit indexed only rows accepted by
`legal-tender.fec.receiver-reported-committee-flow-policy.v1`, then streamed
Schedule B once. The directed signature is:

```text
sending committee + receiving committee + calendar date + signed exact cents
```

Candidate precedence is:

1. exact signature;
2. unique endpoint-and-amount candidate with a date disagreement;
3. unique endpoint-and-date candidate with an amount disagreement; and
4. unmatched known endpoint.

Any multiplicity on either side makes the A signature ambiguous. This is a
diagnostic candidate method, not the future reconciliation policy. It does not
use names, fuzzy matching, classic products, or inferred identities.

## Schedule A candidate states

| State | Signature groups | A rows | B rows | A signed amount | B signed amount | Share of A rows | Share of A amount |
|---|---:|---:|---:|---:|---:|---:|---:|
| Exact one-to-one | 44,975 | 44,975 | 44,975 | $3,224,796,661.60 | $3,224,796,661.60 | 14.02% | 69.01% |
| Unique amount; date disagrees | 138,954 | 138,954 | 138,954 | $608,067,341.26 | $608,067,341.26 | 43.32% | 13.01% |
| Unique date; amount conflicts | 323 | 323 | 323 | $18,024,181.43 | $17,844,968.88 | 0.10% | 0.39% |
| Ambiguous | 7,154 | 7,921 | 189,115 | $226,832,590.53 | $331,469,702.45 | 2.47% | 4.85% |
| Schedule A only | 122,935 | 128,558 | 0 | $595,099,404.67 | $0.00 | 40.08% | 12.74% |
| **Total** | **314,341** | **320,731** | **373,367 candidate rows** | **$4,672,820,179.49** | **$4,182,178,674.19** | **100%** | **100%** |

The B amount total in this table is a sum of diagnostic candidates, not an
effective-outflow total. The ambiguous state can contain many B rows for one A
signature and must not be used as money authority.

## Complete Schedule B disposition

| Disposition | Rows |
|---|---:|
| Exact candidate | 49,010 |
| Unique amount/date-disagreement candidate | 288,169 |
| Unique date/amount-conflict candidate | 36,188 |
| Unmatched row with an A cohort endpoint pair | 9,406,314 |
| Outside all A cohort endpoint pairs | 142,576,784 |
| Ineligible physical comparison shape | 5,187,698 |
| **Total** | **157,544,163** |

An eligible comparison row requires exact valid sender and recipient committee
IDs, agreement between Schedule B's raw and cleaned recipient IDs, a date, and
an exact amount. Rows outside the Schedule A endpoint set are mostly ordinary
Schedule B expenditures to other payees or committee pairs. This audit cannot
label them outgoing committee flows because no Schedule B flow-role policy has
been accepted.

## Quality interpretation

The source layer is high quality: exact object identity, catalog shape, strict
decoding, source lexemes, complete row conservation, and `SUB_ID` uniqueness
all pass. The candidate-reconciliation layer is intentionally unresolved:

- exact matches carry 69.01% of receiver-reported flow amount;
- date-only disagreement adds 13.01%, so date equality cannot be a universal
  identity rule;
- one-sided A rows carry 12.74%, so B cannot replace A;
- ambiguity carries 4.85%, so physical B rows cannot be summed before filing
  and action lineage is classified; and
- unique amount conflicts are rare but must remain visible as disagreement,
  not silently repaired.

The result supports Schedule B source adoption. It rejects direct graph import
or naive A/B deduplication.

## Runtime and artifact

The complete audit finished in 621.579 seconds. Schedule B streaming,
validation, hashing, and uniqueness took 482.990 seconds. Schedule A was read
from 265 verified Parquet shards with 16 workers. The command retained only the
320,731-row A candidate index plus bounded Schedule B uniqueness shards; it did
not materialize the 114.82 GiB Schedule B COPY stream.

Canonical successful result:

```text
/storage/dumps/audits/fec/schedule-ab/2026-08-30/2024/run-2/result.json
SHA-256 e72594cb43d5c6b3c71dfa950ed546f91db842c1f98d9bdaf4c7e9e06a44df6b
11,528 bytes
```

The first run was stopped before publication after a review found that its
candidate-state summary did not conserve cross-class multiplicity. `run-2`
uses the corrected implementation, passes every conservation check, and
validates against the checked-in
[result schema](../../contracts/audits/fec/schedule-ab-alignment/v1/result.schema.json).

## Consequences

1. Close the same-release A/B source-alignment gate.
2. Keep Schedule A and B as separate immutable assertions. Never add their
   amounts merely because a candidate match exists.
3. Add Schedule B to a new coordinated release-inventory version. Do not edit
   the existing v2 inventory.
4. Publish lossless selected-cycle Schedule B facts in a bounded columnar
   layout before defining policy.
5. Define a separate effective-record and outgoing-flow-role calculation that
   classifies every Schedule B fact, including action, memo, line, form,
   purpose, recipient, and filing lineage.
6. Publish fact-level reconciliation states with both evidence identities and
   alternatives. Only then may a unified graph expose one economic-flow
   hypothesis backed by A, B, or both.
7. Run terminal-source attribution over that named, versioned graph
   projection. Preserve unresolved and conflicting money explicitly.
