# Committee reported-receipt inventory

Status: implemented manual Go diagnostic. The receipt inventory is not an
accepted denominator for pooled-fund allocation. The
[candidate upstream calculation](./candidate-upstream.md) supplies the reached
committee set; this slice supplies its recorded Schedule A receipt evidence.

## Source and calculation contract

Read one complete, dense, valid cycle of published Schedule A Parquet facts.
Verify the immutable manifest and all backing shard hashes before calculation.
Scan the existing narrow typed columns once for the cycle, not once per
candidate. Check physical schema, row counts, contiguous source ordinals,
normalization, cycle, and amount-state consistency while scanning.

Reuse the accepted [individual-receipt rule](./calculation-contracts.md) and
[committee-flow rule](./arango-receiver-reported-committee-flow-projection.md).
Keep both decisions independently on every bucket. This adds no amendment,
name-based deduplication, identity resolution, or receipt-type policy.

Each source row has exactly one inventory component, in this precedence order:

1. Invalid or absent exact recipient committee ID: `unresolved_recipient`.
2. Publisher memo-subtotal flag: `memo_subtotal`.
3. Included by both accepted predicates: `overlapping_individual_and_committee`.
4. Included by only the individual predicate: `itemized_individual_only`.
5. Included by only the committee predicate: `committee_flow_only`.
6. Null reported amount: `unknown_amount`.
7. Null publisher individual classification: `unresolved_individual_class`.
8. Remaining observations: `other_reported_receipt`.

The underlying individual and committee decisions survive that precedence.
For example, an unknown amount on a memo row remains unknown in its measures;
it does not become a known zero. The component label does not erase the memo
row's separate publisher classification decision. Receipt roles remain separate
bucket keys, including in-kind, refund, earmark, and unresolved roles.

Buckets preserve source-null, empty, and invalid recipient IDs separately.
Rows, known/unknown amounts, positive/negative/zero counts, and exact signed,
positive, and negative cent subtotals must conserve. Arithmetic overflow fails
the calculation. No floating-point money is used. The presence of a nonempty
conduit ID is a diagnostic count, not confirmation of a conduit identity.

## Grain and drilldown

The full 99-column fact remains the record authority. The inventory retains a
versioned predicate, exact fact-set/manifest identities, first/last source
ordinals, and a compact per-bucket shard-presence bitmap. It does not create
one donor document per receipt or repeat the receipt corpus for each candidate.

The row lookup accepts an exact recipient ID, optional component, exclusive
source-ordinal cursor, and a limit of 1–100. It skips shards known to have no
members, reevaluates the same predicate, and uses a lookahead row to establish
`has_more`. It rehashes each opened shard and returns all physical source and
typed fields. Source null and empty string remain different; int64 values use
decimal strings. Cancellation or integrity failure returns no success page.

Contributor, conduit, and employer fields remain separate source observations.
This is consistent with the FEC's separate reporting of the original
contribution and conduit information; a conduit row must not automatically
become another original donation. See the official
[conduit reporting guidance](https://www.fec.gov/help-candidates-and-committees/filing-reports/contributions-received-through-conduits/).
The inventory implements the already-accepted memo rule. The separate
[same-report association reviewer](./receipt-report-association.md) now tests
cross-row earmark evidence without changing inventory amounts. Neither merges
people by name or classifies an employee's contribution as corporate money.

## Candidate assessment

Recompute the accepted upstream trace from its exact observation bundle and
release-matched linkage facts. Reject a different cycle, Schedule A fact-set
ID, manifest digest, or fact count. Attach component measures to each reached
committee, retaining candidate authorization and current-master coverage.

Do not sum inventory amounts across the network into a candidate funding
total. The same funds can appear in several committees' reported receipts.
Do not infer terminal status from an absent master or missing incoming rows.
Every assessment remains terminal-attribution-ineligible.

## Commands

```bash
legal-tender pipeline fec calculate-committee-funding-basis \
  --storage-root /storage --schedule-a-facts <exact-manifest> \
  --cycle <cycle> --workers 4

legal-tender pipeline fec list-funding-receipts \
  --storage-root /storage --basis-result <inventory-json> \
  --committee <committee-id> --component itemized_individual_only \
  --after-ordinal 0 --limit 20

legal-tender pipeline fec assess-candidate-funding-basis \
  --storage-root /storage --basis-result <inventory-json> \
  --observation-bundle <exact-bundle> --linkage-facts <exact-manifest> \
  --cycle <cycle> --candidate <candidate-id>
```

JSON results go to stdout and diagnostics to stderr. These are manual,
read-only source consumers; no current pointers, Arango collections, HTTP
services, or Dagster assets change. The reader verifies backing once at open;
the CLI opens once per invocation. A future resident consumer can reuse one
reader but needs a separate operational contract. Workers are bounded to four;
the inventory fails above 250,000 buckets and loading fails above 128 MiB.

## What remains before attribution

The inventory covers reported itemized observations, not complete available
cash. We still need an accepted treatment of unitemized receipts, other receipt
families, opening balances and prior-cycle money, chronological availability,
in-kind valuation, and negative adjustments. Individual and committee predicates
may overlap; that overlap is not two amounts to allocate.

Direct reported contributors, explicit earmarks, and modeled pooled-fund
allocation need distinct contracts. No proportional allocation, terminal
classification, or corporate identity policy is accepted by this inventory.
Publication, source-change invalidation, orchestration, and multi-cycle rollout
follow acceptance of the monetary consumer.

The [wire contracts](../../contracts/calculations/fec/committee-funding-basis/v1/)
define the outputs. The [2024 gate](../audit/committee-funding-basis-2026-09-08.md)
records corpus evidence and remaining limitations.

The additive [source-role review](./receipt-source-evidence.md) now resolves
overlap routing for a combined reported-source view and explicitly classifies
unresolved conduit evidence. It leaves this inventory and both historical
monetary predicates unchanged; source-role annotations are not terminal allocation.
