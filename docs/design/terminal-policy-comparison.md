# Terminal definition and allocation comparison

Status: implemented as a read-only Go diagnostic. Its six scenarios remain
unchanged and unselected. The user subsequently accepted the recommended narrow
partial boundary through the separate
[direct source-appearance attribution](./direct-source-attribution.md)
calculation. The [2024 diagnostic gate](../audit/terminal-policy-comparison-2026-09-20.md)
passes for both retained candidate dossiers with exact row and signed-cent
conservation.

## Question and scope

`compare-terminal-policies` answers a narrow design question: what can the
current candidate receipt evidence support if several explicit stopping and
allocation rules are applied to the same population?

The command authenticates one compact candidate dossier and uses its disjoint
candidate-linked Schedule A components. It excludes `memo_subtotal` from the
numeric input while retaining that complete excluded population. This follows
the existing memo contract; a disclosed conduit total is not a second
contribution. FEC guidance likewise describes the original contribution and
the conduit memo entry as two views of the same earmarked activity. See the
FEC guidance for [candidate committees](https://www.fec.gov/help-candidates-and-committees/filing-reports/contributions-received-through-conduits/)
and [political committee conduits](https://www.fec.gov/help-candidates-and-committees/filing-pac-reports/earmarked-contributions/).

The included scope is still not complete candidate funding. It lacks a complete
cash denominator, unitemized source identities, opening balances and accepted
cross-cycle cash continuity. Independent expenditures remain separate.

## Terminal definitions compared

| Definition | Current evidence state | Interpretation |
|---|---|---|
| Explicit earmark origin | Supported at occurrence grain | The original contributor designated the candidate. The conduit is a separate zero-money association. |
| Reported noncommittee appearance | Supported at occurrence grain | A direct source appearance, not a deduplicated or resolved person. |
| Reported committee counterparty | Supported only as the immediate counterparty | The committee-to-candidate receipt is evidenced, but the committee is not thereby the upstream economic origin. |
| Selected topology frontier | Rejected as a financial origin | Missing selected adjacency is not a complete funding history; the A and B frontier populations also differ materially. |
| Resolved person or organization | Blocked | This is the desired semantic boundary, but live entity resolution is not accepted yet. |

FEC guidance also distinguishes transfers between a candidate's own authorized
committees from contributions involving other committees. A generic committee
edge cannot therefore be relabeled as one uniform transfer or origin category.
See [transfers between a candidate's committees](https://www.fec.gov/help-candidates-and-committees/making-disbursements/transfers/).

## Allocation methods compared

Every executable scenario partitions the exact same input into exclusive
`direct`, `explicitly_earmarked`, `proportional`, and `unresolved` buckets.
Rows and known signed minor units must conserve. Unknown-amount rows always stay
unresolved.

- No allocation is the evidence-only baseline.
- Earmark-only allocation assigns only explicit designated receipts to their
  source appearance.
- Reported-direct allocation also assigns other nonmemo itemized-individual
  receipts to their source appearances.
- Committee-stop allocation additionally treats the immediate reported
  committee as the stopping point. It is measurable but does not meet the
  upstream-origin objective.
- Pooled pro-rata and chronological FIFO remain uncalculated. Both return the
  complete input as unresolved because the current evidence does not establish
  the required committee funding denominator or cash availability.
- Path replication is rejected because copying the downstream amount onto every
  upstream path duplicates money at branches.

The diagnostic recommendation was to retain explicit earmarks and direct source
appearances as supported partial allocations, keep them at source-occurrence
grain, and leave all committee-chain dollars unresolved for now. Do not call an
immediate committee or a topology frontier an upstream terminal source. This is
now adopted only through the separate cycle-wide calculation; the diagnostic
artifacts remain historical comparisons and do not become production inputs.

## Reproducibility

```text
legal-tender pipeline fec compare-terminal-policies \
  --candidate-dossier <exact-json> \
  --expected-dossier-id <sha256>
```

The result binds the dossier content ID and file digest, Schedule A fact and
manifest identities, source release, and actual executable digest. The six
scenarios all remain `selected=false`; terminal and allocation policy fields
stay null and terminal eligibility stays false. The normative contract is
[`terminal-policy-comparison/v1`](../../contracts/calculations/fec/terminal-policy-comparison/v1/README.md).

The separate production calculation and its
[complete 2024 gate](../audit/direct-source-attribution-2026-09-20.md) now pass.
This diagnostic did not become that calculation and remains unmodified.
