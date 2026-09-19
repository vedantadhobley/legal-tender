# Candidate-scoped upstream evidence

Status: implemented Go calculation and read-only real 2024 gate. This is the
first candidate-scoped upstream slice, **not completed terminal-source dollar
attribution**. The [integrated candidate view](./candidate-evidence-view.md) now
combines it with receipt populations and optional summary context. Terminal and
allocation policies do not block that observation view; UI/deployment remain separate.

## Question and scope

For one candidate and source cycle: which selected committee receipts enter
the authorized committee group, what committee ancestry is disclosed upstream,
and what prevents attribution of those amounts to terminal donors?

The input is the existing [receiver observation cohort](./committee-flow-reconciliation.md),
not every Schedule A receipt. Individual and other noncommittee receipts are
preserved in canonical facts but excluded from this committee-only graph.
Empty incoming adjacency therefore establishes neither a terminal source nor a
complete funding denominator. The [product contract](./product-contract.md#terminal-source)
defines terminal sources as policy-selected boundaries, not permanent entity
types or merely leaves. Arango traversal cannot supply that policy.

## Inputs and execution

```text
legal-tender pipeline fec trace-candidate-committee-receipts \
  --storage-root /storage \
  --observation-bundle <exact-committee-flow-evidence-bundle.json> \
  --linkage-facts <exact-candidate-committee-linkage-manifest.json> \
  --cycle <even-year> \
  --candidate <exact-FEC-candidate-ID>
```

Go verifies the bundle, both ledgers' backing and reconciliation evidence,
same-cycle committee masters, and exact linkage source ancestry. It consumes
only Schedule A amounts and edges. Schedule B remains comparison ancestry,
never another amount or a traversal shortcut.

The linkage gate reuses the master's source-binding method. Older facts are
usable only when the coordinated release selects their exact archive and
staged member bytes. A same-size or identically named archive is insufficient.
Inputs resolve to immutable manifests, not subsequent current pointer values.

The separate `RunWithReferenceWitnesses` Go entry point is used by the connected
receipt consumer. It requires a freshly verified [reference-content context](./reference-content-equivalence.md),
retains the receipt graph's exact original master/linkage facts and adds the
equivalence proofs to calculation identity. The command above and ordinary
`Run`/`RunWithWitnesses` keep their strict archive contract and unchanged output.

Candidate scope reuses the accepted [authorization policy](./calculation-contracts.md#contract-c-candidate-authorized-committee-scope).
Conflicting designations and shared authorization remain unresolved. No
unambiguous authorized committee is an error, not a fabricated zero result.

The command emits deterministic JSON on stdout and progress on stderr. It
neither queries nor writes Arango: it consumes the same verified evidence
without an interactive query's depth/result cap. It creates no publication
pointer, resident service, Dagster asset, or network request. The
[real gate](../audit/candidate-upstream-2026-09-08.md) retains measurements.

## Candidate boundary accounting

Every selected receipt whose recipient has an authorized or unresolved
candidate linkage appears once in exactly one bucket:

1. `external_to_authorized_scope`: recipient is authorized; sender is neither
   authorized nor an unresolved candidate linkage.
2. `within_authorized_scope`: both committees are authorized for this candidate.
3. `unresolved_authorization_boundary`: either endpoint has an unresolved
   candidate linkage relevant to the boundary.

The third bucket takes precedence. All three conserve candidate-linked
observations. Internal transfers remain evidence, not another receipt entering
the group. Unknown authorization amounts stay outside the external denominator.

These are **selected signed reporting observations**, not total candidate
receipts, cash available, or unique economic payments. Each row retains its
contribution, transfer, in-kind, or refund/repayment role and date. The diagnostic
grand subtotal does not convert those roles into one cash measure. Positive,
negative, and zero populations remain explicit even when net amounts cancel.
Totals use integer strings and arbitrary-precision integer arithmetic.

```text
external selected observation amount
    = terminal allocated amount (0) + unresolved attribution amount
```

Zero terminal allocation means **this method made no allocation**, not that
terminal donors contributed zero. The unresolved result preserves positive
and negative populations. No remainder is labeled grassroots, corporate, or
ideological funding. Unknown authorization is separate from unknown attribution.

## Ancestry, cycles, and retained evidence

Reverse breadth-first traversal starts at the authorized group and visits the
complete reachable selected Schedule A cohort. Missing masters do not erase
reported edges. There is no arbitrary depth truncation. The result includes:

- Candidate receipt observations with exact ordinals, reported fields, and roles.
- Sorted upstream source-row ordinals bound to the exact source fact set and
  shared observation artifact. Full upstream rows are not copied into each
  candidate result; their fields remain retrievable from shared evidence.
- Every reachable committee, same-cycle master fact or explicit absence,
  incoming-observation count, and deterministic shortest-hop source witness.
- Cyclic strongly connected components, including self-loops, and explicit
  per-node attribution limits.

Witnesses describe one connection to any authorized committee, not every path
or an allocated dollar route. Exact subgraph membership supports further path
queries. Iterative SCC analysis is linear in graph size and does not enumerate
exponentially many paths or recursively count the candidate's dollars.

Cycles describe **reported connectivity**. Zero, negative, in-kind, and
out-of-date observations can participate. Source-cycle membership does not
establish chronological fund availability. No date window, opening-balance
guess, or fungibility assumption turns connectivity into chronological
attribution. Every node remains `terminal_attribution_eligible: false`.

## Identity and verification

Policy: `fec/candidate-committee-upstream-evidence@1.0.0`. Calculation identity
hashes deterministic result bytes with an empty calculation ID, including
candidate, cycle, policy, exact inputs, membership, and exclusions. Input-row
ordering does not affect output; changing supporting input identities does.
Published inputs must remain immutable during a read; this is not a lease
against administrative mutation.

The [wire schema](../../contracts/calculations/fec/candidate-committee-upstream/v1/)
requires source identities, explicit limits, nulls, and false eligibility.
Tests cover authorization conflicts, signed sums beyond int64 totals, parallel
observations, self-loops, independent randomized reachability/SCC comparison,
long chains, cancellation, source-role consistency, replay, and exact real
source membership and witness reconstruction.

## Receipt inventory and next allocation gates

The [committee receipt inventory](./committee-funding-basis.md) now selects
donor-bearing receipts for reached committees from existing detailed facts,
preserving contributor, conduit, employer, role, amount, and source identity.
Disclosed contributor occurrences are not yet resolved people or
corporations; shared names or employment fields cannot establish economic
identity or turn employee donations into corporate spending.

Before allocating pooled funds, define the versioned funding basis and its
omissions: non-itemized coverage, other receipt families, opening balances,
negative adjustments, in-kind activity, and time. Candidate summaries are
comparison evidence, not automatic per-committee denominators. A proportional
scenario must state its modeled population and assumptions; incomplete
coverage cannot silently become 100% explained funding.

Direct reported sources, explicit earmarks, proportional scenarios, cyclic
unresolved amounts, and other unknown remainders remain separate. This slice
does not accept a proportional or terminal-classification policy by implication.
Publication, invalidation, Dagster, API exposure, and all-cycle rollout follow
an accepted monetary consumer rather than promoting this diagnostic into a
production funding answer.
