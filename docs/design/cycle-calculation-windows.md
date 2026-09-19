# Cycle partitions and calculation windows

Status: accepted clarification of PD-002, 2026-09-13. Cycle-partitioned source
facts and current single-cycle calculations already exist. General cross-cycle
identity, graph and attribution consumers do not follow automatically from
this clarification; they need their own implementation and acceptance gates.
The [bounded composition test](#bounded-composition-test) now passes at the
stored-model and path-search boundaries, not at the production loader/API boundary.
The subsequent [committee window reader](./funding-window-reader.md) implements
the first selected-ledger consumer with unchanged per-input verification. Its
own contract tracks real-input acceptance; general cross-source work remains open.

## Building blocks, not analytical walls

Preserve fine-grained facts and independently reproducible cycle publications.
A shared publisher archive can back several partitions without being downloaded
or copied once per cycle. A cycle is a useful processing, reporting and query
dimension; it does not require every calculation to consume only that cycle.

Combining cycle partitions means selecting compatible detailed evidence, not
first collapsing it into totals. Per-cycle projections are reusable inputs only
when they retain the information needed by the next calculation.

| Operation | Appropriate inputs and composition |
|---|---|
| Parsing and source validation | Exact source artifacts and cycle partitions; preserve dates, source labels and every occurrence |
| Additive flow totals | Compatible subtotals with disjoint accepted membership, the same definitions and consistent units; preserve coverage |
| Ratios, distinct identities and rankings | Required detailed membership or sufficient mergeable state; recompute ratios from components and rankings before top-N truncation |
| Person/organization resolution | Explicitly selected evidence from one or more cycles, with versioned identity decisions and time-specific relationships |
| Paths, components and centrality | The selected combined graph and its declared topology/time rules, not sums of per-cycle graph metrics |
| Money-flow attribution | Required dated observations and balance/coverage evidence, potentially spanning cycles; no allocation method is selected here |

Opening/closing balances are stocks, not additive period flows. Sender and
receiver observations remain separate ledgers even across cycles. Selecting
overlapping publications or seeing the same economic event on both sides does
not create additional money. Existing source and reconciliation contracts still
govern occurrence membership, revisions and financial interpretation.

## Output scope differs from evidence scope

Cross-cycle work belongs in a separate analysis consumer above the cycle
pipelines. A 2024 ingestion or fact publication does not ingest 2022's records
into its own partition. Instead, the analysis references the already published
2022 and 2024 inputs, preserving both source identities, cycles and dates.
It may produce a 2024 reporting result without moving or relabeling either
cycle's underlying records. A derived window-level index or graph, if needed,
is its own versioned projection rather than another cycle's source dataset.

The default four-cycle view is a configuration of this analysis layer, not a
separate hardcoded processing tier. A consumer can select one, two, four or
another explicit set of available cycles according to its question and evidence
requirements. The first committee-window consumer follows this boundary without
changing the existing single-cycle readers; it is not general cross-source analytics.

A calculation must declare:

- The output period or cycle set and any transaction-date filters.
- The evidence partitions, exact source/fact/graph publications and coverage
  actually used, including any earlier-period context.
- The source snapshots/as-of basis and versioned selection, identity, graph
  and calculation rules.
- Missing evidence and boundary conditions, including unresolved opening funds.

For example, a 2024 output may need evidence about money received by a PAC in
2022. A 2024 cycle boundary cannot establish that the PAC originated the money.
Earlier evidence may improve attribution but does not, by itself, prove which
pooled dollars funded a later payment. Missing prior context remains explicit;
neither an eight-year window nor a path-search cutoff creates a terminal donor.

Reported transaction dates, filing/coverage periods, source-cycle labels and
relationship valid times stay distinct. Cross-cycle identity evidence must not
retroactively apply a current employer, ownership relation or donor threshold
to every historical appearance. An observed graph path is not automatically a
chronologically feasible or financially attributable path.

## Reproducibility and invalidation

Every combined calculation pins the full input set and its policies. Changes to
an earlier partition or shared identity decision can invalidate a later-period
result only through declared dependencies. Recalculation creates a new version;
it does not rewrite the prior published answer or unrelated cycle results.

The latest-four-cycle window remains the default product view, not a retention
limit, a universal evidence window or a rule that all analytics are sums of four
independent answers. Incomplete active cycles and missing earlier context remain
visible. Current single-cycle readers must retain their cycle-compatibility
checks; cross-cycle support needs an explicit consumer, not removal of those
checks or relabeling of existing graphs.

## Implementation sequence

Continue completing the 2024 evidence/identity pipeline without requiring all
four A/B cycles to be published first. Introduce small multi-cycle fixtures when
a consumer needs them, covering duplicated membership, time-specific identity,
non-additive results and missing earlier evidence as relevant to that consumer.
Do not build a generic cross-cycle engine before a concrete calculation needs it.

Additional real cycles validate reuse and coverage; they remain required for
the full default-window rollout, not a blanket prerequisite for designing the
next layer. A calculation requiring earlier real evidence cannot claim complete
acceptance until that evidence is available and its multi-cycle gate passes.
Keep the [connected-graph plan](./connected-funding-graph.md) and
[active queue](../todo.md) explicit about that distinction.

Maintain the assumption inventory during implementation, then complete the
[user review and reproducible checkpoint](./pre-attribution-review.md) before
selecting terminal definitions or dollar allocation. This clarification changes
no source bytes, graph publication, existing calculation policy or runtime code.

## Bounded composition test

On 2026-09-13 the user requested a test before extending the application contract.
Two Go test files exercise existing code with synthetic evidence; no production
loader, financial policy, source publication or Arango database was changed:

- The [stored-model test](../../internal/projection/arango/flowevidence/cross_partition_test.go)
  exercises the actual observation document types, edge-key function,
  `VisitLinks` and `VisitCommitteeDocuments`. The same committee ID connects
  across two source cycles while both reported master names and their fact
  references survive. Equal row ordinals in different fact sets do not collide;
  the two ledgers remain separate. Exact signed amounts and null dates survive
  serialization. A cycle-specific name assertion is not a day-level validity
  interval or verified employment relationship.
- The [composition test](../../internal/projection/arango/fundinggeneration/cross_partition_test.go)
  feeds source-shaped fixtures into the existing `pathTopology` and
  `searchPaths`. Neither partition alone contains the requested route; the
  combined detailed topology has two paths because parallel observations remain
  distinct. The test repeats with another pair of cycles, reorders the inputs,
  checks unchanged source observations, and rejects duplicate membership.
  A four-day window crossing a cycle boundary changes membership by reported
  dates, not cycle labels. Unknown dates remain separate in that bounded
  selection and remain available in all-supplied-evidence topology.

This is a layered fixture proof, not an end-to-end source-to-Arango test.
The composition test's date selector and provenance map are explicitly test-only
scaffolding; its synthetic link-key encoding is not a new production identity.
Passing them does not implement a temporal API, cross-source resolution, snapshot
reconciliation or complete multi-cycle loading. The production generation still
rejects mixed cycles, and its existing compatibility tests continue to pass.

The reverse-date counterexample intentionally remains a topological path. Current
search does not claim chronological money flow, and the test must not silently
introduce an allocation policy. Similarly, removing an edge by a time filter can
produce a no-outgoing search result without making that endpoint a terminal donor.
Unknown validity and prior-period funding remain unresolved.

Both affected packages pass full package tests, race checks and `go vet`.
The subsequent [verified committee-window consumer](./funding-window-reader.md)
retains each input's provenance and historical facets, carries dates
alongside topology, accounts for unknown/outside-window evidence, and preserves
the existing single-cycle checks. The fixture does not justify dropping those
checks, globally merging profiles, or forcing future sources into FEC periods.
