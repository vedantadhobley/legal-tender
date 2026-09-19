# Receipt-to-candidate graph connection

Status: Go read-only consumer and automatic validation command implemented;
regression, targeted race and static checks pass. The first real attempt rejected
archive-only reference differences. The new [reference-content proof](./reference-content-equivalence.md)
passes while preserving both histories. The complete 2024 index census, selected
cross-graph witnesses and byte-identical fresh replay now pass the
[connection gate](../audit/reference-content-equivalence-2026-09-13.md).
Full [receipt cycle publication/replay](./arango-receipt-participant-cycle.md)
is also accepted; this does not verify every candidate/path or an integrated A/B/E generation.
This is one implementation step in the [connected-graph plan](./connected-funding-graph.md),
not acceptance of that entire milestone.

## Behavior

`inspect-receipt-candidate-connection` connects one exact receipt occurrence to
its reported recipient, the existing selected receiver-reported committee chain,
and candidate authorization. It reads both existing graph databases; it neither
copies their collections nor publishes another graph. Candidate and source
ordinal are inputs. Cycle and population come from the completed manifest.

The command reuses [candidate upstream](./candidate-upstream.md) to select a
deterministic shortest-hop witness over the complete selected receiver cohort.
It follows strictly decreasing distance, with no arbitrary eight-hop truncation.
It does not enumerate every possible path. A missing recipient stays unresolved;
no route through the selected cohort means no established path, not a terminal
source. Missing masters retain the existing explicit identity states.

Receipt, conduit association, committee observations and authorization stay
separate typed evidence. Memo, signed and unknown amounts remain unchanged.
The conduit association adds no amount and is context, not another path to
allocate. The command produces no path sum, terminal amount or identity merge.
The receipt's complete retained source row accompanies the verified graph values.
Committee observations retain their exact fact-set/ordinal and calculation
references for the existing source lookup.

## Exact inputs and readback

Require the full-cycle manifest and its expected byte SHA-256. Reject samples,
incomplete scope, invalid identity, altered counts or missing evidence digests.
Recompute its definition from the exact participant/conduit, Schedule A and
master/linkage inputs, using the publisher's retained build identity. Record the
consumer build separately. Resource settings do not select domain behavior.

Check live schema without creation, exact completion, collection counts, and
every selected appearance/receipt/conduit/context document. Check expected
absent edges too. Reconstruct the selected occurrence from retained participant
and full source facts. Revalidate the complete conduit disposition stream rather
than trusting its stored decision. These reads cannot repair missing graph state.

The A/B bundle's existing loader verifies source backing and coordinated-release
reuse. Require the exact same Schedule A fact identity, manifest bytes and
population. The receipt graph retains its exact original committee and linkage
facts. When the flow graph's reference provenance differs, the mandatory Go
reference context proves complete archive/member/schema/fact equivalence before
reuse. Both original fact references remain visible; no publication is relabeled.

Open the completed A/B graph through its existing verified reader and compare
its bundle and source ancestry. Read each chain observation by the pinned fact
set, ledger and source ordinal. Use canonical verified values for the connection
identity, not server JSON property order. The result binds both graph identities,
the upstream calculation and inputs, source occurrence and consumer executable.
Identical pinned inputs and build produce the same connection identity.

## Operation and cost

Run `legal-tender pipeline fec inspect-receipt-candidate-connection --help` for
the full input flags. Supply explicit `--graph-manifest`,
`--expected-graph-sha256`, `--flow-bundle`, `--candidate`,
`--source-row-ordinal` and the same exact source manifests used for publication.
Use configured credentials through `--password-env`, never a password argument.
There is no resume, write, cycle override, sample, terminal or allocation flag.
Source storage and publication mounts can remain read-only.

This is an acceptance/drilldown command, not a low-latency serving API. Opening
existing readers verifies backing artifacts; conduit verification scans its
complete sparse stream. The A/B reader also verifies its selected graph and
loads that existing selected cohort, not the full receipt population. Do not
launch this repeatedly per donor to approximate a cycle-wide calculation.
Measure the real command before selecting serving caches or another access index.

## Automatic acceptance command

`validate-receipt-candidate-connections` uses the same source, graph and flow
inputs but accepts no candidate, ordinal or cycle override. Selection is Go
code under `fec/receipt-candidate-connection-gate@1.0.0`, not a manual audit:

1. Reject incompatible shared receipt identities before expensive source or graph
   reads. Verify the complete CM/CCL reference proofs, then require both completed
   graphs and exact ancestry under that proven context;
   metadata-only agreement never substitutes for complete backing verification.
2. Use the first canonical candidate identifier with an authorized committee
   present in the selected receiver ledger. This is a reproducible test fixture,
   not a financial or identity rule. Amounts and names do not select it.
3. Scan the complete retained participant/conduit stream once. Conserve a disjoint
   census of direct authorization, upstream connection, no selected-cohort route
   and unresolved recipient. Separately count overlapping conduit, missing-master,
   cyclic, memo, negative, zero and unknown-amount cases.
4. Retain the first source ordinal for each present case. Deduplicate shared
   witnesses, reconstruct each full source row and verify every selected graph
   document twice. Require identical replay and agreement with the census.
5. Recheck both graph completion records. Emit one deterministic JSON proof only
   after every check passes; an error exits nonzero without a success result.

The proof binds the executable, policy, exact input manifests, both projections,
the reference-equivalence proofs, upstream calculation, full census, selected
source locators and connection IDs.
`--expected-gate-id` requires an identical proof on a new whole-command invocation.
Timing, memory, credentials and operational paths are not proof-identity operands.
An absent case is explicitly `not_present_in_complete_candidate_cycle_scope`;
it cannot become a fabricated example or a claim that the case is globally absent.

Readers are shared within the invocation. Case selection retains at most one
owned record per category, not an in-memory row per source occurrence. It scans
the compact index, not another full decode or import of the 81-field corpus.
That ordered join is serial; it is not an eight-reader source scan. Existing
backing verification and the selected A/B graph reader still have real startup
cost. Container limits remain mandatory; the gate adds no standing service.

This validates complete input membership and selected cross-graph witnesses for
one automatically selected candidate scope. It is not all-candidate path coverage,
all-path enumeration, person resolution or terminal-dollar attribution. Passing
this command does not enable unattended weekly publication. The remaining
generation readiness, orchestration, other-cycle and retention gates remain in
the [active queue](../todo.md).

## Verification and remaining scope

Fixture checks cover multiple cycles, including a later cycle outside the current
four-cycle window; unchanged-source reuse; changed fact/master/linkage rejection;
partial, corrupt and overflowed completion; deterministic paths longer than eight
hops; direct authorization; missing routes; cyclic/broken witnesses; absent edges;
read-only HTTP behavior; ledger/fact-set-specific lookup; and canonical output
independent of backend JSON property order. The complete Go regression suite
passes, with targeted race and static checks for the receipt graph, A/B graph,
CLI and upstream packages.

Additional fixtures cover automatic selection, input ordering, missing eligible
scope, complete census conservation, overlapping and absent cases, owned source
buffers, duplicate/gapped ordinals, unique-witness replay, changed results,
classification disagreement, cancellation and financial/identity promotion.
The [live audit](../audit/reference-content-equivalence-2026-09-13.md) records real
execution, explicit outcomes, retained proofs and cost. No manual interpretation
of individual examples can substitute for the automatic gate.

Broader typed sender/outside-spending integration, population coverage, the other
detailed A/B cycles, employer/identity assertions and weekly coordination remain
open. A single connected witness is not a complete integrated generation.
