# Generation-bound funding neighborhoods

Status: implemented in Go. Full Go regression, targeted race and static checks
pass. The [retained live gate](../audit/funding-neighborhoods-2026-09-13.md)
and byte-identical fresh-process replay pass for 2024. This is a read-only CLI/reader, not a GUI,
HTTP service, graph reimport, terminal calculation or weekly activation.

## Pinned input boundary

`inspect-funding-neighborhood` opens the exact
[typed generation](./funding-evidence-generation.md) result plus its required
byte checksum. The caller supplies storage and retained receipt graph,
participant and conduit manifest locators. Their identities come from the
generation; they are not replacement input choices.

The reader derives immutable A/B/E bundle and reference manifest paths from
the pinned identities. It reruns the generation verifier and compares the whole
reconstructed result to the saved result. A serialized success claim alone grants
no access. Missing backing, schema changes, reference drift and changed graph
completion fail; there is no current-pointer fallback or repair path.

The original generation producer's executable digest remains part of that
generation's identity. Reverification uses the current implementation of its
unchanged policy, not execution of the old binary. Query results record the new
consumer executable digest separately. A query ID hashes the complete result,
including that consumer identity, generation, request and verified evidence.

## Query behavior

An entity is an exact FEC committee or candidate identifier. Cycle comes from
the verified generation. There are no cycle overrides, arbitrary collections,
raw AQL, terminal policies or name-based identity joins.

The default query returns one-hop pages across the generation's declared families.
`--family` selects one family. `--limit` defaults to two and accepts one through
ten items per family. Contributor appearance IDs remain occurrence identities;
this command does not accept them as resolved people.

- Reported receipts and conduit associations select the committee endpoint.
- Authorization context selects either endpoint and retains its original state.
- Receiver and sender observations select either committee endpoint, separately.
- Reconciliation candidates select adjacent comparison components, not payments.
- Outside support and opposition select the spender or candidate endpoint,
  with each stance in its own page.

Each page retains the generation's family descriptor: source population,
selection, grain, physical projection, amount meaning and overlap group. There
is no combined money total. Full-receipt and selected receiver views overlap.

Separate entity facets expose each graph's identity evidence. `present` includes a
source-backed missing-master stub; its document retains that unresolved state.
`not_present_in_projection` means no facet in that graph, not zero funding.
`not_applicable` and `not_requested` are distinct from an available empty page.

## Evidence and pagination

Returned receipt/conduit documents are reconstructed from the exact participant
occurrence and complete Schedule A source lookup. A conduit page also verifies
the complete compact decision artifact once for its selected rows. The full
related occurrence of a conduit association is not separately returned here.

Selected A/B documents are checked against the verified projection and drill
down to complete reported source occurrences. Authorization exposes exact linkage
fact membership. Reconciliation exposes verified component summaries and member
counts, not unbounded member arrays. Outside spending exposes the verified
resolved calculation group, result identity and exact input references; it does
not enumerate that group's individual Schedule E source members.

Ordering is deterministic by complete relationship identity. Schedule E uses
the full result ID rather than the shortened physical database key. Lookahead
is verified before reporting `has_more`. Continuations bind generation, entity,
family and ordering position. They are unsigned navigation tokens, not access
credentials. Changed scope is rejected; page size may change within its bound.

Every neighborhood rechecks all selected completion boundaries before returning.
Reads require immutable publications; they are not one transaction spanning
the selected databases and do not protect against a privileged concurrent writer that
preserves completion metadata while changing data.

## Shared-conduit extension

The [extended-generation reader](./shared-conduit-generation.md#consumer-boundary)
adds a separate `shared_conduit_association` family and `shared_conduits` facet.
It requires exact base/extension files, preserves the old family and binds cursors
to the outer generation. Shared pages return old/new decisions, complete-group
evidence, the full original occurrence and the full related memo occurrence.
Lookahead receives the same verification as returned rows. An original generation
cannot request this family or silently adopt its locators.

The [shared-query gate](../audit/shared-conduit-queries-2026-09-14.md) owns its
separate acceptance evidence. The
[date-window consumer](./funding-window-reader.md#shared-conduit-generation-inputs)
now uses the same extension with original receipt dates; its source-grain spending
endings retain their separate date contract.

## Operational cost and gate

Output bounds do not bound scan or source-verification work. Receipt adjacency
queries retain the existing server runtime/memory caps. Source drilldown verifies
backing shards; conduit access scans its compact decision artifact. Opening a
reader revalidates the generation's complete reference and selected-graph backing.
Reuse one reader during a bounded session; this is not a low-latency serving gate.

`validate-funding-neighborhoods` chooses witnesses in code: smallest eligible
IDs from verified selected A/B, authorization and outside populations, plus a
conduit artifact witness and a missing-master committee when available. It reads
each declared family, follows its first continuation when present, and checks
cross-family committee/candidate neighborhoods. Absent selection populations stay
explicit. Operator-provided candidate lists and source ordinals are not accepted.

The retained runner executes the gate again in a fresh process, enforces
`--expected-gate-id`, and compares complete result bytes. Both invocations verify
backing independently. This accepts selected one-hop witnesses, not all entities,
arbitrary path queries or person/corporation resolution.

The [typed path reader](./funding-paths.md) now extends this boundary to explicit
multi-hop routes. Its live acceptance is tracked separately. Traversal bounds and
truncation remain explicit; reachable paths do not establish terminal amounts.
Source membership drilldown for outside groups and serving performance remain
separate work in the [active queue](../todo.md).
