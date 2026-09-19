# Typed funding-evidence generation

Status: implemented in Go. Regression, targeted race/static checks and the
[retained 2024 verification and byte-identical replay](../audit/funding-evidence-generation-2026-09-13.md)
pass. This is an exact, read-only generation binding, not a new graph
import, serving API, terminal calculation or weekly activation.

## Boundary

`verify-funding-generation` binds the completed receipt publication, selected
receiver/sender graph and resolved independent-expenditure graph. All are required.
Cycle comes from their verified inputs. The command has no default cycle, current
pointer, candidate selection, optional-family fallback or repair path.

The generation includes exact graph identities, manifest digests, original
source/fact/calculation ancestry, declared relationship families, endpoint
namespaces and coverage. Its ID hashes the deterministic result, including the
consumer executable identity. `--expected-generation-id` rejects changed replay.
Operational paths, credentials, timing and memory do not enter that identity.

The output does not advance a current-generation pointer. A later consumer must
load and reverify the exact inputs; a saved JSON claim of success is not an
in-process authorization to read a different graph.

## Required evidence families

| Family | Grain and meaning | Counting boundary |
|---|---|---|
| Reported receipt | Schedule A occurrence, including memo/signed/unknown source states | Observation, not an effective-payment decision |
| Conduit association | Qualified source/reference association | Adds no money |
| Candidate authorization context | Committee/candidate relationship with retained authorization state | Not a transfer; check `authorized` before using an authorized path |
| Receiver observation | Selected Schedule A committee occurrence | Overlaps the full receipt family; never sum the two views |
| Sender observation | Selected Schedule B occurrence | Separate reported ledger, not a reconciled payment |
| Reconciliation candidate | A/B comparison component | Not another money edge |
| Independent support | Resolved spender/candidate/support group | Separate Schedule E amount, not a candidate receipt |
| Independent opposition | Resolved spender/candidate/opposition group | Remains separate from support |

Every family identifies its physical database/collection, projection, source fact
set where applicable, selection predicate, grain, membership and overlap group.
The nested A/B inputs retain full source populations alongside selected counts.
Schedule E retains unprojectable decisions and amounts outside graph edges.
Receipt unrouted/conduit states and A/B/E missing-master coverage remain visible.
The receipt missing-master count is not recomputed here; the pinned
[full-cycle audit](../audit/arango-receipt-participant-cycle-2026-09-12.md) and
source-backed endpoint reads own that detail. There is deliberately no combined
money-total field.

## Exact source compatibility

The A/B bundle owns the coordinated source-release target. The receipt graph must
share its exact Schedule A fact set, digest, original release and row population.
Its existing source-reuse checks remain unchanged.

The [reference proof](./reference-content-equivalence.md) verifies complete
CN/CM/CCL reference content against that target while retaining both acquisition
histories. CN uses its own versioned policy; existing CM/CCL proof identities stay
unchanged. Receipt and outside candidate references must match in complete content
and schema; all three committee references must also match. Actual reference drift
blocks this version of generation binding. There is no name-based fallback.

Schedule E may retain an earlier release ID only when its original published
archive digest and exact staged relation digest/size match the target release's
selected Schedule E member. The membership proof verifies original/target release
manifests, exact occurrence ancestry and complete retained fact-artifact backing.
It does not rehash or reacquire the all-history raw archive, infer economic
equivalence, relabel old facts, or accept an older source version absent from the
target. A new Schedule E source version requires its own calculation/graph refresh.

## Read-only graph checks

- Receipt opening reuses the completed-cycle reader's strict source ancestry,
  schema, immutable completion and live collection counts. Its published full
  field-readback proof remains pinned. This command does not repeat the full
  receipt field scan or the [connection census](./receipt-candidate-connection.md).
- The existing A/B reader reconstructs and checks every selected entity,
  observation and reconciliation component, including its existing query gates.
- The new resolved Schedule E reader reconstructs the existing model and compares
  every entity, edge and metadata field in batches of at most 1,000 keys. It checks
  exact collection counts, support/opposition counts and signed amounts. Unknown
  fields, absent/extra documents, changed stance, amount or ancestry fail. It never
  invokes the publisher's schema creation or import path.
- All three completion boundaries are checked again before the result is emitted.

These checks rely on the immutable-publication convention; they are not a database
transaction spanning three physical databases or protection from a concurrent
privileged writer that changes documents while preserving completion metadata.

## Endpoint namespaces

Committee and candidate joins use `(cycle, FEC identifier kind, exact FEC ID)`.
Receipt/A/B collections use bare IDs; the outside graph uses `committee_` and
`candidate_` prefixes. The generation records these physical mappings explicitly.
Contributor appearances remain keyed by fact-set/occurrence, not a guessed person.

A namespace mapping does not claim that every endpoint exists in every graph.
Consumers must preserve missing facets and each graph's identity evidence. Shared
FEC IDs and equal reference content do not resolve people, employers or corporate
families, and do not make every relationship a money transfer.

## Command and next gate

The command uses the same exact receipt inputs as
[`inspect-receipt-candidate-connection`](./receipt-candidate-connection.md), plus
`--expected-flow-bundle-sha256`, `--outside-bundle` and
`--expected-outside-bundle-sha256`. Both bundles must name immutable manifests.
It reads the configured password environment variable, never a password argument.
Run with a memory cap; the retained gate uses 4 GiB and `GOMEMLIMIT=2GiB`.

The [generation-bound neighborhood reader](./funding-neighborhoods.md) now
implements typed one-hop queries and source-backed family drilldown; its live
acceptance status is recorded separately. Keep serving readiness, all-candidate paths, other-cycle rollout and
unattended generation publication as explicit gates in the [active queue](../todo.md).
This binding does not complete those gates or choose terminal/allocation policy.
