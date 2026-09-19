# Committee-flow investigative API

Implemented in Go. The API serves one pinned, completed
[observation projection](./arango-committee-flow-evidence.md) through a
read-only reader. The [real 2024 gate](../audit/committee-flow-api-2026-09-08.md)
verifies pagination, both ledgers, graph paths, components, and source lookup.
This is an implementation and isolated gate, not a deployed public service.

## HTTP contract

`GET /v1/committee-flow` returns the pinned projection identity, source inputs,
cycle, coverage, separate A/B measures, and attribution exclusions. All other
evidence routes include that full projection ID. A different ID returns 409;
the server never silently substitutes a newer projection.

Prefix below: `/v1/committee-flow/{projection_id}`.

| Route | Required query values | Result |
|---|---|---|
| `/queries/entities` | None | Referenced committee identities, including unresolved masters |
| `/entities/{committee_id}` | None | One exact committee identity/master record |
| `/queries/observations` | `ledger`, `committee`, `direction` | One-hop reported observations; direction is `inbound`, `outbound`, or `any` |
| `/components/{component_id}` | None | Candidate component summary with separate amounts and member counts |
| `/queries/members` | `ledger`, `component` | Selected observations belonging to that candidate component |
| `/queries/paths` | `ledger`, `committee`, `target`, `max_depth` | Directed paths within the requested depth |
| `/queries/neighborhood` | `ledger`, `committee`, `max_depth` | ANY-direction paths within the requested depth |
| `/queries/cycles` | `ledger`, `committee`, `max_depth` | Nonempty directed return-to-start paths within the requested depth |
| `/queries/shortest` | `ledger`, `committee`, `target`, `max_depth` | One hop-shortest path within that depth, with deterministic tie-breaking |
| `/observations/{ledger}/{observation_key}/source` | None | One full immutable Parquet fact row and its source locator/hash |

`ledger` must be `schedule_a` or `schedule_b`, except for identity queries,
which accept no ledger. Record queries accept `limit=1..100`; path queries
accept `limit=1..25` and `max_depth=1..8`. Default limit is 25. Shortest accepts
only limit 1 and no continuation. Depth has no default: callers must declare
the investigation bound. The schema lives in the
[API contract](../../contracts/api/committee-flow/v1/).

All query routes except shortest support `cursor`. The response contains
`items`, `has_more`, `next_cursor`, the normalized query, and an explicit
coverage scope. Missing-master coverage remains `partial`. A successful query
does not upgrade identity coverage, confirm a payment, or establish a terminal
source. Monetary minor units stay decimal integer strings.

Component membership is paged through observations. The summary omits the
potentially large ordinal arrays and reports each ledger's member count.
Every observation still carries its exact ordinal, fact identity, component,
reported date, signed amount, roles, and policy identities. Full source rows
remain in Parquet; the source route preserves nulls, empty strings, and every
physical field. Source lookup is one observation per request; callers page
the observation/member collection before drilling into selected rows.

## Pagination and traversal semantics

Pages use keyset ordering, not offsets or exposed Arango cursor IDs. Record
pages sort by the unique document key. Path pages sort by hop count and the
ordered edge IDs; this preserves parallel reported observations as distinct
paths. Stable ordering is required to avoid missing or repeated results across
pages. See Arango's [LIMIT ordering guidance](https://docs.arango.ai/arangodb/stable/aql/high-level-operations/limit/).

The opaque continuation is authenticated with a process-local random key and
binds the projection ID, full normalized query, page size, and last key. A
changed query, tampered cursor, or process restart invalidates it. Clients must
restart pagination after a restart; there is no persistent cursor store or
signing-key dependency in this slice.

Each money path uses one explicit edge collection. Component documents and
opposite-ledger observations cannot enter it. Paths between distinct endpoints
use unique vertices. Neighborhoods and cycles permit repeated vertices but
never reuse an edge within a path, preserving self-loops. Return-to-start
queries can include compound cycles; they are not a list of canonical simple
cycles. These rules use Arango's
[collection-set traversals and uniqueness options](https://docs.arango.ai/arangodb/stable/aql/graph-queries/traversals/).

The implementation sorts the bounded-depth result set before applying the
page limit. It does not take an arbitrary pre-sort sample and claim complete
pagination. Dense neighborhoods can still exceed the runtime or memory cap;
that request fails with no success page. `has_more=false` means no remaining
results for this query and its depth/uniqueness rules, not complete global
graph coverage. Shortest returns one shortest nonempty path inside the bound,
not all shortest paths. Money is never a path weight or an additive path total.

## Readiness, integrity, and request cost

Startup resolves the exact bundle, verifies published calculation/source
ancestry, rebuilds the selected model, and reads every graph document back.
It requires matching completed metadata and the exact graph definition.
It cannot create a database, indexes, or collections, import documents, repair
bad data, or advance a pointer. It reuses the existing graph query gate before
opening the HTTP listener.

The process retains the verified model and source membership/indexes. Returned
documents are compared with that model before release to the caller. Paths
are checked for ledger, field equality, continuity, ordering key, and bounds.
An unknown identity is 404; a missing or changed previously verified document
is an integrity failure, not a fabricated empty entity.

`OpenSourceReader` verifies immutable source backing once at startup. Each
lookup checks selected membership, hashes the requested shard using the same
open file that supplies the row, seeks the original ordinal, and compares the
source-derived observation with the accepted selection. It does not rehash
all source shards or reread all selected observations for every HTTP request.
No mutable current pointer is consulted after startup.

Published files and completed graph databases must remain immutable during a
reader's lifetime. Use read-only mounts and a read-only database identity for
deployment. This is not a cross-host lease or protection against arbitrary
concurrent administrative writes/deletions. New publication identities need
new reader instances; rolling selection and a multi-cycle catalog are not
implemented here.

## Limits and errors

Requests allow four concurrent operations, with no unbounded waiting queue.
HTTP operation contexts last at most 15 seconds. Interactive AQL has a
five-second runtime and 128 MiB memory cap; startup full readback retains its
separate verification timeout. Response encoding is capped at 8 MiB; URIs and
server headers are bounded. This is not a public abuse-control system.

Only GET is supported. Unknown, repeated, empty, or contradictory query
parameters fail rather than being silently ignored. No request can choose a
database, collection, source path, raw AQL, or arbitrary source ordinal.

Errors use short codes: 400 invalid query/cursor, 404 unknown evidence, 409
projection mismatch, 429 capacity, 504 context timeout, or 503 unavailable
evidence/query resource failure. No internal paths, credentials, backend
response text, or partial success payload accompanies an error.

## Running and remaining deployment work

```text
legal-tender serve committee-flow-evidence \
  --storage-root /storage --cycle <cycle> \
  --projection-bundle <immutable-evidence-bundle-manifest> \
  --endpoint <configured-arango-http-endpoint> \
  --username <configured-read-only-user> \
  --password-env ARANGO_PASSWORD
```

The default listener is `127.0.0.1:8080`. A readiness JSON line is emitted only
after verification and listener creation; diagnostics use stderr. SIGTERM
drains HTTP requests with a bounded shutdown. Credentials come from the
configured environment. `GET /healthz` is available after readiness and is
subject to the same concurrency admission limit.

Run this service in Docker. The measured one-off gate uses a 4 GiB container,
four CPUs, `GOMEMLIMIT=2GiB`, `GOMAXPROCS=4`, and read-only source storage. No
resident Compose service, host port, proxy route, or shared memory-cap change
was added. Before deployment, declare the service budget, configure a
least-privilege database account, and accept proxy/access-control routing.
There is no API authentication or public exposure in this slice.

Deployment and a thin client are deferred behind
[candidate upstream funding](./candidate-upstream.md). Name search,
reported-date filtering, multi-cycle routing, historical identity refresh,
community/centrality calculations, resolved economic flows, and terminal
attribution remain explicit [deferred work](../todo.md).
