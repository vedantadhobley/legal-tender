# Committee-flow HTTP API gate — 2026-09-08

Status: passed for the pinned 2024
[observation graph](./arango-committee-flow-evidence-2026-09-08.md). The
[read-only API contract](../design/committee-flow-api.md) owns current behavior.
This gate did not deploy a resident service or expose a host port.

## Scope and identity

An opt-in Go integration test opened the real reader, started an HTTP test
server inside the one-off container, and exercised it with an HTTP client.
Published source storage was mounted read-only. The existing development
Arango account was supplied through environment variables without printing
the password. No database, index, graph record, source artifact, or publication
pointer was created or changed by the API.

Projection ID:
`89f049d4604f20a204c0ef52eb7ee5c37bbb7b59a516a901be06742529ce3b3f`.
Bundle ID:
`113c25c47c3d008dd79a470c9cd8e3482bbdcf82c1fc6a53f717561f571e9c3d`.
The source/calculation identities and full projection metadata remain those
of the prior graph gate. Coverage remains `partial`; no identity gains
economic-flow or terminal-attribution eligibility.

## Real HTTP checks

- Startup verifies exact source ancestry, candidate evidence, completed graph
  metadata, graph definition, full document readback, and the existing bounded
  query gate before serving.
- Identity pagination returns all 9,545 referenced committees in 96 pages,
  with no repeated identity and exact count agreement with the projection.
- Both ledgers return paginated observations, exact identity records,
  component summaries, and paginated component members.
- Both ledgers return direct path pages and hop-shortest paths, plus bounded
  neighborhood and return-to-start queries. Continuations do not repeat the
  preceding page. These HTTP samples use depth one; they are not dense
  eight-hop load tests or exhaustive cycle enumeration.
- Full source-row lookup runs twice per ledger. Each request hashes its
  requested shard and preserves the full physical fact row. No complete
  corpus rehash or selected-observation scan occurs per request.
- A cursor reused with a different ledger is rejected. Unknown projection
  IDs and invalid ledger requests return explicit errors.
- All 122 captured versioned API responses, including errors, validate
  against the checked-in response schema. The health response has its own
  small status shape and is excluded from this count.

## Measured cost

| Measurement | Result |
|---|---:|
| Verified reader startup | 70.772 seconds |
| Startup plus the HTTP gate | 71.413 seconds |
| Process peak RSS | 1,667,891,200 bytes |
| Full source request range, four requests | 34.753–41.343 ms |

These are warm local measurements, not production latency guarantees. Startup
includes full source hashing and graph readback. Later requests reuse the
pinned model/indexes but still verify the returned graph fields and requested
source bytes. The gate uses a 4 GiB container, four CPUs, `GOMEMLIMIT=2GiB`, and
`GOMAXPROCS=4`; no service memory cap changed.

The other sampled HTTP requests each completed within 5.2 ms. Some groups also
contain fast rejected requests, so the retained ranges are not latency
percentiles or representative graph-wide benchmarks. Dense bounded-depth
traversals may hit the five-second/128 MiB AQL caps and fail without a success
page. The API does not hide this by sampling before stable ordering.

## Code and contract gates

Go tests cover exact signed values beyond JavaScript integer precision,
negative/zero observations, no-repeat pagination, opposite-ledger rejection,
tampered field rejection, component membership separation, query caps,
malformed filters, invalid cursors, projection changes, cursor invalidation
after restart, concurrency admission, response limits, source membership,
same-size shard corruption, cancellation, and error redaction.

The source-review fixture now supplies the actual fixture shard digest: a
placeholder digest is no longer accepted by the stronger per-lookup check.
No source selection, matching rule, money calculation, or graph identity
changed. The standalone full-validation source lookup remains available.

The complete Go gates and targeted race tests pass. The rewrite Python
boundary/contract suite passes 58 tests. Python adds no serving or source
behavior; its new test and retained validator check only the HTTP wire schema.

## Retained evidence and limits

Under the configured storage root:
`dumps/audits/fec/committee-flow-api/2026-09-08/2024/`.

The audit retains the opt-in runner, test source, request paths/responses,
startup/query timing results, source-schema validator, schema validation
summary, log, and explicit completion marker. Full response samples are audit
data, not checked-in source fixtures. The committed wire fixture is synthetic.

Result SHA-256:
`2edfa0444e8ca67104195182a527698e68d305f162bcfc6fd938f38600786775`.

The gate uses the existing dev database identity; least-privilege account
provisioning, resident Compose service, proxy/access control, and a browser
client are separate deployment/product work. Multi-cycle routing, name/date
filters, economic-flow resolution, and terminal attribution remain in the
[rewrite queue](../todo.md).
