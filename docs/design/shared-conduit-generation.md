# Shared-conduit graph extension

Status: implemented in Go; the [dated gate](../audit/shared-conduit-publication-2026-09-14.md)
owns live import, complete readback and replay acceptance.

## Boundary

The [complete group publication](./shared-reference-group-rule.md) adds supported
associations to occurrences previously excluded only by the one-to-one shape
rule. It does not add receipts or payments. Rebuilding the entire receipt graph
would duplicate hundreds of gigabytes of unchanged encoded documents.

Instead, an isolated `lt_receipt_shared_<cycle>_<identity>` database stores only:

- One source-occurrence reference per new association: exact fact set, ordinal,
  existing appearance key and base receipt projection ID. This is not a new donor.
- One reported conduit edge per new association, preserving the full decision,
  related memo ordinal and amount-comparison axis. Additional money is zero.
- Referenced committee context from the base's exact master facts, preserving
  missing-master states. No name/organization resolution is introduced.

The extension uses the existing receipt graph's collection/transport machinery.
Unused receipt and authorization collections remain empty. The existing base
databases, their documents and completion metadata remain unchanged. The extension
has a named graph; every edge endpoint has a vertex in that database.

## Exact evidence and verification

The caller opens the existing typed generation with the normal source/reference
and live graph checks. The v2 calculation must pin the base graph's exact v1
conduit manifest, participant publication and Schedule A fact ancestry.

The importer reads every old/new calculation decision and verifies all permitted
changes and complete group membership counts through `ReadAdditions`. For each
new edge it also reads the base appearance and checks its exact original
shared-degree disposition, related ordinal, amount comparison, fact set and
source ordinal. The source-occurrence reference and edge then receive complete
field readback from the extension. Context records receive the same readback.

Complete collection counts, ordered payload hashes and unchanged base completion
are required before publishing extension completion. The deterministic definition
pins the base projection, v2 manifest bytes, group policy and executable. A new
build has a new identity; worker/batch layout does not change it.

The generation output embeds the unchanged A/B/E base generation and its exact
file hash, plus the verified extension. Combined conduit membership is the
disjoint old and new association count, not a money sum. Occurrence joins use
`(fact_set_id, source_row_ordinal)`; committee joins use the exact reported FEC ID
within the bound evidence context. Base historical facets remain available.

## Bounded operations and replay

Use one to eight import/readback workers and batches of at most 5,000 records.
The shared batcher also caps encoded batch bytes. Referenced committee context
is capped at 100,000 IDs. No member-sized graph model or raw Parquet rescan is
required by the extension writer.

Require the actual server data volume read-only, identified by its `ENGINE`
file, and explicit free-space reserve, net filesystem-growth and encoded-document
budgets. Persist the initial storage envelope before importing. Retry retains
that initial baseline and rejects different limits. An interrupted incomplete
extension can replay its own deterministic imports; a completed extension only
reads and verifies documents. Corrupt completion or graph fields fail; replay
does not repair published evidence. A shared per-identity file lock excludes
concurrent publishers using this contract.

The retained one-shot runner uses a 4 GiB/no-swap container cap, a 2 GiB Go heap
limit, and eight CPUs. Its storage bounds are 256 GiB free reserve, 16 GiB net
filesystem growth and 4 GiB encoded documents. These are caps, not measured
database size or changed standing service allocations.

```text
legal-tender pipeline fec publish-shared-conduit-generation
  --generation BASE.json --expected-generation-sha256 SHA
  --storage-root ROOT --graph-manifest BASE_GRAPH/manifest.json
  --participants PARTICIPANTS/manifest.json --conduits OLD_CONDUITS/manifest.json
  --shared-conduits NEW_CONDUITS/manifest.json --expected-shared-conduit-id ID
  --endpoint URL --username root --password-env ARANGO_PASSWORD
  --publication-dir NEW_ROOT --lock-dir LOCKS --arango-data-dir SERVER_DATA
  --reserve-free-bytes BYTES --max-filesystem-growth-bytes BYTES
  --max-encoded-bytes BYTES --workers 8 --batch-size 2000
```

The [runner](../../scripts/run-shared-conduit-generation.sh) keeps executable,
source snapshot, input manifests, arguments and checksums. It runs a fresh
four-worker/1,000-row replay after the initial import and requires byte-identical
generation JSON. Credentials are neither arguments nor retained evidence.

## Consumer boundary

The schema is `legal-tender.funding-evidence-generation.shared-conduits.v1`.
The [path](./funding-paths.md) and [neighborhood](./funding-neighborhoods.md)
readers now accept it through `OpenQueryReader`. The
[date-window loader](./funding-window-reader.md#shared-conduit-generation-inputs)
uses that same verifier with explicit v2 input specifications. The base-only
`OpenReader` still rejects it. An original generation keeps its original
coverage; supplying extension locators alongside it fails rather than ignoring
them or silently changing its meaning.

Extended queries require the exact base generation file, shared graph manifest
and shared calculation manifest in addition to the usual base receipt locators.
The embedded base must equal its independently reopened, checksum-pinned file.
The outer identity, combined disjoint membership, extension definition, policy,
calculation and base ancestry must all agree. No current pointer is consulted.

Opening streams both complete old/new decision artifacts and the complete group
evidence. It reconstructs all expected extension payload hashes and compares them
to the immutable graph manifest. It verifies live schema, counts and completion.
This is not another all-document live graph scan. Each query verifies its
selected graph fields and lookahead against pinned source evidence. A full
live-field recheck remains the existing graph replay operation.

`shared_conduit_association` is a separate typed family. Its descriptor identifies
the extension database, collection, projection, policy and source grain. The old
`conduit_association` family remains unchanged. Default neighborhoods include the
additional family and a separate `shared_conduits` entity facet. Pagination remains
key-ordered and bound to the outer generation, entity and family; cursors cannot
cross between old and shared associations.

Shared edges return the original decision, new decision, complete group decision,
full original occurrence and full related memo occurrence. The extension's
appearance reference, original base appearance disposition, edge and committee
context receive exact selected readback. No person identity is inferred.

Use `--entry-family shared_conduit_association` for a shared path entry. The
existing selected-ledger committee chain and candidate ending rules remain
unchanged. Both generations retain the same receipt evidence. Shared links add
zero money and do not establish chronological flow or dollar attribution.

```text
legal-tender pipeline fec inspect-funding-neighborhood
  --generation EXTENDED.json --expected-generation-sha256 SHA
  --base-generation BASE.json --shared-graph-manifest EXTENSION/manifest.json
  --shared-conduits NEW_CONDUITS/manifest.json
  --storage-root ROOT --graph-manifest BASE_GRAPH/manifest.json
  --participants PARTICIPANTS/manifest.json --conduits OLD_CONDUITS/manifest.json
  --endpoint URL --username root --password-env ARANGO_PASSWORD
  --entity FEC_ID --family shared_conduit_association --limit 1
```

The same binding flags apply to `inspect-funding-paths`, the existing neighborhood
and path gates, and `validate-shared-conduit-queries`. The focused shared gate
selects witnesses from verified membership, follows pagination, compares old/new
path entry behavior and checks a committee continuation when available. The
[retained query gate](../audit/shared-conduit-queries-2026-09-14.md) records acceptance.
Its [runner](../../scripts/run-shared-conduit-query-gate.sh) requires a fresh-process
expected-ID replay, byte-identical output and unchanged input checksums.

Memory remains bounded by batches, small committee context and selected evidence,
not shared-group size. Opening verifies complete compact artifacts; each selected
shared page/path scans the v2 decision artifact and reads backing source shards.
Output limits do not bound that work. Reuse a reader for a bounded session; this
is not an interactive-latency serving contract.

A serving API, Dagster activation, current-pointer
cutover, identity resolution and terminal-dollar policy remain separate work.
