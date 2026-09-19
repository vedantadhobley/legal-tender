# Complete-cycle receipt-participant publication

Status: implemented; Go, race, static and real interruption/resume checks pass.
The complete 2024 publication, independent verification, full read-only replay
and retained checksum checks pass. Complete publication evidence is tracked in the
[cycle audit](../audit/arango-receipt-participant-cycle-2026-09-12.md).
This extends the [bounded graph contract](./arango-receipt-participants.md),
using its accepted compact appearance layout and unchanged edge/domain policies.

## Scope and identity

`publish-arango-receipt-participants` derives the complete ordinal range from
the verified participant manifest. It has no sample-size, offset, layout or
expanded-comparison flags. Require complete participant/conduit publications,
immutable Schedule A facts and the same source release's master/linkage context.
No source filtering, name matching, financial selection or terminal policy is added.

The definition uses `legal-tender.arango.receipt-participant-cycle.v1` and binds
the executable, exact inputs and entire source population. The isolated database
is `lt_receipt_cycle_<cycle>_<identity-prefix>`. Worker/batch settings, operational
paths and disk limits do not change graph identity. Sample databases and existing
committee-flow/outside-spending generations are not modified.

The final state is `verified_complete_cycle_receipt_observations`. It means the
declared receipt/conduit population and candidate-authorization context passed
their gates. It does not mean an integrated A/B/E generation, resolved people or
corporations, terminal attribution, or a completed four-cycle product.

## Checkpoints and resume

Use the existing participant source-shard boundaries. At each boundary:

1. Read the complete shard and verify its physical and canonical evidence.
2. Flush every collection buffer and wait for all import/readback workers.
3. Require full compact-source reconstruction and all-field readback.
4. Atomically replace `progress.json` with the verified prefix's counts, physical
   and source digests, encoded byte counts, conduit states and exact definition.

Full-cycle imports request `waitForSync=true` before acknowledging a batch.
The [Arango import API](https://docs.arango.ai/arangodb/stable/develop/http-api/import/)
defines this as waiting for documents to reach disk. The local checkpoint file
and its parent directory are also synced, and its bytes are read back.

A failure can leave an uncheckpointed suffix in the isolated database. The same
executable and exact inputs resume that same identity. Worker and batch settings
may change. Resume still regenerates and reads back the completed prefix, then
compares its checkpoint digests. It does **not** skip evidence verification or
rewrite checkpointed source documents. Only the uncheckpointed suffix and
unfinished small context publication can be replaced deterministically.

This saves repeated writes, not all repeated work: a late restart still pays for
prefix source reconstruction/readback. Checkpoints are not a promise of constant-
time resume. Changed, missing or extra completed evidence fails closed. Missing
checkpointed database/schema state is not recreated. A completed graph replay
only verifies; it cannot repair its data or completion marker.

Each verified shard emits its source ordinal, complete denominator, elapsed
time and read-only status. Credentials, source bodies and server error bodies
are not progress output. Checkpoint presence alone is never publication success.

## Storage admission and bounds

Require three explicit byte limits:

- `--reserve-free-bytes`: free space to preserve on Arango's actual filesystem.
- `--max-filesystem-growth-bytes`: allowed net loss of available filesystem
  bytes relative to this publication's first admission.
- `--max-encoded-bytes`: hard ceiling for the entire graph's encoded document
  payload, including context but excluding the small completion record.

Before creating a publication/database, require enough free space for the reserve
plus the complete growth allowance. Persist the initial device, capacity and
available bytes. Resumes require the same filesystem/capacity and limits; they do
not reset the growth baseline. Check the filesystem before every write batch and
checkpoint. Check the cumulative encoded budget before enqueueing each batch.

`--arango-data-dir` must expose the actual server data mount read-only, including
its `ENGINE` file. The launcher/operator must verify that it belongs to the target
endpoint. An arbitrary local directory cannot establish a remote server's free
space; the command cannot prove that deployment association by itself.

The growth guard measures shared filesystem pressure, not exact database size.
Other writers can consume the allowance; other deletions can reduce measured net
growth. The independent free-space floor still applies. This is a circuit breaker,
not a filesystem reservation or exact per-database disk quota. Already admitted
bounded requests can finish after a guard trips. Leave substantial headroom.

The existing limits remain: one producer, one queued batch, one to eight workers,
5,000 documents and 8 MiB combined payload/proof per batch, 64 KiB per document,
bounded context maps and bounded AQL queries. No full-population document map or
new service is introduced. The input merge and JSON producer remain serial.

## Publication boundary

The configured root contains `<projection-id>/progress.json` during processing.
Only after the complete conduit stream, occurrence and qualified-association
counts, every stored field, exact collection membership, source drilldown and
typed query gates pass may Go write graph completion. It reads that marker back
and checks its cardinality, then publishes `<projection-id>/manifest.json` last.

The manifest contains the exact definition and verified completion evidence.
Creation is atomic and non-overwriting. An existing equivalent manifest is reused;
a different one fails. A crash between database completion and manifest creation
requires a read-only full verification before creating the missing file. No
mutable current-generation pointer or Dagster activation is included.

Use a shared durable `--lock-dir` for all publishers targeting this server. The
lock remains host-local, not a distributed multi-host lease. Do not start this
publisher from multiple hosts without adding an explicit distributed contract.

## Operation

Use the exact source arguments from the bounded command, replacing its name with
`publish-arango-receipt-participants`, omitting all sample/layout/comparison flags,
and adding publication directory, server data directory and the three byte limits.
Keep source storage read-only; mount only publication/audit output writable.

Run the same retained executable and arguments after interruption. Observe the
explicit process exit marker plus the immutable manifest; process disappearance
or a last progress line does not establish completion. Preserve a failed attempt's
logs and checkpoint. Do not delete source artifacts or drop a database to retry.

Next: attach existing committee ancestry under exact generation readiness. The
[connected-graph plan](./connected-funding-graph.md) owns remaining integration.
