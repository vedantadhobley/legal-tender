# Complete-cycle receipt reference join

Status: implemented in Go; the [serial full 2024 corpus gate](../audit/receipt-reference-join-2026-09-11.md)
passes. The [parallel execution](./receipt-reference-parallelism.md) is implemented
and its real equivalence/source gate passes with retained results. This extends
the [participant publication contract](./receipt-participant-publication.md), not
the legacy donor model. It establishes report-reference connectivity and exact
lookup multiplicity. It does not publish contributor vertices, resolve people,
qualify conduit associations or select financial records.

## Input and scope

Require an exact immutable Schedule A manifest for one complete, dense, valid
cycle. Reject mutable current-pointer paths. Verify its backing and every opened
shard, physical schema, occurrence range, cycle and normalization state.
Every occurrence participates in both source scans, regardless of receipt role,
memo status, amount, contributor threshold or whether it carries a reference.

The exact lookup key is `(recipient, file_num, transaction_id)`, within the pinned
fact set and cycle. Keys are length-framed; null, empty and nonempty strings remain
different. No trimming, numeric coercion, name match or transaction-ID suffix
rule creates a key. Positive lookups require the same valid report scope as the
bounded reviewer. Invalid-scope reference rows remain explicit decisions.

## Bounded execution

1. Scan all occurrences for reference requests. Retain each row with a nonempty
   back-reference or back-schedule field. Seed a fixed-size candidate filter with
   its source and target transaction keys when the scope permits a lookup.
2. Scan all occurrences again. Add each filter-positive transaction occurrence
   to an external sort, including unreferenced rows and repeated transaction IDs.
   The filter has no false negatives for inserted keys. False positives only add
   work; exact framed keys determine membership and joins. This is not a donor
   or financial exclusion rule.
3. Merge members before queries for each exact key. Count every member, retaining
   all member ordinals in the lookup evidence. Send multiplicity and the first
   member's schedule/line back to each requesting source ordinal. A first member
   is not a unique target unless the count is one.
4. Join each original request with its source and target lookup results. Apply
   the shared reference policy and write one sparse decision per request, sorted
   by source ordinal. All other input occurrences have the explicit default
   `no_report_reference`; source plus sparse-state counts conserve the full cycle.
5. Publish exact-reference incidences in both directions and distinct-peer counts.
   Reciprocal references produce one peer per endpoint, while shared targets
   remain visible as multiple peers. These are reference connections, not money
   transfers or independent evidence of conduit eligibility.

The filter is a bounded Bloom filter using framed keys and SHA-256-derived bit
positions. Neither its hash nor its false-positive rate resolves identity.
The lookup artifact includes filter-positive extra members and is not a complete
transaction-ID census. It is complete for the explicitly requested keys.

Source readers are bounded to eight. A bounded dispatcher assigns complete report
scopes to 1–8 processing workers; each reader retains its batch until every receiving
worker acknowledges it, including cancellation. Queues and buffer ownership remain
bounded. The total filter and physical workspace budgets are shared, not multiplied.
External sorts bound runs to 100,000 records and 32 MiB of encoded data, merge
fan-in to 16, and encoded records to 64 KiB. Source identifier fields above
4,096 bytes fail rather than truncate. No entire report or equal-key group must
fit in memory.

## Shared semantics and unresolved evidence

The pure `reportreference.Decide` policy is used by both this join and the
[bounded report reviewer](./receipt-report-association.md). It preserves that
reviewer's precedence for missing source IDs, duplicate source keys, ambiguous
targets, missing schedules, absent targets, self-references and schedule mismatches.
The cycle consumer adds `invalid_report_scope` for rows that the bounded reviewer
cannot select. It does not change the existing conduit-role rules.

An absent target is absent only from the pinned cycle/report population, not
from every historical filing. A reported reference can span financial components
other than conduits. Exact-reference degree alone does not establish that an
endpoint has no invalid or ambiguous incident evidence: those requests and target
members remain in the sparse decisions and lookup artifact. The conduit consumer
must check them along with roles, IDs and memo evidence before qualifying a pair.
The [endpoint topology consumer](./receipt-reference-topology.md) now performs
that invalid-incident propagation; source-role qualification remains separate.
`conduit_eligibility_evaluated` and `financial_eligibility` remain false here.

Multiplicity is null when no lookup was evaluated, and zero only for an evaluated
empty target. A source occurrence cannot be absent from its own evaluated key;
that condition fails the job. Duplicate keys never become unique by choosing the
first member or dropping identical occurrences.

## Artifacts and reproducibility

The new isolated output directory contains a `data/` workspace and an atomically
created `manifest.json` only after all stages pass. Retain four checked artifacts:
lookup evidence, sparse decisions, exact incidences and distinct-peer evidence.
Each compressed stream binds counts, physical SHA-256 and decoded-stream SHA-256.
Readers enforce ordering, length limits, checksum and EOF conservation.

The current stream format is zstd over records containing big-endian key length
(4 bytes), payload length (4), tag (1), ordinal (8), then the exact key and JSON
payload bytes. The calculation version pins these meanings; this is an internal
join artifact, not a replacement for source Parquet or an investigative API.

The logical calculation identity binds source identities, executable, cycle,
policy, scope, state counts and decision/incidence/neighbor value digests.
Run geometry, reader/processing worker counts, filter false positives, physical filenames and runtime
measurements do not change logical decisions. The manifest records those physical
details separately. Retained source facts provide every omitted source field.

The write-time workspace cap includes retained, temporary and partial run files.
Merge inputs are removed only after verified output and count conservation; only
files owned by that new workspace can be removed. Failed attempts retain their
remaining files and have no completion manifest. This version restarts from
retained source evidence rather than resuming an unfinished attempt.

## Run and gates

```bash
legal-tender pipeline fec join-receipt-references \
  --storage-root /storage --schedule-a-facts <immutable-manifest> \
  --cycle <cycle> --output-dir <new-directory> \
  --workers 8 --scan-workers 8 --run-rows 100000 --merge-fan-in 8 \
  --filter-bytes 67108864 --max-workspace-bytes 17179869184
```

No partial-cycle option exists. Run in a memory-capped container with source
storage read-only and adequate disk headroom. JSON goes to stdout; progress goes
to stderr. No source pointer, existing calculation, Arango database, Dagster asset
or scheduled production release is changed.

Fixture gates cover cross-run duplicates, unreferenced duplicate targets,
duplicate sources, missing/invalid scope, reciprocal references, fan-out, empty
populations, forced filter false positives, reordered input, run-size invariance,
byte-cap failure, corruption, cancellation and source-buffer ownership. Existing
bounded-review fixtures must remain unchanged. Shuffled-fixture comparisons also
check membership and distinct peers against direct in-memory oracles, independent
of the production key framing, filter and sort. The real gate must additionally
conserve the complete cycle and verify replay, source witnesses and resource use.

Progress now includes each report worker and final artifact-assembly stage, with
partition counts and timings in the manifest. The parallel audit separates
synthetic pipeline throughput from full-source timing and report-skew limits.

Next, qualify conduit associations using this complete lookup evidence and the
shared role rules, then publish source-backed contributor connections. Do not
equate a successful reference join with complete original-product graph coverage.
