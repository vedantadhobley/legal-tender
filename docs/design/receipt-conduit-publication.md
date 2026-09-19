# Cycle-wide reported conduit associations

Status: accepted and retained for 2024, including complete publication, independent
corpus checks and full varied-layout replay. This consumes the accepted
[participant index](./receipt-participant-index.md)
and [reference topology](./receipt-reference-topology.md). It does not write
Arango edges or change existing source, financial or identity publications.

## Evidence boundary

Each non-memo occurrence with a reviewed earmark receipt role receives exactly
one disposition. All other source occurrences remain in the complete participant
index with the explicit default `not_a_non_memo_reviewed_earmark`. The two
populations must conserve the exact input cycle; the successful subset is not
the coverage denominator.

For an occurrence with reference incident evidence, reuse the unchanged
[`earmarkassociation.Decide`](../../internal/calculation/fec/earmarkassociation/policy.go)
policy. Join its sole peer, when present, and supply both endpoints' unsafe flags,
exact neighbor counts, memo/entity roles, raw/clean contributor IDs, dedicated
conduit IDs and exact nullable amounts. Check recipient equality and reciprocal
sole-peer topology. No name, memo substring, transaction suffix, amount threshold
or named-entity exception determines a link.

The policy distinguishes qualified reported associations, invalid incidents,
multiple/shared related records, unsupported roles and conflicting/insufficient
ID evidence. Amount equality, difference and unknown amounts remain a separate
axis. A qualified association preserves the reported conduit ID and both exact
source ordinals. It does not resolve a person or organization, certify a cash
payment, select an amendment, infer fees or allocate terminal money.

An absent sparse endpoint receives
`no_reference_incident_transaction_uniqueness_unassessed`. It must not receive
the bounded reviewer's stronger duplicate-specific or unique-key absence claim:
the parent topology has complete incident coverage but does not inventory every
unreferenced transaction-key duplicate. This new absence label changes no
positive association rule and requires no reference re-fetch.

Every result sets additional money to zero, financial eligibility to false and
conduit identity resolution to false. These are association annotations, not a
second contribution ledger. The complete original and memo facts remain available
through their participant/source identities.

## Exact input and publication contract

Require `manifest.json` paths and expected calculation IDs for both inputs.
Hash and validate the same manifest bytes, including physical descriptors, and
require identical fact-set identity, fact-manifest digest, cycle and row count.
The participant input must be a complete publication, never a benchmark.
Every endpoint and participant shard is read to EOF and checked against its
published physical/value hashes and census before success.

The output pins both parent IDs and manifest hashes, the original fact ancestry,
executable and both publication/association policies. Its identity includes the
source-ordered decision value digest, complete populations and all outcome counts.
Worker count, run size, fan-in, physical temporary names and operational timings
do not change that logical identity. Different executable bytes do change it.

The new directory contains an immutable success manifest and one source-ordered
decision stream. Metadata is capped at 1 MiB and synced before exclusive atomic
publication. Failures retain remaining workspace but publish no success manifest.
No current pointer, weekly schedule, Dagster asset or production graph is changed.

## Bounded execution

1. Stream and verify the sparse endpoint artifact. Split it on the existing
   participant shard boundaries; count exact and unsafe populations again.
2. Independent shard workers read every participant and its endpoint stream.
   Emit immediate dispositions where no sole-peer lookup is needed. Otherwise
   write bounded, source-backed requests ordered by peer ordinal.
3. Merge requests with bounded fan-in. Independent groups merge concurrently;
   the final merge remains a streaming pass. Split by target shard without
   buffering an equal-key group.
4. Independent target-shard workers merge requests with the participant and
   topology streams. Call the shared policy and sort resulting decisions by
   original source ordinal. A shared memo can have many requests without a
   report-sized map or slice.
5. Merge all decisions. Re-read every participant to prove exact applicable
   membership, uniqueness, scope and disposition conservation. Verify all output
   hashes and state/amount counts before creating the success manifest.

Use one to eight shard workers, at most 100,000 records/32 MiB per sort run and
merge fan-in two to sixteen. Existing verified external-sort storage enforces
one shared write-time cap across all workers and intermediate copies: 8 GiB by
default, at most 32 GiB. It deletes only workspace-owned temporary inputs after
their replacements pass full readback. Input artifacts are never removed.

There is no cycle-wide participant map, whole-report materialization or repeated
bounded report review. The job reads compact participant/topology publications,
not the original Schedule A Parquet or a fresh source download. It does retain
serial ordered-stream split/final-merge passes; eight workers is not a claim
that all stages use eight cores or scale linearly.

## Compact representation

Requests preserve original ordinal, exact nullable role/ID fields, nullable signed
cents and invalid-incident flags, keyed by peer ordinal. Nullable strings use a
16-bit byte length with a separate null sentinel, preserving null and empty.
The participant field-size contract remains enforced.

Decisions use the existing checked zstd stream: eight-byte big-endian source
ordinal key, zero tag, and matching record ordinal. Payload: eight-byte sole-peer
ordinal, one outcome byte, one amount-comparison byte, and the exact nine-byte
reported committee ID only for qualified associations. Zero sole-peer ordinal
means no sole peer, not proof of zero neighbors. Outcome dictionaries are wire
codes, not new FEC interpretation rules; their order is pinned by v1.

No reported amount is stored in this annotation stream. The exact original and
memo amounts remain separate values in the participant/source records.

## Command and next boundary

```text
legal-tender pipeline fec publish-receipt-conduit-associations
  --participant-manifest PARTICIPANTS/manifest.json --expected-participant-id ID
  --topology-manifest TOPOLOGY/manifest.json --expected-topology-id ID
  --output-dir NEW --workers 8 --run-rows 100000 --merge-fan-in 8
  --max-workspace-bytes 8589934592
```

Run offline with read-only inputs. The corpus runner reuses the temporary
eight-CPU/4 GiB container cap and `GOMEMLIMIT=2GiB`; no standing budget changes.
The [dated gate](../audit/receipt-conduit-publication-2026-09-12.md) owns measured
counts, resource usage, independent checks, replay and retention status.

The [full-cycle Arango publication](./arango-receipt-participant-cycle.md) and
[typed generation](./funding-evidence-generation.md) now include these accepted
associations. The [shared-reference profile](./shared-reference-profile.md)
examines the population rejected at the shared-degree check without changing
this policy or the graph. Missing identities and unsupported association shapes
remain explicit; complete occurrence coverage is not complete interpretation.

The separate [v2 group publication](./shared-reference-group-rule.md#publication-boundary)
preserves this v1 baseline and upgrades only supported complete shared groups.
Its [isolated graph extension](./shared-conduit-generation.md) does not replace
the v1 graph or silently change existing query consumers.
