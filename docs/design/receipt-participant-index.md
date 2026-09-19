# Source-grain receipt participant index

Status: Go publisher and read-only consumer accepted for the complete 2024 input.
The [retained-data gate](../audit/receipt-participant-index-2026-09-12.md) passes
full readback, artifact replay and source inspection. This implements the
appearance/disposition part of the
[participant contract](./receipt-participant-publication.md), not Arango graph
publication or cycle-wide memo-conduit qualification.

## Identity and evidence

Every occurrence in the exact input fact set gets one `reported_contributor`
appearance. Its identity is SHA-256 of the role, hexadecimal fact-set identity
and one-based ordinal, with explicit delimiters and a big-endian ordinal.
Identical-looking source occurrences remain distinct. Empty descriptions,
missing recipients, memo rows, unknown amounts, negative adjustments and zeroes
remain represented. There is no enrichment or monetary threshold.

The immutable Schedule A fact owns the complete raw record. The access index
stores its ordinal and binds the exact source manifest and source-shard digest.
`inspect-receipt-participant` verifies those links, reads the complete original
fact through the existing reader and checks every indexed value against it.
Names, addresses, employer/occupation text, dates and full memo text are not
copied into the index; they remain in the complete fact. An index row is neither
a resolved person nor an effective payment.

## Shared policy, separate responsibilities

The publisher uses the existing [inventory](./committee-funding-basis.md) and
[source-role policy](./receipt-source-evidence.md). It does not fork receipt,
committee, overlap or conduit rules. `SourceEvidenceRow` is a typed adapter for
those same functions. Existing bounded reviews and calculations retain their
versions and behavior.

Each row preserves the source route, both membership decisions, inventory
component, receipt role, individual/committee overlap, entity conflict, earmark
state, structured-conduit state and raw-reference annotation. Raw contributor,
clean contributor and conduit IDs stay separate. Valid structured IDs remain
identity-unverified observations. Raw reference presence is not a resolved pair.

The manifest explicitly sets contributor identity resolution, reference-conduit
qualification and financial eligibility to false. Additional conduit amount is
`"0"`, meaning this annotation adds no money, not that fees were absent. No
terminal or allocation policy is selected.

The [endpoint topology](./receipt-reference-topology.md) is a separate exact-input
artifact. The separate [conduit publisher](./receipt-conduit-publication.md) now
joins those inputs without another raw-source pass. It keeps absent sparse
endpoints distinct from proven transaction-key uniqueness; the participant
publication itself remains unchanged.

## Physical contract v1

`legal-tender.fec.receipt-participants.v1` stores 24 columns in source order.
Their Go field definitions and Parquet names live in
[`types.go`](../../internal/calculation/fec/receiptparticipants/types.go).
The categories are occurrence locator; recipient and raw role IDs; shared-policy
decisions; source flags; exact nullable signed cents and amount state. It retains
one output row for every input row, not one row per resolved identity.

Each existing source shard produces one zstd Parquet index shard. The fixed
layout uses 32,768-row groups and 8,192-row buffers. State strings use dictionary
encoding. The source reader selects 19 named fields only after verifying the
entire source file digest, exact 99-column schema and row count. The shared
`narrowparquet` adapter uses the previously checked reference-reader mechanism;
it does not hardcode physical column positions or weaken source validation.

Every output shard is synced and completely reread before acceptance. Its
descriptor pins source identity, ordinal range, physical bytes/hash and canonical
value hash. Canonical rows use declaration order, big-endian 64-bit integers,
length-framed strings and 0/1 null/boolean tags. Signed amounts retain their
two's-complement bits. Null and empty strings are distinct. Hashing reuses one
buffer rather than serializing each row to JSON.

The manifest pins executable, policies, source manifest, all shard descriptors
and four conserving state censuses. Its calculation identity excludes elapsed
time, worker count and peak RSS; identical source/layout/build output therefore
has the same identity across worker counts. Build or physical-layout changes may
change that identity even when appearance identities and logical values do not.

## Resource and publication boundaries

- One to eight workers own independent source-to-write-to-readback shard jobs.
  There is no single row dispatcher or cycle-wide participant map.
- One shared write-time data cap covers all workers and partial files: 8 GiB by
  default, at most 32 GiB. A separate 1 MiB manifest reserve is checked before
  starting. The output directory must be new.
- Source role/identifier fields over 4,096 bytes, or selected descriptive text
  over 1 MiB, fail this access contract without truncating or rewriting the source.
- Only dense, valid, immutable Schedule A fact publications are accepted by v1.
  Unsupported source shapes fail the job; they do not silently drop occurrences.
- Cancellation, source mismatch, cap exhaustion or readback disagreement leave
  no success manifest. Partial output remains failure evidence. There is no
  checkpoint resume, mutable current pointer or automatic cleanup.
- Successful publication creates a synced manifest with an exclusive atomic
  link. Inspection requires its expected calculation ID and fails on changed
  ancestry, policy metadata, scope, counts or shard bytes.

The temporary corpus runner uses eight CPUs, a 4 GiB container cap and a 2 GiB Go
memory limit. This is not a new service or a change to standing memory budgets.

## Commands

All commands are under `legal-tender pipeline fec`:

```text
publish-receipt-participants --storage-root ROOT --schedule-a-facts EXACT.json
  --cycle CYCLE --output-dir NEW --workers 8 --max-output-bytes 8589934592

benchmark-receipt-participants --storage-root ROOT --schedule-a-facts EXACT.json
  --cycle CYCLE --output-dir NEW --shard-indices 0,33,66,99,132,165,198,231
  --workers 8 --max-output-bytes 536870912

inspect-receipt-participant --storage-root ROOT --schedule-a-facts EXACT.json
  --participant-manifest PATH/manifest.json --expected-participant-id ID
  --source-row-ordinal ORDINAL
```

The benchmark accepts at most eight whole shards of at most one million rows
each. Its state always says selected shards, never a complete cycle, even for a
small fixture. Sample indices are test inputs, not production selection rules.

The read-only `Inspector.Scan` now verifies all compact shards in parallel and
checks the complete merged census. The [boundary role profiler](./terminal-receipt-roles.md)
uses it without changing this publication or any source-role decision. It accepts
only a complete-cycle publication, not a sample; a callback failure or failed
readback cannot yield an accepted partial population.

Inspection currently scans one compact output shard and opens its full source
record. It is a correctness/drilldown boundary, not an optimized interactive query
service. Arango layout, import/query costs, qualified memo-conduit associations
and attachment to committee/candidate ancestry remain the next publication work.
