# Receipt reference sort-run benchmark — 2026-09-11

Status: passed as a bounded physical-layout benchmark. The
[participant publication contract](../design/receipt-participant-publication.md)
owns the next index and graph boundary. No donor identities, references,
financial records or new Arango connections were resolved or published.

## Inputs and scope

The command used existing published 2024 Schedule A facts, not a new download:

```text
fact_set_id = 8f69a2ef40316f62d8844ecf732342a3b4535136b434f60677d0608cb04465df
executable  = d3a462b9544592c053a4861b503bd0f4fdd74452278f6814af596245d8db4aa2
source_tgz  = c7429048dd72b899c8389daa4e0319c0a96d734aac138edb4912ed3eee5a7d4a
```

Explicit first, middle and final shard samples cover 2,085,606 distinct
occurrences. The middle sample was replayed with the same executable and
parameters. Each invocation verified the full publication backing, then the
selected shard's bytes, complete physical schema and typed occurrence metadata.
Each layout has every sampled occurrence, including memo and unknown-amount rows;
there is no financial or donor selection predicate.

The samples were selected by shard position, not candidate, committee, donor,
amount or expected conclusion. They are not a statistical sample, complete report
population or full-cycle index. The fact set retains its older release ancestry;
the active v4 source release does not silently replace that lineage.

## Measurements

Both candidates contain the same 19 access fields, written in runs of at most
100,000 rows. Each run has complete projected-value readback before its input
buffer is released. Source-order runs keep ordinal order. Report-key runs order
the same occurrences by exact recipient, file number, transaction ID and ordinal.

| Shard | Rows | Source-order bytes | Report-order bytes | Scan/write/readback seconds | Peak process RSS bytes |
|---|---:|---:|---:|---:|---:|
| First, 0 | 1,000,000 | 8,766,555 | 7,930,617 | 10.679 | 279,265,280 |
| Middle, 132 | 1,000,000 | 12,708,072 | 12,558,774 | 9.131 | 306,589,696 |
| Final, 264 | 85,606 | 565,291 | 620,673 | 0.916 | 251,822,080 |
| Middle replay | 1,000,000 | 12,708,072 | 12,558,774 | 9.269 | 290,361,344 |

Full source verification took 8.044–10.152 seconds for the three samples;
replay verification took 8.022 seconds. Sorting took 0.682, 0.545 and 0.045
seconds respectively, within the scan/write/readback measurements. These warm
local runs used four CPUs, a 4 GiB container limit and `GOMEMLIMIT=2GiB`.
RSS excludes some container-charged cache; the container limit remains the
operational guard. These are not whole-cycle or weekly-refresh latency claims.

Sorting reduced compressed size in the first two samples and increased it in
the final sample. Its purpose is to support exact external reference joins,
not to guarantee smaller storage. Neither layout has passed a whole-cycle
publication, global merge, point-query or reference-resolution gate.

## Verification

- Every written value matched its bounded input row, including null versus empty,
  false versus null, signed amounts and duplicate transaction-ID occurrences.
- A separate Go corpus test used the full physical 99-column reader and an
  explicit independent field map to compare all 19 access fields for every
  sampled occurrence. It did not call the benchmark's projection, comparator or
  readback helper. This is a mapping check using the same pinned Parquet library,
  not an independent decoder.
- Stored layout bytes and SHA-256 digests, counts, ordinal ranges, byte-budget
  accounting and evidence IDs passed independent readback.
- The middle replay reproduced every artifact digest and the exact evidence ID:
  `8219915848125dd23931d0262e48f2dfa3bb9ac5e13c34ebe1951ec772258264`.
  Timings and RSS vary and are outside that identity.
- Fixture tests cover null/empty values, signed and large integer amounts,
  duplicate IDs, copied source-buffer ownership, invalid typed metadata,
  deterministic output, no-overwrite behavior, cancellation and byte-cap failure.
- The complete Go suite, `go vet`, and targeted race tests passed. The optional
  corpus test was separately executed with retained inputs and passed; it skips
  only when those inputs are not configured.

## Retention and next step

The executable, source archive, test executable, both layouts, JSON results,
logs and zero exit markers are retained under:

```text
/storage/dumps/audits/fec/receipt-reference-index/2026-09-11/attempt-01/
```

The retained tree occupied 121,299,016 bytes at capture. The accepted Schedule A
current pointer still matched the exact immutable input manifest. No source
archive, graph, calculation publication or Dagster asset was changed.

Next, implement bounded external merge and cycle-wide reference joins with
cross-run uniqueness and reverse-reference checks. Do not turn a sample's absent
target into a negative association, hold the largest report in memory, or create
per-candidate copies of this index. Participant publication and connected graph
acceptance follow that complete-cycle gate.
