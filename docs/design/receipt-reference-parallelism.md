# Bounded parallel receipt-reference processing

Status: implemented; fixture, race, synthetic and real eight-worker equivalence
gates pass. The verified result is durably retained. The existing
[serial corpus gate](../audit/receipt-reference-join-2026-09-11.md) passes independently.
The [parallel audit](../audit/receipt-reference-parallelism-2026-09-11.md) records
measurements and exact build/input identities.

## Why this change

The original implementation parallelized source readers but serialized sorting,
classification and output verification. Increasing a container's CPU quota alone
does not parallelize that code. The complete serial-join gate took 46m 20s with
four source readers and a four-CPU quota; the new run took 22m 19s with four source
readers, eight report workers and an eight-CPU quota. This is an observed
end-to-end implementation improvement, not controlled one-CPU/eight-CPU scaling.
The earlier bounded sort-run benchmark had not measured the complete global join.

Use bounded parallel work for independent reports. Keep source scope, reference
policy, occurrence identity, complete duplicate membership and readback unchanged.
This is an execution improvement, not a new financial or identity policy.

## Partition boundary and equivalence

An exact `(recipient, file_num)` tuple determines the worker. Length-framed nullable
strings distinguish null, empty and nonempty values. FNV-1a chooses a bucket only;
it never establishes equality or identity. Colliding hashes place different
reports in the same worker, where the unchanged full keys still distinguish them.

Every source row participates in both scans and routes identically. Each report's
own transaction keys, requested targets, unreferenced duplicate counterexamples,
source decisions and reverse incidences remain together. No reference can become
unique because its other occurrences went to a different worker. This property
lets each worker reuse the existing engine without copying its semantic rules.

The fixed total Bloom-filter budget is divided among workers, not multiplied.
Partition skew can change filter-only extra candidates, which are not logical
decisions. Requested keys retain complete membership. The lookup artifact is still
a candidate superset, not a whole-cycle transaction census.

Each worker executes sorting, merging, reference classification and neighbor
aggregation independently. One final ordered merge for each artifact restores
the original global stream format. The four artifact families assemble concurrently
because their input files are disjoint. Their ordering and hashes remain independent
of worker scheduling or physical filenames.

Logical identity still binds source, executable, policy and canonical decision,
incidence and neighbor digests. It excludes execution geometry and filter-only
extra members. A new executable has a new calculation identity even when every
decision is unchanged. Compare canonical evidence across builds; compare logical
identities only when the executable and other identity inputs are equal.

## Resource and failure controls

- `--workers` selects 1–8 processing workers; default eight. Source reader count
  is independently bounded to 1–8, also defaulting to eight.
- Keep the 4 GiB container cap and `GOMEMLIMIT=2GiB`; the measured run uses an
  eight-CPU quota and `GOMAXPROCS=8`. Do not auto-size against host RAM or occupy
  every host core by default on this shared node.
- Each worker retains bounded sort runs. Effective per-worker merge fan-in is
  `min(requested_fan_in, floor(32/workers))`: at most 32 simultaneous merge-input
  decoders. Final assembly also has at most 32 input decoders. Existing decoder,
  encoded-record and source-field limits remain.
- One shared workspace owns filenames, verified descriptors and physical-byte
  accounting. Mutex-protected writes enforce the same aggregate cap across
  concurrent writers, including partial files; workers do not each receive a
  separate 16 GiB allowance. Sorting and compression run outside that lock.
- A borrowed source batch is not released until every receiving worker acknowledges
  it, including cancellation. Queues and reused partition buffers are bounded.
- On failure, cancel siblings and drain their acknowledgments. Retain failed
  workspace files; publish no completion manifest. Remove only workspace-owned
  inputs after their replacement passes full readback and conservation.

The largest report still need not fit in RAM. A disproportionately large report
can occupy one worker longer than others; record per-partition counts and stage
times rather than assuming linear speedup. Per-stream ordered assembly and the
source dispatcher remain serial within each stream; this is not a claim that
every stage uses eight cores continuously.

In the accepted real run, the two source passes took 13m 45s, about 62% of total
wall time. Each pass includes decoding, single-dispatcher partitioning, worker
ingestion and backpressure; this does not measure disk time alone. The subsequent
[bounded real-source profile](../audit/receipt-source-scan-profile-2026-09-12.md)
measures 1.4–1.5× faster sampled ingestion with eight readers under the same CPU
quota. It also identifies generic Parquet row-assembly/copy overhead and batch
acknowledgment waits. The subsequent [narrow-reader implementation](../audit/receipt-narrow-reader-2026-09-12.md)
now passes the complete selected-field sample comparison and reduces sampled
ingestion to about nine seconds from twenty-two. Its full-cycle gate passes:
15m 17s versus 22m 19s, with identical output evidence and retained verification.
The higher reader limit and new row-construction path do not change source or
reference semantics; dispatcher redesign remains separate work.

## Narrow source projection

The full 99-column physical schema and opened-file checksum remain mandatory.
Only temporary row construction changes: the library's converted column chunks
feed `NewRowGroupRowReader` directly, then its typed reconstructor produces the
ten-field access row. The source-width conversion path no longer constructs
placeholders for the other columns. All selected source types, optionality and
definition levels must match; nested/repeated sources are rejected. No new source
format, decoder, per-entity rule or loss of retained fact fields is involved.

The previous generic reader lives only in differential tests. Compare every
selected sample value, then complete reference artifacts, before accepting a new
build. A new executable has a new calculation identity even when evidence matches.

## Verification and operation

Tests compare 1, 2, 4 and 8 workers with the original serial engine, including
duplicate targets/sources, missing references, reciprocal links, fan-out, empty
partitions, forced filter false positives and invalid scope. They require identical
canonical decisions, incidence/neighbor digests and same-build logical identity.
A synchronization barrier proves actual concurrent dispatch. Race tests also cover
buffer reuse, cancellation, shared file ownership and concurrent byte-cap failure.

The million-row synthetic benchmark includes both generated-input passes, all
sort/merge/join stages, final assembly and readback. It does not include Parquet
decoding and does not establish full-corpus latency. The real gate must compare
all canonical artifacts and state counts against the retained serial baseline,
then verify all output streams and source witnesses independently of the run.

```bash
legal-tender pipeline fec join-receipt-references \
  --storage-root /storage --schedule-a-facts <immutable-manifest> \
  --cycle <cycle> --output-dir <new-directory> \
  --workers 8 --scan-workers 8 --run-rows 100000 --merge-fan-in 8
```

The manifest records worker count, effective fan-in, per-partition source/reference
counts and stage durations. Logs report scan progress and each worker/assembly
stage. Source facts, financial predicates, Arango graphs and Dagster are unchanged.
