# Bounded real-source scan profile — 2026-09-12

Status: completed. The test-only profile identifies a measured source-reader
limit and substantial Parquet row-assembly overhead. Production behavior remains
unchanged. This follows the [full-cycle parallel gate](./receipt-reference-parallelism-2026-09-11.md),
whose source passes consumed 62% of total wall time.

## Scope and method

The [opt-in Go harness](../../internal/calculation/fec/receiptreferences/source_profile_test.go)
reads eight evenly spaced complete shards from the accepted 2024 fact set:
indices 0, 33, 66, 99, 132, 165, 198 and 231, totaling eight million rows.
Selection depends on source position, not donor, amount, role or candidate.
Every pass uses the unchanged production reader to check each selected shard's
bytes, digest, physical schema, row ordinals, cycle and valid normalization.
It pins the accepted manifest digest without rehashing unselected backing files.

```text
fact_set_id = 8f69a2ef40316f62d8844ecf732342a3b4535136b434f60677d0608cb04465df
manifest    = b664e752a3bae186508a45bb6d09289ba2fe06f0b4149ecbd72063e7ef79b829
test_binary = f5178c8840c63bb1b708266f8483880cc31500070232bae6cb70cca0cdb78d7b
source_tgz  = 9e4bbbf434a2652e74fb62f8effce079083c6769114a0a514d229a57a3674123
application = 710d0536b9254c0187a7dcc79bc332c897b8095a6ab5499ea1ff92df1c4222d3
```

Every case uses a fresh process with an eight-CPU quota, 4 GiB container limit,
`GOMEMLIMIT=2GiB`, `GOMAXPROCS=8`, no network and read-only source storage.
The reader count is the experimental variable; processing workers remain eight.
Production still permits at most four source readers. The diagnostic can exercise
eight through the existing internal scan function without changing the command.

Three modes separate work:

1. **Decode:** production reading and validation, with a counting consumer.
2. **Dispatch:** the same scan, report-bucket dispatcher and eight counting workers.
3. **Ingest:** both actual request/filter and membership/sort passes, using the
   existing engine, 64 MiB total filter, 100k-record runs and 2 GiB shared scratch cap.

Ingestion includes naturally flushed runs and their readback, but not final tail
flushes, global joins, decisions or graph publication. A shard sample can split
reports; no uniqueness, absence, conduit or financial result is derived from it.

## Measurements

Seconds below exclude setup and the separate instrumented profile invocation.
The two ingestion configurations were repeated in reverse order. These are
bounded observations on a shared host, not statistical confidence intervals.

| Work | Four readers | Eight readers |
|---|---:|---:|
| Decode and validate one pass | 8.031 s | 4.448 s |
| Decode plus dispatch/count one pass | 8.278 s | 4.233 s |
| Ingest both passes, first run | 21.926 s | 14.684 s |
| Ingest both passes, reverse-order repeat | 21.032 s | 14.874 s |
| Average active CPUs during first ingestion | 3.43 | 5.75 |
| Peak process RSS, first ingestion | 1.180 GiB | 1.497 GiB |

Eight readers improve the sampled ingestion stages by about 1.4–1.5× under the
same CPU quota. Decode-only work improves by about 1.8×. The dispatch/count
eight-reader run being slightly faster than decode-only is measurement variation,
not a claim that adding dispatch reduces decoding work.

All runs conserve eight million source and visitor rows per pass. All five
ingestion invocations, including the profiled case, retain 1,141,542 reference
requests and 1,629,730 candidate lookup members. Candidate membership here comes
from a sample-seeded filter, not the complete-cycle filter population.

`/proc/self/io` charged zero or 4096 read bytes per pass while logical reads were
hundreds of megabytes. These were warm-cache source reads; physical source-disk
throughput was not limiting this sample. This is not evidence about cold storage,
full-cycle cache pressure or other hosts. Scratch sorting/writes remain included.

## CPU and waiting evidence

The separate four-reader ingestion profile collected 73.67 sampled CPU-seconds
over 21.49 wall-seconds. Its largest flat CPU costs were typed slice copying
(26.08%), Parquet row assembly (21.07%), row-range traversal (11.90%) and memory
copying (6.85%). Those are CPU samples, not additive wall-time percentages.

Inspection of the pinned `parquet-go v0.32.0` code explains a concrete source of
that work: `NewGenericReader[Row]` converts the 99-column physical schema to the
ten-field access schema through `ConvertRowGroup`. Unselected columns do not
load their source pages, but the masked row group still has the source-width
column list. It generates placeholder values, assembles rows and then converts
them down to the selected fields. This is avoidable-work evidence to investigate,
not proof that a replacement reader is already correct or faster.

There is also real synchronization: the single source consumer partitions one
batch, then waits for all its receiving workers before taking the next batch.
In the profiled run, source readers spent 20.69 aggregate seconds waiting for
consumer acknowledgments; the dispatcher spent 8.19 seconds waiting on its
worker acknowledgments. These overlap with other work. The much larger total
block-profile time includes idle workers and the test harness; do not call it
recoverable elapsed time. The mutex profile showed only about 21 ms of sampled
delay, so the shared workspace mutex is not a measured dominant cause here.

The evidence does not justify blaming the dispatcher alone or adding more join
workers first. The four-reader limit underuses the available CPU budget, and
row reconstruction spends substantial CPU on the generic projection path.

## Next implementation

1. Permit a separately bounded eight-reader configuration, preserving the shared
   process/workspace budgets and source-buffer ownership checks. The sample is
   encouraging; a full-source equivalence/resource gate still accepts deployment.
2. Benchmark a genuinely narrow-column reader against the unchanged reader on
   exactly these sources. Compare every selected value, including null versus
   empty, ordinal and signed/type states; keep full shard/schema verification.
   Reducing temporary row work must not remove any of the 99 retained fact fields.
3. Re-measure before introducing a concurrent dispatcher or deeper queues. If
   acknowledgment barriers remain limiting, overlap independent source batches
   with explicit ownership, cancellation, memory bounds and deterministic output.

Do not multiply these sample speedups into a promised full-cycle runtime. The
sample has less filter occupancy, incomplete reports and a different reference
mix. Contributor graph publication remains the product milestone, and no new
Arango edges or terminal-dollar policy is part of this profile.

## Reproduction and retention

The audit tree is retained at:

```text
/storage/dumps/audits/fec/receipt-source-scan-profile/2026-09-12/attempt-01/
```

`RUN.md` and `run.sh` record exact commands, budgets and all diagnostic modes.
Per-case logs, exit markers and `metrics.json` files preserve results. CPU,
blocking and mutex profiles include their text summaries. `suite.exit=0` is the
explicit completion marker. The original working directory remains at
`/tmp/legal-tender-source-profile.NUVKXZ/`.

The application rebuilt byte-identically to the accepted full-cycle executable;
only a test harness was added. The complete Go suite, targeted race tests and vet
pass (`tests.exit=0`; individual command outputs are retained).
Checksum retention verification is recorded alongside the copied evidence.

Follow-up: the [narrow-reader implementation](./receipt-narrow-reader-2026-09-12.md)
now addresses these measured costs. This profile's older executable and evidence
remain unchanged; use the follow-up for the new full-cycle gate status.
