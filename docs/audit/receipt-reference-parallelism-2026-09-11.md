# Parallel reference join — 2026-09-11

Status: implementation, synthetic and real 2024 equivalence/source gates pass.
The complete verified run is durably retained.
The [execution contract](../design/receipt-reference-parallelism.md) owns resource,
partition and equivalence requirements. Do not infer real speedup from the synthetic
benchmark alone or treat this as contributor graph publication.

## Inputs and code

```text
fact_set_id = 8f69a2ef40316f62d8844ecf732342a3b4535136b434f60677d0608cb04465df
executable  = 710d0536b9254c0187a7dcc79bc332c897b8095a6ab5499ea1ff92df1c4222d3
source_tgz  = 51d600c45164ba9d8536557063a4c92c1f249208ce610747633de963f077ea78
test_binary = de3d7b66958fd83cfd352457c3e14c9aac73d4d27a1963b768f81bc810c49c06
```

The retained serial baseline completed in 2,780.008 seconds and its different-
fan-in replay in 2,569.362 seconds. Both conserved the full 264,085,606 rows;
all canonical and lookup artifact digests and logical identity matched. The
separate corpus check passed all streams, state totals and 35 source/target
witnesses. See the [baseline gate](./receipt-reference-join-2026-09-11.md).

## Complete synthetic pipeline benchmark

Each row uses a complete generated report with an exact reference. One million
rows traverse both input passes, all sort/merge/join stages, reverse incidence,
final assembly and readback. Each process receives the same eight-CPU quota,
4 GiB container memory, `GOMEMLIMIT=2GiB` and `GOMAXPROCS=8`; only processing
worker count changes. The fixture does not decode source Parquet.

| Workers | Wall seconds | Rows/second | Peak process RSS bytes | Peak workspace bytes |
|---|---:|---:|---:|---:|
| 1 | 23.025 | 43,432 | 362,160,128 | 41,390,710 |
| 4 | 7.277 | 137,424 | 1,061,863,424 | 64,245,103 |
| 8 | 5.368 | 186,307 | 1,432,113,152 | 65,136,269 |

Eight workers took about 4.29 times less wall time than one in this single-run
synthetic comparison. All decision, incidence and neighbor digests matched.
These are not confidence intervals, full-source timings or weekly-refresh claims.

An intermediate version retained serial final assembly: 23.138, 10.236 and 8.418
seconds for one/four/eight workers. Its measured final assembly consumed over half
the eight-worker runtime. The final implementation therefore assembles the four
independent artifact families concurrently under the same shared byte cap.

The complete Go suite, vet and targeted race tests pass. Fixtures additionally
prove complete duplicate handling across worker counts, same-build identity,
actual concurrent dispatch, input-buffer ownership, cancellation, and shared
workspace conservation/failure behavior.

## Accepted real gate

The eight-worker job reads the identical immutable source manifest, with source
storage mounted read-only, four source readers and the unchanged 16 GiB workspace
cap. It writes only a new isolated directory. A live scan sample used 352% CPU and
1.096 GiB container memory; this is a point sample, not a peak or a stage-speedup
comparison. The host exposed 16 physical cores / 32 hardware threads at the check.

Both full-source scans have now completed: requests took 424.369 seconds and
membership took 401.015 seconds. All eight report workers started their lookup
stages concurrently, with 4,515,340–8,068,764 reference rows per worker. A join-stage
sample used 799.99% CPU and 1.684 GiB container memory under the unchanged 4 GiB
cap. These point samples do not establish average CPU utilization.

The command completed with all 264,085,606 source occurrences and 51,600,308
reference decisions conserved. Every state count and all three canonical
decision/incidence/neighbor artifacts match the baseline, including compressed
bytes and hashes. The separate corpus test read and verified all four streams,
checked lookup witnesses and global incidence totals, and verified 35 full-source
row witnesses across seven observed reference states. It passed in 360.58 seconds,
in addition to the command runtime below. It uses the same Parquet library, not
an independent decoder or a second full-cycle reference algorithm.

| Measurement | Parallel run |
|---|---:|
| Wall seconds | 1,338.845 |
| Peak process RSS bytes | 2,011,222,016 |
| Peak workspace bytes | 5,386,701,602 |
| Retained artifact bytes | 3,117,911,731 |
| Lookup candidate member rows, including filter false positives | 76,623,814 |

The new calculation identity is
`5fbe9e93fa2f9f74f645948b60b98856f3db077be9164d82238a1b1b1b58d57d`.
Its build differs from the baseline. Extra lookup members differ because the fixed
filter budget is partitioned; they do not change the exact reference decisions.

## What the speedup does and does not show

The observed 46m 20s to 22m 19s improvement is about 2.08×. The baseline already
used four source readers and a four-CPU quota, with serial downstream joining.
The new run used four source readers, eight report workers and an eight-CPU quota.
These are different implementations measured on a shared host, not a controlled
one-CPU/eight-CPU experiment. The synthetic equal-budget worker comparison above
does not include the real-source scanning path that dominates the measured run.

| New-run stage | Wall seconds |
|---|---:|
| Complete source request pass | 424.369 |
| Complete source membership pass | 401.015 |
| Longest worker's sequential lookup/decision/neighbor stages | 364.297 |
| Longest concurrent final assembly (lookup evidence) | 139.697 |

The two source passes account for 825.384 seconds, about 62% of elapsed time.
They include Parquet decoding, single-dispatcher partitioning, ingestion/sorting
and waits for worker acknowledgments, not just disk reads. The source reader
limit remains four. The eight-worker join therefore does not parallelize the
entire job eight ways. Worker stage durations overlap; so do the four final
assembly durations. Do not sum those concurrent durations as elapsed time.
The table also excludes startup/source-backing verification and coordination
overhead; it is not an exact exhaustive wall-time partition.

This result accepts correctness and measures an improvement, not optimal scaling.
The next performance check should profile bounded real-source reads under equal
CPU/memory budgets, separating decoding, dispatcher/worker waits and I/O before
choosing an implementation change. No current evidence isolates disk bandwidth,
the dispatcher or decoding as the sole cause. Another full-cycle replay is not
needed merely to discover that the source passes dominate this run.

Follow-up: the [bounded real-source profile](./receipt-source-scan-profile-2026-09-12.md)
now measures reader scaling, row-assembly CPU cost and acknowledgment waits.
It leaves this accepted executable and full-cycle result unchanged.

The retained synthetic CPU profile (`workers-8.cpu.pprof`, `profile-top.txt`)
records 31.75 CPU-seconds over 5.75 wall-seconds. Hashing, zstd work and memory
copies are visible costs; this is a profile of the benchmark, not the full source
run. Its extra profiling invocation is separate from the timing table above.

## Retained evidence

The complete audit tree is retained under:

```text
/storage/dumps/audits/fec/receipt-reference-parallelism/2026-09-11/attempt-01/
```

`cycle/manifest.json` is the successful completion manifest. `cycle.exit`,
`baseline-comparison.exit`, `corpus-check.exit` and `verification.exit` are zero;
the canonical comparison is `true`. Full-suite, race and vet exit markers are
also zero. `SHA256SUMS`, `retention-check.log` and zero `retention-check.exit`
verify every copied file, including the executable, source archive, tests,
benchmarks, profile and results. The original working tree remains unchanged at
`/tmp/legal-tender-reference-parallel.AC8Bt0/`.

The cross-build comparison deliberately excludes executable/calculation IDs and
filter-only extra lookup members. Those may change with the new code/partitioning;
source identity, reference policy, exact decisions and reverse-reference evidence
must not change. Same-build worker-layout identity is fixture-proven here, not yet
a second complete-corpus replay of the new binary.

No existing source pointer, graph, financial calculation or Dagster asset changed.
Contributor/conduit graph publication remains the next product milestone; source
pass profiling is the focused performance follow-up, not a new correctness gate.
