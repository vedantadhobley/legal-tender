# Streaming source-storage gate — 2026-09-09

The [bounded streaming model](../design/fec-streaming-storage.md) replaces the
unused full-uncompressed-extract reserve with actual write enforcement. The
600 GiB hot cap, 500 GiB filesystem floor, and 25 GiB margin are unchanged.
No bulk source was fetched or staged. No retained source was deleted. Active
and default release v3 remain unchanged; no graph or domain calculation changed.

## Verification

The complete Go suite, formatting/module checks, vet, CLI build, and targeted
release/CLI race tests pass. New regression coverage proves:

- Exact cap/floor boundaries, shared concurrent allowances, chunked writes,
  delayed allocation feedback, external usage changes, cancellation, stat errors,
  and overflow fail closed.
- Acquisition and staging cannot own the same storage writer lock together.
  A known oversized full-output scenario blocks before GETs; a later floor
  breach returns a blocked acquisition without marking it complete.
- Partial downloads survive a budget stop and resume byte-exactly. Hard-linked
  and symlink partials cannot truncate retained files. Oversized HTTP responses
  cannot become accepted complete prefixes on retry.
- A cumulative staging cap permits the first relation, blocks the second,
  retains all completed outputs, and removes only the failed attempt's private
  temporary file. Restoring fixture headroom permits same-cap checkpoint retry;
  completed replay performs no extraction and never publishes a release.
- Interrupted-process files in the new workspace count toward the cap. Legacy
  temporary files block writing and are not deleted. Non-A temporary peaks are
  included in the prior-size model in stage order.
- A deterministic 16 MiB incompressible stream has identical compressed and
  uncompressed counts and hashes with and without guards. CAS finalization
  retains one inode's bytes, not two copies.

Independent Python schema/arithmetic/inode checks against the real saved inputs,
plus source-contract and Dagster boundary tests, pass: 30 tests, with 53 existing
Dagster beta warnings. Both diagnostic schema versions remain tested.

## Bounded performance check

A deterministic 32 MiB incompressible fixture runs compression, write, sync,
and full digest/decompression readback. The capped offline Go container uses
four CPUs, a 4 GiB container limit, and a 2 GiB Go memory target. Three benchmark
samples each run three iterations:

| Measurement | Plain | Guarded |
|---|---:|---:|
| Median iteration | 81.39 ms | 82.22 ms |
| Observed throughput range | 405–418 MB/s | 407–415 MB/s |
| Median allocated bytes per iteration | 29,243,602 | 29,285,466 |

The measured median overhead is about 1%; allocation growth is about 42 KB per
iteration. Allocated bytes are not peak RSS. These are small local-file tests,
not full-source extraction, archive-download, or production throughput claims.

## Read-only saved-plan review

The new CLI ran with networking disabled and source storage mounted read-only
at 2026-09-09T20:54:45Z. It used the same unpublished September 9 candidate and
exact prior manifest as the [earlier audit](./fec-storage-review-2026-09-09.md).
It did not refresh publisher metadata.

| Measure | Bytes |
|---|---:|
| Existing unique Schedule A bytes | 297,015,247,494 |
| Remaining downloads, all sources | 129,566,279,592 |
| Prior-size new retained outputs, all sources | 58,005,781,337 |
| Projected peak hot bytes, including margin and non-A temporary peak | 472,033,191,548 |
| Hot cap | 644,245,094,400 |
| Required available bytes, including floor and margin | 751,286,518,529 |
| Observed available filesystem bytes | 1,773,431,455,744 |

The complete scenario now fits: about 439.62 GiB against the 600 GiB hot cap.
Actual new output sizes remain unknown; runtime guards enforce their growth.
The active manifest hashes before and after match the saved baseline. Current
source-file metadata independently reproduces the unchanged hot-byte count.

Evidence lives under `dumps/audits/fec/streaming-storage/2026-09-09/`: exact
plan/prior inputs, v2 review, inode inventory, pointer hashes, test/benchmark
logs, scripts, CLI binary, and explicit exit markers. The earlier v1 evidence
remains unchanged.

Follow-up: the [fresh v4 preflight](./fec-v4-preflight-2026-09-09.md) now confirms
the same candidate and fitting budget without source acquisition. A source-
release fit does not budget downstream materialization or replace the cold-
retention gate for perpetual refreshes.
