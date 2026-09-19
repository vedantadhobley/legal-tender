# Bounded source-release streaming storage

Implemented in Go acquisition and selected-output staging. The
[verification gate](../audit/fec-streaming-storage-2026-09-09.md) covers write
limits, checkpoint retry, byte equivalence, and a read-only real-plan review.
This changes storage enforcement, not source selection, data semantics, or
release publication authority.

## Limits and accounting

Keep the existing 600 GiB Schedule A cap, 500 GiB filesystem available-space
floor, and 25 GiB working margin. Count retained logical regular-file bytes
once per device/inode under `raw/fec/schedule-a/`. Separate copies and sparse
logical bytes count fully. Hard-link aliases do not add bytes; symlinks are not
followed. Available space comes from the storage filesystem's `statfs` result.

The selected-output extractor pipes source bytes directly into zstd. It never
creates a full uncompressed relation. Therefore the default
`largest_extract_working_bytes` is zero, replacing the former
206,363,392,958-byte allowance. This is not a smaller guessed extract size:
actual compressed writes now consume an enforced allowance.

Source archives, existing partials, all retained Schedule A extracts, completed
new extracts, and new compressed temporary files count together. New selected
temporary files for **every** source live under:

```text
raw/fec/schedule-a/staging/<candidate>/selected/<run>/.selected-*.zst
```

Non-A selected outputs leave this counted tree when finalized into their
existing source-family CAS locations. Schedule A outputs stay in the counted
tree. Stable CAS names and checkpoint paths are unchanged. An interrupted
process's temporary files remain counted; a later run never deletes them
implicitly. Legacy `.selected-*.zst` files under `raw/fec/staging/` block new
writes until reviewed, since they are outside the new counted tree.

## Planning versus enforcement

Acquisition checks, before GETs:

```text
hot requirement = current hot bytes + remaining A download + margin
free requirement = all remaining downloads + floor + margin
```

It also derives a full-output scenario from inventory-selected outputs and the
validated prior manifest. Changed source versions use prior compressed sizes
as estimates; unchanged versions require existing backing. No future CAS or
checkpoint deduplication savings are assumed.

For that scenario, walk outputs in Stage order. Accumulate new A bytes; include
each non-A temporary output while it is being written, then remove it from the
hot estimate on finalization. Add the peak of this sequence to the acquisition
hot requirement. The free-space scenario adds all new retained output bytes
to the acquisition free requirement. A temporary stream becomes its final
inode, not a second retained copy.

A complete scenario that exceeds either limit blocks acquisition before GETs.
An unknown seed/output size is not zero or a bound. Acquisition may proceed
with an incomplete scenario if its known acquisition costs pass; runtime
enforcement still applies. The manual review command returns nonzero for an
incomplete scenario. A fitting review is neither a guarantee nor authorization
to download.

## Write-time guards

- Acquisition and staging hold the same nonblocking filesystem lock at
  `raw/fec/.storage.lock` for the operation. Contention returns `storage_busy`.
  The lock file remains present; closing its descriptor releases the lock.
- All download workers share one budget. Staging recalculates a budget before
  each output, so completed retained files reduce the next allowance.
- Split writes into at most 1 MiB chunks. Before each chunk, check cancellation
  and available filesystem bytes, preserving floor plus margin. Refresh hot
  inode accounting after each 8 MiB of writes. Monotonically decreasing local
  allowances also bound own growth if filesystem allocation feedback lags.
- Charge every download to available space; charge A downloads and all selected
  compressed temporary writes to the hot allowance as well. Partial targets
  must be unshared regular files; guarded outputs must be on the storage
  filesystem. Do not mutate an immutable source through a hard-link alias.
- A limit breach stops before the offending chunk and returns a blocked result.
  I/O failures and cancellation remain failures, not successful publications.

This lock coordinates the Go acquisition/staging commands, not every writer on
the host. External programs can consume space between observations. The 25 GiB
margin is a buffer, not a filesystem-wide reservation or quota. Keep all source
writes on one filesystem; do not run legacy/uncoordinated writers against this
tree concurrently. Metadata writes use the margin rather than the bulk writer.

## Failure, retry, and preservation

Keep a failed download's exact partial prefix for conditional range retry. A
response larger than its selected length is rejected without writing an extra
byte; discard that response's suffix so a rejected complete-length prefix
cannot be accepted as complete on retry. Shared/symlink partials fail before
truncation or download.

On extraction failure, remove only that attempt's private temporary file.
Completed CAS outputs and checkpoints remain. On retry, verify and reuse them;
do not redo completed extraction. A CAS object linked before a later failure
also remains retained. Crashed-process remnants require explicit review.

Full compressed/uncompressed digest readback and atomic CAS finalization stay
unchanged. Neither acquisition nor staging can advance the active release
pointer. Publication retains its separate integrity and prior-release gates.

## Compatibility and remaining limits

The diagnostic emits the strict
[storage-review v2 schema](../../contracts/audits/fec/storage-review/v2/result.schema.json).
Historical v1 reports keep their original full-reserve interpretation; they are
not rewritten. Acquisition/stage wire schemas and completed-state replay remain
compatible. Python/Dagster gains no storage calculations or new configuration.

This contract covers source acquisition/staging, not downstream facts,
calculations, or graph storage. It does not solve perpetual retention. The
[release retention contract](./fec-release-strategy.md#retention) still requires
a verified cold destination or an explicit reproducibility decision before
perpetual refresh automation. Do not delete historical evidence to force a fit.
