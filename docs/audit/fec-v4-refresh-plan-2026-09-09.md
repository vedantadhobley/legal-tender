# FEC v4 refresh plan and storage audit — 2026-09-09

Follow-up: the [storage review](./fec-storage-review-2026-09-09.md) fixes the
counter and checks the full prior-size scenario. The findings below preserve
the earlier pre-fix observation; no large acquisition followed either review.

Scope: metadata-only inspection following the
[committee-summary publication gate](./committee-summary-publication-2026-09-08.md).
No source bodies were downloaded, no acquisition/staging command ran, no source
or graph pointer changed, and no retained evidence was deleted. Application code
and storage limits remain unchanged.

## Result

The real v4 plan selects 27 sources. Twenty existing publisher object versions
have changed, four committee-summary members are new, and three sources are
reused: the 2020 candidate master, committee master, and candidate linkage.
Changed publisher versions do not establish changed records or new money.

Two complete HEAD observations, at 18:43 and 18:48 UTC, returned the same source
versions, lengths, modification times, and ETags. Every source was available.
Both saved discoveries and Go plans pass their local v4 JSON schemas and select
the same candidate release. These observations are not a guarantee that later
GET requests will receive unchanged objects; acquisition must revalidate them.

| Changed/new source family | Selected download bytes | Decimal size |
|---|---:|---:|
| Processed Schedule A | 90,173,582,284 | 90.17 GB |
| Processed Schedule B | 39,313,075,207 | 39.31 GB |
| Processed Schedule E | 43,387,186 | 43.39 MB |
| Four committee-summary CSVs | 31,119,037 | 31.12 MB |
| Other changed classic files | 5,115,878 | 5.12 MB |
| **Total** | **129,566,279,592** | **129.57 GB / 120.67 GiB** |

No matching candidate partial files exist, so the observed resume credit is
zero. These are publisher artifact sizes before extraction, not selected-cycle
fact sizes. The new CSVs are a small part of the cost; the newer A/B objects
account for nearly all of it.

The processed A/B/E objects are dated September 6. The new CSVs and changed
classic objects are dated September 9. All original source URLs, final URLs,
version IDs, ETags, lengths, and per-object times are retained in the discovery.
No download-duration estimate was measured.

## Storage: global free space passes, current hot-lane check does not

The data filesystem had 1,404,862,169,088 available bytes (about 1.40 TB).
The existing acquisition model reserves all candidate downloads, a
206,363,392,958-byte largest-extract allowance, and a 25 GiB working margin.
That leaves about 1.04 TB, above its 500 GiB free-space floor.

The Schedule A hot-lane calculation is different. The current
[directory byte counter](../../internal/source/fec/release/acquisition_storage.go)
adds each regular file path's logical size. It does not deduplicate hard links.
Read-only inode inspection found:

- Path-size sum: 386,898,671,788 bytes.
- Unique-file size: 297,015,247,494 bytes.
- Difference: 89,883,424,294 bytes counted twice for one inode.

The duplicate paths are the old `fec-b4f558…` acquisition's Schedule A `.partial`
and `raw/fec/schedule-a/snapshots/2026-08-23/fec_fitem_sched_a.dump`. They are
hard links to the same inode, not two stored copies. Removing one link would
not reclaim that file's data blocks and is not a substitute for correct
accounting. Exact paths and link metadata are in `storage-inodes.json`.
The Schedule B tree also has a snapshot/CAS hard-link pair, confirming that
path count and stored-file count are distinct in this layout.

Under the current [acquisition calculation](../../internal/source/fec/release/acquire.go),
the projected A hot-lane value is 710,279,192,630 bytes, over the unchanged
600 GiB cap (644,245,094,400 bytes). An independent inode-deduplicated calculation
reduces that projection to 620,395,768,336 bytes, leaving only about 23.85 GB
of hot-lane headroom. This is a diagnostic calculation; the application guard
was not changed or bypassed.

## Acquisition headroom is not whole-release staging headroom

[Stage](../../internal/source/fec/release/stage.go) repeats the full largest-
extract plus working-margin reservation before each newly extracted output.
Completed new A extracts accumulate in the same hot lane. Acquisition's initial
check does not forecast those retained outputs.

The four current accepted A extracts total 57,968,298,375 compressed bytes.
If the next extracts were different content but the same sizes, the first two
would consume 32,707,281,443 bytes of the 23.85 GB headroom. Even after fixing
hard-link accounting, the current stage guard would then stop before the 2024
extract. All four retained extracts plus the unchanged reserve would project
678,364,066,711 bytes, 34,118,972,311 bytes over the cap before a subsequent
non-reused output.

That is an illustrative scenario, not a bound or measurement of the new dump.
New extracts may differ in size or reuse existing content-addressed output.
An identical finalized output can avoid new retained bytes but still requires
extraction and temporary space. B remains archive-direct; no new full B COPY
extract is selected. Downstream fact/calculation/graph costs are not included.

The implementation streams COPY through zstd, while the retained budget rule
still reserves a largest-period uncompressed extract. That mismatch deserves
measurement and an explicit reviewed storage model; it does not authorize
silently reducing reserves or raising the cap. Older source evidence still
requires the accepted verified-retention boundary.

## Evidence and next step

Retained under `dumps/audits/fec/v4-refresh-plan/2026-09-09/` in project storage:

- Captured baseline manifest; two discoveries and plans, each with exit markers.
- Read-only storage and inode inventories, the probe script, and cost estimate.
- Independent schema/arithmetic/stability verifier, result, and exit marker.

Baseline: `fec-136903645ee7c7050d463a4f779a051a454ebb87308fd854faabc5ca47a20cdf`.
Its manifest SHA-256 is
`a28d56c7a3be024ed7cd85e2bf0ead21c982c96d00b683ef364c0a37eb9bc3ec`.
The current pointer matched its immutable bytes before and after inspection.
Candidate, **not published**:
`fec-01f93a786b630be40a932543f35251bcb56c42b32d1ae85ac34a101ddc7b56b2`.
Existing source files passed size/name checks; large content hashes were not
reread for this planning probe.

Next correct and test inode-aware accounting, then validate a complete staged-
output/temporary-workspace projection against the accepted cap and retention
rules before downloading. Do not treat the inode fix alone as approval to start.
Refresh publisher metadata again before any later acquisition. The
[work queue](../todo.md) and [release strategy](../design/fec-release-strategy.md)
retain these gates; active/default v3 remains unchanged.
