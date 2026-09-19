# Legacy system boundaries and sources

> **Excavation date:** 2026-08-27  
> **Scope:** Existing Python, Dagster, ArangoDB, and local persisted data.  
> **Target-design authority:** None. This document describes legacy behavior.

## Evidence state

No Legal Tender containers were running during this excavation. Runtime
database state and Dagster run history were therefore unavailable. The local
raw archives, parser dumps, cache files, code, and dated project documents were
available.

- `OBSERVED` — the raw and dump stores exist under
  `~/workspace/data/legal-tender/`.
- `OBSERVED` — raw metadata records successful FEC refreshes on 2026-06-21 and
  a legislators refresh on 2026-06-28.
- `OBSERVED` — no Legal Tender container was running on 2026-08-27.
- `DRIFT` — the project front door previously named 2026-05-16 as the latest
  sync. That date remains the latest documented full validation, not the latest
  raw download or parser dump.
- `UNCLEAR` — whether every 2026-06-21 parsed dump was subsequently used to
  rebuild the aggregation database and candidate outputs.

The absence of a running stack does not invalidate code evidence. It prevents
this pass from labeling database behavior `OBSERVED`.

## Implemented boundary

The scheduled legacy system has this boundary:

```text
FEC bulk ZIPs + legislators YAML
        |
        v
versionless local raw paths
        |
        v
per-cycle Arango collections + replaceable JSONL dumps
        |
        v
cross-cycle aggregation vertices and summary edges
        |
        v
committee classifications, entity resolution, and receipt summaries
        |
        v
candidates.funding_channels projection
```

Dagster defines the dependency graph and schedules work. Python assets perform
downloads, parsing, aggregation, enrichment, graph construction, and final
projection. ArangoDB holds both parsed documents and the named investigative
graph. The filesystem holds the latest source archives, replaceable Arango
dumps, and enrichment caches.

The system does not implement a public investigative API or web UI. The
user-visible surfaces are scripts such as `view_candidate.py` and direct
database queries. The lobbying module is an unused client and does not feed the
pipeline.

## External source inventory

| Source | Legacy role | Fetch form | Parsed or consumed state |
|---|---|---|---|
| FEC `cn` | Candidate master | One ZIP per cycle | `fec_<cycle>.cn` |
| FEC `cm` | Committee master | One ZIP per cycle | `fec_<cycle>.cm` |
| FEC `ccl` | Candidate-committee linkage | One ZIP per cycle | `fec_<cycle>.ccl` |
| FEC `indiv` | Itemized individual receipts | One ZIP per cycle | `fec_<cycle>.indiv` |
| FEC `pas2` | Itemized committee disbursements | One ZIP per cycle | `fec_<cycle>.pas2` |
| FEC `oth` | Other committee receipts | One ZIP per cycle | Filtered `fec_<cycle>.oth` |
| FEC `weball` | Candidate summary totals | One ZIP per cycle | `fec_<cycle>.weball` |
| FEC `webl` | House/Senate summary totals | One ZIP per cycle | `fec_<cycle>.webl` |
| FEC `webk` | PAC summary totals | One ZIP per cycle | `fec_<cycle>.webk` |
| congress-legislators | Current legislator identity and FEC IDs | YAML | Local cache; not part of the money graph |
| reconci.link/Wikidata | Employer, person, and committee-type resolution | Live HTTP plus JSON cache | Corporate-family and classification projections |
| GLEIF | Employer resolution fallback | Live HTTP plus JSON cache | Corporate-family projection |
| Senate LDA v1 endpoint | Intended lobbying source | Unused client | No pipeline state |

`CODE` — the normal scheduled job selects every Dagster asset, including
enrichment and aggregation, after download and parsing. `CODE` — no lobbying
asset is registered.

## Local source store

### Paths and lifecycle

Each FEC source has exactly one active path per cycle:

```text
raw/<cycle>/<source>.zip
raw/<cycle>/metadata.json
```

`CODE` — a successful refresh opens the active path for binary write and writes
the full HTTP response. It does not write to a temporary file, validate a
checksum, retain the old archive, or assign an ingestion/version identifier.
The metadata record is also updated in place and contains the latest download
time, size, and URL only.

Consequences:

- The raw store contains the finest source grain available to the system.
- The raw store is a latest snapshot, not an immutable evidence archive.
- A refresh destroys local access to the prior source release.
- A killed process can leave a partial ZIP at the canonical path.
- Neither source bytes nor parsed facts carry a stable ingestion-run ID.

### Freshness behavior

`CODE` — `data_sync` first compares the remote `Last-Modified` header with the
local file modification time. It falls back to a seven-day age check when that
header is unavailable.

`BUG?` — after `data_sync` decides a remote file is newer, it calls
`download_fec_file` without forcing the refresh. That function performs a
second, independent seven-day freshness check and can return the local archive
without downloading the known-new remote file. The asset can then report the
path as downloaded and count its existing size.

`CODE` — download errors are captured per source, added to an `errors` array,
and do not necessarily fail the overall asset. Downstream assets depend on the
asset completing, not on `errors` being empty.

## Observed persisted source state

Raw metadata and dump metadata show a mixed snapshot:

| Cycle | Core and summaries | `indiv` | `pas2` | `oth` |
|---|---:|---:|---:|---:|
| 2020 | Core 2026-05-06; summaries 2026-06-21 | 2026-05-06 | 2026-05-06 | 2026-05-06 |
| 2022 | 2026-06-21 | 2026-06-21 | 2026-06-21 | 2026-06-21 |
| 2024 | Core and summaries 2026-06-21 | 2026-05-06 | 2026-05-06 | 2026-05-06 |
| 2026 | 2026-06-21 | 2026-06-21 | 2026-06-21 | 2026-06-21 |

`OBSERVED` — parser dumps exist for all nine FEC collections in all four
cycles, but their creation times follow the same mixed-source dates. Dump
metadata reports these transaction collection counts:

| Cycle | `indiv` | filtered `oth` | `pas2` |
|---|---:|---:|---:|
| 2020 | 69,377,425 | 828,152 | 887,829 |
| 2022 | 63,885,896 | 540,481 | 748,765 |
| 2024 | 58,208,756 | 377,505 | 703,597 |
| 2026 | 25,454,311 | 122,856 | 157,846 |

These counts prove what the replaceable dumps contain. They do not prove that
the live aggregation database was rebuilt from those exact dumps.

## Common parser contract

`CODE` — all FEC parsers use local CSV header files as field-order authority.
Those header files are not fetched or versioned by the scheduled pipeline.

Common parsing behavior:

1. Select the first `.txt` member in each ZIP.
2. Decode each line as UTF-8 with `errors="ignore"`.
3. Strip leading and trailing whitespace from the entire line.
4. Split on `|`.
5. Pad missing trailing fields with empty strings.
6. Map values to header names with `dict(zip(...))`.
7. Add `_key` and a materialization-time `updated_at`.
8. Truncate the destination collection before parsing.
9. Import batches with `on_duplicate="replace"`.

This contract does not reject malformed rows. Extra columns are silently
discarded by `zip`; undecodable bytes are silently discarded; missing columns
are silently synthesized as empty strings. No source byte offset, line number,
ZIP member, source checksum, or ingestion ID is stored on the parsed document.

The parser picks the first `.txt` member rather than verifying the expected
member name. The current 2026 `indiv.zip` contains the full `itcont.txt` first
and several `by_date/*.txt` members after it, so the observed archive order
works with the code.

## Per-source parsed behavior

| Source | Parsed grain | Key and replacement behavior | Source-time filtering |
|---|---|---|---|
| `cn` | One current candidate-master row | `CAND_ID`; later duplicate wins | None |
| `cm` | One current committee-master row | `CMTE_ID`; later duplicate wins | None |
| `ccl` | One current linkage row | candidate + committee + linkage ID; fallback line key | None |
| `indiv` | One retained itemized contribution row | `SUB_ID`; later duplicate wins | None |
| `pas2` | One retained itemized disbursement row | `SUB_ID`; later duplicate wins | None |
| `oth` | One retained other-receipt row | `SUB_ID`; later duplicate wins | Keeps only `PAC`, `COM`, `PTY`, and `ORG` entity types |
| `weball` | One FEC candidate summary | `CAND_ID`; later duplicate wins | Requires candidate ID |
| `webl` | One congressional candidate summary | `CAND_ID`; later duplicate wins | Requires candidate ID |
| `webk` | One PAC summary | `CMTE_ID`; later duplicate wins | Requires committee ID |

The master and summary files are already source-defined snapshots or
aggregates. The transaction files have source-row grain, subject to the `oth`
filter and duplicate replacement.

`CODE` — numeric conversion replaces an invalid transaction amount with null.
The original invalid string is not retained separately. Summary parsers do the
same for selected numeric fields.

## Parser dumps

Each parsed collection can be written to one gzipped JSONL path:

```text
dumps/fec/<cycle>/<collection>.jsonl.gz
dumps/fec/<cycle>/metadata.json
```

`CODE` — dump creation overwrites that collection's prior dump. Freshness is
based only on whether the source file modification time is later than the dump
creation time. No source checksum is compared. Restore drops and recreates the
collection, then imports every dumped document.

The dumps improve restart speed. They are not event logs, source archives, or
historical snapshots.

## Scheduling and partial failure

`CODE` — `weekly_fec_refresh` is configured for Sunday at 02:00. Its default is
running unless an environment variable disables it. `CODE` — the full job uses
`AssetSelection.all()` and relies on Dagster dependencies for order.

`OBSERVED` — no Legal Tender containers were running during excavation, so the
schedule could not be active regardless of its configured default.

Most parser assets catch exceptions by cycle, log the error, omit that cycle
from returned stats, and still return a successful Dagster output. This permits
a later graph rebuild to combine fresh, stale, restored, or missing cycles
without a global snapshot contract.

## Lobbying boundary

`CODE` — `src/api/lobbying_api.py` is not registered as an asset and has no
consumer. It targets the retired Senate LDA API shape described in the legacy
lobbying plan. The existing system therefore has no lobbying facts, edges,
aggregates, schedules, or user-visible results.

## Enforced and unenforced invariants

The legacy boundary enforces:

- A fixed configured list of cycles for normal jobs.
- One active raw path and parsed collection name per source and cycle.
- Arango document key uniqueness within each collection.
- Dependency order between registered assets.

It does not enforce:

- Immutable or replayable source versions.
- Atomic raw download replacement.
- Source checksums or byte counts against an independent manifest.
- Parser/header compatibility.
- All-or-nothing cycle snapshots.
- Failure of the run when one source or cycle fails.
- Provenance from a parsed document back to exact source bytes.
- Lobbying ingestion.

## Primary evidence

- [Download orchestration](../../../src/assets/sync/data_sync.py)
- [Source repository and download implementation](../../../src/data/repository.py)
- [FEC schema loader](../../../src/utils/fec_schema.py)
- [Individual-contribution parser](../../../src/assets/fec/indiv.py)
- [Committee-disbursement parser](../../../src/assets/fec/pas2.py)
- [Other-receipts parser and filter](../../../src/assets/fec/oth.py)
- [Arango dump manager](../../../src/utils/arango_dump.py)
- [Dagster jobs](../../../src/jobs/asset_jobs.py)
- [Weekly schedule](../../../src/schedules/__init__.py)
- [Unused lobbying client](../../../src/api/lobbying_api.py)
