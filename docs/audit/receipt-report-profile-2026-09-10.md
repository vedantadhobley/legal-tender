# Same-release receipt report profile — 2026-09-10

Status: the complete 2024 [Go occurrence profile](../design/receipt-report-profile.md)
passes source validation, exact group conservation, independent output checks,
full Go regressions, static analysis, and race tests. This is not a financial
comparison, fact refresh, or terminal-allocation result.

## Source alignment finding

The earlier accepted Schedule A facts contain 264,085,606 rows. The v4-selected
2024 relation contains 264,085,633 rows. Their uncompressed hashes differ:

- Earlier: `3ef0fdcdbf246981e5724e810c89d11b79b702369b7467633e783d3936f2b9a5`.
- V4: `e1390338f92ec88f0a0e50ef02d01ac9297b90c602f6b1af9bdde882f2a07a3e`.

The net difference is 27 rows, not proof that only 27 records changed or that the
new relation is append-only. No old fact ancestry was relabeled, and no shard
adoption or semantic equivalence was claimed.

This diagnostic instead selected the already-staged relation through verified
summary fact set `603d086eb26baa5a9a99d7a717ec5b7469098c173d2d3eabaa119c00d9b7f637`.
It regenerated the accepted summary assertion calculation
`cfa50f9bbc4b64384cbc973951288be0a426d1a8994dae5824d3e6f09a0fad13`.

Both inputs belong to source release
`fec-01f93a786b630be40a932543f35251bcb56c42b32d1ae85ac34a101ddc7b56b2`,
manifest SHA-256 `b030668efca44fcf611b204665d95c00fa7eda9ce3ce31b5711ebbba6805e81c`.
The relation's source archive SHA-256 is
`7d8dcca81181446d25e374780b674f24c94e335e59261b7114580d5c61fdda2b`.

The one-pass Go scan independently recomputed all four physical measurements:

| Measurement | Verified result |
|---|---:|
| Physical rows, all source-valid | 264,085,633 |
| Compressed bytes | 14,773,841,175 |
| Uncompressed bytes | 182,881,318,759 |
| Compressed SHA-256 | `649e2b4e3617938a98f46e69d91ce6aa028b443a9a8d2f3f49e47a1581ee2e81` |

The uncompressed hash is the v4 value above. All six verifier checks passed.
There was no download, extraction, or corpus-sized intermediate publication.

## Actual scope observations

The form/line/decision table has 78 disjoint groups conserving all source rows.
Two physical occurrences have unknown amounts; they remain explicit. The
existing included individual predicate contains 222,205,451 occurrences across
22 form-line combinations. Every included report group has a positive file
number and a syntactically valid committee ID in this snapshot. These are source
references, not independent proof of complete reports or resolved identities.

The included report table has 70,761 groups, 66,079 distinct committee/file pairs,
and 8,997 distinct committee IDs. It conserves the same included occurrence
population and every signed/positive/negative/zero measure, both globally and
within each form/schedule/line group. These two tables must not be added together.

Selected form-line observations illustrate why the broader predicate is not
automatically one summary subtotal:

| Form | Reported line | Included occurrences |
|---|---|---:|
| F3 | 11AI | 10,985,792 |
| F3 | 12 | 484 |
| F3P | 17A | 4,381,396 |
| F3P | 18 | 10 |
| F3X | 11AI | 206,142,624 |
| F3X | 11C | 2,238 |
| F3X | 17 | 686,346 |
| F3X | SL1A | 340 |
| F4 | 14A | 19 |

This is a selected illustration, not the complete partition or a new whitelist.
All 22 included combinations remain in the artifact. The FEC's
[methodology](https://www.fec.gov/campaign-finance-data/about-campaign-finance-data/methodology/)
explicitly describes a multi-line individual classification; its
[receipt-data reference](https://www.fec.gov/campaign-finance-data/about-campaign-finance-data/about-receipts-data/)
separates form-specific contribution, transfer, and other receipt lines. Those
references were rechecked for this review. No named-committee exception or new
production form-line selector was introduced.

Included receipt-date states also conserve exactly:

| State | Occurrences |
|---|---:|
| Within 2023-01-01 through 2024-12-31 | 222,199,033 |
| Before that interval | 6,188 |
| After that interval | 6 |
| Missing | 224 |
| Invalid normalized date | 0 |

No date was clipped or treated as report coverage. The profile has no report
coverage-start/end columns and establishes no report/account selection policy.

## Verification and cost

The full source scan took 508,846 ms (8m 28.846s). The background Go job, including
focused tests, build, complete scan, full regression suite, static analysis, and
race tests, ran from 03:49:09 to 03:58:28 UTC. Its explicit `job.exit` is zero.
Observed scan memory samples stayed below 131 MiB; this is not a measured peak.
The container cap was 4 GiB, with a 2 GiB Go target and four CPUs.

The output has 96,668,064 JSON bytes. Its profile ID is
`764baac5c9cd5eddca3b6c0a9456ba6f2de4049e083594fd0e2e6856bd8ce7c1`;
file SHA-256 is `18e37311c32a8ba65c422ba0d7e1f897a0df5b373fc101b206a910bd577a91c8`.
This verbose audit output is not the accepted recurring calculation format.

Synthetic tests cover distinct null/blank labels, physical duplicates, signs,
unknown amounts, predicate exclusions, missing/invalid/out-of-cycle dates,
overflow, bounded caches, deterministic replay, invalid CLI options, and verifier
callback failure/cancellation. Callback observations never bypass final stream
checks. Full-corpus replay was not repeated; fixture replay and independent
content-identity reconstruction passed.

The independent Python check verifies the complete output schema, source-release
binding, selected descriptor, accepted summary calculation identity, profile ID,
all measure conservation, per-form report regrouping, date conservation, and wire
guards against promotion or an invented delta. It does not independently parse
all 264 million source rows; that complete physical scan is the Go verifier gate.
Python is test-only. All three independent tests passed in 106 seconds, lint
passed, and the explicit `python.exit` is zero.

The source pointer hash is unchanged before/after. All project data was mounted
read-only. No source/fact pointer, graph, Dagster asset, schedule, service, or
stored source record changed. No data was deleted.

## Retained evidence and next step

Evidence is retained under
`dumps/audits/fec/receipt-report-profile/2026-09-10/attempt-01/` in project storage:
exact code snapshot and binary, source-pointer hashes, complete profile, synthetic
fixture, independent test/schema snapshot, commands, logs, and exit markers.

Next define a narrow form-line comparison population from this evidence and
establish unique/effective membership and report/account coverage. The profile
does not establish publisher-key uniqueness or authorize a new financial delta.
Keep the older published facts and readiness review unchanged until the separate
refresh/reuse and comparison contracts pass.
