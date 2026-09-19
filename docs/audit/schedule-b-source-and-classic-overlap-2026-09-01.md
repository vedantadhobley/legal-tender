# Processed Schedule B source and classic-overlap audit

> **Observed:** 2026-09-01  
> **Scope:** Official processed Schedule B archive, exact 2024 inherited
> relation, and current 2024 classic `pas2` and `oth` comparison artifacts.  
> **Disposition:** The physical source, strict parser, and classic comparison
> pass. Sender-side authority remains `draft` until processed Schedule A and
> Schedule B are compared from one coordinated publisher snapshot.

## Result

Processed Schedule B is a valid separate sender-reported source. The exact
2024 relation contains 157,544,163 rows and 123,284,784,602 uncompressed COPY
bytes. Every row has exactly 81 fields, the selected 2024 period, a unique
`SUB_ID`, and valid source lexemes. No row was invalid and no `SUB_ID` was
duplicated.

The classic comparison confirms orientation without creating another money
ledger. On every shared `SUB_ID`, classic `CMTE_ID` equals the Schedule B
sending `cmte_id`. Whenever the classic counterparty is a committee ID, it
equals Schedule B's raw recipient committee ID. Every comparable classic
amount either matches Schedule B exactly or equals Schedule B after
whole-dollar truncation. There are zero unexplained amount conflicts.

This does **not** accept Schedule B as production authority yet. The processed
Schedule B object was published on 2026-08-30, while the retained processed
Schedule A object was published on 2026-08-23. Cross-object reconciliation at
different observation times would mix source drift with semantic disagreement.
The contract therefore remains `draft`, and Schedule B does not enter the
coordinated release inventory in this change.

## Exact inputs

### Processed Schedule B

| Measure | Value |
|---|---|
| Request | `https://www.fec.gov/files/bulk-downloads/data-dump/schedules/fec_fitem_sched_b.dump` |
| Last modified | `2026-08-30T15:21:54Z` |
| S3 version | `JARG_mL4APJoVPy8FUvwiTILidolHqfY` |
| ETag | `8a3ff60c48c379919875a096bc13c732-4687` |
| Archive bytes | 39,310,353,867 |
| Archive SHA-256 | `39669f3c6c19d6f5076648f734f6456e0f8852d6cf37f0133e283fbfce91ac72` |
| Archive creation | `2026-08-30T11:02:00Z` |
| Target relation | `disclosure.fec_fitem_sched_b_2023_2024` |

The archive is a PostgreSQL 15 custom dump with 918 catalog entries, one
parent relation, and 26 inherited two-year partitions from 1975/1976 through
2025/2026. The official
[processed-schedule README](https://www.fec.gov/files/bulk-downloads/data-dump/schedules/README.txt)
describes the all-history A/B/E dumps and their weekly publication model.

The complete archive observation is pinned in the
[source fixture](../../contracts/sources/fec/schedule-b/v1/fixtures/archive/dump-2026-08-30.json).

### Classic comparison artifacts

The current official 2024 classic objects were still the versions published
on 2026-03-08. They were downloaded again with pinned ETags for this audit.
Their bytes and member digests equal the older local copies.

| Input | ZIP bytes | ZIP SHA-256 | Rows | Member SHA-256 |
|---|---:|---|---:|---|
| `pas224.zip` / `itpas2.txt` | 24,683,218 | `81520f5d1371f2e89beecd148193e55fdcff7f1dfa6b3a63770e21b928bcbfaf` | 703,597 | `4b4b9a41274d9b29d8d3dd78426ce39f3a838b8f98b4cbde0672d08ddaaa41b8` |
| `oth24.zip` / `itoth.txt` | 505,214,543 | `0f16299c773b91a9fc4e815c1f220a4e265cac5888b7f9ca02feaab81c791b81` | 18,667,435 | `51bb04a5107ef269baf852da3e643700dfb8490313c8a40bcbc43c04577d8745` |

Both members have zero invalid rows, zero duplicate `SUB_ID`s, and exact
member-byte conservation.

## Complete 2024 Schedule B profile

| Measure | Result |
|---|---:|
| Physical rows | 157,544,163 |
| Unique `SUB_ID`s | 157,544,163 |
| Invalid rows / duplicate IDs | 0 / 0 |
| Uncompressed COPY bytes | 123,284,784,602 (114.82 GiB) |
| COPY SHA-256 | `9ee6eda0ce36d9e3bebbfa9b7d4908456300103ebd392716b3608a300110e239` |
| Negative amounts | 211,035 |
| Amounts with nonzero cents | 37,986,298 (24.11%) |
| Null amounts / disbursement dates | 0 / 0 |
| Raw recipient committee ID | 152,454,315 (96.77%) |
| Clean recipient committee ID | 152,356,465 (96.71%) |
| Candidate ID | 41,434,670 (26.30%) |
| Purpose | 157,479,644 (99.96%) |
| Transaction ID | 157,538,857 (99.997%) |
| Signed source amount | $23,765,207,537.31 |

The signed source amount is a physical-row conservation measure, not an
effective-disbursement or outgoing-flow total. It includes all action and memo
states and must not be presented as spending.

Raw and cleaned recipient IDs never conflict when both exist. The cleaner
only removes or withholds values in this corpus; it does not rewrite one
committee ID to a different committee ID. Both assertions remain preserved.

Only 2,104,519 rows (1.34%) carry a non-null `disb_tp`. The source contract
therefore cannot define sender-side flow by transaction type alone. Purpose,
line, recipient, entity, memo, action, form, and transaction lineage remain
source evidence for a later explicit calculation.

The corpus also contains 1,106,842 memo-X rows, 41 memo-Y rows, 63,617 action-C
rows, and 1,094,684 action-N rows. Source ingestion preserves them. It does
not infer which record is effective.

## Classic overlap and direction

Classic `pas2` remains an exact `SUB_ID` subset of classic `oth`. Schedule B
shares 386,516 IDs with `oth`; 273,764 of those also occur in `pas2`.

| Membership | Schedule B rows | Signed amount |
|---|---:|---:|
| `pas2` and `oth` | 273,764 | $528,273,886.64 |
| `oth` only | 112,752 | $5,212,084,455.47 |
| Neither classic product | 157,157,647 | $18,024,849,195.20 |

The observed overlap is 38.91% of classic `pas2`, 2.07% of classic `oth`, and
0.245% of Schedule B. One-sided membership is expected because the products
select different record populations and have independent publication times.
It is not evidence that one source should patch the other.

### Endpoint orientation

| Check | `pas2` shared rows | `oth` shared rows |
|---|---:|---:|
| Shared `SUB_ID`s | 273,764 | 386,516 |
| Schedule B sender = classic filer | 273,764 | 386,516 |
| Classic counterparty is a committee ID | 273,745 | 374,813 |
| Raw Schedule B recipient = classic counterparty | 273,745 | 374,813 |
| Clean Schedule B recipient = classic counterparty | 273,481 | 374,020 |

The apparent reverse-orientation counts are 264 `pas2` rows and 793 `oth`
rows. They are self-endpoint shapes, not contradictory direction: the same
rows also pass the forward test, and classic filer and counterparty equal the
Schedule B source and raw recipient. The cleaned recipient is absent for those
shapes.

### Amount precision

| Check | `pas2` | `oth` |
|---|---:|---:|
| Comparable amounts | 273,764 | 386,516 |
| Exact cent match | 264,215 | 352,937 |
| Match after classic whole-dollar truncation | 9,549 | 33,579 |
| Other conflict | 0 | 0 |

This confirms the earlier Schedule E finding: classic transaction products
can discard fractional dollars. Processed Schedule B retains exact cents and
must be the amount-bearing sender assertion if its authority gate passes.

## Runtime and storage

The reusable Go audit completed in 591.016 seconds. Strict Schedule B parsing,
hashing, and uniqueness took 556.064 seconds at about 283,320 rows/s and
211.44 MiB/s of uncompressed COPY input. The full run stayed near 2.5 GiB in
the 16 GiB container.

The durable costs are the 36.61 GiB all-history Schedule B archive, 505.5 MiB
of pinned classic ZIPs, and the 10,055-byte canonical audit result. The
114.82 GiB selected COPY stream was never materialized. `SUB_ID` uniqueness
used about 1.17 GiB of temporary fixed-width shards, which were removed after
verification.

Canonical result:

```text
/storage/dumps/audits/fec/schedule-b/2026-08-30/2024/result.json
SHA-256 73b4455fb820935aa135da8d24fd1d07ffb825ac27f57bff47944d82f9c55645
```

## Consequences

1. Keep processed Schedule B separate from Schedule A and all classic files.
2. Accept the physical 81-field boundary, strict parser, exact source
   preservation, and classic direction/precision evidence.
3. Keep `fec/schedule-b@1.0.0` in `draft`; do not add it to the recurring
   release or publish Schedule B facts yet.
4. Acquire processed Schedule A and Schedule B under one coordinated source
   observation, then measure two-sided reconciliation candidates by immutable
   fact identity without adding their amounts.
5. After that gate, define the sender-side effective-record and flow-role
   calculation. Do not encode either policy in source parsing.
6. If accepted, store lossless Schedule B facts in a columnar selected-cycle
   representation. Do not persist or rescan a 114.82 GiB COPY extract for each
   downstream calculation.

The target fact and reconciliation boundaries remain in the
[FEC flow requirements](../design/fec-flow-fact-requirements.md).
