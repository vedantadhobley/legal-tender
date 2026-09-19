# FEC classic-flow and Schedule E audit

> **Observed:** 2026-08-31  
> **Purpose:** Measure the legacy `pas2`/`oth` flow inputs, inspect the real
> processed Schedule E corpus, and select sources for the next Go facts. This
> is point-in-time evidence, not the calculation contract.

## Result

Classic `pas2` must not become another money ledger beside `oth`: every one of
the 703,597 local 2024 `pas2` `SUB_ID`s occurs exactly once in `oth`. `pas2`
adds candidate context to that selected subset.

The legacy graph also combined sender-side and receiver-side committee reports
without reconciliation. Only 427 rows among its measured transfer cohorts had
the same reversed endpoints, date, and amount. Even those are only possible
two-sided reports, not proof of one economic transaction.

Processed Schedule E is accepted as the independent-expenditure occurrence
authority. It is small, updated weekly, carries the required purpose, payee,
time, action, and lineage fields, and preserves cents that current `pas2`
loses. The effective IE calculation remains unaccepted.

Schedule B remains gated. The first flow graph can use accepted Schedule A
receipt facts. Full sender-side coverage and receipt/disbursement
reconciliation require the Schedule B audit later; this audit does not declare
classic data equivalent to Schedule B.

## Exact inputs

### Local 2024 classic snapshot

| Input | ZIP bytes | Member bytes | Member SHA-256 | Rows | Invalid |
|---|---:|---:|---|---:|---:|
| `pas2.zip` / `itpas2.txt` | 24,683,218 | 122,711,206 | `4b4b9a41274d9b29d8d3dd78426ce39f3a838b8f98b4cbde0672d08ddaaa41b8` | 703,597 | 0 |
| `oth.zip` / `itoth.txt` | 505,214,543 | 3,284,045,258 | `51bb04a5107ef269baf852da3e643700dfb8490313c8a40bcbc43c04577d8745` | 18,667,435 | 0 |

The current 2024 `pas2` URL still returned the same member dated 2026-03-08
and the same member digest. Its object was last modified 2026-03-08. Therefore
the 2024 classic product had not incorporated later Schedule E revisions by
the audit date.

### Current processed Schedule E

- request URL:
  `https://www.fec.gov/files/bulk-downloads/data-dump/schedules/fec_fitem_sched_e.dump`;
- source last modified: 2026-08-30 11:03:25 UTC;
- S3 version ID: `5OLl8Fovh28onZQkS4vSfHXFtoyWERLD`;
- bytes: 43,384,475;
- SHA-256:
  `506abf832b98bfd5e366413a9d31ccd8fc1947aabd22130f77c123b5fa30996f`;
- dump database: PostgreSQL 15.10, produced by `pg_dump` 15.16;
- one `fec_fitem_sched_e` table and data entry; and
- 548,318 rows from election cycle 1976 through 2026, with 548,318 unique
  `SUB_ID`s.

The official
[processed-dump README](https://www.fec.gov/files/bulk-downloads/data-dump/schedules/README.txt)
states that the dump is updated weekly. The official
[Schedule E layout](https://www.fec.gov/files/bulk-downloads/data-dump/schedules/README.txt)
contains 80 fields including spender, payee, purpose, candidate, support or
oppose, expense and dissemination dates, exact amount, action code,
transaction lineage, source document, and election cycle.

### Current 2026 classic `pas2`

- source last modified: 2026-08-30 15:59:17 UTC;
- S3 version ID: `7bji.jvntvQbxZ8py3ZuH98h15qb1RUM`;
- ZIP bytes: 7,905,739;
- ZIP SHA-256:
  `042f38561f55df428dbd7e9217136fdfe581fed519460530563b21de2cf79e48`;
- member bytes: 35,621,158;
- member SHA-256:
  `3dcdd2f73d67a4e88ce2a72f9dad2d0bd8813d8de26b7cc0874a590f4ae4686a`;
  and
- 203,945 rows, including 2,902 `24A` and 12,213 `24E` rows.

## Complete 2024 classic scan

The reusable Go command completed in 17.735 seconds:

```text
legal-tender pipeline fec audit-classic-flows \
  --period 2024 \
  --pas2 /storage/raw/2024/pas2.zip \
  --oth /storage/raw/2024/oth.zip
```

It hashes the exact members, strictly validates all rows, groups physical
shapes without collapsing them, measures exact `SUB_ID` membership, reproduces
named legacy predicates, and measures possible two-sided and repeated-key
evidence.

### `pas2` is exactly a selected `oth` projection

| Measure | Result |
|---|---:|
| Unique `pas2` `SUB_ID`s | 703,597 |
| Duplicate `pas2` `SUB_ID` rows | 0 |
| `pas2` IDs found in `oth` | 703,597 |
| `pas2` IDs absent from `oth` | 0 |
| `oth` rows matching a `pas2` ID | 703,597 |

The equality holds for every observed `pas2` transaction type. A target model
may retain the `pas2` candidate assertion, but cannot count its shared amount
again.

### Legacy transfer cohorts

| Legacy predicate | Rows | Signed amount |
|---|---:|---:|
| `pas2` `24K`/`24P`/`24Z` sender projection | 612,713 | $502,816,678.00 |
| `oth` selected receipt-code projection | 83,351 | $3,719,701,387.00 |

The cohorts produced 403,194 distinct `pas2` sender signatures and 82,450
distinct `oth` receiver signatures. Only 419 signatures overlapped, covering
427 possible row pairs and $3,005,498.00. Endpoint/date/amount agreement alone
does not authorize a merge; it only creates a reconciliation candidate.

The broader `oth` type profile also exposes receipt and disbursement
perspectives that the Python predicate omitted, including `24G`, `30G/K`,
`31G/K`, and `32G/K`. The old allowlist is parity evidence, not a complete
target ontology.

### Legacy independent-expenditure cohort

The Python-equivalent `pas2` `24A`/`24E` cohort had:

- 77,854 rows and $4,497,565,949.00 signed amount;
- 8,601 memo-X rows;
- 8,145 rows without a transaction date;
- 74,434 distinct filer-plus-transaction-ID keys; and
- 2,987 repeated keys covering 6,407 rows.

This invalidates the legacy rule that simply summed all rows by spender,
candidate, stance, and cycle.

## Schedule E comparison

### 2024 freshness and identity

The current Schedule E relation has 67,292 rows assigned to 2024 and a signed
amount of $4,415,348,283.90. Against the still-current March 2024 `pas2`
artifact:

| Identity state | Rows |
|---|---:|
| Exact shared IE `SUB_ID` | 66,421 |
| Old `pas2` IE ID absent from current Schedule E | 11,433 |
| Current Schedule E row absent from old `pas2` | 871 |

None of the old `pas2` IDs were recovered through Schedule E `orig_sub_id`.
Because the products have different publication times, these counts describe
the observed drift; they do not by themselves classify each row as amendment,
removal, or addition.

### 2026 same-day precision and coverage

The current artifacts were both published on 2026-08-30. They share 14,732
exact IE `SUB_ID`s. Candidate ID and transaction type agree on every shared
row. Amounts do not:

| Measure | Result |
|---|---:|
| Shared rows with different amount | 9,367 |
| Largest absolute difference per row | $0.99 |
| Rows reported as `$0.00` by `pas2` but nonzero by Schedule E | 535 |
| Aggregate `pas2` precision loss across shared rows | $4,576.32 |

The samples show that `pas2` emits integer-dollar values such as `$60.00`
where Schedule E retains `$60.99`. This is systematic source precision loss,
not a floating-point artifact in the audit.

Coverage also differs:

| State | Rows | Signed amount |
|---|---:|---:|
| `pas2` IE rows absent from Schedule E | 383 | $4,492,676.00 |
| Schedule E rows absent from `pas2` | 203 | $2,939,628.01 |

Thirty-five Schedule E-only rows have no `exp_tp`; those anomalies remain
source facts and cannot be forced into support or opposition. The other
Schedule E-only rows span action states and memo states. This audit does not
silently discard either side or infer an amendment relationship from amount
similarity.

## Decision consequences

1. Accept processed Schedule E as the one recurring IE occurrence authority.
2. Preserve `pas2` as candidate-context and comparison evidence keyed by its
   shared `oth` `SUB_ID`; do not publish another money fact for that projection.
3. Keep 24/48-hour reports separate for timeliness and filing-chain research.
   The FEC explicitly warns that their original and amended reports duplicate
   transactions.
4. Define and fixture-test the Schedule E source contract before release
   expansion.
5. Define the effective IE calculation from action, filing, transaction,
   amendment, memo, and time evidence; do not revive the legacy sum.
6. Build the first PAC-flow graph from accepted Schedule A receipt facts.
7. Audit Schedule B before adding sender-side disbursement facts or claiming
   complete two-sided flow reconciliation.

The corresponding target boundary is the
[FEC flow fact requirements](../design/fec-flow-fact-requirements.md).
