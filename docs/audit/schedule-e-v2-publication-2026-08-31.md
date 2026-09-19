# Schedule E v2 release and lossless publication audit

> **Status:** Accepted source-release, occurrence, and lossless-fact
> publication. Effective independent-expenditure selection remains a separate
> calculation gate.

## Result

The coordinated FEC v2 release is active with processed Schedule E represented
once at its physical all-history grain. Four cycle publications select rows by
the preserved `election_cycle` field. Every selected occurrence produced one
valid lossless fact. No effective-record policy or ArangoDB projection ran.

The active source release is
`fec-76b6660f70406bf0d11537885de172883cdb8af548bfa7df35078a721c8c759a`.
It descends from the v1 release
`fec-a6e6f777c3c506b100a7709815bc89f2ae84749faf4343a4589f7ec1bac066a2`.
Its immutable control-manifest SHA-256 is
`5127484ca20e2e1c9a93604d1dd3bba9413b5e4431b25b0573ff2ef1f442f4e3`.

## Release evidence

| Measure | Value |
|---|---:|
| Inventory | `legal-tender.fec.initial-release-inventory.v2` |
| Source artifacts | 22 |
| Staged outputs | 25 |
| Changed/acquired artifacts | 18 |
| Reused artifacts | 4 |
| Newly staged outputs | 18 |
| Reused outputs | 7 |
| Schedule E source bytes | 43,384,475 |
| Schedule E source SHA-256 | `506abf832b98bfd5e366413a9d31ccd8fc1947aabd22130f77c123b5fa30996f` |
| Schedule E staged rows | 548,318 |
| Schedule E staged bytes | 367,801,873 |
| Schedule E staged zstd bytes | 32,517,795 |
| Schedule E staged SHA-256 | `a9f3589385fcfabf39288fc15a72a072120b2a61575801ce713ef2644766e09d` |

All eight release checks passed: input identity, source-artifact membership,
selected-output membership, output integrity, storage budget, evidence-chain
continuity, active-baseline continuity, and atomic-pointer eligibility. The
active pointer names the immutable v2 manifest.

The final publication verification reread the complete inherited backing set.
This is safe but expensive because the four reused Schedule A extracts total
about 57.97 GB compressed. Avoiding that duplicate scan is an optimization
target; it is not grounds to weaken final backing verification.

## Cycle publications

| Cycle | Occurrences | Occurrence set | Occurrence bytes | Facts | Fact set | Fact bytes |
|---:|---:|---|---:|---:|---|---:|
| 2020 | 75,884 | `2f69e1ebb27493bb14fe10ade42a946bde59a1f84507db22bf141bf8085fd060` | 9,867,638 | 75,884 | `942107919fab18c0363b134236bf09166439ee255415b7324ae00c6046a858f5` | 20,271,607 |
| 2022 | 68,356 | `db8966accff62e1340b3740fb10745e674d224deb9eedcc6a92e1441290addcb` | 8,862,563 | 68,356 | `8e1aab08dd240966c147f9824e9e02d807d494d71e924766b982baa492390041` | 17,452,590 |
| 2024 | 67,292 | `44fd5505bfe03de3080439eda209982a94c69b6d827ffc0d0431230b8595e86f` | 8,722,405 | 67,292 | `f38758f7f151505b892a217c856a3fdc81a93ca0ab0670906baed677951921ee` | 17,060,317 |
| 2026 | 14,935 | `15a76181f693ed4ee04f0c3e2e55c564c8aa8113253c85f527c4f2359cdad33f` | 1,935,620 | 14,935 | `050122feca6f046f263ecdb1e6f11599186ca78b93edd45e17c0fa4d9f4044e0` | 4,028,724 |

The eight compressed publications occupy 88,201,464 bytes. For every cycle:

- the occurrence source-row count is 548,318;
- selected plus other-cycle rows conserves the source row count;
- no row has a null cycle;
- facts equal selected occurrences;
- every fact passed normalization; and
- active pointers are byte-identical to their immutable manifests.

A post-publication pass recomputed every compressed artifact SHA-256 and
confirmed every occurrence and fact check.

## COPY identity reconciliation

The source-contract audit initially measured a restored-table
`COPY TO STDOUT` stream. Release staging preserves the original data rows that
`pg_restore --data-only` emits from the custom dump's `COPY FROM` section.
Both representations contain 548,318 rows and 367,801,873 bytes, but their
ordered byte digests differ:

| Representation | SHA-256 |
|---|---|
| Direct custom-dump COPY payload | `a9f3589385fcfabf39288fc15a72a072120b2a61575801ce713ef2644766e09d` |
| Restored relation `COPY TO STDOUT` | `eb21e03134c3a8cfca9ec0b59d7658730af33c828239ff87bbf7ebd4ec256247` |

The first difference occurs at row 302, where a valid row appears earlier in
the restored heap scan. Sorting complete rows bytewise produced the same
canonical digest for both streams:
`e084ec063a887538bee751d85c81547b0a6b3d641d1259bfb382eb27417e8abd`.
The dump's explicit 80-column COPY header also matches the compiled Schedule E
schema exactly.

This is physical row-order drift, not a source-content difference. Direct dump
order remains the release identity and therefore the basis for global row
ordinals and byte locators. Restored-table order is diagnostic only. Staging
now rejects any Schedule A or Schedule E COPY header whose ordered columns do
not exactly match the compiled source schema.

## Execution boundary

The publications ran through the same contract-validating Python process
adapter used by the Dagster assets. Python launched the Go commands, validated
their versioned JSON manifests, and stored immutable control results. Go owned
the source replay, cycle selection, occurrence identities, lossless
normalization, backing checks, and atomic publication.

The remaining Schedule E gate is the effective-record calculation: resolve
action states, amendment lineage, repeated transaction keys, memo records,
estimates, missing types, and expense versus dissemination time before any
candidate outside-spending total is published.
