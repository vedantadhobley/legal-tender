# Processed Schedule E source-contract audit

> **Status:** Complete source-schema and parser gate for the 2026-08-30 dump.
> Coordinated v2 release, selected-cycle lossless publications, and the
> separate four-cycle effective and identity gates now pass. The source
> contract is accepted.

## Result

The processed Schedule E source now has a machine-readable 80-column contract,
strict bounded-memory Go parser, exact real-row fixtures, command boundary, and
complete-corpus verification.

No row failed physical framing, UTF-8, COPY escape, required-field, numeric
precision/scale, integer precision, timestamp, or source-cycle validation. The
audit did not select effective expenditures and did not mutate ArangoDB.

## Artifact and relation

| Measure | Observed value |
|---|---:|
| Source object last modified | 2026-08-30 11:03:25 UTC |
| Custom dump bytes | 43,384,475 |
| Custom dump SHA-256 | `506abf832b98bfd5e366413a9d31ccd8fc1947aabd22130f77c123b5fa30996f` |
| Relation | `disclosure.fec_fitem_sched_e` |
| Ordered fields | 80 |
| COPY rows | 548,318 |
| COPY bytes | 367,801,873 |
| Restored-table COPY SHA-256 | `eb21e03134c3a8cfca9ec0b59d7658730af33c828239ff87bbf7ebd4ec256247` |
| Direct-dump COPY SHA-256 | `a9f3589385fcfabf39288fc15a72a072120b2a61575801ce713ef2644766e09d` |
| Canonical sorted-row SHA-256 | `e084ec063a887538bee751d85c81547b0a6b3d641d1259bfb382eb27417e8abd` |
| Invalid rows | 0 |
| Verification runtime | 1.298 seconds |

The exact relation DDL and verification identity are pinned in the
[archive observation](../../contracts/sources/fec/schedule-e/v1/fixtures/archive/dump-2026-08-30.json).

The initial verification restored the relation and streamed `COPY TO STDOUT`.
The release stager later preserved the custom dump's direct data-row payload.
The two streams have identical row counts, byte counts, ordered columns, and
complete sorted-row digest. Their ordered SHA-256 values differ because a
valid row first moves at row 302 after restore. This is physical heap-order
drift, not content drift. See the
[v2 publication audit](./schedule-e-v2-publication-2026-08-31.md).

## Corpus shapes preserved

| Shape | Rows |
|---|---:|
| Negative `exp_amt` | 3,112 |
| Non-whole-dollar `exp_amt` | 294,953 |
| Null `exp_amt` | 11 |
| Null `exp_dt` | 25,332 |
| Null `dissem_dt` | 203,298 |
| Null `exp_tp` | 2,895 |
| Memo code `X` | 45,669 |
| Action `A` | 207,930 |
| Action `C` | 203,153 |
| Action `N` | 136,898 |
| Action `T` | 336 |
| Null action code | 1 |

The relation contains cycles 1976 through 2026. The selected product cycles
contain 75,884 rows for 2020, 68,356 for 2022, 67,292 for 2024, and 14,935 for
2026. Cycle is a row-level publication dimension; it is not a physical
relation or transport partition.

These distributions are source evidence, not counting rules. In particular,
the parser conserves null types, null amounts, memo rows, negative values, and
every action state rather than choosing which records contribute to a total.

## Implementation

- [`fec/schedule-e@1.0.0`](../../contracts/sources/fec/schedule-e/v1/) defines
  acquisition, relation schema, identity, time, money, revision, quality, and
  use rules.
- [`internal/source/fec/schedulee/`](../../internal/source/fec/schedulee/)
  owns the compiled schema, record validation, exact-source freezing, and
  verification profile.
- [`internal/source/fec/copytext/`](../../internal/source/fec/copytext/) is the
  shared PostgreSQL COPY decoder now used by Schedule A and Schedule E.
- `legal-tender pipeline fec verify-schedule-e` accepts data-row-only COPY text
  from a file or standard input and emits versioned JSON.

Exact fixtures cover fractional cents, memo X, missing expenditure type,
negative money and original-submission lineage, and an escaped tab. Tests bind
the compiled field order and types to the observed DDL, bind record-schema
membership to all 80 fields, and verify every fixture digest.

## Release boundary implemented

The prior release staging model assumed each selected PostgreSQL relation
mapped one-to-one to a two-year period. That was correct for Schedule A's four
inherited relations and wrong for Schedule E's single all-history relation.

Schedule E entered release-inventory v2 with:

1. one acquired custom dump object;
2. one staged all-history relation extract;
3. exact artifact and extract identities shared by every selected cycle; and
4. four downstream occurrence/fact publications filtered by the preserved
   `election_cycle` value.

This keeps acquisition grain, physical relation grain, and query partition
grain separate. Existing release-inventory v1 membership must not be silently
rewritten.

## Remaining gate

Define and corpus-test the separate effective-independent-expenditure
calculation before publishing candidate outside-spending totals.

## Sources

- [FEC processed schedule dump README](https://www.fec.gov/files/bulk-downloads/data-dump/schedules/README.txt)
- [FEC independent expenditures file description](https://www.fec.gov/campaign-finance-data/independent-expenditures-file-description/)
- [FEC transaction type descriptions](https://www.fec.gov/campaign-finance-data/transaction-type-code-descriptions/)
