# Effective independent-expenditure publication — 2026-08-31

## Result

The first effective independent-expenditure calculation is accepted and
published for cycles 2020, 2022, 2024, and 2026. It reads only the exact
lossless processed Schedule E fact set for one cycle, uses exact signed cents,
and publishes immutable spender-candidate-support/oppose results plus sparse
exceptions. It does not mutate ArangoDB and it does not classify this spending
as candidate-controlled receipts.

The machine contract is
[`effective-independent-expenditures/v1`](../../contracts/calculations/fec/effective-independent-expenditures/v1/).
Go owns the predicate, arithmetic, conservation, and artifact publication.
Python only exposes the partitioned Dagster asset and invokes the Go command.

## Accepted publisher semantics

The selected source is `disclosure.fec_fitem_sched_e`, the processed regular-
report Schedule E relation already captured by coordinated release v2. The FEC
implementation treats regular-report `fec_fitem` rows as most recent, while it
derives a separate most-recent state for notice rows. See the
[OpenFEC Schedule E most-recent migration](https://github.com/fecgov/openFEC/blob/develop/data/migrations/V0173__add_most_recent_to_ofec_sched_e_mv.sql).

The FEC candidate aggregate reads `fec_fitem_sched_e`, requires a reported
amount, excludes memo-code `X`, and keeps the signed sum. See the
[OpenFEC Schedule E aggregate migration](https://github.com/fecgov/openFEC/blob/develop/data/migrations/V0119__update_sched_c_d_e_f_and_related_tables.sql).
The [FEC independent-expenditure file description](https://www.fec.gov/campaign-finance-data/independent-expenditures-file-description/)
also warns that the separate 24/48-hour file retains original and amended
notices. These sources therefore cannot be concatenated or independently
summed.

Legal Tender applies this versioned rule:

1. Exclude memo-code `X` from the effective amount, but retain the fact and
   diagnostic amount.
2. Include every other fact with an exact reported amount. Negative values
   reduce the signed total; zero remains a counted fact.
3. Preserve null or invalid non-memo amounts as unresolved exceptions. Do not
   invent zero.
4. Do not select a local action-code winner or deduplicate repeated spender-
   transaction keys. The processed relation is the publisher-selected regular-
   report view; those fields remain evidence.
5. Attribute an included amount only when spender committee, candidate, and
   `S` or `O` are present. Preserve every unrouteable included amount as an
   exact unattributed exception.
6. Group attributed results by spender committee, candidate, support/oppose,
   and cycle. Support and opposition remain separate.
7. Reject a selected fact set containing a 24/48 report type or F24 notice.

## Four-cycle publication

| Cycle | Facts | Included | Memo X | Attributed | Unattributed | Result groups | Included amount | Calculation set |
|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 2020 | 75,884 | 67,711 | 8,173 | 67,365 | 346 | 6,558 | $3,154,555,705.58 | `db65b2f37db8...` |
| 2022 | 68,356 | 62,212 | 6,144 | 61,965 | 247 | 6,345 | $2,207,958,606.26 | `f47d3f134f02...` |
| 2024 | 67,292 | 58,809 | 8,483 | 58,288 | 521 | 5,495 | $4,340,433,912.51 | `315227127d57...` |
| 2026 | 14,935 | 13,824 | 1,111 | 13,632 | 192 | 2,108 | $662,366,117.25 | `16650f50c32b...` |
| **Total** | **226,467** | **202,556** | **23,911** | **201,250** | **1,306** | — | **$10,365,314,341.60** | — |

Across the four cycles, attributed results conserve
$10,358,126,288.01 and sparse unattributed exceptions conserve $7,188,053.59.
Their sum equals the exact included amount. All cycles had zero invalid source
facts, zero unresolved non-memo amounts, zero missing spender IDs, and zero
notice-like facts. Missing candidate IDs and invalid support/oppose codes remain
visible route diagnostics and can overlap on one exception.

The 2024 replay returned the original calculation-set ID, run ID, publication
time, result digest, and exception digest. Publication is idempotent for an
unchanged fact-set identity and calculation version.

## Notice comparison

The official 24/48 CSVs were downloaded as comparison evidence only. A
spender-plus-transaction-key scan produced:

| Cycle | Notice rows | Non-`N` amendment rows | Distinct notice keys | Repeated notice keys | Shared processed keys |
|---:|---:|---:|---:|---:|---:|
| 2020 | 80,766 | 12,337 | 66,471 | 6,955 | 35,552 |
| 2022 | 61,779 | 6,422 | 52,107 | 5,142 | 32,359 |
| 2024 | 73,449 | 12,876 | 59,524 | 9,016 | 36,222 |
| 2026 | 14,469 | 1,444 | 13,031 | 1,016 | 5,583 |

This is not a record reconciliation. Transaction IDs repeat, notice estimates
and later regular-report values need not be byte-identical, and the two
products have different filing behavior. The overlap is sufficient to prove
that adding notice amounts to processed Schedule E would create substantial
double-counting risk. Notices remain useful for timeliness and filing-chain
investigation.

## Publication gates

Every cycle passed these blocking checks:

- exact fact-manifest lineage and backing-artifact verification;
- notice-source separation;
- one decision per source fact;
- included amount routed to attributed or unattributed exactly once;
- exact signed-cent conservation through grouped results; and
- no dense per-row decision artifact.

Repeated transaction keys and missing expenditure types are retained as
warnings, not silently corrected. The next graph step can project these
results as outside-spending edges with evidence lineage. It must not add them
to candidate receipts or infer coordination, influence, or corruption from
the reported spending alone.
