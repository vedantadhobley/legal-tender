# Complete-cycle report-line profile v2 — 2026-09-10

The [v2 Go profile](../design/receipt-report-profile-v2.md) conserves all
264,085,633 physical 2024 Schedule A occurrences from the exact release selected
by the published committee-summary facts. The full row/byte/hash verification
passes. Independent output conservation, source binding, and exact v1 regrouping
also pass.

This is a source-aligned occurrence diagnostic, not an accepted cycle-summary
financial comparison, effective report selector, or terminal-funding calculation.
No receipt source, fact set, graph, API, or Dagster definition changes.

## Inputs and verification

The summary calculation, selected relation, and source release are exactly those
of the [v1 full-cycle gate](./receipt-report-profile-2026-09-10.md). Active source
release `fec-01f93a786b630be40a932543f35251bcb56c42b32d1ae85ac34a101ddc7b56b2`
has manifest SHA-256
`b030668efca44fcf611b204665d95c00fa7eda9ce3ce31b5711ebbba6805e81c`.
Older accepted Schedule A Parquet facts remain untouched and are not relabeled.

One offline pass recomputed the selected relation's physical evidence:

- 264,085,633 rows, all valid under the pinned COPY source contract.
- 14,773,841,175 compressed bytes; 182,881,318,759 uncompressed bytes.
- Compressed SHA-256:
  `649e2b4e3617938a98f46e69d91ce6aa028b443a9a8d2f3f49e47a1581ee2e81`.
- Uncompressed SHA-256:
  `e1390338f92ec88f0a0e50ef02d01ac9297b90c602f6b1af9bdde882f2a07a3e`.

The full scan took 563,519 ms (9m 23.519s). The job used four CPUs, a 4 GiB
container cap, a 2 GiB Go target, and a 1 GiB per-artifact write limit. All source
storage was read-only. Output was renamed from a partial path only after the Go
command succeeded. Both source-pointer hashes match; the active release did not
change. The full Go suite, vet, and focused funding-basis/CLI race tests pass.

## Scope

Every physical occurrence now appears in a report-line group with independent
raw memo status, publisher individual flag, old-predicate decision, and reviewed
line disposition. Both form and report views conserve all amount/count measures.
Dates are profiled for excluded and unresolved rows as well as included rows.
No original row, unknown value, or out-of-cycle date is silently dropped.

First/last source ordinals are group extrema, not exhaustive membership lists.
The two tables and their overlapping diagnostic subsets must not be added. The
new profile is versioned; v1 remains available through the default command.
The reviewed form-line rule is reused from the
[bounded original-report gate](./receipt-report-lines-2026-09-10.md), not copied
into a new exception table or parser.

The output contains 107 form groups and 166,793 report-line groups, covering
73,277 committee/file pairs and 9,991 reported committee IDs. Compared with v1's
included-individual view, it exposes 7,198 additional report references and 994
additional committees from the **same source bytes**, not newly acquired data.
All references have syntactically valid committee IDs and positive canonical
file numbers. Each pair has one observed form/report-type/report-year shape.
These are source-reference checks, not original-report or amendment validation.

| Reviewed line disposition | Physical occurrences |
|---|---:|
| Reviewed non-memo F3/F3X SA11AI | 220,190,116 |
| Memo subtotal on that form-line | 15,496,488 |
| Other form-line scope | 28,398,965 |
| Unresolved reviewed-line amount | 2 |
| Unresolved reviewed-line memo code | 62 |

All 62 unresolved memo-code occurrences carry raw `Y`. The policy does not
silently interpret this as `X` or blank. The reviewed non-memo line population
includes 3,061,759 rows with publisher `is_individual=false`; that flag is not
used to define form-line membership. None of these occurrence measures proves
cash funding or terminal attribution.

All-occurrence receipt-date counts are 263,522,163 within the cycle, 563,218
before it, 28 after it, 224 missing, and zero invalid normalized dates. These
counts conserve all source occurrences and do not establish report coverage.

The result is 266,269,624 JSON bytes. Its profile ID is
`a84e5ad04a315a2dfedc575ac44aecbba8c816b8265a76b2a9a69cc810b7dcc7`;
file SHA-256 is `acce0a13f2d87abfe78beb66c6bd2ae60f82a53bf656b3745343966ae57f3647`.
The verbose output and general-purpose JSON Schema validation are audit costs,
not accepted recurring storage or per-run processing requirements.

## Independent gate and limits

The independent tests validate the v2 schema, content identity, source ancestry,
every group's measures and date states, and complete regrouping to the retained
same-source v1 output. That comparison covers every prior form and included-report
group, not just global totals, and includes ordinal/date extrema. It does not
independently parse the full raw corpus or rerun the complete source scan.

Five Python checks passed in 762.11 seconds. One separate full-v1 test was skipped
because v1 was not rescanned; its retained artifact hash and every grouped measure
were verified by the v2 comparison. Ruff passes. The Go and independent job exit
markers are both zero. The Go job ran from 06:09:50 to 06:20:03 UTC.

General-purpose schema validation dominates this independent audit cost. A
bounded 1,000-report-group benchmark took 4.717 seconds with the composed key
schema and 2.810 seconds with an equivalent flat-key schema. This is a bounded
measurement, not a full-corpus optimization result. The verified wire contract
was not changed based on that benchmark; compact recurring format and cheaper
complete validation remain deferred work before automation.

Evidence is retained under
`dumps/audits/fec/receipt-report-profile-v2/2026-09-10/attempt-01/` in project
storage: complete profile, exact Go code/binary and test/schema snapshots,
synthetic fixtures, report-reference scope, commands/logs, source-pointer hashes,
bounded schema benchmark, exit markers, and verified tree checksums.

Publisher-key and transaction-ID uniqueness, effective-report selection, complete
original-filing coverage, report/account membership, and cycle-summary comparison
remain unverified. Both eligibility guards stay false. The next work is to inspect
the complete reported-file population and define the missing report/account and
amendment evidence; a file number or receipt-date envelope alone cannot supply it.

Follow-up: the [bounded memo/amount review](./receipt-memo-review-2026-09-10.md)
now traces all 64 reviewed-line exceptions to original-source evidence and verifies
their full selected-report groups without another corpus scan. It preserves
disagreements, not automatic fixes; report metadata qualification remains next.
