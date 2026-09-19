# Summary/receipt readiness gate — 2026-09-10

Status: passed for the manual [Go readiness review](../design/summary-receipt-compatibility.md).
This gate publishes no financial result. All numeric comparison deltas remain
null, and comparison/funding/terminal eligibility remains false.

## Exact inputs

The command uses the verified 2024 receipt inventory calculation
`e31d7e8248ad6594c6ce23f86a0fe0cd2ff0ac13a0e78d079c97562519416985`,
over Schedule A fact set
`8f69a2ef40316f62d8844ecf732342a3b4535136b434f60677d0608cb04465df`.
Its receipt manifest conserves 264,085,606 facts in 265 shards.

The summary fact set is
`603d086eb26baa5a9a99d7a717ec5b7469098c173d2d3eabaa119c00d9b7f637`;
regenerating its grouping reproduces assertion calculation
`cfa50f9bbc4b64384cbc973951288be0a426d1a8994dae5824d3e6f09a0fad13`.

These inputs have **different** source releases:

- Receipts: `fec-a6e6f777c3c506b100a7709815bc89f2ae84749faf4343a4589f7ec1bac066a2`.
- Summaries: `fec-01f93a786b630be40a932543f35251bcb56c42b32d1ae85ac34a101ddc7b56b2`.

Each result binds both exact source-manifest digests. Sharing cycle 2024 does not
make these a same-release comparison. No ancestry was relabeled or substituted.

## Real review cases

The three discrepancy witnesses come from the
[original-report investigation](./summary-report-review-2026-09-10.md).
The receipt-bearing example comes from the
[prior inventory gate](./committee-funding-basis-2026-09-08.md).
The final ID exercises absent data. These IDs are test selections, not runtime rules.

| Committee | Selected Schedule A rows | Summary variants | Result |
|---|---:|---:|---|
| `C00075820` | 723,039 | 1 | Observations retained; source-release comparison blocked. |
| `C00843367` | 81 | 1 | Observations retained; source-release comparison blocked. |
| `C00249581` | 2 | 1 | Observations retained; source-release comparison blocked. |
| `C00000935` | 883,974 | 1 | Observations retained; source-release comparison blocked. |
| `C99999999` | 0 | 0 | No receipt rows or indexed summary in these snapshots; no zero-funding inference. |

The shared verified reader and summary grouping produced all five cases with
identical replay in 15.084 seconds, including initial source verification.
These five JSON artifacts total 59,023 bytes. The complete receipt corpus is
not copied into them, and no Schedule A transaction-row scan was added.

Every component/role measure, individual-predicate measure, and overlap measure
matches the existing inventory. Overlap contributes once to the individual
predicate; the overlap diagnostic must not be added again. Unknown amounts and
signed negative subtotals remain unchanged.

All selected summary field lexemes, typed values, occurrence members, coverage
values, representatives, and diagnostic residuals match the earlier independently
verified assertion artifact. The known source problems remain visible; no
committee-specific correction or favorable report selection was introduced.

The real CLI also completed for `C00000935`, using the public reader path and
the same exact inputs. Its decoded output matches the direct review result.

## Tests and retained evidence

Go focused tests, the full Go suite, static analysis, and race tests pass.
The funding-basis race package completed in 19.849 seconds; the CLI race package
in 1.039 seconds. The real-data job ran from 03:18:32 to 03:19:28 UTC, including
focused/corpus/race/regression checks, binary build, and CLI verification.

Synthetic cases exercise same-release inputs without promoting readiness,
conflicting source digests, mixed cycles, cancellation, absent populations,
negative and unknown amounts, overlap conservation, blanks, field versus scope
conflicts, unavailable/reversed/out-of-cycle dates, and federal sensitivity.
A compiled-policy test checks every field mapping against the pinned policy.

The independent Python gate passed seven tests in 1.79 seconds. Two unrelated
earlier corpus opt-ins were skipped; the new real five-case check ran. It verifies
the result schema/content identity and independently regroups each committee's
receipt buckets and matches every summary field/member to the earlier artifact.
Wire tests reject filled comparison deltas and promoted eligibility flags.
Python remains test-only, not runtime policy.

Initial attempts exposed a production image without pytest, an import-format
issue, a wrong cross-contract schema ID, and a synthetic offset that failed the
existing source schema. The corrected test uses the cached development image,
registers the inventory's exact existing `.dev` schema ID locally alongside
the summary `.local` IDs, and uses a valid fixture locator. No global schema-ID
migration or source change was needed. Earlier failure logs remain retained.
All final checks pass: `go.exit=0` and `python-pass.exit=0`.

Evidence lives under
`dumps/audits/fec/summary-receipt-readiness/2026-09-10/attempt-01/`
in project storage: inputs' exact references, five reviews, synthetic fixture,
CLI result, source snapshot, schemas, binary, scripts, logs, and exit markers.
Tests use offline disposable containers, with source storage read-only.
The Go gate has a 4 GiB container cap and 2 GiB Go target; independent tests
have a 2 GiB cap. No service or persistent memory-budget change was made.

The active v4 manifest digest remains
`b030668efca44fcf611b204665d95c00fa7eda9ce3ce31b5711ebbba6805e81c`.
No acquisition, extraction, raw/fact mutation, calculation pointer, graph edge,
Dagster wiring, schedule, or deletion occurred.

## What remains

This gate assesses readiness, not numerical equivalence. Even matching releases
would still lack verified receipt reporting-period coverage and report/account
scope; the field-specific form/population blockers are independent of that.

Next establish exact compatible ancestry and report/form-line coverage for the
itemized-individual predicate in a bounded cycle-wide pass. That is the first
candidate for an accepted reported-subtotal comparison. Broader funding-family
coverage, corrections, effective-report selection, cash continuity and timing,
terminal allocation, and recurring publication remain separate gates.
