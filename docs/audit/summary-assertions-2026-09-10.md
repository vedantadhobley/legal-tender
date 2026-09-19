# Committee-summary grouping and arithmetic investigation — 2026-09-10

Status: complete. The read-only Go grouping, full four-cycle replay, and
independent raw-CSV gates pass. This is an evidence calculation, not financial
publication or repaired source data.

## Exact input and grouping

The calculation consumes the [published v4 summaries](./fec-v4-publication-2026-09-10.md),
not the older research CSVs. The source release is
`fec-01f93a786b630be40a932543f35251bcb56c42b32d1ae85ac34a101ddc7b56b2`,
with manifest SHA-256
`b030668efca44fcf611b204665d95c00fa7eda9ce3ce31b5711ebbba6805e81c`.
Each output also binds its exact summary fact-manifest digest and artifact hash.

The [accepted grouping rule](../design/committee-summary-assertions.md) requires
the same valid committee and exact equality of all 91 non-candidate source fields
within that fact set. It preserves every occurrence as a member. Candidate
references are not separate receipts, and this rule does not merge committees.

| Cycle | Source occurrences | Assertion groups | Repeated evidence rows | Conflicting committees |
|---|---:|---:|---:|---:|
| 2020 | 13,554 | 13,535 | 19 | 0 |
| 2022 | 13,977 | 13,946 | 31 | 0 |
| 2024 | 14,065 | 13,994 | 71 | 0 |
| 2026 | 14,154 | 14,081 | 73 | 0 |

All 55,750 occurrences belong to 55,556 assertion groups; 194 are extra evidence
members, not extra financial amounts. None is deleted. All committee/cycle
identities in these inputs are indexable; invalid candidate references remain
members with their original typed states. No conflicting variants occur in these
snapshots; fixture tests exercise them and retain every variant without selection.

## Arithmetic after grouping

These counts are assertion-grain, not the earlier occurrence-grain profiles.
Each equation's equal/different/missing triple conserves that cycle's groups.
There are no invalid monetary operands in these captured files.

| Cycle | Cash equal / different / missing | Individual subtotal equal / different / missing |
|---|---|---|
| 2020 | 11,581 / 1,494 / 460 | 12,799 / 237 / 499 |
| 2022 | 12,476 / 1,199 / 271 | 13,511 / 146 / 289 |
| 2024 | 12,668 / 1,022 / 304 | 13,489 / 163 / 342 |
| 2026 | 13,127 / 857 / 97 | 13,884 / 64 / 133 |

Grouping does not resolve the source arithmetic. Across cycles, 4,572 assertions
have a nonzero cash difference and 610 have a nonzero individual-subtotal
difference. Missing operands remain missing rather than becoming zero.

For 2024, substituting the separate federal columns makes 17 previously unequal
cash comparisons balance, but makes 491 previously balanced comparisons differ.
This rejects a general "use whichever totals balance" repair. The result records
both diagnostics without choosing one as financial truth.

### Official-source checks

The [FEC dictionary](https://www.fec.gov/campaign-finance-data/committee-summary-file-description/)
describes the CSV as a two-year combination of filing summaries, with cash
opening at the cycle boundary and closing from the most recent filing. It is
not a report/amendment/account ledger. [Form 3X, pages 2–4](https://www.fec.gov/pdf/forms/fecfrm3x.pdf)
uses total receipts/disbursements in the cash summary and distinguishes federal
totals from those broader columns. The federal-column diagnostic is therefore
a sensitivity check, not an accepted cash identity or fallback.

A high-difference 2024 assertion, `C00843367`, has a $1,500,000 cash residual.
The [official committee summary](https://www.fec.gov/data/committee/C00843367/?cycle=2024)
independently displays the same operands: opening cash $0, receipts
$2,120,568.37, disbursements $598,964.00, and closing cash $21,604.37 for coverage
ending April 30, 2024. Thus this difference is present in the publisher's summary,
not introduced by our parser or grouping. This does not establish its cause.
Do not infer loan forgiveness, a missing payment, or wrongdoing from the residual.

The largest observed 2024 cash difference is `C00075820` at $21,876,124.94.
It remains unresolved. Neither this committee nor any named example appears in
a runtime exception table. Examples were selected by diagnostic magnitude;
their field values and locators remain in the calculation output.

The current evidence supports a report/account/amendment follow-up, not a source
repair. Both financial-use and terminal-attribution eligibility remain false.

## Verification and cost

The normal Go gate, including complete input verification, grouping, identical
replay, and wrong-cycle rejection for all four cycles, passed in 40.799 seconds.
The same package's instrumented race gate passed in 329.773 seconds. That slower
time includes full corpus replay under instrumentation, not production calculation
cost. Source-publication and CLI regressions, static analysis, and binary build
also pass. Source validation remains unchanged.

The independent gate passed 25 tests in 293.17 seconds, with nine unrelated
historical corpus opt-ins skipped. It reconstructed every real assertion's
91-field signature, exact occurrence/fact identities, candidate-reference
memberships, raw spans/hashes, and signed diagnostic result directly from the
release-owned CSVs. It checked each operand list against the pinned policy and
validated the complete result schemas and content identities. Its execution
window was 02:22:25–02:27:18 UTC. This exhaustive Python schema/evidence gate is
not a production runtime step.

The complete Go suite also passed without corpus opt-ins. Follow-up unit/race
tests cover CLI option failures and compiled-equation/pinned-policy agreement.
Formatting, lint, and local documentation link checks pass.

Fixture tests change every non-candidate non-key field, compare blank/zero and
different decimal spellings, retain exact duplicates and invalid candidate IDs,
exercise multi-variant conflicts, reject wrong source/order, and calculate a
difference beyond signed 64-bit range without overflow. This is all Go domain
logic; Python is only an independent evidence test, not runtime ingestion.

The current pretty-printed diagnostic outputs total 301,348,840 bytes. They
include full memberships and operand evidence and are retained as audit artifacts.
This is not the accepted format for scheduled production publication. A compact
published calculation remains separate; raw facts are not duplicated into Arango.

## Retained evidence and limits

Evidence lives under
`dumps/audits/fec/summary-assertions/2026-09-10/attempt-01/` in project storage.
It contains pinned code/contracts, the verified binary, synthetic fixture,
cycle results, test logs, timestamps, and explicit exit markers.

`go.exit` and `python-final.exit` are zero. The first independent-test attempt
failed because a local list shadowed its identity helper; `python.exit=1` and
the original pinned test code/log remain as evidence. The corrected
`final-code/` test then passed without changing the Go calculation or its outputs.
`independent-profile.json` retains complete equation/type profiles and the largest
differences with exact operands and source locators. Its SHA-256 is
`9224cf24e2d2cb106bf7925efca8040421d38ca808d1c084a52eadfe8861af86`.
Post-exit checks confirmed conserved counts, successful markers, and the unchanged
active release digest.

The Go job used a 4 GiB container cap and 3 GiB Go target. The independent test
uses a 2 GiB cap. Both are offline, one-shot containers with source storage
read-only and writes restricted to the audit directory (plus compiler caches
for Go). No source acquisition, extraction, source/fact change, graph mutation,
Dagster trigger, service deployment, or deletion ran.

Next: bounded report-level investigation of the unresolved difference families,
then acceptance of specific scope-qualified summary/receipt comparisons. The
[active queue](../todo.md) keeps complete cash coverage and terminal allocation
separate from grouping.

Follow-up: the [bounded report investigation](./summary-report-review-2026-09-10.md)
now explains three selected discrepancies through original filings and images.
It leaves this calculation, its residual counts, and its eligibility flags
unchanged; further financial consumers remain separate.
