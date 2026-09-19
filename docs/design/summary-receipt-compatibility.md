# Summary-to-receipt comparison readiness

Status: implemented manual Go diagnostic. The
[real 2024 gate](../audit/summary-receipt-readiness-2026-09-10.md) verifies reported
observations and explicit blockers, **not** a numerically reconciled funding
basis. The [policy](../../contracts/calculations/fec/summary-receipt-review/v1/policy.json)
and [wire schema](../../contracts/calculations/fec/summary-receipt-review/v1/result.schema.json)
define this narrow consumer.

The [summary-value use policy](./summary-value-use.md) now defines the broader
consumer boundary. This v1 command remains unchanged and more restrictive than
a future qualified reported-subtotal comparison: its deltas stay null. Summary
blockers do not invalidate the independent processed receipt ledger or graph.

The separate [same-report total-receipts comparison](./report-total-receipts-comparison.md)
now permits exact-file reported-pair arithmetic under its own field/period gates.
It does not prove this command's cycle-summary versus Schedule A population.

## What this boundary answers

For one exact committee and source cycle, show which preserved summary fields
have a possible receipt-side counterpart, what evidence is absent, and whether
the source versions align. Reported values remain readable even when their use
in a financial calculation is blocked.

Keep three answers separate:

- A publisher reported this field value, blank, or invalid lexeme.
- A defined detailed population may be comparable with that reported field.
- A complete cash funding basis could support an allocation.

This version establishes the first answer and assesses missing evidence for the
second. It does not establish the third. It does not replace
[exact assertion grouping](./committee-summary-assertions.md), the
[receipt inventory](./committee-funding-basis.md), or raw facts.

## Field scope

This first consumer assesses the nine fields already covered by the verified
summary diagnostics. All other monetary fields remain preserved in source facts;
they are not assessed or silently discarded by this consumer.

| Summary field | Receipt relationship | Evidence still needed |
|---|---|---|
| `INDV_ITEM_CONTB` | Potential comparison with the accepted individual predicate. | Report/form-line equivalence and period coverage; the publisher classification alone is insufficient. |
| `INDV_UNITEM_CONTB` | No itemized-inventory counterpart. | Preserve the explicit summary observation; never infer it from a difference. |
| `INDV_CONTB` | Broader than the itemized individual cohort. | Compatible unitemized component and complete individual population. |
| `TTL_RECEIPTS` | Broader receipt-family population. | Accepted coverage of all contributing report lines, not a sum of every Schedule A row. |
| `TTL_FED_RECEIPTS` | Federal receipt population. | Form/account-specific federal scope; not interchangeable with total receipts. |
| `COH_BOP`, `COH_COP` | Balances, not receipt transactions. | Exact report/account balance evidence and continuity. |
| `TTL_DISB`, `TTL_FED_DISB` | No receipt-inventory counterpart. | A separately qualified disbursement population; Schedule A does not supply it. |

The individual predicate includes the overlap cohort **once**. Its publisher
classification is not a resolved donor identity or proof that it matches a
specific summary form line. Overlap measures are a diagnostic subset, not another
amount to add. The component/role table alone partitions all selected recipient
rows. Known, unknown, positive, negative, zero, and signed measures conserve.

No receipt rows in an exact snapshot means `no_rows_in_snapshot`, not reported
zero funding or a terminal donor. Missing summary identity produces
`no_indexed_summary_in_snapshot`; unindexable source-row counts remain visible.

## Source and scope gates

The command first opens the existing receipt inventory through its normal
verification boundary, including exact Schedule A manifest and backing hashes.
It regenerates summary grouping through the existing fully verified fact loader.
It accepts no caller-supplied assertion JSON or invented source-compatibility
flag. Wrong cycles or malformed committee IDs fail.

The output binds both fact/manifest identities, inventory and assertion
calculation IDs, and both source-release IDs and digests. Different releases
yield `different_source_release` plus `source_release_mismatch`; both observations
remain visible, but no numeric difference is calculated. The same release ID
with different manifest digests is an integrity error, not a normal blocker.

A common release is necessary for the intended same-release comparison but does
not prove synchronized report coverage. The current inventory lacks report-period
coverage and the summary lacks report/account identities. Those remain explicit
comparison blockers even in same-release fixtures. Neither matching source cycles
nor coincidentally equal amounts supplies the missing proof.

Cash-versus-valuation classification, inter-report continuity, recipient cash
availability, and complete funding-family coverage are separate **funding**
blockers. Cash timing need not block a future scoped reported-subtotal comparison;
it does block cash allocation.

## Variants, dates, and arithmetic

Every selected assertion variant and occurrence member survives. No first/last
variant is selected, and variants are never summed. A field conflict attaches
to that field. Differences in committee type, designation, or coverage dates
also create a scope conflict. A contact-name-only difference does not manufacture
a monetary conflict, but both variants remain visible and unselected.

Blank/invalid dates, reversed coverage, and dates outside the source cycle remain
explicit. No date is clipped, and a shorter valid interval is not relabeled as a
complete cycle. Receipt-period compatibility remains unverified.

A non-equal or unavailable individual-subtotal diagnostic blocks use of that
identity for its three component fields. The cash diagnostic applies to its
four operands. Each field also retains its own raw/typed validity independently.
Federal-column sensitivity stays visible but cannot fail or rescue a financial
comparison as though it were an accepted alternative cash equation.

These rules follow the [original-report investigation](../audit/summary-report-review-2026-09-10.md)
without introducing any named-committee exception or automatic correction.
Filer explanations and manually reviewed transcription discrepancies are not
silently imported as production override rules.

## Command and output

```bash
legal-tender pipeline fec review-summary-receipt-compatibility \
  --storage-root /storage \
  --basis-result <verified-inventory-json> \
  --summary-facts <published-summary-fact-manifest> \
  --cycle <source-cycle> --committee <exact-committee-id>
```

One invocation returns one committee. It scans the small summary fact set and
inventory buckets, but does not rescan Schedule A transaction rows. Backing
hash verification still incurs I/O. This is a manual diagnostic, not a query
plan to repeat per committee in weekly processing. A future cycle-wide consumer
must open verified inputs once and process the cycle in a bounded pass.

The review ID hashes compact Go JSON with `review_id` empty. Ordering is stable;
paths, observation time, and run IDs are not identity inputs. JSON goes to stdout
only after input verification, conservation, and cancellation checks succeed.

All field `delta_minor_units` values are null. `comparison_ready`,
`complete_committee_funding_basis`, and `terminal_attribution_eligible` are false.
The separate summary diagnostics still carry their exact intra-summary residuals;
they are not differences against receipts. Schema checks reject attempts to
promote readiness or fill a comparison delta.

There is no new source acquisition, source/fact change, calculation pointer,
graph edge, Dagster asset, schedule, API dependency, or resident service.
Python is used only for independent test/schema checks, not runtime policy.

## Next acceptance gate

Prove a narrow itemized-individual reported-subtotal comparison. First establish
compatible Schedule A/summary source ancestry without relabeling old facts.
Then inventory report/form-line membership and reporting-period coverage for
that predicate in a cycle-wide pass. Define a numeric diagnostic only once the
question and population are explicit. Any remaining mismatch stays unexplained
unless independent source evidence establishes its meaning.

The additive [same-release occurrence profile](./receipt-report-profile.md) now
selects staged Schedule A through the verified summary's exact release and
inventories its form/line decisions, included report references, and receipt-date
states. It bypasses no fact-publication gate: key uniqueness, effective membership,
and reporting-period coverage remain unestablished. The older readiness review
and its different-release result remain unchanged.

The [bounded report-line gate](./receipt-report-lines.md) now verifies complete
membership and original report-period subtotals for seven retained Form 3 files.
It uses older accepted facts, preserves memo dates outside the cover interval,
and does not establish same-v4 cycle-summary compatibility. The cycle-wide
consumer must retain independent memo/individual axes and all report-line rows,
not only the earlier profile's included-individual report groups.

The [v2 profile](./receipt-report-profile-v2.md) now supplies that all-occurrence
grain for the complete same-release 2024 source, with exact regrouping to every
v1 observation. Report references and receipt-date states remain observations,
not accepted effective-report or account coverage. Numeric compatibility remains
blocked until the missing evidence and unresolved reviewed-line states are addressed.

The [memo/amount investigation](../audit/receipt-memo-review-2026-09-10.md) now
preserves original witnesses for all such reviewed-line exceptions. The next
[report-coverage qualification](./receipt-report-coverage.md) tests metadata
availability, including zero-itemization reports and attachment/amendment scope,
without selecting another recurring amount source.

Other source families, effective-report selection, accepted corrections, cash
allocation, compact recurring publication, and orchestration remain separate
contracts. Do not expand this review into a second report-ingestion pipeline.
