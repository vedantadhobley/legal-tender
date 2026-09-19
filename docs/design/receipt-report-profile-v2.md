# Complete-cycle receipt report-line profile

Status: implemented manual Go diagnostic with a passing
[complete 2024 gate](../audit/receipt-report-profile-v2-2026-09-10.md).
This extends the [v1 occurrence profile](./receipt-report-profile.md) using the
[bounded report-line membership contract](./receipt-report-lines.md). It does
not accept effective financial membership, cycle-summary differences, or terminal
attribution. The existing v1 command and wire output remain available unchanged.

The separate [receipt/window consumer](./receipt-reported-window.md) now
revalidates a pinned saved profile and qualifies a narrower occurrence-subtotal
comparison against bound reports and summary assertions. This does not change
the profile's guards or establish unique/effective transaction membership.

## Grain and conservation

V2 groups **every physical occurrence in the selected cycle**, not only rows
included by the individual predicate. Its keys preserve:

- Exact committee, file number, filing form, schedule, line, report type, and year.
- Raw memo code and publisher individual flag, independently of one another.
- The unchanged individual-predicate decision and the reviewed line disposition.

Null and blank cells remain distinct. Unknown values, unsupported forms/lines,
negative and zero amounts, memo records, and repeated physical rows stay visible.
Each report-line group retains exact known/unknown and signed amount measures,
sign counts, conduit-ID presence, receipt-date states, and first/last source-row
ordinals. Ordinal extrema are **not** a contiguous membership range or a list of
all member locators. Raw source bytes remain the occurrence authority.

The form table and report table are two views of the same occurrences, not two
ledgers. Both conserve the complete row count and every amount/count measure.
Report groups also regroup exactly into each full form-line/memo/individual key.
The included-individual and reviewed non-memo line measures are overlapping
diagnostic subsets; they must not be added to each other or to the complete total.

The line disposition reuses `fec/reported-itemized-line-membership@1.0.0`.
The current reviewed line is `F3`/`F3X`, schedule `SA`, line `11AI`; the publisher
individual flag is not its selection condition. Memo `X` is excluded from that
non-memo subtotal. Other nonblank memo codes and unknown line amounts stay
unresolved. Other forms/lines retain their own groups without a guessed mapping.
This rule has not replaced any accepted historical receipt calculation.

Every report group has missing, invalid, before-cycle, in-cycle, and after-cycle
receipt-date counts, plus valid date extrema. No receipt is clipped by date.
The two-year interval comes from the requested even year, not a fixed cycle list.
Receipt dates, report year, transaction period, and source snapshot remain
different dimensions. Neither observed receipt extrema nor `rpt_tp`/`rpt_yr`
establishes an original report's coverage interval or amendment status.

## Shared source boundary and execution

```bash
legal-tender pipeline fec profile-receipt-report-scope \
  --storage-root /storage \
  --summary-facts <published-committee-summary-manifest> \
  --cycle <source-cycle> --profile-version 2
```

The flag defaults to `1` for compatibility. `2` explicitly selects schema
`legal-tender.fec.receipt-report-occurrence-profile.v2` and policy
`fec/receipt-report-occurrence-profile@2.0.0`. Other versions fail.

Both versions share source selection and verification. The reader verifies the
summary facts and assertion identity, loads their exact immutable release,
selects that release's sole nonempty cycle Schedule A relation, and verifies
every row and both physical stream digests. It does not consult a newer current
pointer or relabel older facts. Any failure discards the profile.

The shared row decoder owns amount normalization, the existing individual
predicate, and receipt-date parsing. V2 adds no source parser or field-position
table; it uses reviewed column names from the pinned source contract.

One bounded pass computes the profile. Each form map, report map, and date cache
has a 500,000-entry limit. Hitting a limit, integer overflow, malformed source
rows, or cancellation fails without successful JSON. The real job uses a 4 GiB
offline container, a 2 GiB Go target, four CPUs, read-only project data, and a
1 GiB per-artifact write limit. It creates no corpus-sized intermediate.

Output order is deterministic. The profile ID hashes compact Go JSON with
`profile_id` empty. Per-run duration is logged, then zeroed in the verifier
object. Local paths and run IDs are not identity inputs. The
[wire contract](../../contracts/audits/fec/receipt-report-profile/v2/README.md)
fixes source ancestry, measures, and explicit non-readiness.

## Acceptance limits and next gate

The scan does not prove `SUB_ID` or transaction-ID uniqueness, select effective
reports, inspect original cover pages, establish account scope, or identify which
reports contributed to a cycle-summary assertion. No numeric summary-minus-detail
difference is produced. Both comparison and terminal-eligibility guards stay false.

The independent check must regroup same-source v2 output into every v1 form and
included-report group, including counts, amounts, dates, and ordinal extrema.
This establishes unchanged old-predicate observations while retaining previously
unprofiled rows. It is not independent parsing or replay of the full raw corpus.

The [bounded memo/amount review](../audit/receipt-memo-review-2026-09-10.md) now
locates all unresolved reviewed-line occurrences in original-source evidence.
No interpretation or correction is promoted. The separate
[receipt/window comparison](./receipt-reported-window.md) now qualifies a narrow
reported subtotal, and the [receipt-family map](./receipt-families.md) defines
the remaining F3/F3X field meanings. Financial report/account acceptance and
effective selection remain subject to the
[coverage requirements](./receipt-report-coverage.md).
Do not infer original-report completeness from a processed file reference, treat
the newest file number as a universal selector, or infer unitemized receipts from
a residual. Original-source acquisition and accepted correction policy remain
separate contracts. No graph, source/fact pointer, API, Dagster asset, schedule,
or resident service changes.
