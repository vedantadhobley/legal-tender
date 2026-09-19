# FEC money and time semantics

> **Status:** Draft target source mapping for the Go rewrite. Official rules
> relevant to the current backfill were audited on 2026-08-27. The first
> processed Schedule A calculation and initial bulk source contracts are
> defined. Electronic-filing contracts and format dispatch remain deferred
> research; initial-contract acceptance gates and broader FEC coverage remain
> open.

## What is already designed

The first vertical slice preserves FEC processed Schedule A rows, links them to
same-period authorized committees, calculates an itemized-individual receipt
component, and reconciles that component against publisher summaries. Its
accepted decisions live in the [calculation contracts](./calculation-contracts.md).

That work does not complete the FEC model. Schedule B/E source contracts and
reporting calculations have since landed; the [as-built ledger](../go-rewrite.md)
routes their current state. Debts, loans, complete raw report families,
unitemized summary composition, and full funding coverage remain open. The old
Python implementation is evidence for these areas, not the target contract.

## Thresholds do not all mean ranges

The classic FEC individual-contributions bulk file is a subset of itemized
individual contributions. For 2015 onward, it includes a contribution after
the contributor's election-cycle-to-date amount exceeds $200 for candidate
committees or calendar-year-to-date amount exceeds $200 for PACs and party
committees. From 1989 through 2014 its reporting-period selection threshold was
$200; from 1975 through 1988 it was $500.

Those rules select rows for one derived bulk file. They do not establish that
an included transaction is at least $200, and they do not turn an absent row
into a `[0, $200]` transaction. A smaller transaction can appear after an
aggregate threshold is crossed. The target processed view therefore uses the
complete processed Schedule A source; routine freshness follows coordinated
processed bulk releases under the
[accepted acquisition strategy](./schedule-a-source-strategy.md). The classic
`indiv` file remains separate comparison evidence and cannot silently fill
either source view.

Candidate committees generally itemize an individual's contribution when the
transaction exceeds $200 or the contributor's election-cycle aggregate exceeds
$200. Amounts that remain unitemized are reported to the FEC as a lump-sum
total without contributor information. That creates two separate measures:

- a reported point for the committee's unitemized total; and
- unresolved contributor identity and composition within that point.

The number of underlying contributors is unknown, voluntary itemization can
occur, and the aggregate test spans transactions. Legal Tender must not infer
one anonymous donor, a donor count, or a per-donor interval from the lump sum.

## Source measurement classes

| FEC evidence | Money treatment | What remains uncertain |
|---|---|---|
| Ordinary Schedule A or B numeric amount | `reported_point` for the declared transaction role | Coverage, memo treatment, amendment state, identity, and economic completeness remain separate. |
| Publisher summary line | `summary_value` point | Composition and compatibility with detailed rows. |
| Unitemized individual summary | `summary_value` point | Contributor identities, count, transaction grain, and composition. |
| 24/48-hour independent-expenditure estimate | `reported_estimate` point | The eventual actual cost has no finite source-supplied bounds. |
| Later regular-report independent-expenditure actual | New `reported_point`, related to the earlier estimate | Matching and effective-version selection. |
| In-kind contribution | `reported_valuation` point using the source's usual-and-normal-charge rule | It is non-cash and can appear as both receipt and offsetting expenditure context. |
| Debt whose exact amount is unknown | `reported_estimate` point | Correct amount and later amendment or correction. |
| Incomplete resolved subtotal over signed rows | `calculated_sum` point for that fact set with partial coverage | The complete result is not automatically lower-bounded because missing adjustments can be negative. |

A reported estimate remains a point in the space of values the filer reported.
It is not a point claim or finite interval for the eventual actual cost. An API
asking for the reported estimate returns the point with its measurement kind;
an API asking for actual cost returns unknown until compatible actual evidence
is selected.

## Independent expenditures

The FEC's current independent-expenditure bulk file contains 24- and 48-hour
reports. The reporting triggers are aggregate thresholds of more than $10,000
up to 20 days before an election and more than $1,000 during the final 19 days.
Those thresholds trigger expedited reports; they are not minimum values for
every transaction in the file.

The file retains transactions from original and amended reports, so naïve sums
double count. Unlike the processed Schedule A slice, this source requires an
explicit effective-record calculation and must preserve all report versions.

When cost is unknown at dissemination, FEC guidance directs a PAC to report an
estimate on its 24- or 48-hour Schedule E and identify the estimate in purpose
text. It later reports actual cost on the next regular report and notes the
relationship in purpose text. The source does not supply a structured margin
of error or finite bounds. The adapter must preserve the purpose text and
report context, then produce source-evidenced estimate/actual relationship
candidates without assuming a match from equal amounts alone.

## In-kind contributions and double-entry context

An in-kind contribution is non-monetary and is valued at the usual and normal
charge. Candidate committees report the value as a receipt and also include it
in operating expenditures to avoid inflating cash on hand. These are related
reporting roles for one disclosed in-kind value, not two additive cash flows.

The normalized evidence retains both occurrences and their source line roles.
A cash-flow calculation excludes or offsets them under an explicit contract. A
support-value calculation can include the valuation once. Neither calculation
rewrites the source rows.

## Debts

Debt timing has its own disclosure thresholds. A debt of $500 or less becomes
reportable after it has remained outstanding for 60 days; a debt above $500 is
reported for the period in which it was incurred. These are timing and coverage
rules, not amount intervals for a missing debt.

If exact debt is unknown, FEC guidance permits an estimated amount followed by
an amendment or a later correction with explanation. Preserve the estimate and
correction as separate assertions. Do not convert the first estimate into a
range unless a source rule supplies bounds. Disputed-debt amounts, when
available, remain separate creditor and committee assertions rather than one
synthetic interval.

## Independent time meanings

The rewrite preserves these clocks instead of stamping every fact with only a
cycle:

| Time | Use |
|---|---|
| Transaction or receipt date | Event-time filters and allocation to requested views. |
| Report coverage start and end | Defines the period claimed by a filing or summary. |
| Filing and posting timestamps | Revision ordering and incremental acquisition. |
| FEC two-year transaction period | Source partition and two-year reporting view. |
| Election designation and election identifier | Primary, general, runoff, special, or other election attribution. |
| Election-cycle-to-date aggregate | Candidate-committee contributor itemization context. |
| Calendar-year-to-date aggregate | PAC and party contributor itemization context. |
| Independent-expenditure dissemination, payment, and report dates | Reporting-trigger and estimate/actual analysis. |
| Debt incurred and outstanding dates | Debt reporting eligibility and duration. |
| Legal Tender retrieval and publication times | Snapshot replay and reproducible as-of answers. |

The last four two-year periods are a query and materialization view over those
preserved times. They are not a destructive normalization rule. A period view
selects facts under a versioned contract and retains its component periods, so
later eight-year, calendar-year, election-specific, and as-of views do not
require reparsing raw data.

## FEC source-contract status

Before the first Go implementation can claim broad FEC coverage:

1. The first machine-readable contracts and fixtures now cover processed
   Schedule A, candidate and committee masters, candidate-committee linkage,
   and both candidate-summary populations. They remain draft under the
   [documented acceptance gates](./fec-source-contracts.md#acceptance-gates).
2. Their source partition, receipt, report, election, coverage-end,
   processing, and snapshot time meanings are separate. Extend that discipline
   to every later FEC contract.
3. Pin electronic-versus-paper and historical coverage boundaries for the
   intended backfill.
4. Add source contracts for Schedule B, Schedule E, debts, loans, and raw
   filing versions before those measures enter product totals.
5. Define effective-version calculations independently for processed sources,
   raw reports, and the independent-expenditure bulk file.
6. Add fixtures for negative adjustments, memo rows, voluntary itemization,
   unitemized summary points, estimates followed by actuals, in-kind double
   reporting, and estimated debt corrections.

## Official references

- [FEC contributions by individuals file description](https://www.fec.gov/campaign-finance-data/contributions-individuals-file-description/)
- [FEC individual-contribution reporting](https://www.fec.gov/help-candidates-and-committees/filing-reports/individual-contributions/)
- [FEC raising methodology and unitemized totals](https://www.fec.gov/data/raising-bythenumbers/)
- [FEC independent-expenditure file description](https://www.fec.gov/campaign-finance-data/independent-expenditures-file-description/)
- [FEC independent-expenditure estimate and actual reporting](https://www.fec.gov/help-candidates-and-committees/filing-pac-reports/estimating-independent-expenditures/)
- [FEC in-kind contribution reporting](https://www.fec.gov/help-candidates-and-committees/filing-reports/in-kind-contributions/)
- [FEC debts owed by a committee](https://www.fec.gov/help-candidates-and-committees/filing-reports/debts-owed-committee/)
