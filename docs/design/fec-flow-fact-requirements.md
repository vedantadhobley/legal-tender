# FEC flow and outside-spending fact requirements

> **Status:** Accepted fact-family authority boundary. Processed Schedule A is
> the selected authority for the implemented receipt slice; its broader source
> contract remains draft pending historic memo/conduit coverage. Its first
> receiver-reported committee-flow calculation boundary, immutable publisher,
> same-release readiness bundle, isolated ArangoDB projection, and Dagster
> chain passed a complete 2024 corpus gate. Processed
> Schedule E has an accepted source contract, four accepted effective and
> identity calculations, and four ready isolated support/opposition graph
> projections. Schedule B's physical, classic-comparison, and same-publisher-
> batch alignment gates pass; active release v3 and the lossless columnar fact
> publisher implement its physical boundary. The accepted Go reporting
> calculation now implements scoped non-memo subtotals and form-line roles.
> Conservative typed sender membership and fact-level candidate reconciliation
> now pass the complete 2024 gate. Source review defines the next observation-
> graph boundary; economic-flow resolution remains open.

## Purpose

Legal Tender needs to trace money through committees, connect disclosed
activity to candidates, and calculate terminal-source attribution without
turning several reports of related activity into several dollars.

The source model therefore preserves three ledgers and a reconciliation layer:

1. receipts reported by a receiving committee;
2. disbursements reported by a sending committee;
3. independent expenditures made around a candidate; and
4. explicit reconciliation assertions between related receipt and
   disbursement observations.

Candidate context, graph edges, terminal classification, and cycle totals are
derived projections. They are not additional source ledgers.

## Required source facts

### Committee receipt observation

One processed Schedule A row is one immutable receipt observation. Preserve at
least:

- source release, selected relation, physical ordinal, and `sub_id`;
- filing, report, transaction, back-reference, and amendment/action lineage;
- receiving filer committee and disclosed contributor or source committee;
- exact reported amount, transaction date, election designation, transaction
  type, memo code, memo text, and contributor text; and
- every remaining publisher field in the columnar fact artifact.

This is the first graph authority for inbound committee money, including money
received by candidate-authorized committees and transfers received by PACs.
The normalized source direction is disclosed source to receiving filer.

The accepted first committee-flow calculation is deliberately narrower than
the source ledger. Raw and cleaned contributor IDs must agree on one exact FEC
committee ID, and the exact receipt type must describe an inbound registered-
filer contribution, in-kind contribution, affiliated transfer, or received
refund/repayment. Receipt codes that describe outbound, memo, earmarked, or
noncommittee roles do not become flow merely because an ID is C-shaped.
Unknown roles and one-sided identities remain explicit. See the
[machine contract](../../contracts/calculations/fec/receiver-reported-committee-flows/v1/contract.json)
and [complete-corpus audit](../audit/receiver-reported-committee-flow-cohort-2026-08-31.md).

### Committee disbursement observation

One processed Schedule B row is one immutable sender-side observation.
It must preserve the sending filer, disclosed recipient, recipient committee
identity, exact amount, disbursement and communication dates, purpose,
category, candidate context, memo and conduit fields, transaction and
back-reference lineage, action code, and source identity.

Schedule B is not required to publish the first receipt-side flow graph. It is
required before Legal Tender claims complete sender-side outflows or
two-sided transfer reconciliation. Its physical contract, complete 2024
parser, classic direction/precision comparison, and same-publisher-batch
Schedule A alignment pass. Unique exact or same-amount/different-date
candidates cover 82.02% of accepted Schedule A flow amount without merging
the ledgers. Release v3 and lossless selected-cycle columnar facts implement
the physical boundary. The complete-corpus reporting-semantics diagnostic now
conserves every row and signed cent while exposing unresolved form-line and
self-recipient states. Typed sender selection and fact-level candidate
reconciliation now pass separately. The
[source-reviewed graph boundary](./arango-committee-flow-evidence.md) preserves
observations without asserting economic payment identity. See the
[alignment audit](../audit/schedule-ab-alignment-2026-09-04.md) and
[Schedule B calculation gates](./schedule-b-calculations.md).

### Candidate-directed context

Classic `pas2` is a candidate-context projection over classic `oth`, not a
second money ledger. For a given classic release, retain its `CAND_ID` and
transaction classification as an assertion attached to the underlying
`SUB_ID`. Do not create another amount when that `SUB_ID` is already present
as an `oth` occurrence.

Candidate-controlled receipts are calculated from receipt observations whose
receiving committees have accepted, cycle-specific authorization evidence.
A `CAND_ID` printed on a sender-side record is useful context, but it does not
replace the recipient and authorization evidence.

### Independent-expenditure observation

One processed Schedule E row is one immutable outside-spending observation.
Preserve at least:

- source release, physical ordinal, `sub_id`, and `orig_sub_id`;
- spender, candidate, support/oppose indicator, election, and office context;
- exact `exp_amt`, expense date, dissemination date, payee, purpose, and
  category;
- filing form, report type and year, file number, transaction and
  back-reference IDs, action code, memo fields, and source document link; and
- nullable or anomalous values without inventing replacements.

Schedule E is the canonical recurring IE source. Classic `pas2` and the
24/48-hour file remain separately identified comparison evidence. They cannot
patch Schedule E or contribute another amount to the IE total.

The accepted effective calculation uses the processed regular-report relation
as the publisher-selected view. It excludes memo-code `X`, preserves exact
signed amounts, retains repeated transaction keys without local deduplication,
and separates unrouteable included amounts as exact exceptions. The 24/48
notice feed remains distinct comparison evidence. See the
[publication audit](../audit/effective-independent-expenditures-2026-08-31.md).

## Reconciliation fact

A committee-flow reconciliation connects, but never merges, a sender-side
Schedule B observation and a receiver-side Schedule A observation. It records:

- both immutable fact identities;
- match method and version;
- exact, compatible, ambiguous, conflicting, or one-sided state;
- compared endpoints, dates, amount, type, memo, and filing evidence; and
- alternatives when the match is not unique.

An eventual economic-flow graph must expose each accepted hypothesis once.
The next graph instead exposes selected disclosure observations and candidate
components separately: a candidate component is not itself one payment or a
money-bearing path. An unmatched observation remains visible; it is not
discarded or silently inferred.

## Derived graph and calculations

The first receipt-side graph may project directed edges from accepted Schedule
A facts. Candidate views join those edges through accepted candidate-committee
linkages. Later Schedule B reconciliation can strengthen or dispute an edge
without doubling its amount.

Terminal-source attribution operates on a named, versioned flow projection.
It must preserve:

- the calculation input total;
- direct, earmarked, proportional, and unresolved allocations separately;
- exclusive terminal amounts plus an unresolved remainder;
- every supporting path and source fact; and
- cycle, as-of time, source release, classification version, and calculation
  version.

The first [candidate upstream calculation](./candidate-upstream.md) now
conserves the selected candidate receipt boundary and returns complete
committee ancestry, cycles, source witnesses, and coverage gaps. It deliberately
leaves terminal amounts unallocated: the committee-only cohort does not supply
donor-bearing receipts or a complete funding basis. UI work follows that monetary
boundary rather than being a prerequisite for it.

Independent expenditure attribution uses a different denominator: the
spender's accepted effective IE amount. It never enters the candidate's
controlled-receipt total and opposition spending is never candidate funding.

## Time and conservation invariants

- Preserve transaction, expenditure, dissemination, filing, coverage, and
  publication times independently.
- A two-year FEC cycle is a query and partition dimension, not the only time
  identity of a fact.
- Calculations distinguish output scope from the exact evidence window, which
  may span cycles; see the [cycle/window contract](./cycle-calculation-windows.md).
  Preserve membership and time when combining partitions, not only totals.
- Keep signed exact cents. Never route money through binary floating point or
  a source that truncates fractional dollars when the canonical source retains
  them.
- Never add sender- and receiver-side observations merely because both exist.
- Never create another money fact from a candidate-context projection.
- Every aggregate conserves its accepted input or reports the unresolved
  difference explicitly.
- A changed source release recomputes only projections whose selected facts or
  versioned rules changed.

## Source disposition

| Source | Current disposition |
|---|---|
| Processed Schedule A | Accepted authority for itemized receipt observations. The first receiver-reported committee-flow policy, immutable publisher, readiness bundle, and isolated exact-lineage graph projection are implemented. |
| Classic `oth` | Preserved comparison evidence; broad classic occurrence set, not an additional canonical amount. |
| Classic `pas2` | Candidate-context and parity evidence keyed by shared `SUB_ID`; rejected as the IE amount authority. |
| Processed Schedule B | Sender-side source with complete artifact, strict 157.5-million-row 2024 parser, classic direction/precision, same-publisher-batch Schedule A alignment, release-v3 membership, and lossless columnar fact publication. Effective-flow and reconciliation calculations remain separate. |
| Processed Schedule E | Accepted authority for IE observations; strict parsing, v2 release inclusion, selected-cycle occurrences, lossless facts, effective spender-candidate-stance calculation, and an isolated 2024 exact-lineage graph projection are implemented. |
| 24/48-hour IE file | Timeliness and filing-chain comparison evidence; contains original and amended reports and cannot be naively summed. |
| Candidate and committee summaries | Publisher assertions for coverage and reconciliation, never replacements for source rows. |

The measurements supporting this disposition are in the
[classic-flow and Schedule E audit](../audit/fec-classic-flow-and-schedule-e-2026-08-31.md).
