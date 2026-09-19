# Committee-flow candidate reconciliation

The Go `reconcile-committee-flows` command selects a conservative sender
cohort from Schedule B and compares it with the unchanged accepted receiver
cohort from Schedule A. The [machine contract](../../contracts/calculations/fec/committee-flow-reconciliation/v1/)
defines source-indexed observations and candidate-component assertions.
This is an evidence calculation, not a new graph or terminal-source method.

## Sender membership

Start with the accepted [Schedule B reporting predicate](./schedule-b-calculations.md).
Require non-memo, reviewed regular-committee reporting, known signed amount,
a valid filer, and agreeing raw/clean recipient committee IDs. Hold self
references outside flow membership. Neither a name nor a lone reported ID
resolves an endpoint.

Then require agreement between reporting role and exact transaction code:

| Reporting role | Type | Selected flow role |
|---|---|---|
| Federal contribution | `24K` | Contribution |
| Federal contribution | `24Z` | In-kind contribution |
| Authorized or affiliated/party transfer | `24G` | Affiliated transfer |
| Party or committee contribution refund | `22Z` | Refund or repayment |
| Loan repayment or other loan repayment | `20R`, `22K` | Refund or repayment |
| Loan made | `22H` | Loan |

These codes follow the FEC's [transaction-type descriptions](https://www.fec.gov/campaign-finance-data/transaction-type-code-descriptions/).
Their combination with reviewed reporting lines is this calculation's
conservative policy, not a publisher-provided economic reconciliation.
`24I` and `24T` remain held earmark-forwarding evidence. Missing codes remain
unresolved; do not classify them as ordinary committee-funded contributions.
The FEC's [earmark reporting guidance](https://www.fec.gov/help-candidates-and-committees/filing-ssf-reports/earmarked-contributions/)
shows why a contribution reporting line alone is insufficient.

A present conduit name holds a record for review. It is not positive proof of
a particular intermediary relationship. Beneficiary-name presence does not
change membership or establish ownership. An included committee endpoint can
be a reported beneficiary rather than the cash payee. In-kind activity stays
distinct from cash; `24K` is not proof of beneficial funding origin.

The unchanged receiver policy maps its contribution, in-kind, affiliated-
transfer, and refund/repayment categories into compatible comparison roles.
It does not select a loan-receipt cohort. Selected B loans therefore remain
one-sided under this version. This is explicit scope, not a missing-report
finding. Candidate-directed context, vendors, individuals, unknown types,
and other reporting categories remain in the source and decision totals.

## Candidate components

Compare records only within the requested cycle and exact directed committee
pair. Create opposite-ledger candidate connections when either:

1. Role and signed amount agree, with no date tolerance or requirement.
2. Role and known calendar date agree, preserving amount disagreements.
3. Signed amount and known date agree, preserving role disagreements.

Take connected components over all these alternatives. Do not greedily claim
an exact pair before considering repeated amounts or dates. A bucket union
needs linear membership work; it does not materialize every Cartesian pair.
Same-side records connect only through an opposite-ledger candidate.

| Component | Meaning |
|---|---|
| Unique exact signature | One selected fact per side; endpoints, role, signed amount, and known date agree |
| Date disagreement | One per side; role and signed amount agree, dates differ |
| Missing date | One per side; role and signed amount agree, a date is absent |
| Amount conflict | One per side; role and date agree, signed amounts differ |
| Role conflict | One per side; amount and date agree, roles differ |
| Ambiguous candidates | Several selected facts belong to one candidate component |
| Unmatched A or B | No opposite selected candidate under this cycle and policy |

Exact-signature corroboration is not proof of unique economic identity. The
selection excludes uncertain records that could supply additional evidence.
Repeated payments across a cycle can create large ambiguous components.
The result has no confidence score, inferred winner, date-based ranking, or
automatic economic-flow eligibility. Different signs remain different amounts.
An unmatched state does not prove that the other committee failed to report.

## Source and publication integrity

Bind both fact manifests and one immutable coordinated FEC release. Rehash
every Parquet shard. Verify full schemas, dense source ordinals, cycle, and
typed money consistency. A fact set published under an older release may be
reused only when its exact selected source remains in the coordinated release.
For A, verify the archive and both compressed/uncompressed selected-relation
digests and sizes. For B, verify the archive digest/size and inventory-selected
cycle relation. Also verify each fact set's original immutable release digest.

Every source row belongs to one selection-decision bucket. Every selected
source ordinal belongs to exactly one candidate component. Persist separate
selected-observation artifacts for A and B and a component artifact containing
their ordinals and separate signed amounts. The result binds each ordinal to
its immutable fact set; `SUB_ID` remains evidence, not cross-ledger identity.
Full source fields remain in Parquet.

Go reopens each content-addressed JSONL/zstd artifact, compares every record,
and verifies its count and digests before returning the result. Replay verifies
existing artifact bytes. The bounded implementation permits at most one
million selected observations per side and 10,000 decision shapes; exceeding
either fails explicitly. It does not truncate or sample the cohort.

The manual command returns deterministic JSON only on success. Runtime and
workers do not affect identity. It does not update an active pointer or skip
source selection on replay. The separate
[publication boundary](./committee-flow-publication.md) now publishes this
unchanged result, implements persistent no-source-row-scan reuse, and freezes
exact observation-consumer readiness. Neither command mutates a graph.
`graph_eligible` is always false: candidate components are not economic flows.

The [source review](../audit/committee-flow-source-review-2026-09-08.md) now
profiles all candidate evidence and verifies targeted source rows. Its findings
support the [accepted observation-graph boundary](./arango-committee-flow-evidence.md),
not promotion of candidate components into economic payments. The separate
[observation graph](./arango-committee-flow-evidence.md) and
[thin Dagster chain](./committee-flow-orchestration.md) now implement that
boundary; economic-payment resolution remains unimplemented.
