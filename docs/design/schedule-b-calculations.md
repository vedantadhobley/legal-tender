# Schedule B calculations

The accepted Schedule B source preserves physical observations. Converting
those observations into money-flow evidence requires two decisions: which
reported amounts belong in a particular subtotal, and what each disbursement
describes. The implemented
[semantics diagnostic](../../contracts/audits/fec/schedule-b-semantics/v1/)
measures these dimensions over published Parquet facts before either decision
changes a graph.

The accepted [processed-disbursement reporting calculation](../../contracts/calculations/fec/processed-disbursement-reporting/v1/)
now implements a precisely scoped non-memo subtotal and form-specific reporting
roles in Go. It preserves separate-scope and unresolved rows. The separate
[committee-flow reconciliation](./committee-flow-reconciliation.md) now selects
typed sender observations and persists fact-level A/B candidate components.
Neither calculation implements economic ownership or graph publication.
The [complete 2024 gate](../audit/schedule-b-reporting-calculation-2026-09-08.md)
records measured conservation, source examples, and remaining exceptions.

The [complete 2024 audit](../audit/schedule-b-semantics-2026-09-08.md) now
conserves all source rows and signed cents. It identifies 8,394 form-line
exceptions and 97,850 raw-only self-recipient IDs for explicit treatment.

## Record selection

The accepted reporting rule uses one publisher-processed snapshot and excludes
memo code `X` from a named non-memo subtotal. FEC's
[itemized model](https://github.com/fecgov/openFEC/blob/develop/webservices/common/models/itemized.py)
maps Schedule B to the same processed relation and defines the memo-X flag.
Action codes, file numbers, original submissions, and transaction references
remain evidence. No action code by itself chooses a winning amendment. The
diagnostic keeps negative and zero amounts, and treats absent amounts
explicitly.

Membership is ordered: memo-X evidence; missing amount; unreviewed schedule;
unreviewed exact form-line; separate reporting scope; invalid regular-filer ID;
then included non-memo itemized disbursement. Inconsistent typed money or memo
projections fail the calculation rather than becoming counting exceptions.

The included subtotal covers only reviewed Schedule B lines on F3, F3P, and
F3X. Convention reporting, electioneering notices, reviewed Levin lines, and
F3X line 24 reported on SB retain distinct scopes outside that subtotal.
Unknown line aliases remain unresolved. Memo-X takes precedence in the
accounting buckets, but its form, role, and scope remain visible.

A named non-memo itemized subtotal is narrower than total committee spending.
It excludes unitemized activity and other schedules and can contain noncash
contributions and conduit reporting. Every source row and known signed cent
is conserved across all four decision buckets; no source facts are collapsed.

## Reporting role

Use filing form and exact line together. The same line number has different
meanings on different forms. Preserve these reporting categories:

- operating, fundraising, and legal/accounting disbursements;
- transfers to authorized, affiliated, or party committees;
- federal contributions, potentially including in-kind or earmarked activity;
- loans and loan repayments;
- contribution refunds to individuals, parties, and other committees; and
- other disbursements, federal election activity, and unresolved form lines.

The reference map follows official
[Form 3](https://www.fec.gov/pdf/forms/fecfrm3.pdf),
[Form 3P](https://www.fec.gov/pdf/forms/fecfrm3p.pdf), and
[Form 3X instructions](https://www.fec.gov/pdf/forms/fecfrm3xi.pdf).
The map describes reporting categories, not ownership or payment recipients.
It does not use names or purpose substrings as a terminal-source classifier.

The reporting calculation also maps the observed Form 4 lines using its
[instructions](https://www.fec.gov/resources/cms-content/documents/policy-guidance/fecfrm4i.pdf)
while keeping convention amounts separate. Raw-record review confirms that
beneficiary names occur on vendor, refund, loan, and contribution records;
name presence is not an earmark classifier. The raw recipient ID can equal
the filer while the named payee is a vendor. Never turn that raw-only ID into
a fallback money-flow endpoint.

`disb_tp`, recipient entity type, and candidate context are independent
evidence. A valid recipient committee identifier on a vendor, refund, loan,
or other-disbursement row does not establish a contribution. Raw and cleaned
recipient IDs are compared without substituting one for the other; self-
recipient shapes remain explicit. A candidate field does not prove that the
candidate controlled the reported amount.

## Economic-flow boundary

The implemented sender policy requires reviewed role/type agreement and exact
recipient evidence. Missing types, earmark codes, and unresolved identities
remain outside its conservative cohort. Fact-level reconciliation preserves
all competing candidate components and each ledger's signed amount. A later
economic-flow hypothesis must link supporting observations and expose
disagreement; it must not count both reports as separate dollars.
See the [flow fact requirements](./fec-flow-fact-requirements.md).

The reporting result deliberately sets `graph_eligible=false`. The CLI emits
a deterministic calculation artifact; immutable publication, persistent
reuse, and Dagster wiring remain separate implementation work. The source
fact-set identity, ordinal, and versioned policy reconstruct membership for
each reporting group without a second dense copy of the source rows.

Terminal-source attribution follows that reconciled graph. It must also
distinguish individuals' disclosed employers, corporate PAC affiliation,
corporate ownership, and inferred interests. None of those relationships makes
an employee's personal contribution a corporate payment.
