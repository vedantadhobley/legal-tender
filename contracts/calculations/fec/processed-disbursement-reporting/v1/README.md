# Processed Schedule B reporting subtotals

Accepted for the Go `calculate-disbursement-reporting` command. This is a
reporting calculation, not a sender-flow or terminal-source calculation.
The [contract](./contract.json) defines membership and the
[result schema](./result.schema.json) defines the deterministic output.

## Membership

Every source ordinal receives one decision. Reject inconsistent typed money
or memo fields before making any decision. Then evaluate, in order:

1. Memo code `X`: retain outside the non-memo subtotal. `Y` is not `X`.
2. Missing amount: unresolved.
3. Unreviewed schedule or exact form-line pair: unresolved.
4. Reviewed convention, electioneering-notice, Levin, or independent-
   expenditure-on-SB scope: retain separately.
5. Invalid regular-committee filer ID: unresolved.
6. Otherwise: include in the reviewed regular-committee non-memo itemized
   disbursement subtotal.

No action code chooses a winning amendment. Use the publisher's processed
snapshot as supplied; do not deduplicate repeated transaction references.
Negative and zero amounts remain signed observations. No summary fills detail.

This subtotal excludes unitemized reporting and other schedules. It can
contain noncash and conduit activity. It is neither total spending nor a
cash-only ledger.

## Reporting roles and evidence

The exact map is embedded in the result and checked against the Go policy.
It follows the official [Form 3](https://www.fec.gov/pdf/forms/fecfrm3.pdf),
[Form 3P](https://www.fec.gov/pdf/forms/fecfrm3p.pdf),
[Form 3X instructions](https://www.fec.gov/pdf/forms/fecfrm3xi.pdf), and
[Form 4 instructions](https://www.fec.gov/resources/cms-content/documents/policy-guidance/fecfrm4i.pdf).
The [FEC forms index](https://www.fec.gov/help-candidates-and-committees/forms/)
identifies Form 9 as electioneering notices. Its observed `F93` line, and
the observed `SL`-prefixed Levin lines, are exact processed-source spellings,
not new interpretations inferred from recipient names.
The complete-corpus acceptance gate currently covers 2024. Other target cycles
still require corpus verification; this is not blanket historical form-version
coverage.

Form 3X line 23 can contain
[forwarded earmarked contributions](https://www.fec.gov/help-candidates-and-committees/filing-political-party-reports/earmarked-contributions/)
as well as a committee's own contributions. In-kind contributions can also
appear there. Form 3P line 23 instead reports operating expenditures. A
reporting category does not establish the funding source or cash recipient.

The result keeps raw/clean recipient agreement, self-recipient state,
transaction type, and beneficiary/conduit-name presence as independent
dimensions. None changes the reporting category or resolves ownership.
Do not classify all beneficiary-bearing records as conduit activity or use
the raw recipient ID as a fallback graph endpoint.

## Evidence and execution

The Go calculation verifies the immutable fact manifest and every backing
Parquet shard, checks the complete physical schema, and scans only needed
columns. It preserves source NULL versus empty text in keys. The source fact
set plus ordinal and versioned policy reconstruct every group's membership;
the first ordinal is a drilldown locator, not a statistical sample.

Independent source, group, and decision totals conserve row counts, known
signed cents, missing amounts, and positive/negative/zero populations. Groups
are sorted by first ordinal. Checked arithmetic rejects overflow. Workers
are limited to 1–16 and grouping to 250,000 keys per shard and combined scan.

Calculation identity binds version, policy digest, cycle, and exact input
identity. The policy digest covers the version and reviewed line map; other
policy changes require a version bump. The group digest covers Go's compact
JSON encoding. Runtime stays on stderr, so successful result bytes are
independent of worker count and execution time.

The command writes JSON to stdout after validation. It has no publication
pointer, persistent cache, or Dagster asset yet. Run it with read-only source
mounts and retain its result and completion marker using the
[operations runbook](../../../../../docs/operations.md).
`graph_eligible` is always false. Existing graphs and facts remain unchanged.
