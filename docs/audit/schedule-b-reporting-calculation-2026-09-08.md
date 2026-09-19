# Schedule B reporting calculation gate

Observed 2026-09-08. The Go
[reporting calculation](../../contracts/calculations/fec/processed-disbursement-reporting/v1/)
now applies explicit non-memo membership and form-specific roles to the
complete accepted 2024 facts. This is not a sender-flow graph or production
publication gate.

## Complete-corpus result

The eight-worker run classified all 157,544,163 facts in 158 shards into
39,989 reporting groups in 114.175 seconds. Sampled memory was 908.4 MiB
under a 4 GiB container cap and `GOMEMLIMIT=2GiB`. It exited zero without an
OOM event. Existing Parquet files were mounted read-only; no archive was
downloaded or extracted and no graph changed.

| Accounting bucket | Rows | Signed reported amount |
|---|---:|---:|
| Reviewed regular-committee non-memo itemized disbursements | 156,432,445 | $22,823,608,735.66 |
| Memo-X evidence | 1,106,842 | $743,029,050.83 |
| Separate non-memo reporting scopes | 4,821 | $198,481,054.85 |
| Unresolved reporting | 55 | $88,695.97 |
| Complete source | 157,544,163 | $23,765,207,537.31 |

All amounts are present. The calculation preserves 211,035 negative and 440
zero observations. The included amount is not total spending, cash-only
outflow, candidate-controlled receipts, or terminal-source money. It is a
precisely scoped processed itemized reporting subtotal.

| Separate non-memo scope | Rows | Signed reported amount |
|---|---:|---:|
| Convention reporting | 4,582 | $184,248,899.46 |
| Electioneering notices | 78 | $12,002,885.86 |
| Levin reporting | 154 | $2,225,862.70 |
| Form 3X line 24 reported on SB | 7 | $3,406.83 |

These categories remain available; they are not discarded records. Memo-X
takes precedence in accounting buckets while its scope and role stay visible.

## Source-record review

A bounded Go probe read 60 source examples. It selected the largest prior
diagnostic group for each reviewed exception or line-23 type/memo category,
then that group's lowest ordinal. This is targeted review, not a statistical
sample or an exhaustive semantic audit.

- Ordinal 118 explicitly describes an earmarked contribution, lacks
  `disb_tp`, and has agreeing raw/clean recipient IDs. A missing type does not
  establish an ordinary committee-funded contribution.
- Ordinal 30 names a retail vendor, while the raw recipient ID equals the
  filer and the cleaned ID is absent. Raw-ID fallback would invent a
  self-transfer.
- Ordinals 6 and 24 have beneficiary names on a loan repayment and a refund.
  Name presence is not a conduit classifier.
- Ordinal 6070 has type `24Z`, a named service provider, and a different
  beneficiary committee. A committee endpoint need not be the cash payee.

The FEC's [earmark guidance](https://www.fec.gov/help-candidates-and-committees/filing-political-party-reports/earmarked-contributions/)
places forwarded contributions on Form 3X line 23. Its
[SSF guidance](https://www.fec.gov/help-candidates-and-committees/making-disbursements-ssf-or-connected-organization/coordinated-communications-ssf/)
also places in-kind contributions there. The calculation records that category
without inferring who supplied or received cash.

The [Form 4 instructions](https://www.fec.gov/resources/cms-content/documents/policy-guidance/fecfrm4i.pdf)
identify convention expenses, loans, and other disbursements. The
[Form 3X instructions](https://www.fec.gov/pdf/forms/fecfrm3xi.pdf)
identify Levin reporting and line 24's independent-expenditure category.
These retain separate scopes; they do not supplement Schedule E or create
new flow edges.

The previous diagnostic's 8,394 unmapped rows consist of reviewed separate
scopes, memo evidence, and 55 still-unreviewed form-line records:

| Exact form/line | Rows | Signed reported amount |
|---|---:|---:|
| F3X / 21 | 49 | $60,957.92 |
| F3X / 17 | 1 | $1,000.00 |
| F3 / 21B | 4 | $26,460.59 |
| F3 / 22 | 1 | $277.46 |

No name, purpose substring, publisher label, or known committee ID remaps
these rows. They remain unresolved with exact source membership.

## Integrity and identity

The result passed its strict JSON Schema, independent integer summation of
every group and decision bucket, and equivalence with the prior
[semantics audit](./schedule-b-semantics-2026-09-08.md) for source counts,
signed amount, non-memo amount, negative rows, and zero rows. The Go checks,
focused race tests, and calculation-contract tests pass.

The four-worker replay took 199.543 seconds and produced byte-identical
result JSON, including every group and identity. Both runs exited zero
without OOM events. Runtime and worker count are not part of result identity.

| Identity | SHA-256 or ID |
|---|---|
| Source fact set | `aa025a0d06c303562d8d3de7975cc203d897c77217d9b149ade7dc033369af4c` |
| Source manifest | `d977930d4b85d623c797a6cf5a251ffd7f105d315d972b54cd3e07ad86f0c9fb` |
| Calculation set | `b8c4fd7f61b8da5d4ee12ee837b7fc9c99444fd632fed1f776b452e0fac9e4cb` |
| Policy digest | `d8755d3d7e0f81c1662c2c7adbf8b4453993a7c8efd60fa929032581ff034e8a` |
| Group digest | `70f6e9cd67e1859fca3c60b945b33a01e00985dcd7b110868a0926e1762b0e07` |
| Result artifact | `5bf903319defce87e4d04ee5becf573aa34d8b9ed4a2ccee1044a6647f8d6ef8` |
| Reviewed records | `a4d600287a61ed5999fadb06cafec8725bf1205bb350780a13c2f6ac4f4b3d02` |

The result is 41,283,963 bytes. It stores derived groups, not a dense second
copy of facts. Fact-set identity, ordinal, and versioned predicate reconstruct
membership. Dates and full filing evidence remain in the immutable facts.

The validated result, reviewed-record JSON, independent validation result,
run/replay logs, digest sidecar, and completion marker are retained at
`/storage/dumps/audits/fec/schedule-b-reporting/2026-09-08/2024/`.

## Remaining boundary

`graph_eligible` is false. Sender-flow membership must distinguish reporting
roles, endpoint identity, in-kind activity, and conduit reporting before
matching qualifying Schedule B observations to Schedule A. Two disclosures
must support one economic-flow hypothesis, not two dollars.

The command has no active pointer, immutable publisher, persistent cache, or
Dagster asset. Those remain separate implementation work. Terminal-source
attribution and existing ArangoDB projections are unchanged.
