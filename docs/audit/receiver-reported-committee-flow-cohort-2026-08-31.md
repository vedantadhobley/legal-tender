# Receiver-reported committee-flow cohort audit — 2026-08-31

## Result

The complete 2024 processed Schedule A corpus supports a conservative first
committee-to-committee flow calculation. The accepted cohort contains 320,731
receiver-reported occurrences grouped into 180,283 source-recipient-role
edges, with a signed total of $4,672,820,179.49.

At the time of this audit, the result was not yet a published calculation or
ArangoDB projection. It is the complete-corpus evidence behind the accepted
[`fec/receiver-reported-committee-flows@1.0.0`](../../contracts/calculations/fec/receiver-reported-committee-flows/v1/contract.json)
membership boundary.

## Exact input

| Field | Value |
|---|---|
| Cycle | `2024` |
| Schedule A fact set | `8f69a2ef40316f62d8844ecf732342a3b4535136b434f60677d0608cb04465df` |
| Source release | `fec-a6e6f777c3c506b100a7709815bc89f2ae84749faf4343a4589f7ec1bac066a2` |
| Manifest SHA-256 | `b664e752a3bae186508a45bb6d09289ba2fe06f0b4149ecbd72063e7ef79b829` |
| Physical schema | `legal-tender.fec.schedule-a-parquet.v1` |
| Rows | 264,085,606 |
| Shards | 265 |

`audit-receipt-master-gaps` first exposed the need for this boundary while
explaining the receipt graph's placeholder entities. The separate
`probe-receiver-committee-flows` command then verified the immutable Schedule
A manifest and every Parquet shard before scanning the full relation.

The final 16-worker scan completed in 110.781 seconds. All 264,085,606 rows
entered exactly one terminal decision. Two rows had unknown amounts; every
other amount observation conserved.

## Rejected broad rule

The first probe treated matching `C########` values in `contbr_id` and
`clean_contbr_id`, a valid recipient committee, non-memo status, and a known
amount as sufficient flow evidence. That rule produced 4,972,970 rows,
188,425 edges, and $6,977 million.

It was wrong. The candidate cohort contained about 2.82 million rows marked
`entity_tp=IND`, dominated by receipt type `24T`, whose preserved description
identifies an earmarked intermediary treasury-out record. Schedule A contains
receipt, memo, intermediary, and outgoing semantic roles. A committee-shaped
ID does not establish money direction.

The broad rule was discarded. No result from it is publishable.

## Accepted identity and role boundary

A row can enter the first receiver-reported flow only when, in order:

1. normalization is valid;
2. the receiving filer is an exact `C########` ID;
3. raw `contbr_id` and publisher-cleaned `clean_contbr_id` are both exact
   committee IDs and agree;
4. the row is not a memo subtotal;
5. the amount is an exact reported signed value; and
6. the exact receipt type maps to an accepted inbound role.

Accepted roles and exact receipt codes are:

| Role | Codes | Rows | Signed amount |
|---|---|---:|---:|
| Affiliated transfer in | `18G`, `30G`, `31G`, `32G` | 26,304 | $3,419,866,954.76 |
| Registered-filer contribution | `15K`, `18K`, `30K`, `31K`, `32K` | 292,944 | $1,249,804,428.98 |
| Registered-filer in-kind contribution | `15Z` | 1,237 | $2,441,507.80 |
| Refund or repayment received | `20R`, `20Y`, `22Z` | 246 | $707,287.95 |

The accepted rows reference 7,556 source committees and 3,914 receiving
committees. They form 180,283 distinct source-recipient-role groups. There are
no included self-edge rows and no included rows with a conduit committee ID;
5,296 included rows retain a back-reference ID.

`entity_tp` and publisher-derived `is_individual` remain diagnostics. They do
not route money. The accepted cohort includes 738 `is_individual=true` rows
with a signed −$734,412.36 and 63 `entity_tp=IND` rows. Exact IDs and receipt
roles remain stronger evidence than those helpers, so these conflicts stay in
the cohort and remain measurable.

## Explicit non-result states

Known exclusions are not lost:

- 7,762,772 outbound-role rows (`24G`, `24I`, `24K`, `24T`, `24Z`);
- 271 semantic-memo-role rows (`10J`, `11J`, `15J`, `18J`, `30F`, `30J`,
  `31F`, `31J`, `32F`, `32J`);
- 32,123,678 earmarked-role rows (`15E`, `30E`, `31E`, `32E`); and
- 2,900 noncommittee-receipt-role rows (`10`, `11`, `12`, `16C`, `30`, `31`,
  `32`).

Earmarked rows remain inputs to later conduit and terminal-source attribution.
They are excluded only from direct committee-flow edges.

The final rule leaves 792,018 exact-ID candidate rows and $217,619,414.45 with
an unresolved receipt role. Of these, 792,009 have no receipt type and nine
use generic type `15`. It also leaves 2,101 one-sided source-ID rows and
$140,547,802.87 unresolved. Neither population is coerced into the graph.

## Method boundary

The executable Go policy now lives in
`internal/calculation/fec/committeeflows`. The diagnostic imports that policy;
it no longer owns a parallel classifier. A canonical fixture locks decision
order, four included roles, signed amounts, memo/outbound/earmark exclusions,
identity conflicts, unknown roles, and helper-field neutrality.

Schedule A is receiver-reported authority. Processed Schedule B remains future
sender-reported reconciliation evidence. It may corroborate or dispute a flow,
but it must not add a second dollar to this receiver-side calculation.

## Follow-up

The immutable calculation publisher, sparse exceptions, grouped results,
conservation manifest, CLI, and minimal Dagster asset shipped on 2026-09-01.
See the [publication audit](./receiver-reported-committee-flow-publication-2026-09-01.md).
The content-addressed ArangoDB projection and multi-hop query gate remain next.
