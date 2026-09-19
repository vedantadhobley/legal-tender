# Shared-reference original-source review — 2026-09-14

Status: final additive group rule passes all selected original-source cases.
The [rule and review contract](../design/shared-reference-group-rule.md) owns
behavior. No existing association publication, graph, source artifact, money
selection or terminal rule changed.

## What the original records show

The Go reviewer selected all eleven structural witnesses from the
[retained profile](./shared-reference-profile-2026-09-14.md), recovered every
immediate exact peer of each shared record, and read all 700 distinct full source
occurrences. Complete incidence and endpoint streams were verified. Original
recipient/report/transaction/schedule fields and directions agree with the
selected reference evidence. All prior sampled role counts, signed sums and
source examples agree with the original facts.

The selected records support a genuine shared-memo shape, not a blanket relaxation
of the degree check:

| Shared ordinal | Exact peers | Source finding | Additive group result |
|---|---:|---|---|
| 1,352 | 140 | All originals reference one memo; source text says the displayed donors exceed the itemization threshold; sums differ | Reported shared-memo association |
| 73,136 | 2 | Both originals reference one memo; sums equal | Reported shared-memo association |
| 699,788 | 47 | 21 originals have conflicting reported committee evidence | Conflicting group IDs |
| 3,698,152 | 2 | Equal sum, but one original has a conflicting reported committee ID | Conflicting group IDs |
| 1,902,233 | 7 | All originals have conflicting reported committee evidence | Conflicting group IDs |
| 1,454,685 | 13 | Memo has `ORG` entity type and no reported committee ID | Unsupported group roles |
| 2,195 | 93 | Previously uncharacterized peer is a non-memo type `10` receipt | Unsupported group roles |
| 73,923,483 | 2 | Shared memo also references another memo with nine exact peers | Non-leaf group peers |
| 1,593,484 | 364 | Four originals have another incoming reference; one additional original has an unsupported contributor role | Non-leaf group peers |
| 127,960,494 | 17 | One conflicting ID plus two null-type, opposite-signed adjustments | Conflicting group IDs; all role evidence retained |
| 124,561 | 2 | Shared record is itself a non-memo original linked to a larger memo group | Non-leaf group peers; all role evidence retained |

The first two cases qualify 142 original occurrences **within this selected
review only**. The nine other groups stay unresolved under the new limited rule.
This is not 142 new Arango edges or a whole-cycle publication. The earlier full
profile's 2,637,285 originals in complete compatible groups identify the next
publication population to test, not an already accepted graph count.

The source texts support the rationale for preserving unequal totals. FEC's
own examples also distinguish original-contributor entries from aggregate
conduit memo totals that can include unitemized receipts. Neither evidence source
proves that every observed difference here is unitemized money, so no residual
amount is invented. [Official reporting examples](https://www.fec.gov/updates/earmarked-contributions/).

Names and memo phrases did not select witnesses, qualify groups or repair IDs.
The record with matching totals and a conflicting ID remains unresolved despite
that equality. The `ORG` case is not converted into a committee through a donor's
ID or a familiar organization name.

Dates remain separate. For example, shared ordinal 73,136 has a 2023-01-09 receipt
date, while original ordinal 73,137 has 2022-12-31. Both belong to the retained
2024 source partition. The reviewer preserves that fact; it does not force dates
inside the acquisition cycle or require original and memo dates to match.

## Implementation and tests

- Added the pure, streaming `earmarkassociation.Group` rule under
  `fec/complete-earmark-memo-star-association@1.0.0`. The one-to-one rule stays
  unchanged. The new rule requires complete, safe, leaf-peer coverage and all
  compatible roles/IDs; it adds zero money and is terminal-ineligible.
- Added `review-shared-receipt-references`, exact profile loading, bounded
  whole-neighborhood source recovery, direction checks and separate old/new rule
  output. Review bounds fail explicitly rather than truncating a group.
- Added batched full-source retrieval with one hash pass per touched shard,
  shared full-row decoding and typed/source classification comparison. The
  existing manifest loader still verifies the complete original backing once.
- Fixture tests cover missing/duplicate/excess peers, unsafe endpoints, mixed
  roles, wrong sole peers, changed report scope, reciprocal incidence dedup,
  corrupted input, size bounds, negative/zero/null amounts and decoder ownership.
  The streaming rule also passes a 100,000-peer synthetic group.
- Full affected calculation-package and CLI/receipt-graph regressions pass.
  Targeted race tests and vet pass. Existing paging and single-ordinal source
  reads agree with the new batch reader on full reconstructed rows.

The earlier source-only run and final additive-rule run return identical complete
source rows, reference neighborhoods, prior profile evidence and old-rule
dispositions. Only the separately labeled group-rule output is added.

## Retained execution

Storage-relative parent:

```text
dumps/audits/fec/shared-reference-source-review/2026-09-14
```

`attempt-01` is the source-only investigation. `attempt-02` is the final review
with the additive rule. `attempt-03` repeats that final executable with a
four-CPU quota. All three have `exit-status.txt` with `exit_code=0`.
The [runner](../../scripts/run-shared-reference-review.sh) retains its executable,
input manifests, arguments, results, timestamps and checksums. Existing inputs
were mounted read-only; networking was disabled.

| Final artifact | SHA-256 / ID |
|---|---|
| Executable | `b14da28a70e75127c549097c3f143cffa6eacff048e71e476ff9ea52cd712bd3` |
| Result | `d60c39c2ed85fe51092768d9a37b36a25603b70c57012360ae650e7af5ed33b6` |
| Profile | `13c3295b8d210f6e82b2ceaa77f5c0a9dde357fb68b3452df0fb9796bb2d9b36` |
| Fact set | `8f69a2ef40316f62d8844ecf732342a3b4535136b434f60677d0608cb04465df` |
| Topology | `3192971a9cbd28696cbf911ece53b7362a5a77c7c84be4e262236dd8b1c2acca` |
| Reference join | `1a66232f2d0afb11a0b4e38af2d7caa23c9ea34db9c909f0698ac00dd7514fab` |

Final timestamps are 20:12:21–20:12:54 UTC: approximately 33 seconds, including
backing verification. The complete final directory occupies 37,515,151 bytes,
including the executable. All retained checksums pass. The container had an
eight-CPU quota, 4 GiB memory/no-swap cap and a 2 GiB Go heap limit. No standing
budgets changed. This is a bounded review latency, not whole-cycle processing
performance or interactive-query latency.

The fresh four-CPU replay ran at 20:17:39–20:18:11 UTC, approximately 32 seconds.
It passed every retained checksum and produced a byte-identical `result.json`
to `attempt-02`. The replay used the same memory limits and did not change any
source or graph publication.

## Next

Later work completed the [full-cycle publication and isolated graph gate](./shared-conduit-publication-2026-09-14.md).
The following was the next boundary at this review's completion:

Publish the new rule over complete compact inputs with bounded streaming group
membership and old/new disposition conservation. Retain a separate immutable
publication and then add its associations to a new graph generation. Do not
overwrite the existing graph, promote every role-compatible shared row, treat
unresolved peers as terminal sources, or use this bounded reviewer as a
whole-cycle execution strategy.
