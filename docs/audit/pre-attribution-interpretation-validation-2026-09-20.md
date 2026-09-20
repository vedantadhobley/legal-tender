# Pre-attribution interpretation validation — 2026-09-20

Status: complete 2024 diagnostic. No source fact, calculation policy, graph,
current pointer, counted amount, terminal rule or allocation rule changed.

The user accepted all four recommendations later on 2026-09-20. Their separate
[2024 calculation publications](./pre-attribution-interpretation-publications-2026-09-20.md)
preserve this diagnostic and the existing v1 calculations and graphs.

This validation answers the three corpus questions left by the
[pre-attribution review](../design/pre-attribution-review.md): what the current
Schedule E replacements actually contain, what one-sided committee IDs mean in
Schedule A and B, and how far apart the current A/B amount-and-role candidates
can be. The existing conduit publication already has a complete-corpus gate and
the FEC reporting instructions answer its outstanding amount/date question.

## Reproduction and evidence

The new read-only Go command verifies the immutable manifests and complete
decision/exception artifacts, checks the Schedule B profile against the exact
reconciliation fact set, and replays all 308,488 saved A/B components:

```text
legal-tender pipeline fec audit-pre-attribution-interpretations \
  --storage-root /storage \
  --candidate-resolution /storage/calculations/fec/independent-expenditure-candidate-resolution/current/2024.json \
  --receiver-flows /storage/calculations/fec/receiver-reported-committee-flows/current/2024.json \
  --schedule-b-semantics /storage/dumps/audits/fec/schedule-b-semantics/2026-09-08/2024/result.json \
  --committee-flow-result /storage/dumps/audits/fec/committee-flow-reconciliation/2026-09-08/2024/result.json \
  --committee-flow-evidence-root /storage/calculations/fec/committee-flow-reconciliation/v1 \
  --output /storage/dumps/audits/fec/pre-attribution-interpretations/2026-09-20/2024/result.json
```

The retained 13,634-byte result has SHA-256
`61e2e9016613b4185def153e53c26e1094a6c06fe02f0733d04b28ca2b64aa8c`.
It passes its [JSON contract](../../contracts/audits/fec/pre-attribution-interpretations/v1/result.schema.json).
All eight blocking checks pass. The complete Go suite and source-contract schema
suite also pass.

## Schedule E candidate endpoints

The FEC describes `CAN_ID` as filer-provided and possibly absent; candidate
name and office context may appear instead. The candidate master separately
defines the Commission-assigned identity. See the FEC
[independent-expenditure file description](https://www.fec.gov/campaign-finance-data/independent-expenditures-file-description/)
and [candidate-master description](https://www.fec.gov/campaign-finance-data/candidate-master-file-description/).

The current 1,811 `resolved` decisions move $299,813,805.04 away from the
reported endpoint:

| Current replacement basis | Rows | Signed amount | Distinct endpoint pairs |
|---|---:|---:|---:|
| Reported ID absent from the pinned candidate master | 757 | $33,509,752.37 | 57 |
| Reported ID exists but conflicts with the exact name/office context | 1,054 | $266,304,052.67 | 48 |

The conflict cohort is 58.20% of replacement rows and 88.82% of replacement
money. This is not a corner case. A current `resolved` aggregate therefore
cannot serve as a source-reported or confirmed default total even though every
replacement is deterministic and preserves its original assertion.

Recommendation at validation time, accepted later on 2026-09-20:

- retain the reported candidate and project-resolved candidate as separate
  endpoints over one source fact;
- call ID/context agreement `confirmed`;
- call an absent-master ID plus unique context `inferred`, without rewriting
  the reported assertion;
- call a present-ID/context disagreement `conflicting`, not `resolved`;
- expose reported and inferred totals separately, including unverified and
  unresolved remainders; and
- do not let an inferred replacement silently enter the default counted edge.

No fuzzy matching or model inference is needed for this boundary.

## One-sided committee IDs

Both detailed ledgers give the same result:

| Ledger | One-sided rows | Signed amount | Non-memo amount | Self references | Raw/clean conflicts |
|---|---:|---:|---:|---:|---:|
| Schedule A | 2,101 | $140,547,802.87 | Not classified at this earlier decision point | 2,101 | 0 |
| Schedule B | 97,850 | $299,309,098.85 | $294,210,741.67 | 97,850 | 0 |

Every one-sided row has the raw committee ID only, and that ID equals the
reporting/receiving committee itself. There are no clean-only rows and no
raw/clean conflicts in either inspected population. Selecting the raw fallback
would manufacture self-flow edges rather than recover counterparties.

Recommendation at validation time, accepted later on 2026-09-20: retain the current requirement that
raw and publisher-cleaned counterparty IDs agree before an edge carries money.
Preserve one-sided rows as reported self-reference evidence outside that
ledger. A later policy version can improve the state name; no fallback edge is
justified.

## Schedule A/B date gaps

The component replay finds 155,364 dated one-to-one components. Of these,
130,650 have the same directed endpoints, role and amount but different dates.
The receiver-side Schedule A date is later in 127,166 cases (97.33%) and earlier
in 3,484 cases. That dominant direction is consistent with separate sender and
receiver reporting, but it does not prove shared payment identity.

| Absolute date gap | Components | Share of different-date candidates |
|---|---:|---:|
| 1–3 days | 21,161 | 16.20% |
| 4–10 days | 44,654 | 34.18% |
| 11–30 days | 50,178 | 38.41% |
| 31–90 days | 13,347 | 10.22% |
| 91–365 days | 1,278 | 0.98% |
| More than 365 days | 32 | 0.02% |

The maximum gap is 1,099 days: two independently reported $2,000 contribution
observations with the same committee pair. This is direct counterevidence to
unbounded role-plus-amount grouping as a default investigative association.
Transaction types `15K`/`24K` account for 104,048 different-date components;
the issue is not confined to an obscure role.

Recommendation at validation time, accepted later on 2026-09-20:

- preserve both ledger observations and all diagnostic candidates;
- publish individual comparison candidates with exact `date_gap_days`, match
  signals and ambiguity, instead of allowing distant matches to union a broad
  component before presentation;
- carry no money and perform no A/B deduplication through this relationship;
- use same-day, 1–10, 11–30, 31–90 and over-90-day evidence bands; and
- if the product needs a default, use at most 30 days as an adjustable display
  filter, not as proof of identity. That retains 88.78% of different-date
  candidates while leaving every later candidate available on request.

The FEC assigns transaction IDs within a filing, so the opposite committees do
not share a global payment identifier. See the FEC
[committee-to-candidate file description](https://www.fec.gov/campaign-finance-data/contributions-committees-candidates-file-description/).

## Conduit association

No new conduit algorithm was required. FEC instructions say the recipient
reports the original contributor as the contribution and the conduit as a
supporting memo item; the conduit date may be later and its memo can contain a
total covering several earmarked contributions. See
[contributions received through conduits](https://www.fec.gov/help-candidates-and-committees/filing-reports/contributions-received-through-conduits/)
and [earmarked-contribution reporting](https://www.fec.gov/help-candidates-and-committees/filing-ssf-reports/earmarked-contributions/).

Recommendation at validation time, accepted later on 2026-09-20: retain the exact-reference pair/star
association with zero added money and without amount/date equality. It is
reported context, not another payment, economic-origin proof or allocation.

## Resulting boundary

The evidence supports one common rule: reported observations, countable
financial edges, inferred endpoint alternatives, conduit context and A/B
comparison candidates must remain distinct edge families. Only an explicitly
accepted financial edge may carry money into later terminal attribution.

The next implementation should version the Schedule E and A/B replacements;
it must not rewrite the existing immutable facts or graphs. The 2024 candidate
slice should consume the new reported/resolved views only after the user accepts
these recommendations.
