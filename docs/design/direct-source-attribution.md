# Direct source-appearance attribution

Status: implemented and accepted for the complete retained 2024 Schedule A
cycle. The [2024 gate](../audit/direct-source-attribution-2026-09-20.md) passes
exact conservation and worker-varied replay. Durable publication and Dagster
wiring are deferred until a concrete downstream consumer needs them. Additional
cycles, resolved identities, committee-chain allocation and graph projection
remain separate work.

## Accepted boundary

The calculation answers one narrow question: within receipts reported by a
uniquely authorized candidate committee, which exact Schedule A source
appearances can carry their own reported amount without inventing a
committee-chain allocation?

The accepted answer is:

- a known, nonmemo `itemized_individual_only` occurrence with an earmarked
  receipt role is `explicitly_earmarked`;
- another known, nonmemo `itemized_individual_only` occurrence is `direct`;
- every other nonmemo candidate-linked occurrence is `unresolved`; and
- memo subtotal appearances remain excluded evidence and add no second amount.

This follows the FEC's reporting distinction. For a candidate committee, the
original earmarked contribution and the conduit memo item describe the same
activity; the conduit entry is not a second contribution. See the FEC guidance
for [candidate committee conduit receipts](https://www.fec.gov/help-candidates-and-committees/filing-reports/contributions-received-through-conduits/)
and [PAC earmark reporting](https://www.fec.gov/help-candidates-and-committees/filing-pac-reports/earmarked-contributions/).

`direct` is a calculation disposition, not a resolved identity claim. The
endpoint remains the reported source appearance at one source-row ordinal. It
is not a deduplicated person, organization, employer relationship or corporate
family.

## Candidate routing

Candidate scope uses the existing accepted same-cycle CCL rule. A committee
routes receipts to a candidate only when its grouped relationship is uniquely
`authorized` through `A` or `P` designation evidence. Invalid,
mixed-designation, conflicting and shared authorization remains outside
financial routing. The calculation does not copy one committee's money to two
candidates.

This authorization join is context, not evidence that money was paid to a
candidate personally. It also does not turn every committee receipt into a
transfer. The FEC distinguishes transfers between a candidate's authorized
committees from contributions involving other committees. See the FEC guidance
on [transfers between candidate committees](https://www.fec.gov/help-candidates-and-committees/making-disbursements/transfers/).

## Exact conservation

The command verifies the complete participant publication, its Schedule A
ancestry, the immutable candidate-receipt fact bundle and that bundle's exact
candidate–committee linkage facts before scanning.

Every participant occurrence enters exactly one cycle-level scope:

```text
complete participant population
├── outside authorized candidate scope
└── authorized committee scope
    ├── direct
    ├── explicitly earmarked
    ├── unresolved
    └── excluded memo subtotal
```

Rows, known/unknown amount counts, sign counts and signed minor units conserve
at both cycle and candidate levels. Unknown amounts stay unresolved. Negative
and zero observations remain explicit.

The compact output stores candidate aggregates and a versioned predicate. It
does not materialize a decision row for every source occurrence. Membership is
reproducible from the exact participant publication and
`(schedule_a_fact_set_id, source_row_ordinal, candidate_id)`.

## Command

```text
legal-tender pipeline fec calculate-direct-source-attribution \
  --storage-root <storage-root> \
  --schedule-a-facts <immutable-schedule-a-manifest> \
  --participant-manifest <complete-cycle-manifest.json> \
  --expected-participant-id <sha256> \
  --receipt-bundle <immutable-bundle-id.json> \
  --workers 8
```

`--expected-calculation-id` turns a replay into an exact identity gate. Worker
count is operational and does not enter logical identity. The normative output
contract is
[`direct-source-appearance-attribution/v1`](../../contracts/calculations/fec/direct-source-appearance-attribution/v1/README.md).

## Deliberate exclusions

The calculation does not:

- allocate committee-chain dollars;
- infer cash availability, FIFO order or pooled shares;
- resolve people, organizations, employers or corporate families;
- attribute unitemized receipts or opening cash to named sources;
- combine candidate receipts with independent expenditures; or
- create or update ArangoDB vertices or edges.

Those exclusions prevent a supported occurrence-level partial attribution from
being presented as complete upstream economic ownership.
