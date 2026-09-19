# Receipt source-role evidence

Status: implemented additive Go policy and read-only review commands. This
resolves overlap routing for a combined reported-source view, not cash
allocation, source-data correction, or terminal donor identity.

The [participant index](./receipt-participant-index.md) now calls this same policy
for each source occurrence. This adds a cycle-scale consumer, not another rule
set; its manifest keeps reference qualification and identity resolution separate.

The [receipt inventory](./committee-funding-basis.md) keeps both accepted
membership predicates. Its overlap component remains unchanged. This policy
adds one explicit source-role decision to each inspected occurrence, bound to
the same inventory, fact set, source ordinal, and full physical fields.

## Why the publisher flag does not resolve identity

The FEC's [`is_individual` methodology](https://www.fec.gov/campaign-finance-data/about-campaign-finance-data/methodology/)
uses transaction types and additional amount/report-line conditions. It is not
an assertion that a resolved natural person owns the contribution. Keep it as
publisher aggregate membership; do not use it as a person-identity edge.

The complete [2024 source review](../audit/receipt-source-evidence-2026-09-08.md)
found that 697 overlapping records fit the published small-dollar/report-line
condition. The remaining 41 appear on Form 3X line 17, outside that published
condition. All 738 carry matching raw/clean committee IDs and receipt roles
already accepted by the committee-flow policy. This is enough to retain the
reported committee observation, not to declare every source field correct.

## Policy `fec/reported-receipt-source-evidence@1.0.0`

1. For accepted committee-flow membership, use
   `reported_committee_observation` and retain the reported source committee ID.
   When the publisher individual predicate also includes the row, set
   `publisher_individual_overlap=true`. Do not emit a second individual source
   or count the amount twice. Do not change either historical aggregate.
2. Preserve `IND`/`CAN` entity-label conflicts as an independent flag. An exact
   reported committee ID is not proof of registration, resolved identity, cash
   ownership, or complete provenance. Existing master/terminal guards still apply.
3. For individual-only membership, retain
   `publisher_individual_identity_unresolved`. If either contributor ID field
   carries an exact-format committee ID without accepted committee-flow
   membership, use `conflicting_contributor_evidence_unresolved`. Do not silently
   promote one-sided or conflicting IDs into an identity.
4. Keep memo-only, unknown-amount, unresolved-recipient, and other receipt
   populations outside any implied resolved-source allocation.
5. Recognize the accepted earmark receipt codes `15E`, `30E`, `31E`, `32E` and
   separately recognize intermediary receipts `15I`/`15T`. Their meanings come
   from the FEC's [transaction code descriptions](https://www.fec.gov/campaign-finance-data/transaction-type-code-descriptions/).
   This adds evidence labels only; it does not change monetary membership.
6. A valid dedicated conduit-ID field plus a reviewed earmark/intermediary role
   yields `reported_structured_id_identity_unverified`. Keep the contributor and
   conduit IDs in different fields. An invalid ID, absent ID, unmatched role,
   or name-only observation has an explicit unresolved state.
7. Preserve memo and conduit-name evidence without parsing arbitrary names or
   ID-like memo substrings into links. A report back-reference requires
   same-filing resolution; it is not automatically a conduit relationship.
8. Every decision is terminal-attribution-ineligible. The
   `additional_conduit_amount_minor_units` value is always `"0"`: the annotation
   creates no additional amount. It does not assert that conduit fees or
   spending were zero.

These are observation-routing rules, not a person/organization resolver. No
candidate, committee, amount threshold, employer, or conduit-name exception is
embedded in the policy. The source-profile comparison to the FEC's published
$200 rule is an audit only; it does not become a replacement classifier.

## Earmark evidence and scope

The [FEC's reporting guidance](https://www.fec.gov/help-candidates-and-committees/filing-pac-reports/earmarked-contributions/)
distinguishes the original contributor, intermediary activity, and memo
reporting. A generic report back-reference can describe other relationships:
the official [reference-field description](https://www.fec.gov/campaign-finance-data/operating-expenditures-file-description/)
uses the report and transaction IDs to associate related records, not to assert
a conduit role.

A bounded audit of one original filing confirms that the sampled earmark rows
have empty structured conduit/back-reference fields in the submitted file as
well as null values in processed facts. The memo retains the description. That
does not prove that every filing has the same gap, nor that a name match can
repair it. The raw file remains audit evidence, not a new production source.

The separate [same-report association](./receipt-report-association.md) now
establishes bounded positive earmark/memo links using exact references and
role-qualified committee IDs. Its two-report original-file gate passes; it does
not alter this policy's earlier annotations. The later
[complete-cycle association publication](./receipt-conduit-publication.md) passes
for 2024; registration verification and broader roles remain open. Missing links stay
unresolved; no name-match fallback repairs them.

## Commands and verification

```bash
legal-tender pipeline fec review-funding-component \
  --storage-root /storage --basis-result <inventory-json> \
  --component overlapping_individual_and_committee

legal-tender pipeline fec inspect-funding-receipts \
  --storage-root /storage --basis-result <inventory-json> \
  --committee <committee-id> --component itemized_individual_only \
  --after-ordinal 0 --limit 20
```

The complete-component reviewer refuses populations above 10,000 rows. It uses
four workers, opens each member shard once, verifies its hash, reads every
selected member, and compares every bucket's counts and exact signed measures
against the inventory. It returns full physical fields and decisions in source
ordinal order. A limit, integrity error, or cancellation cannot produce a
successful partial review.

Inspection wraps the unchanged bounded source-page contract with decisions.
Missing fields, wrong physical types, or disagreement with the accepted row
classification fail annotation. Neither command changes source facts, an
inventory ID, canonical pointers, Arango, Dagster, or the existing API.

The [wire contracts](../../contracts/calculations/fec/committee-funding-basis/v1/)
define the outputs. The next funding gates remain complete receipt coverage,
opening balances, timing, negative adjustments, and economic roles before
terminal-dollar allocation. Production publication and orchestration remain
separate from these manual evidence commands.
