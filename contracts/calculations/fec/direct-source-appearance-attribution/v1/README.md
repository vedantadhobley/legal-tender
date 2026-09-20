# Direct source-appearance attribution v1

This calculation applies the accepted partial attribution boundary to one
complete, immutable Schedule A cycle. It attributes known nonmemo
`itemized_individual_only` occurrences to the uniquely linked candidate while
keeping direct and explicitly earmarked appearances exclusive. It leaves all
other candidate-linked receipt amounts unresolved.

The result does not claim that a reported source appearance is a resolved
person or organization. Employer and occupation text do not establish an
affiliation. A committee counterparty is not treated as the upstream source,
and the calculation does not allocate any committee-chain amount.

## Exact inputs

The calculation binds:

- the complete receipt-participant calculation and manifest bytes;
- its exact Schedule A fact set, manifest, row population and FEC release;
- the immutable candidate-receipt fact bundle; and
- the exact candidate–committee linkage fact set from that bundle.

Only a committee with one accepted same-cycle `A` or `P` authorization may
route occurrences to a candidate. Conflicting, invalid, mixed-designation and
shared authorizations remain outside financial routing.

## Conservation

Every participant occurrence is in exactly one of
`outside_authorized_candidate_scope` or `authorized_committee_scope`. Every
authorized occurrence is then in exactly one of:

- `direct`;
- `explicitly_earmarked`;
- `unresolved`; or
- `excluded_memo_subtotal`.

Rows, known and unknown amount counts, sign counts and signed minor units must
conserve at the cycle and candidate levels. Unknown amounts remain unresolved.
Memo subtotal appearances remain evidence and add no second amount.

Ordinary source membership is reproducible from the versioned predicate and
the tuple `(schedule_a_fact_set_id, source_row_ordinal, candidate_id)`. The
compact result does not copy hundreds of millions of source decisions.

`result.schema.json` is the normative JSON contract.
