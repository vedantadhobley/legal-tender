# Committee-flow comparison candidates v1

This calculation consumes one verified Schedule A/B reconciliation publication
and emits each direct cross-ledger candidate before transitive component union.
Candidates retain both reported observations, exact match signals, signed and
absolute date gaps, evidence bands, and the number of competing candidates on
both sides.

The relationship is diagnostic. It carries no money, does not deduplicate either
ledger, and is not graph-eligible. The operational candidate-capacity guard fails
the publication before writing partial output; it is not a filtering rule.
