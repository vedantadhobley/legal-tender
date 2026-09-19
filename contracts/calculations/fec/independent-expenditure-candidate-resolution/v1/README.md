# Independent-expenditure candidate resolution v1

This contract resolves the candidate reference on every attributed effective
Schedule E fact before graph grouping. It preserves the reported candidate ID
and emits one explicit decision for each fact.

The first method is deliberately narrow. It compares the Schedule E candidate
name and office context with the candidate-master fact set from the same FEC
cycle and coordinated source release. Name comparison uses an uppercase
Unicode alphanumeric token multiset so `SMITH, JANE` and `Jane Smith` compare
equally. It performs no fuzzy, nickname, semantic, or LLM matching.

`confirmed` decisions have exact ID-and-context corroboration. `resolved`
decisions use one unique exact context to repair or supply the ID. `unverified`
decisions retain a reported ID that exists in candidate master when the
reported context cannot corroborate it; this preserves strong source identity
without hiding the quality limitation. Only `ambiguous` and `unresolved`
decisions lack a projectable candidate ID. A future historical-alias input must
be a separate versioned source rather than an implicit scan of other cycle
files.
