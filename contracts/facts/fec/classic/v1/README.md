# Classic FEC normalized facts v1

One fact set is a deterministic projection of one immutable classic FEC
occurrence set. It emits one fact for each unique, source-valid publisher key.
Malformed and duplicate occurrences remain in the evidence set and are counted
as excluded; they are never resolved by row order.

Every fact retains all source fields and adds typed values beside them. Summary
money remains a separate publisher assertion. Exact source text is retained,
and losslessly representable amounts expose signed integer cents as strings.
Blank, unsupported-precision, and overflowing values remain distinct.

Candidate/committee entities, authorized relationships, current profiles,
reconciliation, and money totals are later projections over these facts.
