# Processed Schedule A receipt facts v1

One fact set is a deterministic projection of one immutable processed Schedule
A occurrence set. It emits one fact for each unique, source-valid `SUB_ID`.
Malformed and duplicate occurrences remain explicit evidence exclusions; row
order never chooses a winner.

Each fact retains all 81 source fields with SQL null, empty text, booleans, and
numeric lexemes intact. Typed groups expose source identities, independent time
dimensions, contributor and conduit disclosures, filing references, and signed
money as checked integer cents. Source decimal scale remains separate.

`memoed_subtotal` means only `memo_cd == "X"`. It does not remove the fact or
decide whether its amount counts. Amendment selection, memo and conduit
reconciliation, entity resolution, authorized-committee projection, totals,
and graph edges are later versioned calculations.
