# Effective independent expenditures v1

This contract calculates candidate-directed outside spending from one exact
processed Schedule E fact set. It does not reinterpret filing amendments or
merge the separate 24/48-hour notice feed.

The processed relation is already the FEC's most-recent regular-report row
set. The compact membership predicate excludes memo-code `X`, requires an
exact reported amount, and otherwise includes signed cents without filtering
action code, transaction ID, expenditure type, or date. Included rows with a
spender, candidate, and `S`/`O` stance produce spender-to-candidate results.
Other included rows remain explicit sparse exceptions and conserve their
amount outside the attributed results.

Fine-grained source facts remain immutable. Ordinary membership can be
replayed from the fact-set identity and predicate; the calculation does not
write one duplicate decision record per source fact.
