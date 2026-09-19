# ArangoDB resolved independent-expenditure projection v2

Version 2 consumes the resolved spender-candidate-stance aggregate. It creates
a new content-addressed probe database and does not relabel or mutate the v1
reported-ID projection.

Each edge retains confirmed, resolved, and unverified count and signed-amount
components. Projection metadata also carries the exact count and amount left
outside candidate edges because identity was ambiguous or unresolved.
