# Classic FEC occurrence evidence v1

This contract covers the five cycle-scoped, pipe-delimited FEC products in the
initial coordinated release:

- `candidate-master`
- `committee-master`
- `candidate-committee-linkage`
- `all-candidates-summary`
- `current-campaigns-summary`

One publication names one dataset, cycle, exact source archive, exact selected
member, and coordinated FEC release. The artifacts preserve every physical row
as an occurrence or explicit issue, isolate duplicate publisher keys, and
describe semantic changes from the prior publication for the same dataset and
cycle.

This layer does not create entities, candidate-controlled relationships,
transactions, or totals. Normalized fact contracts consume only valid unique
record versions from this evidence boundary.
