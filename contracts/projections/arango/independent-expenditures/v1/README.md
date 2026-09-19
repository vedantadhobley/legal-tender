# ArangoDB independent-expenditure projection v1

Status: implemented probe contract.

This projection converts one exact published effective independent-expenditure
calculation into query-bearing ArangoDB edges. It does not copy Schedule E
facts and does not change the calculation.

## Inputs

- One immutable `fec/effective-independent-expenditures@1.0.0` calculation
  manifest and its verified result and exception artifacts.
- Candidate-master and committee-master fact manifests from the same cycle and
  coordinated FEC release.

The projection identity hashes the model version, calculation and Schedule E
fact-set identities, both calculation/fact manifest digests, and both master-
fact identities and manifest digests.

## Physical model

The isolated named graph is `independent_expenditures`.

| Collection | Type | Grain |
|---|---|---|
| `entities` | vertex | One referenced cycle-scoped candidate or spending committee. |
| `independent_expenditure_edges` | edge | One effective calculation result from spender committee to candidate, with support/opposition kept separate. |
| `projection_metadata` | document | One content-addressed completion record with exact inputs, counts, amounts, and master coverage. |

Each edge preserves exact signed minor units, source counts, result ID,
calculation-set ID, Schedule E fact-set ID, and source-release ID. It is a
reported independent expenditure around a candidate. It is not a donation,
candidate-controlled receipt, literal payment to the candidate, or inference
of coordination or influence.

Only entities referenced by an edge enter this graph. A missing same-cycle
master fact creates an explicit `missing_master_fact` placeholder and makes the
projection `partial`; it does not drop the edge or invent display attributes.

## Publication

The database name is `lt_ie_probe_<cycle>_<projection-prefix>`. The probe never
writes the legacy database, never drops or truncates a database, and publishes
metadata last. An identical retry verifies counts and exact signed amounts,
then reuses the completed database.

The result contract requires:

- entity, edge, and stance-count conservation;
- exact signed-cent readback conservation from ArangoDB;
- separate support and opposition edges;
- complete input lineage;
- explicit missing-master coverage; and
- repeated candidate-inbound, spender-outbound, and stance-summary query
  execution.
