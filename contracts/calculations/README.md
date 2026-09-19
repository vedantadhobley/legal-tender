# Calculation contracts

Calculation contracts select and relate preserved source evidence without
mutating it. Each version directory contains:

- `contract.json` for input versions, ordered decisions, publication states,
  change propagation, and retention;
- `result.schema.json` for the complete calculation output;
- `fixtures/` for deterministic results derived from pinned source fixtures;
  and
- `fixtures/manifest.json` for input lineage and result digests.

Accepted processed-product calculations include:

| Calculation | Purpose |
|---|---|
| [`fec/candidate-itemized-individual-receipts/v1/`](./fec/candidate-itemized-individual-receipts/v1/) | Calculate authorized-candidate itemized individual receipt components and independent summary reconciliation. |
| [`fec/effective-independent-expenditures/v1/`](./fec/effective-independent-expenditures/v1/) | Calculate signed regular-report outside spending by spender, candidate, and stance. |
| [`fec/receiver-reported-committee-flows/v1/`](./fec/receiver-reported-committee-flows/v1/) | Select conservative receiver-reported committee flow by exact source identity and receipt role. |
| [`fec/receiver-flow-committee-identity-coverage/v1/`](./fec/receiver-flow-committee-identity-coverage/v1/) | Classify selected-master graph gaps by exact official registration evidence and block non-current identities from terminal stopping. |

The deferred raw-electronic-filing calculations are:

| Calculation | Purpose |
|---|---|
| [`fec/efile-report-family/v1/`](./fec/efile-report-family/v1/) | Assemble explicit amendment evidence and select one effective document as observed. |
| [`fec/effective-schedule-a/v1/`](./fec/effective-schedule-a/v1/) | Treat the selected document as a complete Schedule A replacement and emit added, modified, removed, and carried-forward row states. |
| [`fec/schedule-a-reconciliation/v1/`](./fec/schedule-a-reconciliation/v1/) | Match processed rows to exact raw occurrences, then align that evidence to the effective raw filing revision without confusing publisher lag with conflict. |

All remain draft until the corpus gates in the
[effective electronic-filing design](../../docs/design/effective-efile-calculations.md)
and [reconciliation design](../../docs/design/schedule-a-reconciliation.md)
close.
