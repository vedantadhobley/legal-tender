# Compact candidate itemized-individual receipt calculation v1

This physical contract replaces one materialized decision JSON object per
Schedule A fact. Calculation membership is the exact columnar fact-set
manifest plus the versioned ordered predicate in the compact calculation
manifest.

The publisher materializes only:

- unresolved decision and invalid routed-date exceptions, keyed by source row
  ordinal; and
- candidate results and source-summary reconciliations.

Included and ordinary excluded rows remain directly drillable through the
declared predicate columns in the immutable Parquet fact set. No dense bitmap
or duplicate decision ledger is required.

`manifest.schema.json` defines publication and exact input ancestry.
`exception.schema.json` defines sparse exceptional membership. Candidate
results continue to use
`../../v1/result.schema.json` and calculation semantics remain version 1.0.0.
