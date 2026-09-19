# ArangoDB candidate-receipt projection v1

This contract describes the result of the bounded, Go-only ArangoDB projection
probe. The projection consumes one exact candidate-receipt fact bundle, its
compact calculation, and same-release candidate and committee master facts.

The graph stores query-bearing candidate and committee entities, disclosed
candidate-committee relationships, calculated committee receipt components,
and complete candidate calculation results. Fine Schedule A facts remain in
their immutable Parquet fact set.

The probe database is content addressed and begins with `lt_probe_`. It never
writes the legacy `legal_tender` database. Projection metadata is published
last, so an interrupted database is resumable and cannot appear complete.

`result.schema.json` validates the command result and recorded benchmark.
