# Candidate itemized-individual receipt fact bundle v1

This bundle freezes the four exact fact publications required by the compact
candidate receipt calculation:

- Schedule A columnar receipts;
- candidate-committee linkage;
- all-candidates summaries; and
- current-campaigns summaries.

Every input must belong to one FEC cycle and one coordinated source release.
The publisher verifies each mutable pointer against its immutable manifest and
verifies all referenced Parquet shards and classic fact artifacts before it
publishes the bundle.

The bundle contains no copied facts and performs no calculation. Its identity
is derived from the ordered fact roles, exact fact-set IDs, and immutable
manifest digests. The compact calculation resolves only those immutable
manifests; mutable current pointers do not participate after readiness.
