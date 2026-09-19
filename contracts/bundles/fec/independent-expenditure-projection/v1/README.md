# Independent-expenditure projection bundle v1

This bundle freezes the exact publications required by one ArangoDB
independent-expenditure projection:

- one effective Schedule E calculation;
- one candidate-master fact set; and
- one committee-master fact set.

Every input must belong to one FEC cycle and one coordinated source release.
The publisher verifies each mutable pointer against its immutable manifest and
fully verifies both calculation artifacts and both master-fact artifacts.

The bundle contains no copied facts, calculations, or graph documents. Its
identity derives from the calculation, Schedule E fact, and master-fact set
identities and immutable manifest digests. The projector resolves only those
immutable manifests; mutable current pointers do not participate after
readiness.

The bundle does not change graph identity. Identical underlying inputs reuse
the same content-addressed projection even if orchestration reaches them
through a different materialization of this deterministic readiness boundary.
