# Receiver-reported committee-flow projection bundle v1

This readiness bundle freezes one immutable receiver-reported committee-flow
calculation and one same-cycle, same-release committee-master fact set. It
copies no calculation results or master facts.

The bundle is the only accepted input selector for the corresponding ArangoDB
projection. Its loader resolves immutable manifests and verifies every backing
artifact before graph construction.
