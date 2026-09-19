# ArangoDB receiver-reported committee-flow projection v1

This probe projects one verified receiver-flow readiness bundle into a new
content-addressed ArangoDB database. It contains only referenced committee
vertices, grouped receiver-reported flow edges, and completion metadata.

The result contract requires exact count and signed-cent readback, explicit
missing-master coverage, complete-graph topology metrics, and representative
neighborhood, all-path, shortest-path, and cycle query measurements.
