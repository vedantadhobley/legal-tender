# Go and Dagster boundary

> **Status:** Accepted design direction. This is not a description of the
> legacy Python implementation. The rewrite's coordinated FEC source-release
> assets, partitioned Schedule A occurrence/fact assets, and partitioned
> classic FEC occurrence/fact assets implement this boundary. Exact multi-
> partition fact-bundle readiness and the bundle-fed compact calculation also
> implement it and passed the real 2024 gate. Isolated graph projections and
> the [committee-flow chain](./committee-flow-orchestration.md) now implement
> this boundary. The [manual summary handoff](./committee-summary-source.md#manual-dagster-handoff)
> also passes four-cycle replay without automatic triggers; unified production
> serving remains open.

## Decision

Dagster is Legal Tender's data control plane. Go is its data plane and
application language. Python exists only where Dagster's Python API makes it
unavoidable.

This is a hard architecture boundary, not a preference to revisit one asset at
a time. If code can perform a campaign-finance operation without importing
Dagster, it belongs in Go.

```text
Dagster asset, partition, schedule, or sensor
                    |
             thin Python adapter
                    |
          versioned Go command or service
                    |
       raw snapshots and ArangoDB domain state
                    |
       structured result and lineage metadata
                    |
             Dagster materialization
```

Temporal is not part of the target stack. OpenLineage and Marquez are not
initial dependencies. Dagster already supplies the operational asset graph,
run history, partition state, scheduling, and materialization metadata needed
inside this pipeline. Source-record and calculation lineage remain domain data
owned by Go; an orchestration catalog cannot replace them.

## Ownership

| Concern | Owner |
|---|---|
| Asset graph, asset selection, schedules, sensors, automation, partitions, and partition mappings | Dagster definitions in Python |
| Launching a Go operation and translating its result into Dagster events | One thin Python adapter |
| Source discovery, download, fingerprinting, and snapshot manifests | Go |
| Decompression, parsing, normalization, validation, and amendment handling | Go |
| Entity resolution, classification, graph construction, and ArangoDB queries | Go |
| Terminal-source attribution, path calculations, cycle calculations, and aggregates | Go |
| Change-set calculation and affected-entity planning within a partition | Go |
| Investigative API, UI backend, maintenance commands, and analysis tools | Go |
| Operational asset lineage and run history | Dagster |
| Source-to-record-to-fact-to-projection lineage | Go-owned domain records |

Dagster's dependency graph describes when operations may run. It does not
contain the business rules that decide what a contribution means, which entity
it belongs to, where attribution stops, or which derived records changed.

## Python budget

Python may contain only:

- Dagster `Definitions`, assets or asset specifications, jobs, resources,
  schedules, sensors, partitions, partition mappings, and checks.
- Configuration translation required to launch a Go command.
- A small process adapter that validates the command's result envelope and
  emits Dagster metadata, checks, and failures.

Python must not contain:

- FEC, lobbying, corporate-resolution, or graph domain rules.
- Source clients, parsers, row transforms, dataframe processing, or
  normalization.
- AQL generation, direct ArangoDB access, or application queries.
- Entity resolution, committee classification, terminal-source logic, money
  tracing, cycle calculations, or aggregation.
- A second implementation of constants, schemas, or validation rules owned by
  Go.
- The investigative API or UI backend.

The Dagster Python environment should depend on Dagster and the minimum
configuration or schema packages required by the adapter. It should not gain
data clients such as an ArangoDB driver or general dataframe libraries. A
change that adds Python outside the control-plane boundary requires an
architecture decision.

## Go operation contract

Every pipeline operation must be independently executable and testable without
Dagster. A Dagster asset invokes a versioned Go command with explicit inputs
such as source, source snapshot, election cycle, disclosure period,
calculation version, and run identifier.

Each command must:

1. Be idempotent for the same input identity and calculation version.
2. Validate its inputs before mutating derived state.
3. Preserve or reference the input snapshot and source-record lineage.
4. Emit a versioned structured result envelope on standard output.
5. Put human diagnostics on standard error and use stable nonzero exit codes
   for failures.
6. Report input and output watermarks, record counts, changed keys, data-quality
   checks, timing, and calculation version where applicable.
7. Commit domain writes before reporting success.

The Python adapter may validate and forward this envelope. It may not interpret
domain records or repair a failed result.

Use one shared adapter rather than a custom wrapper for every asset. Start with
local process execution inside the Docker deployment. Introduce a remote Go
worker protocol only if measured isolation, scale, or scheduling requirements
justify it.

## Incremental processing

Dagster and Go operate at different invalidation levels:

1. A schedule or sensor asks a Go discovery command to inspect a source.
2. Go compares source fingerprints and snapshot identities with the ingestion
   manifest.
3. Dagster materializes only affected coarse partitions, expected to be based
   on source plus disclosure period or election cycle. The exact partition
   scheme remains subject to source-specific design.
4. Within a partition, Go derives the changed facts, entities, graph edges, and
   projection keys. It recomputes only that work set.
5. Downstream commands consume explicit input watermarks and change sets. They
   skip successfully when their relevant inputs and calculation version are
   unchanged.

Dagster partitions must not expand to one partition per graph entity merely to
model row-level invalidation. Dagster owns observable, retryable units of work;
Go owns fine-grained change propagation. Inherently global calculations such
as some community or centrality algorithms may require a graph-wide refresh,
but only when their input watermark or algorithm version changes.

## Lineage boundary

Dagster records which asset partition and code version produced a
materialization. Go records which immutable source snapshots and records
produced each normalized fact, relationship, attribution, and aggregate.

Every investigative result must remain traceable without Dagster being online.
This keeps evidence lineage available to the API and UI and makes a future
orchestrator change possible without rewriting the political-money model.

OpenLineage or another neutral lineage transport may be added later if systems
outside Dagster need to consume operational lineage. Marquez is not justified
while Dagster is the only lineage consumer and catalog.

## Verification

- Go unit and integration tests are authoritative for all domain behavior.
- Python tests cover only Dagster definition loading, dependency and partition
  wiring, command invocation, result-envelope validation, and event mapping.
- A shared contract fixture verifies that Go output and the Python adapter use
  the same result-envelope version.
- Differential tests compare accepted legacy behavior with Go output at
  preserved source-record grain and at approved projections.

The first vertical-slice documents define the asset graph, partitions, Go
commands, result envelope, and state transitions against this boundary. The
as-built state is tracked in the [Go rewrite ledger](../go-rewrite.md).
