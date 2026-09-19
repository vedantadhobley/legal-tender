# Committee-flow Dagster boundary

Implemented in [the thin asset module](../../orchestration/flow_evidence.py)
and registered in the separate rewrite code location. Go owns selection,
reconciliation, source verification, publication, graph import, and readback.
Python passes manifest paths and exposes validated command results. See the
[publication contract](./committee-flow-publication.md),
[observation graph](./arango-committee-flow-evidence.md), and
[isolated execution gate](../audit/committee-flow-dagster-2026-09-08.md).

## Exact input handoff

| Asset | Partition | Inputs passed to Go |
|---|---|---|
| `fec_committee_flow_reconciliation` | `cycle` | Same-cycle A/B fact outputs and the coordinated release output |
| `fec_committee_flow_evidence_bundle` | `bundle=committee-flow-evidence`, `cycle` | Same-cycle calculation output and only `dataset=committee-master` for that cycle |
| `arango_committee_flow_evidence` | `cycle` | Only the same-cycle evidence bundle output |

These are actual `AssetIn` values, not dependency-only declarations followed
by reads of current pointers. The calculation and bundle outputs are their
canonical immutable manifest paths under the configured storage root.
Control-result copies can live elsewhere; Go does not treat those copies as
published input manifests. Advancing a current pointer does not alter a
previously loaded manifest.

Partition readiness alone does not prove compatible source versions. An
upstream release can change while older facts remain materialized. Go must
reject incompatible A/B/master ancestry before publishing readiness. This
chain does not relax that check or manufacture replacement inputs in Python.

The shared filesystem IO manager retains the latest output value per asset
partition. It is not a run-scoped cross-release snapshot. Immutable manifests,
Go ancestry checks, and the bundle define the accepted snapshot; a mismatched
combination fails rather than silently mixing releases.

## Validation and automation

Each asset has a blocking `go_verified` check with the same explicit partition
definition as its asset. Dagster 1.13.20 does not infer a partitioned check
from a partitioned asset. Partitioned check specifications are a preview API;
retain the pinned Dagster version and rerun the mapping and automation tests
before upgrades.

The adapter requires successful command exit, the checked-in JSON schema,
and the requested cycle. A failure yields a failed check and raises without
emitting an asset output. A success emits the output, then its passed check,
so Dagster associates the check with that materialization. Emitting success
before materialization leaves the check stale for later automation ticks.

All three assets use `eager()` plus `all_deps_blocking_checks_passed()`.
The latter is explicit: eager materialization alone does not enforce upstream
check status. Tests cover missing and failed checks, recovery, and isolation
between cycles. They also prove that a failed Go result cannot materialize
the asset or execute its downstream consumer. Manual commands still enforce
the same Go integrity boundary; checks are not a substitute for that boundary.

Logical calculation, bundle, and projection IDs become Dagster `DataVersion`
values. Runtime measurements and replay flags do not change those versions.
The graph can pass integrity checks while reporting `partial` master coverage;
Python must preserve that state, not upgrade it to complete coverage.

Metadata is allowlisted from validated results. A/B measures remain separate,
and exact minor-unit strings remain strings. Full source drilldown rows stay
in the immutable command-result artifact, not routine Dagster metadata.
No Python money arithmetic, identity resolution, or graph client is added.

The shared adapter resolves canonical `https://legal-tender.local/contracts/`
schema references only from the configured contracts directory. It rejects
foreign authorities, missing contracts, path escapes, and mismatched schema
IDs without HTTP retrieval. This reuses pinned result definitions without
copying money or lineage schemas into the orchestration package.

## Operation and limits

The registered jobs are `fec_committee_flow_reconciliation_job`,
`fec_committee_flow_evidence_bundle_job`, and
`arango_committee_flow_evidence_job`. The automation sensor is
`fec_committee_flow_evidence_automation`; it targets only these assets and
uses the existing `DAGSTER_SCHEDULES_ENABLED` default-status switch.
It adds no independent polling schedule. The coordinated source discovery
schedule remains Monday at 04:00 America/New_York.

All commands use `GoPipelineResource`. The calculation worker count defaults
to four; graph batches default to 5,000. These are execution settings, not
source-specific selection rules. Credentials remain in the configured
environment and never enter command arguments or metadata. Retry policy is
two retries with 60-second exponential delay. No new service or memory budget
is introduced.

The real gate uses already-published 2024 inputs and an isolated Dagster
instance. It does not establish live weekly daemon operation, other-cycle
A/B readiness, or cross-host concurrency guarantees. Reuse still hashes
source shards and verifies graph contents; it is not zero I/O. Cross-release
calculation reuse, historical-identity refresh, and economic-flow/terminal
attribution remain separate work. The [paginated API](./committee-flow-api.md)
now exists as a separate read-only Go process, not a Dagster asset or a
deployed public service.
