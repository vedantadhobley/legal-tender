# Go rewrite checkpoint — 2026-09-19

Status: point-in-time implementation and priority checkpoint before terminal
attribution. This audit does not accept a terminal definition, allocation method,
person identity, corporate affiliation, or production cutover.

## Why this checkpoint exists

The rewrite had accumulated a substantial tested implementation while almost all
new source, contracts, fixtures, orchestration, and design records remained outside
Git tracking. Work had also moved from the core funding product into increasingly
narrow identity and prose experiments. This checkpoint preserves the implementation
and restores the product sequence without discarding those experiments.

Before the checkpoint, Git reported 1,989 untracked files totaling 30.79 MiB. An
unanchored `storage/` ignore rule also hid six required Go files under
`internal/storage/`. The rule now applies only to the repository-root runtime
directory. Generated Dagster state, Python bytecode, caches, local `.env`, and the
external data root remain excluded.

The repository is public. Newly retained fixtures are source and test evidence, not
runtime data dumps. They carry provenance or review manifests and do not contain the
operator's Gmail address, private tailnet identifier, API token, or local absolute
home path. Public-record fixtures can still contain source-reported personal data;
that is a repository-policy consideration, not permission to weaken exact replay.

## What is implemented

- The Go data plane preserves and validates detailed FEC Schedule A, B, and E source
  evidence under versioned contracts. Schedule A and B detailed product work is
  complete for 2024; Schedule E has accepted four-cycle publication.
- Versioned calculations cover candidate receipts, independent expenditures,
  receiver-reported committee flows, report/reference relationships, and explicit
  unresolved or excluded states.
- ArangoDB projections and typed readers support source drilldown, bounded
  neighborhoods, committee paths, candidate authorization context, support and
  opposition, cycle-independent time windows, and exact readback/replay gates.
- Dagster invokes Go through a thin control-plane boundary. Python does not own
  source parsing, financial calculations, graph construction, or domain policy.
- Candidate evidence and read-only serving components prove that the underlying
  publications can be composed. They are development surfaces, not a released UI
  or production API.

The detailed implementation inventory remains in the
[Go rewrite guide](../go-rewrite.md). The current graph boundary and remaining
coverage work are defined in the
[connected funding-graph plan](../design/connected-funding-graph.md).

## What is not implemented or accepted

- No terminal-source classifier or dollar-allocation policy is accepted. The current
  terminal assessment measures topology and evidence gaps; reachability does not
  identify an economic origin.
- Person and organization work produces evidence, candidates, diagnostics, and
  retained model comparisons. It does not approve donor identities, company
  affiliations, corporate-family edges, or financial attribution.
- Detailed Schedule A/B product coverage is not rolled out across the full target
  four-cycle window.
- The existing Dagster wiring has not passed an unattended weekly four-cycle
  changed/unchanged publication exercise. No active daemon or production cutover is
  established by this checkpoint.
- No product UI currently exposes the candidate dossier and evidence drilldown
  required by the [product contract](../design/product-contract.md).

## Accepted next sequence

1. Preserve and verify this repository checkpoint.
2. Complete the four user-facing choices in the
   [pre-attribution interpretation review](../design/pre-attribution-review.md).
3. Build one product-shaped 2024 candidate slice using only accepted evidence and
   calculations. Expose receipts, support, opposition, committee paths, source
   records, coverage, and unresolved states separately.
4. Use that slice to compare explicit terminal definitions and allocation methods.
   Require path-level evidence, versioned policy, amount conservation, and an
   unresolved remainder.
5. Then roll the accepted method across the remaining cycles, prove unattended
   weekly operation, and add the investigative UI.

Autonomous page discovery and additional local-model experiments are paused. They
resume only when an accepted product requirement identifies a specific missing
identity relationship that structured evidence cannot supply.

## Verification

The pinned-container `make check` gate passes: formatting, module tidiness, static
analysis, and the complete Go test suite. The complete Python/Dagster collection
also passes with 192 passed and 194 retained-data-dependent skips. That run exposed
and closed one pre-existing contract-boundary defect: the report-scope assessment
now lives under calculation policies instead of masquerading as source-acquisition
metadata. The focused source-contract/report-scope replay passes 14 tests with five
retained-data-dependent skips.
