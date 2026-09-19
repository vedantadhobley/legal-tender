# Fact-start funding recovery checkpoint

Status: the metadata planner and scoped file verifier are implemented. The unused
[input-copying feature](./funding-recovery-retention.md) was removed on 2026-09-15;
the recovery executor and
checkpoint are **not executed or accepted**. Further implementation is deferred
after the 2026-09-15 scope correction. This is separate operational work, not the
next milestone or a prerequisite for the [graph-assumption review](./pre-attribution-review.md).
The read-only tools remain useful; do not restore the removed feature by default.
No rebuild, source acquisition, graph write, retention
change or baseline-pointer update follows from this document.
The [planner gate](../audit/funding-recovery-plan-2026-09-15.md) records the real
dependency recipe, tests, offline builds and byte-identical replay.

## Starting point and claim

Start with the exact retained normalized facts for the selected funding
generation, plus the source evidence required by their existing validators.
Recompute the downstream calculations and construct empty, isolated ArangoDB
graphs. Do not start from a graph export or reuse published calculation outputs.

The first real subject is the 2024 shared-conduit generation pinned in the
[inventory input](../audit/fixtures/funding-recovery-2026-09-15/inputs.json).
That file is a historical test input, not a default. The implementation must
derive publications, datasets and cycles from exact inputs and reject unsupported
dependencies. It must contain no candidate, committee, cycle or digest exceptions.

Success means: **the declared funding generation can be regenerated from these
verified facts and required source inputs using retained code and runtime**.
It does not prove that raw parsing can be rerun, that all historical execution
records survive, that another cycle works, or that the entire application can
be restored. An offline rebuild from raw archives remains a separate gate.

## Three dependency roles

Keep the [strict provenance inventory](./funding-recovery-inventory.md) intact.
Add a separate versioned recovery recipe with these roles; a file may have more
than one role. Determine roles from each producer's actual reads, not its name.

| Role | Required treatment |
|---|---|
| Execution input | Pin and fully verify every byte needed by the selected producers and validators. Missing or changed input blocks recovery. |
| Comparison evidence | Keep the historical outputs separately pinned and available to the comparator only. They cannot seed the rebuild. |
| Historical provenance | Preserve the complete ancestry inventory and its gaps, including records not read during fact-start execution. No missing record becomes verified or silently disappears. |

Execution inputs include all selected A/B/E fact files and required classic
reference facts, occurrence/release metadata, contracts and supporting artifacts.
Facts alone are not necessarily sufficient: the existing
[reference-content proof](./reference-content-equivalence.md) rehashes original
and target CN/CM/CCL archives, reads staged members and replays normalization.
Those bytes and their runtime dependencies belong in this recipe too.
Retain any additional inputs that the normal readers require; do not bypass
ancestry validation to make a smaller recipe.

The [missing original staging record](../audit/release-stage-evidence-review-2026-09-15.md)
stays missing under its original digest. Its independently regenerated counterpart
and descriptor review remain separate evidence. Classifying the lost record as
historical-only is valid only if the selected execution path does not need it.
A consumer that requires it blocks that path; there is no missing-stage override.
The original inventory therefore remains incomplete even if a later, explicitly
scoped fact-start recovery passes. Neither event recovers the lost original bytes.

## Recipe and preflight

The read-only Go planner reuses the typed inventory and existing
producer contracts; do not introduce a second financial calculation pipeline.
Its closed input pins the subject generation and exact locator specification.
Its deterministic output must bind:

- The starting layer, recipe version, full provenance-inventory digest and
  supported producer/dependency graph, including every unresolved requirement.
- Exact execution-input identities, paths, sizes, schemas and expected hashes;
  separate expected-output references and the unchanged historical gap record.
- Producer and comparison-policy versions, accepted executable/source/module
  archives, compiler and runtime image digests, and required schema/tool inputs.
- Ordered steps, dependency edges, input/output bindings, comparison fields and
  explicit completion/failure conditions. No lookup through current pointers.
- The isolated filesystem/database targets, allowed mounts, resource limits,
  retained-input protection and peak workspace estimate. Unknown cost is not zero.

Metadata planning must not hash the large bodies, run publishers, open a database,
download files or call an API. A plan is not verification or execution authority.
Unresolved producer support, input role, runtime or storage requirements block
execution instead of disappearing from the output. The first planner delivery
must enumerate those gaps for the retained generation, not merely emit a template.

## Gates, in order

1. **Protect inputs and runtime.** Retain the recipe, exact build inputs and
   required images; an image digest alone is not offline availability.
   Protect the required files from automated cleanup for the checkpoint's life.
   Test that cleanup refuses referenced files and fails closed on an unreadable
   retention inventory. A checksum list alone does not enforce retention.
2. **Prove isolation at fixture scale.** Rebuild through the real Go publishers
   in a new workspace and disposable, initially empty ArangoDB. Only the declared
   execution inputs are visible to producers, read-only at their required paths.
   Existing calculation artifacts, graph volumes, current pointers and comparison
   outputs are unavailable to them. No FEC/API/registry network access is allowed;
   the private database connection is the only required application network.
3. **Admit the real run.** Measure live free space and declare finite memory,
   heap, CPU, temporary/output and database-growth budgets before any large scan
   or write. Include simultaneous retained outputs, comparison access and graph
   indexes in peak usage. Do not assume copy/deduplication savings or borrow the
   [source-staging budget](./fec-streaming-storage.md) for downstream work.
   Resolve mount preparation and required input-copy costs in the plan. No active
   source or graph may become writable. Scope the full run separately before launch.
   After admission, hash every execution-input body against its pinned descriptor
   and apply the normal decoders/integrity checks before using it. Presence, size
   and old success flags are not sufficient. Verify all comparison evidence too;
   an untrusted expected result cannot establish equivalence.
4. **Recompute the complete selected generation.** Run the existing participant,
   reference, topology, conduit and shared-group publishers; A/B selections and
   reconciliation; E effective selection, candidate resolution and grouping;
   reference proofs, readiness bundles, graph imports and generation verification.
   The typed dependency graph owns the exact order and any additional prerequisites.
   Preserve every excluded, ambiguous, unresolved and unprojected population.
5. **Compare complete results.** Apply the equality rules below, full graph
   readback and source drilldown. Reuse the existing path, neighborhood and window
   gates for the rebuilt generation. Sample witnesses supplement full population
   checks; matching totals or a few successful queries cannot pass recovery.
6. **Seal the scoped result.** Retain all input/runtime pins, output manifests,
   comparison results, timings/resource measurements and explicit exit markers.
   Fresh-process read-only replay must agree. Cancellation or an incomplete step
   cannot publish success. An idempotent replay of a completed database is useful
   evidence, but cannot substitute for the empty-target rebuild in gate 4.

The result must distinguish execution-input verification, offline runtime
availability, retention protection, fresh reconstruction, logical comparison,
historical-provenance completeness and raw-to-fact recovery. Only the first five
can qualify this fact-start checkpoint; the last two stay separately reported.
It must not change the existing inventory's `recovery_ready` meaning or infer
user acceptance of interpretations. No automatic production promotion follows.

## Equality rules

Retained inputs must match their exact original bytes. Historical publication
IDs, build hashes and manifests are never rewritten to match a new run.

New producer builds can change derived IDs and their downstream references.
The comparator must use explicit, versioned adapters for each supported result
schema. Each adapter must compare every field or name and justify its operational
treatment. Reject unknown schemas/fields and unaccounted differences; do not strip
arbitrary `id`, `hash`, path or timestamp fields recursively.

Compare source-qualified occurrence/fact membership, multiplicity, raw/typed values,
nulls, money, dates, endpoints, relationship kinds/directions, decision states,
policy versions and exclusion reasons exactly. Preserve separate ledgers, signed
amounts, support/opposition, unknown identities and undated authorization context.
An old/new derived-ID mapping must be one-to-one and backed by those compared
inputs and values. Operational build/run/location differences remain recorded
separately. Changed source identity or policy is not an operational difference.

For the same exact build and inputs, retain deterministic artifact/identity checks
where the publisher promises them. For fresh reconstruction under a new build,
require complete logical equivalence and internally valid new ancestry. Never
claim byte-identical historical reproduction from logical equivalence alone.

## First implementation and failure tests

The typed metadata-only recipe planner and scoped execution/comparison file
hashing are implemented. Keep runtime protection, the recovery executor and real graph reconstruction as explicit
subsequent work in the [queue](../todo.md). No new Python domain logic is needed;
Dagster integration follows a verified Go boundary, not the other way around.

Planner tests reject unknown producers, ambiguous roles, missing execution pins,
dependency cycles, path escapes and current-pointer lookups. Metadata fixtures
cover two synthetic cycles with the same code and preserve the original inventory's
missing historical record without treating it as an available execution input.
These fixtures do not run the future isolated reconstruction.

Before real execution, extend fixtures to cover a same-size corrupt fact, a
missing reference-proof input, wrong/offline-missing runtime, stale calculation
injection, nonempty graph target, disk exhaustion and cancellation. Comparator
tests must fail on changed endpoints or decisions even when counts and sums match,
and accept only enumerated operational differences with valid one-to-one mappings.
Prove that required undeclared reads fail in the restricted workspace. These tests
are requirements for implementation, not checks claimed to have passed today.

## Implemented metadata planner

`plan-funding-recovery` accepts the same exact input specification as the
inventory command. It retains the walker's verified metadata bytes instead of
reopening manifests, and embeds the unchanged full provenance inventory with
its canonical JSON digest. Data bodies are only checked for presence/type/size.
There is no blob-hashing, database, API or missing-stage-override option.

The closed producer registry binds normal Go entry points and direct input/output
roles. It emits dependency-first steps, source cycles from metadata, pinned expected
outputs and policy metadata. A/B and E graph constructors are explicit steps even
though their old graph identities are embedded in the generation. Historical
calculation files are comparison-only; required facts, release/occurrence metadata
and selected CN/CM/CCL proof archives/members are execution inputs. The remaining
source/control history stays visible but is not automatically mounted for execution.

`dependency_plan_complete` means the selected supported dependency layout and
required backing are accounted for at metadata-inspection level. It does not mean
full producer semantics, data bytes or output comparisons have passed. Unknown
versions, dependency roles, ambiguous membership and missing required files block
that state. The planner also rejects unsupported publisher/method versions where
they are separate from the manifest schema; normal readers still own full policy
validation. The expected metadata digest pins those policies without applying them.

`execution_ready` remains false. Exact replay-build archives, server image,
isolated mounts/targets, resource admission, protected retention, full body checks,
executor and per-schema comparators remain named blockers, not guessed defaults.
The output is a dependency recipe, not shell commands that can be launched safely.
Its logical byte counts deduplicate paths, not physical inodes or workspace peaks.

Build through the [accepted Go build boundary](../go-build.md), then run:

```bash
bash scripts/run-funding-recovery-inventory.sh \
  /absolute/storage/root \
  /absolute/accepted-build/first/legal-tender \
  /absolute/inputs.json \
  /absolute/new-audit-directory plan
```

This reuses the existing capped, network-disabled inspection container. The
runner records its selected command, checks input/executable digests, runs two
fresh processes and compares complete JSON and exit statuses. Blocked planning
returns nonzero even when replay comparison succeeds. The old four-argument
inventory invocation remains unchanged. `--expected-plan-id` is available on
the direct Go command for exact replay checking.

## Implemented scoped file verification

`verify-funding-recovery-files` reconstructs the typed plan from the exact
generation/locator specification. It does not accept a saved plan as authority.
It hashes every selected execution and comparison file, including metadata, against
the original byte pins. It does not scan historical-only bodies or change the
embedded inventory's verification states. Metadata inspection still enumerates
the full ancestry before selecting files.

The command requires explicit `--max-bytes` and `--workers` arguments. It refuses
body hashing if the dependency plan is incomplete, a selected size is unknown,
or the path-deduplicated total exceeds the byte ceiling. One to eight workers each
use a 128 KiB buffer. Outputs are sorted independently of worker completion order.
The ceiling covers selected logical bytes, not metadata-planning overhead, disk
space or a future rebuild's workspace. A growing file can read at most one byte
beyond its pinned size before failure.

The read-only verifier rejects same-size corruption, missing/truncated files,
symlink paths, non-regular files and observed inode/size/time changes during a read.
Cancellation cannot report success. These are point-in-time checks, not a filesystem
snapshot or protection against a writer changing bytes after verification. Normal
source decoders and domain validators still have to run during reconstruction.

```bash
bash scripts/run-funding-recovery-inventory.sh \
  /absolute/storage/root \
  /absolute/accepted-build/first/legal-tender \
  /absolute/inputs.json \
  /absolute/new-audit-directory verify MAX_BYTES WORKERS
```

The existing network-disabled, 512 MiB/two-CPU container keeps all source mounts
read-only. The runner retains admission arguments and two fresh-process results.
`--expected-verification-id` is also available on the direct Go command. Exit zero
means only that all selected file bytes passed; `recovery_ready` stays false.
Unknown or missing historical-only records remain recorded in the pinned plan.

Permanent retention protection is **not implemented** by this verifier. The unused
[input-copying implementation](./funding-recovery-retention.md) was removed after
fixture testing; no real generation was captured by it. Inspection
of current Go mutation paths found temporary/workspace cleanup, not an automated
published-data garbage collector. Do not claim a checksum list or read-only scan
enforces permanent retention. Establish the checkpoint's protected storage and
cleanup boundary for the real runtime, with fail-closed tests, before reconstruction acceptance.

The [retained-file gate](../audit/funding-recovery-files-2026-09-15.md) records
successful full selected-byte verification, exact replay and live budget rejection.
