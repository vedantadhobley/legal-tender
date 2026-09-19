# Funding recovery inventory — 2026-09-15 UTC

Status: implementation, full Go tests, clean offline builds and byte-identical
real inventory replay pass. Both inventory executions correctly exit nonzero:
the file dependency inventory is incomplete because one exact historical staging manifest is absent.
This is not a completed raw-to-graph recovery checkpoint.

Follow-up: the [historical stage review](./release-stage-evidence-review-2026-09-15.md)
found the already documented manual-output overwrite and now verifies the retained
reconstruction against every published source/output descriptor in code. The
original byte pin remains missing; this inventory's result is not superseded.

## Subject and method

The [input specification](./fixtures/funding-recovery-2026-09-15/inputs.json)
pins the existing shared-conduit generation and its original base generation.
Its SHA-256 is
`7735ec48a129bc9336c3eaf1962666fedab48f6c88bc6220cedab22a184f5d5e`.
These historical IDs are evidence selectors, not matching rules or runtime defaults.

The [typed inspector](../design/funding-recovery-inventory.md) traverses the
manifest-declared dependency chain. The initial real run hashed 43 manifests,
found 1,339 data artifacts with matching declared sizes, and reported one missing
manifest. It did not hash those large data files or access ArangoDB.

All reached participant, reference, conduit, fact-shard, occurrence, calculation,
raw-source and staged-source artifacts passed presence/size checks. This statement
does not establish full closure beyond the unopened missing manifest, prove raw
byte integrity or establish live graph readiness.

## Exact open gap

The historical v3 source release names staging bytes with SHA-256:

`84156d7c692c564e3274e59ee7f47e5125733df60d26f04746ff47a869c19a67`.

The publisher-owned content-addressed path is absent:

```text
control/fec/release/stages/84156d7c692c564e3274e59ee7f47e5125733df60d26f04746ff47a869c19a67.json
```

A read-only check of the run-named copy at
`raw/fec/stages/fec-136903645ee7c7050d463a4f779a051a454ebb87308fd854faabc5ca47a20cdf/live-v3-20260903.json`
found digest `3aca399d465cfdb0f7afb70733e05f11228693d43c3de009164108e0e376cd6c`.
That file is not a valid replacement for the pinned bytes. This observation does
not by itself identify why the records differ or show that source transactions are missing.
The adjacent retained Schedule B columnar audit contains publication results,
not another named staging input.

Locate the exact record in retained evidence, or document a separately verified
reconstruction path. Do not rewrite the release hash, substitute the run-named
file, claim current data corruption, or fetch new source archives as a workaround.

## Verification and retained execution

Targeted Go tests, race checks and static analysis pass. Fixtures cover typed
source artifacts and their different physical roots; exact shared-generation
binding; unknown versions/fields; missing or changed backing; path/symlink/FIFO
guards; conflicting locators; shared dependency deduplication; cycle detection;
deterministic replay; explicit full-hash mode; cancellation; and nonzero CLI
results that retain the partial inventory.

The [release build gate](../go-build.md) passed two clean offline builds with
identical executable bytes, the complete Go suite and negative build checks.

| Identity | SHA-256 |
|---|---|
| Inspector executable | `d1b988a092460c9d22785bda3f58830c097824296b6f4f582aa5132f744e1ce1` |
| Source archive | `6aea5db2c1ecc5e2107e5fae7117e89abd6a301044f55d8e05e645c1a231141f` |
| Module archive | `71fb2573f01965e16a0939901120736c24433681188af4bb1f436ed0ea535d77` |
| Inventory ID | `de25e181221feb4ecc23dbcdfc194bd0bac9362f8770c47ce5a2f085ad384c0b` |
| Result and replay bytes | `2f6bfa5d7aa55bff29b6f58a729dba46a3b3847746b9789c31ca3cc78fed6a9a` |

The final result has 1,383 nodes, 1,626 dependency edges, no dependency cycles
and 70 unverified build/runtime/contract requirements. The 1,339 data files
account for 227,095,725,949 declared/observed bytes by unique path. This is not
physical disk usage: hard links or copies can have different storage costs.
Manifest inspection read no large data bodies.

The two invocations took 0.091 and 0.086 seconds; the capped container recorded
a 19,918,848-byte memory peak. Both `result.exit` and `replay.exit` are `1`,
`replay-check.exit` is `0`, and complete result bytes match. The runner's
`exit-status.txt` remains `1`; an identical incomplete result is not success
for the missing dependency or recovery readiness.

Retained locations beneath storage:

```text
builds/go/2026-09-15/d1b988a092460c9d22785bda3f58830c097824296b6f4f582aa5132f744e1ce1/build/
dumps/audits/fec/funding-recovery-inventory/2026-09-15/attempt-01/
```

The build contains its complete source/module archives, executable pair, tests,
checksums and zero success marker. The inventory contains its exact input,
result/replay, logs, timings, Compose/runner snapshots, executable/runtime identities,
checksums and explicit incomplete/replay markers. The source archive also includes
the inventory runner and Compose recipe. Storage was mounted read-only during both
inspections. No source, graph, calculation, current pointer or existing evidence
was changed. Retaining audit output does not enforce retention of its dependencies.
