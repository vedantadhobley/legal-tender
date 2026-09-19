# Reported identity assertion gate — 2026-09-13

Status: accepted. Fixture field-equivalence, preservation, failure and worker
replay tests pass. Full regression/race/static checks, the complete 2024 scan,
fresh worker-varied replay and sampled independent full-width reader checks
pass. The
[view contract](../design/reported-identity-assertions.md) owns behavior.

## Retained scope

Current attempt: `/storage/dumps/audits/fec/reported-identity-assertions/2026-09-13/attempt-02/`.
The attempt retains the exact executable, Go source snapshot, runner scripts,
input locators, logs, results and explicit exit markers. Source storage is
mounted read-only; only this new audit directory is writable. Both jobs have
networking disabled and need no credentials or Arango access.

Executable SHA256:
`41532bff7f4ffed123e50d7fb148b05dfc1027ca62366b4c0cac08fe87a7d525`.
Go source snapshot SHA256:
`7ffc645799c584fedd10e9970c2117bdf835f20b5bced6f169fb7a830dbef9f9`.

Schedule A fact set:
`8f69a2ef40316f62d8844ecf732342a3b4535136b434f60677d0608cb04465df`.
Committee-master fact set:
`e6699bdf7174bf3865060b14e1025fa45852bac54fd50b5a4b3743d1dd97ac1d`.
The result records both exact manifest checksums and their independent release
ancestry. Their shared cycle is not a claim of a coordinated release.

Both pinned inputs happen to retain release
`fec-a6e6f777c3c506b100a7709815bc89f2ae84749faf4343a4589f7ec1bac066a2`,
manifest SHA256
`b921fda742759747b7e581c8897e8022ea5152eb33b86538ec80dbb9f13b5cad`.
The view does not require matching releases or use that coincidence to resolve
identities.

## Preserved failed attempt

Attempt 01 verified every receipt shard and all committee records, then failed
the view's final metadata validation. The new validator incorrectly expected a
bare SHA256 for the source release ID; the established source contract uses
`fec-` followed by SHA256. This was an implementation bug, not an FEC data issue.
No completed result was emitted. Its failed log, explicit nonzero marker,
executable and source snapshot remain retained.

The fix exposes and reuses the existing release package's identity validator;
it adds no per-release exception. Regression tests now accept the namespaced
identity and reject a bare hash. Attempt 02 reruns the complete source gate
and all Go tests against the corrected build.

## Required gates

- Complete input backing and full original source schema checks.
- Dense receipt ordinals, cycle and normalization state; every projected value
  enters an ordered digest, and every field's null/empty/nonempty census conserves.
- Every pinned committee fact, exact source envelope, raw/typed agreement,
  blank organization fields and full compressed/uncompressed artifact checks.
- Fresh-process worker-varied replay with expected view ID and identical output.
- Separate full-width physical source reader comparisons for first/middle/last
  whole receipt shards. This is sampled reader-path equivalence, not a second
  independent full-corpus implementation.
- Full regression, targeted race/static checks, zero exit markers and checksums.

The corpus command used eight readers; fresh replay used four. Both kept
an eight-CPU quota, 4 GiB cap and `GOMEMLIMIT=2GiB`. The test runner has a
four-CPU quota with the same memory controls. These are one-shot budgets, not
new services or changes to standing Arango/Dagster budgets.

No source data, graph, pointer or schedule is changed. No terminal policy,
identity resolution, employment/ownership inference or dollar attribution is
performed. The user-requested [interpretation review](../design/pre-attribution-review.md)
is still open; this technical gate cannot substitute for that review or a backup.

## Complete 2024 results

| Population | Result |
|---|---:|
| Receipt assertions, across all 265 shards | 264,085,606 |
| Committee organization assertion records | 20,938 |
| New exclusions in either assertion population | 0 |
| Receipt rows with nonempty reported employer text | 242,480,550 |
| Receipt rows with null employer text | 21,605,056 |
| Receipt rows with nonempty occupation text | 259,103,383 |
| Receipt rows with null occupation text | 4,982,223 |
| Committee facts with nonempty connected-organization text | 10,277 |
| Committee facts with empty connected-organization text | 10,661 |

These are field-presence counts, not distinct employers, verified employment,
valid corporate links or effective financial records. Values such as retired,
self-employed and placeholder text remain unclassified. This corpus has no
empty-string receipt values in the projected fields; fixtures separately prove
that empty and null would remain distinct. Its pinned CM fact set has no
upstream exclusions, but the view still records and supports the classic
fact/occurrence distinction rather than assuming all future sets are lossless.

View ID:
`a2a532c46ce8c702ee3e3ff63c72e43142727baaaa3e96b1aede7ac009a4ded7`.
Result and replay SHA256:
`7fac19a3eb5a5783b2dee45a6fac890fb7be8625465b080bcf657fa548cc6c4c`.
Each result is 696,401 bytes, including source-shard references and per-field
counts. The accepted attempt uses about 55 MiB including both executables,
source snapshot, tests and results; no new text corpus was created.

The eight-reader invocation took 71.603 seconds; the four-reader fresh replay
took 105.862 seconds. These runs shared the host with other verification jobs
and are not a controlled scaling experiment. They establish identical output,
not an optimal reader count. The command default remains four.
Recorded cgroup peaks were 412,172,288 and 326,193,152 bytes respectively;
these include charged cache and are not process-RSS measurements.

The separate full-width reader matched every projected value for source shards
0, 132 and 264: 2,085,606 records in total. That check took 3.67 seconds and
passed its explicit exit marker. Both complete CLI invocations independently
verified all source backing and projected-value digests.

The [independent JSON checker](../../scripts/check_identity_assertion_view.jq)
passes both results, recomputing dense scope, all field-state populations,
merged counts and non-promotion flags. Its retained copy and final checksum
manifest support reproduction. The accepted Go source snapshot matches the
working tree's included files; unrelated existing worktree edits remain intact.

All result, replay, test, source-equivalence and final-gate exit markers are
zero. Setup and final checksum verification pass. All four accepted-attempt
containers exited zero without an out-of-memory kill; no verification job
remains running. The failed attempt remains retained separately.
