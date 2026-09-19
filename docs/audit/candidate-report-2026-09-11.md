# Named candidate evidence report — 2026-09-11

The [candidate evidence view](../design/candidate-evidence-view.md) now has an
opt-in v2 presentation with pinned source names, complete connection examples,
separate memo tables and readable coverage explanations. It preserves the full
v1 evidence result. This is not a new money calculation or identity resolver.

## Inputs and reproduction

The two retained 2024 cases from the
[integrated evidence gate](./candidate-evidence-2026-09-11.md) were rebuilt with
the same inventory, observation bundle, linkage facts and summary context.
No candidate-specific runtime rule was added.

- Inventory:
  `e31d7e8248ad6594c6ce23f86a0fe0cd2ff0ac13a0e78d079c97562519416985`.
- Observation bundle:
  `113c25c47c3d008dd79a470c9cd8e3482bbdcf82c1fc6a53f717561f571e9c3d`.
- Committee-name fact set:
  `a9f3235c81ce1487c9322a899a586ec00992adf26b81bf4c76444c0c5f75d254`.
- Committee-name manifest SHA-256:
  `17ba070b8ac450d9d383f4d815250a8c5299f7ac5e936b9a0f7806d10981f088`.
- Candidate-name fact set:
  `3b2fe38aa7b32edf02c08bf47672f00b6f4710563df7ff31344ccac73dc5d1f6`.
- Candidate-name manifest SHA-256:
  `63649ad919978427d415c63cbfc9cbac42ca9dc7dba9f633ce85a05ee005d8a3`.
- Executable SHA-256:
  `1c9a9f5842b2b42c1951ac9d40b5f7f8dcc7a1b7dc58319847f7141b5b7b2138`.

The candidate reference is display context only. Committee names must match the
trace's exact master facts. Every displayed name retains raw text and exact
fact/occurrence identities; no fallback API or fuzzy name match is used.

## Results

| Selected input | Report ID | First run |
|---|---|---:|
| S6OH00163 | `00974d9462272a1980ac6b3132e2c201cd5e1c7b112d98ed299ee99d44c75e64` | 30 s |
| S6PA00217 | `cd2cab876d2c98628fc440b1a30c4ef12927370ef4322b25a17e708db48fe68e` | 31 s |

Each report includes three complete existing witness chains selected by hop
distance and committee ID, not by amount or a preferred conclusion. Hop dates,
signed amounts and source locators remain exact. Date warnings distinguish
connectivity from chronological availability. Allocated path amounts are null.

All nested v1 evidence fields match the prior publication except the executable
digest and the result ID that binds it. The original trace, membership, receipt
populations, summaries, scope blockers and amounts are unchanged. Names do not
resolve the existing missing-master states. Terminal and allocation policies
remain unselected.

## Verification

- Full `go test ./...` and `go vet ./...` pass. Targeted presentation/CLI race
  tests pass. Fixtures cover deterministic paths, wrong/missing witnesses,
  cancellation, date reversal/gaps/same-day records, nonpositive amounts,
  raw-name preservation and disagreements, blank/conflicting names, Markdown
  injection, exact large money formatting, memo separation and output failures.
- Independent Python checks validate the v2 schema, both content hashes, all
  included name assertions against complete pinned reference artifacts, and
  every example's selection, source membership, continuity and date/sign flags.
  They compare every prior core field apart from build-dependent identities.
- Both final JSON and Markdown reports reproduce byte-for-byte. Replay hashes
  and timings are retained; duplicate replay bodies are not retained durably.
- The focused Python suite passes: three passed, one skipped. The skipped test
  is the separate v1 corpus harness; v1 parity is checked directly by the new
  real-report audit. Ruff and changed-document link checks pass.

An initial presentation read-through exposed untranslated reconciled flow-role
labels. The shared label map was completed; no source or calculation rule changed.
Its failure log is retained. A later Ruff check requested `itertools.pairwise`
in the independent audit; that test-only correction and initial lint log are also
retained. Final checks pass with the executable listed above.

## Retained evidence and boundaries

Final evidence is under
`/storage/dumps/audits/fec/candidate-report/2026-09-11/attempt-01/`.
It includes both JSON/Markdown reports, the executable, exact build/test source
archive, drivers, success markers, failure logs and checksums. No bulk source
artifact was copied. The previous candidate-evidence publication is untouched.

The source-current pointer remained at SHA-256
`b030668efca44fcf611b204665d95c00fa7eda9ce3ce31b5711ebbba6805e81c`.
All source mounts were read-only. No graph write, source acquisition, Dagster
activation, identity-resolution change or financial-use promotion occurred.

Next: use this named report to select a concrete connection for source-row
drilldown. Broader relationship coverage, terminal definition, allocation and
production serving remain separate work.
