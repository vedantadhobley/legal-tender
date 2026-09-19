# Integrated candidate evidence — 2026-09-11

Status: implemented and verified for two retained 2024 candidates. The
[candidate evidence command](../design/candidate-evidence-view.md) produces a
readable report and a reproducible machine-readable result. It joins existing
evidence; it does not choose terminal sources or allocate their dollars.

## What is now usable

Each report presents candidate-boundary committee receipts, disjoint receipt
components for candidate-linked committees, and optional reported summary amounts
and coverage dates. Its JSON joins receipt populations to all reached committees,
retains exact upstream observation membership, and expands each non-root's
shortest-hop source witness. The prior trace is unchanged in every field.

| Measurement | S6OH00163 | S6PA00217 |
|---|---:|---:|
| Reached committees | 7,096 | 7,086 |
| Shortest-hop source witnesses | 7,095 | 7,085 |
| Cyclic groups | 18 | 18 |
| Reached without a same-cycle master | 623 | 620 |
| Reached without receipt inventory rows | 1,180 | 1,176 |
| JSON bytes, including newline | 36,045,178 | 35,959,500 |
| First command seconds | 42 | 43 |
| Replay seconds | 39 | 40 |

Timing uses whole elapsed seconds and includes backing verification, summary
grouping and output. These are local measurements, not a refresh guarantee.
No raw source download or Schedule A transaction-value rescan ran. The commands
rechecked published backing bytes and reused the complete saved inventory.
Go containers used 4 GiB, four CPUs and `GOMEMLIMIT=2GiB`; independent tests used
2 GiB and two CPUs. Both had networking disabled and source storage read-only.

The candidate-linked committee summaries come from v4; the detailed receipts
and graph evidence retain older ancestry. Both reports explicitly show
`different_source_release`, unverified receipt/report-period coverage, and
unverified summary account/report scope. Reported summary values remain context,
not substitutes for detail or qualified complete candidate-funding totals.

## Reproduction

Executable SHA-256:
`a1422f57e828bef1ad2628dfb5f4a888be5d5ab1b58f0aaf229d4438c32f3fb0`.

Result identities:

- S6OH00163: `a186c4d31305bae7961805cc8d3f5ed5ed8a8a718a01df1d41bd119c534b9497`.
- S6PA00217: `1dc49ec43d23f0f4731f80f0294b6b2a22d9b7ccd2ebcccd8c7f433c89c366e7`.

The result identity hashes the full compact Go JSON with `result_id` empty.
It includes the executable hash, candidate/cycle, embedded trace, witnesses,
receipt inventory identity, and summary-source/interpretation identities.
Neither runtime timestamps nor output paths affect the result.

Exact inputs:

- Inventory: `e31d7e8248ad6594c6ce23f86a0fe0cd2ff0ac13a0e78d079c97562519416985`.
- Schedule A facts: `8f69a2ef40316f62d8844ecf732342a3b4535136b434f60677d0608cb04465df`.
- Observation bundle: `113c25c47c3d008dd79a470c9cd8e3482bbdcf82c1fc6a53f717561f571e9c3d`.
- Linkage facts: `562246920ef6d5ce508db2c98bf8bc972e2e97041254fde1ce39703ab795dedf`.
- Summary facts: `603d086eb26baa5a9a99d7a717ec5b7469098c173d2d3eabaa119c00d9b7f637`.

Retained results are under the configured storage root at
`dumps/audits/fec/candidate-evidence/2026-09-11/attempt-01/`:
the two candidate-ID `.md` and `.json` files, executable, Go-source archive,
drivers, checksums, verification logs and explicit completion markers.
Replay outputs are identical; their hashes and logs are retained without
duplicating the large result bodies. Existing source artifacts remain in their
original immutable locations, not copied into this audit.

## Verification and limits

- Full Go tests, vet and targeted race checks pass.
- Independent checks validate schemas, result/build identities, every joined
  population and component, all overview counts, summary operands and membership,
  prior trace equivalence, and every witness against the shared zstd source artifact.
- Both shipped CLI runs reproduce JSON and Markdown byte for byte.
- Unit fixtures preserve negative/unknown/memo/overlap populations, empty rows,
  unresolved authorization, optional summary scope, and local comparison blockers.
  Foreign receipt identities, cancellation, invalid executable IDs and report
  overwrite attempts are rejected.
- Initial independent-test harness failures used an incorrect retained-file path,
  a shortened committee-decision label, and the wrong decompressor. A later Ruff
  attempt needed its cache disabled for the read-only repository. Their logs are
  retained separately; none required a source or runtime counting-rule change.

Active-source pointer SHA-256 remained
`b030668efca44fcf611b204665d95c00fa7eda9ce3ce31b5711ebbba6805e81c`.
No graph, source publication, Dagster schedule, service or current pointer changed.

The complete latest-four-cycle view, person/corporation resolution, complete
financial membership, terminal policies and pooled allocation remain open.
The next product step is to inspect this integrated result and prioritize the
missing relationships or coverage that prevent useful investigation. Terminal
classification remains a separate choice, not a missing field to invent.
