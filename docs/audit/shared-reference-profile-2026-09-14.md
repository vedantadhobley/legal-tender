# Shared receipt-reference profile — 2026-09-14

Status: complete retained 2024 analysis. The entire unchanged association stream
matches the accepted publication byte-for-byte. No graph link, counting rule,
source artifact or current pointer changed.

The [Go profile contract](../design/shared-reference-profile.md) owns the method.
The earlier [interpretation review](./pre-attribution-review-2026-09-14.md)
identified shared-degree rejection as an unexplained coverage boundary. This run
characterizes that population; it does not accept a broader association rule.

## Findings

All 264,085,606 participant occurrences were verified. The unchanged calculation
again classified 33,262,189 non-memo earmark occurrences, including 14,495,753
rejections for a related occurrence with multiple exact peers. Those rejections
refer to 38,273 distinct related occurrences.

The separate role inspection found:

| Role/ID outcome | Shared-rejected original occurrences |
|---|---:|
| Existing roles and reported IDs compatible | 13,343,713 |
| Related role outside the accepted pattern | 1,142,922 |
| Conflicting reported committee evidence | 9,075 |
| Original contributor role outside the accepted pattern | 43 |

Compatibility covers 92.05% of the shared-rejected population. It is not a
confidence score, resolved identity, distinct payment count or eligibility rule.
The group dimensions give a narrower result than that headline:

| Exact-peer coverage | Role pattern among profiled originals | Groups | Original occurrences |
|---|---|---:|---:|
| Complete | All compatible | 22,334 | 2,637,285 |
| Complete | Mixed | 35 | 197,710 |
| Complete | None compatible | 246 | 378,065 |
| Other peers not characterized | All compatible | 10,956 | 10,425,693 |
| Other peers not characterized | Mixed | 34 | 83,995 |
| Other peers not characterized | None compatible | 4,668 | 773,005 |

Complete means every exact peer is a safe non-memo earmark original with this
related occurrence as its sole peer. It does not mean the reporting record
captures all real money. Partial groups retain the uncharacterized count rather
than assigning roles from a selected subset. The largest encountered shared
endpoint has 123,825 exact peers; the join did not materialize that group.

Among complete, all-compatible groups, 1,681 groups / 8,337 original occurrences
have equal original-sum and related amounts. The remaining 20,653 groups /
2,628,948 originals have unequal amounts. All amounts in this profiled population
are known; negative and zero source values remain in the sums.

This result does not justify demanding equality or filling the difference.
FEC guidance describes conduit memo totals that can include unitemized receipts;
it does not prove the explanation for any particular difference here. See the
[official reporting examples](https://www.fec.gov/updates/earmarked-contributions/).

## Automatic witnesses and immediate next work

Each of the eleven observed structural classes retains the lowest related
ordinal and its first original for each role outcome. Examples were chosen by
the same Go code used for the complete scan, not by committee name or known ID.

- Related ordinal 1,352 has 140 compatible exact peers, with unequal sums.
- Related ordinal 73,136 has two compatible exact peers, with equal sums.
- Related ordinal 2,195 has 92 profiled originals but 93 exact peers. Its
  remaining peer is not characterized by this analysis.
- Related ordinal 3,698,152 has two originals whose amounts sum to the related
  amount, but one original's reported committee ID conflicts. Equality must not
  erase the conflict.

These are verified compact role/topology witnesses. Complete original facts,
full memo text, dates, report IDs and reference direction remain in the pinned
source/reference publications. Full original-source witness drilldown was not
performed in this run.

Next: inspect those automatically selected source examples and specify a
versioned group association rule for the supported shapes. For partial groups,
use retained reference incidences to characterize the missing peers before
claiming complete group coverage. Do not blanket-promote the 13.34 million
compatible rows. Keep reported associations distinct from payments and terminal
allocation. No new full-cycle download or database migration is needed for this
next investigation.

## Reproduction and verification

Retained job root, relative to the project storage root:

```text
dumps/audits/fec/shared-reference-profile/2026-09-14/attempt-01
```

The [runner](../../scripts/run-shared-reference-profile.sh) retained the executable,
two input manifests, stdout result, progress log, calculation artifacts,
checksums and `exit-status.txt` with `exit_code=0`. Storage inputs were read-only;
only the dated analysis parent was writable. Networking was disabled.

| Identity | SHA-256 / ID |
|---|---|
| Executable | `5cb10548a3ed5cc07b81fefee2d8e4b18820312e55cc222321a744dc489abb2d` |
| Profile | `13c3295b8d210f6e82b2ceaa77f5c0a9dde357fb68b3452df0fb9796bb2d9b36` |
| Diagnostic association replay | `f90ddf3822aa7d93471129d6e3ec943ec8b718c2ce7a30d1a8e2fe21caea19fb` |
| Participant parent | `5cf4f803465c19f8abc0f4cd3eab87b132184bc47536b6aa3c0c5753dbbe0a1e` |
| Topology parent | `3192971a9cbd28696cbf911ece53b7362a5a77c7c84be4e262236dd8b1c2acca` |
| Decision artifact | `6580956ba65bd1c0845e9b7cd610983fcffea3a5a96e583e1ba18496bd06296c` |
| Decision canonical values | `8ff2967130c9da69a59f50f8ee09f9404ef1f27fb93926aca2335731e3ff2403` |

The new calculation ID reflects the new executable; the decision artifact,
canonical values, every state/amount count and all parent identities exactly
match the [accepted conduit publication](./receipt-conduit-publication-2026-09-12.md).
Fresh retained-file checksums, including the full decision artifact, pass.

Measured complete execution: 314.981 seconds, 1,679,192,064-byte peak process RSS,
628,353,983-byte peak workspace and 124,747,339 retained decision bytes.
The whole job directory is 158,716,856 bytes, including the executable and
metadata. The container had eight CPUs, a 4 GiB memory/no-swap cap and a 2 GiB Go
heap limit; the disk workspace cap was 8 GiB. No standing budgets changed.

Focused policy, profile, full-join, failure-boundary and CLI tests pass. Race
tests for the association/profile packages and targeted vet pass. Funding-basis,
reference, receipt-graph and CLI regressions pass; graph HTTP/storage fixtures
ran in an isolated container with real temporary filesystem headroom.
Fixture replays vary shards, workers and sort layout, including signed sums,
null amounts, mixed roles, unsafe endpoints, partial groups and witness ownership.
A second complete real profile run was not performed; fixture determinism and
full real equivalence of the unchanged association output are distinct evidence.

This retained executable can replay against the pinned input publications.
It is not a full raw-to-graph recovery checkpoint or a source-retention policy.
