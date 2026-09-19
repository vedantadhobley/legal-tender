# Cross-cycle resolved independent-expenditure gate — 2026-08-31

## Outcome

The unchanged candidate-resolution method, resolved grouping policy,
readiness bundle, and ArangoDB v2 projection passed for 2020, 2022, 2024, and
2026. Every cycle used exact artifacts from coordinated source release
`fec-76b6660f70406bf0d11537885de172883cdb8af548bfa7df35078a721c8c759a`.
No API, classic transaction file, cross-cycle identity lookup, fuzzy match, or
candidate-specific exception participated.

All four graph projections are `ready`. They have zero missing candidate or
spender masters, exact count and signed-cent readback, and content-addressed
replay. The three newly tested cycles also returned their original immutable
resolution, aggregate, and bundle identities on replay.

## Methodology hardening

This gate also closed three contract risks before running more data:

1. The processed Schedule E source contract is now `accepted` after its
   complete-corpus source, parser, occurrence, fact, and downstream effective-
   calculation gates passed. Schedule A remains `draft`: it is the selected
   authority for the current receipt slice, but its broader historic memo and
   conduit coverage gate is still open.
2. The shared source-contract JSON Schema now validates every checked-in source
   contract in the normal test suite. A separate regression test locks the
   intentional Schedule A `draft` and Schedule E `accepted` distinction.
3. Go tests now load the checked-in candidate-resolution and resolved-grouping
   policy fixtures. Runtime state, method, identity, grouping, exception,
   count, and exact signed-amount behavior must match the machine contract.
4. The 2020–2026 period set is frozen membership of release inventories v1 and
   v2. A future rolling-window change requires a new inventory version. Tests
   prove callers cannot mutate the replayable inventories through returned
   slices.

Candidate- and committee-master occurrences and facts for 2020, 2022, and
2026 had not previously been materialized. This run published only those six
missing reference slices from the already accepted release. It did not
redownload or supplement source data.

## Resolution and coverage

| Cycle | Source decisions | Confirmed | Resolved | Unverified | Ambiguous | Unresolved | Unprojectable amount |
|---|---:|---:|---:|---:|---:|---:|---:|
| 2020 | 67,365 | 51,357 | 534 | 14,971 | 1 | 502 | $3,933,205.46 |
| 2022 | 61,965 | 44,512 | 511 | 16,674 | 0 | 268 | $8,376,624.57 |
| 2024 | 58,288 | 45,185 | 1,811 | 10,996 | 0 | 296 | $18,549,639.21 |
| 2026 | 13,632 | 11,165 | 230 | 2,106 | 8 | 123 | $1,814,102.08 |

Unprojectable fact coverage ranges from 0.432502% in 2022 to 0.960974% in
2026. Unprojectable signed-amount coverage ranges from 0.124709% in 2020 to
0.427683% in 2024. The nonzero ambiguous populations in 2020 and 2026 prove
that the explicit ambiguity route is live; those facts and cents remain sparse
exceptions rather than forced candidate edges.

Across the four cycle-scoped publications, 201,250 source decisions conserve
$10,358,126,288.01. The graphs contain 200,052 projectable decisions totaling
$10,325,452,716.69. The remaining 1,198 decisions and $32,673,571.32 remain
outside candidate edges with exact evidence.

## Projection gate

| Cycle | Resolved groups / edges | Candidates | Spenders | Projected signed amount | Projection ID |
|---|---:|---:|---:|---:|---|
| 2020 | 6,404 | 1,133 | 913 | $3,149,960,967.43 | `adf8cac5ec4cfa9b14a1d8b9a7c5bd7e25d808a0c0935dc440b64c38d825cc6b` |
| 2022 | 6,234 | 1,287 | 863 | $2,199,157,270.29 | `20d0d9370ddbed6834521f718ee1ff8966cdee085cd054e7c5823bfc7715a147` |
| 2024 | 5,303 | 911 | 947 | $4,318,692,700.10 | `1817e94e6ef06e700bd8368f256c2ad88b8592906e3a8a6b760191eee39c44f0` |
| 2026 | 2,063 | 738 | 532 | $657,641,778.87 | `6bfb68aa5020c6edcc4ff0ebc7d5b8663e3c3c47db1e665999aeb8a27954e071` |

Expected and observed vertex, edge, stance, and signed-amount values matched
for every cycle. All representative ten-run query p95 measurements remained
below one millisecond. ArangoDB storage figures are mutable operational
observations under background compaction and do not enter projection identity.

## Immutable calculation identities

| Cycle | Candidate resolution | Resolved aggregate | Readiness bundle |
|---|---|---|---|
| 2020 | `7d686616087aeda822e9ed5b5267ab77406feb5cba5299b8c0ad66f08ea26f93` | `4bb0627b16f624498ffe4fdec88063dde2cd5c500eb35b4a1c871950444d5338` | `f492ecc3c641f45ac6702e56c23265212e2f7373cbc12735696080f6f4f53447` |
| 2022 | `89433ae66a0dcde81971e57b6a0818eac4a7e4532b168c326986787ecf187957` | `2649b2eda8904bedd31f33c9650ba982f66ca8ee2304149844c530b68540403c` | `393877acc676eb86321af2ea224879e57515bc2fc2b60a2d45317a64eeef319f` |
| 2024 | `e947b7ff3e9231864d582526972e07a7acf3a8301c2ffb43e76de8e340ee43ba` | `484d2ddc86fe0e82e0e2e7d5a470f67b4d89cfddd0c3784a43b9d57b2263a830` | `67cfb78d87076b2897ed8e7fd1eb5999e6fca477307190b419f7f7e83451333f` |
| 2026 | `0fdec2d270eeeb661925eaa84af2c08a33589c8e1dc38c8914071f5e88eb7d60` | `cfd56b77d428c03722a32193bfa203268abc707f03be3066a2ec64d09a258856` | `7f9a79f8f87f732230d9c7203cacc78e39f4d687d2bf4b4cd17ebf5bf93bb55e` |

## Verdict and next boundary

The 2024 method did not depend on a 2024-only identity exception. Variation in
resolved, unverified, ambiguous, and unresolved rates is visible and conserved
instead of normalized away. The outside-spending component now passes its
four-cycle physical and methodological gate.

The next graph work is the Schedule A master/history audit and receipt-side
PAC-flow projection. Schedule B remains a separate sender-side source audit
and later A-to-B reconciliation boundary; it must not block the first
receiver-reported flow projection or be summed with Schedule A.
