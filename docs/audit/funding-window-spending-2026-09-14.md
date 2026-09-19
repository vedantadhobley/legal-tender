# Source-grain spending window gate — 2026-09-14

Status: accepted for the retained 2024 generation. All 30 cases and fresh-process
replay pass with byte-identical results. All four completion markers are zero;
independent setup, output, regression, resource and source-snapshot checks pass.
Full Go regression, targeted race checks and static analysis pass.

The [source-member contract](../design/funding-window-reader.md#source-grain-schedule-e-connections)
defines the reader. This gate adds acceptance code, not graph writes, new source
rules, data acquisition, identity resolution or attribution policy.

## Reproduction boundary

Attempt: `/storage/dumps/audits/fec/funding-window-spending/2026-09-14/attempt-01/`.
The input specification is copied unchanged from the accepted
[receipt/candidate window gate](./funding-window-connections-2026-09-13.md).

- Generation: `35d249388ad51d1b03a98324e2f096bc20e945a35d2a38206b12de7702e1d9cd`.
- Input SHA256: `c5f9610e4f23112b6f3dcb3a2719a877300e9e1d4c8836da1c0025e80432299a`.
- Test executable SHA256: `140c607a0ebeefc965b838a807269bf58eba0c06751c063e1a08d9f416eb63f1`.
- Source archive SHA256: `6feb17b5f3a01aa07505a920982bca273c8d5256cc619604a4ac79a5c8b41c39`.

The [opt-in test](../../internal/projection/arango/fundinggeneration/window_spending_live_test.go)
opens the public `WindowReader` and invokes `ConnectionPaths`. Its identity binds
the test executable. CLI equivalence remains covered by the existing full-chain
synthetic loader/CLI fixture; this audit does not claim a new live CLI execution.
Ordinary tests skip the live gate without `LT_WINDOW_INPUTS`.

The [runner](../../scripts/run-window-spending-gate.sh) expects a new `/audit`
directory containing `window.test`, `inputs.json`, `source.tar.gz`, `run.sh` and
`SETUP_SHA256SUMS`. Pass the configured password privately and set
`LT_WINDOW_ENDPOINT`; neither credential values nor a cycle-selection heuristic
belong in the retained specification. Source storage is mounted read-only.

The runner verifies setup hashes, executes the gate, starts a fresh process with
the expected gate ID, requires identical result bytes, and records final hashes,
timing and cgroup memory measurements. Acceptance requires `run.exit`, `gate.exit`,
`replay.exit` and `tests.exit` all zero. Retain the source snapshot, executable,
inputs, scripts, logs, outputs and checksum files. A result file alone is not proof
of completion. Do not overwrite an attempt after a failure or source change.

## Independent checks and automatic selection

The [independent census](../../internal/projection/arango/fundinggeneration/window_spending_census_test.go)
decodes every retained Schedule E fact and candidate-resolution decision. It does
not call the production dated-member visitor or date selector. It reuses the
accepted effective predicate; it does not invent a second money policy. Decisions
join by fact ID, with complete membership conservation. Source and decision
hashes retain every field for comparison without storing every full fact in RAM.
Published aggregates supply independent parent identities and values.

Every query's coverage must match independently grouped source facts by native
date, stance, effective state, amount/route exceptions, candidate-resolution
state and projectability. Known amount counts distinguish null from zero.
The complete selected A/B date census is independently checked as well.

For each stance and date field, the gate selects a group with multiple reported
days. It queries all supplied dates, the selected day and the following day.
Direct queries must return exactly the expected bounded source-member IDs, not
just plausible records. Their unchanged parent aggregates must retain the full
published counts, signed amounts, sign counts and resolution breakdowns.

Additional automatic cases seek unknown dates, different native date fields,
negative and zero amounts, and resolved/unverified candidate references. Selection
respects the public result limit; absent returnable shapes remain explicit gaps,
not fabricated cases or claims of global absence. Unknown-date exclusion and
alternate-date-field checks ensure no fallback date is introduced.

Both selected committee ledgers also supply automatic upstream path witnesses.
Source-backed receipt entries at those origins extend the paths where available.
Queries retain exact source routing and historical facets, with no path-money
sum, ledger merge or terminal classification.

The [result checker](../../internal/projection/arango/fundinggeneration/window_spending_gate_checks_test.go)
compares full source/decision identities, both native dates, selected amounts,
unchanged parents, coverage, path continuity, input lineage and eligibility.
[Failure tests](../../internal/projection/arango/fundinggeneration/window_spending_gate_unit_test.go)
alter these values and recompute result IDs. A matching checksum cannot conceal
incorrect source evidence or an incomplete result.

## Resources and remaining gates

The live runner uses eight CPUs, a 4 GiB container cap, a 2 GiB Go heap target and
no swap allowance. The regression runner uses four CPUs with the same memory
controls and no network. The existing development ArangoDB retains its unchanged
configuration. Cgroup peak includes charged filesystem cache; it is not process
RSS, proven spare capacity or a second-cycle capacity forecast.

The complete independent census conserves 67,292 source facts, 58,288 published
candidate-resolution decisions and 57,992 projectable source members. The 296
unprojectable candidate decisions remain explicit rather than becoming edges.
All source facts contribute to date/policy coverage, not just projectable members.
There are 7,942 unknown expenditure dates, 415 unknown dissemination dates and
37,676 records with two known but different native dates. These counts describe
the retained source, not newly downloaded records or missing source evidence.

Both passes verified every automatic case without a selection gap and returned
62 bounded path witnesses. The first pass's 30 query checks took 220.705 seconds
combined, ranging from 4.123 to 9.767 seconds each; query timings include
independent result checking. The remainder includes source/graph opening,
census and witness preparation.

| Measurement | Result |
|---|---|
| Complete opening, census and first gate | 375.202 seconds |
| Fresh-process opening, census and replay | 372.453 seconds |
| Each result | 3,489,974 bytes; byte-identical |
| Cgroup peak, including charged filesystem cache | 4,296,011,776 bytes |
| Retained attempt including executable and source snapshot | About 33 MiB |

The runner reached its memory circuit breaker: `memory.events.max=72443`, with
`oom=0` and `oom_kill=0`. The cgroup peak includes cache and transient accounting;
it is not process RSS or evidence of spare capacity. This run qualifies this
bounded workload, not a larger input window or an additional cycle's capacity.

Gate ID: `c004513e02437a71815c605d124c715146c7d379fcb52bed9a9c1d2a825da404`.
Result/replay SHA256: `972103e4063c4171f890e0c75b1415c3695ed7355ef04695235cfb4e802c2146`.

Independent final readback verified `SETUP_SHA256SUMS`, `FINAL_SHA256SUMS`,
`REGRESSION_SHA256SUMS`, `ACCEPTANCE_SHA256SUMS`, the four zero exit markers, exact
result comparison and archived-source comparison with the working Go tree.
Both containers exited zero without OOM kills; `container-states.txt` retains
their states. Both stopped test containers were removed; all audit evidence
remains intact. No project data or persistent volumes were deleted.

This acceptance gate is not a low-latency serving benchmark: opening performs
complete backing/readback checks, and each query revalidates source membership.
Real second-cycle acceptance remains separate. The user-requested
[interpretation review and evidence checkpoint](../design/pre-attribution-review.md)
must precede selecting terminal definitions or allocation rules.
