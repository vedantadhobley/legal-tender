# Receipt and candidate window connection gate — 2026-09-13

Status: accepted for the retained 2024 generation. All 15 cases and fresh-process
replay pass with identical result bytes and zero exit codes. Full Go
regression, targeted race tests and static checks pass. The
[connection contract](../design/funding-window-reader.md#receipt-and-candidate-connections)
defines the reader; this gate adds acceptance code, not another graph or money policy.

## Scope and reproducibility

Attempt: `/storage/dumps/audits/fec/funding-window-connections/2026-09-13/attempt-01/`.
It retains the test executable, source snapshot, exact input specification,
runner, results, timing, memory measurement, regression logs and exit markers.
The input specification is copied unchanged from the accepted
[committee-window gate](./funding-window-2026-09-13.md).

- Generation ID: `35d249388ad51d1b03a98324e2f096bc20e945a35d2a38206b12de7702e1d9cd`.
- Input specification SHA256: `c5f9610e4f23112b6f3dcb3a2719a877300e9e1d4c8836da1c0025e80432299a`.

The [opt-in Go gate](../../internal/projection/arango/fundinggeneration/window_connection_live_test.go)
opens the public `WindowReader` and calls `ConnectionPaths`. The result binds the
test executable, not a separate CLI executable. Existing full-chain synthetic
integration tests cover CLI equivalence; this audit does not claim a new live CLI
invocation. Ordinary tests skip the live gate unless `LT_WINDOW_INPUTS` is supplied.

The [retained-run script](../../scripts/run-window-connection-gate.sh) expects a
new `/audit` directory containing `window.test`, `inputs.json`, `source.tar.gz`,
the copied script and `SETUP_SHA256SUMS`. Set `LT_WINDOW_ENDPOINT` and pass the
existing `ARANGO_PASSWORD` privately. `/storage` is read-only; only `/audit` is
writable. No source path or cycle is inferred from the current date.

The runner verifies setup checksums, runs the opt-in test, records `gate.exit`,
then invokes the same executable in a new process with `LT_WINDOW_EXPECTED_GATE`.
It records `replay.exit`, requires identical result bytes, and checks final
artifact hashes. Success requires `run.exit`, `gate.exit`, `replay.exit` and
`tests.exit` to be zero, plus completed checksum verification. A result file or
disappeared process alone is not acceptance.

## Automated selection and independent checks

The gate decodes every selected compact A/B observation independently of the new
reader's date selector. Per-publication day counts and unknown-date counts are
compared with every query's complete included/before/after/unknown census.
This is complete selected-ledger coverage, not every raw receipt or every path.

For each ledger, source topology and authorization assertions select a
deterministic two-observation candidate witness. No politician, committee,
source ordinal or cycle is a production special case. The selected route supplies
the inclusive date window. Cases check unfiltered and bounded candidate routes,
an explicit hop cutoff, a source-backed receipt entry, and its date exclusion.
Receipt selection reads one first page per supplied publication at the chosen
origin; an absent receipt there is reported as a selection gap, not global absence.

The first qualified conduit from the complete compact conduit publication supplies
another source-backed witness. Cases inspect its entry without requiring a
downstream candidate path, include its reported day, and exclude the following day.
The association remains zero additional money, with the underlying receipt date
rather than invented relationship validity. Missing cases remain explicit.

The [independent result checker](../../internal/projection/arango/fundinggeneration/window_connection_gate_checks_test.go)
also verifies source-qualified receipt ordinals, typed source dates, A/B ledger
and fact-set routing, original link identities, connected path endpoints,
authorization fact membership with unknown validity, and publication facets.
Failure fixtures alter values and recompute result identities: changed dates,
mixed ledgers, missing source links, invented authorization dates and changed
eligibility must still fail. A matching result digest is not the validation rule.

## Resources and remaining boundaries

The complete independent census conserved 320,731 selected Schedule A observations
and 341,720 selected Schedule B observations. Every query's date partition matched
that census. All selected receipt/conduit witnesses had known dates; unit fixtures
cover unknown-date behavior. No witness case was skipped in this real run.

Both ledger routes returned source-backed committee and receipt candidate
connections before and after bounding their dates. Moving the window to the day
after the selected receipt excluded the entry and returned no path. Zero committee
hops exposed the expected frontier. The conduit entry was present on its receipt
date and absent the following day, with zero additional amount throughout.

| Measurement | Result |
|---|---|
| Complete opening and first gate | 197.125 seconds |
| Fresh-process opening and replay | 194.686 seconds |
| Each result | 1,091,367 bytes; byte-identical |
| Cgroup peak, including charged filesystem cache | 4,295,933,952 bytes |
| Retained attempt including executable and source snapshot | About 27 MiB |

Gate ID: `c4c37eb5e8316de6d05e452310a573c82a0a3a90fa09263aaacc74ac92a2f7d4`.
Result/replay SHA256: `81f20da65549fa28ef067938a2b277d1315bb40d27c725773f13f97d7c05eed1`.
Test executable SHA256: `a118f1e9ce167770ab731f7e43df53f21b37646507fdb67f03d0fe79124e4c7b`.

Independent readback passed `SETUP_SHA256SUMS`, `FINAL_SHA256SUMS` and
`REGRESSION_SHA256SUMS`, all four exit markers, the result byte comparison and
the archived-source comparison with the working Go tree. Both containers exited
zero without OOM kills; their states are retained in `container-states.txt`.
Both stopped test containers were removed, leaving the audit artifacts intact.
The peak is near the circuit breaker; it includes charged
filesystem cache and is not process RSS or second-cycle memory headroom.

The live runner has eight CPUs, a 4 GiB container cap, no container swap allowance
and a 2 GiB Go heap target. The separate regression runner uses four CPUs and the
same memory controls without networking. The development ArangoDB is read-only
to this workflow and keeps its existing configuration.

This gate adds no acquisition, source publication, graph import, pointer change,
Dagster activation, identity resolution or terminal attribution. It does not
qualify two real cycles or low-latency serving. Date-selected Schedule E membership
and the user-requested [interpretation review](../design/pre-attribution-review.md)
remain separate work.
