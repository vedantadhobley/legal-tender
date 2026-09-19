# Approved v4 acquisition — 2026-09-09

Status: completed successfully at 22:31:15 UTC, after starting at 21:32:58 UTC
(58m 17s). All three durable exit markers are zero, the Go result is
`status=acquired`, and independent verification passed. V3 remains active;
staging and release activation have not run. Do not repeat this acquisition.

The user approved the 129,566,279,592-byte acquisition reviewed in the
[fresh preflight](./fec-v4-preflight-2026-09-09.md). A new pre-launch HEAD pass
and plan matched every approved publisher selection and its metadata, and the
live storage review passed. The original approved plan bytes remain the input;
the new observation did not silently select another candidate.

## Execution boundary

- Manual Go acquisition, not a new Dagster schedule or release activation.
- Container: `legal-tender-v4-acquisition-20260909`, using the existing cached
  development runtime with the exact CLI binary from the approved preflight.
- Memory cap 2 GiB, Go memory target 1 GiB, four CPUs, and three download workers.
  PostgreSQL client 15.19 supplies `pg_restore --list`; no database restore runs.
- Source storage is mounted read-only except `raw/fec/`; the job also has its
  own writable audit directory. Release pointers and downstream data are
  read-only. No credentials or `.env` file are passed to the container.
- The existing 600 GiB hot cap, 500 GiB filesystem floor, 25 GiB margin, shared
  writer lock, and runtime growth guards remain enabled.
- No staging, release publication, summary fact publication, graph mutation,
  weekly automation, or historical-evidence deletion is authorized by this run.

The first sustained check at 21:34:38 UTC found 4,329,494,111 captured/partial
bytes (3.34%). The container used about 74 MiB of its 2 GiB cap. The active
manifest still matched the baseline. This is a point-in-time progress check,
not a completed acquisition or throughput guarantee.

## Verified result

The completed acquisition contains all 27 selected artifacts: 24 acquired and
three reused. New downloads total 129,566,279,592 bytes. The result reports no
issues. Independent verification passed schema, exact input/artifact identity,
stored file metadata, post-capture publisher-version agreement, and durable
acquisition-state checks. It did not reread all downloaded bodies; the Go
acquisition hashed those bodies while capturing them.

Both active-manifest hashes are
`a28d56c7a3be024ed7cd85e2bf0ead21c982c96d00b683ef364c0a37eb9bc3ec`.
The active release remains
`fec-136903645ee7c7050d463a4f779a051a454ebb87308fd854faabc5ca47a20cdf`;
candidate
`fec-01f93a786b630be40a932543f35251bcb56c42b32d1ae85ac34a101ddc7b56b2`
is acquired, not published. Independent verification also confirmed that the
active immutable manifest remained unchanged.

A read-only post-exit check at 22:32:01 UTC confirmed all success markers,
100% captured bytes, no remaining Schedule A/B/E partials, and unchanged v3.
Unique Schedule A hot storage was 387,188,829,778 bytes, with
1,636,167,815,168 filesystem bytes available. These are point-in-time values,
not a reservation for staging. The one-shot acquisition container exited and
removed itself; the job evidence and acquired source artifacts remain.

## Durable state and completion checks

Job evidence lives under
`dumps/audits/fec/v4-acquisition/2026-09-09/attempt-01/` in project storage.
The directory holds the exact inputs, binary, runner/verifier/monitor scripts,
pre-launch observations and plan, storage review, logs, result, and markers.

- `run.exit`: runner completion; absence is not success.
- `acquisition.exit` and `acquisition.json`: Go exit and typed acquisition result.
- `verification.exit` and `verification.json`: independent schema, identity,
  file-size/name, post-capture version, durable-state, and unchanged-v3 checks.
- `acquisition.log`: capture and archive/CSV validation phases.
- `runner.log`: wrapper or independent-verification failures.
- `current-before.sha256` / `current-after.sha256`: active-manifest identity.

Success requires all three exit markers to be zero, `status=acquired`, and a
passing verification result. A vanished container is not proof: the one-shot
container removes itself on exit, while evidence and source partials persist.
Read the markers or run the preserved read-only `monitor.py` with `/job` and
`/storage` mounted read-only. While the container runs, the same monitor is
available through `docker exec`.

The stable Go run ID is `v4-acquire-20260909-01`. On failure, inspect the typed
issue and preserved prefixes before deciding on a retry. Do not substitute a
new plan under that identity or discard retained bytes. Successful acquisition
hashes downloads, rechecks every publisher version, validates ZIP CRCs, dump
headers/selected TOCs, and committee-summary CSVs, and writes immutable source
artifacts plus the acquisition state. It does not yet validate every selected
Schedule A/B/E row; source extraction and downstream fact gates remain separate.

Follow-up: the separately [verified stage](./fec-v4-staging-2026-09-09.md)
completed with all 25 selected outputs, passing integrity/storage checks, and
independent verification. Field-level occurrence/fact validation, coordinated
release publication, summary publication, and explicit v4 activation remain
separate gates. V3 stays active until those gates pass.
