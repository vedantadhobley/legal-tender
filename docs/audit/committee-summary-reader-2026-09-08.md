# Committee-summary Go reader gate — 2026-09-08

Result: the strict Go reader/verifier passes all four captured cycle CSVs and
byte-identical replay. Independent source-to-Go comparison verifies every raw
field and typed money/date/identity value, not just row counts or selected totals.
The [source contract](../design/committee-summary-source.md) remains draft pending
publication acceptance. No release pointer, financial fact set, graph, or Dagster
asset changed.

## Exact inputs and results

Inputs are the complete artifacts pinned by the
[source review](./committee-summary-source-2026-09-08.md) and retained under
`/storage/dumps/audits/fec/committee-summary-source/2026-09-08/`.
There was no new acquisition or API call.

| Cycle | Source records | Invalid dates | Invalid candidate references | Reversed intervals | Result bytes |
|---|---:|---:|---:|---:|---:|
| 2020 | 13,554 | 9 | 0 | 1 | 18,510 |
| 2022 | 13,977 | 7 | 2 | 0 | 18,290 |
| 2024 | 14,065 | 3 | 0 | 1 | 17,185 |
| 2026 | 14,152 | 1 | 7 | 0 | 18,051 |

All 55,748 records and their byte spans are conserved. Both field hashes and
typed-value hashes match an independent CSV/Decimal/date implementation. All
75 monetary columns pass exact conversion; source blanks remain blank, negative
values remain signed, and invalid dates have no invented typed date. Every source
cycle value matches its requested partition.

The original repeated-committee group counts are reproduced: 18, 31, 65, and 70.
Their non-candidate fields are identical within each group. The reader keeps
every row; no financial grouping or singleton statement is published.

All nine new identity issues are nonblank `CAND_ID` strings shaped like committee
IDs rather than candidate IDs. The 2022 witnesses are source ordinals 13,813 and
13,855. Seven additional witnesses occur in 2026. The verifier preserves these
strings and marks only their candidate-reference interpretation invalid; it does
not fix them by identity inference. Such rows remain in all field/money/date
profiles but cannot enter the valid composite identity index. Independent tests
verify both their exact source locations and that index exclusion count.

The results retain all prior date, blank-value, and diagnostic equation findings.
A completed scan does not resolve the cause of a summary difference or establish
report/account scope. No terminal-dollar eligibility changes.

## Reproducibility

The retained output directory is
`/storage/dumps/audits/fec/committee-summary-reader/2026-09-08/`.
Each `<cycle>.json` has a byte-identical `<cycle>-replay.json`, separate logs, and
explicit zero exit markers. The result schema and fingerprint encoding are in
the [verification contract](../../contracts/audits/fec/committee-summary-verification/v1/).

| Cycle | Result SHA-256 |
|---|---|
| 2020 | `3065f8d57440d2af45f3f4b61876600746d01179c4a74c9adedb0febc5ca4c2a` |
| 2022 | `ba18c10c5a17255a6492a60c0c1b1c3184b56519bf3f93139c246f46445d4cbc` |
| 2024 | `4b7b7c40a2a3763e21728705a1c917b25e965ca322792a2097ad6349e5af0884` |
| 2026 | `3af91667d48530690823847b6ea61d6d985e92c5673200268c110d2a5aa96b9f` |

One additional 2024 run took **0.514 seconds** inside the container, excluding
container startup. It reused local cached source bytes; this is not a cold-I/O
benchmark. Its JSON equals the earlier 2024 result. Runs used two CPUs, a 512 MiB
container cap, and `GOMEMLIMIT=256MiB`; the cap is not a measured peak-memory claim.

## Validation

- Go formatting, module-diff check, `go vet ./...`, and the complete Go test suite pass.
- Focused reader/CLI race tests pass.
- Exact fixtures exercise leading decimals, blank and negative money, invalid
  dates, reversed intervals, repeated references, and byte-level row locators.
- Adversarial tests reject corruption, wrong capture size/cycle, schema changes,
  malformed quotes, short/long rows, blank records, invalid UTF-8, CR, missing
  final LF, and resource-limit violations without a successful result.
- Multiline/quoted UTF-8 fields survive; cancellation and observer errors fail
  cleanly. Duplicate/conflicting identities remain counted, and equation tests
  reject signed-integer wraparound as a false equality.
- Independent source/reader checks pass: 27 tests, including all four complete
  files and replay. The broader rewrite suite passes: 84 passed, 10 unrelated
  opt-in corpus tests skipped, and 60 existing Dagster warnings.

For independent readback, set `LT_COMMITTEE_SUMMARY_REVIEW` to the retained source
directory and `LT_COMMITTEE_SUMMARY_RESULTS` to the retained output directory,
both mounted read-only in the existing test image:

```bash
python -m pytest -q -p no:cacheprovider \
  tests/test_source_contracts.py tests/test_committee_summary_source.py \
  tests/test_committee_summary_reader_corpus.py
```

The next gate is immutable occurrence/fact publication and versioned release
membership. Source-qualified assertion grouping/reconciliation follows; report-
time funding and terminal allocation remain unimplemented.
