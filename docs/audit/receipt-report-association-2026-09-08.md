# Receipt report-association gate — 2026-09-08

Status: bounded real-source gate passed for two reports from one committee.
This is not a complete-cycle conduit coverage claim. The
[implemented policy](../design/receipt-report-association.md) is a conservative
reported earmark/memo association; registration, original-donor resolution,
cash timing, and terminal-dollar attribution remain unverified.

## Inputs and scope

Use the unchanged 2024 [receipt inventory](./committee-funding-basis-2026-09-08.md):
`e31d7e8248ad6594c6ce23f86a0fe0cd2ff0ac13a0e78d079c97562519416985`,
Schedule A fact set
`8f69a2ef40316f62d8844ecf732342a3b4535136b434f60677d0608cb04465df`.
No Schedule A source, inventory, graph, release pointer, or monetary predicate
was changed. The reviewer uses Go, four CPUs, a 4 GiB container cap,
`GOMEMLIMIT=2GiB`, and read-only source storage without network access.

A bounded four-shard discovery found both directions of report references,
including references with non-conduit roles. That sample selected actual source
report IDs for inspection; it did not define a hardcoded committee rule.
Original [filing 1730369](https://docquery.fec.gov/dcdev/posted/1730369.fec)
and [filing 1753173](https://docquery.fec.gov/dcdev/posted/1753173.fec)
were then retrieved as size-limited research evidence. A different filing,
1713787, exceeded the 10 MiB download cap and was not used as complete original
evidence. No API or new production raw-filing source was introduced.

## Complete selected-report results

Both reports belong to reported committee ID `C00849901`. The reviewer scans
all of that committee's inventory-bearing shards, not just adjacent rows or
the shard holding the initial witness.

| File | Schedule A records | Exact references | Earmark/memo associations | Different reported amounts |
|---|---:|---:|---:|---:|
| 1730369 | 13 | 2 | 2 | 1 |
| 1753173 | 74 | 29 | 29 | 0 |

The independent audit checks every selected transaction ID against every
Schedule A row in each whole original file. It also checks committee ID,
back-reference ID/schedule, entity, memo flag, amount, date, and schedule/line.
The pinned 45-field research layout is applied by field name, with exact
physical width and format-8.4 header checks. Each original transaction ID is
unique in these tested Schedule A populations. All comparisons pass.

All 31 positive associations have exact memo-to-original references and matching
raw/clean memo committee IDs, also present in the original memo's donor-ID
field. The rule identifies a reported conduit association to `C00401224`.
It does not certify registration or overwrite the original contributor fields.
The opposite reference direction passes synthetic tests and was observed in
processed source rows, but its complete original-file gate is still deferred.

One original contribution at ordinal 33,105,737 links to memo ordinal 1,107.
Their respective reported amounts are $2,000 and $200, with dates September 20
and September 29, 2023. Both values match the original file. The reference
survives; `different_reported_amount` remains explicit. Nothing infers fees,
corrects the source, or treats the memo as another contribution. The other pair
in that report has two $250 observations with different reported dates.

Every association emits `additional_amount_minor_units="0"` and
`terminal_attribution_eligible=false`. This asserts no extra money created by
the annotation, not zero conduit fees or complete provenance.

## Reproducibility and cost

Final report reviews took 15.334 and 15.468 seconds, including backing-data
verification. Replay took 15.387 and 15.643 seconds and produced byte-identical
outputs. Only full selected report rows are reconstructed; the receipt corpus
is not rewritten or copied. This bounded reviewer is not proposed as the
cycle-wide production indexing strategy.

Retained directory:
`/storage/dumps/audits/fec/receipt-report-association/2026-09-08/2024/`.

| Artifact | Bytes | SHA-256 |
|---|---:|---|
| `1730369.fec` | 4,794 | `0a12e970d484a19aa5a72e4eb38e1379cecbfb0e9c1b7a8db38aa05fdbfa66ea` |
| `1753173.fec` | 21,751 | `8f82657d70261d1e472c666b49f95991e6ca021bfd3eddb6addb2873d6f72ddf` |
| `report-1730369-final.json` | 58,599 | `3db80e7ef709084c6225030d755501c369d9378b8db9b94b6c1063e324d51bd7` |
| `report-1753173.json` | 335,180 | `eacdd7a37742883b1c7c1c389a6bfdd3872c65431d0675b58d2cd7797157f485` |

Replay files use `report-<file>-replay.json`. Matching `.exit` markers are zero.
The independent audit is `tests/test_receipt_report_corpus.py`, enabled with
`LT_RECEIPT_REPORT_AUDIT` set to that retained directory. Ordinary schema tests
need no source storage. Go tests cover both directions, duplicate source/target
IDs, missing/wrong schedules, self references, shared memos, entity/role and
ID conflicts, amount differences, exact report scope, cancellation, limits,
strict physical-field decoding, and deterministic replay.

Formatting, dependency consistency, `go vet`, the complete Go suite, and
focused funding-basis race tests pass. The rewrite Python boundary suite passes
with 62 tests and seven opt-in corpus skips; the separately enabled report
corpus/schema gate passes all six tests against the retained storage copies.
The existing Dagster beta warnings do not indicate new boundary failures.

The next implementation is the [funding coverage audit](../design/funding-coverage-and-time.md),
not graph-wide proportional normalization. Complete-cycle association
publication and additional identity/role coverage remain separate work.
