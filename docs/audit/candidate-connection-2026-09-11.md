# Candidate connection source drilldown — 2026-09-11

The [candidate evidence command](../design/candidate-evidence-view.md#source-row-drilldown)
now has a companion `inspect-candidate-connection` command. It opens one concrete
connection from an expected v2 report and returns its full retained source row.
The implementation reuses the existing published source reader; no receipt
aggregation, source acquisition, graph write or Dagster activation occurs.

## Selected inputs and results

Both inputs come from the immutable
[named-report gate](./candidate-report-2026-09-11.md). Each selected row is already
a hop in that report's concrete path examples. These are audit inputs only;
runtime code contains no candidate- or committee-specific selection rules.

| Parent input | Source ordinal | First run | Exact replay |
|---|---:|---:|---:|
| S6OH00163 | 210227192 | 17 s | 16 s |
| S6PA00217 | 211693515 | 17 s | 17 s |

Connection IDs:

- `bf45e9bfafa888faed7b20181cf766a7e3395d60764f37721ad9b9dc474ec179`.
- `98a1f2cc62603101f2f7e1052c703931e4e33d5ecfd07a47d33125b214d47f05`.

Lookup executable SHA-256:
`911b151a2ede6910742c0386f2cbe3d1fea9101c3b8ea884f1effef3f3e293ee`.

Each output contains all 99 physical fields, including raw source strings,
nulls, typed values, and row locators. The two JSON outputs total 14,564 bytes.
The readable views quote strings and distinguish null, empty, false and zero.
Parent names remain inherited display assertions. No terminal amount is assigned.

## Verification

- Require a separately supplied expected report ID. Check the v2 report content
  identity and nested v1 evidence identity; reject malformed, duplicate-key,
  unknown-field and oversized report JSON.
- Require concrete witness or candidate-boundary membership. Duplicate or
  conflicting observations fail; an arbitrary upstream ordinal is not accepted
  merely because it appears elsewhere in the graph.
- Match the exact published calculation digest, source inputs and cycle before
  source lookup. Compare all observation fields, not just the source ordinal.
- Hash and read the requested physical shard through the same file handle and
  verify the full source-derived observation. Startup still verifies complete
  A/B backing; only requested rows are decoded.
- A separate Go corpus test directly opens Parquet, seeks both ordinals and
  compares every physical field with each result. It does not call the runtime
  source-reader helpers. It uses the same pinned Parquet library, not an
  independent decoder.
- Python checks verify output/parent hashes, exact parent membership and names,
  raw/typed amount and date equality, source manifest/shard identities, and
  membership against the complete saved published observation artifact.
- Both JSON and Markdown replay byte-for-byte. The parent JSON/Markdown files
  remain byte-identical. Rebuilding the executable after adding corpus tests
  produces the same executable bytes.

Full Go tests, vet, targeted race tests and the real physical-row corpus test pass.
The focused Python suite passes with four passed and two skipped; the skips are
the separate earlier candidate-report corpus harnesses. The new real connection
audit runs. Ruff and changed-document link checks pass.

An initial formatting test exposed numeric HTML entities being split by the
shared Markdown escaping helper. HTML escaping now runs last, with explicit
quote/apostrophe regression coverage. No source value or monetary policy changed;
the initial failure log is retained. Existing immutable reports were not rewritten.

## Verification limits

The parent is content-checked, not fully recalculated or authenticated as to
authorship. The selected observation and full source row are independently
verified against published evidence. The lookup does not independently resolve
candidate authorization, donor identity, report amendments, memo meaning or
cash availability. Allocation stays null and terminal eligibility stays false.

No original filing was downloaded, and no full raw COPY relation was extracted.
This gate inspects retained processed source facts; following filing references
to original disclosures is a separate operation.

## Retained evidence

Final evidence is under
`/storage/dumps/audits/fec/candidate-connection/2026-09-11/attempt-01/`.
It includes the two JSON/Markdown outputs, exact executable and build/test source
archive, drivers, initial failure evidence, final test logs, replay hashes and
explicit success markers. Duplicate replay bodies and bulk sources are not
copied into durable audit storage.

The source-current pointer remains at SHA-256
`b030668efca44fcf611b204665d95c00fa7eda9ce3ce31b5711ebbba6805e81c`.
All source and parent mounts were read-only. No resident service was started.

Next: connect the report and drilldown in a navigable investigation surface;
broader relationship coverage and terminal/allocation policy remain separate.
