# Same-report earmark memo association

Status: implemented manual Go evidence reviewer. The
[two-report source gate](../audit/receipt-report-association-2026-09-08.md)
passes. This is a reported association, not verified conduit registration,
resolved original-donor identity, or terminal-dollar allocation.

## Source meaning and scope

The FEC [conduit reporting example](https://www.fec.gov/help-candidates-and-committees/filing-reports/contributions-received-through-conduits/)
distinguishes the original contribution from the supporting conduit memo. The
FECFile instructions create that memo through a transaction split. Receipt dates
on the two entries can describe different events. A generic back-reference still
does not prove a conduit role: it can also connect other reporting components.

Policy `fec/same-report-earmark-memo-association@1.0.0` makes a conservative
structural inference from that reporting pattern. The output calls it
`reported_earmark_memo_association`, not a certified transfer or payment.
It uses exact source fields, not names, employers, ID-like memo text, amount
equality, transaction-ID suffixes, adjacent rows, or a named-committee rule.

The reviewer exhausts one exact recipient committee and `file_num` across
**every component** of the pinned Schedule A cycle. This scope is
`published_schedule_a_cycle_report`. It does not claim complete original-filing
coverage, other schedules, other source cycles, or all amendments. An absent
target means absent from this scope, not absent from the FEC's entire corpus.
The independent original-file comparison establishes broader completeness only
for its explicitly tested reports.

## Reference and role gates

1. Preserve every occurrence and all 99 physical fields. Require exact report
   scope, source ordinal, full shard digest, and unchanged inventory membership.
2. Index all transaction IDs in that scope before linking anything. An ID must
   identify exactly one source occurrence at each endpoint. Duplicate rows are
   not deduplicated to make a link possible.
3. Resolve a back-reference only with an explicit Schedule A family or exact
   target line (`SA` or `SA` plus the target's source line). Missing schedules,
   wrong lines, self-references, absent targets, and duplicate IDs remain
   distinct outcomes. Never derive a schedule from the transaction ID text.
4. Examine both reference directions. Require one distinct related record at
   each endpoint. Reciprocal references to the same pair do not duplicate it;
   fan-out, shared memos, and contradictory/incomplete incident references block
   positive association.
5. The original must be non-memo, use the existing reviewed earmark role
   (`15E`, `30E`, `31E`, `32E`), and have an `IND` or `CAN` entity label. This
   first rule does not resolve those identities or cover organization-origin
   earmarks or intermediary receipt codes `15I`/`15T`.
6. The related occurrence must be memo-only, carry `PAC`, `PTY`, or `CCM`, and
   have exact-format matching raw/clean committee IDs. A nonempty transaction
   code must also have the reviewed earmark role; conflicting economic roles
   are not overridden by the memo flag. Other entity forms remain unreviewed.
7. Any nonempty original contributor-ID or dedicated conduit-ID evidence that
   disagrees with that committee ID blocks the association. A positive result
   retains a **reported ID**, not a verified master identity. Registration,
   historical identity, and terminal eligibility require separate consumers.
8. Compare reported amounts separately: equal, different, or unknown. A source
   reference can exist despite unequal amounts. Do not repair either amount or
   assign the difference to fees, corruption, or another unsupported meaning.
   Every association adds exactly zero new money and remains terminal-ineligible.

The original [source-role annotations](./receipt-source-evidence.md), receipt
inventory, and accepted monetary predicates stay unchanged. This new output
does not silently promote an earlier unresolved contributor identity.

## Implementation and limits

```bash
legal-tender pipeline fec review-funding-report \
  --storage-root /storage --basis-result <inventory-json> \
  --committee <exact-committee-id> --file-number <exact-file-number>
```

The role/conflict/amount decision is now owned by `earmarkassociation.Decide`;
the reviewer supplies its in-memory complete-report topology. The
[cycle topology consumer](./receipt-reference-topology.md) prepares equivalent
incident guards for reference endpoints without repeating this reviewer per report.
Neither the accepted policy version nor this command's wire output changes.

The command uses the existing component shard bitmaps, four bounded workers,
and the verified receipt reader with an additional narrow file-number column.
It opens each relevant shard once and reconstructs only selected full rows.
It sorts results by source ordinal and emits one reference decision per row
plus one association decision per non-memo earmark. Output is deterministic.

A report above 10,000 selected rows fails without a successful partial output.
Cancellation, malformed physical values, or changed backing bytes also fail.
This is a bounded semantic gate, not a cycle-wide association publisher or a
query plan to repeat for every candidate/report in weekly automation.

The [wire contract](../../contracts/calculations/fec/committee-funding-basis/v1/report.schema.json)
enforces the scope, reference/association states, zero additional money,
terminal guard, and bounded full-source evidence. Go owns all runtime logic;
Python only validates schemas and retained audit artifacts. Arango, Dagster,
canonical pointers, and processed source facts remain unchanged.

## Next boundaries

The [cycle-wide conduit publisher](./receipt-conduit-publication.md) now uses the
shared policy over exact participant/reference inputs; its complete 2024 and
replay gates pass. It does not repeat this bounded reviewer per report.
The [next connected-graph milestone](./connected-funding-graph.md) is typed Arango
publication. Cross-cycle report completeness, registration
verification and broader earmark shapes remain separate gates. Do not expand
this in-memory report limit or repeat it for every report to implement them. The
[funding coverage audit](./funding-coverage-and-time.md) now identifies the
missing committee/report financial scope; its remaining time and cash gates
must pass before allocation can use a complete denominator.
