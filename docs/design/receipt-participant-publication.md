# Cycle-wide receipt participants and report references

Status: logical publication contract for the next connected-graph slice.
The bounded sort-run benchmark and [cycle-wide reference join](./receipt-reference-join.md)
are implemented; the latter's serial corpus gate and replay pass. Its new parallel
execution also passes the real equivalence/source gate and is retained. The
[participant index](./receipt-participant-index.md) now implements source-grain
appearances and shared-policy dispositions; its full 2024 readback, artifact
replay and source-inspection gates pass and are retained. The separate
[conduit publisher](./receipt-conduit-publication.md) now passes the full 2024
publication, corpus and replay gates and is retained. The bounded
[Arango importer](./arango-receipt-participants.md) is implemented; complete-cycle
graph publication remains open. This contract follows the
[product evidence states](./product-contract.md#facts-resolution-attribution-and-context-stay-distinguishable)
and [evidence model](./evidence-model.md), not the Python collection design.
The [three-shard corpus gate](../audit/receipt-reference-index-2026-09-11.md)
passes exact field readback and replay; it does not accept a full-cycle layout.

## Source and identity

Use an exact published Schedule A fact manifest and its immutable backing.
The first corpus gate uses the existing dense, valid 2024 fact set; rejecting an
unsupported input shape must not silently discard its occurrences. No fetch,
source rewrite or selection through a mutable latest pointer belongs in this job.

A reported contributor appearance is not a resolved person or organization.
Its identity binds the fact set, source ordinal and reported role. Distinct
occurrences remain distinct even when every contributor field matches. Names,
addresses, employer text and reported IDs are evidence, not automatic identity
keys. An empty contributor description remains unresolved evidence, not an
invented person. A later identity assertion can join appearances without replacing
them or rewriting their original facts.

The retained fact owns every raw and typed field. An access index may omit fields
only when its exact occurrence locator opens that complete fact. Reported name,
employer, occupation, address, dates and memo text remain available at source grain;
omitting them from a narrow join index does not discard them from the system.
Amount and enrichment thresholds must not determine whether the underlying
reported appearance survives.

## Relationship and counting boundaries

- Preserve the recipient, contributor and conduit roles separately. A reported
  contributor relationship describes the source assertion; it is not by itself
  an effective cash payment, resolved identity or terminal classification.
- Reuse the reviewed [source-role policy](./receipt-source-evidence.md) for its
  accepted cases. Committee/individual overlap does not create a second donor
  amount. Memo, negative, zero, unknown-amount, conflicting-ID and unsupported-role
  populations retain their source facts and explicit states.
- Attach participant evidence to the existing committee/candidate authorization
  context through exact IDs and ancestry. Missing masters retain unresolved
  endpoints; authorization does not turn into a monetary transfer.
- A supported conduit association links source occurrences and their roles. It
  adds no new money and does not establish that both rows describe two payments.
- Keep reported employer associations separate from resolved employment or
  corporate identity. Neither proves corporate direction of a contribution.

Every input occurrence must have a deterministic disposition and retained
membership. Before publication, report the populations connected, kept as
evidence-only, or unsupported, with reasons. Do not claim coverage by counting
only the successfully projected subset. The new consumer must not change the
older receipt predicates, facts or accepted graph publications.

## Reference scope and resolution

The lookup key is exact `(fact set, cycle, recipient, file_num, tran_id)`.
Preserve null, empty and nonempty source strings separately; do not trim, parse
or rewrite IDs to manufacture equality. Invalid or missing report scope cannot
establish a positive same-report association.

Build reference membership from **all** occurrences in the selected cycle, not
only included receipts or rows with back-references. An unreferenced occurrence
can be a target or duplicate-ID counterexample. Keep original ordinals for every
duplicate key; never deduplicate the key into apparent uniqueness.

Resolve reference candidates only after that scope is complete. The
[same-report association contract](./receipt-report-association.md) owns the
reviewed schedule, role, uniqueness, incident-reference and conflict rules.
Its current positive earmark pattern is not a claim to cover every earmark form.
Absent targets mean absent from the declared published-cycle/report population,
not absent from all filings or from other cycles. Generic back-references are
not automatically conduit links. Amount equality is not identity evidence.

The cycle implementation must share the semantic rules with the bounded reviewer
and preserve their fixtures. It must not call that reviewer once per report or
copy its rules into a diverging classifier. Global ordering and reference joins
must account for cross-run duplicates, reverse references and fan-out before
positive associations are emitted.

The [endpoint topology consumer](./receipt-reference-topology.md) now propagates
invalid incident evidence and shares the extracted role policy with that reviewer.
It is complete for reference endpoints, not a census of unrelated transaction-key
duplicates. A later all-receipt disposition must keep unassessed uniqueness explicit
or obtain that membership; absence from the sparse index is not proof of uniqueness.

## Physical plan and bounded benchmark

The implemented `benchmark-receipt-reference-index` reads a caller-selected
shard prefix from the exact retained 99-column facts. It verifies the publication
backing, selected shard bytes, full source schema, cycle, ordinal and amount-state
consistency. It projects 19 typed/raw access fields and compares two layouts:

1. Source-order runs, preserving consecutive occurrence order.
2. The same rows sorted by recipient, file number, transaction ID and ordinal,
   with null before empty before nonempty strings and ordinal as the tie-breaker.

Both layouts use Parquet/zstd and retain every projected value. Full readback
compares each cell to the bounded input run, including nulls, false, signed
amounts and duplicate-ID occurrences. Files are created exclusively. A shared
write-time byte cap includes both layouts and partial failed writes. Failed
attempts remain inspectable; no success result is emitted on a failed gate.

The benchmark caps one sample at one million rows, one sort run at 100,000 rows,
and one invocation at 100 runs and 1 GiB of layout output. The normal output cap
is 256 MiB. It measures process peak RSS and scan/sort/write/readback time.
Timing and RSS are not part of the deterministic evidence ID; the executable,
exact input, sample range, configuration and output digests are.

```bash
legal-tender pipeline fec benchmark-receipt-reference-index \
  --storage-root /storage --schedule-a-facts <exact-manifest> \
  --cycle <cycle> --shard-index <zero-based-index> \
  --max-rows 1000000 --run-rows 100000 --output-dir <new-directory>
```

These are independent sorted runs, **not** a completed external merge, global
index, reference resolution or graph publication. A shard prefix may split a
report. It cannot prove reference absence, report completeness, global identity
cardinality, point-query performance or whole-cycle storage. Do not extrapolate
the measured compression ratio into a production acceptance claim.

## Publisher acceptance and next implementation

The new reference join implements bounded external merge over the full cycle;
its serial corpus gate and parallel equivalence pass. The largest report must not have to fit in memory;
bound merge fan-in, record sizes and disk growth explicitly. Match decisions
must be unchanged when run size or input shard boundaries change.

Require full input/disposition conservation, exact source and output readback,
cross-run ambiguity tests, deterministic replay, and measured full-cycle memory,
storage and runtime before accepting the index/participant publication. Readiness
must bind its exact fact ancestry and policy versions. Then add the typed Arango
connections and candidate-path gates from the
[connected graph plan](./connected-funding-graph.md#next-deliverable-connected-2024-contributor-evidence).
Do not enable weekly automation or terminal allocation through this benchmark.
