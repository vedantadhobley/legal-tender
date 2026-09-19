# Pre-attribution checkpoint proposal — 2026-09-14

Status: selected review evidence pinned and rechecked; **not a completed
raw-to-graph recovery checkpoint and not user acceptance**. The
[interpretation review](./pre-attribution-review-2026-09-14.md) explains what
the current transformations mean. No new baseline pointer, snapshot, data copy,
export, download or graph write was performed.

Follow-up: the [typed inventory audit](./funding-recovery-inventory-2026-09-15.md)
now replaces manual enumeration for the selected shared-conduit generation.
Its exact missing historical staging record, unverified runtime requirements
and default presence/size-only blob checks keep recovery acceptance open.

## Exact subject

Generation:
`35d249388ad51d1b03a98324e2f096bc20e945a35d2a38206b12de7702e1d9cd`.
Generation result bytes:
`1a3a48c7a9c58337a626999cdce0531a938d03262029ccc5b4f30dd67c9e9cfe`.
The [generation gate](./funding-evidence-generation-2026-09-13.md) owns its
original execution evidence, family populations and readback scope.

Window spending gate:
`c004513e02437a71815c605d124c715146c7d379fcb52bed9a9c1d2a825da404`.
Result/replay bytes:
`972103e4063c4171f890e0c75b1415c3695ed7355ef04695235cfb4e802c2146`.
The [window gate](./funding-window-spending-2026-09-14.md) owns the complete
source census and exact query requests. Its input file pins one 2024 generation;
it is not a four-cycle or latest-current snapshot. Each saved case records its
own date field, date window, ledger, source entry and traversal bounds.

## Machine-checkable evidence pins

The checked-in [SHA256SUMS](./fixtures/pre-attribution-2026-09-14/SHA256SUMS)
uses paths relative to the storage root. It pins:

- Generation and window outputs, exact window input specification, and both
  retained producer/consumer executable and source snapshots.
- Receipt graph, participant/conduit/topology, A/B and E readiness manifests.
- The three original/coordinated release manifests, A/B/E facts, A/E occurrence
  manifests and the six CN/CM/CCL master/linkage contexts used by this generation.
- A/B reconciliation, E resolution/aggregation manifests, and the small E
  source-fact and resolution-decision artifacts.

The literal IDs in this inventory identify historical evidence. They are not
runtime matching rules or default-cycle settings. Another generation needs a
new inventory; it must not replace these pins or consult `current.json` on replay.

Inside a network-disabled container with `/storage` and the repo mounted
read-only, the check is ordinary checksum verification:

```bash
cd /storage
sha256sum --check /repo/docs/audit/fixtures/pre-attribution-2026-09-14/SHA256SUMS
```

This uses the standard checksum tool rather than adding a duplicate application
command. It fails on absent, unreadable or changed listed files. It does not
validate unlisted dependencies or turn a saved successful result into proof
about today's live database.

Fresh verification on this date checked every listed file successfully in a
read-only, network-disabled, two-CPU container capped at 256 MiB with no swap
allowance. It also rechecked the retained generation/window checksum inventories,
zero success markers and byte-identical result/replay pairs. Those are checks of
saved evidence, not a new execution of all prior corpus gates.

The current source change adds resolver characterization tests only. The prior
source snapshots remain exact evidence for their earlier builds; the new tests
are not retroactively included in those archives. No runtime behavior changed.

## Large-source availability is a weaker, separate check

The exact archive paths named by the historical source identities still exist:

| Archive | Observed file bytes | Source SHA-256 identity |
|---|---:|---|
| Schedule A | 90,175,845,817 | `35974c29037cf502752c0d961361aa15e77e674e2fdeee53006a8509f62804fb` |
| Schedule B | 39,310,353,867 | `39669f3c6c19d6f5076648f734f6456e0f8852d6cf37f0133e283fbfce91ac72` |
| Schedule E | 43,384,475 | `506abf832b98bfd5e366413a9d31ccd8fc1947aabd22130f77c123b5fa30996f` |

Paths use `raw/fec/schedule-{a,b,e}/artifacts/sha256/<first-two>/<digest>`
beneath storage. These were file-stat checks only. This review did **not** rehash
the three archives, rescan the large A/B Parquet sets or independently reconstruct
the source from them. The checks do not prove raw bytes are uncorrupted. No
replacement source needs to be downloaded merely because the old archive is old.

## Requirements before calling this a recovery baseline

1. Enumerate the complete typed-manifest dependency graph for the selected
   generation: every source release/member, occurrence/index shard, fact shard,
   calculation artifact, participant/reference/conduit artifact, master fact and
   graph-build input. Fail on unknown manifest versions. Do not use a heuristic
   search for JSON strings that look like paths.
2. Distinguish full checksum verification, size/presence checks and prior
   attestations per dependency. Report absent or unreadable backing. No claim of
   complete closure follows from the smaller review inventory above.
3. Retain exact executables/source, build commands, Go/module/toolchain inputs,
   extraction tools, runtime/Arango image identities, schema contracts and
   resource settings needed for replay. The two pinned source snapshots are not
   by themselves a verified offline build environment for every producer.
4. Define and enforce retention for that dependency closure. A checksum file
   does not prevent a cleanup job from deleting referenced blobs. No existing
   cleanup/retention behavior was changed by this review.
5. Run a separate isolated rebuild gate from the declared recovery layer. State
   whether it starts from raw bytes, facts or calculated evidence. Compare
   logical identities, complete counts/values and source drilldown; distinguish
   meaningful output from run timestamps/operational paths. A graph export or
   physical backup, if wanted, requires its own scope and verification.
6. Record the user's accepted interpretation decisions separately from passing
   software checks. Neither implies terminal identity or dollar eligibility.

Until then, the accurate description is **pinned review evidence over an
already replay-tested generation**, not “everything is backed up” or “clean
raw-to-graph restoration has passed.” The typed-inventory follow-up above now
identifies the next exact metadata gap; another full historical download is not
the next step.
