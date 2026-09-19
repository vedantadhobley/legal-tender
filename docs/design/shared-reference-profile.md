# Shared receipt-reference profile

`profile-shared-receipt-references` explains the population rejected by the
[conduit publisher](./receipt-conduit-publication.md) because its related record
has more than one exact peer. It is a diagnostic, not an expanded association
policy or a new money ledger.

## Question and evidence boundary

The existing rule checks related-record degree before memo roles and ID
agreement. A shared-degree rejection therefore does not establish that the
related occurrence is a conduit memo. This profile inspects the later checks
without pretending the topology is one-to-one.

FEC reporting guidance describes the original contribution and a separate memo
for the conduit, which can report an aggregate amount forwarded through that
conduit. Its examples also include unitemized contributions in a conduit total.
These are reasons to inspect group structure and preserve unequal amounts, not
to assume every shared reference is valid or infer an unitemized residual.
See [FEC earmarked-contribution examples](https://www.fec.gov/updates/earmarked-contributions/)
and [contributions received through conduits](https://www.fec.gov/help-candidates-and-committees/filing-reports/contributions-received-through-conduits/).

## Implemented analysis

The Go command replays the existing bounded participant/topology join. Its
association decisions, amount comparisons and complete membership checks stay
unchanged. `earmarkassociation.InspectRoles` exposes the same role/ID checks
used by the accepted one-to-one rule; it returns role compatibility, not an
association decision or permission to allocate money.

For each shared related occurrence, the profile counts:

- All applicable sole-peer requests, including those excluded for unsafe
  original references, and the exact distinct-peer count from the topology.
- The subset rejected specifically for shared degree, with role/ID outcomes.
- Known, missing, positive, negative and zero original amounts. Signed sums
  use arbitrary precision; null is not zero.
- The related amount once per group, separately from original amounts.
- Whether the sum of the shared-rejected originals equals the related amount.
  Missing amounts produce an unknown comparison; equality is not eligibility.

`complete_safe_earmark_leaf_coverage` means every exact peer is represented by
a distinct safe, non-memo earmark original whose only exact peer is this related
occurrence. The existing dense participant scan and source-ordered decision
verification establish unique original membership. Reciprocal references are
already deduplicated by the parent topology. A subset cannot be labeled complete.

`other_peers_not_characterized` retains the remaining exact-peer count. It does
not guess those peers' roles or claim they are invalid. It includes unsafe
originals, non-earmark peers, and peers with more than one neighbor. This profile
does not follow their complete adjacency; that requires the retained reference
incidences, not just the endpoint summary.

Peer coverage, all/mixed/no-compatible roles, and same/different/unknown amount
comparison form separate group dimensions. Each structural class retains the
lowest related source ordinal and its first original for each role outcome.
These examples are selected by code, never by candidate, committee name, amount
threshold or known source ordinal. Exact fact/participant ancestry locates the
complete source rows for later drilldown.

## Reproduction and resource bounds

```text
legal-tender pipeline fec profile-shared-receipt-references
  --participant-manifest PARTICIPANTS/manifest.json --expected-participant-id ID
  --topology-manifest TOPOLOGY/manifest.json --expected-topology-id ID
  --output-dir NEW --workers 8 --run-rows 100000 --merge-fan-in 8
  --max-workspace-bytes 8589934592
```

The exact inputs must cover the same complete fact set. Every parent shard and
endpoint stream receives the publisher's existing hash/census verification.
The output contains the unchanged decision publication plus
`shared-reference-profile.json`. A missing profile file is not profile success,
even if the baseline calculation finished. Each metadata file is capped at
1 MiB; profile mode reserves 2 MiB beyond the workspace cap.

The profile ID binds the baseline calculation ID, analysis policy, all class
counts, exact sums and witnesses. It excludes worker tuning, paths and runtime
metrics. There is no cycle-specific matching code. The cycle scopes the input
publication, not the interpretation rule.

Only a fixed set of structural classes and bounded witness records stays in
memory. A large equal-key group is streamed, not materialized. The existing
one-to-eight workers, bounded merge runs and shared write-time disk cap apply.
Use the existing transient eight-CPU/4 GiB container budget with a 2 GiB Go heap
limit. No standing service or memory budget changes.

The [runner](../../scripts/run-shared-reference-profile.sh) retains the binary,
input manifests, output, logs, checksums and an explicit exit marker in a new job
directory. Mount source storage read-only and only that dated analysis parent
writable. It does not fetch data, use a database, change current pointers or
allocate terminal dollars. The [dated result](../audit/shared-reference-profile-2026-09-14.md)
records the real run and its limitations.

## Next boundary

The [automatic original-source review and additive group rule](./shared-reference-group-rule.md)
now follow each selected group's complete exact-peer neighborhood and preserve
the original facts. The one-to-one profile remains unchanged. The separate rule
supports complete, safe, role-compatible groups. Its full-cycle publication and
[isolated graph extension](./shared-conduit-generation.md) now pass the
[2024 gate](../audit/shared-conduit-publication-2026-09-14.md). Extension-aware
query readers are next. Role-compatible profile rows are not added edges merely
because this diagnostic ran.
