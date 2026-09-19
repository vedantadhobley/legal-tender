# Complete shared-memo group associations

Status: additive Go rule, automatic original-source reviewer and complete compact
publication implemented. The [full-cycle gate](../audit/shared-conduit-publication-2026-09-14.md)
owns publication and replay status.
The [selected 2024 source review](../audit/shared-reference-source-review-2026-09-14.md)
owns the selected original-source evidence. Neither calculation changes financial
selection. The separate [graph extension](./shared-conduit-generation.md) avoids
copying or changing the existing receipt graph.

## Source basis and limits

The [shared-reference profile](./shared-reference-profile.md) showed that many
one-to-one-rule exclusions have compatible source roles but share a related
record. Original-source review now distinguishes aggregate conduit memos from
non-memo records, mixed roles, reference chains and conflicting IDs.

FEC guidance describes separate original-contributor records and conduit memo
totals, including examples with unitemized contributions in the memo total.
Consequently, unequal original and memo amounts need not disqualify a reported
association. This is not evidence that any particular difference is unitemized
money, fees or missing donations. See [FEC reporting examples](https://www.fec.gov/updates/earmarked-contributions/)
and [contributions received through conduits](https://www.fec.gov/help-candidates-and-committees/filing-reports/contributions-received-through-conduits/).

The implementation does not match names or memo-text phrases. Those fields
remain source evidence for review. It does not repair conflicting IDs, infer
employment/ownership, resolve an unregistered organization or determine legal
conduit status from an entity code.

## Versioned rule

`fec/complete-earmark-memo-star-association@1.0.0` is separate from the unchanged
`fec/same-report-earmark-memo-association@1.0.0` one-to-one rule. A new positive
decision is `reported_shared_earmark_memo_association`.

Its caller must supply a complete, exact same-report reference neighborhood:
identical fact ancestry, recipient and report/file scope; exact transaction and
schedule resolution; unique source and target transaction keys; reciprocal
deduplication; and invalid-incident safety from the complete reference topology.
Source ordinals are occurrence locators, not matching heuristics.

The group rule requires:

1. One shared related occurrence with at least two distinct exact peers.
2. Every peer present exactly once in increasing source-ordinal order.
3. No unsafe incident at the shared record or any peer.
4. Every peer has the shared record as its sole exact peer.
5. Every peer passes the existing role/ID inspection: non-memo reviewed earmark,
   `IND`/`CAN` original role; memo `PAC`/`PTY`/`CCM` related role; permitted related
   receipt type; matching valid raw/clean related committee IDs; and no conflict
   with original raw/clean/conduit or related conduit IDs.

This is a deliberately limited assertion for a complete, uniform group. A
partial or mixed group is unresolved under this version, not invalid money.
One conflicting peer prevents this whole-group assertion; the compatible peers
remain available for a later independently justified per-member rule.

Decisions preserve incomplete coverage, unsafe incidents, non-leaf peers,
conflicting IDs and unsupported roles separately, with role counts. The
accumulator has bounded state independent of group size and rejects duplicate,
out-of-order, wrong-sole-peer and excess members. It does not retain all peers.

No amount comparison, sign, monetary threshold, committee identity, memo text,
transaction suffix or same-date condition determines eligibility. The association
adds zero money and stays terminal-ineligible. It attaches a reported conduit
identifier to original occurrences without creating an additional payment.

## Automatic source review

`review-shared-receipt-references` takes the immutable profile and its expected
identity. It selects the lowest-related-ordinal witness from every profile class,
then:

1. Verifies the complete reference incidence stream and retains every exact peer
   of each selected group, including peers omitted by the profile's earmark subset.
2. Verifies the complete endpoint stream, exact/unsafe census and selected topology.
3. Verifies the original fact manifest and complete backing through the existing
   loader; it never substitutes a newer release or mutable current input.
4. Batch-reads selected full source rows, rehashing each touched shard once.
   Full-row reconstruction is shared with the existing receipt reader. The typed
   receipt classification is checked against those reconstructed fields.
5. Rechecks literal recipient, report, transaction and schedule relationships,
   reference directions, original role evidence and the prior profile's selected
   group counts, sums and examples. Established unique-key multiplicities come
   from exact incidence membership, not an assumption based on the sample.
6. Emits unchanged one-to-one dispositions, all-peer role coverage and the separate
   group-rule result alongside complete source rows and exact parent identities.

The reviewer retains at most 4,096 selected source occurrences, counts roots in
that bound, and rejects an oversized group selection without truncation. CLI
output is capped at 32 MiB. These are review limits, not production group limits;
do not run this reviewer per candidate or turn it into a whole-cycle processor.

The review covers each selected root's immediate exact neighbors. It exposes
their full degree and safety but does not recursively load the other neighbors
of a non-leaf peer. It does not establish complete report-body or economic-payment
coverage. Original dates are preserved even when they fall outside the nominal
two-year acquisition partition.

```text
legal-tender pipeline fec review-shared-receipt-references
  --storage-root ROOT --schedule-a-facts EXACT_FACT_MANIFEST.json
  --profile PROFILE/shared-reference-profile.json --expected-profile-id ID
  --topology-manifest TOPOLOGY/manifest.json
  --reference-manifest REFERENCES/manifest.json
```

The [offline runner](../../scripts/run-shared-reference-review.sh) retains the
binary, input manifests, command arguments, results, checksums and explicit exit
marker. Existing inputs are read-only. The transient container budget remains
eight CPUs, 4 GiB memory/no-swap and a 2 GiB Go heap limit; it is not a standing
service or a claim that all serial stream verification uses eight cores.

## Publication boundary

`publish-shared-receipt-conduit-associations` produces
`legal-tender.fec.receipt-conduit-associations.v2`, under publication policy
`fec/cycle-receipt-conduit-evidence@2.0.0`. It requires the exact accepted v1
baseline manifest/ID as well as complete participant/topology inputs. The normal
v1 command and its positive rule remain unchanged.

The publisher first reproduces every old decision and checks its entire canonical
value stream, state counts and amount-comparison counts against the accepted
baseline. It also reads the accepted physical artifact to EOF. Only then does it
apply the separate group rule:

1. Stream exact sole-peer requests in shared-root/source-ordinal order. Requests
   include unsafe originals; they are not silently removed to obtain a safe group.
2. Accumulate roles and safety with bounded state. Compare the distinct request
   count to the complete endpoint degree. Equality proves every exact peer is
   present. A smaller count stays incomplete; omitted peers are not assigned roles.
3. Retain one group decision for every root with an old shared-degree rejection.
   This scope excludes roots whose old decisions were entirely unsafe. Partial
   groups expose their uncharacterized peer count, not the full reviewer's reasons
   for neighbors that this compact request stream does not contain.
4. Read requests again and emit changes only for positively qualified complete
   groups. External sorting restores source order without buffering a whole group.
5. Merge changes with the reproduced baseline. A change must refer to an old
   shared-degree rejection and preserve its ordinal, related ordinal and amount
   comparison. Copy every other record verbatim. Recheck full disposition counts
   and hashes before the immutable manifest is written.

The output retains a complete updated decision stream and a compact group-decision
stream. Its identity pins both policies, the exact old baseline, source parents,
build, logical hashes and counts. Physical filenames, compression layout, worker
count and timing do not change its logical identity. The shared workspace remains
bounded, with the same one-to-eight workers and external-sort controls as v1.

`ReadAdditions` is the graph consumer boundary. It reads both complete decision
streams and checks that every difference is one permitted upgrade. Every upgrade
must match a qualified group's root, committee ID and exact member count. Its
qualified-root lookup is capped at 100,000 roots; it never stores member lists.
Exceeding that resource bound fails rather than dropping data.

```text
legal-tender pipeline fec publish-shared-receipt-conduit-associations
  --participant-manifest PARTICIPANTS/manifest.json --expected-participant-id ID
  --topology-manifest TOPOLOGY/manifest.json --expected-topology-id ID
  --baseline-manifest OLD_CONDUITS/manifest.json --expected-baseline-id ID
  --output-dir NEW --workers 8 --run-rows 100000 --merge-fan-in 8
  --max-workspace-bytes 8589934592
```

The [offline runner](../../scripts/run-shared-conduit-publication.sh) retains
binary, input manifests, arguments, output artifacts, checksums and an explicit
exit marker. No source download, current-pointer change, financial selection or
terminal classification is part of this publication.
