# Cycle-wide conduit qualification gate — 2026-09-12

Status: accepted and retained. Complete 2024 publication, independent corpus
checks, full same-build replay and copied-file verification pass.
The [contract](../design/receipt-conduit-publication.md) owns the evidence and
resource boundaries. This is not an Arango publication.

## Inputs and reproducible build

- Participant calculation:
  `5cf4f803465c19f8abc0f4cd3eab87b132184bc47536b6aa3c0c5753dbbe0a1e`.
- Topology calculation:
  `3192971a9cbd28696cbf911ece53b7362a5a77c7c84be4e262236dd8b1c2acca`.
- Participant manifest SHA-256:
  `fb8788e9a5cdea6c099f38dbb5be5fa41992b425c1a471c85d5343a5097d61b8`.
- Topology manifest SHA-256:
  `cdd8c803f529fa2643b85c17ff2b1b1a6de428d792fb65588f0777393e433a23`.
- Shared fact set:
  `8f69a2ef40316f62d8844ecf732342a3b4535136b434f60677d0608cb04465df`.
- Shared fact manifest SHA-256:
  `b664e752a3bae186508a45bb6d09289ba2fe06f0b4149ecbd72063e7ef79b829`.
- Application SHA-256:
  `c6c31b2a9e1016a80e98bdf467be510916f9cb04b731b7d02797ab8b6b6677be`.
- Corpus-test executable SHA-256:
  `5cc74b693b1c20db0906f8a1f76f1ca38c7311af0d56163505347b8481b224ef`.
- Source/test archive SHA-256:
  `68c6f250d23e05bd613b67b35d4bcbf62f5174be18a5464d112cc3b5dd8099b2`.
- Final source/test archive, including extra manifest rejection fixtures:
  `2b61f4236383e243afc30fe46051b89fb60848d41f562ec9b0b4cfc085b2b69a`.
  Rebuilding the application from this source gives the identical application
  hash above; no runtime behavior changed after the full run started.

Inputs are the retained [participant](./receipt-participant-index-2026-09-12.md)
and [topology](./receipt-reference-topology-2026-09-12.md) publications. There is
no new raw-source scan, download, extraction, graph write or current-pointer change.

## Local gates

Full Go tests and vet pass. Focused race tests pass. Fixtures compare the new
execution against the existing shared policy over source-shard sizes 1, 7 and
11, worker counts 1, 4 and 8, and different run sizes/fan-in. Complete decision
value digests remain identical. Cases cover forward/reciprocal references,
cross-shard peers, shared memos, invalid incoming and own references, unsupported
roles, conflicting IDs, nullable fields, negative/zero/unknown amounts and
unassessed absence. Amount equality is never a qualification gate.

Failure tests cover canceled workers, output-cap exhaustion, wrong populations,
bad peer/recipient membership, duplicate dispositions, malformed record payloads
and missing CLI identities. An applicable occurrence cannot silently disappear;
an inapplicable one cannot receive a conduit disposition. The compact wire format
preserves null versus empty source IDs and extreme signed cents in requests.

## Complete-cycle run

Working directory: `/tmp/legal-tender-conduits.kGwIF8/`.
The network-disabled runner uses read-only input, eight CPUs, a 4 GiB memory cap,
`GOMEMLIMIT=2GiB` and an 8 GiB shared workspace cap. It checks the complete
264,085,606-occurrence participant population and all sparse topology endpoints.
No success is accepted until explicit publication and corpus exit markers pass.

Calculation ID:
`1c597367db6598b0e8705e45b7b2d1cb83e6e0664f7425aa6e5677913a90ff05`.
The first complete run finished in 307.553 seconds (5m 7.553s). It preserves one
disposition for each of 33,262,189 non-memo earmark occurrences. The other
230,823,417 source appearances remain in the parent index with an explicit
not-applicable-to-this-rule disposition. They are not discarded or classified
as absent funding.

| Disposition | Occurrences |
|---|---:|
| Qualified reported earmark/memo association | 14,143,626 |
| Related record has multiple exact peers | 14,495,753 |
| Related role outside the accepted pattern | 2,072,109 |
| No reference incident; unrelated transaction uniqueness unassessed | 1,492,074 |
| Ambiguous or incomplete reference evidence | 994,041 |
| Original has multiple exact peers | 50,137 |
| Conflicting committee evidence | 14,211 |
| Original contributor role outside the accepted pattern | 189 |
| Related committee ID unresolved | 49 |

These are occurrence dispositions, not counts of unique donors, payments or
filing errors. In particular, a shared related record is not automatically a
shared conduit memo: peer-degree rejection precedes role qualification. The
accepted rule does not turn every reporting shape into a one-to-one association.
Broader association shapes require their own source-backed policy, not a bypass
for records that fail this one. All source evidence survives for that work.

The result records 16,218,964 equal-amount comparisons, 15,201,610 different-amount
comparisons and 1,841,615 unassessed comparisons across all dispositions. There
are no unknown-amount comparisons in this particular eligible population; the
unknown case is covered by fixtures. None of these counts adds a monetary total.

Output: 124,747,339 bytes. Peak workspace: 628,353,983 bytes. Publisher peak RSS:
1,683,828,736 bytes. The first runner's cgroup peak is 2,804,367,360 bytes,
including charged cache and the separate checker, within the 4 GiB cap.

The output stream's physical SHA-256 is
`6580956ba65bd1c0845e9b7cd610983fcffea3a5a96e583e1ba18496bd06296c`;
its value SHA-256 is
`8ff2967130c9da69a59f50f8ee09f9404ef1f27fb93926aca2335731e3ff2403`.

The separate read-only corpus gate passes in 26.23 seconds: every disposition
and state/amount count, all 31 expectations from the two unchanged retained
reports, and 42 data-selected witnesses covering every outcome plus periodic
source positions. Witnesses reopen exact participant and endpoint streams before
calling the shared policy; no raw-source or report-by-report cycle scan occurs.

Full same-build replay passes with four workers, 32,768-row sort runs and fan-in
three, replacing eight workers/100,000/fan-in eight. It took 458.623 seconds,
with 562,511,872 bytes peak process RSS and 628,432,141 bytes peak workspace.
Its complete calculation ID, state counts, amount-comparison counts, artifact
bytes and both physical/value hashes are identical. Only the temporary/output
filename and operational measurements differ. The replay container's memory
peak is 1,114,583,040 bytes. This deliberately varied several execution settings;
it is a reproducibility gate, not an isolated CPU-scaling benchmark.

## Retention and unchanged state

All artifacts are copied and checksum-verified at
`/storage/dumps/audits/fec/receipt-conduit-publication/2026-09-12/attempt-01/`.
`cycle/manifest.json` and `replay/manifest.json` publish the same calculation.
The directory retains both outputs, application/test binaries, byte-identical
application rebuild, source snapshots, scripts, code/corpus logs, resource
measurements and explicit zero exit markers. `SHA256SUMS` covers the copied
artifacts; `retention.exit` records success. The retained tree is about 336 MB.

No input, existing graph, older audit, source pointer or Dagster definition was
modified or deleted. Only this job's verified, workspace-owned intermediate
sort files were removed after replacement readback. Its final working artifacts
also remain in the temporary directory above.

Next: typed streaming Arango import/readback and exact committee/candidate ancestry.
This calculation must not be described as resolved donors, terminal dollar
allocation or a completed graph.
