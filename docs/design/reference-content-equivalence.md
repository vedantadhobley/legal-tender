# Reference-content equivalence

Status: implemented in Go. Full regression, targeted race/static checks and
CM/CCL repackaging/corruption fixtures pass. The real reference proofs, complete
2024 census, selected cross-graph witnesses and byte-identical fresh replay pass
the [connection gate](../audit/reference-content-equivalence-2026-09-13.md).
This is content reuse, not provenance replacement or entity resolution.

## Why this boundary exists

The [first connection attempt](../audit/receipt-candidate-connection-2026-09-12.md)
found the same selected `cm.txt` and `ccl.txt` member hashes in different ZIP
archives. Existing occurrence identities correctly retain archive identity, so
their fact-set references differ. That does not require copying all receipt edges
into another graph, but matching metadata hashes alone cannot authorize reuse.

Source identity and content equivalence remain separate assertions. This proof
supports candidate-master, committee-master and candidate-committee-linkage inputs.
Candidate-master proof uses `fec/candidate-reference-content-equivalence@1.0.0`
for the [generation boundary](./funding-evidence-generation.md); existing CM/CCL
proof policy and output identities are unchanged.
It does not reconcile payments, infer identities, combine ledgers, or fill gaps.

## Verification

`occurrence.ProveClassicReference` takes an exact existing fact manifest and an
exact target release ID/SHA-256. It performs these checks without writing files:

1. Load immutable fact and occurrence manifests. Require the pinned source
   contract, parser, normalizer and fact schema, matching cycle/ancestry, and
   complete valid unique-row coverage. Invalid, duplicate or excluded source
   populations cannot qualify through this contract.
2. Load and hash both source-release manifests. Resolve the exact declared member
   and source archive in each release. Reject absent or ambiguous selections.
3. Rehash both complete ZIP archives. Open each selected member, reject duplicate
   ZIP names, and verify the member's size, complete SHA-256 and ZIP checksum.
   The selected member bytes must agree; archive hashes need not agree.
4. Replay the existing normalizer over each staged member. Verify its compressed
   and uncompressed bytes and the complete physical row count. Match every
   reconstructed fact to the stored artifact, including raw fields, typed fields,
   record identities, source provenance, nulls and issues. No row sampling or
   float conversion participates in this proof.
5. Compare complete ordered content fingerprints. Only occurrence-set ID,
   occurrence ID and source-release ID are omitted from that fingerprint; they
   were checked in the full original fact and remain explicit proof inputs.

The shared normalizer now accepts a streaming fact callback. Existing publication
uses that same function; proof generation does not fork parsing or normalization.
An exact physical row-count check applies to both publication and verification.

The versioned proof records both release identities and manifest hashes, both
archive hashes and sizes, the original fact and occurrence identities, schema
versions, member name/hash/size, complete row count and ordered content digest.
Its ID hashes the deterministic proof. Runtime, paths and memory settings do not
change that identity. The enclosing connection/gate also pins its executable.

Resource limits reject, never truncate: at most 64 MiB for each archive or staged
representation and 1,000,000 reference rows. These are limits for this small
reference verifier, not the size limits or scope of Schedule A/B/E ingestion.
Actual growth beyond them requires a reviewed budget change.

## Connected-graph use

`flowreconciliation.LoadReferenceContext` verifies three proofs against the
committee-flow bundle's exact release:

- Receipt graph committee-master facts against the selected member.
- Flow graph committee-master facts against that same member.
- Receipt graph linkage facts against the selected linkage member.

The two committee proofs must agree on all content/schema evidence. Their fact,
occurrence and archive identities remain separate. Linkage content is checked
against the target release directly; no synthetic replacement linkage fact set
or mutable current pointer is required.

The returned context has private fields and is bound to the exact bundle ID and
digest. A stored `passed` JSON object cannot create an accepted context. Every
fresh invocation reruns verification; proof caching/invalidation is not yet added.

`candidateupstream.RunWithReferenceWitnesses` then reuses the existing traversal
with the receipt graph's original committee and linkage facts. Its input identity
includes the three proofs. Both live graphs still undergo their original exact
readback; the flow graph retains its own newer master facts. Neither graph's
metadata, documents or publication pointers are replaced.

The ordinary upstream command and strict classic-reference loader still require
their original exact archive/member ancestry. This is a separate explicit proof
path, not a global relaxation. Existing upstream output remains unchanged when
no reference context is used.

The [connection command](./receipt-candidate-connection.md) invokes this verifier
automatically before large source scans or database reads, with no new bypass flag
or record-specific exception. Its exact shared Schedule A identity requirement
does not change. An actual reference content/schema change still stops integration.

## Verification and limits

Fixtures cover both supported reference datasets across 2020, 2022, 2024 and 2026,
different ZIPs with the same member, distinct original/target fact publications,
deterministic replay, wrong source hashes, changed content, duplicate ZIP members,
corrupt ZIP/staged bytes, forged facts with resealed artifact metadata, schema
drift, cancellation and unforgeable in-process context binding.

This proof establishes reference-content compatibility only. It does not claim
all candidate paths, terminal-dollar attribution, complete four-cycle graphs or
unattended weekly readiness. The [connected-graph plan](./connected-funding-graph.md)
still owns those wider milestones.
