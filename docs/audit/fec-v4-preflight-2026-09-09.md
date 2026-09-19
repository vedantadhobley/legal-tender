# Fresh v4 discovery and storage preflight — 2026-09-09

Fresh metadata and storage review pass after the
[streaming-storage gate](./fec-streaming-storage-2026-09-09.md). This is a
pre-acquisition check, not a source release. No source bodies were downloaded,
no source files were deleted, and no acquisition, staging, publication, graph,
or scheduling operation ran. Active/default v3 remains unchanged.

## Publisher observations

Two complete HEAD passes ran at 21:19:21–21:19:24 UTC. Every configured v4
source was available. Both passes returned identical versions, lengths, ETags,
modification times, and final URLs. Their observation timestamps correctly
differ. Both plans select the same candidate as the earlier review.

There are 24 changed/new inputs and three reused inputs: the 2020 `cn`, `cm`,
and `ccl` artifacts. The four committee-summary CSVs are new release members;
the other 20 changes are publisher-version changes to existing members. No
changed-record count can be inferred from HEAD metadata.

| Changed/new inputs | Download bytes |
|---|---:|
| Processed Schedule A | 90,173,582,284 |
| Processed Schedule B | 39,313,075,207 |
| Processed Schedule E | 43,387,186 |
| Four committee-summary CSVs | 31,119,037 |
| Other classic inputs | 5,115,878 |
| Total | 129,566,279,592 |

The total is **129.57 GB / 120.67 GiB before extraction**. No candidate partial
exists, so there is no resume credit. No download-duration estimate was measured.

## Storage result

The existing Schedule A tree still contains 297,015,247,494 unique logical
bytes. The streaming prior-size scenario projects 472,033,191,548 hot bytes
including margin and the non-A temporary peak: **439.62 GiB against 600 GiB**.
That leaves about 160.38 GiB of modeled hot headroom.

The filesystem had 1,773,223,440,384 available bytes at inspection. The complete
scenario requires 751,286,518,529 available bytes, including all remaining
downloads, assumed new retained outputs, the 500 GiB floor, and the 25 GiB
margin. Both acquisition and full-output scenario checks pass.

Prior output sizes are estimates, not bounds on new data. Runtime guards must
enforce actual growth. These figures cover source acquisition/staging, not
downstream fact, calculation, or graph materialization. Cold retention remains
required before perpetual refresh automation.

## Verification and preserved evidence

The current Go CLI was built offline. The metadata probe mounted source storage
read-only and used no credentials. Independent verification passes all source
membership, schema, version selection, repeat-observation, and download/partial
arithmetic checks. All 23 prior source artifacts pass size and digest-basename
checks; this probe did not reread their large bodies. The storage schema,
independent inode/peak arithmetic, and unchanged-pointer tests pass (three tests).

The active manifest equals its immutable backing and the captured baseline:
`fec-136903645ee7c7050d463a4f779a051a454ebb87308fd854faabc5ca47a20cdf`,
SHA-256 `a28d56c7a3be024ed7cd85e2bf0ead21c982c96d00b683ef364c0a37eb9bc3ec`.
The candidate remains unpublished:
`fec-01f93a786b630be40a932543f35251bcb56c42b32d1ae85ac34a101ddc7b56b2`.

Evidence is retained under `dumps/audits/fec/v4-preflight/2026-09-09/`: the
baseline, both discoveries/plans, v2 storage review, inode inventory,
before/after pointer hashes, independent verification, scripts, CLI binary,
test logs, and explicit exit markers. Earlier audits remain unchanged.

Follow-up: the user approved the [acquisition](./fec-v4-acquisition-2026-09-09.md),
which completed separately with restricted write mounts, zero exit markers,
and passing independent verification. This preflight itself downloaded no
source bodies. Acquisition checked response identities and post-capture
publisher versions; the two preflight HEAD passes alone did not guarantee later
responses. Staging, selected-row integrity checks,
coordinated publication, summary publication, and explicit v4 activation follow
as separate gates. Do not enable weekly acquisition or delete retained evidence
as part of that approval.
