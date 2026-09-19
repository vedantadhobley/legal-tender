# Receiver-reported committee-flow publication — 2026-09-01

## Result

The accepted receiver-reported committee-flow policy now has an immutable Go
publisher, verified loader, CLI, JSON contracts, and a minimal Dagster asset.
The complete 2024 publication conserved all 264,085,606 Schedule A facts and
every known signed cent. An unchanged replay returned the byte-identical
manifest and reused both content-addressed artifacts.

This publication is receiver-side evidence. It does not reconcile Schedule B,
project graph edges, infer terminal sources, or count outside spending as a
candidate-controlled receipt.

## Exact lineage and identity

| Field | Value |
|---|---|
| Cycle | `2024` |
| Source release | `fec-a6e6f777c3c506b100a7709815bc89f2ae84749faf4343a4589f7ec1bac066a2` |
| Schedule A fact set | `8f69a2ef40316f62d8844ecf732342a3b4535136b434f60677d0608cb04465df` |
| Input manifest SHA-256 | `b664e752a3bae186508a45bb6d09289ba2fe06f0b4149ecbd72063e7ef79b829` |
| Physical schema | `legal-tender.fec.schedule-a-parquet.v1` |
| Calculation set | `023ecf57e2baf00f93a8fdea3014fdb35b2c7305af91424111a12de74869eda6` |
| Policy | `legal-tender.fec.receiver-reported-committee-flow-policy.v1` |
| Publisher | `legal-tender.fec.receiver-reported-committee-flow-publisher.v1` |
| Immutable and current manifest SHA-256 | `f7fd6e888593555b17947a49a9f7d881e78997cb7cc363606b55f78be6287453` |
| Run ID | `receiver-flow-2024-v1` |
| Published at | `2026-09-01T04:06:19.855171349Z` |

The calculation-set identity binds the exact fact-set ID, manifest digest,
physical schema, calculation contract, policy, publisher, result schema, and
exception schema. The current pointer and immutable manifest were byte equal.

## Terminal decisions

Every source fact entered exactly one ordered terminal state.

| Decision | Rows |
|---|---:|
| Included receiver-reported committee flow | 320,731 |
| Excluded: no exact source committee ID | 206,869,946 |
| Excluded: memo subtotal | 16,211,187 |
| Excluded: outbound receipt role | 7,762,772 |
| Excluded: semantic-memo receipt role | 271 |
| Excluded: earmarked receipt role | 32,123,678 |
| Excluded: noncommittee receipt role | 2,900 |
| Unresolved: one-sided source committee ID | 2,101 |
| Unresolved: amount | 2 |
| Unresolved: receipt role | 792,018 |
| Invalid normalization | 0 |
| Unresolved recipient committee ID | 0 |
| Conflicting source committee IDs | 0 |
| **Source facts** | **264,085,606** |

The included rows form 180,283 source-recipient-role results over 7,556 source
committees and 3,914 receiving committees. They contain 319,976 positive, 687
negative, and 68 zero rows. No included row or result is a self-edge.

## Exact money conservation

| Disposition | Signed amount |
|---|---:|
| Included | $4,672,820,179.49 |
| Excluded | $48,378,169,804.83 |
| Unresolved known amounts | $358,167,217.32 |
| **Known source amount** | **$53,409,157,201.64** |

Two source rows have no amount observation. All other amounts conserve as
exact signed integer cents. Grouped result amounts equal the included total.

## Artifacts

| Artifact | Records | Uncompressed bytes | Compressed bytes | Compressed SHA-256 |
|---|---:|---:|---:|---|
| Sparse unresolved exceptions | 794,121 | 443,026,376 | 39,068,683 | `008a610488c5bd1840eb3f1cbd576739b69fb6ae555736d813a8f5a26ceae4ff` |
| Grouped results | 180,283 | 86,892,849 | 8,525,706 | `2d927b9752e146530fc75a166b1e9017f9155c00ca28ddd45fe629ad872885da` |

Ordinary included and excluded membership is not copied into a dense decision
artifact. It is reproducible from the exact Schedule A fact set and the
predicate embedded in the manifest. Only unresolved decisions are
materialized separately.

## Runtime, replay, and checks

The first 16-worker publication took about 106.87 seconds, including input
verification, the complete scan, artifact publication, and manifest writes.
The unchanged replay took about 8.83 seconds and returned the same manifest
bytes. It verified the existing current manifest and both backing artifact
digests without rescanning the corpus.

All nine manifest checks passed. Eight are blocking: input lineage, decision
conservation, amount-observation conservation, signed conservation, exception
conservation, grouped-result conservation, result-sign conservation, and the
absence of a dense decision artifact. Helper-field neutrality remains an
observed invariant: names, entity type, individual classification, and
committee masters do not decide membership.

The emitted manifest passed the checked-in Draft 2020-12 JSON Schema. Go tests
cover real Parquet scanning, terminal decisions, sparse exceptions, grouped
results, exact signed conservation, immutable replay, and policy-to-manifest
rule equality. Dagster tests verify the partitioned asset invocation and full
definition loading.

## Next gate

Publish an exact-input projection-readiness bundle with same-cycle committee
masters, then project these grouped results into a new content-addressed
ArangoDB graph. The graph gate must preserve the calculation lineage, expose
unresolved coverage, read every count and signed amount back, and benchmark
multi-hop paths and cycles before terminal-source tracing begins.
