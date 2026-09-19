# Schedule B release-v3 and columnar publication audit

> **Scope:** Coordinated FEC release v3, archive-direct processed Schedule B,
> and complete lossless 2024 Parquet publication and replay.  
> **Disposition:** The physical source and fact boundary passes. Schedule B is
> accepted as an independent sender-reported source. No effective-record,
> outgoing-flow, reconciliation, aggregate, or graph policy is accepted here.

## Result

Active release-inventory v3 adds the official processed Schedule B archive and
four selected two-year relations without changing v1 or v2. Schedule B uses
`archive_direct`, so release staging remains at 25 outputs and does not retain
the 114.82 GiB 2024 COPY stream.

The complete 2024 publisher conserved all 157,544,163 physical rows as
157,544,163 lossless facts. Every row was valid, every `SUB_ID` was unique, all
158 Parquet shards passed full-schema readback, and the canonical replay was
byte-identical. The accepted source contract is
[`fec/schedule-b@1.0.0`](../../contracts/sources/fec/schedule-b/v1/).

## Coordinated release

| Evidence | Result |
|---|---|
| Inventory | `legal-tender.fec.initial-release-inventory.v3`; 23 artifacts |
| Active release | `fec-136903645ee7c7050d463a4f779a051a454ebb87308fd854faabc5ca47a20cdf` |
| Release manifest SHA-256 | `a28d56c7a3be024ed7cd85e2bf0ead21c982c96d00b683ef364c0a37eb9bc3ec` |
| Prior release | v2 `fec-76b6660f70406bf0d11537885de172883cdb8af548bfa7df35078a721c8c759a` |
| Schedule B artifact | 39,310,353,867 bytes; SHA-256 `39669f3c6c19d6f5076648f734f6456e0f8852d6cf37f0133e283fbfce91ac72` |
| Publisher object | version `JARG_mL4APJoVPy8FUvwiTILidolHqfY`; modified 2026-08-30 15:21:54 UTC |
| 2024 relation | `disclosure.fec_fitem_sched_b_2023_2024`; 81 fields; `archive_direct` |

Metadata-only discovery observed all 23 sources in 1.344 seconds. Relative to
the retained v2 release, the plan selected 18 changed objects and reused five;
Schedule B was the only newly required fact family. The exact retained
Schedule B object already matched the current publisher version, length, and
digest, so the manual migration promoted those local bytes without another
39.31 GB body transfer. Acquisition validated 18 selected artifacts and reused
five in 31.703 seconds.

Staging published 17 changed outputs and reused eight, for the contracted 25
outputs. Schedule B added no staged output. A same-discovery plan against the
published v3 manifest returned `no_change`, with all 23 sources reused and no
authorized body request.

## Complete 2024 fact gate

| Measure | Result |
|---|---:|
| Source rows / facts / valid facts | 157,544,163 / 157,544,163 / 157,544,163 |
| Invalid rows / duplicate `SUB_ID`s | 0 / 0 |
| Unique `SUB_ID`s | 157,544,163 |
| COPY bytes | 123,284,784,602 (114.82 GiB), streamed only |
| Parquet shards / row groups | 158 / 1,261 |
| Parquet bytes | 7,862,059,821 (7.32 GiB) |
| COPY-to-Parquet byte ratio | 15.68:1 |
| Full publication wall time | 4,195.543 seconds (1h 09m 55.543s) |
| End-to-end source rows per second | 37,550 |
| Sampled container memory | about 457 MiB of a 16 GiB cap |
| Free storage after publication | about 1.4 TiB |

The 98-column schema preserves all 81 decoded source lexemes, one-based row
ordinal, raw byte offset and length, exact-cent money projections with source
scale and state, local timestamps and dates, report and two-year periods, and
the policy-free memo-X flag. Source lexemes remain authoritative. The fact
publisher does not choose which filing action is effective and does not infer
who received an economic flow.

Exact replay identities:

| Identity | SHA-256 |
|---|---|
| 2024 COPY bytes | `9ee6eda0ce36d9e3bebbfa9b7d4908456300103ebd392716b3608a300110e239` |
| Fact semantic stream | `79323ffe464eec9aa9ef7285376f01de131be737f0da71873d4c61b6b6b3d5d1` |
| Canonical fact set | `aa025a0d06c303562d8d3de7975cc203d897c77217d9b149ade7dc033369af4c` |
| Canonical manifest bytes | `d977930d4b85d623c797a6cf5a251ffd7f105d315d972b54cd3e07ad86f0c9fb` |

All eight blocking checks passed: release lineage, archive integrity, COPY
replay, source-row conservation, strict source validity, submission identity,
Parquet round trip, and immutable shards. Both canonical outputs validated
against the checked-in JSON Schema. The active pointer, immutable manifest,
adoption result, and replay result are byte-identical.

## Incremental behavior and replay

The initial writer used a release-bound provisional fact ID. Review during the
corpus run found that this would rebuild Schedule B whenever an unrelated FEC
source changed. The accepted identity instead binds the Schedule B artifact,
relation, physical schema, publisher version, and shard configuration.

The corrected publisher adopted the 158 verified shards under the canonical
identity in 8.9 seconds without invoking `pg_restore`. A second digest-verified
replay completed in 8.2 seconds and emitted the exact same manifest bytes. A
descendant release that reuses the Schedule B artifact now follows this fast
path. A changed Schedule B artifact or versioned schema/publisher configuration
still forces complete source replay.

Canonical files:

```text
/storage/facts/fec/schedule-b/columnar/current/2024.json
/storage/facts/fec/schedule-b/columnar/manifests/aa025a0d06c303562d8d3de7975cc203d897c77217d9b149ade7dc033369af4c.json
/storage/dumps/audits/fec/schedule-b-columnar/2026-09-03/2024/run-2/result.json
/storage/dumps/audits/fec/schedule-b-columnar/2026-09-03/2024/run-3/result.json
```

## Manual control-artifact caveat

Two manual stage containers were accidentally directed to the same convenience
stdout file. The successful retry's exact stage bytes existed and were
validated when release publication advanced, but the older canceled process
later overwrote that file. The active manifest still embeds all 25 exact output
descriptors and names the consumed stage SHA-256
`84156d7c692c564e3274e59ee7f47e5125733df60d26f04746ff47a869c19a67`;
that standalone byte stream is no longer retained.

An independently regenerated valid stage result is preserved under SHA-256
`3aca399d465cfdb0f7afb70733e05f11228693d43c3de009164108e0e376cd6c`.
Its candidate, plan, acquisition, and all 25 output descriptors exactly match
the active release. This is an operational control-retention defect, not a
source-row or fact-integrity difference.

The direct `publish-release` CLI now copies the exact plan, acquisition, and
stage input bytes into immutable content-addressed control paths before it can
advance the active pointer. Tests cover idempotence and collision rejection.
The next changed v3 release should explicitly confirm that the referenced
stage digest exists at its canonical control path.

## Consequences

1. Promote `fec/schedule-b@1.0.0` from `draft` to `accepted` for preserved
   sender-reported observations.
2. Keep raw Schedule B facts outside ArangoDB. Later calculations and graph
   projections reference their fact-set and row identities.
3. Define effective-record membership before reporting Schedule B spending.
4. Define outgoing-flow roles before claiming committee transfer coverage.
5. Reconcile Schedule A and B only after both policies exist, and never add
   the two assertions as separate dollars for one economic-flow hypothesis.
