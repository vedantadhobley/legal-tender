# Receiver-flow committee-master gap audit — 2026-09-01

## Result

The exact 2024 receiver-reported committee-flow graph references 707 committee
IDs that are absent from its same-release, cycle-scoped committee master. The
gap is now fully classified:

| State | Committee IDs | Share |
|---|---:|---:|
| Found in an official historical cycle master | 675 | 95.47% |
| Absent from every audited 1980–2026 master | 32 | 4.53% |
| Found only in a different 2024 release | 0 | 0% |

All 707 are source endpoints. No missing ID is a recipient endpoint, has a
same-release candidate linkage, or points to a candidate summary. The graph's
recipient vertices are therefore complete against the selected master. Its
source identity coverage is partial.

The 675 historical registrations are valid identity assertions, but they do
not silently repair the cycle-scoped graph input. The remaining 32 IDs are
reported source identifiers, not proven FEC committee identities. Neither
population is eligible for terminal-source classification until the graph has
separate historical-registration and unresolved-reported-ID states.

## Exact audited lineage

| Input | Identity |
|---|---|
| Cycle | `2024` |
| Coordinated source release | `fec-a6e6f777c3c506b100a7709815bc89f2ae84749faf4343a4589f7ec1bac066a2` |
| Receiver-flow readiness bundle | `63a87bdcb239a0da6ac4b4a3c69f9eaf42fca6be690fb2a2f6e3872cef70f827` |
| Receiver-flow calculation | `023ecf57e2baf00f93a8fdea3014fdb35b2c7305af91424111a12de74869eda6` |
| Selected 2024 committee master | `e6699bdf7174bf3865060b14e1025fa45852bac54fd50b5a4b3743d1dd97ac1d` |
| Different-release 2024 comparison | `26c2ae5089155cf134257f381060050c5bd4ef64b400bfc37b92d4f3cc16d19c` |
| Normalized 2020 master | `5827c1de31ef9abe1e22064ed21b1570db3801edea565ca677a40be869f18ecc` |
| Normalized 2022 master | `33bd92640cbcf99d8e34e53fe4a57ba122cd280add3f89e1623c20e961de7169` |
| Normalized 2026 master | `bb90044ac7df45a2cd2863f20fcd5e951a7e61ce0fe4ebe7c79a466766abdaf5` |
| Raw official cycle masters | Every even cycle from 1980 through 2018 |
| Same-release evidence | Candidate-committee linkage, all-candidates summary, current-campaigns summary |

The 20 raw archives contain 218,607 committee-master records. Each archive is
hashed and each assertion retains its archive digest, source-row number, and
source-row digest. The FEC describes the committee master as one record per
registered committee and publishes a separate archive for each two-year
period. Committee IDs remain attached to the same committee across periods.
See the official [committee-master description](https://www.fec.gov/campaign-finance-data/committee-master-file-description/)
and [bulk-data catalog](https://www.fec.gov/data/browse-data/).

The audit never uses a comparison master to mutate or backfill the selected
master. It attaches comparison assertions to each missing ID and conserves the
original graph boundary.

## Money exposure

| Population | Result groups | Receipt rows | Signed amount | Share of all graph flow |
|---|---:|---:|---:|---:|
| Complete receiver-flow graph | 180,283 | 320,731 | $4,672,820,179.49 | 100% |
| Any missing-master endpoint | 1,400 | 2,162 | $24,035,846.03 | 0.5144% |
| Missing source only | 1,400 | 2,162 | $24,035,846.03 | 0.5144% |
| Missing recipient only | 0 | 0 | $0.00 | 0% |
| Both endpoints missing | 0 | 0 | $0.00 | 0% |
| IDs absent from every audited master | — | 50 | $173,821.08 | 0.00372% |

The 675 historically registered IDs account for the remaining 2,112 exposed
receipts and $23,862,024.95. Their money is preserved. Historical registration
changes identity coverage, not the receipt amount.

## Deep source-row trace

The optional deep gate rehashed and scanned the exact Schedule A fact set:

| Field | Value |
|---|---|
| Fact set | `8f69a2ef40316f62d8844ecf732342a3b4535136b434f60677d0608cb04465df` |
| Manifest SHA-256 | `b664e752a3bae186508a45bb6d09289ba2fe06f0b4149ecbd72063e7ef79b829` |
| Physical schema | `legal-tender.fec.schedule-a-parquet.v1` |
| Facts | 264,085,606 |
| Shards | 265 |
| Runtime | 1,067.8 seconds |

It reproduced all 50 receipts and every signed cent behind the 32 unresolved
IDs. Each audit row retains the source row ordinal, raw byte range, recipient,
raw and cleaned contributor IDs, contributor name, receipt role and type,
amount, transaction ID, filing form, file number, and `SUB_ID`.

The evidence shows two useful shapes:

- Eighteen `C00300xxx` IDs name Wisconsin county Republican organizations.
  They occur in 19 `18K` receipts to one recipient and total $1,399.00. The
  IDs do not occur in any audited federal committee master. They remain
  filer-reported identifiers, not inferred federal registrations.
- Fourteen other IDs name recognizable committees or political organizations
  in 31 receipts totaling $172,422.08. Several names match a different
  official committee ID. For example, the receipt reports Smithfield HAMPAC as
  `C00035907`, while official history identifies that committee as
  `C00359075`; it reports House Majority PAC as `C00704163`, while official
  history identifies it as `C00495028`. These are evidence of bad reported
  identifiers, not authority to rewrite rows by name.

The deep trace is opt-in because the Parquet corpus has no contributor-ID
index. The normal master audit completes in seconds. `--trace-source-receipts`
adds the complete forensic scan and source-evidence conservation gate; it does
not belong on the weekly Dagster path.

## Source anomaly preserved

The 1994 official archive contains one row with an embedded NUL byte in
`CONNECTED_ORG_NM`: row 964 for committee `C00080291`. The parser emits one
`invalid_utf8` source issue, retains the exact row digest and committee
assertion, and does not replace or discard the byte. The other 218,606 raw
history records passed the classic committee-master shape checks.

## Blocking checks

The fast audit passed seven blocking checks. The deep trace passed an eighth:

1. calculation-result count and amount conservation;
2. missing-committee row conservation;
3. exhaustive classification conservation;
4. endpoint-role conservation;
5. unique missing-edge exposure conservation;
6. exhaustive linkage-presence partition;
7. comparison assertions never backfill the selected graph master; and
8. exact source-row count and signed-amount conservation for all 32 IDs absent
   from every audited master.

## Consequences

The first four consequences landed in the additive v2 identity calculation and
ArangoDB projection. The v1 graph remains unchanged. V2 preserves 675
historical registrations and 32 unresolved reported identifiers as distinct
states, retains assertion lineage, and makes all 707 terminal-identity-
ineligible. See the
[identity-coverage graph gate](./arango-receiver-flow-identity-coverage-2026-09-01.md).

The remaining next boundary is independent Schedule B sender-side evidence,
followed by Schedule A/B reconciliation without double counting.
