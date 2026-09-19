# Federal Election Commission contracts

These versioned contracts pin the FEC source boundary for the Go rewrite.
They preserve publisher grain and keep acquisition period, transaction time,
reporting time, election time, processing time, and Legal Tender snapshot time
independent.

| Dataset | Contract | Fixture evidence |
|---|---|---|
| Candidate master | [`candidate-master/v1/`](./candidate-master/v1/) | Exact 2024 `cn.txt` rows mapped to canonical JSON. |
| Committee master | [`committee-master/v1/`](./committee-master/v1/) | Exact 2024 `cm.txt` rows, including connected-organization text and source-empty codes. |
| Candidate-committee linkage | [`candidate-committee-linkage/v1/`](./candidate-committee-linkage/v1/) | Exact 2024 `ccl.txt` rows with equal and differing candidate/FEC election years. |
| All-candidates summary | [`all-candidates-summary/v1/`](./all-candidates-summary/v1/) | Exact 2024 `weball24.txt` rows with distinct coverage dates and a negative value. |
| Current House and Senate campaigns | [`current-campaigns-summary/v1/`](./current-campaigns-summary/v1/) | Exact 2024 `webl24.txt` rows; this remains a separate publisher population. |
| Committee financial summary | [Committee summary draft](./committee-summary/v1/) | Four complete cycle CSV reviews and exact 2024 records covering repeated candidate references, blanks, negative money, and invalid dates; not a release input yet. |
| Processed Schedule A | [`schedule-a/v1/`](./schedule-a/v1/) | Complete artifact/catalog evidence, four conserved target extracts totaling 891M rows, verified zstd storage copies, and paired exact COPY/canonical fixtures. |
| Processed Schedule B | [`schedule-b/v1/`](./schedule-b/v1/) | Accepted after complete artifact/catalog, strict 157.5M-row parser, classic direction/amount, same-publisher-batch Schedule A alignment, release-v3 membership, and lossless 2024 columnar publication/replay gates. |
| Processed Schedule E | [`schedule-e/v1/`](./schedule-e/v1/) | Exact 80-column DDL, complete 548,318-row parser replay, and real COPY fixtures for fractional, negative, memo, null-type, and escaped-text shapes. |
| Targeted processed Schedule A observations | [`schedule-a-api/v1/`](./schedule-a-api/v1/) | Exact-value API pages for one original filing image range and one complete zero-result amendment range at a stated observation time. |
| Electronic-filing listings | [`efile-listings/v1/`](./efile-listings/v1/) | Canonical exact-field API pages for an original and amendment, including asymmetric amendment-chain assertions. |
| Raw electronic-filing documents | [`efile-documents/v1/`](./efile-documents/v1/) | Three complete exact-byte `.fec` fixtures, including one original/amendment family and a small mixed-schedule report. |
| As-filed Schedule A | [`efile-schedule-a/v1/`](./efile-schedule-a/v1/) | Exact row bytes plus lossless 45-field mappings; one identical memo row is retained in both an original and amendment. |
| Electronic-filing format dispatch | [`efile-format/v1/`](./efile-format/v1/) | Exact HDR byte fragments for 8.3, 8.4, and 8.5 plus the ordered 45-field Schedule A layout. |

Maturity is explicit in each contract. Processed Schedule B and Schedule E are
accepted. Schedule B passed its physical, classic-comparison, same-publisher-
batch Schedule A alignment, release-v3 membership, and complete selected-cycle
fact publication/replay gates. Its effective-record, outgoing-flow, and A/B
reconciliation policies remain separate calculations.
The classic reference/summary products and processed Schedule A/B/E are the
current bulk-release inputs. Schedule E enters release-inventory v2 because its
single all-history relation does not share Schedule A's physical period
partitions. Schedule B enters active release-inventory v3 through four
archive-direct relations. The API/raw contracts and
format dispatch are deferred research. The pipe-delimited fixtures are canonical
representations of exact local source lines, not replacements for the raw ZIP
artifacts. Schedule A's API-derived fixtures are explicitly
`synthetic_json`; they do not prove restored data values or row conservation.
Its exact 2026-08-23 archive-range observation separately proves the complete
catalog, PostgreSQL relation order and types, declared nullability/defaults,
search-vector columns, and target-partition inheritance. The same observation
now pins the whole artifact and complete 2025/2026 extract; eight paired exact
COPY/canonical fixtures prove physical decoding. All four target data blocks
pass extraction and lossless compressed-storage conservation. Runtime Go
extraction, checkpointed selected-stream storage, and atomic coordinated
source-release publication are implemented. Immutable occurrence publication
exists for Schedule A and all five classic products. Lossless normalized facts
exist for all six initial source families. Schedule A's complete real-cycle
fact publication remains an acceptance gate. Schedule E's strict Go parser
passes all 548,318 current rows; its occurrence/fact publication and effective-
record calculation remain separate gates.

Routine Schedule A freshness uses the processed bulk dump under the accepted
[release strategy](../../../docs/design/fec-release-strategy.md). The API,
exact `.fec`, as-filed Schedule A, and effective-report contracts preserve a
rejected alternative's research. They do not feed or repair initial production
and require a later accepted decision before they can support a separately
labeled as-filed product. The targeted API reconciliation fixture remains
diagnostic evidence, not a complete baseline.

The supporting evidence and remaining acceptance gates are in the
[FEC source-contract audit](../../../docs/design/fec-source-contracts.md).
