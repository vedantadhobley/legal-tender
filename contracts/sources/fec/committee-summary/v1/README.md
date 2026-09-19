# Committee-summary source contract

Draft, research only. The [source boundary](../../../../../docs/design/committee-summary-source.md)
selects the official cycle CSV for committee-level summary assertions, not report
history or terminal attribution.

- [Contract](./contract.json) — acquisition, physical, typed-field, identity,
  revision, and consumer boundaries.
- [Raw record schema](./record.schema.json) — all 92 ordered source fields;
  `x-money-fields` and `x-date-fields` identify separate typed normalization.
- [Review evidence](./review.json) — four complete artifact identities, observed
  profiles, documentation mismatches, retained-evidence paths, and fixture locators.
- [Exact sample](./fixtures/sample.csv) — source header followed by eight selected
  2024 records, with byte hashes and original ordinals in the review. This is a
  constructed subset of exact records, not a complete or contiguous source file.

The raw schema intentionally accepts invalid date strings. This proves source
preservation, not valid typed fields. The invalid `99999999` date, reversed
interval, blanks, negative subtotal, leading-decimal money, and repeated committee
financial values now pass the Go reader's issue and multiplicity tests.

Independent Python tests inspect contract/research evidence only. They are not a
production parser or a new Dagster domain layer. The strict Go reader/verifier
passes all four complete cycles and independent value fingerprints. Immutable
source publication and opt-in release v4 now pass fixture integration and complete
real artifact readback; see the
[publication gate](../../../../../docs/audit/committee-summary-publication-2026-09-08.md).
Real coordinated release acceptance and activation remain open. The research
captures remain outside the active release and no same-release consumer is wired.
