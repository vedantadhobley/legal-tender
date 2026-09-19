# Processed Schedule B source contract

This accepted contract preserves one sender-reported disbursement observation per
physical row of one official processed Schedule B partition. It does not
define an effective-disbursement calculation and does not merge a sender row
with a receiver-reported Schedule A row.

The complete 2026-08-30 artifact and archive catalog pin 39,310,353,867 bytes,
81 ordered fields, and 26 inherited two-year partitions. The strict Go parser
accepted all 157,544,163 rows in the 2024 relation with zero invalid rows or
duplicate `SUB_ID`s. Exact classic comparison confirms sender orientation and
amount precision without treating `pas2` or `oth` as another ledger.

The same-publisher-batch Schedule A/B gate now passes against the immutable
2026-08-30 objects. A complete diagnostic comparison conserved every row and
found unique exact or same-amount/different-date candidates for 57.35% of the
accepted Schedule A flow rows and 82.02% of their signed amount. It never
merged the assertions. See the
[alignment audit](../../../../../docs/audit/schedule-ab-alignment-2026-09-04.md).

Release-inventory v3 and the lossless 98-column Parquet publisher now implement
that boundary. The complete 2024 publication conserved all 157,544,163 rows in
158 shards with zero invalid rows or duplicate `SUB_ID`s; digest-verified replay
returned the byte-identical manifest. The contract is `accepted`. Effective
records, outgoing flow roles, and A/B reconciliation remain separate
calculation contracts.
