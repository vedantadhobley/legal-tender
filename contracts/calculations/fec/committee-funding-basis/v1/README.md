# Committee reported-receipt inventory

The [implementation contract](../../../../../docs/design/committee-funding-basis.md)
owns the classification and evidence boundary. This manual Go diagnostic does
not define a cash denominator or terminal attribution policy.

- [Inventory schema](./result.schema.json) — disjoint, conserving per-recipient
  receipt components and exact fact/shard membership references.
- [Source-page schema](./page.schema.json) — bounded full source occurrences,
  exclusive source-ordinal cursor, no donor identity merging.
- [Candidate assessment schema](./assessment.schema.json) — inventories attached
  to exact-source upstream committees, without network-wide money totals.
- [Complete-component review](./component-review.schema.json) — all source
  occurrences in a small component, exact measures, and source-role decisions.
- [Inspected source page](./inspect-page.schema.json) — unchanged source page
  wrapped with [source-role decisions](./evidence-decision.schema.json).
- [Report-line review](./report-lines.schema.json) — bounded full report
  membership, independent form/line/memo/individual axes, exact ordinals, and
  comparison guards; see the [runtime contract](../../../../../docs/design/receipt-report-lines.md).

The additive [source-role policy](../../../../../docs/design/receipt-source-evidence.md)
defines observation routing and conduit/reference uncertainty. It does not
change inventory membership or resolve terminal identity.

Money and 64-bit physical source values serialize as decimal strings. Unknown
source values remain null. A known signed subtotal is not a lower bound when
unknown signed amounts exist. Shard presence uses base64-encoded bytes; bit
`index % 8` of byte `index / 8` identifies a shard containing a bucket member.

Go additionally checks exact source backing, content identity, sorted unique
buckets, count/sign/amount conservation, source ordinal bounds, bitmap bounds,
and fixed exclusions. JSON Schema alone does not establish these invariants.

The inventory ID is SHA-256 of Go's compact JSON encoding of the full result
with an empty `calculation_id`, without a trailing newline. Worker count and
local paths are not identity inputs. This checksum establishes content identity,
not publisher authentication or an independently verified economic conclusion.
