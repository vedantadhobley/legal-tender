# Committee-summary verification

Implemented read-only Go scan. The [source boundary](../../../../../docs/design/committee-summary-source.md)
defines exact-byte acquisition ancestry and the limits of these summary values.
The [result schema](./result.schema.json) describes successful complete scans,
including typed issues; failures do not emit a partial successful result.

`complete` refers to all captured CSV records. It does not mean complete reports,
valid candidate references, reconciled financial statements, or terminal funding.
`terminal_attribution_eligible` is always false. Interpretation of raw code fields
remains `preserved_not_interpreted`.

Two SHA-256 streams prove normalization independently of artifact hashing:

- `fields_sha256`: each source string in the pinned 92-column order, for every
  row in source order.
- `typed_values_sha256`: for each row, each of the 75 money columns in pinned
  order contributes state, cents string (empty when absent), and decimal source
  scale string; each date column then contributes state and ISO date or empty;
  finally `CMTE_ID`, `CAND_ID`, and `FEC_ELECTION_YR` contribute state and value
  or empty. Raw values remain separately bound by the field stream.

Each string is encoded as its UTF-8 byte length in unsigned 64-bit little-endian
form followed by its bytes. No separator or map iteration order determines the
hash. Column order and normalization version are pinned by the parser contract.

Issue counts are complete; examples are the first ten per code in source order,
with exact raw locators. Repeated committee/composite identities and exact raw
duplicates remain observations, not record deletion or accepted financial grouping.
