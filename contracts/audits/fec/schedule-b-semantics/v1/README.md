# Schedule B reporting and record-selection diagnostic

The Go command `audit-schedule-b-semantics` reads one verified published
Schedule B columnar fact set. It rehashes every backing shard, checks the
complete physical schema, and projects only the columns needed for this scan.
Every source ordinal belongs to one disjoint reporting/identity shape. Each
shape conserves row counts, source signed cents, a non-memo subtotal hypothesis,
and filing-reference presence. Unknown form/line/schedule combinations remain
explicit. Source NULL and empty text remain distinct.

Reporting roles come from exact form and line combinations in the official
[Form 3](https://www.fec.gov/pdf/forms/fecfrm3.pdf),
[Form 3P](https://www.fec.gov/pdf/forms/fecfrm3p.pdf), and
[Form 3X instructions](https://www.fec.gov/pdf/forms/fecfrm3xi.pdf), inspected
2026-09-08. The source has no filing-format version field; this mapping is a
diagnostic for the target cycles, not blanket historical interpretation.
Line 23 means operating expenditures on Form 3P and federal contributions on
Form 3X. The latter may include in-kind contributions. A form-line category
does not establish an economic transfer or its beneficial source.

The memo-X projection follows the FEC's
[BaseItemized model](https://github.com/fecgov/openFEC/blob/develop/webservices/common/models/itemized.py).
The diagnostic measures all action codes without rebuilding the publisher's
amendment processing or deduplicating repeated transaction identifiers.
It does not declare the non-memo sum to be total spending or accepted graph
money. Original and back-reference fields measure presence and same-row
equality only; they are not a cross-row identity index.

The lowest source ordinal is retained as an example for each shape, with its
submission, filing, transaction, recipient, and original/back-reference
identities. It is a drilldown locator, not a statistical sample. Sorted groups
have a SHA-256 over Go's compact JSON encoding, making profile results
independent of worker completion order. Money is encoded as integer-cent
strings. Runtime and source evidence remain separate from that profile digest.

The command outputs this strict result schema only after all source rows and
group counts are conserved. It writes no facts, calculation pointers, or
graph state. Default concurrency is four workers; a 100,000-shape limit fails
explicitly before unbounded grouping growth.
