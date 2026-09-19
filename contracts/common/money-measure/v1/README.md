# Money measure API contract v1

[`schema.json`](./schema.json) defines the language-neutral monetary value used
by Go services and API clients. The semantic rationale lives in the
[money-measure design](../../../../docs/design/money-measures.md).

Minor-unit values are signed base-10 JSON strings. Go uses checked `int64`
values internally, while the wire contract avoids JSON and JavaScript integer
precision loss. No exponent, decimal point, leading zero, or negative zero is
valid.

The JSON Schema validates shape and state-dependent nullability. Domain
validation must additionally prove:

- a `point` has equal lower and upper values;
- every lower bound is less than or equal to its upper bound;
- an equal bounded pair is inclusive at both ends and normalized to `point`;
- the result is compatible with its semantic role, measurement kind, and
  source rule;
- checked arithmetic did not overflow Go `int64`;
- issue codes required by a source or calculation rule are present.

The examples are contract fixtures, not claims about a specific filing:

- [`fec-itemized-receipt.json`](./examples/fec-itemized-receipt.json)
- [`fec-independent-expenditure-estimate.json`](./examples/fec-independent-expenditure-estimate.json)
- [`fec-independent-expenditure-actual-unobserved.json`](./examples/fec-independent-expenditure-actual-unobserved.json)
- [`fec-unitemized-summary.json`](./examples/fec-unitemized-summary.json)
- [`fec-partial-signed-subtotal.json`](./examples/fec-partial-signed-subtotal.json)
- [`lda-under-reporting-band.json`](./examples/lda-under-reporting-band.json)
- [`lda-rounded-estimate.json`](./examples/lda-rounded-estimate.json)

The independent-expenditure estimate is a point only in the space of values
reported by the filer. It is not a bounded claim about the eventual actual
cost. The partial signed subtotal is also a point for the resolved fact set,
but it is neither the complete amount nor a lower bound for unresolved signed
rows.
