# Money measures and uncertain totals

> **Status:** Accepted semantic contract for the Go rewrite. The v1 wire schema
> and fixtures exist under
> [`contracts/common/money-measure/v1/`](../../contracts/common/money-measure/v1/).
> Source parsing and scoped Go calculations are implemented; universal interval
> propagation and terminal-dollar allocation remain separate gates. See the
> [as-built ledger](../go-rewrite.md) for current runtime boundaries.

## Purpose

Legal Tender must not turn a source threshold, rounded estimate, missing
coverage, unresolved identity, or uncertain filing revision into a precise
dollar total. Monetary uncertainty is part of the answer.

This contract applies across campaign receipts, transfers, independent
expenditures, terminal-source attribution, lobbying reports, and cross-cycle
views. It does not make those financial meanings interchangeable.

## Three separate questions

Every monetary value has three layers:

1. **Representation:** What exact text or number did the source publish?
2. **Measurement:** What does that published value claim and with what
   threshold, rounding, accounting method, or precision?
3. **Result:** What interval or scenario can this calculation honestly return?

Exact decimal parsing answers only the first question. Parsing `20000.00` into
integer cents without loss does not prove that $20,000 was an exact ledger
amount. It can still be a publisher-required rounded estimate.

## Source money observation

A normalized source fact retains these fields where applicable:

| Field | Meaning |
|---|---|
| `semantic_role` | Source-specific meaning such as receipt, refund, lobbying income, lobbying expense, transfer, or independent expenditure. |
| `currency` | Source currency; initially `USD`. |
| `raw_value` | Exact source text or JSON value. |
| `reported_minor_units` | Losslessly parsed signed integer cents when the source publishes a numeric point. |
| `observation_state` | `reported_value`, `threshold_band`, `source_blank`, `not_applicable`, `invalid`, or `unobserved`. |
| `measurement_kind` | `reported_point`, `rounded_estimate`, `threshold_band`, `summary_value`, or another source-contract value. |
| `precision_increment_minor_units` | Declared rounding increment when the source rule supplies one. |
| `source_lower_bound_minor_units` | Lower bound established directly by the source rule, if any. |
| `source_upper_bound_minor_units` | Upper bound established directly by the source rule, if any. |
| `lower_bound_inclusive` / `upper_bound_inclusive` | Boundary semantics established by the source wording. |
| `accounting_method` | Source accounting or reporting method when it changes measurement meaning. |
| `source_rule_version` | Versioned rule that interprets the field for its reporting period. |

`unobserved` means the required representation has not been captured. It is
not equivalent to a blank field. `not_applicable` means the field cannot carry
money for this record under the source rule. It is not a reported zero.

## Calculated money interval

A monetary calculation returns one of these amount states:

| State | Required bounds | Meaning |
|---|---|---|
| `point` | Equal inclusive lower and upper bounds | The calculation yields one amount from its reported inputs. |
| `bounded` | Finite lower and upper bounds | The result is known to lie in an interval. |
| `lower_bounded` | Finite lower bound only | The result is at least the bound under an explicit non-negative-domain rule. |
| `upper_bounded` | Finite upper bound only | The result is below or at the bound under an explicit rule. |
| `unbounded` | No defensible finite bound | Evidence exists, but the amount cannot be bounded. |
| `not_applicable` | No bounds | The monetary question does not apply. |
| `invalid` | No publishable bounds | Source or calculation invariants failed. |

Every finite bound uses signed integer minor units and declares whether the
boundary is inclusive. A point is exact only as the output of the declared
calculation over the declared source measurements. It is not a claim about
undisclosed real-world money.

### Examples

- A disclosed FEC receipt of `$125.00` is a reported point and can contribute
  the point interval `[$125.00, $125.00]` to a compatible calculation.
- An LDA form that says `Less than $5,000` supplies `[0, $5,000)` only when the
  applicable source rule establishes non-negative income or expense.
- An LDA API null without its printable form is `unobserved`, with no numeric
  bounds.
- A rounded reported point carries its rounding increment. The calculation may
  derive a range only after the effective rounding and tie-boundary rule is
  accepted for that reporting method and period.
- A known subtotal from incomplete signed transactions is not automatically a
  lower bound. Missing negative adjustments could reduce the final result.

## Orthogonal uncertainty dimensions

An amount interval does not encode every kind of uncertainty. Each material
result also carries:

| Dimension | Required meaning |
|---|---|
| `coverage_state` | Whether the required source partitions and fields are complete, partial, incompatible, or unknown. |
| `attribution_state` | Whether the amount is directly disclosed, exclusively allocated, proportionally allocated, mixed, or unresolved. |
| `revision_state` | Whether the result uses a publisher-effective view, all versions, a provisional inferred version, or unresolved alternatives. |
| `identity_state` | Whether every calculation-controlling identity is resolved, partly resolved, or unresolved. |
| `evidence_status` | Whether the measure is reported, calculated, reconciled, or contextual. |

These dimensions must remain independently filterable. A bounded reported
amount can still have unresolved attribution. A point total can still have
partial coverage. High identity confidence cannot make an estimated amount
more precise.

## Alternative scenarios are not ranges

Unresolved amendments, entity resolutions, or classification choices can
produce several internally consistent answers. Those answers form a scenario
set:

```text
scenario ID
    + controlling decisions
    + money interval
    + fact-set manifest
    + calculation version
```

Do not collapse scenarios to their minimum and maximum when that would imply
that intermediate values are possible or hide correlated decisions. A UI may
summarize the span only if it also exposes the discrete scenarios.

## Interval arithmetic

Calculations follow these rules:

1. Use checked signed integer arithmetic. No float participates in parsing,
   allocation, aggregation, comparison, or serialization.
2. Add intervals only when currency, semantic role, time scope, calculation
   basis, and fact-set membership are compatible.
3. Preserve open and closed boundaries. The sum of `n` `[0, $5,000)` bands is
   `[0, n × $5,000)`, not a point estimate.
4. Never substitute a midpoint, threshold minus one cent, or zero for an
   interval or unknown value.
5. Unknown input makes the output unknown unless a source or domain invariant
   supplies a defensible bound.
6. A partial non-negative fact set may establish a lower bound. A partial
   signed fact set does not establish one without stronger rules.
7. An exclusion contributes zero only inside the calculation whose decision
   manifest proves that the record is outside scope. The source observation
   itself does not become zero.
8. Ratio allocation uses exact rational arithmetic followed by a declared,
   deterministic minor-unit allocation rule. Allocated outputs conserve to
   the input measure under that rule.
9. Cross-cycle addition preserves each cycle component and states whether
   values are nominal or adjusted. A range selection is a view, not a new
   source fact.

## Conservation and graph flow

For an exclusive decomposition, component intervals and the unresolved
remainder must reconcile to the input measure. The calculation publishes its
conservation equation and any rounding remainder.

Graph paths are explanatory evidence, not independently additive money rows.
Several paths can represent the same pooled funds, and cycles can revisit the
same money. Terminal-source attribution must therefore:

- Start from a named input measure.
- Use an accepted flow-allocation contract.
- Prevent overlapping paths from duplicating the input.
- Keep direct, earmarked, and proportional allocation distinguishable.
- Return exclusive terminal components plus an unresolved remainder.
- Carry amount intervals and scenario decisions through the allocation.

An all-paths query may return every relevant route. It must not invite clients
to sum path amounts unless the response explicitly marks those paths as an
exclusive decomposition.

## API and UI contract

The common wire value is defined by the versioned
[`schema.json`](../../contracts/common/money-measure/v1/schema.json). Minor
units serialize as signed base-10 strings, not JSON numbers. This keeps the
wire representation exact for JavaScript and other clients without weakening
the checked `int64` arithmetic used by Go.

Every material monetary answer exposes:

- Formatted range or point.
- Signed lower and upper minor-unit bounds and boundary inclusivity.
- Semantic role and currency.
- Measurement kind and applicable source rule.
- Coverage, attribution, revision, identity, and evidence states.
- Unresolved remainder when the result is a decomposition.
- Input fact-set manifest and calculation version.

Presentation examples include:

```text
$125,000 reported
$120,000–<$145,000 reported range
at least $2.1 million from covered records
amount unknown; printable filing not observed
$2.4 million total; $400,000 source unresolved
```

The UI must not display an interval midpoint as the amount. Filters and
rankings distinguish `definitely above`, `possibly above`, and `overlaps`
instead of silently comparing uncertain values as points.

The money value is embedded in a larger result envelope. Fact-set manifests,
calculation versions, source citations, scenarios, and decomposition remainders
belong to that envelope because they identify the answer, not the numeric value
type itself.

## Concrete Go target

The rebuild uses one domain type and one strict JSON representation. The target
package is `internal/money`; source packages supply rule tables and semantic
roles.

```go
package money

type MinorUnits int64

type AmountState string

const (
	AmountPoint        AmountState = "point"
	AmountBounded      AmountState = "bounded"
	AmountLowerBounded AmountState = "lower_bounded"
	AmountUpperBounded AmountState = "upper_bounded"
	AmountUnbounded    AmountState = "unbounded"
	AmountNotApplicable AmountState = "not_applicable"
	AmountInvalid      AmountState = "invalid"
)

type Amount struct {
	State          AmountState `json:"state"`
	Lower          *MinorUnits `json:"lower_minor_units"`
	Upper          *MinorUnits `json:"upper_minor_units"`
	LowerInclusive *bool       `json:"lower_inclusive"`
	UpperInclusive *bool       `json:"upper_inclusive"`
}

type Measurement struct {
	Basis                    Basis             `json:"basis"`
	Kind                     MeasurementKind   `json:"kind"`
	ObservationState         *ObservationState `json:"observation_state"`
	Reported                 *MinorUnits       `json:"reported_minor_units"`
	PrecisionIncrement       *MinorUnits       `json:"precision_increment_minor_units"`
	AccountingMethod         *string           `json:"accounting_method"`
	SourceRuleVersion        *string           `json:"source_rule_version"`
}

type Uncertainty struct {
	Coverage    CoverageState    `json:"coverage"`
	Attribution AttributionState `json:"attribution"`
	Revision    RevisionState    `json:"revision"`
	Identity    IdentityState    `json:"identity"`
	Evidence    EvidenceStatus   `json:"evidence"`
}

type Measure struct {
	SchemaVersion string      `json:"schema_version"`
	SemanticRole  string      `json:"semantic_role"`
	Currency      string      `json:"currency"`
	Amount        Amount      `json:"amount"`
	Measurement   Measurement `json:"measurement"`
	Uncertainty   Uncertainty `json:"uncertainty"`
	Issues        []string    `json:"issues"`
}
```

`Basis`, `MeasurementKind`, and every uncertainty state are closed string
types matching the JSON Schema enums. `MinorUnits.MarshalJSON` emits a quoted
base-10 integer; `UnmarshalJSON` rejects numbers, decimals, exponent notation,
leading zeroes, negative zero, and `int64` overflow. The package exposes
constructors for point, bounded, one-sided, unknown, not-applicable, and invalid
amounts instead of asking callers to assemble pointer combinations.

`Measure.Validate` enforces the invariants that JSON Schema cannot express:

- point bounds are equal and inclusive;
- lower bounds do not exceed upper bounds;
- equal finite bounds normalize to `point`;
- precision increments are positive;
- source-reported numeric measurement kinds retain the reported point;
- threshold bands do not invent a reported point;
- calculated values have no source observation state;
- source rules authorize the measurement kind and amount state.

All arithmetic methods return `(Measure, error)` and use checked operations.
They do not saturate, wrap, coerce unknowns to zero, or accept mixed currency
and semantic roles.

## Domain consequences

| Domain | Treatment |
|---|---|
| FEC itemized transactions | Usually reported points; signed corrections and effective-record rules remain calculation concerns. |
| FEC publisher summaries | Reported summary points with independent coverage and source semantics; never transaction replacements. |
| LDA LD-2 income and expenses | Threshold bands or reported estimates qualified by reporting method and period. |
| LDA LD-203 items | Reported item points; FEC reconciliation links evidence without duplicating amounts. |
| Independent expenditures | Reported expenditure measures with separate amendment, support/oppose, and upstream-attribution contracts. |
| Terminal-source attribution | Conservation-bound calculated intervals or scenarios, never a sum of arbitrary paths. |
| Missing or unitemized detail | Preserve a known publisher total separately from unresolved composition; do not invent donor identities. |

## Implementation boundary

Go owns money parsing, interval arithmetic, scenario evaluation, conservation,
and serialization. Source adapters emit observations; versioned calculations
emit result measures. Dagster transports only structured counts, states,
digests, and partition metadata from those Go commands.

The eventual shared Go package must provide checked arithmetic and table-driven
tests for every interval state, open boundary, signed value, unknown input,
allocation remainder, and scenario. Domain packages supply source-rule tables;
the common package does not hardcode LDA or FEC thresholds.
