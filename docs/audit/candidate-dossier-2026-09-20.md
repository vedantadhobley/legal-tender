# Candidate dossier gate — 2026-09-20

Status: implemented and verified for two retained 2024 candidate reports. The
new `build-candidate-dossier` command combines an exact candidate-evidence v2
report with the complete published candidate-interpretation set. It produces a
compact product-facing JSON view without selecting terminal sources or changing
any existing fact, calculation, graph or current pointer.

## Boundary

The dossier keeps two financial evidence domains separate:

- candidate-linked reported receipt populations for committees classified as
  authorized or unresolved in the parent report; and
- independent expenditures whose candidate appears as a source-reported endpoint,
  a corroborated safe default, an inferred alternative, or a conflicting alternative.

The four independent-expenditure views overlap and are not additive. Each
relevant interpretation occurs once in the evidence list and names every view it
supports. Support and opposition stay separate. Signed, positive, negative and
zero measures remain exact integer minor units.

The result also retains the exact parent report and evidence IDs, receipt and
committee-flow inputs, candidate-name source, candidate-interpretation manifest
and artifact identities, one candidate-linked committee population, and three
representative connection paths. Terminal, allocation, person/corporation,
lobbying, legislation, votes and official-action fields remain outside this
version.

## Exact source versions

The first live attempt correctly failed a proposed same-release requirement. The
retained publications use exact immutable inputs from different release snapshots:

- Schedule A receipt facts: `fec-a6e6f777c3c506b100a7709815bc89f2ae84749faf4343a4589f7ec1bac066a2`;
- committee-flow reconciliation: `fec-136903645ee7c7050d463a4f779a051a454ebb87308fd854faabc5ca47a20cdf`;
- Schedule E candidate interpretations: `fec-76b6660f70406bf0d11537885de172883cdb8af548bfa7df35078a721c8c759a`.

The final contract requires one cycle but does not manufacture release equality
across non-additive domains. It records each exact release and reports
`different_source_releases`. A later aligned-snapshot view can impose a stronger
boundary with its own contract.

## Real results

| Candidate ID | Dossier ID | Relevant Schedule E rows | Reported endpoint | Safe default | Inferred alternative | Conflicting alternative | JSON bytes |
|---|---|---:|---:|---:|---:|---:|---:|
| `S6OH00163` | `d153cff4a636ffefe4b2cac8c68c65de4e206673990dd129aab7c3be96bfd51f` | 781 | 757 / $138,444,339.40 | 725 / $135,311,934.34 | 23 / $302,440.28 | 1 / $61.04 | 1,354,247 |
| `S6PA00217` | `88988fe92dfa14378dd7a768e2dea9304eb2ce3b19550728a8a5912331800695` | 1,206 | 1,195 / $123,966,667.43 | 818 / $95,666,527.98 | 8 / $6,160,855.13 | 3 / $1,045,500.00 | 2,086,236 |

Rows and amounts in the four view columns must not be added. The safe default is
a subset of reported endpoints. Alternative views can identify the selected
candidate while the preserved source-reported endpoint identifies another ID.
The evidence array prevents duplicate storage within the dossier.

Executable SHA-256:
`9c33db34cf2655d885f8979c1c3325d6558c15356721e99c1897608f2831254a`.

Output SHA-256 values:

- `S6OH00163`: `57149a23fd0d75b9f7792f95bb8836ccde2fd76419f4a0dd8f17298330541e24`;
- `S6PA00217`: `c4a6cd0b783f1f7c08f0cfa8f0d261b0d85f88e6b1d23b014ddbdd5e4a83ec14`.

## Verification

- The command strictly authenticates the supplied v2 report and its nested
  evidence identity. It labels this as content verification, not a full receipt
  recalculation.
- The candidate-interpretation loader verifies the pointer against the immutable
  manifest, decodes every interpretation, validates every semantic state and ID,
  and recomputes complete counts and signed amounts before success.
- The full Go test suite passes. Unit tests cover endpoint-role separation,
  overlapping reported/safe-default membership, inferred/conflicting alternatives,
  signed measures, stance separation and selected-candidate name evidence.
- Both real outputs pass the complete local JSON Schema registry. Source storage
  was mounted read-only. No network, graph write, source fetch, Dagster activation,
  current-pointer update or model call occurred.

The generated live JSON and disposable executable remain under `/tmp` and are not
durable project evidence. The identities above make the gate reproducible from
the retained parent reports and published interpretation artifact.
