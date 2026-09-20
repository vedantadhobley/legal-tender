# Direct source-appearance attribution gate — 2026-09-20

Status: pass for the complete retained 2024 Schedule A participant population.
This gate implements the accepted partial direct/earmark boundary. It writes no
durable publication and changes no graph.

## Pinned inputs

| Input | Identity |
|---|---|
| Receipt participant calculation | `5cf4f803465c19f8abc0f4cd3eab87b132184bc47536b6aa3c0c5753dbbe0a1e` |
| Participant manifest SHA-256 | `fb8788e9a5cdea6c099f38dbb5be5fa41992b425c1a471c85d5343a5097d61b8` |
| Schedule A fact set | `8f69a2ef40316f62d8844ecf732342a3b4535136b434f60677d0608cb04465df` |
| Schedule A manifest SHA-256 | `b664e752a3bae186508a45bb6d09289ba2fe06f0b4149ecbd72063e7ef79b829` |
| FEC source release | `fec-a6e6f777c3c506b100a7709815bc89f2ae84749faf4343a4589f7ec1bac066a2` |
| Candidate-receipt fact bundle | `b00ce42a65696310b8c1f8c3f8f3bc28077f5bc774e4955657cc16b210d629a3` |
| Bundle manifest SHA-256 | `66108c97917ea4cc18b9f22c4481862d888f5844e0d8c7cd3e73938bfd596de0` |
| Candidate–committee linkage fact set | `4327fff8f584be8670174977b8fd5b93da4b2700c98c81915b5acce40cd8b718` |
| Linkage manifest SHA-256 | `619fb6f7ee745644a24bf98ebc983fca7906ebb6158c12080b78e556ab0b81e2` |

The input contains 264,085,606 participant occurrences and 8,619 linkage facts.
All inputs were opened through their existing strict backing verifiers.

## Authorization result

The linkage population contains 8,076 candidates. Accepted unique
authorization routes 7,830 committees to 7,470 candidates. Another 378 grouped
relationships remain unresolved and 376 are unauthorized. Neither population
routes money to a candidate.

## Complete result

Amounts below are signed Schedule A observation amounts. They are not complete
candidate funding totals.

| Population | Rows | Signed amount |
|---|---:|---:|
| Complete participant population | 264,085,606 | $53,409,157,201.64 |
| Outside authorized candidate scope | 242,314,829 | $30,976,713,106.69 |
| Authorized committee scope | 21,770,777 | $22,432,444,094.95 |
| Excluded memo subtotal | 10,113,274 | $19,218,309,970.68 |
| Included nonmemo scope | 11,657,503 | $3,214,134,124.27 |
| Direct reported appearances | 2,288,558 | $682,144,739.50 |
| Explicitly earmarked appearances | 9,071,300 | $1,050,070,316.91 |
| Unresolved | 297,645 | $1,481,919,067.86 |

The unresolved population consists of:

| Reason | Rows | Known signed amount |
|---|---:|---:|
| Committee chain unresolved | 239,682 | $865,657,016.31 |
| Other reported source role unresolved | 57,608 | $616,432,912.53 |
| Individual/committee role conflict | 353 | -$170,860.98 |
| Unknown amount | 2 | unknown |

Rows, known and unknown amount counts, positive/negative/zero counts and signed
minor units conserve exactly at the complete-cycle and per-candidate levels.

## Prior diagnostic witnesses

The cycle-wide result exactly reproduces the previously retained candidate
diagnostic values:

| Candidate ID | Direct | Explicitly earmarked | Unresolved |
|---|---:|---:|---:|
| `S6OH00163` | $10,001,086.56 | $40,694,770.49 | $16,683,733.21 |
| `S6PA00217` | $6,545,728.03 | $20,579,969.18 | $12,838,784.52 |

These are source-appearance dispositions, not resolved donor identities.

## Reproducibility

- Calculation ID:
  `b70511820dc8685ca4cf238dce1d3c2297ab5108bf1acd6fcb190ccd33c88f24`.
- Executable SHA-256:
  `952033b7e1a2f0267aa6fc20c3fbc5ba5dead5e69a394728147ce941f7c41495`.
- Result bytes: 17,358,351.
- Result SHA-256:
  `79d23847774fb5803e2e836b6ce958657fb4151059ea9830892181f1f39f0782`.
- An eight-worker scan and a four-worker replay produced byte-identical JSON.
  The replay supplied the first calculation ID as a required identity.
- The result passes the normative Draft 2020-12 JSON Schema.
- The complete Go test suite passes. Unit coverage includes exact sign and
  unknown conservation, shared authorization, contradictory participant states,
  incomplete scans and worker-count independence.

Disposable results and logs are under
`/tmp/legal-tender-direct-attribution-2026-09-20/`. The calculation is already
deterministic and reproducible. Durable publication and Dagster wiring are
deferred until a concrete downstream graph, API or scheduled-calculation consumer
needs the result. The [high-level inventory](./go-rewrite-inventory-2026-09-20.md)
owns the corrected sequence. Any later graph projection must retain the
source-appearance versus resolved-entity boundary.
