# Terminal-policy comparison gate — 2026-09-20

Status: implemented and verified for the two retained 2024 candidate dossiers.
This is a policy comparison, not a terminal-attribution publication. The user
later accepted its narrow direct/earmark recommendation through a separate
[cycle-wide calculation](./direct-source-attribution-2026-09-20.md); these
diagnostic artifacts remain unchanged.

## Boundary

The Go command authenticates each dossier, removes the disjoint
`memo_subtotal` component from the numeric scope, and compares six scenarios.
Every scenario conserves the same included rows and exact signed cents across
direct, explicitly earmarked, proportional and unresolved buckets. Blocked
pro-rata and FIFO methods allocate zero and retain the full scope as unresolved.

Source appearances remain distinct. A reported contributor is not a resolved
person or organization. A reported committee is an immediate counterparty only.
No graph, source fact, calculation, current pointer or Dagster asset changed.

## Real results

Amounts are signed Schedule A observation subtotals within this diagnostic
scope, not complete candidate-controlled receipts.

| Candidate ID | Included nonmemo scope | Explicit earmark only | Reported direct + earmark | Unresolved after reported direct | Immediate committee-stop direct + earmark | Unresolved after committee stop |
|---|---:|---:|---:|---:|---:|---:|
| `S6OH00163` | $67,379,590.26 | $40,694,770.49 | $50,695,857.05 | $16,683,733.21 | $65,672,015.34 | $1,707,574.92 |
| `S6PA00217` | $39,964,481.73 | $20,579,969.18 | $27,125,697.21 | $12,838,784.52 | $38,129,519.30 | $1,834,962.43 |

The committee-stop result demonstrates why stopping policy matters. It is not
accepted: most of the remaining numeric scope can be relabeled as an immediate
committee counterparty, but that does not identify the upstream source the
product is intended to investigate.

Both dossiers contain zero unknown-amount rows in the included nonmemo scope.
The implementation nevertheless conserves unknown rows and forces them into
unresolved attribution for future inputs.

## Identities

Executable SHA-256:
`dfbe11869914353cf65e575e7abb1614105733a8b8cac0fc82dc2c1de3039bfe`.

| Candidate ID | Comparison ID | Output SHA-256 |
|---|---|---|
| `S6OH00163` | `658deadc6399e7f5fdcd80e4a39b0738035e6c41d1fa395b8a77a725f43826e8` | `92089da597e0058bdc306bec2e9647af45f7982f2168d540758372d0ad62dfa5` |
| `S6PA00217` | `ffad8ff3229a522fee8884672c6e7926b1b4507c5302bcf2f500ffeee4ad332d` | `064307c5ba22c5d5792366ffc1f778b8567680cf69d92fce0d695fab13ea8047` |

Disposable outputs and the executable are under
`/tmp/legal-tender-terminal-policy-2026-09-20/`. The input dossier IDs and
source lineages remain those recorded in the
[candidate-dossier gate](./candidate-dossier-2026-09-20.md).

## Verification

- The full Go suite passes after the new pure calculation, authenticated dossier
  reader and CLI were added.
- Unit tests cover exact scenario conservation, negative amounts, unknown rows,
  unresolved candidate authorization, duplicate committees, broken parent
  populations and order-independent identity.
- Both live outputs pass the normative JSON Schema.
- `terminal_policy`, `allocation_policy`, and terminal eligibility remain unset.

The evidence supports a narrow partial policy: explicit earmarks and direct
reported source appearances can retain exact dollars at occurrence grain.
Pooled committee-chain attribution still requires an accepted funding basis,
time model and entity boundary. The recommendation is not adopted by this
historical gate; the later cycle-wide calculation records its separate adoption.
