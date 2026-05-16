# Validation

How we validate that the pipeline produces correct output, what numbers
we currently meet, what we haven't validated yet, and what to run before
every fix.

## The four-gate contract

Every code change that touches data flow runs through four gates before
commit. If any gate fails, the change gets reverted in the same session
rather than left in main.

| # | Gate | What it catches | How to run |
|---|---|---|---|
| 1 | **Bulk median** | Total-receipts drift vs FEC's own published totals | `docker exec legal-tender-dev-webserver python3 /workspace/scripts/validation_report.py` |
| 2 | **Named-candidate diff** | Per-channel attribution shifts for candidates we know well | `docker exec -w /workspace legal-tender-dev-webserver python3 scripts/view_candidate.py "<name>"` for each of Cruz / Trump / Harris / Bacon / Sanders, compare to `docs/audit/baseline-2026-05-12.md` |
| 3 | **Target case** | The specific bug the fix is *trying* to fix actually flips | Hand-crafted ArangoDB query stated in the fix's commit message |
| 4 | **Pytest green** | Unit-test invariants in `tests/` stay holding | `docker exec -w /workspace legal-tender-dev-webserver pytest tests/ -q` |

Acceptance thresholds:

- Gate 1 (bulk median |Δ|): must stay ≤ 5%. **Currently 2.3%** (was 2.4% pre-resync). Anything > 5% is rejected.
- Gate 1 (BWC sanity across all 4 cycles): must stay within ±5% delta on each cycle. Currently -2.7% to -5.8%.
- Gate 2 (named-candidate top-15): no organization should swing >20% without a documented reason. No organization should disappear from the top-15 entirely.
- Gate 3: explicit pass/fail stated in the fix's commit message. Fix isn't done until target case passes.
- Gate 4: 65/65 tests pass. Anything less is rejected.

## ⚠ Validation methodology caveat

The bulk-median gate compares our `funding_channels.total_funding` against FEC's `weball.TTL_RECEIPTS`. For candidates without itemized donor records (`individuals.data_quality.primary_source == 'fec_summary'`), our number IS FEC's number copied through via the `total_from_individuals_external` fallback in `committee_receipts`. The comparison is tautological for those cases — passing the gate doesn't validate that we have donor-level detail to back the total. See @docs/data-quality.md and the 2026-05-16 entry in @docs/decisions.md for the full critique. A follow-up gate "for candidates with N≥100 itemized records, the whale/grassroots split matches a direct ranking of those records" is queued.

## Current validation state (2026-05-16 resync)

### Bulk validation vs FEC `weball.TTL_RECEIPTS`

11,433 candidate-cycle comparisons across all 4 cycles. "delta" = (ours − FEC) / FEC.

| Metric (post-resync) | Value |
|---|---|
| total comparisons | 11,433 |
| median \|Δ\| | **2.3%** |
| within ±5% of FEC | 66.0% (was 65.0%) |
| within ±10% of FEC | 78.2% (was 76.9%) |
| within ±25% of FEC | 89.2% (was 88.1%) |

All within-tolerance buckets improved by ~1 percentage point. Fresh Q1 2026 indiv records (esp. for newer candidates like Hamawy) shifted some candidates from `fec_summary` fallback to itemized detail.

Healthy. The p99 number is dominated by one or two outlier records per cycle (data quirks like the "CHRISTINA CLEMENT LLC" 75,476% delta in 2024 — a different-candidate-with-same-name aliasing).

### BWC sanity (Bacon NE-2, the "small consistent House campaign" canary)

| Cycle | Ours | FEC | Δ | Δ% |
|---|---|---|---|---|
| 2020 | $779K | $807K | -$28K | -3.4% |
| 2022 | $905K | $930K | -$26K | -2.7% |
| 2024 | $928K | $956K | -$27K | -2.9% |
| 2026 | $285K | $303K | -$18K | -5.8% |

Tight, consistent ~3-6% underattribution likely from sub-itemization-threshold donors that don't appear in `indiv.zip`.

### Named-candidate top-15 (subjective gate, asserted via spot-check)

Manual sanity-check that the top-attributed orgs make political sense for each candidate. Run `view_candidate.py` on each.

| Candidate | Top 5 by_organization should include | Currently passes? |
|---|---|---|
| Cruz (S/TX) | Energy/finance, Texas oil & gas, trial lawyers (anti-), Adelson | ✓ |
| Trump (P) | Musk/DOGE, Mellon/Pan Am, Uihlein/Uline, Perlmutter/Marvel, Lutnick/Cantor Fitzgerald | ✓ |
| Harris (P) | Tech (LinkedIn, Asana), Bloomberg, labor (SEIU) | ✓ |
| Bacon (H/NE-2) | Letter Carriers, Plumbers, Realtors, Bankers, Honeywell, NAR | ✓ |
| Sanders (P) | grassroots-dominated (Alphabet/Amazon employees, universities), basically zero whales | ✓ |

These are kept tight in `docs/audit/baseline-2026-05-12.md`. Any fix that
disturbs these should be visible in a diff against that baseline.

### Pytest

68 tests, ~25s. Coverage:

- `tests/test_name_match.py` (42 tests) — acronym / portmanteau / edit-distance / token-containment signals
- `tests/test_wikidata_resolver.py` (17 tests) — employer reconciliation (Goldman, Apple, IBM, Citadel, Baupost, Beal Bank, Mountaire, LinkedIn etc.) end-to-end through reconci.link → GLEIF
- `tests/test_whale_resolver.py` (9 tests) — person→company P1830/P108/P39 walk (Musk, Adelson, Griffin, Mellon, Koch)

These run on every commit. Live network — set `NO_NETWORK=1` to skip the integration tests.

## What we've validated

| What | How | Status |
|---|---|---|
| Total receipts per candidate vs FEC's own totals | bulk median \|Δ\| 2.4% | ✓ |
| BWC small-House-campaign consistency across cycles | within -3% to -6% | ✓ |
| Trace algorithm correctness (cycle break + mult caps) | manually verified; pre-fix totals were $16T for House races | ✓ |
| Unitemized-grassroots fix | median dropped 33% → 10.8% → 2.4% | ✓ |
| PCC reroute (Sanders 2020 case) | Bernie 2020 -58% → 0% | ✓ |
| Stale-merge ArangoDB UPSERT issue | `mergeObjects: false` documented | ✓ (committee_receipts only; other sites not audited) |
| Super PAC indiv vs webk.INDV_CONTRIB issue | Trump WhatsApp $10M → $1M | ✓ |
| Self-funder gap | Trone/Lamon/Gibbons/Bloomberg/Steyer all fixed | ✓ |
| Trade-association vs ideological classification (M-ORG_TP refinement) | Wikidata P31 lookup, 33 committees ($116.7M) flipped 2026-05-13 | ✓ on the flipped set |
| Parent-org inheritance (NAR Cong Fund / Club for Growth Action / NRA ILA) | Phase 2a/2b inheritance | ✓ target cases pass |
| Recursive IE trace through passthrough Super PACs | by_pac shrunk from catch-all to ~0; by_corporation now 22% of IE+ | ✓ |
| Employer resolver hit rate | ~60% via reconci.link + GLEIF | ✓ for the resolved fraction |
| Whale resolver | ~55% via reconci + P108/P1830/P39 walk | ✓ for the resolved fraction |

## What we haven't validated

Things that are *known to be imperfect* but unvalidated quantitatively:

| What | Why unvalidated | Plan |
|---|---|---|
| **Trade-vs-ideological classification quality** | We flipped 33 committees but haven't manually-verified all of them are correct. We also know AAJ (trial lawyers, $24M) and Council of Insurance Agents ($17M) stayed ideological per Wikidata's call; debatable. | Audit the trade-class P31 cache (`/storage/cache/trade_assoc_classification.json`) by eyeballing the 33 flips + the 380 not-flipped. Add to `docs/audit/` as a one-off audit doc. |
| **Visible misresolutions in `by_organization`** | "TARGETED VICTORY", "PRESIDENT", "United States Department of the Army", "State of Nebraska", "Asana Journal" — reconci returning the wrong Q-id for short/generic FEC employer strings. | Implement generic-string rejection in `name_match.py` (treat US state names, federal-agency names, common job titles as "low corroboration" inputs). See `docs/todo.md` Section "Next session 2". |
| **Same-entity splits in `corporate_families`** | Pan Am Systems ($615M) + Pan Am Railways ($308M) = one Mellon entity; GREYLOCK + Greylock Partners; Adelson Drug Clinic + Adelson Clinic | Post-resolution P749 (parent-organization) walk to merge families that share a parent. Deferred. |
| **Wikidata coverage gap for M-ORG_TP refinement** | AOPA ($3.8M), NFIB ($3.8M), BCBS Michigan ($4.5M), APTA ($4.0M), ACOG ($3.5M) — real trade orgs that Wikidata didn't classify specifically enough. They stay ideological. Total: ~$30M of coverage gap. | Acceptable trade-off (better to underflip than overflip). Could be revisited if OpenCorporates Layer 3 adds richer classification. |
| **Disambiguation gap in whale resolver** | "JOHN ARNOLD" → wrong (historical figure); "PAUL SINGER" → wrong (musician?). High-receipts whales whose Wikidata top hit is a non-business namesake. | Same OpenCorporates dependency. Documented. |
| **Trace algorithm convergence** | Current fix uses 8-level + cap-based; slight under-attribution where legitimate compound mults would exceed 1.0. | Replace with proper fixed-point iteration with convergence detection. Deferred — not blocking. |
| **Sub-itemization-threshold donors** | Donors who gave <$200 cumulative per committee aren't in `indiv.zip` and aren't graph-traced individually. We use `weball.TTL_INDIV_CONTRIB` for the aggregate sum. | Can't be improved with current FEC bulk data. |
| **Other UPSERT sites' stale-merge** | Same `mergeObjects: false` bug pattern likely exists in graph/enrichment/aggregation assets beyond committee_receipts. | Audit all UPSERT call sites. Documented in todo. |
| **Cycle assignment** | Spot-checked: 99%+ of `indiv.TRANSACTION_DT` falls in expected 2-year window per cycle. Tail of ~0.5% are amendments/corrections (noise). | Audited 2026-05-09; not a real bug. |
| **JFC passthrough proportional accounting** | Spot-checked TEAM SCALISE 2022: $28.2M received → $12M transferred to Scalise For Congress = 42.5% multiplier. Correct passthrough math. | Audited 2026-05-09; not a real bug. |
| **spent_on support/oppose classification** | Spot-checked 2024 IEs: FF PAC supports Harris, AMERICA PAC supports Trump, MAGA Inc supports Trump AND opposes Harris (correctly tagged). | Audited 2026-05-09; not a real bug. |

## How to run validations

```bash
# Gate 1: bulk validation
docker exec legal-tender-dev-webserver python3 /workspace/scripts/validation_report.py

# Gate 2: named-candidate spot-check
docker exec -w /workspace legal-tender-dev-webserver python3 scripts/view_candidate.py "CRUZ"
docker exec -w /workspace legal-tender-dev-webserver python3 scripts/view_candidate.py P80001571  # Trump
docker exec -w /workspace legal-tender-dev-webserver python3 scripts/view_candidate.py "HARRIS, KAMALA"
docker exec -w /workspace legal-tender-dev-webserver python3 scripts/view_candidate.py H6NE02125  # Bacon
docker exec -w /workspace legal-tender-dev-webserver python3 scripts/view_candidate.py P60007168  # Sanders

# Gate 3: target case — depends on fix. Example commit messages show the exact AQL.

# Gate 4: pytest
docker exec -w /workspace legal-tender-dev-webserver pytest tests/ -q
```

After any change to a Dagster asset that affects classification or attribution:

```bash
# Re-materialize the dependent assets
docker exec -w /workspace legal-tender-dev-webserver dagster asset materialize \
    --select committee_classification -m src
docker exec -w /workspace legal-tender-dev-webserver dagster asset materialize \
    --select candidate_funding -m src
```

Then re-run all four gates.

## Baselines

| Baseline | File | Captured |
|---|---|---|
| 2026-05-12 baseline | `docs/audit/baseline-2026-05-12.md` | start of terminal-node-fix session |

Diff against this is the qualitative gate for any classification or attribution change. If a fix produces an output where the named-candidate top-15 differs substantively without explanation, it's rejected.

## Validation tooling we haven't built yet

- **Automated diff against baseline**: a script that takes a baseline file + current state and reports any org that swings >20% or disappears. Currently this is manual.
- **JSON-output mode for view_candidate.py**: enables programmatic diff. On `docs/todo.md`.
- **Per-asset metrics to Prometheus**: row counts, classification distribution, cache hit rates. On the Phase 4 production-readiness list.
- **Continuous validation in CI**: re-run gates on every PR. Phase 4.

## What I don't claim this validation catches

This pipeline is rigorously validated for **total flow conservation**
(money in = money out, modulo documented gaps). It's *less* rigorously
validated for **attribution accuracy** — i.e., the question "is the
right organization getting credit for this dollar?" 

The pieces of attribution accuracy that ARE validated:
- The trace algorithm itself (cycle break, multiplier caps, proportional attribution)
- Whether terminal-node classifications make sense (eyeball + Wikidata P31 cross-check)
- Whether the top-attributed orgs for named candidates pass a sniff test

The pieces that AREN'T:
- Whether every individual donor is correctly resolved to a corporate identity (the resolver's ~60% hit rate is acceptable but not perfect)
- Whether Wikidata's classification is right for every PAC
- Whether the IE trace's depth-2+ recursion produces precisely the right multipliers

These caveats are why the named-candidate gate exists — to catch attribution-accuracy regressions that bulk validation can't see.
