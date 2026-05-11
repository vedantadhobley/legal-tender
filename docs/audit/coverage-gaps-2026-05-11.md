# Coverage gap analysis — 2026-05-11

**Data source**: cache from the most recent full-5K run before the resolver
simplification (`/storage/cache/wikidata.json`, 4,999 entries).

**Headline**: hit rate ~55% (2,539 wikidata + gleif resolved / 4,999 total).
Categorization of the 2,460 `not_found` to identify concrete next steps for
reaching ~80% coverage.

## Resolved breakdown

| Source | Count | % |
|---|---|---|
| `wikidata` | 2,254 | 45.1% |
| `gleif` | 285 | 5.7% |
| `not_found` | 2,460 | 49.2% |
| **Total** | 4,999 | 100% |

## Not-found categorization (sampled from first 80)

Hand-classified the first 80 `not_found` entries by failure mode and
estimated population-level percentages:

| Category | % of not_found | Sample names | Best remedy |
|---|---|---|---|
| **A. Small US private firm with no Wikidata page** | ~40% | RYAN SPECIALTY GROUP, FTX DIGITAL MARKETS, QUADRIVIUM, VALMORE MANAGEMENT, SOUTHERN WASTE SYSTEMS, INVEMED, K ROWE INVESTMENTS, PRITZKER GROUP, ALSOP LOUIE PARTNERS, FRANKLIN MOUNTAIN MANAGEMENT, BUCKLEY MUETHING CAPITAL MANAGEMENT, COOPER HOUSE | **OpenCorporates** (~200M companies; most US small private LLCs are there) |
| **B. Wikidata DOES have it but at a different name** | ~15% | GREYLOCK → "Greylock Partners", WILMERHALE → "Wilmer Cutler Pickering Hale and Dorr", THIRD POINT → "Third Point LLC", SUSQUEHANNA INTERNATIONAL GRP → "SIG" / "Susquehanna International Group", D.E. SHAW RESEARCH → "D. E. Shaw & Co." | **Smarter input normalization** before query — suffix-stripping was doing real work here; LLM-based "canonicalize this messy name" pass would catch most. |
| **C. Typo / data garbling** | ~10% | UIINE (→ ULINE), SELF EMPLYED (→ SELF EMPLOYED), GREYLOCK MTG (→ Greylock Mortgage?) | **LLM normalization pass** OR fuzzy-correct against a known-names dictionary |
| **D. Family foundation / private charity** | ~10% | LAURA & ISAAC PERLMUTTER FNDN, MARCUS FOUNDATIONS, GABY FOUNDATION, SCOTT FOUNDATION, HILDEBRAND FOUNDATION, FOUNDATION FOR A JUST SOCIETY | **Wikidata coverage is patchy here.** Big foundations (Gates, Ford, Hewlett) are in Wikidata. Small family foundations aren't. **OpenCorporates** has many of them; otherwise IRS Form 990 data (`propublica.org/nonprofits` has an API) would cover the tail. |
| **E. Generic / non-entity text** | ~8% | CANDIDATE, PRESIDENT, CO-OWNER, CHARTER, SELF EMPLYED, MILES PER HOUR | **Extend `NON_EMPLOYERS`** in employer_normalization.py. These shouldn't even reach the resolver. |
| **F. Local / municipal / labor union** | ~5% | ELECTRICIANS LOCAL 98, AFGHANISTAN WAR COMMISSION, SAN DIEGO FOR EVERY CHILD, NJS HAIR CARE | **No good single source.** Union locals have their own registries; municipal entities are in OpenCorporates; small businesses too. Mostly OpenCorporates coverage. |
| **G. Foreign / non-US entity** | ~5% | (few in this sample) | **Wikidata + OpenCorporates global coverage**. Lower priority for FEC data. |
| **H. Genuinely unresolvable** | ~7% | KAITAR RESOURCES, JIANOR, STG, ICSI, MILES PER HOUR | **Accept**. The long tail isn't worth chasing. |

Total estimated remedy impact if all proposed sources are added:

- OpenCorporates (Layer 3): ~40% recovery + chunks of (D) + (F) = **~50-55% of current not-founds**
- Smarter normalization (Layer 0): ~15% (B) + ~10% (C) = **~25% of current not-founds**
- Extending `NON_EMPLOYERS`: ~8% (E) filtered out at input (these stop showing as
  not-found because they don't enter the resolver). Doesn't improve hit-rate
  *measure* but cleans the data.

**Combined ceiling estimate**: current 55% → ~85% with OpenCorporates +
normalization. The remaining ~15% is genuinely unresolvable or wouldn't
benefit from any source we have access to (small foreign firms, single-name
garbled entries, etc.).

## False positives in the resolved set

Spot-check of the 2,254 `wikidata`-resolved entries:

- `RDV` → "North Vietnam" (score 100) — entity matching is correct (top
  search hit IS North Vietnam); the FEC name is just bad
- `STEYER` → "Steyr" (Austrian city, score 100) — Tom Steyer the donor's
  firm Fahr LLC isn't findable; the search returned the wrong "Steyer"
- A handful of generic placeholder names matching unrelated Wikidata
  entries at score 100

These are inherent in any free-text resolution problem. Cost: small dollars
attributed to bogus entities. Recovery: visible in `by_organization`
output, can be overridden per-name if material. **Not addressing in this
pass** — the trade-off was explicit in the simplification (precision over
recall in the old approach silently rejected real entities like LINKEDIN
and BAUPOST GROUP, which is worse).

## Recommended next steps in priority order

1. **OpenCorporates as Layer 3** — biggest single coverage win (~50%
   recovery of current not-founds). Free tier 500/day works for the long
   tail spread over time; paid for production. Implementation pattern is
   identical to GLEIF's strict-match: full-text search, accept exact
   post-suffix-strip match in preferred jurisdiction.

2. **LLM-based normalization pass (Layer 0)** — joi-hosted Qwen handles
   typos and partial names. One LLM call per name with a tight prompt:
   "Normalize this FEC employer string to a canonical company name; if
   it's nonsense, return null." Run BEFORE Layer 1 reconci lookup.
   Recovers ~25% of current not-founds. Latency cost ~300ms per name
   batched.

3. **Extend `NON_EMPLOYERS`** — add `CANDIDATE`, `PRESIDENT`, `CO-OWNER`,
   `CHARTER`, `MILES PER HOUR` and similar artifacts. Trivial change but
   moves these out of the not-found bucket entirely.

4. **Foundation-coverage extension** — ProPublica Nonprofit Explorer API
   covers IRS Form 990 filers. Cheap to integrate; would catch family
   foundations that aren't in Wikidata. Lower priority than (1)-(3).

5. **Dollar-weighted hit-rate metric** — current "hit rate by count"
   treats a $500M employer the same as a $12K one. Dollar-weighted
   metric is what matters for the user's actual use case ("trace
   political money to corporations"). The top 100 employers probably
   represent 70-80% of corporate-attributable money; if those resolve,
   coverage-by-dollars is high even at moderate count-coverage. Adding
   this metric to the validation harness would show a different (much
   better) picture than the current count-only number.

## What we're NOT doing

- ❌ Adding more filter rules. The simplification deleted the filter
  layer; we're not bringing it back in any form.
- ❌ Adding more YAML overrides for false positives. The few legitimate
  override cases (NEA → teachers union) can come back as data files
  IF AND ONLY IF the false-positive cost is material and surfacing in
  user-facing reports.
- ❌ Maintaining a Q-id classification taxonomy. Wikidata's ontology
  is the source of truth; we use what reconci.link returns.

## Open questions

1. **How much of the not-found is concentrated in small-$ employers vs
   high-$?** A dollar-weighted view changes priorities. If most not-found
   employers are <$50K aggregate, the long tail isn't worth OpenCorporates
   spending; if a few are >$1M, OpenCorporates pays off quickly.

2. **For "show the path" downstream UI**: provenance is already in
   `ResolutionResult.alternatives`. We could surface "this resolution
   was via GLEIF / Wikidata / not-found because XYZ" in the candidate
   detail page. Worth doing once the resolver layers stabilize.

3. **What's the smallest LLM that handles the normalization task well?**
   joi has Qwen3.5-122B-A10B. The task ("canonicalize messy company
   name") might work on a much smaller model — worth a quick A/B if
   latency is a concern.
