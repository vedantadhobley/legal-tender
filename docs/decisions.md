# Decisions Log

Append-only record of architectural and operational decisions, ordered by date. Earlier entries (Feb 6-7, 2026, originally `PIPELINE_FIXES.md`) are the data-model decisions for the funding-channels work. Later entries are the professionalization-effort decisions.

When a non-obvious choice gets made, append a dated entry here with: what we decided, what we considered, why we picked the chosen path, and what still leaves open.

---

## 2026-05-16 — The pipeline was confidently misrepresenting candidates with thin FEC data; weekly sync turned on; donor_detail_coverage now surfaced

Session triggered by a user question: "what about Adam Hamawy in NJ-12, I think he's clean." The pipeline confidently reported Hamawy as `99.4% grassroots, 0.4% whale, 0% unaccounted, $544K direct funding`. **Every part of the breakdown was made up.** We had zero itemized donor records for him; the pipeline silently fell back to FEC's summary totals and dumped the whole amount into "grassroots" as if that were a real classification.

### What we'd actually built vs what we'd actually shipped

By the time this session started we had:

- A working two-phase trace algorithm with cycle break + multiplier caps
- A simplified Wikidata resolver (reconci.link → GLEIF) with comprehensive _NON_EMPLOYER_QIDS reject set
- Phase 3.5 same-entity merge consolidating Pan Am Systems + Railways into one Mellon entity ($923M)
- Trade-class P31 refinement, recursive IE trace, parent-org inheritance generalization
- A four-gate validation contract (bulk median |Δ| 2.4%, named-candidate diff, target case, pytest)

What we had NOT built:

- A data-freshness mechanism that actually ran
- Honest surfacing of when our donor-level detail is missing vs present
- Validation that distinguishes "our totals match FEC's totals" from "we have donor detail to back our totals"

The validation methodology had a hole. The bulk median |Δ| check compared our `funding_channels.total_funding` to FEC's `weball.TTL_RECEIPTS`. For candidates with no itemized records, our number literally *is* FEC's number copied through (via the `total_from_individuals_source='fec_summary'` fallback in `committee_receipts`). The check was tautological exactly for the cases that needed checking.

### The three bugs

**Bug 1 — Data was 5+ weeks stale.**

Last sync 2026-05-07. Raw FEC files from 2026-05-06. `indiv.zip` in our DB only covered through 2025-12-31 (Q4 2025); Q1 2026 filings (deadline April 30) weren't ingested. The bug isn't that the FEC parser was wrong — the bug is that *nothing was scheduled to refresh the data*.

Root cause: `src/schedules/__init__.py` had `default_status=STOPPED`. The original design said "manual start in dev, flip on in prod" — and no prod deployment ever happened. The schedule sat in STOPPED indefinitely.

Compounding root cause: `src/schedules/` was being matched by the too-broad `.gitignore` pattern `schedules/` (intended to ignore Dagster's runtime schedule-storage directory). The source-of-truth schedule file was never tracked in git, never appeared in PR review, never got challenged. Nobody could see that STOPPED was the default because git was hiding the file.

**Bug 2 — fec_summary fallback silently created fake whale/grassroots splits.**

When `committee_receipts` doesn't have itemized indiv records for a committee, it falls back to FEC's webl/weball summary totals. That fallback is correct (the alternative is reporting $0 total receipts, which is worse). But:

- `whale_donor_total = 0` (no records to identify whales from)
- `small_donor_total = total_from_individuals` (entire amount → grassroots by default)
- `donor_detail_coverage` was never computed or surfaced

Downstream, `candidate_funding` produced output that looked identical for a candidate with full indiv detail vs a candidate with zero indiv records — same shape, same fields, same percentages. The output was structurally incapable of expressing the difference.

Hamawy's case: $544K showed as "99% grassroots." Zero itemized records existed. The grassroots number was a relabeled summary.

**Bug 3 — `unaccounted: 0.0%` lied.**

Because total_receipts (from summary) matched total_accounted (the same number routed to grassroots), the unaccounted residual was 0 — implying perfect attribution. The actual uncertainty (we have no donor-level detail for the entire amount) wasn't anywhere in the output. `unaccounted` was reading the data-flow tautology as confidence.

### Fixes shipped this session

1. **Weekly schedule turned on by default.** `weekly_fec_refresh` now reads `DAGSTER_SCHEDULES_ENABLED` env (default RUNNING; opt-out via 0/false/off/no). Cron `0 2 * * 0` America/New_York. Goes live this Sunday 2 AM.

2. **`.gitignore` exception for `src/schedules/`.** Force-added the file. Future schedule changes are PR-reviewable.

3. **`individuals.data_quality` block in candidate_funding output.** Per-cycle and aggregate. Fields:
   - `detail_coverage` — fraction of individuals total backed by itemized records
   - `individuals_itemized` — dollars from indiv.zip
   - `individuals_summary_only` — dollars from FEC summary fallback
   - `primary_source` — `indiv_zip` | `fec_summary` | `mixed` | `unknown`

4. **view_candidate.py warning banner** before Ch4 breakdown when coverage < 99.9%. Names the dollar amount that's summary-only and the primary source.

5. **Manual sync kicked off.** `data_sync` re-ran 2026-05-16; got fresh `indiv.zip` (~60MB larger). FEC parsers re-materializing as this is written. Then candidate_funding + validation.

### What's NOT yet fixed

- The `unaccounted` field still says 0% for summary-only candidates. Should be expanded to include the `individuals_summary_only` dollars as "uncertain attribution" — but that's a follow-up; this commit focused on making the data quality visible, not yet on changing what `unaccounted` reports.

- Validation methodology still compares totals to totals. Needs a separate gate: "for candidates with ≥N itemized records, our whale/grassroots breakdown matches the records." Otherwise the bulk-median gate continues to be tautological for thin-data candidates.

- Per-candidate data freshness (max TRANSACTION_DT under each candidate) not surfaced. Currently you have to query the DB to find out which cycle's data is "current" for a given candidate.

- The schedule turning on doesn't mean the pipeline succeeds. Need monitoring/alerting on the Sunday job: did data_sync complete, did indiv parse, did committee_receipts update? Otherwise we discover a stuck pipeline only when someone notices the data is stale again.

### The non-code lesson

This is the second time in the project history that "the validation passed" gave false confidence. The first was the trace algorithm producing $16T totals for House races (May 2026, fixed via cycle break + caps). Both times the validation methodology was missing the failure mode it was supposed to catch.

When validation is too narrow, "passing" is worse than "failing" — it converts an open problem into a closed one in our heads, and we stop looking. Worth being more honest about what each validation gate actually proves (totals vs detail vs attribution vs coherence) instead of treating "all gates pass" as a synonym for "the system is correct."

---

## 2026-05-14 — Generic-string rejection + same-entity merge for corporate_families

Session focused on cleaning up two classes of misresolution visible in `by_organization` cross-cuts:

1. Wikidata's reconci.link returning the wrong kind of entity for short/ generic FEC employer strings — "TARGETED VICTORY" → corp (actually a digital ad agency), "PRESIDENT" → corp (a job title FEC donors typed in the employer field), "United States Department of the Army" / "State of Nebraska" → corp (federal-agency / state-name leakage), "Asana Journal" → wrong Q-id for a generic word.

2. Genuinely-same real-world entities resolving to distinct Wikidata Q-ids and showing up as separate corporate_families. Pan Am Systems ($616M) and Pan Am Railways ($308M) are both Timothy Mellon entities; showing them as $923M one Mellon entity is what the data actually means.

Three commits this session: `eb38496` (audit-derived terminal-node cleanup, shipped earlier in the arc), `c47543a` + `0a7c334` (P31 reject set evolution), `fd2c7d1` (P749 same-entity merge).

### Audit-derived terminal-node cleanup (commit `eb38496`)

Four principled fixes from the 2026-05-13 terminal-node audit:

- `" SEPARATE SEGREGATED FUND"` added to `_CMTE_NAME_SUFFIXES`. SSF is FEC's own legal term for the PAC arm of a corp/union; recognizing FEC vocabulary, same shape as other suffix entries. Catches AANA + similar.
- Prefix-stripping for "POLITICAL ACTION COMMITTEE OF THE X" / "PAC OF X" in `_pac_search_name`. Symmetric to existing suffix-stripping; FEC has two equivalent naming conventions. Catches AAOS.
- Phase 1b stale-flag cleanup pass clears `terminal_type_refined_from_m_org_wikidata` / `terminal_type_wikidata_*` at start. Prevents IFW/ASIS-style cases where a re-classification doesn't take because the flag from a prior run persists.
- Phase 2a CONNECTED-match relaxed to prefix-match (≥10 chars guardrail) for super_pac_unclassified → labor_union inheritance. Catches UFCW SPAC ($20M), UNITE HERE PAC ($27M), IUOE SPAC ($20M), USW WORKS ($9M), CA Nurses PAC ($8M) — ~$85M proper org-rollup.

Each fix passes the four-gate contract.

### Resolver discipline shift: from reactive list-growth to comprehensive categorical reference data (commits `c47543a` + `0a7c334`)

The initial fix for the misresolutions was reactive: add a list of "bad" Q-ids to reject when seen as top reconci hit. This was the right *shape* (using Wikidata's own classification to gate acceptance) but the *application* was wrong: I added entries one-at-a-time as new wrong matches surfaced. The user pushed back hard:

> "make sure that your solutions arent arbitrary hardcodes"
> "it seems like we're hardcoding again"

The principled resolution: **comprehensive categorical reference data, built once up front by enumerating Wikidata's relevant taxonomy categories, not grown reactively per-case.**

Concretely, `_NON_EMPLOYER_QIDS` expanded from 12 ad-hoc entries to ~35 entries organized in 9 categories matching Wikidata's own ontology:

- Government / political entities (Q35657 US state, Q910252 head of state, etc.)
- Sovereign-state / country (Q6256, Q3624078)
- Administrative-territorial / settlement (Q15642541 admin region, Q486972 human settlement, Q515 city, Q3957 town, Q532 village, Q15284 municipality)
- Offices, positions, occupations (Q17279032 federal department, Q4164871 position, Q12737077 occupation, Q28640 profession, Q11488158 administrative occupation)
- Creative works (Q11424 film, Q571 book, Q7725634 literary work, Q47461344 written work, Q5398426 TV series, Q21191270 TV series episode, Q482994 album, Q7366 song, Q386724 work, Q7889 video game, Q13442814 scholarly article, Q5633421 scientific journal)
- Concepts / abstract (Q34770 language, Q11879003 academic discipline, Q101352 family name, Q133327 patronymic, Q11173 chemical compound, Q12140 medication, Q134808 polysaccharide)
- Geographic features (Q23397 lake, Q4022 river, Q8502 mountain)
- Wikimedia administrivia (Q4167410 disambiguation page, Q4167836 category, Q13406463 list article)
- Humans (Q5 — top reconci hit being a person means the resolver should NOT attribute the donation to that person as if they were a corporation)

Plus an empty-types structural rule: if reconci returns a Q-id with no P31 classification at all, that's "an entity Wikidata has but hasn't classified" — too risky to accept as a corporation. Single rule, not a list. Method tagged `empty_types_no_classification` in the cache.

The Q5 (human) inclusion required making `_accept_candidate` parametric so the whale path (which DELIBERATELY wants human Q-ids as input — that's how we resolve "JAN KOUM" → companies he founded) doesn't get broken. New signature: `_accept_candidate(input_name, candidate, reject_types: frozenset = frozenset(), require_typed: bool = False)`. Employer path passes `reject_types=_NON_EMPLOYER_QIDS, require_typed=True`. Whale path passes the defaults.

Also: removed the "fall through to candidates[1] if candidates[0] rejected" logic. It generated absurd worse-than-reject fallbacks: "CEO" → "Clinical and Experimental Otorhinolaryngology" (real Wikidata journal Q-id), "Department of the Army" → "badges of the United States Army" (Q-id about military insignia). When the top hit is rejected, we return not_found rather than scraping the bottom of the candidate barrel. Lower hit rate, much higher precision — for FEC-corporate-attribution use the right trade-off.

GLEIF Layer 2 needed a parallel filter: when reconci rejected via these methods, the rejection should propagate; without it, GLEIF was rescuing some names with strict-match (USA → "United States" via GLEIF's strict_match_us). Added `_REJECTION_PREFIXES` filter at the resolve_batch boundary so GLEIF doesn't see those names.

The asset (`wikidata_resolution.py`) also gained a broader rejection-prefix list at the Phase 2 assemble step to drop these from corporate_families entirely (vs preserving them as not_found).

**Discipline takeaway**: when a categorical filter is the right structural answer, build it comprehensively by enumerating the relevant taxonomy, NOT reactively per-case. The categories themselves are reference data — generic, principled, bounded. Adding entries within an existing category as new patterns surface is acceptable; growing a list of one-off Q-ids per misresolution is not.

### Same-entity merge via shared Wikidata upstream Q-id (commit `fd2c7d1`)

Pan Am Systems (Q7129582) and Pan Am Railways (Q2048811) both have P112 (founder) = Q7807399 (Timothy Mellon). They're the same real-world Mellon entity. Without merging, by_organization shows them as $616M + $308M as if they were unrelated; with merging it shows $923M one Mellon entity, which is what the data means.

Mechanism (Phase 3.5 in `wikidata_corporate_resolution`):

1. For each corporate_family with a Wikidata Q-id, fetch P112 (founder), P127 (owned by), P749 (parent organization) upstream Q-ids.
2. Group families by shared upstream Q-id.
3. Apply guardrails; union-find for transitive clusters.
4. Pick canonical (highest total_influence), fold others' totals + member_employers + linked_whales in, record `merged_from` provenance.

**Guardrails** (each one rejects a real-data false positive surfaced in the 146-cluster dry-run):

- **n=2 only.** n≥3 catches "located in USA" (Q30) with 64 unrelated megacaps clustered, "founded by Elon Musk" (Q317521) with OpenAI/SpaceX/X/PayPal/Tesla clustered. Founder/HQ/exchange-listing are *not* same-entity signals.
- **Shared name prefix ≥6 chars after legal-suffix strip.** "Pan Am" / "Pan Am" = 7 ✓. "Citadel" / "Citadel" = 8 ✓. "Bain " / "Bain " = 5 ✗ (intentionally rejects Bain Capital + Bain & Company — same founder, distinct PE vs consulting firms).
- **Educational-institution exclusion.** Both names containing University/College/School → reject. Otherwise university campuses sharing a parent system (UIUC + UIC under "University of Illinois system") would over-merge.

**Considered alternatives:**

1. **Shared P749 (parent org) only — most strict.** Rejected because Pan Am's connection is via P112 (Mellon = founder), not P749. Loses the headline win.
2. **Match-canonicalize via Wikidata's `wd:parentItem` SPARQL closure.** Equivalent to walking parent orgs to a fixed point. Too aggressive — would walk Microsoft + Google + IBM up to "American multinational technology corporation" and try to merge.
3. **LLM-based same-entity detection on top-N candidates.** Doable on joi, but adds a moving part for a problem that's mostly solved with shape rules.
4. **Hand-curated `EMPLOYER_FAMILY_ALIASES` table.** Already exists; doesn't scale.

**Results, this run**: 11 merges. Headline Pan Am ($923M one Mellon entity). Clean wins: Bloomberg TV/Beta, Marvel Comics+Entertainment+Games (union-find), DreamWorks+Animation, Rocket Companies+Mortgage+Loans, Hilton, Coca-Cola, Capitol Records. Two minor false positives accepted: Universal Music Group + Universal Television (~$1M, different corporate trees — UMG ≠ NBCUniversal but the rule can't tell), Samsung Electronics America + Samsung Heavy Industries (~$1M, genuinely distinct subsidiaries that share parent corp).

**NOT caught** (no shared upstream — name-only variants): GREYLOCK + Greylock Partners; Adelson Drug Clinic + Adelson Clinic; ULINE INDUSTRIES + ULINE. These keep the `EMPLOYER_FAMILY_ALIASES` table relevant. Future path: name-similarity-only merger pass operating on canonical_employers, OR delete EMPLOYER_FAMILY_ALIASES if Adelson/Uline get proper Wikidata coverage.

**Cache**: `<cache_dir>/wikidata_upstream.json`, schema `{qid: {p112, p127, p749, fetched_at}}`. Incremental flush every 200 entities, atomic .tmp rename.

**Validation (all four gates pass):**

- Gate 1 (bulk median): 2.4% unchanged from baseline (merge changes WHO gets credit, not HOW MUCH — total receipts are conserved).
- Gate 2 (named-candidate): Trump by_organization shows Pan Am Systems at $19.89M consolidated (was 2 separate entries). Other top orgs (Department of Government Efficiency / Musk, U Line / Uihlein, Marvel Entertainment / Perlmutter, America First Policies / McMahon, Budget Suites / Bigelow) unchanged. Bacon, Sanders also stable.
- Gate 3 (target case): Pan Am surviving family doc has `merged_from: [{name: "Pan Am Railways", qid: "Q2048811"}]`.
- Gate 4 (pytest): 65/65.

**Open follow-ups:**

- The Universal Music/TV and Samsung Electronics/Heavy false positives could be filtered by examining whether the shared upstream Q-id is itself a "corporate holding company" type vs a "founder" type. P112 (founder) tends to over-merge sibling companies founded by same person; P749 (parent org) tends to over-merge subsidiaries that share a holding company. Future work: track which property the upstream came from and apply different thresholds.
- The dry-run found Citadel Enterprise Americas LLC + Citadel Securities as a candidate merge, but only Citadel Enterprise existed in this run's corporate_families (Citadel Securities was resolved to a different canonical or below threshold). Not a regression; just a coverage gap.

---

## 2026-05-12 + 2026-05-13 — Terminal-node fixes + view tool + classification refinement

Two-session arc focused on (a) making the terminal-node classification layer more trustworthy and (b) finally making the pipeline's output human-viewable. Validation methodology formalized in @docs/validation.md.

### 2026-05-12 morning — Baseline snapshot

`docs/audit/baseline-2026-05-12.md` captures the known-good comparison point every subsequent fix diffs against:

- Bulk validation: 11,567 candidate-cycle comparisons, median |Δ| = 2.4%, 65% within ±5%, 77% within ±10% of FEC `weball.TTL_RECEIPTS`.
- BWC sanity (Bacon NE-2) across all 4 cycles: -2.7% to -5.8%, consistent.
- Named-candidate top-15 `by_organization` for Cruz, Trump, Harris, Bacon, Sanders.
- terminal_type distribution: 16,587 campaign / 9,339 passthrough / 6,277 super_pac_unclassified / 2,179 corporation / 809 trade_association / 427 ideological / 394 labor_union / 57 cooperative / 31 unknown.
- Top 20 `super_pac_unclassified` by receipts: SLF PAC $1.14B, SMP $1.11B, MAGA Inc $521M, FAIRSHAKE $358M, AMERICA PAC $311M, DEMOCRACY PAC $279M, Club for Growth Action $263M, AIPAC's UDP $209M, etc. Cumulative ~$6.3B+ that was previously terminating the trace at these committees.

### 2026-05-12 — Phase 2a/2b parent-org inheritance generalization

The May-9 NEA-Fund inheritance fix was narrow: `corporation`-typed committees inherit from a sibling matched by `CONNECTED_ORG_NM`. Generalized:

- **Phase 2a**: source filter expanded to `corporation` OR `super_pac_unclassified` OR `unknown`. Same parent-lookup by `CONNECTED_ORG_NM`, with FEC's literal-string 'NONE' value treated as empty.
- **Phase 2b (new)**: when CONNECTED is empty/NONE, derive a `root_name` by stripping known committee-form suffixes (POLITICAL ACTION COMMITTEE, CONGRESSIONAL FUND, VICTORY FUND, INSTITUTE FOR LEGISLATIVE ACTION, ACTION, PAC, OF AMERICA, etc.). Cluster by root_name. Within each cluster of ≥2 members, if any has a specific terminal_type, all loosely-typed siblings inherit. Conservative min_root_length=8 prevents stripping-to-junk over-clustering.

52 committees ($350M in receipts) reclassified. Target cases all hit:
- Club for Growth Action ($263.5M): super_pac_unclassified → ideological (cluster=CLUB FOR GROWTH)
- NAR Congressional Fund ($60M): super_pac_unclassified → ideological (CONNECTED match)
- NRA Institute for Legislative Action: super_pac_unclassified → ideological (cluster=NATIONAL RIFLE ASSOCIATION)
- Communications Workers of America Working Voices ($35M): super_pac_unclassified → labor_union
- No Labels Action ($3.5M), Gun Owners of America ($1.1M), Moms for Liberty Action ($500K), American Chemistry Council: smaller flips

### 2026-05-12 — Recursive IE trace through passthrough Super PACs

`trace_ie_sources` was single-level: walk one hop upstream of the spending PAC, attribute terminal-type committees to by_corporation, dump everything else (including passthrough PACs) into by_pac. Stopped at the first passthrough — and because the dominant funding pattern post-Citizens-United is Super PACs funded by other Super PACs (SLF Fund ← One Nation ← donors), the *actual* donors disappeared into by_pac as opaque line items.

Made it recursive with the same Phase-1-propagate + Phase-2-attribute structure as `trace_committee_sources`. Same correctness guards (cycle break, per-edge fraction cap at 1.0, accumulated multiplier cap at 1.0). by_pac now only collects passthrough PACs whose receipts are zero/missing (rare — "trace ended here" signal rather than silently dropped attribution).

Aggregate effect:
- IE+ resolved to by_corporation: 22.3% (~$840M across all candidates)
- IE+ stuck in by_pac: 0.0%
- IE- resolved to by_corporation: 26.6% (~$1.73B)
- IE- stuck in by_pac: 0.1%

Also surfaced `by_individual` in the output (the recursive trace's individual attributions were happening internally but not written to the candidate document pre-fix). New `_top_individuals` helper preserves {name, amount, employer}; `_merge_individuals_list` for cross-cycle merge.

### 2026-05-12 — `scripts/view_candidate.py`

~470 LOC, `rich`-based terminal renderer. First time the pipeline's output is viewable by a human without writing an ad-hoc AQL query. CLI: `python view_candidate.py "<name or CAND_ID>" [--cycle YYYY] [--top N]`. Renders header, channels summary, Ch1 by type, Ch2/3 with top_pacs/by_corporation/by_individual/stuck-by_pac, Ch4 whale corp-connected + independent + grassroots, Ch5 unaccounted with breakdown, by_organization cross-cut with via_donors. Disambiguates name substrings to a CAND_ID list when ambiguous.

Visible output-quality issues surfaced (not blockers; expected per the resolver's documented trade-offs):
- "TARGETED VICTORY" misresolved as a corporation (it's a campaign-services firm)
- "Asana Journal" instead of "Asana"
- "PRESIDENT" / "United States Department of the Army" / "State of Nebraska" as org names (job-title / geographic leakage from FEC employer fields)

These are documented in @docs/validation.md "What we haven't validated" and `docs/todo.md` "Session 2 follow-ups."

### 2026-05-13 — M-ORG_TP refinement: token list → Wikidata P31

Initial implementation used a 50-entry hand-curated `_TRADE_PROFESSION_TOKENS` list (REALTORS, BANKERS, MANUFACTURERS, DENTISTS, ...) to refine ORG_TP=M committees from the catch-all `ideological` bucket to `trade_association`. Worked ($215M flipped, including AAJ, Council of Insurance Agents) but Vedanta correctly identified it as the exact "growing hardcoded list" shape we've been removing all session.

Considered alternatives:
1. Keep the list as reference data (same shape as `_STOPWORDS` / `_LEGAL_SUFFIXES`)
2. **Wikidata P31 lookup via reconci.link** ← chosen
3. LLM-based classification via Qwen on joi
4. Wikidata SPARQL with type filter (rejected — reintroduces the spaghetti we deleted on 2026-05-11)
5. OpenCorporates' industry codes (rejected — paid tier, single-source)
6. Collapse the distinction entirely

Chose option 2 because:
- Same infrastructure as employer/whale resolvers (no new dependency)
- Deterministic (same input → same output)
- Provenance is auditable (Q-id + class name stored per refined committee)
- The "list" is ~10 Q-ids from Wikidata's own taxonomy of professional-org types, all empirically observed in our corpus — different shape from the eye-curated string list

Implementation:
- For each Phase-1-classified `ideological` ORG_TP=M committee, derive a search name by stripping parenthetical content + PAC suffixes
- Reconcile via reconci.link, cached at `<cache_dir>/trade_assoc_classification.json` (schema-versioned: cache stores full P31 type list, `is_trade` derived at read-time so set evolution doesn't require cache invalidation)
- Top candidate's P31 types checked against `_TRADE_CLASS_QIDS`. If any match → flip.

`_TRADE_CLASS_QIDS` set (all empirically observed):
- Q2178147 trade association
- Q829080 professional association
- Q10729872 medical association
- Q4287745 medical organization
- Q1865205 bar association
- Q897399 chamber of commerce and industry
- Q18325460 501(c)(6) organization (US tax code for business leagues)
- Q16904718 agricultural organization
- Q114301854 veterinary medical association
- Q70363673 pharmaceutical societies

Run result: **33 committees ($116.7M) refined**. Coverage is ~$50M smaller than the token-list approach was, because Wikidata classifies AAJ ($24M trial lawyers) and Council of Insurance Agents ($17M) as `advocacy group` (Q431603) rather than trade. Defensible — those orgs do as much advocacy as trade representation; deferring to Wikidata's judgment is more principled than my eye-curated list.

Coverage gap accepted explicitly: AOPA / NFIB / BCBS Michigan / APTA / ACOG (~$30M total) — real trade orgs but Wikidata's P31 doesn't include any of our trade-class Q-ids. They stay ideological. Better default (under-flip rather than over-flip).

### Validation methodology documented

`docs/validation.md` written as the canonical reference for the four-gate validation contract:

1. **Bulk median** vs FEC weball.TTL_RECEIPTS (≤5% threshold; currently 2.4%)
2. **Named-candidate diff** for Cruz / Trump / Harris / Bacon / Sanders top-15 by_organization
3. **Target case** explicit per-fix pass/fail
4. **Pytest** 68/68

Plus catalog of what we've validated, what we haven't, and how to run each gate. All five fixes in this two-session arc passed all four gates.

---

## 2026-05-11 PM — Multi-signal corroboration + whale-path simplification + audit-driven deletions

Three commits this afternoon, building on the morning's resolver simplification.

### Commit 1 — `feat(resolver): multi-signal corroboration for low-confidence reconci matches`

Reconci.link returns a single 0-100 relevance score. Trusting only the high end (≥70) misses real matches that are under-scored because the FEC employer is a contraction or acronym of a much longer canonical name (WILMERHALE → "Wilmer Cutler Pickering Hale and Dorr" at reconci 49). Trusting the high end uncritically lets short-input fuzz matches through (RDV → "North Vietnam" at score 100, the poster-child false-positive).

Built `src/rag/name_match.py` with four deterministic signals:
- `acronym_match` (tries both with-and-without stopwords so "BCG" → "Boston Consulting Group" works AND "WH" → "Wilmer and Hale" works)
- `token_subset_concat_match` (in-order subset concatenation for portmanteau cases)
- `edit_distance_close` (Levenshtein ≤ 2 for typos)
- `token_containment_score` (bidirectional substring coverage)

Decision rule in `wikidata_resolver._accept_candidate`:
- score ≥ 70 → accept, unless input is ≤4 chars AND no signal fires
- 40 ≤ score < 70 → accept iff at least one signal fires
- score < 40 → reject

No LLM, no hardcoded case overrides — each signal is a general property of FEC-name variation. Live smoke: RDV correctly rejected via `short_input_no_corroboration`; WILMERHALE / GREYLOCK / BCG / BAUPOST GROUP all resolve. 42 unit tests added (microsecond-fast, no network).

### Commit 2 — `feat(whale): minimal person→company resolver, drop legacy filter chain`

The whale path (person-name → companies via P108/P1830/P39) was the last consumer of the pre-simplification filter chain in `wikidata_client.py`. Same architectural overhaul as the morning's employer path:

1. Reconcile name via reconci.link (no type filter — empirically reliable for distinctive person names)
2. Accept via the same HIGH/LOW threshold + corroboration rule used for employers
3. Extract company Q-ids from a fixed set of corporate-relationship properties: P1830 (owner of), P112 (founder), P169 (CEO), P488 (chair), P1037 (manages), P3320 (board member), P108 (employer), and P39 (position held) with the P642 ("of") qualifier
4. Fetch each company's English label via existing `_entity_data`

**Adding P1830 was the key recovery.** Kenneth Griffin's connection to Citadel is stored there, not P108. Wikidata's coverage of US hedge-fund founders is uneven; P1830 catches Musk/Adelson/Griffin while P108 catches Koum/Mellon/Koch.

`wikidata_client.py` shrank 416 → 150 LOC after deleting `_resolve_person_rest`, `_resolve_person_safe`, `resolve_people_rest`, `_should_reject_match`, `_is_generic_match`, `_is_government_entity`, the description-pattern constants, the 60-entry `_NON_CORPORATE_P31` set, and the parallel-worker helpers.

**Disambiguation gap accepted explicitly.** "JOHN ARNOLD" and "PAUL SINGER" disambiguate to wrong-person Wikidata entries (historical/religious figures rank higher than the hedge-fund founders). Same disambiguation problem OpenCorporates Layer 3 would address; deferred until that ships.

### Commit 3 — `chore: hardcode + dead-code audit, delete confirmed-dead modules`

Audit doc `docs/audit/hardcodes-2026-05-11-pm.md` catalogs every remaining hardcoded constant in `src/rag/` with keep/trim/delete verdicts. Executed the safe deletions from section 7:

- `src/resources/embedding.py` (320 LOC) — `EmbeddingResource` was registered in `Definitions(...)` but no asset declared it as a dep or called `embed_*`. Pure facade.
- `src/api/election_api.py` (67 LOC) — placeholder, never wired.
- `_wbsearchentities` in `wikidata_client.py` (20 LOC) — last caller was the legacy whale path deleted earlier in the session.
- `WIKIDATA_API_ENDPOINT` constant — only used by the deleted `_wbsearchentities`.
- `_FOUNDER_POSITION_QIDS` empty placeholder set in `whale_resolver.py`.
- `ORG_TYPE_QID` constant + 18-line dead comment about the long-deleted `wikidata_ontology` module.

~440 LOC net removed. 68 tests still pass.

### Honest summary of where the day landed

Three valid criticisms:
- We spent a full day on infrastructure (resolver simplification, audit, dead-code purge) with zero user-visible feature progress.
- Hit rate on corporate resolution sits ~55-65%. OpenCorporates Layer 3 + LLM input normalization would close most of the long-tail gap; both deferred.
- `candidate_upstream.py` is still 1,263 LOC and `pies_v3.py` is still in the tree at 679 LOC.

Three real wins:
- The resolver pipeline is now ~500 LOC of principled code (`wikidata_resolver.py` + `whale_resolver.py` + `name_match.py`) instead of ~1,800 LOC of filter-chain band-aids.
- Bulk validation median |delta| is 2.5%, 62% within ±5% of FEC published totals. The pipeline is more correct than it feels.
- The next session has a concrete plan with validation gates (see `todo.md` "Next session"): extend the parent-org-inheritance pattern, stop treating `super_pac_unclassified` as terminal, and ship a thin CLI to actually look at `funding_channels.by_organization`.

---

## 2026-05-11 — Corporate-identity resolution: delete the filter layer, accept reconci-top + GLEIF

**The 24-hour summary.** Spent a full day iterating through three increasingly elaborate filter architectures on top of the Wikidata reconciliation API:

1. Q-id whitelist (`_STRICT_CORPORATE_QIDS`) + description-pattern blacklist + suffix-retry list. Grew with every wrong match.
2. Ontology walker — replace the Q-id whitelist with a P279 (subclass-of) traversal that asks Wikidata itself "is this Q-id descended from an organization root, or from a non-employer root?" Required maintaining short subtree-root sets (ORG_ROOTS, NON_EMPLOYER_ROOTS) but grew the latter set each time a test exposed an ambiguous case (municipality, state, etc.).
3. Test suite — built a 79-test pytest fixture to catch corruption in seconds instead of after 90-minute full runs. Caught real bugs (Q22687 bank misclassified, parallel-walk cache corruption, mixed-ancestry walks producing non-deterministic results across cache states).

After all of this: the filter layer was fundamentally fighting Wikidata's classification. Wikidata thinks municipalities are organizations; our domain doesn't. Wikidata thinks the village of "Greylock" is at search-rank parity with the firm "Greylock Partners"; our domain wants the firm. Every new "fix" was a band-aid on top of a band-aid. We had several "this is the architectural fix" pivots in the same day, each one introducing new edge cases.

**The actual fix: stop classifying.** Delete the entire filter layer. Replace with:

1. Wikidata reconci.link top hit at `score >= 70` → accept
2. GLEIF strict-match fallback for not-founds → accept
3. Everything else → `not_found`

That's it. No `_NON_CORPORATE_P31` (60 entries — deleted). No `_GENERIC_DESCRIPTION_PATTERNS` (25 entries — deleted). No `_GOVERNMENT_DESCRIPTION_PATTERNS` (deleted). No ontology walker (deleted entirely — `src/rag/wikidata_ontology.py` gone). No YAML overrides (`config/wikidata_overrides.yaml` deleted). No subtree-root sets, no P279 traversal, no cache-corruption-recovery scripts.

Code: ~550 LOC of resolver → ~150 LOC. Tests stayed (~18 cases, 6 seconds). All 18 pass.

**Trade-off accepted explicitly.** We will accept some bogus matches that Wikidata's data quality produces:

- `RDV` → "North Vietnam" (score 100 fuzzy match)
- `STEYER` → "Steyr" (Austrian city, score 100)
- `CO-OWNER` → some unrelated entity

These show up in downstream `by_organization` cross-cuts where they're inspectable. For the FEC-corporate-attribution use case, missing a corporation (false negative) is worse than misattributing one (false positive — visible and recoverable). The earlier filter chain was tuned for precision over recall and silently rejected real entities (`LINKEDIN`, `BAUPOST GROUP`, `BALLMER GROUP` — all returned at score 100 from reconci, all wrongly rejected by the type filter).

**Architectural pattern documented**: `docs/corporate-resolution.md`. Layered fallback (knowledge graph → official registry → extension slots) with confidence threshold + provenance for review. Portable to other entity-resolution problems.

**Hardcode audit shipped**: `docs/audit/hardcodes-2026-05-11.md`. Each remaining hardcoded data structure classified as legitimate reference data (legal-form suffixes, non-employer placeholders), calibrated parameter (confidence threshold), or dead code targeted for deletion (~600+ LOC of the old filter chain still sits in `wikidata_client.py` waiting for removal).

**Open follow-ups deferred from earlier filter-era plans**:
- ~~Cache TTL enforcement~~ — the new resolver doesn't have the same cache-corruption pressure; the cache is just a per-name resolution result. TTL still useful but lower priority.
- ~~Re-walk False entries to fix corruption~~ — moot, ontology cache deleted.
- ~~OpenCorporates as Layer 2~~ — still on the table as Layer 3 for coverage extension. ~200M companies including most US private LLCs. Free tier 500/day; paid for production scale.
- LLM-based input normalization for garbled FEC names. Future Layer.

**Acceptance metric for "no regression on resolver simplification"**: 18-test pytest suite green. Full 5K validation deferred — the elaborate filter layer it was meant to validate is gone, and the trade-off is now explicit.

---

## 2026-05-10 — Wikidata corporate-identity resolution: pivoting from band-aid filters to reconciliation-API + OpenCorporates

**Context.** Corporate identity resolution is structurally central to legal-tender — the whole project is "trace dollars to corporate origins," and without correct mapping from FEC employer strings to corporate identities, the `whale → corporation` claim that powers `by_organization` cross-cuts is unreliable. Over this session we pushed hit-rate from ~600 (pre-this-session, mostly via SPARQL UNION queries that the WDQS endpoint couldn't reliably serve) to ~3,049 wikidata-resolved out of 5,054 canonical employers (60% hit-rate) by switching the primary path from SPARQL to MediaWiki REST (`wbsearchentities` + `Special:EntityData`), then layering in:

- top-N candidate filtering (`limit=5`)
- description-keyword blacklist for generic-concept matches
- government-entity description filter
- P31 (instance-of) blacklist for non-corporate types (60+ Q-ids)
- strict P31 corporate whitelist gate on suffix-retry path
- alternate-form retry (suffix stripping)
- hardcoded `_EMPLOYER_OVERRIDES` per-name forced Q-ids (NEA, CITADEL family)
- hardcoded `EMPLOYER_FAMILY_ALIASES` for entity-merge cases (ADELSON CLINIC → ADELSON DRUG CLINIC)
- bare-generic-word filter (CORPORATION, COMPANY, BUSINESS as full-string employer values)

This brought visible quality to the top-30 corporate_families and zeroed the obvious bogus matches (advocacy group, Sigmund Freud, Federal Government, Corporation video game). But it's a band-aid layer — ~250 lines of filter-shaped code across two files, growing one entry at a time as new wrong matches surface. Each new wrong match teaches us a new filter pattern. That's whack-a-mole.

**The structural mistake.** wbsearchentities ranks by string relevance over *all of Wikidata* (humans, films, books, vaccines, cities, given names, organizations, video games — everything). Then we post-filter the wrong ones. The right structure: never let non-organizations into the candidate set in the first place.

**Three architectures considered.**

1. **Bulk SPARQL fetch + local corporate index.** Pre-fetch every Wikidata entity that's `wdt:P31/wdt:P279* wd:Q43229` (organization or subclass), with labels, `skos:altLabel` aliases, `wdt:P749` parent links, and sitelink counts. Local JSON file (~100-300MB), refreshed quarterly. Lookup: exact label/alias match against pre-filtered org-only index, sitelink-count tiebreak for ambiguity. Eliminates the entire P31 blacklist (entities not in index can't be candidates), most of `_EMPLOYER_OVERRIDES` (Wikidata's own aliases catch NEA-style cases), and the suffix-retry mechanism (aliases handle suffix variants). Trade-off: 100MB local file, quarterly maintenance, a one-time bulk-fetch cost (~1-2 hrs of paginated SPARQL).

2. **Wikidata Reconciliation API** (`https://wikidata.reconci.link/`). Third-party hosted ElasticSearch over Wikidata, designed for OpenRefine-style entity reconciliation. Supports `type` filter (Q43229) and returns ranked candidates with scores and types. Same architectural fix (typed candidate space, scored matches) without the bulk fetch — query on demand. Built and maintained by Antonin Delpeuch.

3. **OpenCorporates** (`https://api.opencorporates.com/`). Authoritative corporate registry data covering 200M+ companies including small US private (where Wikidata's coverage is poor). Free tier 500 reqs/day, paid for production. Best for the entities Wikidata genuinely doesn't have (ULINE-style, FAHR-style — though our spike of reconci.link found ULINE in Wikidata after all, just not via wbsearchentities).

**WDQS health-check today (2026-05-10 03:55 UTC).** Out of 16 test queries against `query.wikidata.org/sparql`: 8 timeouts (30-60s), 5 × 502 Bad Gateway, 1 × 429, only 2 successes. Trivial liveness query (one rdfs:label fetch) timed out at 30s. We landed in the middle of WDQS migration weather (the team is migrating off Blazegraph to a different backend, with periodic multi-hour outages during the transition). Bulk SPARQL fetch is not viable today.

**Reconciliation-API spike (2026-05-10 04:00 UTC).** 20 hard cases batched in 7.3s. Findings:
- Better matching where wbsearchentities failed: ULINE → Uline (Q7879030, Wikidata DOES have it), BCG → Boston Consulting Group, BLACKSTONE GROUP → Blackstone Inc. via alias, CITADEL → Citadel LLC (no override needed), CITADEL INVESTMENT GROUP → same.
- Sanity cases all pass (Goldman, Apple, Google, IBM, Pan Am Railways).
- Still wrong: NEA → Newspaper Enterprise Association (defunct press agency) at top, same as wbsearchentities. Need irreducible override.
- Still ambiguous: KKR cricket team + Kohlberg Kravis Roberts + Federation of Mutual Aid Assocs all score 100. Need sitelinks tiebreak.
- Type filter is soft, not hard: STEYER → Steyr Austrian city scored 100 even with `type=Q43229`. Need client-side P31 verification using returned types.
- Confidence threshold needed: FAHR → Deutz-Fahr at score 57 — fuzzy match on partial token, should be rejected.
- `CORPORATION → IBM` at score 85 from fuzzy matching the literal word in descriptions — handled at normalization layer (existing `NON_EMPLOYERS`).
- True not-founds: PRATT INDUSTRIES, ADELSON CLINIC, ADELSON DRUG CLINIC.

**Decision.** Pivot to **reconciliation API as primary + OpenCorporates as fallback for not-founds**, with client-side scoring/threshold/tiebreak.

- Phase 1: Thin client around `wikidata.reconci.link` (batch up to 50 names per request).
- Phase 2: Resolver that filters returned candidates by P31 corporate whitelist, applies confidence threshold, fetches sitelinks for tiebreak when multiple score-100 candidates survive.
- Phase 3: OpenCorporates lookup as second-layer fallback for names where reconci.link returned no acceptable candidate (gated by daily-budget on the 500-req/day free tier; cache results aggressively).
- Phase 4: Wire into asset, replace `_resolve_company_rest` body.
- Phase 5: Validation harness — diff old vs new on 5K employers, categorize improved/regressed/unchanged, spot-check regressions.
- Phase 6: Delete obsolete band-aids. Targets: `_NON_CORPORATE_P31`, `_GENERIC_DESCRIPTION_PATTERNS`, `_GOVERNMENT_DESCRIPTION_PATTERNS`, `_RETRY_SUFFIX_TOKENS`, `_alternate_employer_forms`, most of `_EMPLOYER_OVERRIDES`, `_resolve_company_one_query`. Move surviving ~3-5 overrides + `EMPLOYER_FAMILY_ALIASES` to YAML data files with rationale fields.

**Pre-commit acceptance metrics.** Verify the architectural improvement is real, not feel-good:
1. Lines of "filter-shaped" code in `src/rag/`: today ~250. After: 0 in code, OK to have ≤30 lines of declarative scoring/threshold logic.
2. Hardcoded Q-id mappings: today ~12 across `_EMPLOYER_OVERRIDES` + `EMPLOYER_FAMILY_ALIASES`. After: 0 in code, ≤5 in `data_gaps.yaml` with verified rationale per entry.
3. No regressions on validation harness for the previously-correct ~3,000 wikidata-resolved employers.
4. Hit-rate same or higher than current 60%.

**Why not bulk-fetch + local index.** Considered as the primary architecture but two factors flipped the choice:
- WDQS is currently down for the migration — blocking. The reconciliation API runs on separate infrastructure (ElasticSearch) and is healthy.
- Reconciliation API gives the same architectural property (typed candidate space, ranked results with scores) on demand without the 100MB local file or quarterly maintenance.
- If reconci.link becomes flaky in production, falling back to bulk-fetch is still available — the local-index plan stays in this decisions log as a documented fallback architecture.

**Trade-offs accepted.**
- Third-party dependency on `wikidata.reconci.link` (single maintainer; if it goes down, fall back to bulk-fetch + local index).
- OpenCorporates 500-reqs/day free-tier ceiling means we can't bulk-resolve all 5K employers via OpenCorporates — only the ~10-20% that reconci.link doesn't cover. Acceptable.
- Per-name HTTP round-trip cost (~365ms in batched mode = ~30 min for 5K names). Same as today, no regression.

**Open follow-ups.**
- Sitelinks tiebreak requires entity-data fetch per ambiguous candidate — adds a few hundred more REST calls. Acceptable.
- OpenCorporates parent-company resolution — not free on the free tier, deferred.
- For lobbying integration (separate effort, see `docs/lobbying-integration.md`), the same reconciliation infrastructure will resolve LDA client names — keep this layer reusable.

---

## 2026-05-08 — Bulk validation against FEC weball + unitemized grassroots gap

After parsing FEC's `weball`/`webl`/`webk` summary files (per-candidate / per-PAC totals from FEC's own aggregation), wrote `scripts/validation_report.py` to cross-reference our `direct_funding` against `weball.TTL_RECEIPTS` for every candidate. The bulk validation revealed a systematic undercount for grassroots-heavy candidates.

**The pattern**: 100% of the worst offenders by absolute dollar delta are major Senate/Presidential candidates with massive small-donor bases. All under FEC's number, none over.

| Cycle | Top offenders (% under FEC) |
|---|---|
| 2020 | Sanders -58%, Warren -61%, Trump -40%, Biden -33% |
| 2022 | Oz -69%, Fetterman -56%, Demings -47%, Warnock -40% |
| 2024 | Trone -99%, Allred -47%, Tester -36%, Harris/Biden -26% |
| 2026 | Krishnamoorthi -67%, Kelly -66%, Talarico -57% |

Median delta across 10,634 cycle-candidate comparisons: **33%**. Only 12.5% within ±5% of FEC.

**Root cause**: We compute `total_from_individuals` by summing records in FEC's `indiv.zip`. But `indiv.zip` only contains *itemized* donations (typically donors who gave $200+ cumulative per committee per year). **Unitemized small donations are NOT in indiv.zip** — FEC reports them only as a sum in candidate filings.

For BWC (incumbent, mostly $200+ donors): negligible gap → 0.7% delta.
For Sanders (huge sub-$200 grassroots base): we miss ~$76M → 58% delta.

**Fix (in flight)**: Modify `committee_receipts` to use `weball.TTL_INDIV_CONTRIB` (for principal campaign committees) and `webk.INDV_CONTRIB` (for PACs) as the authoritative individuals total. Compute grassroots as `TTL_INDIV_CONTRIB − whale_donor_total`. Whale machinery untouched — they continue being graph-traced for corporate attribution.

**What this is NOT a fix for**:
- The whale-threshold model (already documented in `funding-channels.md` as a "high-engagement proxy" — accurate within its definition)
- The whale → employer conflation (intentional, used by `by_organization` cross-cut, not double-counting)
- Trace algorithm bugs (those were the prior fix; cycle-break + multiplier caps)

**What this exposes about our prior fix**: the cycle-break/cap fix prevented catastrophic blowup but masked this real gap. Without cross-validating against FEC's published numbers, we'd have shipped "looks plausible by magnitude" data with structural undercounts for the most-watched candidates.

**Validation harness as a permanent check**: `scripts/validate_funding_channels.py` does smoke tests (magnitudes, BWC sanity, channel-sum). `scripts/validation_report.py` does the bulk cross-reference. Run after every aggregation re-run; flag regressions.

## 2026-05-08 — Per-election limit threshold model documented as "high-engagement proxy"

The whale threshold (`PER_ELECTION_LIMITS` in `donors.py`) is hardcoded per cycle:
```python
{"2020": 2_800, "2022": 2_900, "2024": 3_300, "2026": 3_500}
```

Source: FEC publishes contribution limits at fec.gov/help-candidates-and-committees/candidate-taking-receipts/contribution-limits. Inflation-adjusted every 2 years.

**What we're computing isn't strictly "FEC max-out"**: we mark a donor as a whale if they aggregated `≥ per_election_limit` at any single committee in a cycle, regardless of whether any single election (primary, general, runoff) was technically maxed. So a donor giving $3,000 to primary + $3,000 to general at one candidate ($6,000 cycle total) is a whale per our definition; technically they didn't max either election.

This is documented in `funding-channels.md` as a "high-engagement donor proxy" — useful for graph traversal and corporate attribution. The mis-classification at the boundary is small and doesn't affect totals (they end up in `whale.independent` or `whale.corporate_connected` based on employer link, contributing the same dollar amount either way).

**Trade-offs of hardcoded values**:
- ✅ Simple, predictable, no runtime API dep
- ✅ Changes only every 2 years (trivial PR)
- ❌ Requires explicit human update for each new cycle

**Next maintenance window**: 2027 — when FEC publishes 2028 cycle limits, add `"2028": <new_limit>` to `PER_ELECTION_LIMITS` AND to `ACTIVE_CYCLES` (still hardcoded in 17 files; centralization is on the todo list).

## 2026-05-08 — Trace algorithm bug: cycle-break + multiplier caps

**Problem.** First successful aggregation run (after fixing the spent_on int/float crash and dropping the wikidata_corporate_resolution dep) produced wildly wrong totals: the top candidate at $118 quintillion, BWC's NJ-12 House race at $16 trillion (vs ~$1-3M raw indiv data suggests). Validation immediately surfaced the bug.

**Root cause.** Two compounding issues in `trace_committee_sources` (the multi-hop trace in `src/assets/aggregation/candidate_upstream.py`):

1. **No cycle detection.** The committee graph has bidirectional transfers — `A↔B` 2-cycles are common (sampled 10+ instances on first query). The trace propagates level-by-level for `max_trace_depth=8` levels without tracking visited committees, so cycles cause unbounded multiplier accumulation.

2. **No upper bound on per-edge `amount / from_receipts`.** Some committees have `total_receipts = $1` (tiny technical filings) but outgoing transfers of $10K+. Dividing produces `amount/receipts = 10,000`, blowing up a single hop's multiplier by 10,000× before any compounding.

The IE trace function (`trace_ie_sources`) already had the right guards (`min(1.0, ie_amount / total_receipts)`) and is single-level so doesn't have the cycle issue. The committee trace was the only buggy one.

**Fix.** Three guards added to `trace_committee_sources`:

1. **Track propagated-from committees.** A `propagated_from` set marks committees whose outgoing edges have already been processed. The trace skips re-processing them, breaking 2-cycles cleanly.
2. **Cap per-edge fraction at 1.0.** `edge_fraction = min(1.0, amount / from_receipts)` enforces the physical constraint that a committee can't transfer out more than it received. Handles the $1-receipts data quirk.
3. **Cap accumulated multiplier at 1.0.** `all_mults[cmte_id] = min(1.0, all_mults[cmte_id] + new_mult)` enforces the semantic constraint that no committee can be responsible for >100% of a candidate's money.

**What this is NOT.** This isn't a full algorithmic refactor. It's a minimal-edit correction. A clean rewrite would do proper fixed-point iteration with convergence detection and could potentially recover legitimate compounded mults that the cap may slightly under-report. For now we accept the small under-report as the price of preventing the catastrophic over-report.

**Side benefit.** The fix made `candidate_funding` ~3× faster (1h44m → ~50m). The previous run was wasting work iterating through pathological graph cycles to `max_depth=8`. The cycle break terminates much earlier.

**Validation harness.** `scripts/validate_funding_channels.py` was added to surface obvious algorithm bugs in seconds (top-candidate magnitude check, BWC sanity check, distribution outliers, channel-sum consistency). Run after every aggregation: `docker exec legal-tender-dev-webserver python3 /workspace/scripts/validate_funding_channels.py`.

**Open follow-ups (logged in `docs/todo.md`):**
- Replace the cap-based fix with proper fixed-point iteration with convergence detection.
- Investigate why some campaign committees have `total_receipts = $1`. Is this an FEC bulk-data quirk we should normalize at parse time, or do those committees genuinely have those records?
- Re-run wikidata_corporate_resolution after the upstream client fixes (negative cache + batched VALUES + backoff) so corporate attribution is real.

## 2026-05-08 — Brain stack architecture (Phase 2 of professionalization)

Built `~/workspace/obsidian/` as a self-hosted second-brain stack. Decisions made along the way:

### Where it lives

**Decision**: Brain stack at `~/workspace/obsidian/`, NOT under `~/workspace/monitor/`.

**Considered**: Bolting it onto the existing monitor stack (Prometheus, Grafana, Loki, etc.) since both are "always-on infrastructure I want web-accessible from any tailnet device."

**Why separate**: monitor is observability (read-only metrics, alerting); brain is knowledge (read+write, RAG, MCP). Different lifecycle, different data shapes, different security posture. Combining them would muddy both stacks. The "everything in one place" experience comes from a Homepage/Dashy dashboard linking to both, not from co-location.

### Port allocation

**Decision**: Brain stack uses **300x range** (specifically 3006-3008).

**Considered**: 32xx/42xx (the project-prod/dev pattern used by found-footy/legal-tender/etc.).

**Why 300x**: brain is *infrastructure*, not a project — it aggregates across projects, has no prod/dev split, and matches the always-on services already in 300x (grafana, prometheus, vikunja). The user's port allocation table (now in `~/.claude/CLAUDE.md`) reserves 300x for infra services. 421x or other ranges would have been awkward.

### Containerized everything

**Decision**: All four services (Khoj, Open WebUI, brain-postgres, basic-memory) run in Docker. **No host-side `pipx install`.**

**Considered**: `pipx install basic-memory` on the host (the basic-memory docs' default suggestion).

**Why Docker-only**: matches the user's preference (codified in `~/.claude/CLAUDE.md` "Tooling installation policy"). Reproducible from `docker compose up -d`. Survives host reprovisioning.

### `docker compose up -d` does everything

**Decision**: A `khoj-init` one-shot service runs after Khoj is healthy, idempotently configures it, and exits 0. No manual UI clicking required.

**Considered**: A README-documented manual setup ("log into the admin UI, click here, paste this URL, etc.").

**Why automated**: the user explicitly stated "perfect world is `docker compose up` does everything." Manual setup steps are the wrong shape — they don't survive reprovisioning, can't be code-reviewed, and document drift the moment Khoj's UI changes.

**Implementation**: `~/workspace/obsidian/scripts/init-khoj.py` uses Khoj's Django ORM directly to create AI Model APIs, ChatModel, SearchModelConfig, ServerChatSettings, LocalMarkdownConfig, and an API token; then walks the vault and uploads markdown via `PUT /api/content`. All steps use `update_or_create` for idempotency.

### Anonymous-mode user resolution (the trickiest gotcha)

**Decision**: Vault content (LocalMarkdownConfig + uploaded files) attaches to user `username="default"`, NOT the Django admin user `vedanta@brain.local`.

**Why**: Khoj's anonymous-mode middleware (`configure.py:187`) resolves all incoming requests to `KhojUser.objects.filter(username="default")`. The Django admin user (created from `KHOJ_ADMIN_EMAIL`) is a *separate* user, only used for `/server/admin/` access. If we attach content to the admin user but anonymous chat queries land on the default user, the LLM sees a user with zero entries → no Notes tool → falls back to web search.

**This took 30 minutes to find** — the bug surfaced as "chat returns generic answers about NGOs and World Bank instead of legal-tender's actual funding channels." Easy to mistake as a search-config issue or RAG ranking problem; the real cause was multi-tenancy.

### `KHOJ_ALLOWED_DOMAIN=*`

**Decision**: Set the env var to `*` so Django's `DisallowedHost` doesn't reject requests with `Host: luv` or other tailnet hostnames.

**Considered**: Setting it to a specific domain or the tailnet name.

**Why wildcard**: tailnet is the perimeter; we don't need Django's host-header protection. Tightening this is on the TODO list for if/when the brain is ever exposed beyond tailnet (which is "never" per the user's stance on Google ties).

### Khoj patches via local Dockerfile

**Decision**: Build Khoj from a local `Dockerfile.khoj` that applies sed-based patches to the upstream image. Tag it `brain-khoj:patched`. Both `khoj` and `khoj-init` services use the patched image.

**Considered**: Submitting upstream PRs and waiting for merge. Forking khoj-ai/khoj.

**Why local Dockerfile**: needed the fixes immediately, not blocked on upstream review. The Dockerfile is small (one `RUN sed`), self-documenting (comment block lists each patch's purpose), and verifies on build (greps for the patched lines, fails if absent). When upstream merges, drop the Dockerfile and use the official image.

**Patches applied (2026-05-08)**:
- `src/khoj/processor/conversation/openai/utils.py:513,613`: `buf += <delta>` lacks `or ""` guard. llama.cpp's OpenAI-compatible streaming sends final chunks with `delta.content=None`; Khoj's code crashes with `TypeError: can only concatenate str (not "NoneType") to str`. OpenAI's own server papers over this by sending `""`. Fix: `buf += <delta> or ""`.

### Telemetry off

**Decision**: `KHOJ_TELEMETRY_DISABLE=true`.

**Why**: Khoj phones home to `khoj.beta.haletic.com/v1/telemetry`. From this network the call times out, blocking each chat by ~5-10s before falling through. Match the privacy-conscious / no-Google-ties principle.

### Host symlinks don't translate to containers

**Decision**: The vault on the host has `brain/projects/legal-tender → ~/workspace/dev/legal-tender/docs` symlinks for human convenience. But for the containers, we **bypass the symlinks entirely** and Docker-mount each repo's `docs/` directly:

```yaml
- ${HOME}/workspace/dev/legal-tender/docs:/data/brain/projects/legal-tender:ro
```

**Why**: a symlink stores an absolute host path. Inside the container, that host path doesn't exist — `os.walk` falls into the symlink and finds nothing. Docker-mounting the real source bypasses this entirely. Costs: one line of compose per project. Onboarding a new project = add one mount line to `khoj` and `khoj-init` services.

### What's NOT yet in the vault

`AGENTS.md` and `CLAUDE.md` (the agent-context files) live at each repo's root, not in `docs/`. So they're NOT in the vault and Khoj does NOT index them. They're loaded directly by Claude Code at session start (separate retrieval path).

**Open question**: should AGENTS.md also be in the vault for cross-project search? Right now if you ask Khoj "what conventions does legal-tender use?", it finds the docs but not AGENTS.md's terse "things to check before X" list. Might add a mount.

### What's deferred

- Khoj-as-MCP: not officially shipped per upstream as of 2026-05-07. Once confirmed, wire `khoj` into `~/.claude/mcp.json` alongside basic-memory.
- claudewatch (drift detection for AGENTS.md / docs): planned for Phase 5 (continuous operation).
- Quartz publishing (static site of the vault): only if we ever want a public-facing read-only view.
- Homepage dashboard at `~/workspace/homepage/`: nice-to-have for unified tailnet entry, separate from brain.
- Laptop-side native Obsidian + Syncthing: only if we want desktop editing of vault notes. Browser-based Khoj covers most use cases.

---

# Pipeline — Historical Decisions (Feb 2026, originally PIPELINE_FIXES.md)

**Started**: February 6, 2026
**Last Updated**: February 7, 2026
**Branch**: `feature/employer-enrichment` (now merged into main)

## The Goal

For any federal candidate, compute **how they are funded** by tracing money backwards through the FEC committee graph to its origin. The output is broken into funding channels — distinct ways money reaches or affects a candidate.

## Funding Channels

These are the categories of money flow we compute for each candidate, per election cycle and in aggregate.

### Channel 1: Organizational Direct Funding

PAC money that flows through the committee graph into the candidate's affiliated committees, traced upstream through JFCs, victory funds, and passthrough committees to terminal organizational sources.

**How it works**: Start at the candidate's affiliated committees. Walk backwards through `transferred_to` edges. At each committee, check `terminal_type`:
- **Terminal** (corporation, trade_association, labor_union, ideological, cooperative) → stop, attribute the money to that organization.
- **Passthrough** (JFC, party committee, unknown) → keep tracing upstream. Proportionally attribute using `edge_amount / committee_total_receipts`.

**Graph edges used**: `affiliated_with` (candidate → committee), `transferred_to` (committee → committee)

**What this captures**: Corporate PAC money, trade association money, labor union money, ideological PAC money — ALL organizational money that flows into a candidate's committees through any number of hops. The terminal organizations are the answer.

**What this does NOT capture**: Money from committees where `terminal_type = super_pac_unclassified` — Super PACs with no `ORG_TP` in FEC data. These currently stop tracing (we can't classify them further without additional data). This is part of the unaccounted gap.

### Channel 2: Independent Expenditure Support

Outside money spent FOR the candidate by Super PACs and other committees. This money never touches the candidate's committees — it's spent independently (TV ads, mailers, etc.).

**How it works**: Look at `spent_on` edges where `support_oppose = 'S'`. For each committee that spent on the candidate, trace its funding upstream via `transferred_to` and `contributed_to` to find who funded it. Proportionally attribute using `ie_amount / committee_total_receipts`.

**Graph edges used**: `spent_on` (committee → candidate), then `transferred_to` and `contributed_to` for upstream tracing.

**What this captures**: The organizations and individuals behind the Super PACs that spend in support of the candidate.

### Channel 3: Independent Expenditure Opposition

Same as Channel 2, but `support_oppose = 'O'` — money spent AGAINST the candidate. This is important context: a candidate might have $10M in IE support but $50M in IE opposition.

### Channel 4: Individual Contributions

All money from individual people to the candidate's affiliated committees, or proportionally attributed from upstream passthrough committees.

**Two tiers**:
- **Whale donors** (per-election max-out): Anyone whose largest single-committee total meets or exceeds the FEC per-election individual contribution limit for that cycle ($2,800 in 2020, $2,900 in 2022, $3,300 in 2024). Fully traced through graph with per-donor detail — name, employer, corporate connection. Split into corporate-connected (employees of known corps) and independent.
- **Grassroots donors** (below max-out limit): Known total from raw FEC `indiv` data but no per-donor detail in the graph (the max-out threshold is a principled, FEC-regulation-based graph optimization for employer analysis). Split into:
  - **Direct**: grassroots to candidate's own affiliated committees (from `committee_receipts.small_donor_total`)
  - **Upstream**: grassroots at passthrough committees (JFCs, conduits, party committees) attributed proportionally through the transfer chain

**Graph edges used**: `contributed_to` (donor → committee) for whale tier, `committee_receipts` raw FEC aggregation for grassroots tier.

### Channel 5: Unaccounted (True Residual)

The gap between committee total_receipts and all accounted money (traced whale + org + grassroots). Should be **3-5%** for well-traced candidates.

**What's in here**:
- Unitemized individual contributions (<$200 aggregate, not in FEC indiv file at all)
- Deep proportional trace loss (multiplier falls below 0.0001 threshold)
- Committees with no receipt data (no `total_receipts` set)
- Data gaps and edge cases

### Future: Lobbying

Lobbying disclosures are a completely different data source (we have the API via `lobbying_api.py`). The connection to a specific candidate is indirect — organizations lobby Congress, not individual candidates. The link would be "Organization X spent $Y lobbying on bills that Candidate Z voted on / committees Z sits on."

This is its own project and does not block the other channels.

### The Unaccounted Gap

At every committee hop in the trace, we compute: `total_receipts - SUM(all_traced_inflows) = gap`. This gap is real money we can't attribute, coming from:
- Unitemized small donors below the $200 reporting threshold (partially quantifiable from committee filings)
- 501(c)(4) dark money where donor disclosure isn't required
- Data we simply don't have edges for (timing mismatches, FEC data gaps)
- `super_pac_unclassified` committees with no ORG_TP that we stop tracing at

This is a first-class number in every channel, not an afterthought. It's arguably the most interesting signal — it's where the dark money lives.

## Output Structure

Per candidate, per cycle (and aggregate across cycles):

```
funding_channels: {
    organizational_direct: {
        total: $X,
        traced: $Y,           -- money successfully traced to terminal orgs
        unaccounted: $Z,       -- gap lost in passthrough committees
        by_organization: [     -- terminal orgs, sorted by amount
            { name, terminal_type, amount, pct }
        ]
    },
    ie_support: {
        total: $X,             -- total IE spend supporting this candidate  
        traced: $Y,            -- traced to upstream funders of those Super PACs
        unaccounted: $Z,
        by_organization: [...],
        by_committee: [...]    -- which Super PACs did the spending
    },
    ie_oppose: {
        total: $X,
        traced: $Y,
        unaccounted: $Z,
        by_organization: [...],
        by_committee: [...]
    },
    individuals: {
        itemized_total: $X,    -- sum of contributed_to edges to candidate's committees
        unitemized_total: $Y,  -- from committee summary filings (no donor detail)
        total: $X + $Y,
        top_donors: [...],
        corporate_connected: {
            total: $X,         -- whale/employer-linked individual money (metadata only)
            by_company: [...]
        }
    },
    summary: {
        total_pro: organizational_direct + ie_support + individuals,
        total_against: ie_oppose,
        total_traceable: sum of all traced amounts,
        total_unaccounted: sum of all gaps,
        pct_organizational: organizational_direct.traced / total_pro,
        pct_individual: individuals.total / total_pro,
        pct_ie: ie_support.total / total_pro,
        pct_dark: total_unaccounted / (total_pro + total_unaccounted)
    }
}
```

## Current Graph Edges

| Edge Collection | Count | What It Represents |
|---|---|---|
| `contributed_to` | 5,680,106 | Individual/candidate donor → committee (ENTITY_TP IN ['IND','CAN']) |
| `transferred_to` | 654,530 | Committee → committee (PAC-to-PAC, party, JFC transfers) |
| `affiliated_with` | 22,808 | Committee → candidate (one edge per cycle — deduplicate with UNIQUE) |
| `spent_on` | 20,373 | Committee → candidate (IE spending, support or oppose) |
| `employed_by` | 147,810 | Donor → employer |
| **donors** | 955,137 | Unique per-election max-out donors in graph |

## Committee Classifications

`terminal_type` on 30,840 committees (set by `committee_classification` asset):

| terminal_type | Count | % | Behavior |
|---|---|---|---|
| campaign | 13,493 | 43.8% | Candidate's own committee — starting point for tracing |
| passthrough | 8,139 | 26.4% | Trace upstream (JFCs, party committees, conduits) |
| super_pac_unclassified | 4,533 | 14.7% | Stop — no ORG_TP to classify further |
| corporation | 2,048 | 6.6% | Terminal → organizational direct |
| unknown | 994 | 3.2% | Trace upstream |
| trade_association | 786 | 2.5% | Terminal → organizational direct |
| ideological | 399 | 1.3% | Terminal → organizational direct |
| labor_union | 387 | 1.3% | Terminal → organizational direct |
| cooperative | 54 | 0.2% | Terminal → organizational direct |

## Committee Receipts

16,488 committees with `total_receipts` populated. $40.7B total individual contributions across all committees.

---

## Completed Fixes

### FIX 1 — ENTITY_TP filter in donors and contributed_to ✅

**Date**: Feb 6, 2026  
**Files**: `src/assets/graph/donors.py`, `src/assets/graph/contributed_to.py`

The FEC's `indiv.txt` is actually Schedule A itemized receipts — ALL contributions, not just individuals. Contains `ENTITY_TP` field: IND, ORG, PAC, COM, CAN, CCM, PTY.

Added `FILTER doc.ENTITY_TP == 'IND'` to both assets. This excludes ORG/PAC/COM/CCM/PTY (already captured via `oth` → `transferred_to`) and CAN (candidate self-funding — see Fix 9 below).

| Metric | Before | After |
|---|---|---|
| Donors | 287,744 | 279,624 (-8,120) |
| contributed_to edges | 3,340,147 | 3,326,235 (-13,912) |
| donor_classification orgs | 4,921 (1.7%) | 254 (0.1%) |

### FIX 2 — Conduit filter ✅

**Date**: Feb 6, 2026  
**Files**: `src/assets/graph/donors.py`, `src/assets/graph/contributed_to.py`

Conduits (ActBlue, WinRed) aggregate earmarked donations. Their bulk transfers double-count money already captured as individual contributions. Added `FILTER NOT REGEX_TEST(doc.NAME, '(ACTBLUE|WINRED|EARMARK|CONDUIT)', true)`.

Removed 11 conduit donors, 410 edges, $518M of double-counted money.

### FIX 8 — committee_classification dependency ✅

**Date**: Feb 6, 2026  
**Files**: `src/assets/aggregation/candidate_upstream.py`

`candidate_upstream` reads `terminal_type` from committees but never declared `committee_classification` as a dependency. The classification asset had never run — all 30,841 committees had `terminal_type: null`, causing all organizational money to be misclassified.

Added dependency, ran classification. Now 3,674 committees correctly typed as terminal organizational sources.

### FIX 9 — Include candidate self-funding (ENTITY_TP='CAN') ✅

**Date**: Feb 6, 2026  
**Files**: `src/assets/graph/donors.py`, `src/assets/graph/contributed_to.py`

Fix 1 filtered to `ENTITY_TP == 'IND'` only. This excluded candidate self-funding (`ENTITY_TP='CAN'`, 36,392 records). Self-funding IS an individual contribution — it's a person writing a check to their own committee. The corporate connection (Bloomberg → Bloomberg LP) is metadata tracked via employer linkage, same as any other whale.

Changed filter to `ENTITY_TP IN ['IND', 'CAN']`.

| Metric | After Fix 1 | After Fix 9 |
|---|---|---|
| Donors | 279,624 | 280,513 (+889 candidates) |
| contributed_to edges | 3,326,235 | 3,327,970 (+1,735) |

### FIX 10 — Rewrite candidate_upstream → candidate_funding ✅

**Date**: Feb 6, 2026  
**Files**: `src/assets/aggregation/candidate_upstream.py` (824 lines, complete rewrite)

Rewrote the core asset from the broken terminal-source-type model to the funding channels model. Key changes:
- Renamed asset from `candidate_upstream` to `candidate_funding`
- Config class renamed `CandidateFundingConfig`
- `trace_committee_sources()` returns `organizational` dict (all 5 org types) + `individuals` + `traced_total`
- New `trace_ie_sources()` function (extracted from old `trace_ie_corporate_sources`)
- New `compute_funding_channels()` replaces old computation
- Added `TERMINAL_TYPE_BUCKET` mapping dict and `safe_pct()` helper
- Output writes to `candidates.funding_channels` (was `candidates.funding_sources`)
- Validation section prints all 5 channels with tree-style formatting

BFS tracing engine preserved — same proportional attribution algorithm, restructured output.

### FIX 3 — Remove committee_financials (redundant) ✅

**Date**: Feb 6, 2026  
**Files**: Deleted `src/assets/enrichment/committee_financials.py`, updated deps in `committee_receipts.py`, `candidate_summaries.py`, `committee_summaries.py`, all `__init__.py` files

`committee_receipts` overwrites everything `committee_financials` computed. Removed the redundant 162-line asset.

### FIX 4 — Remove dead employer enrichment chain ✅

**Date**: Feb 6, 2026  
**Files**: Deleted `employer_clustering.py` (443 lines), `employer_cluster_integration.py` (192 lines), `corporate_hierarchy.py` (339 lines). Removed `employer_unification_job`. Updated all `__init__.py` and job files.

6-asset chain producing 540 usable records. Kept `employers`, `canonical_employers`, and `wikidata_resolution`. Removed O(n²) clustering and fragile parent detection (974 lines total).

### FIX 6 — Remove dead Python classification functions ✅

**Date**: Feb 6, 2026  
**Files**: `src/assets/enrichment/committee_classification.py`, `src/assets/enrichment/donor_classification.py`

Removed dead `classify_committee()` and `classify_donor()` Python functions — all classification happens in AQL. Also fixed a runtime bug: the logging code in `committee_classification` still called the deleted `classify_committee()` function. Replaced with inline Python logic mirroring the AQL ternary chain.

### FIX 7 — wikidata_resolution field name mismatch ✅

**Date**: Feb 6, 2026  
**Files**: `src/assets/enrichment/wikidata_resolution.py`

Changed `ce.employee_count` → `ce.employee_donor_count` (the actual field name set by `canonical_employers`).

### FIX 11 — Deduplicate affiliated committee IDs + unaccounted breakdown ✅

**Date**: Feb 7, 2026  
**Files**: `src/assets/aggregation/candidate_upstream.py`

**Root cause**: The `affiliated_with` edge collection has one edge per (committee, candidate, cycle). The AQL query `FOR v IN INBOUND c affiliated_with RETURN v._key` returned duplicate committee IDs — Cruz's senate committee appeared 3× (once per cycle), tripling its receipts in the unaccounted calculation.

**Fix**: Added `UNIQUE()` to the AQL query: `UNIQUE(FOR v IN INBOUND c affiliated_with RETURN v._key)`.

Also enriched the unaccounted channel with receipt breakdown data already available from `committee_receipts`:
- `breakdown.small_donor_estimate` — individual contributions not in our graph (mostly unitemized <$200)
- `breakdown.from_individuals` — total individual receipts for candidate's committees
- `breakdown.from_committees` — total committee transfer receipts

| Candidate | Old Receipts (duped) | New Receipts (deduped) | Inflation | Old Unaccounted | New Unaccounted |
|---|---|---|---|---|---|
| Cruz | $215.7M | $71.9M | 3.0× | 91.9% | 75.6% |
| Trump | $2.51B | $984.7M | 2.6× | 93.8% | 84.1% |
| Harris | $1.85B | $1.80B | 1.03× | 86.2% | 85.8% |

---

## Validation Results (Feb 7, 2026)

Pipeline run: `donors` → `contributed_to` → `committee_classification` → `committee_receipts` → `candidate_funding`

**candidate_funding**: 11,796 candidates processed, 6,305 with funding data, completed in 6m11s.

| | **Harris (Pres)** | **Trump** | **Cruz (Senate)** |
|---|---|---|---|
| **Total Funding** | $2.27B | $1.24B | $78.1M |
| **Ch1 Org Direct** | $13.7M (0.6%) | $3.3M (0.3%) | $2.1M (2.6%) |
| — Corp | $1.7M | $1.5M | $806K |
| — Trade | $2.5M | $816K | $777K |
| — Labor | $7.5M | $144K | $174K |
| — Ideological | $1.8M | $760K | $289K |
| — Cooperative | $139K | $66K | $9K |
| **Ch2 IE Support** | $548M (24.1%) | $300M (24.2%) | $8.7M (11.2%) |
| **Ch3 IE Oppose** | $561M | $492M | $2.9M |
| **Ch4 Individuals** | $1.71B (75.3%) | $937M (75.5%) | $67.4M (86.2%) |
| — Whale (max-out) | $719M (31.6%) | $280M (22.6%) | $22.6M (28.9%) |
|   — Corp-connected | $52.8M | $20.1M | $877K |
|   — Independent | $666M | $260M | $21.7M |
| — Grassroots (sub-limit) | $991M (43.6%) | $656M (52.9%) | $44.8M (57.3%) |
|   — Direct | $729M | $205M | $43.5M |
|   — Upstream | $262M | $451M | $1.3M |
| **Ch5 Unaccounted** | $73.7M (4.1%) | $44.8M (4.6%) | $2.5M (3.5%) |
| **Receipts** | $1.80B | $985M | $71.9M |
| **Accounted** | $1.72B | $940M | $69.4M |

**Observations**:
- Unaccounted now 3-5% across all candidates (down from 75-86%)! Residual is unitemized <$200 donors + deep trace loss.
- Grassroots individuals (sub-$10K aggregate) are the dominant funding channel: 43-57% of total funding.
- Trump's upstream grassroots ($451M) is massive — reflects the WinRed/JFC small-dollar fundraising machine.
- Harris's whale donors ($719M, 31.6%) exceed Trump's ($280M, 22.6%) in both absolute and relative terms.
- IE spending is roughly equal for both presidential candidates (~$300M support, ~$500M+ oppose).
- Org direct is small for presidential races (<1%) but meaningful for Senate (Cruz at 2.6%).
- Labor strongly favors Harris ($7.5M vs $144K). Corp/trade more balanced but still tilt slightly Harris.

---

### FIX 12 — Grassroots channel + two-phase proportional trace

**Problem**: Unaccounted was 75-86% — absurdly high. Two distinct bugs:

**Bug A — Missing grassroots channel**: The `donors` graph had a $10K aggregate threshold (since replaced by per-election max-out in Fix 13). Only whale donors got graph vertices/edges. But `committee_receipts` correctly sums ALL raw FEC `indiv` transactions. The difference (sub-threshold itemized donors) was dumped into "unaccounted" even though it's a known, quantified amount.

**Bug B — visited_edges BFS bug**: When a committee transfers money via multiple edges (one per cycle), the BFS enqueued the source committee multiple times but `visited_edges` meant only the FIRST dequeue processed any edges. Harris Victory Fund→Harris: 3 edges ($586M, $237M, $6M) but only the $586M edge's mult was used for whale/org tracing. Lost $244M from HVF alone, cascading through DNC ($138M more).

**Fix A**: Fold sub-$10K individuals into the individuals channel as "grassroots". Direct (to candidate's committees) comes from committee_receipts. Upstream (at passthrough committees) is attributed proportionally during trace.

**Fix B**: Replaced BFS with two-phase proportional trace:
- Phase 1: Propagate multipliers level-by-level through passthrough graph, accumulating total mult per committee. Terminal org attributions happen here. Multiple transfer edges between same committees → correctly accumulated.
- Phase 2: Process each committee ONCE with its total mult. Attribute whale individuals and upstream grassroots.

**Result**: Unaccounted dropped from 75-86% to 3-5%. The $10K threshold stays as a graph optimization (we only need per-donor employer detail for whales), but the accounting now properly classifies ALL traceable money.

---

## Pending Fixes

### FIX 5 — Consolidate normalization functions

**Priority**: MODERATE  
**Files**: Create `src/utils/normalize.py`, update all assets

Three different normalization functions across the codebase → silent key mismatches. Consolidate to one.

---

## Completed Fix: Per-Election Max-Out Threshold

### FIX 13 — Replace $10K arbitrary threshold with FEC per-election max-out limit ✅

**Date**: Feb 7, 2026  
**Files**: `src/assets/graph/donors.py`, docstring updates to `contributed_to.py`, `committee_receipts.py`, `candidate_upstream.py`

**Problem**: The $10K aggregate threshold for whale donors was arbitrary and backwards. It summed a donor's contributions across ALL committees — someone giving $3,300 to 4 different candidates ($13,200 total) qualified as a whale, but someone maxing out $6,600 to a SINGLE candidate didn't. The threshold had no connection to FEC regulations.

**Insight**: The FEC sets per-election individual contribution limits, indexed for inflation every odd year:
- 2020: $2,800/election ($5,600/cycle)
- 2022: $2,900/election ($5,800/cycle)
- 2024: $3,300/election ($6,600/cycle)
- 2026: $3,500/election ($7,000/cycle)

Anyone who maxes out to even ONE committee is demonstrating intentional, strategic giving — the exact behavior that makes employer/corporate linkage interesting.

**Solution**: Rewrote `donors.py` with double-COLLECT AQL pattern:
1. First COLLECT by (name, employer, cmte_id) → per-committee totals
2. Second COLLECT by (name, employer) → roll up with MAX(cmte_total)
3. FILTER max_single_cmte >= per-election limit for that cycle

Single scan per cycle, no nested joins. ~3 minutes per cycle on tuned ArangoDB.

**Also tuned ArangoDB** for the host hardware (AMD Ryzen AI MAX+ 395, 16c/32t, 128GB RAM):
- 16GB detected memory (up from 8GB default)
- 64 max threads, 16 IO threads, 16 min threads
- 4GB query memory limit
- RocksDB: 8 high/low priority threads, 16 max background jobs, 3GB edge cache
- Both `docker-compose.dev.yml` and `docker-compose.yml` updated

| Metric | Old ($10K agg) | New (max-out) | Change |
|---|---|---|---|
| Donors (2020) | 142,654 | 510,752 | 3.58× |
| Donors (2022) | 99,291 | 362,298 | 3.65× |
| Donors (2024) | ~82,000 | 369,736 | ~4.5× |
| Total unique donors | 280,513 | 955,137 | 3.40× |
| Materialization time | ~4m | 10m28s | Larger dataset |

**Impact**: 3.4× more donors in the graph, each one a person who demonstrated max-out giving behavior to at least one committee. The whale/grassroots split is now grounded in FEC law rather than an arbitrary dollar amount. Downstream: contributed_to edges grew from 3,327,970 → 5,680,106 (1.71×, 14m36s). Committee receipts and candidate funding pending rematerialization.

---

## Execution Plan

| Phase | Fixes | Status |
|---|---|---|
| ✅ Done | 1, 2, 8 | Clean donor data, committee classifications, basic tracing |
| ✅ Done | 9, 10, 3, 4, 6, 7 | Self-funding, funding channels rewrite, dead code removal |
| ✅ Done | 11, 12 | Dedup affiliated committees, grassroots channel, two-phase trace |
| ✅ Done | 13 | Per-election max-out threshold, ArangoDB tuning |
| Next | 5 | Normalize functions |
