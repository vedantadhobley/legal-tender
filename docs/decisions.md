# Decisions Log

Append-only record of architectural and operational decisions, ordered by date. Earlier entries (Feb 6-7, 2026, originally `PIPELINE_FIXES.md`) are the data-model decisions for the funding-channels work. Later entries are the professionalization-effort decisions.

When a non-obvious choice gets made, append a dated entry here with: what we decided, what we considered, why we picked the chosen path, and what still leaves open.

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
