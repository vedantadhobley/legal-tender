# Pipeline — Data Model & Fix Tracker

**Started**: February 6, 2026  
**Last Updated**: February 6, 2026  
**Branch**: `feature/employer-enrichment`

---

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

### Channel 4: Individual Contributions (Itemized)

People donating directly to the candidate's affiliated committees. These are itemized in FEC filings because they exceed the $200 reporting threshold.

**How it works**: Look at `contributed_to` edges going into the candidate's affiliated committees. These are already terminal — the individual IS the source. No upstream tracing needed.

**Metadata per individual**: Name, employer, total amount. For whale donors ($10K+), we also track corporate connections (via employer mapping or Wikidata resolution) — not because the individual is acting on behalf of that corporation, but because it's useful context. A Bank of America teller donating $500 isn't corporate influence. A billionaire CEO whose net worth IS the company — that's a different signal. The corporate connection is metadata, not a reclassification.

**Candidate self-funding**: Candidates who fund their own campaigns (ENTITY_TP='CAN') show up here as individual donors. Bloomberg writing a $1B check to his own committee is an individual contribution from a person who happens to also be the candidate. His corporate connection (Bloomberg LP) is metadata. This is correct — corporations can't donate directly to campaigns. All self-funding is personal funds.

**Graph edges used**: `contributed_to` (donor → committee), `affiliated_with` (committee → candidate)

### Channel 5: Unitemized Individual Contributions

Donations under $200 that don't get itemized in FEC filings. We have NO graph edges for these — they exist only as a lump sum total in committee summary filings (`weball`/`webk` data, field: `INDIV_UNITEM`).

**How it works**: Pull the unitemized total from the committee's summary filing data. No tracing possible — we know the total but not who these donors are.

**Why this matters**: For populist candidates (Sanders, Trump small-dollar operations), this can be the majority of their direct funding. Without this, the "unaccounted" gap would be misleadingly large.

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
| `contributed_to` | 3,327,970 | Individual/candidate donor → committee (ENTITY_TP IN ['IND','CAN']) |
| `transferred_to` | 654,530 | Committee → committee (PAC-to-PAC, party, JFC transfers) |
| `affiliated_with` | 22,808 | Committee → candidate (one edge per cycle — deduplicate with UNIQUE) |
| `spent_on` | 20,373 | Committee → candidate (IE spending, support or oppose) |
| `employed_by` | 147,810 | Donor → employer |
| **donors** | 280,513 | Unique individual donors in graph |

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

Rewrote the core asset from the broken "five pies" terminal-source-type model to the funding channels model. Key changes:
- Renamed asset from `candidate_upstream` to `candidate_funding`
- Config class renamed `CandidateFundingConfig`
- `trace_committee_sources()` returns `organizational` dict (all 5 org types) + `individuals` + `traced_total`
- New `trace_ie_sources()` function (extracted from old `trace_ie_corporate_sources`)
- New `compute_funding_channels()` replaces `compute_five_pies()`
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

**candidate_funding**: 11,796 candidates processed, 4,901 with funding data, completed in 5m51s.

| | **Harris (Pres)** | **Trump** | **Cruz (Senate)** |
|---|---|---|---|
| **Total Traced** | $804M | $457M | $26.3M |
| **Ch1 Org Direct** | $2.0M (0.2%) | $834K (0.2%) | $1.26M (4.8%) |
| — Corp | $207K | $503K | $445K |
| — Trade | $40K | $188K | $498K |
| — Labor | $1.03M | $31K | $88K |
| — Ideological | $717K | $101K | $227K |
| — Cooperative | $767 | $10K | $6K |
| **Ch2 IE Support** | $548M (68.2%) | $300M (65.8%) | $8.7M (33.2%) |
| **Ch3 IE Oppose** | $561M | $492M | $2.9M |
| **Ch4 Individuals** | $254M (31.6%) | $155M (34.0%) | $16.3M (62.0%) |
| — Corp-connected | $11.9M | $5.9M | $597K |
| — Independent | $242M | $149M | $15.7M |
| **Ch5 Unaccounted** | $1.54B (85.8%) | $828M (84.1%) | $54M (75.6%) |
| — Small donors est. | $729M | $205M | $43.5M |
| — Receipts | $1.80B | $985M | $71.9M |
| — Traced | $256M | $156M | $17.6M |

**Observations**:
- IE spending dominates presidential races (~66% of traced funding for both Harris and Trump)
- Senate races (Cruz) are more individual-heavy (62%) with meaningful org direct (4.8%)
- Labor flows to Harris ($1.03M vs $31K for Trump). Corp flows to Trump ($503K vs $207K for Harris)
- Unaccounted is structurally inevitable — unitemized small donors (<$200) alone are $729M for Harris, $205M for Trump, $43.5M for Cruz. These are real contributors we simply have no name-level data for.
- The remaining gap beyond small donors is proportional trace loss: when a passthrough JFC raised $100M but sent $1M to this candidate, we only trace 1% of the JFC's upstream sources.

---

## Pending Fixes

### FIX 5 — Consolidate normalization functions

**Priority**: MODERATE  
**Files**: Create `src/utils/normalize.py`, update all assets

Three different normalization functions across the codebase → silent key mismatches. Consolidate to one.

---

## Execution Plan

| Phase | Fixes | Status |
|---|---|---|
| ✅ Done | 1, 2, 8 | Clean donor data, committee classifications, basic tracing |
| ✅ Done | 9, 10, 3, 4, 6, 7 | Self-funding, funding channels rewrite, dead code removal |
| ✅ Done | 11 (unaccounted dedup) | Deduplicated affiliated_with cmte_ids, added small-donor breakdown |
| Next | 5 | Normalize functions |
