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

### Channel 4: Individual Contributions

All money from individual people to the candidate's affiliated committees, or proportionally attributed from upstream passthrough committees.

**Two tiers**:
- **Whale donors** ($10K+ aggregate): Fully traced through graph with per-donor detail — name, employer, corporate connection. Split into corporate-connected (employees of known corps) and independent.
- **Grassroots donors** (sub-$10K aggregate): Known total from raw FEC `indiv` data but no per-donor detail in the graph (the $10K threshold is a graph optimization for employer analysis). Split into:
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
| — Whale ($10K+) | $719M (31.6%) | $280M (22.6%) | $22.6M (28.9%) |
|   — Corp-connected | $52.8M | $20.1M | $877K |
|   — Independent | $666M | $260M | $21.7M |
| — Grassroots (<$10K) | $991M (43.6%) | $656M (52.9%) | $44.8M (57.3%) |
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

**Bug A — Missing grassroots channel**: The `donors` graph has a $10K aggregate threshold. Only whale donors ($10K+) get graph vertices/edges. But `committee_receipts` correctly sums ALL raw FEC `indiv` transactions. The difference (sub-$10K itemized donors) was dumped into "unaccounted" even though it's a known, quantified amount.

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

## Execution Plan

| Phase | Fixes | Status |
|---|---|---|
| ✅ Done | 1, 2, 8 | Clean donor data, committee classifications, basic tracing |
| ✅ Done | 9, 10, 3, 4, 6, 7 | Self-funding, funding channels rewrite, dead code removal |
| ✅ Done | 11, 12 | Dedup affiliated committees, grassroots channel, two-phase trace |
| Next | 5 | Normalize functions |
