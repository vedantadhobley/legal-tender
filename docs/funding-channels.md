# Funding Channels

The funding-channels model is the project's core deliverable: for any federal candidate, a five-channel decomposition of "where did their money come from" — traced backwards through PAC chains to terminal sources.

This doc explains the model, the algorithm, the design choices, and the known gaps. It's the canonical reference; `pipeline.md` covers the layer/asset overview, this covers the *meaning* of the output.

## Overview

For each candidate, per cycle and aggregated across cycles, we produce a `funding_channels` record on the candidate document with this shape:

```jsonc
{
  "by_cycle": { "2020": {...}, "2022": {...}, "2024": {...}, "2026": {...} },
  "aggregate": {
    "total_funding": 12_230_000,
    "direct_funding": 12_230_000,        // total − IE
    "ie_support": 151,
    "ie_oppose": 0,
    "organizational_direct": { "total": 2_450_000, "by_type": {...} },
    "ie": { "support": {...}, "oppose": {...} },
    "individuals": { "total": 9_780_000, "whale": {...}, "grassroots": {...} },
    "unaccounted": { "total": 0, "pct": 0.0 },
    "by_organization": [...]            // cross-cut report (not a 6th channel)
  },
  "cycles_available": ["2020", "2022", "2024", "2026"],
  "computed_at": "2026-05-08T..."
}
```

The five channels are mutually exclusive at the dollar level — no money is counted twice.

## The five channels

### Channel 1 — Organizational Direct

**Definition**: Money flowing through the committee graph from terminal organizational sources (corporate PACs, trade association PACs, labor union PACs, ideological PACs, cooperative PACs) to the candidate's affiliated committees.

**How traced**: starting at each of the candidate's affiliated committees (per `affiliated_with` edges), walk backwards through `transferred_to` edges. At each hop, classify the upstream committee:

- **Terminal** (corporation, trade_association, labor_union, ideological, cooperative): attribute the transfer amount × current multiplier directly to the org. Stop walking.
- **Passthrough** (passthrough JFCs, super_pac_unclassified, campaign): propagate the multiplier to the next level. Multiplier = `min(1.0, amount / from_committee_total_receipts)` — a fraction of the upstream's outgoing money that flowed to this candidate's chain.

What this captures: a corporate PAC giving $10K directly = $10K in this channel. A corporate PAC giving $10K to a JFC, where the JFC then sends 20% of its money to the candidate's principal committee, would attribute $10K × 0.20 = $2K to the corporate PAC, found via the JFC passthrough.

What this does NOT capture: money from `super_pac_unclassified` committees — these are committees with no `ORG_TP` classification in FEC data. They typically appear as IE spenders (Channel 2/3) where they're traced separately.

### Channel 2 — IE Support (Independent Expenditures FOR the candidate)

**Definition**: Outside money spent FOR the candidate by Super PACs and other committees that filed `support_oppose=S` independent expenditures. This money never touches the candidate's committees — it's spent on TV ads, mailers, etc.

**How traced**: look at `spent_on` edges with `support_oppose=S`. For each spending committee, trace upstream via `transferred_to` and `contributed_to` to find who funded it. Attribute proportionally: `spending_amount × (donor_amount / spending_committee_total_receipts)`, capped at 1.0.

What this captures: the donors and orgs behind Super PACs that ran ads supporting the candidate. Bundled to corporations where employer/wikidata resolution succeeds.

### Channel 3 — IE Oppose

Same as IE Support but for `support_oppose=O` — money spent **against** an opponent of the candidate. Mathematically symmetric to IE Support. Reported separately because the political meaning differs (negative ads about your opponent help you).

### Channel 4 — Individuals

**Definition**: All money from individual donors (FEC entity types `IND` and `CAN` for self-funding). Split into two sub-channels by donation magnitude.

#### 4a. Whale (high-dollar, graph-traced)

A donor we classify as a *whale* — see [Whale threshold](#whale-threshold-model) below for the precise definition. Tracked at per-donor granularity in the graph. Their employer is resolved via `canonical_employers` and Wikidata to a corporate entity where possible.

Sub-buckets:

- **`corporate_connected`**: whales whose employer resolves to a corporate family. Attribute their donation amount to that corporation in `by_organization`. (See [Whale → employer conflation](#whale--employer-conflation) for what this means and doesn't mean.)
- **`independent`**: whales without a corporate employer link (retired, self-employed, unresolved).

#### 4b. Grassroots (sub-whale, summed)

All individual donations *below* the whale threshold. Includes:

- Itemized donations from `indiv.zip` that didn't reach the whale threshold
- **Unitemized small donations** (sub-$200 cumulative donors) — *NOT in `indiv.zip`*, which is why we need FEC's `weball.TTL_INDIV_CONTRIB` as the authoritative number

Grassroots is reported as a known total, not as per-donor records. We don't graph millions of small-donor edges; we trust FEC's published aggregations and back out grassroots = `TTL_INDIV_CONTRIB − whale_donor_total`.

### Channel 5 — Unaccounted

**Definition**: True residual gap. Total committee receipts (per FEC `weball.TTL_RECEIPTS`) minus everything we successfully attributed (whale + grassroots + organizational_direct). Should be small (target: 3-5%) representing:

- Trace loss through deep passthrough chains
- Edge cases in committee classification
- Data inconsistencies between FEC files

If this number is large, something upstream is broken (committee_receipts staleness, trace bugs, etc.).

## Whale threshold model

A donor is classified as a *whale* — and thus tracked in the donors graph for corporate attribution — if they aggregated **≥ the FEC per-election limit at any single committee in a cycle**.

```python
# In src/assets/graph/donors.py — per-cycle threshold
PER_ELECTION_LIMITS = {
    "2020": 2_800,
    "2022": 2_900,
    "2024": 3_300,
    "2026": 3_500,
}
```

Source: [FEC contribution limits](https://www.fec.gov/help-candidates-and-committees/candidate-taking-receipts/contribution-limits/) — adjusted for inflation every 2 years.

### What the threshold actually identifies (vs. what it sounds like)

**It sounds like**: "donors who hit the FEC max-out limit per election."

**It actually is**: "donors who aggregated ≥ the per-election limit at any single committee in a cycle, regardless of whether they technically maxed any specific election."

Concretely:

| Donor scenario | Per our def: whale? | Per FEC: max-out? |
|---|---|---|
| $3,300 to Cruz primary, $0 to general | ✅ yes | yes |
| $3,000 primary + $3,000 general (one candidate) = $6,000 cycle total | ✅ yes (max_cmte=$6K) | NO (neither election was max'd) |
| $3,000 to Cruz + $3,000 to Cornyn (split) | ❌ no (max_cmte=$3K) | NO |
| $1M to a Super PAC | ✅ yes (max_cmte=$1M) | technically no limit there |

We're computing a **high-engagement-donor proxy**, not strict FEC compliance. The threshold is a useful cutoff for "who's worth tracing through the graph for corporate attribution" because:
- Sub-threshold donors are typically genuinely individual political acts
- At-or-above-threshold donors tend to be bundled, organized, or corporate-affiliated
- Tracking them per-donor (rather than as anonymous grassroots) lets us link to employers

For the small fraction of edge-case donors that are mis-classified by this proxy (e.g., a $3,000 + $3,000 split-cycle donor who reads as a whale), the impact is minor: they show up in `whale.independent` if no employer link exists, or `whale.corporate_connected` if one does. They contribute to the right total either way.

### Why hardcoded values

Trade-off chosen: hardcoded `PER_ELECTION_LIMITS` dict with cycle keys, manually updated when FEC publishes new limits (every 2 years).

- ✅ Simple, no runtime dependency on FEC API
- ✅ Predictable — values change biannually, trivial PR
- ✅ Visible in code review
- ❌ Requires explicit update at start of each new cycle (next review due 2027 for 2028 cycle limit)

Future-proofing: when we add 2028 to `ACTIVE_CYCLES`, also update `PER_ELECTION_LIMITS["2028"]`. The Stop hook can warn if cycle is added without limit.

## Whale → employer conflation

For corporate accountability research, we link whale donors to their employer. This is **the model choice that some readers will find aggressive** and is worth being explicit about.

### How it works

1. Whale donor's `EMPLOYER` field from FEC indiv.zip is normalized via `canonical_employers` (e.g., "GOOGLE LLC", "GOOGLE, INC.", "ALPHABET" → canonical "ALPHABET")
2. `wikidata_corporate_resolution` resolves the canonical employer to a parent corporate family (when Wikidata has the data)
3. The whale's donation amount is attributed to the corporate family in `corp_connected` bucket and in the `by_organization` cross-cut

### What this implies

When we say *"$250K from Goldman Sachs employees"*, that's **the sum of itemized whale donations from individuals who listed Goldman Sachs (or a Goldman Sachs subsidiary) as their employer**. It's not literal corporate money — Goldman Sachs the corporation can't legally give that money directly to a candidate (post-Citizens United, only Super PACs).

The interpretive frame:
- ✅ Empirically accurate: senior partners often coordinate giving, bundlers solicit colleagues, executive donations align with firm policy positions
- ⚠️ Personally noisy: a junior employee making a personal political donation isn't "firm money" in any meaningful sense
- 📊 Statistically useful: at the firm level, the aggregate is a reasonable signal of corporate political alignment

### How this interacts with `by_organization`

`by_organization` is a **cross-cut report**, not a sixth channel. It re-queries the same dollars from a different angle:

```jsonc
"by_organization": [
  {
    "name": "GOLDMAN SACHS",
    "direct_pac": 50_000,           // from organizational_direct (channel 1)
    "direct_employees": 250_000,    // from individuals.whale.corporate_connected (channel 4)
    "ie_support": 1_000_000,        // from IE Support traced upstream (channel 2)
    "ie_oppose": 0,
    "total_pro": 1_300_000          // sum of pro-candidate channels for this org
  }
]
```

**No double counting**: each dollar is counted in exactly one channel (1, 2, 3, or 4). `by_organization` queries the cross-channel sum *for one specific organization* by combining all four. Sum of channel totals is invariant.

### Skeptical readings to keep in mind

When using this data:
- A high `direct_employees` number means *Goldman Sachs employees, in aggregate, gave to this candidate as bundled whale donors*, not that Goldman Sachs the corporation made a $250K political donation
- For research/journalism the firm-level rollup is usually the more useful unit (individual donors are noisy; bundling patterns are signal)
- For granular accountability the per-donor names are still queryable in `individuals.whale.corporate_connected.by_company.donors`

## The trace algorithm

The two-phase proportional trace from `compute_funding_channels` in `src/assets/aggregation/candidate_upstream.py`. See `decisions.md` for the algorithmic correctness fixes shipped 2026-05-08.

### Phase 1 — Propagate multipliers level-by-level through passthroughs

```
Initialize:
  all_mults = {start_committee: 1.0 for each affiliated committee}
  current_level = {start_committee: 1.0 ...}
  propagated_from = set(starting committees)   // cycle-break tracker

For depth in range(8):  // max trace depth
  next_level = {}
  
  For each (cmte_id, mult) in current_level:
    For each transfer edge INTO cmte_id (from upstream committee):
      term_type = upstream.terminal_type
      attr_amount = transfer_amount × mult
      
      If terminal (corp/trade/labor/ideological/cooperative):
        org_results[bucket][upstream_name] += attr_amount
        traced_total += attr_amount
        // STOP — terminals don't propagate further
      
      Elif passthrough or campaign:
        If upstream is in propagated_from: skip   // cycle break
        If upstream.total_receipts <= 0: skip     // can't divide by zero
        edge_fraction = min(1.0, transfer_amount / upstream.total_receipts)
        new_mult = mult × edge_fraction
        next_level[upstream] += new_mult
        all_mults[upstream] = min(1.0, all_mults[upstream] + new_mult)
  
  propagated_from.update(current_level.keys())
  current_level = next_level
```

### Phase 2 — Attribute individuals at each visited committee

```
For each (cmte_id, mult) in all_mults:
  // Whale individual contributions (graph-traced)
  For each contributed_to edge from a whale donor TO cmte_id:
    attr_amount = donor_amount × mult
    individual_results[donor_name].amount += attr_amount
    traced_total += attr_amount
  
  // Grassroots at non-starting committees (proportional sum)
  If cmte_id is not a starting committee:
    grassroots_upstream += cmte.small_donor_total × mult
```

### Three correctness invariants (added 2026-05-08)

1. **Cycle break**: `propagated_from` set prevents A↔B bidirectional transfers from inflating multipliers indefinitely. Many JFC pairs in real FEC data have transfers in both directions.
2. **Per-edge cap**: `min(1.0, amount / from_receipts)` — a committee can't transfer out more than it received. The data sometimes appears to violate this (committees with $1 in `total_receipts` but $10K outgoing — likely technical filings without matching receipts records); the cap handles it.
3. **Accumulated-mult cap**: `min(1.0, all_mults[cmte] + new_mult)` — no committee can be responsible for >100% of any candidate's money. Bounds compounding.

The fix prevents catastrophic over-attribution (pre-fix, top candidate was $118 quintillion). It's mathematically conservative — a fixed-point iteration would be cleaner, see open follow-ups in `todo.md`.

## Cross-channel mathematical invariants

For any candidate, per cycle:

```
total_funding = direct_funding + ie_support + ie_oppose
direct_funding = organizational_direct.total + individuals.total + (something matching unaccounted residual)
individuals.total = whale.total + grassroots.total
whale.total = corporate_connected.total + independent.total
unaccounted = direct_committee_receipts − (whale + grassroots + org_direct)
```

These should hold exactly (to floating-point tolerance) for every candidate. The `validate_funding_channels.py` script samples 100 candidates to check this; any drift indicates a bug.

## Known gaps as of 2026-05-08

### The unitemized grassroots gap (active fix in flight)

**Problem**: We compute `total_from_individuals` by summing records in `indiv.zip`. But FEC's `indiv.zip` only contains *itemized* donations (typically $200+ cumulative per donor per committee per year). **Unitemized small donations are NOT in indiv.zip** — FEC reports them only as a SUM in candidate filings.

**Impact**: Major undercount for grassroots-heavy candidates:
- BWC (mostly itemized donors): 0.7% delta from FEC published — fine
- Bernie Sanders (huge unitemized base): 58% undercount — broken
- Major presidentials (Biden, Trump, Harris): 25-40% undercount

**Fix in progress**: Use FEC's `weball.TTL_INDIV_CONTRIB` (per-candidate, includes both itemized and unitemized) as the authoritative individuals total. Whale tracking stays per-donor in graph (for corporate attribution); grassroots becomes:

```
grassroots = weball.TTL_INDIV_CONTRIB − whale_donor_total
```

This decomposition preserves all the existing whale-trace machinery and just fills in the missing unitemized portion.

### Other open issues

See `todo.md` for the current list. As of 2026-05-08:

- 6/100 sampled candidates have channel-sum mismatches >5% (slight double-counting somewhere, possibly IE/direct overlap for some configurations)
- `wikidata_corporate_resolution` still has bugs (no negative cache, no batched VALUES queries) — corporate attribution is partial; deferred until that asset is fixed
- The cap-based trace fix could be replaced with proper fixed-point iteration with convergence detection

## Validation

Run `scripts/validate_funding_channels.py` for fast smoke tests (BWC sanity, magnitude sanity, channel-sum consistency).

Run `scripts/validation_report.py` for the bulk cross-reference against FEC `weball.TTL_RECEIPTS` — this is the gold standard. Should report median delta < 10% once the unitemized-grassroots fix lands.

## Related docs

- `pipeline.md` — the layer/asset overview, what runs in what order
- `fec-data.md` — FEC bulk file format reference
- `decisions.md` — log of algorithmic and architectural decisions over time
- `todo.md` — current open work and known issues
