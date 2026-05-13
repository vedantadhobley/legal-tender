# Terminal-node classification audit — 2026-05-13

Comprehensive audit of every `terminal_type` bucket after today's M-ORG_TP refinement via Wikidata P31. Goal: identify systemic misclassifications, coverage gaps, and the dollar value affected by each.

Audit method: query top 30-40 committees by `total_receipts` per bucket; eyeball each entry against its CMTE_NM + provenance flags; cross-reference suspicious cases against FEC's `cm`/`webk` records.

## Summary

| Bucket | Committees | Total receipts | Verdict |
|---|---|---|---|
| campaign | 16,587 | (not audited — straightforward) | — |
| **passthrough** | 9,339 | **$47.01B** | Clean. Top 40 all correctly conduits / JFCs / parties / leadership PACs |
| **super_pac_unclassified** | 6,226 | **$11.39B** | Correctly handled by recursive trace. Not a misclassification — these are vehicles, not sources |
| **corporation** | 2,179 | **$1.50B** | Clean except 2 visible mis-classifications totaling $56M |
| **trade_association** | 847 | **$0.78B** | Clean. 33 Wikidata-flipped entries all correct. 2 stale provenance flags (cosmetic) |
| **labor_union** | 409 | **$1.66B** | Clean. All real unions including Phase 2a/2b inheritance recoveries |
| **ideological** | 425 | **$0.91B** | Mostly clean. Coverage gap: ~$30-40M of trade-shaped PACs that Wikidata P31 didn't flip |
| **cooperative** | 57 | **$30M** | Clean. All real ag/electric cooperatives + credit unions |
| **unknown** | 31 | (negligible attributed) | 3 "phantom committees" totaling $211M with empty metadata + zero edges — cosmetic noise, no candidate impact |

Total: 36,100 committees across all buckets. Aggregate receipts: ~$62B.

## Per-bucket findings

### corporation ($1.50B, 2,179 committees) — clean except two

Top 30 are all real corporate PACs (Honeywell, Boeing, Northrop, UPS, Lockheed, Comcast, Koch, UnitedHealth, GM, RTX, AT&T, Charter, PwC, Walmart, Cigna, UBS, L3Harris, BNSF, General Dynamics, NextEra, Pfizer, Eli Lilly, Union Pacific, Aflac, New York Life).

**Mis-classified:**

| CMTE_NM | $ | Should be | Why |
|---|---|---|---|
| DEMOCRACY ENGINE, INC., PAC | $46.3M | `passthrough` | Payment-processor conduit for Dem small-dollar money — structurally like WinRed/ActBlue. ORG_TP=C is technically correct (Delaware corp) but functionally wrong. Fix: add to `CONDUIT_PATTERNS` list in `candidate_upstream.py`. |
| BLACK AMERICA'S POLITICAL ACTION COMMITTEE (BAMPAC) | $10.3M | `ideological` | Republican-aligned advocacy org, not a corporate PAC. ORG_TP=C is wrong upstream FEC labeling. No automated fix available — would need a per-entity override. Defer. |

Total mis-classified: $56.6M (3.8% of bucket). Acceptable.

### trade_association ($0.78B, 847 committees) — clean

Top 30 all real trade/professional bodies. Provenance breakdown:
- 24 from `ORG_TP=T` (FEC self-classified)
- 9 from Wikidata P31 (today's refinement) or Phase 2a/2b inheritance

All 33 Wikidata P31 flips manually verified — every flip is semantically correct.

**Cosmetic issue:** 2 committees (IFW + ASIS) have stale `terminal_type_refined_from_m_org_wikidata=True` provenance flags from before Q484652 was removed from `_TRADE_CLASS_QIDS`. Their `terminal_type` was correctly reverted by Phase 1 to `ideological`, but the provenance fields still say "international organization." Fix: at the start of Phase 1b, clear all `terminal_type_refined_from_m_org_wikidata` / `terminal_type_wikidata_*` flags before re-deriving.

### ideological ($0.91B, 425 committees) — coverage gap is the main concern

Top genuine ideological: AIPAC ($109M), NRA ($54M), J Street ($35M), Club for Growth Action ($263M Phase-2b-inherited) + PAC ($16M), RJC ($9.4M), LCV Victory Fund ($221M inherited) + Action Fund ($9.2M), Pro-Israel America, Sierra Club, Planned Parenthood, Reproductive Freedom, Citizens United, No Labels (Action $3.5M + PAC $1.6M), Safari Club, Humane World, Environment America, Environmental Defense Action Fund.

**False negatives** — committees currently `ideological` that are structurally trade-shaped per a non-Wikidata read but that Wikidata didn't tag with our trade-class Q-id set:

| CMTE_NM | $ | Wikidata's call | What it actually is |
|---|---|---|---|
| AMERICAN ASSOCIATION FOR JUSTICE | $24.0M | "advocacy group" | Trial lawyers trade body (formerly ATLA) |
| COUNCIL OF INSURANCE AGENTS & BROKERS | $17.2M | Wrong entity (Alberta Insurance Council) | Insurance brokerage trade body |
| AAOS Orthopaedic Surgeons | $7.4M | No hit (CMTE_NM prefix "POLITICAL ACTION COMMITTEE OF THE" not stripped) | Medical society |
| AANA Nurse Anesthetists | $6.7M | No hit ("SEPARATE SEGREGATED FUND" suffix not stripped) | Medical society |
| ACEP Emergency Physicians | $6.1M | No hit (complex multi-org name) | Medical society |
| Texas Farm Bureau AGFUND | $5.7M | "farm bureau" (Q-id NOT in our set) | Agricultural trade body |
| Blue Cross Blue Shield of Michigan | $4.5M | "company" only | Insurance trade body |
| APTA Physical Therapy | $4.0M | Wrong entity (scientific journal) | Medical society |
| NFIB | $3.8M | "nonprofit organization" only | Small-business trade body |
| AOPA Pilots | $3.8M | "advocacy group" | Pilot trade body (debatable — does both) |
| ACOG OB-GYN | $3.5M | Wrong entity (scholarly article) | Medical society |
| American Academy of Ophthalmology | $3.3M | No hit | Medical society |
| American Sugar Cane League | $3.1M | No hit | Agricultural trade |
| NVCA Venture Capital | $2.6M | No hit | Industry trade |
| American Osteopathic Information Association | $2.3M | No hit | Medical society |
| American Soybean Association | $1.9M | "association" only | Agricultural trade |
| American Association of Nurse Practitioners | $1.6M | "organization" only | Medical society |
| MLB Commissioner Office | $1.5M | No hit | Industry trade (sports) |
| NASW Social Workers | $1.3M | No hit | Professional society |
| Ohio Farm Bureau | $1.2M | No hit | Agricultural trade |

**Total false-negative coverage gap: ~$80M** (vs $117M flipped). Roughly 60/40 flip-rate.

Reasons for failure (in rough order of frequency):
1. **Wikidata classifies as "advocacy group" rather than trade** — semantic call by Wikidata. Defensible for borderline cases (AAJ, AOPA do both advocacy and trade). 30-40% of gap.
2. **Search-name stripping fails** — "POLITICAL ACTION COMMITTEE OF THE X" prefix, "SEPARATE SEGREGATED FUND" suffix, parentheticals with abbreviations. ~25% of gap.
3. **Wikidata wrong entity match** — scientific journals (APTA), scholarly articles (ACOG). Caused by ambiguous PAC abbreviations resolving to publications. ~15% of gap.
4. **Wikidata only generic classifications** — "nonprofit organization", "organization", "association" without a specific trade-class P31. ~20% of gap.
5. **No Wikidata hit at all** — small PACs not in Wikidata. ~15% of gap. (OpenCorporates would help here.)

**Fix candidates** (ordered by impact-per-effort):
- **Add "farm bureau" Q-id** to `_TRADE_CLASS_QIDS` — would catch Texas Farm Bureau ($5.7M), Ohio Farm Bureau ($1.2M), other state farm bureaus. Q-id is whatever Wikidata uses; need to look up.
- **Extend `_CMTE_NAME_SUFFIXES`** with " SEPARATE SEGREGATED FUND". Catches AANA ($6.7M) + any other SSF-named PACs.
- **Add CMTE_NM prefix-stripping** for "POLITICAL ACTION COMMITTEE OF THE X" / "PAC OF X" → use X as search name. Catches AAOS ($7.4M).
- **OpenCorporates Layer 3** (already in roadmap) — catches the small no-Wikidata-hit cases.

### labor_union ($1.66B, 409 committees) — clean

Top 30 all real unions: SEIU COPE, AFSCME, UFCW, Teamsters (D.R.I.V.E.), AFT, 1199SEIU, IBEW, UAW, LIUNA, UA Plumbers, CWA Working Voices (inherited), CWA-COPE, NEA Fund (inherited), NEA Advocacy Fund (inherited), Working for Working Americans (inherited from Carpenters), Letter Carriers, Firefighters, ALPA Pilots, Sheet Metal, Operating Engineers, etc.

Phase 2a/2b inheritance pulled in 16 union-affiliated committees that had ORG_TP=C (corporation) or super_pac_unclassified. All inherited correctly.

No misclassifications visible.

### cooperative ($30M, 57 committees) — clean

All 25 entries above $100K are real agricultural/electric cooperatives or credit unions: American Crystal Sugar, Amalgamated Sugar, Southern MN Beet Sugar, California Dairies, Minn-Dak Farmers, Dairy Farmers of America, CHS, Land O'Lakes, Ocean Spray, Blue Diamond Growers, Sugar Cane Growers Coop, Sunkist Growers, Riceland Foods, Almond Alliance, Security Service FCU, etc.

**One FEC-data quirk noted:** BLUMENAUER CENTURY FUND ($600K) is a leadership PAC (CMTE_TP=N) but ORG_TP=V (cooperative). Our Phase 1 logic gives ORG_TP precedence, so it lands here. Upstream FEC labeling issue; not worth fixing.

### unknown ($211M in 3 phantom committees, 31 committees) — cosmetic noise

**3 phantom committees with empty metadata** totaling $211M:
- `C30003578`: $100M receipts, 1 indiv record, ZERO edges to anywhere (no donors, no transfers, no IE, no candidate links)
- `C00831388`: $61.4M receipts, 16 indiv records, 4 contributed_to edges
- `C00695320`: $50M receipts, ZERO records anywhere

These have no `cm.zip` master record (empty CMTE_NM/CMTE_TP/ORG_TP) but receipts attached. FEC bulk-data inconsistency — a few `indiv.zip` records contain CMTE_IDs that don't appear in `cm.zip`.

**Impact**: zero. These committees have no `affiliated_with` edges to any candidate, so they're not in any candidate's trace path. They're orphan records taking up space in the committees collection. Could be filtered out by requiring `cm.zip` membership at committee-build time, but it's cosmetic noise, not blocking.

Remaining 28 unknown entries are all $0 or near-$0 — dead/inactive committees.

### super_pac_unclassified ($11.39B, 6,226 committees) — design question, not a bug

This is the biggest non-campaign bucket by receipts ($11.39B). The top 40 alone are $6.97B (61% of bucket).

These are structurally Super PACs (CMTE_TP=O, the FEC type for IE-only committees that take unlimited contributions and spend on IEs). They're NOT "unclassified" in the sense of "we don't know what they are." They're known and well-defined; they just don't have ORG_TP because Super PACs don't have a "connected organization" the way a corporate PAC does.

Top entries by political shape (informally):

**Republican machine SPACs**: SLF PAC $1.14B, MAGA Inc $521M, AMERICA PAC $311M (Musk), American Crossroads $149M (Rove), Never Back Down $145M (DeSantis), America First Action $154M, Restoration of America $192M, Keystone Renewal $64M, Sentinel Action Fund $60M

**Democratic machine SPACs**: SMP $1.11B, WINSENATE $313M, DEMOCRACY PAC + II $455M (Soros), BlackPAC $92M, Georgia Honor $80M, Forward Majority $77M, Independence USA $71M (Bloomberg), Workers Vote $68M

**Issue/cause SPACs**: Everytown Victory Fund $79M, Planned Parenthood Votes $77M, EDF Action Votes $46M, Defend American Jobs $70M, School Freedom Fund $46M, Leading the Future $125M, Women Vote $115M (EMILY's List arm)

**Industry SPAC**: FAIRSHAKE $358M (crypto)

**Foreign-policy lobby SPAC**: UDP $209M (AIPAC's Super PAC)

**Decision: NO 6th terminal_type bucket.** These aren't source-of-money entities; they're vehicles. The recursive IE trace (shipped 2026-05-12) correctly attributes their actual donors to `by_corporation` / `by_individual`. Adding a "super_pac" bucket would imply they're terminal, which they're not.

**Potential improvement (deferred):** ~10 of these have CONNECTED_ORG_NM matching a labor union or trade body that COULD inherit via Phase 2a if matching were fuzzier:

| Super PAC | $ | CONNECTED_ORG_NM | Could inherit |
|---|---|---|---|
| UNITE HERE PAC | $26.7M | UNITE HERE | labor_union |
| UFCW Super PAC | $20.3M | UNITED FOOD AND COMMERCIAL WORKERS INTERNATIONAL UNION | labor_union |
| EARN IUOE | $20.1M | EARN INTERNATIONAL UNION OF OPERATING ENGINEERS | labor_union |
| USW WORKS | $9.3M | UNITED STEELWORKERS | labor_union |
| VOTE NURSES VALUES PAC | $8.3M | CALIFORNIA NURSES ASSOCIATION | labor_union or trade |

Phase 2a currently uses exact-string CMTE_NM match. These miss because the parent labor cmte's name is like "UFCW INTERNATIONAL UNION ACTIVE BALLOT CLUB" not exactly "UFCW INTERNATIONAL UNION." Fix: relax Phase 2a to startswith() or prefix-match, OR add a CMTE_NM-cluster sub-pass. Net gain: ~$85M attributed at the proper rollup level.

## Aggregate misclassification picture

| Issue | $ affected | Severity |
|---|---|---|
| Corp bucket — Democracy Engine + BAMPAC | $56.6M | Low (visible in by_organization; documented) |
| Trade-shape coverage gap in ideological | $80M | Medium (real trade orgs stay in advocacy bucket) |
| Phantom committees in unknown | $211M | None (no candidate impact) |
| Super PACs that could inherit labor/trade from CONNECTED | ~$85M | Low (correct attribution via recursive trace; this would just improve org-rollup display) |
| Stale provenance flags (IFW + ASIS) | $0 | Cosmetic |

**Total visible misclassification: ~$140M** out of ~$62B classified (0.2% of total receipts). This is the same order of magnitude as our bulk-validation median |Δ| (2.4%).

## Recommended fixes (ordered by impact-per-effort)

1. **Extend `_CMTE_NAME_SUFFIXES`** with " SEPARATE SEGREGATED FUND" — catches AANA ($6.7M) and a handful of others. ~2-line code change. Re-run Phase 2b's name-cluster inheritance picks up these from siblings.

2. **Add CMTE_NM prefix-stripping** for "POLITICAL ACTION COMMITTEE OF THE X" / "PAC OF X" — catches AAOS ($7.4M) and the misformed FEC namings. Small code change in `_pac_search_name`.

3. **Add Wikidata "farm bureau" Q-id** to `_TRADE_CLASS_QIDS` (after looking up the actual Q-id) — catches Texas Farm Bureau ($5.7M) and similar state farm bureaus.

4. **Filter DEMOCRACY ENGINE** in CONDUIT_PATTERNS — moves $46M from corporation to passthrough so it doesn't appear as a "corporate source" in by_organization. One-line addition.

5. **Add provenance-flag cleanup** at the start of Phase 1b — clear `terminal_type_refined_from_m_org_wikidata` and `terminal_type_wikidata_*` flags before re-deriving so stale flags don't accumulate when the trade-class set changes.

6. **Relax Phase 2a CONNECTED-match to prefix/startswith** for super_pac_unclassified → labor_union inheritance — picks up UFCW SPAC, UNITE HERE PAC, etc. (~$85M proper org-rollup).

7. **Deferred (waiting on OpenCorporates API key)**: closes most of the remaining $30-40M no-Wikidata-hit coverage gap.

## What this audit confirms

- **The trace algorithm correctness is independent of classification**: $11.39B in super_pac_unclassified is correctly attributed downstream via recursive IE trace; the classification only affects rollup display.
- **The Wikidata P31 approach is correct** but limited by Wikidata's own coverage of small PACs and by semantic disagreements ("advocacy group" vs "trade body" for AAJ-like cases). We're getting ~60% of what's possible.
- **Phase 2a/2b inheritance works** — it pulled NEA Fund, CWA Working Voices, Working for Working Americans, NRA ILA, Club for Growth Action, NAR Congressional Fund into the right buckets without manual overrides.
- **The terminal_type system is structurally sound** — every bucket's top entries are correctly classified by category; misclassifications are at the long-tail edge cases.

## Next session candidates

Listed in `docs/todo.md`. The audit-derived ones:

- Cleanup pass for stale Wikidata provenance flags
- Add the four targeted token / Q-id additions (SSF suffix, PAC-OF prefix, farm bureau Q-id, DEMOCRACY ENGINE conduit)
- Fuzzy CONNECTED match in Phase 2a for labor-union SPACs
