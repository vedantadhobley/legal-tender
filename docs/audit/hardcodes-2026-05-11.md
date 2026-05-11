# Hardcoded data audit — `src/rag/` resolver pipeline

**Date**: 2026-05-11
**Trigger**: end of the day-long debugging session that ended in deleting the
ontology walker. User flagged that hardcoded lists have a habit of growing
into band-aids and asked for a proper audit.

For each hardcoded item: what it is, where it lives, what calls it, and a
verdict — **keep** (legitimate reference data), **trim** (still useful but
should be smaller), or **delete** (dead code or band-aid).

---

## Verdict summary

| Item | File | Size | Status | Verdict |
|---|---|---|---|---|
| Endpoint URLs (WIKIDATA_*, GLEIF_ENDPOINT, RECONCI_ENDPOINT) | client / reconci / gleif | 4 strings | Live | **Keep** — config |
| USER_AGENT | 3 files | 1 string | Live | **Keep** — required by APIs |
| Timeout / retry / batch-size / backoff params | 3 files | ~10 numbers | Live | **Keep** — tuned config |
| `CONFIDENCE_THRESHOLD = 70` | resolver | 1 number | Live | **Keep** — calibrated threshold |
| `ORG_TYPE_QID = "Q43229"` | reconci | 1 string | **Unused** (default now None) | **Delete** |
| `LEGAL_SUFFIXES` regex list | employer_normalization | ~30 entries | Live | **Keep** — finite legal-form catalog |
| `ABBREVIATIONS` (INTL → INTERNATIONAL etc.) | employer_normalization | ~17 entries | Live | **Keep** — finite list |
| `NON_EMPLOYERS` (RETIRED, STUDENT, CORPORATION) | employer_normalization | ~40 entries | Live | **Keep** — filters non-corporate FEC data-entry artifacts |
| `CAMPAIGN_COMMITTEE_MARKERS` (FOR CONGRESS etc.) | employer_normalization | ~9 entries | Live | **Keep** — filters self-funder committee leakage |
| `EMPLOYER_FAMILY_ALIASES` (ADELSON CLINIC → ADELSON DRUG CLINIC) | employer_normalization | 2 entries | Live | **Trim or delete** — only 2 entries, low value |
| `_STRIP_SUFFIX_TOKENS` (GLEIF strict-match) | gleif | ~30 tokens | Live | **Keep** — same as LEGAL_SUFFIXES, for GLEIF path |
| `_RELATIONSHIP_PRIORITY` (founded > ceo_of > employed_by) | wikidata_client | 5 entries | Live (whale path) | **Keep** — relationship ranking config |
| `_GENERIC_DESCRIPTION_PATTERNS` (~25 string patterns) | wikidata_client | ~25 entries | **Dead** in new resolver path, used in legacy `_should_reject_match` | **Delete or partial** — whale path may still want some of it |
| `_GOVERNMENT_DESCRIPTION_PATTERNS` (~10 patterns) | wikidata_client | ~10 entries | **Dead** in new path | **Delete or partial** |
| `_NON_CORPORATE_P31` (~60 Q-ids) | wikidata_client | ~60 entries | **Dead** | **Delete** |
| `_STRICT_CORPORATE_P31` (~22 Q-ids) | wikidata_client | ~22 entries | **Dead** | **Delete** |
| `_RETRY_SUFFIX_TOKENS` (GROUP/HOLDINGS/etc.) | wikidata_client | ~20 entries | **Dead** in new path | **Delete** |
| Many wikidata_client functions (~25 of 27) | wikidata_client | ~1100 LOC | Various — used by whale path + legacy fallback | **Trim** — see below |

---

## Per-item detail

### Endpoint URLs + API config (`*_ENDPOINT`, `USER_AGENT`, timeouts, retries)

Locations:
- `src/rag/wikidata_client.py:37-58`
- `src/rag/wikidata_reconci.py:44-77`
- `src/rag/gleif.py:50-58`

These are configuration: external service URLs, the user-agent string Wikimedia
requires for non-trivial use, request timeouts, retry counts and backoff
parameters. None of them grow on bug-by-bug basis.

**Verdict: keep.** All defensible config. Could move to a single `config.py`
or `.env` if we wanted, but not band-aid.

### `CONFIDENCE_THRESHOLD = 70.0` (resolver)

Single calibrated number. Below it the reconci-link scores are noisy stem-
matches (Deutz-Fahr matched FAHR at score 57); above it the matches are
almost always correct.

**Verdict: keep.** Calibrated parameter, not a list of exceptions.

### `ORG_TYPE_QID = "Q43229"` (reconci)

Was the default `type` param for reconci.link. We discovered the `type` filter
on reconci.link is a *hard* filter (rejects entities whose direct P31 doesn't
include Q43229, even if subclass-of-organization in the ontology). Now we pass
`type=None` by default. The constant exists but is unused.

**Verdict: delete.** Dead value from the type-filter era.

### `LEGAL_SUFFIXES`, `ABBREVIATIONS` (employer_normalization)

`LEGAL_SUFFIXES` (~30 regex patterns): strip "INC", "LLC", "CORP", etc. from
trailing positions. Used during normalization before search.

`ABBREVIATIONS` (~17 entries): expand "INTL" → "INTERNATIONAL" etc.

Both are **finite, closed-domain reference data.** The set of legal-form
suffixes in US/international corporations is bounded and slow-changing.
Same with the common-abbreviation expansions.

**Verdict: keep.** Pure reference data.

### `NON_EMPLOYERS` (employer_normalization, ~40 entries)

`{'RETIRED', 'SELF-EMPLOYED', 'STUDENT', 'HOMEMAKER', 'NONE', 'N/A',
'CORPORATION', 'COMPANY', 'BUSINESS', ...}` — filter out FEC employer-field
values that aren't actual employer names. Donors writing "RETIRED" or just
"CORPORATION" as their employer field; we treat these as "no employer."

Each entry covers a high-frequency artifact in real FEC data. The list is
finite — there are only so many ways donors mess up the employer field.

**Verdict: keep.** Defensible — these aren't "Wikidata classifier exceptions",
they're "values that aren't names at all". Acceptable to add to occasionally
when a new placeholder shape appears.

### `CAMPAIGN_COMMITTEE_MARKERS` (~9 entries)

Strings like "FOR CONGRESS", "FOR SENATE", "VICTORY FUND" — self-funder donors
sometimes list their own campaign committee in the employer field.

**Verdict: keep.** Finite, addresses a real-world data shape.

### `EMPLOYER_FAMILY_ALIASES` (2 entries)

```python
{
    "ADELSON CLINIC": "ADELSON DRUG CLINIC",
    "ULINE INDUSTRIES": "ULINE",
}
```

Only 2 entries. Hardcoded merge of two FEC-name variants into one canonical.
Could be argued as legitimate domain knowledge (these ARE the same entity
in real life) but it has the band-aid shape — adds when discovered, no
upper bound.

**Verdict: trim or delete.** Two entries don't justify the indirection.
Either expand into a real per-name override system (with rationale + dates)
OR delete and accept the slight name-fragmentation. Recommend delete; the
$ split (e.g. $109M + $201M) is visible in `by_organization` output and the
user can decide whether to add the alias post-hoc.

### `_GENERIC_DESCRIPTION_PATTERNS` (~25), `_GOVERNMENT_DESCRIPTION_PATTERNS` (~10)
### `_NON_CORPORATE_P31` (~60 Q-ids), `_STRICT_CORPORATE_P31` (~22 Q-ids)
### `_RETRY_SUFFIX_TOKENS` (~20 entries)

All in `wikidata_client.py`. None called by the new resolver path. Used by:
- `_should_reject_match` (description pattern filters)
- `_entity_has_non_corporate_p31`, `_entity_has_strict_corporate_p31`
- `_alternate_employer_forms` (suffix retry)
- `_resolve_company_one_query`, `_resolve_company_rest`, `_resolve_company_safe`
- `_resolve_person_rest`, `_resolve_person_safe`

The function chain that ends in `resolve_companies_rest` is the LEGACY
employer-resolution path. The asset still imports it as fallback (via
`resolution_path: 'rest'` config option) but the default is now 'resolver'
which doesn't touch any of it.

`_resolve_person_rest` / `resolve_people_rest` IS still used — the **whale
resolution path still goes through wikidata_client directly**, not through
the new resolver. So some of these constants ARE still called by whale code
even though employer code doesn't.

**Verdict: trim.** Either:
1. Apply the same simplification to the whale path (delete the filter chain,
   trust top reconci hits) and then delete ALL of these constants.
2. Keep them for the whale path's filter chain (which has the same band-aid
   problems but at smaller scale — ~1,964 whales vs 5K employers).

Recommend (1) — apply the same simplification to whales. Whales are people-
typed (P31=Q5) so the candidate space is naturally narrower; even simpler
than employers.

### `_RELATIONSHIP_PRIORITY` (5 entries)

```python
{'founded': 0, 'ceo_of': 1, 'owns': 2, 'manages': 3, 'employed_by': 4}
```

Ranks Wikidata person-to-company relationships for the whale path. A whale
with both "founded company X" and "employed by Y" gets "X" as primary.

**Verdict: keep.** 5-entry ranking. Defensible domain modeling.

---

## Wikidata-client function dead-code map

The 27 top-level functions in `wikidata_client.py`:

**Still called by the new resolver path (via wikidata_reconci.py or gleif.py — neither imports wikidata_client directly):**
- None — the new resolver doesn't touch wikidata_client at all.

**Still called by the asset's whale path:**
- `resolve_people` (SPARQL whales — fallback when SPARQL is healthy)
- `resolve_people_rest` (REST whales — current default)
- `_resolve_person_rest`, `_resolve_person_safe` (internals)
- `_wbsearchentities`, `_entity_data`, `_execute_rest`, `_claim_qid` (low-level)
- `_should_reject_match`, `_is_generic_match`, `_is_government_entity`, `_entity_has_non_corporate_p31` (filters)
- `reset_circuit_breaker`

**Dead (employer-resolution legacy, not called by anything):**
- `resolve_companies`, `resolve_companies_rest`, `_resolve_company_safe`,
  `_resolve_company_rest`, `_resolve_company_one_query`
- `_alternate_employer_forms`
- `_entity_has_strict_corporate_p31`
- `_build_company_batch_query`
- `_execute_sparql` (only `_execute_rest` is used)
- `resolve_company_to_canonical` (legacy shim)
- `_build_person_batch_query` (used by `resolve_people`, kept if whales stay)
- `resolve_person_to_companies` (legacy shim)

**Total dead LOC if we drop the legacy employer path: ~600 lines.**

If we also simplify whales the same way: ~900-1000 lines deletable.

---

## Recommended cleanup PR

In order of safety:

1. **Delete `ORG_TYPE_QID` constant** (1 line, unused).
2. **Delete legacy employer path**: `resolve_companies`, `resolve_companies_rest`, `_resolve_company_*`, `_alternate_employer_forms`, `_entity_has_strict_corporate_p31`, `_STRICT_CORPORATE_P31`, `_RETRY_SUFFIX_TOKENS`, `_build_company_batch_query`, `_execute_sparql`, `resolve_company_to_canonical`. Updates the asset's import to drop these. ~600 LOC removed.
3. **Delete `EMPLOYER_FAMILY_ALIASES`** in employer_normalization.py. The 2 entries don't justify the indirection. ~10 LOC.
4. **Apply resolver-simplification to whale path**: replace `resolve_people_rest` with a direct reconci.link call + reconci's score threshold. Delete `_resolve_person_*` and `_should_reject_match` and `_is_generic_match` and `_is_government_entity` and `_entity_has_non_corporate_p31` and `_GENERIC_DESCRIPTION_PATTERNS` and `_GOVERNMENT_DESCRIPTION_PATTERNS` and `_NON_CORPORATE_P31`. ~400 LOC removed.

Result: `wikidata_client.py` drops from 1309 → ~150 LOC, becomes just a thin HTTP wrapper around MediaWiki REST. Most of the file was the band-aid layer.

---

## Open questions

1. **Are the `LEGAL_SUFFIXES` regex patterns redundant with `_STRIP_SUFFIX_TOKENS` in gleif.py?** Both strip the same legal-form suffixes; one is regex-based, one is token-based. Could DRY into one helper.

2. **Is `NON_EMPLOYERS` over-aggressive?** It includes generic words like "CORPORATION", "COMPANY", "BUSINESS" as full-string matches. A donor whose employer is literally "Corporation" (rare but possible) gets filtered. Acceptable trade-off but worth verifying via a quick "how many donors have these as employer?" query.

3. **Where should pure config (endpoint URLs, USER_AGENT, timeouts) live long-term?** Currently spread across 3 files. A single `src/rag/_config.py` or `.env` would be cleaner.
