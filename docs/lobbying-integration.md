# Lobbying Integration — Design

> **Status:** design doc, not yet implemented. Captures the shape of the work
> and the design decisions that need to be made before coding.

The project's working title is "trace every dollar to its origin." Lobbying
data is the obvious adjacent dataset. This doc lays out what lobbying data
*is*, how it differs from FEC contribution data, and the design choices
required to integrate it cleanly without overstating what we know.

## What lobbying data is

The Lobbying Disclosure Act of 1995 (LDA) requires registered lobbyists and
their employers to file quarterly reports with the Senate Office of Public
Records. The Senate exposes these as a JSON API: `lda.senate.gov/api/v1/`.

A typical LDA filing (Form LD-2) contains:

- **Client** — the organization being lobbied for (e.g. "AT&T Inc.")
- **Registrant** — the lobbying firm or in-house team (e.g. "Brownstein
  Hyatt Farber Schreck")
- **Filing period** — which quarter (e.g. Q3 2024)
- **Income / Expenses** — dollar amount spent on lobbying that quarter
- **Lobbying activities** — list of:
  - **Issue codes** — general topic taxonomy (e.g. `TAX`, `TRA`,
    `HCR` ~80 codes total)
  - **Specific issues** — free text describing what was lobbied (e.g.
    "H.R. 5376 — Inflation Reduction Act")
  - **Government entities contacted** — House, Senate, specific
    agencies, sometimes specific committees
  - **Lobbyists** — registered lobbyists working this filing

What's *not* in lobbying disclosures:

- **Specific candidate or member of Congress contacted.** Filings disclose
  "the House" or "Senate Banking Committee" — never "Senator Cruz personally."
- **Outcome.** No record of whether lobbying succeeded.
- **Unregistered influence.** Activities below the LDA threshold (~$3K /
  quarter) don't have to be reported. Foreign agents file separately under
  FARA. Trade-association internal advocacy often slips between cracks.

This is fundamentally **different from FEC contribution data**, which has
explicit donor → recipient committee → candidate edges. Lobbying has a
client and a topic but no direct candidate edge.

## What we have already

- `src/api/lobbying_api.py` (63 LOC) — thin client wrapping
  `lda.senate.gov/api/v1/filings/`. Two functions: `search_filings(...)`,
  `get_filing(filing_id)`. Used nowhere else in the codebase.
- `src/api/congress_api.py` — Congress.gov API client. Could provide
  committee assignments per member per cycle.
- `src/assets/mapping/member_fec_mapping.py` — already maps Congress.gov
  members to FEC candidate IDs.

So the building blocks exist; nothing is wired together.

## The linkage problem

To connect lobbying activity to specific candidates, we need a join key.
Lobbying filings name the *organization* and *general issue area*, never
the candidate. Three plausible bridges:

### Option A: Committee jurisdiction

```
client X lobbies on TAX issues
  → House Ways & Means + Senate Finance handle tax
  → candidates on those committees have a relationship to X
```

**Pros:** Straightforward to implement. Issue code → committee mapping is a
small static table. Congress API gives committee membership per cycle.

**Cons:** Fuzzy. "Lobbied on TAX" doesn't mean "lobbied a specific Ways &
Means member." A 100-member committee gets credit for everything its
jurisdiction covers. Diluted signal.

### Option B: Specific bill → vote record

```
client X lobbied on H.R. 5376
  → look up H.R. 5376's vote record
  → candidates who voted yes/no had a position influenced (or not) by X
```

**Pros:** Concrete. Bill IDs are unambiguous in well-formed filings.

**Cons:** Many filings reference bills imprecisely ("various tax-related
legislation"). Vote records exist for floor votes but not for the many
bills that die in committee. Position correlation isn't causation —
Senator's vote may not be because of lobbyist's influence.

### Option C: Parallel dataset, no forced linkage

Don't try to join lobbying to candidates. Present lobbying as its own
view: *"For organization X, here's their lobbying spending alongside their
PAC contributions and IE spending."*

```
GOLDMAN SACHS:
  PAC contributions to candidates: $5.2M (from FEC)
  IE attribution via founders: $0 (no Wikidata link)
  Lobbying spend 2024: $4.8M (from LDA)
  Top issues lobbied: BANKING, TAX, FINANCIAL SERVICES
```

**Pros:** Honest about what the data does and doesn't tell us. Avoids the
overreach of "lobbying ⇒ influenced this specific senator." Useful for
research even without joining to candidates.

**Cons:** Less satisfying as a "trace upstream" story. Doesn't show in
candidate funding_channels output.

## Recommended approach

**Option C first, optionally A as a second layer.**

Reasoning:
- Option C is cheap (~3-5 days of work: sync filings → parse to Arango →
  join to existing `corporate_families` collection by client name → add a
  `lobbying` view to `output_check.py` and any future API).
- Option C produces an honest, useful artifact even on day 1.
- Option A can be added later as a "members likely influenced by X's
  lobbying" cross-cut, separate from `funding_channels`. Avoids
  contaminating the funding_channels schema with a fuzzier signal.
- Option B is too far in the weeds to attempt for the project's primary use
  case; it's a research-grade analysis better done ad hoc.

## Concrete first-pass scope

If/when we start this:

1. **Sync asset.** `lobbying_filings_sync` — paginate `search_filings` with
   `year` filter for active cycles, store raw JSON in `cache/lobbying/{year}/`.
   Incremental: only refetch quarters newer than last fetch.
2. **Parse asset.** `lobbying_filings` — JSON → Arango. New collections:
   - `lobbying_clients` (vertex) — one per client_name (post-canonicalization)
   - `lobbying_registrants` (vertex) — one per registrant_name
   - `lobbying_filings` (document) — period, $, issue codes, raw filing json
   - `lobbied_for` (edge) — registrant → client
3. **Client → corporate_family join.** Client names overlap heavily with
   FEC employer/donor canonical names. Use the same canonical_employers /
   wikidata_corporate_resolution machinery to map clients to existing
   `corporate_families` entries. New edge: `corporate_families/X
   --lobbies--> issue_areas/Y` with $ totals.
4. **Surface in output_check.py.** When you look up Cruz, show his
   committee assignments (from Congress API + member_fec_mapping) and the
   top organizations lobbying on those committees' jurisdictions. Don't
   claim influence — just show the parallel data.
5. **Optional Option-A layer.** Issue-code → committee-jurisdiction static
   table. Cross-reference filings with current committee membership.
   Surface as a separate "by_committee" cross-cut.

## What to defer

- **FARA filings** (foreign agents). Separate database, separate compliance.
  Worth a parallel asset eventually but lower priority.
- **State-level lobbying.** Each state has its own disclosure regime. Not
  unifiable without massive scope expansion.
- **Lobbyist → candidate personal contributions.** Already captured in
  FEC indiv data; the ENTITY_TP=IND records of registered lobbyists
  flow through the existing donors graph. No new work needed for that
  signal.
- **Bills + votes (Option B).** Adjacent project. Out of scope for v1.

## Open questions

- **Client name canonicalization.** LDA client names will fragment the same
  way FEC employer names do (Goldman Sachs Group / Goldman Sachs / GS Bank).
  The same Wikidata-driven consolidation we need for FEC will apply here.
  This is another reason Wikidata is load-bearing.
- **Reporting cadence.** LDA filings are quarterly; FEC contributions are
  itemized per transaction. How do we present time-series?
- **Should lobbying contribute to funding_channels.unaccounted?** Currently
  unaccounted is "FEC trace loss + unitemized donors." Lobbying isn't
  receipts to the candidate's account — it's third-party spending on
  influence. Probably keep them entirely separate views.

## Source priority order before starting

1. **Wikidata corporate-family resolution must be running** (currently
   blocked, see todo.md). Without it, client name joins will be incomplete
   and the lobbying view will look as fragmented as FEC employer data.
2. **Donor canonicalization landed** (gated on #1) — same reason: name
   variants need to be merged before joining to FEC entities.
3. **Then** lobbying sync + parse + join can begin.

Until #1 and #2 are done, lobbying integration is premature — we'd be
joining one fragmented dataset to another.
