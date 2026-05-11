# Corporate-identity resolution

A reusable pattern for resolving messy real-world entity strings (e.g., a
freeform "employer" field on a campaign-finance filing) to canonical
corporate identities (Wikidata Q-id, LEI, etc.) with provenance.

This doc is intentionally **portable**. The same pattern works for any
problem shaped like:

> Given input string `s`, find the canonical entity for `s` across multiple
> data sources, with provenance for review when matches look wrong.

In `legal-tender` we use it for FEC donor employer fields. Same machinery
applies to FollowTheMoney style entity resolution, KYC name screening,
public-record join keys, etc.

## Goal

Map noisy free-text strings → canonical, reviewable, machine-readable
identifiers. Specifically:

| Input | Desired output |
|---|---|
| `"GOLDMAN SACHS"` | Wikidata Q193326, label "Goldman Sachs" |
| `"BAUPOST GROUP"` | Wikidata Q4873568, label "Baupost Group" |
| `"PRATT INDUSTRIES"` | LEI 5493000ZP35JPH3P9055, legal name "PRATT INDUSTRIES, INC." |
| `"RETIRED"` | Filtered at input — not an entity at all |
| `"XYZ12345NONSENSE"` | `not_found` with provenance: tried sources A,B; no match |

## Anti-goal: don't recreate the source-of-truth's classification

The temptation when handling messy data is to layer your own classification
rules — "if it's a video game, reject; if it's a hotel chain, accept; if
it's a municipality with government-org ancestry, reject." This leads to a
growing rule set that fights the upstream data's own ontology.

The pattern below **trusts the source** and provides reviewable provenance
when matches go wrong, rather than pre-emptively classifying every edge case.

## The pattern (layered fallback with confidence)

```
                ┌────────────────────────────┐
                │  Layer 1: knowledge graph  │
   FEC string ─→│  (Wikidata reconciliation)│─→ if confidence ≥ T: take it
                └────────────────────────────┘
                              │
                              │ not_found
                              ↓
                ┌────────────────────────────┐
                │  Layer 2: official registry│
                │  (GLEIF LEI registry)     │─→ if strict name match: take it
                └────────────────────────────┘
                              │
                              │ not_found
                              ↓
                ┌────────────────────────────┐
                │  Layer N: extension slots │
                │  (OpenCorporates, SEC,    │
                │   state registries, ...)  │
                └────────────────────────────┘
                              │
                              ↓
                       not_found
                       (with provenance)
```

Each layer has a **distinct precision/recall profile**. The ordering is from
broadest coverage (Wikidata knows Goldman Sachs, Apple, most well-known
entities) to narrowest coverage but highest precision (GLEIF knows only
LEI-registered entities — mostly financial — but the match is authoritative).

A failed layer returns `not_found` cleanly. The next layer takes its shot.

## Layer 1: knowledge graph (Wikidata via reconci.link)

**What it is**: An ElasticSearch-backed reconciliation service over the
entire Wikidata knowledge graph. Returns top-N candidates per query with a
relevance score (0-100) and the candidate's RDF types.

**Service**: `https://wikidata.reconci.link/en/api` — third-party, free,
maintained by Antonin Delpeuch. (Wikimedia's own SPARQL endpoint is the
alternative if reconci.link goes down; see `wikidata_client.py` for the
SPARQL paths kept as fallback.)

**Why it's the default first layer**: covers ~2 million organizations
globally including most well-known ones; fast; batched (up to 50 names per
HTTP request); returns scores so we can threshold.

**Acceptance rule**: top hit at `score >= 70`. Below 70 the matches are
mostly noise (substring matches, stem-similarity false positives like
"FAHR" → "Deutz-Fahr" at score 57).

**What it misses**: small US private firms (not famous enough for
Wikipedia), state-and-local entities not deemed notable, recently founded
companies, entities whose only Wikidata entry is the wrong "Q" for our
context (e.g., a song or town with the same name).

## Layer 2: official registry (GLEIF)

**What it is**: The Global Legal Entity Identifier registry — the
authoritative source for LEI codes mandated by financial regulators
worldwide. ~2.6M active LEIs.

**Service**: `https://api.gleif.org/api/v1/lei-records` — free, no auth,
authoritative.

**Why it's a useful second layer**: catches small US private firms whose
LEI registration is mandatory for financial transactions even though they
have no Wikipedia page. Pratt Industries Inc., Uline Inc., Mountaire Corp,
LinkedIn Corporation — all in GLEIF, none findable cleanly in Wikidata.

**Acceptance rule**: **strict match only.** The GLEIF legal_name (after
stripping legal-form suffixes) must equal the input (after stripping
legal-form suffixes), case-insensitive. ACTIVE status required.

We're strict here on purpose. GLEIF's full-text search returns lots of
noise on short queries (KKR returns 1094 mostly-Indian-company hits with
matching letters). Strict equality eliminates false positives at the cost
of missing fuzzy matches; the strictness is what makes GLEIF a useful
*fallback* rather than a primary.

**What it misses**: entities below the LEI registration threshold (small
non-financial LLCs, sole proprietorships, religious orgs, etc.); entities
that haven't bothered registering.

## Layer 3+: extension slots

The pattern accepts any number of additional layers, each with its own
precision/recall profile. Candidates not yet implemented:

- **OpenCorporates** (`api.opencorporates.com`): ~200M companies including
  most US small private. Free tier 500 reqs/day; paid tier for production.
  Would catch most of the long-tail not-founds.
- **SEC EDGAR**: US public companies + subsidiaries. Authoritative for that
  slice.
- **State corporation registries**: Delaware, NY, CA, TX, etc. each
  publish their corp registries; would catch small-LLC long tail.
- **Local LLM-based normalization**: for garbled FEC names (typos, partial
  entries), ask an LLM "normalize this to a canonical company name" before
  the layered lookup.

## Provenance, not classification

Every resolution carries:
- `source` — which layer matched (`wikidata`, `gleif`, ...)
- `canonical` — the layer's canonical name for the entity
- `wikidata_id` (or `external_id` for non-Wikidata sources)
- `confidence` — score from the matching layer (0-100)
- `method` — finer-grained label (`reconci_top`, `gleif_strict_match_us`)
- `alternatives` — top other candidates the layer considered (for review)

When a resolution looks wrong, the alternatives field tells you what else
the layer saw. If the bogus match is material, two options:

1. **Override at the call-site**, e.g., a YAML file with `{fec_name → forced_qid}`. Keep this list short; entries should have rationale + verification dates.
2. **Accept and document** — for a $4K bogus match, not worth the override.

We deliberately **don't** maintain a classifier that says "this Q-id is or
isn't an employer." Wikidata's own ontology is the source of truth; if it
disagrees with our domain model, we live with the small false-positive
rate and surface bogus matches in downstream reporting where they're easy
to spot.

## What we ELIMINATED and why

Earlier iterations tried to filter Wikidata candidates by their P31 (RDF
type) — accept only candidates whose type was in an org-shaped whitelist
(or not in a non-org blacklist). This grew into a band-aid layer:

- A 60-Q-id "non-corporate" blacklist (films, books, vaccines, given names, ...)
- A 22-Q-id "strict corporate" whitelist (business, bank, university, ...)
- A 25-string description-pattern blacklist ("type of", "fictional", ...)
- A 10-string government-description filter ("federal government", ...)
- A 20-token suffix-retry list (GROUP, HOLDINGS, ...)
- A walker over Wikidata's P279 subclass tree with its own root sets

Each entry was added to fix a specific case. The list grew with every new
weird match. Cache corruption from parallel walks introduced silent
classification flips. Days of debugging.

The exit point: **delete it all**. The single confidence threshold + GLEIF
fallback + few-entry overrides recovers the legitimate matches without the
maintenance overhead. Some bogus matches survive (RDV → "Rice dwarf
virus") but they show up in `by_organization` output where they're
inspectable.

The lesson: don't try to re-classify the source data. Trust + threshold +
provenance >> filter chain.

## Code shape

```
src/rag/
├── wikidata_reconci.py    # Layer 1 client (~250 LOC)
├── gleif.py               # Layer 2 client (~260 LOC)
├── wikidata_resolver.py   # Layered orchestration (~150 LOC)
└── wikidata_client.py     # Lower-level HTTP for Wikidata REST + SPARQL
                           # (used for whale-person-to-company lookups
                           # and as a backup path; legacy filter chain
                           # within is mostly dead code)
```

`wikidata_resolver.resolve_batch(names) → {name: ResolutionResult}` is the
public API. ResolutionResult has the schema noted above (source, canonical,
wikidata_id, external_id, method, confidence, alternatives).

## To port to another project

1. Copy `wikidata_reconci.py` (Layer 1 client).
2. Copy `gleif.py` (Layer 2 client) — adapt the strict-match heuristic if
   your domain has different name conventions.
3. Copy `wikidata_resolver.py` — minimal orchestration, easy to adapt.
4. Skip `wikidata_client.py` unless you need SPARQL fallback or person→
   company lookups.
5. Confidence threshold (70) is FEC-employer-tuned. If your data is cleaner
   (e.g., already normalized company names), raise to 80-90. If messier
   (typos common), lower to 50-60 and accept more noise.

The only application-specific bits are: which layers to use, in what
order, and the confidence threshold. Everything else is generic name-to-
entity resolution.

## Limits — current ceiling

For FEC donor employers, current coverage is roughly:

- Layer 1 (Wikidata): ~50-55% of names — well-known corporates resolve
  cleanly at score 100
- Layer 2 (GLEIF): adds ~5-10% — small US private firms with LEIs
- Combined: ~55-65% hit rate

The remaining ~35-45% break down (rough proportions):

- **Wikidata data gaps** (~40%): real corporations that simply don't have
  a Wikidata entry. Adding OpenCorporates closes this.
- **FEC data hygiene** (~25%): "RETIRED", "CO-OWNER", "CANDIDATE", etc.
  Already filtered at `NON_EMPLOYERS` in `employer_normalization.py`; the
  remaining "non-employer" strings are the long tail.
- **Name garbling** (~20%): typos, partial entries, multi-word employer
  fields with weird formatting. Would benefit from LLM-based normalization.
- **Foreign entities** (~10%): non-US firms whose names FEC donors listed
  in English. Neither Wikidata nor GLEIF cover them well.
- **Genuine non-entities** (~5%): random text that's not a company at all.

Path to higher coverage: add OpenCorporates (Layer 3), then LLM-based
input normalization, then accept that the long tail is irreducible.
