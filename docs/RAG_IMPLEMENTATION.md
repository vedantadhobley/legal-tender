# RAG Implementation Plan for Entity Resolution & Corporate Influence Analysis

**Last Updated**: January 9, 2026

---

## Executive Summary

This document outlines the RAG (Retrieval-Augmented Generation) implementation strategy for Legal Tender. The primary goals are:

1. **Employer Name Normalization** - Resolve variations like "Google LLC", "Google Inc", "GOOGLE" to a single canonical entity
2. **Whale-to-Corporation Association** - Link mega-donors to their corporate affiliations for true influence analysis
3. **Reclassify "Individual" Funding** - Expose that much "individual" money is actually corporate influence through executive donations

The result will transform our funding analysis from "68% individual" to a more accurate picture showing corporate reach through employee donations.

---

## The Problem: Current Data Quality Issues

### 1. Employer Name Fragmentation

The same company appears hundreds of ways in FEC data:

```
GOOGLE:
  - "GOOGLE LLC"
  - "GOOGLE INC"
  - "GOOGLE INC."
  - "GOOGLE"
  - "GOOGLE, LLC"
  - "ALPHABET INC"
  - "ALPHABET"
  - "ALPHABET INC."
  - "WAYMO LLC"           # Subsidiary
  - "YOUTUBE LLC"         # Subsidiary
  - "DEEPMIND"            # Subsidiary
  
MICROSOFT:
  - "MICROSOFT CORPORATION"
  - "MICROSOFT CORP"
  - "MICROSOFT CORP."
  - "MICROSOFT"
  - "MSFT"
  - "LINKEDIN"            # Subsidiary
  - "GITHUB INC"          # Subsidiary
  - "ACTIVISION BLIZZARD" # Acquisition
```

**Current Impact**: 
- Corporate influence is fragmented across dozens of employer keys
- Can't accurately answer "How much did Google employees give to Cruz?"
- Subsidiaries and parent companies tracked separately

### 2. Whale Donors Without Corporate Context

We have whale-tier donors ($50K+) identified, but their donations are counted as "individual":

```
Example: Elon Musk
  - Donated $50M+ to various PACs/campaigns
  - Listed employer: "TESLA INC" / "SPACEX" / "SELF-EMPLOYED"
  - Currently counted as: "individual" money
  - Should be counted as: corporate influence (Tesla/SpaceX executive)
```

**The Fiction of "Individual" Donations**:
- A $50M donation from a corporate CEO is NOT equivalent to a $200 donation from a teacher
- Both are technically "individuals" in FEC data
- But one represents concentrated corporate wealth/influence, the other grassroots support

### 3. The "Individual Funding" Illusion

Current stats show:
- **68.2% individual** / 31.8% organization (globally)
- Ted Cruz: **75.9% individual**
- Bernie Sanders: **58.6% individual**

**Reality**:
Much of that "individual" money is:
- Corporate executives donating personal funds
- Bundled donations from employees of the same company
- Founders/billionaires whose wealth comes from corporate holdings

---

## RAG Solution Architecture

### Why RAG?

We need an LLM to:
1. **Semantic matching** - Recognize "GOOGLE LLC" and "ALPHABET INC" are the same entity
2. **Subsidiary resolution** - Know that YouTube is owned by Google/Alphabet
3. **Entity classification** - Determine if a whale donor is a corporate executive
4. **Structured extraction** - Output normalized entity data

Traditional string matching fails because:
- No exhaustive list of company name variations
- Subsidiaries require knowledge of corporate structure
- Spelling errors and abbreviations are unpredictable

### Local LLM Setup

Using **llama.cpp** container running locally (NOT Claude API):

```yaml
# docker-compose.dev.yml addition
llama:
  image: ghcr.io/ggerganov/llama.cpp:server
  ports:
    - "8080:8080"
  volumes:
    - ~/.legal-tender/models:/models
  command: >
    -m /models/mistral-7b-instruct-v0.2.Q4_K_M.gguf
    --host 0.0.0.0
    --port 8080
    --ctx-size 4096
    --n-gpu-layers 35
```

**Why Local LLM?**
- **Cost**: Zero API costs for millions of entity resolutions
- **Privacy**: Sensitive donor data never leaves our infrastructure
- **Speed**: Low latency for batch processing
- **Control**: No rate limits or service dependencies

### RAG Pipeline Components

```
┌─────────────────────────────────────────────────────────────────┐
│                    RAG Entity Resolution Pipeline                │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. RETRIEVAL (Vector Search)                                   │
│     ┌──────────────────────────────────────────────────┐        │
│     │ Query: "GOOGLE LLC"                              │        │
│     │ Search: employer vector index                     │        │
│     │ Results: Similar employer names + known aliases   │        │
│     └──────────────────────────────────────────────────┘        │
│                           ↓                                      │
│  2. CONTEXT BUILDING                                            │
│     ┌──────────────────────────────────────────────────┐        │
│     │ - Candidate matches from vector search            │        │
│     │ - Known corporate hierarchies (parent/subsidiary) │        │
│     │ - SEC/Wikipedia data for company info            │        │
│     └──────────────────────────────────────────────────┘        │
│                           ↓                                      │
│  3. GENERATION (Local LLM)                                      │
│     ┌──────────────────────────────────────────────────┐        │
│     │ Prompt: Given these employer names, which are     │        │
│     │ the same company? Return canonical name + aliases │        │
│     │                                                    │        │
│     │ Output: {                                          │        │
│     │   "canonical": "Alphabet Inc.",                    │        │
│     │   "ticker": "GOOGL",                               │        │
│     │   "aliases": ["GOOGLE LLC", "GOOGLE INC", ...],   │        │
│     │   "subsidiaries": ["YouTube", "Waymo", ...]       │        │
│     │ }                                                  │        │
│     └──────────────────────────────────────────────────┘        │
│                           ↓                                      │
│  4. STORAGE (ArangoDB)                                          │
│     ┌──────────────────────────────────────────────────┐        │
│     │ employers collection:                             │        │
│     │   canonical_name, aliases[], subsidiaries[],      │        │
│     │   ticker, industry, is_fortune_500               │        │
│     └──────────────────────────────────────────────────┘        │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Implementation Phases

### Phase 1: Employer Name Normalization

**Goal**: Resolve all employer name variations to canonical entities

**Process**:
1. Extract unique employer names from `donors` collection (~50K unique)
2. Cluster similar names using embedding similarity
3. Use LLM to confirm/merge clusters
4. Build `canonical_employers` collection with aliases

**Schema**:
```javascript
// canonical_employers collection
{
  "_key": "alphabet_inc",
  "canonical_name": "Alphabet Inc.",
  "display_name": "Google/Alphabet",
  "ticker": "GOOGL",
  "industry": "Technology",
  "is_fortune_500": true,
  "aliases": [
    "GOOGLE LLC", "GOOGLE INC", "GOOGLE INC.", "GOOGLE",
    "ALPHABET INC", "ALPHABET", "ALPHABET INC."
  ],
  "subsidiaries": [
    "YouTube LLC", "Waymo LLC", "DeepMind", "Fitbit Inc"
  ],
  "parent_company": null,  // or _key of parent if subsidiary
  "resolved_by": "llm",    // or "manual", "pattern"
  "confidence": 0.95
}
```

**Edge Collection**:
```javascript
// employer_alias_of (edge)
{
  "_from": "employers/GOOGLE_LLC",      // original fragmented key
  "_to": "canonical_employers/alphabet_inc",
  "alias_type": "name_variation"        // or "subsidiary", "acquisition"
}
```

### Phase 2: Whale-to-Corporation Association

**Goal**: Link mega-donors to their corporate affiliations

**Process**:
1. For each whale donor ($50K+), gather:
   - Listed employer(s) from FEC data
   - Donation patterns (which committees, how much)
   - Name recognition (CEO, founder, executive?)
2. Use LLM to classify:
   - Corporate executive (link to company)
   - Self-employed entrepreneur (link to their company)
   - Inherited wealth (family office)
   - Professional (doctor, lawyer - no corporate link)
3. Create `whale_profiles` with corporate associations

**Schema**:
```javascript
// Enriched donor document
{
  "_key": "MUSK_ELON_TESLA",
  "canonical_name": "Elon Musk",
  "whale_tier": "ultra",
  "total_amount": 50000000,
  
  // NEW: Corporate association
  "corporate_associations": [
    {
      "company_key": "canonical_employers/tesla_inc",
      "company_name": "Tesla Inc.",
      "role": "CEO",
      "association_type": "executive",
      "confidence": 0.99
    },
    {
      "company_key": "canonical_employers/spacex",
      "company_name": "SpaceX",
      "role": "CEO",
      "association_type": "executive",
      "confidence": 0.99
    }
  ],
  
  // NEW: Funding reclassification
  "funding_classification": "corporate_executive",  // vs "grassroots_individual"
  "corporate_influence_flag": true
}
```

### Phase 3: Funding Source Reclassification

**Goal**: Reclassify "individual" funding to show true corporate influence

**New Categories**:
```
FUNDING SOURCES (Revised):
├── INDIVIDUAL
│   ├── grassroots          # < $10K total, non-executive
│   ├── significant         # $10K-$50K, non-executive
│   └── professional        # Lawyers, doctors, etc. (no corp link)
│
├── CORPORATE INFLUENCE (NEW)
│   ├── executive_direct    # C-suite donations
│   ├── founder_direct      # Company founder donations
│   ├── bundled_employees   # Multiple employees of same company
│   └── family_office       # Inherited wealth / investment firms
│
└── ORGANIZATIONAL
    ├── corporate_pac       # Traditional corporate PACs
    ├── labor_union         # Union PACs
    ├── trade_association   # Industry groups
    └── ideological         # Issue-based PACs
```

**New Metrics**:
```javascript
// candidate.funding_summary (revised)
{
  "total_raised": 89900000,
  
  "funding_sources": {
    // OLD (kept for backward compatibility)
    "individual_pct": 75.9,
    "organization_pct": 24.1,
    
    // NEW: True source breakdown
    "true_sources": {
      "grassroots_individual_pct": 35.2,    // Small donors, no corp ties
      "corporate_influence_pct": 52.8,       // Executives + bundled
      "organizational_pct": 12.0             // PACs, unions
    },
    
    // NEW: Corporate influence detail
    "corporate_influence": {
      "executive_donations": 28500000,       // From C-suite
      "bundled_employee_donations": 12300000, // Same company clusters
      "total": 40800000
    }
  },
  
  // NEW: Top corporate influencers
  "top_corporate_influencers": [
    {
      "company": "Goldman Sachs",
      "total": 2500000,
      "executive_amount": 1800000,
      "employee_count": 342
    }
  ]
}
```

---

## Technical Implementation

### 1. Vector Embedding for Employer Similarity

```python
# src/rag/embeddings.py
from sentence_transformers import SentenceTransformer

class EmployerEmbedder:
    def __init__(self):
        self.model = SentenceTransformer('all-MiniLM-L6-v2')
    
    def embed_employer(self, name: str) -> List[float]:
        """Generate embedding for employer name."""
        # Normalize before embedding
        normalized = name.upper().strip()
        normalized = re.sub(r'\b(LLC|INC|CORP|CORPORATION|CO|COMPANY)\b\.?', '', normalized)
        return self.model.encode(normalized).tolist()
    
    def find_similar(self, name: str, candidates: List[str], threshold: float = 0.8):
        """Find similar employer names."""
        query_emb = self.embed_employer(name)
        # Vector search in ArangoDB
        # ... (AQL query with ArangoSearch)
```

### 2. LLM Entity Resolution

```python
# src/rag/entity_resolver.py
import requests

class LocalLLMResolver:
    def __init__(self, llm_url: str = "http://llama:8080"):
        self.llm_url = llm_url
    
    def resolve_employer_cluster(self, names: List[str]) -> dict:
        """Use LLM to resolve a cluster of similar employer names."""
        prompt = f"""You are an entity resolution expert. Given these employer names from FEC donation records, determine if they refer to the same company.

Employer names:
{chr(10).join(f'- {name}' for name in names)}

If they are the same company, respond with JSON:
{{
  "is_same_entity": true,
  "canonical_name": "Official Company Name",
  "ticker": "TICKER or null",
  "aliases": ["list", "of", "all", "variations"],
  "subsidiaries": ["known", "subsidiaries"],
  "confidence": 0.95
}}

If they are different companies, respond with:
{{
  "is_same_entity": false,
  "reason": "explanation"
}}

Respond ONLY with valid JSON, no other text."""

        response = requests.post(
            f"{self.llm_url}/completion",
            json={
                "prompt": prompt,
                "temperature": 0.1,
                "max_tokens": 500,
                "stop": ["}"]
            }
        )
        return self._parse_response(response.json()["content"] + "}")
    
    def classify_whale_donor(self, donor_info: dict) -> dict:
        """Classify a whale donor's corporate association."""
        prompt = f"""You are analyzing a major political donor for corporate influence.

Donor Information:
- Name: {donor_info['name']}
- Listed Employer: {donor_info['employer']}
- Total Donations: ${donor_info['total_amount']:,.0f}
- Whale Tier: {donor_info['whale_tier']}

Based on this information, classify this donor:

1. Is this person likely a corporate executive, founder, or significant shareholder?
2. What company are they most associated with?
3. Should their donations be classified as "corporate influence" rather than "individual"?

Respond with JSON:
{{
  "is_corporate_executive": true/false,
  "company_association": "Company Name" or null,
  "role_estimate": "CEO/Founder/Executive/Professional/Unknown",
  "classification": "corporate_executive" | "founder" | "professional" | "grassroots",
  "confidence": 0.0-1.0,
  "reasoning": "brief explanation"
}}"""

        # ... LLM call
```

### 3. Dagster Asset for RAG Enrichment

```python
# src/assets/enrichment/rag_entity_resolution.py
@asset(
    name="rag_employer_resolution",
    group_name="enrichment",
    deps=["employers", "donors"],
)
def rag_employer_resolution_asset(
    context: AssetExecutionContext,
    arango: ArangoDBResource,
) -> Output[Dict[str, Any]]:
    """Use RAG to resolve employer name variations to canonical entities."""
    
    # 1. Load unique employer names
    # 2. Cluster by embedding similarity
    # 3. Use LLM to confirm/merge clusters
    # 4. Create canonical_employers collection
    # 5. Create employer_alias_of edges
    
    ...
```

---

## Expected Impact

### Before RAG (Current)

```
Ted Cruz Funding:
  Individual: 75.9% ($68.2M)
  Organization: 24.1% ($21.7M)
  
  "Individual" includes:
  - Goldman Sachs executives: $2.5M
  - Oil & gas executives: $4.1M
  - Real estate executives: $1.8M
  - ... all counted as "individual"
```

### After RAG (Target)

```
Ted Cruz Funding (Revised):
  Grassroots Individual: 32.4% ($29.1M)   # True small donors
  Corporate Influence: 55.5% ($49.9M)     # Executives + bundled
  Organizational: 12.1% ($10.9M)          # PACs
  
  Top Corporate Influencers:
  1. Goldman Sachs: $2.5M (exec: $1.8M, employees: 342)
  2. Koch Industries: $1.9M (exec: $1.2M, employees: 89)
  3. AT&T: $1.4M (exec: $0.9M, employees: 156)
```

### Key Metric Changes

| Metric | Before RAG | After RAG | Delta |
|--------|------------|-----------|-------|
| "Individual" funding | 68.2% | ~35% | -33% |
| Corporate influence | (hidden) | ~45% | +45% |
| Organizational | 31.8% | ~20% | -12% |

The ~33% that moves from "individual" to "corporate influence" represents:
- Executive donations misclassified as individual
- Bundled employee donations from same companies
- Founder/billionaire donations tied to corporate holdings

---

## Data Sources for RAG Context

### 1. SEC EDGAR Data
- Company names, tickers, subsidiaries
- Executive compensation (identify who executives are)
- 10-K filings for corporate structure

### 2. Wikipedia/Wikidata
- Company aliases and former names
- Acquisition history
- Key executive names

### 3. Fortune 500/Forbes Lists
- Top companies by revenue
- Billionaire list (for whale matching)

### 4. OpenCorporates
- Business registration data
- Company relationships

---

## Success Criteria

1. **Employer Resolution Rate**: >95% of employer names resolved to canonical entities
2. **Whale Classification**: >90% of whale donors classified for corporate association
3. **Data Quality**: <5% false positive rate on entity merges
4. **Performance**: Process full corpus in <4 hours using local LLM
5. **Insight Quality**: Ability to answer "How much did [Company] influence [Candidate]?" accurately

---

## Next Steps

1. [ ] Set up llama.cpp container with appropriate model
2. [ ] Implement embedding pipeline for employer names
3. [ ] Build clustering algorithm for similar employers
4. [ ] Create LLM prompts for entity resolution
5. [ ] Design canonical_employers collection schema
6. [ ] Build Dagster asset for RAG enrichment
7. [ ] Update aggregation to use new corporate influence metrics
8. [ ] Validate results against known corporate-politician relationships

---

## Appendix: Example Entity Resolutions

### Google/Alphabet Cluster
```
Input names:
  GOOGLE, GOOGLE LLC, GOOGLE INC, GOOGLE INC., ALPHABET, ALPHABET INC,
  YOUTUBE, YOUTUBE LLC, WAYMO, WAYMO LLC, DEEPMIND, FITBIT, FITBIT INC
  
LLM Resolution:
{
  "canonical_name": "Alphabet Inc.",
  "ticker": "GOOGL",
  "aliases": ["GOOGLE", "GOOGLE LLC", "GOOGLE INC", ...],
  "subsidiaries": [
    {"name": "YouTube LLC", "type": "wholly_owned"},
    {"name": "Waymo LLC", "type": "wholly_owned"},
    {"name": "DeepMind", "type": "wholly_owned"},
    {"name": "Fitbit Inc", "type": "acquisition"}
  ]
}
```

### Whale Donor Classification
```
Input:
  Name: "BEZOS, JEFFREY"
  Employer: "AMAZON.COM" / "SELF-EMPLOYED" / "BEZOS EXPEDITIONS"
  Total: $15,000,000
  
LLM Classification:
{
  "is_corporate_executive": true,
  "company_association": "Amazon.com Inc.",
  "role_estimate": "Founder",
  "classification": "founder",
  "confidence": 0.99,
  "reasoning": "Jeffrey Bezos is the founder of Amazon. His donations represent
               corporate influence from Amazon's leadership, not grassroots support."
}
```
