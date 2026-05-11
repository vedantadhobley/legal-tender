"""External API utilities for entity resolution and enrichment.

⚠️ NOTE: Despite the directory name "rag", this is NOT Retrieval-Augmented Generation.
This is a library of name-resolution helpers wrapping external services:

- employer_normalization.py — rule-based cleaning of FEC employer strings
- wikidata_reconci.py — Wikidata reconciliation API client (Layer 1)
- gleif.py — GLEIF LEI registry client (Layer 2)
- wikidata_resolver.py — layered fallback orchestration (Wikidata + GLEIF)
- wikidata_client.py — low-level Wikidata REST client (transport + entity-data fetch)
- whale_resolver.py — person→company resolution (parallels wikidata_resolver)

These utilities are imported by Dagster assets (not standalone scripts).
Example: `wikidata_resolution.py` imports `resolve_batch` from `wikidata_resolver`.

The "rag" name is historical and should be read as "Resolution And Grouping".
"""

from src.rag.employer_normalization import (
    NON_EMPLOYERS,
    compute_normalized_key,
    find_potential_matches,
    normalize_employer_name,
)
from src.rag.wikidata_client import WikidataCircuitOpen, reset_circuit_breaker
from src.rag.wikidata_resolver import resolve_batch
from src.rag.whale_resolver import resolve_people_batch

__all__ = [
    # Employer normalization
    "normalize_employer_name",
    "compute_normalized_key",
    "find_potential_matches",
    "NON_EMPLOYERS",
    # Resolution pipeline
    "resolve_batch",
    # Whale path (person → company)
    "resolve_people_batch",
    # Circuit breaker control
    "reset_circuit_breaker",
    "WikidataCircuitOpen",
]
