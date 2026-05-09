"""External API utilities for entity resolution and enrichment.

⚠️ NOTE: Despite the directory name "rag", this is NOT Retrieval-Augmented Generation.
This is a library of utility functions for external API calls:

- employer_normalization.py - Rule-based employer name cleaning (LLC removal, etc.)
- wikidata_client.py - SPARQL queries to Wikidata for corporate relationships

These utilities are USED BY Dagster assets (not standalone scripts).
Example: wikidata_resolution.py imports from here to query Wikidata.

The "rag" name is historical and should be read as "Resolution And Grouping" utilities.
"""

from src.rag.employer_normalization import (
    normalize_employer_name,
    compute_normalized_key,
    find_potential_matches,
    NON_EMPLOYERS,
)

from src.rag.wikidata_client import (
    resolve_companies,
    resolve_people,
    resolve_company_to_canonical,
    resolve_person_to_companies,
    reset_circuit_breaker,
    WikidataCircuitOpen,
)

__all__ = [
    # Employer normalization
    'normalize_employer_name',
    'compute_normalized_key',
    'find_potential_matches',
    'NON_EMPLOYERS',
    # Wikidata resolution (batched API)
    'resolve_companies',
    'resolve_people',
    # Single-name shims
    'resolve_company_to_canonical',
    'resolve_person_to_companies',
    # Circuit breaker control
    'reset_circuit_breaker',
    'WikidataCircuitOpen',
]
