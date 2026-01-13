"""RAG utilities for entity resolution and enrichment."""

from src.rag.employer_normalization import (
    normalize_employer_name,
    compute_normalized_key,
    find_potential_matches,
    get_canonical_mapping,
    CANONICAL_MAPPINGS,
    NON_EMPLOYERS,
)

__all__ = [
    'normalize_employer_name',
    'compute_normalized_key', 
    'find_potential_matches',
    'get_canonical_mapping',
    'CANONICAL_MAPPINGS',
    'NON_EMPLOYERS',
]
