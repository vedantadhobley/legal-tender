"""RAG utilities for entity resolution and enrichment.

Uses Wikidata as the primary source for corporate relationships.
No hardcoding - all corporate knowledge comes from external sources.
"""

from src.rag.employer_normalization import (
    normalize_employer_name,
    compute_normalized_key,
    find_potential_matches,
    NON_EMPLOYERS,
)

from src.rag.wikidata_client import (
    search_company,
    get_subsidiaries,
    get_parent_company,
    get_person_companies,
    get_companies_by_owner,
    resolve_company_to_canonical,
    resolve_person_to_companies,
    CompanyInfo,
    PersonCompanyLink,
)

__all__ = [
    # Employer normalization
    'normalize_employer_name',
    'compute_normalized_key', 
    'find_potential_matches',
    'NON_EMPLOYERS',
    # Wikidata resolution
    'search_company',
    'get_subsidiaries',
    'get_parent_company',
    'get_person_companies',
    'get_companies_by_owner',
    'resolve_company_to_canonical',
    'resolve_person_to_companies',
    'CompanyInfo',
    'PersonCompanyLink',
]
