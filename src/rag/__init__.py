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
