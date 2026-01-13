"""Employer name normalization utilities.

Tier 1: Rule-based preprocessing for employer name normalization.
Handles common patterns like LLC/Inc/Corp removal, case normalization,
and punctuation cleanup. This catches ~70% of variations.

Tier 2: Similarity clustering for typos and near-matches.
Tier 3: LLM verification for subsidiaries and ambiguous cases.
"""

import re
from typing import List, Tuple, Optional
import hashlib


# Common legal suffixes to remove
LEGAL_SUFFIXES = [
    r'\s+LLC\.?$',
    r'\s+L\.L\.C\.?$',
    r'\s+INC\.?$',
    r'\s+INCORPORATED$',
    r'\s+CORP\.?$',
    r'\s+CORPORATION$',
    r'\s+CO\.?$',
    r'\s+COMPANY$',
    r'\s+LTD\.?$',
    r'\s+LIMITED$',
    r'\s+LP$',
    r'\s+L\.P\.?$',
    r'\s+LLP$',
    r'\s+L\.L\.P\.?$',
    r'\s+PC$',
    r'\s+P\.C\.?$',
    r'\s+PLLC$',
    r'\s+P\.L\.L\.C\.?$',
    r'\s+PA$',
    r'\s+P\.A\.?$',
    r'\s+NA$',
    r'\s+N\.A\.?$',
    r'\s+PLC$',
    r'\s+GMBH$',
    r'\s+AG$',
    r'\s+SA$',
    r'\s+NV$',
    r'\s+BV$',
]

# Common abbreviation expansions (bidirectional matching)
ABBREVIATIONS = {
    'INTL': 'INTERNATIONAL',
    'NATL': 'NATIONAL',
    'MGMT': 'MANAGEMENT',
    'SVCS': 'SERVICES',
    'ASSOC': 'ASSOCIATES',
    'ASSN': 'ASSOCIATION',
    'GRP': 'GROUP',
    'DEPT': 'DEPARTMENT',
    'GOVT': 'GOVERNMENT',
    'UNIV': 'UNIVERSITY',
    'HOSP': 'HOSPITAL',
    'MED': 'MEDICAL',
    'CTR': 'CENTER',
    'INST': 'INSTITUTE',
    'FDN': 'FOUNDATION',
    'FNDN': 'FOUNDATION',
}

# Non-employer values to flag
NON_EMPLOYERS = {
    'RETIRED', 'SELF', 'SELF EMPLOYED', 'SELF-EMPLOYED', 'SELFEMPLOYED',
    'NOT EMPLOYED', 'UNEMPLOYED', 'NONE', 'N/A', 'NA', 'STUDENT',
    'HOMEMAKER', 'HOME MAKER', 'HOUSEWIFE', 'HUSBAND', 'WIFE',
    'NOT APPLICABLE', 'INFORMATION REQUESTED', 'REQUESTED',
    'INFORMATION REQUESTED PER BEST EFFORTS', 'REFUSED',
}


def normalize_employer_name(name: str) -> Tuple[str, dict]:
    """
    Normalize an employer name using rule-based preprocessing.
    
    Returns:
        Tuple of (normalized_name, metadata_dict)
        metadata includes: original, transformations_applied, is_non_employer
    """
    if not name:
        return '', {'original': name, 'is_non_employer': True, 'reason': 'empty'}
    
    original = name
    transformations = []
    
    # Step 1: Uppercase and strip
    name = name.upper().strip()
    
    # Step 2: Check for non-employer values
    if name in NON_EMPLOYERS or any(ne in name for ne in ['SELF EMPLOYED', 'NOT EMPLOYED', 'INFORMATION REQUESTED']):
        return name, {
            'original': original,
            'is_non_employer': True,
            'reason': 'non_employer_value',
            'category': _categorize_non_employer(name)
        }
    
    # Step 3: Remove legal suffixes
    for suffix_pattern in LEGAL_SUFFIXES:
        new_name = re.sub(suffix_pattern, '', name, flags=re.IGNORECASE)
        if new_name != name:
            transformations.append(f'removed_suffix:{suffix_pattern}')
            name = new_name
    
    # Step 4: Remove trailing punctuation
    name = re.sub(r'[\.,;:]+$', '', name)
    if name != original.upper().strip():
        transformations.append('removed_trailing_punct')
    
    # Step 5: Normalize internal punctuation
    # "A.T.&T." → "AT&T", but keep meaningful punctuation
    name = re.sub(r'\.(?=[A-Z])', '', name)  # Remove periods between letters
    name = re.sub(r'\s*&\s*', ' & ', name)   # Normalize ampersand spacing
    
    # Step 6: Normalize whitespace
    name = re.sub(r'\s+', ' ', name).strip()
    
    # Step 7: Handle common variations
    # Remove "THE " prefix for matching purposes
    if name.startswith('THE '):
        name = name[4:]
        transformations.append('removed_the_prefix')
    
    return name, {
        'original': original,
        'normalized': name,
        'transformations': transformations,
        'is_non_employer': False
    }


def _categorize_non_employer(name: str) -> str:
    """Categorize non-employer values."""
    name = name.upper()
    if any(x in name for x in ['RETIRED', 'RETIREE']):
        return 'retired'
    if any(x in name for x in ['SELF', 'OWN BUSINESS', 'ENTREPRENEUR']):
        return 'self_employed'
    if any(x in name for x in ['HOMEMAKER', 'HOME MAKER', 'HOUSEWIFE', 'HUSBAND']):
        return 'homemaker'
    if any(x in name for x in ['STUDENT', 'GRADUATE']):
        return 'student'
    if any(x in name for x in ['UNEMPLOYED', 'NOT EMPLOYED']):
        return 'unemployed'
    if any(x in name for x in ['REQUESTED', 'REFUSED', 'N/A', 'NONE']):
        return 'not_provided'
    return 'other'


def compute_normalized_key(name: str) -> str:
    """
    Compute a deterministic key for a normalized employer name.
    Used for grouping variations together.
    """
    normalized, _ = normalize_employer_name(name)
    # Create a hash for the normalized name
    return hashlib.md5(normalized.encode()).hexdigest()[:16]


def find_potential_matches(name: str, candidates: List[str], threshold: float = 0.8) -> List[Tuple[str, float]]:
    """
    Find potential matches for a name from a list of candidates.
    Uses simple token overlap scoring (Tier 2 preview).
    
    For full Tier 2, we'll use embedding similarity.
    """
    normalized, _ = normalize_employer_name(name)
    tokens = set(normalized.split())
    
    matches = []
    for candidate in candidates:
        cand_normalized, _ = normalize_employer_name(candidate)
        cand_tokens = set(cand_normalized.split())
        
        # Jaccard similarity
        if not tokens or not cand_tokens:
            continue
        intersection = tokens & cand_tokens
        union = tokens | cand_tokens
        similarity = len(intersection) / len(union)
        
        if similarity >= threshold:
            matches.append((candidate, similarity))
    
    return sorted(matches, key=lambda x: -x[1])


# Pre-defined canonical mappings for major companies
# These are high-confidence mappings that don't need LLM verification
CANONICAL_MAPPINGS = {
    # Tech giants
    'GOOGLE': {'canonical': 'ALPHABET INC', 'subsidiaries': ['YOUTUBE', 'WAYMO', 'DEEPMIND', 'FITBIT', 'GOOGLE FIBER', 'GOOGLE VENTURES', 'CAPITALG']},
    'ALPHABET': {'canonical': 'ALPHABET INC', 'parent': True},
    'YOUTUBE': {'canonical': 'ALPHABET INC', 'subsidiary_of': 'ALPHABET INC'},
    
    'MICROSOFT': {'canonical': 'MICROSOFT CORPORATION', 'subsidiaries': ['LINKEDIN', 'GITHUB', 'ACTIVISION', 'BLIZZARD']},
    'LINKEDIN': {'canonical': 'MICROSOFT CORPORATION', 'subsidiary_of': 'MICROSOFT CORPORATION'},
    'GITHUB': {'canonical': 'MICROSOFT CORPORATION', 'subsidiary_of': 'MICROSOFT CORPORATION'},
    
    'AMAZON': {'canonical': 'AMAZON.COM INC', 'subsidiaries': ['AWS', 'WHOLE FOODS', 'TWITCH', 'MGM']},
    'AWS': {'canonical': 'AMAZON.COM INC', 'subsidiary_of': 'AMAZON.COM INC'},
    
    'META': {'canonical': 'META PLATFORMS INC', 'subsidiaries': ['FACEBOOK', 'INSTAGRAM', 'WHATSAPP', 'OCULUS']},
    'FACEBOOK': {'canonical': 'META PLATFORMS INC', 'subsidiary_of': 'META PLATFORMS INC'},
    
    'APPLE': {'canonical': 'APPLE INC'},
    
    # Finance
    'CITADEL': {'canonical': 'CITADEL LLC', 'subsidiaries': ['CITADEL SECURITIES', 'CITADEL INVESTMENT GROUP', 'CITADEL ASSET MANAGEMENT']},
    'GOLDMAN SACHS': {'canonical': 'GOLDMAN SACHS GROUP INC'},
    'GOLDMAN': {'canonical': 'GOLDMAN SACHS GROUP INC'},
    'JP MORGAN': {'canonical': 'JPMORGAN CHASE & CO'},
    'JPMORGAN': {'canonical': 'JPMORGAN CHASE & CO'},
    'MORGAN STANLEY': {'canonical': 'MORGAN STANLEY'},
    'BLACKSTONE': {'canonical': 'BLACKSTONE INC'},
    'BLACKROCK': {'canonical': 'BLACKROCK INC'},
    
    # Healthcare/Pharma
    'PFIZER': {'canonical': 'PFIZER INC'},
    'JOHNSON & JOHNSON': {'canonical': 'JOHNSON & JOHNSON'},
    'J&J': {'canonical': 'JOHNSON & JOHNSON'},
    
    # Major donors with known variations
    'ADELSON': {'canonical': 'ADELSON ENTERPRISES', 'subsidiaries': ['ADELSON CLINIC', 'ADELSON DRUG CLINIC', 'LAS VEGAS SANDS', 'THE VENETIAN']},
    'ADELSON CLINIC': {'canonical': 'ADELSON ENTERPRISES', 'subsidiary_of': 'ADELSON ENTERPRISES'},
    'ADELSON DRUG CLINIC': {'canonical': 'ADELSON ENTERPRISES', 'subsidiary_of': 'ADELSON ENTERPRISES'},
    'LAS VEGAS SANDS': {'canonical': 'ADELSON ENTERPRISES', 'subsidiary_of': 'ADELSON ENTERPRISES'},
    'VENETIAN': {'canonical': 'ADELSON ENTERPRISES', 'subsidiary_of': 'ADELSON ENTERPRISES'},
    
    'SOROS': {'canonical': 'SOROS FUND MANAGEMENT LLC'},
    'SOROS FUND': {'canonical': 'SOROS FUND MANAGEMENT LLC'},
    'SOROS FUND MANAGEMENT': {'canonical': 'SOROS FUND MANAGEMENT LLC'},
    
    'BLOOMBERG': {'canonical': 'BLOOMBERG LP'},
    
    'RENAISSANCE': {'canonical': 'RENAISSANCE TECHNOLOGIES LLC'},
    'RENAISSANCE TECHNOLOGIES': {'canonical': 'RENAISSANCE TECHNOLOGIES LLC'},
    
    'LONE PINE': {'canonical': 'LONE PINE CAPITAL LLC'},
    'LONE PINE CAPITAL': {'canonical': 'LONE PINE CAPITAL LLC'},
    
    'SUSQUEHANNA': {'canonical': 'SUSQUEHANNA INTERNATIONAL GROUP'},
    'SIG': {'canonical': 'SUSQUEHANNA INTERNATIONAL GROUP'},  # SIG = Susquehanna International Group
    
    # Retail
    'WALMART': {'canonical': 'WALMART INC'},
    'WAL-MART': {'canonical': 'WALMART INC'},
    'WAL MART': {'canonical': 'WALMART INC'},
    
    # Energy
    'EXXON': {'canonical': 'EXXON MOBIL CORPORATION'},
    'EXXONMOBIL': {'canonical': 'EXXON MOBIL CORPORATION'},
    'CHEVRON': {'canonical': 'CHEVRON CORPORATION'},
    
    # Telecom
    'AT&T': {'canonical': 'AT&T INC'},
    'ATT': {'canonical': 'AT&T INC'},
    'VERIZON': {'canonical': 'VERIZON COMMUNICATIONS INC'},
}


def get_canonical_mapping(name: str) -> Optional[dict]:
    """
    Check if a normalized name has a pre-defined canonical mapping.
    Returns the mapping dict if found, None otherwise.
    """
    normalized, _ = normalize_employer_name(name)
    
    # Direct match
    if normalized in CANONICAL_MAPPINGS:
        return CANONICAL_MAPPINGS[normalized]
    
    # Check if name starts with a known company
    for key, mapping in CANONICAL_MAPPINGS.items():
        if normalized.startswith(key + ' ') or normalized == key:
            return mapping
    
    return None


if __name__ == '__main__':
    # Test cases
    test_names = [
        'GOOGLE LLC',
        'GOOGLE INC.',
        'Google',
        'ALPHABET INC',
        'MICROSOFT CORPORATION',
        'MICROSOFT CORP.',
        'Microsoft',
        'CITADEL LLC',
        'CITADEL INVESTMENT GROUP LLC',
        'CITADELL LLC',  # Typo
        'SELF-EMPLOYED',
        'RETIRED',
        'HOMEMAKER',
        'JP MORGAN CHASE & CO.',
        'JPMORGAN CHASE',
        'THE BOEING COMPANY',
    ]
    
    print("=" * 70)
    print("EMPLOYER NAME NORMALIZATION TEST")
    print("=" * 70)
    
    for name in test_names:
        normalized, meta = normalize_employer_name(name)
        canonical = get_canonical_mapping(name)
        
        print(f"\nInput: {name}")
        print(f"  Normalized: {normalized}")
        if meta.get('is_non_employer'):
            print(f"  Non-employer: {meta.get('category')}")
        if meta.get('transformations'):
            print(f"  Transforms: {meta.get('transformations')}")
        if canonical:
            print(f"  Canonical: {canonical.get('canonical')}")
