"""Employer name normalization utilities.

Rule-based preprocessing for employer name normalization:
- LLC/Inc/Corp suffix removal
- Case normalization  
- Punctuation cleanup
- Non-employer detection (retired, self-employed, etc.)

This catches ~70% of variations. Remaining corporate hierarchy
(subsidiaries, parent companies) is resolved via Wikidata in
the wikidata_corporate_resolution Dagster asset.
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

# Non-employer values to flag.
#
# Beyond the obvious placeholders (RETIRED / SELF-EMPLOYED / N/A), this
# also catches values that drift through to Wikidata as bogus "employer
# = SOMETHING" lookups: ENTREPRENEUR (Wikidata resolves to a Q-id for
# the concept), NOT-EMPLOYED (with a hyphen), and INVESTOR which often
# describes the donor rather than identifying an employer.
NON_EMPLOYERS = {
    'RETIRED', 'SELF', 'SELF EMPLOYED', 'SELF-EMPLOYED', 'SELFEMPLOYED',
    'NOT EMPLOYED', 'NOT-EMPLOYED', 'UNEMPLOYED', 'NONE', 'N/A', 'NA', 'STUDENT',
    'HOMEMAKER', 'HOME MAKER', 'HOUSEWIFE', 'HUSBAND', 'WIFE',
    'NOT APPLICABLE', 'INFORMATION REQUESTED', 'REQUESTED',
    'INFORMATION REQUESTED PER BEST EFFORTS', 'REFUSED',
    'ENTREPRENEUR', 'INVESTOR', 'PRIVATE INVESTOR', 'PHILANTHROPIST',
    'BUSINESSMAN', 'BUSINESSWOMAN', 'CONSULTANT', 'ATTORNEY', 'LAWYER',
    'PHYSICIAN', 'DOCTOR', 'EXECUTIVE', 'CEO', 'OWNER',
    # Generic legal/business words appearing as the entire employer
    # field, with no actual company name. Surfaced via $57M attributed
    # to "Corporation" the 1988 video game when donors typed
    # "CORPORATION" alone in the FEC employer field.
    'CORPORATION', 'COMPANY', 'BUSINESS', 'CORP', 'INC',
    'INCORPORATED', 'LLC', 'LP', 'LLP', 'LIMITED', 'LTD',
    'EMPLOYED', 'EMPLOYEE', 'EMPLOYER',
}


# Corporate-family alias map: forces unification of FEC employer names
# that refer to the same entity but appear as separate canonical groups
# because Wikidata either has no entry for them or has them as separate
# Q-ids. Applied in wikidata_resolution Phase 2/3 when assembling
# corporate_families — entries whose canonical_name (post-Wikidata
# resolution) matches a key here get remapped to the value, merging
# their dollars into the target family.
#
# Use sparingly. Each entry should be:
#   - A real semantic equivalence (same physical entity / same parent)
#   - High-value enough to justify a hardcode (≥$X00M typically)
#
# Discovered via spot-checks of top-30 corporate_families:
EMPLOYER_FAMILY_ALIASES = {
    # FEC employer name (or Wikidata canonical, uppercase keying)
    #   →  preferred unified canonical name
    "ADELSON CLINIC": "ADELSON DRUG CLINIC",  # Miriam Adelson's clinic
    "ULINE INDUSTRIES": "ULINE",  # Same Uline (the box company)
}


# Campaign-committee-name patterns that occasionally leak in as donor
# "employers" (e.g. "LEXI REESE FOR SENATE", "TED LIEU FOR CONGRESS
# COMMITTEE"). These are not employers — the donor is filing a
# self-funding contribution and listing their own campaign committee.
# Detected by substring match (case-insensitive).
CAMPAIGN_COMMITTEE_MARKERS = (
    'FOR CONGRESS', 'FOR SENATE', 'FOR PRESIDENT',
    'FOR HOUSE', 'CAMPAIGN COMMITTEE', 'POLITICAL ACTION',
    ' PAC', '_PAC', 'VICTORY FUND',
)


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

    # Step 2b: Filter out campaign committee names that leak through as
    # "employer" — donors self-funding their own campaign sometimes list
    # the campaign committee name in the employer field.
    if any(marker in name for marker in CAMPAIGN_COMMITTEE_MARKERS):
        return name, {
            'original': original,
            'is_non_employer': True,
            'reason': 'campaign_committee_leakage',
            'category': 'political',
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


# NOTE: Corporate hierarchy (subsidiaries, parent companies) is resolved via Wikidata
# in the wikidata_corporate_resolution asset. No hardcoded mappings needed here.


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
        
        print(f"\nInput: {name}")
        print(f"  Normalized: {normalized}")
        if meta.get('is_non_employer'):
            print(f"  Non-employer: {meta.get('category')}")
        if meta.get('transformations'):
            print(f"  Transforms: {meta.get('transformations')}")
