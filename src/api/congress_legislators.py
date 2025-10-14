"""
Congress Legislators API
Downloads and parses legislators data from unitedstates/congress-legislators GitHub repo.
Provides FEC IDs and other identifiers for all current members of Congress.
"""

import requests
import yaml
from datetime import datetime
from typing import Dict, List, Any, Optional

from ..data import get_repository, DataRepository


def download_legislators_file(repository: Optional[DataRepository] = None) -> List[Dict[str, Any]]:
    """
    Download legislators-current.yaml from GitHub using the data repository.
    
    This file contains all current Members of Congress with:
    - FEC candidate IDs
    - Bioguide IDs (matches ProPublica API)
    - Other external IDs (GovTrack, OpenSecrets, etc.)
    - Basic biographical info
    - Term information
    
    Args:
        repository: DataRepository instance (creates one if not provided)
        
    Returns:
        List of legislator dictionaries
    """
    if repository is None:
        repository = get_repository()
    
    cache_path = repository.legislators_current_path
    
    print("📥 Downloading legislators-current.yaml from GitHub...")
    url = "https://unitedstates.github.io/congress-legislators/legislators-current.yaml"
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        # Save to repository
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(cache_path, 'w') as f:
            f.write(response.text)
        
        print(f"✓ Saved to {cache_path}")
        
        # Parse and return
        legislators = yaml.safe_load(response.text)
        print(f"✓ Loaded {len(legislators)} current legislators")
        
        # Update metadata
        repository.update_file_metadata(
            repository.legislators_metadata_path,
            'current',
            downloaded_at=datetime.now().isoformat(),
            size_bytes=len(response.text),
            count=len(legislators)
        )
        
        return legislators
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Error downloading legislators file: {e}")
        raise


def get_current_legislators(
    use_cache: bool = True,
    repository: Optional[DataRepository] = None
) -> List[Dict[str, Any]]:
    """
    Get current legislators, using cache if available and recent.
    
    Args:
        use_cache: If True, use cached file if it's less than 7 days old
        repository: DataRepository instance (creates one if not provided)
        
    Returns:
        List of legislator dictionaries
    """
    if repository is None:
        repository = get_repository()
    
    cache_path = repository.legislators_current_path
    
    if use_cache and repository.is_file_fresh(cache_path, max_age_days=7):
        file_mod_time = datetime.fromtimestamp(cache_path.stat().st_mtime)
        age_days = (datetime.now() - file_mod_time).days
        
        print(f"📂 Using cached legislators file ({age_days} days old)")
        with open(cache_path) as f:
            legislators = yaml.safe_load(f)
            print(f"✓ Loaded {len(legislators)} legislators from cache")
            return legislators
    
    # Download fresh
    return download_legislators_file(repository)


def extract_fec_ids(legislators: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    """
    Extract FEC candidate IDs for all legislators.
    
    Args:
        legislators: List of legislator dictionaries from legislators file
        
    Returns:
        Dictionary mapping bioguide_id -> list of FEC candidate IDs
    """
    mapping = {}
    
    for member in legislators:
        bioguide_id = member['id']['bioguide']
        fec_ids = member['id'].get('fec', [])
        
        if fec_ids:
            mapping[bioguide_id] = fec_ids
    
    print(f"✓ Extracted FEC IDs for {len(mapping)}/{len(legislators)} legislators "
          f"({len(mapping)/len(legislators)*100:.1f}% coverage)")
    
    return mapping


def get_current_term(member: Dict[str, Any]) -> Dict[str, Any]:
    """
    Get the current term information for a legislator.
    
    Args:
        member: Legislator dictionary from legislators file
        
    Returns:
        Dictionary with current term information
    """
    if not member.get('terms'):
        return {}
    
    # Last term in the list is the current one
    current_term = member['terms'][-1]
    
    return {
        'chamber': 'senate' if current_term['type'] == 'sen' else 'house',
        'state': current_term['state'],
        'party': current_term['party'],
        'class': current_term.get('class'),  # Only for senators
        'district': current_term.get('district'),  # Only for representatives
        'term_start': current_term['start'],
        'term_end': current_term['end'],
        'state_rank': current_term.get('state_rank'),  # junior/senior for senators
    }


if __name__ == '__main__':
    # Test the module
    print("Testing congress_legislators module...\n")
    
    # Download legislators
    legislators = get_current_legislators(use_cache=False)
    
    # Extract FEC IDs
    fec_mapping = extract_fec_ids(legislators)
    
    # Show some examples
    print("\n📊 Sample legislators with FEC IDs:")
    for i, member in enumerate(legislators[:5], 1):
        bioguide_id = member['id']['bioguide']
        name = member['name'].get('official_full', f"{member['name']['first']} {member['name']['last']}")
        fec_ids = member['id'].get('fec', [])
        term = get_current_term(member)
        
        print(f"\n{i}. {name}")
        print(f"   Bioguide: {bioguide_id}")
        print(f"   FEC IDs: {', '.join(fec_ids) if fec_ids else 'None'}")
        print(f"   Chamber: {term['chamber']}, State: {term['state']}, Party: {term['party']}")
