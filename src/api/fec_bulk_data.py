"""
FEC Bulk Data API

Downloads and parses FEC bulk data files from S3 bucket.
Uses DataRepository for organized storage.
"""

import requests
import zipfile
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

from ..data import get_repository, DataRepository


FEC_BULK_URL = "https://www.fec.gov/files/bulk-downloads"

# Mapping of FEC basenames to friendly names and repository path methods
FEC_FILE_MAPPING = {
    'cn': 'fec_candidates_path',
    'cm': 'fec_committees_path',
    'ccl': 'fec_linkages_path',
    'weball': 'fec_candidate_summary_path',
    'webl': 'fec_committee_summary_path',
    'webk': 'fec_pac_summary_path',
    'indiv': 'fec_individual_contributions_path',
    'oppexp': 'fec_independent_expenditures_path',
    'pas2': 'fec_committee_transfers_path',
}


def download_fec_file(
    file_basename: str,
    cycle: str,
    force_refresh: bool = False,
    repository: Optional[DataRepository] = None
) -> Path:
    """
    Download a FEC bulk data file using the data repository.
    
    Args:
        file_basename: FEC file basename (e.g., 'cn', 'cm', 'oppexp')
        cycle: Election cycle (e.g., '2024', '2026')
        force_refresh: Force re-download even if cached
        repository: DataRepository instance (creates one if not provided)
        
    Returns:
        Path to downloaded file
    """
    if repository is None:
        repository = get_repository()
    
    # Get the repository path method
    if file_basename not in FEC_FILE_MAPPING:
        raise ValueError(f"Unknown FEC file basename: {file_basename}")
    
    path_method_name = FEC_FILE_MAPPING[file_basename]
    path_method = getattr(repository, path_method_name)
    cache_path = path_method(cycle)
    
    # Check if cached file is fresh
    if not force_refresh and repository.is_file_fresh(cache_path, max_age_days=7):
        age_days = (datetime.now() - datetime.fromtimestamp(cache_path.stat().st_mtime)).days
        print(f"✓ Using cached {cache_path.name} ({age_days} days old)")
        return cache_path
    
    # Download the file
    year_suffix = cycle[-2:]
    fec_filename = f"{file_basename}{year_suffix}.zip"
    url = f"{FEC_BULK_URL}/{cycle}/{fec_filename}"
    
    print(f"⬇️  Downloading {fec_filename}...")
    
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    
    # Ensure parent directory exists
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Save the file
    with open(cache_path, 'wb') as f:
        f.write(response.content)
    
    size_mb = len(response.content) / 1024 / 1024
    print(f"✓ Downloaded {cache_path.name} ({size_mb:.1f} MB)")
    
    # Update metadata
    repository.update_file_metadata(
        repository.fec_cycle_metadata_path(cycle),
        file_basename,
        downloaded_at=datetime.now().isoformat(),
        size_bytes=len(response.content),
        url=url
    )
    
    return cache_path


def parse_fec_file(zip_path: Path) -> List[Dict[str, str]]:
    """Parse a FEC bulk data ZIP file."""
    records = []
    with zipfile.ZipFile(zip_path) as zf:
        txt_files = [name for name in zf.namelist() if name.endswith('.txt')]
        if not txt_files:
            return records
        
        with zf.open(txt_files[0]) as f:
            content = f.read().decode('utf-8')
            lines = content.strip().split('\n')
            if len(lines) < 2:
                return records
            
            header = lines[0].split('|')
            for line in lines[1:]:
                if not line.strip():
                    continue
                parts = line.split('|')
                record = {header[i]: parts[i] if i < len(parts) else '' for i in range(len(header))}
                records.append(record)
    
    print(f"Parsed {len(records)} records")
    return records


def load_fec_candidates(
    cycle: str,
    force_refresh: bool = False,
    repository: Optional[DataRepository] = None
) -> Dict[str, Dict[str, str]]:
    """
    Load FEC candidate master file.
    
    Args:
        cycle: Election cycle (e.g., '2024', '2026')
        force_refresh: Force re-download
        repository: DataRepository instance
        
    Returns:
        Dict mapping candidate ID to candidate data
    """
    zip_path = download_fec_file('cn', cycle, force_refresh, repository)
    records = parse_fec_file(zip_path)
    
    candidates = {}
    for record in records:
        cand_id = record.get('CAND_ID')
        if cand_id:
            candidates[cand_id] = {
                'id': cand_id,
                'name': record.get('CAND_NAME', ''),
                'party': record.get('CAND_PTY_AFFILIATION', ''),
                'election_year': record.get('CAND_ELECTION_YR', ''),
                'state': record.get('CAND_OFFICE_ST', ''),
                'office': record.get('CAND_OFFICE', ''),
                'district': record.get('CAND_OFFICE_DISTRICT', ''),
            }
    
    print(f"✓ Loaded {len(candidates)} candidates")
    return candidates


def load_committee_linkages(
    cycle: str,
    force_refresh: bool = False,
    repository: Optional[DataRepository] = None
) -> Dict[str, List[str]]:
    """
    Load FEC candidate-committee linkages file.
    
    Args:
        cycle: Election cycle (e.g., '2024', '2026')
        force_refresh: Force re-download
        repository: DataRepository instance
        
    Returns:
        Dict mapping candidate ID to list of committee IDs
    """
    zip_path = download_fec_file('ccl', cycle, force_refresh, repository)
    records = parse_fec_file(zip_path)
    
    linkages = {}
    for record in records:
        cand_id = record.get('CAND_ID')
        committee_id = record.get('CMTE_ID')
        if cand_id and committee_id:
            if cand_id not in linkages:
                linkages[cand_id] = []
            linkages[cand_id].append(committee_id)
    
    print(f"✓ Loaded linkages for {len(linkages)} candidates")
    return linkages


# ==========================================================================
# Convenience functions for common operations
# ==========================================================================

def load_independent_expenditures(
    cycle: str,
    force_refresh: bool = False,
    repository: Optional[DataRepository] = None
) -> List[Dict[str, str]]:
    """
    Load FEC independent expenditures file (oppexp).
    
    Warning: This file can be very large (2-3GB).
    
    Args:
        cycle: Election cycle (e.g., '2024', '2026')
        force_refresh: Force re-download
        repository: DataRepository instance
        
    Returns:
        List of expenditure records
    """
    zip_path = download_fec_file('oppexp', cycle, force_refresh, repository)
    records = parse_fec_file(zip_path)
    print(f"✓ Loaded {len(records)} independent expenditures")
    return records


def get_all_cycles(repository: Optional[DataRepository] = None) -> List[str]:
    """
    Get list of all available FEC cycles in the repository.
    
    Args:
        repository: DataRepository instance
        
    Returns:
        List of cycle strings (e.g., ['2024', '2026'])
    """
    if repository is None:
        repository = get_repository()
    
    cycles = []
    for cycle_dir in repository.fec_dir.iterdir():
        if cycle_dir.is_dir() and cycle_dir.name.isdigit():
            cycles.append(cycle_dir.name)
    
    return sorted(cycles)
