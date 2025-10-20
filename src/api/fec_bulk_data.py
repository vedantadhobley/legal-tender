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


def parse_fec_candidates_file(zip_path: Path) -> List[Dict[str, str]]:
    """Parse FEC candidate master file (cn.txt).
    
    Format: CAND_ID|CAND_NAME|CAND_PTY_AFFILIATION|CAND_ELECTION_YR|CAND_OFFICE_ST|
            CAND_OFFICE|CAND_OFFICE_DISTRICT|CAND_ICI|CAND_STATUS|CAND_PCC|
            CAND_ST1|CAND_ST2|CAND_CITY|CAND_ST|CAND_ZIP
    """
    records = []
    with zipfile.ZipFile(zip_path) as zf:
        txt_files = [name for name in zf.namelist() if name.endswith('.txt')]
        if not txt_files:
            return records
        
        with zf.open(txt_files[0]) as f:
            content = f.read().decode('utf-8', errors='ignore')
            lines = content.strip().split('\n')
            
            for line in lines:
                if not line.strip():
                    continue
                parts = line.split('|')
                if len(parts) >= 15:  # Ensure we have all columns
                    records.append({
                        'CAND_ID': parts[0],
                        'CAND_NAME': parts[1],
                        'CAND_PTY_AFFILIATION': parts[2],
                        'CAND_ELECTION_YR': parts[3],
                        'CAND_OFFICE_ST': parts[4],
                        'CAND_OFFICE': parts[5],
                        'CAND_OFFICE_DISTRICT': parts[6],
                        'CAND_ICI': parts[7],  # Incumbent/Challenger/Open
                        'CAND_STATUS': parts[8],
                        'CAND_PCC': parts[9],  # Principal Campaign Committee
                    })
    
    return records


def parse_fec_linkages_file(zip_path: Path) -> List[Dict[str, str]]:
    """Parse FEC candidate-committee linkages file (ccl.txt).
    
    Format: CAND_ID|CAND_ELECTION_YR|FEC_ELECTION_YR|CMTE_ID|CMTE_TP|CMTE_DSGN|LINKAGE_ID
    """
    records = []
    with zipfile.ZipFile(zip_path) as zf:
        txt_files = [name for name in zf.namelist() if name.endswith('.txt')]
        if not txt_files:
            return records
        
        with zf.open(txt_files[0]) as f:
            content = f.read().decode('utf-8', errors='ignore')
            lines = content.strip().split('\n')
            
            for line in lines:
                if not line.strip():
                    continue
                parts = line.split('|')
                if len(parts) >= 7:
                    records.append({
                        'CAND_ID': parts[0],
                        'CAND_ELECTION_YR': parts[1],
                        'FEC_ELECTION_YR': parts[2],
                        'CMTE_ID': parts[3],
                        'CMTE_TP': parts[4],
                        'CMTE_DSGN': parts[5],
                        'LINKAGE_ID': parts[6],
                    })
    
    return records


def parse_indiv_file(zip_path: Path, limit: Optional[int] = None) -> List[Dict[str, str]]:
    """Parse FEC individual contributions file (indiv.txt).
    
    Format: CMTE_ID|AMNDT_IND|RPT_TP|TRANSACTION_PGI|IMAGE_NUM|TRANSACTION_TP|
            ENTITY_TP|NAME|CITY|STATE|ZIP_CODE|EMPLOYER|OCCUPATION|
            TRANSACTION_DT|TRANSACTION_AMT|OTHER_ID|TRAN_ID|FILE_NUM|MEMO_CD|MEMO_TEXT|SUB_ID
    
    Key fields:
    - NAME: Donor name
    - EMPLOYER: Donor's employer (key for corporate influence!)
    - OCCUPATION: Donor's job
    - TRANSACTION_AMT: Amount donated
    - CMTE_ID: Committee that received the donation
    - TRANSACTION_DT: Date (MMDDYYYY format)
    
    Args:
        zip_path: Path to indiv zip file
        limit: Optional limit on number of records (for testing)
        
    Returns:
        List of contribution records
    """
    records = []
    with zipfile.ZipFile(zip_path) as zf:
        txt_files = [name for name in zf.namelist() if name.endswith('.txt')]
        if not txt_files:
            print(f"⚠️  No .txt files found in {zip_path}")
            return records
        
        print(f"📖 Parsing {txt_files[0]} from {zip_path.name}...")
        if limit:
            print(f"   Limited to first {limit:,} records")
        
        with zf.open(txt_files[0]) as f:
            for i, line in enumerate(f):
                if limit and i >= limit:
                    print(f"   Reached limit of {limit:,} records")
                    break
                
                # Log progress every 100k lines
                if i % 100000 == 0 and i > 0:
                    print(f"   Processed {i:,} lines, {len(records):,} valid records...")
                
                if not line.strip():
                    continue
                
                try:
                    line_str = line.decode('utf-8', errors='ignore')
                    parts = line_str.split('|')
                    
                    if len(parts) >= 21:
                        records.append({
                            'CMTE_ID': parts[0],
                            'AMNDT_IND': parts[1],
                            'RPT_TP': parts[2],
                            'TRANSACTION_PGI': parts[3],
                            'IMAGE_NUM': parts[4],
                            'TRANSACTION_TP': parts[5],
                            'ENTITY_TP': parts[6],
                            'NAME': parts[7],
                            'CITY': parts[8],
                            'STATE': parts[9],
                            'ZIP_CODE': parts[10],
                            'EMPLOYER': parts[11],
                            'OCCUPATION': parts[12],
                            'TRANSACTION_DT': parts[13],
                            'TRANSACTION_AMT': parts[14],
                            'OTHER_ID': parts[15],
                            'TRAN_ID': parts[16],
                            'FILE_NUM': parts[17],
                            'MEMO_CD': parts[18],
                            'MEMO_TEXT': parts[19],
                            'SUB_ID': parts[20],
                        })
                except Exception:
                    # Skip malformed lines
                    continue
    
    return records


def parse_oppexp_file(zip_path: Path, limit: Optional[int] = None) -> List[Dict[str, str]]:
    """Parse FEC independent expenditures file (oppexp.txt).
    
    Format: CMTE_ID|AMNDT_IND|RPT_YR|RPT_TP|IMAGE_NUM|LINE_NUM|FORM_TP_CD|SCHED_TP_CD|
            NAME|STREET_1|STREET_2|CITY|STATE|ZIP|TRANSACTION_DT|TRANSACTION_AMT|
            TRANSACTION_PGI|PURPOSE|CATEGORY|CATEGORY_DESC|MEMO_CD|MEMO_TEXT|ENTITY_TP|
            SUB_ID|FILE_NUM|TRAN_ID|BACK_REF_TRAN_ID|BACK_REF_SCHED_NAME|CAND_ID|
            CAND_NAME|CAND_OFFICE|CAND_OFFICE_ST|CAND_OFFICE_DISTRICT|CONDUIT_NAME|
            CONDUIT_STREET1|CONDUIT_STREET2|CONDUIT_CITY|CONDUIT_STATE|CONDUIT_ZIP|
            PAYEE_EMPLOYER|PAYEE_OCCUPATION|DISSEMINATION_DT|COMM_DT|SUPPORT_OPPOSE_INDICATOR
    
    Key fields:
    - CAND_ID: Candidate this spending is FOR or AGAINST
    - CAND_NAME: Candidate name
    - TRANSACTION_AMT: Amount spent
    - SUPPORT_OPPOSE_INDICATOR: 'S' = Support, 'O' = Oppose (WE USE NEGATIVE VALUES FOR OPPOSE!)
    - PURPOSE: What the money was spent on
    - CMTE_ID: Super PAC that spent the money
    - TRANSACTION_DT: Date (MMDDYYYY format)
    
    Args:
        zip_path: Path to oppexp zip file
        limit: Optional limit on number of records (for testing)
        
    Returns:
        List of expenditure records
    """
    records = []
    with zipfile.ZipFile(zip_path) as zf:
        txt_files = [name for name in zf.namelist() if name.endswith('.txt')]
        if not txt_files:
            print(f"⚠️  No .txt files found in {zip_path}")
            return records
        
        print(f"📖 Parsing {txt_files[0]} from {zip_path.name}...")
        
        with zf.open(txt_files[0]) as f:
            for i, line in enumerate(f):
                if limit and i >= limit:
                    break
                
                # Log progress every 100k lines
                if i % 100000 == 0 and i > 0:
                    print(f"   Processed {i:,} lines, {len(records):,} valid records...")
                
                if not line.strip():
                    continue
                
                try:
                    line_str = line.decode('utf-8', errors='ignore')
                    parts = line_str.split('|')
                    
                    if len(parts) >= 41:
                        records.append({
                            'CMTE_ID': parts[0],
                            'AMNDT_IND': parts[1],
                            'RPT_YR': parts[2],
                            'RPT_TP': parts[3],
                            'IMAGE_NUM': parts[4],
                            'LINE_NUM': parts[5],
                            'FORM_TP_CD': parts[6],
                            'SCHED_TP_CD': parts[7],
                            'NAME': parts[8],
                            'STREET_1': parts[9],
                            'STREET_2': parts[10],
                            'CITY': parts[11],
                            'STATE': parts[12],
                            'ZIP': parts[13],
                            'TRANSACTION_DT': parts[14],
                            'TRANSACTION_AMT': parts[15],
                            'TRANSACTION_PGI': parts[16],
                            'PURPOSE': parts[17],
                            'CATEGORY': parts[18],
                            'CATEGORY_DESC': parts[19],
                            'MEMO_CD': parts[20],
                            'MEMO_TEXT': parts[21],
                            'ENTITY_TP': parts[22],
                            'SUB_ID': parts[23],
                            'FILE_NUM': parts[24],
                            'TRAN_ID': parts[25],
                            'BACK_REF_TRAN_ID': parts[26],
                            'BACK_REF_SCHED_NAME': parts[27],
                            'CAND_ID': parts[28],
                            'CAND_NAME': parts[29],
                            'CAND_OFFICE': parts[30],
                            'CAND_OFFICE_ST': parts[31],
                            'CAND_OFFICE_DISTRICT': parts[32],
                            'CONDUIT_NAME': parts[33],
                            'CONDUIT_STREET1': parts[34],
                            'CONDUIT_STREET2': parts[35],
                            'CONDUIT_CITY': parts[36],
                            'CONDUIT_STATE': parts[37],
                            'CONDUIT_ZIP': parts[38],
                            'PAYEE_EMPLOYER': parts[39],
                            'PAYEE_OCCUPATION': parts[40],
                            # Some files have these additional fields
                            'DISSEMINATION_DT': parts[41] if len(parts) > 41 else '',
                            'COMM_DT': parts[42] if len(parts) > 42 else '',
                            'SUPPORT_OPPOSE_INDICATOR': parts[43] if len(parts) > 43 else '',
                        })
                except Exception:
                    # Skip malformed lines
                    continue
    
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
    records = parse_fec_candidates_file(zip_path)
    
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
    records = parse_fec_linkages_file(zip_path)
    
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
        List of expenditure records (raw pipe-delimited data)
    """
    zip_path = download_fec_file('oppexp', cycle, force_refresh, repository)
    
    # For now, return raw records - we'll add proper parsing when we need it
    records = []
    with zipfile.ZipFile(zip_path) as zf:
        txt_files = [name for name in zf.namelist() if name.endswith('.txt')]
        if txt_files:
            with zf.open(txt_files[0]) as f:
                content = f.read().decode('utf-8', errors='ignore')
                lines = content.strip().split('\n')
                for line in lines:
                    if line.strip():
                        records.append({'raw': line})
    
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
