"""
Data Repository Manager

Manages the local data repository structure for legal-tender project.
All downloaded data (FEC bulk files, legislators data, etc.) is organized
in a clear, maintainable directory structure.

Storage location: ~/workspace/.legal-tender/data/
This allows data to:
- Survive container rebuilds
- Be shared between dev/prod
- Be excluded from git

This module is Dagster-agnostic - it can be used standalone or orchestrated by Dagster.
"""

from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime
import json
import shutil

from src.utils.storage import get_data_dir, get_cycle_data_dir


class DataRepository:
    """
    Manages the data repository structure and file organization.
    
    Directory Structure (at ~/workspace/.legal-tender/data/):
    ├── legislators/
    │   ├── current.yaml              # Current legislators
    │   ├── historical.yaml           # Historical legislators
    │   └── metadata.json             # Download timestamps, versions
    ├── 2024/                         # FEC data by cycle (flat, no nesting)
    │   ├── cn.zip                    # Candidate Master File
    │   ├── cm.zip                    # Committee Master File
    │   ├── ccl.zip                   # Candidate-Committee Linkages
    │   ├── weball.zip                # Candidate Summary (All)
    │   ├── webl.zip                  # Committee Summary
    │   ├── webk.zip                  # PAC Summary
    │   ├── indiv.zip                 # Individual Contributions (40M+ records!)
    │   ├── pas2.zip                  # Itemized Transactions
    │   ├── oth.zip                   # Other Receipts
    │   └── metadata.json
    ├── 2026/
    │   └── ... (same structure)
    ├── congress_api/
    │   ├── members/
    │   │   ├── {bioguide_id}.json   # Individual member details
    │   │   └── metadata.json
    │   └── bills/
    │       └── ...
    └── metadata.json                 # Repository-level metadata
    """
    
    def __init__(self, base_path: Optional[str] = None):
        """
        Initialize the data repository.
        
        Args:
            base_path: Base directory for all data storage.
                       Defaults to ~/workspace/.legal-tender/data/
        """
        if base_path is None:
            self.base_path = get_data_dir()
        else:
            self.base_path = Path(base_path)
        self._ensure_structure()
    
    def _ensure_structure(self):
        """Create the directory structure if it doesn't exist."""
        # Main directories
        directories = [
            self.base_path,
            self.legislators_dir,
            self.fec_dir,
            self.congress_api_dir,
            self.congress_api_dir / "members",
            self.congress_api_dir / "bills",
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    # ==========================================================================
    # Directory Properties
    # ==========================================================================
    
    @property
    def legislators_dir(self) -> Path:
        """Directory for legislators data."""
        return self.base_path / "legislators"
    
    @property
    def fec_dir(self) -> Path:
        """Base directory for FEC data (backward compat - same as base_path)."""
        return self.base_path
    
    @property
    def congress_api_dir(self) -> Path:
        """Directory for Congress API cached data."""
        return self.base_path / "congress_api"
    
    def fec_cycle_dir(self, cycle: str) -> Path:
        """Get FEC directory for a specific cycle (e.g., ~/workspace/.legal-tender/data/2024/)."""
        cycle_dir = get_cycle_data_dir(cycle)
        cycle_dir.mkdir(parents=True, exist_ok=True)
        return cycle_dir
    
    # ==========================================================================
    # Legislators File Paths
    # ==========================================================================
    
    @property
    def legislators_current_path(self) -> Path:
        """Path to current legislators file."""
        return self.legislators_dir / "current.yaml"
    
    @property
    def legislators_historical_path(self) -> Path:
        """Path to historical legislators file."""
        return self.legislators_dir / "historical.yaml"
    
    @property
    def legislators_metadata_path(self) -> Path:
        """Path to legislators metadata file."""
        return self.legislators_dir / "metadata.json"
    
    # ==========================================================================
    # FEC File Paths (using raw FEC filenames)
    # ==========================================================================
    
    def fec_cn_path(self, cycle: str) -> Path:
        """Path to FEC candidates file (cn.zip) - flat structure per fec.md."""
        return self.fec_cycle_dir(cycle) / "cn.zip"
    
    def fec_cm_path(self, cycle: str) -> Path:
        """Path to FEC committees file (cm.zip) - flat structure per fec.md."""
        return self.fec_cycle_dir(cycle) / "cm.zip"
    
    def fec_ccl_path(self, cycle: str) -> Path:
        """Path to FEC linkages file (ccl.zip) - flat structure per fec.md."""
        return self.fec_cycle_dir(cycle) / "ccl.zip"
    
    def fec_weball_path(self, cycle: str) -> Path:
        """Path to FEC candidate summary file (weball.zip) - flat structure per fec.md."""
        return self.fec_cycle_dir(cycle) / "weball.zip"
    
    def fec_webl_path(self, cycle: str) -> Path:
        """Path to FEC committee summary file (webl.zip) - flat structure per fec.md."""
        return self.fec_cycle_dir(cycle) / "webl.zip"
    
    def fec_webk_path(self, cycle: str) -> Path:
        """Path to FEC PAC summary file (webk.zip) - flat structure per fec.md."""
        return self.fec_cycle_dir(cycle) / "webk.zip"
    
    def fec_indiv_path(self, cycle: str) -> Path:
        """Path to FEC individual contributions file (indiv.zip) - flat structure per fec.md."""
        return self.fec_cycle_dir(cycle) / "indiv.zip"
    
    def fec_pas2_path(self, cycle: str) -> Path:
        """Path to FEC itemized transactions file (pas2.zip).
        
        NOTE: This file contains ALL itemized transactions (Schedule A receipts 
        and Schedule B disbursements), not just committee-to-committee transfers!
        Transaction types include: 24A, 24C, 24E, 24F, 24H, 24K, 24N, 24P, 24R, 24Z
        Flat structure per fec.md.
        """
        return self.fec_cycle_dir(cycle) / "pas2.zip"
    
    def fec_oth_path(self, cycle: str) -> Path:
        """Path to FEC other receipts file (oth.zip).
        
        Contains Schedule A receipts that don't fit other categories:
        - Committee-to-committee transfers (PAC → PAC, PAC → Party)
        - Corporate/union contributions to PACs
        - Refunds, interest, other miscellaneous receipts
        Flat structure per fec.md.
        """
        return self.fec_cycle_dir(cycle) / "oth.zip"
    
    def fec_cycle_metadata_path(self, cycle: str) -> Path:
        """Path to FEC cycle metadata file."""
        return self.fec_cycle_dir(cycle) / "metadata.json"
    
    @property
    def fec_metadata_path(self) -> Path:
        """Path to FEC-level metadata file."""
        return self.fec_dir / "metadata.json"
    
    # ==========================================================================
    # Congress API Paths
    # ==========================================================================
    
    def congress_member_path(self, bioguide_id: str) -> Path:
        """Path to cached Congress API member data."""
        return self.congress_api_dir / "members" / f"{bioguide_id}.json"
    
    # ==========================================================================
    # Metadata Management
    # ==========================================================================
    
    def load_metadata(self, metadata_path: Path) -> Dict[str, Any]:
        """Load metadata from a JSON file."""
        if not metadata_path.exists():
            return {}
        
        try:
            with open(metadata_path) as f:
                return json.load(f)
        except Exception:
            return {}
    
    def save_metadata(self, metadata_path: Path, metadata: Dict[str, Any]):
        """Save metadata to a JSON file."""
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)
    
    def update_file_metadata(
        self,
        metadata_path: Path,
        file_key: str,
        **kwargs
    ):
        """
        Update metadata for a specific file.
        
        Args:
            metadata_path: Path to metadata file
            file_key: Key for this file in metadata
            **kwargs: Metadata fields to update (downloaded_at, size, checksum, etc.)
        """
        metadata = self.load_metadata(metadata_path)
        
        if 'files' not in metadata:
            metadata['files'] = {}
        
        if file_key not in metadata['files']:
            metadata['files'][file_key] = {}
        
        metadata['files'][file_key].update(kwargs)
        metadata['files'][file_key]['updated_at'] = datetime.now().isoformat()
        
        self.save_metadata(metadata_path, metadata)
    
    # ==========================================================================
    # File Age Checking
    # ==========================================================================
    
    def is_file_fresh(self, file_path: Path, max_age_days: int = 7) -> bool:
        """
        Check if a file is fresh (recently downloaded).
        
        Args:
            file_path: Path to file
            max_age_days: Maximum age in days
            
        Returns:
            True if file exists and is fresh
        """
        if not file_path.exists():
            return False
        
        file_age = datetime.now() - datetime.fromtimestamp(file_path.stat().st_mtime)
        return file_age.days < max_age_days
    
    # ==========================================================================
    # Utility Methods
    # ==========================================================================
    
    def get_repository_stats(self) -> Dict[str, Any]:
        """Get statistics about the data repository."""
        stats = {
            'base_path': str(self.base_path.absolute()),
            'total_size_mb': 0,
            'legislators': {
                'current_exists': self.legislators_current_path.exists(),
                'current_age_days': None,
            },
            'fec_cycles': {},
            'congress_api_members': 0,
        }
        
        # Calculate total size
        for path in self.base_path.rglob('*'):
            if path.is_file():
                stats['total_size_mb'] += path.stat().st_size / 1024 / 1024
        
        stats['total_size_mb'] = round(stats['total_size_mb'], 2)
        
        # Legislators age
        if self.legislators_current_path.exists():
            age = datetime.now() - datetime.fromtimestamp(
                self.legislators_current_path.stat().st_mtime
            )
            stats['legislators']['current_age_days'] = age.days
        
        # FEC cycles
        for cycle_dir in self.fec_dir.iterdir():
            if cycle_dir.is_dir() and cycle_dir.name.isdigit():
                cycle = cycle_dir.name
                stats['fec_cycles'][cycle] = {
                    'candidates': self.fec_cn_path(cycle).exists(),
                    'committees': self.fec_cm_path(cycle).exists(),
                    'linkages': self.fec_ccl_path(cycle).exists(),
                }
        
        # Congress API member count
        members_dir = self.congress_api_dir / "members"
        if members_dir.exists():
            stats['congress_api_members'] = len(list(members_dir.glob('*.json')))
        
        return stats
    
    def clear_cycle(self, cycle: str):
        """Clear all data for a specific FEC cycle."""
        cycle_dir = self.fec_cycle_dir(cycle)
        if cycle_dir.exists():
            shutil.rmtree(cycle_dir)
            print(f"Cleared cycle {cycle}")
    
    def clear_all(self):
        """Clear all data (use with caution!)."""
        if self.base_path.exists():
            shutil.rmtree(self.base_path)
            self._ensure_structure()
            print("Cleared all data")


# ==========================================================================
# File Download Utilities
# ==========================================================================

FEC_BULK_URL = "https://www.fec.gov/files/bulk-downloads"

# Mapping of FEC basenames to repository path methods
FEC_FILE_MAPPING = {
    'cn': 'fec_cn_path',
    'cm': 'fec_cm_path',
    'ccl': 'fec_ccl_path',
    'weball': 'fec_weball_path',
    'webl': 'fec_webl_path',
    'webk': 'fec_webk_path',
    'indiv': 'fec_indiv_path',
    'pas2': 'fec_pas2_path',
    'oth': 'fec_oth_path',
}


def download_fec_file(
    file_basename: str,
    cycle: str,
    force_refresh: bool = False,
    repository: Optional['DataRepository'] = None
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
    import requests
    
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


# ==========================================================================
# Global Repository Instance
# ==========================================================================

# Singleton instance
_repository: Optional[DataRepository] = None


def get_repository(base_path: Optional[str] = None) -> DataRepository:
    """
    Get the global data repository instance.
    
    Args:
        base_path: Base directory for data storage.
                   Defaults to ~/workspace/.legal-tender/data/ or LEGAL_TENDER_DATA_DIR env var.
        
    Returns:
        DataRepository instance
    """
    global _repository
    
    if _repository is None:
        _repository = DataRepository(base_path)
    
    return _repository


if __name__ == '__main__':
    # Demo/test the repository structure
    print("Data Repository Structure")
    print("=" * 80)
    
    repo = get_repository()
    
    print("\n📁 Directory Structure:")
    print(f"Base: {repo.base_path.absolute()}")
    print(f"├── Legislators: {repo.legislators_dir}")
    print(f"├── FEC: {repo.fec_dir}")
    print(f"└── Congress API: {repo.congress_api_dir}")
    
    print("\n📊 File Paths (2024 cycle):")
    print(f"Legislators Current: {repo.legislators_current_path}")
    print(f"FEC cn:              {repo.fec_cn_path('2024')}")
    print(f"FEC cm:              {repo.fec_cm_path('2024')}")
    print(f"FEC ccl:             {repo.fec_ccl_path('2024')}")
    print(f"FEC pas2:            {repo.fec_pas2_path('2024')}")
    
    print("\n📈 Repository Stats:")
    stats = repo.get_repository_stats()
    print(json.dumps(stats, indent=2))
    
    print("\n✅ Repository structure initialized!")
