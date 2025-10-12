"""
Data Repository Manager

Manages the local data repository structure for legal-tender project.
All downloaded data (FEC bulk files, legislators data, etc.) is organized
in a clear, maintainable directory structure.

This module is Dagster-agnostic - it can be used standalone or orchestrated by Dagster.
"""

from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime
import json
import shutil


class DataRepository:
    """
    Manages the data repository structure and file organization.
    
    Directory Structure:
    data/
    ├── legislators/
    │   ├── current.yaml              # Current legislators
    │   ├── historical.yaml           # Historical legislators
    │   └── metadata.json             # Download timestamps, versions
    ├── fec/
    │   ├── 2024/
    │   │   ├── candidates.zip        # cn24.zip -> candidates.zip
    │   │   ├── committees.zip        # cm24.zip -> committees.zip
    │   │   ├── linkages.zip          # ccl24.zip -> linkages.zip
    │   │   ├── summaries/
    │   │   │   ├── candidate_summary.zip    # weball24.zip
    │   │   │   ├── committee_summary.zip    # webl24.zip
    │   │   │   └── pac_summary.zip          # webk24.zip
    │   │   ├── transactions/
    │   │   │   ├── individual_contributions.zip  # indiv24.zip
    │   │   │   ├── independent_expenditures.zip  # oppexp24.zip
    │   │   │   └── committee_transfers.zip       # pas224.zip
    │   │   └── metadata.json
    │   ├── 2026/
    │   │   └── ... (same structure)
    │   └── metadata.json             # FEC data metadata
    ├── congress_api/
    │   ├── members/
    │   │   ├── {bioguide_id}.json   # Individual member details
    │   │   └── metadata.json
    │   └── bills/
    │       └── ...
    └── metadata.json                 # Repository-level metadata
    """
    
    def __init__(self, base_path: str = "data"):
        """
        Initialize the data repository.
        
        Args:
            base_path: Base directory for all data storage
        """
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
        """Directory for FEC bulk data."""
        return self.base_path / "fec"
    
    @property
    def congress_api_dir(self) -> Path:
        """Directory for Congress API cached data."""
        return self.base_path / "congress_api"
    
    def fec_cycle_dir(self, cycle: str) -> Path:
        """Get FEC directory for a specific cycle."""
        cycle_dir = self.fec_dir / cycle
        cycle_dir.mkdir(parents=True, exist_ok=True)
        return cycle_dir
    
    def fec_summaries_dir(self, cycle: str) -> Path:
        """Get FEC summaries directory for a cycle."""
        summaries_dir = self.fec_cycle_dir(cycle) / "summaries"
        summaries_dir.mkdir(parents=True, exist_ok=True)
        return summaries_dir
    
    def fec_transactions_dir(self, cycle: str) -> Path:
        """Get FEC transactions directory for a cycle."""
        transactions_dir = self.fec_cycle_dir(cycle) / "transactions"
        transactions_dir.mkdir(parents=True, exist_ok=True)
        return transactions_dir
    
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
    # FEC File Paths (with friendly names)
    # ==========================================================================
    
    def fec_candidates_path(self, cycle: str) -> Path:
        """Path to FEC candidates file (cn{YY}.zip)."""
        return self.fec_cycle_dir(cycle) / "candidates.zip"
    
    def fec_committees_path(self, cycle: str) -> Path:
        """Path to FEC committees file (cm{YY}.zip)."""
        return self.fec_cycle_dir(cycle) / "committees.zip"
    
    def fec_linkages_path(self, cycle: str) -> Path:
        """Path to FEC linkages file (ccl{YY}.zip)."""
        return self.fec_cycle_dir(cycle) / "linkages.zip"
    
    def fec_candidate_summary_path(self, cycle: str) -> Path:
        """Path to FEC candidate summary file (weball{YY}.zip)."""
        return self.fec_summaries_dir(cycle) / "candidate_summary.zip"
    
    def fec_committee_summary_path(self, cycle: str) -> Path:
        """Path to FEC committee summary file (webl{YY}.zip)."""
        return self.fec_summaries_dir(cycle) / "committee_summary.zip"
    
    def fec_pac_summary_path(self, cycle: str) -> Path:
        """Path to FEC PAC summary file (webk{YY}.zip)."""
        return self.fec_summaries_dir(cycle) / "pac_summary.zip"
    
    def fec_individual_contributions_path(self, cycle: str) -> Path:
        """Path to FEC individual contributions file (indiv{YY}.zip)."""
        return self.fec_transactions_dir(cycle) / "individual_contributions.zip"
    
    def fec_independent_expenditures_path(self, cycle: str) -> Path:
        """Path to FEC independent expenditures file (oppexp{YY}.zip)."""
        return self.fec_transactions_dir(cycle) / "independent_expenditures.zip"
    
    def fec_committee_transfers_path(self, cycle: str) -> Path:
        """Path to FEC committee transfers file (pas2{YY}.zip)."""
        return self.fec_transactions_dir(cycle) / "committee_transfers.zip"
    
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
                    'candidates': self.fec_candidates_path(cycle).exists(),
                    'committees': self.fec_committees_path(cycle).exists(),
                    'linkages': self.fec_linkages_path(cycle).exists(),
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
# Global Repository Instance
# ==========================================================================

# Singleton instance
_repository: Optional[DataRepository] = None


def get_repository(base_path: str = "data") -> DataRepository:
    """
    Get the global data repository instance.
    
    Args:
        base_path: Base directory for data storage
        
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
    print(f"FEC Candidates:      {repo.fec_candidates_path('2024')}")
    print(f"FEC Committees:      {repo.fec_committees_path('2024')}")
    print(f"FEC Linkages:        {repo.fec_linkages_path('2024')}")
    print(f"FEC Expenditures:    {repo.fec_independent_expenditures_path('2024')}")
    
    print("\n📈 Repository Stats:")
    stats = repo.get_repository_stats()
    print(json.dumps(stats, indent=2))
    
    print("\n✅ Repository structure initialized!")
