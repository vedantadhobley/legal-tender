"""Centralized storage configuration for Legal Tender.

All persistent data lives in ~/workspace/.legal-tender/ to:
- Survive container rebuilds
- Be shared between dev/prod
- Keep project directory clean
- Enable easy backup/restore

Structure:
    ~/workspace/.legal-tender/
    ├── data/                      # Raw FEC downloads
    │   ├── 2020/
    │   │   ├── cn.zip
    │   │   ├── cm.zip
    │   │   ├── pas2.zip
    │   │   └── ...
    │   ├── 2022/
    │   ├── 2024/
    │   └── 2026/
    ├── arango/                    # ArangoDB JSONL dumps
    │   ├── fec/                   # Raw FEC collections
    │   │   ├── 2020/
    │   │   │   ├── cn.jsonl.gz
    │   │   │   ├── cm.jsonl.gz
    │   │   │   └── ...
    │   │   ├── 2022/
    │   │   ├── 2024/
    │   │   └── 2026/
    │   ├── enriched/              # Enriched collections
    │   │   ├── 2020/
    │   │   ├── 2022/
    │   │   ├── 2024/
    │   │   └── 2026/
    │   ├── aggregation/           # Aggregation collections (cycle-independent)
    │   │   ├── candidate_summaries.jsonl.gz
    │   │   ├── committee_summaries.jsonl.gz
    │   │   └── ...
    │   └── graphs/                # Graph definitions
    │       └── money_flow.json
"""

import os
from pathlib import Path
from typing import Optional

# Environment variable overrides (for containerized environments)
STORAGE_ROOT_ENV = "LEGAL_TENDER_STORAGE"
DATA_DIR_ENV = "LEGAL_TENDER_DATA_DIR"
ARANGO_DIR_ENV = "LEGAL_TENDER_ARANGO_DIR"
BSON_DIR_ENV = "LEGAL_TENDER_BSON_DIR"  # Legacy, kept for backward compat

# Default storage root
DEFAULT_STORAGE_ROOT = Path.home() / "workspace" / ".legal-tender"


def get_storage_root() -> Path:
    """Get the root storage directory.
    
    Priority:
        1. LEGAL_TENDER_STORAGE env var
        2. ~/workspace/.legal-tender/
    """
    env_root = os.environ.get(STORAGE_ROOT_ENV)
    if env_root:
        return Path(env_root)
    return DEFAULT_STORAGE_ROOT


def get_data_dir() -> Path:
    """Get the raw FEC data directory.
    
    Priority:
        1. LEGAL_TENDER_DATA_DIR env var
        2. {storage_root}/data/
    """
    env_dir = os.environ.get(DATA_DIR_ENV)
    if env_dir:
        return Path(env_dir)
    return get_storage_root() / "data"


def get_bson_dir() -> Path:
    """Get the legacy BSON dumps directory (DEPRECATED).
    
    Priority:
        1. LEGAL_TENDER_BSON_DIR env var
        2. {storage_root}/bson/
        
    DEPRECATED: This was used for MongoDB dumps which have been removed.
    Kept for backward compatibility only. Use get_arango_dump_dir() instead.
    """
    env_dir = os.environ.get(BSON_DIR_ENV)
    if env_dir:
        return Path(env_dir)
    return get_storage_root() / "bson"


def get_arango_dump_dir() -> Path:
    """Get the ArangoDB JSONL dumps directory.
    
    Priority:
        1. LEGAL_TENDER_ARANGO_DIR env var
        2. {storage_root}/arango/
    """
    env_dir = os.environ.get(ARANGO_DIR_ENV)
    if env_dir:
        return Path(env_dir)
    return get_storage_root() / "arango"


def get_arango_fec_dump_dir(cycle: str) -> Path:
    """Get the ArangoDB dump directory for raw FEC collections.
    
    Example: ~/workspace/.legal-tender/arango/fec/2024/
    """
    return get_arango_dump_dir() / "fec" / cycle


def get_arango_enriched_dump_dir(cycle: str) -> Path:
    """Get the ArangoDB dump directory for enriched collections.
    
    Example: ~/workspace/.legal-tender/arango/enriched/2024/
    """
    return get_arango_dump_dir() / "enriched" / cycle


def get_arango_aggregation_dump_dir() -> Path:
    """Get the ArangoDB dump directory for aggregation collections.
    
    Example: ~/workspace/.legal-tender/arango/aggregation/
    """
    return get_arango_dump_dir() / "aggregation"


def get_arango_graph_dump_dir() -> Path:
    """Get the ArangoDB dump directory for graph definitions.
    
    Example: ~/workspace/.legal-tender/arango/graphs/
    """
    return get_arango_dump_dir() / "graphs"


def get_fec_dump_dir(cycle: str) -> Path:
    """Get the BSON dump directory for raw FEC collections.
    
    Example: ~/workspace/.legal-tender/bson/fec/2024/
    """
    return get_bson_dir() / "fec" / cycle


def get_enriched_dump_dir(cycle: str) -> Path:
    """Get the BSON dump directory for enriched collections.
    
    Example: ~/workspace/.legal-tender/bson/enriched/2024/
    """
    return get_bson_dir() / "enriched" / cycle


def get_aggregation_dump_dir() -> Path:
    """Get the BSON dump directory for aggregation collections.
    
    Example: ~/workspace/.legal-tender/bson/aggregation/
    """
    return get_bson_dir() / "aggregation"


def get_cycle_data_dir(cycle: str) -> Path:
    """Get the raw FEC data directory for a specific cycle.
    
    Example: ~/workspace/.legal-tender/data/2024/
    """
    return get_data_dir() / cycle


def ensure_storage_structure() -> dict:
    """Create the full storage directory structure.
    
    Returns a dict with all created paths for logging.
    """
    paths_created = {
        'storage_root': get_storage_root(),
        'data_dir': get_data_dir(),
        'arango_dir': get_arango_dump_dir(),
        'arango_fec_dumps': [],
        'arango_enriched_dumps': [],
        'arango_aggregation_dump': get_arango_aggregation_dump_dir(),
        'arango_graph_dump': get_arango_graph_dump_dir(),
        # Legacy BSON (kept for backward compatibility)
        'bson_dir': get_bson_dir(),
    }
    
    # Create base directories
    get_storage_root().mkdir(parents=True, exist_ok=True)
    get_data_dir().mkdir(parents=True, exist_ok=True)
    get_arango_dump_dir().mkdir(parents=True, exist_ok=True)
    get_arango_aggregation_dump_dir().mkdir(parents=True, exist_ok=True)
    get_arango_graph_dump_dir().mkdir(parents=True, exist_ok=True)
    
    # Create cycle-specific directories
    cycles = ["2020", "2022", "2024", "2026"]
    for cycle in cycles:
        # Data directories
        cycle_data = get_cycle_data_dir(cycle)
        cycle_data.mkdir(parents=True, exist_ok=True)
        
        # ArangoDB FEC dump directories
        arango_fec_dump = get_arango_fec_dump_dir(cycle)
        arango_fec_dump.mkdir(parents=True, exist_ok=True)
        paths_created['arango_fec_dumps'].append(arango_fec_dump)
        
        # ArangoDB enriched dump directories
        arango_enriched_dump = get_arango_enriched_dump_dir(cycle)
        arango_enriched_dump.mkdir(parents=True, exist_ok=True)
        paths_created['arango_enriched_dumps'].append(arango_enriched_dump)
    
    return paths_created


def get_storage_info() -> dict:
    """Get information about current storage configuration."""
    storage_root = get_storage_root()
    data_dir = get_data_dir()
    arango_dir = get_arango_dump_dir()
    
    info = {
        'storage_root': str(storage_root),
        'storage_root_exists': storage_root.exists(),
        'data_dir': str(data_dir),
        'data_dir_exists': data_dir.exists(),
        'arango_dir': str(arango_dir),
        'arango_dir_exists': arango_dir.exists(),
        'env_overrides': {
            'LEGAL_TENDER_STORAGE': os.environ.get(STORAGE_ROOT_ENV),
            'LEGAL_TENDER_DATA_DIR': os.environ.get(DATA_DIR_ENV),
            'LEGAL_TENDER_ARANGO_DIR': os.environ.get(ARANGO_DIR_ENV),
        }
    }
    
    # Check disk usage if root exists
    if storage_root.exists():
        try:
            import shutil
            total, used, free = shutil.disk_usage(storage_root)
            info['disk'] = {
                'total_gb': round(total / (1024**3), 2),
                'used_gb': round(used / (1024**3), 2),
                'free_gb': round(free / (1024**3), 2),
            }
        except Exception:
            pass
    
    return info
