"""Centralized storage configuration for Legal Tender.

All persistent data lives in ~/workspace/data/legal-tender/ to:
- Survive container rebuilds
- Be shared between dev/prod compose stacks (same bind mount, separate runtime volumes)
- Keep project source dirs clean
- Enable easy backup/restore (only `dumps/` needs snapshotting; `raw/` is re-downloadable)

Structure:
    ~/workspace/data/legal-tender/
    ├── raw/                       # Raw FEC downloads + reference data
    │   ├── 2020/  2022/  2024/  2026/
    │   │   ├── cn.zip  cm.zip  pas2.zip  ...
    │   ├── legislators/           # congress-legislators YAML
    │   └── headers/               # FEC field-header CSVs
    ├── dumps/                     # ArangoDB JSONL dumps (fast reload)
    │   ├── fec/                   # Raw FEC collections
    │   │   ├── 2020/  2022/  2024/  2026/
    │   │   │   ├── cn.jsonl.gz  cm.jsonl.gz  ...
    │   ├── enriched/              # Enriched collections (per cycle)
    │   ├── aggregation/           # Aggregation collections (cycle-independent)
    │   └── graphs/                # Graph definitions
    └── cache/                     # API caches (regeneratable)
        ├── congress_api/
        └── (future: wikidata_cache.json, embeddings)
"""

import os
from pathlib import Path
from typing import Optional

# Environment variable overrides (for containerized environments)
STORAGE_ROOT_ENV = "LEGAL_TENDER_STORAGE"
RAW_DIR_ENV = "LEGAL_TENDER_RAW_DIR"
DUMPS_DIR_ENV = "LEGAL_TENDER_DUMPS_DIR"
CACHE_DIR_ENV = "LEGAL_TENDER_CACHE_DIR"

# Default storage root
DEFAULT_STORAGE_ROOT = Path.home() / "workspace" / "data" / "legal-tender"


def get_storage_root() -> Path:
    """Get the root storage directory.

    Priority:
        1. LEGAL_TENDER_STORAGE env var
        2. ~/workspace/data/legal-tender/
    """
    env_root = os.environ.get(STORAGE_ROOT_ENV)
    if env_root:
        return Path(env_root)
    return DEFAULT_STORAGE_ROOT


def get_raw_dir() -> Path:
    """Get the raw FEC + reference data directory.

    Priority:
        1. LEGAL_TENDER_RAW_DIR env var
        2. {storage_root}/raw/
    """
    env_dir = os.environ.get(RAW_DIR_ENV)
    if env_dir:
        return Path(env_dir)
    return get_storage_root() / "raw"


def get_dumps_dir() -> Path:
    """Get the ArangoDB JSONL dumps directory.

    Priority:
        1. LEGAL_TENDER_DUMPS_DIR env var
        2. {storage_root}/dumps/
    """
    env_dir = os.environ.get(DUMPS_DIR_ENV)
    if env_dir:
        return Path(env_dir)
    return get_storage_root() / "dumps"


def get_cache_dir() -> Path:
    """Get the API cache directory.

    Priority:
        1. LEGAL_TENDER_CACHE_DIR env var
        2. {storage_root}/cache/
    """
    env_dir = os.environ.get(CACHE_DIR_ENV)
    if env_dir:
        return Path(env_dir)
    return get_storage_root() / "cache"


def get_cycle_raw_dir(cycle: str) -> Path:
    """Get the raw FEC data directory for a specific cycle.

    Example: ~/workspace/data/legal-tender/raw/2024/
    """
    return get_raw_dir() / cycle


def get_fec_dumps_dir(cycle: str) -> Path:
    """Get the dumps directory for raw FEC collections.

    Example: ~/workspace/data/legal-tender/dumps/fec/2024/
    """
    return get_dumps_dir() / "fec" / cycle


def get_enriched_dumps_dir(cycle: str) -> Path:
    """Get the dumps directory for enriched collections.

    Example: ~/workspace/data/legal-tender/dumps/enriched/2024/
    """
    return get_dumps_dir() / "enriched" / cycle


def get_aggregation_dumps_dir() -> Path:
    """Get the dumps directory for aggregation collections.

    Example: ~/workspace/data/legal-tender/dumps/aggregation/
    """
    return get_dumps_dir() / "aggregation"


def get_graph_dumps_dir() -> Path:
    """Get the dumps directory for graph definitions.

    Example: ~/workspace/data/legal-tender/dumps/graphs/
    """
    return get_dumps_dir() / "graphs"


def ensure_storage_structure() -> dict:
    """Create the full storage directory structure.

    Returns a dict with all created paths for logging.
    """
    paths_created = {
        'storage_root': get_storage_root(),
        'raw_dir': get_raw_dir(),
        'dumps_dir': get_dumps_dir(),
        'cache_dir': get_cache_dir(),
        'fec_dumps': [],
        'enriched_dumps': [],
        'aggregation_dumps': get_aggregation_dumps_dir(),
        'graph_dumps': get_graph_dumps_dir(),
    }

    # Create base directories
    get_storage_root().mkdir(parents=True, exist_ok=True)
    get_raw_dir().mkdir(parents=True, exist_ok=True)
    get_dumps_dir().mkdir(parents=True, exist_ok=True)
    get_cache_dir().mkdir(parents=True, exist_ok=True)
    get_aggregation_dumps_dir().mkdir(parents=True, exist_ok=True)
    get_graph_dumps_dir().mkdir(parents=True, exist_ok=True)

    # Create cycle-specific directories
    cycles = ["2020", "2022", "2024", "2026"]
    for cycle in cycles:
        cycle_raw = get_cycle_raw_dir(cycle)
        cycle_raw.mkdir(parents=True, exist_ok=True)

        fec_dump = get_fec_dumps_dir(cycle)
        fec_dump.mkdir(parents=True, exist_ok=True)
        paths_created['fec_dumps'].append(fec_dump)

        enriched_dump = get_enriched_dumps_dir(cycle)
        enriched_dump.mkdir(parents=True, exist_ok=True)
        paths_created['enriched_dumps'].append(enriched_dump)

    return paths_created


def get_storage_info() -> dict:
    """Get information about current storage configuration."""
    storage_root = get_storage_root()
    raw_dir = get_raw_dir()
    dumps_dir = get_dumps_dir()
    cache_dir = get_cache_dir()

    info = {
        'storage_root': str(storage_root),
        'storage_root_exists': storage_root.exists(),
        'raw_dir': str(raw_dir),
        'raw_dir_exists': raw_dir.exists(),
        'dumps_dir': str(dumps_dir),
        'dumps_dir_exists': dumps_dir.exists(),
        'cache_dir': str(cache_dir),
        'cache_dir_exists': cache_dir.exists(),
        'env_overrides': {
            'LEGAL_TENDER_STORAGE': os.environ.get(STORAGE_ROOT_ENV),
            'LEGAL_TENDER_RAW_DIR': os.environ.get(RAW_DIR_ENV),
            'LEGAL_TENDER_DUMPS_DIR': os.environ.get(DUMPS_DIR_ENV),
            'LEGAL_TENDER_CACHE_DIR': os.environ.get(CACHE_DIR_ENV),
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
