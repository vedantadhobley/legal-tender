"""
Data Sync Asset

Independent Dagster asset for downloading and syncing all external data sources.
This runs as its own scheduled job on Sundays to keep data fresh.

Checks Last-Modified headers from sources and only downloads if remote file is newer.
"""

from dagster import asset, AssetExecutionContext, Config, MetadataValue, Output
from datetime import datetime
from typing import Dict, Any, List, Optional
import requests

from ..data import get_repository
from ..api.congress_legislators import download_legislators_file, get_current_legislators, extract_fec_ids
from ..api.fec_bulk_data import download_fec_file, FEC_FILE_MAPPING


class DataSyncConfig(Config):
    """Configuration for data sync operations."""
    
    force_refresh: bool = False
    """Force re-download even if files are fresh"""
    
    cycles: List[str] = ["2024", "2026"]
    """FEC cycles to sync"""
    
    sync_legislators: bool = True
    """Download legislators file"""
    
    sync_fec_core: bool = True
    """Download FEC core files (candidates, committees, linkages)"""
    
    sync_fec_summaries: bool = False
    """Download FEC summary files (~7MB)"""
    
    sync_fec_transactions: bool = False
    """Download FEC transaction files (~4GB)"""
    
    check_remote_modified: bool = True
    """Check Last-Modified headers before downloading"""


def get_remote_last_modified(url: str) -> Optional[datetime]:
    """
    Get Last-Modified timestamp from remote file without downloading.
    
    Args:
        url: URL to check
        
    Returns:
        datetime of last modification, or None if unavailable
    """
    try:
        response = requests.head(url, timeout=10)
        response.raise_for_status()
        
        last_modified = response.headers.get('Last-Modified')
        if last_modified:
            # Parse HTTP date format: "Tue, 15 Oct 2024 14:30:00 GMT"
            return datetime.strptime(last_modified, '%a, %d %b %Y %H:%M:%S %Z')
    except Exception as e:
        print(f"⚠️  Could not check remote Last-Modified for {url}: {e}")
    
    return None


def should_download_file(
    local_path,
    remote_url: str,
    force_refresh: bool = False,
    check_remote: bool = True
) -> bool:
    """
    Determine if a file should be downloaded.
    
    Args:
        local_path: Path to local file
        remote_url: URL of remote file
        force_refresh: Force download regardless of state
        check_remote: Check remote Last-Modified header
        
    Returns:
        True if file should be downloaded
    """
    if force_refresh:
        return True
    
    # If file doesn't exist locally, download
    if not local_path.exists():
        return True
    
    # If we're checking remote modification time
    if check_remote:
        remote_modified = get_remote_last_modified(remote_url)
        if remote_modified:
            local_modified = datetime.fromtimestamp(local_path.stat().st_mtime)
            
            # Download if remote is newer
            if remote_modified > local_modified:
                print(f"ℹ️  Remote file is newer (remote: {remote_modified}, local: {local_modified})")
                return True
            else:
                print(f"✓ Local file is up to date (last modified: {local_modified})")
                return False
    
    # Fall back to age-based check (7 days)
    file_age = datetime.now() - datetime.fromtimestamp(local_path.stat().st_mtime)
    if file_age.days >= 7:
        print(f"ℹ️  Local file is {file_age.days} days old, refreshing...")
        return True
    
    print(f"✓ Local file is fresh ({file_age.days} days old)")
    return False


@asset(
    group_name="data_sync",
    compute_kind="download",
    description="Syncs all external data sources (legislators, FEC bulk data) on a weekly schedule"
)
def data_sync_asset(
    context: AssetExecutionContext,
    config: DataSyncConfig
) -> Output[Dict[str, Any]]:
    """
    Download and sync all external data sources.
    
    This is the primary data ingestion asset that runs on a weekly schedule.
    It checks remote Last-Modified timestamps and only downloads if files have been updated.
    
    Returns:
        Dict with sync statistics and metadata
    """
    context.log.info("=" * 80)
    context.log.info("DATA SYNC - Starting weekly data refresh")
    context.log.info("=" * 80)
    
    repo = get_repository()
    stats = {
        'sync_time': datetime.now().isoformat(),
        'legislators': {},
        'fec': {},
        'files_downloaded': [],
        'files_skipped': [],
        'total_bytes': 0,
        'errors': []
    }
    
    # =========================================================================
    # Phase 1: Sync Legislators Data
    # =========================================================================
    
    if config.sync_legislators:
        context.log.info("\n📋 Phase 1: Syncing Legislators Data")
        context.log.info("-" * 80)
        
        try:
            legislators_url = "https://unitedstates.github.io/congress-legislators/legislators-current.yaml"
            local_path = repo.legislators_current_path
            
            if should_download_file(
                local_path,
                legislators_url,
                config.force_refresh,
                config.check_remote_modified
            ):
                context.log.info("⬇️  Downloading legislators file...")
                legislators = download_legislators_file(repo)
                fec_mapping = extract_fec_ids(legislators)
                
                file_size = local_path.stat().st_size
                stats['files_downloaded'].append('legislators/current.yaml')
                stats['total_bytes'] += file_size
                stats['legislators'] = {
                    'downloaded': True,
                    'count': len(legislators),
                    'fec_coverage': len(fec_mapping),
                    'size_bytes': file_size
                }
                context.log.info(f"✓ Downloaded {len(legislators)} legislators")
                context.log.info(f"✓ FEC ID coverage: {len(fec_mapping)}/{len(legislators)} "
                               f"({len(fec_mapping)/len(legislators)*100:.1f}%)")
            else:
                # Load from cache
                legislators = get_current_legislators(use_cache=True, repository=repo)
                fec_mapping = extract_fec_ids(legislators)
                stats['files_skipped'].append('legislators/current.yaml')
                stats['legislators'] = {
                    'downloaded': False,
                    'cached': True,
                    'count': len(legislators),
                    'fec_coverage': len(fec_mapping)
                }
                context.log.info("✓ Using cached legislators file")
        
        except Exception as e:
            context.log.error(f"❌ Error syncing legislators: {e}")
            stats['errors'].append(f"legislators: {str(e)}")
    
    # =========================================================================
    # Phase 2: Sync FEC Core Files
    # =========================================================================
    
    if config.sync_fec_core:
        context.log.info("\n💰 Phase 2: Syncing FEC Core Files")
        context.log.info("-" * 80)
        
        core_files = ['cn', 'cm', 'ccl']
        
        for cycle in config.cycles:
            context.log.info(f"\n{cycle} Cycle:")
            
            if cycle not in stats['fec']:
                stats['fec'][cycle] = {
                    'files_downloaded': [],
                    'files_skipped': [],
                    'bytes': 0
                }
            
            for basename in core_files:
                try:
                    # Get repository path
                    path_method = getattr(repo, FEC_FILE_MAPPING[basename])
                    local_path = path_method(cycle)
                    
                    # Build remote URL
                    year_suffix = cycle[-2:]
                    fec_filename = f"{basename}{year_suffix}.zip"
                    remote_url = f"https://www.fec.gov/files/bulk-downloads/{cycle}/{fec_filename}"
                    
                    if should_download_file(
                        local_path,
                        remote_url,
                        config.force_refresh,
                        config.check_remote_modified
                    ):
                        context.log.info(f"⬇️  Downloading {local_path.name}...")
                        path = download_fec_file(basename, cycle, config.force_refresh, repo)
                        
                        file_size = path.stat().st_size
                        stats['files_downloaded'].append(f"fec/{cycle}/{local_path.name}")
                        stats['fec'][cycle]['files_downloaded'].append(local_path.name)
                        stats['fec'][cycle]['bytes'] += file_size
                        stats['total_bytes'] += file_size
                        
                        context.log.info(f"✓ Downloaded {local_path.name} ({file_size/1024/1024:.1f} MB)")
                    else:
                        stats['files_skipped'].append(f"fec/{cycle}/{local_path.name}")
                        stats['fec'][cycle]['files_skipped'].append(local_path.name)
                        context.log.info(f"✓ Skipped {local_path.name} (up to date)")
                
                except Exception as e:
                    context.log.error(f"❌ Error downloading {basename} for {cycle}: {e}")
                    stats['errors'].append(f"fec/{cycle}/{basename}: {str(e)}")
    
    # =========================================================================
    # Phase 3: Sync FEC Summary Files (Optional)
    # =========================================================================
    
    if config.sync_fec_summaries:
        context.log.info("\n📊 Phase 3: Syncing FEC Summary Files")
        context.log.info("-" * 80)
        
        summary_files = ['weball', 'webl', 'webk']
        
        for cycle in config.cycles:
            context.log.info(f"\n{cycle} Cycle Summaries:")
            
            for basename in summary_files:
                try:
                    path_method = getattr(repo, FEC_FILE_MAPPING[basename])
                    local_path = path_method(cycle)
                    
                    year_suffix = cycle[-2:]
                    fec_filename = f"{basename}{year_suffix}.zip"
                    remote_url = f"https://www.fec.gov/files/bulk-downloads/{cycle}/{fec_filename}"
                    
                    if should_download_file(
                        local_path,
                        remote_url,
                        config.force_refresh,
                        config.check_remote_modified
                    ):
                        context.log.info(f"⬇️  Downloading {local_path.name}...")
                        path = download_fec_file(basename, cycle, config.force_refresh, repo)
                        
                        file_size = path.stat().st_size
                        stats['files_downloaded'].append(f"fec/{cycle}/summaries/{local_path.name}")
                        stats['fec'][cycle]['files_downloaded'].append(local_path.name)
                        stats['fec'][cycle]['bytes'] += file_size
                        stats['total_bytes'] += file_size
                        
                        context.log.info(f"✓ Downloaded {local_path.name} ({file_size/1024/1024:.1f} MB)")
                    else:
                        stats['files_skipped'].append(f"fec/{cycle}/summaries/{local_path.name}")
                        stats['fec'][cycle]['files_skipped'].append(local_path.name)
                
                except Exception as e:
                    context.log.error(f"❌ Error downloading {basename} summary for {cycle}: {e}")
                    stats['errors'].append(f"fec/{cycle}/summaries/{basename}: {str(e)}")
    
    # =========================================================================
    # Phase 4: Sync FEC Transaction Files (Optional, Large!)
    # =========================================================================
    
    if config.sync_fec_transactions:
        context.log.info("\n💸 Phase 4: Syncing FEC Transaction Files (Large!)")
        context.log.info("-" * 80)
        context.log.warning("⚠️  Transaction files can be 2-3GB each!")
        
        transaction_files = ['oppexp', 'indiv', 'pas2']
        
        for cycle in config.cycles:
            context.log.info(f"\n{cycle} Cycle Transactions:")
            
            for basename in transaction_files:
                try:
                    path_method = getattr(repo, FEC_FILE_MAPPING[basename])
                    local_path = path_method(cycle)
                    
                    year_suffix = cycle[-2:]
                    fec_filename = f"{basename}{year_suffix}.zip"
                    remote_url = f"https://www.fec.gov/files/bulk-downloads/{cycle}/{fec_filename}"
                    
                    if should_download_file(
                        local_path,
                        remote_url,
                        config.force_refresh,
                        config.check_remote_modified
                    ):
                        context.log.info(f"⬇️  Downloading {local_path.name} (may take several minutes)...")
                        path = download_fec_file(basename, cycle, config.force_refresh, repo)
                        
                        file_size = path.stat().st_size
                        stats['files_downloaded'].append(f"fec/{cycle}/transactions/{local_path.name}")
                        stats['fec'][cycle]['files_downloaded'].append(local_path.name)
                        stats['fec'][cycle]['bytes'] += file_size
                        stats['total_bytes'] += file_size
                        
                        context.log.info(f"✓ Downloaded {local_path.name} ({file_size/1024/1024:.1f} MB)")
                    else:
                        stats['files_skipped'].append(f"fec/{cycle}/transactions/{local_path.name}")
                        stats['fec'][cycle]['files_skipped'].append(local_path.name)
                
                except Exception as e:
                    context.log.error(f"❌ Error downloading {basename} transactions for {cycle}: {e}")
                    stats['errors'].append(f"fec/{cycle}/transactions/{basename}: {str(e)}")
    
    # =========================================================================
    # Summary
    # =========================================================================
    
    context.log.info("\n" + "=" * 80)
    context.log.info("DATA SYNC COMPLETE")
    context.log.info("=" * 80)
    context.log.info(f"Files Downloaded: {len(stats['files_downloaded'])}")
    context.log.info(f"Files Skipped:    {len(stats['files_skipped'])}")
    context.log.info(f"Total Downloaded: {stats['total_bytes']/1024/1024:.2f} MB")
    context.log.info(f"Errors:           {len(stats['errors'])}")
    
    if stats['errors']:
        context.log.warning("\nErrors encountered:")
        for error in stats['errors']:
            context.log.warning(f"  - {error}")
    
    # Build metadata for Dagster UI
    metadata = {
        "sync_time": MetadataValue.text(stats['sync_time']),
        "files_downloaded": MetadataValue.int(len(stats['files_downloaded'])),
        "files_skipped": MetadataValue.int(len(stats['files_skipped'])),
        "total_mb_downloaded": MetadataValue.float(stats['total_bytes']/1024/1024),
        "errors": MetadataValue.int(len(stats['errors'])),
    }
    
    if stats.get('legislators', {}).get('count'):
        metadata["legislators_count"] = MetadataValue.int(stats['legislators']['count'])
        metadata["fec_coverage"] = MetadataValue.float(
            stats['legislators']['fec_coverage'] / stats['legislators']['count'] * 100
        )
    
    # Add per-cycle stats
    for cycle, cycle_stats in stats.get('fec', {}).items():
        metadata[f"{cycle}_files_downloaded"] = MetadataValue.int(len(cycle_stats['files_downloaded']))
        metadata[f"{cycle}_mb_downloaded"] = MetadataValue.float(cycle_stats['bytes']/1024/1024)
    
    return Output(
        value=stats,
        metadata=metadata
    )
