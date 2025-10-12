#!/usr/bin/env python3
"""
Data Repository Inspector

Shows what's currently downloaded and provides utilities for managing the data repository.
"""

import sys
import json
from pathlib import Path
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from data import get_repository


def format_size(bytes_size):
    """Format bytes to human-readable string."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} TB"


def format_age(path):
    """Format file age."""
    if not path.exists():
        return "missing"
    
    age = datetime.now() - datetime.fromtimestamp(path.stat().st_mtime)
    if age.days == 0:
        return "today"
    elif age.days == 1:
        return "1 day ago"
    else:
        return f"{age.days} days ago"


def inspect_repository():
    """Inspect and display repository contents."""
    repo = get_repository()
    
    print("=" * 80)
    print("Data Repository Inspector")
    print("=" * 80)
    print(f"\nBase Path: {repo.base_path.absolute()}")
    
    # Overall stats
    stats = repo.get_repository_stats()
    print(f"\nTotal Size: {stats['total_size_mb']:.2f} MB")
    
    # Legislators
    print("\n" + "─" * 80)
    print("📋 LEGISLATORS")
    print("─" * 80)
    
    if repo.legislators_current_path.exists():
        size = repo.legislators_current_path.stat().st_size
        age = format_age(repo.legislators_current_path)
        print(f"✓ current.yaml        {format_size(size):>12}  ({age})")
    else:
        print("✗ current.yaml        not downloaded")
    
    if repo.legislators_metadata_path.exists():
        metadata = repo.load_metadata(repo.legislators_metadata_path)
        if 'current' in metadata.get('files', {}):
            file_meta = metadata['files']['current']
            print(f"  └─ Downloaded:     {file_meta.get('downloaded_at', 'unknown')}")
            print(f"  └─ Records:        {file_meta.get('count', 'unknown')}")
    
    # FEC Data
    print("\n" + "─" * 80)
    print("💰 FEC BULK DATA")
    print("─" * 80)
    
    cycles = ['2024', '2026']
    for cycle in cycles:
        cycle_dir = repo.fec_cycle_dir(cycle)
        
        if not any(cycle_dir.rglob('*.zip')):
            print(f"\n{cycle} Cycle: No files downloaded")
            continue
        
        print(f"\n{cycle} Cycle:")
        
        # Core files
        core_files = [
            ('candidates.zip', repo.fec_candidates_path(cycle)),
            ('committees.zip', repo.fec_committees_path(cycle)),
            ('linkages.zip', repo.fec_linkages_path(cycle)),
        ]
        
        print("  Core Files:")
        for name, path in core_files:
            if path.exists():
                size = path.stat().st_size
                age = format_age(path)
                print(f"    ✓ {name:20} {format_size(size):>12}  ({age})")
            else:
                print(f"    ✗ {name:20} not downloaded")
        
        # Summaries
        summaries_dir = repo.fec_summaries_dir(cycle)
        if any(summaries_dir.glob('*.zip')):
            summary_files = [
                ('candidate_summary.zip', repo.fec_candidate_summary_path(cycle)),
                ('committee_summary.zip', repo.fec_committee_summary_path(cycle)),
                ('pac_summary.zip', repo.fec_pac_summary_path(cycle)),
            ]
            
            print("\n  Summaries:")
            for name, path in summary_files:
                if path.exists():
                    size = path.stat().st_size
                    age = format_age(path)
                    print(f"    ✓ {name:20} {format_size(size):>12}  ({age})")
        
        # Transactions
        transactions_dir = repo.fec_transactions_dir(cycle)
        if any(transactions_dir.glob('*.zip')):
            transaction_files = [
                ('independent_expenditures.zip', repo.fec_independent_expenditures_path(cycle)),
                ('individual_contributions.zip', repo.fec_individual_contributions_path(cycle)),
                ('committee_transfers.zip', repo.fec_committee_transfers_path(cycle)),
            ]
            
            print("\n  Transactions:")
            for name, path in transaction_files:
                if path.exists():
                    size = path.stat().st_size
                    age = format_age(path)
                    print(f"    ✓ {name:30} {format_size(size):>12}  ({age})")
        
        # Metadata
        metadata_path = repo.fec_cycle_metadata_path(cycle)
        if metadata_path.exists():
            metadata = repo.load_metadata(metadata_path)
            print(f"\n  Metadata: {len(metadata.get('files', {}))} file(s) tracked")
    
    # Congress API cache
    print("\n" + "─" * 80)
    print("🏛️  CONGRESS API CACHE")
    print("─" * 80)
    
    members_dir = repo.congress_api_dir / "members"
    if members_dir.exists():
        member_files = list(members_dir.glob('*.json'))
        if member_files:
            total_size = sum(f.stat().st_size for f in member_files)
            print(f"Members cached: {len(member_files)}")
            print(f"Total size:     {format_size(total_size)}")
        else:
            print("No members cached")
    else:
        print("No members cached")
    
    print("\n" + "=" * 80)


def show_metadata():
    """Show detailed metadata for all files."""
    repo = get_repository()
    
    print("\n" + "=" * 80)
    print("Metadata Details")
    print("=" * 80)
    
    # Legislators metadata
    if repo.legislators_metadata_path.exists():
        print("\n📋 Legislators Metadata:")
        metadata = repo.load_metadata(repo.legislators_metadata_path)
        print(json.dumps(metadata, indent=2))
    
    # FEC metadata
    for cycle in ['2024', '2026']:
        metadata_path = repo.fec_cycle_metadata_path(cycle)
        if metadata_path.exists():
            print(f"\n💰 FEC {cycle} Cycle Metadata:")
            metadata = repo.load_metadata(metadata_path)
            print(json.dumps(metadata, indent=2))


def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Inspect and manage the data repository'
    )
    parser.add_argument(
        '--metadata',
        action='store_true',
        help='Show detailed metadata'
    )
    parser.add_argument(
        '--json',
        action='store_true',
        help='Output stats as JSON'
    )
    
    args = parser.parse_args()
    
    if args.json:
        repo = get_repository()
        stats = repo.get_repository_stats()
        print(json.dumps(stats, indent=2))
    elif args.metadata:
        inspect_repository()
        show_metadata()
    else:
        inspect_repository()


if __name__ == '__main__':
    main()
