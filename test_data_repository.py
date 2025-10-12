#!/usr/bin/env python3
"""
Test script for the new data repository structure.
This can be run standalone (no Dagster required).
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from data import get_repository


def test_repository_structure():
    """Test the data repository structure."""
    print("=" * 80)
    print("Testing Data Repository Structure")
    print("=" * 80)
    
    repo = get_repository()
    
    print("\n📁 Base Directory:")
    print(f"   {repo.base_path.absolute()}")
    
    print("\n📂 Main Directories:")
    print(f"   Legislators: {repo.legislators_dir}")
    print(f"   FEC:         {repo.fec_dir}")
    print(f"   Congress API:{repo.congress_api_dir}")
    
    print("\n📄 Legislators File Paths:")
    print(f"   Current:    {repo.legislators_current_path}")
    print(f"   Historical: {repo.legislators_historical_path}")
    print(f"   Metadata:   {repo.legislators_metadata_path}")
    
    print("\n📊 FEC File Paths (2024 cycle):")
    print(f"   Candidates:              {repo.fec_candidates_path('2024')}")
    print(f"   Committees:              {repo.fec_committees_path('2024')}")
    print(f"   Linkages:                {repo.fec_linkages_path('2024')}")
    print(f"   Candidate Summary:       {repo.fec_candidate_summary_path('2024')}")
    print(f"   Independent Expend:      {repo.fec_independent_expenditures_path('2024')}")
    print(f"   Individual Contrib:      {repo.fec_individual_contributions_path('2024')}")
    
    print("\n📊 FEC File Paths (2026 cycle):")
    print(f"   Candidates:              {repo.fec_candidates_path('2026')}")
    print(f"   Committees:              {repo.fec_committees_path('2026')}")
    print(f"   Linkages:                {repo.fec_linkages_path('2026')}")
    
    print("\n📈 Repository Stats:")
    stats = repo.get_repository_stats()
    print(f"   Total Size:              {stats['total_size_mb']} MB")
    print(f"   Legislators Current:     {'✓ exists' if stats['legislators']['current_exists'] else '✗ missing'}")
    if stats['legislators']['current_age_days'] is not None:
        print(f"   Legislators Age:         {stats['legislators']['current_age_days']} days")
    print(f"   FEC Cycles:              {len(stats['fec_cycles'])} cycles")
    for cycle, files in stats['fec_cycles'].items():
        print(f"      {cycle}: candidates={files['candidates']}, committees={files['committees']}, linkages={files['linkages']}")
    print(f"   Congress API Members:    {stats['congress_api_members']} cached")
    
    print("\n✅ Repository structure test complete!")


def test_legislators_download():
    """Test downloading legislators file."""
    print("\n" + "=" * 80)
    print("Testing Legislators Download")
    print("=" * 80 + "\n")
    
    from api.congress_legislators import get_current_legislators, extract_fec_ids
    
    try:
        # Get legislators (will use cache if available)
        legislators = get_current_legislators(use_cache=True)
        
        print(f"\n✓ Downloaded {len(legislators)} legislators")
        
        # Extract FEC IDs
        fec_mapping = extract_fec_ids(legislators)
        print(f"✓ Extracted FEC IDs for {len(fec_mapping)} legislators")
        
        # Show sample
        print("\n📊 Sample legislators:")
        for i, member in enumerate(legislators[:3], 1):
            bioguide_id = member['id']['bioguide']
            name = member['name'].get('official_full', f"{member['name']['first']} {member['name']['last']}")
            fec_ids = member['id'].get('fec', [])
            
            print(f"\n{i}. {name}")
            print(f"   Bioguide: {bioguide_id}")
            print(f"   FEC IDs: {', '.join(fec_ids) if fec_ids else 'None'}")
        
        print("\n✅ Legislators download test complete!")
        return True
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return False


def test_fec_download():
    """Test downloading FEC files."""
    print("\n" + "=" * 80)
    print("Testing FEC Download (small files only)")
    print("=" * 80 + "\n")
    
    from api.fec_bulk_data import load_fec_candidates, load_committee_linkages
    
    try:
        # Download 2024 candidates file (~350KB)
        print("Downloading 2024 candidates...")
        candidates = load_fec_candidates('2024')
        print(f"✓ Loaded {len(candidates)} candidates")
        
        # Show sample
        print("\n📊 Sample candidates:")
        for i, (cand_id, cand) in enumerate(list(candidates.items())[:3], 1):
            print(f"\n{i}. {cand['name']}")
            print(f"   ID: {cand_id}")
            print(f"   Party: {cand['party']}, Office: {cand['office']}, State: {cand['state']}")
        
        # Download 2024 linkages file (~500KB)
        print("\nDownloading 2024 committee linkages...")
        linkages = load_committee_linkages('2024')
        print(f"✓ Loaded linkages for {len(linkages)} candidates")
        
        print("\n✅ FEC download test complete!")
        return True
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    # Test repository structure
    test_repository_structure()
    
    # Ask before downloading
    print("\n" + "=" * 80)
    response = input("\n🌐 Run download tests? This will download ~1MB of data. (y/n): ")
    
    if response.lower() == 'y':
        success_legislators = test_legislators_download()
        success_fec = test_fec_download()
        
        print("\n" + "=" * 80)
        print("Summary")
        print("=" * 80)
        print(f"Legislators: {'✅ PASS' if success_legislators else '❌ FAIL'}")
        print(f"FEC:         {'✅ PASS' if success_fec else '❌ FAIL'}")
        
        # Show final stats
        print("\n📊 Final Repository Stats:")
        repo = get_repository()
        stats = repo.get_repository_stats()
        print(f"   Total Size: {stats['total_size_mb']} MB")
        print(f"   Files Created:")
        if stats['legislators']['current_exists']:
            print(f"      ✓ data/legislators/current.yaml")
        for cycle, files in stats['fec_cycles'].items():
            if files['candidates']:
                print(f"      ✓ data/fec/{cycle}/candidates.zip")
            if files['linkages']:
                print(f"      ✓ data/fec/{cycle}/linkages.zip")
    else:
        print("\nSkipping download tests.")
    
    print("\n✨ All tests complete!")
