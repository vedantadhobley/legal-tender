#!/usr/bin/env python3
"""
Quick test script to download FEC data files.
Run this to test downloads before running full Dagster pipeline.
"""

from src.data import get_repository
from src.data.repository import download_fec_file

def main():
    print("=" * 80)
    print("FEC Data Download Test")
    print("=" * 80)
    
    repo = get_repository()
    
    # Test small file first (cn - candidates, ~2MB)
    print("\n1. Testing small file download (cn - candidates)...")
    try:
        path = download_fec_file('cn', '2024', force_refresh=False, repository=repo)
        print(f"✅ Success: {path}")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # Test medium file (pas2 - committee transfers, ~300MB)
    print("\n2. Testing medium file download (pas2 - committee transfers)...")
    try:
        path = download_fec_file('pas2', '2024', force_refresh=False, repository=repo)
        print(f"✅ Success: {path}")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # Prompt before large file
    print("\n3. Large file download (indiv - individual contributions, 2-4GB)")
    response = input("   Download large file? This will take 5-10 minutes. (y/N): ")
    
    if response.lower() == 'y':
        print("   Downloading indiv for 2024...")
        try:
            path = download_fec_file('indiv', '2024', force_refresh=False, repository=repo)
            print(f"✅ Success: {path}")
            print(f"   Size: {path.stat().st_size / 1024 / 1024:.1f} MB")
        except Exception as e:
            print(f"❌ Error: {e}")
    else:
        print("   Skipped large file download")
    
    print("\n" + "=" * 80)
    print("Test complete!")
    print("=" * 80)

if __name__ == '__main__':
    main()
