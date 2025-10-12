#!/usr/bin/env python3
"""
Quick test script to verify FEC bulk data caching is working.
Run this outside of Dagster to test the caching mechanism.
"""
import sys
sys.path.insert(0, '/home/vedanta/legal-tender/src')

from api.fec_bulk_data import get_cache
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def test_cache():
    """Test that caching is working properly."""
    print("\n" + "="*80)
    print("FEC BULK DATA CACHE TEST")
    print("="*80)
    
    cache = get_cache()
    
    # Test 1: Load small files (should cache after first download)
    print("\n[TEST 1] Loading 2024 cycle metadata (cn.txt, cm.txt, ccl.txt)...")
    print("First run will download, second run should use cache.\n")
    
    success = cache.load_cycle(2024)
    if success:
        print(f"✅ Loaded {len(cache.candidates[2024])} candidates")
        print(f"✅ Loaded {len(cache.committees[2024])} committees")
        print(f"✅ Loaded {len(cache.linkages[2024])} linkages")
    else:
        print("❌ Failed to load cycle 2024")
        return
    
    # Test 2: Try loading again (should use cache)
    print("\n[TEST 2] Loading 2024 cycle again (should say 'already loaded')...")
    cache.load_cycle(2024)
    
    # Test 3: Search for a test candidate
    print("\n[TEST 3] Searching for a test candidate (Ocasio-Cortez, NY, House)...")
    candidate = cache.search_candidate("Ocasio-Cortez", "NY", "H", cycles=[2024])
    if candidate:
        print(f"✅ Found: {candidate['name']} ({candidate['candidate_id']})")
        print(f"   Office: {candidate['office_full']}")
        print(f"   Party: {candidate['party']}")
    else:
        print("❌ Candidate not found")
    
    # Test 4: Check cache directory
    print("\n[TEST 4] Checking cache directory...")
    import os
    cache_dir = "/app/cache/fec_bulk"
    
    # If running outside Docker, check host path
    if not os.path.exists(cache_dir):
        cache_dir = "/home/vedanta/legal-tender/data/fec_cache"
    
    if os.path.exists(cache_dir):
        files = os.listdir(cache_dir)
        if files:
            print(f"✅ Cache directory exists with {len(files)} files:")
            for f in sorted(files)[:10]:  # Show first 10
                if f.endswith('.md'):
                    continue
                size = os.path.getsize(os.path.join(cache_dir, f))
                size_mb = size / (1024 * 1024)
                print(f"   - {f}: {size_mb:.1f} MB")
            if len(files) > 10:
                print(f"   ... and {len(files) - 10} more")
        else:
            print("⚠️  Cache directory exists but is empty (first run)")
    else:
        print(f"⚠️  Cache directory not found at {cache_dir}")
    
    print("\n" + "="*80)
    print("TEST COMPLETE")
    print("="*80)
    print("\nTo force refresh (ignore cache), run with:")
    print("  FEC_FORCE_REFRESH=true python test_cache.py")
    print("\nOr delete cache manually:")
    print("  rm -rf /home/vedanta/legal-tender/data/fec_cache/*")
    print("="*80 + "\n")

if __name__ == "__main__":
    test_cache()
