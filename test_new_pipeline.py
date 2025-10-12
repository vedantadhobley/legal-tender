"""Test script for new member FEC mapping pipeline."""

# Test imports
print("Testing imports...")
try:
    from src.api.congress_legislators import get_current_legislators, extract_fec_ids
    print("✅ congress_legislators module")
except Exception as e:
    print(f"❌ congress_legislators: {e}")

try:
    from src.api.fec_bulk_data import load_fec_candidates, load_committee_linkages
    print("✅ fec_bulk_data module")
except Exception as e:
    print(f"❌ fec_bulk_data: {e}")

try:
    from src.api.congress_api import get_member
    print("✅ congress_api.get_member")
except Exception as e:
    print(f"❌ congress_api: {e}")

# Test basic functionality (without Dagster)
print("\n" + "="*60)
print("Testing data fetching...")
print("="*60)

# Test legislators file
print("\n1. Testing legislators file download...")
try:
    legislators = get_current_legislators(use_cache=True)
    print(f"   ✅ Loaded {len(legislators)} legislators")
    
    fec_mapping = extract_fec_ids(legislators)
    print(f"   ✅ Found FEC IDs for {len(fec_mapping)} members")
    
    # Show sample
    sample = list(legislators)[0]
    name = sample['name'].get('official_full', 'Unknown')
    bioguide = sample['id']['bioguide']
    fec_ids = sample['id'].get('fec', [])
    print(f"\n   Sample: {name}")
    print(f"   - Bioguide: {bioguide}")
    print(f"   - FEC IDs: {fec_ids}")
    
except Exception as e:
    print(f"   ❌ Error: {e}")
    import traceback
    traceback.print_exc()

# Test FEC bulk data
print("\n2. Testing FEC bulk data download...")
try:
    candidates_2024 = load_fec_candidates('2024')
    print(f"   ✅ Loaded {len(candidates_2024)} candidates from 2024 cycle")
    
    # Show sample
    sample_id = list(candidates_2024.keys())[0]
    sample_data = candidates_2024[sample_id]
    print(f"\n   Sample: {sample_data['name']}")
    print(f"   - ID: {sample_id}")
    print(f"   - Office: {sample_data['office']} - {sample_data['state']}")
    
except Exception as e:
    print(f"   ❌ Error: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*60)
print("✅ Test complete!")
print("="*60)
