"""
Script to test API keys and connectivity for Congress, FEC, and Lobbying APIs.
"""
from src.api import congress_api, fec_api, lobbying_api

# Test Congress API
print("Testing Congress API...")
members = congress_api.get_members()
if members:
    print(f"Congress API: Success. Found {len(members.get('members', []))} members.")
else:
    print("Congress API: Failed.")

# Test FEC API (replace with a real candidate_id for a real test)
print("\nTesting FEC API...")
candidate_id = "P80001571"  # Example: Barack Obama 2012
committees = fec_api.get_candidate_committees(candidate_id)
if committees:
    print(f"FEC API: Success. Found {len(committees)} committees. Fetching contributions for first committee...")
    contrib_data = fec_api.get_committee_contributions(committees[0])
    if contrib_data:
        print(f"FEC API: Success. Found {contrib_data.get('pagination', {}).get('count', 0)} contributions for committee {committees[0]}.")
    else:
        print(f"FEC API: Failed to fetch contributions for committee {committees[0]}.")
else:
    print("FEC API: Failed to fetch committees.")

# Test Lobbying API
print("\nTesting Lobbying API...")
lobbying = lobbying_api.search_filings(limit=1)
if lobbying:
    print(f"Lobbying API: Success. Found {lobbying.get('count', 0)} filings.")
else:
    print("Lobbying API: Failed.")
