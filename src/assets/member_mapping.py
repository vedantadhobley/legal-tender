"""Member FEC mapping assets - Maps Congress members to FEC IDs and financial data.

This module implements the hybrid approach:
1. Download legislators-current.yaml from GitHub (has FEC IDs pre-mapped)
2. Download FEC bulk data (cn, cm, ccl files) for validation
3. Optionally enhance with ProPublica API for photos and extra metadata
4. Build complete member profiles with FEC IDs, committee IDs, and external IDs
"""

from typing import List, Dict, Any
from datetime import datetime

from dagster import (
    asset,
    AssetExecutionContext,
    MetadataValue,
    Output,
    Config,
    AssetIn,
)

from src.data import get_repository
from src.api.congress_legislators import (
    get_current_legislators,
    extract_fec_ids,
    get_current_term,
)
from src.api.fec_bulk_data import (
    load_fec_candidates,
    load_committee_linkages,
)
from src.api.congress_api import get_member  # For photo URLs and enhanced data
from src.resources.mongo import MongoDBResource


class MemberMappingConfig(Config):
    """Configuration for member mapping asset."""
    
    force_refresh: bool = False  # Force re-download of all cached data
    skip_propublica: bool = False  # Skip ProPublica API calls (faster, but no photos)
    cycles: List[str] = ["2024", "2026"]  # FEC cycles to load


@asset(
    name="member_fec_mapping",
    description="Complete mapping of Congress members to FEC IDs, committees, and financial data",
    group_name="fec",
    compute_kind="bulk_data",
    ins={"data_sync": AssetIn("data_sync")},
    metadata={
        "source": "GitHub legislators + FEC bulk data + ProPublica API",
        "cycles": "2024, 2026",
        "cache_strategy": "7-day TTL",
    },
)
def member_fec_mapping_asset(
    context: AssetExecutionContext,
    config: MemberMappingConfig,
    mongo: MongoDBResource,
    data_sync: Dict[str, Any],
) -> Output[Dict[str, Dict[str, Any]]]:
    """Build complete member mapping from multiple data sources.
    
    Data Flow:
    1. Download legislators-current.yaml (1MB, <1 sec) - has FEC IDs pre-mapped
    2. Download FEC bulk data for specified cycles (cn, ccl files - ~4MB total)
    3. Validate FEC IDs against bulk data
    4. Extract committee IDs from linkage files
    5. Optionally enhance with ProPublica API (photos, social media)
    6. Store complete profiles in MongoDB
    
    Args:
        config: Configuration object
        mongo: MongoDB resource
        
    Returns:
        Dictionary mapping bioguide_id -> complete member profile
    """
    context.log.info("=" * 80)
    context.log.info("🏛️  BUILDING MEMBER FEC MAPPING")
    context.log.info("=" * 80)
    
    # Log data sync results
    if data_sync:
        sync_time = data_sync.get('sync_time', 'unknown')
        files_downloaded = len(data_sync.get('files_downloaded', []))
        files_skipped = len(data_sync.get('files_skipped', []))
        context.log.info(f"✓ Data sync completed at {sync_time}")
        context.log.info(f"  - Files downloaded: {files_downloaded}")
        context.log.info(f"  - Files skipped: {files_skipped}")
        context.log.info("")
    
    context.log.info("Configuration:")
    context.log.info(f"  - Force Refresh: {config.force_refresh}")
    context.log.info(f"  - Skip ProPublica: {config.skip_propublica}")
    context.log.info(f"  - FEC Cycles: {', '.join(config.cycles)}")
    context.log.info("")
    
    # Get repository instance
    repo = get_repository()
    
    # ==========================================================================
    # PHASE 1: Load Legislators File (already downloaded by data_sync)
    # ==========================================================================
    context.log.info("📥 PHASE 1: Loading Legislators File")
    context.log.info("-" * 80)
    
    legislators = get_current_legislators(use_cache=True, repository=repo)
    context.log.info(f"✅ Loaded {len(legislators)} current legislators")
    
    # Extract FEC IDs
    fec_id_mapping = extract_fec_ids(legislators)
    context.log.info(f"✅ Found FEC IDs for {len(fec_id_mapping)} legislators " +
                     f"({len(fec_id_mapping)/len(legislators)*100:.1f}% coverage)")
    context.log.info("")
    
    # ==========================================================================
    # PHASE 2: Load FEC Bulk Data (already downloaded by data_sync)
    # ==========================================================================
    context.log.info("📥 PHASE 2: Loading FEC Bulk Data")
    context.log.info("-" * 80)
    
    # Load candidate and linkage data for all cycles
    all_candidates = {}
    all_linkages = {}
    
    for cycle in config.cycles:
        context.log.info(f"Loading {cycle} cycle...")
        candidates = load_fec_candidates(cycle, force_refresh=False, repository=repo)
        linkages = load_committee_linkages(cycle, force_refresh=False, repository=repo)
        
        all_candidates[cycle] = candidates
        all_linkages[cycle] = linkages
        
        context.log.info(f"  ✅ {len(candidates)} candidates, {len(linkages)} with committees")
    
    context.log.info("")
    
    # ==========================================================================
    # PHASE 3: Build Mapping
    # ==========================================================================
    context.log.info("🔨 PHASE 3: Building Member Mapping")
    context.log.info("-" * 80)
    
    mapping = {}
    stats = {
        'total': 0,
        'with_fec_ids': 0,
        'fec_validated': 0,
        'with_committees': 0,
        'propublica_success': 0,
        'propublica_failed': 0,
        'propublica_skipped': 0,
    }
    
    for i, member in enumerate(legislators, 1):
        bioguide_id = member['id']['bioguide']
        name = member['name'].get('official_full', f"{member['name'].get('first', '')} {member['name'].get('last', '')}")
        stats['total'] += 1
        
        if i % 50 == 0 or i == 1:
            context.log.info(f"[{i}/{len(legislators)}] Processing {name}...")
        
        # Get FEC IDs from legislators file
        fec_ids = member['id'].get('fec', [])
        if fec_ids:
            stats['with_fec_ids'] += 1
        
        # Validate FEC IDs against bulk data
        validated_fec_ids = []
        for fec_id in fec_ids:
            for cycle in config.cycles:
                if fec_id in all_candidates[cycle]:
                    validated_fec_ids.append(fec_id)
                    stats['fec_validated'] += 1
                    break  # Found in at least one cycle
        
        validated_fec_ids = list(set(validated_fec_ids))  # Deduplicate
        
        # Get committee IDs from linkages
        committee_ids = []
        for fec_id in validated_fec_ids:
            for cycle in config.cycles:
                if fec_id in all_linkages[cycle]:
                    committee_ids.extend(all_linkages[cycle][fec_id])
        
        committee_ids = list(set(committee_ids))  # Deduplicate
        
        if committee_ids:
            stats['with_committees'] += 1
        
        # Get current term info
        current_term = get_current_term(member)
        
        # Build base mapping (without ProPublica data)
        mapping[bioguide_id] = {
            'name': member['name'],
            'bio': member.get('bio', {}),
            'fec': {
                'candidate_ids': validated_fec_ids,
                'committee_ids': committee_ids,
                'validated': len(validated_fec_ids) > 0,
                'last_validated': datetime.now().isoformat(),
            },
            'congress': current_term,
            'external_ids': {
                k: v for k, v in member['id'].items() 
                if k != 'fec'  # Don't duplicate
            },
            'data_sources': {
                'legislators_file': datetime.now().isoformat(),
                'fec_bulk': datetime.now().isoformat(),
            },
        }
    
    # ==========================================================================
    # PHASE 4: Enhance with ProPublica (Optional)
    # ==========================================================================
    if not config.skip_propublica:
        context.log.info("")
        context.log.info("🌐 PHASE 4: Enhancing with ProPublica API")
        context.log.info("-" * 80)
        context.log.info("⚠️  Note: This will make ~538 API calls at ~2 req/sec (~5 minutes)")
        context.log.info("")
        
        import time
        
        for i, bioguide_id in enumerate(list(mapping.keys()), 1):
            if i % 50 == 0:
                context.log.info(f"[{i}/{len(mapping)}] Fetching ProPublica data...")
            
            try:
                # Fetch from ProPublica API
                member_details = get_member(bioguide_id)
                
                if member_details:
                    # Add ProPublica data
                    mapping[bioguide_id]['image_url'] = member_details.get('image_url')
                    mapping[bioguide_id]['url'] = member_details.get('url')
                    mapping[bioguide_id]['office'] = member_details.get('office')
                    mapping[bioguide_id]['phone'] = member_details.get('phone')
                    
                    # Social media
                    mapping[bioguide_id]['social_media'] = {
                        'twitter': member_details.get('twitter_account'),
                        'facebook': member_details.get('facebook_account'),
                        'youtube': member_details.get('youtube_account'),
                    }
                    
                    mapping[bioguide_id]['data_sources']['propublica_api'] = datetime.now().isoformat()
                    stats['propublica_success'] += 1
                else:
                    stats['propublica_failed'] += 1
                
                # Rate limiting: 2 req/sec
                time.sleep(0.5)
                
            except Exception as e:
                context.log.warning(f"ProPublica API error for {bioguide_id}: {e}")
                stats['propublica_failed'] += 1
    else:
        stats['propublica_skipped'] = len(legislators)
        context.log.info("")
        context.log.info("⏭️  PHASE 4: Skipped (skip_propublica=True)")
    
    # ==========================================================================
    # PHASE 5: Store in MongoDB
    # ==========================================================================
    context.log.info("")
    context.log.info("💾 PHASE 5: Storing in MongoDB")
    context.log.info("-" * 80)
    
    with mongo.get_client() as client:
        collection = mongo.get_collection(client, "member_fec_mapping")
        
        # Clear existing data
        collection.delete_many({})
        context.log.info("🗑️  Cleared existing mapping data")
        
        # Insert new mapping
        docs = [
            {
                '_id': bioguide_id,
                **member_data,
                'updated_at': datetime.now(),
            }
            for bioguide_id, member_data in mapping.items()
        ]
        
        collection.insert_many(docs)
        context.log.info(f"✅ Stored {len(docs)} member profiles")
    
    # ==========================================================================
    # Summary
    # ==========================================================================
    context.log.info("")
    context.log.info("📊 MAPPING SUMMARY")
    context.log.info("=" * 80)
    context.log.info(f"Total Members:           {stats['total']}")
    context.log.info(f"With FEC IDs:            {stats['with_fec_ids']} ({stats['with_fec_ids']/stats['total']*100:.1f}%)")
    context.log.info(f"FEC IDs Validated:       {stats['fec_validated']}")
    context.log.info(f"With Committee IDs:      {stats['with_committees']} ({stats['with_committees']/stats['total']*100:.1f}%)")
    
    if not config.skip_propublica:
        context.log.info(f"ProPublica Success:      {stats['propublica_success']}")
        context.log.info(f"ProPublica Failed:       {stats['propublica_failed']}")
    else:
        context.log.info(f"ProPublica Skipped:      {stats['propublica_skipped']}")
    
    context.log.info("=" * 80)
    context.log.info("🎉 MEMBER FEC MAPPING COMPLETE!")
    context.log.info("=" * 80)
    
    # Sample member for preview
    sample_member = list(mapping.values())[0] if mapping else {}
    
    return Output(
        value=mapping,
        metadata={
            "total_members": stats['total'],
            "with_fec_ids": stats['with_fec_ids'],
            "fec_ids_validated": stats['fec_validated'],
            "with_committees": stats['with_committees'],
            "propublica_success": stats['propublica_success'],
            "propublica_failed": stats['propublica_failed'],
            "cycles_loaded": MetadataValue.json(config.cycles),
            "sample_member": MetadataValue.json(sample_member),
            "cache_directory": "data/fec_cache",
            "mongodb_collection": "member_fec_mapping",
        }
    )
