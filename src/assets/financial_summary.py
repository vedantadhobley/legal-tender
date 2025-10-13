"""Financial summary assets - Aggregated campaign finance data for members.

This module loads FEC summary data (webl files) to get aggregated financial totals
for each member's committees without needing the full transaction-level data.
"""

from typing import Dict, Any, List
from datetime import datetime
from pathlib import Path
import zipfile

from dagster import (
    asset,
    AssetExecutionContext,
    MetadataValue,
    Output,
    Config,
    AssetIn,
)

from src.data import get_repository
from src.api.fec_bulk_data import download_fec_file
from src.resources.mongo import MongoDBResource


class FinancialSummaryConfig(Config):
    """Configuration for financial summary asset."""
    
    cycles: List[str] = ["2024", "2026"]  # FEC cycles to load
    force_refresh: bool = False  # Force re-download


def parse_webl_file(zip_path: Path) -> List[Dict[str, Any]]:
    """Parse FEC webl (candidate financial summary) file.
    
    File format: Pipe-delimited, no header row, 30 columns
    
    Key columns (0-indexed):
    - [0]: CAND_ID - Candidate ID (H/S/P prefix)
    - [1]: CAND_NAME - Candidate name
    - [5]: TTL_RECEIPTS - Total money raised
    - [7]: TTL_DISB - Total disbursements (spent)
    - [9]: COH_COP - Cash on hand (end of period)
    - [17]: TTL_INDIV_CONTRIB - Total from individual contributors
    - [28]: DEBTS_OWED_BY - Debts owed
    - [27]: CVG_END_DT - Coverage end date
    """
    records = []
    with zipfile.ZipFile(zip_path) as zf:
        txt_files = [name for name in zf.namelist() if name.endswith('.txt')]
        if not txt_files:
            return records
        
        with zf.open(txt_files[0]) as f:
            content = f.read().decode('utf-8', errors='ignore')
            lines = content.strip().split('\n')
            
            for line in lines:
                if not line.strip():
                    continue
                parts = line.split('|')
                if len(parts) >= 30:  # Ensure we have all columns
                    try:
                        records.append({
                            'CAND_ID': parts[0],
                            'CAND_NAME': parts[1],
                            'TTL_RECEIPTS': float(parts[5]) if parts[5] else 0.0,
                            'TTL_DISB': float(parts[7]) if parts[7] else 0.0,
                            'COH_COP': float(parts[9]) if parts[9] else 0.0,
                            'TTL_INDIV_CONTRIB': float(parts[17]) if parts[17] else 0.0,
                            'DEBTS_OWED_BY': float(parts[28]) if parts[28] else 0.0,
                            'CVG_END_DT': parts[27],
                        })
                    except (ValueError, IndexError):
                        # Skip malformed lines
                        continue
    
    return records


@asset(
    name="member_financial_summary",
    description="Aggregated financial data for all members (total raised, spent, cash on hand)",
    group_name="fec",
    compute_kind="bulk_data",
    ins={"member_fec_mapping": AssetIn("member_fec_mapping")},
    metadata={
        "source": "FEC bulk data (webl files - committee summaries)",
        "cycles": "2024, 2026",
    },
)
def member_financial_summary_asset(
    context: AssetExecutionContext,
    config: FinancialSummaryConfig,
    mongo: MongoDBResource,
    member_fec_mapping: Dict[str, Any],
) -> Output[Dict[str, Dict[str, Any]]]:
    """Build financial summaries for all members from FEC webl files.
    
    This gives us aggregated totals (raised, spent, cash) without needing
    the full transaction-level data (which is 1.5GB).
    
    Flow:
    1. Load webl files for each cycle (committee financial summaries)
    2. Match committees to members using member_fec_mapping
    3. Aggregate totals if member has multiple committees
    4. Store in MongoDB
    
    Args:
        config: Configuration
        mongo: MongoDB resource
        member_fec_mapping: Member FEC mapping from previous asset
        
    Returns:
        Dict mapping bioguide_id -> financial summary
    """
    context.log.info("=" * 80)
    context.log.info("💰 BUILDING MEMBER FINANCIAL SUMMARIES")
    context.log.info("=" * 80)
    
    repo = get_repository()
    
    # ==========================================================================
    # PHASE 1: Load pas2 files for all cycles
    # ==========================================================================
    context.log.info("\n📥 PHASE 1: Loading Committee Financial Data")
    context.log.info("-" * 80)
    
    all_committee_data = {}
    
    for cycle in config.cycles:
        context.log.info(f"\nLoading {cycle} cycle...")
        
        # Download webl file (committee financial summaries)
        zip_path = download_fec_file('webl', cycle, config.force_refresh, repo)
        
        # Parse candidate summaries
        records = parse_webl_file(zip_path)
        context.log.info(f"✅ Loaded {len(records)} candidate summaries")
        
        # Index by candidate ID
        for record in records:
            cand_id = record['CAND_ID']
            if cand_id not in all_committee_data:
                all_committee_data[cand_id] = []
            all_committee_data[cand_id].append({
                'cycle': cycle,
                **record
            })
    
    context.log.info(f"\n✅ Total unique candidates: {len(all_committee_data)}")
    
    # ==========================================================================
    # PHASE 2: Match candidates to members
    # ==========================================================================
    context.log.info("\n🔗 PHASE 2: Matching Candidates to Members")
    context.log.info("-" * 80)
    
    member_financials = {}
    members_with_data = 0
    members_without_data = 0
    
    for bioguide_id, member_data in member_fec_mapping.items():
        name = member_data['name'].get('official_full', 'Unknown')
        candidate_ids = member_data.get('fec', {}).get('candidate_ids', [])
        
        if not candidate_ids:
            members_without_data += 1
            continue
        
        # Find financial data for this member's candidate IDs
        member_summaries = []
        for cand_id in candidate_ids:
            if cand_id in all_committee_data:
                member_summaries.extend(all_committee_data[cand_id])
        
        if not member_summaries:
            members_without_data += 1
            continue
        
        # Aggregate totals across all cycles and committees
        total_receipts = 0
        total_disbursements = 0
        cash_on_hand = 0
        total_indiv_contrib = 0
        total_debts = 0
        
        for summary in member_summaries:
            try:
                total_receipts += float(summary.get('TTL_RECEIPTS', 0) or 0)
                total_disbursements += float(summary.get('TTL_DISB', 0) or 0)
                cash_on_hand += float(summary.get('COH_COP', 0) or 0)
                total_indiv_contrib += float(summary.get('TTL_INDIV_CONTRIB', 0) or 0)
                total_debts += float(summary.get('DEBTS_OWED_BY', 0) or 0)
            except (ValueError, TypeError):
                continue
        
        member_financials[bioguide_id] = {
            'bioguide_id': bioguide_id,
            'name': name,
            'candidate_ids': candidate_ids,
            'total_raised': total_receipts,
            'total_spent': total_disbursements,
            'cash_on_hand': cash_on_hand,
            'individual_contributions': total_indiv_contrib,
            'debts': total_debts,
            'cycles_covered': config.cycles,
            'raw_summaries': member_summaries,  # Keep raw data for analysis
            'updated_at': datetime.now(),
        }
        
        members_with_data += 1
    
    context.log.info(f"✅ Members with financial data: {members_with_data}")
    context.log.info(f"⚠️  Members without data: {members_without_data}")
    
    # ==========================================================================
    # PHASE 3: Store in MongoDB
    # ==========================================================================
    context.log.info("\n💾 PHASE 3: Storing in MongoDB")
    context.log.info("-" * 80)
    
    with mongo.get_client() as client:
        collection = mongo.get_collection(client, "member_financial_summary")
        
        # Clear existing data
        collection.delete_many({})
        context.log.info("🗑️  Cleared existing financial data")
        
        # Insert new data
        if member_financials:
            docs = [
                {'_id': bioguide_id, **data}
                for bioguide_id, data in member_financials.items()
            ]
            collection.insert_many(docs)
            context.log.info(f"✅ Stored {len(docs)} member financial summaries")
    
    # ==========================================================================
    # PHASE 4: Show top fundraisers
    # ==========================================================================
    context.log.info("\n🏆 TOP 10 FUNDRAISERS:")
    context.log.info("-" * 80)
    
    top_fundraisers = sorted(
        member_financials.items(),
        key=lambda x: x[1]['total_raised'],
        reverse=True
    )[:10]
    
    for i, (bioguide_id, data) in enumerate(top_fundraisers, 1):
        context.log.info(
            f"{i:2d}. {data['name']:30s} "
            f"${data['total_raised']:>12,.0f} raised  "
            f"${data['cash_on_hand']:>12,.0f} cash"
        )
    
    context.log.info("\n" + "=" * 80)
    context.log.info("🎉 FINANCIAL SUMMARY COMPLETE!")
    context.log.info("=" * 80)
    
    # Sample for metadata
    sample = {}
    if member_financials:
        sample = dict(list(member_financials.values())[0])
        # Remove non-JSON-serializable fields
        if 'raw_summaries' in sample:
            del sample['raw_summaries']  # Too big for metadata
        if 'updated_at' in sample:
            sample['updated_at'] = sample['updated_at'].isoformat()  # Convert datetime to string
    
    return Output(
        value=member_financials,
        metadata={
            "total_members": len(member_fec_mapping),
            "members_with_data": members_with_data,
            "members_without_data": members_without_data,
            "cycles_covered": MetadataValue.json(config.cycles),
            "sample_member": MetadataValue.json(sample),
            "mongodb_collection": "member_financial_summary",
        }
    )
