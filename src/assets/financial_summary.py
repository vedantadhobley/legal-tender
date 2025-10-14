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
    
    cycles: List[str] = ["2020", "2022", "2024", "2026"]  # 8 years of data
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
        
        # Deduplicate by (CAND_ID, CVG_END_DT, cycle) - same candidate/date/cycle = duplicate
        seen = set()
        unique_summaries = []
        for summary in member_summaries:
            key = (
                summary.get('CAND_ID'),
                summary.get('CVG_END_DT'),
                summary.get('cycle')
            )
            if key not in seen:
                seen.add(key)
                unique_summaries.append(summary)
        
        # Sort by coverage end date to get most recent data
        unique_summaries.sort(
            key=lambda x: x.get('CVG_END_DT', ''),
            reverse=True
        )
        
        # Organize by cycle for clean per-cycle storage
        # Group summaries by cycle
        by_cycle = {}
        for summary in unique_summaries:
            cycle = summary.get('cycle')
            if cycle not in by_cycle:
                by_cycle[cycle] = []
            by_cycle[cycle].append(summary)
        
        # For each cycle, compute totals (deduped within cycle)
        cycle_data = {}
        for cycle in sorted(by_cycle.keys(), reverse=True):
            cycle_summaries = by_cycle[cycle]
            
            total_receipts = 0
            total_disbursements = 0
            total_indiv_contrib = 0
            cash_on_hand = 0
            total_debts = 0
            
            for summary in cycle_summaries:
                try:
                    total_receipts += float(summary.get('TTL_RECEIPTS', 0) or 0)
                    total_disbursements += float(summary.get('TTL_DISB', 0) or 0)
                    total_indiv_contrib += float(summary.get('TTL_INDIV_CONTRIB', 0) or 0)
                    # For point-in-time values, use the most recent (already sorted)
                    if cash_on_hand == 0:
                        cash_on_hand = float(summary.get('COH_COP', 0) or 0)
                    if total_debts == 0:
                        total_debts = float(summary.get('DEBTS_OWED_BY', 0) or 0)
                except (ValueError, TypeError):
                    continue
            
            cycle_data[cycle] = {
                'total_raised': round(total_receipts, 2),
                'total_spent': round(total_disbursements, 2),
                'cash_on_hand': round(cash_on_hand, 2),
                'individual_contributions': round(total_indiv_contrib, 2),
                'debts': round(total_debts, 2),
                'num_entries': len(cycle_summaries)
            }
        
        # Compute career totals across all cycles (round to avoid floating-point errors)
        career_total_raised = round(sum(c['total_raised'] for c in cycle_data.values()), 2)
        career_total_spent = round(sum(c['total_spent'] for c in cycle_data.values()), 2)
        career_indiv_contrib = round(sum(c['individual_contributions'] for c in cycle_data.values()), 2)
        
        # Most recent cycle (for quick reference)
        most_recent_cycle = sorted(cycle_data.keys(), reverse=True)[0] if cycle_data else None
        
        member_financials[bioguide_id] = {
            'bioguide_id': bioguide_id,
            'name': name,
            'candidate_ids': candidate_ids,
            
            # Per-cycle data (clean, deduplicated)
            'by_cycle': cycle_data,
            
            # Career totals (sum across all cycles)
            'career_totals': {
                'total_raised': career_total_raised,
                'total_spent': career_total_spent,
                'individual_contributions': career_indiv_contrib,
                'cycles_included': sorted(cycle_data.keys())
            },
            
            # Quick stats for rankings
            'latest_cycle': most_recent_cycle,
            'cycles_with_data': len(cycle_data),
            
            # Metadata
            'updated_at': datetime.now(),
        }
        
        members_with_data += 1
        
        # Log deduplication for interesting cases
        if len(member_summaries) != len(unique_summaries):
            context.log.info(
                f"  📊 {name}: {len(member_summaries)} entries → "
                f"{len(unique_summaries)} unique (removed {len(member_summaries) - len(unique_summaries)} duplicates)"
            )
    
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
    # PHASE 4: Show top fundraisers (by career total)
    # ==========================================================================
    context.log.info("\n🏆 TOP 10 FUNDRAISERS (Career Total):")
    context.log.info("-" * 80)
    
    top_fundraisers = sorted(
        member_financials.items(),
        key=lambda x: x[1]['career_totals']['total_raised'],
        reverse=True
    )[:10]
    
    for i, (bioguide_id, data) in enumerate(top_fundraisers, 1):
        career = data['career_totals']
        cycles = career['cycles_included']
        context.log.info(
            f"{i:2d}. {data['name']:30s} "
            f"${career['total_raised']:>12,.0f} raised  "
            f"({len(cycles)} cycles: {min(cycles)}-{max(cycles)})"
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
