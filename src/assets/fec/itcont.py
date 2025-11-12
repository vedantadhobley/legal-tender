"""Individual Contributions Asset - Parse FEC individual contributions (indiv.zip) using raw FEC field names

This file contains Schedule A itemized individual contributions >$200.

CRITICAL FILTERING STRATEGY (November 2025):
- indiv.zip contains ~40M records per cycle (2-4GB compressed)
- We ONLY need mega-donations ≥$10,000 for Super PAC and big donor visibility
- Filter at parse time: TRANSACTION_AMT >= 10000 AND ENTITY_TP = "IND"
- Result: 40M → ~150K records (99.6% reduction)
- Processing time: 5-10 minutes with streaming + threshold filtering

What we load:
- Individual mega-donations ≥$10,000 to ANY committee
- Super PAC unlimited donations from billionaires/corporations
- Large party committee donations (up to $413K/year limit)
- High-dollar candidate committee donations (close to $6,600 limit)

What we SKIP:
- Small individual donations <$10,000 (~99.6% of records)
- Use webl.zip summary files for aggregate small donor totals

CRITICAL for Super PAC transparency:
- 44.9% of Super PACs have ZERO visible upstream funding without this
- Example: RED SENATE spent $914K, only $1K visible (need individual donors)
- Enables tracing: "MUSK, ELON → Super PAC → Candidate" influence chains
"""

from typing import Dict, Any, List
from datetime import datetime
import zipfile

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config, AssetIn

from src.data import get_repository
from src.resources.mongo import MongoDBResource


class IndividualContributionsConfig(Config):
    cycles: List[str] = ["2020", "2022", "2024", "2026"]
    threshold: int = 100000  # Only load mega-donors with ≥$100K total donations


@asset(
    name="itcont",
    description="FEC individual contributions file (indiv.zip) - Mega-donors ≥$100K aggregate only",
    group_name="fec",
    compute_kind="bulk_data",
    ins={"data_sync": AssetIn("data_sync")},
)
def itcont_asset(
    context: AssetExecutionContext,
    config: IndividualContributionsConfig,
    mongo: MongoDBResource,
    data_sync: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """Parse indiv.zip files and store mega-donations in fec_{cycle}.itcont collections using raw FEC field names."""
    
    repo = get_repository()
    stats = {
        'total_mega_donations': 0,
        'by_cycle': {},
        'top_donors': {},
        'threshold': config.threshold
    }
    
    with mongo.get_client() as client:
        for cycle in config.cycles:
            context.log.info(f"💰 {cycle} Cycle:")
            
            try:
                collection = mongo.get_collection(client, "itcont", database_name=f"fec_{cycle}")
                collection.delete_many({})
                
                zip_path = repo.fec_indiv_path(cycle)
                if not zip_path.exists():
                    context.log.warning(f"⚠️  File not found: {zip_path}")
                    continue
                
                # PHASE 1: Aggregate all donations by (name, employer, committee)
                context.log.info("   📂 Phase 1: Aggregating donations by donor...")
                donor_aggregates = {}
                
                total_parsed = 0
                individual_count = 0
                
                with zipfile.ZipFile(zip_path) as zf:
                    txt_files = [f for f in zf.namelist() if f.endswith('.txt')]
                    if not txt_files:
                        continue
                    
                    with zf.open(txt_files[0]) as f:
                        for line in f:
                            decoded = line.decode('utf-8', errors='ignore').strip()
                            if not decoded:
                                continue
                            
                            fields = decoded.split('|')
                            if len(fields) < 21:
                                continue
                            
                            total_parsed += 1
                            
                            # Log progress every 1M records
                            if total_parsed % 1000000 == 0:
                                context.log.info(
                                    f"      Parsed {total_parsed:,} records, "
                                    f"tracking {len(donor_aggregates):,} unique donors..."
                                )
                            
                            # Only aggregate individual donations
                            entity_tp = fields[6]
                            if entity_tp != "IND":
                                continue
                            
                            try:
                                transaction_amt = float(fields[14]) if fields[14] else 0
                            except (ValueError, IndexError):
                                continue
                            
                            if transaction_amt <= 0:
                                continue
                            
                            individual_count += 1
                            
                            # Aggregate by (name, employer, committee)
                            name = fields[7]
                            employer = fields[11] or 'Not Provided'
                            cmte_id = fields[0]
                            donor_key = (name, employer, cmte_id)
                            
                            if donor_key not in donor_aggregates:
                                donor_aggregates[donor_key] = {
                                    'total_amount': 0,
                                    'transactions': []
                                }
                            
                            donor_aggregates[donor_key]['total_amount'] += transaction_amt
                            donor_aggregates[donor_key]['transactions'].append({
                                'CMTE_ID': cmte_id,
                                'AMNDT_IND': fields[1],
                                'RPT_TP': fields[2],
                                'TRANSACTION_PGI': fields[3],
                                'IMAGE_NUM': fields[4],
                                'TRANSACTION_TP': fields[5],
                                'ENTITY_TP': entity_tp,
                                'NAME': name,
                                'CITY': fields[8],
                                'STATE': fields[9],
                                'ZIP_CODE': fields[10],
                                'EMPLOYER': employer,
                                'OCCUPATION': fields[12],
                                'TRANSACTION_DT': fields[13],
                                'TRANSACTION_AMT': transaction_amt,
                                'OTHER_ID': fields[15],
                                'TRAN_ID': fields[16],
                                'FILE_NUM': fields[17],
                                'MEMO_CD': fields[18],
                                'MEMO_TEXT': fields[19],
                                'SUB_ID': fields[20],
                            })
                
                context.log.info(f"   ✅ Phase 1 complete: {total_parsed:,} total records, {individual_count:,} individual donations")
                context.log.info(f"      Found {len(donor_aggregates):,} unique (donor, committee) pairs")
                
                # PHASE 2: Filter for mega-donors (≥$100K aggregate) and insert
                context.log.info(f"   📂 Phase 2: Filtering for mega-donors (≥${config.threshold:,})...")
                
                batch = []
                mega_donors = 0
                mega_donations = 0
                cycle_stats = {
                    'amount_distribution': {
                        '100k-250k': 0,
                        '250k-500k': 0,
                        '500k-1M': 0,
                        '1M+': 0
                    }
                }
                
                for donor_key, donor_data in donor_aggregates.items():
                    if donor_data['total_amount'] >= config.threshold:
                        mega_donors += 1
                        
                        # Track distribution
                        total = donor_data['total_amount']
                        if total >= 1000000:
                            cycle_stats['amount_distribution']['1M+'] += 1
                        elif total >= 500000:
                            cycle_stats['amount_distribution']['500k-1M'] += 1
                        elif total >= 250000:
                            cycle_stats['amount_distribution']['250k-500k'] += 1
                        else:
                            cycle_stats['amount_distribution']['100k-250k'] += 1
                        
                        # Insert all transactions from this mega-donor
                        for txn in donor_data['transactions']:
                            txn['updated_at'] = datetime.now()
                            batch.append(txn)
                            mega_donations += 1
                            
                            if len(batch) >= 10000:
                                collection.insert_many(batch, ordered=False)
                                batch = []
                
                # Insert remaining
                if batch:
                    collection.insert_many(batch, ordered=False)
                
                context.log.info(
                    f"   ✅ {cycle}: {mega_donors:,} mega-donors (≥${config.threshold:,}), "
                    f"{mega_donations:,} total transactions"
                )
                context.log.info(f"      Total parsed: {total_parsed:,} records")
                context.log.info(f"      Individual donations: {individual_count:,}")
                context.log.info(f"      Mega-donors: {mega_donors:,} ({(mega_donors/len(donor_aggregates)*100):.3f}% of unique donors)")
                context.log.info(f"      Amount distribution: {cycle_stats['amount_distribution']}")
                if cycle_stats['by_transaction_type']:
                    context.log.info(
                        f"      Top transaction types: "
                        f"{dict(sorted(cycle_stats['by_transaction_type'].items(), key=lambda x: x[1], reverse=True)[:5])}"
                    )
                
                stats['by_cycle'][cycle] = {
                    'mega_donors': mega_donors,
                    'total_donations': mega_donations,
                    'total_parsed': total_parsed,
                    'individual_count': individual_count,
                    'amount_distribution': cycle_stats['amount_distribution']
                }
                stats['total_mega_donations'] += mega_donations
                
                # Create indexes on key fields
                collection.create_index([("CMTE_ID", 1)])  # CRITICAL: Recipient committee
                collection.create_index([("NAME", 1)])     # Donor name for deduplication
                collection.create_index([("EMPLOYER", 1)]) # Corporate influence analysis
                collection.create_index([("TRANSACTION_DT", -1)])
                collection.create_index([("TRANSACTION_AMT", -1)])  # Largest donations first
                collection.create_index([("CMTE_ID", 1), ("NAME", 1), ("EMPLOYER", 1)])  # Aggregate by donor
                collection.create_index([("NAME", 1), ("EMPLOYER", 1)])  # Find all donations from specific person
                
            except Exception as e:
                context.log.error(f"   ❌ Error processing {cycle}: {e}")
    
    return Output(
        value=stats,
        metadata={
            "total_mega_donations": stats['total_mega_donations'],
            "threshold": MetadataValue.text(f"${config.threshold:,}"),
            "cycles_processed": MetadataValue.json(config.cycles),
            "mongodb_databases": MetadataValue.json([f"fec_{c}" for c in config.cycles]),
            "mongodb_collection": "itcont",
        }
    )
