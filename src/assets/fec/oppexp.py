"""FEC Operating Expenditures (oppexp.zip) Parser.

This file contains committee operating expenditures - money spent on campaign
activities like ads, printing, consulting, events, etc. This shows WHERE
campaign money goes (vendors, services, media buys).

Structure: ZIP file containing TXT file with pipe-delimited records
Output: fec_2024.oppexp collection

Key Fields:
- CMTE_ID: Committee making the expenditure
- NAME: Payee (vendor/service provider)
- TRANSACTION_AMT: Amount spent
- PURPOSE: What the money was spent on
- CATEGORY: Type of expenditure (ads, materials, etc.)
"""

import zipfile
import io
from typing import Dict, Any

from dagster import asset, AssetExecutionContext, AssetIn, Output

from src.data import get_repository
from src.resources.mongo import MongoDBResource


# Field mapping for oppexp.zip (25 fields)
OPPEXP_FIELDS = [
    'CMTE_ID',              # 1
    'AMNDT_IND',            # 2
    'RPT_YR',               # 3
    'RPT_TP',               # 4
    'IMAGE_NUM',            # 5
    'LINE_NUM',             # 6
    'FORM_TP_CD',           # 7
    'SCHED_TP_CD',          # 8
    'NAME',                 # 9 - Payee
    'CITY',                 # 10
    'STATE',                # 11
    'ZIP_CODE',             # 12
    'TRANSACTION_DT',       # 13
    'TRANSACTION_AMT',      # 14
    'TRANSACTION_PGI',      # 15
    'PURPOSE',              # 16
    'CATEGORY',             # 17
    'CATEGORY_DESC',        # 18
    'MEMO_CD',              # 19
    'MEMO_TEXT',            # 20
    'ENTITY_TP',            # 21
    'SUB_ID',               # 22
    'FILE_NUM',             # 23
    'TRAN_ID',              # 24
    'BACK_REF_TRAN_ID',     # 25
]


@asset(
    name="oppexp",
    description="Operating expenditures from oppexp.zip - WHERE committees spend money",
    group_name="fec",
    compute_kind="bulk_data",
    ins={"data_sync": AssetIn("data_sync")},
    metadata={
        "source": "oppexp.zip",
        "database": "fec_2024",
        "collection": "oppexp",
        "fields": len(OPPEXP_FIELDS),
    },
)
def oppexp_asset(
    context: AssetExecutionContext,
    mongo: MongoDBResource,
    data_sync: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """Parse oppexp.zip and load into MongoDB."""
    
    context.log.info("=" * 80)
    context.log.info("💸 PARSING OPERATING EXPENDITURES (oppexp.zip)")
    context.log.info("=" * 80)
    
    repo = get_repository()
    oppexp_path = repo.fec_oppexp_path(cycle="2024")
    
    context.log.info(f"📂 File: {oppexp_path}")
    context.log.info("")
    
    stats = {
        'records_processed': 0,
        'records_inserted': 0,
    }
    
    with mongo.get_client() as client:
        collection = mongo.get_collection(client, "oppexp", database_name="fec_2024")
        
        # Clear existing data
        collection.delete_many({})
        context.log.info("🗑️  Cleared existing data")
        context.log.info("")
        
        # Open ZIP and parse
        context.log.info("📖 Reading ZIP file...")
        with zipfile.ZipFile(oppexp_path, 'r') as zip_file:
            txt_files = [f for f in zip_file.namelist() if f.endswith('.txt')]
            
            if not txt_files:
                context.log.warning("⚠️  No .txt files found in ZIP")
                return Output(value=stats, metadata=stats)
            
            txt_file = txt_files[0]
            context.log.info(f"📄 Processing: {txt_file}")
            context.log.info("")
            
            batch = []
            batch_size = 5000
            
            with zip_file.open(txt_file) as f:
                text_data = io.TextIOWrapper(f, encoding='utf-8', errors='replace')
                
                for line in text_data:
                    line = line.strip()
                    if not line:
                        continue
                    
                    values = line.split('|')
                    
                    # Build document
                    doc = {}
                    for i, field in enumerate(OPPEXP_FIELDS):
                        value = values[i] if i < len(values) else ''
                        doc[field] = value.strip() if value else ''
                    
                    batch.append(doc)
                    stats['records_processed'] += 1
                    
                    # Insert batch
                    if len(batch) >= batch_size:
                        collection.insert_many(batch)
                        stats['records_inserted'] += len(batch)
                        context.log.info(f"   💾 Inserted {stats['records_inserted']:,} records...")
                        batch = []
                
                # Insert remaining
                if batch:
                    collection.insert_many(batch)
                    stats['records_inserted'] += len(batch)
        
        context.log.info("")
        context.log.info(f"✅ Total records: {stats['records_inserted']:,}")
        context.log.info("")
        
        # Create indexes
        context.log.info("📇 Creating indexes...")
        collection.create_index([("CMTE_ID", 1)])
        collection.create_index([("NAME", 1)])  # Payee
        collection.create_index([("PURPOSE", 1)])
        collection.create_index([("CATEGORY", 1)])
        collection.create_index([("TRANSACTION_AMT", -1)])
        collection.create_index([("TRANSACTION_DT", -1)])
        context.log.info("✅ Indexes created")
        context.log.info("")
    
    context.log.info("=" * 80)
    context.log.info("🎉 OPPEXP PARSING COMPLETE!")
    context.log.info("=" * 80)
    
    return Output(
        value=stats,
        metadata={
            "records": stats['records_inserted'],
            "database": "fec_2024",
            "collection": "oppexp",
        }
    )
