"""Candidate Summary Asset — parse FEC weball.zip files.

weball is FEC's own per-candidate aggregation: TTL_RECEIPTS, TTL_INDIV_CONTRIB,
OTHER_POL_CMTE_CONTRIB, etc. for each candidate's principal campaign committee
in a given cycle. Used as ground-truth validation for our computed
funding_channels totals.

30 fields per record. _key is CAND_ID.
"""

from typing import Dict, Any, List
from datetime import datetime
import zipfile

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config, AssetIn

from src.data import get_repository
from src.resources.arango import ArangoDBResource
from src.utils.arango_dump import should_restore_from_dump, create_collection_dump, restore_collection_from_dump
from src.utils.fec_schema import FECSchema
from src.config import ACTIVE_CYCLES


# Numeric fields per the weball schema (positions 5-17, 24-26, 28-29 in raw record)
_NUMERIC_FIELDS = {
    "TTL_RECEIPTS", "TRANS_FROM_AUTH", "TTL_DISB", "TRANS_TO_AUTH",
    "COH_BOP", "COH_COP", "CAND_CONTRIB", "CAND_LOANS", "OTHER_LOANS",
    "CAND_LOAN_REPAY", "OTHER_LOAN_REPAY", "DEBTS_OWED_BY", "TTL_INDIV_CONTRIB",
    "GEN_ELECTION_PRECENT", "OTHER_POL_CMTE_CONTRIB", "POL_PTY_CONTRIB",
    "INDIV_REFUNDS", "CMTE_REFUNDS",
}


class WeballConfig(Config):
    cycles: List[str] = list(ACTIVE_CYCLES)
    force_refresh: bool = False


@asset(
    name="weball",
    description="FEC candidate summary file (weball.zip) — per-candidate aggregated totals from FEC",
    group_name="fec",
    compute_kind="bulk_data",
    ins={"data_sync": AssetIn("data_sync")},
)
def weball_asset(
    context: AssetExecutionContext,
    config: WeballConfig,
    arango: ArangoDBResource,
    data_sync: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """Parse weball.zip files into fec_{cycle}.weball collections."""

    repo = get_repository()
    stats = {"total_records": 0, "by_cycle": {}}

    with arango.get_client() as client:
        for cycle in config.cycles:
            context.log.info(f"📊 {cycle} Cycle:")

            try:
                db = arango.get_database(client, f"fec_{cycle}")
                collection = arango.get_collection(db, "weball")
                zip_path = repo.fec_weball_path(cycle)

                if not zip_path.exists():
                    context.log.warning(f"⚠️  File not found: {zip_path}")
                    continue

                if not config.force_refresh and should_restore_from_dump("weball", zip_path, "fec", cycle):
                    context.log.info("   🚀 Restoring from dump...")
                    result = restore_collection_from_dump(
                        db=db, collection_name="weball", dump_type="fec",
                        cycle=cycle, context=context,
                    )
                    if result:
                        record_count = result["record_count"]
                        stats["by_cycle"][cycle] = record_count
                        stats["total_records"] += record_count
                        continue
                    else:
                        context.log.warning("   ⚠️ Restore failed, falling back to parsing...")

                context.log.info(f"   📂 Parsing {zip_path.name}...")
                collection.truncate()

                batch = []
                schema = FECSchema()
                with zipfile.ZipFile(zip_path) as zf:
                    txt_files = [f for f in zf.namelist() if f.endswith(".txt")]
                    if not txt_files:
                        continue
                    with zf.open(txt_files[0]) as f:
                        for line in f:
                            decoded = line.decode("utf-8", errors="ignore").strip()
                            if not decoded:
                                continue
                            record = schema.parse_line("weball", decoded)
                            if not record or not record.get("CAND_ID"):
                                continue

                            record["_key"] = record["CAND_ID"]
                            record["updated_at"] = datetime.now().isoformat()

                            # Coerce numeric fields (FEC delivers them as strings)
                            for field in _NUMERIC_FIELDS:
                                v = record.get(field)
                                if v in (None, "", "0"):
                                    record[field] = float(v) if v == "0" else None
                                else:
                                    try:
                                        record[field] = float(v)
                                    except (ValueError, TypeError):
                                        record[field] = None

                            batch.append(record)

                if batch:
                    result = arango.bulk_import(collection, batch, on_duplicate="replace")
                    context.log.info(
                        f"   ✅ {cycle}: {len(batch):,} candidate summaries "
                        f"(created: {result['created']}, updated: {result['updated']})"
                    )
                    stats["by_cycle"][cycle] = len(batch)
                    stats["total_records"] += len(batch)

                    collection.add_persistent_index(fields=["CAND_ID"], unique=True)
                    collection.add_persistent_index(fields=["CAND_OFFICE_ST", "CAND_OFFICE_DISTRICT"])
                    collection.add_persistent_index(fields=["CAND_PTY_AFFILIATION"])

                    create_collection_dump(
                        db=db, collection_name="weball", source_file=zip_path,
                        dump_type="fec", cycle=cycle, context=context,
                    )

            except Exception as e:
                context.log.error(f"   ❌ Error processing {cycle}: {e}")
                import traceback
                context.log.error(traceback.format_exc())

    return Output(
        value=stats,
        metadata={
            "total_records": stats["total_records"],
            "cycles_processed": MetadataValue.json(config.cycles),
            "arangodb_databases": MetadataValue.json([f"fec_{c}" for c in config.cycles]),
            "arangodb_collection": "weball",
        },
    )
