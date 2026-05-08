"""PAC Summary Asset — parse FEC webk.zip files.

webk is FEC's per-PAC aggregation: TTL_RECEIPTS, INDV_CONTRIB,
OTHER_POL_CMTE_CONTRIB, IND_EXP, etc. for every committee that filed.
27 fields per record. _key is CMTE_ID.

Used to validate our committee_receipts enrichment + cross-check the
trace algorithm's per-committee receipt divisor.
"""

from typing import Dict, Any, List
from datetime import datetime
import zipfile

from dagster import asset, AssetExecutionContext, MetadataValue, Output, Config, AssetIn

from src.data import get_repository
from src.resources.arango import ArangoDBResource
from src.utils.arango_dump import should_restore_from_dump, create_collection_dump, restore_collection_from_dump
from src.utils.fec_schema import FECSchema


_NUMERIC_FIELDS = {
    "TTL_RECEIPTS", "TRANS_FROM_AFF", "INDV_CONTRIB", "OTHER_POL_CMTE_CONTRIB",
    "CAND_CONTRIB", "CAND_LOANS", "TTL_LOANS_RECEIVED", "TTL_DISB",
    "TRANF_TO_AFF", "INDV_REFUNDS", "OTHER_POL_CMTE_REFUNDS",
    "CAND_LOAN_REPAY", "LOAN_REPAY", "COH_BOP", "COH_COP", "DEBTS_OWED_BY",
    "NONFED_TRANS_RECEIVED", "CONTRIB_TO_OTHER_CMTE", "IND_EXP",
    "PTY_COORD_EXP", "NONFED_SHARE_EXP",
}


class WebkConfig(Config):
    cycles: List[str] = ["2020", "2022", "2024", "2026"]
    force_refresh: bool = False


@asset(
    name="webk",
    description="FEC PAC summary file (webk.zip) — per-committee aggregated totals from FEC",
    group_name="fec",
    compute_kind="bulk_data",
    ins={"data_sync": AssetIn("data_sync")},
)
def webk_asset(
    context: AssetExecutionContext,
    config: WebkConfig,
    arango: ArangoDBResource,
    data_sync: Dict[str, Any],
) -> Output[Dict[str, Any]]:
    """Parse webk.zip files into fec_{cycle}.webk collections."""

    repo = get_repository()
    stats = {"total_records": 0, "by_cycle": {}}

    with arango.get_client() as client:
        for cycle in config.cycles:
            context.log.info(f"📊 {cycle} Cycle:")
            try:
                db = arango.get_database(client, f"fec_{cycle}")
                collection = arango.get_collection(db, "webk")
                zip_path = repo.fec_webk_path(cycle)

                if not zip_path.exists():
                    context.log.warning(f"⚠️  File not found: {zip_path}")
                    continue

                if not config.force_refresh and should_restore_from_dump("webk", zip_path, "fec", cycle):
                    context.log.info("   🚀 Restoring from dump...")
                    result = restore_collection_from_dump(
                        db=db, collection_name="webk", dump_type="fec",
                        cycle=cycle, context=context,
                    )
                    if result:
                        record_count = result["record_count"]
                        stats["by_cycle"][cycle] = record_count
                        stats["total_records"] += record_count
                        continue

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
                            record = schema.parse_line("webk", decoded)
                            if not record or not record.get("CMTE_ID"):
                                continue
                            record["_key"] = record["CMTE_ID"]
                            record["updated_at"] = datetime.now().isoformat()
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
                        f"   ✅ {cycle}: {len(batch):,} PAC summaries "
                        f"(created: {result['created']}, updated: {result['updated']})"
                    )
                    stats["by_cycle"][cycle] = len(batch)
                    stats["total_records"] += len(batch)

                    collection.add_persistent_index(fields=["CMTE_ID"], unique=True)
                    collection.add_persistent_index(fields=["CMTE_TP"])

                    create_collection_dump(
                        db=db, collection_name="webk", source_file=zip_path,
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
            "arangodb_collection": "webk",
        },
    )
