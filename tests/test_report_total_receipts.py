"""Independent field mapping, pair scope, and exact amount checks; no network."""
import base64
import hashlib
import json
import os
from decimal import Decimal
from pathlib import Path

import pytest

from tests.test_receipt_report_metadata import workbook_fields

ROOT = Path(__file__).resolve().parents[1]
POLICY = ROOT / "contracts/calculations/fec/report-total-receipts/v1/policy.json"
AUDIT = os.environ.get("LT_REPORT_FIELD_AUDIT")
REPORTS = os.environ.get("LT_REPORT_FIELD_REPORTS")
SCHEMAS = os.environ.get("LT_REPORT_FIELD_SCHEMAS")
REQUIRES_AUDIT = pytest.mark.skipif(not all((AUDIT, REPORTS, SCHEMAS)), reason="requires retained field gate; no network")


def test_policy_scope_and_guards():
    p = json.loads(POLICY.read_text())
    assert p["use"] == "same_file_reported_value_comparison"
    assert p["paper"]["cover_sequences"] == [22, 44]
    assert not any(p["guards"].values())
    assert p["metadata"]["fields_by_endpoint"] == {"/v1/filings/": "total_receipts", "/v1/reports/pac-party/": "total_receipts_period"}


@REQUIRES_AUDIT
def test_exact_workbook_field_and_column():
    p = json.loads(POLICY.read_text())
    b = (Path(SCHEMAS) / "paper-v34.xlsx").read_bytes()
    assert hashlib.sha256(b).hexdigest() == p["paper"]["schema_sha256"]
    fields = workbook_fields(b, "F3X")
    for sequence, line in ((22, "6 (c)"), (44, "19")):
        assert fields[sequence][:3] == [line, "Total Receipts", "AMT-12"]
    assert "Total Federal Receipts" in fields[45]  # Not the same field.
    assert fields[75][0] == "6 (c)" and fields[94][0] == "19"  # Separate YTD positions.


@REQUIRES_AUDIT
@pytest.mark.parametrize("file,name", [("1813890", "1813890.fec"), ("1876290", "1876290.fec"), ("1882886", "1882886.fec"), ("1833804", "1833804-prefix.fec")])
def test_retained_field_comparisons(file, name):
    a = json.loads((Path(AUDIT) / f"{file}-receipts.json").read_text(), parse_float=Decimal)
    e = a["evidence"]
    source = (Path(REPORTS) / name).read_bytes()
    assert hashlib.sha256(source).hexdigest() == e["body"]["sha256"]
    assert b"".join(base64.b64decode(r["raw_base64"], validate=True) for r in e["records"]) == source
    assert not any(a[k] for k in ("financial_component_eligible", "cycle_comparison_ready", "terminal_attribution_eligible"))
    assert not e["financial_selection_ready"] and not e["history_complete"] and not e["original_image_verified"]
    assert len(a["comparisons"]) == len(e["metadata"])
    for i, c in enumerate(a["comparisons"]):
        assert c["metadata_index"] == i
        record = e["metadata"][i]["record"]["raw"]
        assert record["file_number"] == int(file)
        assert c["raw"] == record[c["field"]]
        if c["minor_units"] is not None:
            assert Decimal(str(c["raw"])) * 100 == int(c["minor_units"])
        if file != "1813890":
            assert c["blockers"] and not c["comparable"] and c["delta_minor_units"] is None
        else:
            assert c["comparable"] and c["blockers"] == []
            assert record["committee_id"] == e["cover"]["committee_id"]
            assert record["coverage_start_date"] == a["period"]["start"]
            assert record["coverage_end_date"] == a["period"]["end"]
            assert int(c["delta_minor_units"]) == int(c["minor_units"]) - int(a["cover_fields"][0]["minor_units"]) == 0
    if file == "1813890":
        assert a["period"] == {"start": "2024-04-01", "end": "2024-06-30"}
        assert [(f["sequence"], f["minor_units"]) for f in a["cover_fields"]] == [(22, "69907"), (44, "69907")]
        # Neither the conflicting individual subtotal nor the unrelated YTD
        # transcription inconsistency is silently repaired or substituted.
        cover = e["cover"]["fields"]
        assert cover[28] == "699033.00" and cover[30] == "699.00"
        assert cover[74] == "69910.00" and cover[93] == "699.10"
    elif file == "1833804":
        assert a["period"] is None and a["cover_fields"] == [] and a["cover_blockers"] == ["unqualified_cover_layout"]
    else:
        assert a["cover_blockers"] == ["cover_total_receipts:blank"]
        assert all(f["raw"] == "" and f["state"] == "blank" for f in a["cover_fields"])
