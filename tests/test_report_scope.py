"""Independent pinned-workbook and retained-output checks; no HTTP or OCR."""
import base64
import hashlib
import json
import os
from decimal import Decimal
from pathlib import Path

import pytest

from tests.test_receipt_report_metadata import workbook_fields

ROOT = Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "contracts/calculations/fec/report-scope/v1/policy.json"
AUDIT = os.environ.get("LT_REPORT_SCOPE_AUDIT")
REPORTS = os.environ.get("LT_REPORT_SCOPE_REPORTS")
SCHEMAS = os.environ.get("LT_REPORT_SCOPE_SCHEMAS")
REQUIRES_AUDIT = pytest.mark.skipif(not all((AUDIT, REPORTS, SCHEMAS)), reason="requires retained report-scope audit; no network")


def test_report_scope_contract_guards():
    contract = json.loads(CONTRACT.read_text())
    assert contract["guards"] == {"original_image_verified": False, "history_complete": False, "financial_selection_ready": False}
    assert set(contract["dispositions"]) == {"financial_cover_present", "supplemental_attachment_shape", "unresolved"}
    assert contract["f3x"]["fields"] == 125
    assert contract["paper_header"]["original_id_sequence"] == 6


@REQUIRES_AUDIT
def test_all_qualified_fields_against_exact_paper_workbook():
    contract = json.loads(CONTRACT.read_text())
    body = (Path(SCHEMAS) / "paper-v34.xlsx").read_bytes()
    assert hashlib.sha256(body).hexdigest() == contract["source_evidence"]["sha256"]
    f3x = workbook_fields(body, "F3X")
    assert sorted(f3x) == list(range(1, 126))
    amount_positions = [seq for seq, values in f3x.items() if "AMT-12" in values]
    assert amount_positions == [seq for lo, hi in contract["f3x"]["amount_ranges_inclusive"] for seq in range(lo, hi + 1)]
    for key, label in {"committee_id": "FILER FEC COMMITTEE ID", "report_code": "REPORT CODE", "coverage_start": "COVERAGE FROM DATE", "coverage_end": "COVERAGE THROUGH DATE"}.items():
        assert label in f3x[contract["f3x"][key]]
    assert "NOT USED" in workbook_fields(body, "HDR")[6]
    sc1 = workbook_fields(body, "Sch C1")
    assert sorted(sc1) == list(range(1, 48))
    assert "AMT OF LOAN" in sc1[9] and "NAME LENDER" in sc1[3]


@REQUIRES_AUDIT
@pytest.mark.parametrize("file,name,disposition", [
    ("1876290", "1876290.fec", "supplemental_attachment_shape"),
    ("1882886", "1882886.fec", "supplemental_attachment_shape"),
    ("1813890", "1813890.fec", "financial_cover_present"),
    ("1833804", "1833804-prefix.fec", "unresolved"),
])
def test_retained_scope_outputs(file, name, disposition):
    source = (Path(REPORTS) / name).read_bytes()
    a = json.loads((Path(AUDIT) / f"{file}-scope.json").read_text(), parse_float=Decimal)
    assert a["disposition"] == disposition
    assert a["body"]["sha256"] == hashlib.sha256(source).hexdigest()
    assert a["body"]["bytes"] == len(source)
    assert not any(a[k] for k in ("original_image_verified", "history_complete", "financial_selection_ready"))
    rebuilt = b""
    for ordinal, r in enumerate(a["records"], 1):
        raw = base64.b64decode(r["raw_base64"], validate=True)
        assert r["ordinal"] == ordinal and r["offset"] == len(rebuilt) and r["bytes"] == len(raw)
        assert r["sha256"] == hashlib.sha256(raw).hexdigest()
        rebuilt += raw
    assert rebuilt == source
    assert all(row["record"]["file_number"] == file for row in a["metadata"])
    assert sum(i["matching_rows"] for i in a["metadata_inputs"]) == len(a["metadata"])
    for d in a["metadata_differences"]:
        assert d["values"] == [a["metadata"][i]["record"]["raw"][d["field"]] for i in d["assertion_indexes"]]
    if file == "1833804":
        assert a["cover"] is None and a["capture_extent"] == "prefix" and a["publisher_bytes"] == 19020241
        assert not a["records"][-1]["complete"]
        assert a["issues"] == ["partial_document_capture", "unsupported_header_layout"]
        return
    assert a["capture_extent"] == "complete_response" and a["issues"] == []
    cover = source.split(b"\n")[1].rstrip(b"\r").split(b"\x1c")
    assert a["cover"]["fields"] == [f.decode("ascii") for f in cover]
    ranges = json.loads(CONTRACT.read_text())["f3x"]["amount_ranges_inclusive"]
    assert [f["sequence"] for f in a["cover"]["amounts"]] == [seq for lo, hi in ranges for seq in range(lo, hi + 1)]
    for f in a["cover"]["amounts"]:
        assert f["raw"] == cover[f["sequence"] - 1].decode("ascii")
        if f["raw"]:
            assert f["state"] == "valid"
            assert Decimal(f["raw"]) * 100 == int(f["minor_units"])
        else:
            assert f["state"] == "blank" and "minor_units" not in f
    if file in ("1876290", "1882886"):
        assert all(f["state"] == "blank" for f in a["cover"]["amounts"])
        assert len(a["records"]) == 3
        reports = next(m["record"]["raw"] for m in a["metadata"] if m["endpoint"] == "/v1/reports/pac-party/")
        assert all(reports[f] == 0 for f in ("cash_on_hand_beginning_period", "total_receipts_period", "total_disbursements_period", "cash_on_hand_end_period"))
