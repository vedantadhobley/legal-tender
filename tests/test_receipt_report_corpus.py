"""Opt-in original-file comparison; audit only, not a second receipt pipeline."""

from collections import Counter
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path

import pytest

from tests.test_funding_basis_contracts import validators


AUDIT = os.environ.get("LT_RECEIPT_REPORT_AUDIT")
pytestmark = pytest.mark.skipif(not AUDIT, reason="requires retained receipt-report audits")
ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("file,artifact,raw_sha,rows,links", [
    ("1730369", "report-1730369-final.json", "0a12e970d484a19aa5a72e4eb38e1379cecbfb0e9c1b7a8db38aa05fdbfa66ea", 13, 2),
    ("1753173", "report-1753173.json", "8f82657d70261d1e472c666b49f95991e6ca021bfd3eddb6addb2873d6f72ddf", 74, 29),
])
def test_report_complete_original_membership_and_exact_associations(file, artifact, raw_sha, rows, links):
    report = json.loads((Path(AUDIT) / artifact).read_text())
    validators()["report.schema"].validate(report)
    assert report["committee_id"] == "C00849901" and report["file_num"] == file
    assert report["cycle"] == "2024" and report["scope"] == "published_schedule_a_cycle_report"
    assert report["input"]["fact_set_id"] == "8f69a2ef40316f62d8844ecf732342a3b4535136b434f60677d0608cb04465df"
    assert report["inventory_calculation_id"] == "e31d7e8248ad6594c6ce23f86a0fe0cd2ff0ac13a0e78d079c97562519416985"
    body = (Path(AUDIT) / f"{file}.fec").read_bytes()
    assert hashlib.sha256(body).hexdigest() == raw_sha
    physical = [line.rstrip(b"\r").split(b"\x1c") for line in body.split(b"\n")]
    assert physical[0][:3] == [b"HDR", b"FEC", b"8.4"]
    source = [row for row in physical if row[0].startswith(b"SA")]
    layout = json.loads((ROOT / "contracts/sources/fec/efile-format/v1/schedule-a-fields.json").read_text())
    index = {f["name"]: f["sequence"] - 1 for f in layout["fields"]}
    assert all(len(row) == layout["logical_field_count"] for row in source)
    by_id = {row[index["transaction_id"]].decode("ascii"): row for row in source}
    assert len(by_id) == len(source) == len(report["receipts"]) == rows
    assert set(by_id) == {r["fields"]["tran_id"] for r in report["receipts"]}
    by_ordinal = {r["source_row_ordinal"]: r["fields"] for r in report["receipts"]}
    mapping = {
        "cmte_id": "filer_committee_id_number", "tran_id": "transaction_id",
        "back_ref_tran_id": "back_reference_transaction_id_number",
        "back_ref_sched_nm": "back_reference_schedule_name", "entity_tp": "entity_type",
        "memo_cd": "memo_code",
    }
    for f in by_ordinal.values():
        raw = by_id[f["tran_id"]]
        for processed, original in mapping.items():
            assert (f[processed] or "") == raw[index[original]].decode("ascii")
        assert Decimal(raw[index["contribution_amount"]].decode("ascii")) * 100 == int(f["lt_receipt_amount_minor_units"])
        assert raw[index["contribution_date"]].decode("ascii") == f["contb_receipt_dt"][:10].replace("-", "")
        assert raw[index["form_type"]].decode("ascii") == f["schedule_type"] + f["line_num"]
    assert len(report["references"]) == rows
    assert Counter(ref["state"] for ref in report["references"]) == {
        "exact_same_report_reference": links, "no_report_reference": rows - links,
    }
    assert len(report["associations"]) == links
    for association in report["associations"]:
        assert association["state"] == "reported_earmark_memo_association"
        original = by_ordinal[association["earmark_source_row_ordinal"]]
        memo = by_ordinal[association["related_source_row_ordinals"][0]]
        assert original["receipt_tp"] in ("15E", "30E", "31E", "32E")
        assert not original["lt_memoed_subtotal"] and memo["lt_memoed_subtotal"]
        assert memo["back_ref_tran_id"] == original["tran_id"]
        assert memo["back_ref_sched_nm"] == original["schedule_type"] + original["line_num"]
        assert association["reported_conduit_committee_id"] == memo["contbr_id"] == memo["clean_contbr_id"]
        assert by_id[memo["tran_id"]][index["donor_committee_fec_id"]].decode("ascii") == memo["contbr_id"]
        same = original["lt_receipt_amount_minor_units"] == memo["lt_receipt_amount_minor_units"]
        assert association["related_amount_comparison"] == ("same_reported_amount" if same else "different_reported_amount")
        assert association["additional_amount_minor_units"] == "0" and not association["terminal_attribution_eligible"]
    assert sum(a["related_amount_comparison"] == "different_reported_amount" for a in report["associations"]) == (1 if file == "1730369" else 0)


def test_report_replay():
    for file, artifact in (("1730369", "report-1730369-final.json"), ("1753173", "report-1753173.json")):
        assert (Path(AUDIT) / artifact).read_bytes() == (Path(AUDIT) / f"report-{file}-replay.json").read_bytes()
