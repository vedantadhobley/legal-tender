"""Opt-in independent source review; not a raw-filing ingestion pipeline."""

from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import re

import pytest

from tests.test_funding_basis_contracts import validators


AUDIT = os.environ.get("LT_RECEIPT_EVIDENCE_AUDIT")
pytestmark = pytest.mark.skipif(not AUDIT, reason="requires retained receipt-source review artifacts")
ROOT = Path(__file__).resolve().parents[1]


def load(name):
    return json.loads((Path(AUDIT) / name).read_text())


def test_complete_overlap_review_and_reported_source_routes():
    raw, review = load("overlaps.json"), load("overlaps-reviewed.json")
    validators()["component-review.schema"].validate(review)
    assert review["receipts"] == raw["receipts"]
    assert review["input"] == raw["input"]
    assert review["measures"] == raw["measures"]
    assert len(review["receipts"]) == len(review["decisions"]) == 738
    amounts = [int(row["fields"]["lt_receipt_amount_minor_units"]) for row in review["receipts"]]
    m = review["measures"]
    assert sum(amounts) == int(m["signed_minor_units"])
    assert sum(a for a in amounts if a > 0) == int(m["positive_minor_units"])
    assert sum(a for a in amounts if a < 0) == int(m["negative_minor_units"])
    explained = 0
    for row, decision in zip(review["receipts"], review["decisions"], strict=True):
        f = row["fields"]
        assert decision["source_row_ordinal"] == row["source_row_ordinal"]
        assert decision["source_route"] == "reported_committee_observation"
        assert decision["reported_source_committee_id"] == f["contbr_id"] == f["clean_contbr_id"]
        assert re.fullmatch(r"C[0-9]{8}", f["contbr_id"])
        assert decision["publisher_individual_overlap"] and f["is_individual"]
        assert not decision["terminal_attribution_eligible"]
        assert decision["additional_conduit_amount_minor_units"] == "0"
        assert decision["individual_entity_type_conflict"] == (f["entity_tp"] in ("IND", "CAN"))
        assert int(f["lt_receipt_amount_minor_units"]) <= 20000 and f["memo_cd"] is None
        documented_line = (
            f["filing_form"] in ("F3", "F3X") and f["line_num"] in ("11AI", "12")
        ) or (f["filing_form"] == "F3P" and f["line_num"] in ("17A", "17AI", "18"))
        explained += documented_line
        if not documented_line:
            assert f["filing_form"] == "F3X" and f["line_num"] == "17"
    assert explained == 697
    assert sum(d["individual_entity_type_conflict"] for d in review["decisions"]) == 7
    assert Counter(row["key"]["receipt_role"] for row in review["receipts"]) == {
        "affiliated_transfer_in": 720, "registered_filer_contribution": 18,
    }


def test_earmark_inspection_and_original_filing_evidence():
    inspected = load("earmarks-inspected.json")
    validators()["inspect-page.schema"].validate(inspected)
    assert len(inspected["decisions"]) == 3
    for decision in inspected["decisions"]:
        assert decision["source_route"] == "publisher_individual_identity_unresolved"
        assert decision["earmark_state"] == "reported_earmarked_receipt"
        assert decision["conduit_state"] == "earmark_conduit_unresolved"
        assert decision["reported_conduit_committee_id"] is None
        assert decision["report_reference_state"] == "no_report_reference"
        assert not decision["terminal_attribution_eligible"]
    body = (Path(AUDIT) / "1708331.fec").read_bytes()
    assert hashlib.sha256(body).hexdigest() == "c9814592585ab534f038649424d59222f254970d3af57dafcea95e149edb1e1b"
    records = [line.rstrip(b"\r").split(b"\x1c") for line in body.split(b"\n")]
    assert records[0][:3] == [b"HDR", b"FEC", b"8.4"]
    # Use pinned research field names, not copied numeric source positions.
    layout = json.loads((ROOT / "contracts/sources/fec/efile-format/v1/schedule-a-fields.json").read_text())
    index = {f["name"]: f["sequence"] - 1 for f in layout["fields"]}
    wanted = {r["fields"]["tran_id"]: r["fields"] for r in inspected["source_page"]["receipts"]}
    matched = []
    for row in records:
        if not row[0].startswith(b"SA"):
            continue
        assert len(row) == layout["logical_field_count"]
        transaction = row[index["transaction_id"]].decode("ascii")
        assert row[index["back_reference_transaction_id_number"]].decode("ascii") not in wanted
        if transaction not in wanted:
            continue
        f = wanted[transaction]
        assert f["file_num"] == "1708331"
        assert row[index["filer_committee_id_number"]].decode("ascii") == f["cmte_id"]
        assert row[index["contribution_amount"]].decode("ascii") == f["contb_receipt_amt"]
        assert row[index["memo_text_description"]].decode("ascii") == f["memo_text"]
        assert row[index["conduit_name"]] == b"" and f["conduit_cmte_nm"] is None
        assert row[index["back_reference_transaction_id_number"]] == b"" and f["back_ref_tran_id"] is None
        matched.append(transaction)
    assert len(matched) == len(set(matched)) == len(wanted)
