"""Independent pinned original-file audit, not a runtime filing parser."""

import copy
import hashlib
import json
import os
import xml.etree.ElementTree as ET
import zipfile
from collections import Counter
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from tests.test_funding_basis_contracts import validators
from tests.test_summary_assertion_corpus import go_json

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = os.environ.get("LT_REPORT_LINES_OUTPUT")
STORAGE = os.environ.get("LT_REPORT_LINES_STORAGE")
CASES = [
    ("C00843367", "1714573", "F3", 8),
    ("C00843367", "1766866", "F3", 56),
    ("C00843367", "1743911", "F3", 11),
    ("C00843367", "1780310", "F3", 6),
    ("C00843367", "1780346", "F3", 0),
    ("C00849901", "1730369", "F3", 13),
    ("C00849901", "1753173", "F3", 74),
]
REQUIRES_CORPUS = pytest.mark.skipif(not OUTPUT or not STORAGE, reason="requires bounded report outputs and retained originals")


def test_report_line_wire_guards():
    validator = validators()["report-lines.schema"]
    measures = {k: 0 for k in validators()["result.schema"].schema["$defs"]["measures"]["required"]}
    for name in ("signed_minor_units", "positive_minor_units", "negative_minor_units"):
        measures[name] = "0"
    result = {
        "schema_version": "legal-tender.fec.receipt-report-line-review.v1",
        "review_id": "a" * 64, "policy": "fec/reported-itemized-line-membership@1.0.0",
        "cycle": "2024", "committee_id": "C00000001", "file_num": "1",
        "inventory_calculation_id": "b" * 64,
        "receipt_input": {"fact_set_id": "c" * 64, "manifest_sha256": "d" * 64, "facts": 1, "shards": 1},
        "receipt_source": {"release_id": "fec-" + "e" * 64, "manifest_sha256": "f" * 64},
        "total": measures, "reviewed_nonmemo_line_population": measures,
        "groups": [], "transaction_identity_issues": [],
        "comparison_blockers": ["original_filing_membership_unverified", "report_period_and_account_coverage_unverified",
                                "effective_report_selection_unverified", "cycle_summary_compatibility_unverified", "no_rows_in_snapshot"],
        "comparison_ready": False, "terminal_attribution_eligible": False,
    }
    validator.validate(result)
    for key, value in (("comparison_ready", True), ("terminal_attribution_eligible", True), ("comparison_blockers", [])):
        bad = copy.deepcopy(result)
        bad[key] = value
        assert list(validator.iter_errors(bad))


def pinned_sources():
    return {name: sha for sha, name in (
        line.split() for line in (ROOT / "docs/audit/fixtures/summary-report-review-2026-09-10.sha256").read_text().splitlines()
    )}


def source_body(path, sha):
    assert path.stat().st_size <= 10 * 1024 * 1024
    body = path.read_bytes()
    assert hashlib.sha256(body).hexdigest() == sha
    return body


def cents(text):
    amount = Decimal(text) * 100
    assert amount.is_finite() and amount == amount.to_integral_value()
    return int(amount)


def measures(fields):
    amounts = [int(f["lt_receipt_amount_minor_units"]) for f in fields if f["lt_receipt_amount_minor_units"] is not None]
    return {
        "rows": len(fields), "known_amount_rows": len(amounts), "unknown_amount_rows": len(fields) - len(amounts),
        "positive_rows": sum(a > 0 for a in amounts), "negative_rows": sum(a < 0 for a in amounts),
        "zero_rows": amounts.count(0), "nonempty_conduit_id_rows": sum(bool(f["conduit_cmte_id"]) for f in fields),
        "signed_minor_units": str(sum(amounts)), "positive_minor_units": str(sum(a for a in amounts if a > 0)),
        "negative_minor_units": str(sum(a for a in amounts if a < 0)),
    }


@REQUIRES_CORPUS
def test_pinned_workbook_field_mapping():
    path = Path(STORAGE) / "dumps/audits/fec/summary-report-review/2026-09-10/attempt-01/FEC_EFO_Format_Specifications.xlsx"
    source_body(path, pinned_sources()[path.name])
    ns = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    with zipfile.ZipFile(path) as archive:
        ss = ["".join(t.text or "" for t in item.findall(".//s:t", ns))
              for item in ET.fromstring(archive.read("xl/sharedStrings.xml"))]
        for sheet, offsets in ((10, {16: "COVERAGE FROM DATE", 17: "COVERAGE THROUGH DATE", 33: "11(a i.) Individuals Itemized"}),
                               (19, {14: "COVERAGE FROM DATE", 15: "COVERAGE THROUGH DATE", 30: "11(a)i  Itemized"})):
            fields = {}
            for row in ET.fromstring(archive.read(f"xl/worksheets/sheet{sheet}.xml")).findall(".//s:row", ns):
                values = {}
                for cell in row.findall("s:c", ns):
                    value = cell.find("s:v", ns)
                    if value is not None:
                        values[cell.attrib["r"].rstrip("0123456789")] = ss[int(value.text)] if cell.attrib.get("t") == "s" else value.text
                if values.get("A", "").isdigit():
                    fields[int(values["A"])] = values.get("B")
            assert {offset: fields[offset] for offset in offsets} == offsets


@REQUIRES_CORPUS
@pytest.mark.parametrize("committee,file,form,row_count", CASES)
def test_complete_original_line_membership_and_cover(committee, file, form, row_count):
    report = json.loads((Path(OUTPUT) / f"{file}-report.json").read_text())
    result = json.loads((Path(OUTPUT) / f"{file}-lines.json").read_text())
    validators()["report.schema"].validate(report)
    validators()["report-lines.schema"].validate(result)
    review_id = result["review_id"]
    unhashed = copy.deepcopy(result)
    unhashed["review_id"] = ""
    assert hashlib.sha256(go_json(unhashed)).hexdigest() == review_id
    assert result["committee_id"] == report["committee_id"] == committee
    assert result["file_num"] == report["file_num"] == file
    assert result["cycle"] == report["cycle"] == "2024"
    assert result["receipt_input"] == report["input"]
    assert result["receipt_input"]["fact_set_id"] == "8f69a2ef40316f62d8844ecf732342a3b4535136b434f60677d0608cb04465df"
    assert result["receipt_source"] == {
        "release_id": "fec-a6e6f777c3c506b100a7709815bc89f2ae84749faf4343a4589f7ec1bac066a2",
        "manifest_sha256": "b921fda742759747b7e581c8897e8022ea5152eb33b86538ec80dbb9f13b5cad",
    }
    if committee == "C00843367":
        directory = Path(STORAGE) / "dumps/audits/fec/summary-report-review/2026-09-10/attempt-01"
        raw_sha = pinned_sources()[file + ".fec"]
    else:
        directory = Path(STORAGE) / "dumps/audits/fec/receipt-report-association/2026-09-08/2024"
        raw_sha = {
            "1730369": "0a12e970d484a19aa5a72e4eb38e1379cecbfb0e9c1b7a8db38aa05fdbfa66ea",
            "1753173": "8f82657d70261d1e472c666b49f95991e6ca021bfd3eddb6addb2873d6f72ddf",
        }[file]
    width, start, end, subtotal = (93, 16, 17, 33) if form == "F3" else (123, 14, 15, 30)
    body = source_body(directory / f"{file}.fec", raw_sha)
    # LF defines records; str.splitlines() would also split the 0x1c fields.
    physical = [line.rstrip(b"\r").split(b"\x1c") for line in body.split(b"\n") if line.rstrip(b"\r")]
    assert physical[0][:3] == [b"HDR", b"FEC", b"8.4"]
    cover = physical[1]
    assert len(cover) == width and cover[0] in [f"{form}{a}".encode() for a in "NAT"]
    assert cover[1].decode() == committee
    from_date, through_date = (date.fromisoformat(cover[i - 1].decode()) for i in (start, end))
    assert from_date <= through_date
    layout = json.loads((ROOT / "contracts/sources/fec/efile-format/v1/schedule-a-fields.json").read_text())
    index = {f["name"]: f["sequence"] - 1 for f in layout["fields"]}
    source = [row for row in physical if row[0].startswith(b"SA")]
    assert all(len(row) == 45 for row in source)
    by_id = {row[index["transaction_id"]].decode(): row for row in source}
    assert len(by_id) == len(source) == len(report["receipts"]) == row_count
    assert all(by_id) and set(by_id) == {r["fields"]["tran_id"] for r in report["receipts"]}
    by_ordinal = {r["source_row_ordinal"]: r["fields"] for r in report["receipts"]}
    assert len(by_ordinal) == row_count
    date_states = Counter()
    outside_period = []
    original_nonmemo = []
    for fields in by_ordinal.values():
        raw = by_id[fields["tran_id"]]
        for processed, original in (("cmte_id", "filer_committee_id_number"), ("memo_cd", "memo_code"),
                                    ("back_ref_tran_id", "back_reference_transaction_id_number"),
                                    ("back_ref_sched_nm", "back_reference_schedule_name"), ("entity_tp", "entity_type")):
            assert (fields[processed] or "") == raw[index[original]].decode()
        assert fields["filing_form"] == form
        assert raw[index["form_type"]].decode() == fields["schedule_type"] + fields["line_num"]
        assert cents(raw[index["contribution_amount"]].decode()) == int(fields["lt_receipt_amount_minor_units"])
        receipt_date = date.fromisoformat(raw[index["contribution_date"]].decode())
        assert fields["contb_receipt_dt"][:10] == receipt_date.isoformat()
        date_states["before" if receipt_date < from_date else "after" if receipt_date > through_date else "within"] += 1
        if not from_date <= receipt_date <= through_date:
            outside_period.append(fields)
        assert raw[index["memo_code"]] in (b"", b"X")
        if raw[0] == b"SA11AI" and raw[index["memo_code"]] == b"":
            original_nonmemo.append(fields)
    membership = [n for group in result["groups"] for n in group["source_row_ordinals"]]
    assert Counter(membership) == Counter(by_ordinal.keys())
    for group in result["groups"]:
        members = [by_ordinal[n] for n in group["source_row_ordinals"]]
        assert group["measures"] == measures(members)
        for fields in members:
            key = group["key"]
            for field, cell in (("filing_form", "filing_form"), ("schedule_type", "schedule_type"), ("line_num", "line_num"), ("memo_cd", "memo_code")):
                assert key[cell] == {"present": fields[field] is not None, "value": fields[field] or ""}
            assert key["publisher_individual_state"] == ("source_null" if fields["is_individual"] is None else str(fields["is_individual"]).lower())
            want = "outside_reviewed_form_line" if fields["line_num"] != "11AI" else "excluded_memo_subtotal" if fields["memo_cd"] == "X" else "reviewed_nonmemo_line"
            assert key["disposition"] == want
    assert result["total"] == measures(list(by_ordinal.values()))
    assert result["reviewed_nonmemo_line_population"] == measures(original_nonmemo)
    assert not result["transaction_identity_issues"]
    # These dates are in the originals too. Preserve the memo occurrences;
    # do not use receipt-date clipping to reconstruct physical report scope.
    assert date_states["before"] == 0
    assert date_states["after"] == {"1730369": 1, "1753173": 8}.get(file, 0)
    assert all(f["memo_cd"] == "X" and f["is_individual"] is False for f in outside_period)
    assert all(from_date <= date.fromisoformat(f["contb_receipt_dt"][:10]) <= through_date for f in original_nonmemo)
    detail = int(result["reviewed_nonmemo_line_population"]["signed_minor_units"])
    cover_amount = cents(cover[subtotal - 1].decode())
    summary = {"file": file, "form": form, "rows": row_count, "nonmemo_line_rows": len(original_nonmemo),
               "coverage_start": from_date.isoformat(), "coverage_end": through_date.isoformat(), "receipt_dates": dict(date_states),
               "detail_minor_units": str(detail), "cover_minor_units": str(cover_amount), "difference_minor_units": str(cover_amount - detail)}
    print(json.dumps(summary, sort_keys=True))
    assert detail == cover_amount
    assert not result["comparison_ready"] and not result["terminal_attribution_eligible"]
