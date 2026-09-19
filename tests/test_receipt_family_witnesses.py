"""Complete original F3X witness; independent of runtime financial selection."""
import base64
import hashlib
import json
import os
from collections import Counter, defaultdict
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from tests.test_report_field_binding import minor, workbook_rows

ROOT = Path(__file__).resolve().parents[1]
AUDIT = os.environ.get("LT_FAMILY_WITNESS_AUDIT")
STORAGE = os.environ.get("LT_FAMILY_WITNESS_STORAGE")
REQUIRES = pytest.mark.skipif(not AUDIT or not STORAGE, reason="requires retained complete F3X witness")


def read(path):
    return json.loads(path.read_bytes(), parse_float=Decimal)


def pins():
    return {name: digest for digest, name in (line.split() for line in (ROOT / "docs/audit/fixtures/receipt-family-witnesses-2026-09-11.sha256").read_text().splitlines())}


def test_witness_pins_are_complete_and_not_runtime_defaults():
    assert set(pins()) == {"1690269.fec", "1690269.fec.headers"}
    policy = read(ROOT / "contracts/calculations/fec/receipt-family-comparison/v1/policy.json")
    assert not any(policy["guards"].values())
    assert "1690269" not in json.dumps(policy)


@pytest.fixture(scope="module")
def witness():
    path = Path(AUDIT)
    for name, pin in pins().items():
        assert hashlib.sha256((path / name).read_bytes()).hexdigest() == pin
    body = (path / "1690269.fec").read_bytes()
    assert len(body) == 824322 and body.endswith(b"\n")
    r = read(path / "nrcc-january.json")
    # The file's record separator is LF, not Unicode/ASCII control separators.
    records = [line.removesuffix(b"\n").removesuffix(b"\r").split(b"\x1c") for line in body.split(b"\n")[:-1]]
    return r, body, records


@REQUIRES
def test_transport_complete_original_and_observed_chain(witness):
    r, body, records = witness
    review = r["family_reports"]
    assert len(review["bindings"]) == 1
    b = review["bindings"][0]
    d = b["document"]
    assert d["capture_extent"] == "complete_response" and d["representation"] == "electronic_8.4"
    assert d["disposition"] == "electronic_cover_parsed" and not d["issues"]
    assert b["scope_bound"] and not b["scope_blockers"]
    assert b["observation_index"] in review["membership"]["chain_candidate_indexes"]
    obs = review["membership"]["observations"][b["observation_index"]]
    assert obs["file_number"] == "1690269" and obs["report_form"] == "Form 3X"
    assert obs["report_type"] == "M2" and obs["report_year"] == 2023
    assert obs["period"] == {"start":"2023-01-01", "end":"2023-01-31"}
    assert obs["window_relation"] == "inside"
    raw = [base64.b64decode(row["raw_base64"]) for row in d["records"]]
    assert all(row["complete"] for row in d["records"])
    assert b"".join(raw) == body and len(raw) == len(records) == 3321

    offset = 0
    for source, row in zip(raw, d["records"], strict=True):
        assert row["offset"] == offset
        assert row["bytes"] == len(source) and row["sha256"] == hashlib.sha256(source).hexdigest()
        offset += len(source)
    assert records[0][:3] == [b"HDR", b"FEC", b"8.4"]
    assert records[1][0:2] == [b"F3XN", b"C00075820"] and len(records[1]) == 123
    assert sum(row[0].startswith(b"F3X") for row in records) == 1
    assert records[0][5:7] == [b"", b""]
    source = next(row["raw"] for p in review["membership"]["evidence"]["pages"] for row in p["records"] if row["file_number"] == "1690269")
    assert source["amendment_chain"] == ["1690269"] and source["amendment_indicator"] == "N" and not source["is_amended"]


@REQUIRES
def test_all_schedule_a_occurrences_match_saved_profile_grain(witness):
    r, _, records = witness
    layout = read(ROOT / "contracts/sources/fec/efile-format/v1/schedule-a-fields.json")
    index = {f["name"]:f["sequence"]-1 for f in layout["fields"]}
    workbook = Path(STORAGE) / "dumps/audits/fec/report-field-binding/2026-09-10/attempt-01/FEC_Format_v8.4.xlsx"
    assert hashlib.sha256(workbook.read_bytes()).hexdigest() == "9d3775d73e9398144b0e0267415ba53e1b5c6a326110b327ce2cd58d233bf3d6"
    sheet = workbook_rows(workbook, ("Sch A",))["Sch A"]
    for field in layout["fields"]:
        if field["name"] in ("form_type", "filer_committee_id_number", "transaction_id", "contribution_date", "contribution_amount", "memo_code"):
            assert sheet[field["sequence"]]["B"].strip() == field["publisher_name"]
    rows = [row for row in records if row[0].startswith(b"SA")]
    assert len(rows) == 2595 and all(len(row) == 45 for row in rows)
    ids = Counter(row[index["transaction_id"]] for row in rows)
    assert len(ids) == len(rows) and b"" not in ids
    assert all(row[index["filer_committee_id_number"]] == b"C00075820" for row in rows)
    originals = defaultdict(list)
    for row in rows:
        originals[(row[0].decode(), row[index["memo_code"]].decode())].append(row)
    groups = r["reports"][0]["groups"]
    profile_body = (Path(STORAGE) / "dumps/audits/fec/receipt-report-profile-v2/2026-09-10/attempt-01/profile.json").read_bytes()
    assert hashlib.sha256(profile_body).hexdigest() == "acce0a13f2d87abfe78beb66c6bd2ae60f82a53bf656b3745343966ae57f3647"
    profile = json.loads(profile_body)
    assert groups == [g for g in profile["report_line_groups"] if g["key"]["committee"] == {"present":True,"value":"C00075820"} and g["key"]["file_num"] == {"present":True,"value":"1690269"}]
    assert r["profile_id"] == profile["profile_id"] and r["schedule_a_source"] == profile["schedule_a_source"]
    grouped = defaultdict(list)
    for group in groups:
        k = group["key"]
        assert k["committee"]["value"] == "C00075820" and k["file_num"]["value"] == "1690269"
        assert k["filing_form"]["value"] == "F3X" and k["report_type"]["value"] == "M2" and k["report_year"]["value"] == "2023"
        grouped[(k["schedule_type"]["value"]+k["line_num"]["value"], k["memo_code"]["value"])].append(group)
    assert set(originals) == set(grouped)
    for key, original in originals.items():
        gs = grouped[key]
        values = [int(minor(row[index["contribution_amount"]].decode())) for row in original]
        expected = {"rows":len(values), "known_amount_rows":len(values), "unknown_amount_rows":0,
                    "positive_rows":sum(v>0 for v in values), "negative_rows":sum(v<0 for v in values), "zero_rows":values.count(0),
                    "signed_minor_units":sum(values), "positive_minor_units":sum(v for v in values if v>0), "negative_minor_units":sum(v for v in values if v<0)}
        for field, value in expected.items():
            assert sum(int(g["measures"][field]) for g in gs) == value
        dates = [date.fromisoformat(row[index["contribution_date"]].decode()) for row in original]
        # Original membership is not date-clipped to recreate a cover subtotal.
        assert min(dates).isoformat() == min(g["receipt_dates"]["first_observed_date"] for g in gs)
        assert max(dates).isoformat() == max(g["receipt_dates"]["last_observed_date"] for g in gs)
        start, end = date(int(r["reported"]["cycle"])-1, 1, 1), date(int(r["reported"]["cycle"]), 12, 31)
        for field, count in {"missing_rows":0, "invalid_rows":0, "before_cycle_rows":sum(d<start for d in dates), "in_cycle_rows":sum(start<=d<=end for d in dates), "after_cycle_rows":sum(d>end for d in dates)}.items():
            assert sum(g["receipt_dates"][field] for g in gs) == count
    assert r["reports"][0]["total_occurrences"]["negative_rows"] == 1
    assert r["reports"][0]["total_occurrences"]["negative_minor_units"] == "-46907"
    assert sum(g["receipt_dates"]["before_cycle_rows"] for g in groups) == 62
    # The profile has no per-row transaction IDs: uniqueness above is original-only.
    assert r["unique_transaction_membership_proven"] is False


@REQUIRES
def test_positive_families_cover_metadata_detail_and_empty_fields(witness):
    r, _, records = witness
    mapping = read(ROOT / "contracts/calculations/fec/receipt-families/v1/contract.json")
    workbook = Path(STORAGE) / "dumps/audits/fec/report-field-binding/2026-09-10/attempt-01/FEC_Format_v8.4.xlsx"
    assert hashlib.sha256(workbook.read_bytes()).hexdigest() == mapping["sources"]["format84"]["sha256"]
    fields = workbook_rows(workbook)["F3X"]
    expected = {"other_committee_contributions": (67, "173100000"), "affiliated_or_party_transfers": (18, "54848772")}
    for f in r["reports"][0]["families"]:
        field = f["field"]
        spec = next(v for v in mapping["forms"]["F3X"]["leaves"] if v["id"] == field["id"])
        assert spec["sequence"] == field["sequence"] and fields[spec["sequence"]]["B"] == spec["label"]
        assert f["binding"]["reported_value_bound"]
        assert f["reported_minor_units"] == minor(records[1][spec["sequence"]-1].decode()) == minor(f["binding"]["metadata"]["raw"])
        if field["id"] in expected:
            count, amount = expected[field["id"]]
            assert f["nonmemo_occurrences"]["rows"] == count
            assert f["state"] == "equal" and not f["blockers"]
            assert f["detail_minor_units"] == f["reported_minor_units"] == amount and f["delta_minor_units"] == "0"
            if field["id"] == "affiliated_or_party_transfers":
                memo = [r["reports"][0]["groups"][i] for i in f["group_indexes"] if r["reports"][0]["groups"][i]["key"]["memo_code"]["value"] == "X"]
                assert sum(g["measures"]["rows"] for g in memo) == 61
                assert sum(g["receipt_dates"]["before_cycle_rows"] for g in memo) == 56
                assert sum(int(g["measures"]["signed_minor_units"]) for g in memo) == 66065000
        else:
            assert f["reported_minor_units"] == "0" and not f["group_indexes"]
            assert f["detail_minor_units"] is None and f["delta_minor_units"] is None
            assert f["blockers"] == ["no_nonmemo_family_detail"]
    assert not any(r[k] for k in read(ROOT / "contracts/calculations/fec/receipt-family-comparison/v1/policy.json")["guards"])


@REQUIRES
def test_unchanged_seven_fields_and_summary_scope_limits(witness):
    r, _, _ = witness
    assert len(r["reported"]["window"]["fields"]) == 7
    assert all(f["reported_window_ready"] for f in r["reported"]["window"]["fields"])
    assert r["reported"]["financial_use_eligible"] is False
    assert r["family_window_comparison_ready"] is False
    for assertion in r["reported"]["comparisons"]:
        for f in assertion["fields"]:
            if f["summary_field"] not in ("COH_BOP",):
                assert not f["reported_comparison_ready"]
