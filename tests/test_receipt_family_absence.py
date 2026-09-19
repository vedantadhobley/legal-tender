"""Independent raw-record checks for additive reported-zero evidence only."""
import base64
import hashlib
import json
import os
import re
from collections import Counter, defaultdict
from pathlib import Path

import pytest

from tests.test_report_field_binding import minor, workbook_rows

ROOT = Path(__file__).resolve().parents[1]
AUDIT = os.environ.get("LT_FAMILY_ABSENCE_AUDIT")
STORAGE = os.environ.get("LT_FAMILY_ABSENCE_STORAGE")
POLICY = ROOT / "contracts/calculations/fec/receipt-family-absence/v1/policy.json"
CASES = ("sid-window", "sid-cycle", "sid-missing-cover", "nrcc-unresolved", "nrcc-january")
REQUIRES = pytest.mark.skipif(not AUDIT or not STORAGE, reason="requires retained absence gate")


def read(path):
    return json.loads(path.read_bytes())


def test_absence_policy_is_separate_from_detail_and_financial_selection():
    p = read(POLICY)
    old = read(ROOT / "contracts/calculations/fec/receipt-family-comparison/v1/policy.json")
    assert p["version"] == "legal-tender.fec.receipt-family-absence.v1"
    assert p["families"] == old["families"] and p["upstream_comparison_version"] == old["version"]
    assert not any(p["guards"].values())
    assert "no_nonmemo_family_detail_blocks" in old["empty_detail"]
    assert "absent_detail_stays_null" in p["preservation"]
    assert "memo_only_zero_amount_or_positive_negative_cancellation_population" in p["blocked"]
    assert "original_and_profile_all_schedule_a_line_counts_agree_for_this_report" in p["required"]
    assert not re.search(r'C\d{8}|1690269|1714573|2024', json.dumps(p))


@REQUIRES
def test_census_layout_widths_and_filer_positions_against_workbook():
    p = read(POLICY)
    workbook = Path(STORAGE) / "dumps/audits/fec/report-field-binding/2026-09-10/attempt-01/FEC_Format_v8.4.xlsx"
    assert hashlib.sha256(workbook.read_bytes()).hexdigest() == p["schema_sha256"]
    layouts = [p["census"]["schedule_a"], *p["census"]["other_records"]]
    sheets = workbook_rows(workbook, tuple(layout["sheet"] for layout in layouts))
    for layout in layouts:
        rows = sheets[layout["sheet"]]
        assert max(rows) == layout["fields"]
        assert rows[2]["B"].strip() == "FILER COMMITTEE ID NUMBER"
        assert rows[1]["B"].strip() == ("REC TYPE" if layout["sheet"] == "Text" else "FORM TYPE")
        if "tag_pattern" in layout:
            assert re.fullmatch(layout["tag_pattern"], rows[1]["E"].strip())


@REQUIRES
@pytest.mark.parametrize("name", CASES)
def test_retained_zero_evidence_against_complete_original(name):
    r = read(Path(AUDIT) / f"{name}.json")
    p = read(POLICY)
    base = Path(STORAGE) / "dumps/audits/fec"
    prior = base / ("receipt-family-witnesses" if name == "nrcc-january" else "receipt-family-comparison") / "2026-09-11/attempt-01" / f"{name}.json"
    assert r["compared"] == read(prior)
    assert r["version"] == p["version"] and not any(r[k] for k in p["guards"])
    old = r["compared"]
    assert len(r["reports"]) == len(old["reports"])
    for review, report in zip(r["reports"], old["reports"], strict=True):
        assert old["reports"][review["report_index"]] == report
        binding = old["family_reports"]["bindings"][report["binding_index"]]
        d = binding["document"]
        census = review["original_census"]
        assert census["version"] == p["census_version"]
        assert census["body"] == d["body"] and census["headers"] == d["headers"]
        raw = [base64.b64decode(row["raw_base64"]) for row in d["records"]]
        assert hashlib.sha256(b"".join(raw)).hexdigest() == census["body"]["sha256"]
        assert b"".join(raw) == Path(census["body"]["path"]).read_bytes()
        rows = [line.removesuffix(b"\n").removesuffix(b"\r").split(b"\x1c") for line in raw]
        profile = Counter()
        for g in report["groups"]:
            k = g["key"]
            if k["schedule_type"] == {"present": True, "value": "SA"}:
                profile[k["line_num"]["value"]] += g["measures"]["rows"]
        original, other = defaultdict(list), defaultdict(list)
        for ordinal, row in enumerate(rows[2:], 3):
            source = d["records"][ordinal - 1]
            payload = raw[ordinal - 1].removesuffix(b"\n").removesuffix(b"\r")
            if not source["complete"] or len(row) < 2 or any(v != 0x1c and not 32 <= v <= 126 for v in payload):
                assert {"record_ordinal": ordinal, "code": "unreadable_record_structure"} in census["issues"]
                continue
            tag = row[0].decode("ascii")
            (original if tag.startswith("SA") else other)[tag].append(ordinal)
        assert {line["tag"]: line["record_ordinals"] for line in census["schedule_a_lines"]} == dict(original)
        assert {line["tag"]: line["record_ordinals"] for line in census["other_records"]} == dict(other)
        if census["layout_census_complete"]:
            assert d["capture_extent"] == "complete_response" and not d["issues"]
            assert all(row["complete"] for row in d["records"])
            assert not census["issues"] and not census["scope_issues"]
            assert census["schema_sha256"] == p["schema_sha256"]
            for row in rows[2:]:
                assert row[1] == rows[1][1]
                tag = row[0].decode()
                if tag.startswith("SA"):
                    assert len(row) == p["census"]["schedule_a"]["fields"]
                else:
                    layout = next(v for v in p["census"]["other_records"] if re.fullmatch(v["tag_pattern"], tag))
                    assert len(row) == layout["fields"]
        match = census["layout_census_complete"] and profile == {line[2:]: len(ordinals) for line, ordinals in original.items()}
        assert review["original_profile_line_counts_match"] == match
        for f, previous in zip(review["families"], report["families"], strict=True):
            field = previous["field"]
            assert (f["family_id"], f["line"]) == (field["id"], field["line"])
            assert f["profile_rows"] == profile[f["line"]]
            assert f["original_observed_rows"] == len(original.get("SA" + f["line"], []))
            zero = binding["scope_bound"] and match and previous["reported_minor_units"] == "0" and not profile[f["line"]] and not original.get("SA" + f["line"])
            assert f["state"] == ("qualified_reported_zero" if zero else "blocked")
            if zero:
                assert not f["blockers"] and f["qualified_reported_zero_minor_units"] == "0"
                assert minor(rows[1][field["sequence"] - 1].decode()) == "0"
                assert previous["binding"]["reported_value_bound"]
                assert minor(previous["binding"]["metadata"]["raw"]) == "0"
                assert previous["detail_minor_units"] is None and previous["state"] == "blocked"
            else:
                assert f["blockers"] and f["qualified_reported_zero_minor_units"] is None


@REQUIRES
def test_retained_scope_limits_and_positive_families_are_preserved():
    expected = {"sid-window": (23, 7), "sid-cycle": (23, 7), "sid-missing-cover": (19, 6), "nrcc-unresolved": (0, 4), "nrcc-january": (2, 2)}
    for name, (qualified, blocked) in expected.items():
        r = read(Path(AUDIT) / f"{name}.json")
        states = Counter(f["state"] for report in r["reports"] for f in report["families"])
        assert states["qualified_reported_zero"] == qualified and states["blocked"] == blocked
        assert not r["family_window_comparison_ready"]
        for review, old in zip(r["reports"], r["compared"]["reports"], strict=True):
            if old["file_number"] == "1766839":
                assert all(f["state"] == "blocked" for f in review["families"])
            for f, previous in zip(review["families"], old["families"], strict=True):
                if previous["state"] == "equal":
                    assert f["state"] == "blocked" and previous["detail_minor_units"] != "0"
