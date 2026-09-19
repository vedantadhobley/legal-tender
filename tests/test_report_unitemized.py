"""Independent retained-source checks for explicit unitemized observations."""
import base64
import hashlib
import json
import os
from decimal import Decimal
from pathlib import Path

import pytest

from tests.test_receipt_report_metadata import workbook_fields

ROOT = Path(__file__).resolve().parents[1]
POLICY = ROOT / "contracts/calculations/fec/report-unitemized/v1/policy.json"
AUDIT = os.environ.get("LT_REPORT_UNITEMIZED_AUDIT")
REPORTS = os.environ.get("LT_REPORT_UNITEMIZED_REPORTS")
SCHEMAS = os.environ.get("LT_REPORT_UNITEMIZED_SCHEMAS")
REQUIRES_AUDIT = pytest.mark.skipif(not all((AUDIT, REPORTS, SCHEMAS)), reason="requires retained unitemized gate; no network")


def test_policy_explicit_value_and_guards():
    p = json.loads(POLICY.read_text())
    assert p["amount_method"] == "explicit_reported_field_only"
    assert p["donor_composition"] == "not_identified_by_summary"
    assert p["paper"]["period_positions"] == {"itemized": 29, "unitemized": 30, "total": 31}
    assert p["metadata"]["endpoint"] == "/v1/reports/pac-party/"
    assert not any(p["guards"].values())


@REQUIRES_AUDIT
def test_pinned_workbook_mapping():
    p = json.loads(POLICY.read_text())
    body = (Path(SCHEMAS) / "paper-v34.xlsx").read_bytes()
    assert hashlib.sha256(body).hexdigest() == p["paper"]["schema_sha256"]
    fields = workbook_fields(body, "F3X")
    for sequence, line, label in ((29, "11 (a) (i)", "Itemized"), (30, "11 (a) (ii)", "Unitemized"), (31, "11 (a) (iii)", "Total")):
        assert fields[sequence][:3] == [line, label, "AMT-12"]
        assert fields[sequence + 50][:3] == [line, label, "AMT-12"]
    swagger = (Path(SCHEMAS) / "swagger.json").read_bytes()
    assert hashlib.sha256(swagger).hexdigest() == p["metadata"]["schema_sha256"]


def check_summary(summary, source, paper=False):
    for kind in ("itemized", "unitemized", "total"):
        field = summary[kind]
        raw = source[field["sequence"] - 1] if paper else source[field["field"]]
        assert field["raw"] == raw
        assert type(field["raw"]) is type(raw)
        if raw is None:
            assert field["state"] == "source_null" and field["minor_units"] is None
        elif raw == "":
            assert field["state"] == "blank" and field["minor_units"] is None
        else:
            assert field["state"] == "valid"
            assert Decimal(str(raw)) * 100 == int(field["minor_units"])
    if all(summary[k]["state"] == "valid" for k in ("itemized", "unitemized", "total")):
        delta = int(summary["total"]["minor_units"]) - int(summary["itemized"]["minor_units"]) - int(summary["unitemized"]["minor_units"])
        assert str(delta) == summary["subtotal_delta_minor_units"]
        assert summary["subtotal_state"] == ("balanced" if delta == 0 else "mismatch")
    else:
        assert summary["subtotal_state"] == "unavailable" and summary["subtotal_delta_minor_units"] is None


@REQUIRES_AUDIT
@pytest.mark.parametrize("file,name", [("1813890", "1813890.fec"), ("1876290", "1876290.fec"), ("1882886", "1882886.fec"), ("1833804", "1833804-prefix.fec")])
def test_retained_observations(file, name):
    a = json.loads((Path(AUDIT) / f"{file}-unitemized.json").read_text(), parse_float=Decimal)
    e = a["evidence"]
    source = (Path(REPORTS) / name).read_bytes()
    assert hashlib.sha256(source).hexdigest() == e["body"]["sha256"]
    assert b"".join(base64.b64decode(r["raw_base64"], validate=True) for r in e["records"]) == source
    assert not any(a[k] for k in ("financial_component_eligible", "cycle_comparison_ready", "terminal_attribution_eligible"))
    assert not e["financial_selection_ready"] and not e["history_complete"] and not e["original_image_verified"]
    assert a["amount_method"] == "explicit_reported_field_only" and a["donor_composition"] == "not_identified_by_summary"
    assert len(a["metadata"]) == len(e["metadata"])
    if a["cover"] is not None:
        check_summary(a["cover"], e["cover"]["fields"], paper=True)
    for i, m in enumerate(a["metadata"]):
        assert m["metadata_index"] == i
        assertion = e["metadata"][i]
        raw = assertion["record"]["raw"]
        assert raw["file_number"] == int(file)
        # No retained witness has both a qualified paper amount and a supplied
        # matching PAC report amount; numeric pairs are covered by Go fixtures.
        assert m["comparison"]["blockers"] and not m["comparison"]["comparable"] and m["comparison"]["delta_minor_units"] is None
        if assertion["endpoint"] == "/v1/filings/":
            assert m["availability"] == "not_supplied_by_endpoint" and m["summary"] is None
            assert "individual_unitemized_contributions_period" not in raw
        else:
            assert assertion["endpoint"] == "/v1/reports/pac-party/" and m["availability"] == "supplied"
            check_summary(m["summary"], raw)
            assert m["period"] == {"start": raw["coverage_start_date"].removesuffix("T00:00:00"), "end": raw["coverage_end_date"].removesuffix("T00:00:00")}
    if file == "1833804":
        assert a["cover"] is None and a["cover_period"] is None
        pac = next(m for m in a["metadata"] if m["availability"] == "supplied")
        assert pac["period"] == {"start": "2024-09-01", "end": "2024-09-30"}
        assert pac["summary"]["unitemized"]["raw"] == "2256923.61"
        assert pac["summary"]["unitemized"]["minor_units"] == "225692361"
        assert pac["summary"]["subtotal_state"] == "balanced"
    elif file == "1813890":
        assert a["cover"]["unitemized"]["raw"] == "0.00" and a["cover"]["unitemized"]["minor_units"] == "0"
        assert a["cover"]["subtotal_delta_minor_units"] == "-69833400"
        assert a["cover_period"] == {"start": "2024-04-01", "end": "2024-06-30"}
    else:
        assert a["cover"]["unitemized"]["state"] == "blank" and a["cover"]["unitemized"]["minor_units"] is None
