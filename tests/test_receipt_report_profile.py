"""Independent output conservation and source binding, not another runtime."""

import copy
import hashlib
import json
import os
from collections import defaultdict
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from referencing import Resource

from orchestration.go_process import _local_registry
from tests.test_summary_assertion_corpus import go_json

CONTRACTS = Path(__file__).resolve().parents[1] / "contracts"
SCHEMA = CONTRACTS / "audits/fec/receipt-report-profile/v1/result.schema.json"


def validator():
    basis = json.loads((CONTRACTS / "calculations/fec/committee-funding-basis/v1/result.schema.json").read_text())
    registry = _local_registry(CONTRACTS).with_resource(basis["$id"], Resource.from_contents(basis))
    return Draft202012Validator(json.loads(SCHEMA.read_text()), registry=registry, format_checker=FormatChecker())


def test_report_profile_schema():
    Draft202012Validator.check_schema(json.loads(SCHEMA.read_text()))


def add_measures(groups):
    keys = ("rows", "known_amount_rows", "unknown_amount_rows", "positive_rows", "negative_rows", "zero_rows", "signed_minor_units", "positive_minor_units", "negative_minor_units", "nonempty_conduit_id_rows")
    result = {k: sum(int(g["measures"][k]) for g in groups) for k in keys}
    return {k: str(v) if k.endswith("minor_units") else v for k, v in result.items()}


def check_conservation(result):
    forms, reports = result["form_line_decisions"], result["individual_report_groups"]
    assert add_measures(forms) == result["total_occurrences"]
    assert add_measures([g for g in forms if g["key"]["individual_decision"] == "included"]) == result["individual_predicate_occurrences"]
    assert add_measures(reports) == result["individual_predicate_occurrences"]
    by_form = defaultdict(list)
    for group in reports:
        key = tuple((group["key"][name]["present"], group["key"][name]["value"]) for name in ("filing_form", "schedule_type", "line_num"))
        by_form[key].append(group)
    for group in forms:
        if group["key"]["individual_decision"] != "included":
            continue
        key = tuple((group["key"][name]["present"], group["key"][name]["value"]) for name in ("filing_form", "schedule_type", "line_num"))
        assert add_measures(by_form.pop(key)) == group["measures"]
    assert not by_form
    for groups in (forms, reports):
        keys = [json.dumps(g["key"], sort_keys=True) for g in groups]
        assert len(set(keys)) == len(keys)
        for g in groups:
            m = g["measures"]
            assert m["rows"] == m["known_amount_rows"] + m["unknown_amount_rows"]
            assert m["known_amount_rows"] == m["positive_rows"] + m["negative_rows"] + m["zero_rows"]
            assert int(m["signed_minor_units"]) == int(m["positive_minor_units"]) + int(m["negative_minor_units"])
            assert int(m["positive_minor_units"]) >= 0 >= int(m["negative_minor_units"])
    for g in reports:
        assert 1 <= g["first_source_row_ordinal"] <= g["last_source_row_ordinal"] <= result["total_occurrences"]["rows"]
        assert sum(v for k, v in g["receipt_dates"].items() if k.endswith("_rows")) == g["measures"]["rows"]
        dates = g["receipt_dates"]
        assert bool(dates["first_observed_date"]) == bool(dates["last_observed_date"])
        if dates["first_observed_date"]:
            assert dates["first_observed_date"] <= dates["last_observed_date"]
    body = dict(result, profile_id="")
    assert hashlib.sha256(go_json(body)).hexdigest() == result["profile_id"]


def test_report_profile_synthetic_conservation():
    path = os.environ.get("LT_REPORT_PROFILE_FIXTURE")
    if not path:
        pytest.skip("requires Go synthetic fixture")
    result = json.loads(Path(path).read_text())
    check_conservation(result)
    assert result["total_occurrences"]["rows"] == 9
    assert result["individual_predicate_occurrences"]["rows"] == 5
    assert result["individual_predicate_occurrences"]["signed_minor_units"] == "1825"
    dates = [g["receipt_dates"] for g in result["individual_report_groups"]]
    assert sum(d["missing_rows"] for d in dates) == 2
    assert sum(d["invalid_rows"] for d in dates) == 1
    assert sum(d["before_cycle_rows"] for d in dates) == 1
    assert sum(d["after_cycle_rows"] for d in dates) == 1


def test_report_profile_real_binding_and_conservation():
    path, storage = os.environ.get("LT_REPORT_PROFILE_OUTPUT"), os.environ.get("LT_REPORT_PROFILE_STORAGE")
    if not path or not storage:
        pytest.skip("requires completed Go profile and read-only source storage")
    result = json.loads(Path(path).read_text())
    check = validator()
    check.validate(result)
    check_conservation(result)
    root = Path(storage)
    source = result["summary_input"]
    raw = (root / "releases/fec/manifests" / (source["source_release_id"] + ".json")).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == source["source_release_manifest_sha256"]
    release = json.loads(raw)
    selected = [s for s in release["staged_outputs"] if s["source_id"] == "fec:schedule-a:processed" and s["period"] == result["cycle"]]
    assert selected == [result["schedule_a_source"]]
    s, v = selected[0], result["verification"]
    assert v["complete"] and v["rows"] == v["valid_rows"] == s["row_count"] == result["total_occurrences"]["rows"]
    assert v["invalid_rows"] == 0
    assert v["expected_period"] == result["cycle"]
    for prefix in ("compressed", "uncompressed"):
        assert v[prefix + "_bytes"] == s[prefix + "_byte_count"]
        assert v[prefix + "_sha256"] == s[prefix + "_sha256"]
    assert {c["id"] for c in v["checks"]} == {"row_validity", "row_count", "uncompressed_byte_count", "uncompressed_sha256", "compressed_byte_count", "compressed_sha256"}
    assert all(c["passed"] and c["expected"] == c["actual"] for c in v["checks"])
    assertions = json.loads((root / "dumps/audits/fec/summary-assertions/2026-09-10/attempt-01" / (result["cycle"] + ".json")).read_text())
    assert source == assertions["input"]
    assert result["summary_calculation_id"] == assertions["calculation_id"]
    for field in ("comparison_ready", "terminal_attribution_eligible"):
        bad = copy.deepcopy(result)
        bad[field] = True
        assert list(check.iter_errors(bad))
    bad = copy.deepcopy(result)
    bad["delta_minor_units"] = "0"
    assert list(check.iter_errors(bad))
    bad = copy.deepcopy(result)
    bad["not_established"] = []
    assert list(check.iter_errors(bad))
