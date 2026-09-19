"""Independent all-occurrence conservation and exact regrouping to the v1 audit."""

import copy
import hashlib
import json
import os
import re
from collections import defaultdict
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from referencing import Resource

from orchestration.go_process import _local_registry
from tests.test_receipt_report_profile import CONTRACTS, add_measures
from tests.test_summary_assertion_corpus import go_json

SCHEMA = CONTRACTS / "audits/fec/receipt-report-profile/v2/result.schema.json"
FORM_CELLS = ("filing_form", "schedule_type", "line_num")
REPORT_CELLS = ("committee", "file_num", *FORM_CELLS, "report_type", "report_year")
AXES = (*FORM_CELLS, "memo_code", "publisher_individual_state", "disposition", "individual_decision")


def validator():
    basis = json.loads((CONTRACTS / "calculations/fec/committee-funding-basis/v1/result.schema.json").read_text())
    registry = _local_registry(CONTRACTS).with_resource(basis["$id"], Resource.from_contents(basis))
    return Draft202012Validator(json.loads(SCHEMA.read_text()), registry=registry, format_checker=FormatChecker())


def test_v2_schema_and_guards():
    schema = json.loads(SCHEMA.read_text())
    Draft202012Validator.check_schema(schema)
    for name in ("comparison_ready", "terminal_attribution_eligible"):
        check = Draft202012Validator(schema["properties"][name])
        check.validate(False)
        assert list(check.iter_errors(True))
    assert list(Draft202012Validator(schema["properties"]["not_established"]).iter_errors([]))


def key_tuple(key, names):
    return tuple((key[n]["present"], key[n]["value"]) if isinstance(key[n], dict) else key[n] for n in names)


def check_profile(result):
    forms, reports = result["form_line_groups"], result["report_line_groups"]
    assert add_measures(forms) == add_measures(reports) == result["total_occurrences"]
    assert add_measures([g for g in forms if g["key"]["individual_decision"] == "included"]) == result["individual_predicate_occurrences"]
    assert add_measures([g for g in forms if g["key"]["disposition"] == "reviewed_nonmemo_line"]) == result["reviewed_nonmemo_line_occurrences"]
    by_form = defaultdict(list)
    for g in reports:
        by_form[key_tuple(g["key"], AXES)].append(g)
        d, m = g["receipt_dates"], g["measures"]
        assert sum(v for k, v in d.items() if k.endswith("_rows")) == m["rows"]
        assert 1 <= g["first_source_row_ordinal"] <= g["last_source_row_ordinal"] <= result["total_occurrences"]["rows"]
        assert bool(d["first_observed_date"]) == bool(d["last_observed_date"]) == bool(d["before_cycle_rows"] + d["in_cycle_rows"] + d["after_cycle_rows"])
        assert d["first_observed_date"] <= d["last_observed_date"]
    for g in forms:
        assert add_measures(by_form.pop(key_tuple(g["key"], AXES))) == g["measures"]
    assert not by_form
    for groups, names in ((forms, AXES), (reports, (*REPORT_CELLS, *AXES[3:]))):
        keys = [key_tuple(g["key"], names) for g in groups]
        assert len(keys) == len(set(keys))
        for g in groups:
            k, m = g["key"], g["measures"]
            assert m["rows"] == m["known_amount_rows"] + m["unknown_amount_rows"]
            assert m["known_amount_rows"] == m["positive_rows"] + m["negative_rows"] + m["zero_rows"]
            assert int(m["signed_minor_units"]) == int(m["positive_minor_units"]) + int(m["negative_minor_units"])
            assert int(m["positive_minor_units"]) >= 0 >= int(m["negative_minor_units"])
            memo, individual = k["memo_code"]["value"], k["publisher_individual_state"]
            # This legacy predicate checks classification before memo status.
            decision = "unresolved_individual_class" if individual == "source_null" else "excluded_non_individual" if individual == "false" else "excluded_memo_subtotal" if memo == "X" else "unresolved_amount" if m["unknown_amount_rows"] else "included"
            assert k["individual_decision"] == decision
            form, schedule, line = (k[n]["value"] for n in FORM_CELLS)
            disposition = "outside_reviewed_form_line" if form not in ("F3", "F3X") or schedule != "SA" or line != "11AI" else "excluded_memo_subtotal" if memo == "X" else "unresolved_memo_code" if memo else "unresolved_line_amount" if m["unknown_amount_rows"] else "reviewed_nonmemo_line"
            assert k["disposition"] == disposition
    assert hashlib.sha256(go_json(dict(result, profile_id=""))).hexdigest() == result["profile_id"]


def test_v2_fixture():
    path = os.environ.get("LT_REPORT_PROFILE_V2_FIXTURE")
    if not path:
        pytest.skip("requires Go synthetic profile")
    result = json.loads(Path(path).read_text())
    check_profile(result)
    assert result["total_occurrences"]["rows"] == 12
    assert result["reviewed_nonmemo_line_occurrences"]["rows"] == 5
    assert result["reviewed_nonmemo_line_occurrences"]["signed_minor_units"] == "2850"
    # Synthetic input metadata is intentionally absent; validate the group wire.
    check = validator()
    for name in ("form_line_groups", "report_line_groups"):
        check.evolve(schema=check.schema["properties"][name]).validate(result[name])
    for name in ("form_line_groups", "report_line_groups"):
        bad = copy.deepcopy(result[name][0])
        bad["key"]["unexpected_field"] = "lost grain"
        assert list(check.evolve(schema=check.schema["properties"][name]["items"]).iter_errors(bad))


def compare_v1(result, old):
    for name in ("summary_input", "summary_calculation_id", "schedule_a_source", "verification", "total_occurrences", "individual_predicate_occurrences"):
        assert result[name] == old[name]
    forms, reports = defaultdict(list), defaultdict(list)
    for g in result["form_line_groups"]:
        forms[key_tuple(g["key"], (*FORM_CELLS, "individual_decision"))].append(g)
    for g in old["form_line_decisions"]:
        assert add_measures(forms.pop(key_tuple(g["key"], (*FORM_CELLS, "individual_decision")))) == g["measures"]
    assert not forms
    for g in result["report_line_groups"]:
        if g["key"]["individual_decision"] == "included":
            reports[key_tuple(g["key"], REPORT_CELLS)].append(g)
    for g in old["individual_report_groups"]:
        members = reports.pop(key_tuple(g["key"], REPORT_CELLS))
        assert add_measures(members) == g["measures"]
        assert min(m["first_source_row_ordinal"] for m in members) == g["first_source_row_ordinal"]
        assert max(m["last_source_row_ordinal"] for m in members) == g["last_source_row_ordinal"]
        for name, value in g["receipt_dates"].items():
            values = [m["receipt_dates"][name] for m in members]
            if name.endswith("_rows"):
                assert sum(values) == value
            elif name == "first_observed_date":
                assert min((v for v in values if v), default="") == value
            else:
                assert max(values) == value
    assert not reports


def test_v2_real_source_binding_and_complete_v1_regrouping():
    path, storage = os.environ.get("LT_REPORT_PROFILE_V2_OUTPUT"), os.environ.get("LT_REPORT_PROFILE_STORAGE")
    if not path or not storage:
        pytest.skip("requires complete profile and retained read-only source data")
    result = json.loads(Path(path).read_text())
    validator().validate(result)
    check_profile(result)
    root = Path(storage)
    raw = (root / "releases/fec/manifests" / (result["summary_input"]["source_release_id"] + ".json")).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == result["summary_input"]["source_release_manifest_sha256"]
    release = json.loads(raw)
    assert [s for s in release["staged_outputs"] if s["source_id"] == "fec:schedule-a:processed" and s["period"] == result["cycle"]] == [result["schedule_a_source"]]
    old_body = (root / "dumps/audits/fec/receipt-report-profile/2026-09-10/attempt-01/profile.json").read_bytes()
    assert hashlib.sha256(old_body).hexdigest() == "18e37311c32a8ba65c422ba0d7e1f897a0df5b373fc101b206a910bd577a91c8"
    old = json.loads(old_body)
    compare_v1(result, old)
    assert not result["comparison_ready"] and not result["terminal_attribution_eligible"]
    dispositions = defaultdict(int)
    for g in result["form_line_groups"]:
        dispositions[g["key"]["disposition"]] += g["measures"]["rows"]
    dates = {name: sum(g["receipt_dates"][name] for g in result["report_line_groups"])
             for name in ("missing_rows", "invalid_rows", "before_cycle_rows", "in_cycle_rows", "after_cycle_rows")}
    invalid_reference_rows = 0
    for g in result["report_line_groups"]:
        k = g["key"]
        valid_committee = re.fullmatch(r"C[0-9]{8}", k["committee"]["value"])
        valid_file = re.fullmatch(r"[1-9][0-9]*", k["file_num"]["value"]) and int(k["file_num"]["value"]) <= 2**64 - 1
        if not valid_committee or not valid_file:
            invalid_reference_rows += g["measures"]["rows"]
    summary = {
        "profile_id": result["profile_id"], "rows": result["total_occurrences"]["rows"],
        "form_groups": len(result["form_line_groups"]), "report_groups": len(result["report_line_groups"]),
        "committee_file_pairs": len({key_tuple(g["key"], ("committee", "file_num")) for g in result["report_line_groups"]}),
        "reported_committees": len({key_tuple(g["key"], ("committee",)) for g in result["report_line_groups"]}),
        "invalid_report_reference_rows": invalid_reference_rows, "line_disposition_rows": dict(dispositions),
        "all_occurrence_date_states": dates,
        "nonmemo_reviewed_false_individual_rows": sum(g["measures"]["rows"] for g in result["form_line_groups"]
                                                     if g["key"]["disposition"] == "reviewed_nonmemo_line" and g["key"]["publisher_individual_state"] == "false"),
        "v1_regrouping": "exact",
    }
    print(json.dumps(summary, sort_keys=True))
