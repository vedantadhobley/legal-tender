"""Independent pinned-source checks, not a runtime financial calculation."""
import hashlib
import json
import os
from collections import defaultdict
from pathlib import Path

import pytest

from tests.test_receipt_report_lines import measures

ROOT = Path(__file__).resolve().parents[1]
POLICY = ROOT / "contracts/calculations/fec/receipt-reported-window/v1/policy.json"
AUDIT = os.environ.get("LT_RECEIPT_WINDOW_AUDIT")
PROFILE = os.environ.get("LT_RECEIPT_WINDOW_PROFILE")
PRIOR = os.environ.get("LT_RECEIPT_WINDOW_PRIOR")
LINES = os.environ.get("LT_RECEIPT_WINDOW_LINES")
REQUIRES = pytest.mark.skipif(not all((AUDIT, PROFILE, PRIOR, LINES)), reason="requires retained receipt-window evidence")


def read(path):
    return json.loads(path.read_bytes())


@pytest.fixture(scope="module")
def profile():
    raw = Path(PROFILE).read_bytes()
    assert len(raw) == 266269624
    assert hashlib.sha256(raw).hexdigest() == "acce0a13f2d87abfe78beb66c6bd2ae60f82a53bf656b3745343966ae57f3647"
    return json.loads(raw)


def test_policy_is_occurrence_comparison_not_financial_selection():
    p = read(POLICY)
    assert not any(p["guards"].values())
    assert p["summary_field"] == "INDV_ITEM_CONTB"
    assert p["reported_field"] == "individual_itemized_contributions_period"
    assert "explicit_bound_zero" in p["empty_value"]
    assert "without_date_clipping_or_individual_flag_filtering" in p["amounts"]


@REQUIRES
@pytest.mark.parametrize("name", ["sid-window", "sid-cycle", "sid-missing-cover", "nrcc-unresolved"])
def test_complete_selected_profile_membership_and_exact_comparisons(profile, name):
    r = read(Path(AUDIT) / f"{name}.json")
    prior = read(Path(PRIOR) / f"{name}.json")
    assert r["reported"] == prior
    assert r["profile_id"] == profile["profile_id"]
    assert r["schedule_a_source"] == profile["schedule_a_source"]
    assert prior["summary_input"] == profile["summary_input"]
    assert prior["summary_calculation_id"] == profile["summary_calculation_id"]
    assert not any(r[k] for k in read(POLICY)["guards"])
    window = prior["window"]
    field = next(f for f in window["fields"] if f["name"] == read(POLICY)["reported_field"])
    assert [d["binding_index"] for d in r["reports"]] == field["member_binding_indexes"]
    counts = 0
    amount = 0
    for d in r["reports"]:
        binding = window["bindings"][d["binding_index"]]
        assert binding["scope_bound"]
        assert d["file_number"] == binding["document"]["file_number"]
        observation = window["membership"]["observations"][binding["observation_index"]]
        assert binding["observation_index"] in window["membership"]["chain_candidate_indexes"]
        groups = [g for g in profile["report_line_groups"] if g["key"]["committee"] == {"present": True, "value": prior["committee_id"]} and g["key"]["file_num"] == {"present": True, "value": d["file_number"]}]
        assert d["groups"] == groups
        assert sum(g["measures"]["rows"] for g in groups) == d["total_occurrences"]["rows"]
        reviewed = [g for g in groups if g["key"]["disposition"] == "reviewed_nonmemo_line"]
        assert sum(g["measures"]["rows"] for g in reviewed) == d["reviewed_nonmemo_line_occurrences"]["rows"]
        assert sum(int(g["measures"]["signed_minor_units"]) for g in reviewed) == int(d["reviewed_nonmemo_line_occurrences"]["signed_minor_units"])
        counts += d["reviewed_nonmemo_line_occurrences"]["rows"]
        amount += int(d["reviewed_nonmemo_line_occurrences"]["signed_minor_units"])
        for g in groups:
            assert g["key"]["report_type"]["value"] == observation["report_type"]
            assert int(g["key"]["report_year"]["value"]) == observation["report_year"]
        if d["state"] != "blocked":
            assert int(d["delta_minor_units"]) == int(d["reported_minor_units"]) - int(d["detail_minor_units"])
        else:
            assert d["detail_minor_units"] is None and d["delta_minor_units"] is None and d["blockers"]
    assert counts == r["reviewed_nonmemo_line_occurrences"]["rows"]
    assert amount == int(r["reviewed_nonmemo_line_occurrences"]["signed_minor_units"])
    if name == "sid-window":
        assert r["state"] == "equal" and r["occurrence_comparison_ready"]
        assert r["detail_minor_units"] == r["reported_minor_units"] == "11431500"
        assert r["delta_minor_units"] == "0" and counts == 54 and len(r["reports"]) == 5
    else:
        assert r["state"] == "blocked" and not r["occurrence_comparison_ready"]
        assert r["detail_minor_units"] is None and r["delta_minor_units"] is None and r["blockers"]
    for assertion, comparison in zip(prior["comparisons"], r["summary_comparisons"], strict=True):
        assert comparison["assertion_id"] == assertion["assertion_id"]
        assert comparison["summary_field"] == "INDV_ITEM_CONTB"
        assert comparison["reported_comparison_ready"] == (name == "sid-window")
        if comparison["reported_comparison_ready"]:
            assert comparison["state"] == "equal"
            assert int(comparison["delta_minor_units"]) == int(comparison["summary_minor_units"]) - int(comparison["window_minor_units"]) == 0
        else:
            assert comparison["delta_minor_units"] is None


@REQUIRES
def test_old_complete_report_rows_corroborate_v4_groups_without_relabeling(profile):
    r = read(Path(AUDIT) / "sid-window.json")
    assert sum(d["total_occurrences"]["rows"] for d in r["reports"]) == 81
    for d in r["reports"]:
        report = read(Path(LINES) / f'{d["file_number"]}-report.json')
        assert report["input"]["fact_set_id"] == "8f69a2ef40316f62d8844ecf732342a3b4535136b434f60677d0608cb04465df"
        old_lines = read(Path(LINES) / f'{d["file_number"]}-lines.json')
        assert old_lines["receipt_source"]["release_id"] != profile["summary_input"]["source_release_id"]
        grouped = defaultdict(list)
        for row in report["receipts"]:
            f = row["fields"]
            key = {}
            for raw, cell in (("cmte_id", "committee"), ("file_num", "file_num"), ("filing_form", "filing_form"), ("schedule_type", "schedule_type"), ("line_num", "line_num"), ("memo_cd", "memo_code"), ("rpt_tp", "report_type"), ("rpt_yr", "report_year")):
                key[cell] = {"present": f[raw] is not None, "value": f[raw] or ""}
            key["publisher_individual_state"] = "source_null" if f["is_individual"] is None else str(f["is_individual"]).lower()
            key["individual_decision"] = row["key"]["individual_decision"]
            key["disposition"] = "outside_reviewed_form_line" if f["line_num"] != "11AI" else "excluded_memo_subtotal" if f["memo_cd"] == "X" else "reviewed_nonmemo_line"
            grouped[json.dumps(key, sort_keys=True)].append(f)
        assert len(grouped) == len(d["groups"])
        for group in d["groups"]:
            fields = grouped.pop(json.dumps(group["key"], sort_keys=True))
            assert measures(fields) == group["measures"]
        assert not grouped
    # The original-file audit separately checks all old row transaction IDs,
    # exact amounts and raw memo flags. Group equality does not establish v4 IDs.


@REQUIRES
def test_empty_termination_is_corroborated_and_cash_disagreement_survives():
    import base64

    r = read(Path(AUDIT) / "sid-window.json")
    empty = [d for d in r["reports"] if not d["groups"]]
    assert len(empty) == 1
    d = empty[0]
    assert d["value_basis"] == "empty_profile_with_original_and_explicit_reported_zero"
    b = r["reported"]["window"]["bindings"][d["binding_index"]]
    assert b["document"]["capture_extent"] == "complete_response"
    records = [base64.b64decode(v["raw_base64"]) for v in b["document"]["records"]]
    body = b"".join(records)
    assert hashlib.sha256(body).hexdigest() == b["document"]["body"]["sha256"]
    assert not any(raw.startswith(b"SA") for raw in records)
    assert d["reported_minor_units"] == d["detail_minor_units"] == "0"
    assert r["reported"]["summary"]["assertions"][0]["diagnostic_equations"]["cash"]["delta_minor_units"] == "150000000"
    assert any(e["residual_minor_units"] == "-150000000" for e in r["reported"]["window"]["equations"])
    assert r["reported"]["comparisons"][0]["cycle_span"]["outside_activity_known"] is False
