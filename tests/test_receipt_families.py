"""Source-map audit only; no runtime receipt or cash calculation lives here."""
import copy
import hashlib
import json
import os
from collections import Counter, defaultdict
from pathlib import Path

import pytest

from tests.test_receipt_report_lines import (
    CASES,
    cents,
    measures,
    pinned_sources,
    source_body,
)
from tests.test_report_field_binding import workbook_rows

ROOT = Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "contracts/calculations/fec/receipt-families/v1/contract.json"
AUDIT = os.environ.get("LT_RECEIPT_FAMILIES_AUDIT")
STORAGE = os.environ.get("LT_RECEIPT_FAMILIES_STORAGE")
PROFILE = os.environ.get("LT_RECEIPT_FAMILIES_PROFILE")
WORKBOOK = os.environ.get("LT_RECEIPT_FAMILIES_WORKBOOK")
REQUIRES = pytest.mark.skipif(not all((AUDIT, STORAGE, PROFILE, WORKBOOK)), reason="requires retained bounded source audit")


def read(path):
    return json.loads(path.read_bytes())


def expanded_terms(spec, name, ancestors=()):
    assert name not in ancestors, "cyclic subtotal dependency"
    leaves = {v["id"] for v in spec["leaves"]}
    if name in leaves:
        return {name: 1}
    aggregates = {v["id"]: v for v in spec["aggregates"]}
    assert name in aggregates, "unknown subtotal operand"
    result = defaultdict(int)
    for member, coefficient in aggregates[name]["terms"].items():
        assert coefficient in (-1, 1)
        for leaf, weight in expanded_terms(spec, member, (*ancestors, name)).items():
            result[leaf] += coefficient * weight
    return {k: v for k, v in result.items() if v}


def route(contract, key):
    # Exact-key independent oracle. It never selects financial members.
    if not key["filing_form"]["present"] or not key["line_num"]["present"] or key["schedule_type"] != {"present": True, "value": "SA"}:
        return "unmapped", None
    spec = contract["forms"].get(key["filing_form"]["value"])
    if spec is None:
        return "outside_form", None
    line = key["line_num"]["value"]
    for leaf in spec["leaves"]:
        if leaf["line"] == line and leaf["detail_schedule"] == "SA":
            return "mapped_sa_family", leaf["id"]
    if any(a["line"] == line for a in spec["aggregates"]):
        return "aggregate_line_reference", None
    return "unmapped", None


def test_contract_fields_disjoint_subtotals_and_guards():
    c = read(CONTRACT)
    assert c["version"] == "legal-tender.fec.receipt-family-map.v1"
    assert c["status"] == "reviewed-source-map-not-runtime-financial-policy"
    assert not any(c["guards"].values())
    summary = read(ROOT / "contracts/sources/fec/committee-summary/v1/record.schema.json")["properties"]
    for form, spec in c["forms"].items():
        fields = spec["leaves"] + spec["aggregates"]
        assert len({f["id"] for f in fields}) == len(fields)
        assert len({f["line"] for f in fields}) == len(fields)
        assert sorted(f["sequence"] for f in fields) == list(range(33 if form == "F3" else 30, 47))
        assert all(f["summary_field"] in summary for f in fields)
        assert expanded_terms(spec, "total_receipts") == {leaf["id"]: 1 for leaf in spec["leaves"]}
        if form == "F3X":
            assert expanded_terms(spec, "total_federal_receipts") == {leaf["id"]: 1 for leaf in spec["leaves"] if leaf["detail_schedule"] not in ("H3", "H5")}
        for leaf in spec["leaves"]:
            if leaf["detail_schedule"] is None:
                assert leaf["detail_relation"] == "explicit_cover_only"
            if leaf["detail_schedule"] in ("H3", "H5"):
                assert leaf["detail_relation"] == "other_schedule_and_account_scope"


def test_same_line_means_different_family_and_no_aliases():
    c = read(CONTRACT)
    key = {"filing_form": {"present": True, "value": "F3"}, "schedule_type": {"present": True, "value": "SA"}, "line_num": {"present": True, "value": "11D"}}
    assert route(c, key) == ("mapped_sa_family", "candidate_contributions")
    key["filing_form"]["value"] = "F3X"
    assert route(c, key) == ("aggregate_line_reference", None)
    for form, line, state in [("F3X", "19A", "unmapped"), ("F3X", "SL1A", "unmapped"), ("F3X", "SL2", "unmapped"), ("F3X", "11ai", "unmapped"), ("F3P", "17A", "outside_form"), ("F4", "14A", "outside_form"), ("F9", "F92", "outside_form"), ("F3X", "18A", "unmapped")]:
        key["filing_form"]["value"], key["line_num"]["value"] = form, line
        assert route(c, key) == (state, None)
    key["line_num"]["present"] = False
    assert route(c, key) == ("unmapped", None)
    broken = copy.deepcopy(c["forms"]["F3"])
    broken["aggregates"][-1]["terms"]["total_receipts"] = 1
    with pytest.raises(AssertionError, match="cyclic"):
        expanded_terms(broken, "total_receipts")


@pytest.fixture(scope="module")
def profile():
    raw = Path(PROFILE).read_bytes()
    assert len(raw) == 266269624 and hashlib.sha256(raw).hexdigest() == "acce0a13f2d87abfe78beb66c6bd2ae60f82a53bf656b3745343966ae57f3647"
    p = json.loads(raw)
    assert p["profile_id"] == "a84e5ad04a315a2dfedc575ac44aecbba8c816b8265a76b2a9a69cc810b7dcc7"
    return p


@REQUIRES
def test_source_pins_and_every_workbook_receipt_field():
    contract = read(CONTRACT)
    for line in (ROOT / "docs/audit/fixtures/receipt-families-2026-09-11.sha256").read_text().splitlines():
        digest, name = line.split()
        body = (Path(AUDIT) / name).read_bytes()
        assert 0 < len(body) < 2 * 1024 * 1024
        assert hashlib.sha256(body).hexdigest() == digest
        if name.endswith(".pdf"):
            assert body.startswith(b"%PDF-")
    workbook = Path(WORKBOOK)
    assert hashlib.sha256(workbook.read_bytes()).hexdigest() == contract["sources"]["format84"]["sha256"]
    sheets = workbook_rows(workbook)
    for form, spec in contract["forms"].items():
        assert max(sheets[form]) == spec["cover_width"]
        for field in spec["leaves"] + spec["aggregates"]:
            row = sheets[form][field["sequence"]]
            assert row["B"] == field["label"]
            assert row["C"].strip() == "AMT-12"


@REQUIRES
def test_complete_saved_profile_routing_without_discarding_occurrences(profile):
    c = read(CONTRACT)
    states = Counter()
    summaries = defaultdict(lambda: {"rows": 0, "known_amount_rows": 0, "unknown_amount_rows": 0, "signed_minor_units": 0, "positive_minor_units": 0, "negative_minor_units": 0, "positive_rows": 0, "negative_rows": 0, "zero_rows": 0, "nonempty_conduit_id_rows": 0})
    physical = []
    for group in profile["form_line_groups"]:
        state, family = route(c, group["key"])
        states[state] += group["measures"]["rows"]
        for name, value in group["measures"].items():
            summaries[state][name] += int(value)
        physical.append({"key": group["key"], "measures": group["measures"], "map_state": state, "family": family})
    for name, total in profile["total_occurrences"].items():
        assert sum(summary[name] for summary in summaries.values()) == int(total)
    assert sum(states.values()) == 264085633
    assert states["aggregate_line_reference"] == 7
    assert states["unmapped"] == 532
    # Emit every original form group, including signed/unknown/memo axes.
    print("AUDIT_JSON " + json.dumps({"profile_id": profile["profile_id"], "profile_states": dict(states), "state_measures": dict(summaries), "form_groups": physical}, sort_keys=True))


@REQUIRES
@pytest.mark.parametrize("committee,file,form,row_count", CASES)
def test_all_retained_f3_receipt_families_and_cover_equations(profile, committee, file, form, row_count):
    spec = read(CONTRACT)["forms"][form]
    storage = Path(STORAGE)
    lines_dir = storage / "dumps/audits/fec/receipt-report-lines/2026-09-10/attempt-01"
    report = read(lines_dir / f"{file}-report.json")
    if committee == "C00843367":
        directory = storage / "dumps/audits/fec/summary-report-review/2026-09-10/attempt-01"
        pin = pinned_sources()[file + ".fec"]
    else:
        directory = storage / "dumps/audits/fec/receipt-report-association/2026-09-08/2024"
        pin = {"1730369": "0a12e970d484a19aa5a72e4eb38e1379cecbfb0e9c1b7a8db38aa05fdbfa66ea", "1753173": "8f82657d70261d1e472c666b49f95991e6ca021bfd3eddb6addb2873d6f72ddf"}[file]
    raw = source_body(directory / f"{file}.fec", pin)
    rows = [r.rstrip(b"\r").split(b"\x1c") for r in raw.split(b"\n") if r.rstrip(b"\r")]
    cover = rows[1]
    assert rows[0][:3] == [b"HDR", b"FEC", b"8.4"]
    assert len(cover) == spec["cover_width"] and cover[1].decode() == committee
    original = [r for r in rows if r[0].startswith(b"SA")]
    assert len(original) == row_count == len(report["receipts"])
    assert all(len(r) == 45 for r in original)
    layout = read(ROOT / "contracts/sources/fec/efile-format/v1/schedule-a-fields.json")
    index = {f["name"]: f["sequence"] - 1 for f in layout["fields"]}
    by_id = {r[index["transaction_id"]].decode(): r for r in original}
    assert len(by_id) == len(original) and set(by_id) == {r["fields"]["tran_id"] for r in report["receipts"]}
    by_line = defaultdict(list)
    for r in report["receipts"]:
        f = r["fields"]
        source = by_id[f["tran_id"]]
        assert source[index["form_type"]].decode() == f["schedule_type"] + f["line_num"]
        assert source[index["memo_code"]].decode() == (f["memo_cd"] or "")
        assert cents(source[index["contribution_amount"]].decode()) == int(f["lt_receipt_amount_minor_units"])
        assert f["memo_cd"] in (None, "", "X")
        by_line[f["line_num"]].append(f)
    assert not set(by_line) - {f["line"] for f in spec["leaves"] if f["detail_schedule"] == "SA"}
    fields = spec["leaves"] + spec["aggregates"]
    cover_values = {f["id"]: cents(cover[f["sequence"] - 1].decode()) for f in fields}
    comparisons = []
    for leaf in spec["leaves"]:
        members = by_line[leaf["line"]]
        nonmemo = [r for r in members if not r["memo_cd"]]
        observed = sum(int(r["lt_receipt_amount_minor_units"]) for r in nonmemo) if nonmemo else None
        all_groups = [g for g in profile["report_line_groups"] if g["key"]["committee"]["value"] == committee and g["key"]["file_num"]["value"] == file and g["key"]["line_num"]["value"] == leaf["line"]]
        assert sum(g["measures"]["rows"] for g in all_groups) == len(members)
        for name, value in measures(members).items():
            assert sum(int(g["measures"][name]) for g in all_groups) == int(value)
        comparisons.append({"family": leaf["id"], "line": leaf["line"], "detail_relation": leaf["detail_relation"], "physical_rows": len(members), "nonmemo_rows": len(nonmemo), "observed_detail_minor_units": None if observed is None else str(observed), "cover_minor_units": str(cover_values[leaf["id"]]), "reported_minus_observed_minor_units": None if observed is None else str(cover_values[leaf["id"]] - observed)})
    equations = {a["id"]: str(cover_values[a["id"]] - sum(coefficient * cover_values[name] for name, coefficient in a["terms"].items())) for a in spec["aggregates"]}
    # Preserve nonzero equations rather than repair the original cover.
    print("AUDIT_JSON " + json.dumps({"file": file, "receipt_families": comparisons, "cover_equation_residuals": equations}, sort_keys=True))
    if file == "1780310":
        other = next(c for c in comparisons if c["family"] == "other_receipts")
        assert other["cover_minor_units"] == "37" and other["physical_rows"] == 0
        assert other["observed_detail_minor_units"] is None and other["reported_minus_observed_minor_units"] is None
    assert report["input"]["fact_set_id"] == "8f69a2ef40316f62d8844ecf732342a3b4535136b434f60677d0608cb04465df"
    assert profile["summary_input"]["source_release_id"] == "fec-01f93a786b630be40a932543f35251bcb56c42b32d1ae85ac34a101ddc7b56b2"
