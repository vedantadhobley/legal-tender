"""Independent raw CSV/assertion and family-window comparisons; audit only."""
import csv
import hashlib
import io
import json
import os
import re
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
POLICY = ROOT / "contracts/calculations/fec/receipt-family-summary/v1/policy.json"
AUDIT = os.environ.get("LT_FAMILY_SUMMARY_AUDIT")
STORAGE = os.environ.get("LT_FAMILY_SUMMARY_STORAGE")
CASES = ("sid-window", "sid-cycle", "sid-missing-cover", "nrcc-unresolved", "nrcc-january")
REQUIRES = pytest.mark.skipif(not AUDIT or not STORAGE, reason="requires retained read-only gate")


def read(path):
    return json.loads(path.read_bytes(), parse_float=Decimal)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def test_policy_maps_exact_source_leaves_without_financial_overrides():
    p = read(POLICY)
    source = read(ROOT / "contracts/calculations/fec/receipt-families/v1/contract.json")
    prior = read(ROOT / "contracts/calculations/fec/receipt-family-window/v1/policy.json")
    assert p["upstream_version"] == prior["version"] and p["source_map"] == source["version"]
    assert p["scope"] == "reported_summary_coverage" and p["source_alignment"] == "independent_snapshots"
    assert not any(p["guards"].values())
    for form, mappings in p["mappings"].items():
        leaves = {f["id"]: f for f in source["forms"][form]["leaves"]}
        for family, field in mappings.items():
            assert leaves[family]["summary_field"] == field
            assert leaves[family]["detail_relation"] == "all_required_itemized"
            assert leaves[family]["detail_schedule"] == "SA"
            assert not field.startswith("TTL_")
    assert not re.search(r'C\d{8}|1690269|1714573|2024', json.dumps(p))
    assert "not proof of principal origin" in p["loan_scope"]


@REQUIRES
@pytest.mark.parametrize("name", CASES)
def test_raw_summary_fields_full_membership_and_independent_scope(name):
    root = Path(STORAGE)
    r = read(Path(AUDIT) / f"{name}.json")
    w = r["family_window"]
    assert w == read(root / "dumps/audits/fec/receipt-family-window/2026-09-11/attempt-01" / f"{name}.json")
    p = read(POLICY)
    assert r["version"] == p["version"] and r["source_alignment"] == p["source_alignment"]
    assert not any(r[k] for k in p["guards"])
    s = w["reviewed"]["compared"]["reported"]
    ancestry = s["summary_input"]
    manifest_raw = (root / "facts/fec/committee-summary/v1/manifests" / f'{ancestry["fact_set_id"]}.json').read_bytes()
    assert sha(manifest_raw) == ancestry["manifest_sha256"]
    manifest = json.loads(manifest_raw)
    assert manifest["source_release_id"] == ancestry["source_release_id"]
    assert manifest["source_release_manifest_sha256"] == ancestry["source_release_manifest_sha256"]
    source = (root / manifest["source_artifact"]["storage_key"]).read_bytes()
    assert sha(source) == manifest["source_artifact"]["sha256"] == ancestry["source_artifact_sha256"]
    rows = list(csv.DictReader(io.StringIO(source.decode(), newline=""), strict=True))
    selected = {i: row for i, row in enumerate(rows, 1) if row["CMTE_ID"] == s["committee_id"]}
    seen = set()
    assert not r["blockers"] and not s["summary"]["conflict_fields"]
    for assertion, pair in zip(s["summary"]["assertions"], r["assertions"], strict=True):
        assert pair["assertion_id"] == assertion["assertion_id"]
        assert pair["representative_fact_id"] == assertion["representative_fact_id"] == assertion["members"][0]["fact_id"]
        raw = selected[assertion["members"][0]["ordinal"]]
        for member in assertion["members"]:
            ordinal = member["ordinal"]
            assert ordinal not in seen and ordinal in selected
            seen.add(ordinal)
            assert sha(source[member["offset"]:member["offset"] + member["length"]]) == member["raw_sha256"]
            assert selected[ordinal]["CAND_ID"] == member["candidate_raw"]
            assert all(selected[ordinal][k] == v for k, v in raw.items() if k != "CAND_ID")
        for i, (family, field) in enumerate(zip(w["families"], pair["fields"], strict=True)):
            assert field["family_index"] == i
            spec = family["field"]
            summary_field = p["mappings"][spec["form"]][spec["id"]]
            operand = field["summary"]
            assert operand["field"] == summary_field and operand["raw"] == raw[summary_field]
            expected = str(int(Decimal(raw[summary_field]) * 100)) if raw[summary_field] else None
            scale = len(raw[summary_field].split(".")[1]) if "." in raw[summary_field] else 0
            assert operand["value"] == {"state": "valid" if expected is not None else "source_blank", "minor_units": expected, "source_scale": scale}
            scope_ready = w["window"]["start"] == assertion["coverage_start"]["value"] and w["window"]["end"] == assertion["coverage_end"]["value"]
            for side, basis in (("versus_reported_window", "reported"), ("versus_qualified_detail_window", "comparison")):
                value = field[side]
                window_value = family[f"{basis}_window_minor_units"]
                ready = scope_ready and family[f"{basis}_window_ready"] and expected is not None
                assert value["summary_field"] == summary_field and value["window_field"] == spec["metadata_field"]
                assert value["scope_basis"] == p["scope"]
                assert value["summary_minor_units"] == expected and value["window_minor_units"] == window_value
                assert value["reported_comparison_ready"] == ready
                if ready:
                    delta = str(int(expected) - int(window_value))
                    assert value["delta_minor_units"] == delta and not value["blockers"]
                    assert value["state"] == ("equal" if delta == "0" else "different")
                else:
                    assert value["delta_minor_units"] is None and value["state"] == "blocked" and value["blockers"]
    assert seen == set(selected)


@REQUIRES
def test_late_reported_span_does_not_repair_cash_or_prove_terminal_origin():
    r = read(Path(AUDIT) / "sid-window.json")
    values = {f["summary"]["field"]: f for f in r["assertions"][0]["fields"]}
    assert values["CAND_LOAN"]["summary"]["value"]["minor_units"] == "200000000"
    assert all(f["versus_reported_window"]["state"] == f["versus_qualified_detail_window"]["state"] == "equal" for f in values.values())
    s = r["family_window"]["reviewed"]["compared"]["reported"]
    assert s["summary"]["assertions"][0]["diagnostic_equations"]["cash"]["delta_minor_units"] == "150000000"
    assert not r["financial_use_eligible"] and not r["terminal_attribution_eligible"]


@REQUIRES
def test_positive_f3x_month_is_not_a_cycle_summary_comparison():
    r = read(Path(AUDIT) / "nrcc-january.json")
    w = r["family_window"]
    assert all(f["state"] == "equal" for f in w["families"])
    for f in r["assertions"][0]["fields"]:
        for side in ("versus_reported_window", "versus_qualified_detail_window"):
            assert f[side]["state"] == "blocked" and f[side]["delta_minor_units"] is None
            assert "summary_end_mismatch" in f[side]["blockers"]
    loan = next(f for f in r["assertions"][0]["fields"] if f["summary"]["field"] == "OTH_LOANS")
    assert loan["summary"]["raw"] == "" and loan["summary"]["value"]["minor_units"] is None
    assert loan["versus_reported_window"]["window_minor_units"] == "0"
    assert "summary_value_source_blank" in loan["versus_reported_window"]["blockers"]


@REQUIRES
def test_pinned_source_definitions_are_unchanged():
    root = Path(STORAGE) / "dumps/audits/fec"
    source = read(ROOT / "contracts/calculations/fec/receipt-families/v1/contract.json")["sources"]
    for key, name in (("f3", "f3-instructions.pdf"), ("f3x", "f3x-instructions.pdf")):
        assert sha((root / "receipt-families/2026-09-11/attempt-01" / name).read_bytes()) == source[key]["sha256"]
    assert sha((root / "report-field-binding/2026-09-10/attempt-01/FEC_Format_v8.4.xlsx").read_bytes()) == source["format84"]["sha256"]
    assert sha((root / "committee-summary-source/2026-09-08/committee-summary-description.html").read_bytes()) == "6fa2fa43035697db5d8de79590e8ecdc45f1c3325734d99991711f144eaa4141"
