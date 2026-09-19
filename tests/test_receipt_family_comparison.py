"""Independent retained-source checks; no production calculation runs in Python."""
import base64
import hashlib
import json
import os
from decimal import Decimal
from pathlib import Path

import pytest

from tests.test_report_field_binding import minor, workbook_rows

ROOT = Path(__file__).resolve().parents[1]
POLICY = ROOT / "contracts/calculations/fec/receipt-family-comparison/v1/policy.json"
MAP = ROOT / "contracts/calculations/fec/receipt-families/v1/contract.json"
AUDIT = os.environ.get("LT_FAMILY_COMPARISON_AUDIT")
PROFILE = os.environ.get("LT_FAMILY_COMPARISON_PROFILE")
PRIOR = os.environ.get("LT_FAMILY_COMPARISON_PRIOR")
WORKBOOK = os.environ.get("LT_FAMILY_COMPARISON_WORKBOOK")
REQUIRES = pytest.mark.skipif(not all((AUDIT, PROFILE, PRIOR, WORKBOOK)), reason="requires retained bounded comparison gate")


def read(path):
    return json.loads(path.read_bytes(), parse_float=Decimal)


def test_family_comparison_policy_is_additive_and_narrow():
    p, mapping = read(POLICY), read(MAP)
    assert p["version"] == "legal-tender.fec.receipt-family-comparison.v1"
    assert p["map_version"] == mapping["version"]
    assert not any(p["guards"].values())
    for form, names in p["families"].items():
        assert len(names) == len(set(names))
        for name in names:
            leaf = next(f for f in mapping["forms"][form]["leaves"] if f["id"] == name)
            assert leaf["detail_schedule"] == "SA" and leaf["detail_relation"] == "all_required_itemized"
    assert "explicit_zero_cover" in p["empty_detail"]


@pytest.fixture(scope="module")
def profile():
    body = Path(PROFILE).read_bytes()
    assert hashlib.sha256(body).hexdigest() == "acce0a13f2d87abfe78beb66c6bd2ae60f82a53bf656b3745343966ae57f3647"
    return json.loads(body)


@REQUIRES
@pytest.mark.parametrize("name", ["sid-window", "sid-cycle", "sid-missing-cover", "nrcc-unresolved"])
def test_source_membership_exact_fields_and_group_conservation(profile, name):
    r = read(Path(AUDIT) / f"{name}.json")
    old = read(Path(PRIOR) / f"{name}.json")
    policy, mapping = read(POLICY), read(MAP)
    assert r["version"] == policy["version"] and r["map_version"] == mapping["version"]
    assert r["reported"] == old["reported"]
    assert r["profile_id"] == profile["profile_id"]
    assert r["schedule_a_source"] == profile["schedule_a_source"]
    assert r["reported"]["summary_input"] == profile["summary_input"]
    assert r["reported"]["summary_calculation_id"] == profile["summary_calculation_id"]
    assert not any(r[k] for k in policy["guards"])
    review = r["family_reports"]
    assert review["membership"] == old["reported"]["window"]["membership"]
    assert review["document_set"] == old["reported"]["window"]["document_set"]
    assert len(r["reports"]) == len(review["bindings"])
    workbook = Path(WORKBOOK)
    assert hashlib.sha256(workbook.read_bytes()).hexdigest() == mapping["sources"]["format84"]["sha256"]
    sheets = workbook_rows(workbook)
    metadata_schema = read(ROOT / "contracts/sources/fec/report-metadata/v1/record.schema.json")["$defs"]
    for d in r["reports"]:
        b = review["bindings"][d["binding_index"]]
        original = old["reported"]["window"]["bindings"][d["binding_index"]]["document"]
        assert b["document"]["body"] == original["body"]
        records = [base64.b64decode(row["raw_base64"]) for row in b["document"]["records"]]
        assert hashlib.sha256(b"".join(records)).hexdigest() == b["document"]["body"]["sha256"]
        groups = [g for g in profile["report_line_groups"] if g["key"]["committee"] == {"present": True, "value": r["reported"]["committee_id"]} and g["key"]["file_num"] == {"present": True, "value": d["file_number"]}]
        assert d["groups"] == groups
        for key, total in d["total_occurrences"].items():
            assert sum(int(g["measures"][key]) for g in groups) == int(total)
        seen = list(d["outside_family_group_indexes"])
        obs = review["membership"]["observations"][b["observation_index"]]
        form = {"Form 3": "F3", "Form 3X": "F3X"}[obs["report_form"]]
        raw_metadata = next(rec["raw"] for page in review["membership"]["evidence"]["pages"] for rec in page["records"] if rec["file_number"] == d["file_number"])
        assert [f["field"]["id"] for f in d["families"]] == policy["families"][form]
        cover = records[1].rstrip(b"\r\n").split(b"\x1c")
        for f in d["families"]:
            spec = f["field"]
            leaf = next(v for v in mapping["forms"][form]["leaves"] if v["id"] == spec["id"])
            assert (spec["form"], spec["line"], spec["sequence"]) == (form, leaf["line"], leaf["sequence"])
            assert sheets[form][spec["sequence"]]["B"] == leaf["label"]
            schema = metadata_schema["HouseSenate" if form == "F3" else "PacParty"]
            assert schema["properties"][spec["metadata_field"]]["type"] == ["number", "null"]
            binding = f["binding"]
            assert binding in b["fields"]
            assert binding["cover"][0]["raw"] == cover[spec["sequence"] - 1].decode()
            assert binding["metadata"]["raw"] == raw_metadata[spec["metadata_field"]]
            if binding["reported_value_bound"]:
                assert b["scope_bound"]
                assert b["observation_index"] in review["membership"]["chain_candidate_indexes"]
                assert minor(binding["cover"][0]["raw"]) == minor(raw_metadata[spec["metadata_field"]]) == f["reported_minor_units"]
            else:
                assert f["state"] == "blocked" and f["reported_minor_units"] is None
            indexes = [i for i, g in enumerate(groups) if g["key"]["filing_form"] == {"present": True, "value": form} and g["key"]["schedule_type"] == {"present": True, "value": "SA"} and g["key"]["line_num"] == {"present": True, "value": leaf["line"]}]
            assert f["group_indexes"] == indexes
            seen += indexes
            nonmemo = [groups[i] for i in indexes if not groups[i]["key"]["memo_code"]["value"]]
            for key, value in f["nonmemo_occurrences"].items():
                assert sum(int(g["measures"][key]) for g in nonmemo) == int(value)
            if f["state"] == "blocked":
                assert f["blockers"] and f["detail_minor_units"] is None and f["delta_minor_units"] is None
            else:
                assert not f["blockers"] and nonmemo
                assert f["detail_minor_units"] == f["nonmemo_occurrences"]["signed_minor_units"]
                assert int(f["delta_minor_units"]) == int(f["reported_minor_units"]) - int(f["detail_minor_units"])
                assert f["state"] == ("equal" if f["delta_minor_units"] == "0" else "different")
        assert sorted(seen) == list(range(len(groups)))


@REQUIRES
def test_real_loan_pairs_and_absent_detail_are_not_cash():
    r = read(Path(AUDIT) / "sid-window.json")
    comparisons = [(d["file_number"], f) for d in r["reports"] for f in d["families"]]
    ready = [(file, f) for file, f in comparisons if f["state"] != "blocked"]
    assert {file for file, _ in ready} == {"1714573", "1743911"}
    assert len(ready) == 2
    for _, f in ready:
        assert f["field"]["id"] == "candidate_made_or_guaranteed_loans"
        assert f["detail_minor_units"] == f["reported_minor_units"] == "100000000"
    empty = next(d for d in r["reports"] if d["file_number"] == "1780346")
    assert not empty["groups"]
    assert all(f["reported_minor_units"] == "0" and f["detail_minor_units"] is None and "no_nonmemo_family_detail" in f["blockers"] for f in empty["families"])
    superseded = next(d for d in r["reports"] if d["file_number"] == "1766839")
    assert "not_observed_chain_candidate" in superseded["blockers"]
    assert all(f["state"] == "blocked" for f in superseded["families"])
