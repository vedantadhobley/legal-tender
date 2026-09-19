"""Independent source-map, original-byte and retained-profile checks for v2."""
import base64
import copy
import hashlib
import json
import os
from decimal import Decimal
from pathlib import Path

import pytest

from tests.test_report_field_binding import minor, workbook_rows

ROOT = Path(__file__).resolve().parents[1]
AUDIT = os.environ.get("LT_FAMILY_V2_AUDIT")
STORAGE = os.environ.get("LT_FAMILY_V2_STORAGE")
REQUIRES = pytest.mark.skipif(not all((AUDIT, STORAGE)), reason="requires retained offline v2 gate")
CASES = ("sid-window", "sid-cycle", "sid-missing-cover", "nrcc-unresolved", "nrcc-january")
MAP = ROOT / "contracts/calculations/fec/receipt-families/v1/contract.json"
POLICY = ROOT / "contracts/calculations/fec/receipt-family-comparison/v2/policy.json"
NEW_METADATA = {
    ("F3", "11D"): "candidate_contribution_period",
    ("F3", "14"): "total_offsets_to_operating_expenditures_period",
    ("F3", "15"): "other_receipts_period",
    ("F3X", "14"): "loan_repayments_received_period",
    ("F3X", "15"): "offsets_to_operating_expenditures_period",
    ("F3X", "16"): "fed_candidate_contribution_refunds_period",
    ("F3X", "17"): "other_fed_receipts_period",
}


def read(path):
    return json.loads(path.read_bytes(), parse_float=Decimal)


def test_v2_policy_covers_all_remaining_sa_leaves_without_collapsing_scope():
    policy, mapping = read(POLICY), read(MAP)
    assert policy["version"] == "legal-tender.fec.receipt-family-comparison.v2"
    assert policy["map_version"] == mapping["version"]
    assert not any(policy["guards"].values())
    assert set(policy["detail_relations"]) == {"all_required_itemized", "thresholded_component_of_total"}
    for form, names in policy["families"].items():
        leaves = mapping["forms"][form]["leaves"]
        expected = [leaf for leaf in leaves if leaf["detail_schedule"] == "SA" and leaf["detail_relation"] != "itemized_component"]
        assert len(names) == len(set(names)) == len(expected)
        assert set(names) == {leaf["id"] for leaf in expected}


@pytest.fixture(scope="module")
def evidence():
    base = Path(STORAGE) / "dumps/audits/fec"
    path = base / "receipt-report-profile-v2/2026-09-10/attempt-01/profile.json"
    body = path.read_bytes()
    assert hashlib.sha256(body).hexdigest() == "acce0a13f2d87abfe78beb66c6bd2ae60f82a53bf656b3745343966ae57f3647"
    profile = json.loads(body)
    del body
    mapping = read(MAP)
    workbook = base / "report-field-binding/2026-09-10/attempt-01/FEC_Format_v8.4.xlsx"
    assert hashlib.sha256(workbook.read_bytes()).hexdigest() == mapping["sources"]["format84"]["sha256"]
    return base, profile, workbook_rows(workbook)


@REQUIRES
@pytest.mark.parametrize("name", CASES)
def test_v2_exact_original_binding_routing_and_partial_scope(evidence, name):
    base, profile, sheets = evidence
    r = read(Path(AUDIT) / f"{name}.json")
    old = read(base / f"receipt-family-window/2026-09-11/attempt-01/{name}.json")["reviewed"]["compared"]
    policy, mapping = read(POLICY), read(MAP)
    assert r["version"] == policy["version"]
    for key in ("reported", "profile", "profile_id", "schedule_a_source"):
        assert r[key] == old[key]
    assert r["profile_id"] == profile["profile_id"]
    assert r["schedule_a_source"] == profile["schedule_a_source"]
    assert r["reported"]["summary_input"] == profile["summary_input"]
    assert not any(r[key] for key in policy["guards"])
    review = r["family_reports"]
    assert review["version"] == "legal-tender.fec.receipt-family-reports.v2"
    assert review["membership"] == old["family_reports"]["membership"]
    assert review["document_set"] == old["family_reports"]["document_set"]
    schema = read(ROOT / "contracts/sources/fec/report-metadata/v1/record.schema.json")["$defs"]
    for d, previous in zip(r["reports"], old["reports"], strict=True):
        binding = review["bindings"][d["binding_index"]]
        original = binding["document"]
        assert original["version"] == "legal-tender.fec.receipt-family-cover.v2"
        records = [base64.b64decode(row["raw_base64"]) for row in original["records"]]
        assert hashlib.sha256(b"".join(records)).hexdigest() == original["body"]["sha256"]
        cover = records[1].rstrip(b"\r\n").split(b"\x1c")
        obs = review["membership"]["observations"][binding["observation_index"]]
        form = {"Form 3": "F3", "Form 3X": "F3X"}[obs["report_form"]]
        metadata = next(rec["raw"] for page in review["membership"]["evidence"]["pages"] for rec in page["records"] if rec["file_number"] == d["file_number"])
        groups = [g for g in profile["report_line_groups"] if g["key"]["committee"] == {"present": True, "value": r["reported"]["committee_id"]} and g["key"]["file_num"] == {"present": True, "value": d["file_number"]}]
        assert d["groups"] == previous["groups"] == groups
        assert d["total_occurrences"] == previous["total_occurrences"]
        assert d["blockers"] == previous["blockers"]
        assert [f["field"]["id"] for f in d["families"]] == policy["families"][form]
        seen = list(d["outside_family_group_indexes"])
        old_fields = {f["field"]["id"]: f for f in previous["families"]}
        for f in d["families"]:
            spec = f["field"]
            leaf = next(v for v in mapping["forms"][form]["leaves"] if v["id"] == spec["id"])
            assert (spec["form"], spec["line"], spec["sequence"], spec["detail_relation"]) == (form, leaf["line"], leaf["sequence"], leaf["detail_relation"])
            assert sheets[form][spec["sequence"]]["B"] == leaf["label"]
            if spec["id"] in old_fields:
                without_relation = copy.deepcopy(f)
                without_relation["field"].pop("detail_relation")
                assert without_relation == old_fields[spec["id"]]
            else:
                assert spec["metadata_field"] == NEW_METADATA[form, spec["line"]]
            types = schema["HouseSenate" if form == "F3" else "PacParty"]["properties"][spec["metadata_field"]]["type"]
            assert "number" in types and "null" in types
            field = f["binding"]
            assert field in binding["fields"]
            sequences = sorted([spec["sequence"]] + ([spec["corroborating_sequence"]] if "corroborating_sequence" in spec else []))
            assert [v["sequence"] for v in field["cover"]] == sequences
            if form == "F3" and spec["id"] == "operating_offsets":
                assert sequences == [28, 44]
                assert sheets[form][28]["B"] == "(7b) Total Offset to Operating Expenditures"
            for v in field["cover"]:
                assert v["raw"] == cover[v["sequence"] - 1].decode()
            assert field["metadata"]["raw"] == metadata[spec["metadata_field"]]
            if field["reported_value_bound"]:
                assert binding["scope_bound"] and not field["blockers"]
                assert binding["observation_index"] in review["membership"]["chain_candidate_indexes"]
                assert all(minor(v["raw"]) == minor(metadata[spec["metadata_field"]]) == f["reported_minor_units"] for v in field["cover"])
            else:
                assert f["state"] == "blocked" and f["reported_minor_units"] is None
            indexes = [i for i, g in enumerate(groups) if g["key"]["filing_form"] == {"present": True, "value": form} and g["key"]["schedule_type"] == {"present": True, "value": "SA"} and g["key"]["line_num"] == {"present": True, "value": leaf["line"]}]
            assert f["group_indexes"] == indexes
            seen += indexes
            nonmemo = [groups[i] for i in indexes if not groups[i]["key"]["memo_code"]["value"]]
            for key, total in f["nonmemo_occurrences"].items():
                assert sum(int(g["measures"][key]) for g in nonmemo) == int(total)
            if f["state"] == "blocked":
                assert f["blockers"] and f["detail_minor_units"] is None and f["delta_minor_units"] is None
            else:
                assert not f["blockers"] and nonmemo
                assert f["detail_minor_units"] == f["nonmemo_occurrences"]["signed_minor_units"]
                if leaf["detail_relation"] == "thresholded_component_of_total":
                    assert f["state"] == "component_not_comparable" and f["delta_minor_units"] is None
                else:
                    assert int(f["delta_minor_units"]) == int(f["reported_minor_units"]) - int(f["detail_minor_units"])
                    assert f["state"] == ("equal" if f["delta_minor_units"] == "0" else "different")
        assert sorted(seen) == list(range(len(groups)))


@REQUIRES
def test_positive_reported_other_receipts_without_detail_stay_unknown():
    r = read(Path(AUDIT) / "sid-window.json")
    positives = [f for d in r["reports"] for f in d["families"] if f["field"]["id"] == "other_receipts" and f["reported_minor_units"] is not None and int(f["reported_minor_units"]) > 0]
    assert positives
    for f in positives:
        assert f["state"] == "blocked" and "no_nonmemo_family_detail" in f["blockers"]
        assert f["detail_minor_units"] is None and f["delta_minor_units"] is None
