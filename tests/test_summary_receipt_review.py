"""Wire and independent artifact checks only; the runtime boundary is Go."""

import copy
import hashlib
import json
import os
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from referencing import Resource

from orchestration.go_process import _local_registry
from tests.test_summary_assertion_corpus import go_json

CONTRACTS = Path(__file__).resolve().parents[1] / "contracts"
SCHEMA = CONTRACTS / "calculations/fec/summary-receipt-review/v1/result.schema.json"


def validator():
    # The existing inventory contract has a .dev ID; summary contracts use .local.
    # Register its exact pinned ID locally, without aliases or HTTP resolution.
    basis = json.loads((CONTRACTS / "calculations/fec/committee-funding-basis/v1/result.schema.json").read_text())
    registry = _local_registry(CONTRACTS).with_resource(basis["$id"], Resource.from_contents(basis))
    return Draft202012Validator(
        json.loads(SCHEMA.read_text()),
        registry=registry,
        format_checker=FormatChecker(),
    )


def test_summary_receipt_review_schema():
    Draft202012Validator.check_schema(json.loads(SCHEMA.read_text()))


def test_summary_receipt_review_fixture_guards():
    path = os.environ.get("LT_SUMMARY_REVIEW_FIXTURE")
    if not path:
        pytest.skip("requires synthetic Go review")
    result = json.loads(Path(path).read_text())
    check = validator()
    check.validate(result)
    for field in ("comparison_ready", "complete_committee_funding_basis", "terminal_attribution_eligible"):
        bad = copy.deepcopy(result)
        bad[field] = True
        assert list(check.iter_errors(bad))
    bad = copy.deepcopy(result)
    bad["assertions"][0]["fields"][0]["delta_minor_units"] = "0"
    assert list(check.iter_errors(bad))
    bad = copy.deepcopy(result)
    bad["comparison_blockers"] = []
    assert list(check.iter_errors(bad))
    bad = copy.deepcopy(result)
    bad["source_alignment"] = "different_source_release"
    assert list(check.iter_errors(bad))


def test_published_summary_receipt_review_against_prior_verified_artifacts():
    output, storage = os.environ.get("LT_SUMMARY_REVIEW_OUTPUT"), os.environ.get("LT_SUMMARY_REVIEW_STORAGE")
    if not output or not storage:
        pytest.skip("requires captured Go reviews and read-only published evidence")
    root = Path(storage)
    basis = json.loads(Path(os.environ["LT_SUMMARY_REVIEW_BASIS"]).read_text())
    assertions = json.loads((root / "dumps/audits/fec/summary-assertions/2026-09-10/attempt-01/2024.json").read_text())
    by_committee = {g["committee_id"]: g for g in assertions["committees"]}
    check = validator()
    files = sorted(Path(output).glob("C*.json"))
    assert len(files) == 5
    keys = list(basis["total"])

    def measures(buckets):
        sums = {k: sum(int(b["measures"][k]) for b in buckets) for k in keys}
        return {k: str(v) if k.endswith("minor_units") else v for k, v in sums.items()}

    for path in files:
        result = json.loads(path.read_text())
        check.validate(result)
        identity = result["review_id"]
        result["review_id"] = ""
        assert hashlib.sha256(go_json(result)).hexdigest() == identity
        result["review_id"] = identity
        assert result["receipt_input"] == basis["input"]
        assert result["inventory_calculation_id"] == basis["calculation_id"]
        assert result["summary_input"] == assertions["input"]
        assert result["summary_calculation_id"] == assertions["calculation_id"]
        assert result["cycle"] == basis["cycle"] == assertions["cycle"]
        assert result["source_alignment"] == "different_source_release"
        assert "source_release_mismatch" in result["comparison_blockers"]
        selected = [b for b in basis["buckets"] if b["key"]["recipient"] == {"present": True, "value": result["committee_id"]}]
        assert result["receipts"]["total"] == measures(selected)
        assert result["receipts"]["individual_predicate"] == measures([b for b in selected if b["key"]["individual_decision"] == "included"])
        assert result["receipts"]["individual_committee_overlap"] == measures([b for b in selected if b["key"]["component"] == "overlapping_individual_and_committee"])
        components = {(b["key"]["component"], b["key"]["receipt_role"]) for b in selected}
        assert len(result["receipts"]["components"]) == len(components)
        for component in result["receipts"]["components"]:
            assert component["measures"] == measures([b for b in selected if (b["key"]["component"], b["key"]["receipt_role"]) == (component["component"], component["receipt_role"])])
        group = by_committee.get(result["committee_id"])
        assert len(result["assertions"]) == (len(group["assertions"]) if group else 0)
        originals = {a["assertion_id"]: a for a in group["assertions"]} if group else {}
        for variant in result["assertions"]:
            original = originals[variant["assertion_id"]]
            for key in ("members", "representative_fact_id", "committee_type_raw", "designation_raw", "coverage_start", "coverage_end"):
                assert variant[key] == original[key]
            fields = {o["field"]: o for e in original["diagnostic_equations"].values() for o in e["operands"]}
            for field in variant["fields"]:
                assert field["raw"] == fields[field["field"]]["raw"]
                assert field["value"] == fields[field["field"]]["value"]
                assert field["delta_minor_units"] is None
            assert variant["summary_diagnostics"] == {k: {"state": v["state"], "delta_minor_units": v["delta_minor_units"]} for k, v in original["diagnostic_equations"].items()}
