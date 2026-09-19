"""Wire validation only; all receipt selection and runtime behavior are Go."""

import copy
import json
from pathlib import Path

from jsonschema import Draft202012Validator
from referencing import Registry, Resource


ROOT = Path(__file__).resolve().parents[1] / "contracts/calculations/fec/committee-funding-basis/v1"


def validators():
    schemas = {p.stem: json.loads(p.read_text()) for p in ROOT.glob("*.schema.json")}
    registry = Registry().with_resources(
        (s["$id"], Resource.from_contents(s)) for s in schemas.values()
    )
    return {k: Draft202012Validator(s, registry=registry) for k, s in schemas.items()}


def test_funding_basis_schema_resources():
    for validator in validators().values():
        Draft202012Validator.check_schema(validator.schema)


def test_receipt_page_money_and_terminal_guards():
    validator = validators()["page.schema"]
    page = {
        "schema_version": "legal-tender.fec.funding-receipt-page.v1",
        "calculation_id": "a" * 64,
        "input": {"fact_set_id": "b" * 64, "manifest_sha256": "c" * 64, "facts": 1, "shards": 1},
        "committee_id": "C00000001", "component": "", "after_source_row_ordinal": 0,
        "receipts": [], "next_after_source_row_ordinal": None, "has_more": False,
        "terminal_attribution_eligible": False,
    }
    validator.validate(page)
    bad = copy.deepcopy(page)
    bad["terminal_attribution_eligible"] = True
    assert list(validator.iter_errors(bad))
    measures_schema = validators()["result.schema"].schema["$defs"]["measures"]
    measures = {k: 0 for k in measures_schema["required"]}
    for k in ("signed_minor_units", "positive_minor_units", "negative_minor_units"):
        measures[k] = "0"
    validator = Draft202012Validator(measures_schema)
    validator.validate(measures)
    measures["signed_minor_units"] = 0.1
    assert list(validator.iter_errors(measures))


def test_report_association_wire_guards():
    schema = validators()["report.schema"].schema["properties"]["associations"]["items"]
    validator = Draft202012Validator(schema)
    association = {
        "earmark_source_row_ordinal": 1, "state": "reported_earmark_memo_association",
        "related_source_row_ordinals": [2], "reported_conduit_committee_id": "C00000003",
        "related_amount_comparison": "different_reported_amount",
        "additional_amount_minor_units": "0", "terminal_attribution_eligible": False,
    }
    validator.validate(association)
    for key, value in (("additional_amount_minor_units", "200"), ("terminal_attribution_eligible", True),
                       ("reported_conduit_committee_id", None), ("related_source_row_ordinals", [2, 3]),
                       ("state", "related_role_unresolved"), ("related_amount_comparison", "not_assessed")):
        bad = copy.deepcopy(association)
        bad[key] = value
        assert list(validator.iter_errors(bad))
