"""Focused machine-contract checks for the first Go receipt calculation."""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
CONTRACTS_ROOT = REPOSITORY_ROOT / "contracts"
CALCULATION_ROOT = (
    CONTRACTS_ROOT
    / "calculations"
    / "fec"
    / "candidate-itemized-individual-receipts"
    / "v1"
)
EFFECTIVE_IE_ROOT = (
    CONTRACTS_ROOT
    / "calculations"
    / "fec"
    / "effective-independent-expenditures"
    / "v1"
)
CANDIDATE_RESOLUTION_ROOT = (
    CONTRACTS_ROOT
    / "calculations"
    / "fec"
    / "independent-expenditure-candidate-resolution"
    / "v1"
)
RESOLVED_IE_ROOT = (
    CONTRACTS_ROOT
    / "calculations"
    / "fec"
    / "resolved-independent-expenditures"
    / "v1"
)
COMMITTEE_FLOW_ROOT = (
    CONTRACTS_ROOT
    / "calculations"
    / "fec"
    / "receiver-reported-committee-flows"
    / "v1"
)
COMMITTEE_IDENTITY_ROOT = (
    CONTRACTS_ROOT
    / "calculations"
    / "fec"
    / "receiver-flow-committee-identity-coverage"
    / "v1"
)


def _json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _registry() -> Registry:
    resources = []
    for path in CONTRACTS_ROOT.rglob("*.json"):
        document = _json(path)
        if not isinstance(document, dict):
            continue
        if document.get("$schema") and (identifier := document.get("$id")):
            resources.append((str(identifier), Resource.from_contents(document)))
    return Registry().with_resources(resources)


def test_candidate_upstream_wire_guards() -> None:
    schema = _json(CONTRACTS_ROOT / "calculations/fec/candidate-committee-upstream/v1/result.schema.json")
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema, registry=_registry())
    fact = {"fact_set_id": "a" * 64, "manifest_sha256": "b" * 64,
            "source_release_id": "fec-" + "c" * 64,
            "source_artifact_sha256": "d" * 64, "facts": 1}
    zero = {"records": 0, "positive_records": 0, "negative_records": 0,
            "zero_records": 0, "signed_minor_units": "0",
            "positive_minor_units": "0", "negative_minor_units": "0"}
    # Wire-only synthetic zero-cohort example. This is not a published result
    # identity or a statement that the candidate's complete receipts are zero.
    value = {
        "schema_version": schema["properties"]["schema_version"]["const"],
        "calculation_id": "e" * 64,
        "cycle": "2024", "candidate_id": "H0AA00001",
        "inputs": {"bundle_id": "f" * 64, "bundle_sha256": "a" * 64,
                   "reconciliation": {"calculation_set_id": "e" * 64, "manifest_sha256": "b" * 64},
                   "sources": {"release_id": fact["source_release_id"], "release_sha256": "c" * 64,
                               "schedule_a": fact, "schedule_b": fact},
                   "committee_master": fact, "linkages": fact},
        "committee_relationships": [{"committee_id": "C00000001", "state": "authorized",
                                     "designation_codes": ["P"], "supporting_fact_ids": ["link"]}],
        "accounting": {key: copy.deepcopy(zero) for key in (
            "candidate_linked_observations", "external_to_authorized_scope",
            "within_authorized_scope", "unresolved_authorization_boundary", "unresolved_attribution")},
        "candidate_observations": [], "upstream_source_row_ordinals": [], "cyclic_components": [],
        "nodes": [{"committee_id": "C00000001", "candidate_authorized": True,
                   "master_fact_id": None, "minimum_hops_to_authorized_scope": 0,
                   "witness_source_row_ordinal": None, "selected_incoming_observations": 0,
                   "cyclic_component_id": None,
                   "attribution_boundary_reasons": ["missing_same_cycle_committee_master"],
                   "terminal_attribution_eligible": False}],
    }
    for key in ("policy", "state", "scope", "ledger", "time_semantics", "attribution_state",
                "terminal_attribution_eligible", "not_covered"):
        value[key] = copy.deepcopy(schema["properties"][key]["const"])
    value["accounting"]["terminal_allocated_minor_units"] = "0"
    validator.validate(value)
    for key in value:
        bad = copy.deepcopy(value)
        del bad[key]
        assert list(validator.iter_errors(bad)), key
    for key, changed in (("ledger", "schedule_b"), ("terminal_attribution_eligible", True),
                         ("scope", "complete_candidate_receipts"), ("not_covered", []),
                         ("attribution_state", "complete"), ("extra", "unknown")):
        bad = copy.deepcopy(value)
        bad[key] = changed
        assert list(validator.iter_errors(bad)), key
    bad = copy.deepcopy(value)
    del bad["nodes"][0]["terminal_attribution_eligible"]
    assert list(validator.iter_errors(bad))
    bad = copy.deepcopy(value)
    bad["accounting"]["terminal_allocated_minor_units"] = "1"
    assert list(validator.iter_errors(bad))
    bad = copy.deepcopy(value)
    bad["accounting"]["external_to_authorized_scope"]["signed_minor_units"] = 0.0
    assert list(validator.iter_errors(bad))


def test_committee_flow_evidence_readiness_contract() -> None:
    schema = _json(CONTRACTS_ROOT / "bundles/fec/committee-flow-evidence/v1/manifest.schema.json")
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema, registry=_registry())
    fact = {
        "fact_set_id": "a" * 64,
        "manifest_sha256": "b" * 64,
        "source_release_id": "fec-" + "c" * 64,
        "source_artifact_sha256": "d" * 64,
        "facts": 1,
    }
    bundle = {
        "schema_version": "legal-tender.fec.committee-flow-evidence-bundle.v1",
        "bundle_id": "e" * 64,
        "cycle": "2024",
        "state": "ready",
        "consumer": "committee_flow_evidence",
        "calculation": {"calculation_set_id": "a" * 64, "manifest_sha256": "f" * 64},
        "input": {"release_id": fact["source_release_id"], "release_sha256": "b" * 64, "schedule_a": fact, "schedule_b": fact},
        "committee_master": fact,
        "identity_scope": "same_cycle_master_only",
        "economic_flow_eligible": False,
        "checks": [item["const"] for item in schema["properties"]["checks"]["prefixItems"]],
    }
    validator.validate(bundle)
    for key, value in (
        ("economic_flow_eligible", True),
        ("consumer", "combined_money_graph"),
        ("identity_scope", "automatic_history"),
        ("cycle", "2023"),
        ("checks", bundle["checks"][:-1]),
        ("checks", list(reversed(bundle["checks"]))),
        ("extra", "unknown"),
    ):
        bad = copy.deepcopy(bundle)
        bad[key] = value
        assert list(validator.iter_errors(bad)), key
    for key in bundle:
        bad = copy.deepcopy(bundle)
        del bad[key]
        assert list(validator.iter_errors(bad)), key


def test_candidate_receipt_calculation_contract_and_result_fixture() -> None:
    contract_schema = _json(CONTRACTS_ROOT / "calculation-contract.schema.json")
    Draft202012Validator.check_schema(contract_schema)
    Draft202012Validator(contract_schema, format_checker=FormatChecker()).validate(
        _json(CALCULATION_ROOT / "contract.json")
    )

    result_schema = _json(CALCULATION_ROOT / "result.schema.json")
    Draft202012Validator.check_schema(result_schema)
    Draft202012Validator(
        result_schema,
        registry=_registry(),
        format_checker=FormatChecker(),
    ).validate(_json(CALCULATION_ROOT / "fixtures" / "resolved-summary-gap.json"))


def test_disbursement_reporting_contract_and_schema() -> None:
    root = (
        CONTRACTS_ROOT / "calculations" / "fec" / "processed-disbursement-reporting" / "v1"
    )
    contract = _json(root / "contract.json")
    Draft202012Validator(
        _json(CONTRACTS_ROOT / "calculation-contract.schema.json"),
        format_checker=FormatChecker(),
    ).validate(contract)
    schema = _json(root / "result.schema.json")
    Draft202012Validator.check_schema(schema)
    assert schema["properties"]["graph_eligible"] == {"const": False}
    for fixture in contract["fixtures"]:
        fixture_digest = hashlib.sha256((root / fixture["path"]).read_bytes()).hexdigest()
        assert fixture_digest == fixture["sha256"]
        for path in fixture["input_paths"]:
            assert (root / path).is_file()
    assert set(schema["properties"]["decisions"]["required"]) == {
        state["id"] for state in contract["method"]["decision_states"]
    }


def test_committee_flow_reconciliation_contracts() -> None:
    root = CONTRACTS_ROOT / "calculations/fec/committee-flow-reconciliation/v1"
    contract = _json(root / "contract.json")
    Draft202012Validator(_json(CONTRACTS_ROOT / "calculation-contract.schema.json")).validate(contract)
    for name in ("result.schema.json", "observation.schema.json", "assertion.schema.json"):
        Draft202012Validator.check_schema(_json(root / name))
    result = _json(root / "result.schema.json")
    assert result["properties"]["graph_eligible"] == {"const": False}
    assertion = _json(root / "assertion.schema.json")
    assert set(assertion["properties"]["state"]["enum"]) == {
        state["id"] for state in contract["method"]["decision_states"]
    }
    observation = Draft202012Validator(_json(root / "observation.schema.json"))
    for fixture in contract["fixtures"]:
        path = root / fixture["path"]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == fixture["sha256"]
        for case in _json(path):
            for row in case["a"] + case["b"]:
                observation.validate(row)
            assert set(case["states"]) <= set(assertion["properties"]["state"]["enum"])


def test_committee_flow_review_schema() -> None:
    root = CONTRACTS_ROOT / "audits/fec/committee-flow-review/v1"
    schema = _json(root / "result.schema.json")
    Draft202012Validator.check_schema(schema)
    assert schema["properties"]["graph_eligible"] == {"const": False}
    validator = Draft202012Validator(schema, registry=_registry())
    fixture = {
        "schema_version": "legal-tender.fec.committee-flow-review.v1",
        "result_sha256": "a" * 64,
        "calculation_set_id": "b" * 64,
        "shapes": [], "one_to_one_date_differences": [], "examples": [],
        "source_examples": [], "verified_source_shards": 0, "graph_eligible": False,
    }
    validator.validate(fixture)
    fixture["graph_eligible"] = True
    assert not validator.is_valid(fixture)


def test_receipt_decision_schema_preserves_one_explicit_state() -> None:
    schema = _json(CALCULATION_ROOT / "decision.schema.json")
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(
        {
            "schema_version": (
                "legal-tender.fec.itemized-individual-receipt-decision.v1"
            ),
            "calculation_contract": "fec/candidate-itemized-individual-receipts",
            "calculation_version": "1.0.0",
            "fact_id": "fact-1",
            "natural_key": "schedule-a:2024:123",
            "cycle": "2024",
            "recipient_committee_id": "C00000005",
            "received_on": "2024-01-15",
            "state": "included",
            "amount_minor_units": "-25",
        }
    )


def test_schedule_a_columnar_manifest_and_physical_contracts() -> None:
    root = CONTRACTS_ROOT / "facts" / "fec" / "schedule-a" / "columnar" / "v1"
    manifest_schema = _json(root / "manifest.schema.json")
    Draft202012Validator.check_schema(manifest_schema)

    physical = _json(root / "physical-schema.json")
    assert physical["schema_version"] == "legal-tender.fec.schedule-a-parquet.v1"
    assert physical["column_count"] == 99
    assert physical["source_columns"]["count"] == 81
    assert len(physical["added_columns"]) == 18


def test_schedule_a_compact_occurrence_contracts() -> None:
    root = CONTRACTS_ROOT / "evidence" / "fec" / "schedule-a" / "compact" / "v1"
    for name in (
        "manifest.schema.json",
        "row-exception.schema.json",
        "key-exception.schema.json",
        "delta.schema.json",
    ):
        Draft202012Validator.check_schema(_json(root / name))


def test_compact_candidate_receipt_membership_contracts() -> None:
    root = CALCULATION_ROOT.parent / "compact" / "v1"
    for name in ("manifest.schema.json", "exception.schema.json"):
        Draft202012Validator.check_schema(_json(root / name))


def test_effective_independent_expenditure_contracts_and_fixture_digest() -> None:
    contract = _json(EFFECTIVE_IE_ROOT / "contract.json")
    contract_schema = _json(CONTRACTS_ROOT / "calculation-contract.schema.json")
    Draft202012Validator(contract_schema, format_checker=FormatChecker()).validate(
        contract
    )

    for name in ("manifest.schema.json", "result.schema.json", "exception.schema.json"):
        Draft202012Validator.check_schema(_json(EFFECTIVE_IE_ROOT / name))

    fixture_path = EFFECTIVE_IE_ROOT / "fixtures" / "policy-cases.json"
    fixture_digest = hashlib.sha256(fixture_path.read_bytes()).hexdigest()
    assert contract["fixtures"][0]["sha256"] == fixture_digest

    fixture = _json(fixture_path)
    expected = fixture["expected"]
    assert expected["decision_counts"] == {
        "source_facts": 6,
        "included": 4,
        "excluded_memo": 1,
        "excluded_memo_amount_unresolved": 0,
        "unresolved_amount": 1,
    }
    assert int(expected["amounts"]["included_minor_units"]) == (
        int(expected["amounts"]["attributed_minor_units"])
        + int(expected["amounts"]["unattributed_minor_units"])
    )


def test_independent_expenditure_candidate_resolution_contracts() -> None:
    contract = _json(CANDIDATE_RESOLUTION_ROOT / "contract.json")
    contract_schema = _json(CONTRACTS_ROOT / "calculation-contract.schema.json")
    Draft202012Validator(
        contract_schema, format_checker=FormatChecker()
    ).validate(contract)

    for name in ("manifest.schema.json", "decision.schema.json"):
        Draft202012Validator.check_schema(_json(CANDIDATE_RESOLUTION_ROOT / name))

    fixture_path = CANDIDATE_RESOLUTION_ROOT / "fixtures" / "policy-cases.json"
    assert (
        contract["fixtures"][0]["sha256"]
        == hashlib.sha256(fixture_path.read_bytes()).hexdigest()
    )

    expected = _json(fixture_path)["expected"]
    assert expected["counts"] == {
        "source_effective_facts": 6,
        "confirmed": 1,
        "resolved": 1,
        "unverified": 1,
        "ambiguous": 1,
        "unresolved": 2,
    }
    amounts = expected["amounts"]
    assert int(amounts["source_effective_minor_units"]) == sum(
        int(amounts[field])
        for field in (
            "confirmed_minor_units",
            "resolved_minor_units",
            "unverified_minor_units",
            "ambiguous_minor_units",
            "unresolved_minor_units",
        )
    )


def test_resolved_independent_expenditure_contracts() -> None:
    contract = _json(RESOLVED_IE_ROOT / "contract.json")
    contract_schema = _json(CONTRACTS_ROOT / "calculation-contract.schema.json")
    Draft202012Validator(
        contract_schema, format_checker=FormatChecker()
    ).validate(contract)

    for name in ("manifest.schema.json", "result.schema.json", "exception.schema.json"):
        Draft202012Validator.check_schema(_json(RESOLVED_IE_ROOT / name))

    fixture_path = RESOLVED_IE_ROOT / "fixtures" / "policy-cases.json"
    assert (
        contract["fixtures"][0]["sha256"]
        == hashlib.sha256(fixture_path.read_bytes()).hexdigest()
    )
    expected = _json(fixture_path)["expected"]
    counts = expected["counts"]
    assert counts["source_decisions"] == (
        counts["projectable_decisions"] + counts["unprojectable_decisions"]
    )
    amounts = expected["amounts"]
    assert int(amounts["source_minor_units"]) == (
        int(amounts["projectable_minor_units"])
        + int(amounts["unprojectable_minor_units"])
    )
    result = expected["results"][0]
    assert int(result["signed_amount_minor_units"]) == sum(
        int(result["resolution_amounts"][field])
        for field in (
            "confirmed_minor_units",
            "resolved_minor_units",
            "unverified_minor_units",
        )
    )


def test_receiver_reported_committee_flow_contracts() -> None:
    contract = _json(COMMITTEE_FLOW_ROOT / "contract.json")
    contract_schema = _json(CONTRACTS_ROOT / "calculation-contract.schema.json")
    Draft202012Validator(
        contract_schema, format_checker=FormatChecker()
    ).validate(contract)

    for name in ("manifest.schema.json", "result.schema.json", "exception.schema.json"):
        Draft202012Validator.check_schema(_json(COMMITTEE_FLOW_ROOT / name))

    fixture_path = COMMITTEE_FLOW_ROOT / "fixtures" / "policy-cases.json"
    assert (
        contract["fixtures"][0]["sha256"]
        == hashlib.sha256(fixture_path.read_bytes()).hexdigest()
    )
    expected = _json(fixture_path)["expected"]
    counts = expected["decision_counts"]
    assert counts["source_facts"] == sum(
        count for state, count in counts.items() if state != "source_facts"
    )
    amounts = expected["amounts"]
    assert int(amounts["known_source_minor_units"]) == sum(
        int(amounts[field])
        for field in (
            "included_minor_units",
            "excluded_minor_units",
            "unresolved_minor_units",
        )
    )
    assert int(amounts["included_minor_units"]) == sum(
        int(result["amount_minor_units"]) for result in expected["results"]
    )


def test_receiver_flow_committee_identity_coverage_contracts() -> None:
    contract = _json(COMMITTEE_IDENTITY_ROOT / "contract.json")
    contract_schema = _json(CONTRACTS_ROOT / "calculation-contract.schema.json")
    Draft202012Validator(
        contract_schema, format_checker=FormatChecker()
    ).validate(contract)

    for name in ("manifest.schema.json", "decision.schema.json"):
        Draft202012Validator.check_schema(_json(COMMITTEE_IDENTITY_ROOT / name))

    fixture_path = COMMITTEE_IDENTITY_ROOT / "fixtures" / "policy-cases.json"
    assert (
        contract["fixtures"][0]["sha256"]
        == hashlib.sha256(fixture_path.read_bytes()).hexdigest()
    )
    expected = _json(fixture_path)["expected"]
    assert expected["decisions"] == (
        expected["historical_registrations"]
        + expected["alternate_release_registrations"]
        + expected["unresolved_reported_ids"]
    )
    assert expected["terminal_identity_eligible"] == 0
    assert expected["terminal_identity_ineligible"] == expected["decisions"]

    decision_schema = _json(COMMITTEE_IDENTITY_ROOT / "decision.schema.json")
    Draft202012Validator(decision_schema).validate(
        {
            "schema_version": (
                "legal-tender.fec.receiver-flow-committee-identity-coverage.v1"
            ),
            "decision_id": "1" * 64,
            "calculation_set_id": "2" * 64,
            "cycle": "2024",
            "committee_id": "C00123456",
            "state": "historical_registration",
            "endpoint_roles": ["source"],
            "terminal_identity_eligible": False,
            "evidence_codes": [
                "exact_reported_id_in_official_historical_master"
            ],
            "same_cycle_comparison_assertions": [],
            "historical_assertions": [
                {
                    "cycle": "2018",
                    "source_kind": "official_bulk_archive",
                    "archive_sha256": "3" * 64,
                    "source_row": 7,
                    "source_row_sha256": "4" * 64,
                    "issue_codes": [],
                    "name": "HISTORICAL COMMITTEE",
                    "party_affiliation": "",
                    "designation_code": "U",
                    "committee_type_code": "Q",
                    "organization_type_code": "C",
                    "connected_organization": "",
                }
            ],
        }
    )


def test_candidate_receipt_fact_bundle_contract() -> None:
    root = (
        CONTRACTS_ROOT
        / "bundles"
        / "fec"
        / "candidate-itemized-individual-receipts"
        / "v1"
    )
    Draft202012Validator.check_schema(_json(root / "manifest.schema.json"))


def test_independent_expenditure_projection_bundle_contract() -> None:
    root = (
        CONTRACTS_ROOT
        / "bundles"
        / "fec"
        / "independent-expenditure-projection"
        / "v1"
    )
    schema = _json(root / "manifest.schema.json")
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(
        _json(root / "fixtures" / "ready-2024.json")
    )


def test_receiver_reported_committee_flow_projection_bundle_contract() -> None:
    root = (
        CONTRACTS_ROOT
        / "bundles"
        / "fec"
        / "receiver-reported-committee-flow-projection"
        / "v1"
    )
    Draft202012Validator.check_schema(_json(root / "manifest.schema.json"))

    v2_root = root.parent / "v2"
    Draft202012Validator.check_schema(_json(v2_root / "manifest.schema.json"))


def test_resolved_independent_expenditure_projection_bundle_contract() -> None:
    root = (
        CONTRACTS_ROOT
        / "bundles"
        / "fec"
        / "resolved-independent-expenditure-projection"
        / "v1"
    )
    schema = _json(root / "manifest.schema.json")
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(
        _json(root / "fixtures" / "ready-2024.json")
    )


def test_arango_candidate_receipt_projection_contract() -> None:
    root = (
        CONTRACTS_ROOT
        / "projections"
        / "arango"
        / "candidate-receipts"
        / "v1"
    )
    schema = _json(root / "result.schema.json")
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(
        _json(root / "fixtures" / "partial-2024.json")
    )


def test_arango_independent_expenditure_projection_contract() -> None:
    root = (
        CONTRACTS_ROOT
        / "projections"
        / "arango"
        / "independent-expenditures"
        / "v1"
    )
    schema = _json(root / "result.schema.json")
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(
        _json(root / "fixtures" / "partial-2024.json")
    )

    resolved_root = root.parent / "v2"
    resolved_schema = _json(resolved_root / "result.schema.json")
    Draft202012Validator.check_schema(resolved_schema)
    Draft202012Validator(
        resolved_schema, format_checker=FormatChecker()
    ).validate(_json(resolved_root / "fixtures" / "partial-2024.json"))


def test_arango_receiver_reported_committee_flow_projection_contract() -> None:
    root = (
        CONTRACTS_ROOT
        / "projections"
        / "arango"
        / "receiver-reported-committee-flows"
        / "v1"
    )
    Draft202012Validator.check_schema(_json(root / "result.schema.json"))
    Draft202012Validator.check_schema(_json(root.parent / "v2" / "result.schema.json"))
