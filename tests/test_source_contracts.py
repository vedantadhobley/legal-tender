import hashlib
import json
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parents[1]
CONTRACTS = ROOT / "contracts"


def _json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def test_all_source_contract_metadata_matches_shared_schema():
    schema = _json(CONTRACTS / "source-contract.schema.json")
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    paths = sorted((CONTRACTS / "sources").glob("**/contract.json"))
    assert paths
    for path in paths:
        errors = sorted(validator.iter_errors(_json(path)), key=lambda error: list(error.path))
        assert not errors, f"{path.relative_to(ROOT)}: {[error.message for error in errors]}"


def test_processed_schedule_contract_maturity_is_explicit():
    schedule_a = _json(CONTRACTS / "sources/fec/schedule-a/v1/contract.json")
    schedule_b = _json(CONTRACTS / "sources/fec/schedule-b/v1/contract.json")
    schedule_e = _json(CONTRACTS / "sources/fec/schedule-e/v1/contract.json")

    assert schedule_a["status"] == "draft"
    assert any(
        check["id"] == "memo_conduit_coverage" and check["severity"] == "partial"
        for check in schedule_a["quality"]["checks"]
    )
    assert schedule_b["status"] == "accepted"
    assert {
        check["id"]
        for check in schedule_b["quality"]["checks"]
        if check["severity"] == "block"
    } >= {"artifact_identity", "classic_overlap", "schedule_a_alignment"}
    assert schedule_e["status"] == "accepted"
    assert any(
        check["id"] == "effective_calculation" and check["severity"] == "observe"
        for check in schedule_e["quality"]["checks"]
    )


def test_wikimedia_organization_source_fixtures():
    _validate_wikimedia_discovery_fixtures("organization-candidates")


def test_wikimedia_affiliation_discovery_source_fixtures():
    _validate_wikimedia_discovery_fixtures("affiliation-discovery")


def _validate_wikimedia_discovery_fixtures(dataset):
    root = CONTRACTS / f"sources/wikimedia/{dataset}/v1"
    contract = _json(root / "contract.json")
    assert contract["status"] == "draft"
    Draft202012Validator(
        _json(CONTRACTS / "source-contract.schema.json"),
        format_checker=FormatChecker(),
    ).validate(contract)
    schema = _json(root / contract["physical_schema"]["record_schema"])
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    for fixture in contract["fixtures"]:
        path = root / fixture["path"]
        errors = list(validator.iter_errors(_json(path)))
        assert not errors, [error.message for error in errors]


def test_all_audit_result_schemas_are_valid():
    paths = sorted((CONTRACTS / "audits").glob("**/result.schema.json"))
    assert paths
    for path in paths:
        schema = _json(path)
        Draft202012Validator.check_schema(schema)


def test_wikimedia_role_statement_contract():
    for dataset in ("role-statements", "relationship-statements"):
        root = CONTRACTS / f"sources/wikimedia/{dataset}/v1"
        contract = _json(root / "contract.json")
        assert contract["status"] == "draft"
        Draft202012Validator(
            _json(CONTRACTS / "source-contract.schema.json"),
            format_checker=FormatChecker(),
        ).validate(contract)
        assert not contract["semantics"]["money_fields"]
        for fixture in contract["fixtures"]:
            raw = (root / fixture["path"]).read_bytes()
            assert hashlib.sha256(raw).hexdigest() == fixture["sha256"]


def test_gleif_organization_registry_source_fixture():
    root = CONTRACTS / "sources/gleif/lei-record/v1"
    contract = _json(root / "contract.json")
    assert contract["status"] == "draft"
    Draft202012Validator(
        _json(CONTRACTS / "source-contract.schema.json"),
        format_checker=FormatChecker(),
    ).validate(contract)
    schema = _json(root / "record.schema.json")
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    for fixture in contract["fixtures"]:
        validator.validate(_json(root / fixture["path"]))


def test_gleif_name_search_source_fixtures():
    root = CONTRACTS / "sources/gleif/name-search/v1"
    contract = _json(root / "contract.json")
    Draft202012Validator(
        _json(CONTRACTS / "source-contract.schema.json"),
        format_checker=FormatChecker(),
    ).validate(contract)
    assert contract["status"] == "draft"
    assert contract["physical_schema"]["record_schema"] is None  # Compiled Go page profile.
    for fixture in contract["fixtures"]:
        body = (root / fixture["path"]).read_bytes()
        assert hashlib.sha256(body).hexdigest() == fixture["sha256"]


def test_company_html_evidence_source_fixtures():
    root = CONTRACTS / "sources/company/html-evidence/v1"
    contract = _json(root / "contract.json")
    Draft202012Validator(
        _json(CONTRACTS / "source-contract.schema.json"),
        format_checker=FormatChecker(),
    ).validate(contract)
    assert contract["status"] == "draft"
    assert not contract["semantics"]["money_fields"]
    for fixture in contract["fixtures"]:
        body = (root / fixture["path"]).read_bytes()
        assert hashlib.sha256(body).hexdigest() == fixture["sha256"]


def test_sec_issuer_directory_source_fixture():
    root = CONTRACTS / "sources/sec/company-tickers/v1"
    contract = _json(root / "contract.json")
    assert contract["status"] == "draft"
    Draft202012Validator(
        _json(CONTRACTS / "source-contract.schema.json"),
        format_checker=FormatChecker(),
    ).validate(contract)
    schema = _json(root / "record.schema.json")
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    for fixture in contract["fixtures"]:
        path = root / fixture["path"]
        validator.validate(_json(path))
        if "sha256" in fixture:
            assert hashlib.sha256(path.read_bytes()).hexdigest() == fixture["sha256"]
    for bad in [None, {}, {"0": None}, {"0": {"cik_str": 1, "ticker": "X"}}]:
        assert not validator.is_valid(bad)


def test_sec_issuer_filing_source_fixture():
    root = CONTRACTS / "sources/sec/inline-registrant-identity/v1"
    contract = _json(root / "contract.json")
    Draft202012Validator(
        _json(CONTRACTS / "source-contract.schema.json"),
        format_checker=FormatChecker(),
    ).validate(contract)
    assert contract["status"] == "draft"
    assert contract["physical_schema"]["media_type"] == "text/html"
    assert contract["physical_schema"]["record_schema"] is None  # Go XML profile, not JSON
    for fixture in contract["fixtures"]:
        assert fixture["representation"] == "exact_bytes"
        body = (root / fixture["path"]).read_bytes()
        assert hashlib.sha256(body).hexdigest() == fixture["sha256"]


def test_schedule_b_columnar_fact_contract_schemas_are_valid():
    root = CONTRACTS / "facts/fec/schedule-b/columnar/v1"
    physical = _json(root / "physical-schema.json")
    assert physical["column_count"] == 98
    assert physical["source_columns"]["count"] == 81
    schema = _json(root / "manifest.schema.json")
    Draft202012Validator.check_schema(schema)


def test_fec_release_v3_contract_schemas_and_inventory_are_valid():
    root = CONTRACTS / "releases/fec/v3"
    for path in sorted(root.glob("*.schema.json")):
        Draft202012Validator.check_schema(_json(path))
    inventory_schema = _json(root / "inventory.schema.json")
    errors = sorted(
        Draft202012Validator(
            inventory_schema, format_checker=FormatChecker()
        ).iter_errors(_json(root / "inventory.json")),
        key=lambda error: list(error.path),
    )
    assert not errors, [error.message for error in errors]
