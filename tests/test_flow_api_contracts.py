"""HTTP wire contracts only; source policy remains in Go."""

import copy
import json
from pathlib import Path

import pytest

from orchestration.go_process import GoCommandError, _decode_and_validate

CONTRACTS = Path(__file__).resolve().parent.parent / "contracts"
BASE = CONTRACTS / "api/committee-flow/v1"


def validate(value):
    return _decode_and_validate(
        json.dumps(value).encode(), BASE / "response.schema.json", None, CONTRACTS
    )


def test_api_preserves_exact_observations_and_partial_coverage():
    fixture = json.loads((BASE / "fixtures/observation-page.json").read_text())
    assert validate(fixture) == fixture
    for key in fixture["projection"]:
        bad = copy.deepcopy(fixture)
        del bad["projection"][key]
        with pytest.raises(GoCommandError):
            validate(bad)
    for key, value in (
        ("signed_amount_minor_units", 9007199254740992.0),
        ("terminal_attribution_eligible", True),
        ("economic_flow_status", "resolved_payment"),
        ("ledger", "combined"),
    ):
        bad = copy.deepcopy(fixture)
        bad["data"]["items"][0][key] = value
        with pytest.raises(GoCommandError):
            validate(bad)


def test_api_errors_do_not_admit_internal_details():
    assert validate({"error": "evidence_unavailable"})
    with pytest.raises(GoCommandError):
        validate({"error": "evidence_unavailable", "internal_error": "private"})
