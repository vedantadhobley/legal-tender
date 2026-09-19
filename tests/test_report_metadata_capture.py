"""Independent bounded HTTP evidence verification; test-only Python, no network."""

import hashlib
import json
import os
from decimal import Decimal
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource

ROOT = Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "contracts/sources/fec/report-metadata/v1"
AUDIT = os.environ.get("LT_REPORT_METADATA_CAPTURE_AUDIT")


def read(path):
    return json.loads(path.read_bytes(), parse_float=Decimal)


def validator(name):
    schemas = [read(CONTRACT / n) for n in (
        "request.schema.json", "fetch-result.schema.json", "capture.schema.json",
    )]
    registry = Registry().with_resources((s["$id"], Resource.from_contents(s)) for s in schemas)
    return Draft202012Validator(read(CONTRACT / name), registry=registry, format_checker=FormatChecker())


def test_capture_schemas_and_closed_budgets():
    for name in ("request.schema.json", "fetch-result.schema.json"):
        Draft202012Validator.check_schema(read(CONTRACT / name))
    request = {
        "contract": "fec/report-metadata@1.0.0",
        "schema_sha256": "0cca7f2af73270b35d7276e53d99b56b2f8f9970b33cb5d36a85fdb8b773caed",
        "endpoint": "/v1/filings/", "query": {"file_numbers": [101], "per_page": 100},
        "limits": {"pages": 3, "requests": 4, "attempts_per_page": 2, "source_bytes": 2097152},
    }
    check = validator("request.schema.json")
    check.validate(request)
    assert not check.is_valid({**request, "api_key": "not-allowed"})
    assert not check.is_valid({**request, "endpoint": "/v1/reports/pac-party/"})
    assert not check.is_valid({**request, "limits": {**request["limits"], "requests": 49}})


def artifact(root, ref):
    path = root / ref["path"]
    assert path.resolve().is_relative_to(root.resolve())
    b = path.read_bytes()
    assert len(b) == ref["bytes"] and hashlib.sha256(b).hexdigest() == ref["sha256"]
    return b


@pytest.mark.skipif(not AUDIT, reason="requires bounded live Go capture evidence")
def test_live_capture_complete_readback_and_replay():
    audit = Path(AUDIT)
    root = audit / "capture"
    result = read(root / "result.json")
    validator("fetch-result.schema.json").validate(result)
    assert result == read(audit / "capture-result.json")
    assert result["state"] in {"captured", "blocked", "incomplete"}
    assert not result["history_complete"] and not result["financial_selection_ready"]
    assert result["request"] == read(audit / "request.json") == read(root / "request.json")
    completed = result["state"] == "captured"
    if completed:
        capture = json.loads(artifact(root, result["capture"]))
        reviewed = json.loads(artifact(root, result["review"]), parse_float=Decimal)
        assert reviewed["capture_sha256"] == result["capture"]["sha256"]
    else:
        assert result["capture"] is None and result["review"] is None
        assert not (root / "capture.json").exists() and not (root / "review.json").exists()
        capture_path = max(root.glob("checkpoint-*.json"))
        capture = read(capture_path)
        reviewed = read(root / capture_path.name.replace("checkpoint-", "review-"))
        assert reviewed["capture_sha256"] == hashlib.sha256(capture_path.read_bytes()).hexdigest()
    validator("capture.schema.json").validate(capture)
    assert reviewed == read(audit / "replay.json")
    assert reviewed["pagination_state"] == ("empty_page_observed" if completed else "exact_count_satisfied")
    assert reviewed["issues"] == []
    assert reviewed["missing_requested_files"] == []
    assert not reviewed["history_complete"] and not reviewed["financial_selection_ready"]
    rows = []
    for page, output in zip(capture["pages"], reviewed["pages"], strict=True):
        raw = artifact(root, page["body"])
        artifact(root, page["headers"])
        original = json.loads(raw, parse_float=Decimal)
        assert [r["raw"] for r in output["records"]] == original["results"]
        assert output["pagination"] == original["pagination"]
        assert page["time_basis"] == "client_clock"
        rows.extend(original["results"])
    if completed:
        assert original["results"] == []
    assert len(rows) == reviewed["rows"] == len(capture["query"]["file_numbers"])
    assert {r["file_number"] for r in rows} == set(capture["query"]["file_numbers"])
    assert result["bytes_read"] == sum(a["bytes_read"] for a in result["attempts"])
    assert result["bytes_read"] <= result["request"]["limits"]["source_bytes"]
    assert len(result["attempts"]) <= result["request"]["limits"]["requests"]
    for sequence, attempt in enumerate(result["attempts"], 1):
        assert sequence == attempt["sequence"]
        assert read(root / f"attempt-{sequence:03}.json") == attempt
        start = read(root / f"attempt-{sequence:03}-start.json")
        assert start["outcome"] == "started" and start["url"] == attempt["url"]
        query = parse_qs(urlsplit(attempt["url"]).query)
        assert set(query) == {"file_number", "per_page", "page"}
        assert {int(v) for v in query["file_number"]} == set(capture["query"]["file_numbers"])
        for kind in ("body", "headers"):
            if attempt[kind] is not None:
                artifact(root, attempt[kind])
    # Public test key must still not leak into retained requests or responses.
    for path in root.iterdir():
        assert b"DEMO_KEY" not in path.read_bytes()
    for name in ("replay", "go-gates", "go-test", "go-vet", "go-race", "build"):
        assert (audit / f"{name}.exit").read_text().strip() == "0"
    assert (audit / "capture.exit").read_text().strip() == ("0" if completed else "1")
    if completed:
        assert (audit / "live-gate.exit").read_text().strip() == "0"
    else:
        assert not (audit / "live-gate.exit").exists()
        assert result["reason"] == "credential_echo_suppressed"
        last = result["attempts"][-1]
        assert last["status"] == 429 and last["body"] is None and last["headers"] is None
