"""Independent contract/readback checks; no runtime Python or network."""

import hashlib
import json
import os
import re
from decimal import Decimal
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "contracts/sources/fec/report-metadata/v1"
AUDIT = os.environ.get("LT_REPORT_METADATA_READER_AUDIT")


def read(path):
    return json.loads(path.read_bytes(), parse_float=Decimal)


def validator(path):
    schema = read(path)
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, format_checker=FormatChecker())


def test_metadata_contracts_and_synthetic_fixture():
    for name in ("capture.schema.json", "record.schema.json", "page.schema.json"):
        Draft202012Validator.check_schema(read(CONTRACT / name))
    validator(ROOT / "contracts/source-contract.schema.json").validate(read(CONTRACT / "contract.json"))
    validator(CONTRACT / "record.schema.json").validate(read(CONTRACT / "fixtures/filings-record.json"))
    validator(ROOT / "contracts/audits/fec/report-metadata/v1/result.schema.json")
    assert read(CONTRACT / "contract.json")["status"] == "draft"  # History/refresh gate remains open.


@pytest.mark.skipif(not AUDIT, reason="requires retained offline Go metadata review")
@pytest.mark.parametrize("stem,rows", [("filing-witnesses", 7), ("sid-reports", 10), ("nrcc-reports", 34)])
def test_complete_original_value_and_result_readback(stem, rows):
    audit = Path(AUDIT)
    capture_path = audit / f"{stem}-capture.json"
    capture = read(capture_path)
    validator(CONTRACT / "capture.schema.json").validate(capture)
    page_capture = capture["pages"][0]
    pins = {name: sha for sha, name in (
        line.split() for line in (ROOT / "docs/audit/fixtures/receipt-report-metadata-2026-09-10.sha256").read_text().splitlines()
    )}
    for kind in ("body", "headers"):
        artifact = page_capture[kind]
        body = (audit / artifact["path"]).read_bytes()
        assert len(body) == artifact["bytes"]
        assert hashlib.sha256(body).hexdigest() == artifact["sha256"] == pins[artifact["path"]]

    source_body = (audit / page_capture["body"]["path"]).read_bytes()
    source = json.loads(source_body, parse_float=Decimal)
    expected_record_validator = validator(CONTRACT / "record.schema.json")
    for record in source["results"]:
        expected_record_validator.validate(record)
    result_path = audit / f"{stem}-review.json"
    result = read(result_path)
    validator(ROOT / "contracts/audits/fec/report-metadata/v1/result.schema.json").validate(result)
    assert result_path.read_bytes() == (audit / f"{stem}-replay.json").read_bytes()
    assert result["capture_sha256"] == hashlib.sha256(capture_path.read_bytes()).hexdigest()
    assert result["state"] == "validated_observations" and result["pagination_state"] == "exact_count_satisfied"
    assert result["rows"] == len(source["results"]) == rows
    assert not result["history_complete"] and not result["financial_selection_ready"]
    assert result["issues"] == [] and result["missing_requested_files"] == []
    assert result["endpoint"] == capture["endpoint"] and result["query"] == capture["query"]
    assert result["pages"][0]["capture"] == page_capture
    assert result["pages"][0]["pagination"] == source["pagination"]

    # Independently locate and hash each original JSON object, not its re-encoding.
    text = source_body.decode("utf-8")
    starts = list(re.finditer(r'"results"\s*:\s*\[', text))
    assert len(starts) == 1
    position = starts[0].end()
    decoder = json.JSONDecoder(parse_float=Decimal)
    for ordinal, (original, output) in enumerate(zip(source["results"], result["pages"][0]["records"], strict=True), 1):
        while text[position] in " \t\r\n,":
            position += 1
        decoded, end = decoder.raw_decode(text, position)
        assert decoded == original == output["raw"]
        assert output["sha256"] == hashlib.sha256(text[position:end].encode("utf-8")).hexdigest()
        assert output["ordinal"] == ordinal and output["file_number"] == str(original["file_number"])
        assert output["committee_id"] == original["committee_id"]
        position = end
    for suffix in ("review", "replay"):
        assert (audit / f"{stem}-{suffix}.exit").read_text().strip() == "0"


@pytest.mark.skipif(not AUDIT, reason="requires retained offline Go metadata review")
def test_endpoint_conflicts_survive_readback():
    audit = Path(AUDIT)
    def record(stem, file):
        return next(r["raw"] for r in read(audit / f"{stem}-review.json")["pages"][0]["records"] if r["file_number"] == file)
    filing, report = record("filing-witnesses", "1833804"), record("nrcc-reports", "1833804")
    assert filing["is_amended"] is False and report["is_amended"] is True
    assert filing["most_recent"] is False and report["most_recent"] is True
    assert filing["amendment_chain"] == [1833804] and report["amendment_chain"] == ["1833804"]
    assert record("filing-witnesses", "1882886")["previous_file_number"] == -1147523
    assert record("nrcc-reports", "1882886")["previous_file_number"] is None
    assert record("sid-reports", "1780346")["individual_itemized_contributions_period"] == "0.00"
