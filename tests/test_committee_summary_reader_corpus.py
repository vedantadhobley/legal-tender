"""Independent whole-corpus comparison with the Go verifier's actual results."""

import csv
import hashlib
import io
import json
import os
import re
from collections import Counter
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "contracts/sources/fec/committee-summary/v1"
SOURCE = json.loads((CONTRACT / "record.schema.json").read_text())
REVIEW = json.loads((CONTRACT / "review.json").read_text())
RESULT_SCHEMA = json.loads((ROOT / "contracts/audits/fec/committee-summary-verification/v1/result.schema.json").read_text())


def _text(h, value):
    encoded = value.encode("utf-8")
    h.update(len(encoded).to_bytes(8, "little"))
    h.update(encoded)


def _identity(field, raw):
    if not raw:
        return "source_blank", ""
    patterns = {"CMTE_ID": r"C[0-9]{8}", "CAND_ID": r"[HPS][A-Z0-9]{8}", "FEC_ELECTION_YR": r"[0-9]{3}[02468]"}
    return ("valid", raw) if re.fullmatch(patterns[field], raw) else ("invalid", "")


@pytest.mark.parametrize("profile", REVIEW["profiles"], ids=lambda p: str(p["cycle"]))
def test_go_summary_verifier_against_all_source_fields(profile):
    source_dir = os.environ.get("LT_COMMITTEE_SUMMARY_REVIEW")
    result_dir = os.environ.get("LT_COMMITTEE_SUMMARY_RESULTS")
    if not source_dir or not result_dir:
        pytest.skip("set LT_COMMITTEE_SUMMARY_REVIEW and LT_COMMITTEE_SUMMARY_RESULTS")
    cycle = profile["cycle"]
    result_path = Path(result_dir) / f"{cycle}.json"
    encoded = result_path.read_bytes()
    assert encoded == (Path(result_dir) / f"{cycle}-replay.json").read_bytes()
    assert (Path(result_dir) / f"{cycle}.exit").read_text().strip() == "0"
    assert (Path(result_dir) / f"{cycle}-replay.exit").read_text().strip() == "0"
    result = json.loads(encoded)
    Draft202012Validator(RESULT_SCHEMA).validate(result)
    assert result["expected"] == {"cycle": str(cycle), "bytes": profile["size_bytes"], "sha256": profile["sha256"]}
    raw = (Path(source_dir) / profile["file"]).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == profile["sha256"]
    lines = raw.splitlines(keepends=True)
    assert result["header_bytes"] == len(lines[0])
    assert result["header_bytes"] + result["record_bytes"] == len(raw)
    reader = csv.DictReader(io.StringIO(raw.decode("utf-8"), newline=""), strict=True)
    assert reader.fieldnames == SOURCE["x-source-field-order"]
    fields_hash, typed_hash = hashlib.sha256(), hashlib.sha256()
    money = {name: dict.fromkeys(("valid", "blank", "invalid", "positive", "negative", "zero", "leading_decimal"), 0) for name in SOURCE["x-money-fields"]}
    dates = {name: dict.fromkeys(("valid", "blank", "invalid"), 0) for name in SOURCE["x-date-fields"]}
    ids = {name: dict.fromkeys(("valid", "blank", "invalid"), 0) for name in ("CMTE_ID", "CAND_ID", "FEC_ELECTION_YR")}
    issues = Counter()
    examples = {}
    rows_with_issues = 0
    rows = 0
    offset = len(lines[0])
    for ordinal, row in enumerate(reader, 1):
        rows += 1
        assert None not in row and None not in row.values()
        for field in SOURCE["x-source-field-order"]:
            _text(fields_hash, row[field])
        row_issues = []
        for field in SOURCE["x-money-fields"]:
            value = row[field]
            if value == "":
                state, minor, scale = "source_blank", "", 0
                money[field]["blank"] += 1
            else:
                assert re.fullmatch(SOURCE["x-money-lexeme-pattern"], value)
                decimal_minor = Decimal(value) * 100
                assert decimal_minor == decimal_minor.to_integral_value()
                minor = str(int(decimal_minor))
                assert -(2**63) <= int(minor) < 2**63
                state = "valid"
                scale = len(value.split(".")[1]) if "." in value else 0
                money[field]["valid"] += 1
                money[field]["zero" if int(minor) == 0 else "positive" if int(minor) > 0 else "negative"] += 1
            money[field]["leading_decimal"] += bool(re.match(r"-?\.", value))
            for item in (state, minor, str(scale)):
                _text(typed_hash, item)
        normalized_dates = {}
        for field in SOURCE["x-date-fields"]:
            value = row[field]
            state, normalized = "source_blank", ""
            if value:
                try:
                    assert re.fullmatch(r"[0-9]{8}", value)
                    normalized = datetime.strptime(value, "%Y%m%d").date().isoformat()
                    state = "valid"
                    normalized_dates[field] = normalized
                except ValueError:
                    state = "invalid"
                    row_issues.append(("invalid_date", field))
            dates[field]["blank" if state == "source_blank" else state] += 1
            _text(typed_hash, state)
            _text(typed_hash, normalized)
        for field in ids:
            state, value = _identity(field, row[field])
            ids[field]["blank" if state == "source_blank" else state] += 1
            _text(typed_hash, state)
            _text(typed_hash, value)
            if state == "invalid" or state == "source_blank" and field != "CAND_ID":
                row_issues.append(("invalid_identity", field))
        if len(normalized_dates) == 2 and normalized_dates["CVG_START_DT"] > normalized_dates["CVG_END_DT"]:
            row_issues.append(("reversed_interval", "CVG_START_DT"))
        rows_with_issues += bool(row_issues)
        for code, field in row_issues:
            issues[code] += 1
            samples = examples.setdefault(code, [])
            if len(samples) < result["max_issue_examples_per_code"]:
                samples.append({"ordinal": ordinal, "offset": offset, "raw_sha256": hashlib.sha256(lines[ordinal]).hexdigest(), "code": code, "field": field})
        offset += len(lines[ordinal])
    assert rows == profile["rows"] == result["rows"]
    assert offset == len(raw)
    assert fields_hash.hexdigest() == result["fields_sha256"]
    assert typed_hash.hexdigest() == result["typed_values_sha256"]
    assert money == result["money"]
    assert dates == result["dates"]
    assert ids == result["identifiers"]
    assert issues == result["issue_counts"]
    assert examples == result["issue_examples"]
    assert rows_with_issues == result["rows_with_issues"]
    for name in ("cash", "individual"):
        for state in ("equal", "different", "missing"):
            assert result["diagnostic_equations"][name][state] == profile["diagnostic_equations"][f"{name}_{state}"]
        assert result["diagnostic_equations"][name]["invalid"] == 0
    for name, count in profile["valid_interval_profile"].items():
        assert result["intervals"][name] == count
    assert result["intervals"]["valid_endpoints"] + result["intervals"]["unavailable_endpoints"] == rows
    for name in ("committee_ids", "repeated_committee_ids", "repeated_committee_occurrences", "repeated_committee_extra_rows"):
        assert result["multiplicity"][name] == profile[name]
    assert result["multiplicity"]["equal_non_candidate_groups"] == profile["repeated_committee_ids"]
    assert result["multiplicity"]["conflicting_non_candidate_groups"] == 0
    assert result["multiplicity"]["duplicate_composite_extra_rows"] == 0
    assert result["multiplicity"]["exact_duplicate_extra_rows"] == 0
    assert result["multiplicity"]["unindexed_committee_rows"] == ids["CMTE_ID"]["invalid"] == 0
    assert result["multiplicity"]["unindexed_composite_rows"] == ids["CAND_ID"]["invalid"]
    assert result["multiplicity"]["composite_keys"] + result["multiplicity"]["unindexed_composite_rows"] == rows
