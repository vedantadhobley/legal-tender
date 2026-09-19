"""Independent source-contract evidence checks; no production ingestion code."""

import csv
import hashlib
import io
import json
import os
import re
from collections import Counter, defaultdict
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator


ROOT = Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "contracts/sources/fec/committee-summary/v1"
SCHEMA = json.loads((CONTRACT / "record.schema.json").read_text())
REVIEW = json.loads((CONTRACT / "review.json").read_text())


def _read_csv(raw):
    reader = csv.reader(io.StringIO(raw.decode("utf-8"), newline=""), strict=True)
    assert next(reader) == SCHEMA["x-source-field-order"]
    result = []
    for row in reader:
        assert len(row) == len(SCHEMA["x-source-field-order"])
        result.append(dict(zip(SCHEMA["x-source-field-order"], row, strict=True)))
    return result


def _money(raw):
    if raw == "":
        return None
    assert re.fullmatch(SCHEMA["x-money-lexeme-pattern"], raw), raw
    value = Decimal(raw) * 100
    assert value == value.to_integral_value()
    assert -(2**63) <= value < 2**63
    return int(value)


def _date(raw):
    assert re.fullmatch(r"[0-9]{8}", raw)
    return datetime.strptime(raw, "%Y%m%d").date()


def test_committee_summary_contract_preserves_source_shape_and_scope():
    contract = json.loads((CONTRACT / "contract.json").read_text())
    Draft202012Validator.check_schema(SCHEMA)
    assert contract["status"] == "accepted"
    checks = {check["id"]: check for check in contract["quality"]["checks"]}
    assert checks["financial_assertion_conflict"]["severity"] == "block"
    assert checks["report_and_account_scope"]["severity"] == "partial"
    assert contract["physical_schema"]["media_type"] == "text/csv"
    assert len(SCHEMA["required"]) == 92
    assert SCHEMA["required"] == SCHEMA["x-source-field-order"]
    assert set(SCHEMA["properties"]) == set(SCHEMA["required"])
    assert len(SCHEMA["x-money-fields"]) == 75
    assert set(SCHEMA["x-money-fields"]) == {
        field["path"] for field in contract["semantics"]["money_fields"]
    }
    assert set(SCHEMA["x-money-fields"]).isdisjoint(SCHEMA["x-date-fields"])
    assert all(value is False for value in REVIEW["guards"].values())


def test_committee_summary_verification_schema():
    schema = json.loads((ROOT / "contracts/audits/fec/committee-summary-verification/v1/result.schema.json").read_text())
    Draft202012Validator.check_schema(schema)
    assert schema["properties"]["terminal_attribution_eligible"] == {"const": False}
    assert schema["properties"]["complete"] == {"const": True}


def test_committee_summary_exact_fixtures_and_typed_exceptions():
    raw = (CONTRACT / "fixtures/sample.csv").read_bytes()
    fixture = REVIEW["fixtures"]
    assert hashlib.sha256(raw).hexdigest() == fixture["sha256"]
    lines = raw.splitlines(keepends=True)
    assert hashlib.sha256(lines[0]).hexdigest() == fixture["header_sha256"]
    rows = _read_csv(raw)
    assert len(rows) == len(fixture["rows"]) == 8
    validator = Draft202012Validator(SCHEMA)
    for row, metadata in zip(rows, fixture["rows"], strict=True):
        validator.validate(row)
        line = lines[metadata["sample_ordinal"]]
        assert hashlib.sha256(line).hexdigest() == metadata["sha256"]
        for field in SCHEMA["x-money-fields"]:
            _money(row[field])
    assert _money(rows[1]["COH_BOP"]) is None
    assert _money(rows[1]["INDV_UNITEM_CONTB"]) is None
    assert rows[2]["TTL_RECEIPTS"] == ".32"
    assert _money(rows[2]["TTL_RECEIPTS"]) == 32
    assert _money(rows[3]["INDV_UNITEM_CONTB"]) == -6426498
    assert rows[4]["CVG_START_DT"] == "99999999"
    with pytest.raises(ValueError):
        _date(rows[4]["CVG_START_DT"])
    assert rows[5]["CMTE_ID"] == rows[6]["CMTE_ID"]
    assert [k for k in rows[5] if rows[5][k] != rows[6][k]] == ["CAND_ID"]
    assert _date(rows[7]["CVG_START_DT"]) > _date(rows[7]["CVG_END_DT"])


def test_committee_summary_header_changes_are_not_silently_aliased():
    raw = (CONTRACT / "fixtures/sample.csv").read_bytes()
    for difference in REVIEW["documentation_mismatches"]:
        old, new = difference["header"], difference["dictionary"]
        assert SCHEMA["x-source-field-order"][difference["position"] - 1] == old
        with pytest.raises(AssertionError):
            _read_csv(raw.replace(old.encode(), new.encode(), 1))
    with pytest.raises(AssertionError):
        _read_csv(raw + b"\n")


@pytest.mark.parametrize("raw,expected", [(".27", 27), ("-.01", -1), ("0", 0), ("", None)])
def test_committee_summary_money_lexemes(raw, expected):
    assert _money(raw) == expected


@pytest.mark.parametrize("raw", ["NaN", "1e3", "0.001", " 1", "1,000", "92233720368547758.08"])
def test_committee_summary_unsupported_money_is_not_coerced(raw):
    with pytest.raises(AssertionError):
        _money(raw)


@pytest.mark.parametrize("profile", REVIEW["profiles"], ids=lambda p: str(p["cycle"]))
def test_complete_pinned_committee_summary_corpus(profile):
    directory = os.environ.get("LT_COMMITTEE_SUMMARY_REVIEW")
    if not directory:
        pytest.skip("set LT_COMMITTEE_SUMMARY_REVIEW to the retained research directory")
    path = Path(directory) / profile["file"]
    assert path.stat().st_size == profile["size_bytes"] <= 16 * 1024 * 1024
    raw = path.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == profile["sha256"]
    headers = path.with_suffix(".headers").read_text()
    for key, value in profile["http"].items():
        values = re.findall(r"^" + re.escape(key) + r": (.*)$", headers, re.I | re.M)
        assert values[-1] == value
    assert hashlib.sha256(raw.splitlines(keepends=True)[0]).hexdigest() == profile["header_sha256"]
    rows = _read_csv(raw)
    assert len(rows) == profile["rows"]
    assert raw.endswith(b"\n") and b"\r" not in raw
    assert raw.count(b"\n") == len(rows) + 1
    assert all(re.fullmatch(r"C[0-9]{8}", r["CMTE_ID"]) for r in rows)
    assert {r["FEC_ELECTION_YR"] for r in rows} == {str(profile["cycle"])}
    groups = defaultdict(list)
    dates = Counter()
    invalid_dates = []
    equations = Counter()
    leading_decimal = 0
    for ordinal, row in enumerate(rows, 1):
        groups[row["CMTE_ID"]].append(row)
        for field in SCHEMA["x-money-fields"]:
            _money(row[field])
            leading_decimal += bool(re.match(r"-?\.", row[field]))
        parsed = {}
        for field in SCHEMA["x-date-fields"]:
            try:
                parsed[field] = _date(row[field])
            except ValueError:
                invalid_dates.append({"ordinal": ordinal, "committee_id": row["CMTE_ID"], "field": field, "raw": row[field]})
        if len(parsed) == 2:
            start, end = parsed["CVG_START_DT"], parsed["CVG_END_DT"]
            dates["reversed"] += start > end
            dates["start_before_cycle"] += start < date(profile["cycle"] - 1, 1, 1)
            dates["end_after_cycle"] += end > date(profile["cycle"], 12, 31)
            dates["start_after_cycle_start"] += start > date(profile["cycle"] - 1, 1, 1)
        for name, fields in (
            ("cash", ("COH_BOP", "TTL_RECEIPTS", "TTL_DISB", "COH_COP")),
            ("individual", ("INDV_ITEM_CONTB", "INDV_UNITEM_CONTB", "INDV_CONTB")),
        ):
            values = [_money(row[k]) for k in fields]
            if None in values:
                state = "missing"
            else:
                equal = values[0] + values[1] == values[2]
                if name == "cash":
                    equal = values[0] + values[1] - values[2] == values[3]
                state = "equal" if equal else "different"
            equations[name + "_" + state] += 1
    assert leading_decimal == profile["leading_decimal_money_fields"]
    assert invalid_dates == profile["invalid_dates"]
    assert dates == profile["valid_interval_profile"]
    assert equations == profile["diagnostic_equations"]
    assert len(groups) == profile["committee_ids"]
    repeated = [group for group in groups.values() if len(group) > 1]
    assert len(repeated) == profile["repeated_committee_ids"]
    assert sum(map(len, repeated)) == profile["repeated_committee_occurrences"]
    assert len(rows) - len(groups) == profile["repeated_committee_extra_rows"]
    for group in repeated:
        assert [k for k in group[0] if len({r[k] for r in group}) > 1] == ["CAND_ID"]
    assert len({(r["CMTE_ID"], r["FEC_ELECTION_YR"], r["CAND_ID"]) for r in rows}) == len(rows)
    for field, expected in profile["key_money"].items():
        assert sum(r[field] == "" for r in rows) == expected["blank"]
        assert sum(r[field].startswith("-") for r in rows) == expected["negative"]
    if profile["cycle"] == REVIEW["fixtures"]["source_cycle"]:
        for evidence in REVIEW["evidence"]:
            evidence_path = Path(directory) / evidence["file"]
            assert evidence_path.stat().st_size == evidence["size_bytes"]
            assert hashlib.sha256(evidence_path.read_bytes()).hexdigest() == evidence["sha256"]
        lines = raw.splitlines(keepends=True)
        fixture = lines[0] + b"".join(lines[r["source_ordinal"]] for r in REVIEW["fixtures"]["rows"])
        assert fixture == (CONTRACT / "fixtures/sample.csv").read_bytes()
