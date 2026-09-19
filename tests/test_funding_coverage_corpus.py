"""Independent, opt-in audit of preserved source facts; no runtime domain code."""

from collections import Counter
import ctypes
from ctypes.util import find_library
from datetime import datetime
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path

import pytest

from tests.test_funding_basis_contracts import validators

AUDIT = os.environ.get("LT_FUNDING_COVERAGE_AUDIT")
STORAGE = os.environ.get("LT_FUNDING_COVERAGE_STORAGE")
pytestmark = pytest.mark.skipif(not AUDIT or not STORAGE, reason="requires retained funding coverage and source artifacts")


def load():
    return json.loads((Path(AUDIT) / "coverage.json").read_text())


def decoded_facts(descriptor):
    # Reuse the OS decoder in the existing image. No new Python dependency.
    root = Path(STORAGE).resolve()
    path = (root / descriptor["storage_key"]).resolve()
    assert path.is_relative_to(root)
    compressed = path.read_bytes()
    assert len(compressed) == descriptor["compressed_byte_count"]
    assert hashlib.sha256(compressed).hexdigest() == descriptor["compressed_sha256"]
    size = descriptor["uncompressed_byte_count"]
    assert 0 < size <= 64 * 1024 * 1024
    lib = ctypes.CDLL(find_library("zstd"))
    lib.ZSTD_decompress.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p, ctypes.c_size_t]
    lib.ZSTD_decompress.restype = ctypes.c_size_t
    lib.ZSTD_isError.argtypes = [ctypes.c_size_t]
    lib.ZSTD_isError.restype = ctypes.c_uint
    destination = ctypes.create_string_buffer(size)
    length = lib.ZSTD_decompress(destination, size, compressed, len(compressed))
    assert not lib.ZSTD_isError(length) and length == size
    body = destination.raw
    assert hashlib.sha256(body).hexdigest() == descriptor["uncompressed_sha256"]
    rows = [json.loads(line) for line in body.splitlines()]
    assert len(rows) == descriptor["record_count"]
    return rows


def test_coverage_contract_identity_replay_and_no_allocation():
    report = load()
    validator = validators()["coverage.schema"]
    validator.validate(report)
    audit_id = report["audit_id"]
    report["audit_id"] = ""
    assert hashlib.sha256(json.dumps(report, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest() == audit_id
    assert (Path(AUDIT) / "coverage.json").read_bytes() == (Path(AUDIT) / "coverage-replay.json").read_bytes()
    assert not report["terminal_attribution_eligible"] and not report["complete_committee_funding_basis"]
    states = {r["requirement"]: r["state"] for r in report["requirements"]}
    assert states["committee_report_opening_balance"] == "scope_incompatible"
    assert states["committee_unitemized_receipts"] == "absent_from_supplied_sources"
    assert states["recipient_cash_availability"] == "unresolved"
    report["audit_id"] = audit_id
    report["complete_committee_funding_basis"] = True
    assert list(validator.iter_errors(report))


def test_each_summary_population_against_all_source_fields():
    report = load()
    expected = {"all-candidates-summary": (3826, 30), "current-campaigns-summary": (2368, 9)}
    assert {p["dataset"] for p in report["candidate_summary_profiles"]} == set(expected)
    for profile in report["candidate_summary_profiles"]:
        manifest_path = Path(STORAGE) / "facts/fec/classic" / profile["dataset"] / "manifests" / f"{profile['fact_set_id']}.json"
        data = manifest_path.read_bytes()
        assert hashlib.sha256(data).hexdigest() == profile["manifest_sha256"]
        manifest = json.loads(data)
        assert manifest["counts"] == profile["publication_counts"]
        assert manifest["source_release_id"] == report["source_release_id"]
        facts = decoded_facts(profile["facts_artifact"])
        count, excluded = expected[profile["dataset"]]
        assert len(facts) == profile["verified_rows"] == count
        assert profile["publication_counts"]["excluded_occurrences"] == excluded
        assert profile["state"] == "valid_fact_subset_source_exclusions"
        seen = set()
        dates = Counter()
        money = {f["field"]: Counter() for f in profile["money_fields"]}
        for fact in facts:
            source, typed = fact["source_fields"], fact["typed_fields"]
            assert set(source) == set(profile["source_fields"])
            assert fact["cycle"] == report["cycle"] and fact["source_release_id"] == report["source_release_id"]
            assert source["CAND_ID"] == typed["candidate_id"] and source["CAND_ID"] not in seen
            seen.add(source["CAND_ID"])
            date = datetime.strptime(source["CVG_END_DT"], "%m/%d/%Y").date().isoformat() if source["CVG_END_DT"] else None
            assert date == typed["coverage_through"]
            dates[date] += 1
            for field, counter in money.items():
                raw = source[field]
                assert raw == typed["money"][field]["raw_value"]
                if raw == "":
                    counter["blank_rows"] += 1
                    assert typed["money"][field]["reported_minor_units"] is None
                else:
                    value = Decimal(raw)
                    assert value * 100 == int(typed["money"][field]["reported_minor_units"])
                    counter["negative_rows" if value < 0 else "positive_rows" if value > 0 else "zero_rows"] += 1
        for field in profile["money_fields"]:
            assert sum(money[field["field"]].values()) == count
            for key in ("blank_rows", "negative_rows", "positive_rows", "zero_rows"):
                assert field[key] == money[field["field"]][key]
        assert dates == Counter({d["date"]: d["rows"] for d in profile["coverage_through"]})
        for date in profile["coverage_through"]:
            year = int(report["cycle"])
            relation = "source_blank" if date["date"] is None else "before_source_cycle" if date["date"] < f"{year-1}-01-01" else "after_source_cycle" if date["date"] > f"{year}-12-31" else "within_source_cycle"
            assert relation == date["source_cycle_relation"]


def test_receipt_role_inventory_conservation():
    report = load()
    inventory = json.loads((Path(STORAGE) / "dumps/audits/fec/committee-funding-basis/2026-09-08/2024/inventory.json").read_text())
    assert report["inventory_calculation_id"] == inventory["calculation_id"]
    assert report["receipt_input"] == inventory["input"]
    grouped = {}
    for bucket in inventory["buckets"]:
        key = (bucket["key"]["component"], bucket["key"]["receipt_role"])
        grouped.setdefault(key, Counter()).update({k: int(v) for k, v in bucket["measures"].items()})
    assert len(grouped) == len(report["receipt_roles"])
    for role in report["receipt_roles"]:
        assert grouped[(role["component"], role["receipt_role"])] == {k: int(v) for k, v in role["measures"].items()}
    assert sum(r["measures"]["rows"] for r in report["receipt_roles"]) == 264085606
