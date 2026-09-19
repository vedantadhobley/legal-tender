"""Independent publication/schema checks; no production domain logic."""

import copy
import csv
import ctypes
from ctypes.util import find_library
from datetime import datetime
from decimal import Decimal
import hashlib
import io
import json
import os
import re
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from orchestration.go_process import _local_registry
from orchestration.resources import GoPipelineResource
from tests.test_committee_summary_reader_corpus import _identity

ROOT = Path(__file__).resolve().parents[1]
CONTRACTS = ROOT / "contracts"
FACTS = CONTRACTS / "facts/fec/committee-summary/v1"
SOURCE = json.loads((CONTRACTS / "sources/fec/committee-summary/v1/record.schema.json").read_text())
REVIEW = json.loads((CONTRACTS / "sources/fec/committee-summary/v1/review.json").read_text())


def validator(path):
    return Draft202012Validator(json.loads(path.read_text()), registry=_local_registry(CONTRACTS), format_checker=FormatChecker())


def test_summary_publication_schemas_and_inventory():
    for path in [*FACTS.glob("*.schema.json"), *(CONTRACTS / "releases/fec/v4").glob("*.schema.json")]:
        Draft202012Validator.check_schema(json.loads(path.read_text()))
    v3 = json.loads((CONTRACTS / "releases/fec/v3/inventory.json").read_text())
    v4 = json.loads((CONTRACTS / "releases/fec/v4/inventory.json").read_text())
    validator(CONTRACTS / "releases/fec/v4/inventory.schema.json").validate(v4)
    assert v4["sources"][:len(v3["sources"])] == v3["sources"]
    assert [s["source_id"] for s in v4["sources"][len(v3["sources"]):]] == [f"fec:committee-summary:{c}" for c in v4["periods"]]
    assert all(s["artifact_format"] == "committee_summary_csv" and not s["selected_members"] and not s["selected_relations"] for s in v4["sources"][len(v3["sources"]):])


def test_dagster_v4_schema_resolver():
    resource = GoPipelineResource(binary_path="unused", artifact_root="unused", contracts_root=str(CONTRACTS), current_fec_release_manifest="", storage_root="unused")
    resolve = resource._fec_release_schema_resolver("release-manifest.schema.json")
    for version in (1, 2, 3, 4):
        assert resolve({"inventory_version": f"legal-tender.fec.initial-release-inventory.v{version}"}) == CONTRACTS / f"releases/fec/v{version}/release-manifest.schema.json"
    with pytest.raises(ValueError):
        resolve({"inventory_version": "unreviewed"})


def test_real_go_fixture_manifests_pass_contracts():
    output = os.environ.get("LT_SUMMARY_CORPUS_OUTPUT")
    if not output:
        pytest.skip("requires isolated Go fixture outputs")
    path = Path(output) / "fixture"
    manifest = json.loads((path / "manifest.json").read_text())
    release = json.loads((path / "release.json").read_text())
    validator(CONTRACTS / "releases/fec/v4/release-manifest.schema.json").validate(release)
    check = validator(FACTS / "manifest.schema.json")
    check.validate(manifest)
    for field, value in (("terminal_attribution_eligible", True), ("readback_verified", False), ("fact_type", "money_total")):
        bad = copy.deepcopy(manifest)
        bad[field] = value
        assert list(check.iter_errors(bad))


def decoded(descriptor, root):
    path = (root / descriptor["storage_key"]).resolve()
    assert path.is_relative_to(root.resolve())
    compressed = path.read_bytes()
    assert len(compressed) == descriptor["compressed_byte_count"]
    assert hashlib.sha256(compressed).hexdigest() == descriptor["compressed_sha256"]
    size = descriptor["uncompressed_byte_count"]
    assert 0 < size < 256 * 1024 * 1024
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
    return io.BytesIO(body)


def identity(*parts):
    h = hashlib.sha256()
    for part in parts:
        encoded = part.encode()
        h.update(len(encoded).to_bytes(8, "big"))
        h.update(encoded)
    return h.hexdigest()


@pytest.mark.parametrize("profile", REVIEW["profiles"], ids=lambda p: str(p["cycle"]))
def test_all_stored_summary_values_and_locators(profile):
    output = os.environ.get("LT_SUMMARY_CORPUS_OUTPUT")
    source = os.environ.get("LT_COMMITTEE_SUMMARY_REVIEW")
    if not output or not source:
        pytest.skip("requires isolated Go artifacts and retained review CSVs")
    root = Path(output) / str(profile["cycle"])
    gate = json.loads((root / "artifact-gate.json").read_text())
    assert gate["scope"] == "unpublished_artifact_gate_not_a_coordinated_release"
    assert gate["replay_identical"] is True
    raw = (Path(source) / profile["file"]).read_bytes()
    assert_summary_facts(profile, raw, gate["facts"], gate["verification"], root)


def assert_summary_facts(profile, raw, descriptor, verification, root):
    """Compare actual stored facts with independent CSV parsing at any snapshot."""
    assert hashlib.sha256(raw).hexdigest() == profile["sha256"]
    assert len(raw) == profile["size_bytes"]
    # CSV line_num identifies physical lines consumed by each logical record,
    # including quoted multiline fields. Raw byte spans retain every LF.
    lines = io.BytesIO(raw).readlines()
    reader = csv.reader(io.StringIO(raw.decode(), newline=""), strict=True)
    fields = next(reader)
    assert fields == SOURCE["x-source-field-order"]
    previous_line = reader.line_num
    stored = decoded(descriptor, root)
    fact_validator = validator(FACTS / "fact.schema.json")
    seen = set()
    offset = sum(map(len, lines[:previous_line]))
    issues = {}
    for ordinal, values in enumerate(reader, 1):
        assert len(values) == len(fields)
        source_row = dict(zip(fields, values, strict=True))
        raw_record = b"".join(lines[previous_line:reader.line_num])
        previous_line = reader.line_num
        fact = json.loads(next(stored))
        row = fact["record"]
        assert row["source_fields"] == source_row
        assert row["ordinal"] == ordinal and row["offset"] == offset and row["length"] == len(raw_record)
        assert row["raw_sha256"] == hashlib.sha256(raw_record).hexdigest()
        offset += len(raw_record)
        assert fact["origin_snapshot_id"] == profile["sha256"]
        assert fact["cycle"] == str(profile["cycle"])
        assert fact["fact_type"] == "fec.committee_summary.v1"
        assert fact["source_contract"] == "fec/committee-summary@1.0.0"
        assert fact["parser_version"] == "legal-tender.fec.committee-summary-reader.v1"
        occurrence = identity("fec.committee-summary.occurrence.v1", profile["sha256"], "whole_csv", fact["cycle"], str(ordinal))
        assert fact["occurrence_id"] == occurrence
        assert fact["publisher_reference"] == f"unkeyed:{occurrence}"
        version = identity("fec.committee-summary.record-version.v1", fact["cycle"], fact["publisher_reference"], row["raw_sha256"])
        assert fact["source_record_version_id"] == version
        assert fact["fact_id"] == identity(fact["fact_type"], version, fact["parser_version"])
        assert fact["fact_id"] not in seen
        seen.add(fact["fact_id"])
        assert set(row["money"]) == set(SOURCE["x-money-fields"])
        for name in SOURCE["x-money-fields"]:
            value = source_row[name]
            expected = {"state": "source_blank", "minor_units": None, "source_scale": 0}
            if value:
                assert re.fullmatch(SOURCE["x-money-lexeme-pattern"], value)
                amount = Decimal(value) * 100
                assert amount == amount.to_integral_value() and -(2**63) <= amount < 2**63
                expected = {"state": "valid", "minor_units": str(int(amount)), "source_scale": len(value.split(".")[1]) if "." in value else 0}
            assert row["money"][name] == expected
        row_issues = []
        assert set(row["dates"]) == set(SOURCE["x-date-fields"])
        for name in SOURCE["x-date-fields"]:
            value = source_row[name]
            expected = {"state": "source_blank", "value": None}
            if value:
                try:
                    if not re.fullmatch(r"[0-9]{8}", value):
                        raise ValueError("not an eight-digit source date")
                    expected = {"state": "valid", "value": datetime.strptime(value, "%Y%m%d").date().isoformat()}
                except ValueError:
                    expected = {"state": "invalid", "value": None}
                    row_issues.append({"code": "invalid_date", "field": name})
            assert row["dates"][name] == expected
        assert set(row["identifiers"]) == {"CMTE_ID", "CAND_ID", "FEC_ELECTION_YR"}
        for name in ("CMTE_ID", "CAND_ID", "FEC_ELECTION_YR"):
            state, value = _identity(name, source_row[name])
            assert row["identifiers"][name] == {"state": state, "value": value if state == "valid" else None}
            if state == "invalid" or state == "source_blank" and name != "CAND_ID":
                row_issues.append({"code": "invalid_identity", "field": name})
        start, end = row["dates"]["CVG_START_DT"], row["dates"]["CVG_END_DT"]
        if start["state"] == end["state"] == "valid" and start["value"] > end["value"]:
            row_issues.append({"code": "reversed_interval", "field": "CVG_START_DT"})
        assert row["issues"] == row_issues
        for issue in row_issues:
            issues[issue["code"]] = issues.get(issue["code"], 0) + 1
        if ordinal == 1 or row_issues:
            fact_validator.validate(fact)
    assert stored.read() == b""
    assert len(seen) == profile["rows"] == descriptor["record_count"] == verification["rows"]
    assert offset == len(raw)
    assert issues == verification["issue_counts"]


def test_published_release_summaries_complete_readback_and_replay():
    audit_path = os.environ.get("LT_SUMMARY_RELEASE_AUDIT")
    storage_path = os.environ.get("LT_SUMMARY_STORAGE_ROOT")
    if not audit_path or not storage_path:
        pytest.skip("requires a completed real release and summary publication audit")
    audit, storage = Path(audit_path), Path(storage_path)
    release_bytes = (audit / "release.json").read_bytes()
    release = json.loads(release_bytes)
    validator(CONTRACTS / "releases/fec/v4/release-manifest.schema.json").validate(release)
    assert release_bytes == (audit / "release-replay.json").read_bytes()
    assert release_bytes == (storage / "releases/fec/manifests" / (release["release_id"] + ".json")).read_bytes()
    release_sha = hashlib.sha256(release_bytes).hexdigest()
    selected = {item["source_id"]: item for item in release["artifacts"]}
    actual_cycles = {path.name for path in (audit / "summaries").iterdir() if path.is_dir()}
    assert actual_cycles == set(release["periods"])
    for cycle in release["periods"]:
        path = audit / "summaries" / cycle
        encoded = (path / "manifest.json").read_bytes()
        assert encoded == (path / "replay.json").read_bytes()
        manifest = json.loads(encoded)
        validator(FACTS / "manifest.schema.json").validate(manifest)
        assert encoded == (storage / "facts/fec/committee-summary/v1/manifests" / (manifest["fact_set_id"] + ".json")).read_bytes()
        assert manifest["source_release_id"] == release["release_id"]
        assert manifest["source_release_manifest_sha256"] == release_sha
        source = selected[f"fec:committee-summary:{cycle}"]
        assert manifest["source_artifact"] == source
        assert manifest["cycle"] == cycle
        assert manifest["verification"]["expected"] == {"cycle": cycle, "bytes": source["byte_count"], "sha256": source["sha256"]}
        raw_path = (storage / source["storage_key"]).resolve()
        assert raw_path.is_relative_to(storage.resolve())
        profile = {"cycle": cycle, "sha256": source["sha256"], "size_bytes": source["byte_count"], "rows": manifest["verification"]["rows"]}
        assert_summary_facts(profile, raw_path.read_bytes(), manifest["facts"], manifest["verification"], storage)
