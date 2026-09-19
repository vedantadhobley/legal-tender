"""Independent, offline summary/source and reported-window comparison checks."""
import csv
import hashlib
import io
import json
import os
from decimal import Decimal, localcontext
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
POLICY = ROOT / "contracts/calculations/fec/summary-report-window/v1/policy.json"
AUDIT = os.environ.get("LT_SUMMARY_WINDOW_AUDIT")
STORAGE = os.environ.get("LT_SUMMARY_STORAGE_ROOT")
PRIOR = os.environ.get("LT_SUMMARY_WINDOW_PRIOR")
REQUIRES = pytest.mark.skipif(not all((AUDIT, STORAGE, PRIOR)), reason="requires retained offline summary/window gate")


def read(path):
    return json.loads(path.read_bytes(), parse_float=Decimal)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def test_policy_has_independent_sources_and_closed_funding_guards():
    policy = read(POLICY)
    assert policy["source_alignment"] == "independent_snapshots"
    assert not any(policy["guards"].values())
    assert policy["delta"] == "summary_minus_window_exact_signed_minor_units"
    assert len(policy["summary_fields"]) == 7
    assert "CVG_START_DT is not a replacement cash date" in policy["opening_scope"]


@REQUIRES
@pytest.mark.parametrize("name", ["sid-window", "sid-cycle", "sid-missing-cover", "nrcc-unresolved"])
def test_retained_source_identity_membership_values_and_scope(name):
    r = read(Path(AUDIT) / f"{name}.json")
    assert r["version"] == read(POLICY)["version"]
    assert r["window"] == read(Path(PRIOR) / f"{name}.json")
    assert r["source_alignment"] == "independent_snapshots"
    assert not any(r[k] for k in read(POLICY)["guards"])
    w = r["window"]
    assert r["committee_id"] == w["membership"]["evidence"]["query"]["committee_id"]
    assert int(r["cycle"]) == w["membership"]["evidence"]["query"]["cycle"]
    root = Path(STORAGE)
    ancestry = r["summary_input"]
    manifest_bytes = (root / "facts/fec/committee-summary/v1/manifests" / f'{ancestry["fact_set_id"]}.json').read_bytes()
    assert sha(manifest_bytes) == ancestry["manifest_sha256"]
    manifest = json.loads(manifest_bytes)
    assert manifest["cycle"] == r["cycle"]
    assert manifest["source_release_id"] == ancestry["source_release_id"]
    assert manifest["source_release_manifest_sha256"] == ancestry["source_release_manifest_sha256"]
    source = (root / manifest["source_artifact"]["storage_key"]).read_bytes()
    assert sha(source) == manifest["source_artifact"]["sha256"] == ancestry["source_artifact_sha256"]
    rows = list(csv.DictReader(io.StringIO(source.decode(), newline=""), strict=True))
    assert len(rows) == r["summary_counts"]["source_rows"]
    selected = {i: row for i, row in enumerate(rows, 1) if row["CMTE_ID"] == r["committee_id"]}
    assert r["summary"]["conflict_fields"] == []
    seen = set()
    for assertion, comparison in zip(r["summary"]["assertions"], r["comparisons"], strict=True):
        assert comparison["assertion_id"] == assertion["assertion_id"]
        for member in assertion["members"]:
            ordinal = member["ordinal"]
            assert ordinal in selected and ordinal not in seen
            seen.add(ordinal)
            raw = selected[ordinal]
            assert raw["CAND_ID"] == member["candidate_raw"]
            assert sha(source[member["offset"]:member["offset"] + member["length"]]) == member["raw_sha256"]
        raw = selected[assertion["members"][0]["ordinal"]]
        assert assertion["coverage_start"]["value"] == f'{raw["CVG_START_DT"][:4]}-{raw["CVG_START_DT"][4:6]}-{raw["CVG_START_DT"][6:]}'
        for f, window_field in zip(comparison["fields"], w["fields"], strict=True):
            assert f["window_field"] == window_field["name"]
            assert f["summary_minor_units"] == str(int(Decimal(raw[f["summary_field"]]) * 100))
            assert f["window_minor_units"] == window_field["window_value_minor_units"]
            expected_ready = name == "sid-window" and f["summary_field"] == "COH_COP"
            assert f["reported_comparison_ready"] == expected_ready
            if expected_ready:
                assert not f["blockers"] and f["state"] == "equal"
                assert int(f["delta_minor_units"]) == int(f["summary_minor_units"]) - int(f["window_minor_units"]) == 0
                assert w["membership"]["window"]["end"] == assertion["coverage_end"]["value"]
            else:
                assert f["delta_minor_units"] is None and f["state"] == "blocked" and f["blockers"]
    assert seen == set(selected)


@REQUIRES
def test_equal_values_do_not_prove_compatible_dates_or_resolve_cash():
    r = read(Path(AUDIT) / "sid-window.json")
    fields = r["comparisons"][0]["fields"]
    assert all(f["summary_minor_units"] == f["window_minor_units"] for f in fields)
    assert all("cycle_prefix_unqualified" in f["blockers"] for f in fields[:5])
    assert fields[5]["summary_minor_units"] == "0"
    assert fields[5]["blockers"] == ["cycle_opening_boundary_unqualified"]
    assert r["summary"]["assertions"][0]["diagnostic_equations"]["cash"]["delta_minor_units"] == "150000000"
    # The two existing diagnostics have opposite coefficient orientations.
    equations = r["window"]["equations"]
    assert any(e["residual_minor_units"] == "-150000000" for e in equations)


@REQUIRES
def test_fixture_signed_difference_exceeds_int64_without_rounding():
    r = read(Path(AUDIT) / "fixture.json")
    f = r["comparisons"][0]["fields"][3]
    with localcontext() as context:
        context.prec = 60
        expected = Decimal(f["summary_minor_units"]) - Decimal(f["window_minor_units"])
    assert int(expected) < -(2**63)
    assert str(expected) == f["delta_minor_units"] == "-9223372036854777808"
    assert f["reported_comparison_ready"] and f["state"] == "different" and not f["blockers"]
