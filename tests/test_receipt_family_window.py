"""Independent per-day coverage and source-operand checks; no runtime Python."""
import base64
import hashlib
import json
import os
import re
from collections import Counter
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from tests.test_report_field_binding import minor

ROOT = Path(__file__).resolve().parents[1]
AUDIT = os.environ.get("LT_FAMILY_WINDOW_AUDIT")
STORAGE = os.environ.get("LT_FAMILY_WINDOW_STORAGE")
CASES = ("sid-window", "sid-cycle", "sid-missing-cover", "nrcc-unresolved", "nrcc-january")
REQUIRES = pytest.mark.skipif(not AUDIT or not STORAGE, reason="requires retained window gate")


def read(path):
    return json.loads(path.read_bytes(), parse_float=Decimal)


def days(period):
    start, end = date.fromisoformat(period["start"]), date.fromisoformat(period["end"])
    return {start + timedelta(days=i) for i in range((end-start).days+1)}


def test_family_window_policy_has_no_named_or_financial_overrides():
    p = read(ROOT / "contracts/calculations/fec/receipt-family-window/v1/policy.json")
    before = read(ROOT / "contracts/calculations/fec/receipt-family-absence/v1/policy.json")
    assert p["version"] == "legal-tender.fec.receipt-family-window.v1"
    assert p["upstream_version"] == before["version"] and p["families"] == before["families"]
    assert not any(p["guards"].values())
    assert "no_family_summary_comparisons" in p["limits"]
    assert "not_subtracted_across_unequal_scopes" in p["partial_sums"]
    assert "no_float" in p["numeric"]
    assert not re.search(r'C\d{8}|1690269|1714573|2024', json.dumps(p))


@REQUIRES
@pytest.mark.parametrize("name", CASES)
def test_exact_ancestry_members_operands_and_days(name):
    r = read(Path(AUDIT) / f"{name}.json")
    prior = read(Path(STORAGE) / "dumps/audits/fec/receipt-family-absence/2026-09-11/attempt-01" / f"{name}.json")
    assert r["reviewed"] == prior
    assert not r["financial_use_eligible"] and not r["terminal_attribution_eligible"]
    # Earlier reported-zero output retains its own deliberately closed guard.
    assert not prior["family_window_comparison_ready"]
    comparison = prior["compared"]
    m = comparison["family_reports"]["membership"]
    assert r["window"] == m["window"]
    bindings = comparison["family_reports"]["bindings"]
    by_observation = {bindings[report["binding_index"]]["observation_index"]: i for i, report in enumerate(comparison["reports"])}
    candidates = [i for i in m["chain_candidate_indexes"] if m["observations"][i]["window_relation"] != "outside"]
    window = days(r["window"])
    source = {row["file_number"]: row["raw"] for p in m["evidence"]["pages"] for row in p["records"]}
    for f in r["families"]:
        assert [member["observation_index"] for member in f["members"]] == candidates
        field = f["field"]
        for member in f["members"]:
            oi = member["observation_index"]
            obs = m["observations"][oi]
            expected_report = by_observation.get(oi)
            assert member["report_index"] == expected_report
            if expected_report is None:
                assert member["family_index"] is None and "missing_document" in member["reported_blockers"]
                assert member["reported_minor_units"] is None and member["comparison_minor_units"] is None
                continue
            report = comparison["reports"][expected_report]
            fi = member["family_index"]
            family = report["families"][fi]
            zero = prior["reports"][expected_report]["families"][fi]
            assert family["field"] == field and zero["family_id"] == field["id"]
            binding = bindings[report["binding_index"]]
            assert binding["observation_index"] == oi
            if member["reported_minor_units"] is not None:
                assert obs["window_relation"] == "inside" and days(obs["period"]) <= window
                assert binding["scope_bound"] and family["binding"]["reported_value_bound"]
                assert not member["reported_blockers"]
                d = binding["document"]
                body = Path(d["body"]["path"]).read_bytes()
                assert hashlib.sha256(body).hexdigest() == d["body"]["sha256"]
                records = [base64.b64decode(row["raw_base64"]) for row in d["records"]]
                assert b"".join(records) == body
                cover = records[1].removesuffix(b"\n").removesuffix(b"\r").split(b"\x1c")
                raw = source[report["file_number"]]
                assert obs["period"] == {"start": raw["coverage_start_date"].split("T")[0], "end": raw["coverage_end_date"].split("T")[0]}
                assert member["reported_minor_units"] == minor(cover[field["sequence"]-1].decode()) == minor(raw[field["metadata_field"]])
            if member["comparison_minor_units"] is not None:
                assert member["reported_minor_units"] is not None and not member["comparison_blockers"]
                if member["value_basis"] == "nonmemo_occurrence_subtotal":
                    assert family["state"] in ("equal", "different") and family["nonmemo_occurrences"]["rows"] > 0
                    assert member["comparison_minor_units"] == family["detail_minor_units"]
                    groups = [report["groups"][i] for i in family["group_indexes"]]
                    # Retain memo X evidence but do not sum it; no date/individual filtering.
                    nonmemo = [g for g in groups if g["key"]["memo_code"] != {"present": True, "value": "X"}]
                    assert str(sum(int(g["measures"]["signed_minor_units"]) for g in nonmemo)) == member["comparison_minor_units"]
                else:
                    assert member["value_basis"] == "qualified_reported_zero_without_occurrences"
                    assert zero["state"] == "qualified_reported_zero" and zero["profile_rows"] == zero["original_observed_rows"] == 0
                    assert member["comparison_minor_units"] == "0" and family["detail_minor_units"] is None
                assert int(member["delta_minor_units"]) == int(member["reported_minor_units"]) - int(member["comparison_minor_units"])
                assert member["state"] == ("equal" if member["delta_minor_units"] == "0" else "different")
            else:
                assert member["delta_minor_units"] is None and member["state"] == "blocked" and member["comparison_blockers"]
        for kind in ("reported", "comparison"):
            included = [v for v in f["members"] if v[f"{kind}_minor_units"] is not None]
            amounts = [int(v[f"{kind}_minor_units"]) for v in included]
            counts = Counter(day for v in included for day in days(m["observations"][v["observation_index"]]["period"]) & window)
            cov = f[f"{kind}_coverage"]
            actual = {}
            for segment in cov["segments"]:
                for day in days(segment):
                    assert day not in actual
                    actual[day] = segment["membership_count"]
            assert set(actual) == window and all(actual[day] == counts[day] for day in window)
            assert cov["window_days"] == len(window)
            assert cov["gap_days"] == sum(counts[day] == 0 for day in window)
            assert cov["covered_days"] == sum(counts[day] > 0 for day in window)
            assert cov["overlap_days"] == sum(counts[day] > 1 for day in window)
            assert not cov["cross_boundary_indexes"]
            total = str(sum(amounts)) if amounts else None
            assert f[f"observed_{kind}_sum_minor_units"] == total
            ready = m["observed_partition_ready"] and len(included) == len(candidates) and bool(included) and all(counts[day] == 1 for day in window)
            assert f[f"{kind}_window_ready"] == ready
            assert f[f"{kind}_window_minor_units"] == (total if ready else None)
            assert bool(f[f"{kind}_blockers"]) != ready
        if f["comparison_window_ready"]:
            assert f["reported_window_ready"]
            assert int(f["delta_minor_units"]) == int(f["reported_window_minor_units"]) - int(f["comparison_window_minor_units"])
            assert f["state"] == ("equal" if f["delta_minor_units"] == "0" else "different")
        else:
            assert f["delta_minor_units"] is None and f["state"] == "blocked"


@REQUIRES
def test_retained_scope_outcomes():
    for name, ready, gap in [("sid-window", True, 0), ("sid-cycle", False, 335), ("sid-missing-cover", False, 92), ("nrcc-unresolved", False, 30), ("nrcc-january", True, 0)]:
        r = read(Path(AUDIT) / f"{name}.json")
        for f in r["families"]:
            assert f["reported_window_ready"] is ready and f["comparison_window_ready"] is ready
            assert f["reported_coverage"]["gap_days"] == gap and f["comparison_coverage"]["gap_days"] == gap
    january = read(Path(AUDIT) / "nrcc-january.json")
    amounts = {f["field"]["id"]: f["reported_window_minor_units"] for f in january["families"]}
    assert amounts == {"party_contributions": "0", "other_committee_contributions": "173100000", "affiliated_or_party_transfers": "54848772", "loans_received": "0"}
