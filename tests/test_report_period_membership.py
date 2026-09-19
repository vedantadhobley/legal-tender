"""Independent source membership and per-day coverage checks, no network."""
import hashlib
import json
import os
import re
from collections import Counter
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
POLICY = ROOT / "contracts/calculations/fec/report-period-membership/v1/policy.json"
AUDIT = os.environ.get("LT_REPORT_PERIOD_AUDIT")
METADATA = os.environ.get("LT_REPORT_PERIOD_METADATA")
REQUIRES_AUDIT = pytest.mark.skipif(not AUDIT or not METADATA, reason="requires retained period gate; no network")


def read(path):
    return json.loads(path.read_bytes(), parse_float=Decimal)


def days(start, end):
    start, end = date.fromisoformat(start), date.fromisoformat(end)
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def test_membership_policy_guards():
    p = read(POLICY)
    assert p["version"] == "legal-tender.fec.report-period-membership-review.v1"
    assert not any(p["guards"].values())
    assert p["forms_by_endpoint"] == {"/v1/reports/pac-party/": "Form 3X", "/v1/reports/house-senate/": "Form 3"}
    assert "amount aggregation" in p["non_goals"]


@REQUIRES_AUDIT
@pytest.mark.parametrize("name,stem,rows,groups,candidates,gap,ready", [
    ("nrcc-cycle", "nrcc-reports", 34, 24, 23, 30, False),
    ("nrcc-2023", "nrcc-reports", 34, 24, 23, 0, True),
    ("sid-cycle", "sid-reports", 10, 5, 5, 335, False),
    ("sid-reported-window", "sid-reports", 10, 5, 5, 0, True),
])
def test_complete_retained_membership_and_timeline(name, stem, rows, groups, candidates, gap, ready):
    r = read(Path(AUDIT) / f"{name}.json")
    e = r["evidence"]
    capture_path = Path(METADATA) / f"{stem}-capture.json"
    capture = read(capture_path)
    assert e["capture_sha256"] == hashlib.sha256(capture_path.read_bytes()).hexdigest()
    pins = {name: sha for sha, name in (line.split() for line in (ROOT / "docs/audit/fixtures/receipt-report-metadata-2026-09-10.sha256").read_text().splitlines())}
    originals = []
    for page, output in zip(capture["pages"], e["pages"], strict=True):
        for kind in ("body", "headers"):
            artifact = page[kind]
            body = (Path(METADATA) / artifact["path"]).read_bytes()
            assert len(body) == artifact["bytes"] and hashlib.sha256(body).hexdigest() == artifact["sha256"] == pins[artifact["path"]]
        source = (Path(METADATA) / page["body"]["path"]).read_text()
        position = re.search(r'"results"\s*:\s*\[', source).end()
        decoder = json.JSONDecoder(parse_float=Decimal)
        for ordinal, record in enumerate(output["records"], 1):
            while source[position] in " \t\r\n,":
                position += 1
            raw, end = decoder.raw_decode(source, position)
            assert raw == record["raw"] and record["ordinal"] == ordinal
            assert record["sha256"] == hashlib.sha256(source[position:end].encode()).hexdigest()
            originals.append(raw)
            position = end
    assert len(originals) == e["rows"] == len(r["observations"]) == rows
    assert len(r["cohorts"]) == groups and len(r["chain_candidate_indexes"]) == candidates
    assert r["observed_partition_ready"] is ready and r["chain_coverage"]["gap_days"] == gap
    assert not r["financial_membership_ready"] and not r["cycle_total_ready"]
    assert not e["history_complete"] and not e["financial_selection_ready"]
    assert r["publisher_member_indexes"] == [i for i, raw in enumerate(originals) if raw["is_amended"] is False]
    seen = list(r["ungrouped_indexes"])
    for i, (o, raw) in enumerate(zip(r["observations"], originals, strict=True)):
        assert o["file_number"] == str(raw["file_number"])
        assert o["period"] == {"start": raw["coverage_start_date"].removesuffix("T00:00:00"), "end": raw["coverage_end_date"].removesuffix("T00:00:00")}
        assert (o["report_form"], o["report_year"], o["report_type"]) == (raw["report_form"], raw["report_year"], raw["report_type"])
        assert o["publisher_state"] == ("not_amended" if raw["is_amended"] is False else "amended")
    for c in r["cohorts"]:
        seen.extend(c["observation_indexes"])
        for i in c["observation_indexes"]:
            o = r["observations"][i]
            assert (o["period"]["start"], o["period"]["end"], o["report_form"], o["report_type"], o["report_year"]) == (c["start"], c["end"], c["report_form"], c["report_type"], c["report_year"])
        selected = c["chain_candidate_index"]
        if selected is not None:
            assert not c["blockers"] and c["publisher_member_indexes"] == [selected]
            chain = [str(int(Decimal(v))) for v in originals[selected]["amendment_chain"]]
            assert set(chain) == {str(originals[i]["file_number"]) for i in c["observation_indexes"]}
            for i in c["observation_indexes"]:
                raw = originals[i]
                assert raw["means_filed"] == "e-file"
                assert raw["is_amended"] is (i != selected)
                pos = chain.index(str(raw["file_number"]))
                assert raw["amendment_chain"] == chain[:pos + 1]
        else:
            assert c["blockers"]
    assert sorted(seen) == list(range(rows))
    window = set(days(r["window"]["start"], r["window"]["end"]))
    for prefix, membership in (("publisher", r["publisher_member_indexes"]), ("chain", r["chain_candidate_indexes"])):
        cov = r[prefix + "_coverage"]
        expected, crossing = Counter(), []
        for i in membership:
            p = r["observations"][i]["period"]
            report_days = set(days(p["start"], p["end"]))
            expected.update(report_days & window)
            if report_days & window and report_days - window:
                crossing.append(i)
        actual = {}
        for segment in cov["segments"]:
            for day in days(segment["start"], segment["end"]):
                assert day not in actual
                actual[day] = segment["membership_count"]
        assert set(actual) == window and all(actual[d] == expected[d] for d in window)
        assert cov["window_days"] == len(window)
        assert cov["covered_days"] == sum(expected[d] > 0 for d in window)
        assert cov["gap_days"] == sum(expected[d] == 0 for d in window)
        assert cov["overlap_days"] == sum(expected[d] > 1 for d in window)
        assert cov["cross_boundary_indexes"] == crossing


@REQUIRES_AUDIT
def test_attachment_and_termination_do_not_invent_financial_coverage():
    nrcc = read(Path(AUDIT) / "nrcc-cycle.json")
    unresolved = [c for c in nrcc["cohorts"] if c["blockers"]]
    assert len(unresolved) == 1
    c = unresolved[0]
    assert (c["start"], c["end"]) == ("2024-09-01", "2024-09-30")
    assert {nrcc["observations"][i]["file_number"] for i in c["observation_indexes"]} == {"1833804", "1876290", "1882886"}
    assert {nrcc["observations"][i]["file_number"] for i in c["publisher_member_indexes"]} == {"1882886"}
    assert c["chain_candidate_index"] is None
    assert nrcc["publisher_coverage"]["gap_days"] == 0 and nrcc["chain_coverage"]["gap_days"] == 30
    sid = read(Path(AUDIT) / "sid-cycle.json")
    gaps = [(s["start"], s["end"]) for s in sid["chain_coverage"]["segments"] if s["membership_count"] == 0]
    assert gaps == [("2023-01-01", "2023-03-31"), ("2024-05-01", "2024-12-31")]
    termination = next(o for o in sid["observations"] if o["report_type"] == "TER")
    assert termination["period"]["end"] == "2024-04-30"
