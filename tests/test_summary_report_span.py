"""Independent dated-source evidence and v2 calendar-span comparison gate."""
import hashlib
import json
import os
from datetime import date, timedelta
from html.parser import HTMLParser
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
AUDIT = os.environ.get("LT_SUMMARY_SPAN_AUDIT")
PRIOR = os.environ.get("LT_SUMMARY_SPAN_PRIOR")
REPORTS = os.environ.get("LT_SUMMARY_SPAN_REPORTS")
REQUIRES = pytest.mark.skipif(not all((AUDIT, PRIOR, REPORTS)), reason="requires retained offline span gate")


def read(path):
    return json.loads(path.read_bytes())


class Text(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts = []

    def handle_data(self, data):
        self.parts.append(data)


def normalized_html(path):
    parser = Text()
    parser.feed(path.read_text())
    return " ".join(" ".join(parser.parts).split())


def test_v2_policy_separates_reported_comparison_and_cycle_coverage():
    p = read(ROOT / "contracts/calculations/fec/summary-report-window/v2/policy.json")
    assert p["version"] == "legal-tender.fec.summary-report-window.v2"
    assert not any(p["guards"].values()) and not any(p["span_guards"].values())
    assert "CVG_START_DT" in p["flow_scope"] and "CVG_END_DT" in p["flow_scope"]
    assert "registration" in p["first_report"] and "not" in p["cumulative_columns"]


@REQUIRES
def test_official_scope_and_first_report_evidence_is_pinned():
    audit = Path(AUDIT)
    pins = {}
    for line in (ROOT / "docs/audit/fixtures/cycle-prefix-2026-09-10.sha256").read_text().splitlines():
        sha, name = line.split()
        pins[name] = sha
    sources = read(audit / "source-evidence.json")
    assert len(sources) == 4
    for source in sources:
        assert source["url"].startswith("https://www.fec.gov/")
        for kind in ("body", "headers"):
            item = source[kind]
            raw = (audit / item["path"]).read_bytes()
            assert len(raw) == item["bytes"]
            assert hashlib.sha256(raw).hexdigest() == item["sha256"] == pins[item["path"]]
    page = normalized_html(audit / "committee-overview.html")
    assert "Registration date: June 20, 2023" in page
    assert "raised in total receipts by this committee from April 01, 2023 to April 30, 2024" in page
    assert "spent in total disbursements by this committee from April 01, 2023 to April 30, 2024" in page
    guidance = normalized_html(audit / "first-report-guidance.html")
    assert "occurred before registration" in guidance and "beginning of the committee’s financial activity" in guidance
    cumulative = normalized_html(audit / "candidate-cycle-guidance.html")
    assert "Column B lists totals for the election cycle-to-date" in cumulative
    assert "six years for Senate candidates" in cumulative
    dictionary = normalized_html(audit / "summary-dictionary.html")
    assert "Beginning date for the first report during the two year period" in dictionary
    assert "Cash balance for the committee at the start of the two-year period" in dictionary


@REQUIRES
def test_registration_is_not_first_report_period_boundary():
    path = Path(REPORTS) / "C00843367-filings.json"
    pins = dict(line.split()[::-1] for line in (ROOT / "docs/audit/fixtures/summary-report-review-2026-09-10.sha256").read_text().splitlines())
    assert hashlib.sha256(path.read_bytes()).hexdigest() == pins[path.name]
    capture = read(path)
    assert capture["pagination"]["is_count_exact"] and capture["pagination"]["count"] == len(capture["results"]) == 18
    registrations = [r for r in capture["results"] if r["form_type"] == "F1" and r["amendment_indicator"] == "N"]
    assert len(registrations) == 1 and registrations[0]["file_number"] == 1708499
    assert registrations[0]["receipt_date"] == "2023-06-20T00:00:00"
    covers = [r for r in capture["results"] if r["form_type"] == "F3"]
    first = min(covers, key=lambda r: r["coverage_start_date"])
    assert first["file_number"] == 1714573 and first["coverage_start_date"] == "2023-04-01"
    assert first["coverage_start_date"] < registrations[0]["receipt_date"][:10]


def days(interval):
    lo, hi = date.fromisoformat(interval["start"]), date.fromisoformat(interval["end"])
    return {lo + timedelta(days=i) for i in range((hi - lo).days + 1)}


@REQUIRES
@pytest.mark.parametrize("name", ["sid-window", "sid-cycle", "sid-missing-cover", "nrcc-unresolved"])
def test_v2_exact_dates_money_and_unchanged_v1(name):
    audit = Path(AUDIT)
    old = read(Path(PRIOR) / f"{name}.json")
    assert (audit / f"{name}-v1.json").read_bytes() == (Path(PRIOR) / f"{name}.json").read_bytes()
    r = read(audit / f"{name}.json")
    for key in ("window", "summary", "summary_input", "summary_calculation_id", "summary_counts", "source_alignment"):
        assert r[key] == old[key]
    assert not r["financial_use_eligible"] and not r["terminal_attribution_eligible"] and not r["same_report_membership_proven"]
    for a, comparison in zip(r["summary"]["assertions"], r["comparisons"], strict=True):
        span = comparison["cycle_span"]
        assert span["reported"]["start"] == a["coverage_start"]["value"]
        assert span["reported"]["end"] == a["coverage_end"]["value"]
        assert span["state"] == "reported_span_only"
        assert not span["financial_coverage_established"] and not span["outside_activity_known"]
        observed = set()
        for key in ("outside_reported_prefix", "reported", "outside_reported_suffix"):
            interval = span[key]
            if interval is not None:
                represented = days(interval)
                assert not observed.intersection(represented)
                assert len(represented) == interval["days"]
                assert set(interval) == {"start", "end", "days"}  # No manufactured amount.
                observed.update(represented)
        assert observed == days(span["cycle"])
        for i, f in enumerate(comparison["fields"]):
            assert f["summary_minor_units"] == old["comparisons"][0]["fields"][i]["summary_minor_units"]
            assert f["window_minor_units"] == old["comparisons"][0]["fields"][i]["window_minor_units"]
            ready = name == "sid-window" and i != 5
            assert f["reported_comparison_ready"] == ready
            if ready:
                assert f["state"] == "equal" and not f["blockers"]
                assert int(f["delta_minor_units"]) == int(f["summary_minor_units"]) - int(f["window_minor_units"]) == 0
            else:
                assert f["state"] == "blocked" and f["delta_minor_units"] is None and f["blockers"]
        if name.startswith("sid"):
            assert (span["outside_reported_prefix"]["days"], span["reported"]["days"], span["outside_reported_suffix"]["days"]) == (90, 396, 245)
    if name == "sid-window":
        assert r["summary"]["assertions"][0]["diagnostic_equations"]["cash"]["delta_minor_units"] == "150000000"
        assert any(e["residual_minor_units"] == "-150000000" for e in r["window"]["equations"])


@REQUIRES
def test_go_fixture_retains_unknown_calendar_boundaries():
    r = read(Path(AUDIT) / "span-fixture.json")
    c = r["comparisons"][0]
    assert c["cycle_span"]["outside_reported_prefix"]["days"] == 90
    assert all(f["reported_comparison_ready"] for f in c["fields"][:5])
    assert c["fields"][5]["blockers"] == ["cycle_opening_boundary_unqualified"]
    assert not c["cycle_span"]["outside_activity_known"]
