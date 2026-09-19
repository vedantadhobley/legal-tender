"""Bounded, hash-pinned source audit; not a runtime parser or correction policy."""

import hashlib
import json
import os
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path

import pytest

from tests.test_receipt_report_lines import cents, measures

ROOT = Path(__file__).resolve().parents[1]
AUDIT = os.environ.get("LT_MEMO_REVIEW_DIR")
PROFILE = os.environ.get("LT_MEMO_REVIEW_PROFILE")
PINS = ROOT / "docs/audit/fixtures/receipt-memo-review-2026-09-10.sha256"
REQUIRES_AUDIT = pytest.mark.skipif(not AUDIT, reason="requires retained bounded memo evidence")
# Exact paper witnesses only. P3.4 is not the electronic format-8.4 layout.
PAPER = {
    "1879526": (9, 9, 56000), "1840593": (8, 2, 95300),
    "1723437": (3, 3, 116025), "1765938": (8, 7, 19875),
    "1821649": (6, 5, 3864), "1813852": (7, 4, 72000),
    "1882880": (1, 1, 20000), "1735670": (6, 2, 10565),
    "1767007": (18, 16, 1396891), "1733875": (13, 13, 65500),
}
UNKNOWN_IDS = {"A-234660", "A-281132"}
COVER_CENTS = {"1879526": 56000, "1840593": 2450500, "1723437": 116025,
               "1765938": 20875, "1821649": 7864, "1813852": 51000,
               "1882880": 20000, "1735670": 10565, "1767007": 255181, "1733875": 65500}


def pins():
    return {name: sha for sha, name in (line.split() for line in PINS.read_text().splitlines())}


def source(name):
    path = Path(AUDIT) / name
    assert path.stat().st_size <= 4 * 1024 * 1024
    body = path.read_bytes()
    assert hashlib.sha256(body).hexdigest() == pins()[name]
    return body


def records(name):
    # LF separates records; splitlines() would incorrectly split 0x1c fields.
    return [line.rstrip(b"\r").split(b"\x1c") for line in source(name).split(b"\n") if line.rstrip(b"\r")]


def result(file, suffix):
    path = Path(AUDIT) / f"{file}-{suffix}.json"
    assert path.stat().st_size <= 32 * 1024 * 1024
    return json.loads(path.read_bytes())


def test_memo_audit_fixture_scope():
    assert len(PAPER) == 10
    assert sum(v[0] for v in PAPER.values()) == 79
    assert sum(v[1] for v in PAPER.values()) == 62
    assert sum(v[2] for v in PAPER.values()) == 1856020
    assert set(pins()) == {"selected-profile.json", "1730162-electronic.fec"} | {
        file + suffix for file in PAPER for suffix in ("-paper.fec", "-image.pdf")
    }


@REQUIRES_AUDIT
@pytest.mark.skipif(not PROFILE, reason="requires completed v2 profile, not a new source scan")
def test_selection_is_complete_for_retained_v2_profile():
    selected = json.loads(source("selected-profile.json"))
    path = Path(PROFILE)
    assert path.stat().st_size == 266269624
    body = path.read_bytes()
    assert hashlib.sha256(body).hexdigest() == selected["profile_sha256"]
    profile = json.loads(body)
    assert profile["profile_id"] == selected["profile_id"]
    assert profile["schedule_a_source"] == selected["schedule_a_source"]
    unresolved = [g for g in profile["report_line_groups"] if g["key"]["disposition"] in ("unresolved_memo_code", "unresolved_line_amount")]
    pairs = {(g["key"]["committee"]["value"], g["key"]["file_num"]["value"]) for g in unresolved}
    assert selected["reports"] == [{"committee": c, "file": f} for c, f in sorted(pairs)]
    assert selected["groups"] == [g for g in profile["report_line_groups"] if (g["key"]["committee"]["value"], g["key"]["file_num"]["value"]) in pairs]
    assert sum(g["measures"]["rows"] for g in unresolved) == 64


@REQUIRES_AUDIT
def test_old_fact_cohort_matches_v4_grouped_evidence_without_relabeling():
    selected = json.loads(source("selected-profile.json"))
    assert len(selected["groups"]) == 26
    remaining = {json.dumps(g["key"], sort_keys=True): g for g in selected["groups"]}
    assert len(remaining) == 26
    total = 0
    for case in selected["reports"]:
        file = case["file"]
        report, lines = result(file, "report"), result(file, "lines")
        assert report["committee_id"] == lines["committee_id"] == case["committee"]
        assert report["file_num"] == lines["file_num"] == file
        assert report["input"] == lines["receipt_input"]
        assert report["input"]["fact_set_id"] == "8f69a2ef40316f62d8844ecf732342a3b4535136b434f60677d0608cb04465df"
        assert lines["receipt_source"] == {
            "release_id": "fec-a6e6f777c3c506b100a7709815bc89f2ae84749faf4343a4589f7ec1bac066a2",
            "manifest_sha256": "b921fda742759747b7e581c8897e8022ea5152eb33b86538ec80dbb9f13b5cad",
        }
        assert not lines["comparison_ready"] and not lines["terminal_attribution_eligible"]
        assert not report["terminal_attribution_eligible"]
        by_ordinal = {r["source_row_ordinal"]: r for r in report["receipts"]}
        assert len(by_ordinal) == len(report["receipts"])
        assert Counter(n for g in lines["groups"] for n in g["source_row_ordinals"]) == Counter(by_ordinal.keys())
        for group in lines["groups"]:
            members = [by_ordinal[n] for n in group["source_row_ordinals"]]
            key = dict(group["key"], committee={"present": True, "value": case["committee"]},
                       file_num={"present": True, "value": file})
            # The profile additionally separates the old individual predicate.
            partitions = defaultdict(list)
            for row in members:
                f = row["fields"]
                enriched = dict(key, individual_decision=row["key"]["individual_decision"],
                                report_type={"present": f["rpt_tp"] is not None, "value": f["rpt_tp"] or ""},
                                report_year={"present": f["rpt_yr"] is not None, "value": f["rpt_yr"] or ""})
                partitions[json.dumps(enriched, sort_keys=True)].append(f)
            assert group["measures"] == measures([r["fields"] for r in members])
            for encoded, fields in partitions.items():
                reference = remaining.pop(encoded)
                assert reference["measures"] == measures(fields)
                dates = [f["contb_receipt_dt"][:10] for f in fields if f["contb_receipt_dt"] is not None]
                expected = {"missing_rows": len(fields) - len(dates), "invalid_rows": 0,
                            "before_cycle_rows": sum(d < "2023-01-01" for d in dates),
                            "in_cycle_rows": sum("2023-01-01" <= d <= "2024-12-31" for d in dates),
                            "after_cycle_rows": sum(d > "2024-12-31" for d in dates),
                            "first_observed_date": min(dates, default=""), "last_observed_date": max(dates, default="")}
                assert reference["receipt_dates"] == expected
                # Ordinals belong to different releases and must not be equated.
        total += len(report["receipts"])
    assert total == 2863 and not remaining


def paper_observation(row):
    assert len(row) == 24
    return (row[0].decode(), row[1].decode(), date.fromisoformat(row[13].decode()).isoformat(),
            cents(row[20].decode()), row[21].decode(), row[22].decode(), row[23].decode())


def processed_observation(fields):
    return (fields["schedule_type"] + fields["line_num"], fields["cmte_id"], fields["contb_receipt_dt"][:10],
            int(fields["lt_receipt_amount_minor_units"]), fields["memo_cd"] or "", fields["memo_text"] or "", fields["image_num"])


@REQUIRES_AUDIT
@pytest.mark.parametrize("file", sorted(PAPER))
def test_paper_transcription_membership_and_unresolved_flags(file):
    rows = records(file + "-paper.fec")
    assert rows[0][:3] == [b"HDR", b"P3.4", b"Data Capture System"]
    cover = rows[1]
    form = "F3" if cover[0] == b"F3N" else "F3X"
    assert cover[0] == (form + "N").encode()
    assert len(cover) == (95 if form == "F3" else 125)
    report = result(file, "report")
    assert cover[1].decode() == report["committee_id"]
    receipts = [r for r in rows if r[0].startswith(b"SA")]
    # Count-preserving multiset, not a guessed paper transaction-ID join.
    assert Counter(map(paper_observation, receipts)) == Counter(processed_observation(r["fields"]) for r in report["receipts"])
    assert len(receipts) == PAPER[file][0]
    assert all(r["fields"]["tran_id"] is None for r in report["receipts"])
    assert all(r["fields"]["filing_form"] == form for r in report["receipts"])
    unusual = [r for r in receipts if r[0] == b"SA11AI" and r[21] == b"Y"]
    assert (len(unusual), sum(cents(r[20].decode()) for r in unusual)) == PAPER[file][1:]
    grouped = [g for g in result(file, "lines")["groups"] if g["key"]["disposition"] == "unresolved_memo_code"]
    assert sum(g["measures"]["rows"] for g in grouped) == len(unusual)
    pdf = source(file + "-image.pdf")
    assert pdf.startswith(b"%PDF-")  # Pins the image; no OCR/visual-validation claim.
    start, end, subtotal = (14, 15, 31) if form == "F3" else (12, 13, 28)
    period = [date.fromisoformat(cover[i].decode()).isoformat() for i in (start, end)]
    assert period[0] <= period[1]
    all_line = sum(cents(r[20].decode()) for r in receipts if r[0] == b"SA11AI")
    blank_line = sum(cents(r[20].decode()) for r in receipts if r[0] == b"SA11AI" and not r[21])
    assert cents(cover[subtotal].decode()) == COVER_CENTS[file]
    assert (all_line == COVER_CENTS[file]) == (file not in {"1840593", "1813852", "1767007"})
    assert blank_line != COVER_CENTS[file]
    print(json.dumps({"file": file, "coverage": period, "cover_itemized_minor_units": str(cents(cover[subtotal].decode())),
                      "all_line_minor_units": str(all_line), "blank_memo_line_minor_units": str(blank_line),
                      "Y_minor_units": str(PAPER[file][2])}, sort_keys=True))


@REQUIRES_AUDIT
def test_electronic_amount_discrepancies_are_present_in_original():
    rows = records("1730162-electronic.fec")
    assert rows[0][:3] == [b"HDR", b"FEC", b"8.4"]
    assert rows[1][:2] == [b"F3N", b"C00845032"] and len(rows[1]) == 93
    layout = json.loads((ROOT / "contracts/sources/fec/efile-format/v1/schedule-a-fields.json").read_text())
    ix = {f["name"]: f["sequence"] - 1 for f in layout["fields"]}
    original = [r for r in rows if r[0].startswith(b"SA")]
    assert all(len(r) == 45 for r in original)
    by_id = {r[ix["transaction_id"]].decode(): r for r in original}
    report = result("1730162", "report")
    assert len(by_id) == len(original) == len(report["receipts"]) == 2784
    assert set(by_id) == {r["fields"]["tran_id"] for r in report["receipts"]}
    unknown = set()
    for row in report["receipts"]:
        f = row["fields"]
        raw = by_id[f["tran_id"]]
        for field, original_field in (("cmte_id", "filer_committee_id_number"), ("memo_cd", "memo_code"),
                                      ("memo_text", "memo_text_description")):
            assert (f[field] or "") == raw[ix[original_field]].decode()
        assert f["schedule_type"] + f["line_num"] == raw[0].decode()
        assert f["contb_receipt_dt"][:10] == date.fromisoformat(raw[ix["contribution_date"]].decode()).isoformat()
        original_cents = cents(raw[ix["contribution_amount"]].decode())
        if f["lt_receipt_amount_minor_units"] is None:
            assert f["contb_receipt_amt"] is None
            assert f["tran_id"] in UNKNOWN_IDS and original_cents == 1
            unknown.add(f["tran_id"])
        else:
            assert int(f["lt_receipt_amount_minor_units"]) == original_cents
    assert unknown == UNKNOWN_IDS
    original_line = [r for r in original if r[0] == b"SA11AI" and not r[ix["memo_code"]]]
    original_cents = sum(cents(r[ix["contribution_amount"]].decode()) for r in original_line)
    processed_cents = int(result("1730162", "lines")["reviewed_nonmemo_line_population"]["signed_minor_units"])
    assert original_cents - processed_cents == 2
    assert original_cents == cents(rows[1][32].decode()) == 61136496
    print(json.dumps({"file": "1730162", "original_nonmemo_minor_units": str(original_cents),
                      "processed_known_nonmemo_minor_units": str(processed_cents),
                      "cover_itemized_minor_units": str(cents(rows[1][32].decode()))}, sort_keys=True))
