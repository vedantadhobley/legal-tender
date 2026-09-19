"""Independent positive original/profile witnesses; no financial counting policy."""
import base64
import hashlib
import json
import os
from collections import Counter, defaultdict
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from tests.test_report_field_binding import minor, workbook_rows

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "docs/audit/fixtures/positive-receipt-families-2026-09-11.json"
AUDIT = os.environ.get("LT_POSITIVE_FAMILY_AUDIT")
STORAGE = os.environ.get("LT_POSITIVE_FAMILY_STORAGE")
REQUIRES = pytest.mark.skipif(not AUDIT or not STORAGE, reason="requires bounded originals and saved profile")


def read(path):
    return json.loads(path.read_bytes(), parse_float=Decimal)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def test_fixture_covers_remaining_initial_family_shapes_without_runtime_exceptions():
    fixture = read(FIXTURE)
    source = read(ROOT / "contracts/calculations/fec/receipt-families/v1/contract.json")
    policy = read(ROOT / "contracts/calculations/fec/receipt-family-comparison/v1/policy.json")
    covered = set()
    for case in fixture["cases"]:
        leaves = {f["id"]: f for f in source["forms"][case["form"]]["leaves"]}
        assert case["targets"] and case["file"] not in json.dumps(policy)
        for family, amount in case["targets"].items():
            assert int(amount) > 0
            assert leaves[family]["detail_schedule"] == "SA" and leaves[family]["detail_relation"] == "all_required_itemized"
            covered.add((case["form"], family))
    # Positive shapes already checked by the preceding retained source gates.
    covered |= {("F3", "candidate_made_or_guaranteed_loans"), ("F3X", "other_committee_contributions"), ("F3X", "affiliated_or_party_transfers")}
    assert covered == {(form, family) for form, families in policy["families"].items() for family in families}
    assert not any(policy["guards"].values())
    assert sum(not c["retained"] for c in fixture["cases"]) == 4


@pytest.fixture(scope="module")
def sources():
    fixture = read(FIXTURE)
    raw = (Path(STORAGE) / fixture["profile_storage_key"]).read_bytes()
    assert sha(raw) == fixture["profile_sha256"]
    profile = json.loads(raw)
    layout = read(ROOT / "contracts/sources/fec/efile-format/v1/schedule-a-fields.json")
    indexes = {f["name"]: f["sequence"] - 1 for f in layout["fields"]}
    workbook = Path(STORAGE) / "dumps/audits/fec/report-field-binding/2026-09-10/attempt-01/FEC_Format_v8.4.xlsx"
    assert sha(workbook.read_bytes()) == "9d3775d73e9398144b0e0267415ba53e1b5c6a326110b327ce2cd58d233bf3d6"
    sheets = workbook_rows(workbook, ("Sch A", "F3", "F3X"))
    for field in layout["fields"]:
        if field["name"] in ("form_type", "filer_committee_id_number", "transaction_id", "contribution_date", "contribution_amount", "memo_code"):
            assert sheets["Sch A"][field["sequence"]]["B"].strip() == field["publisher_name"]
    return fixture, profile, indexes, sheets


def measures(rows, index):
    values = [int(minor(row[index["contribution_amount"]].decode("ascii"))) for row in rows]
    return {"rows": len(values), "known_amount_rows": len(values), "unknown_amount_rows": 0,
            "positive_rows": sum(v > 0 for v in values), "negative_rows": sum(v < 0 for v in values), "zero_rows": values.count(0),
            "signed_minor_units": sum(values), "positive_minor_units": sum(v for v in values if v > 0), "negative_minor_units": sum(v for v in values if v < 0)}


@REQUIRES
@pytest.mark.parametrize("file", [c["file"] for c in read(FIXTURE)["cases"]])
def test_complete_original_profile_and_positive_cover_fields(file, sources):
    fixture, profile, index, sheets = sources
    c = next(c for c in fixture["cases"] if c["file"] == file)
    verify_original_family_case(c, fixture, profile, index, sheets, AUDIT, STORAGE, os.environ.get("LT_POSITIVE_FAMILY_RECORD_RESULTS") == "1")


def verify_original_family_case(c, fixture, profile, index, sheets, audit, storage, record_results=False, v2=False):
    file = c["file"]
    base = Path(storage if c["retained"] else audit)
    body = (base / c["body"]).read_bytes()
    assert sha(body) == c["body_sha256"]
    assert sha((base / c["headers"]).read_bytes()) == c["headers_sha256"]
    assert len(body) <= 4 << 20
    parts = body.split(b"\n")
    raw_rows = [part + b"\n" for part in parts[:-1]]
    if parts[-1]:
        raw_rows.append(parts[-1])  # Preserve the complete HTTP body's unterminated last row.
    rows = [r.removesuffix(b"\n").removesuffix(b"\r").split(b"\x1c") for r in raw_rows]
    assert 2 < len(rows) <= 4096 and b"".join(raw_rows) == body
    r = read(Path(audit) / f"{file}-source.json")
    a = r["assessment"]
    assert a["version"] == "legal-tender.fec.receipt-family-cover." + ("v2" if v2 else "v1")
    assert a["file_number"] == file and a["source_url"] == f"https://docquery.fec.gov/dcdev/posted/{file}.fec"
    assert a["representation"] == "electronic_8.4" and a["capture_extent"] == "complete_response" and not a["issues"]
    assert a["disposition"] == "electronic_cover_parsed" and not a["financial_selection_ready"] and not a["history_complete"]
    assert not a["metadata"] and not any(r[k] for k in ("metadata_binding_proven", "financial_use_eligible", "terminal_attribution_eligible"))
    assert len(a["records"]) == len(rows)
    offset = 0
    for ordinal, (raw, record) in enumerate(zip(raw_rows, a["records"], strict=True), 1):
        assert record["ordinal"] == ordinal and record["offset"] == offset and record["bytes"] == len(raw)
        assert record["complete"] and base64.b64decode(record["raw_base64"]) == raw and record["sha256"] == sha(raw)
        offset += len(raw)
    assert rows[0][:3] == [b"HDR", b"FEC", b"8.4"]
    assert rows[1][1].decode("ascii") == c["committee"] and len(rows[1]) == (93 if c["form"] == "F3" else 123)
    assert [s.decode("utf8") for s in rows[1]] == a["cover"]["fields"]
    unsupported = c.get("expected_unreviewed_records", {})
    assert r["census"]["layout_census_complete"] == (not unsupported) and not r["census"]["scope_issues"]
    expected_issues = [{"record_ordinal": i, "code": "unreviewed_record_family"} for i, row in enumerate(rows, 1) if row[0].decode("ascii") in unsupported]
    assert r["census"]["issues"] == expected_issues
    assert {tag: sum(row[0].decode("ascii") == tag for row in rows) for tag in unsupported} == unsupported
    for key, predicate in (("schedule_a_lines", lambda tag: tag.startswith("SA")), ("other_records", lambda tag: not tag.startswith("SA"))):
        expected = defaultdict(list)
        for ordinal, row in enumerate(rows[2:], 3):
            tag = row[0].decode("ascii")
            if predicate(tag):
                expected[tag].append(ordinal)
        assert r["census"][key] == [{"tag": tag, "record_ordinals": ordinals} for tag, ordinals in sorted(expected.items())]

    groups = r["profile_groups"]
    assert r["profile_id"] == profile["profile_id"] and r["summary_input"] == profile["summary_input"] and r["schedule_a_source"] == profile["schedule_a_source"]
    assert groups == [g for g in profile["report_line_groups"] if g["key"]["committee"] == {"present": True, "value": c["committee"]} and g["key"]["file_num"] == {"present": True, "value": file}]
    original = defaultdict(list)
    ids = Counter()
    for row in rows[2:]:
        if not row[0].startswith(b"SA"):
            continue  # Loans/debts remain raw evidence, not additional receipts.
        assert len(row) == 45 and row[index["filer_committee_id_number"]].decode("ascii") == c["committee"]
        ids[row[index["transaction_id"]]] += 1
        original[(row[0].decode("ascii"), row[index["memo_code"]].decode("ascii"))].append(row)
    assert b"" not in ids and all(count == 1 for count in ids.values())
    grouped = defaultdict(list)
    for g in groups:
        k = g["key"]
        assert k["filing_form"] == {"present": True, "value": c["form"]}
        assert k["schedule_type"] == {"present": True, "value": "SA"}
        assert k["report_type"]["value"] == a["cover"]["report_code"]
        assert k["report_year"]["value"] == a["cover"]["coverage_end"][:4]
        grouped[("SA" + k["line_num"]["value"], k["memo_code"]["value"])].append(g)
    assert set(grouped) == set(original)
    comparisons, differences = [], []
    start, end = date(int(fixture["cycle"]) - 1, 1, 1), date(int(fixture["cycle"]), 12, 31)
    for (tag, memo), originals in sorted(original.items()):
        gs = grouped[(tag, memo)]
        observed = measures(originals, index)
        stored = {key: sum(int(g["measures"][key]) for g in gs) for key in observed}
        assert stored["rows"] == observed["rows"]
        dates = [date.fromisoformat(row[index["contribution_date"]].decode("ascii")) for row in originals]
        assert min(dates).isoformat() == min(g["receipt_dates"]["first_observed_date"] for g in gs)
        assert max(dates).isoformat() == max(g["receipt_dates"]["last_observed_date"] for g in gs)
        for key, n in {"missing_rows": 0, "invalid_rows": 0, "before_cycle_rows": sum(d < start for d in dates), "in_cycle_rows": sum(start <= d <= end for d in dates), "after_cycle_rows": sum(d > end for d in dates)}.items():
            assert sum(g["receipt_dates"][key] for g in gs) == n
        delta = str(observed["signed_minor_units"] - stored["signed_minor_units"])
        comparisons.append({"tag": tag, "memo": memo, "original": observed, "processed": stored, "original_minus_processed_known_minor_units": delta, "state": "equal" if observed == stored else "different"})
        if observed != stored:
            differences.append({"tag": tag, "memo": memo, "processed_unknown_amount_rows": stored["unknown_amount_rows"], "original_minus_processed_known_minor_units": delta})
            # The existing nulls stay unknown. They are not replaced by original cents.
            assert stored["unknown_amount_rows"] == observed["known_amount_rows"] - stored["known_amount_rows"]
            assert observed["positive_rows"] - stored["positive_rows"] == stored["unknown_amount_rows"]
            assert observed["positive_minor_units"] - stored["positive_minor_units"] == int(delta)
            assert all(observed[key] == stored[key] for key in ("negative_rows", "zero_rows", "negative_minor_units"))
    assert differences == c["expected_group_differences"]

    contract = read(ROOT / "contracts/calculations/fec/receipt-families/v1/contract.json")
    leaves = {leaf["id"]: leaf for leaf in contract["forms"][c["form"]]["leaves"]}
    families = []
    for family, expected in c["targets"].items():
        leaf = leaves[family]
        assert sheets[c["form"]][leaf["sequence"]]["B"] == leaf["label"]
        original_amount = minor(rows[1][leaf["sequence"] - 1].decode("ascii"))
        comparison = next(g for g in comparisons if g["tag"] == "SA" + leaf["line"] and g["memo"] == "")
        detail = c["detail_targets"][family] if v2 else expected
        assert original_amount == expected
        assert detail == str(comparison["original"]["signed_minor_units"]) == str(comparison["processed"]["signed_minor_units"])
        assert comparison["state"] == "equal" and int(expected) > 0
        spec = next(f for f in a["period_fields"] if any(a["sequence"] == leaf["sequence"] for a in f["amounts"]))
        assert all(a["state"] == "valid" and a["minor_units"] == expected for a in spec["amounts"])
        if not v2:
            assert len(spec["amounts"]) == 1
        family_result = {"family": family, "line": leaf["line"], "nonmemo_rows": comparison["original"]["rows"], "minor_units": expected}
        if v2:
            relation = leaf["detail_relation"]
            family_result.update(detail_minor_units=detail, detail_relation=relation, cover_detail_difference_minor_units=None if relation == "thresholded_component_of_total" else str(int(expected)-int(detail)))
        families.append(family_result)
    result = {"file": file, "form": c["form"], "original_bytes": len(body), "original_records": len(rows), "schedule_a_rows": sum(len(rows) for rows in original.values()), "families": families, "groups": comparisons, "known_differences": differences, "layout_census_complete": r["census"]["layout_census_complete"], "unreviewed_records": unsupported, "financial_use_eligible": False}
    if record_results:
        (Path(audit) / f"{file}-comparison.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


@REQUIRES
def test_existing_unknown_amounts_and_loan_records_remain_separate(sources):
    fixture, _, _, _ = sources
    rows = {c["file"]: read(Path(AUDIT) / f'{c["file"]}-source.json') for c in fixture["cases"]}
    reused = rows["1730162"]
    assert sum(g["measures"]["unknown_amount_rows"] for g in reused["profile_groups"]) == 2
    for file in ("1754019", "1709268"):
        r = rows[file]
        assert sum(g["measures"]["rows"] for g in r["profile_groups"]) == 1
        assert any(line["tag"] == "SC/10" for line in r["census"]["other_records"])
        assert not r["financial_use_eligible"] and not r["metadata_binding_proven"]
    # The source has an amendment. Reading it did not decide its financial role.
    assert rows["1754019"]["assessment"]["cover"]["form"] == "F3A"
    assert not rows["1754019"]["assessment"]["history_complete"]
