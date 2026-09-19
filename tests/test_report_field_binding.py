"""Independent workbook, raw-byte, metadata, and reported-value binding checks."""
import base64
import hashlib
import json
import os
import xml.etree.ElementTree as ET
import zipfile
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
LAYOUT = ROOT / "contracts/sources/fec/electronic-cover/v1/layout.json"
POLICY = ROOT / "contracts/calculations/fec/report-field-binding/v1/policy.json"
AUDIT = os.environ.get("LT_REPORT_BINDING_AUDIT")
REPORTS = os.environ.get("LT_REPORT_BINDING_REPORTS")
METADATA = os.environ.get("LT_REPORT_BINDING_METADATA")
REQUIRES_AUDIT = pytest.mark.skipif(not all((AUDIT, REPORTS, METADATA)), reason="requires retained offline gate")


def read(path):
    return json.loads(path.read_bytes(), parse_float=Decimal)


def pins():
    result = {}
    for fixture in ("summary-report-review", "report-field-binding", "receipt-report-metadata"):
        for line in (ROOT / f"docs/audit/fixtures/{fixture}-2026-09-10.sha256").read_text().splitlines():
            sha, name = line.split()
            result[name] = sha
    return result


def minor(raw):
    scaled = Decimal(raw) * 100
    assert scaled == scaled.to_integral_value()
    return str(int(scaled))


def test_binding_contract_keeps_financial_guards_separate():
    policy, layout = read(POLICY), read(LAYOUT)
    assert policy["version"] == "legal-tender.fec.report-field-binding.v1"
    assert not any(policy["guards"].values()) and not any(layout["guards"].values())
    assert layout["header"]["physical_widths"] == [7, 8]
    assert "cycle totals" in policy["non_goals"]
    for form in ("F3", "F3X"):
        assert len(layout["layouts"][form]["period_fields"]) == 7


def workbook_rows(path, sheet_names=("HDR", "F3", "F3X")):
    ns = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    result = {}
    with zipfile.ZipFile(path) as z:
        strings = ["".join(t.text or "" for t in x.findall(".//s:t", ns)) for x in ET.fromstring(z.read("xl/sharedStrings.xml"))]
        relations = {r.attrib["Id"]: r.attrib["Target"] for r in ET.fromstring(z.read("xl/_rels/workbook.xml.rels"))}
        for sheet in ET.fromstring(z.read("xl/workbook.xml")).findall("s:sheets/s:sheet", ns):
            if sheet.attrib["name"] not in sheet_names:
                continue
            relation = sheet.attrib["{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"]
            rows = {}
            for row in ET.fromstring(z.read("xl/" + relations[relation])).findall(".//s:row", ns):
                cells = {}
                for cell in row.findall("s:c", ns):
                    v = cell.find("s:v", ns)
                    if v is not None:
                        cells[cell.attrib["r"].rstrip("0123456789")] = strings[int(v.text)] if cell.attrib.get("t") == "s" else v.text
                if cells.get("A", "").isdigit():
                    rows[int(cells["A"])] = cells
            result[sheet.attrib["name"]] = rows
    return result


@REQUIRES_AUDIT
def test_official_84_workbook_mapping():
    layout = read(LAYOUT)
    for name in ("format84-member.headers", "format84-member.bin", "FEC_Format_v8.4.xlsx"):
        assert hashlib.sha256((Path(AUDIT) / name).read_bytes()).hexdigest() == pins()[name]
    workbook = Path(AUDIT) / "FEC_Format_v8.4.xlsx"
    assert len(workbook.read_bytes()) == layout["source"]["bytes"]
    assert hashlib.sha256(workbook.read_bytes()).hexdigest() == layout["source"]["sha256"]
    sheets = workbook_rows(workbook)
    assert "original report" in sheets["HDR"][6]["G"]
    assert sheets["HDR"][7]["C"] == "N-3"
    assert sheets["HDR"][8]["B"].strip() == "HDRcomment"
    for form in ("F3", "F3X"):
        spec, rows = layout["layouts"][form], sheets[form]
        assert sorted(rows) == list(range(1, spec["width"] + 1))
        assert rows[spec["report_code"]]["B"] == "REPORT CODE"
        assert rows[spec["coverage_start"]]["B"] == "COVERAGE FROM DATE"
        assert rows[spec["coverage_end"]]["B"] == "COVERAGE THROUGH DATE"
        amounts = {seq for lo, hi in spec["amount_ranges_inclusive"] for seq in range(lo, hi + 1)}
        assert amounts == {seq for seq, row in rows.items() if row.get("C", "").strip() == "AMT-12"}
        for field, sequences in spec["period_fields"].items():
            for seq in sequences:
                label = rows[seq]["B"].lower()
                assert seq in amounts
                if "unitemized" in field:
                    assert "unitemized" in label
                elif "itemized" in field:
                    assert "itemized" in label and "unitemized" not in label
                elif "individual" in field:
                    assert "total" in label and "11(a" in label
                elif "receipts" in field:
                    assert "total receipts" in label and "federal" not in label
                elif "disbursements" in field:
                    assert "total disbursements" in label
                elif "beginning" in field:
                    assert "cash" in label and "beginning" in label
                else:
                    assert "cash" in label and "close" in label
        # Official column-B headings separate the same-named running totals.
        assert max(seq for values in spec["period_fields"].values() for seq in values) < (63 if form == "F3" else 74)


@REQUIRES_AUDIT
@pytest.mark.parametrize("file", ["1714573", "1766866", "1743911", "1780310", "1780346", "1766839", "1833804"])
def test_retained_binding_is_exact_source_evidence(file):
    r = read(Path(AUDIT) / f"{file}-binding.json")
    d, m = r["document"], r["membership"]
    name = file + ("-prefix.fec" if file == "1833804" else ".fec")
    body = (Path(REPORTS) / name).read_bytes()
    for kind, artifact in (("body", name), ("headers", name + ".headers")):
        raw = (Path(REPORTS) / artifact).read_bytes()
        assert d[kind]["sha256"] == pins()[artifact] == hashlib.sha256(raw).hexdigest()
        assert d[kind]["bytes"] == len(raw)
    offset = 0
    for i, rec in enumerate(d["records"], 1):
        raw = base64.b64decode(rec["raw_base64"], validate=True)
        assert rec["ordinal"] == i and rec["offset"] == offset and rec["bytes"] == len(raw)
        assert raw == body[offset:offset + len(raw)] and rec["sha256"] == hashlib.sha256(raw).hexdigest()
        offset += len(raw)
    assert offset == len(body)
    lines = body.splitlines()  # bytes.splitlines does not treat 1c as a newline.
    header = lines[0].decode("ascii").split("\x1c")
    cover = lines[1].decode("ascii").split("\x1c")
    assert d["header_fields"] == header and d["cover"]["fields"] == cover
    spec = read(LAYOUT)["layouts"]["F3X" if cover[0].startswith("F3X") else "F3"]
    assert len(cover) == spec["width"] and header[:3] == ["HDR", "FEC", "8.4"]
    expected_seqs = [seq for lo, hi in spec["amount_ranges_inclusive"] for seq in range(lo, hi + 1)]
    assert [f["sequence"] for f in d["cover"]["amounts"]] == expected_seqs
    for f in d["cover"]["amounts"]:
        assert f["raw"] == cover[f["sequence"] - 1]
        assert f["state"] == ("valid" if f["raw"] else "blank")
        if f["raw"]:
            assert f["minor_units"] == minor(f["raw"])
    capture_path = Path(METADATA) / ("nrcc-reports-capture.json" if file == "1833804" else "sid-reports-capture.json")
    assert m["evidence"]["capture_sha256"] == hashlib.sha256(capture_path.read_bytes()).hexdigest()
    source = []
    for page in read(capture_path)["pages"]:
        for kind in ("body", "headers"):
            a = page[kind]
            raw = (Path(METADATA) / a["path"]).read_bytes()
            assert a["sha256"] == pins()[a["path"]] == hashlib.sha256(raw).hexdigest() and a["bytes"] == len(raw)
        source.extend(read(Path(METADATA) / page["body"]["path"])["results"])
    index = next(i for i, raw in enumerate(source) if str(raw["file_number"]) == file)
    assert r["observation_index"] == index
    raw = source[index]
    should_bind = file not in ("1766839", "1833804")
    assert r["scope_bound"] is should_bind
    if should_bind:
        assert index in m["chain_candidate_indexes"] and raw["is_amended"] is False
        assert raw["amendment_indicator"] == cover[0][-1]
        if len(raw["amendment_chain"]) > 1:
            assert header[5] == "FEC-" + raw["amendment_chain"][0]
    for f in r["fields"]:
        assert [v["sequence"] for v in f["cover"]] == spec["period_fields"][f["name"]]
        assert f["metadata"]["raw"] == raw[f["name"]]
        assert f["metadata"]["minor_units"] == minor(raw[f["name"]])
        for v in f["cover"]:
            assert v["raw"] == cover[v["sequence"] - 1]
            assert v["minor_units"] == minor(v["raw"]) == f["metadata"]["minor_units"]
        assert f["delta_minor_units"] == "0" and f["reported_value_bound"] is should_bind
    assert len(r["fields"]) == 7
    assert not any(r[key] for key in ("cycle_total_ready", "cash_basis_ready", "terminal_attribution_eligible"))
    assert not m["financial_membership_ready"] and not m["cycle_total_ready"]


@REQUIRES_AUDIT
def test_matching_cash_fields_do_not_hide_carry_forward_difference():
    def fields(file):
        r = read(Path(AUDIT) / f"{file}-binding.json")
        assert r["scope_bound"] and not r["cash_basis_ready"]
        return {f["name"]: int(f["cover"][0]["minor_units"]) for f in r["fields"]}
    prior, termination = fields("1780310"), fields("1780346")
    assert termination["cash_on_hand_beginning_period"] - prior["cash_on_hand_end_period"] == -150000000
    partial = read(Path(AUDIT) / "1833804-binding.json")
    assert partial["scope_blockers"] == ["not_observed_chain_candidate", "partial_document_capture"]
    assert partial["document"]["capture_extent"] == "prefix"
    assert partial["fields"][1]["metadata"]["minor_units"] == "225692361"
