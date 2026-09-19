"""Pinned source qualification only; no runtime acquisition or report selector."""

import csv
import hashlib
import io
import json
import os
import re
import struct
import xml.etree.ElementTree as ET
import zipfile
import zlib
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
AUDIT = os.environ.get("LT_REPORT_METADATA_AUDIT")
ORIGINALS = os.environ.get("LT_REPORT_METADATA_ORIGINALS")
REQUIRES_AUDIT = pytest.mark.skipif(not AUDIT, reason="requires pinned report-metadata audit")
REQUIRES_ORIGINALS = pytest.mark.skipif(not AUDIT or not ORIGINALS, reason="requires retained original report witnesses")
NS = {"s": "http://s3.amazonaws.com/doc/2006-03-01/"}


def pins(name="receipt-report-metadata"):
    path = ROOT / f"docs/audit/fixtures/{name}-2026-09-10.sha256"
    return {file: sha for sha, file in (line.split() for line in path.read_text().splitlines())}


def source(name):
    p = Path(AUDIT) / name
    assert p.stat().st_size <= 6 * 1024 * 1024
    b = p.read_bytes()
    assert hashlib.sha256(b).hexdigest() == pins()[name]
    return b


def original(name):
    p = Path(ORIGINALS) / name
    assert p.stat().st_size <= 10 * 1024 * 1024
    b = p.read_bytes()
    assert hashlib.sha256(b).hexdigest() == pins("summary-report-review")[name]
    return b


def payload(name):
    return json.loads(source(name), parse_float=Decimal)


def records(body):
    return [r.rstrip(b"\r").split(b"\x1c") for r in body.split(b"\n") if r.rstrip(b"\r")]


def complete_page(name, count):
    d = payload(name)
    assert d["pagination"] == {"count": count, "is_count_exact": True, "page": 1, "pages": 1, "per_page": 100}
    assert len(d["results"]) == count
    rows = {r["file_number"]: r for r in d["results"]}
    assert len(rows) == count
    return rows


def range_headers(name, body):
    headers = source(name + ".headers").decode()
    assert "206" in headers.splitlines()[0]
    lo, hi, total = map(int, re.search(r"(?im)^content-range: bytes (\d+)-(\d+)/(\d+)", headers).groups())
    assert len(body) == hi - lo + 1 and hi < total
    etag = re.search(r"(?im)^etag: ([^\r\n]+)", headers)[1]
    return lo, hi, total, etag


def workbook_fields(body, sheet):
    ns = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    with zipfile.ZipFile(io.BytesIO(body)) as z:
        assert sum(x.file_size for x in z.infolist()) < 20 * 1024 * 1024
        strings = ["".join(t.text or "" for t in r.findall(".//s:t", ns))
                   for r in ET.fromstring(z.read("xl/sharedStrings.xml"))]
        sheets = ET.fromstring(z.read("xl/workbook.xml")).findall("s:sheets/s:sheet", ns)
        selected = next(s for s in sheets if s.attrib["name"] == sheet)
        rel_id = selected.attrib["{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"]
        target = next(r.attrib["Target"] for r in ET.fromstring(z.read("xl/_rels/workbook.xml.rels")) if r.attrib["Id"] == rel_id)
        fields = {}
        for row in ET.fromstring(z.read("xl/" + target)).findall(".//s:row", ns):
            values = []
            for cell in row.findall("s:c", ns):
                v = cell.find("s:v", ns)
                if v is not None:
                    values.append(strings[int(v.text)] if cell.attrib.get("t") == "s" else v.text)
            if values and values[0].isdigit():
                fields[int(values[0])] = values[1:]
        return fields


def test_qualification_fixture_manifest():
    p = pins()
    assert {"swagger.json", "filing-witnesses.json", "sid-reports.json", "nrcc-reports.json", "paper-v34.xlsx"} <= p.keys()
    assert all(re.fullmatch(r"[0-9a-f]{64}", v) for v in p.values())


@REQUIRES_AUDIT
def test_all_qualification_artifacts_pinned():
    for name in pins():
        source(name)


@REQUIRES_AUDIT
def test_period_bulk_headers_do_not_supply_filing_identity():
    for kind in ("HOUSE_SENATE_CAMPAIGNS", "PAC", "PARTY"):
        stem = f"2024_{kind}_DOWNLOAD-prefix"
        body = source(stem + ".csv")
        headers = source(stem + ".headers").decode()
        assert len(body) == 8192 and "206" in headers.splitlines()[0]
        assert "content-range: bytes 0-8191/" in headers.lower()
        # Only complete first records are used; the final prefix record may be partial.
        rows = csv.reader(io.StringIO(body.decode()))
        columns, sample = next(rows), next(rows)
        assert len(columns) == len(sample) == 98
        assert columns.count("CAND_ID") == 2
        assert {"FORM_TP_CD", "RPT_TP", "RPT_YR", "CVG_START_DT", "CVG_END_DT"} <= set(columns)
        assert not {"FILE_NUM", "FILE_NUMBER", "AMENDMENT_CHAIN", "AMNDT_IND"} & set(columns)
        assert "candidateCommitteeId=" + sample[columns.index("CMTE_ID")] in sample[columns.index("LINK_IMAGE")]
        assert "tabIndex=3" in sample[columns.index("LINK_IMAGE")]


@REQUIRES_AUDIT
def test_bulk_listing_scope_and_missing_days_are_not_empty_reports():
    for name in ("bulk-downloads", "data-dump", "schedules", "2024", "historical-reports", "data.fec.gov", "fecviewer", "data_requests"):
        d = ET.fromstring(source(name + "-listing.xml"))
        assert d.findtext("s:IsTruncated", namespaces=NS) == "false"
    for name in ("paper", "electronic"):
        assert ET.fromstring(source(name + "-listing.xml")).findtext("s:IsTruncated", namespaces=NS) == "true"
    d = ET.fromstring(source("schedules-listing.xml"))
    assert {x.text.rsplit("/", 1)[-1] for x in d.findall("s:Contents/s:Key", NS)} == {
        "README.txt", "fec_fitem_sched_a.dump", "fec_fitem_sched_b.dump", "fec_fitem_sched_e.dump", "ofec_committee_history.dump",
    }
    june = ET.fromstring(source("paper-202606-listing.xml"))
    assert june.findtext("s:IsTruncated", namespaces=NS) == "false"
    days = {int(x.text.rsplit("/", 1)[-1][6:8]) for x in june.findall("s:Contents/s:Key", NS)}
    assert set(range(1, 31)) - days == {8}
    september = ET.fromstring(source("paper-202609-listing.xml"))
    entries = september.findall("s:Contents", NS)
    assert september.findtext("s:IsTruncated", namespaces=NS) == "false" and len(entries) == 9
    assert all(x.findtext("s:Key", namespaces=NS).endswith(".nofiles.zip") and x.findtext("s:Size", namespaces=NS) == "1" for x in entries)
    # These are publisher object-list observations, not evidence that no filings exist.


@REQUIRES_AUDIT
def test_selected_schema_members_match_bounded_zip_bytes():
    for package, capture, decoded in (("eFilingFormats", "efile-readme", "efile-readme.txt"), ("PaperFormats", "paper-v34", "paper-v34.xlsx")):
        tail = source(package + "-tail.bin")
        marker = tail.rfind(b"PK\x05\x06")
        end = struct.unpack_from("<4s4H2LH", tail, marker)
        assert end[1] == end[2] == 0 and end[3] == end[4] and end[7] == 0
        lo, hi, total, etag = range_headers(package + "-tail", tail)
        assert hi + 1 == total and end[6] + end[5] == lo + marker
        directory = payload(package + "-directory.json")
        assert len(directory) == end[4]
        pos = end[6] - lo
        for entry in directory:
            h = struct.unpack_from("<4s6H3L5H2L", tail, pos)
            assert h[0] == b"PK\x01\x02"
            assert entry == {"name": tail[pos+46:pos+46+h[10]].decode(), "flags": h[3], "method": h[4],
                             "crc": h[7], "compressed": h[8], "size": h[9], "offset": h[-1]}
            pos += 46 + h[10] + h[11] + h[12]
        assert pos == marker
        expected = next(x for x in payload("selected-members.json") if x["package"] == package)
        row = expected["member"]
        assert row in directory
        body = source(capture + "-member.bin")
        member_lo, _, member_total, member_etag = range_headers(capture + "-member", body)
        assert member_lo == row["offset"] and member_total == total == expected["archive_bytes"]
        assert member_etag == etag
        h = struct.unpack_from("<4s5H3L2H", body)
        assert h[0] == b"PK\x03\x04" and h[3] == row["method"] == 8
        assert h[2] == row["flags"]
        assert body[30:30+h[9]].decode() == row["name"]
        offset = 30 + h[9] + h[10]
        inflater = zlib.decompressobj(-15)
        raw = inflater.decompress(body[offset:offset+row["compressed"]], row["size"] + 1)
        assert inflater.eof and not inflater.unused_data and not inflater.unconsumed_tail
        assert len(raw) == row["size"] and zlib.crc32(raw) == row["crc"]
        assert raw == source(decoded)
        assert hashlib.sha256(raw).hexdigest() == expected["sha256"]


@REQUIRES_AUDIT
def test_endpoint_types_and_incremental_limits_stay_explicit():
    schema = payload("swagger.json")
    paths = schema["paths"]
    parameters = {p["name"] for p in paths["/v1/filings/"]["get"]["parameters"]}
    assert {"min_receipt_date", "max_receipt_date", "file_number", "cycle", "report_year"} <= parameters
    assert not {"min_update_date", "max_update_date", "min_load_timestamp", "updated_since"} & parameters
    filings = complete_page("filing-witnesses.json", 7)
    reports = complete_page("nrcc-reports.json", 34)
    assert filings[1833804]["is_amended"] is False and reports[1833804]["is_amended"] is True
    assert filings[1833804]["most_recent"] is False and reports[1833804]["most_recent"] is True
    assert filings[1882886]["is_amended"] is None and reports[1882886]["is_amended"] is False
    assert filings[1882886]["previous_file_number"] == -1147523
    assert reports[1882886]["previous_file_number"] is None
    assert filings[1882886]["amendment_chain"] is reports[1882886]["amendment_chain"] is None
    assert filings[1833804]["amendment_chain"] == [1833804]
    assert reports[1833804]["amendment_chain"] == ["1833804"]
    # Schema advertises numeric report-chain members, but the source returns strings.
    assert schema["definitions"]["CommitteeReportsPacParty"]["properties"]["amendment_chain"]["items"]["type"] == "number"
    assert isinstance(reports[1833804]["previous_file_number"], Decimal)
    assert reports[1833804]["previous_file_number"] == Decimal("1833804.0")


@REQUIRES_ORIGINALS
def test_original_new_amendment_zero_schedule_and_paper_witnesses():
    filings = complete_page("filing-witnesses.json", 7)
    reports = complete_page("sid-reports.json", 10)
    assert source("nrcc-reports.json") == original("C00075820-reports.json")
    assert set(filings) == {1766839, 1780310, 1780346, 1833804, 1876290, 1882886, 1813890}
    assert filings[1780310]["amendment_chain"] == [1766839, 1780310]
    assert filings[1766839]["amendment_chain"] == [1766839]  # Not the whole later chain.
    assert filings[1780346]["previous_file_number"] == 1780346  # Root self-reference, not a cycle.
    for file, suffix in ((1766839, "N"), (1780310, "A"), (1780346, "T")):
        rows = records(original(f"{file}.fec"))
        assert rows[0][:3] == [b"HDR", b"FEC", b"8.4"] and rows[1][0].decode() == "F3" + suffix
        assert filings[file]["means_filed"] == "e-file" and filings[file]["amendment_indicator"] == suffix
        cover = rows[1]
        for field, index in (("coverage_start_date", 15), ("coverage_end_date", 16)):
            day = date.fromisoformat(cover[index].decode()).isoformat()
            assert filings[file][field] == reports[file][field][:10] == day
        if file == 1780310:
            assert rows[0][5] == b"FEC-1766839"
        if file == 1780346:
            assert not any(r[0].startswith(b"SA") for r in rows)
            reported = reports[file]["individual_itemized_contributions_period"]
            assert reported == "0.00"  # String in this endpoint, despite numeric Swagger.
            schema = payload("swagger.json")["definitions"]["CommitteeReportsHouseSenate"]["properties"]
            assert schema["individual_itemized_contributions_period"]["type"] == "number"
            assert Decimal(cover[32].decode()) == Decimal(reported) == 0
    paper = source("paper-v34.xlsx")
    assert "NOT USED" in workbook_fields(paper, "HDR")[6]
    form_fields = workbook_fields(paper, "F3X")
    assert "COVERAGE FROM DATE" in form_fields[13] and "COVERAGE THROUGH DATE" in form_fields[14]
    assert "BEGINNING IMAGE NUMBER" in form_fields[123]
    assert "X = True" in workbook_fields(paper, "Sch A")[22]
    for file in (1876290, 1882886, 1813890):
        rows = records(original(f"{file}.fec"))
        assert rows[0][:2] == [b"HDR", b"P3.4"]
        assert filings[file]["means_filed"] == "paper"
        cover = rows[1]
        assert len(cover) == 125
        for field, index in (("coverage_start_date", 12), ("coverage_end_date", 13)):
            assert filings[file][field] == date.fromisoformat(cover[index].decode()).isoformat()
        assert cover[122].decode() == filings[file]["beginning_image_number"]
        if file == 1882886:
            assert cover[0] == b"F3XA" and not cover[20] and not cover[21] and not cover[23] and not cover[24]
            report = complete_page("nrcc-reports.json", 34)[file]
            amounts = [report[k] for k in ("cash_on_hand_beginning_period", "total_receipts_period", "total_disbursements_period", "cash_on_hand_end_period")]
            assert all(isinstance(v, Decimal) and v == 0 for v in amounts)
    # The original image's attachment-only nature remains a pinned prior manual review.
    assert original("nrcc-paper-amendment.pdf").startswith(b"%PDF-")
