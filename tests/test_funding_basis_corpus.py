"""Opt-in independent checks of retained Go outputs; never a pipeline step."""

import base64
from collections import defaultdict
import hashlib
import json
import os
from pathlib import Path

import pytest

from tests.test_funding_basis_contracts import validators


AUDIT = os.environ.get("LT_FUNDING_BASIS_AUDIT")
pytestmark = pytest.mark.skipif(not AUDIT, reason="requires retained full-cycle funding-basis audit")


def load(name):
    return json.loads((Path(AUDIT) / name).read_text())


def totals(values):
    result = defaultdict(int)
    for value in values:
        for key, amount in value.items():
            result[key] += int(amount)
    return dict(result)


def test_complete_inventory_and_accepted_cohort_equivalence():
    r = load("inventory.json")
    validators()["result.schema"].validate(r)
    identifier = r["calculation_id"]
    r["calculation_id"] = ""
    encoded = json.dumps(r, separators=(",", ":"), ensure_ascii=False)
    for char, escaped in (("&", "\\u0026"), ("<", "\\u003c"), (">", "\\u003e"), ("\u2028", "\\u2028"), ("\u2029", "\\u2029")):
        encoded = encoded.replace(char, escaped)
    assert hashlib.sha256(encoded.encode()).hexdigest() == identifier
    assert totals(b["measures"] for b in r["buckets"]) == {k: int(v) for k, v in r["total"].items()}
    assert r["total"]["rows"] == r["input"]["facts"] == 264085606
    for bucket in r["buckets"]:
        m = bucket["measures"]
        assert m["rows"] == m["known_amount_rows"] + m["unknown_amount_rows"]
        assert m["known_amount_rows"] == m["positive_rows"] + m["negative_rows"] + m["zero_rows"]
        assert int(m["signed_minor_units"]) == int(m["positive_minor_units"]) + int(m["negative_minor_units"])
        bits = base64.b64decode(bucket["shard_presence_bitmap"], validate=True)
        assert len(bits) == (r["input"]["shards"] + 7) // 8 and any(bits)
        for index in range(r["input"]["shards"], len(bits) * 8):
            assert not bits[index // 8] & (1 << (index % 8))
    # Pinned acceptance evidence, not runtime classification constants. These
    # are the independently accepted direct-probe and receiver-flow populations.
    decisions = defaultdict(int)
    for b in r["buckets"]:
        decisions[b["key"]["individual_decision"]] += b["measures"]["rows"]
    assert dict(decisions) == {
        "included": 222205451, "excluded_non_individual": 23960090,
        "excluded_memo_subtotal": 17920063, "unresolved_amount": 2,
    }
    individual = totals(b["measures"] for b in r["buckets"] if b["key"]["individual_decision"] == "included")
    committee = totals(b["measures"] for b in r["buckets"] if b["key"]["committee_decision"] == "included_receiver_reported_committee_flow")
    assert individual["signed_minor_units"] == 1588756934113
    assert committee["rows"] == 320731
    assert committee["signed_minor_units"] == 467282017949


def test_candidate_assessments_and_receipt_pages():
    r = load("inventory.json")
    checks = validators()
    by_committee = defaultdict(lambda: defaultdict(list))
    for b in r["buckets"]:
        by_committee[b["key"]["recipient"]["value"]][b["key"]["component"]].append(b["measures"])
    for candidate in ("S6OH00163", "S6PA00217"):
        a = load(candidate + ".json")
        checks["assessment.schema"].validate(a)
        assert a["inventory_calculation_id"] == r["calculation_id"]
        ids = [c["committee_id"] for c in a["committees"]]
        assert ids == sorted(set(ids))
        for c in a["committees"]:
            actual = {m["component"]: {k: int(v) for k, v in m["measures"].items()} for m in c["components"]}
            expected = {key: totals(values) for key, values in by_committee[c["committee_id"]].items()}
            assert actual == expected
    first, second = load("page-1.json"), load("page-2.json")
    assert second["after_source_row_ordinal"] == first["next_after_source_row_ordinal"]
    for page in (first, second, load("unknown-page.json"), load("overlap-page.json")):
        checks["page.schema"].validate(page)
        assert page["calculation_id"] == r["calculation_id"]
        previous = page["after_source_row_ordinal"]
        for row in page["receipts"]:
            assert row["source_row_ordinal"] > previous
            previous = row["source_row_ordinal"]
            assert int(row["fields"]["lt_source_row_ordinal"]) == previous
            assert row["fields"]["cmte_id"] == page["committee_id"]
            assert row["key"]["component"] == page["component"]
            assert "contbr_nm" in row["fields"] and "conduit_cmte_nm" in row["fields"] and "contbr_employer" in row["fields"]
    assert first == load("page-1-replay.json")
    unknown = load("unknown-page.json")
    assert len(unknown["receipts"]) == 2 and not unknown["has_more"]
    assert all(row["fields"]["lt_receipt_amount_minor_units"] is None for row in unknown["receipts"])
    overlaps = load("overlap-page.json")
    assert len(overlaps["receipts"]) == 3
    assert all(row["fields"]["is_individual"] is True and row["fields"]["entity_tp"] == "PAC" for row in overlaps["receipts"])
