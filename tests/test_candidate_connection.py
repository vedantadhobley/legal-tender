"""Independent connection identity, published membership and field checks."""

import datetime as dt
import hashlib
import json
import os
from decimal import Decimal
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from tests.test_candidate_report import validator as report_validator
from tests.test_committee_summary_publication import decoded
from tests.test_summary_assertion_corpus import go_json

SCHEMA = Path(__file__).resolve().parents[1] / "contracts/calculations/fec/candidate-connection/v1/result.schema.json"


def test_connection_contract_keeps_verification_scoped_and_allocation_null():
    schema = json.loads(SCHEMA.read_text())
    Draft202012Validator.check_schema(schema)
    assert schema["properties"]["parent_verification"]["const"] == "content_identity_checked_not_full_recalculation"
    assert schema["properties"]["terminal_attribution_eligible"] == {"const": False}
    guard = Draft202012Validator(schema["properties"]["allocated_amount_minor_units"])
    guard.validate(None)
    for value in (0, "0", "100", "proportional"):
        assert list(guard.iter_errors(value))


def test_real_connection_matches_parent_publication_and_retained_fields():
    output, storage = os.environ.get("LT_CONNECTION_OUTPUT"), os.environ.get("LT_CONNECTION_STORAGE")
    if not output or not storage:
        pytest.skip("requires retained connection outputs and read-only source storage")
    audit, root = Path(output), Path(storage)
    check = report_validator().evolve(schema=json.loads(SCHEMA.read_text()))
    paths = sorted(audit.glob("[HSP]????????-*.json"))
    assert len(paths) >= 2
    build = hashlib.sha256((audit / "legal-tender").read_bytes()).hexdigest()
    wanted = {}
    for path in paths:
        result = json.loads(path.read_text())
        check.validate(result)
        identity = result["connection_id"]
        result["connection_id"] = ""
        assert hashlib.sha256(go_json(result)).hexdigest() == identity
        result["connection_id"] = identity
        assert result["executable_sha256"] == build
        parent_path = root / "dumps/audits/fec/candidate-report/2026-09-11/attempt-01" / (result["candidate_id"] + ".json")
        raw = parent_path.read_bytes()
        assert hashlib.sha256(raw).hexdigest() == result["parent_document_sha256"]
        parent = json.loads(raw)
        assert result["parent_report_id"] == parent["report_id"]
        parent["report_id"] = ""
        assert hashlib.sha256(go_json(parent)).hexdigest() == result["parent_report_id"]
        core = parent["evidence"]
        assert core["result_id"] == result["parent_evidence_id"]
        core["result_id"] = ""
        assert hashlib.sha256(go_json(core)).hexdigest() == result["parent_evidence_id"]
        assert result["cycle"] == core["cycle"]
        assert result["inputs"] == core["committee_trace"]["inputs"]["sources"]
        assert result["calculation"] == core["committee_trace"]["inputs"]["reconciliation"]
        observation = result["observation"]
        ordinal = observation["source_row_ordinal"]
        groups = {
            "connection_witness": core["connection_witnesses"],
            "candidate_boundary_observation": [c["observation"] for c in core["committee_trace"]["candidate_observations"]],
        }
        membership = []
        for kind, rows in groups.items():
            selected = [o for o in rows if o["source_row_ordinal"] == ordinal]
            if selected:
                assert selected == [observation]
                membership.append(kind)
        assert result["report_membership"] == membership
        endpoints = {observation["sender_committee_id"], observation["reported_recipient_committee_id"]}
        assert result["parent_name_assertions"] == [n for n in parent["names"] if n["entity_kind"] == "committee" and n["entity_id"] in endpoints]
        source = result["source"]
        assert source["side"] == "schedule_a"
        assert source["fact_set_id"] == core["receipt_input"]["fact_set_id"]
        assert source["source_row_ordinal"] == ordinal
        fields = source["fields"]
        assert len(fields) == 99
        assert fields["lt_source_row_ordinal"] == str(ordinal)
        assert fields["sub_id"] == observation["sub_id"]
        assert fields["cmte_id"] == observation["reported_recipient_committee_id"]
        assert fields["contbr_id"] == fields["clean_contbr_id"] == observation["sender_committee_id"]
        assert fields["receipt_tp"] == observation["transaction_type"]
        assert fields["lt_receipt_amount_minor_units"] == observation["signed_amount_minor_units"]
        assert Decimal(fields["contb_receipt_amt"]) * 100 == Decimal(observation["signed_amount_minor_units"])
        assert fields["lt_receipt_date"] == observation["date_days"]
        if fields["contb_receipt_dt"] is not None:
            day = dt.datetime.fromisoformat(fields["contb_receipt_dt"]).date()
            assert (day - dt.date(1970, 1, 1)).days == observation["date_days"]
        manifest_path = root / "facts/fec/schedule-a/columnar/manifests" / (source["fact_set_id"] + ".json")
        raw = manifest_path.read_bytes()
        assert hashlib.sha256(raw).hexdigest() == result["inputs"]["schedule_a"]["manifest_sha256"]
        manifest = json.loads(raw)
        shards = [s for s in manifest["shards"] if s["first_source_row_ordinal"] <= ordinal <= s["last_source_row_ordinal"]]
        assert len(shards) == 1 and shards[0]["sha256"] == source["shard_sha256"]
        key = (result["calculation"]["calculation_set_id"], result["calculation"]["manifest_sha256"])
        wanted.setdefault(key, {})[ordinal] = observation
        markdown = path.with_suffix(".md").read_text()
        for field in fields:
            assert field.replace("_", "\\_") in markdown
        assert identity in markdown and "not recalculate the whole parent" in markdown
        assert "unknown, not zero" in markdown
        if (audit / "replay.exit").exists():
            assert (audit / "replay.exit").read_text().strip() == "0"
            for suffix in (".json", ".md"):
                assert path.with_suffix(suffix).read_bytes() == (audit / "replay" / path.with_suffix(suffix).name).read_bytes()
    base = root / "calculations/fec/committee-flow-reconciliation/v1"
    for (calculation, digest), observations in wanted.items():
        raw = (base / "manifests" / (calculation + ".json")).read_bytes()
        assert hashlib.sha256(raw).hexdigest() == digest
        manifest = json.loads(raw)
        seen = set()
        with decoded(manifest["schedule_a"]["observations"], base) as stream:
            for line in stream:
                observation = json.loads(line)
                ordinal = observation["source_row_ordinal"]
                if ordinal in observations:
                    assert ordinal not in seen
                    assert observation == observations[ordinal]
                    seen.add(ordinal)
        assert seen == set(observations)
