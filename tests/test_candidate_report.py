"""Independent source-name, witness-path and unchanged-evidence checks."""

import hashlib
import json
import os
from collections import defaultdict
from itertools import pairwise
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator
from referencing import Registry, Resource

from tests.test_committee_summary_publication import decoded
from tests.test_summary_assertion_corpus import go_json

CONTRACTS = Path(__file__).resolve().parents[1] / "contracts"
SCHEMA = CONTRACTS / "calculations/fec/candidate-evidence/v2/result.schema.json"


def validator():
    resources = []
    for path in CONTRACTS.rglob("*.schema.json"):
        schema = json.loads(path.read_text())
        if "$id" in schema:
            resources.append((schema["$id"], Resource.from_contents(schema)))
    return Draft202012Validator(
        json.loads(SCHEMA.read_text()), registry=Registry().with_resources(resources)
    )


def test_report_contract_is_additive_and_cannot_allocate_path_money():
    schema = json.loads(SCHEMA.read_text())
    Draft202012Validator.check_schema(schema)
    assert schema["properties"]["evidence"]["$ref"].endswith("candidate-evidence/v1/result.schema.json")
    assert "candidate_total" not in schema["properties"]
    guard = Draft202012Validator(schema["$defs"]["path"]["properties"]["allocated_amount_minor_units"])
    guard.validate(None)
    for value in ("0", "100", 0, -1):
        assert list(guard.iter_errors(value))


def source_names(root, source):
    dataset = source["dataset"]
    path = root / f"facts/fec/classic/{dataset}/manifests/{source['fact_set_id']}.json"
    raw = path.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == source["manifest_sha256"]
    manifest = json.loads(raw)
    for key in ("dataset", "cycle", "fact_set_id", "source_release_id", "source_release_manifest_sha256"):
        assert source[key] == manifest[key]
    assert source["facts"] == manifest["counts"]["facts"]
    id_field, name_field = ("CAND_ID", "CAND_NAME") if dataset == "candidate-master" else ("CMTE_ID", "CMTE_NM")
    names, seen = defaultdict(list), set()
    with decoded(manifest["facts"], root) as stream:
        for line in stream:
            fact = json.loads(line)
            assert fact["fact_id"] not in seen
            seen.add(fact["fact_id"])
            assert fact["state"] == "valid"
            for key in ("dataset", "cycle", "source_release_id", "source_contract", "occurrence_set_id"):
                assert fact[key] == manifest[key]
            fields = fact["source_fields"]
            names[fields[id_field]].append({
                "fact_id": fact["fact_id"], "occurrence_id": fact["occurrence_id"],
                "raw_name": fields[name_field],
            })
    assert len(seen) == source["facts"]
    return {key: sorted(value, key=lambda a: a["fact_id"]) for key, value in names.items()}


def test_real_report_preserves_core_and_uses_exact_names_and_paths():
    output, storage = os.environ.get("LT_CANDIDATE_REPORT_OUTPUT"), os.environ.get("LT_CANDIDATE_REPORT_STORAGE")
    if not output or not storage:
        pytest.skip("requires retained report outputs and read-only source storage")
    audit, root = Path(output), Path(storage)
    check, sources = validator(), {}
    paths = sorted(audit.glob("[HSP]????????.json"))
    assert len(paths) >= 2
    build = hashlib.sha256((audit / "legal-tender").read_bytes()).hexdigest()
    for path in paths:
        report = json.loads(path.read_text())
        check.validate(report)
        identity = report["report_id"]
        report["report_id"] = ""
        assert hashlib.sha256(go_json(report)).hexdigest() == identity
        report["report_id"] = identity
        core = report["evidence"]
        assert core["executable_sha256"] == build
        core_identity = core["result_id"]
        core["result_id"] = ""
        assert hashlib.sha256(go_json(core)).hexdigest() == core_identity
        core["result_id"] = core_identity
        prior = json.loads((root / "dumps/audits/fec/candidate-evidence/2026-09-11/attempt-01" / path.name).read_text())
        # Only executable identity (and the result hash that binds it) can change.
        assert {k: v for k, v in core.items() if k not in ("result_id", "executable_sha256")} == {
            k: v for k, v in prior.items() if k not in ("result_id", "executable_sha256")
        }
        for kind in ("candidate", "committee"):
            source = report[f"{kind}_name_source"]
            assert source["dataset"] == f"{kind}-master"
            assert source["cycle"] == core["cycle"]
            key = source["fact_set_id"]
            if key not in sources:
                sources[key] = source_names(root, source)
        master = core["committee_trace"]["inputs"]["committee_master"]
        for key in ("fact_set_id", "manifest_sha256", "source_release_id", "facts"):
            assert report["committee_name_source"][key] == master[key]
        assert [n["entity_id"] for n in report["names"]] == [core["candidate_id"]] + [c["committee_id"] for c in core["committees"]]
        for name in report["names"]:
            key = report[f"{name['entity_kind']}_name_source"]["fact_set_id"]
            expected = sources[key].get(name["entity_id"], [])
            assert name["assertions"] == expected
            raw_names = {a["raw_name"] for a in expected}
            state = "no_reference_record" if not expected else "conflicting_reported_names" if len(raw_names) > 1 else "source_name_blank" if raw_names == {""} else "reported_name"
            assert name["state"] == state
        trace = core["committee_trace"]
        nodes = {n["committee_id"]: n for n in trace["nodes"]}
        witnesses = {w["source_row_ordinal"]: w for w in core["connection_witnesses"]}
        distances = defaultdict(list)
        for node in nodes.values():
            if not node["candidate_authorized"]:
                distances[node["minimum_hops_to_authorized_scope"]].append(node["committee_id"])
        starts = [min(distances[h]) for h in sorted(distances)[:3]]
        assert [p["from_committee_id"] for p in report["path_examples"]] == starts
        for example in report["path_examples"]:
            current, expected = example["from_committee_id"], []
            while not nodes[current]["candidate_authorized"]:
                witness = witnesses[nodes[current]["witness_source_row_ordinal"]]
                expected.append(witness)
                current = witness["reported_recipient_committee_id"]
            assert example["hops"] == expected
            assert example["authorized_committee_id"] == current
            assert len(expected) == nodes[example["from_committee_id"]]["minimum_hops_to_authorized_scope"]
            dates = [h["date_days"] for h in expected if h["date_days"] is not None]
            assert example["has_missing_reported_dates"] == (len(dates) != len(expected))
            assert example["has_decreasing_reported_dates"] == any(b < a for a, b in pairwise(dates))
            assert example["has_same_day_hops"] == any(b == a for a, b in pairwise(dates))
            assert example["has_nonpositive_amount"] == any(int(h["signed_amount_minor_units"]) <= 0 for h in expected)
            assert example["allocated_amount_minor_units"] is None
        markdown = path.with_suffix(".md").read_text()
        assert identity in markdown
        assert "Untranslated source state" not in markdown
        assert "the same dollars" in markdown and "unknown, not zero" in markdown
        assert "Memo evidence — separate, not additional funding" in markdown
        if (audit / "replay.exit").exists():
            assert (audit / "replay.exit").read_text().strip() == "0"
            assert path.read_bytes() == (audit / "replay" / path.name).read_bytes()
            assert path.with_suffix(".md").read_bytes() == (audit / "replay" / path.with_suffix(".md").name).read_bytes()
