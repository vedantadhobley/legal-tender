"""Independent result, source-membership and replay checks; no runtime policy."""

import copy
import hashlib
import json
import os
from collections import defaultdict
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator
from referencing import Registry, Resource

from tests.test_committee_summary_publication import decoded
from tests.test_summary_assertion_corpus import go_json

CONTRACTS = Path(__file__).resolve().parents[1] / "contracts"
SCHEMA = CONTRACTS / "calculations/fec/candidate-evidence/v1/result.schema.json"


def validator():
    resources = []
    for path in CONTRACTS.rglob("*.schema.json"):
        schema = json.loads(path.read_text())
        if "$id" in schema:
            resources.append((schema["$id"], Resource.from_contents(schema)))
    return Draft202012Validator(
        json.loads(SCHEMA.read_text()), registry=Registry().with_resources(resources)
    )


def test_candidate_evidence_contract_does_not_choose_terminal_or_allocation():
    schema = json.loads(SCHEMA.read_text())
    Draft202012Validator.check_schema(schema)
    for field in ("terminal_policy", "allocation_policy", "terminal_amount_minor_units"):
        check = Draft202012Validator(schema["properties"][field])
        check.validate(None)
        for value in ("0", 0, "leaf", "proportional"):
            assert list(check.iter_errors(value))
    assert "candidate_total" not in schema["properties"]
    assert schema["properties"]["terminal_attribution_eligible"] == {"const": False}


def merge(buckets):
    keys = (
        "rows", "known_amount_rows", "unknown_amount_rows", "positive_rows",
        "negative_rows", "zero_rows", "signed_minor_units", "positive_minor_units",
        "negative_minor_units", "nonempty_conduit_id_rows",
    )
    totals = {k: sum(int(b["measures"][k]) for b in buckets) for k in keys}
    assert totals["rows"] == totals["known_amount_rows"] + totals["unknown_amount_rows"]
    assert totals["known_amount_rows"] == sum(totals[k] for k in ("positive_rows", "negative_rows", "zero_rows"))
    assert totals["signed_minor_units"] == totals["positive_minor_units"] + totals["negative_minor_units"]
    return {k: str(v) if k.endswith("minor_units") else v for k, v in totals.items()}


def test_real_candidate_evidence_join_and_source_witnesses():
    output = os.environ.get("LT_CANDIDATE_EVIDENCE_OUTPUT")
    storage = os.environ.get("LT_CANDIDATE_EVIDENCE_STORAGE")
    if not output or not storage:
        pytest.skip("requires retained candidate runs and read-only source storage")
    root, audit = Path(storage), Path(output)
    inventory = json.loads((root / "dumps/audits/fec/committee-funding-basis/2026-09-08/2024/inventory.json").read_text())
    by_committee = defaultdict(list)
    for bucket in inventory["buckets"]:
        if bucket["key"]["recipient"]["present"]:
            by_committee[bucket["key"]["recipient"]["value"]].append(bucket)
    assertions = json.loads((root / "dumps/audits/fec/summary-assertions/2026-09-10/attempt-01/2024.json").read_text())
    summary_by_id = {c["committee_id"]: c for c in assertions["committees"]}
    check = validator()
    paths = sorted(audit.glob("[HSP]????????.json"))
    assert len(paths) >= 2
    for path in paths:
        r = json.loads(path.read_text())
        check.validate(r)
        identity = r["result_id"]
        r["result_id"] = ""
        assert hashlib.sha256(go_json(r)).hexdigest() == identity
        r["result_id"] = identity
        assert r["executable_sha256"] == hashlib.sha256((audit / "legal-tender").read_bytes()).hexdigest()
        assert r["inventory_calculation_id"] == inventory["calculation_id"]
        assert r["receipt_input"] == inventory["input"]
        candidate = r["candidate_id"]
        trace = r["committee_trace"]
        old = json.loads((root / f"dumps/audits/fec/candidate-upstream/2026-09-08/2024/accepted/{candidate}.json").read_text())
        assert trace == old
        assert r["cycle"] == trace["cycle"] == inventory["cycle"]
        nodes = {n["committee_id"]: n for n in trace["nodes"]}
        relationships = {c["committee_id"]: c["state"] for c in trace["committee_relationships"]}
        selected = set(nodes) | {k for k, v in relationships.items() if v in ("authorized", "unresolved")}
        assert [c["committee_id"] for c in r["committees"]] == sorted(selected)
        missing_rows = 0
        for committee in r["committees"]:
            cid = committee["committee_id"]
            buckets = by_committee[cid]
            pop = committee["reported_receipt_population"]
            assert committee["reached_by_selected_committee_trace"] == (cid in nodes)
            assert committee["candidate_authorization"] == relationships.get(cid, "not_candidate_linked")
            assert pop["total"] == merge(buckets)
            assert pop["individual_predicate"] == merge([b for b in buckets if b["key"]["individual_decision"] == "included"])
            assert pop["individual_committee_overlap"] == merge([b for b in buckets if b["key"]["component"] == "overlapping_individual_and_committee"])
            groups = defaultdict(list)
            for b in buckets:
                groups[(b["key"]["component"], b["key"]["receipt_role"])].append(b)
            assert len(pop["components"]) == len(groups)
            assert [(p["component"], p["receipt_role"]) for p in pop["components"]] == sorted(groups)
            for p in pop["components"]:
                assert p["measures"] == merge(groups[(p["component"], p["receipt_role"])])
            if cid in nodes:
                incoming = sum(b["measures"]["rows"] for b in buckets if b["key"]["committee_decision"] == "included_receiver_reported_committee_flow")
                assert nodes[cid]["selected_incoming_observations"] == incoming
                missing_rows += not buckets
            if not buckets:
                assert "no_receipt_rows_is_not_reported_zero" in committee["coverage_limits"]
            review = committee["candidate_linked_summary_review"]
            if relationships.get(cid) not in ("authorized", "unresolved"):
                assert review is None
                continue
            assert review["receipts"] == pop
            assert review["summary_input"] == assertions["input"]
            assert review["summary_calculation_id"] == assertions["calculation_id"]
            assert "source_release_mismatch" in review["comparison_blockers"]
            expected = summary_by_id.get(cid, {"assertions": []})
            assert [a["assertion_id"] for a in review["assertions"]] == [a["assertion_id"] for a in expected["assertions"]]
            for a, source in zip(review["assertions"], expected["assertions"], strict=True):
                assert a["members"] == source["members"]
                assert a["coverage_start"] == source["coverage_start"]
                assert a["coverage_end"] == source["coverage_end"]
                operands = {v["field"]: v for e in source["diagnostic_equations"].values() for v in e["operands"]}
                for field in a["fields"]:
                    assert field["raw"] == operands[field["field"]]["raw"]
                    assert field["value"] == operands[field["field"]]["value"]
                    assert field["delta_minor_units"] is None
        overview = r["overview"]
        assert overview["reached_without_receipt_rows"] == missing_rows
        assert overview["reached_committees"] == len(nodes)
        assert overview["reached_without_same_cycle_master"] == sum(n["master_fact_id"] is None for n in nodes.values())
        assert overview["cyclic_components"] == len(trace["cyclic_components"])
        assert overview["authorized_committees"] == sum(v == "authorized" for v in relationships.values())
        assert overview["unresolved_linked_committees"] == sum(v == "unresolved" for v in relationships.values())
        witnesses = {w["source_row_ordinal"]: w for w in r["connection_witnesses"]}
        assert len(witnesses) == len(r["connection_witnesses"])
        assert list(witnesses) == sorted(witnesses)
        assert set(witnesses) == {n["witness_source_row_ordinal"] for n in nodes.values() if n["witness_source_row_ordinal"] is not None}
        for n in nodes.values():
            if n["candidate_authorized"]:
                continue
            w = witnesses[n["witness_source_row_ordinal"]]
            assert w["sender_committee_id"] == n["committee_id"]
            target = nodes[w["reported_recipient_committee_id"]]
            assert target["minimum_hops_to_authorized_scope"] == n["minimum_hops_to_authorized_scope"] - 1
        base = root / "calculations/fec/committee-flow-reconciliation/v1"
        publication = json.loads((base / "manifests" / (trace["inputs"]["reconciliation"]["calculation_set_id"] + ".json")).read_text())
        artifact = publication["schedule_a"]["observations"]
        assert artifact["compression"] == "zstd"
        seen, count, uncompressed = set(), 0, hashlib.sha256()
        with decoded(artifact, base) as source:
            for line in source:
                uncompressed.update(line)
                count += 1
                observation = json.loads(line)
                ordinal = observation["source_row_ordinal"]
                if ordinal in witnesses:
                    assert observation == witnesses[ordinal]
                    seen.add(ordinal)
        assert count == artifact["record_count"]
        assert uncompressed.hexdigest() == artifact["uncompressed_sha256"]
        assert seen == set(witnesses)
        report = (audit / f"{candidate}.md").read_text()
        assert identity in report and "unknown, not zero" in report
        assert "different_source_release" in report
        # Only check replay artifacts when the explicit replay driver produced them.
        replay = audit / "replay" / path.name
        if (audit / "replay.exit").exists():
            assert (audit / "replay.exit").read_text().strip() == "0"
            assert replay.read_bytes() == path.read_bytes()
            assert (audit / "replay" / f"{candidate}.md").read_bytes() == (audit / f"{candidate}.md").read_bytes()
        for field, value in (("terminal_attribution_eligible", True), ("terminal_amount_minor_units", "0"), ("allocation_policy", "proportional")):
            # Validate the changed top-level field alone; do not repeat the whole
            # large graph traversal to prove three small wire guards.
            bad = copy.deepcopy(r[field])
            assert bad != value
            guard = Draft202012Validator(check.schema["properties"][field])
            assert list(guard.iter_errors(value))
