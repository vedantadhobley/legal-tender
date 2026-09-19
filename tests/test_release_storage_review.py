"""Independent schema/arithmetic checks; no production Python storage logic."""

import copy
import hashlib
import json
import os
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[1]
SCHEMAS = {
    version: ROOT / f"contracts/audits/fec/storage-review/{version}/result.schema.json"
    for version in ("v1", "v2")
}


@pytest.mark.parametrize("version", SCHEMAS)
def test_storage_review_schema_rejects_false_certainty(version):
    schema = json.loads(SCHEMAS[version].read_text())
    Draft202012Validator.check_schema(schema)
    output_schema = {"$defs": schema["$defs"], **schema["$defs"]["output"]}
    validator = Draft202012Validator(output_schema)
    unknown = {"source_id": "fec:test", "selection": "test", "period": "2024", "basis": "unknown"}
    validator.validate(unknown)
    assert not validator.is_valid({**unknown, "new_compressed_bytes": 0})
    reused = {**unknown, "basis": "unchanged_source", "new_compressed_bytes": 0}
    validator.validate(reused)
    assert not validator.is_valid({**reused, "new_compressed_bytes": 1})
    assert not validator.is_valid({**unknown, "basis": "prior_size_new_content"})


def test_real_storage_review_against_pinned_inputs():
    directory = os.environ.get("LEGAL_TENDER_STORAGE_REVIEW_AUDIT")
    if not directory:
        pytest.skip("set LEGAL_TENDER_STORAGE_REVIEW_AUDIT for read-only saved evidence checks")
    root = Path(directory)
    report = json.loads((root / "review.json").read_text())
    plan = json.loads((root / "plan.json").read_text())
    prior = json.loads((root / "baseline-release.json").read_text())
    version = report["schema_version"].rsplit(".", 1)[1]
    schema = json.loads(SCHEMAS[version].read_text())
    Draft202012Validator(schema).validate(report)
    assert report["candidate_release_id"] == plan["candidate_release_id"]
    assert report["prior_release_id"] == prior["release_id"] == plan["prior_release_id"]
    acquisition = report["acquisition"]
    inodes = {}
    for line in (root / "raw-file-inventory.txt").read_text().splitlines():
        size, device, inode, links, path = line.split(" ", 4)
        if path.startswith("/storage/raw/fec/schedule-a/"):
            identity = (device, inode)
            if identity in inodes:
                assert inodes[identity] == int(size)
            inodes[identity] = int(size)
    assert acquisition["schedule_a_hot_bytes_before"] == sum(inodes.values())
    before = (root / "current-before.sha256").read_text()
    assert before == (root / "current-after.sha256").read_text()
    assert before.split()[0] == hashlib.sha256((root / "baseline-release.json").read_bytes()).hexdigest()
    assert acquisition["candidate_download_bytes"] == sum(
        source["content_length"] for source in plan["selected_sources"]
        if source["source_id"] in plan["changed_source_ids"]
    )
    scenario = report["staging_scenario"]
    prior_outputs = {(o["source_id"], o["selection"]): o for o in prior["staged_outputs"]}
    for output in scenario["outputs"]:
        before = prior_outputs[(output["source_id"], output["selection"])]
        if output["source_id"] in plan["reused_source_ids"]:
            assert output["basis"] == "unchanged_source"
            assert output["new_compressed_bytes"] == 0
        else:
            assert output["basis"] == "prior_size_new_content"
            assert output["new_compressed_bytes"] == before["compressed_byte_count"]
    assert len(scenario["outputs"]) == len(prior_outputs)
    total = sum(o["new_compressed_bytes"] for o in scenario["outputs"])
    new_a = sum(o["new_compressed_bytes"] for o in scenario["outputs"] if o["source_id"] == "fec:schedule-a:processed")
    working = acquisition["largest_extract_working_bytes"] + acquisition["working_margin_bytes"]
    assert scenario["new_output_bytes"] == total
    assert scenario["new_schedule_a_output_bytes"] == new_a
    assert "fec:schedule-a:processed" in plan["changed_source_ids"]
    hot_peak = new_a
    if version == "v2":
        assert acquisition["largest_extract_working_bytes"] == 0
        hot_peak = retained_a = 0
        for output in scenario["outputs"]:
            size = output["new_compressed_bytes"]
            if output["source_id"] == "fec:schedule-a:processed":
                retained_a += size
                hot_peak = max(hot_peak, retained_a)
            else:
                hot_peak = max(hot_peak, retained_a + size)
    assert scenario["hot_bytes_with_reserve"] == acquisition["projected_schedule_a_hot_bytes"] + hot_peak
    assert scenario["required_free_bytes"] == acquisition["remaining_download_bytes"] + total + working + acquisition["free_floor_bytes"]
    assert scenario["hot_cap_excess_bytes"] == max(0, scenario["hot_bytes_with_reserve"] - acquisition["schedule_a_hot_cap_bytes"])
    assert scenario["free_space_shortfall_bytes"] == max(0, scenario["required_free_bytes"] - acquisition["free_bytes_before"])
    assert scenario["complete"] is True
    assert scenario["fits_budget"] == (scenario["hot_cap_excess_bytes"] == scenario["free_space_shortfall_bytes"] == 0)
    # The report cannot claim a fitting envelope when a component is unknown.
    invalid = copy.deepcopy(report)
    invalid["staging_scenario"].update(complete=False, fits_budget=True)
    assert not Draft202012Validator(schema).is_valid(invalid)
