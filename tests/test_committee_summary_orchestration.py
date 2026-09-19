"""Manual summary orchestration; Go retains all source and money semantics."""

import copy
import hashlib
import json
import os
from pathlib import Path

import pytest
from dagster import (
    AssetKey,
    AssetSelection,
    AutomationConditionSensorDefinition,
    DagsterInstance,
    Definitions,
    FilesystemIOManager,
    RetryPolicy,
    SourceAsset,
    asset,
    evaluate_automation_conditions,
    materialize,
)

from orchestration.assets import fec_cycle_partitions
from orchestration.committee_summary import fec_committee_summary_facts
from orchestration.definitions import defs, fec_committee_summary_fact_job
from orchestration.go_process import GoCommandError, _decode_and_validate
from orchestration.resources import GoPipelineResource

ROOT = Path(__file__).resolve().parents[1]
CONTRACTS = ROOT / "contracts"
SCHEMA = CONTRACTS / "facts/fec/committee-summary/v1/manifest.schema.json"
FIXTURE = Path(__file__).parent / "fixtures/committee-summary-manifest.json"


def validate(payload):
    return _decode_and_validate(json.dumps(payload).encode(), SCHEMA, None, CONTRACTS)


def test_summary_envelope_is_local_and_fail_closed(monkeypatch):
    import urllib.request

    monkeypatch.setattr(
        urllib.request, "urlopen", lambda *a, **k: pytest.fail("schema network access")
    )
    value = json.loads(FIXTURE.read_text())
    assert validate(value) == value
    for field in value:
        bad = copy.deepcopy(value)
        del bad[field]
        with pytest.raises(GoCommandError):
            validate(bad)
    for field, replacement in (
        ("readback_verified", False),
        ("terminal_attribution_eligible", True),
        ("state", "pending"),
        ("fact_set_id", "../../escape"),
        ("unexpected", "must not reach metadata"),
    ):
        bad = copy.deepcopy(value)
        bad[field] = replacement
        with pytest.raises(GoCommandError):
            validate(bad)
    bad = copy.deepcopy(value)
    bad["verification"]["complete"] = False
    with pytest.raises(GoCommandError):
        validate(bad)


def test_summary_registration_is_partitioned_and_manual_only():
    Definitions.validate_loadable(defs)
    definition = fec_committee_summary_facts
    graph = defs.resolve_asset_graph()
    assert definition.partitions_def == fec_cycle_partitions
    assert definition.asset_deps[definition.key] == {
        AssetKey("fec_release_publication")
    }
    check = next(iter(definition.check_specs))
    assert check.blocking and check.partitions_def == fec_cycle_partitions
    assert definition.get_asset_spec().automation_condition is None
    assert fec_committee_summary_fact_job.selection.resolve(graph) == {definition.key}
    for sensor in defs.sensors:
        if isinstance(sensor, AutomationConditionSensorDefinition):
            assert definition.key not in sensor.asset_selection.resolve(graph)
        assert all(
            target.job_name != fec_committee_summary_fact_job.name
            for target in sensor.targets
        )
    for schedule in defs.schedules:
        assert schedule.job_name != fec_committee_summary_fact_job.name

    @asset(name="fec_release_publication")
    def release():
        return "/pinned/release.json"

    with DagsterInstance.ephemeral() as instance:
        instance.add_dynamic_partitions("fec_cycle", ["2024", "2026"])
        materialize([release], instance=instance)
        result = evaluate_automation_conditions(
            defs=Definitions(assets=[release, definition], resources=defs.resources),
            instance=instance,
            asset_selection=AssetSelection.assets(definition),
        )
        assert result.get_requested_partitions(definition.key) == set()


def fake_resource(tmp_path, monkeypatch, cycle="2024"):
    payload = json.loads(FIXTURE.read_text())
    payload["cycle"] = cycle
    payload["verification"]["expected"]["cycle"] = cycle
    payload["source_artifact"]["source_id"] = f"fec:committee-summary:{cycle}"
    (tmp_path / "result.json").write_text(json.dumps(payload))
    binary = tmp_path / "fake-go"
    binary.write_text("""#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
root = Path(os.environ["SUMMARY_TEST_ROOT"])
with (root / "arguments.jsonl").open("a") as stream:
    stream.write(json.dumps(sys.argv[1:]) + "\\n")
if (root / "fail").exists():
    sys.stderr.write("source ancestry rejected")
    sys.exit(1)
sys.stdout.write((root / "result.json").read_text())
""")
    binary.chmod(0o750)
    monkeypatch.setenv("SUMMARY_TEST_ROOT", str(tmp_path))
    return GoPipelineResource(
        binary_path=str(binary),
        artifact_root=str(tmp_path / "control"),
        contracts_root=str(CONTRACTS),
        current_fec_release_manifest="must-not-read-current",
        storage_root=str(tmp_path / "storage"),
        occurrence_timeout_seconds=30,
    )


def pinned_release(path):
    @asset(name="fec_release_publication")
    def release():
        return str(path)

    return release


@pytest.mark.parametrize("cycle", ["2024", "2026"])
def test_exact_handoff_check_metadata_and_replay(tmp_path, monkeypatch, cycle):
    resource = fake_resource(tmp_path, monkeypatch, cycle)
    release = pinned_release("/immutable/exact-release.json")
    with DagsterInstance.ephemeral() as instance:
        instance.add_dynamic_partitions("fec_cycle", [cycle])
        materialize([release], instance=instance)
        executions = [
            materialize(
                [SourceAsset(release.key), fec_committee_summary_facts],
                resources={"go_pipeline": resource},
                instance=instance,
                partition_key=cycle,
            )
            for _ in range(2)
        ]
    payload = json.loads((tmp_path / "result.json").read_text())
    for result in executions:
        assert result.success
        check = result.get_asset_check_evaluations()[0]
        assert check.passed and check.partition == cycle
        assert check.target_materialization_data.run_id == result.run_id
        assert result.output_for_node("fec_committee_summary_facts") == str(
            Path(resource.storage_root)
            / "facts/fec/committee-summary/v1/manifests"
            / f"{payload['fact_set_id']}.json"
        )
        m = result.asset_materializations_for_node("fec_committee_summary_facts")[0]
        assert m.tags["dagster/data_version"] == payload["fact_set_id"]
        assert (
            m.metadata["source_release_manifest_sha256"].value
            == payload["source_release_manifest_sha256"]
        )
        assert m.metadata["terminal_attribution_eligible"].value is False
        assert (
            m.metadata["issue_counts"].data == payload["verification"]["issue_counts"]
        )
        assert "verification" not in m.metadata and "money" not in m.metadata
    arguments = [
        json.loads(line)
        for line in (tmp_path / "arguments.jsonl").read_text().splitlines()
    ]
    for args, result in zip(arguments, executions, strict=True):
        assert args == [
            "pipeline",
            "fec",
            "publish-committee-summary",
            "--storage-root",
            resource.storage_root,
            "--release",
            "/immutable/exact-release.json",
            "--cycle",
            cycle,
            "--run-id",
            result.run_id,
        ]


def test_retry_after_lost_acknowledgement_keeps_exact_inputs(tmp_path, monkeypatch):
    import orchestration.resources as resources_module

    monkeypatch.setattr(RetryPolicy, "calculate_delay", lambda *a, **k: 0)
    resource = fake_resource(tmp_path, monkeypatch)
    execute = resources_module.run_json_command
    returned = []

    def lose_first_acknowledgement(**kwargs):
        result = execute(**kwargs)
        returned.append(result)
        if len(returned) == 1:
            raise GoCommandError("simulated lost completion acknowledgement")
        return result

    monkeypatch.setattr(
        resources_module, "run_json_command", lose_first_acknowledgement
    )
    with DagsterInstance.ephemeral() as instance:
        instance.add_dynamic_partitions("fec_cycle", ["2024"])
        result = materialize(
            [pinned_release("/immutable/release.json"), fec_committee_summary_facts],
            instance=instance,
            resources={"go_pipeline": resource},
            partition_key="2024",
        )
    assert result.success and len(returned) == 2
    assert returned[0].artifact_path == returned[1].artifact_path
    assert returned[0].sha256 == returned[1].sha256
    arguments = (tmp_path / "arguments.jsonl").read_text().splitlines()
    assert len(arguments) == 2 and arguments[0] == arguments[1]
    assert (
        len(result.asset_materializations_for_node("fec_committee_summary_facts")) == 1
    )
    checks = result.get_asset_check_evaluations()
    assert [check.passed for check in checks] == [False, True]
    assert checks[-1].target_materialization_data.run_id == result.run_id


@pytest.mark.parametrize("failure", ["exit", "schema", "cycle"])
def test_failure_blocks_output_and_consumer(tmp_path, monkeypatch, failure):
    monkeypatch.setattr(RetryPolicy, "calculate_delay", lambda *a, **k: 0)
    resource = fake_resource(tmp_path, monkeypatch)
    if failure == "exit":
        (tmp_path / "fail").touch()
    else:
        path = tmp_path / "result.json"
        payload = json.loads(path.read_text())
        payload["cycle" if failure == "cycle" else "readback_verified"] = (
            "2026" if failure == "cycle" else False
        )
        path.write_text(json.dumps(payload))

    @asset(partitions_def=fec_cycle_partitions)
    def consumer(fec_committee_summary_facts):
        pytest.fail("consumer ran after a rejected Go result")

    with DagsterInstance.ephemeral() as instance:
        instance.add_dynamic_partitions("fec_cycle", ["2024"])
        result = materialize(
            [
                pinned_release("/immutable/release.json"),
                fec_committee_summary_facts,
                consumer,
            ],
            instance=instance,
            resources={"go_pipeline": resource},
            partition_key="2024",
            raise_on_error=False,
        )
    assert not result.success
    assert not result.asset_materializations_for_node("fec_committee_summary_facts")
    checks = result.get_asset_check_evaluations()
    assert checks and all(not c.passed and c.partition == "2024" for c in checks)


def test_manual_job_replays_published_release(tmp_path):
    """Opt-in real Go gate. Mount source/fact storage read-only; no daemon needed."""
    release_path = os.environ.get("LT_SUMMARY_DAGSTER_RELEASE")
    if not release_path:
        pytest.skip("requires a published release and real Go binary")
    storage = Path(os.environ["LT_SUMMARY_STORAGE_ROOT"])
    release_bytes = Path(release_path).read_bytes()
    release = json.loads(release_bytes)
    assert (
        Path(release_path)
        == storage / "releases/fec/manifests" / f"{release['release_id']}.json"
    )
    current = storage / "releases/fec/current.json"
    current_before = current.read_bytes()
    resource = GoPipelineResource(
        binary_path=os.environ["LEGAL_TENDER_BINARY"],
        artifact_root=str(tmp_path / "control"),
        contracts_root=str(CONTRACTS),
        # A missing pointer is intentional: only the loaded upstream value counts.
        current_fec_release_manifest=str(tmp_path / "unused-current.json"),
        storage_root=str(storage),
        occurrence_timeout_seconds=120,
    )
    resources = {
        "go_pipeline": resource,
        "io_manager": FilesystemIOManager(base_dir=str(tmp_path / "io")),
    }
    release_asset = pinned_release(release_path)
    test_defs = Definitions(
        assets=[SourceAsset(release_asset.key), fec_committee_summary_facts],
        jobs=[fec_committee_summary_fact_job],
        resources=resources,
    )
    results = []
    instance_dir = tmp_path / "instance"
    instance_dir.mkdir()
    with DagsterInstance.local_temp(str(instance_dir)) as instance:
        instance.add_dynamic_partitions("fec_cycle", release["periods"])
        materialize([release_asset], instance=instance, resources=resources)
        job = test_defs.resolve_job_def(fec_committee_summary_fact_job.name)
        for cycle in release["periods"]:
            previous = None
            for _ in range(2):
                result = job.execute_in_process(instance=instance, partition_key=cycle)
                assert result.success
                check = result.get_asset_check_evaluations()[0]
                assert check.passed and check.partition == cycle
                assert check.target_materialization_data.run_id == result.run_id
                manifest = Path(result.output_for_node("fec_committee_summary_facts"))
                encoded = manifest.read_bytes()
                value = validate(json.loads(encoded))
                assert value["cycle"] == cycle
                assert value["source_release_id"] == release["release_id"]
                assert (
                    value["source_release_manifest_sha256"]
                    == hashlib.sha256(release_bytes).hexdigest()
                )
                m = result.asset_materializations_for_node(
                    "fec_committee_summary_facts"
                )[0]
                artifact = Path(m.metadata["control_artifact_path"].value)
                assert artifact.read_bytes() == encoded
                assert m.tags["dagster/data_version"] == value["fact_set_id"]
                if previous is not None:
                    assert previous == encoded
                previous = encoded
                results.append(
                    {
                        "cycle": cycle,
                        "run_id": result.run_id,
                        "manifest": str(manifest),
                        "fact_set_id": value["fact_set_id"],
                        "manifest_sha256": hashlib.sha256(encoded).hexdigest(),
                        "rows": value["facts"]["record_count"],
                        "check_passed": check.passed,
                    }
                )
    assert Path(release_path).read_bytes() == release_bytes
    assert current.read_bytes() == current_before
    (tmp_path / "verification.json").write_text(
        json.dumps(
            {
                "complete": True,
                "release_id": release["release_id"],
                "release_sha256": hashlib.sha256(release_bytes).hexdigest(),
                "runs": results,
                "current_unchanged": True,
            },
            indent=2,
        )
        + "\n"
    )
