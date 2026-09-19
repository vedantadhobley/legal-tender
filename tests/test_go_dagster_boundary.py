"""Contract and wiring tests for the minimal Go/Dagster boundary."""

from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest
from dagster import (
    AssetKey,
    DagsterInstance,
    Definitions,
    MultiPartitionKey,
    RunRequest,
    SkipReason,
    SourceAsset,
    asset,
    materialize,
)

from orchestration.assets import (
    _classic_to_ie_projection_bundle_mapping,
    _classic_to_receiver_flow_projection_bundle_mapping,
    _classic_to_receipt_bundle_mapping,
    _cycle_to_bundle_mapping,
    _cycle_to_ie_projection_bundle_mapping,
    _cycle_to_receiver_flow_projection_bundle_mapping,
    arango_independent_expenditures,
    arango_receiver_reported_committee_flows,
    arango_resolved_independent_expenditures,
    fec_candidate_itemized_receipts,
    fec_candidate_receipt_bundle_partitions,
    fec_candidate_receipt_fact_bundle,
    fec_classic_facts,
    fec_classic_occurrences,
    fec_classic_slice_partitions,
    fec_cycle_partitions,
    fec_effective_independent_expenditures,
    fec_independent_expenditure_candidate_resolution,
    fec_independent_expenditure_projection_bundle,
    fec_independent_expenditure_projection_bundle_partitions,
    fec_release_acquisition,
    fec_release_candidate,
    fec_release_discovery,
    fec_release_publication,
    fec_release_stage,
    fec_receiver_reported_committee_flows,
    fec_receiver_committee_flow_projection_bundle,
    fec_receiver_committee_flow_projection_bundle_partitions,
    fec_resolved_independent_expenditure_projection_bundle,
    fec_resolved_independent_expenditure_projection_bundle_partitions,
    fec_resolved_independent_expenditures,
    fec_schedule_a_facts,
    fec_schedule_a_occurrences,
    fec_schedule_b_facts,
    fec_schedule_e_facts,
    fec_schedule_e_occurrences,
)
from orchestration.definitions import (
    _acquisition_request_from_metadata,
    _classic_occurrence_requests_from_metadata,
    _occurrence_requests_from_metadata,
    _publication_request_from_metadata,
    _schedule_b_fact_requests_from_metadata,
    _schedule_e_occurrence_requests_from_metadata,
    _staging_request_from_metadata,
    monday_fec_release_schedule,
)
from orchestration.go_process import GoCommandError, run_json_command
from orchestration.resources import GoPipelineResource

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
CONTRACTS_ROOT = REPOSITORY_ROOT / "contracts"
FEC_RELEASE_CONTRACT = CONTRACTS_ROOT / "releases" / "fec" / "v1"


def test_rewrite_definitions_load() -> None:
    from orchestration.definitions import defs

    Definitions.validate_loadable(defs)
    assert monday_fec_release_schedule.cron_schedule == "0 4 * * 1"
    assert monday_fec_release_schedule.execution_timezone == "America/New_York"


def test_receipt_bundle_partition_mappings_select_exact_fact_partitions() -> None:
    instance = DagsterInstance.ephemeral()
    instance.add_dynamic_partitions("fec_cycle", ["2026"])
    bundle_subset = fec_candidate_receipt_bundle_partitions.empty_subset().with_partition_keys(
        [
            MultiPartitionKey(
                {
                    "bundle": "candidate-itemized-individual-receipts",
                    "cycle": "2026",
                }
            )
        ]
    )

    classic_result = (
        _classic_to_receipt_bundle_mapping.get_upstream_mapped_partitions_result_for_partitions(
            bundle_subset,
            fec_candidate_receipt_bundle_partitions,
            fec_classic_slice_partitions,
            dynamic_partitions_store=instance,
        )
    )
    classic_keys = {
        tuple(sorted(key.keys_by_dimension.items()))
        for key in classic_result.partitions_subset.get_partition_keys()
    }
    assert classic_keys == {
        (("cycle", "2026"), ("dataset", "candidate-committee-linkage")),
        (("cycle", "2026"), ("dataset", "all-candidates-summary")),
        (("cycle", "2026"), ("dataset", "current-campaigns-summary")),
    }

    schedule_result = (
        _cycle_to_bundle_mapping.get_upstream_mapped_partitions_result_for_partitions(
            bundle_subset,
            fec_candidate_receipt_bundle_partitions,
            fec_cycle_partitions,
            dynamic_partitions_store=instance,
        )
    )
    assert set(schedule_result.partitions_subset.get_partition_keys()) == {"2026"}
    instance.dispose()


def test_ie_projection_bundle_mappings_select_calculation_and_master_partitions() -> None:
    instance = DagsterInstance.ephemeral()
    instance.add_dynamic_partitions("fec_cycle", ["2026"])
    bundle_subset = (
        fec_independent_expenditure_projection_bundle_partitions.empty_subset().with_partition_keys(
            [
                MultiPartitionKey(
                    {
                        "bundle": "independent-expenditure-projection",
                        "cycle": "2026",
                    }
                )
            ]
        )
    )

    classic_result = (
        _classic_to_ie_projection_bundle_mapping.get_upstream_mapped_partitions_result_for_partitions(
            bundle_subset,
            fec_independent_expenditure_projection_bundle_partitions,
            fec_classic_slice_partitions,
            dynamic_partitions_store=instance,
        )
    )
    classic_keys = {
        tuple(sorted(key.keys_by_dimension.items()))
        for key in classic_result.partitions_subset.get_partition_keys()
    }
    assert classic_keys == {
        (("cycle", "2026"), ("dataset", "candidate-master")),
        (("cycle", "2026"), ("dataset", "committee-master")),
    }

    calculation_result = (
        _cycle_to_ie_projection_bundle_mapping.get_upstream_mapped_partitions_result_for_partitions(
            bundle_subset,
            fec_independent_expenditure_projection_bundle_partitions,
            fec_cycle_partitions,
            dynamic_partitions_store=instance,
        )
    )
    assert set(calculation_result.partitions_subset.get_partition_keys()) == {"2026"}
    instance.dispose()


def test_receiver_flow_projection_bundle_mappings_select_exact_inputs() -> None:
    instance = DagsterInstance.ephemeral()
    instance.add_dynamic_partitions("fec_cycle", ["2026"])
    bundle_subset = (
        fec_receiver_committee_flow_projection_bundle_partitions.empty_subset().with_partition_keys(
            [
                MultiPartitionKey(
                    {
                        "bundle": (
                            "receiver-reported-committee-flow-projection"
                        ),
                        "cycle": "2026",
                    }
                )
            ]
        )
    )

    classic_result = (
        _classic_to_receiver_flow_projection_bundle_mapping.get_upstream_mapped_partitions_result_for_partitions(
            bundle_subset,
            fec_receiver_committee_flow_projection_bundle_partitions,
            fec_classic_slice_partitions,
            dynamic_partitions_store=instance,
        )
    )
    classic_keys = {
        tuple(sorted(key.keys_by_dimension.items()))
        for key in classic_result.partitions_subset.get_partition_keys()
    }
    assert classic_keys == {
        (("cycle", "2026"), ("dataset", "committee-master")),
    }

    calculation_result = (
        _cycle_to_receiver_flow_projection_bundle_mapping.get_upstream_mapped_partitions_result_for_partitions(
            bundle_subset,
            fec_receiver_committee_flow_projection_bundle_partitions,
            fec_cycle_partitions,
            dynamic_partitions_store=instance,
        )
    )
    assert set(calculation_result.partitions_subset.get_partition_keys()) == {"2026"}
    instance.dispose()


def test_assets_invoke_go_and_preserve_contract_results(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    discovery_path = tmp_path / "fake-discovery.json"
    discovery_path.write_text(
        json.dumps(_discovery_fixture(), separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    plan_path = FEC_RELEASE_CONTRACT / "fixtures" / "update-available.json"
    acquisition_path = tmp_path / "fake-acquisition.json"
    acquisition_path.write_text(
        json.dumps(_acquisition_fixture(), separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    stage_path = tmp_path / "fake-stage.json"
    stage_path.write_text(
        json.dumps(_stage_fixture(), separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    manifest_path = tmp_path / "fake-manifest.json"
    manifest_path.write_text(
        json.dumps(_manifest_fixture(), separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    occurrence_path = tmp_path / "fake-occurrence-manifest.json"
    occurrence_path.write_text(
        json.dumps(_occurrence_fixture(), separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    schedule_fact_path = tmp_path / "fake-schedule-a-fact-manifest.json"
    schedule_fact_path.write_text(
        json.dumps(_schedule_a_fact_fixture(), separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    arguments_log = tmp_path / "arguments.jsonl"
    current_manifest = tmp_path / "current-release.json"
    current_manifest.write_text("{}\n", encoding="utf-8")
    fake_binary = _write_fake_binary(tmp_path)
    monkeypatch.setenv("FAKE_DISCOVERY_PATH", str(discovery_path))
    monkeypatch.setenv("FAKE_PLAN_PATH", str(plan_path))
    monkeypatch.setenv("FAKE_ACQUISITION_PATH", str(acquisition_path))
    monkeypatch.setenv("FAKE_STAGE_PATH", str(stage_path))
    monkeypatch.setenv("FAKE_MANIFEST_PATH", str(manifest_path))
    monkeypatch.setenv("FAKE_OCCURRENCE_PATH", str(occurrence_path))
    monkeypatch.setenv("FAKE_SCHEDULE_FACT_PATH", str(schedule_fact_path))
    monkeypatch.setenv("FAKE_ARGUMENTS_LOG", str(arguments_log))

    instance = DagsterInstance.ephemeral()
    instance.add_dynamic_partitions("fec_cycle", ["2026"])
    result = materialize(
        [
            fec_release_discovery,
            fec_release_candidate,
            fec_release_acquisition,
            fec_release_stage,
            fec_release_publication,
            fec_schedule_a_occurrences,
            fec_schedule_a_facts,
        ],
        resources={
            "go_pipeline": GoPipelineResource(
                binary_path=str(fake_binary),
                artifact_root=str(tmp_path / "control"),
                contracts_root=str(CONTRACTS_ROOT),
                current_fec_release_manifest=str(current_manifest),
                storage_root=str(tmp_path / "storage"),
                timeout_seconds=10,
                acquisition_timeout_seconds=20,
                staging_timeout_seconds=20,
                occurrence_timeout_seconds=20,
            )
        },
        run_config={
            "ops": {
                "fec_release_acquisition": {
                    "config": {"release_plan_path": str(plan_path)}
                },
                "fec_release_stage": {
                    "config": {
                        "release_plan_path": str(plan_path),
                        "acquisition_path": str(acquisition_path),
                    }
                },
                "fec_release_publication": {
                    "config": {
                        "release_plan_path": str(plan_path),
                        "acquisition_path": str(acquisition_path),
                        "staged_release_path": str(stage_path),
                    }
                },
                "fec_schedule_a_occurrences": {
                    "config": {"source_release_manifest_path": str(manifest_path)}
                },
                "fec_schedule_a_facts": {
                    "config": {"source_release_manifest_path": str(manifest_path)}
                },
            }
        },
        partition_key="2026",
        instance=instance,
    )
    instance.dispose()

    assert result.success
    discovery_materialization = result.asset_materializations_for_node(
        "fec_release_discovery"
    )[0]
    candidate_materialization = result.asset_materializations_for_node(
        "fec_release_candidate"
    )[0]
    acquisition_materialization = result.asset_materializations_for_node(
        "fec_release_acquisition"
    )[0]
    stage_materialization = result.asset_materializations_for_node("fec_release_stage")[
        0
    ]
    publication_materialization = result.asset_materializations_for_node(
        "fec_release_publication"
    )[0]
    occurrence_materialization = result.asset_materializations_for_node(
        "fec_schedule_a_occurrences"
    )[0]
    fact_materialization = result.asset_materializations_for_node(
        "fec_schedule_a_facts"
    )[0]
    assert discovery_materialization.metadata["source_count"].value == 21
    assert discovery_materialization.metadata["unavailable_source_count"].value == 0
    assert candidate_materialization.metadata["status"].value == "update_available"
    assert candidate_materialization.metadata["changed_source_count"].value == 21
    assert acquisition_materialization.metadata["status"].value == "acquired"
    assert acquisition_materialization.metadata["artifact_count"].value == 21
    assert stage_materialization.metadata["status"].value == "staged"
    assert stage_materialization.metadata["output_count"].value == 24
    assert publication_materialization.metadata["state"].value == "published"
    assert publication_materialization.metadata["staged_output_count"].value == 24
    assert occurrence_materialization.metadata["cycle"].value == "2026"
    assert occurrence_materialization.metadata["occurrence_set_id"].value == "f" * 64
    assert fact_materialization.metadata["fact_set_id"].value == "5" * 64

    discovery_artifacts = list((tmp_path / "control" / "discoveries").glob("*.json"))
    plan_artifacts = list((tmp_path / "control" / "plans").glob("*.json"))
    acquisition_artifacts = list((tmp_path / "control" / "acquisitions").glob("*.json"))
    stage_artifacts = list((tmp_path / "control" / "stages").glob("*.json"))
    manifest_artifacts = list((tmp_path / "control" / "manifests").glob("*.json"))
    occurrence_artifacts = list(
        (tmp_path / "control" / "occurrences" / "2026").glob("*.json")
    )
    fact_artifacts = list(
        (tmp_path / "control" / "facts" / "schedule-a" / "2026").glob("*.json")
    )
    assert len(discovery_artifacts) == 1
    assert len(plan_artifacts) == 1
    assert len(acquisition_artifacts) == 1
    assert len(stage_artifacts) == 1
    assert len(manifest_artifacts) == 1
    assert len(occurrence_artifacts) == 1
    assert len(fact_artifacts) == 1
    assert (
        json.loads(discovery_artifacts[0].read_text(encoding="utf-8"))["schema_version"]
        == "legal-tender.fec.discovery.v1"
    )
    assert json.loads(plan_artifacts[0].read_text(encoding="utf-8"))["status"] == (
        "update_available"
    )

    invocations = [
        json.loads(line)
        for line in arguments_log.read_text(encoding="utf-8").splitlines()
    ]
    assert invocations[0] == ["pipeline", "fec", "discover"]
    assert invocations[1][:3] == ["pipeline", "fec", "plan-release"]
    observation_index = invocations[1].index("--observations") + 1
    assert Path(invocations[1][observation_index]) == discovery_artifacts[0]
    current_index = invocations[1].index("--current") + 1
    assert Path(invocations[1][current_index]) == current_manifest
    assert invocations[2][:3] == ["pipeline", "fec", "acquire"]
    assert Path(invocations[2][invocations[2].index("--plan") + 1]) == plan_path
    assert Path(invocations[2][invocations[2].index("--storage-root") + 1]) == (
        tmp_path / "storage"
    )
    assert invocations[2][invocations[2].index("--run-id") + 1]
    assert invocations[3][:3] == ["pipeline", "fec", "stage-release"]
    assert Path(invocations[3][invocations[3].index("--plan") + 1]) == plan_path
    assert Path(invocations[3][invocations[3].index("--acquisition") + 1]) == (
        acquisition_path
    )
    assert invocations[4][:3] == ["pipeline", "fec", "publish-release"]
    assert Path(invocations[4][invocations[4].index("--stage") + 1]) == stage_path
    assert Path(invocations[4][invocations[4].index("--current") + 1]) == (
        current_manifest
    )
    assert invocations[5][:3] == [
        "pipeline",
        "fec",
        "publish-schedule-a-compact-occurrences",
    ]
    assert Path(invocations[5][invocations[5].index("--release") + 1]) == (
        manifest_path
    )
    assert invocations[5][invocations[5].index("--cycle") + 1] == "2026"
    assert invocations[6][:3] == [
        "pipeline",
        "fec",
        "publish-schedule-a-columnar-facts",
    ]
    assert Path(invocations[6][invocations[6].index("--occurrences") + 1]).is_file()


def test_candidate_sensor_only_authorizes_update_available() -> None:
    class Value:
        def __init__(self, value: str):
            self.value = value

    no_change = _acquisition_request_from_metadata({"status": Value("no_change")})
    assert isinstance(no_change, SkipReason)

    request = _acquisition_request_from_metadata(
        {
            "status": Value("update_available"),
            "candidate_release_id": Value("fec-" + "a" * 64),
            "control_artifact_path": Value("/storage/control/plan.json"),
            "control_artifact_sha256": Value("b" * 64),
        }
    )
    assert isinstance(request, RunRequest)
    assert request.run_key == f"fec-{'a' * 64}:{'b' * 64}"
    assert request.run_config["ops"]["fec_release_acquisition"]["config"] == {
        "release_plan_path": "/storage/control/plan.json"
    }


def test_stage_and_publication_sensors_preserve_exact_evidence_paths() -> None:
    class Value:
        def __init__(self, value: str):
            self.value = value

    assert isinstance(
        _staging_request_from_metadata({"status": Value("failed")}), SkipReason
    )
    staging = _staging_request_from_metadata(
        {
            "status": Value("acquired"),
            "candidate_release_id": Value("fec-" + "a" * 64),
            "control_artifact_path": Value("/storage/control/acquisition.json"),
            "control_artifact_sha256": Value("c" * 64),
            "release_plan_artifact_path": Value("/storage/control/plan.json"),
        }
    )
    assert isinstance(staging, RunRequest)
    assert staging.run_config["ops"]["fec_release_stage"]["config"] == {
        "release_plan_path": "/storage/control/plan.json",
        "acquisition_path": "/storage/control/acquisition.json",
    }

    assert isinstance(
        _publication_request_from_metadata({"status": Value("blocked")}),
        SkipReason,
    )
    publication = _publication_request_from_metadata(
        {
            "status": Value("staged"),
            "candidate_release_id": Value("fec-" + "a" * 64),
            "control_artifact_path": Value("/storage/control/stage.json"),
            "control_artifact_sha256": Value("d" * 64),
            "release_plan_artifact_path": Value("/storage/control/plan.json"),
            "acquisition_artifact_path": Value("/storage/control/acquisition.json"),
        }
    )
    assert isinstance(publication, RunRequest)
    assert publication.run_config["ops"]["fec_release_publication"]["config"] == {
        "release_plan_path": "/storage/control/plan.json",
        "acquisition_path": "/storage/control/acquisition.json",
        "staged_release_path": "/storage/control/stage.json",
    }


def test_occurrence_sensor_fans_out_exact_release_to_source_cycles() -> None:
    class Value:
        def __init__(self, value):
            self.value = value

    assert isinstance(
        _occurrence_requests_from_metadata({"state": Value("failed")}), SkipReason
    )
    requests = _occurrence_requests_from_metadata(
        {
            "state": Value("published"),
            "release_id": Value("fec-" + "a" * 64),
            "control_artifact_path": Value("/storage/control/release.json"),
            "control_artifact_sha256": Value("b" * 64),
            "periods": Value(["2020", "2022", "2024", "2026"]),
        }
    )
    assert [request.partition_key for request in requests] == [
        "2020",
        "2022",
        "2024",
        "2026",
    ]
    assert requests[-1].run_config["ops"]["fec_schedule_a_occurrences"]["config"] == {
        "source_release_manifest_path": "/storage/control/release.json"
    }
    assert requests[-1].run_config["ops"]["fec_schedule_a_facts"]["config"] == {
        "source_release_manifest_path": "/storage/control/release.json"
    }


def test_classic_occurrence_sensor_fans_out_every_dataset_and_cycle() -> None:
    class Value:
        def __init__(self, value):
            self.value = value

    assert isinstance(
        _classic_occurrence_requests_from_metadata({"state": Value("failed")}),
        SkipReason,
    )
    requests = _classic_occurrence_requests_from_metadata(
        {
            "state": Value("published"),
            "release_id": Value("fec-" + "a" * 64),
            "control_artifact_path": Value("/storage/control/release.json"),
            "control_artifact_sha256": Value("b" * 64),
            "periods": Value(["2020", "2022", "2024", "2026"]),
        }
    )
    assert len(requests) == 20
    assert requests[0].partition_key == MultiPartitionKey(
        {"dataset": "candidate-master", "cycle": "2020"}
    )
    assert requests[-1].partition_key == MultiPartitionKey(
        {"dataset": "current-campaigns-summary", "cycle": "2026"}
    )
    assert requests[-1].run_config["ops"]["fec_classic_occurrences"]["config"] == {
        "source_release_manifest_path": "/storage/control/release.json",
        "dataset": "current-campaigns-summary",
        "cycle": "2026",
    }
    assert requests[-1].run_config["ops"]["fec_classic_facts"]["config"] == {
        "source_release_manifest_path": "/storage/control/release.json"
    }


def test_schedule_e_sensor_requires_v2_and_fans_out_exact_release() -> None:
    class Value:
        def __init__(self, value):
            self.value = value

    base = {
        "state": Value("published"),
        "release_id": Value("fec-" + "a" * 64),
        "control_artifact_path": Value("/storage/control/release.json"),
        "control_artifact_sha256": Value("b" * 64),
        "periods": Value(["2020", "2022", "2024", "2026"]),
    }
    v1 = dict(base)
    v1["inventory_version"] = Value(
        "legal-tender.fec.initial-release-inventory.v1"
    )
    assert isinstance(_schedule_e_occurrence_requests_from_metadata(v1), SkipReason)

    v2 = dict(base)
    v2["inventory_version"] = Value(
        "legal-tender.fec.initial-release-inventory.v2"
    )
    requests = _schedule_e_occurrence_requests_from_metadata(v2)
    assert [request.partition_key for request in requests] == [
        "2020",
        "2022",
        "2024",
        "2026",
    ]
    assert requests[-1].run_config["ops"]["fec_schedule_e_occurrences"][
        "config"
    ] == {"source_release_manifest_path": "/storage/control/release.json"}
    assert requests[-1].run_config["ops"]["fec_schedule_e_facts"]["config"] == {
        "source_release_manifest_path": "/storage/control/release.json"
    }
    v2["inventory_version"] = Value("legal-tender.fec.initial-release-inventory.v4")
    assert _schedule_e_occurrence_requests_from_metadata(v2) == requests


def test_schedule_b_sensor_requires_v3_and_fans_out_exact_release() -> None:
    class Value:
        def __init__(self, value):
            self.value = value

    base = {
        "state": Value("published"),
        "release_id": Value("fec-" + "a" * 64),
        "control_artifact_path": Value("/storage/control/release.json"),
        "control_artifact_sha256": Value("b" * 64),
        "periods": Value(["2020", "2022", "2024", "2026"]),
    }
    v2 = dict(base)
    v2["inventory_version"] = Value(
        "legal-tender.fec.initial-release-inventory.v2"
    )
    assert isinstance(_schedule_b_fact_requests_from_metadata(v2), SkipReason)

    v3 = dict(base)
    v3["inventory_version"] = Value(
        "legal-tender.fec.initial-release-inventory.v3"
    )
    requests = _schedule_b_fact_requests_from_metadata(v3)
    assert [request.partition_key for request in requests] == [
        "2020",
        "2022",
        "2024",
        "2026",
    ]
    assert requests[-1].run_config["ops"]["fec_schedule_b_facts"]["config"] == {
        "source_release_manifest_path": "/storage/control/release.json"
    }
    v3["inventory_version"] = Value("legal-tender.fec.initial-release-inventory.v4")
    assert _schedule_b_fact_requests_from_metadata(v3) == requests


def test_schedule_b_fact_asset_preserves_go_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    release_path = tmp_path / "release-v3.json"
    release_path.write_text("{}\n", encoding="utf-8")
    fact_path = tmp_path / "schedule-b-fact.json"
    fact_path.write_text(
        json.dumps(_schedule_b_fact_fixture(), separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    arguments_log = tmp_path / "schedule-b-arguments.jsonl"
    fake_binary = _write_fake_binary(tmp_path)
    monkeypatch.setenv("FAKE_SCHEDULE_B_FACT_PATH", str(fact_path))
    monkeypatch.setenv("FAKE_ARGUMENTS_LOG", str(arguments_log))

    instance = DagsterInstance.ephemeral()
    instance.add_dynamic_partitions("fec_cycle", ["2024"])
    result = materialize(
        [SourceAsset(AssetKey("fec_release_publication")), fec_schedule_b_facts],
        resources={
            "go_pipeline": GoPipelineResource(
                binary_path=str(fake_binary),
                artifact_root=str(tmp_path / "control"),
                contracts_root=str(CONTRACTS_ROOT),
                current_fec_release_manifest=str(tmp_path / "current.json"),
                storage_root=str(tmp_path / "storage"),
                timeout_seconds=10,
                acquisition_timeout_seconds=20,
                staging_timeout_seconds=20,
                occurrence_timeout_seconds=20,
            )
        },
        run_config={
            "ops": {
                "fec_schedule_b_facts": {
                    "config": {"source_release_manifest_path": str(release_path)}
                }
            }
        },
        partition_key="2024",
        instance=instance,
    )
    instance.dispose()
    assert result.success
    materialization = result.asset_materializations_for_node("fec_schedule_b_facts")[0]
    assert materialization.metadata["fact_set_id"].value == "1" * 64
    assert materialization.metadata["source_artifact_sha256"].value == "4" * 64
    invocation = json.loads(arguments_log.read_text(encoding="utf-8"))
    assert invocation[:3] == [
        "pipeline",
        "fec",
        "publish-schedule-b-columnar-facts",
    ]
    assert invocation[invocation.index("--cycle") + 1] == "2024"


def test_schedule_e_occurrence_and_fact_assets_preserve_go_results(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    release_path = tmp_path / "release-v2.json"
    release_path.write_text("{}\n", encoding="utf-8")
    occurrence_path = tmp_path / "schedule-e-occurrence.json"
    occurrence_path.write_text(
        json.dumps(_schedule_e_occurrence_fixture(), separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    fact_path = tmp_path / "schedule-e-fact.json"
    fact_path.write_text(
        json.dumps(_schedule_e_fact_fixture(), separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    calculation_path = tmp_path / "effective-independent-expenditures.json"
    calculation_path.write_text(
        json.dumps(_effective_independent_expenditure_fixture(), separators=(",", ":"))
        + "\n",
        encoding="utf-8",
    )
    resolution_path = tmp_path / "independent-expenditure-candidate-resolution.json"
    resolution_path.write_text(
        json.dumps(_candidate_resolution_fixture(), separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    resolved_path = tmp_path / "resolved-independent-expenditures.json"
    resolved_path.write_text(
        json.dumps(_resolved_independent_expenditure_fixture(), separators=(",", ":"))
        + "\n",
        encoding="utf-8",
    )
    arguments_log = tmp_path / "schedule-e-arguments.jsonl"
    fake_binary = _write_fake_binary(tmp_path)
    monkeypatch.setenv("FAKE_SCHEDULE_E_OCCURRENCE_PATH", str(occurrence_path))
    monkeypatch.setenv("FAKE_SCHEDULE_E_FACT_PATH", str(fact_path))
    monkeypatch.setenv("FAKE_EFFECTIVE_IE_PATH", str(calculation_path))
    monkeypatch.setenv("FAKE_IE_CANDIDATE_RESOLUTION_PATH", str(resolution_path))
    monkeypatch.setenv("FAKE_RESOLVED_IE_PATH", str(resolved_path))
    monkeypatch.setenv("FAKE_ARGUMENTS_LOG", str(arguments_log))

    instance = DagsterInstance.ephemeral()
    instance.add_dynamic_partitions("fec_cycle", ["2024"])
    result = materialize(
        [
            SourceAsset(AssetKey("fec_release_publication")),
            SourceAsset(
                AssetKey("fec_classic_facts"),
                partitions_def=fec_classic_slice_partitions,
            ),
            fec_schedule_e_occurrences,
            fec_schedule_e_facts,
            fec_effective_independent_expenditures,
            fec_independent_expenditure_candidate_resolution,
            fec_resolved_independent_expenditures,
        ],
        resources={
            "go_pipeline": GoPipelineResource(
                binary_path=str(fake_binary),
                artifact_root=str(tmp_path / "control"),
                contracts_root=str(CONTRACTS_ROOT),
                current_fec_release_manifest=str(tmp_path / "current.json"),
                storage_root=str(tmp_path / "storage"),
                timeout_seconds=10,
                acquisition_timeout_seconds=20,
                staging_timeout_seconds=20,
                occurrence_timeout_seconds=20,
            )
        },
        run_config={
            "ops": {
                "fec_schedule_e_occurrences": {
                    "config": {"source_release_manifest_path": str(release_path)}
                },
                "fec_schedule_e_facts": {
                    "config": {"source_release_manifest_path": str(release_path)}
                },
            }
        },
        partition_key="2024",
        instance=instance,
    )
    instance.dispose()

    assert result.success
    occurrence = result.asset_materializations_for_node(
        "fec_schedule_e_occurrences"
    )[0]
    fact = result.asset_materializations_for_node("fec_schedule_e_facts")[0]
    calculation = result.asset_materializations_for_node(
        "fec_effective_independent_expenditures"
    )[0]
    resolution = result.asset_materializations_for_node(
        "fec_independent_expenditure_candidate_resolution"
    )[0]
    resolved = result.asset_materializations_for_node(
        "fec_resolved_independent_expenditures"
    )[0]
    assert occurrence.metadata["occurrence_set_id"].value == "2" * 64
    assert occurrence.metadata["counts"].data["selected_rows"] == 1
    assert fact.metadata["fact_set_id"].value == "3" * 64
    assert fact.metadata["fact_type"].value == (
        "fec.schedule_e_independent_expenditure.v1"
    )
    assert calculation.metadata["calculation_set_id"].value == "4" * 64
    assert calculation.metadata["decision_counts"].data["included"] == 1
    assert calculation.metadata["amounts"].data["included_minor_units"] == "125"
    assert resolution.metadata["calculation_set_id"].value == "5" * 64
    assert resolution.metadata["counts"].data["confirmed"] == 1
    assert resolution.metadata["amounts"].data["source_effective_minor_units"] == "125"
    assert resolved.metadata["calculation_set_id"].value == "9" * 64
    assert resolved.metadata["counts"].data["projectable_decisions"] == 1
    assert resolved.metadata["amounts"].data["projectable_minor_units"] == "125"

    invocations = [
        json.loads(line)
        for line in arguments_log.read_text(encoding="utf-8").splitlines()
    ]
    assert invocations[0][:3] == [
        "pipeline",
        "fec",
        "publish-schedule-e-occurrences",
    ]
    assert invocations[0][invocations[0].index("--cycle") + 1] == "2024"
    assert invocations[1][:3] == ["pipeline", "fec", "publish-schedule-e-facts"]
    assert Path(invocations[1][invocations[1].index("--occurrences") + 1]).is_file()
    assert invocations[2][:3] == [
        "pipeline",
        "fec",
        "publish-effective-independent-expenditures",
    ]
    assert Path(
        invocations[2][invocations[2].index("--schedule-e-facts") + 1]
    ).is_file()
    assert invocations[3][:3] == [
        "pipeline",
        "fec",
        "publish-independent-expenditure-candidate-resolution",
    ]
    assert invocations[3][invocations[3].index("--cycle") + 1] == "2024"
    assert Path(invocations[3][invocations[3].index("--effective") + 1]).is_file()
    assert invocations[4][:3] == [
        "pipeline",
        "fec",
        "publish-resolved-independent-expenditures",
    ]
    assert Path(
        invocations[4][invocations[4].index("--candidate-resolution") + 1]
    ).is_file()


def test_receiver_reported_committee_flow_asset_invokes_go(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_path = tmp_path / "schedule-a-facts.json"
    source_path.write_text("{}\n", encoding="utf-8")
    calculation_path = tmp_path / "receiver-committee-flows.json"
    calculation_path.write_text(
        json.dumps(_receiver_committee_flow_fixture(), separators=(",", ":"))
        + "\n",
        encoding="utf-8",
    )
    arguments_log = tmp_path / "receiver-flow-arguments.jsonl"
    fake_binary = _write_fake_binary(tmp_path)
    monkeypatch.setenv("FAKE_RECEIVER_FLOW_PATH", str(calculation_path))
    monkeypatch.setenv("FAKE_ARGUMENTS_LOG", str(arguments_log))
    resource = GoPipelineResource(
        binary_path=str(fake_binary),
        artifact_root=str(tmp_path / "control"),
        contracts_root=str(CONTRACTS_ROOT),
        current_fec_release_manifest=str(tmp_path / "current.json"),
        storage_root=str(tmp_path / "storage"),
        timeout_seconds=10,
        acquisition_timeout_seconds=20,
        staging_timeout_seconds=20,
        occurrence_timeout_seconds=20,
    )
    instance = DagsterInstance.ephemeral()
    instance.add_dynamic_partitions("fec_cycle", ["2024"])

    @asset(name="fec_schedule_a_facts", partitions_def=fec_cycle_partitions)
    def fake_schedule_a_facts() -> str:
        return str(source_path)

    result = materialize(
        [
            fake_schedule_a_facts,
            fec_receiver_reported_committee_flows,
        ],
        resources={"go_pipeline": resource},
        partition_key="2024",
        instance=instance,
    )
    instance.dispose()
    assert result.success
    materialization = result.asset_materializations_for_node(
        "fec_receiver_reported_committee_flows"
    )[0]
    assert materialization.metadata["calculation_set_id"].value == "6" * 64
    assert materialization.metadata["amounts"].data["included_minor_units"] == "125"
    invocation = json.loads(arguments_log.read_text(encoding="utf-8"))
    assert invocation[:3] == [
        "pipeline",
        "fec",
        "publish-receiver-committee-flows",
    ]
    assert invocation[invocation.index("--cycle") + 1] == "2024"
    assert Path(invocation[invocation.index("--schedule-a-facts") + 1]).is_file()


def test_receiver_flow_projection_bundle_and_arango_asset_invoke_go(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle_path = tmp_path / "receiver-flow-projection-bundle.json"
    bundle_path.write_text(
        json.dumps(
            _receiver_flow_projection_bundle_fixture(), separators=(",", ":")
        )
        + "\n",
        encoding="utf-8",
    )
    projection_path = tmp_path / "receiver-flow-arango-projection.json"
    projection_path.write_text(
        json.dumps(_arango_receiver_flow_fixture(), separators=(",", ":"))
        + "\n",
        encoding="utf-8",
    )
    arguments_log = tmp_path / "receiver-flow-projection-arguments.jsonl"
    fake_binary = _write_fake_binary(tmp_path)
    monkeypatch.setenv(
        "FAKE_RECEIVER_FLOW_PROJECTION_BUNDLE_PATH", str(bundle_path)
    )
    monkeypatch.setenv("FAKE_ARANGO_RECEIVER_FLOW_PATH", str(projection_path))
    monkeypatch.setenv("FAKE_ARGUMENTS_LOG", str(arguments_log))
    resource = GoPipelineResource(
        binary_path=str(fake_binary),
        artifact_root=str(tmp_path / "control"),
        contracts_root=str(CONTRACTS_ROOT),
        current_fec_release_manifest=str(tmp_path / "current.json"),
        storage_root=str(tmp_path / "storage"),
        timeout_seconds=10,
        acquisition_timeout_seconds=20,
        staging_timeout_seconds=20,
        occurrence_timeout_seconds=20,
    )

    instance = DagsterInstance.ephemeral()
    instance.add_dynamic_partitions("fec_cycle", ["2024"])
    bundle_result = materialize(
        [
            SourceAsset(
                AssetKey("fec_receiver_reported_committee_flows"),
                partitions_def=fec_cycle_partitions,
            ),
            SourceAsset(
                AssetKey("fec_classic_facts"),
                partitions_def=fec_classic_slice_partitions,
            ),
            fec_receiver_committee_flow_projection_bundle,
        ],
        resources={"go_pipeline": resource},
        partition_key=MultiPartitionKey(
            {
                "bundle": "receiver-reported-committee-flow-projection",
                "cycle": "2024",
            }
        ),
        instance=instance,
    )
    assert bundle_result.success
    bundle = bundle_result.asset_materializations_for_node(
        "fec_receiver_committee_flow_projection_bundle"
    )[0]
    assert bundle.metadata["bundle_id"].value == "a" * 64
    assert bundle.metadata["counts"].data == {
        "calculation_results": 1,
        "committee_facts": 2,
    }

    projection_result = materialize(
        [
            SourceAsset(
                AssetKey("fec_receiver_committee_flow_projection_bundle"),
                partitions_def=(
                    fec_receiver_committee_flow_projection_bundle_partitions
                ),
            ),
            arango_receiver_reported_committee_flows,
        ],
        resources={"go_pipeline": resource},
        run_config={
            "ops": {
                "arango_receiver_reported_committee_flows": {
                    "config": {
                        "endpoint": "http://arango.test:8529",
                        "username": "tester",
                        "batch_size": 100,
                        "query_repetitions": 3,
                    }
                }
            }
        },
        partition_key="2024",
        instance=instance,
    )
    instance.dispose()
    assert projection_result.success
    projection = projection_result.asset_materializations_for_node(
        "arango_receiver_reported_committee_flows"
    )[0]
    assert projection.metadata["projection_id"].value == "b" * 64
    assert projection.metadata["observed_counts"].data["edges"] == 1
    assert projection.metadata["topology"].data["weak_components"] == 1

    invocations = [
        json.loads(line)
        for line in arguments_log.read_text(encoding="utf-8").splitlines()
    ]
    assert invocations[0][:3] == [
        "pipeline",
        "fec",
        "publish-receiver-committee-flow-projection-bundle",
    ]
    assert invocations[1][:3] == [
        "pipeline",
        "fec",
        "probe-arango-receiver-committee-flows",
    ]
    assert Path(
        invocations[1][invocations[1].index("--projection-bundle") + 1]
    ).is_file()


def test_ie_projection_bundle_and_arango_asset_invoke_go(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle_path = (
        CONTRACTS_ROOT
        / "bundles"
        / "fec"
        / "independent-expenditure-projection"
        / "v1"
        / "fixtures"
        / "ready-2024.json"
    )
    projection_path = (
        CONTRACTS_ROOT
        / "projections"
        / "arango"
        / "independent-expenditures"
        / "v1"
        / "fixtures"
        / "partial-2024.json"
    )
    arguments_log = tmp_path / "ie-projection-bundle-arguments.jsonl"
    fake_binary = _write_fake_binary(tmp_path)
    monkeypatch.setenv("FAKE_IE_PROJECTION_BUNDLE_PATH", str(bundle_path))
    monkeypatch.setenv("FAKE_ARANGO_IE_PATH", str(projection_path))
    monkeypatch.setenv("FAKE_ARGUMENTS_LOG", str(arguments_log))
    resource = GoPipelineResource(
        binary_path=str(fake_binary),
        artifact_root=str(tmp_path / "control"),
        contracts_root=str(CONTRACTS_ROOT),
        current_fec_release_manifest=str(tmp_path / "current.json"),
        storage_root=str(tmp_path / "storage"),
        timeout_seconds=10,
        acquisition_timeout_seconds=20,
        staging_timeout_seconds=20,
        occurrence_timeout_seconds=20,
    )

    instance = DagsterInstance.ephemeral()
    instance.add_dynamic_partitions("fec_cycle", ["2024"])
    bundle_partition = MultiPartitionKey(
        {"bundle": "independent-expenditure-projection", "cycle": "2024"}
    )
    bundle_result = materialize(
        [
            SourceAsset(
                AssetKey("fec_effective_independent_expenditures"),
                partitions_def=fec_cycle_partitions,
            ),
            SourceAsset(
                AssetKey("fec_classic_facts"),
                partitions_def=fec_classic_slice_partitions,
            ),
            fec_independent_expenditure_projection_bundle,
        ],
        resources={"go_pipeline": resource},
        partition_key=bundle_partition,
        instance=instance,
    )
    assert bundle_result.success
    bundle_materialization = bundle_result.asset_materializations_for_node(
        "fec_independent_expenditure_projection_bundle"
    )[0]
    stored_bundle_path = bundle_materialization.metadata[
        "control_artifact_path"
    ].value
    assert Path(stored_bundle_path).is_file()
    assert bundle_materialization.metadata["counts"].data == {
        "calculation_results": 5495,
        "candidate_facts": 9798,
        "committee_facts": 20938,
    }

    projection_result = materialize(
        [
            SourceAsset(
                AssetKey("fec_independent_expenditure_projection_bundle"),
                partitions_def=(
                    fec_independent_expenditure_projection_bundle_partitions
                ),
            ),
            arango_independent_expenditures,
        ],
        resources={"go_pipeline": resource},
        run_config={
            "ops": {
                "arango_independent_expenditures": {
                    "config": {
                        "endpoint": "http://arango.test:8529",
                        "username": "tester",
                        "batch_size": 100,
                        "query_repetitions": 3,
                    }
                }
            }
        },
        partition_key="2024",
        instance=instance,
    )
    instance.dispose()
    assert projection_result.success
    projection = projection_result.asset_materializations_for_node(
        "arango_independent_expenditures"
    )[0]
    assert projection.metadata["projection_id"].value == "a" * 64
    assert projection.metadata["observed_counts"].data["edges"] == 1

    invocations = [
        json.loads(line)
        for line in arguments_log.read_text(encoding="utf-8").splitlines()
    ]
    assert invocations[0][:3] == [
        "pipeline",
        "fec",
        "publish-independent-expenditure-projection-bundle",
    ]
    assert invocations[0][invocations[0].index("--cycle") + 1] == "2024"
    assert invocations[1][:3] == [
        "pipeline",
        "fec",
        "probe-arango-independent-expenditures",
    ]
    assert Path(
        invocations[1][invocations[1].index("--projection-bundle") + 1]
    ).is_file()


def test_resolved_ie_projection_bundle_and_arango_asset_invoke_go(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle_path = (
        CONTRACTS_ROOT
        / "bundles"
        / "fec"
        / "resolved-independent-expenditure-projection"
        / "v1"
        / "fixtures"
        / "ready-2024.json"
    )
    projection_path = (
        CONTRACTS_ROOT
        / "projections"
        / "arango"
        / "independent-expenditures"
        / "v2"
        / "fixtures"
        / "partial-2024.json"
    )
    arguments_log = tmp_path / "resolved-ie-projection-arguments.jsonl"
    fake_binary = _write_fake_binary(tmp_path)
    monkeypatch.setenv("FAKE_RESOLVED_IE_PROJECTION_BUNDLE_PATH", str(bundle_path))
    monkeypatch.setenv("FAKE_ARANGO_RESOLVED_IE_PATH", str(projection_path))
    monkeypatch.setenv("FAKE_ARGUMENTS_LOG", str(arguments_log))
    resource = GoPipelineResource(
        binary_path=str(fake_binary),
        artifact_root=str(tmp_path / "control"),
        contracts_root=str(CONTRACTS_ROOT),
        current_fec_release_manifest=str(tmp_path / "current.json"),
        storage_root=str(tmp_path / "storage"),
        timeout_seconds=10,
        acquisition_timeout_seconds=20,
        staging_timeout_seconds=20,
        occurrence_timeout_seconds=20,
    )

    instance = DagsterInstance.ephemeral()
    instance.add_dynamic_partitions("fec_cycle", ["2024"])
    bundle_partition = MultiPartitionKey(
        {
            "bundle": "resolved-independent-expenditure-projection",
            "cycle": "2024",
        }
    )
    bundle_result = materialize(
        [
            SourceAsset(
                AssetKey("fec_resolved_independent_expenditures"),
                partitions_def=fec_cycle_partitions,
            ),
            SourceAsset(
                AssetKey("fec_classic_facts"),
                partitions_def=fec_classic_slice_partitions,
            ),
            fec_resolved_independent_expenditure_projection_bundle,
        ],
        resources={"go_pipeline": resource},
        partition_key=bundle_partition,
        instance=instance,
    )
    assert bundle_result.success
    bundle = bundle_result.asset_materializations_for_node(
        "fec_resolved_independent_expenditure_projection_bundle"
    )[0]
    assert bundle.metadata["counts"].data["calculation_results"] == 5303
    assert bundle.metadata["counts"].data["calculation_exceptions"] == 296

    projection_result = materialize(
        [
            SourceAsset(
                AssetKey(
                    "fec_resolved_independent_expenditure_projection_bundle"
                ),
                partitions_def=(
                    fec_resolved_independent_expenditure_projection_bundle_partitions
                ),
            ),
            arango_resolved_independent_expenditures,
        ],
        resources={"go_pipeline": resource},
        run_config={
            "ops": {
                "arango_resolved_independent_expenditures": {
                    "config": {
                        "endpoint": "http://arango.test:8529",
                        "username": "tester",
                        "batch_size": 100,
                        "query_repetitions": 3,
                    }
                }
            }
        },
        partition_key="2024",
        instance=instance,
    )
    instance.dispose()
    assert projection_result.success
    projection = projection_result.asset_materializations_for_node(
        "arango_resolved_independent_expenditures"
    )[0]
    assert projection.metadata["projection_id"].value == "a" * 64
    assert projection.metadata["candidate_resolution_coverage"].data == {
        "source_decisions": 5,
        "projectable_decisions": 3,
        "unprojectable_decisions": 2,
        "projectable_minor_units": "75",
        "unprojectable_minor_units": "50",
    }

    invocations = [
        json.loads(line)
        for line in arguments_log.read_text(encoding="utf-8").splitlines()
    ]
    assert invocations[0][:3] == [
        "pipeline",
        "fec",
        "publish-resolved-independent-expenditure-projection-bundle",
    ]
    assert invocations[1][:3] == [
        "pipeline",
        "fec",
        "probe-arango-resolved-independent-expenditures",
    ]
    assert Path(
        invocations[1][invocations[1].index("--projection-bundle") + 1]
    ).is_file()


def test_classic_occurrence_and_fact_assets_preserve_go_results(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    release_path = tmp_path / "release.json"
    release_path.write_text(
        json.dumps(_manifest_fixture(), separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    occurrence_path = tmp_path / "classic-occurrence.json"
    occurrence_path.write_text(
        json.dumps(_classic_occurrence_fixture(), separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    fact_path = tmp_path / "classic-fact.json"
    fact_path.write_text(
        json.dumps(_classic_fact_fixture(), separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    arguments_log = tmp_path / "classic-arguments.jsonl"
    fake_binary = _write_fake_binary(tmp_path)
    monkeypatch.setenv("FAKE_CLASSIC_OCCURRENCE_PATH", str(occurrence_path))
    monkeypatch.setenv("FAKE_CLASSIC_FACT_PATH", str(fact_path))
    monkeypatch.setenv("FAKE_ARGUMENTS_LOG", str(arguments_log))

    instance = DagsterInstance.ephemeral()
    partition_key = MultiPartitionKey(
        {"dataset": "candidate-master", "cycle": "2024"}
    )
    instance.add_dynamic_partitions("fec_cycle", ["2024"])
    result = materialize(
        [
            SourceAsset(AssetKey("fec_release_publication")),
            fec_classic_occurrences,
            fec_classic_facts,
        ],
        resources={
            "go_pipeline": GoPipelineResource(
                binary_path=str(fake_binary),
                artifact_root=str(tmp_path / "control"),
                contracts_root=str(CONTRACTS_ROOT),
                current_fec_release_manifest=str(tmp_path / "current.json"),
                storage_root=str(tmp_path / "storage"),
                timeout_seconds=10,
                acquisition_timeout_seconds=20,
                staging_timeout_seconds=20,
                occurrence_timeout_seconds=20,
            )
        },
        run_config={
            "ops": {
                "fec_classic_occurrences": {
                    "config": {
                        "source_release_manifest_path": str(release_path),
                        "dataset": "candidate-master",
                        "cycle": "2024",
                    }
                },
                "fec_classic_facts": {
                    "config": {"source_release_manifest_path": str(release_path)}
                },
            }
        },
        partition_key=partition_key,
        instance=instance,
    )
    instance.dispose()

    assert result.success
    occurrence = result.asset_materializations_for_node("fec_classic_occurrences")[0]
    fact = result.asset_materializations_for_node("fec_classic_facts")[0]
    assert occurrence.metadata["dataset"].value == "candidate-master"
    assert occurrence.metadata["occurrence_set_id"].value == "9" * 64
    assert fact.metadata["fact_type"].value == "fec.candidate_assertion.v1"
    assert fact.metadata["fact_set_id"].value == "8" * 64

    invocations = [
        json.loads(line)
        for line in arguments_log.read_text(encoding="utf-8").splitlines()
    ]
    assert invocations[0][:3] == [
        "pipeline",
        "fec",
        "publish-classic-occurrences",
    ]
    assert invocations[0][invocations[0].index("--dataset") + 1] == ("candidate-master")
    assert invocations[1][:3] == ["pipeline", "fec", "publish-classic-facts"]
    assert Path(invocations[1][invocations[1].index("--occurrences") + 1]).is_file()


def test_fact_bundle_and_compact_calculation_assets_invoke_go(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle_path = tmp_path / "fact-bundle.json"
    bundle_path.write_text(
        json.dumps(_fact_bundle_fixture(), separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    calculation_path = tmp_path / "compact-calculation.json"
    calculation_path.write_text(
        json.dumps(_candidate_receipt_calculation_fixture(), separators=(",", ":"))
        + "\n",
        encoding="utf-8",
    )
    arguments_log = tmp_path / "bundle-arguments.jsonl"
    fake_binary = _write_fake_binary(tmp_path)
    monkeypatch.setenv("FAKE_FACT_BUNDLE_PATH", str(bundle_path))
    monkeypatch.setenv("FAKE_CALCULATION_PATH", str(calculation_path))
    monkeypatch.setenv("FAKE_ARGUMENTS_LOG", str(arguments_log))
    resource = GoPipelineResource(
        binary_path=str(fake_binary),
        artifact_root=str(tmp_path / "control"),
        contracts_root=str(CONTRACTS_ROOT),
        current_fec_release_manifest=str(tmp_path / "current.json"),
        storage_root=str(tmp_path / "storage"),
        timeout_seconds=10,
        acquisition_timeout_seconds=20,
        staging_timeout_seconds=20,
        occurrence_timeout_seconds=20,
    )

    instance = DagsterInstance.ephemeral()
    instance.add_dynamic_partitions("fec_cycle", ["2026"])
    bundle_partition = MultiPartitionKey(
        {"bundle": "candidate-itemized-individual-receipts", "cycle": "2026"}
    )
    bundle_result = materialize(
        [
            SourceAsset(
                AssetKey("fec_schedule_a_facts"),
                partitions_def=fec_cycle_partitions,
            ),
            SourceAsset(
                AssetKey("fec_classic_facts"),
                partitions_def=fec_classic_slice_partitions,
            ),
            fec_candidate_receipt_fact_bundle,
        ],
        resources={"go_pipeline": resource},
        partition_key=bundle_partition,
        instance=instance,
    )
    assert bundle_result.success
    bundle_materialization = bundle_result.asset_materializations_for_node(
        "fec_candidate_receipt_fact_bundle"
    )[0]
    stored_bundle_path = bundle_materialization.metadata["control_artifact_path"].value
    assert Path(stored_bundle_path).is_file()
    assert bundle_materialization.metadata["bundle_id"].value == "b" * 64

    calculation_result = materialize(
        [
            SourceAsset(
                AssetKey("fec_candidate_receipt_fact_bundle"),
                partitions_def=fec_candidate_receipt_bundle_partitions,
            ),
            fec_candidate_itemized_receipts,
        ],
        resources={"go_pipeline": resource},
        partition_key="2026",
        instance=instance,
    )
    instance.dispose()
    assert calculation_result.success
    calculation_materialization = calculation_result.asset_materializations_for_node(
        "fec_candidate_itemized_receipts"
    )[0]
    assert calculation_materialization.metadata["calculation_set_id"].value == (
        "4" * 64
    )

    invocations = [
        json.loads(line)
        for line in arguments_log.read_text(encoding="utf-8").splitlines()
    ]
    assert invocations[0][:3] == [
        "pipeline",
        "fec",
        "publish-candidate-itemized-receipts-fact-bundle",
    ]
    assert invocations[0][invocations[0].index("--cycle") + 1] == "2026"
    assert invocations[1][:3] == [
        "pipeline",
        "fec",
        "publish-candidate-itemized-receipts-compact",
    ]
    assert Path(invocations[1][invocations[1].index("--fact-bundle") + 1]).is_file()


def test_adapter_rejects_invalid_go_output(tmp_path: Path) -> None:
    fake_binary = tmp_path / "invalid-result"
    fake_binary.write_text(
        "#!/usr/bin/env python3\nimport sys\nsys.stdout.write('{}\\n')\n",
        encoding="utf-8",
    )
    fake_binary.chmod(0o755)

    with pytest.raises(GoCommandError, match="violates discovery.schema.json"):
        run_json_command(
            binary_path=str(fake_binary),
            arguments=["pipeline", "fec", "discover"],
            schema_path=FEC_RELEASE_CONTRACT / "discovery.schema.json",
            artifact_root=tmp_path / "control",
            artifact_kind="discoveries",
            timeout_seconds=10,
        )
    assert not (tmp_path / "control").exists()


def test_adapter_preserves_contract_valid_failure_output(tmp_path: Path) -> None:
    blocked_fixture = (
        FEC_RELEASE_CONTRACT / "fixtures" / "acquisition-storage-blocked.json"
    )
    fake_binary = tmp_path / "blocked-result"
    fake_binary.write_text(
        "#!/usr/bin/env python3\n"
        "from pathlib import Path\n"
        "import sys\n"
        f"sys.stdout.buffer.write(Path({str(blocked_fixture)!r}).read_bytes())\n"
        "sys.stderr.write('storage gate blocked acquisition\\n')\n"
        "raise SystemExit(1)\n",
        encoding="utf-8",
    )
    fake_binary.chmod(0o755)

    with pytest.raises(GoCommandError, match="exited with code 1") as raised:
        run_json_command(
            binary_path=str(fake_binary),
            arguments=["pipeline", "fec", "acquire"],
            schema_path=FEC_RELEASE_CONTRACT / "acquisition-result.schema.json",
            artifact_root=tmp_path / "control",
            artifact_kind="acquisitions",
            timeout_seconds=10,
        )
    assert raised.value.result is not None
    assert raised.value.result.payload["status"] == "blocked"
    assert raised.value.result.artifact_path.is_file()


def test_python_control_plane_has_no_domain_or_data_clients() -> None:
    allowed_import_roots = {
        "__future__",
        "collections",
        "dataclasses",
        "dagster",
        "hashlib",
        "json",
        "jsonschema",
        "orchestration",
        "os",
        "pathlib",
        "referencing",
        "subprocess",
        "tempfile",
        "typing",
    }
    unexpected: list[str] = []
    for source_path in sorted((REPOSITORY_ROOT / "orchestration").glob("*.py")):
        tree = ast.parse(
            source_path.read_text(encoding="utf-8"), filename=str(source_path)
        )
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                roots = [alias.name.split(".", 1)[0] for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                roots = [node.module.split(".", 1)[0]]
            else:
                continue
            for root in roots:
                if root not in allowed_import_roots:
                    unexpected.append(f"{source_path.name}: {root}")
    assert unexpected == []


def _discovery_fixture() -> dict[str, object]:
    inventory = json.loads(
        (FEC_RELEASE_CONTRACT / "inventory.json").read_text(encoding="utf-8")
    )
    observed_at = "2026-08-31T08:00:00Z"
    return {
        "schema_version": "legal-tender.fec.discovery.v1",
        "inventory_version": inventory["inventory_version"],
        "started_at": observed_at,
        "completed_at": "2026-08-31T08:00:30Z",
        "observations": [
            {
                "source_id": source["source_id"],
                "request_method": "HEAD",
                "request_url": source["request_url"],
                "final_url": source["request_url"],
                "observed_at": observed_at,
                "status": "available",
                "http_status": 200,
                "version_identity": f"etag:{source['source_id']}",
                "version_basis": "etag",
                "etag": source["source_id"],
            }
            for source in inventory["sources"]
        ],
    }


def _acquisition_fixture() -> dict[str, object]:
    inventory = json.loads(
        (FEC_RELEASE_CONTRACT / "inventory.json").read_text(encoding="utf-8")
    )
    started_at = "2026-08-31T08:02:00Z"
    completed_at = "2026-08-31T08:03:00Z"
    digest = "c" * 64
    return {
        "schema_version": "legal-tender.fec.acquisition.v1",
        "inventory_version": inventory["inventory_version"],
        "candidate_release_id": "fec-" + "a" * 64,
        "plan_sha256": "b" * 64,
        "run_id": "fake-dagster-run",
        "status": "acquired",
        "started_at": started_at,
        "completed_at": completed_at,
        "storage": {
            "schedule_a_hot_bytes_before": 0,
            "candidate_download_bytes": 21,
            "remaining_download_bytes": 21,
            "projected_schedule_a_hot_bytes": 233206938579,
            "schedule_a_hot_cap_bytes": 644245094400,
            "free_bytes_before": 1099511627776,
            "free_floor_bytes": 536870912000,
            "working_margin_bytes": 26843545600,
            "largest_extract_working_bytes": 206363392958,
            "passed": True,
        },
        "artifacts": [
            {
                "source_id": source["source_id"],
                "disposition": "acquired",
                "version_identity": f"etag:{source['source_id']}",
                "byte_count": 1,
                "sha256": digest,
                "storage_key": f"raw/fec/artifacts/sha256/cc/{digest}",
                "acquired_at": started_at,
                "container_check": "fixture",
            }
            for source in inventory["sources"]
        ],
        "post_capture": {
            "started_at": started_at,
            "completed_at": completed_at,
            "versions": [
                {
                    "source_id": source["source_id"],
                    "observed_at": started_at,
                    "status": "available",
                    "version_identity": f"etag:{source['source_id']}",
                }
                for source in inventory["sources"]
            ],
        },
        "issues": [],
    }


def _stage_fixture() -> dict[str, object]:
    started_at = "2026-08-31T08:04:00Z"
    outputs = _staged_outputs_fixture()
    return {
        "schema_version": "legal-tender.fec.staged-release.v1",
        "inventory_version": "legal-tender.fec.initial-release-inventory.v1",
        "candidate_release_id": "fec-" + "a" * 64,
        "plan_sha256": "b" * 64,
        "acquisition_sha256": "c" * 64,
        "run_id": "fake-dagster-stage-run",
        "status": "staged",
        "started_at": started_at,
        "completed_at": "2026-08-31T08:05:00Z",
        "storage": {
            "schedule_a_hot_bytes_before": 1,
            "schedule_a_hot_bytes_after": 2,
            "schedule_a_hot_cap_bytes": 644245094400,
            "free_bytes_before": 1099511627776,
            "free_bytes_after": 1099511627775,
            "free_floor_bytes": 536870912000,
            "working_margin_bytes": 26843545600,
            "largest_extract_working_bytes": 206363392958,
            "passed": True,
        },
        "outputs": outputs,
        "checks": _checks_fixture(),
        "issues": [],
    }


def _staged_outputs_fixture() -> list[dict[str, object]]:
    inventory = json.loads(
        (FEC_RELEASE_CONTRACT / "inventory.json").read_text(encoding="utf-8")
    )
    outputs: list[dict[str, object]] = []
    for source in inventory["sources"]:
        for member in source["selected_members"]:
            outputs.append(
                _staged_output(
                    source_id=source["source_id"],
                    kind="member",
                    selection=member,
                    period=source["periods"][0],
                )
            )
        for index, relation in enumerate(source["selected_relations"]):
            output = _staged_output(
                source_id=source["source_id"],
                kind="relation",
                selection=relation,
                period=source["periods"][index],
            )
            output["row_count"] = 1
            output["contracted_field_count"] = 81
            outputs.append(output)
    return outputs


def _staged_output(
    *, source_id: str, kind: str, selection: str, period: str
) -> dict[str, object]:
    digest = "d" * 64
    storage_key = f"raw/fec/selected/sha256/dd/{digest}.zst"
    if kind == "relation":
        storage_key = f"raw/fec/schedule-a/extracts/sha256/dd/{digest}.copy.zst"
    return {
        "source_id": source_id,
        "disposition": "staged",
        "selection_kind": kind,
        "selection": selection,
        "period": period,
        "representation": (
            "selected_member_zstd"
            if kind == "member"
            else "postgresql_copy_text_data_rows_zstd"
        ),
        "source_artifact_sha256": "c" * 64,
        "uncompressed_byte_count": 1,
        "uncompressed_sha256": "e" * 64,
        "compression": "zstd",
        "compression_level": 3,
        "compressed_byte_count": 1,
        "compressed_sha256": digest,
        "storage_key": storage_key,
        "decompression_validated": True,
        "staged_at": "2026-08-31T08:04:30Z",
    }


def _checks_fixture() -> list[dict[str, object]]:
    return [
        {"id": check, "passed": True, "severity": "block", "detail": check}
        for check in (
            "input_identity",
            "source_artifact_membership",
            "selected_output_membership",
            "output_integrity",
            "storage_budget",
        )
    ]


def _manifest_fixture() -> dict[str, object]:
    inventory = json.loads(
        (FEC_RELEASE_CONTRACT / "inventory.json").read_text(encoding="utf-8")
    )
    observed_at = "2026-08-31T08:00:00Z"
    artifacts = []
    for source in inventory["sources"]:
        artifacts.append(
            {
                "source_id": source["source_id"],
                "request_url": source["request_url"],
                "final_url": source["request_url"],
                "observed_at": observed_at,
                "version_identity": f"etag:{source['source_id']}",
                "version_basis": "etag",
                "etag": source["source_id"],
                "content_length": 1,
                "byte_count": 1,
                "sha256": "c" * 64,
                "storage_key": "raw/fec/artifacts/sha256/cc/" + "c" * 64,
                "acquired_at": "2026-08-31T08:02:00Z",
            }
        )
    return {
        "$schema": "release-manifest.schema.json",
        "schema_version": "legal-tender.fec.release.v1",
        "inventory_version": inventory["inventory_version"],
        "release_id": "fec-" + "a" * 64,
        "run_id": "fake-dagster-publish-run",
        "plan_sha256": "b" * 64,
        "acquisition_sha256": "c" * 64,
        "stage_sha256": "d" * 64,
        "state": "published",
        "selected_at": "2026-08-31T08:01:00Z",
        "published_at": "2026-08-31T08:06:00Z",
        "periods": inventory["periods"],
        "artifacts": artifacts,
        "staged_outputs": _staged_outputs_fixture(),
        "checks": _checks_fixture(),
    }


def _occurrence_fixture() -> dict[str, object]:
    def artifact(kind: str, digest: str, records: int) -> dict[str, object]:
        return {
            "record_count": records,
            "uncompressed_byte_count": records,
            "uncompressed_sha256": digest,
            "compressed_byte_count": 1,
            "compressed_sha256": digest,
            "compression": "zstd",
            "storage_key": (
                f"evidence/fec/schedule-a/compact/{kind}/sha256/{digest[:2]}/"
                f"{digest}.jsonl.zst"
            ),
        }

    def index(digest: str) -> dict[str, object]:
        return {
            "record_count": 1,
            "record_bytes": 48,
            "uncompressed_byte_count": 48,
            "uncompressed_sha256": digest,
            "compressed_byte_count": 1,
            "compressed_sha256": digest,
            "compression": "zstd",
            "encoding": (
                "uint64-be-sub-id,uint64-be-row-ordinal,sha256-semantic-digest"
            ),
            "storage_key": (
                "evidence/fec/schedule-a/compact/key-index/sha256/11/"
                + digest
                + ".bin.zst"
            ),
        }

    return {
        "$schema": "manifest.schema.json",
        "schema_version": (
            "legal-tender.fec.schedule-a-compact-occurrence-set.v1"
        ),
        "occurrence_set_id": "f" * 64,
        "source_release_id": "fec-" + "a" * 64,
        "source_release_manifest_sha256": "e" * 64,
        "source_artifact_sha256": "c" * 64,
        "staged_output_sha256": "d" * 64,
        "relation": "disclosure.fec_fitem_sched_a_2025_2026",
        "cycle": "2026",
        "run_id": "fake-occurrence-run",
        "state": "published",
        "parser_version": "legal-tender.fec.schedule-a-occurrence-parser.v2",
        "semantic_schema_version": "legal-tender.fec.schedule-a-semantic-digest.v1",
        "publisher_version": (
            "legal-tender.fec.schedule-a-compact-occurrence-publisher.v1"
        ),
        "index_schema_version": "legal-tender.fec.schedule-a-compact-key-index.v1",
        "occurrence_encoding": "dense-source-row-ordinal-v1",
        "change_mode": "bootstrap-dense-membership",
        "published_at": "2026-08-31T08:07:00Z",
        "configuration": {
            "partitions": 1,
            "partition_hash": "fnv1a32-sub-id-decimal-mod-v1",
            "index_record_bytes": 48,
            "index_encoding": (
                "uint64-be-sub-id,uint64-be-row-ordinal,sha256-semantic-digest"
            ),
        },
        "counts": {
            "total": 1,
            "valid": 1,
            "invalid": 0,
            "keyed": 1,
            "unique_keys": 1,
            "invalid_keys": 0,
            "duplicate_keys": 0,
            "duplicate_occurrences": 0,
        },
        "changes": {
            "added": 0,
            "changed": 0,
            "absent": 0,
            "unchanged": 0,
            "invalid": 0,
        },
        "source_replay": {
            "rows": 1,
            "compressed_bytes": 1,
            "compressed_sha256": "5" * 64,
            "uncompressed_bytes": 1,
            "uncompressed_sha256": "6" * 64,
        },
        "index_partitions": [
            {
                "partition": 0,
                "first_sub_id": "1",
                "last_sub_id": "1",
                "index": index("1" * 64),
                "key_exceptions": artifact("key-exceptions", "2" * 64, 0),
            }
        ],
        "row_exceptions": artifact("row-exceptions", "3" * 64, 0),
        "deltas": artifact("deltas", "4" * 64, 0),
        "checks": [
            {"id": check, "passed": True, "severity": "block", "detail": check}
            for check in (
                "source_release_lineage",
                "staged_output_integrity",
                "occurrence_conservation",
                "row_state_conservation",
                "key_index",
                "semantic_change_set",
            )
        ],
    }


def _classic_occurrence_fixture() -> dict[str, object]:
    def artifact(kind: str, digest: str, records: int) -> dict[str, object]:
        return {
            "record_count": records,
            "uncompressed_byte_count": records,
            "uncompressed_sha256": digest,
            "compressed_byte_count": 1,
            "compressed_sha256": digest,
            "compression": "zstd",
            "storage_key": (
                "evidence/fec/classic/candidate-master/"
                f"{kind}/sha256/{digest[:2]}/{digest}.jsonl.zst"
            ),
        }

    return {
        "$schema": "manifest.schema.json",
        "schema_version": "legal-tender.fec.classic-occurrence-set.v1",
        "occurrence_set_id": "9" * 64,
        "dataset": "candidate-master",
        "source_contract": "fec/candidate-master@1.0.0",
        "source_id": "fec:cn:2024",
        "source_release_id": "fec-" + "a" * 64,
        "source_release_manifest_sha256": "e" * 64,
        "source_artifact_sha256": "c" * 64,
        "staged_output_sha256": "d" * 64,
        "member": "cn.txt",
        "cycle": "2024",
        "run_id": "fake-classic-occurrence-run",
        "state": "published",
        "parser_version": "legal-tender.fec.classic-occurrence-parser.v2",
        "semantic_schema_version": "legal-tender.fec.classic-semantic-digest.v1",
        "published_at": "2026-08-31T08:07:00Z",
        "counts": {
            "total": 1,
            "valid": 1,
            "invalid": 0,
            "keyed": 1,
            "unique_keys": 1,
            "invalid_keys": 0,
            "duplicate_keys": 0,
            "duplicate_occurrences": 0,
        },
        "changes": {
            "added": 1,
            "changed": 0,
            "absent": 0,
            "unchanged": 0,
            "invalid": 0,
        },
        "artifacts": {
            "occurrences": artifact("occurrences", "1" * 64, 1),
            "issues": artifact("issues", "2" * 64, 0),
            "natural_index": artifact("natural-index", "3" * 64, 1),
            "changes": artifact("changes", "4" * 64, 1),
        },
        "checks": [
            {"id": check, "passed": True, "severity": "block", "detail": check}
            for check in (
                "source_release_lineage",
                "staged_output_integrity",
                "occurrence_conservation",
                "row_state_conservation",
                "natural_key_index",
                "semantic_change_set",
            )
        ]
        + [
            {
                "id": "projection_readiness",
                "passed": True,
                "severity": "warning",
                "detail": "ready",
            }
        ],
    }


def _schedule_b_fact_fixture() -> dict[str, object]:
    return {
        "$schema": "manifest.schema.json",
        "schema_version": "legal-tender.fec.schedule-b-columnar-fact-set.v1",
        "fact_set_id": "1" * 64,
        "fact_type": "fec.schedule_b_disbursement.v1",
        "cycle": "2024",
        "relation": "disclosure.fec_fitem_sched_b_2023_2024",
        "source_contract": "fec/schedule-b@1.0.0",
        "source_release_id": "fec-" + "2" * 64,
        "source_release_manifest_sha256": "3" * 64,
        "source_artifact_sha256": "4" * 64,
        "source_artifact_byte_count": 10,
        "source_artifact_storage_key": "raw/fec/schedule-b/artifacts/sha256/44/" + "4" * 64,
        "run_id": "fake-schedule-b-run",
        "state": "published",
        "physical_schema_version": "legal-tender.fec.schedule-b-parquet.v1",
        "publisher_version": "legal-tender.fec.schedule-b-columnar-publisher.v1",
        "parquet_library": "github.com/parquet-go/parquet-go@v0.32.0",
        "published_at": "2026-09-04T12:00:00Z",
        "configuration": {
            "rows_per_shard": 1,
            "rows_per_row_group": 1,
            "column_count": 98,
            "compression": "parquet-zstd",
            "locator": "source locator",
        },
        "counts": {
            "source_rows": 1,
            "facts": 1,
            "valid_facts": 1,
            "invalid_source_rows": 0,
            "unique_sub_ids": 1,
            "duplicate_sub_ids": 0,
        },
        "source_replay": {
            "archive_bytes": 10,
            "archive_sha256": "4" * 64,
            "copy_rows": 1,
            "copy_bytes": 5,
            "copy_sha256": "5" * 64,
            "fact_semantic_sha256": "6" * 64,
        },
        "shards": [
            {
                "index": 0,
                "first_source_row_ordinal": 1,
                "last_source_row_ordinal": 1,
                "first_raw_byte_offset": 0,
                "last_raw_byte_end": 5,
                "source_rows": 1,
                "facts": 1,
                "row_groups": 1,
                "bytes": 10,
                "sha256": "7" * 64,
                "semantic_sha256": "8" * 64,
                "storage_key": "facts/fec/schedule-b/columnar/shards/sha256/77/" + "7" * 64 + ".parquet",
            }
        ],
        "checks": [
            {"id": f"check_{index}", "passed": True, "severity": "block", "detail": "fixture"}
            for index in range(8)
        ],
    }


def _schedule_e_occurrence_fixture() -> dict[str, object]:
    return {
        "$schema": "manifest.schema.json",
        "schema_version": "legal-tender.fec.schedule-e-occurrence-set.v1",
        "occurrence_set_id": "2" * 64,
        "source_release_id": "fec-" + "a" * 64,
        "source_release_manifest_sha256": "e" * 64,
        "source_artifact_sha256": "c" * 64,
        "staged_output_sha256": "d" * 64,
        "relation": "disclosure.fec_fitem_sched_e",
        "cycle": "2024",
        "run_id": "fake-schedule-e-occurrence-run",
        "state": "published",
        "parser_version": "legal-tender.fec.schedule-e-occurrence-parser.v1",
        "published_at": "2026-08-31T08:07:00Z",
        "counts": {
            "source_rows": 3,
            "selected_rows": 1,
            "other_cycle_rows": 2,
            "null_cycle_rows": 0,
        },
        "occurrences": {
            "record_count": 1,
            "uncompressed_byte_count": 1,
            "uncompressed_sha256": "6" * 64,
            "compressed_byte_count": 1,
            "compressed_sha256": "6" * 64,
            "compression": "zstd",
            "storage_key": (
                "evidence/fec/schedule-e/occurrences/sha256/66/"
                + "6" * 64
                + ".jsonl.zst"
            ),
        },
        "checks": [
            {"id": check, "passed": True, "severity": "block", "detail": check}
            for check in (
                "source_release_lineage",
                "staged_output_integrity",
                "source_row_conservation",
                "cycle_partition_conservation",
                "selected_occurrence_conservation",
                "submission_identity_uniqueness",
            )
        ],
    }


def _schedule_e_fact_fixture() -> dict[str, object]:
    return {
        "$schema": "manifest.schema.json",
        "schema_version": "legal-tender.fec.schedule-e-fact-set.v1",
        "fact_set_id": "3" * 64,
        "fact_type": "fec.schedule_e_independent_expenditure.v1",
        "cycle": "2024",
        "source_contract": "fec/schedule-e@1.0.0",
        "source_release_id": "fec-" + "a" * 64,
        "source_release_manifest_sha256": "e" * 64,
        "occurrence_set_id": "2" * 64,
        "occurrence_manifest_sha256": "7" * 64,
        "run_id": "fake-schedule-e-fact-run",
        "state": "published",
        "normalizer_version": "legal-tender.fec.schedule-e-normalizer.v1",
        "fact_schema_version": (
            "legal-tender.fec.schedule-e-independent-expenditure.v1"
        ),
        "published_at": "2026-08-31T08:08:00Z",
        "counts": {
            "source_occurrences": 1,
            "facts": 1,
            "valid_facts": 1,
            "invalid_facts": 0,
        },
        "facts": {
            "record_count": 1,
            "uncompressed_byte_count": 1,
            "uncompressed_sha256": "8" * 64,
            "compressed_byte_count": 1,
            "compressed_sha256": "8" * 64,
            "compression": "zstd",
            "storage_key": (
                "facts/fec/schedule-e/facts/sha256/88/"
                + "8" * 64
                + ".jsonl.zst"
            ),
        },
        "checks": [
            {"id": check, "passed": True, "severity": "block", "detail": check}
            for check in (
                "occurrence_lineage",
                "selected_output_integrity",
                "lossless_projection",
                "fact_state_conservation",
            )
        ],
    }


def _effective_independent_expenditure_fixture() -> dict[str, object]:
    def artifact(kind: str, digest: str, records: int) -> dict[str, object]:
        return {
            "record_count": records,
            "uncompressed_byte_count": records,
            "uncompressed_sha256": digest,
            "compressed_byte_count": 1,
            "compressed_sha256": digest,
            "compression": "zstd",
            "storage_key": (
                "calculations/fec/effective-independent-expenditures/"
                f"{kind}/sha256/{digest[:2]}/{digest}.jsonl.zst"
            ),
        }

    return {
        "$schema": "manifest.schema.json",
        "schema_version": (
            "legal-tender.fec.effective-independent-expenditure-set.v1"
        ),
        "calculation_set_id": "4" * 64,
        "calculation": "fec/effective-independent-expenditures",
        "calculation_version": "1.0.0",
        "publisher_version": (
            "legal-tender.fec.effective-independent-expenditure-publisher.v1"
        ),
        "result_schema_version": (
            "legal-tender.fec.effective-independent-expenditure-spender-candidate.v1"
        ),
        "exception_schema_version": (
            "legal-tender.fec.effective-independent-expenditure-exception.v1"
        ),
        "cycle": "2024",
        "source_release_id": "fec-" + "a" * 64,
        "input_fact_set": {
            "role": "schedule_e_independent_expenditures",
            "dataset": "schedule-e",
            "fact_type": "fec.schedule_e_independent_expenditure.v1",
            "fact_set_id": "3" * 64,
            "manifest_sha256": "5" * 64,
        },
        "run_id": "fake-effective-independent-expenditure-run",
        "state": "published",
        "published_at": "2026-08-31T08:09:00Z",
        "predicate": {
            "version": (
                "legal-tender.fec.effective-independent-expenditure-membership-predicate.v1"
            ),
            "input_fact_schema_version": (
                "legal-tender.fec.schedule-e-independent-expenditure.v1"
            ),
            "membership_identity": "Schedule E fact-set ID plus fact ID",
            "required_fields": [
                "fact_id",
                "typed_fields.expenditure.memo_code",
                "typed_fields.expenditure.amount.observation_state",
                "typed_fields.expenditure.amount.reported_minor_units",
                "typed_fields.spender.committee_id",
                "typed_fields.candidate.candidate_id",
                "typed_fields.candidate.support_oppose_code",
            ],
            "decision_order": [
                {"state": "excluded_memo", "all": ["memo_code == X"]},
                {
                    "state": "unresolved_amount",
                    "all": [
                        "memo_code != X",
                        "reported_minor_units is absent or invalid",
                    ],
                },
                {
                    "state": "included",
                    "all": [
                        "memo_code != X",
                        "reported_minor_units is an exact signed integer",
                    ],
                },
            ],
            "route_requirements": [
                "spender committee ID present",
                "candidate ID present",
                "support/oppose code is S or O",
            ],
            "exceptional_states": ["unresolved_amount", "included_unattributed"],
        },
        "decision_counts": {
            "source_facts": 1,
            "included": 1,
            "excluded_memo": 0,
            "excluded_memo_amount_unresolved": 0,
            "unresolved_amount": 0,
        },
        "route_counts": {
            "attributed": 1,
            "unattributed": 0,
            "missing_spender": 0,
            "missing_candidate": 0,
            "invalid_support_oppose": 0,
            "result_groups": 1,
        },
        "source_shape_counts": {
            "valid_facts": 1,
            "invalid_facts": 0,
            "action_add": 1,
            "action_change": 0,
            "action_no_change": 0,
            "action_terminate": 0,
            "action_null": 0,
            "action_other": 0,
            "notice_like_facts": 0,
            "missing_expenditure_type": 0,
            "transaction_id_missing": 0,
            "transaction_keyed_facts": 1,
            "distinct_transaction_keys": 1,
            "repeated_transaction_keys": 0,
            "repeated_transaction_occurrences": 0,
        },
        "amounts": {
            "included_minor_units": "125",
            "excluded_memo_minor_units": "0",
            "attributed_minor_units": "125",
            "unattributed_minor_units": "0",
        },
        "exceptions": artifact("exceptions", "6" * 64, 0),
        "results": artifact("results", "7" * 64, 1),
        "checks": [
            {"id": check, "passed": True, "severity": "block", "detail": check}
            for check in (
                "input_lineage",
                "notice_separation",
                "decision_conservation",
                "route_conservation",
                "signed_conservation",
                "result_conservation",
                "repeated_key_observation",
                "no_dense_decision_artifact",
            )
        ],
    }


def _candidate_resolution_fixture() -> dict[str, object]:
    digest = "6" * 64
    return {
        "$schema": "manifest.schema.json",
        "schema_version": (
            "legal-tender.fec.independent-expenditure-candidate-resolution-set.v1"
        ),
        "calculation_set_id": "5" * 64,
        "calculation": "fec/independent-expenditure-candidate-resolution",
        "calculation_version": "1.0.0",
        "publisher_version": (
            "legal-tender.fec.independent-expenditure-candidate-resolution-publisher.v1"
        ),
        "decision_schema_version": (
            "legal-tender.fec.independent-expenditure-candidate-resolution.v1"
        ),
        "cycle": "2024",
        "source_release_id": "fec-" + "a" * 64,
        "input_calculation": {
            "role": "effective_independent_expenditures",
            "calculation": "fec/effective-independent-expenditures",
            "calculation_version": "1.0.0",
            "calculation_set_id": "4" * 64,
            "manifest_sha256": "b" * 64,
            "schedule_e_fact_set_id": "3" * 64,
            "schedule_e_manifest_sha256": "5" * 64,
        },
        "input_candidate_fact_set": {
            "role": "cycle_candidate_master",
            "dataset": "candidate-master",
            "fact_type": "fec.candidate_assertion.v1",
            "fact_set_id": "8" * 64,
            "manifest_sha256": "c" * 64,
        },
        "run_id": "fake-candidate-resolution-run",
        "state": "published",
        "published_at": "2026-08-31T08:10:00Z",
        "method": {
            "version": (
                "legal-tender.fec.independent-expenditure-candidate-resolution-method.v2"
            ),
            "name_normalization": "exact tokens",
            "context_rules": ["president", "senate", "house", "year evidence"],
            "decision_order": [
                {
                    "state": "confirmed",
                    "method": "reported_id_exact_context",
                    "when": "confirmed",
                },
                {
                    "state": "resolved",
                    "method": "unique_exact_name_office_context",
                    "when": "unique",
                },
                {
                    "state": "ambiguous",
                    "method": "multiple_exact_name_office_context",
                    "when": "multiple",
                },
                {
                    "state": "unverified",
                    "method": "reported_id_context_unverified",
                    "when": "unverified",
                },
                {
                    "state": "unverified",
                    "method": "reported_id_insufficient_context",
                    "when": "insufficient ID",
                },
                {
                    "state": "unresolved",
                    "method": "insufficient_reported_context",
                    "when": "insufficient",
                },
                {
                    "state": "unresolved",
                    "method": "no_exact_name_office_context",
                    "when": "absent",
                },
            ],
        },
        "counts": {
            "source_effective_facts": 1,
            "candidate_facts": 1,
            "usable_candidate_facts": 1,
            "confirmed": 1,
            "resolved": 0,
            "unverified": 0,
            "ambiguous": 0,
            "unresolved": 0,
        },
        "amounts": {
            "source_effective_minor_units": "125",
            "confirmed_minor_units": "125",
            "resolved_minor_units": "0",
            "unverified_minor_units": "0",
            "ambiguous_minor_units": "0",
            "unresolved_minor_units": "0",
        },
        "decisions": {
            "record_count": 1,
            "uncompressed_byte_count": 1,
            "uncompressed_sha256": digest,
            "compressed_byte_count": 1,
            "compressed_sha256": digest,
            "compression": "zstd",
            "storage_key": (
                "calculations/fec/independent-expenditure-candidate-resolution/"
                f"decisions/sha256/{digest[:2]}/{digest}.jsonl.zst"
            ),
        },
        "checks": [
            {"id": check, "passed": True, "severity": "block", "detail": check}
            for check in (
                "input_lineage",
                "cycle_release_coherence",
                "effective_membership_replay",
                "decision_conservation",
                "signed_conservation",
                "no_silent_resolution",
                "unresolved_preservation",
            )
        ],
    }


def _resolved_independent_expenditure_fixture() -> dict[str, object]:
    def artifact(kind: str, digest: str, records: int) -> dict[str, object]:
        return {
            "record_count": records,
            "uncompressed_byte_count": records,
            "uncompressed_sha256": digest,
            "compressed_byte_count": 1,
            "compressed_sha256": digest,
            "compression": "zstd",
            "storage_key": (
                "calculations/fec/resolved-independent-expenditures/"
                f"{kind}/sha256/{digest[:2]}/{digest}.jsonl.zst"
            ),
        }

    digest = "9" * 64
    return {
        "$schema": "manifest.schema.json",
        "schema_version": (
            "legal-tender.fec.resolved-independent-expenditure-set.v1"
        ),
        "calculation_set_id": digest,
        "calculation": "fec/resolved-independent-expenditures",
        "calculation_version": "1.0.0",
        "publisher_version": (
            "legal-tender.fec.resolved-independent-expenditure-publisher.v1"
        ),
        "result_schema_version": (
            "legal-tender.fec.resolved-independent-expenditure-spender-candidate.v1"
        ),
        "exception_schema_version": (
            "legal-tender.fec.resolved-independent-expenditure-exception.v1"
        ),
        "cycle": "2024",
        "source_release_id": "fec-" + "a" * 64,
        "input_candidate_resolution": {
            "role": "independent_expenditure_candidate_resolution",
            "calculation": "fec/independent-expenditure-candidate-resolution",
            "calculation_version": "1.0.0",
            "calculation_set_id": "5" * 64,
            "manifest_sha256": "8" * 64,
            "decision_schema_version": (
                "legal-tender.fec.independent-expenditure-candidate-resolution.v1"
            ),
            "decisions_sha256": "6" * 64,
        },
        "run_id": "fake-resolved-independent-expenditure-run",
        "state": "published",
        "published_at": "2026-08-31T08:12:00Z",
        "grouping_policy": {
            "version": (
                "legal-tender.fec.resolved-independent-expenditure-grouping-policy.v1"
            ),
            "group_by": [
                "spender_committee_id",
                "resolved_candidate_id",
                "support_oppose",
            ],
            "projected_states": ["confirmed", "resolved", "unverified"],
            "exception_states": ["ambiguous", "unresolved"],
        },
        "counts": {
            "source_decisions": 1,
            "projectable_decisions": 1,
            "unprojectable_decisions": 0,
            "confirmed": 1,
            "resolved": 0,
            "unverified": 0,
            "ambiguous": 0,
            "unresolved": 0,
            "result_groups": 1,
            "exceptions": 0,
        },
        "amounts": {
            "source_minor_units": "125",
            "projectable_minor_units": "125",
            "unprojectable_minor_units": "0",
            "confirmed_minor_units": "125",
            "resolved_minor_units": "0",
            "unverified_minor_units": "0",
            "ambiguous_minor_units": "0",
            "unresolved_minor_units": "0",
        },
        "results": artifact("resolved-ie-results", "7" * 64, 1),
        "exceptions": artifact("resolved-ie-exceptions", "8" * 64, 0),
        "checks": [
            {"id": check, "passed": True, "severity": "block", "detail": check}
            for check in (
                "input_lineage",
                "current_resolution_method",
                "decision_conservation",
                "route_conservation",
                "signed_conservation",
                "result_conservation",
                "exception_conservation",
                "quality_preservation",
            )
        ],
    }


def _schedule_a_fact_fixture() -> dict[str, object]:
    return {
        "$schema": "manifest.schema.json",
        "schema_version": "legal-tender.fec.schedule-a-columnar-fact-set.v1",
        "fact_set_id": "5" * 64,
        "fact_type": "fec.schedule_a_receipt.v1",
        "cycle": "2026",
        "source_contract": "fec/schedule-a@1.0.0",
        "source_release_id": "fec-" + "a" * 64,
        "source_release_manifest_sha256": "e" * 64,
        "occurrence_set_id": "f" * 64,
        "occurrence_manifest_sha256": "7" * 64,
        "run_id": "fake-schedule-a-fact-run",
        "state": "published",
        "normalizer_version": "legal-tender.fec.schedule-a-normalizer.v1",
        "fact_schema_version": "legal-tender.fec.schedule-a-receipt.v1",
        "physical_schema_version": "legal-tender.fec.schedule-a-parquet.v1",
        "publisher_version": "legal-tender.fec.schedule-a-columnar-publisher.v1",
        "parquet_library": "github.com/parquet-go/parquet-go@v0.32.0",
        "published_at": "2026-08-31T08:08:00Z",
        "configuration": {
            "rows_per_shard": 1000000,
            "rows_per_row_group": 128000,
            "column_count": 99,
            "compression": "parquet-zstd",
            "locator": "one-based-source-row-ordinal-and-copy-byte-range",
        },
        "counts": {
            "source_occurrences": 1,
            "facts": 1,
            "valid_facts": 1,
            "invalid_facts": 0,
            "excluded_occurrences": 0,
            "source_invalid_occurrences": 0,
            "source_duplicate_occurrences": 0,
        },
        "source_replay": {
            "rows": 1,
            "compressed_bytes": 1,
            "compressed_sha256": "6" * 64,
            "uncompressed_bytes": 1,
            "uncompressed_sha256": "7" * 64,
            "fact_semantic_sha256": "8" * 64,
        },
        "shards": [
            {
                "index": 0,
                "first_source_row_ordinal": 1,
                "last_source_row_ordinal": 1,
                "first_raw_byte_offset": 0,
                "last_raw_byte_end": 1,
                "source_rows": 1,
                "facts": 1,
                "valid_facts": 1,
                "invalid_facts": 0,
                "row_groups": 1,
                "bytes": 1,
                "sha256": "9" * 64,
                "semantic_sha256": "a" * 64,
                "storage_key": (
                    "facts/fec/schedule-a/columnar/shards/sha256/99/"
                    + "9" * 64
                    + ".parquet"
                ),
            }
        ],
        "checks": [
            {"id": check, "passed": True, "severity": "block", "detail": check}
            for check in (
                "occurrence_lineage",
                "source_replay",
                "physical_schema",
                "shard_integrity",
                "unique_fact_projection",
                "fact_state_conservation",
                "source_byte_conservation",
            )
        ],
    }


def _classic_fact_fixture() -> dict[str, object]:
    return {
        "$schema": "manifest.schema.json",
        "schema_version": "legal-tender.fec.classic-fact-set.v1",
        "fact_set_id": "8" * 64,
        "dataset": "candidate-master",
        "fact_type": "fec.candidate_assertion.v1",
        "cycle": "2024",
        "source_contract": "fec/candidate-master@1.0.0",
        "source_release_id": "fec-" + "a" * 64,
        "source_release_manifest_sha256": "e" * 64,
        "occurrence_set_id": "9" * 64,
        "occurrence_manifest_sha256": "7" * 64,
        "run_id": "fake-classic-fact-run",
        "state": "published",
        "normalizer_version": "legal-tender.fec.classic-normalizer.v1",
        "fact_schema_version": "legal-tender.fec.classic-fact.v1",
        "published_at": "2026-08-31T08:08:00Z",
        "counts": {
            "source_occurrences": 1,
            "facts": 1,
            "valid_facts": 1,
            "invalid_facts": 0,
            "excluded_occurrences": 0,
            "source_invalid_occurrences": 0,
            "source_duplicate_occurrences": 0,
        },
        "facts": {
            "record_count": 1,
            "uncompressed_byte_count": 1,
            "uncompressed_sha256": "6" * 64,
            "compressed_byte_count": 1,
            "compressed_sha256": "6" * 64,
            "compression": "zstd",
            "storage_key": (
                "facts/fec/classic/candidate-master/facts/sha256/66/"
                + "6" * 64
                + ".jsonl.zst"
            ),
        },
        "checks": [
            {"id": check, "passed": True, "severity": "block", "detail": check}
            for check in (
                "occurrence_lineage",
                "selected_output_integrity",
                "unique_projection",
                "occurrence_conservation",
                "fact_state_conservation",
            )
        ],
    }


def _fact_bundle_fixture() -> dict[str, object]:
    return {
        "$schema": "manifest.schema.json",
        "schema_version": (
            "legal-tender.fec.candidate-itemized-individual-receipts-fact-bundle.v1"
        ),
        "bundle_id": "b" * 64,
        "bundle_type": "fec/candidate-itemized-individual-receipts",
        "bundle_version": "1.0.0",
        "publisher_version": (
            "legal-tender.fec.candidate-itemized-individual-receipts-"
            "fact-bundle-publisher.v1"
        ),
        "cycle": "2026",
        "source_release_id": "fec-" + "a" * 64,
        "input_fact_sets": _receipt_input_fact_sets_fixture(),
        "run_id": "fake-fact-bundle-run",
        "state": "ready",
        "published_at": "2026-08-31T08:09:00Z",
        "counts": {
            "schedule_a_facts": 1,
            "candidate_committee_linkages": 1,
            "all_candidates_summaries": 1,
            "current_campaigns_summaries": 1,
        },
        "checks": [
            {"id": check, "passed": True, "severity": "block", "detail": check}
            for check in (
                "exact_role_set",
                "cycle_coherence",
                "source_release_coherence",
                "manifest_immutability",
                "backing_integrity",
                "calculation_readiness",
            )
        ],
    }


def _receipt_input_fact_sets_fixture() -> list[dict[str, object]]:
    return [
        {
            "role": "all_candidates_summary",
            "dataset": "all-candidates-summary",
            "fact_type": "fec.candidate_summary_all.v1",
            "fact_set_id": "1" * 64,
            "manifest_sha256": "a" * 64,
        },
        {
            "role": "candidate_committee_linkage",
            "dataset": "candidate-committee-linkage",
            "fact_type": "fec.candidate_committee_linkage.v1",
            "fact_set_id": "2" * 64,
            "manifest_sha256": "b" * 64,
        },
        {
            "role": "current_campaigns_summary",
            "dataset": "current-campaigns-summary",
            "fact_type": "fec.campaign_summary.v1",
            "fact_set_id": "3" * 64,
            "manifest_sha256": "c" * 64,
        },
        {
            "role": "schedule_a_receipts",
            "dataset": "schedule-a",
            "fact_type": "fec.schedule_a_receipt.v1",
            "fact_set_id": "5" * 64,
            "manifest_sha256": "d" * 64,
        },
    ]


def _candidate_receipt_calculation_fixture() -> dict[str, object]:
    def artifact(kind: str, digest: str, records: int) -> dict[str, object]:
        return {
            "record_count": records,
            "uncompressed_byte_count": records,
            "uncompressed_sha256": digest,
            "compressed_byte_count": 1,
            "compressed_sha256": digest,
            "compression": "zstd",
            "storage_key": (
                "calculations/fec/candidate-itemized-individual-receipts/compact/"
                f"{kind}/sha256/{digest[:2]}/{digest}.jsonl.zst"
            ),
        }

    def reconciliation(total: int) -> dict[str, int]:
        return {
            "total": total,
            "comparable": total,
            "exact": total,
            "within_five_percent": total,
            "within_ten_percent": total,
            "within_twenty_five_percent": total,
            "nonzero_difference": 0,
            "zero_summary": 0,
            "nonzero_summary": total,
            "not_comparable": 0,
        }

    return {
        "$schema": "manifest.schema.json",
        "schema_version": (
            "legal-tender.fec.candidate-itemized-individual-receipts-compact-set.v1"
        ),
        "calculation_set_id": "4" * 64,
        "calculation": "fec/candidate-itemized-individual-receipts",
        "calculation_version": "1.0.0",
        "publisher_version": (
            "legal-tender.fec.candidate-itemized-individual-receipts-"
            "compact-publisher.v1"
        ),
        "result_schema_version": (
            "legal-tender.fec.candidate-itemized-individual-receipts.v1"
        ),
        "cycle": "2026",
        "source_release_id": "fec-" + "a" * 64,
        "input_fact_sets": _receipt_input_fact_sets_fixture(),
        "run_id": "fake-candidate-receipt-calculation-run",
        "state": "published",
        "published_at": "2026-08-31T08:10:00Z",
        "predicate": {
            "version": (
                "legal-tender.fec.itemized-individual-receipt-membership-"
                "predicate.v1"
            ),
            "input_physical_schema_version": (
                "legal-tender.fec.schedule-a-parquet.v1"
            ),
            "membership_identity": (
                "columnar fact-set ID plus one-based source row ordinal"
            ),
            "required_columns": [
                "lt_source_row_ordinal",
                "cmte_id",
                "contb_receipt_dt",
                "lt_receipt_date",
                "is_individual",
                "lt_memoed_subtotal",
                "lt_receipt_amount_minor_units",
                "lt_receipt_amount_state",
                "lt_normalization_state",
            ],
            "decision_order": [
                {"state": "unresolved_individual_class", "all": ["null class"]},
                {"state": "excluded_non_individual", "all": ["false class"]},
                {"state": "excluded_memo_subtotal", "all": ["memo subtotal"]},
                {"state": "unresolved_amount", "all": ["invalid amount"]},
                {"state": "included", "all": ["accepted amount"]},
            ],
            "exceptional_states": [
                "unresolved_individual_class",
                "unresolved_amount",
                "invalid_receipt_date",
            ],
        },
        "decision_counts": {
            "included": 1,
            "excluded_non_individual": 0,
            "excluded_memo_subtotal": 0,
            "unresolved_individual_class": 0,
            "unresolved_amount": 0,
            "included_amount_minor_units": "5000",
        },
        "result_counts": {
            "source_rows": 1,
            "validated_rows": 1,
            "candidate_routed_rows": 1,
            "rows_without_candidate_route": 0,
            "routed_invalid_receipt_dates": 0,
            "linkage_facts": 1,
            "summary_facts": 2,
            "candidates": 1,
            "complete_candidates": 1,
            "partial_candidates": 0,
            "not_comparable_candidates": 0,
        },
        "reconciliations": {
            "overall": reconciliation(2),
            "by_summary_fact_type": {
                "fec.candidate_summary_all.v1": reconciliation(1),
                "fec.campaign_summary.v1": reconciliation(1),
            },
        },
        "exceptions": artifact("exceptions", "6" * 64, 0),
        "results": artifact("results", "7" * 64, 1),
        "checks": [
            {"id": check, "passed": True, "severity": "block", "detail": check}
            for check in (
                "input_lineage",
                "columnar_integrity",
                "decision_conservation",
                "exception_conservation",
                "route_conservation",
                "candidate_conservation",
                "result_conservation",
                "no_dense_decision_artifact",
            )
        ],
    }


def _receiver_flow_projection_bundle_fixture() -> dict[str, object]:
    return {
        "$schema": "manifest.schema.json",
        "schema_version": (
            "legal-tender.fec.receiver-reported-committee-flow-"
            "projection-bundle.v1"
        ),
        "bundle_id": "a" * 64,
        "bundle_type": "fec/receiver-reported-committee-flow-projection",
        "bundle_version": "1.0.0",
        "publisher_version": (
            "legal-tender.fec.receiver-reported-committee-flow-projection-"
            "bundle-publisher.v1"
        ),
        "cycle": "2024",
        "source_release_id": "fec-" + "1" * 64,
        "input_calculation": {
            "role": "receiver_reported_committee_flows",
            "calculation": "fec/receiver-reported-committee-flows",
            "calculation_version": "1.0.0",
            "calculation_set_id": "2" * 64,
            "manifest_sha256": "3" * 64,
            "schedule_a_fact_set_id": "4" * 64,
            "schedule_a_manifest_sha256": "5" * 64,
        },
        "input_fact_sets": [
            {
                "role": "committee_master",
                "dataset": "committee-master",
                "fact_type": "fec.committee_assertion.v1",
                "fact_set_id": "6" * 64,
                "manifest_sha256": "7" * 64,
            }
        ],
        "run_id": "fixture-run",
        "state": "ready",
        "published_at": "2026-09-01T00:00:00Z",
        "counts": {"calculation_results": 1, "committee_facts": 2},
        "checks": [
            {
                "id": f"check_{index}",
                "passed": True,
                "severity": "block",
                "detail": "fixture",
            }
            for index in range(6)
        ],
    }


def _arango_receiver_flow_fixture() -> dict[str, object]:
    counts = {
        "entities": 2,
        "present_committee_masters": 2,
        "missing_committee_masters": 0,
        "edges": 1,
        "registered_filer_contribution_edges": 1,
        "in_kind_contribution_edges": 0,
        "affiliated_transfer_in_edges": 0,
        "refund_repayment_received_edges": 0,
    }
    amounts = {
        "total_minor_units": "125",
        "registered_filer_contribution_minor_units": "125",
        "in_kind_contribution_minor_units": "0",
        "affiliated_transfer_in_minor_units": "0",
        "refund_repayment_received_minor_units": "0",
    }
    query = {
        "repetitions": 3,
        "result_rows": 1,
        "minimum_micros": 1,
        "median_micros": 2,
        "p95_micros": 3,
        "maximum_micros": 3,
    }
    return {
        "schema_version": (
            "legal-tender.arango.receiver-reported-committee-flows-"
            "probe-result.v1"
        ),
        "projection_version": (
            "legal-tender.arango.receiver-reported-committee-flows-"
            "projection.v1"
        ),
        "projection_id": "b" * 64,
        "state": "ready",
        "cycle": "2024",
        "database": "lt_flow_probe_2024_" + "b" * 16,
        "graph": "receiver_reported_committee_flows",
        "run_id": "fixture-run",
        "observed_at": "2026-09-01T00:00:00Z",
        "inputs": {
            "source_release_id": "fec-" + "1" * 64,
            "readiness_bundle_id": "a" * 64,
            "readiness_bundle_manifest_sha256": "8" * 64,
            "calculation_set_id": "2" * 64,
            "calculation_manifest_sha256": "3" * 64,
            "schedule_a_fact_set_id": "4" * 64,
            "schedule_a_manifest_sha256": "5" * 64,
            "committee_fact_set_id": "6" * 64,
            "committee_manifest_sha256": "7" * 64,
        },
        "expected_counts": counts,
        "observed_counts": counts,
        "expected_amounts": amounts,
        "observed_amounts": amounts,
        "topology": {
            "weak_components": 1,
            "strong_components": 2,
            "cyclic_strong_components": 0,
            "committees_in_cycles": 0,
            "representative_path_hops": 1,
            "representative_cycle_hops": 0,
        },
        "reused_projection": False,
        "missing_master_facts": {"committees": 0},
        "storage": {
            "collections": [
                {
                    "name": name,
                    "documents": documents,
                    "document_bytes": 10,
                    "index_count": 1,
                    "index_bytes": 5,
                    "cache_bytes": 0,
                }
                for name, documents in (
                    ("entities", 2),
                    ("receiver_reported_flows", 1),
                    ("projection_metadata", 1),
                )
            ],
            "document_bytes": 30,
            "index_bytes": 15,
            "combined_bytes": 45,
        },
        "representative_source_committee_id": "C00000001",
        "representative_target_committee_id": "C00000002",
        "representative_cycle_committee_id": "",
        "queries": [
            {"id": query_id, **query}
            for query_id in (
                "direction_agnostic_neighborhood",
                "ranked_paths_between_committees",
                "directed_shortest_path",
            )
        ],
        "checks": [
            {
                "id": f"check_{index}",
                "passed": True,
                "severity": "block",
                "detail": "fixture",
            }
            for index in range(9)
        ],
    }


def _receiver_committee_flow_fixture() -> dict[str, object]:
    def artifact(kind: str, digest: str, records: int) -> dict[str, object]:
        return {
            "record_count": records,
            "uncompressed_byte_count": records,
            "uncompressed_sha256": digest,
            "compressed_byte_count": 1,
            "compressed_sha256": digest,
            "compression": "zstd",
            "storage_key": (
                "calculations/fec/receiver-reported-committee-flows/"
                f"{kind}/sha256/{digest[:2]}/{digest}.jsonl.zst"
            ),
        }

    receipt_rules = [
        ("registered_filer_contribution", "included_receiver_reported_committee_flow", ["15K", "18K", "30K", "31K", "32K"]),
        ("registered_filer_in_kind_contribution", "included_receiver_reported_committee_flow", ["15Z"]),
        ("affiliated_transfer_in", "included_receiver_reported_committee_flow", ["18G", "30G", "31G", "32G"]),
        ("refund_or_repayment_received", "included_receiver_reported_committee_flow", ["20R", "20Y", "22Z"]),
        ("outbound", "excluded_outbound_receipt_role", ["24G", "24I", "24K", "24T", "24Z"]),
        ("semantic_memo", "excluded_semantic_memo_receipt_role", ["10J", "11J", "15J", "18J", "30F", "30J", "31F", "31J", "32F", "32J"]),
        ("earmarked", "excluded_earmarked_receipt_role", ["15E", "30E", "31E", "32E"]),
        ("noncommittee_receipt", "excluded_noncommittee_receipt_role", ["10", "11", "12", "16C", "30", "31", "32"]),
    ]
    decisions = {
        "source_facts": 1,
        "invalid_normalization": 0,
        "unresolved_recipient_committee_id": 0,
        "excluded_no_source_committee_id": 0,
        "unresolved_one_sided_source_committee_id": 0,
        "unresolved_conflicting_source_committee_ids": 0,
        "excluded_memo_subtotal": 0,
        "unresolved_amount": 0,
        "excluded_outbound_receipt_role": 0,
        "excluded_semantic_memo_receipt_role": 0,
        "excluded_earmarked_receipt_role": 0,
        "excluded_noncommittee_receipt_role": 0,
        "unresolved_receipt_role": 0,
        "included_receiver_reported_committee_flow": 1,
    }
    return {
        "$schema": "manifest.schema.json",
        "schema_version": "legal-tender.fec.receiver-reported-committee-flow-set.v1",
        "calculation_set_id": "6" * 64,
        "calculation": "fec/receiver-reported-committee-flows",
        "calculation_version": "1.0.0",
        "policy_version": "legal-tender.fec.receiver-reported-committee-flow-policy.v1",
        "publisher_version": "legal-tender.fec.receiver-reported-committee-flow-publisher.v1",
        "result_schema_version": "legal-tender.fec.receiver-reported-committee-flow.v1",
        "exception_schema_version": "legal-tender.fec.receiver-reported-committee-flow-exception.v1",
        "cycle": "2024",
        "source_release_id": "fec-" + "a" * 64,
        "input_fact_set": {
            "role": "schedule_a_receipts",
            "dataset": "schedule-a",
            "fact_type": "fec.schedule_a_receipt.v1",
            "fact_set_id": "3" * 64,
            "manifest_sha256": "5" * 64,
            "physical_schema_version": "legal-tender.fec.schedule-a-parquet.v1",
        },
        "run_id": "fake-receiver-flow-run",
        "state": "published",
        "published_at": "2026-09-01T00:00:00Z",
        "predicate": {
            "version": "legal-tender.fec.receiver-reported-committee-flow-policy.v1",
            "input_physical_schema_version": "legal-tender.fec.schedule-a-parquet.v1",
            "membership_identity": "Schedule A fact-set ID plus one-based source row ordinal",
            "required_columns": [
                "lt_source_row_ordinal", "lt_normalization_state", "cmte_id",
                "contbr_id", "clean_contbr_id", "lt_memoed_subtotal",
                "lt_receipt_amount_state", "lt_receipt_amount_minor_units",
                "receipt_tp",
            ],
            "decision_order": [
                {"state": state, "all": [state]}
                for state in (
                    "invalid_normalization",
                    "unresolved_recipient_committee_id",
                    "excluded_no_source_committee_id",
                    "unresolved_one_sided_source_committee_id",
                    "unresolved_conflicting_source_committee_ids",
                    "excluded_memo_subtotal",
                    "unresolved_amount",
                    "receipt_type_role_decision",
                )
            ],
            "receipt_type_rules": [
                {"role": role, "decision": decision, "codes": codes}
                for role, decision, codes in receipt_rules
            ],
            "exceptional_states": [
                "invalid_normalization",
                "unresolved_recipient_committee_id",
                "unresolved_one_sided_source_committee_id",
                "unresolved_conflicting_source_committee_ids",
                "unresolved_amount",
                "unresolved_receipt_role",
            ],
            "excluded_states": [
                "excluded_no_source_committee_id",
                "excluded_memo_subtotal",
                "excluded_outbound_receipt_role",
                "excluded_semantic_memo_receipt_role",
                "excluded_earmarked_receipt_role",
                "excluded_noncommittee_receipt_role",
            ],
        },
        "decision_counts": decisions,
        "result_counts": {
            "known_amount_rows": 1,
            "unknown_amount_rows": 0,
            "included_rows": 1,
            "included_positive_rows": 1,
            "included_negative_rows": 0,
            "included_zero_rows": 0,
            "result_groups": 1,
            "source_committees": 1,
            "recipient_committees": 1,
            "self_edge_rows": 0,
            "self_edge_groups": 0,
        },
        "amounts": {
            "known_source_minor_units": "125",
            "included_minor_units": "125",
            "excluded_minor_units": "0",
            "unresolved_minor_units": "0",
        },
        "exceptions": artifact("exceptions", "7" * 64, 0),
        "results": artifact("results", "8" * 64, 1),
        "checks": [
            {"id": f"check_{index}", "passed": True, "severity": "block", "detail": "fixture"}
            for index in range(9)
        ],
    }


def _write_fake_binary(tmp_path: Path) -> Path:
    binary_path = tmp_path / "legal-tender-fake"
    binary_path.write_text(
        """#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys

arguments = sys.argv[1:]
with Path(os.environ["FAKE_ARGUMENTS_LOG"]).open("a", encoding="utf-8") as log:
    log.write(json.dumps(arguments) + "\\n")
if arguments == ["pipeline", "fec", "discover"]:
    result_path = Path(os.environ["FAKE_DISCOVERY_PATH"])
elif arguments[:3] == ["pipeline", "fec", "plan-release"]:
    observation_index = arguments.index("--observations") + 1
    if not Path(arguments[observation_index]).is_file():
        sys.stderr.write("saved observations are missing\\n")
        raise SystemExit(1)
    result_path = Path(os.environ["FAKE_PLAN_PATH"])
elif arguments[:3] == ["pipeline", "fec", "acquire"]:
    plan_index = arguments.index("--plan") + 1
    if not Path(arguments[plan_index]).is_file():
        sys.stderr.write("saved release plan is missing\\n")
        raise SystemExit(1)
    result_path = Path(os.environ["FAKE_ACQUISITION_PATH"])
elif arguments[:3] == ["pipeline", "fec", "stage-release"]:
    result_path = Path(os.environ["FAKE_STAGE_PATH"])
elif arguments[:3] == ["pipeline", "fec", "publish-release"]:
    result_path = Path(os.environ["FAKE_MANIFEST_PATH"])
elif arguments[:3] == ["pipeline", "fec", "publish-schedule-a-compact-occurrences"]:
    result_path = Path(os.environ["FAKE_OCCURRENCE_PATH"])
elif arguments[:3] == ["pipeline", "fec", "publish-schedule-a-columnar-facts"]:
    result_path = Path(os.environ["FAKE_SCHEDULE_FACT_PATH"])
elif arguments[:3] == ["pipeline", "fec", "publish-schedule-b-columnar-facts"]:
    result_path = Path(os.environ["FAKE_SCHEDULE_B_FACT_PATH"])
elif arguments[:3] == ["pipeline", "fec", "publish-schedule-e-occurrences"]:
    result_path = Path(os.environ["FAKE_SCHEDULE_E_OCCURRENCE_PATH"])
elif arguments[:3] == ["pipeline", "fec", "publish-schedule-e-facts"]:
    result_path = Path(os.environ["FAKE_SCHEDULE_E_FACT_PATH"])
elif arguments[:3] == ["pipeline", "fec", "publish-effective-independent-expenditures"]:
    result_path = Path(os.environ["FAKE_EFFECTIVE_IE_PATH"])
elif arguments[:3] == ["pipeline", "fec", "publish-independent-expenditure-candidate-resolution"]:
    result_path = Path(os.environ["FAKE_IE_CANDIDATE_RESOLUTION_PATH"])
elif arguments[:3] == ["pipeline", "fec", "publish-resolved-independent-expenditures"]:
    result_path = Path(os.environ["FAKE_RESOLVED_IE_PATH"])
elif arguments[:3] == ["pipeline", "fec", "publish-independent-expenditure-projection-bundle"]:
    result_path = Path(os.environ["FAKE_IE_PROJECTION_BUNDLE_PATH"])
elif arguments[:3] == ["pipeline", "fec", "publish-resolved-independent-expenditure-projection-bundle"]:
    result_path = Path(os.environ["FAKE_RESOLVED_IE_PROJECTION_BUNDLE_PATH"])
elif arguments[:3] == ["pipeline", "fec", "probe-arango-independent-expenditures"]:
    result_path = Path(os.environ["FAKE_ARANGO_IE_PATH"])
elif arguments[:3] == ["pipeline", "fec", "probe-arango-resolved-independent-expenditures"]:
    result_path = Path(os.environ["FAKE_ARANGO_RESOLVED_IE_PATH"])
elif arguments[:3] == ["pipeline", "fec", "publish-candidate-itemized-receipts-fact-bundle"]:
    result_path = Path(os.environ["FAKE_FACT_BUNDLE_PATH"])
elif arguments[:3] == ["pipeline", "fec", "publish-candidate-itemized-receipts-compact"]:
    result_path = Path(os.environ["FAKE_CALCULATION_PATH"])
elif arguments[:3] == ["pipeline", "fec", "publish-receiver-committee-flows"]:
    result_path = Path(os.environ["FAKE_RECEIVER_FLOW_PATH"])
elif arguments[:3] == ["pipeline", "fec", "publish-receiver-committee-flow-projection-bundle"]:
    result_path = Path(os.environ["FAKE_RECEIVER_FLOW_PROJECTION_BUNDLE_PATH"])
elif arguments[:3] == ["pipeline", "fec", "probe-arango-receiver-committee-flows"]:
    result_path = Path(os.environ["FAKE_ARANGO_RECEIVER_FLOW_PATH"])
elif arguments[:3] == ["pipeline", "fec", "publish-classic-occurrences"]:
    result_path = Path(os.environ["FAKE_CLASSIC_OCCURRENCE_PATH"])
elif arguments[:3] == ["pipeline", "fec", "publish-classic-facts"]:
    result_path = Path(os.environ["FAKE_CLASSIC_FACT_PATH"])
else:
    sys.stderr.write("unexpected arguments\\n")
    raise SystemExit(2)
sys.stdout.buffer.write(result_path.read_bytes())
""",
        encoding="utf-8",
    )
    binary_path.chmod(0o755)
    return binary_path
