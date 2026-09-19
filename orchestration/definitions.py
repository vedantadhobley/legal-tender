"""Loadable Dagster definitions for the Legal Tender Go rewrite."""

from __future__ import annotations

import os
from pathlib import Path

from dagster import (
    AssetKey,
    AssetSelection,
    AutomationConditionSensorDefinition,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    FilesystemIOManager,
    MultiPartitionKey,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    asset_sensor,
    define_asset_job,
)

from orchestration.assets import (
    FEC_CLASSIC_DATASETS,
    arango_independent_expenditures,
    arango_receiver_reported_committee_flows,
    arango_resolved_independent_expenditures,
    fec_candidate_itemized_receipts,
    fec_candidate_receipt_fact_bundle,
    fec_classic_facts,
    fec_classic_occurrences,
    fec_cycle_partitions,
    fec_effective_independent_expenditures,
    fec_independent_expenditure_candidate_resolution,
    fec_independent_expenditure_projection_bundle,
    fec_receiver_committee_flow_projection_bundle,
    fec_receiver_reported_committee_flows,
    fec_release_acquisition,
    fec_release_candidate,
    fec_release_discovery,
    fec_release_publication,
    fec_release_stage,
    fec_resolved_independent_expenditure_projection_bundle,
    fec_resolved_independent_expenditures,
    fec_schedule_a_facts,
    fec_schedule_a_occurrences,
    fec_schedule_b_facts,
    fec_schedule_e_facts,
    fec_schedule_e_occurrences,
)
from orchestration.committee_summary import fec_committee_summary_facts
from orchestration.flow_evidence import (
    arango_committee_flow_evidence,
    fec_committee_flow_evidence_bundle,
    fec_committee_flow_reconciliation,
)
from orchestration.resources import GoPipelineResource

_REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
_STORAGE_ROOT = Path(os.environ.get("LEGAL_TENDER_STORAGE", "/storage"))

fec_release_planning_job = define_asset_job(
    name="fec_release_planning",
    selection=AssetSelection.assets(fec_release_discovery, fec_release_candidate),
    description="Observe all required FEC versions and construct the Go-owned release plan.",
)

fec_release_acquisition_job = define_asset_job(
    name="fec_release_acquisition_job",
    selection=AssetSelection.assets(fec_release_acquisition),
    description="Acquire exactly one update_available FEC release plan.",
)

fec_release_staging_job = define_asset_job(
    name="fec_release_staging_job",
    selection=AssetSelection.assets(fec_release_stage),
    description="Extract and verify one acquired FEC release without reacquiring it.",
)

fec_release_publication_job = define_asset_job(
    name="fec_release_publication_job",
    selection=AssetSelection.assets(fec_release_publication),
    description="Atomically publish one fully staged FEC release.",
)

fec_schedule_a_occurrence_job = define_asset_job(
    name="fec_schedule_a_occurrence_job",
    selection=AssetSelection.assets(fec_schedule_a_occurrences, fec_schedule_a_facts),
    description="Publish compact Schedule A evidence and lossless columnar facts for one cycle.",
)

fec_schedule_b_fact_job = define_asset_job(
    name="fec_schedule_b_fact_job",
    selection=AssetSelection.assets(fec_schedule_b_facts),
    description="Publish lossless Schedule B Parquet facts directly from one release-owned cycle relation.",
)

fec_committee_summary_fact_job = define_asset_job(
    name="fec_committee_summary_fact_job",
    selection=AssetSelection.assets(fec_committee_summary_facts),
    description="Manually publish one summary cycle from its immutable upstream release; no acquisition or automation.",
)

fec_schedule_e_occurrence_job = define_asset_job(
    name="fec_schedule_e_occurrence_job",
    selection=AssetSelection.assets(fec_schedule_e_occurrences, fec_schedule_e_facts),
    description="Publish lossless Schedule E occurrence evidence and facts for one cycle.",
)

fec_effective_independent_expenditures_job = define_asset_job(
    name="fec_effective_independent_expenditures_job",
    selection=AssetSelection.assets(fec_effective_independent_expenditures),
    description="Publish one cycle's effective signed Schedule E outside-spending components.",
)

fec_independent_expenditure_candidate_resolution_job = define_asset_job(
    name="fec_independent_expenditure_candidate_resolution_job",
    selection=AssetSelection.assets(fec_independent_expenditure_candidate_resolution),
    description="Resolve every effective Schedule E candidate reference against the same-release candidate master.",
)

fec_resolved_independent_expenditures_job = define_asset_job(
    name="fec_resolved_independent_expenditures_job",
    selection=AssetSelection.assets(fec_resolved_independent_expenditures),
    description="Group projectable candidate decisions and preserve ambiguous or unresolved decisions as sparse exceptions.",
)

arango_independent_expenditures_job = define_asset_job(
    name="arango_independent_expenditures_job",
    selection=AssetSelection.assets(arango_independent_expenditures),
    description="Project one effective Schedule E cycle into an isolated evidence-backed ArangoDB graph.",
)

arango_resolved_independent_expenditures_job = define_asset_job(
    name="arango_resolved_independent_expenditures_job",
    selection=AssetSelection.assets(arango_resolved_independent_expenditures),
    description="Project one resolved outside-spending cycle into a new evidence-backed ArangoDB graph.",
)

fec_independent_expenditure_projection_bundle_job = define_asset_job(
    name="fec_independent_expenditure_projection_bundle_job",
    selection=AssetSelection.assets(fec_independent_expenditure_projection_bundle),
    description="Freeze the exact calculation and master facts required by one outside-spending graph projection.",
)

fec_resolved_independent_expenditure_projection_bundle_job = define_asset_job(
    name="fec_resolved_independent_expenditure_projection_bundle_job",
    selection=AssetSelection.assets(
        fec_resolved_independent_expenditure_projection_bundle
    ),
    description="Freeze the exact resolved calculation, resolution ancestry, and master facts required by the v2 graph.",
)

fec_classic_occurrence_job = define_asset_job(
    name="fec_classic_occurrence_job",
    selection=AssetSelection.assets(fec_classic_occurrences, fec_classic_facts),
    description="Publish immutable occurrence evidence and normalized facts for one classic FEC dataset and cycle.",
)

fec_candidate_receipt_fact_bundle_job = define_asset_job(
    name="fec_candidate_receipt_fact_bundle_job",
    selection=AssetSelection.assets(fec_candidate_receipt_fact_bundle),
    description="Freeze the exact four same-release fact sets required by one receipt calculation cycle.",
)

fec_candidate_itemized_receipts_job = define_asset_job(
    name="fec_candidate_itemized_receipts_job",
    selection=AssetSelection.assets(fec_candidate_itemized_receipts),
    description=(
        "Publish one cycle's compact candidate components and summary "
        "reconciliations from one ready fact bundle."
    ),
)

fec_receiver_reported_committee_flows_job = define_asset_job(
    name="fec_receiver_reported_committee_flows_job",
    selection=AssetSelection.assets(fec_receiver_reported_committee_flows),
    description="Publish one cycle's compact receiver-reported committee-flow calculation.",
)

fec_receiver_committee_flow_projection_bundle_job = define_asset_job(
    name="fec_receiver_committee_flow_projection_bundle_job",
    selection=AssetSelection.assets(fec_receiver_committee_flow_projection_bundle),
    description=(
        "Freeze one receiver-flow calculation and its exact same-release "
        "committee master for graph publication."
    ),
)

arango_receiver_reported_committee_flows_job = define_asset_job(
    name="arango_receiver_reported_committee_flows_job",
    selection=AssetSelection.assets(arango_receiver_reported_committee_flows),
    description=(
        "Project one receiver-reported committee-flow cycle into an isolated "
        "evidence-backed ArangoDB graph."
    ),
)

_schedule_status = (
    DefaultScheduleStatus.STOPPED
    if os.environ.get("DAGSTER_SCHEDULES_ENABLED", "").lower()
    in {"0", "false", "off", "no"}
    else DefaultScheduleStatus.RUNNING
)

_sensor_status = (
    DefaultSensorStatus.STOPPED
    if _schedule_status == DefaultScheduleStatus.STOPPED
    else DefaultSensorStatus.RUNNING
)

fec_committee_flow_reconciliation_job = define_asset_job(
    name="fec_committee_flow_reconciliation_job",
    selection=AssetSelection.assets(fec_committee_flow_reconciliation),
    description="Publish a cycle's source-grain A/B candidate evidence from exact upstream manifests.",
)
fec_committee_flow_evidence_bundle_job = define_asset_job(
    name="fec_committee_flow_evidence_bundle_job",
    selection=AssetSelection.assets(fec_committee_flow_evidence_bundle),
    description="Verify and freeze the mapped calculation and same-cycle committee-master inputs.",
)
arango_committee_flow_evidence_job = define_asset_job(
    name="arango_committee_flow_evidence_job",
    selection=AssetSelection.assets(arango_committee_flow_evidence),
    description="Project an exact observation bundle with separate ledgers and complete readback.",
)
fec_committee_flow_evidence_automation_sensor = AutomationConditionSensorDefinition(
    name="fec_committee_flow_evidence_automation",
    target=AssetSelection.assets(
        fec_committee_flow_reconciliation,
        fec_committee_flow_evidence_bundle,
        arango_committee_flow_evidence,
    ),
    default_status=_sensor_status,
    description="Run the Go observation chain only after its exact mapped inputs and blocking checks are ready.",
)

fec_candidate_receipt_automation_sensor = AutomationConditionSensorDefinition(
    name="fec_candidate_receipt_automation",
    target=AssetSelection.assets(
        fec_candidate_receipt_fact_bundle,
        fec_candidate_itemized_receipts,
    ),
    default_status=_sensor_status,
    description=(
        "Materialize a same-release fact bundle only after its mapped inputs, "
        "then run the compact receipt calculation from that bundle."
    ),
)

fec_receiver_reported_committee_flow_automation_sensor = (
    AutomationConditionSensorDefinition(
        name="fec_receiver_reported_committee_flow_automation",
        target=AssetSelection.assets(fec_receiver_reported_committee_flows),
        default_status=_sensor_status,
        description=(
            "Run the receiver-reported committee-flow calculation when a "
            "cycle's Schedule A fact set changes."
        ),
    )
)

fec_receiver_committee_flow_projection_automation_sensor = (
    AutomationConditionSensorDefinition(
        name="fec_receiver_committee_flow_projection_automation",
        target=AssetSelection.assets(
            fec_receiver_committee_flow_projection_bundle,
            arango_receiver_reported_committee_flows,
        ),
        default_status=_sensor_status,
        description=(
            "Freeze the exact receiver-flow graph inputs when the calculation "
            "and committee master are ready, then project them into ArangoDB."
        ),
    )
)

fec_effective_independent_expenditure_automation_sensor = (
    AutomationConditionSensorDefinition(
        name="fec_effective_independent_expenditure_automation",
        target=AssetSelection.assets(fec_effective_independent_expenditures),
        default_status=_sensor_status,
        description=(
            "Run the compact effective independent-expenditure calculation "
            "when a cycle's Schedule E fact set changes."
        ),
    )
)

fec_independent_expenditure_candidate_resolution_automation_sensor = (
    AutomationConditionSensorDefinition(
        name="fec_independent_expenditure_candidate_resolution_automation",
        target=AssetSelection.assets(fec_independent_expenditure_candidate_resolution),
        default_status=_sensor_status,
        description=(
            "Resolve candidate references when either a cycle's effective "
            "Schedule E calculation or coordinated classic facts change."
        ),
    )
)

fec_resolved_independent_expenditure_automation_sensor = (
    AutomationConditionSensorDefinition(
        name="fec_resolved_independent_expenditure_automation",
        target=AssetSelection.assets(fec_resolved_independent_expenditures),
        default_status=_sensor_status,
        description=(
            "Regroup one cycle when its exact candidate-resolution "
            "calculation changes."
        ),
    )
)

fec_independent_expenditure_projection_automation_sensor = (
    AutomationConditionSensorDefinition(
        name="fec_independent_expenditure_projection_automation",
        target=AssetSelection.assets(
            fec_resolved_independent_expenditure_projection_bundle,
            arango_resolved_independent_expenditures,
        ),
        default_status=_sensor_status,
        description=(
            "Freeze one resolved calculation and exact master bundle when all "
            "mapped inputs are ready, then project the v2 graph into ArangoDB."
        ),
    )
)

monday_fec_release_schedule = ScheduleDefinition(
    name="monday_fec_release_planning",
    job=fec_release_planning_job,
    cron_schedule="0 4 * * 1",
    execution_timezone="America/New_York",
    default_status=_schedule_status,
)


@asset_sensor(
    name="fec_release_acquisition_sensor",
    asset_key=AssetKey("fec_release_candidate"),
    job=fec_release_acquisition_job,
    default_status=_sensor_status,
)
def fec_release_acquisition_sensor(context, asset_event):
    """Run acquisition only for an explicit update_available plan."""

    materialization = asset_event.dagster_event.event_specific_data.materialization
    return _acquisition_request_from_metadata(materialization.metadata)


def _acquisition_request_from_metadata(metadata):
    """Map one candidate materialization without reading its plan in Python."""

    status = metadata["status"].value
    if status != "update_available":
        return SkipReason(
            f"release plan status {status!r} does not authorize acquisition"
        )

    candidate_release_id = metadata["candidate_release_id"].value
    plan_path = metadata["control_artifact_path"].value
    plan_sha256 = metadata["control_artifact_sha256"].value
    return RunRequest(
        run_key=f"{candidate_release_id}:{plan_sha256}",
        run_config={
            "ops": {
                "fec_release_acquisition": {"config": {"release_plan_path": plan_path}}
            }
        },
        tags={
            "legal_tender/candidate_release_id": candidate_release_id,
            "legal_tender/release_plan_sha256": plan_sha256,
        },
    )


@asset_sensor(
    name="fec_release_staging_sensor",
    asset_key=AssetKey("fec_release_acquisition"),
    job=fec_release_staging_job,
    default_status=_sensor_status,
)
def fec_release_staging_sensor(context, asset_event):
    """Stage only an explicit acquired result and its exact plan."""

    materialization = asset_event.dagster_event.event_specific_data.materialization
    return _staging_request_from_metadata(materialization.metadata)


def _staging_request_from_metadata(metadata):
    status = metadata["status"].value
    if status != "acquired":
        return SkipReason(f"acquisition status {status!r} does not authorize staging")

    candidate_release_id = metadata["candidate_release_id"].value
    acquisition_path = metadata["control_artifact_path"].value
    acquisition_sha256 = metadata["control_artifact_sha256"].value
    plan_path = metadata["release_plan_artifact_path"].value
    return RunRequest(
        run_key=f"{candidate_release_id}:{acquisition_sha256}",
        run_config={
            "ops": {
                "fec_release_stage": {
                    "config": {
                        "release_plan_path": plan_path,
                        "acquisition_path": acquisition_path,
                    }
                }
            }
        },
        tags={
            "legal_tender/candidate_release_id": candidate_release_id,
            "legal_tender/acquisition_sha256": acquisition_sha256,
        },
    )


@asset_sensor(
    name="fec_release_publication_sensor",
    asset_key=AssetKey("fec_release_stage"),
    job=fec_release_publication_job,
    default_status=_sensor_status,
)
def fec_release_publication_sensor(context, asset_event):
    """Publish only an explicit staged result and its exact upstream evidence."""

    materialization = asset_event.dagster_event.event_specific_data.materialization
    return _publication_request_from_metadata(materialization.metadata)


def _publication_request_from_metadata(metadata):
    status = metadata["status"].value
    if status != "staged":
        return SkipReason(f"staging status {status!r} does not authorize publication")

    candidate_release_id = metadata["candidate_release_id"].value
    stage_path = metadata["control_artifact_path"].value
    stage_sha256 = metadata["control_artifact_sha256"].value
    return RunRequest(
        run_key=f"{candidate_release_id}:{stage_sha256}",
        run_config={
            "ops": {
                "fec_release_publication": {
                    "config": {
                        "release_plan_path": metadata[
                            "release_plan_artifact_path"
                        ].value,
                        "acquisition_path": metadata["acquisition_artifact_path"].value,
                        "staged_release_path": stage_path,
                    }
                }
            }
        },
        tags={
            "legal_tender/candidate_release_id": candidate_release_id,
            "legal_tender/stage_sha256": stage_sha256,
        },
    )


@asset_sensor(
    name="fec_schedule_a_occurrence_sensor",
    asset_key=AssetKey("fec_release_publication"),
    job=fec_schedule_a_occurrence_job,
    default_status=_sensor_status,
)
def fec_schedule_a_occurrence_sensor(context, asset_event):
    """Fan one published coordinated release out to its source-native cycles."""

    materialization = asset_event.dagster_event.event_specific_data.materialization
    requests = _occurrence_requests_from_metadata(materialization.metadata)
    if isinstance(requests, SkipReason):
        return requests
    periods = [request.partition_key for request in requests]
    context.instance.add_dynamic_partitions(fec_cycle_partitions.name, periods)
    return requests


def _occurrence_requests_from_metadata(metadata):
    state = metadata["state"].value
    if state != "published":
        return SkipReason(
            f"source release state {state!r} does not authorize occurrence publication"
        )

    release_id = metadata["release_id"].value
    release_path = metadata["control_artifact_path"].value
    release_sha256 = metadata["control_artifact_sha256"].value
    periods_value = metadata["periods"]
    periods = getattr(periods_value, "data", None)
    if periods is None:
        periods = periods_value.value
    return [
        RunRequest(
            run_key=f"{release_id}:{release_sha256}:{cycle}",
            partition_key=cycle,
            run_config={
                "ops": {
                    "fec_schedule_a_occurrences": {
                        "config": {"source_release_manifest_path": release_path}
                    },
                    "fec_schedule_a_facts": {
                        "config": {"source_release_manifest_path": release_path}
                    },
                }
            },
            tags={
                "legal_tender/source_release_id": release_id,
                "legal_tender/source_release_sha256": release_sha256,
                "legal_tender/fec_cycle": cycle,
            },
        )
        for cycle in periods
    ]


@asset_sensor(
    name="fec_schedule_e_occurrence_sensor",
    asset_key=AssetKey("fec_release_publication"),
    job=fec_schedule_e_occurrence_job,
    default_status=_sensor_status,
)
def fec_schedule_e_occurrence_sensor(context, asset_event):
    """Fan one published v2 release out to Schedule E cycle partitions."""

    materialization = asset_event.dagster_event.event_specific_data.materialization
    requests = _schedule_e_occurrence_requests_from_metadata(materialization.metadata)
    if isinstance(requests, SkipReason):
        return requests
    periods = [request.partition_key for request in requests]
    context.instance.add_dynamic_partitions(fec_cycle_partitions.name, periods)
    return requests


def _schedule_e_occurrence_requests_from_metadata(metadata):
    state = metadata["state"].value
    if state != "published":
        return SkipReason(
            f"source release state {state!r} does not authorize Schedule E publication"
        )
    inventory_version = metadata["inventory_version"].value
    if inventory_version not in {
        "legal-tender.fec.initial-release-inventory.v2",
        "legal-tender.fec.initial-release-inventory.v3",
        "legal-tender.fec.initial-release-inventory.v4",
    }:
        return SkipReason(
            f"source inventory {inventory_version!r} does not contain Schedule E"
        )

    release_id = metadata["release_id"].value
    release_path = metadata["control_artifact_path"].value
    release_sha256 = metadata["control_artifact_sha256"].value
    periods_value = metadata["periods"]
    periods = getattr(periods_value, "data", None)
    if periods is None:
        periods = periods_value.value
    return [
        RunRequest(
            run_key=f"{release_id}:{release_sha256}:schedule-e:{cycle}",
            partition_key=cycle,
            run_config={
                "ops": {
                    "fec_schedule_e_occurrences": {
                        "config": {"source_release_manifest_path": release_path}
                    },
                    "fec_schedule_e_facts": {
                        "config": {"source_release_manifest_path": release_path}
                    },
                }
            },
            tags={
                "legal_tender/source_release_id": release_id,
                "legal_tender/source_release_sha256": release_sha256,
                "legal_tender/fec_source": "schedule-e",
                "legal_tender/fec_cycle": cycle,
            },
        )
        for cycle in periods
    ]


@asset_sensor(
    name="fec_schedule_b_fact_sensor",
    asset_key=AssetKey("fec_release_publication"),
    job=fec_schedule_b_fact_job,
    default_status=_sensor_status,
)
def fec_schedule_b_fact_sensor(context, asset_event):
    """Fan one published v3 release out to Schedule B cycle partitions."""

    materialization = asset_event.dagster_event.event_specific_data.materialization
    requests = _schedule_b_fact_requests_from_metadata(materialization.metadata)
    if isinstance(requests, SkipReason):
        return requests
    periods = [request.partition_key for request in requests]
    context.instance.add_dynamic_partitions(fec_cycle_partitions.name, periods)
    return requests


def _schedule_b_fact_requests_from_metadata(metadata):
    state = metadata["state"].value
    if state != "published":
        return SkipReason(
            f"source release state {state!r} does not authorize Schedule B publication"
        )
    inventory_version = metadata["inventory_version"].value
    if inventory_version not in {
        "legal-tender.fec.initial-release-inventory.v3",
        "legal-tender.fec.initial-release-inventory.v4",
    }:
        return SkipReason(
            f"source inventory {inventory_version!r} does not contain Schedule B"
        )

    release_id = metadata["release_id"].value
    release_path = metadata["control_artifact_path"].value
    release_sha256 = metadata["control_artifact_sha256"].value
    periods_value = metadata["periods"]
    periods = getattr(periods_value, "data", None)
    if periods is None:
        periods = periods_value.value
    return [
        RunRequest(
            run_key=f"{release_id}:{release_sha256}:schedule-b:{cycle}",
            partition_key=cycle,
            run_config={
                "ops": {
                    "fec_schedule_b_facts": {
                        "config": {"source_release_manifest_path": release_path}
                    }
                }
            },
            tags={
                "legal_tender/source_release_id": release_id,
                "legal_tender/source_release_sha256": release_sha256,
                "legal_tender/fec_source": "schedule-b",
                "legal_tender/fec_cycle": cycle,
            },
        )
        for cycle in periods
    ]


@asset_sensor(
    name="fec_classic_occurrence_sensor",
    asset_key=AssetKey("fec_release_publication"),
    job=fec_classic_occurrence_job,
    default_status=_sensor_status,
)
def fec_classic_occurrence_sensor(context, asset_event):
    """Fan a coordinated source release out to its 20 classic member slices."""

    materialization = asset_event.dagster_event.event_specific_data.materialization
    requests = _classic_occurrence_requests_from_metadata(materialization.metadata)
    if isinstance(requests, SkipReason):
        return requests
    periods = sorted(
        {request.partition_key.keys_by_dimension["cycle"] for request in requests}
    )
    context.instance.add_dynamic_partitions(fec_cycle_partitions.name, periods)
    return requests


def _classic_occurrence_requests_from_metadata(metadata):
    state = metadata["state"].value
    if state != "published":
        return SkipReason(
            f"source release state {state!r} does not authorize classic occurrence publication"
        )

    release_id = metadata["release_id"].value
    release_path = metadata["control_artifact_path"].value
    release_sha256 = metadata["control_artifact_sha256"].value
    periods_value = metadata["periods"]
    periods = getattr(periods_value, "data", None)
    if periods is None:
        periods = periods_value.value
    return [
        RunRequest(
            run_key=f"{release_id}:{release_sha256}:{dataset}:{cycle}",
            partition_key=MultiPartitionKey({"dataset": dataset, "cycle": cycle}),
            run_config={
                "ops": {
                    "fec_classic_occurrences": {
                        "config": {
                            "source_release_manifest_path": release_path,
                            "dataset": dataset,
                            "cycle": cycle,
                        }
                    },
                    "fec_classic_facts": {
                        "config": {"source_release_manifest_path": release_path}
                    },
                }
            },
            tags={
                "legal_tender/source_release_id": release_id,
                "legal_tender/source_release_sha256": release_sha256,
                "legal_tender/fec_dataset": dataset,
                "legal_tender/fec_cycle": cycle,
            },
        )
        for cycle in periods
        for dataset in FEC_CLASSIC_DATASETS
    ]


defs = Definitions(
    assets=[
        fec_committee_summary_facts,
        fec_committee_flow_reconciliation,
        fec_committee_flow_evidence_bundle,
        arango_committee_flow_evidence,
        arango_independent_expenditures,
        arango_receiver_reported_committee_flows,
        arango_resolved_independent_expenditures,
        fec_release_discovery,
        fec_release_candidate,
        fec_release_acquisition,
        fec_release_stage,
        fec_release_publication,
        fec_classic_occurrences,
        fec_classic_facts,
        fec_schedule_a_occurrences,
        fec_schedule_a_facts,
        fec_schedule_b_facts,
        fec_schedule_e_occurrences,
        fec_schedule_e_facts,
        fec_effective_independent_expenditures,
        fec_independent_expenditure_candidate_resolution,
        fec_independent_expenditure_projection_bundle,
        fec_resolved_independent_expenditures,
        fec_resolved_independent_expenditure_projection_bundle,
        fec_candidate_receipt_fact_bundle,
        fec_candidate_itemized_receipts,
        fec_receiver_reported_committee_flows,
        fec_receiver_committee_flow_projection_bundle,
    ],
    resources={
        "io_manager": FilesystemIOManager(
            base_dir=os.environ.get(
                "LEGAL_TENDER_DAGSTER_IO",
                str(_STORAGE_ROOT / "control" / "dagster-io"),
            )
        ),
        "go_pipeline": GoPipelineResource(
            binary_path=os.environ.get(
                "LEGAL_TENDER_BINARY", "/usr/local/bin/legal-tender"
            ),
            artifact_root=os.environ.get(
                "LEGAL_TENDER_CONTROL_ARTIFACTS",
                str(_STORAGE_ROOT / "control" / "fec" / "release"),
            ),
            contracts_root=os.environ.get(
                "LEGAL_TENDER_CONTRACTS_ROOT", str(_REPOSITORY_ROOT / "contracts")
            ),
            current_fec_release_manifest=os.environ.get(
                "LEGAL_TENDER_FEC_CURRENT_RELEASE_MANIFEST",
                str(_STORAGE_ROOT / "releases" / "fec" / "current.json"),
            ),
            storage_root=str(_STORAGE_ROOT),
            timeout_seconds=int(
                os.environ.get("LEGAL_TENDER_GO_COMMAND_TIMEOUT_SECONDS", "300")
            ),
            acquisition_timeout_seconds=int(
                os.environ.get("LEGAL_TENDER_FEC_ACQUISITION_TIMEOUT_SECONDS", "86400")
            ),
            staging_timeout_seconds=int(
                os.environ.get("LEGAL_TENDER_FEC_STAGING_TIMEOUT_SECONDS", "86400")
            ),
            publication_timeout_seconds=int(
                os.environ.get("LEGAL_TENDER_FEC_PUBLICATION_TIMEOUT_SECONDS", "86400")
            ),
            occurrence_timeout_seconds=int(
                os.environ.get("LEGAL_TENDER_FEC_OCCURRENCE_TIMEOUT_SECONDS", "86400")
            ),
        ),
    },
    jobs=[
        fec_committee_summary_fact_job,
        fec_committee_flow_reconciliation_job,
        fec_committee_flow_evidence_bundle_job,
        arango_committee_flow_evidence_job,
        arango_independent_expenditures_job,
        arango_receiver_reported_committee_flows_job,
        arango_resolved_independent_expenditures_job,
        fec_release_planning_job,
        fec_release_acquisition_job,
        fec_release_staging_job,
        fec_release_publication_job,
        fec_classic_occurrence_job,
        fec_schedule_a_occurrence_job,
        fec_schedule_b_fact_job,
        fec_schedule_e_occurrence_job,
        fec_effective_independent_expenditures_job,
        fec_independent_expenditure_candidate_resolution_job,
        fec_independent_expenditure_projection_bundle_job,
        fec_resolved_independent_expenditures_job,
        fec_resolved_independent_expenditure_projection_bundle_job,
        fec_candidate_receipt_fact_bundle_job,
        fec_candidate_itemized_receipts_job,
        fec_receiver_reported_committee_flows_job,
        fec_receiver_committee_flow_projection_bundle_job,
    ],
    schedules=[monday_fec_release_schedule],
    sensors=[
        fec_committee_flow_evidence_automation_sensor,
        fec_release_acquisition_sensor,
        fec_release_staging_sensor,
        fec_release_publication_sensor,
        fec_classic_occurrence_sensor,
        fec_schedule_a_occurrence_sensor,
        fec_schedule_b_fact_sensor,
        fec_schedule_e_occurrence_sensor,
        fec_effective_independent_expenditure_automation_sensor,
        fec_independent_expenditure_candidate_resolution_automation_sensor,
        fec_resolved_independent_expenditure_automation_sensor,
        fec_independent_expenditure_projection_automation_sensor,
        fec_candidate_receipt_automation_sensor,
        fec_receiver_reported_committee_flow_automation_sensor,
        fec_receiver_committee_flow_projection_automation_sensor,
    ],
)

__all__ = [
    "arango_committee_flow_evidence_job",
    "arango_independent_expenditures_job",
    "arango_receiver_reported_committee_flows_job",
    "arango_resolved_independent_expenditures_job",
    "defs",
    "fec_candidate_itemized_receipts_job",
    "fec_candidate_receipt_automation_sensor",
    "fec_candidate_receipt_fact_bundle_job",
    "fec_classic_occurrence_job",
    "fec_classic_occurrence_sensor",
    "fec_committee_flow_evidence_automation_sensor",
    "fec_committee_flow_evidence_bundle_job",
    "fec_committee_flow_reconciliation_job",
    "fec_committee_summary_fact_job",
    "fec_effective_independent_expenditure_automation_sensor",
    "fec_effective_independent_expenditures_job",
    "fec_independent_expenditure_candidate_resolution_automation_sensor",
    "fec_independent_expenditure_candidate_resolution_job",
    "fec_independent_expenditure_projection_automation_sensor",
    "fec_independent_expenditure_projection_bundle_job",
    "fec_receiver_committee_flow_projection_automation_sensor",
    "fec_receiver_committee_flow_projection_bundle_job",
    "fec_receiver_reported_committee_flow_automation_sensor",
    "fec_receiver_reported_committee_flows_job",
    "fec_release_acquisition_job",
    "fec_release_acquisition_sensor",
    "fec_release_planning_job",
    "fec_release_publication_job",
    "fec_release_publication_sensor",
    "fec_release_staging_job",
    "fec_release_staging_sensor",
    "fec_resolved_independent_expenditure_automation_sensor",
    "fec_resolved_independent_expenditure_projection_bundle_job",
    "fec_resolved_independent_expenditures_job",
    "fec_schedule_a_occurrence_job",
    "fec_schedule_a_occurrence_sensor",
    "fec_schedule_b_fact_job",
    "fec_schedule_b_fact_sensor",
    "fec_schedule_e_occurrence_job",
    "fec_schedule_e_occurrence_sensor",
    "monday_fec_release_schedule",
]
