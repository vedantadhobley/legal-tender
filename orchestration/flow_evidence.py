"""Dagster wiring for the accepted Go committee-flow observation boundary."""

import os
from pathlib import Path

from dagster import (
    AssetCheckResult,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetIn,
    AutomationCondition,
    Backoff,
    Config,
    DataVersion,
    DimensionPartitionMapping,
    Failure,
    IdentityPartitionMapping,
    MetadataValue,
    MultiPartitionMapping,
    MultiPartitionsDefinition,
    MultiToSingleDimensionPartitionMapping,
    Output,
    RetryPolicy,
    StaticPartitionMapping,
    StaticPartitionsDefinition,
    asset,
)

from orchestration.assets import _artifact_metadata, _execute, fec_cycle_partitions
from orchestration.resources import GoPipelineResource

_BUNDLE = "committee-flow-evidence"
_CHECK = "go_verified"
_RETRY = RetryPolicy(max_retries=2, delay=60, backoff=Backoff.EXPONENTIAL)
_AUTOMATION = (
    AutomationCondition.eager() & AutomationCondition.all_deps_blocking_checks_passed()
)
fec_committee_flow_evidence_partitions = MultiPartitionsDefinition(
    {
        "bundle": StaticPartitionsDefinition([_BUNDLE]),
        "cycle": fec_cycle_partitions,
    }
)
_cycle_mapping = MultiToSingleDimensionPartitionMapping(
    partition_dimension_name="cycle"
)
_master_mapping = MultiPartitionMapping(
    {
        "cycle": DimensionPartitionMapping("cycle", IdentityPartitionMapping()),
        "dataset": DimensionPartitionMapping(
            "bundle", StaticPartitionMapping({"committee-master": _BUNDLE})
        ),
    }
)


class CommitteeFlowCalculationConfig(Config):
    workers: int = 4


class CommitteeFlowGraphConfig(Config):
    endpoint: str = (
        f"http://{os.environ.get('ARANGO_HOST', 'legal-tender-dev-arango')}:"
        f"{os.environ.get('ARANGO_PORT', '8529')}"
    )
    username: str = os.environ.get("ARANGO_USER", "root")
    batch_size: int = 5_000


def _checked(context, resource, cycle, **kwargs):
    """Forward Go success/failure as a blocking check, without domain rules."""
    try:
        result = _execute(context, resource, **kwargs)
        if result.payload["cycle"] != cycle:
            raise Failure(
                "Go result cycle differs from the requested Dagster partition"
            )
    except Failure as error:
        yield AssetCheckResult(
            passed=False,
            asset_key=context.asset_key,
            check_name=_CHECK,
            description=error.description,
        )
        raise
    return result


def _passed(context, result):
    # Attach success to this materialization, not the previous one. Dagster
    # treats a later materialization as invalidating an earlier check event.
    return AssetCheckResult(
        passed=True,
        asset_key=context.asset_key,
        check_name=_CHECK,
        metadata={"go_state": result.payload["state"], **_artifact_metadata(result)},
    )


def _output(result, identity_field, fields, value=None, extra=None):
    payload = result.payload
    metadata = {
        key: (
            MetadataValue.json(payload[key])
            if isinstance(payload[key], (dict, list))
            else payload[key]
        )
        for key in fields
    }
    metadata.update(_artifact_metadata(result))
    metadata.update(extra or {})
    return Output(
        value=value or str(result.artifact_path),
        metadata=metadata,
        data_version=DataVersion(payload[identity_field]),
    )


@asset(
    group_name="rewrite_fec_calculation",
    partitions_def=fec_cycle_partitions,
    ins={
        "fec_schedule_a_facts": AssetIn(),
        "fec_schedule_b_facts": AssetIn(),
        "fec_release_publication": AssetIn(),
    },
    check_specs=[
        AssetCheckSpec(
            _CHECK,
            asset="fec_committee_flow_reconciliation",
            blocking=True,
            partitions_def=fec_cycle_partitions,
        )
    ],
    automation_condition=_AUTOMATION,
    retry_policy=_RETRY,
    description="Publish exact A/B observation reconciliation from immutable upstream fact and release manifests.",
)
def fec_committee_flow_reconciliation(
    context: AssetExecutionContext,
    config: CommitteeFlowCalculationConfig,
    go_pipeline: GoPipelineResource,
    fec_schedule_a_facts: str,
    fec_schedule_b_facts: str,
    fec_release_publication: str,
):
    cycle = context.partition_key
    result = yield from _checked(
        context,
        go_pipeline,
        cycle,
        arguments=[
            "pipeline",
            "fec",
            "publish-committee-flow-reconciliation",
            "--storage-root",
            go_pipeline.storage_root,
            "--cycle",
            cycle,
            "--schedule-a-facts",
            fec_schedule_a_facts,
            "--schedule-b-facts",
            fec_schedule_b_facts,
            "--release",
            fec_release_publication,
            "--workers",
            str(config.workers),
        ],
        schema_name="calculations/fec/committee-flow-reconciliation/v1/result.schema.json",
        artifact_kind=f"calculations/committee-flow-reconciliation/{cycle}",
        timeout_seconds=go_pipeline.occurrence_timeout_seconds,
    )
    path = (
        Path(go_pipeline.storage_root)
        / "calculations/fec/committee-flow-reconciliation/v1/manifests"
        / (result.payload["calculation_set_id"] + ".json")
    )
    yield _output(
        result,
        "calculation_set_id",
        [
            "schema_version",
            "calculation_set_id",
            "cycle",
            "state",
            "input",
            "policy",
            "summary",
            "assertions",
            "graph_eligible",
        ],
        value=str(path),
        extra={
            "published_manifest_path": MetadataValue.path(path),
            "schedule_a_selected": MetadataValue.json(
                result.payload["schedule_a"]["selected"]
            ),
            "schedule_b_selected": MetadataValue.json(
                result.payload["schedule_b"]["selected"]
            ),
        },
    )
    yield _passed(context, result)


@asset(
    group_name="rewrite_fec_projection",
    partitions_def=fec_committee_flow_evidence_partitions,
    ins={
        "fec_committee_flow_reconciliation": AssetIn(partition_mapping=_cycle_mapping),
        "fec_classic_facts": AssetIn(partition_mapping=_master_mapping),
    },
    check_specs=[
        AssetCheckSpec(
            _CHECK,
            asset="fec_committee_flow_evidence_bundle",
            blocking=True,
            partitions_def=fec_committee_flow_evidence_partitions,
        )
    ],
    automation_condition=_AUTOMATION,
    retry_policy=_RETRY,
    description="Freeze the exact published reconciliation and mapped same-cycle committee-master input.",
)
def fec_committee_flow_evidence_bundle(
    context: AssetExecutionContext,
    go_pipeline: GoPipelineResource,
    fec_committee_flow_reconciliation: str,
    fec_classic_facts: str,
):
    keys = context.partition_key.keys_by_dimension
    if (
        keys["bundle"] != _BUNDLE
        or len(context.asset_partition_keys_for_input("fec_classic_facts")) != 1
    ):
        raise Failure(
            "evidence readiness requires exactly its mapped committee-master partition"
        )
    cycle = keys["cycle"]
    result = yield from _checked(
        context,
        go_pipeline,
        cycle,
        arguments=[
            "pipeline",
            "fec",
            "publish-committee-flow-evidence-bundle",
            "--storage-root",
            go_pipeline.storage_root,
            "--cycle",
            cycle,
            "--calculation",
            fec_committee_flow_reconciliation,
            "--committee-facts",
            fec_classic_facts,
        ],
        schema_name="bundles/fec/committee-flow-evidence/v1/manifest.schema.json",
        artifact_kind=f"bundles/committee-flow-evidence/{cycle}",
        timeout_seconds=go_pipeline.occurrence_timeout_seconds,
    )
    path = (
        Path(go_pipeline.storage_root)
        / "bundles/fec/committee-flow-evidence/v1/manifests"
        / (result.payload["bundle_id"] + ".json")
    )
    yield _output(
        result,
        "bundle_id",
        [
            "schema_version",
            "bundle_id",
            "cycle",
            "state",
            "consumer",
            "calculation",
            "input",
            "committee_master",
            "identity_scope",
            "economic_flow_eligible",
            "checks",
        ],
        value=str(path),
        extra={"published_manifest_path": MetadataValue.path(path)},
    )
    yield _passed(context, result)


@asset(
    group_name="rewrite_fec_projection",
    partitions_def=fec_cycle_partitions,
    ins={
        "fec_committee_flow_evidence_bundle": AssetIn(partition_mapping=_cycle_mapping)
    },
    check_specs=[
        AssetCheckSpec(
            _CHECK,
            asset="arango_committee_flow_evidence",
            blocking=True,
            partitions_def=fec_cycle_partitions,
        )
    ],
    automation_condition=_AUTOMATION,
    retry_policy=_RETRY,
    description="Project the exact ready observation bundle; preserve separate ledgers and partial master coverage.",
)
def arango_committee_flow_evidence(
    context: AssetExecutionContext,
    config: CommitteeFlowGraphConfig,
    go_pipeline: GoPipelineResource,
    fec_committee_flow_evidence_bundle: str,
):
    cycle = context.partition_key
    result = yield from _checked(
        context,
        go_pipeline,
        cycle,
        arguments=[
            "pipeline",
            "fec",
            "probe-arango-committee-flow-evidence",
            "--storage-root",
            go_pipeline.storage_root,
            "--cycle",
            cycle,
            "--projection-bundle",
            fec_committee_flow_evidence_bundle,
            "--endpoint",
            config.endpoint,
            "--username",
            config.username,
            "--batch-size",
            str(config.batch_size),
        ],
        schema_name="projections/arango/committee-flow-evidence/v1/result.schema.json",
        artifact_kind=f"projections/arango/committee-flow-evidence/{cycle}",
        timeout_seconds=go_pipeline.occurrence_timeout_seconds,
    )
    yield _output(
        result,
        "projection_id",
        [
            "schema_version",
            "projection_id",
            "database",
            "state",
            "reused",
            "cycle",
            "bundle_id",
            "bundle_sha256",
            "entities",
            "unresolved_same_cycle_masters",
            "schedule_a",
            "schedule_b",
            "components",
            "verified_source_shards",
            "queries",
            "storage",
            "checks",
            "elapsed_seconds",
            "process_peak_rss_bytes",
        ],
        extra={
            "projection_bundle_manifest_path": MetadataValue.path(
                fec_committee_flow_evidence_bundle
            )
        },
    )
    yield _passed(context, result)
