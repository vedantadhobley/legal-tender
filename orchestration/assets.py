"""Dagster assets that map opaque Go release results into observability."""

import os
from typing import Any

from dagster import (
    AssetDep,
    AssetExecutionContext,
    AssetIn,
    AutomationCondition,
    Backoff,
    Config,
    DataVersion,
    DimensionPartitionMapping,
    DynamicPartitionsDefinition,
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

from orchestration.go_process import GoCommandError, StoredCommandResult
from orchestration.resources import GoPipelineResource

_GROUP_NAME = "rewrite_fec_release"
_EVIDENCE_GROUP_NAME = "rewrite_fec_evidence"
_CALCULATION_GROUP_NAME = "rewrite_fec_calculation"
_PROJECTION_GROUP_NAME = "rewrite_fec_projection"

fec_cycle_partitions = DynamicPartitionsDefinition(name="fec_cycle")
FEC_CLASSIC_DATASETS = (
    "candidate-master",
    "committee-master",
    "candidate-committee-linkage",
    "all-candidates-summary",
    "current-campaigns-summary",
)
_RECEIPT_CLASSIC_DATASETS = (
    "candidate-committee-linkage",
    "all-candidates-summary",
    "current-campaigns-summary",
)
_RECEIPT_BUNDLE_KIND = "candidate-itemized-individual-receipts"
_IE_PROJECTION_BUNDLE_KIND = "independent-expenditure-projection"
_RESOLVED_IE_PROJECTION_BUNDLE_KIND = (
    "resolved-independent-expenditure-projection"
)
_RECEIVER_FLOW_PROJECTION_BUNDLE_KIND = (
    "receiver-reported-committee-flow-projection"
)
fec_classic_slice_partitions = MultiPartitionsDefinition(
    {
        "cycle": fec_cycle_partitions,
        "dataset": StaticPartitionsDefinition(FEC_CLASSIC_DATASETS),
    }
)
fec_candidate_receipt_bundle_partitions = MultiPartitionsDefinition(
    {
        "bundle": StaticPartitionsDefinition([_RECEIPT_BUNDLE_KIND]),
        "cycle": fec_cycle_partitions,
    }
)
fec_independent_expenditure_projection_bundle_partitions = MultiPartitionsDefinition(
    {
        "bundle": StaticPartitionsDefinition([_IE_PROJECTION_BUNDLE_KIND]),
        "cycle": fec_cycle_partitions,
    }
)
fec_resolved_independent_expenditure_projection_bundle_partitions = (
    MultiPartitionsDefinition(
        {
            "bundle": StaticPartitionsDefinition(
                [_RESOLVED_IE_PROJECTION_BUNDLE_KIND]
            ),
            "cycle": fec_cycle_partitions,
        }
    )
)
fec_receiver_committee_flow_projection_bundle_partitions = (
    MultiPartitionsDefinition(
        {
            "bundle": StaticPartitionsDefinition(
                [_RECEIVER_FLOW_PROJECTION_BUNDLE_KIND]
            ),
            "cycle": fec_cycle_partitions,
        }
    )
)
_classic_to_receipt_bundle_mapping = MultiPartitionMapping(
    {
        "cycle": DimensionPartitionMapping(
            dimension_name="cycle",
            partition_mapping=IdentityPartitionMapping(),
        ),
        "dataset": DimensionPartitionMapping(
            dimension_name="bundle",
            partition_mapping=StaticPartitionMapping(
                {dataset: _RECEIPT_BUNDLE_KIND for dataset in _RECEIPT_CLASSIC_DATASETS}
            ),
        ),
    }
)
_cycle_to_bundle_mapping = MultiToSingleDimensionPartitionMapping(
    partition_dimension_name="cycle"
)
_classic_to_ie_projection_bundle_mapping = MultiPartitionMapping(
    {
        "cycle": DimensionPartitionMapping(
            dimension_name="cycle",
            partition_mapping=IdentityPartitionMapping(),
        ),
        "dataset": DimensionPartitionMapping(
            dimension_name="bundle",
            partition_mapping=StaticPartitionMapping(
                {
                    "candidate-master": _IE_PROJECTION_BUNDLE_KIND,
                    "committee-master": _IE_PROJECTION_BUNDLE_KIND,
                }
            ),
        ),
    }
)
_cycle_to_ie_projection_bundle_mapping = MultiToSingleDimensionPartitionMapping(
    partition_dimension_name="cycle"
)
_classic_to_resolved_ie_projection_bundle_mapping = MultiPartitionMapping(
    {
        "cycle": DimensionPartitionMapping(
            dimension_name="cycle",
            partition_mapping=IdentityPartitionMapping(),
        ),
        "dataset": DimensionPartitionMapping(
            dimension_name="bundle",
            partition_mapping=StaticPartitionMapping(
                {
                    "candidate-master": _RESOLVED_IE_PROJECTION_BUNDLE_KIND,
                    "committee-master": _RESOLVED_IE_PROJECTION_BUNDLE_KIND,
                }
            ),
        ),
    }
)
_cycle_to_resolved_ie_projection_bundle_mapping = (
    MultiToSingleDimensionPartitionMapping(partition_dimension_name="cycle")
)
_classic_to_receiver_flow_projection_bundle_mapping = MultiPartitionMapping(
    {
        "cycle": DimensionPartitionMapping(
            dimension_name="cycle",
            partition_mapping=IdentityPartitionMapping(),
        ),
        "dataset": DimensionPartitionMapping(
            dimension_name="bundle",
            partition_mapping=StaticPartitionMapping(
                {
                    "committee-master": (
                        _RECEIVER_FLOW_PROJECTION_BUNDLE_KIND
                    )
                }
            ),
        ),
    }
)
_cycle_to_receiver_flow_projection_bundle_mapping = (
    MultiToSingleDimensionPartitionMapping(partition_dimension_name="cycle")
)


class FECReleaseAcquisitionConfig(Config):
    """Exact immutable release-plan artifact selected by the asset sensor."""

    release_plan_path: str


class FECReleaseStagingConfig(Config):
    """Exact release-plan and acquisition artifacts selected by the sensor."""

    release_plan_path: str
    acquisition_path: str


class FECReleasePublicationConfig(Config):
    """Exact evidence chain authorized for atomic publication."""

    release_plan_path: str
    acquisition_path: str
    staged_release_path: str


class FECScheduleAOccurrenceConfig(Config):
    """Exact published source release selected by the asset sensor."""

    source_release_manifest_path: str


class FECScheduleAFactConfig(Config):
    """Exact coordinated source release for a Schedule A occurrence projection."""

    source_release_manifest_path: str


class FECScheduleBFactConfig(Config):
    """Exact v3 coordinated release containing the Schedule B archive."""

    source_release_manifest_path: str


class FECScheduleEOccurrenceConfig(Config):
    """Exact v2 source release selected by the Schedule E asset sensor."""

    source_release_manifest_path: str


class FECScheduleEFactConfig(Config):
    """Exact coordinated source release for a Schedule E occurrence projection."""

    source_release_manifest_path: str


class FECClassicOccurrenceConfig(Config):
    """Exact source release and classic dataset slice selected by the sensor."""

    source_release_manifest_path: str
    dataset: str
    cycle: str


class FECClassicFactConfig(Config):
    """Exact coordinated source release for a classic occurrence projection."""

    source_release_manifest_path: str


class ArangoIndependentExpenditureProjectionConfig(Config):
    """Arango endpoint and bounded probe settings for graph publication."""

    endpoint: str = (
        f"http://{os.environ.get('ARANGO_HOST', 'legal-tender-dev-arango')}:"
        f"{os.environ.get('ARANGO_PORT', '8529')}"
    )
    username: str = os.environ.get("ARANGO_USER", "root")
    batch_size: int = 5_000
    query_repetitions: int = 10


@asset(
    name="fec_release_discovery",
    group_name=_GROUP_NAME,
    description="Metadata-only observation of the complete Go-owned FEC release inventory.",
)
def fec_release_discovery(
    context: AssetExecutionContext,
    go_pipeline: GoPipelineResource,
) -> Output[str]:
    result = _execute(
        context,
        go_pipeline,
        arguments=["pipeline", "fec", "discover"],
        schema_name="discovery.schema.json",
        artifact_kind="discoveries",
    )
    observations = result.payload["observations"]
    metadata: dict[str, Any] = {
        "schema_version": result.payload["schema_version"],
        "inventory_version": result.payload["inventory_version"],
        "started_at": result.payload["started_at"],
        "completed_at": result.payload["completed_at"],
        "source_count": len(observations),
        "available_source_count": sum(
            observation["status"] == "available" for observation in observations
        ),
        "unavailable_source_count": sum(
            observation["status"] == "unavailable" for observation in observations
        ),
        "source_versions": MetadataValue.json(
            [
                {
                    "source_id": observation["source_id"],
                    "status": observation["status"],
                    "version_identity": observation.get("version_identity"),
                }
                for observation in observations
            ]
        ),
        **_artifact_metadata(result),
    }
    return Output(
        value=str(result.artifact_path),
        metadata=metadata,
        data_version=DataVersion(result.sha256),
    )


@asset(
    name="fec_release_candidate",
    group_name=_GROUP_NAME,
    description="Go-owned release decision over one saved discovery and the active release.",
)
def fec_release_candidate(
    context: AssetExecutionContext,
    go_pipeline: GoPipelineResource,
    fec_release_discovery: str,
) -> Output[str]:
    arguments = [
        "pipeline",
        "fec",
        "plan-release",
        "--observations",
        fec_release_discovery,
    ]
    try:
        current_manifest = go_pipeline.current_manifest_path()
    except ValueError as error:
        raise Failure(description=str(error)) from error
    if current_manifest is not None:
        arguments.extend(["--current", str(current_manifest)])

    result = _execute(
        context,
        go_pipeline,
        arguments=arguments,
        schema_name="release-plan.schema.json",
        artifact_kind="plans",
    )
    payload = result.payload
    metadata: dict[str, Any] = {
        "schema_version": payload["schema_version"],
        "inventory_version": payload["inventory_version"],
        "status": payload["status"],
        "planned_at": payload["planned_at"],
        "changed_source_count": len(payload["changed_source_ids"]),
        "reused_source_count": len(payload["reused_source_ids"]),
        "issue_count": len(payload["issues"]),
        "changed_source_ids": MetadataValue.json(payload["changed_source_ids"]),
        "reused_source_ids": MetadataValue.json(payload["reused_source_ids"]),
        "issues": MetadataValue.json(payload["issues"]),
        "discovery_artifact_path": MetadataValue.path(fec_release_discovery),
        **_artifact_metadata(result),
    }
    for field in (
        "candidate_release_id",
        "prior_release_id",
        "discovery_started_at",
        "discovery_completed_at",
    ):
        if value := payload.get(field):
            metadata[field] = value
    if payload["selected_sources"]:
        metadata["selected_source_versions"] = MetadataValue.json(
            [
                {
                    "source_id": source["source_id"],
                    "version_identity": source["version_identity"],
                }
                for source in payload["selected_sources"]
            ]
        )

    return Output(
        value=str(result.artifact_path),
        metadata=metadata,
        data_version=DataVersion(result.sha256),
    )


@asset(
    name="fec_release_acquisition",
    group_name=_GROUP_NAME,
    deps=[fec_release_candidate],
    description="Resumable, storage-gated capture of one Go-owned update_available plan.",
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=60,
        backoff=Backoff.EXPONENTIAL,
    ),
)
def fec_release_acquisition(
    context: AssetExecutionContext,
    config: FECReleaseAcquisitionConfig,
    go_pipeline: GoPipelineResource,
) -> Output[str]:
    arguments = [
        "pipeline",
        "fec",
        "acquire",
        "--plan",
        config.release_plan_path,
        "--storage-root",
        go_pipeline.storage_root,
        "--run-id",
        context.run.run_id,
    ]
    try:
        current_manifest = go_pipeline.current_manifest_path()
    except ValueError as error:
        raise Failure(description=str(error)) from error
    if current_manifest is not None:
        arguments.extend(["--current", str(current_manifest)])

    result = _execute(
        context,
        go_pipeline,
        arguments=arguments,
        schema_name="acquisition-result.schema.json",
        artifact_kind="acquisitions",
        timeout_seconds=go_pipeline.acquisition_timeout_seconds,
    )
    payload = result.payload
    storage = payload["storage"]
    metadata: dict[str, Any] = {
        "schema_version": payload["schema_version"],
        "inventory_version": payload["inventory_version"],
        "candidate_release_id": payload["candidate_release_id"],
        "plan_sha256": payload["plan_sha256"],
        "status": payload["status"],
        "artifact_count": len(payload["artifacts"]),
        "acquired_source_count": sum(
            artifact["disposition"] == "acquired" for artifact in payload["artifacts"]
        ),
        "reused_source_count": sum(
            artifact["disposition"] == "reused" for artifact in payload["artifacts"]
        ),
        "storage_preflight": MetadataValue.json(storage),
        "source_artifacts": MetadataValue.json(
            [
                {
                    "source_id": artifact["source_id"],
                    "disposition": artifact["disposition"],
                    "sha256": artifact["sha256"],
                    "storage_key": artifact["storage_key"],
                }
                for artifact in payload["artifacts"]
            ]
        ),
        "post_capture_source_count": len(payload["post_capture"]["versions"]),
        "release_plan_artifact_path": MetadataValue.path(config.release_plan_path),
        **_artifact_metadata(result),
    }
    if prior_release_id := payload.get("prior_release_id"):
        metadata["prior_release_id"] = prior_release_id
    return Output(
        value=str(result.artifact_path),
        metadata=metadata,
        data_version=DataVersion(result.sha256),
    )


@asset(
    name="fec_release_stage",
    group_name=_GROUP_NAME,
    deps=[fec_release_acquisition],
    description="Checkpointed extraction and verification of all inventory-selected FEC data.",
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=60,
        backoff=Backoff.EXPONENTIAL,
    ),
)
def fec_release_stage(
    context: AssetExecutionContext,
    config: FECReleaseStagingConfig,
    go_pipeline: GoPipelineResource,
) -> Output[str]:
    arguments = [
        "pipeline",
        "fec",
        "stage-release",
        "--plan",
        config.release_plan_path,
        "--acquisition",
        config.acquisition_path,
        "--storage-root",
        go_pipeline.storage_root,
        "--run-id",
        context.run.run_id,
    ]
    try:
        current_manifest = go_pipeline.current_manifest_path()
    except ValueError as error:
        raise Failure(description=str(error)) from error
    if current_manifest is not None:
        arguments.extend(["--current", str(current_manifest)])

    result = _execute(
        context,
        go_pipeline,
        arguments=arguments,
        schema_name="staged-release.schema.json",
        artifact_kind="stages",
        timeout_seconds=go_pipeline.staging_timeout_seconds,
    )
    payload = result.payload
    metadata: dict[str, Any] = {
        "schema_version": payload["schema_version"],
        "inventory_version": payload["inventory_version"],
        "candidate_release_id": payload["candidate_release_id"],
        "plan_sha256": payload["plan_sha256"],
        "acquisition_sha256": payload["acquisition_sha256"],
        "status": payload["status"],
        "output_count": len(payload["outputs"]),
        "new_output_count": sum(
            output["disposition"] == "staged" for output in payload["outputs"]
        ),
        "reused_output_count": sum(
            output["disposition"] == "reused" for output in payload["outputs"]
        ),
        "storage_check": MetadataValue.json(payload["storage"]),
        "checks": MetadataValue.json(payload["checks"]),
        "release_plan_artifact_path": MetadataValue.path(config.release_plan_path),
        "acquisition_artifact_path": MetadataValue.path(config.acquisition_path),
        **_artifact_metadata(result),
    }
    if prior_release_id := payload.get("prior_release_id"):
        metadata["prior_release_id"] = prior_release_id
    return Output(
        value=str(result.artifact_path),
        metadata=metadata,
        data_version=DataVersion(result.sha256),
    )


@asset(
    name="fec_release_publication",
    group_name=_GROUP_NAME,
    deps=[fec_release_stage],
    description="Locked immutable-manifest write and atomic active-release replacement.",
    retry_policy=RetryPolicy(max_retries=3, delay=10, backoff=Backoff.EXPONENTIAL),
)
def fec_release_publication(
    context: AssetExecutionContext,
    config: FECReleasePublicationConfig,
    go_pipeline: GoPipelineResource,
) -> Output[str]:
    arguments = [
        "pipeline",
        "fec",
        "publish-release",
        "--plan",
        config.release_plan_path,
        "--acquisition",
        config.acquisition_path,
        "--stage",
        config.staged_release_path,
        "--storage-root",
        go_pipeline.storage_root,
        "--run-id",
        context.run.run_id,
    ]
    current_target = go_pipeline.current_manifest_target_path()
    if current_target is not None:
        arguments.extend(["--current", str(current_target)])

    result = _execute(
        context,
        go_pipeline,
        arguments=arguments,
        schema_name="release-manifest.schema.json",
        artifact_kind="manifests",
        timeout_seconds=go_pipeline.publication_timeout_seconds,
    )
    payload = result.payload
    metadata: dict[str, Any] = {
        "schema_version": payload["schema_version"],
        "inventory_version": payload["inventory_version"],
        "release_id": payload["release_id"],
        "state": payload["state"],
        "published_at": payload["published_at"],
        "plan_sha256": payload["plan_sha256"],
        "acquisition_sha256": payload["acquisition_sha256"],
        "stage_sha256": payload["stage_sha256"],
        "source_artifact_count": len(payload["artifacts"]),
        "staged_output_count": len(payload["staged_outputs"]),
        "periods": MetadataValue.json(payload["periods"]),
        "checks": MetadataValue.json(payload["checks"]),
        "release_plan_artifact_path": MetadataValue.path(config.release_plan_path),
        "acquisition_artifact_path": MetadataValue.path(config.acquisition_path),
        "staged_release_artifact_path": MetadataValue.path(config.staged_release_path),
        **_artifact_metadata(result),
    }
    if prior_release_id := payload.get("prior_release_id"):
        metadata["prior_release_id"] = prior_release_id
    return Output(
        value=str(result.artifact_path),
        metadata=metadata,
        data_version=DataVersion(result.sha256),
    )


@asset(
    name="fec_schedule_a_occurrences",
    group_name=_EVIDENCE_GROUP_NAME,
    deps=[fec_release_publication],
    partitions_def=fec_cycle_partitions,
    description="Compact Schedule A membership, key index, sparse exceptions, and actual semantic changes.",
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=60,
        backoff=Backoff.EXPONENTIAL,
    ),
)
def fec_schedule_a_occurrences(
    context: AssetExecutionContext,
    config: FECScheduleAOccurrenceConfig,
    go_pipeline: GoPipelineResource,
) -> Output[str]:
    cycle = context.partition_key
    result = _execute(
        context,
        go_pipeline,
        arguments=[
            "pipeline",
            "fec",
            "publish-schedule-a-compact-occurrences",
            "--release",
            config.source_release_manifest_path,
            "--cycle",
            cycle,
            "--storage-root",
            go_pipeline.storage_root,
            "--run-id",
            context.run.run_id,
        ],
        schema_name="evidence/fec/schedule-a/compact/v1/manifest.schema.json",
        artifact_kind=f"occurrences/{cycle}",
        timeout_seconds=go_pipeline.occurrence_timeout_seconds,
    )
    payload = result.payload
    metadata: dict[str, Any] = {
        "schema_version": payload["schema_version"],
        "occurrence_set_id": payload["occurrence_set_id"],
        "source_release_id": payload["source_release_id"],
        "source_release_manifest_sha256": payload["source_release_manifest_sha256"],
        "source_artifact_sha256": payload["source_artifact_sha256"],
        "staged_output_sha256": payload["staged_output_sha256"],
        "cycle": payload["cycle"],
        "state": payload["state"],
        "counts": MetadataValue.json(payload["counts"]),
        "changes": MetadataValue.json(payload["changes"]),
        "configuration": MetadataValue.json(payload["configuration"]),
        "source_replay": MetadataValue.json(payload["source_replay"]),
        "index_partition_count": len(payload["index_partitions"]),
        "row_exceptions": MetadataValue.json(payload["row_exceptions"]),
        "deltas": MetadataValue.json(payload["deltas"]),
        "checks": MetadataValue.json(payload["checks"]),
        "source_release_artifact_path": MetadataValue.path(
            config.source_release_manifest_path
        ),
        **_artifact_metadata(result),
    }
    if prior_occurrence_set_id := payload.get("prior_occurrence_set_id"):
        metadata["prior_occurrence_set_id"] = prior_occurrence_set_id
    return Output(
        value=str(result.artifact_path),
        metadata=metadata,
        data_version=DataVersion(payload["occurrence_set_id"]),
    )


@asset(
    name="fec_schedule_a_facts",
    group_name=_EVIDENCE_GROUP_NAME,
    partitions_def=fec_cycle_partitions,
    description="Lossless 99-column Parquet receipt facts for one compact Schedule A occurrence partition.",
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=60,
        backoff=Backoff.EXPONENTIAL,
    ),
)
def fec_schedule_a_facts(
    context: AssetExecutionContext,
    config: FECScheduleAFactConfig,
    go_pipeline: GoPipelineResource,
    fec_schedule_a_occurrences: str,
) -> Output[str]:
    cycle = context.partition_key
    result = _execute(
        context,
        go_pipeline,
        arguments=[
            "pipeline",
            "fec",
            "publish-schedule-a-columnar-facts",
            "--release",
            config.source_release_manifest_path,
            "--occurrences",
            fec_schedule_a_occurrences,
            "--storage-root",
            go_pipeline.storage_root,
            "--run-id",
            context.run.run_id,
        ],
        schema_name="facts/fec/schedule-a/columnar/v1/manifest.schema.json",
        artifact_kind=f"facts/schedule-a/{cycle}",
        timeout_seconds=go_pipeline.occurrence_timeout_seconds,
    )
    payload = result.payload
    metadata: dict[str, Any] = {
        "schema_version": payload["schema_version"],
        "fact_set_id": payload["fact_set_id"],
        "fact_type": payload["fact_type"],
        "cycle": payload["cycle"],
        "source_contract": payload["source_contract"],
        "source_release_id": payload["source_release_id"],
        "occurrence_set_id": payload["occurrence_set_id"],
        "counts": MetadataValue.json(payload["counts"]),
        "configuration": MetadataValue.json(payload["configuration"]),
        "physical_schema_version": payload["physical_schema_version"],
        "parquet_library": payload["parquet_library"],
        "source_replay": MetadataValue.json(payload["source_replay"]),
        "shard_count": len(payload["shards"]),
        "checks": MetadataValue.json(payload["checks"]),
        "source_release_artifact_path": MetadataValue.path(
            config.source_release_manifest_path
        ),
        "occurrence_artifact_path": MetadataValue.path(fec_schedule_a_occurrences),
        **_artifact_metadata(result),
    }
    return Output(
        value=str(result.artifact_path),
        metadata=metadata,
        data_version=DataVersion(payload["fact_set_id"]),
    )


@asset(
    name="fec_schedule_b_facts",
    group_name=_EVIDENCE_GROUP_NAME,
    deps=[fec_release_publication],
    partitions_def=fec_cycle_partitions,
    description="Lossless 98-column Parquet disbursement facts streamed directly from one release-owned Schedule B relation.",
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=60,
        backoff=Backoff.EXPONENTIAL,
    ),
)
def fec_schedule_b_facts(
    context: AssetExecutionContext,
    config: FECScheduleBFactConfig,
    go_pipeline: GoPipelineResource,
) -> Output[str]:
    cycle = context.partition_key
    result = _execute(
        context,
        go_pipeline,
        arguments=[
            "pipeline",
            "fec",
            "publish-schedule-b-columnar-facts",
            "--release",
            config.source_release_manifest_path,
            "--cycle",
            cycle,
            "--storage-root",
            go_pipeline.storage_root,
            "--run-id",
            context.run.run_id,
        ],
        schema_name="facts/fec/schedule-b/columnar/v1/manifest.schema.json",
        artifact_kind=f"facts/schedule-b/{cycle}",
        timeout_seconds=go_pipeline.occurrence_timeout_seconds,
    )
    payload = result.payload
    metadata: dict[str, Any] = {
        "schema_version": payload["schema_version"],
        "fact_set_id": payload["fact_set_id"],
        "fact_type": payload["fact_type"],
        "cycle": payload["cycle"],
        "relation": payload["relation"],
        "source_contract": payload["source_contract"],
        "source_release_id": payload["source_release_id"],
        "source_artifact_sha256": payload["source_artifact_sha256"],
        "counts": MetadataValue.json(payload["counts"]),
        "configuration": MetadataValue.json(payload["configuration"]),
        "physical_schema_version": payload["physical_schema_version"],
        "parquet_library": payload["parquet_library"],
        "source_replay": MetadataValue.json(payload["source_replay"]),
        "shard_count": len(payload["shards"]),
        "checks": MetadataValue.json(payload["checks"]),
        "source_release_artifact_path": MetadataValue.path(
            config.source_release_manifest_path
        ),
        **_artifact_metadata(result),
    }
    return Output(
        value=str(result.artifact_path),
        metadata=metadata,
        data_version=DataVersion(payload["fact_set_id"]),
    )


@asset(
    name="fec_schedule_e_occurrences",
    group_name=_EVIDENCE_GROUP_NAME,
    deps=[fec_release_publication],
    partitions_def=fec_cycle_partitions,
    description="Every physical Schedule E row selected from the all-history source for one cycle.",
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=60,
        backoff=Backoff.EXPONENTIAL,
    ),
)
def fec_schedule_e_occurrences(
    context: AssetExecutionContext,
    config: FECScheduleEOccurrenceConfig,
    go_pipeline: GoPipelineResource,
) -> Output[str]:
    cycle = context.partition_key
    result = _execute(
        context,
        go_pipeline,
        arguments=[
            "pipeline",
            "fec",
            "publish-schedule-e-occurrences",
            "--release",
            config.source_release_manifest_path,
            "--cycle",
            cycle,
            "--storage-root",
            go_pipeline.storage_root,
            "--run-id",
            context.run.run_id,
        ],
        schema_name="evidence/fec/schedule-e/v1/manifest.schema.json",
        artifact_kind=f"occurrences/schedule-e/{cycle}",
        timeout_seconds=go_pipeline.occurrence_timeout_seconds,
    )
    payload = result.payload
    metadata: dict[str, Any] = {
        "schema_version": payload["schema_version"],
        "occurrence_set_id": payload["occurrence_set_id"],
        "source_release_id": payload["source_release_id"],
        "source_release_manifest_sha256": payload[
            "source_release_manifest_sha256"
        ],
        "source_artifact_sha256": payload["source_artifact_sha256"],
        "staged_output_sha256": payload["staged_output_sha256"],
        "cycle": payload["cycle"],
        "state": payload["state"],
        "counts": MetadataValue.json(payload["counts"]),
        "checks": MetadataValue.json(payload["checks"]),
        "source_release_artifact_path": MetadataValue.path(
            config.source_release_manifest_path
        ),
        **_artifact_metadata(result),
    }
    if prior_occurrence_set_id := payload.get("prior_occurrence_set_id"):
        metadata["prior_occurrence_set_id"] = prior_occurrence_set_id
    return Output(
        value=str(result.artifact_path),
        metadata=metadata,
        data_version=DataVersion(payload["occurrence_set_id"]),
    )


@asset(
    name="fec_schedule_e_facts",
    group_name=_EVIDENCE_GROUP_NAME,
    partitions_def=fec_cycle_partitions,
    description="Lossless typed independent-expenditure assertions for one Schedule E cycle.",
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=60,
        backoff=Backoff.EXPONENTIAL,
    ),
)
def fec_schedule_e_facts(
    context: AssetExecutionContext,
    config: FECScheduleEFactConfig,
    go_pipeline: GoPipelineResource,
    fec_schedule_e_occurrences: str,
) -> Output[str]:
    cycle = context.partition_key
    result = _execute(
        context,
        go_pipeline,
        arguments=[
            "pipeline",
            "fec",
            "publish-schedule-e-facts",
            "--release",
            config.source_release_manifest_path,
            "--occurrences",
            fec_schedule_e_occurrences,
            "--storage-root",
            go_pipeline.storage_root,
            "--run-id",
            context.run.run_id,
        ],
        schema_name="facts/fec/schedule-e/v1/manifest.schema.json",
        artifact_kind=f"facts/schedule-e/{cycle}",
        timeout_seconds=go_pipeline.occurrence_timeout_seconds,
    )
    payload = result.payload
    metadata = {
        "schema_version": payload["schema_version"],
        "fact_set_id": payload["fact_set_id"],
        "fact_type": payload["fact_type"],
        "cycle": payload["cycle"],
        "source_contract": payload["source_contract"],
        "source_release_id": payload["source_release_id"],
        "occurrence_set_id": payload["occurrence_set_id"],
        "counts": MetadataValue.json(payload["counts"]),
        "checks": MetadataValue.json(payload["checks"]),
        "source_release_artifact_path": MetadataValue.path(
            config.source_release_manifest_path
        ),
        "occurrence_artifact_path": MetadataValue.path(
            fec_schedule_e_occurrences
        ),
        **_artifact_metadata(result),
    }
    return Output(
        value=str(result.artifact_path),
        metadata=metadata,
        data_version=DataVersion(payload["fact_set_id"]),
    )


@asset(
    name="fec_effective_independent_expenditures",
    group_name=_CALCULATION_GROUP_NAME,
    partitions_def=fec_cycle_partitions,
    description="Exact signed support/oppose outside-spending components from one processed Schedule E fact set.",
    automation_condition=AutomationCondition.eager(),
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=60,
        backoff=Backoff.EXPONENTIAL,
    ),
)
def fec_effective_independent_expenditures(
    context: AssetExecutionContext,
    go_pipeline: GoPipelineResource,
    fec_schedule_e_facts: str,
) -> Output[str]:
    cycle = context.partition_key
    result = _execute(
        context,
        go_pipeline,
        arguments=[
            "pipeline",
            "fec",
            "publish-effective-independent-expenditures",
            "--schedule-e-facts",
            fec_schedule_e_facts,
            "--storage-root",
            go_pipeline.storage_root,
            "--run-id",
            context.run.run_id,
        ],
        schema_name=(
            "calculations/fec/effective-independent-expenditures/v1/"
            "manifest.schema.json"
        ),
        artifact_kind=f"calculations/effective-independent-expenditures/{cycle}",
        timeout_seconds=go_pipeline.occurrence_timeout_seconds,
    )
    payload = result.payload
    metadata: dict[str, Any] = {
        "schema_version": payload["schema_version"],
        "calculation_set_id": payload["calculation_set_id"],
        "calculation": payload["calculation"],
        "calculation_version": payload["calculation_version"],
        "source_release_id": payload["source_release_id"],
        "cycle": payload["cycle"],
        "state": payload["state"],
        "predicate": MetadataValue.json(payload["predicate"]),
        "decision_counts": MetadataValue.json(payload["decision_counts"]),
        "route_counts": MetadataValue.json(payload["route_counts"]),
        "source_shape_counts": MetadataValue.json(payload["source_shape_counts"]),
        "amounts": MetadataValue.json(payload["amounts"]),
        "input_fact_set": MetadataValue.json(payload["input_fact_set"]),
        "exceptions": MetadataValue.json(payload["exceptions"]),
        "results": MetadataValue.json(payload["results"]),
        "checks": MetadataValue.json(payload["checks"]),
        "schedule_e_fact_manifest_path": MetadataValue.path(fec_schedule_e_facts),
        **_artifact_metadata(result),
    }
    return Output(
        value=str(result.artifact_path),
        metadata=metadata,
        data_version=DataVersion(payload["calculation_set_id"]),
    )


@asset(
    name="fec_independent_expenditure_candidate_resolution",
    group_name=_CALCULATION_GROUP_NAME,
    partitions_def=fec_cycle_partitions,
    deps=[
        AssetDep(
            "fec_classic_facts",
            partition_mapping=_cycle_to_bundle_mapping,
        )
    ],
    description="Per-fact candidate identity decisions from effective Schedule E and the same-release candidate master.",
    automation_condition=AutomationCondition.eager(),
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=60,
        backoff=Backoff.EXPONENTIAL,
    ),
)
def fec_independent_expenditure_candidate_resolution(
    context: AssetExecutionContext,
    go_pipeline: GoPipelineResource,
    fec_effective_independent_expenditures: str,
) -> Output[str]:
    cycle = context.partition_key
    result = _execute(
        context,
        go_pipeline,
        arguments=[
            "pipeline",
            "fec",
            "publish-independent-expenditure-candidate-resolution",
            "--cycle",
            cycle,
            "--effective",
            fec_effective_independent_expenditures,
            "--storage-root",
            go_pipeline.storage_root,
            "--run-id",
            context.run.run_id,
        ],
        schema_name=(
            "calculations/fec/independent-expenditure-candidate-resolution/v1/"
            "manifest.schema.json"
        ),
        artifact_kind=(
            f"calculations/independent-expenditure-candidate-resolution/{cycle}"
        ),
        timeout_seconds=go_pipeline.occurrence_timeout_seconds,
    )
    payload = result.payload
    metadata: dict[str, Any] = {
        "schema_version": payload["schema_version"],
        "calculation_set_id": payload["calculation_set_id"],
        "calculation": payload["calculation"],
        "calculation_version": payload["calculation_version"],
        "source_release_id": payload["source_release_id"],
        "cycle": payload["cycle"],
        "state": payload["state"],
        "method": MetadataValue.json(payload["method"]),
        "counts": MetadataValue.json(payload["counts"]),
        "amounts": MetadataValue.json(payload["amounts"]),
        "input_calculation": MetadataValue.json(payload["input_calculation"]),
        "input_candidate_fact_set": MetadataValue.json(
            payload["input_candidate_fact_set"]
        ),
        "decisions": MetadataValue.json(payload["decisions"]),
        "checks": MetadataValue.json(payload["checks"]),
        "effective_calculation_manifest_path": MetadataValue.path(
            fec_effective_independent_expenditures
        ),
        **_artifact_metadata(result),
    }
    return Output(
        value=str(result.artifact_path),
        metadata=metadata,
        data_version=DataVersion(payload["calculation_set_id"]),
    )


@asset(
    name="fec_resolved_independent_expenditures",
    group_name=_CALCULATION_GROUP_NAME,
    partitions_def=fec_cycle_partitions,
    description="Resolved spender-candidate-stance groups plus exact ambiguous and unresolved exceptions.",
    automation_condition=AutomationCondition.eager(),
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=60,
        backoff=Backoff.EXPONENTIAL,
    ),
)
def fec_resolved_independent_expenditures(
    context: AssetExecutionContext,
    go_pipeline: GoPipelineResource,
    fec_independent_expenditure_candidate_resolution: str,
) -> Output[str]:
    cycle = context.partition_key
    result = _execute(
        context,
        go_pipeline,
        arguments=[
            "pipeline",
            "fec",
            "publish-resolved-independent-expenditures",
            "--cycle",
            cycle,
            "--candidate-resolution",
            fec_independent_expenditure_candidate_resolution,
            "--storage-root",
            go_pipeline.storage_root,
            "--run-id",
            context.run.run_id,
        ],
        schema_name=(
            "calculations/fec/resolved-independent-expenditures/v1/"
            "manifest.schema.json"
        ),
        artifact_kind=f"calculations/resolved-independent-expenditures/{cycle}",
        timeout_seconds=go_pipeline.occurrence_timeout_seconds,
    )
    payload = result.payload
    metadata: dict[str, Any] = {
        "schema_version": payload["schema_version"],
        "calculation_set_id": payload["calculation_set_id"],
        "calculation": payload["calculation"],
        "calculation_version": payload["calculation_version"],
        "source_release_id": payload["source_release_id"],
        "cycle": payload["cycle"],
        "state": payload["state"],
        "grouping_policy": MetadataValue.json(payload["grouping_policy"]),
        "counts": MetadataValue.json(payload["counts"]),
        "amounts": MetadataValue.json(payload["amounts"]),
        "input_candidate_resolution": MetadataValue.json(
            payload["input_candidate_resolution"]
        ),
        "results": MetadataValue.json(payload["results"]),
        "exceptions": MetadataValue.json(payload["exceptions"]),
        "checks": MetadataValue.json(payload["checks"]),
        "candidate_resolution_manifest_path": MetadataValue.path(
            fec_independent_expenditure_candidate_resolution
        ),
        **_artifact_metadata(result),
    }
    return Output(
        value=str(result.artifact_path),
        metadata=metadata,
        data_version=DataVersion(payload["calculation_set_id"]),
    )


@asset(
    name="arango_independent_expenditures",
    group_name=_PROJECTION_GROUP_NAME,
    partitions_def=fec_cycle_partitions,
    ins={
        "fec_independent_expenditure_projection_bundle": AssetIn(
            partition_mapping=_cycle_to_ie_projection_bundle_mapping
        )
    },
    description="Content-addressed support/opposition graph projection from one effective Schedule E calculation.",
    automation_condition=AutomationCondition.eager(),
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=60,
        backoff=Backoff.EXPONENTIAL,
    ),
)
def arango_independent_expenditures(
    context: AssetExecutionContext,
    config: ArangoIndependentExpenditureProjectionConfig,
    go_pipeline: GoPipelineResource,
    fec_independent_expenditure_projection_bundle: str,
) -> Output[str]:
    cycle = context.partition_key
    result = _execute(
        context,
        go_pipeline,
        arguments=[
            "pipeline",
            "fec",
            "probe-arango-independent-expenditures",
            "--cycle",
            cycle,
            "--storage-root",
            go_pipeline.storage_root,
            "--projection-bundle",
            fec_independent_expenditure_projection_bundle,
            "--endpoint",
            config.endpoint,
            "--username",
            config.username,
            "--batch-size",
            str(config.batch_size),
            "--query-repetitions",
            str(config.query_repetitions),
            "--run-id",
            context.run.run_id,
        ],
        schema_name=(
            "projections/arango/independent-expenditures/v1/"
            "result.schema.json"
        ),
        artifact_kind=f"projections/arango/independent-expenditures/{cycle}",
        timeout_seconds=go_pipeline.occurrence_timeout_seconds,
    )
    payload = result.payload
    metadata: dict[str, Any] = {
        "schema_version": payload["schema_version"],
        "projection_version": payload["projection_version"],
        "projection_id": payload["projection_id"],
        "state": payload["state"],
        "cycle": payload["cycle"],
        "database": payload["database"],
        "graph": payload["graph"],
        "inputs": MetadataValue.json(payload["inputs"]),
        "expected_counts": MetadataValue.json(payload["expected_counts"]),
        "observed_counts": MetadataValue.json(payload["observed_counts"]),
        "expected_amounts": MetadataValue.json(payload["expected_amounts"]),
        "observed_amounts": MetadataValue.json(payload["observed_amounts"]),
        "reused_projection": payload["reused_projection"],
        "missing_master_facts": MetadataValue.json(
            payload["missing_master_facts"]
        ),
        "storage": MetadataValue.json(payload["storage"]),
        "representative_candidate_id": payload["representative_candidate_id"],
        "representative_spender_committee_id": payload[
            "representative_spender_committee_id"
        ],
        "queries": MetadataValue.json(payload["queries"]),
        "checks": MetadataValue.json(payload["checks"]),
        "projection_bundle_manifest_path": MetadataValue.path(
            fec_independent_expenditure_projection_bundle
        ),
        **_artifact_metadata(result),
    }
    return Output(
        value=str(result.artifact_path),
        metadata=metadata,
        data_version=DataVersion(payload["projection_id"]),
    )


@asset(
    name="arango_resolved_independent_expenditures",
    group_name=_PROJECTION_GROUP_NAME,
    partitions_def=fec_cycle_partitions,
    ins={
        "fec_resolved_independent_expenditure_projection_bundle": AssetIn(
            partition_mapping=_cycle_to_resolved_ie_projection_bundle_mapping
        )
    },
    description="Content-addressed support/opposition graph from resolved candidate groups with explicit unresolved coverage.",
    automation_condition=AutomationCondition.eager(),
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=60,
        backoff=Backoff.EXPONENTIAL,
    ),
)
def arango_resolved_independent_expenditures(
    context: AssetExecutionContext,
    config: ArangoIndependentExpenditureProjectionConfig,
    go_pipeline: GoPipelineResource,
    fec_resolved_independent_expenditure_projection_bundle: str,
) -> Output[str]:
    cycle = context.partition_key
    result = _execute(
        context,
        go_pipeline,
        arguments=[
            "pipeline",
            "fec",
            "probe-arango-resolved-independent-expenditures",
            "--cycle",
            cycle,
            "--storage-root",
            go_pipeline.storage_root,
            "--projection-bundle",
            fec_resolved_independent_expenditure_projection_bundle,
            "--endpoint",
            config.endpoint,
            "--username",
            config.username,
            "--batch-size",
            str(config.batch_size),
            "--query-repetitions",
            str(config.query_repetitions),
            "--run-id",
            context.run.run_id,
        ],
        schema_name=(
            "projections/arango/independent-expenditures/v2/"
            "result.schema.json"
        ),
        artifact_kind=(
            f"projections/arango/resolved-independent-expenditures/{cycle}"
        ),
        timeout_seconds=go_pipeline.occurrence_timeout_seconds,
    )
    payload = result.payload
    metadata: dict[str, Any] = {
        "schema_version": payload["schema_version"],
        "projection_version": payload["projection_version"],
        "projection_id": payload["projection_id"],
        "state": payload["state"],
        "cycle": payload["cycle"],
        "database": payload["database"],
        "graph": payload["graph"],
        "inputs": MetadataValue.json(payload["inputs"]),
        "expected_counts": MetadataValue.json(payload["expected_counts"]),
        "observed_counts": MetadataValue.json(payload["observed_counts"]),
        "expected_amounts": MetadataValue.json(payload["expected_amounts"]),
        "observed_amounts": MetadataValue.json(payload["observed_amounts"]),
        "candidate_resolution_coverage": MetadataValue.json(
            payload["candidate_resolution_coverage"]
        ),
        "reused_projection": payload["reused_projection"],
        "missing_master_facts": MetadataValue.json(
            payload["missing_master_facts"]
        ),
        "storage": MetadataValue.json(payload["storage"]),
        "representative_candidate_id": payload["representative_candidate_id"],
        "representative_spender_committee_id": payload[
            "representative_spender_committee_id"
        ],
        "queries": MetadataValue.json(payload["queries"]),
        "checks": MetadataValue.json(payload["checks"]),
        "projection_bundle_manifest_path": MetadataValue.path(
            fec_resolved_independent_expenditure_projection_bundle
        ),
        **_artifact_metadata(result),
    }
    return Output(
        value=str(result.artifact_path),
        metadata=metadata,
        data_version=DataVersion(payload["projection_id"]),
    )


@asset(
    name="fec_classic_occurrences",
    group_name=_EVIDENCE_GROUP_NAME,
    deps=[fec_release_publication],
    partitions_def=fec_classic_slice_partitions,
    description="Immutable row evidence and semantic changes for one classic FEC dataset and cycle.",
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=60,
        backoff=Backoff.EXPONENTIAL,
    ),
)
def fec_classic_occurrences(
    context: AssetExecutionContext,
    config: FECClassicOccurrenceConfig,
    go_pipeline: GoPipelineResource,
) -> Output[str]:
    partition = context.partition_key
    keys = getattr(partition, "keys_by_dimension", None)
    if keys is None:
        raise Failure(description="classic FEC asset requires a multi-partition key")
    dataset = keys["dataset"]
    cycle = keys["cycle"]
    if dataset != config.dataset or cycle != config.cycle:
        raise Failure(
            description=(
                f"classic FEC partition {context.partition_key!r} does not match "
                f"configured slice dataset={config.dataset!r}, cycle={config.cycle!r}"
            )
        )
    result = _execute(
        context,
        go_pipeline,
        arguments=[
            "pipeline",
            "fec",
            "publish-classic-occurrences",
            "--release",
            config.source_release_manifest_path,
            "--dataset",
            config.dataset,
            "--cycle",
            config.cycle,
            "--storage-root",
            go_pipeline.storage_root,
            "--run-id",
            context.run.run_id,
        ],
        schema_name="evidence/fec/classic/v1/manifest.schema.json",
        artifact_kind=f"occurrences/classic/{config.dataset}/{config.cycle}",
        timeout_seconds=go_pipeline.occurrence_timeout_seconds,
    )
    payload = result.payload
    metadata: dict[str, Any] = {
        "schema_version": payload["schema_version"],
        "occurrence_set_id": payload["occurrence_set_id"],
        "dataset": payload["dataset"],
        "source_contract": payload["source_contract"],
        "source_id": payload["source_id"],
        "source_release_id": payload["source_release_id"],
        "source_release_manifest_sha256": payload["source_release_manifest_sha256"],
        "source_artifact_sha256": payload["source_artifact_sha256"],
        "staged_output_sha256": payload["staged_output_sha256"],
        "member": payload["member"],
        "cycle": payload["cycle"],
        "state": payload["state"],
        "counts": MetadataValue.json(payload["counts"]),
        "changes": MetadataValue.json(payload["changes"]),
        "artifacts": MetadataValue.json(payload["artifacts"]),
        "checks": MetadataValue.json(payload["checks"]),
        "source_release_artifact_path": MetadataValue.path(
            config.source_release_manifest_path
        ),
        **_artifact_metadata(result),
    }
    if prior_occurrence_set_id := payload.get("prior_occurrence_set_id"):
        metadata["prior_occurrence_set_id"] = prior_occurrence_set_id
    return Output(
        value=str(result.artifact_path),
        metadata=metadata,
        data_version=DataVersion(payload["occurrence_set_id"]),
    )


@asset(
    name="fec_classic_facts",
    group_name=_EVIDENCE_GROUP_NAME,
    partitions_def=fec_classic_slice_partitions,
    description="Typed, lossless source assertions for one unique classic FEC occurrence slice.",
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=60,
        backoff=Backoff.EXPONENTIAL,
    ),
)
def fec_classic_facts(
    context: AssetExecutionContext,
    config: FECClassicFactConfig,
    go_pipeline: GoPipelineResource,
    fec_classic_occurrences: str,
) -> Output[str]:
    partition = context.partition_key
    keys = getattr(partition, "keys_by_dimension", None)
    if keys is None:
        raise Failure(description="classic FEC asset requires a multi-partition key")
    dataset = keys["dataset"]
    cycle = keys["cycle"]
    result = _execute(
        context,
        go_pipeline,
        arguments=[
            "pipeline",
            "fec",
            "publish-classic-facts",
            "--release",
            config.source_release_manifest_path,
            "--occurrences",
            fec_classic_occurrences,
            "--storage-root",
            go_pipeline.storage_root,
            "--run-id",
            context.run.run_id,
        ],
        schema_name="facts/fec/classic/v1/manifest.schema.json",
        artifact_kind=f"facts/classic/{dataset}/{cycle}",
        timeout_seconds=go_pipeline.occurrence_timeout_seconds,
    )
    payload = result.payload
    metadata: dict[str, Any] = {
        "schema_version": payload["schema_version"],
        "fact_set_id": payload["fact_set_id"],
        "dataset": payload["dataset"],
        "fact_type": payload["fact_type"],
        "cycle": payload["cycle"],
        "source_contract": payload["source_contract"],
        "source_release_id": payload["source_release_id"],
        "occurrence_set_id": payload["occurrence_set_id"],
        "counts": MetadataValue.json(payload["counts"]),
        "facts": MetadataValue.json(payload["facts"]),
        "checks": MetadataValue.json(payload["checks"]),
        "source_release_artifact_path": MetadataValue.path(
            config.source_release_manifest_path
        ),
        "occurrence_artifact_path": MetadataValue.path(fec_classic_occurrences),
        **_artifact_metadata(result),
    }
    return Output(
        value=str(result.artifact_path),
        metadata=metadata,
        data_version=DataVersion(payload["fact_set_id"]),
    )


@asset(
    name="fec_independent_expenditure_projection_bundle",
    group_name=_PROJECTION_GROUP_NAME,
    partitions_def=fec_independent_expenditure_projection_bundle_partitions,
    deps=[
        AssetDep(
            fec_effective_independent_expenditures,
            partition_mapping=_cycle_to_ie_projection_bundle_mapping,
        ),
        AssetDep(
            fec_classic_facts,
            partition_mapping=_classic_to_ie_projection_bundle_mapping,
        ),
    ],
    description="Exact same-cycle and same-release calculation/master bundle required by the independent-expenditure graph projection.",
    automation_condition=AutomationCondition.eager(),
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=60,
        backoff=Backoff.EXPONENTIAL,
    ),
)
def fec_independent_expenditure_projection_bundle(
    context: AssetExecutionContext,
    go_pipeline: GoPipelineResource,
) -> Output[str]:
    partition = context.partition_key
    keys = getattr(partition, "keys_by_dimension", None)
    if keys is None or keys["bundle"] != _IE_PROJECTION_BUNDLE_KIND:
        raise Failure(
            description=(
                "independent-expenditure projection bundle requires its "
                "declared multi-partition key"
            )
        )
    cycle = keys["cycle"]
    result = _execute(
        context,
        go_pipeline,
        arguments=[
            "pipeline",
            "fec",
            "publish-independent-expenditure-projection-bundle",
            "--cycle",
            cycle,
            "--storage-root",
            go_pipeline.storage_root,
            "--run-id",
            context.run.run_id,
        ],
        schema_name=(
            "bundles/fec/independent-expenditure-projection/v1/"
            "manifest.schema.json"
        ),
        artifact_kind=f"bundles/independent-expenditure-projection/{cycle}",
        timeout_seconds=go_pipeline.occurrence_timeout_seconds,
    )
    payload = result.payload
    metadata: dict[str, Any] = {
        "schema_version": payload["schema_version"],
        "bundle_id": payload["bundle_id"],
        "bundle_type": payload["bundle_type"],
        "bundle_version": payload["bundle_version"],
        "source_release_id": payload["source_release_id"],
        "cycle": payload["cycle"],
        "state": payload["state"],
        "input_calculation": MetadataValue.json(payload["input_calculation"]),
        "input_fact_sets": MetadataValue.json(payload["input_fact_sets"]),
        "counts": MetadataValue.json(payload["counts"]),
        "checks": MetadataValue.json(payload["checks"]),
        **_artifact_metadata(result),
    }
    return Output(
        value=str(result.artifact_path),
        metadata=metadata,
        data_version=DataVersion(payload["bundle_id"]),
    )


@asset(
    name="fec_resolved_independent_expenditure_projection_bundle",
    group_name=_PROJECTION_GROUP_NAME,
    partitions_def=fec_resolved_independent_expenditure_projection_bundle_partitions,
    deps=[
        AssetDep(
            fec_resolved_independent_expenditures,
            partition_mapping=_cycle_to_resolved_ie_projection_bundle_mapping,
        ),
        AssetDep(
            fec_classic_facts,
            partition_mapping=_classic_to_resolved_ie_projection_bundle_mapping,
        ),
    ],
    description="Exact resolved calculation, candidate-resolution ancestry, and master facts required by the v2 graph.",
    automation_condition=AutomationCondition.eager(),
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=60,
        backoff=Backoff.EXPONENTIAL,
    ),
)
def fec_resolved_independent_expenditure_projection_bundle(
    context: AssetExecutionContext,
    go_pipeline: GoPipelineResource,
) -> Output[str]:
    partition = context.partition_key
    keys = getattr(partition, "keys_by_dimension", None)
    if (
        keys is None
        or keys["bundle"] != _RESOLVED_IE_PROJECTION_BUNDLE_KIND
    ):
        raise Failure(
            description=(
                "resolved independent-expenditure projection bundle "
                "requires its declared multi-partition key"
            )
        )
    cycle = keys["cycle"]
    result = _execute(
        context,
        go_pipeline,
        arguments=[
            "pipeline",
            "fec",
            "publish-resolved-independent-expenditure-projection-bundle",
            "--cycle",
            cycle,
            "--storage-root",
            go_pipeline.storage_root,
            "--run-id",
            context.run.run_id,
        ],
        schema_name=(
            "bundles/fec/resolved-independent-expenditure-projection/v1/"
            "manifest.schema.json"
        ),
        artifact_kind=(
            f"bundles/resolved-independent-expenditure-projection/{cycle}"
        ),
        timeout_seconds=go_pipeline.occurrence_timeout_seconds,
    )
    payload = result.payload
    metadata: dict[str, Any] = {
        "schema_version": payload["schema_version"],
        "bundle_id": payload["bundle_id"],
        "bundle_type": payload["bundle_type"],
        "bundle_version": payload["bundle_version"],
        "source_release_id": payload["source_release_id"],
        "cycle": payload["cycle"],
        "state": payload["state"],
        "input_calculation": MetadataValue.json(payload["input_calculation"]),
        "input_fact_sets": MetadataValue.json(payload["input_fact_sets"]),
        "counts": MetadataValue.json(payload["counts"]),
        "checks": MetadataValue.json(payload["checks"]),
        **_artifact_metadata(result),
    }
    return Output(
        value=str(result.artifact_path),
        metadata=metadata,
        data_version=DataVersion(payload["bundle_id"]),
    )


@asset(
    name="fec_candidate_receipt_fact_bundle",
    group_name=_CALCULATION_GROUP_NAME,
    partitions_def=fec_candidate_receipt_bundle_partitions,
    deps=[
        AssetDep(
            fec_schedule_a_facts,
            partition_mapping=_cycle_to_bundle_mapping,
        ),
        AssetDep(
            fec_classic_facts,
            partition_mapping=_classic_to_receipt_bundle_mapping,
        ),
    ],
    description="Exact same-cycle and same-release fact bundle required by the compact receipt calculation.",
    automation_condition=AutomationCondition.eager(),
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=60,
        backoff=Backoff.EXPONENTIAL,
    ),
)
def fec_candidate_receipt_fact_bundle(
    context: AssetExecutionContext,
    go_pipeline: GoPipelineResource,
) -> Output[str]:
    partition = context.partition_key
    keys = getattr(partition, "keys_by_dimension", None)
    if keys is None or keys["bundle"] != _RECEIPT_BUNDLE_KIND:
        raise Failure(description="candidate receipt fact bundle requires its declared multi-partition key")
    cycle = keys["cycle"]
    result = _execute(
        context,
        go_pipeline,
        arguments=[
            "pipeline",
            "fec",
            "publish-candidate-itemized-receipts-fact-bundle",
            "--cycle",
            cycle,
            "--storage-root",
            go_pipeline.storage_root,
            "--run-id",
            context.run.run_id,
        ],
        schema_name="bundles/fec/candidate-itemized-individual-receipts/v1/manifest.schema.json",
        artifact_kind=f"bundles/candidate-itemized-individual-receipts/{cycle}",
        timeout_seconds=go_pipeline.occurrence_timeout_seconds,
    )
    payload = result.payload
    metadata: dict[str, Any] = {
        "schema_version": payload["schema_version"],
        "bundle_id": payload["bundle_id"],
        "bundle_type": payload["bundle_type"],
        "bundle_version": payload["bundle_version"],
        "source_release_id": payload["source_release_id"],
        "cycle": payload["cycle"],
        "state": payload["state"],
        "counts": MetadataValue.json(payload["counts"]),
        "input_fact_sets": MetadataValue.json(payload["input_fact_sets"]),
        "checks": MetadataValue.json(payload["checks"]),
        **_artifact_metadata(result),
    }
    return Output(
        value=str(result.artifact_path),
        metadata=metadata,
        data_version=DataVersion(payload["bundle_id"]),
    )


@asset(
    name="fec_candidate_itemized_receipts",
    group_name=_CALCULATION_GROUP_NAME,
    partitions_def=fec_cycle_partitions,
    ins={
        "fec_candidate_receipt_fact_bundle": AssetIn(
            partition_mapping=_cycle_to_bundle_mapping
        )
    },
    description="Compact candidate itemized-individual components and independent summary reconciliations for one ready fact bundle.",
    automation_condition=AutomationCondition.eager(),
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=60,
        backoff=Backoff.EXPONENTIAL,
    ),
)
def fec_candidate_itemized_receipts(
    context: AssetExecutionContext,
    go_pipeline: GoPipelineResource,
    fec_candidate_receipt_fact_bundle: str,
) -> Output[str]:
    cycle = context.partition_key
    result = _execute(
        context,
        go_pipeline,
        arguments=[
            "pipeline",
            "fec",
            "publish-candidate-itemized-receipts-compact",
            "--fact-bundle",
            fec_candidate_receipt_fact_bundle,
            "--storage-root",
            go_pipeline.storage_root,
            "--run-id",
            context.run.run_id,
        ],
        schema_name=(
            "calculations/fec/candidate-itemized-individual-receipts/compact/v1/"
            "manifest.schema.json"
        ),
        artifact_kind=f"calculations/candidate-itemized-individual-receipts/compact/{cycle}",
        timeout_seconds=go_pipeline.occurrence_timeout_seconds,
    )
    payload = result.payload
    metadata: dict[str, Any] = {
        "schema_version": payload["schema_version"],
        "calculation_set_id": payload["calculation_set_id"],
        "calculation": payload["calculation"],
        "calculation_version": payload["calculation_version"],
        "source_release_id": payload["source_release_id"],
        "cycle": payload["cycle"],
        "state": payload["state"],
        "predicate": MetadataValue.json(payload["predicate"]),
        "decision_counts": MetadataValue.json(payload["decision_counts"]),
        "result_counts": MetadataValue.json(payload["result_counts"]),
        "reconciliations": MetadataValue.json(payload["reconciliations"]),
        "input_fact_sets": MetadataValue.json(payload["input_fact_sets"]),
        "exceptions": MetadataValue.json(payload["exceptions"]),
        "results": MetadataValue.json(payload["results"]),
        "checks": MetadataValue.json(payload["checks"]),
        "fact_bundle_manifest_path": MetadataValue.path(
            fec_candidate_receipt_fact_bundle
        ),
        **_artifact_metadata(result),
    }
    return Output(
        value=str(result.artifact_path),
        metadata=metadata,
        data_version=DataVersion(payload["calculation_set_id"]),
    )


@asset(
    name="fec_receiver_reported_committee_flows",
    group_name=_CALCULATION_GROUP_NAME,
    partitions_def=fec_cycle_partitions,
    description="Exact receiver-reported committee-flow components from one Schedule A fact set.",
    automation_condition=AutomationCondition.eager(),
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=60,
        backoff=Backoff.EXPONENTIAL,
    ),
)
def fec_receiver_reported_committee_flows(
    context: AssetExecutionContext,
    go_pipeline: GoPipelineResource,
    fec_schedule_a_facts: str,
) -> Output[str]:
    cycle = context.partition_key
    result = _execute(
        context,
        go_pipeline,
        arguments=[
            "pipeline",
            "fec",
            "publish-receiver-committee-flows",
            "--cycle",
            cycle,
            "--schedule-a-facts",
            fec_schedule_a_facts,
            "--storage-root",
            go_pipeline.storage_root,
            "--run-id",
            context.run.run_id,
        ],
        schema_name=(
            "calculations/fec/receiver-reported-committee-flows/v1/"
            "manifest.schema.json"
        ),
        artifact_kind=f"calculations/receiver-reported-committee-flows/{cycle}",
        timeout_seconds=go_pipeline.occurrence_timeout_seconds,
    )
    payload = result.payload
    metadata: dict[str, Any] = {
        "schema_version": payload["schema_version"],
        "calculation_set_id": payload["calculation_set_id"],
        "calculation": payload["calculation"],
        "calculation_version": payload["calculation_version"],
        "policy_version": payload["policy_version"],
        "source_release_id": payload["source_release_id"],
        "cycle": payload["cycle"],
        "state": payload["state"],
        "predicate": MetadataValue.json(payload["predicate"]),
        "decision_counts": MetadataValue.json(payload["decision_counts"]),
        "result_counts": MetadataValue.json(payload["result_counts"]),
        "amounts": MetadataValue.json(payload["amounts"]),
        "input_fact_set": MetadataValue.json(payload["input_fact_set"]),
        "exceptions": MetadataValue.json(payload["exceptions"]),
        "results": MetadataValue.json(payload["results"]),
        "checks": MetadataValue.json(payload["checks"]),
        "schedule_a_fact_manifest_path": MetadataValue.path(fec_schedule_a_facts),
        **_artifact_metadata(result),
    }
    return Output(
        value=str(result.artifact_path),
        metadata=metadata,
        data_version=DataVersion(payload["calculation_set_id"]),
    )


@asset(
    name="fec_receiver_committee_flow_projection_bundle",
    group_name=_PROJECTION_GROUP_NAME,
    partitions_def=fec_receiver_committee_flow_projection_bundle_partitions,
    deps=[
        AssetDep(
            fec_receiver_reported_committee_flows,
            partition_mapping=_cycle_to_receiver_flow_projection_bundle_mapping,
        ),
        AssetDep(
            fec_classic_facts,
            partition_mapping=(
                _classic_to_receiver_flow_projection_bundle_mapping
            ),
        ),
    ],
    description=(
        "Freeze the exact receiver-flow calculation and same-release "
        "committee master required by one graph projection."
    ),
    automation_condition=AutomationCondition.eager(),
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=60,
        backoff=Backoff.EXPONENTIAL,
    ),
)
def fec_receiver_committee_flow_projection_bundle(
    context: AssetExecutionContext,
    go_pipeline: GoPipelineResource,
) -> Output[str]:
    partition = context.partition_key
    keys = getattr(partition, "keys_by_dimension", None)
    if (
        keys is None
        or keys["bundle"] != _RECEIVER_FLOW_PROJECTION_BUNDLE_KIND
    ):
        raise Failure(
            description=(
                "receiver-flow projection bundle requires its declared "
                "multi-partition key"
            )
        )
    cycle = keys["cycle"]
    result = _execute(
        context,
        go_pipeline,
        arguments=[
            "pipeline",
            "fec",
            "publish-receiver-committee-flow-projection-bundle",
            "--cycle",
            cycle,
            "--storage-root",
            go_pipeline.storage_root,
            "--run-id",
            context.run.run_id,
        ],
        schema_name=(
            "bundles/fec/receiver-reported-committee-flow-projection/v1/"
            "manifest.schema.json"
        ),
        artifact_kind=(
            f"bundles/receiver-reported-committee-flow-projection/{cycle}"
        ),
        timeout_seconds=go_pipeline.occurrence_timeout_seconds,
    )
    payload = result.payload
    metadata: dict[str, Any] = {
        "schema_version": payload["schema_version"],
        "bundle_id": payload["bundle_id"],
        "bundle_type": payload["bundle_type"],
        "bundle_version": payload["bundle_version"],
        "source_release_id": payload["source_release_id"],
        "cycle": payload["cycle"],
        "state": payload["state"],
        "input_calculation": MetadataValue.json(payload["input_calculation"]),
        "input_fact_sets": MetadataValue.json(payload["input_fact_sets"]),
        "counts": MetadataValue.json(payload["counts"]),
        "checks": MetadataValue.json(payload["checks"]),
        **_artifact_metadata(result),
    }
    return Output(
        value=str(result.artifact_path),
        metadata=metadata,
        data_version=DataVersion(payload["bundle_id"]),
    )


@asset(
    name="arango_receiver_reported_committee_flows",
    group_name=_PROJECTION_GROUP_NAME,
    partitions_def=fec_cycle_partitions,
    ins={
        "fec_receiver_committee_flow_projection_bundle": AssetIn(
            partition_mapping=(
                _cycle_to_receiver_flow_projection_bundle_mapping
            )
        )
    },
    description=(
        "Project receiver-reported committee flows into an isolated, "
        "content-addressed ArangoDB graph."
    ),
    automation_condition=AutomationCondition.eager(),
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=60,
        backoff=Backoff.EXPONENTIAL,
    ),
)
def arango_receiver_reported_committee_flows(
    context: AssetExecutionContext,
    config: ArangoIndependentExpenditureProjectionConfig,
    go_pipeline: GoPipelineResource,
    fec_receiver_committee_flow_projection_bundle: str,
) -> Output[str]:
    cycle = context.partition_key
    result = _execute(
        context,
        go_pipeline,
        arguments=[
            "pipeline",
            "fec",
            "probe-arango-receiver-committee-flows",
            "--cycle",
            cycle,
            "--storage-root",
            go_pipeline.storage_root,
            "--projection-bundle",
            fec_receiver_committee_flow_projection_bundle,
            "--endpoint",
            config.endpoint,
            "--username",
            config.username,
            "--batch-size",
            str(config.batch_size),
            "--query-repetitions",
            str(config.query_repetitions),
            "--run-id",
            context.run.run_id,
        ],
        schema_name=(
            "projections/arango/receiver-reported-committee-flows/v1/"
            "result.schema.json"
        ),
        artifact_kind=(
            f"projections/arango/receiver-reported-committee-flows/{cycle}"
        ),
        timeout_seconds=go_pipeline.occurrence_timeout_seconds,
    )
    payload = result.payload
    metadata: dict[str, Any] = {
        "schema_version": payload["schema_version"],
        "projection_version": payload["projection_version"],
        "projection_id": payload["projection_id"],
        "state": payload["state"],
        "cycle": payload["cycle"],
        "database": payload["database"],
        "graph": payload["graph"],
        "inputs": MetadataValue.json(payload["inputs"]),
        "expected_counts": MetadataValue.json(payload["expected_counts"]),
        "observed_counts": MetadataValue.json(payload["observed_counts"]),
        "expected_amounts": MetadataValue.json(payload["expected_amounts"]),
        "observed_amounts": MetadataValue.json(payload["observed_amounts"]),
        "topology": MetadataValue.json(payload["topology"]),
        "reused_projection": payload["reused_projection"],
        "missing_master_facts": MetadataValue.json(
            payload["missing_master_facts"]
        ),
        "storage": MetadataValue.json(payload["storage"]),
        "representative_source_committee_id": payload[
            "representative_source_committee_id"
        ],
        "representative_target_committee_id": payload[
            "representative_target_committee_id"
        ],
        "representative_cycle_committee_id": payload[
            "representative_cycle_committee_id"
        ],
        "queries": MetadataValue.json(payload["queries"]),
        "checks": MetadataValue.json(payload["checks"]),
        "projection_bundle_manifest_path": MetadataValue.path(
            fec_receiver_committee_flow_projection_bundle
        ),
        **_artifact_metadata(result),
    }
    return Output(
        value=str(result.artifact_path),
        metadata=metadata,
        data_version=DataVersion(payload["projection_id"]),
    )


def _execute(
    context: AssetExecutionContext,
    go_pipeline: GoPipelineResource,
    *,
    arguments: list[str],
    schema_name: str,
    artifact_kind: str,
    timeout_seconds: int | None = None,
) -> StoredCommandResult:
    try:
        result = go_pipeline.execute(
            arguments=arguments,
            schema_name=schema_name,
            artifact_kind=artifact_kind,
            timeout_seconds=timeout_seconds,
        )
    except GoCommandError as error:
        metadata: dict[str, Any] | None = None
        if error.result is not None:
            metadata = {
                "failed_control_artifact_path": MetadataValue.path(
                    error.result.artifact_path
                ),
                "failed_control_artifact_sha256": error.result.sha256,
                "failed_status": error.result.payload.get("status", "unknown"),
                "failed_issues": MetadataValue.json(
                    error.result.payload.get("issues", [])
                ),
            }
        raise Failure(description=str(error), metadata=metadata) from error
    if result.diagnostics:
        context.log.info("Go diagnostics: %s", result.diagnostics)
    return result


def _artifact_metadata(result: StoredCommandResult) -> dict[str, Any]:
    return {
        "control_artifact_path": MetadataValue.path(result.artifact_path),
        "control_artifact_sha256": result.sha256,
    }


__all__ = [
    "FEC_CLASSIC_DATASETS",
    "ArangoIndependentExpenditureProjectionConfig",
    "FECClassicFactConfig",
    "FECClassicOccurrenceConfig",
    "FECReleaseAcquisitionConfig",
    "FECReleasePublicationConfig",
    "FECReleaseStagingConfig",
    "FECScheduleAFactConfig",
    "FECScheduleAOccurrenceConfig",
    "FECScheduleEFactConfig",
    "FECScheduleEOccurrenceConfig",
    "arango_independent_expenditures",
    "arango_receiver_reported_committee_flows",
    "arango_resolved_independent_expenditures",
    "fec_candidate_itemized_receipts",
    "fec_candidate_receipt_bundle_partitions",
    "fec_candidate_receipt_fact_bundle",
    "fec_classic_facts",
    "fec_classic_occurrences",
    "fec_classic_slice_partitions",
    "fec_cycle_partitions",
    "fec_effective_independent_expenditures",
    "fec_independent_expenditure_candidate_resolution",
    "fec_independent_expenditure_projection_bundle",
    "fec_independent_expenditure_projection_bundle_partitions",
    "fec_release_acquisition",
    "fec_release_candidate",
    "fec_release_discovery",
    "fec_release_publication",
    "fec_release_stage",
    "fec_receiver_reported_committee_flows",
    "fec_receiver_committee_flow_projection_bundle",
    "fec_receiver_committee_flow_projection_bundle_partitions",
    "fec_resolved_independent_expenditure_projection_bundle",
    "fec_resolved_independent_expenditure_projection_bundle_partitions",
    "fec_resolved_independent_expenditures",
    "fec_schedule_a_facts",
    "fec_schedule_a_occurrences",
    "fec_schedule_b_facts",
    "fec_schedule_e_facts",
    "fec_schedule_e_occurrences",
]
