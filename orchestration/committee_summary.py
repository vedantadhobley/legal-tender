"""Manual-only Dagster handoff to the lossless Go summary publisher."""

from pathlib import Path

from dagster import (
    AssetCheckResult,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetIn,
    Backoff,
    DataVersion,
    Failure,
    MetadataValue,
    Output,
    RetryPolicy,
    asset,
)

from orchestration.assets import _artifact_metadata, _execute, fec_cycle_partitions
from orchestration.resources import GoPipelineResource


@asset(
    group_name="rewrite_fec_evidence",
    partitions_def=fec_cycle_partitions,
    ins={"fec_release_publication": AssetIn()},
    check_specs=[
        AssetCheckSpec(
            "go_verified",
            asset="fec_committee_summary_facts",
            blocking=True,
            partitions_def=fec_cycle_partitions,
        )
    ],
    # Deliberately no automation condition or source-release sensor.
    automation_condition=None,
    retry_policy=RetryPolicy(max_retries=2, delay=60, backoff=Backoff.EXPONENTIAL),
    description="Manually publish one cycle's lossless summary facts from an exact release output; no financial grouping.",
)
def fec_committee_summary_facts(
    context: AssetExecutionContext,
    go_pipeline: GoPipelineResource,
    fec_release_publication: str,
):
    cycle = context.partition_key
    try:
        result = _execute(
            context,
            go_pipeline,
            arguments=[
                "pipeline",
                "fec",
                "publish-committee-summary",
                "--storage-root",
                go_pipeline.storage_root,
                "--release",
                fec_release_publication,
                "--cycle",
                cycle,
                "--run-id",
                context.run.run_id,
            ],
            schema_name="facts/fec/committee-summary/v1/manifest.schema.json",
            artifact_kind=f"facts/committee-summary/{cycle}",
            timeout_seconds=go_pipeline.occurrence_timeout_seconds,
        )
        if result.payload["cycle"] != cycle:
            raise Failure(
                "Go result cycle differs from the requested Dagster partition"
            )
    except Failure as error:
        yield AssetCheckResult(
            passed=False, check_name="go_verified", description=error.description
        )
        raise

    payload = result.payload
    manifest = (
        Path(go_pipeline.storage_root)
        / "facts/fec/committee-summary/v1/manifests"
        / f"{payload['fact_set_id']}.json"
    )
    yield Output(
        str(manifest),
        data_version=DataVersion(payload["fact_set_id"]),
        metadata={
            **{
                field: payload[field]
                for field in (
                    "schema_version",
                    "fact_set_id",
                    "state",
                    "cycle",
                    "source_release_id",
                    "source_release_manifest_sha256",
                    "readback_verified",
                    "terminal_attribution_eligible",
                )
            },
            "records": payload["facts"]["record_count"],
            "issue_counts": MetadataValue.json(payload["verification"]["issue_counts"]),
            "source_release_manifest_path": MetadataValue.path(fec_release_publication),
            "fact_manifest_path": MetadataValue.path(manifest),
            **_artifact_metadata(result),
        },
    )
    # Success must follow Output so the check targets this materialization.
    yield AssetCheckResult(
        passed=True, check_name="go_verified", metadata=_artifact_metadata(result)
    )
