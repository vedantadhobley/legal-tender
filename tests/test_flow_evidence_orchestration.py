"""Only orchestration, command contracts, and failure propagation live here."""

import copy
import json
from pathlib import Path

import pytest
from dagster import (
    AssetCheckResult,
    AssetKey,
    AssetSelection,
    DagsterInstance,
    Definitions,
    MultiPartitionKey,
    RetryPolicy,
    SourceAsset,
    asset,
    evaluate_automation_conditions,
    materialize,
)

from orchestration.assets import fec_classic_slice_partitions, fec_cycle_partitions
from orchestration.definitions import (
    defs,
    fec_committee_flow_evidence_automation_sensor,
)
from orchestration.flow_evidence import (
    _cycle_mapping,
    _master_mapping,
    arango_committee_flow_evidence,
    fec_committee_flow_evidence_bundle,
    fec_committee_flow_evidence_partitions,
    fec_committee_flow_reconciliation,
)
from orchestration.go_process import (
    GoCommandError,
    _decode_and_validate,
    _local_registry,
)
from orchestration.resources import GoPipelineResource

ROOT = Path(__file__).resolve().parent.parent
CONTRACTS = ROOT / "contracts"
CALC = "calculations/fec/committee-flow-reconciliation/v1/result.schema.json"
BUNDLE = "bundles/fec/committee-flow-evidence/v1/manifest.schema.json"
GRAPH = "projections/arango/committee-flow-evidence/v1/result.schema.json"


def fixture(kind):
    schema = json.loads((CONTRACTS / kind).read_text())
    fact = {
        "fact_set_id": "a" * 64,
        "manifest_sha256": "b" * 64,
        "source_release_id": "fec-" + "c" * 64,
        "source_artifact_sha256": "d" * 64,
        "facts": 1,
    }
    inputs = {
        "release_id": fact["source_release_id"],
        "release_sha256": "e" * 64,
        "schedule_a": fact,
        "schedule_b": fact,
    }
    measures = {
        "rows": 1,
        "known_amount_rows": 1,
        "signed_amount_minor_units": "9007199254740993",
    }
    common = {
        "schema_version": schema["properties"]["schema_version"]["const"],
        "cycle": "2026",
    }
    if kind == CALC:
        artifact = {
            "record_count": 1,
            "uncompressed_byte_count": 1,
            "uncompressed_sha256": "b" * 64,
            "compressed_byte_count": 1,
            "compressed_sha256": "c" * 64,
            "compression": "zstd",
            "storage_key": "evidence/" + "a" * 64 + "/fixture.jsonl.zst",
        }
        side = {
            "total": measures,
            "decisions": [],
            "selected": measures,
            "observations": artifact,
        }
        policy = {
            k: v["const"]
            for k, v in schema["properties"]["policy"]["properties"].items()
            if "const" in v
        }
        return {
            **common,
            "calculation_set_id": "1" * 64,
            "state": "complete_candidate_reconciliation",
            "input": inputs,
            "policy": {**policy, "sender_rules": []},
            "schedule_a": side,
            "schedule_b": side,
            "summary": [],
            "assertions": artifact,
            "graph_eligible": False,
        }
    checks = [v["const"] for v in schema["properties"]["checks"]["prefixItems"]]
    if kind == BUNDLE:
        return {
            **common,
            "bundle_id": "2" * 64,
            "state": "ready",
            "consumer": "committee_flow_evidence",
            "calculation": {
                "calculation_set_id": "1" * 64,
                "manifest_sha256": "3" * 64,
            },
            "input": inputs,
            "committee_master": fact,
            "identity_scope": "same_cycle_master_only",
            "economic_flow_eligible": False,
            "checks": checks,
        }
    return {
        **common,
        "projection_id": "4" * 64,
        "database": "lt_flow_evidence_2026_" + "4" * 32,
        "state": "partial",
        "reused": True,
        "bundle_id": "2" * 64,
        "bundle_sha256": "5" * 64,
        "entities": 2,
        "unresolved_same_cycle_masters": 1,
        "schedule_a": measures,
        "schedule_b": measures,
        "components": 1,
        "source_drilldown": [],
        "verified_source_shards": 2,
        "queries": [
            {
                "ledger": side,
                "kind": kind,
                "rows": 1,
                "milliseconds": 1,
                "status": "passed",
            }
            for side in ("schedule_a", "schedule_b")
            for kind in ("paths", "one_sided_lookup")
        ],
        "storage": [
            {
                "collection": name,
                "document_bytes": 1,
                "index_bytes": 1,
                "cache_bytes": 0,
            }
            for name in (
                "entities",
                "receiver_reported_observations",
                "sender_reported_observations",
                "reconciliation_components",
            )
        ],
        "checks": checks,
        "elapsed_seconds": 1,
        "process_peak_rss_bytes": 1,
        "completed_at": "2026-09-08T00:00:00Z",
    }


def validate(value, kind):
    return _decode_and_validate(
        json.dumps(value).encode(), CONTRACTS / kind, None, CONTRACTS
    )


def test_local_contracts_accept_results_and_reject_unverified_output():
    for kind in (CALC, BUNDLE, GRAPH):
        value = fixture(kind)
        assert validate(value, kind) == value
        for key in value:
            bad = copy.deepcopy(value)
            del bad[key]
            with pytest.raises(GoCommandError):
                validate(bad, kind)
    for path, replacement in [
        (("checks",), []),
        (("queries", 0, "ledger"), "combined"),
        (("queries", 0, "status"), "failed"),
        (("schedule_a", "signed_amount_minor_units"), 1.25),
        (("database",), "legal_tender"),
        (("state",), "pending"),
    ]:
        bad = fixture(GRAPH)
        parent = bad
        for key in path[:-1]:
            parent = parent[key]
        parent[path[-1]] = replacement
        with pytest.raises(GoCommandError):
            validate(bad, GRAPH)


def test_reference_registry_is_local_and_rejects_escape(tmp_path, monkeypatch):
    import urllib.request

    monkeypatch.setattr(
        urllib.request, "urlopen", lambda *a, **k: pytest.fail("schema network access")
    )
    from referencing.exceptions import Unresolvable

    registry = _local_registry(tmp_path / "contracts")
    for uri in (
        "https://example.test/contract.json",
        "file:///etc/passwd",
        "https://legal-tender.local/contracts/../outside.json",
    ):
        with pytest.raises(Unresolvable):
            registry.resolver().lookup(uri)
    missing = tmp_path / "schema.json"
    missing.write_text(
        json.dumps({"$ref": "https://legal-tender.local/contracts/missing.json"})
    )
    with pytest.raises(GoCommandError, match="local contracts"):
        _decode_and_validate(b"{}", missing, None, tmp_path / "contracts")


def test_partition_mapping_is_exact_in_both_directions():
    with DagsterInstance.ephemeral() as instance:
        instance.add_dynamic_partitions("fec_cycle", ["2024", "2026"])
        key = MultiPartitionKey({"bundle": "committee-flow-evidence", "cycle": "2026"})
        subset = (
            fec_committee_flow_evidence_partitions.empty_subset().with_partition_keys(
                [key]
            )
        )
        mapped = _master_mapping.get_upstream_mapped_partitions_result_for_partitions(
            subset,
            fec_committee_flow_evidence_partitions,
            fec_classic_slice_partitions,
            dynamic_partitions_store=instance,
        )
        assert set(mapped.partitions_subset.get_partition_keys()) == {
            MultiPartitionKey({"cycle": "2026", "dataset": "committee-master"})
        }
        assert not mapped.required_but_nonexistent_subset.get_partition_keys()
        mapped = _cycle_mapping.get_upstream_mapped_partitions_result_for_partitions(
            subset,
            fec_committee_flow_evidence_partitions,
            fec_cycle_partitions,
            dynamic_partitions_store=instance,
        )
        assert set(mapped.partitions_subset.get_partition_keys()) == {"2026"}
        downstream = _master_mapping.get_downstream_partitions_for_partitions(
            fec_classic_slice_partitions.empty_subset().with_partition_keys(
                [MultiPartitionKey({"cycle": "2026", "dataset": "candidate-master"})]
            ),
            fec_classic_slice_partitions,
            fec_committee_flow_evidence_partitions,
            dynamic_partitions_store=instance,
        )
        assert not downstream.get_partition_keys()
        graph = _cycle_mapping.get_upstream_mapped_partitions_result_for_partitions(
            fec_cycle_partitions.empty_subset().with_partition_keys(["2026"]),
            fec_cycle_partitions,
            fec_committee_flow_evidence_partitions,
            dynamic_partitions_store=instance,
        )
        assert set(graph.partitions_subset.get_partition_keys()) == {key}
    Definitions.validate_loadable(defs)
    assert fec_committee_flow_evidence_automation_sensor.asset_selection.resolve(
        defs.resolve_asset_graph()
    ) == {
        AssetKey("fec_committee_flow_reconciliation"),
        AssetKey("fec_committee_flow_evidence_bundle"),
        AssetKey("arango_committee_flow_evidence"),
    }
    for definition in (
        fec_committee_flow_reconciliation,
        fec_committee_flow_evidence_bundle,
        arango_committee_flow_evidence,
    ):
        assert len(list(definition.check_specs)) == 1
        spec = next(iter(definition.check_specs))
        assert spec.blocking
        assert spec.partitions_def == definition.partitions_def


def test_automation_requires_passed_checks_for_the_mapped_cycle():
    graph_key = arango_committee_flow_evidence.key
    partition = lambda cycle: MultiPartitionKey(
        {"cycle": cycle, "bundle": "committee-flow-evidence"}
    )
    passed = True

    @asset(
        name="fec_committee_flow_evidence_bundle",
        partitions_def=fec_committee_flow_evidence_partitions,
        check_specs=list(fec_committee_flow_evidence_bundle.check_specs),
    )
    def upstream():
        from dagster import Output

        yield Output("pinned-manifest")
        yield AssetCheckResult(passed=passed)

    test_defs = Definitions(
        assets=[upstream, arango_committee_flow_evidence],
        resources=defs.resources,
    )
    with DagsterInstance.ephemeral() as instance:
        instance.add_dynamic_partitions("fec_cycle", ["2024", "2026"])
        cursor = None

        def requested():
            nonlocal cursor
            result = evaluate_automation_conditions(
                defs=test_defs,
                instance=instance,
                asset_selection=AssetSelection.assets(arango_committee_flow_evidence),
                cursor=cursor,
            )
            cursor = result.cursor
            return result.get_requested_partitions(graph_key)

        def checked(cycle, success):
            nonlocal passed
            passed = success
            result = materialize(
                [upstream],
                instance=instance,
                partition_key=partition(cycle),
                raise_on_error=False,
            )
            assert result.success == success
            assert result.get_asset_check_evaluations()[0].partition == partition(cycle)

        assert requested() == set()
        checked("2024", True)
        checked("2026", False)
        assert requested() == {"2024"}
        assert requested() == set()
        checked("2026", True)
        assert requested() == {"2026"}
        checked("2024", False)
        assert requested() == set()
        checked("2024", True)
        assert requested() == {"2024"}


def resource(tmp_path, monkeypatch):
    fixtures = {
        "publish-committee-flow-reconciliation": fixture(CALC),
        "publish-committee-flow-evidence-bundle": fixture(BUNDLE),
        "probe-arango-committee-flow-evidence": fixture(GRAPH),
    }
    source = tmp_path / "results.json"
    source.write_text(json.dumps(fixtures))
    binary = tmp_path / "fake-go"
    binary.write_text("""#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
args = sys.argv[1:]
with Path(os.environ["FLOW_ARGS"]).open("a") as f:
    f.write(json.dumps(args)+"\\n")
payload = json.loads(Path(os.environ["FLOW_RESULTS"]).read_text())[args[2]]
if os.environ.get("FLOW_FAIL_COMMAND") == args[2]:
    sys.stderr.write("source ancestry rejected")
    sys.exit(1)
print(json.dumps(payload))
""")
    binary.chmod(0o750)
    monkeypatch.setenv("FLOW_RESULTS", str(source))
    monkeypatch.setenv("FLOW_ARGS", str(tmp_path / "args.jsonl"))
    monkeypatch.setenv("ARANGO_PASSWORD", "test-secret-not-an-argument")
    return GoPipelineResource(
        binary_path=str(binary),
        artifact_root=str(tmp_path / "control"),
        contracts_root=str(CONTRACTS),
        current_fec_release_manifest="must-not-read",
        storage_root=str(tmp_path / "storage"),
        occurrence_timeout_seconds=10,
    )


def sources():
    @asset(name="fec_schedule_a_facts", partitions_def=fec_cycle_partitions)
    def a():
        return "/pinned/a.json"

    @asset(name="fec_schedule_b_facts", partitions_def=fec_cycle_partitions)
    def b():
        return "/pinned/b.json"

    @asset(name="fec_release_publication")
    def release():
        return "/pinned/release.json"

    @asset(name="fec_classic_facts", partitions_def=fec_classic_slice_partitions)
    def master():
        return "/pinned/master.json"

    return a, b, release, master


def test_assets_forward_exact_inputs_and_stable_versions(tmp_path, monkeypatch):
    r = resource(tmp_path, monkeypatch)
    a, b, release, master = sources()
    with DagsterInstance.ephemeral() as instance:
        instance.add_dynamic_partitions("fec_cycle", ["2026"])
        resources = {"go_pipeline": r}
        calc = materialize(
            [a, b, release, fec_committee_flow_reconciliation],
            resources=resources,
            instance=instance,
            partition_key="2026",
        )
        assert calc.success
        assert calc.get_asset_check_evaluations()[0].passed
        assert calc.get_asset_check_evaluations()[0].partition == "2026"
        assert (
            calc.get_asset_check_evaluations()[0].target_materialization_data.run_id
            == calc.run_id
        )
        materialize(
            [master],
            instance=instance,
            partition_key=MultiPartitionKey(
                {"cycle": "2026", "dataset": "committee-master"}
            ),
        )
        bundle = materialize(
            [
                SourceAsset(
                    fec_committee_flow_reconciliation.key,
                    partitions_def=fec_cycle_partitions,
                ),
                SourceAsset(master.key, partitions_def=fec_classic_slice_partitions),
                fec_committee_flow_evidence_bundle,
            ],
            resources=resources,
            instance=instance,
            partition_key=MultiPartitionKey(
                {"cycle": "2026", "bundle": "committee-flow-evidence"}
            ),
        )
        assert bundle.success and bundle.get_asset_check_evaluations()[0].passed
        assert (
            bundle.get_asset_check_evaluations()[0].target_materialization_data.run_id
            == bundle.run_id
        )
        graph_assets = [
            SourceAsset(
                fec_committee_flow_evidence_bundle.key,
                partitions_def=fec_committee_flow_evidence_partitions,
            ),
            arango_committee_flow_evidence,
        ]
        first = materialize(
            graph_assets, resources=resources, instance=instance, partition_key="2026"
        )
        payloads = json.loads((tmp_path / "results.json").read_text())
        payloads["probe-arango-committee-flow-evidence"]["elapsed_seconds"] = 99
        (tmp_path / "results.json").write_text(json.dumps(payloads))
        second = materialize(
            graph_assets, resources=resources, instance=instance, partition_key="2026"
        )
    assert first.success and second.success
    for execution in (first, second):
        m = execution.asset_materializations_for_node("arango_committee_flow_evidence")[
            0
        ]
        assert m.metadata["state"].value == "partial"
        assert (
            m.metadata["schedule_a"].data["signed_amount_minor_units"]
            == "9007199254740993"
        )
        assert m.tags["dagster/data_version"] == "4" * 64
        assert "source_drilldown" not in m.metadata
        assert execution.get_asset_check_evaluations()[0].passed
        assert execution.get_asset_check_evaluations()[0].partition == "2026"
        assert (
            execution.get_asset_check_evaluations()[
                0
            ].target_materialization_data.run_id
            == execution.run_id
        )
    arguments = [
        json.loads(line) for line in (tmp_path / "args.jsonl").read_text().splitlines()
    ]
    assert arguments[0][arguments[0].index("--release") + 1] == "/pinned/release.json"
    assert (
        arguments[0][arguments[0].index("--schedule-a-facts") + 1] == "/pinned/a.json"
    )
    assert (
        arguments[0][arguments[0].index("--schedule-b-facts") + 1] == "/pinned/b.json"
    )
    assert (
        arguments[1][arguments[1].index("--committee-facts") + 1]
        == "/pinned/master.json"
    )
    assert arguments[1][arguments[1].index("--calculation") + 1] == str(
        tmp_path
        / "storage/calculations/fec/committee-flow-reconciliation/v1/manifests"
        / ("1" * 64 + ".json")
    )
    assert arguments[2][arguments[2].index("--projection-bundle") + 1] == str(
        tmp_path
        / "storage/bundles/fec/committee-flow-evidence/v1/manifests"
        / ("2" * 64 + ".json")
    )
    assert "test-secret-not-an-argument" not in (tmp_path / "args.jsonl").read_text()
    assert "must-not-read" not in (tmp_path / "args.jsonl").read_text()


@pytest.mark.parametrize("failure", ["exit", "schema", "cycle"])
def test_failed_go_result_blocks_materialization_and_consumers(
    tmp_path, monkeypatch, failure
):
    monkeypatch.setattr(RetryPolicy, "calculate_delay", lambda *a, **k: 0)
    r = resource(tmp_path, monkeypatch)
    a, b, release, _ = sources()
    if failure == "exit":
        monkeypatch.setenv("FLOW_FAIL_COMMAND", "publish-committee-flow-reconciliation")
    else:
        payloads = json.loads((tmp_path / "results.json").read_text())
        payloads["publish-committee-flow-reconciliation"][
            "cycle" if failure == "cycle" else "graph_eligible"
        ] = ("2024" if failure == "cycle" else True)
        (tmp_path / "results.json").write_text(json.dumps(payloads))

    @asset(partitions_def=fec_cycle_partitions)
    def consumer(fec_committee_flow_reconciliation):
        pytest.fail("consumer ran after rejected Go result")

    with DagsterInstance.ephemeral() as instance:
        instance.add_dynamic_partitions("fec_cycle", ["2026"])
        result = materialize(
            [a, b, release, fec_committee_flow_reconciliation, consumer],
            resources={"go_pipeline": r},
            instance=instance,
            partition_key="2026",
            raise_on_error=False,
            run_config={"execution": {"config": {"retries": {"disabled": {}}}}},
        )
    assert not result.success
    assert not result.asset_materializations_for_node(
        "fec_committee_flow_reconciliation"
    )
    assert not result.asset_materializations_for_node("consumer")
    assert (
        result.get_asset_check_evaluations()
        and not result.get_asset_check_evaluations()[0].passed
    )
