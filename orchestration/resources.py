"""Dagster resources at the Go process boundary."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from dagster import ConfigurableResource

from orchestration.go_process import StoredCommandResult, run_json_command

_FEC_INVENTORY_CONTRACT_VERSIONS = {
    "legal-tender.fec.initial-release-inventory.v1": "v1",
    "legal-tender.fec.initial-release-inventory.v2": "v2",
    "legal-tender.fec.initial-release-inventory.v3": "v3",
    "legal-tender.fec.initial-release-inventory.v4": "v4",
}


class GoPipelineResource(ConfigurableResource):
    """Configuration for the one shared local Go process adapter."""

    binary_path: str
    artifact_root: str
    contracts_root: str
    current_fec_release_manifest: str
    storage_root: str
    timeout_seconds: int = 300
    acquisition_timeout_seconds: int = 86400
    staging_timeout_seconds: int = 86400
    publication_timeout_seconds: int = 86400
    occurrence_timeout_seconds: int = 86400

    def execute(
        self,
        *,
        arguments: Sequence[str],
        schema_name: str,
        artifact_kind: str,
        timeout_seconds: int | None = None,
    ) -> StoredCommandResult:
        if "/" in schema_name:
            schema_path = Path(self.contracts_root) / schema_name
            schema_path_resolver = None
        else:
            schema_path = (
                Path(self.contracts_root) / "releases" / "fec" / "v1" / schema_name
            )
            schema_path_resolver = self._fec_release_schema_resolver(schema_name)
        return run_json_command(
            binary_path=self.binary_path,
            arguments=arguments,
            schema_path=schema_path,
            artifact_root=Path(self.artifact_root),
            artifact_kind=artifact_kind,
            timeout_seconds=(
                self.timeout_seconds if timeout_seconds is None else timeout_seconds
            ),
            schema_path_resolver=schema_path_resolver,
            contracts_root=Path(self.contracts_root),
        )

    def _fec_release_schema_resolver(self, schema_name: str):
        contracts_root = Path(self.contracts_root)

        def resolve(payload: dict[str, object]) -> Path:
            inventory_version = payload.get("inventory_version")
            contract_version = (
                _FEC_INVENTORY_CONTRACT_VERSIONS.get(inventory_version)
                if isinstance(inventory_version, str)
                else None
            )
            if contract_version is None:
                raise ValueError(
                    f"unknown FEC inventory version in Go result: {inventory_version!r}"
                )
            return contracts_root / "releases" / "fec" / contract_version / schema_name

        return resolve

    def current_manifest_path(self) -> Path | None:
        if not self.current_fec_release_manifest:
            return None
        path = Path(self.current_fec_release_manifest)
        if not path.exists():
            return None
        if not path.is_file():
            raise ValueError(f"current FEC release manifest is not a file: {path}")
        return path

    def current_manifest_target_path(self) -> Path | None:
        if not self.current_fec_release_manifest:
            return None
        return Path(self.current_fec_release_manifest)
