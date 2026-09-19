"""Run one Go pipeline command and preserve its contract-validated result."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import tempfile
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import SchemaError, ValidationError
from referencing import Registry, Resource
from referencing.exceptions import NoSuchResource, Unresolvable


@dataclass(frozen=True)
class StoredCommandResult:
    """An opaque Go result plus its immutable control-artifact identity."""

    payload: dict[str, Any]
    artifact_path: Path
    sha256: str
    diagnostics: str


class GoCommandError(RuntimeError):
    """A stable adapter failure safe to expose through Dagster."""

    def __init__(
        self, message: str, *, result: StoredCommandResult | None = None
    ) -> None:
        super().__init__(message)
        self.result = result


def run_json_command(
    *,
    binary_path: str,
    arguments: Sequence[str],
    schema_path: Path,
    artifact_root: Path,
    artifact_kind: str,
    timeout_seconds: int,
    schema_path_resolver: Callable[[dict[str, Any]], Path] | None = None,
    contracts_root: Path | None = None,
) -> StoredCommandResult:
    """Execute a Go command without a shell and store valid stdout by digest."""

    if timeout_seconds <= 0:
        raise GoCommandError("Go command timeout must be greater than zero")

    try:
        completed = subprocess.run(
            [binary_path, *arguments],
            capture_output=True,
            check=False,
            timeout=timeout_seconds,
        )
    except FileNotFoundError as error:
        raise GoCommandError(f"Go pipeline binary not found: {binary_path}") from error
    except PermissionError as error:
        raise GoCommandError(
            f"Go pipeline binary is not executable: {binary_path}"
        ) from error
    except subprocess.TimeoutExpired as error:
        raise GoCommandError(
            f"Go pipeline command exceeded its {timeout_seconds}-second timeout"
        ) from error

    diagnostics = completed.stderr.decode("utf-8", errors="replace").strip()
    if completed.returncode != 0:
        detail = f": {diagnostics}" if diagnostics else ""
        failed_result: StoredCommandResult | None = None
        if completed.stdout:
            try:
                failed_result = _validated_stored_result(
                    content=completed.stdout,
                    diagnostics=diagnostics,
                    schema_path=schema_path,
                    schema_path_resolver=schema_path_resolver,
                    artifact_root=artifact_root,
                    artifact_kind=artifact_kind,
                    contracts_root=contracts_root,
                )
            except GoCommandError as result_error:
                detail += f"; invalid failure result: {result_error}"
        raise GoCommandError(
            f"Go pipeline command exited with code {completed.returncode}{detail}",
            result=failed_result,
        )

    return _validated_stored_result(
        content=completed.stdout,
        diagnostics=diagnostics,
        schema_path=schema_path,
        schema_path_resolver=schema_path_resolver,
        artifact_root=artifact_root,
        artifact_kind=artifact_kind,
        contracts_root=contracts_root,
    )


def _validated_stored_result(
    *,
    content: bytes,
    diagnostics: str,
    schema_path: Path,
    schema_path_resolver: Callable[[dict[str, Any]], Path] | None,
    artifact_root: Path,
    artifact_kind: str,
    contracts_root: Path | None = None,
) -> StoredCommandResult:
    payload = _decode_and_validate(
        content, schema_path, schema_path_resolver, contracts_root
    )
    digest = hashlib.sha256(content).hexdigest()
    artifact_path = _store_immutable_result(
        artifact_root=artifact_root,
        artifact_kind=artifact_kind,
        digest=digest,
        content=content,
    )
    return StoredCommandResult(
        payload=payload,
        artifact_path=artifact_path,
        sha256=digest,
        diagnostics=diagnostics,
    )


def _decode_and_validate(
    content: bytes,
    schema_path: Path,
    schema_path_resolver: Callable[[dict[str, Any]], Path] | None,
    contracts_root: Path | None = None,
) -> dict[str, Any]:
    try:
        payload = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise GoCommandError(
            f"Go pipeline stdout is not one JSON document: {error}"
        ) from error
    if not isinstance(payload, dict):
        raise GoCommandError("Go pipeline stdout must be a JSON object")

    if schema_path_resolver is not None:
        try:
            schema_path = schema_path_resolver(payload)
        except (TypeError, ValueError) as error:
            raise GoCommandError(
                f"cannot select pipeline result schema: {error}"
            ) from error

    try:
        with schema_path.open("r", encoding="utf-8") as schema_file:
            schema = json.load(schema_file)
        validator = Draft202012Validator(
            schema,
            format_checker=FormatChecker(),
            registry=_local_registry(contracts_root),
        )
        validator.validate(payload)
    except FileNotFoundError as error:
        raise GoCommandError(
            f"pipeline result schema not found: {schema_path}"
        ) from error
    except (UnicodeDecodeError, json.JSONDecodeError, SchemaError) as error:
        raise GoCommandError(
            f"pipeline result schema is invalid: {schema_path}: {error}"
        ) from error
    except ValidationError as error:
        location = ".".join(str(part) for part in error.absolute_path) or "<root>"
        raise GoCommandError(
            f"Go pipeline result violates {schema_path.name} at {location}: {error.message}"
        ) from error
    except Unresolvable as error:
        raise GoCommandError(
            "pipeline schema reference is not available in local contracts"
        ) from error
    return payload


def _local_registry(contracts_root: Path | None) -> Registry:
    """Resolve pinned contract IDs locally. Never fetch schemas over HTTP."""
    root = contracts_root.resolve() if contracts_root is not None else None
    prefix = "https://legal-tender.local/contracts/"

    def retrieve(uri: str) -> Resource:
        if root is None or not uri.startswith(prefix):
            raise NoSuchResource(ref=uri)
        path = (root / uri[len(prefix) :]).resolve()
        if not path.is_relative_to(root):
            raise NoSuchResource(ref=uri)
        try:
            contents = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(contents, dict) or contents.get("$id") != uri:
                raise NoSuchResource(ref=uri)
            return Resource.from_contents(contents)
        except (OSError, ValueError) as error:
            raise NoSuchResource(ref=uri) from error

    return Registry(retrieve=retrieve)


def _store_immutable_result(
    *, artifact_root: Path, artifact_kind: str, digest: str, content: bytes
) -> Path:
    if not artifact_kind or any(
        part in {"", ".", ".."} for part in artifact_kind.split("/")
    ):
        raise GoCommandError(f"invalid control-artifact kind: {artifact_kind!r}")

    destination_dir = artifact_root.joinpath(*artifact_kind.split("/"))
    destination_dir.mkdir(mode=0o750, parents=True, exist_ok=True)
    destination = destination_dir / f"{digest}.json"
    if destination.exists():
        if destination.read_bytes() != content:
            raise GoCommandError(f"control-artifact digest collision at {destination}")
        return destination

    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", prefix=".pending-", dir=destination_dir, delete=False
        ) as temporary_file:
            temporary_path = Path(temporary_file.name)
            temporary_file.write(content)
            temporary_file.flush()
            os.fsync(temporary_file.fileno())
        os.chmod(temporary_path, 0o640)
        os.replace(temporary_path, destination)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
    return destination
