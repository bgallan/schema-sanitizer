"""Public immutable ``SourceManifest`` input contracts."""

from __future__ import annotations

import inspect
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

import schema_sanitizer as ss
from schema_sanitizer.api_impl.input.manifest_preparation import (
    prepare_source_manifest_input,
)
from schema_sanitizer.api_impl.source_manifest_diagnostics import (
    patch_source_manifest_diagnostics,
)
from schema_sanitizer.api_impl.streams import diagnostics_stats
from schema_sanitizer.input_impl.prepared import PreparedPublicInput
from schema_sanitizer.sources import RemoteFile, SourceManifest
from schema_sanitizer.sources.models import PublicInput


def _file(name: str, generation: str, *, uri: str | None = None) -> RemoteFile:
    """Return one immutable fake GCS CSV object."""
    return RemoteFile(
        uri=uri or f"gs://bucket/events/{name}",
        name=name,
        size=7,
        updated=datetime(2026, 8, 1, tzinfo=UTC),
        generation=generation,
    )


def test_source_manifest_is_exported_and_public_inputs_are_typed() -> None:
    """The public package and every converter expose the manifest input type."""
    assert ss.SourceManifest is SourceManifest
    assert PublicInput is not None
    functions = (
        ss.iter_batches,
        ss.to_pyarrow,
        ss.to_pandas,
        ss.to_polars,
        ss.to_duckdb,
        ss.to_csv,
        ss.to_jsonl,
        ss.to_parquet,
    )
    for function in functions:
        annotation = inspect.signature(function).parameters["input_path"].annotation
        assert "PublicInput" in str(annotation)


def test_manifest_rejects_unsupported_or_unversioned_entries() -> None:
    """Only versioned objects from the declared supported prefix are usable."""
    with pytest.raises(ValueError, match="supported remote URI"):
        SourceManifest("/tmp/events", [])
    with pytest.raises(ValueError, match="only versioned GCS"):
        SourceManifest("s3://bucket/events", [])
    with pytest.raises(ValueError, match="immutable object generation"):
        SourceManifest(
            "gs://bucket/events",
            [RemoteFile("gs://bucket/events/a.csv", "a.csv")],
        )
    with pytest.raises(ValueError, match="same supported filesystem"):
        SourceManifest(
            "gs://bucket/events",
            [_file("a.csv", "1", uri="s3://bucket/events/a.csv")],
        )
    with pytest.raises(ValueError, match="outside the declared source prefix"):
        SourceManifest(
            "gs://bucket/events",
            [_file("a.csv", "1", uri="gs://bucket/other/a.csv")],
        )


def test_manifest_preparation_uses_exact_objects_without_relisting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Preparation forwards only frozen identities and never resolves a prefix."""
    manifest = SourceManifest(
        "gs://bucket/events",
        [_file("same.csv", "9"), _file("same.csv", "2")],
    )
    captured: list[RemoteFile] = []

    def prepared_from_files(files: list[RemoteFile], *_args: object, **_kwargs: object):
        """Capture the exact manifest objects passed to preparation."""
        captured.extend(files)
        return PreparedPublicInput(object(), "csv", "stream")

    monkeypatch.setattr(
        "schema_sanitizer.api_impl.input.manifest_preparation."
        "remote_native_directory_prepared_from_files",
        prepared_from_files,
    )

    prepared = prepare_source_manifest_input(
        manifest,
        input_format="csv",
        input_mode="single_file",
        input_text_encoding="utf-8",
        xml_row_tag=None,
        csv_delimiter=",",
        csv_has_header=True,
        memory_limit_bytes=None,
        threading_mode="single",
        operation_context=None,
    )

    assert prepared.source_manifest is manifest
    assert [file.content_identity for file in captured] == list(manifest.content_identities)
    assert len({file.name for file in captured}) == 2
    assert all(file.name.endswith("-same.csv") for file in captured)


def test_parquet_manifest_reuses_staging_and_cleanup_lifecycle(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Parquet manifests stage exact generations and retain bounded cleanup."""
    manifest = SourceManifest(
        "gs://bucket/events",
        [_file("a.parquet", "21")],
    )
    staged_dir = tmp_path / "staged"
    staged_dir.mkdir()
    closed: list[str] = []
    captured: list[tuple[str, str | None]] = []

    class _Keepalive:
        """Record closure of one staged or prepared resource."""

        def __init__(self, name: str) -> None:
            """Store the resource label used by the assertion."""
            self.name = name

        def close(self) -> None:
            """Append this resource to the observed close order."""
            closed.append(self.name)

    staged = SimpleNamespace(
        path=staged_dir,
        source_file_by_name={"00000000-a.parquet": "gs://bucket/events/a.parquet"},
        close=_Keepalive("staging").close,
    )

    def stage(files: list[RemoteFile], **_kwargs: object):
        """Capture staged identities and return the local directory double."""
        captured.extend(file.content_identity for file in files)
        assert [file.name for file in files] == ["00000000-a.parquet"]
        return staged

    def prepare(*_args: object, **kwargs: object) -> PreparedPublicInput:
        """Return a prepared Parquet input tied to the staged provenance."""
        assert kwargs["source_file_by_name"] == staged.source_file_by_name
        return PreparedPublicInput(object(), "parquet", "directory", _Keepalive("prepared"))

    monkeypatch.setattr(
        "schema_sanitizer.api_impl.input.manifest_preparation."
        "remote_staging.stage_remote_files_to_directory",
        stage,
    )
    monkeypatch.setattr(
        "schema_sanitizer.api_impl.input.manifest_preparation.prepare_directory",
        prepare,
    )

    prepared = prepare_source_manifest_input(
        manifest,
        input_format="parquet",
        input_mode="directory",
        input_text_encoding="utf-8",
        xml_row_tag=None,
        csv_delimiter=",",
        csv_has_header=True,
        memory_limit_bytes=None,
        threading_mode="single",
        operation_context=None,
    )

    assert captured == list(manifest.content_identities)
    assert prepared.source_manifest is manifest
    prepared.close()
    assert closed == ["staging", "prepared"]


def test_manifest_can_be_reused_without_identity_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Inference and materialization preparations retain identical generations."""
    manifest = SourceManifest(
        "gs://bucket/events",
        [_file("a.csv", "10"), _file("b.csv", "11")],
    )
    calls: list[tuple[tuple[str, str | None], ...]] = []

    def prepared_from_files(files: list[RemoteFile], *_args: object, **_kwargs: object):
        """Record identities for each reuse of the immutable manifest."""
        calls.append(tuple(file.content_identity for file in files))
        return PreparedPublicInput(object(), "csv", "stream")

    monkeypatch.setattr(
        "schema_sanitizer.api_impl.input.manifest_preparation."
        "remote_native_directory_prepared_from_files",
        prepared_from_files,
    )

    kwargs = dict(
        input_format="csv",
        input_mode="directory",
        input_text_encoding="utf-8",
        xml_row_tag=None,
        csv_delimiter=",",
        csv_has_header=True,
        memory_limit_bytes=None,
        threading_mode="single",
        operation_context=None,
    )
    first = prepare_source_manifest_input(manifest, **kwargs)
    second = prepare_source_manifest_input(manifest, **kwargs)

    assert first.source_manifest is second.source_manifest is manifest
    assert calls == [manifest.content_identities, manifest.content_identities]


def test_manifest_conversion_rejects_empty_or_wrong_suffix() -> None:
    """Planning may retain empty days, but conversion requires matching files."""
    empty = SourceManifest("gs://bucket/events", [])
    with pytest.raises(ValueError, match="contains no remote objects"):
        prepare_source_manifest_input(
            empty,
            input_format="csv",
            input_mode="single_file",
            input_text_encoding="utf-8",
            xml_row_tag=None,
            csv_delimiter=",",
            csv_has_header=True,
            memory_limit_bytes=None,
            threading_mode="single",
            operation_context=None,
        )

    wrong = SourceManifest(
        "gs://bucket/events",
        [_file("a.jsonl", "1")],
    )
    with pytest.raises(ValueError, match="requires extension .csv"):
        prepare_source_manifest_input(
            wrong,
            input_format="csv",
            input_mode="single_file",
            input_text_encoding="utf-8",
            xml_row_tag=None,
            csv_delimiter=",",
            csv_has_header=True,
            memory_limit_bytes=None,
            threading_mode="single",
            operation_context=None,
        )


def test_non_manifest_diagnostics_keep_the_existing_shape() -> None:
    """Ordinary inputs do not acquire manifest-only statistics keys."""
    stats = diagnostics_stats(SimpleNamespace())

    assert "source_manifest_uri" not in stats
    assert "source_object_count" not in stats
    assert "source_objects" not in stats


def test_manifest_uri_and_generation_are_exposed_in_diagnostics() -> None:
    """Public stats retain the complete deterministic source identity list."""
    manifest = SourceManifest(
        "gs://bucket/events",
        [_file("b.csv", "2"), _file("a.csv", "1")],
    )
    diagnostics = SimpleNamespace()
    target = SimpleNamespace(_raw=SimpleNamespace(diagnostics=diagnostics))

    patch_source_manifest_diagnostics(target, manifest)
    stats = diagnostics_stats(diagnostics)

    assert stats["source_manifest_uri"] == manifest.source_uri
    assert stats["source_object_count"] == 2
    assert stats["source_objects"] == [
        {"uri": "gs://bucket/events/a.csv", "generation": "1"},
        {"uri": "gs://bucket/events/b.csv", "generation": "2"},
    ]


def test_existing_local_file_input_keeps_manifest_stats_absent(tmp_path: Path) -> None:
    """The manifest integration does not alter ordinary local conversions."""
    source = tmp_path / "local.csv"
    output = tmp_path / "local.jsonl"
    source.write_text("id\n1\n", encoding="utf-8")

    result = ss.to_jsonl(source, output, input_format="csv")

    assert output.is_file()
    assert "source_manifest_uri" not in result.stats
    assert "source_object_count" not in result.stats
    assert "source_objects" not in result.stats


def test_file_converter_consumes_only_manifest_versions_without_listing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The public file route stages exact generations and preserves source_file."""
    manifest = SourceManifest(
        "gs://bucket/events",
        [_file("a.csv", "1"), _file("b.csv", "2")],
    )
    downloaded: list[tuple[str, str | None]] = []

    def fail_listing(*_args: object, **_kwargs: object):
        """Fail if manifest conversion attempts remote discovery."""
        raise AssertionError("a supplied SourceManifest must never relist its prefix")

    def download(files: list[RemoteFile], directory: str, **_kwargs: object) -> None:
        """Materialize the exact selected generations into staging."""
        for file in files:
            downloaded.append(file.content_identity)
            row_id = "1" if file.generation == "1" else "2"
            Path(directory, file.name).write_text(f"id\n{row_id}\n", encoding="utf-8")

    monkeypatch.setattr(
        "schema_sanitizer.remote_impl.sync_backend.list_remote_directory", fail_listing
    )
    monkeypatch.setattr(
        "schema_sanitizer.remote_impl.sync_backend.download_files_to_directory", download
    )

    output = tmp_path / "manifest.jsonl"
    result = ss.to_jsonl(manifest, output, input_format="csv")
    rows = [__import__("json").loads(line) for line in output.read_text().splitlines()]

    assert downloaded == list(manifest.content_identities)
    assert [row["id"] for row in rows] == ["1", "2"]
    assert [row["source_file"] for row in rows] == [
        "gs://bucket/events/a.csv",
        "gs://bucket/events/b.csv",
    ]
    assert result.stats["source_objects"] == [
        {"uri": "gs://bucket/events/a.csv", "generation": "1"},
        {"uri": "gs://bucket/events/b.csv", "generation": "2"},
    ]


def test_analytical_converter_accepts_manifest_and_attaches_diagnostics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Analytical conversion shares preparation and immutable diagnostics."""
    from schema_sanitizer.api_impl.results import Result

    manifest = SourceManifest("gs://bucket/events", [_file("a.csv", "7")])

    def download(files: list[RemoteFile], directory: str, **_kwargs: object) -> None:
        """Stage the analytical manifest while checking its identity list."""
        assert [file.content_identity for file in files] == list(manifest.content_identities)
        Path(directory, files[0].name).write_text("id\n1\n", encoding="utf-8")

    def materialize(opened, *, target: str, threading_mode: str):
        """Return a lightweight analytical result after closing the stream."""
        assert target == "polars"
        assert threading_mode == "single"
        opened.close()
        return Result(SimpleNamespace(diagnostics=SimpleNamespace()), clean_data="frame")

    monkeypatch.setattr(
        "schema_sanitizer.remote_impl.sync_backend.download_files_to_directory", download
    )
    monkeypatch.setattr(
        "schema_sanitizer.api_impl.analytical.materialize_opened_registry_stream", materialize
    )

    result = ss.to_polars(manifest, input_format="csv")

    assert result.clean_data == "frame"
    assert result.stats["source_manifest_uri"] == manifest.source_uri
    assert result.stats["source_objects"] == [
        {"uri": "gs://bucket/events/a.csv", "generation": "7"}
    ]


def test_source_manifest_owners_and_documentation_remain_bounded() -> None:
    """Manifest input responsibilities remain cohesive and publicly documented."""
    root = Path(__file__).resolve().parents[2]
    owners = (
        root / "src/schema_sanitizer/sources/models.py",
        root / "src/schema_sanitizer/api_impl/input/manifest_preparation.py",
        root / "src/schema_sanitizer/api_impl/source_manifest_diagnostics.py",
    )

    assert all(owner.is_file() for owner in owners)
    assert all(len(owner.read_text(encoding="utf-8").splitlines()) <= 500 for owner in owners)
    guide = root / "docs/reference/inputs-and-filesystems.md"
    assert guide.is_file()
    assert "SourceManifest" in guide.read_text(encoding="utf-8")
    assert "reference/inputs-and-filesystems.md" in (root / "docs/README.md").read_text(
        encoding="utf-8"
    )
