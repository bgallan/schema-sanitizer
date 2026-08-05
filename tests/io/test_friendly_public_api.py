"""Contracts for the configured public facade and source models."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from conftest import require_native

import schema_sanitizer as ss
from schema_sanitizer import sources
from schema_sanitizer.remote_impl import sync_backend


def test_configured_sanitizer_executes_real_conversion(tmp_path: Path) -> None:
    """The friendly facade reaches the native engine without an adapter-specific path."""
    require_native()
    pytest.importorskip("pyarrow")
    path = tmp_path / "rows.csv"
    path.write_text("id,name\n1,Ada\n", encoding="utf-8")
    sanitizer = ss.Sanitizer(
        ss.SanitizeOptions(
            input_format="csv",
            parsing=ss.ParsingOptions(integers=True),
        )
    )

    row = sanitizer.to_pyarrow(path).clean_data.to_pylist()[0]

    assert row["id"] == 1
    assert row["name"] == "Ada"


def test_configured_sanitizer_reuses_nested_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The facade translates reusable options into the existing converter contract."""
    captured: dict[str, object] = {}

    def convert(input_path: object, **kwargs: object) -> object:
        captured["input_path"] = input_path
        captured.update(kwargs)
        return SimpleNamespace(clean_data="frame")

    monkeypatch.setattr("schema_sanitizer.api_impl.analytical.to_polars", convert)
    sanitizer = ss.Sanitizer(
        ss.SanitizeOptions(
            input_format="csv",
            csv=ss.CsvOptions(delimiter=";", header_mode="union"),
            parsing=ss.ParsingOptions(iso_dates=True),
            resources=ss.ResourceOptions(
                multi_threading=True,
                memory_limit_bytes=1024,
            ),
        )
    )

    result = sanitizer.to_polars("rows", schema_registry={"generation": 1})

    assert result.clean_data == "frame"
    assert captured["input_path"] == "rows"
    assert captured["input_format"] == "csv"
    assert captured["csv_delimiter"] == ";"
    assert captured["csv_header_mode"] == "union"
    assert captured["parse_iso_dates"] is True
    assert captured["multi_threading"] is True
    assert captured["memory_limit_bytes"] == 1024
    assert captured["schema_registry"] == {"generation": 1}


def test_source_facade_builds_exact_modified_time_manifest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Public discovery filters a stable GCS listing without exposing internals."""
    before = ss.RemoteFile(
        "gs://bucket/raw/before.csv",
        "before.csv",
        updated=datetime(2026, 1, 1, tzinfo=UTC),
        generation="1",
    )
    selected = ss.RemoteFile(
        "gs://bucket/raw/selected.csv",
        "selected.csv",
        updated=datetime(2026, 1, 2, 12, tzinfo=UTC),
        generation="2",
    )
    monkeypatch.setattr(sources, "list_objects", lambda *_args, **_kwargs: (before, selected))

    manifest = sources.discover(
        "gs://bucket/raw",
        modified_between=(
            datetime(2026, 1, 2, tzinfo=UTC),
            datetime(2026, 1, 3, tzinfo=UTC),
        ),
    )

    assert manifest.files == (selected,)
    assert manifest.content_identities == ((selected.uri, "2"),)


def test_public_file_publication_delegates_to_safe_backend(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The public operation publishes a closed local file and reports its size."""
    path = tmp_path / "day.parquet"
    path.write_bytes(b"parquet")
    calls: list[tuple[str, str, int | None]] = []
    monkeypatch.setattr(
        sync_backend,
        "upload_file",
        lambda local, uri, *, memory_limit_bytes: calls.append((local, uri, memory_limit_bytes)),
    )

    size = sources.publish_file_atomic(
        path,
        "gs://bucket/output/day.parquet",
        memory_limit_bytes=4096,
    )

    assert size == 7
    assert calls == [(str(path), "gs://bucket/output/day.parquet", 4096)]
