"""Current public-API and offline smoke coverage for example 08."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from examples.example_08 import bigquery_client, local_validation, runtime_support
from examples.example_08.cli import build_parser
from examples.example_08.runtime_support import Example08Config, NativeGcsWorkflowClient

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
EXAMPLE_ROOT = REPOSITORY_ROOT / "examples" / "example_08"
GCS_ENTRYPOINT = EXAMPLE_ROOT / "08_gcs_csv_modified_window_to_polars_parquet.py"
LOCAL_ENTRYPOINT = EXAMPLE_ROOT / "08_local_csv_directory_to_polars.py"


def _required_cloud_args() -> list[str]:
    """Return the smallest valid cloud CLI argument set."""
    return [
        "--source-csv-prefix",
        "gs://raw/records",
        "--start-date",
        "2026-08-01",
        "--end-date",
        "2026-08-01",
        "--silver-parquet-prefix",
        "gs://silver/records",
        "--partition-timestamp-column",
        "event_timestamp",
        "--parquet-file-prefix",
        "records",
        "--target-table",
        "project.dataset.records",
    ]


@pytest.mark.parametrize(
    ("entrypoint", "expected_option"),
    [
        (GCS_ENTRYPOINT, "--target-table"),
        (LOCAL_ENTRYPOINT, "--output-parquet"),
    ],
)
def test_example_08_cli_help_is_offline(
    entrypoint: Path,
    expected_option: str,
) -> None:
    """Both documented scripts expose help without credentials or network access."""
    completed = subprocess.run(
        [sys.executable, "-B", str(entrypoint), "--help"],
        cwd=REPOSITORY_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert expected_option in completed.stdout
    assert "--multi-threading | --no-multi-threading" in completed.stdout
    assert "--memory-limit-bytes" in completed.stdout


@pytest.mark.parametrize("multi_threading", [False, True], ids=["single", "multi"])
def test_example_08_local_entrypoint_runs_without_cloud_services(
    tmp_path: Path,
    multi_threading: bool,
) -> None:
    """The local CLI reconciles heterogeneous CSVs and writes readable Parquet."""
    source = tmp_path / "csv"
    source.mkdir()
    (source / "a.csv").write_text(
        "record_id,1/Created\nr1,yes\n",
        encoding="utf-8",
    )
    (source / "b.csv").write_text(
        "2/Updated,record_id\nlater,r2\n",
        encoding="utf-8",
    )
    output = tmp_path / "normalized.parquet"

    completed = subprocess.run(
        [
            sys.executable,
            "-B",
            str(LOCAL_ENTRYPOINT),
            str(source),
            "--output-parquet",
            str(output),
            "--memory-limit-bytes",
            str(64 << 20),
            "--multi-threading" if multi_threading else "--no-multi-threading",
        ],
        cwd=REPOSITORY_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert "CSV files: 2" in completed.stdout
    assert "Rows: 2" in completed.stdout
    assert f"Parquet: {output}" in completed.stdout
    inspection = subprocess.run(
        [
            sys.executable,
            "-B",
            "-c",
            (
                "import json,sys; import polars as pl; "
                "rows=pl.read_parquet(sys.argv[1]).select('record_id','event').to_dicts(); "
                "print(json.dumps(rows, sort_keys=True))"
            ),
            str(output),
        ],
        cwd=REPOSITORY_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert inspection.returncode == 0, inspection.stderr
    rows = {row["record_id"]: row for row in json.loads(inspection.stdout)}
    assert rows["r1"]["event"] == [{"event_id": 1, "event_text": "Created", "payload": "yes"}]
    assert rows["r2"]["event"] == [{"event_id": 2, "event_text": "Updated", "payload": "later"}]


def test_local_validation_transfers_frame_and_closes_result(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A caller-owned Polars frame does not leave its Result owner pending."""
    frame = object()
    state = {"closed": False}
    captured: dict[str, Any] = {}

    class ResultDouble:
        """Minimal current Result ownership contract."""

        clean_data = frame

        def close(self) -> None:
            """Record the explicit ownership handoff."""
            state["closed"] = True

    def fake_to_polars(path: Path, **kwargs: Any) -> ResultDouble:
        """Capture the public converter options."""
        captured["path"] = path
        captured.update(kwargs)
        return ResultDouble()

    def fake_normalize(value: object, **kwargs: Any) -> str:
        """Require Result cleanup before downstream Polars work starts."""
        assert state["closed"] is True
        assert value is frame
        captured.update({f"normalize_{key}": item for key, item in kwargs.items()})
        return "normalized"

    monkeypatch.setattr(local_validation.ss, "to_polars", fake_to_polars)
    monkeypatch.setattr(local_validation, "normalize_event_columns_inferred", fake_normalize)

    result = local_validation.load_local_csv_directory_to_polars(
        tmp_path,
        multi_threading=True,
        memory_limit_bytes=32 << 20,
    )

    assert result == "normalized"
    assert captured["path"] == tmp_path
    assert captured["input_format"] == "csv"
    assert captured["csv_header_mode"] == "union"
    assert captured["multi_threading"] is True
    assert captured["memory_limit_bytes"] == 32 << 20


def test_cloud_config_uses_public_resource_options_for_gcs_listing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Listing and conversion share one immutable public resource policy."""
    config = Example08Config(
        source_csv_prefix="gs://raw/records",
        silver_parquet_prefix="gs://silver/records",
        start_date=date(2026, 8, 1),
        end_date=date(2026, 8, 1),
        target_table="project.dataset.records",
        partition_timestamp_column="event_timestamp",
        parquet_file_prefix="records",
        multi_threading=True,
        memory_limit_bytes=64 << 20,
    )
    captured: dict[str, Any] = {}

    def fake_list_objects(source_prefix: str, **kwargs: Any) -> tuple[Any, ...]:
        """Capture the stable sources facade call."""
        captured["source_prefix"] = source_prefix
        captured.update(kwargs)
        return ()

    monkeypatch.setattr(runtime_support.ss.sources, "list_objects", fake_list_objects)
    client = NativeGcsWorkflowClient(resources=config.resources)

    assert (
        client.list_csv_objects(
            config.source_csv_prefix,
            memory_limit_bytes=config.memory_limit_bytes,
        )
        == ()
    )
    assert captured == {
        "source_prefix": "gs://raw/records",
        "suffixes": ("csv",),
        "resources": config.resources,
    }


def test_cloud_config_rejects_ambiguous_resource_values() -> None:
    """Example configuration inherits public bool and positive-budget validation."""
    common: dict[str, Any] = {
        "source_csv_prefix": "gs://raw/records",
        "silver_parquet_prefix": "gs://silver/records",
        "start_date": date(2026, 8, 1),
        "end_date": date(2026, 8, 1),
        "target_table": "project.dataset.records",
        "partition_timestamp_column": "event_timestamp",
        "parquet_file_prefix": "records",
    }
    with pytest.raises(TypeError, match="multi_threading must be a bool"):
        Example08Config(**common, multi_threading="multi")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="memory_limit_bytes must be greater than zero"):
        Example08Config(**common, memory_limit_bytes=True)


def test_cloud_parser_rejects_conflicting_bigquery_credentials() -> None:
    """BigQuery authentication has one unambiguous owner."""
    with pytest.raises(SystemExit):
        build_parser().parse_args(
            [
                *_required_cloud_args(),
                "--credentials-file",
                "service-account.json",
                "--credentials-json",
                "{}",
            ]
        )


def test_bigquery_adapter_closes_query_owners_and_uses_current_ddl_api(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The example retains no ADBC cursor or connection after schema discovery."""
    events: list[str] = []
    executed: dict[str, Any] = {}
    table_ref = SimpleNamespace(sql_identifier="`project.dataset.records`")

    class Cursor:
        """Context-managed ADBC cursor double."""

        def __enter__(self) -> Cursor:
            events.append("cursor-enter")
            return self

        def __exit__(self, *_exc: object) -> None:
            events.append("cursor-exit")

        def execute(self, query: str) -> None:
            executed["schema_query"] = query

        def fetch_arrow_table(self) -> Any:
            return SimpleNamespace(schema="arrow-schema")

    class Connection:
        """Context-managed ADBC connection double."""

        def __enter__(self) -> Connection:
            events.append("connection-enter")
            return self

        def __exit__(self, *_exc: object) -> None:
            events.append("connection-exit")

        def cursor(self) -> Cursor:
            return Cursor()

    dbapi = SimpleNamespace(connect=lambda **_kwargs: Connection())
    monkeypatch.setattr(bigquery_client, "parse_table_ref", lambda *_args, **_kwargs: table_ref)
    monkeypatch.setattr(bigquery_client, "import_bigquery_adbc", lambda: (dbapi, object()))
    monkeypatch.setattr(
        bigquery_client,
        "bigquery_db_kwargs_from_namespace",
        lambda *_args: {"project_id": "project"},
    )
    monkeypatch.setattr(
        bigquery_client,
        "external_table_ddl",
        lambda *_args, **_kwargs: ("CREATE OR REPLACE EXTERNAL TABLE", ()),
    )
    monkeypatch.setattr(
        bigquery_client,
        "execute_bigquery_sql",
        lambda **kwargs: executed.update(kwargs),
    )
    args = argparse.Namespace(target_table="project.dataset.records", bigquery_project=None)
    client = bigquery_client.AdbcBigQueryWorkflowClient(args)

    assert client.read_target_schema(args.target_table) == "arrow-schema"
    assert events == [
        "connection-enter",
        "cursor-enter",
        "cursor-exit",
        "connection-exit",
    ]
    assert executed["schema_query"] == "SELECT * FROM `project.dataset.records` LIMIT 0"

    client.replace_external_table(
        args.target_table,
        source_uri_pattern="gs://silver/records/*",
        hive_uri_prefix="gs://silver/records",
        partition_columns=(("year", "INT64"),),
        reference_file_schema_uri="gs://silver/records/year=2026/reference.parquet",
        final_schema="arrow-schema",
    )
    assert executed["dbapi"] is dbapi
    assert executed["db_kwargs"] == {"project_id": "project"}
    assert executed["query"] == "CREATE OR REPLACE EXTERNAL TABLE"


def test_example_08_only_imports_documented_schema_sanitizer_namespaces() -> None:
    """Example code must not depend on implementation-package layout."""
    source = "\n".join(
        path.read_text(encoding="utf-8") for path in sorted(EXAMPLE_ROOT.glob("*.py"))
    )
    for private_namespace in (
        "schema_sanitizer.api_impl",
        "schema_sanitizer.core_impl",
        "schema_sanitizer.input_impl",
        "schema_sanitizer.options_impl",
        "schema_sanitizer.remote_impl",
    ):
        assert private_namespace not in source
