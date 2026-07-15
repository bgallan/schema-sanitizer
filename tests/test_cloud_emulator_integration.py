"""End-to-end SDK interoperability tests against local cloud emulators."""

from __future__ import annotations

from pathlib import Path

import pytest

AZURITE_API_VERSION = "2025-07-05"


@pytest.fixture(autouse=True)
def _require_cloud_emulators(pytestconfig: pytest.Config, monkeypatch: pytest.MonkeyPatch) -> None:
    """Configure emulator clients exclusively from explicit pytest options."""
    if not pytestconfig.getoption("--run-cloud-emulators"):
        pytest.skip("cloud emulators are not configured")

    from azure.storage.blob.aio import BlobServiceClient
    from botocore.config import Config

    from schema_sanitizer.remote_impl.providers import azure, gcs, s3

    s3_endpoint = str(pytestconfig.getoption("--s3-emulator-endpoint")).strip()
    azure_connection = str(pytestconfig.getoption("--azure-emulator-connection-string")).strip()
    gcs_endpoint = str(pytestconfig.getoption("--gcs-emulator-endpoint")).strip()
    if not s3_endpoint or not azure_connection or not gcs_endpoint:
        pytest.fail("all cloud emulator endpoint options are required")

    monkeypatch.setattr(
        s3,
        "client_options",
        lambda: {
            "endpoint_url": s3_endpoint,
            "region_name": "us-east-1",
            "aws_access_key_id": "minioadmin",
            "aws_secret_access_key": "minioadmin",  # pragma: allowlist secret
            "config": Config(s3={"addressing_style": "path"}),
        },
    )

    async def open_azurite_service(_ref: object) -> object:
        """Open Azurite from the explicit connection string."""
        return BlobServiceClient.from_connection_string(
            azure_connection,
            api_version=AZURITE_API_VERSION,
        )

    monkeypatch.setattr(azure, "open_service", open_azurite_service)
    monkeypatch.setattr(gcs, "api_base", lambda: gcs_endpoint)
    monkeypatch.setattr(gcs, "access_token", lambda: "emulator-token")


def _write_sources(root: Path) -> tuple[Path, Path]:
    """Write two source documents used by every emulator pipeline."""
    first = root / "a.json"
    second = root / "b.json"
    first.write_text('{"id":1,"name":"alpha"}\n', encoding="utf-8")
    second.write_text('{"id":2,"name":"beta"}\n', encoding="utf-8")
    return first, second


def _assert_remote_conversion(
    tmp_path: Path,
    *,
    input_uri: str,
    output_uri: str,
    upload,
    list_files,
    file_exists,
    download_file,
) -> None:
    """Run and verify one remote-directory to remote-Parquet conversion."""
    import pyarrow.parquet as pq

    import schema_sanitizer as ss
    from schema_sanitizer.remote_impl.transport import run_sync

    first, second = _write_sources(tmp_path)
    run_sync(upload(str(first), f"{input_uri}a.json"))
    run_sync(upload(str(second), f"{input_uri}b.json"))

    discovered = run_sync(list_files(input_uri, (".json",)))
    assert [item.name for item in discovered] == ["a.json", "b.json"]

    ss.to_parquet(
        input_uri,
        output_uri,
        input_format="json",
        input_mode="directory",
        parquet_compression=None,
    )
    assert run_sync(file_exists(output_uri)) is True

    local_output = tmp_path / "result.parquet"
    run_sync(download_file(output_uri, str(local_output)))
    table = pq.read_table(local_output)
    assert table.num_rows == 2
    assert table.column("id").to_pylist() == [1, 2]


def test_minio_remote_directory_conversion(tmp_path: Path) -> None:
    """MinIO exercises the real aiobotocore S3 client and remote pipeline."""
    from schema_sanitizer.remote_impl.providers import s3
    from schema_sanitizer.remote_impl.transport import run_sync

    bucket = "schema-sanitizer-integration"

    async def create_bucket() -> None:
        """Create the MinIO test bucket through the real S3 client."""
        async with await s3.open_client() as client:
            await client.create_bucket(Bucket=bucket)

    run_sync(create_bucket())
    _assert_remote_conversion(
        tmp_path,
        input_uri=f"s3://{bucket}/input/",
        output_uri=f"s3://{bucket}/output/result.parquet",
        upload=s3.upload_file,
        list_files=s3.list_files,
        file_exists=s3.file_exists,
        download_file=s3.download_file,
    )


def test_azurite_remote_directory_conversion(tmp_path: Path) -> None:
    """Azurite exercises the real async Azure Blob SDK and remote pipeline."""
    from schema_sanitizer.remote_impl.providers import azure
    from schema_sanitizer.remote_impl.transport import run_sync

    container = "schema-sanitizer-integration"
    base_uri = f"az://devstoreaccount1/{container}/"

    async def create_container() -> None:
        """Create the Azurite test container through the real Azure client."""
        service = await azure.open_service(azure.parse_uri(f"{base_uri}seed"))
        try:
            await service.create_container(container)
        finally:
            await service.close()

    run_sync(create_container())
    _assert_remote_conversion(
        tmp_path,
        input_uri=f"{base_uri}input/",
        output_uri=f"{base_uri}output/result.parquet",
        upload=azure.upload_file,
        list_files=azure.list_files,
        file_exists=azure.file_exists,
        download_file=azure.download_file,
    )


def test_fake_gcs_remote_directory_conversion(tmp_path: Path) -> None:
    """fake-gcs-server exercises JSON API requests without ADC credentials."""
    from schema_sanitizer.remote_impl.providers import gcs
    from schema_sanitizer.remote_impl.transport import open_aiohttp_session, run_sync

    bucket = "schema-sanitizer-integration"

    async def create_bucket() -> None:
        """Create the fake-GCS bucket through its JSON API."""
        url = f"{gcs.api_base()}/storage/v1/b"
        async with await open_aiohttp_session(gcs.request_headers()) as session:
            async with session.post(
                url, params={"project": "test"}, json={"name": bucket}
            ) as response:
                body = await response.text()
                assert response.status in {200, 201}, body

    run_sync(create_bucket())
    _assert_remote_conversion(
        tmp_path,
        input_uri=f"gs://{bucket}/input/",
        output_uri=f"gs://{bucket}/output/result.parquet",
        upload=gcs.upload_file,
        list_files=gcs.list_directory,
        file_exists=gcs.file_exists,
        download_file=gcs.download_file,
    )
