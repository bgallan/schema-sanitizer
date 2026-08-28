#!/usr/bin/env python3
"""Benchmark single/multi remote pipelines against explicit local emulators.

It creates deterministic sources, configures S3, GCS, and Azure emulators, and compares
single and multi pipeline results.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import statistics
import tempfile
import time
from collections.abc import Callable
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

import schema_sanitizer as ss
from schema_sanitizer.remote_impl.transport import open_aiohttp_session, run_sync

AZURITE_API_VERSION = "2025-07-05"


def _canonical(value: Any) -> Any:
    """Remove operation-generated clocks recursively."""
    if isinstance(value, dict):
        return {
            key: _canonical(item)
            for key, item in value.items()
            if key not in {"detected_at", "ingestion_timestamp"}
        }
    if isinstance(value, list):
        return [_canonical(item) for item in value]
    if isinstance(value, str) and value[:1] in {"[", "{"}:
        try:
            return _canonical(json.loads(value))
        except json.JSONDecodeError:
            return value
    return value


def _digest(path: Path) -> str:
    """Hash ordered logical Parquet rows."""
    rows = _canonical(pq.read_table(path).to_pylist())
    encoded = json.dumps(rows, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode()).hexdigest()


def _write_sources(root: Path, *, source_count: int, rows: int) -> list[Path]:
    """Create deterministic source objects outside measured intervals."""
    paths = []
    for source in range(source_count):
        first = rows * source // source_count
        last = rows * (source + 1) // source_count
        path = root / f"part-{source:04d}.json"
        path.write_text(
            "\n".join(
                json.dumps(
                    {
                        "source": source,
                        "ordinal": ordinal,
                        "payload": {
                            "name": f"row-{ordinal}",
                            "values": [ordinal, ordinal + 1, ordinal + 2],
                        },
                    },
                    separators=(",", ":"),
                )
                for ordinal in range(first, last)
            )
            + "\n",
            encoding="utf-8",
        )
        paths.append(path)
    return paths


def _configure_s3(
    endpoint: str,
) -> tuple[str, Callable[[str, str], Any], Callable[[str, str], Any]]:
    """Configure async and sync S3 clients for one explicit MinIO endpoint."""
    from botocore.config import Config

    from schema_sanitizer.remote_impl.providers import s3, s3_sync

    options = {
        "endpoint_url": endpoint,
        "region_name": "us-east-1",
        "aws_access_key_id": "minioadmin",
        "aws_secret_access_key": "minioadmin",  # pragma: allowlist secret
        "config": Config(
            max_pool_connections=32,
            retries={"total_max_attempts": 1, "mode": "standard"},
            s3={"addressing_style": "path"},
        ),
    }
    sync_options = {
        **options,
        "config": Config(
            max_pool_connections=1,
            retries={"total_max_attempts": 1, "mode": "standard"},
            s3={"addressing_style": "path"},
        ),
    }
    s3.client_options = lambda: dict(options)
    s3_sync._client_options = lambda: dict(sync_options)  # type: ignore[attr-defined]
    bucket = "schema-sanitizer-benchmark"

    async def create_bucket() -> None:
        """Create the emulator bucket required by the provider benchmark."""
        async with await s3.open_client() as client:
            try:
                await client.create_bucket(Bucket=bucket)
            except Exception as exc:
                response = getattr(exc, "response", {})
                code = response.get("Error", {}).get("Code") if isinstance(response, dict) else None
                if code not in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
                    raise

    run_sync(create_bucket())
    return f"s3://{bucket}/", s3.upload_file, s3.download_file


def _configure_gcs(
    endpoint: str,
) -> tuple[str, Callable[[str, str], Any], Callable[[str, str], Any]]:
    """Configure async and sync GCS JSON clients for fake-gcs-server."""
    from schema_sanitizer.remote_impl.providers import gcs, gcs_sync

    gcs.api_base = lambda: endpoint
    gcs.access_token = lambda: "emulator-token"
    gcs_sync.api_base = lambda: endpoint
    gcs_sync.access_token = lambda: "emulator-token"
    bucket = "schema-sanitizer-benchmark"

    async def create_bucket() -> None:
        """Create the emulator bucket required by the provider benchmark."""
        async with await open_aiohttp_session(gcs.request_headers()) as session:
            async with session.post(
                f"{endpoint}/storage/v1/b",
                params={"project": "test"},
                json={"name": bucket},
            ) as response:
                body = await response.text()
                if response.status not in {200, 201, 409}:
                    raise RuntimeError(f"GCS emulator bucket creation failed: {body}")

    run_sync(create_bucket())
    return f"gs://{bucket}/", gcs.upload_file, gcs.download_file


def _configure_azure(
    connection_string: str,
) -> tuple[str, Callable[[str, str], Any], Callable[[str, str], Any]]:
    """Configure async and sync Azure clients for one explicit Azurite instance."""
    from azure.storage.blob import BlobServiceClient as SyncBlobServiceClient
    from azure.storage.blob.aio import BlobServiceClient as AsyncBlobServiceClient

    from schema_sanitizer.remote_impl.providers import azure, azure_sync

    async def open_async(_ref: object) -> object:
        """Open the asynchronous provider scope used by the benchmark."""
        return AsyncBlobServiceClient.from_connection_string(
            connection_string,
            api_version=AZURITE_API_VERSION,
        )

    @contextmanager
    def open_sync(_ref: object):
        """Open the synchronous provider scope used by the benchmark."""
        service = SyncBlobServiceClient.from_connection_string(
            connection_string,
            api_version=AZURITE_API_VERSION,
        )
        try:
            yield service
        finally:
            service.close()

    azure.open_service = open_async
    azure_sync.open_service = open_sync
    container = "schema-sanitizer-benchmark"

    async def create_container() -> None:
        """Create the emulator container required by the Azure benchmark."""
        service = AsyncBlobServiceClient.from_connection_string(
            connection_string,
            api_version=AZURITE_API_VERSION,
        )
        try:
            try:
                await service.create_container(container)
            except Exception as exc:
                if getattr(exc, "status_code", None) != 409:
                    raise
        finally:
            await service.close()

    run_sync(create_container())
    return f"az://devstoreaccount1/{container}/", azure.upload_file, azure.download_file


def _measure_provider(
    name: str,
    *,
    base_uri: str,
    upload: Callable[[str, str], Any],
    download: Callable[[str, str], Any],
    sources: list[Path],
    workspace: Path,
    memory_limit: int,
    warmups: int,
    repeats: int,
) -> dict[str, Any]:
    """Upload a corpus, measure both modes, and require logical equivalence."""
    input_uri = f"{base_uri}input/"
    for source in sources:
        run_sync(upload(str(source), f"{input_uri}{source.name}"))

    samples: dict[str, list[float]] = {"single": [], "multi": []}
    digests: dict[str, str] = {}
    for mode in ("single", "multi"):
        for iteration in range(warmups + repeats):
            output_uri = f"{base_uri}output/{name}-{mode}-{iteration}.parquet"
            started = time.perf_counter()
            result = ss.to_parquet(
                input_uri,
                output_uri,
                input_format="json",
                input_mode="directory",
                memory_limit_bytes=memory_limit,
                multi_threading=mode == "multi",
                parquet_compression="snappy",
            )
            close = getattr(result, "close", None)
            if callable(close):
                close()
            elapsed = time.perf_counter() - started
            if iteration >= warmups:
                samples[mode].append(elapsed)
            local = workspace / f"{name}-{mode}-{iteration}.parquet"
            run_sync(download(output_uri, str(local)))
            if iteration == warmups + repeats - 1:
                digests[mode] = _digest(local)
            local.unlink()
    if digests["single"] != digests["multi"]:
        raise RuntimeError(f"{name}: single and multi remote outputs differ")
    medians = {mode: statistics.median(values) for mode, values in samples.items()}
    return {
        "samples": samples,
        "medians": medians,
        "speedup_single_over_multi": medians["single"] / medians["multi"],
        "logical_outputs_equivalent": True,
    }


def main() -> None:
    """Run the explicit emulator provider matrix and write one JSON report."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--s3-endpoint", required=True)
    parser.add_argument("--gcs-endpoint", required=True)
    parser.add_argument("--azure-connection-string", required=True)
    parser.add_argument("--rows", type=int, default=20_000)
    parser.add_argument("--source-count", type=int, default=8)
    parser.add_argument("--memory-mib", type=int, default=128)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if (
        args.rows <= 0
        or args.source_count <= 0
        or args.source_count > args.rows
        or args.memory_mib <= 0
        or args.warmups < 0
        or args.repeats <= 0
    ):
        parser.error("invalid rows/source-count/memory/warmup/repeat values")

    providers = {
        "s3": _configure_s3(args.s3_endpoint),
        "gcs": _configure_gcs(args.gcs_endpoint),
        "azure": _configure_azure(args.azure_connection_string),
    }
    with tempfile.TemporaryDirectory(prefix="schema-sanitizer-remote-benchmark-") as raw:
        workspace = Path(raw)
        sources = _write_sources(
            workspace,
            source_count=args.source_count,
            rows=args.rows,
        )
        results = {
            name: _measure_provider(
                name,
                base_uri=base_uri,
                upload=upload,
                download=download,
                sources=sources,
                workspace=workspace,
                memory_limit=args.memory_mib << 20,
                warmups=args.warmups,
                repeats=args.repeats,
            )
            for name, (base_uri, upload, download) in providers.items()
        }
    report = {
        "schema_version": 1,
        "rows": args.rows,
        "source_count": args.source_count,
        "memory_mib": args.memory_mib,
        "warmups": args.warmups,
        "repeats": args.repeats,
        "providers": results,
        "logical_outputs_equivalent": True,
    }
    encoded = json.dumps(report, indent=2, sort_keys=True)
    print(encoded)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
