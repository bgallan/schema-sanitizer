"""Lazy remote JSON source-plan tests."""

from __future__ import annotations

from _support.input_contract import *  # noqa: F403


def test_remote_json_directory_preparation_uses_lazy_native_source_stage(
    monkeypatch, tmp_path
) -> None:
    """Verify UTF-8 remote JSON directories stage native child sources lazily."""
    from schema_sanitizer.api_impl.input import preparation as public_input
    from schema_sanitizer.api_impl.source_plan.attached import (
        remote_native_multisource_manifest_from_data,
    )
    from schema_sanitizer.input_impl.source_plan import path_source_tuples
    from schema_sanitizer.remote_impl import staging as remote_staging
    from schema_sanitizer.remote_impl import sync_backend
    from schema_sanitizer.remote_impl.staging import StagedPath
    from schema_sanitizer.sources import RemoteFile

    staged_calls = []

    def fake_list_remote_directory_files(uri, suffixes, *, memory_limit_bytes=None):
        """Return deterministic remote children without staging them."""
        assert uri == "s3://bucket/partition/"
        assert suffixes == (".json",)
        return [
            RemoteFile("s3://bucket/partition/a.json", "a.json", 8),
            RemoteFile("s3://bucket/partition/b.json", "b.json", 8),
        ]

    def fake_stage_remote_files_to_directory(files, **kwargs):
        """Stage only the requested remote chunk."""
        assert kwargs["memory_limit_bytes"] is None
        staged_calls.append([file.name for file in files])
        staged_dir = tmp_path / f"staged-{len(staged_calls)}"
        staged_dir.mkdir()
        for file in files:
            (staged_dir / file.name).write_text('{"a":1}\n', encoding="utf-8")
        return StagedPath(
            str(staged_dir),
            is_dir=True,
            source_file_by_name={file.name: file.uri for file in files},
        )

    monkeypatch.setattr(
        sync_backend,
        "list_remote_directory",
        fake_list_remote_directory_files,
    )
    monkeypatch.setattr(
        remote_staging,
        "stage_remote_files_to_directory",
        fake_stage_remote_files_to_directory,
    )

    prepared = public_input.prepare_public_input(
        "s3://bucket/partition/",
        input_format="json",
        input_mode="directory",
        input_text_encoding="utf-8",
        xml_row_tag=None,
        csv_delimiter=",",
        csv_has_header=True,
        memory_limit_bytes=None,
    )
    try:
        assert prepared.format == "json"
        assert prepared.source == "stream"
        manifest = remote_native_multisource_manifest_from_data(prepared.data)
        assert manifest is not None
        assert staged_calls == []
        first = manifest.stage_chunk(0)
        assert first is not None
        assert staged_calls == [["a.json", "b.json"]]
        assert first.manifest.source_batch is not None
        assert path_source_tuples(first.manifest.source_batch) == [
            (
                "json",
                str(tmp_path / "staged-1" / "a.json"),
                "s3://bucket/partition/a.json",
            ),
            (
                "json",
                str(tmp_path / "staged-1" / "b.json"),
                "s3://bucket/partition/b.json",
            ),
        ]
        first.close()
    finally:
        prepared.close()


def test_discovered_remote_json_directory_uses_same_lazy_source_plan(
    monkeypatch,
) -> None:
    """Verify pre-discovered remote directories reuse the canonical remote source-plan path."""
    import schema_sanitizer.input_impl.source_plan as source_plan_model
    from schema_sanitizer.api_impl.input import preparation as public_input
    from schema_sanitizer.api_impl.source_plan import attached as source_plan_attached
    from schema_sanitizer.input_impl.directory_inputs import (
        DiscoveredDirectoryInput,
        discovered_directory_inputs,
    )
    from schema_sanitizer.remote_impl import sync_backend
    from schema_sanitizer.remote_impl.packetization import remote_staging_packet_policy
    from schema_sanitizer.sources import RemoteFile

    files = (
        RemoteFile("s3://bucket/partition/a.json", "a.json", 8),
        RemoteFile("s3://bucket/partition/b.json", "b.json", 8),
    )

    def fail_listing(*_args, **_kwargs):
        """Fail if discovered remote inputs are listed again."""
        raise AssertionError("discovered remote directory should not be relisted")

    monkeypatch.setattr(sync_backend, "list_remote_directory", fail_listing)

    with discovered_directory_inputs(
        {
            "s3://bucket/partition/": DiscoveredDirectoryInput(
                input_format="json",
                remote_files=files,
            )
        }
    ):
        prepared = public_input.prepare_public_input(
            "s3://bucket/partition/",
            input_format="json",
            input_mode="directory",
            input_text_encoding="utf-8",
            xml_row_tag=None,
            csv_delimiter=",",
            csv_has_header=True,
            memory_limit_bytes=None,
        )

    try:
        plan = source_plan_attached.source_plan_from_data(prepared.data)
        manifest = source_plan_attached.remote_native_multisource_manifest_from_data(prepared.data)
        assert prepared.format == "json"
        assert prepared.source == "stream"
        assert plan is not None
        assert plan.kind == source_plan_model.REMOTE_CHUNKS
        assert plan.route_name == "remote_native_manifest_chunks"
        assert manifest is not None
        assert manifest.files == tuple(files)
        assert manifest.chunk_size == remote_staging_packet_policy(None).max_files
    finally:
        prepared.close()
