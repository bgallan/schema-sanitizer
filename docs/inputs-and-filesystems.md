# Inputs and filesystems

Public converters accept Python rows, local paths, supported remote URIs, and
immutable GCS manifests. Selection is explicit and deterministic.

## Index

- [Formats](#formats)
- [Python rows](#python-rows)
- [Files and directories](#files-and-directories)
- [Source manifests](#source-manifests)
- [Generated columns](#generated-columns)
- [Supported locations](#supported-locations)
- [Credentials](#credentials)
- [Remote execution](#remote-execution)
- [Publication and retries](#publication-and-retries)
- [Memory ownership](#memory-ownership)

## [Formats](#index)

Paths and URIs require `input_format`. The value is not inferred from an
extension or file contents.

| Value | Extensions | Source shape |
|---|---|---|
| `csv` | `.csv` | Delimited records. |
| `json` | `.json` | One complete document treated as one row. |
| `json_array` | `.json` | Top-level array of row objects. |
| `jsonl` | `.jsonl` | One JSON object per line. |
| `ndjson` | `.ndjson` | One JSON object per line. |
| `xml` | `.xml` | One document or streamed `xml_row_tag` elements. |
| `parquet` | `.parquet`, `.pq` | Parquet rows. |
| `python` | none | Iterable of dictionaries. |

The explicit format must agree with a path's supported extension. JSON and JSON
array inputs intentionally share `.json` while retaining different root
semantics.

## [Python rows](#index)

Lists, tuples, generators, and one-shot iterables of dictionaries use one
ordered logical stream:

```python
rows = ({"id": index, "payload": f"row-{index}"} for index in range(100_000))

result = ss.to_parquet(
    rows,
    "clean.parquet",
    input_format="python",  # optional for a recognized row iterable
    multi_threading=True,
    memory_limit_bytes=256 * 1024 * 1024,
)
```

Generators are not converted into one unbounded list. Replay data remains
charged to the operation. Iteration and dictionary inspection require the GIL;
native inference, materialization, and output can still use the operation arena.

## [Files and directories](#index)

`input_mode="single_file"` processes exactly one file.
`input_mode="directory"` processes matching direct children in deterministic
filename order and never recurses into subdirectories.

```python
result = ss.to_pandas(
    "raw/2026-08/",
    input_format="jsonl",
    input_mode="directory",
)
```

CSV directories use `csv_header_mode="exact"` by default. Select `union` when
headers can be reordered or extended. See [CSV header modes](csv-header-modes.md).

## [Source manifests](#index)

A `SourceManifest` freezes an already selected set of GCS object generations:

```python
from datetime import UTC, datetime

manifest = ss.sources.discover(
    "gs://raw-bucket/events",
    suffixes=("csv",),
    modified_between=(
        datetime(2026, 8, 5, tzinfo=UTC),
        datetime(2026, 8, 6, tzinfo=UTC),
    ),
    multi_threading=True,
)

result = ss.to_polars(manifest, input_format="csv")
```

The window is half-open: `start <= updated < end`. Each download requests the
selected generation, preventing a later overwrite from changing the run.
`sources.list_objects` returns metadata without building a manifest.

See [Source manifests](source-manifests.md) for validation and diagnostics.

## [Generated columns](#index)

Every output appends these root fields in this order:

1. `schema_registry`
1. `schema_drifts`
1. `source_file`
1. `ingestion_timestamp`

Registry and drift values appear on the first row and are null afterwards.
`source_file` and the operation-captured UTC microsecond timestamp appear on
every row. Source fields using these reserved root names are rejected.

For remote inputs, `source_file` retains the object URI rather than the local
staging path.

## [Supported locations](#index)

| Location | Input | Output | Directory listing |
|---|---|---|---|
| Local path / `file://` | yes | yes | yes |
| `gs://` / `gcs://` | yes | yes | yes |
| `s3://` | yes | yes | yes |
| Azure Blob and ABFS forms | yes | yes | yes |
| HTTP(S) | one file | one file | no |

Cloud listing is non-recursive, deterministic, and bounded. Generic HTTP has
no portable directory-listing contract.

## [Credentials](#index)

- GCS uses Google Application Default Credentials.
- S3 uses the normal AWS credential chain through `aiobotocore`/`botocore`.
- Azure uses `DefaultAzureCredential`.
- HTTP(S) uses the URI and supported request configuration; it does not invent
  cloud credentials.

Credentials and provider clients are operation-owned. They are not stored in
global application-visible clients.

## [Remote execution](#index)

Remote input is streamed into replayable local staging files. Remote output is
converted locally and uploaded only after the writer closes successfully.

- Single mode uses blocking provider clients on the caller thread.
- Multi mode uses bounded asynchronous clients and one lazy event-loop host for
  listing, staging, prefetch, and upload.
- Compatible sessions and connection pools are reused within the operation.
- Closing or cancelling an operation drains work before releasing staging
  ownership.

Concurrency, packet sizes, prefetch, retry attempts, and temporary-storage
permits are derived from `memory_limit_bytes`.

## [Publication and retries](#index)

Local output uses a sibling staging file and replaces the destination only
after success. Existing destinations remain intact on conversion failure.

Remote strategies depend on the provider:

- S3 uses bounded multipart upload for large objects and commits parts in
  ordinal order.
- GCS uses resumable sessions and reconciles committed offsets.
- Azure uses block upload with bounded concurrency.
- HTTP uses one ordered `PUT` and does not follow upload redirects.

Transient HTTP `GET`, `HEAD`, and idempotent `PUT` requests use bounded retries.
Each download retry truncates its staging file; each upload retry reopens the
completed spool from byte zero.

`sources.publish_file_atomic(local_path, destination_uri)` exposes the same safe
publication path for an application-created local file and returns its verified
local size.

## [Memory ownership](#index)

Files may be larger than `memory_limit_bytes` because transfer, readers, and
writers stream them. Metadata, queues, buffers, replay spools, and temporary
storage permits remain bounded. See [Reader memory accounting](reader-memory-accounting.md)
and [Concurrency and memory](concurrency-memory-hardening.md).
