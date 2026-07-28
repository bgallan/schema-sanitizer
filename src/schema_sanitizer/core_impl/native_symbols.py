"""Required ABI3 symbols grouped by their native runtime domain.

The package and its extension are released as one unit, so production code
binds the current ABI directly instead of probing historical symbol sets.
"""

from __future__ import annotations

from .native_runtime import native_core as _native

# Arrow schema and stream wrappers.
ARROW_DIRECT_SCHEMA_SUPPORTED = _native.arrow_direct_schema_supported
ARROW_SCHEMA_CONTRACT_PAYLOAD = _native.arrow_schema_contract_payload
COALESCING_STREAM_WRAP = _native.coalescing_stream_wrap
METADATA_STREAM_WRAP = _native.metadata_stream_wrap

# CSV and JSONL streams and file sinks.
CSV_NESTED_STREAM_WRAP = _native.csv_nested_stream_wrap
CSV_SCHEMA_SUPPORTED = _native.csv_schema_supported
CSV_STREAM_WRITE = _native.csv_stream_write
CSV_STREAM_WRITE_WITH_METADATA = _native.csv_stream_write_with_metadata
JSONL_SCHEMA_SUPPORTED = _native.jsonl_schema_supported
JSONL_STREAM_WRITE = _native.jsonl_stream_write
JSONL_STREAM_WRITE_WITH_METADATA = _native.jsonl_stream_write_with_metadata

# Parquet inspection, reading, and writing.
PARQUET_STREAM_WRITE = _native.parquet_stream_write
PARQUET_STREAM_WRITE_WITH_METADATA = _native.parquet_stream_write_with_metadata
PARQUET_FOOTER_INFO_JSON = _native.parquet_footer_info_json
PARQUET_STREAM_PREFLIGHT_JSON = _native.parquet_stream_preflight_json
PARQUET_STREAM_READ = _native.parquet_stream_read

# Source preparation and row conversion.
JSON_ARRAY_TO_JSONL_BYTES = _native.json_array_to_jsonl_bytes
JSON_ARRAY_FILES_TO_JSONL_BYTES = _native.json_array_files_to_jsonl_bytes
PATH_SOURCE_PLAN_CREATE = _native.path_source_plan_create
PYTHON_ROWS_JSONL_BYTES = _native.python_rows_jsonl_bytes
PYTHON_ITER_ROWS_JSONL_BYTES = _native.python_iter_rows_jsonl_bytes
XML_FOLDER_EFFECTIVE_ROW_TAG = _native.xml_folder_effective_row_tag
