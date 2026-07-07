"""Central cached access to optional native ABI3 helper functions."""

from __future__ import annotations

from .native_cache import NativeFunctionCache

ARROW_DIRECT_SCHEMA_SUPPORTED = NativeFunctionCache("arrow_direct_schema_supported")
ARROW_SCHEMA_CONTRACT_PAYLOAD = NativeFunctionCache("arrow_schema_contract_payload")
CSV_NESTED_STREAM_WRAP = NativeFunctionCache("csv_nested_stream_wrap")
CSV_SCHEMA_SUPPORTED = NativeFunctionCache("csv_schema_supported")
CSV_STREAM_WRITE = NativeFunctionCache("csv_stream_write")
CSV_STREAM_WRITE_WITH_METADATA = NativeFunctionCache("csv_stream_write_with_metadata")
COALESCING_STREAM_WRAP = NativeFunctionCache("coalescing_stream_wrap")
JSON_COMPACT_BYTES = NativeFunctionCache("json_compact_bytes")
JSON_ARRAY_TO_JSONL_BYTES = NativeFunctionCache("json_array_to_jsonl_bytes")
JSON_ARRAY_FILES_TO_JSONL_BYTES = NativeFunctionCache("json_array_files_to_jsonl_bytes")
JSONL_SCHEMA_SUPPORTED = NativeFunctionCache("jsonl_schema_supported")
JSONL_STREAM_WRITE = NativeFunctionCache("jsonl_stream_write")
JSONL_STREAM_WRITE_WITH_METADATA = NativeFunctionCache("jsonl_stream_write_with_metadata")
METADATA_STREAM_WRAP = NativeFunctionCache("metadata_stream_wrap")
PARQUET_STREAM_WRITE = NativeFunctionCache("parquet_stream_write")
PARQUET_STREAM_WRITE_WITH_METADATA = NativeFunctionCache("parquet_stream_write_with_metadata")
PARQUET_FOOTER_INFO_JSON = NativeFunctionCache("parquet_footer_info_json")
PARQUET_STREAM_READ = NativeFunctionCache("parquet_stream_read")
PATH_SOURCE_PLAN_CREATE = NativeFunctionCache("path_source_plan_create")
PYTHON_ROWS_JSONL_BYTES = NativeFunctionCache("python_rows_jsonl_bytes")
REGISTRY_STATE_FROM_JSON = NativeFunctionCache("registry_state_from_json")
XML_FOLDER_EFFECTIVE_ROW_TAG = NativeFunctionCache("xml_folder_effective_row_tag")
