// Declares Python ABI3 capsule and bridge helpers.

#pragma once

#ifndef Py_LIMITED_API
#define Py_LIMITED_API 0x030A0000
#endif

#include <Python.h>

#ifndef _PyCFunction_CAST
#ifdef PyCFunction_CAST
#define _PyCFunction_CAST(func) PyCFunction_CAST(func)
#else
#define _PyCFunction_CAST(func)                                                \
  reinterpret_cast<PyCFunction>(reinterpret_cast<void (*)(void)>(func))
#endif
#endif

#include "internal/abi/schema_sanitizer_c_bridge.hh"
#include <cstddef>
#include <cstdint>
#include <memory>

namespace sanitize {
class ChunkSource;
}
namespace core_abi3_internal {

enum class PythonSourceKind { kPath, kText, kStream, kUnknown };

// Converts a bridge status and message into the active Python exception.
void raise_status_error(int status, char *err);
PyObject *fsencode_path(PyObject *obj);
int bytes_or_str_view(PyObject *obj, const char **out_ptr, Py_ssize_t *out_len);
int tuple_set_item_steal(PyObject *tup, Py_ssize_t index, PyObject *item);
int readonly_buffer_view(PyObject *obj, const std::uint8_t **out_ptr,
                         Py_ssize_t *out_len, PyObject **out_owner);
// Returns false with the current Python exception set when a signal is pending.
bool check_python_signals();
// Installs Python signal polling on a native context created for this module.
void install_python_interrupt_check(schema_sanitizer_context *ctx);
// Creates a native chunk source backed by a seekable Python byte reader.
std::shared_ptr<sanitize::ChunkSource>
make_python_reader_chunk_source(PyObject *reader);
// Parses a Python source selector.
PythonSourceKind parse_python_source_kind(const char *source_name) noexcept;
// Returns whether an object exposes the reader protocol expected by native
// chunk sources.
bool python_reader_has_read_seek(PyObject *reader) noexcept;
// Sets the standard reader protocol TypeError.
void set_python_reader_type_error();
// Returns a sequence item, borrowing for list/tuple and owning otherwise.
PyObject *sequence_item_borrowed_or_new(PyObject *seq, Py_ssize_t index,
                                        bool *borrowed);

// Extracts a native context pointer from a Python capsule.
schema_sanitizer_context *unwrap_context(PyObject *obj);
// Wraps a native context pointer in an owning Python capsule.
PyObject *wrap_context_capsule(schema_sanitizer_context *ctx);
// Extracts a native prepared-options pointer from a Python capsule.
schema_sanitizer_prepared_options *unwrap_prepared_options(PyObject *obj);
// Extracts a native diagnostics pointer from a Python capsule.
schema_sanitizer_diagnostics *unwrap_diagnostics(PyObject *obj);
// Wraps prepared options in an owning Python capsule.
PyObject *
wrap_prepared_options_capsule(schema_sanitizer_prepared_options *prepared);
// Wraps diagnostics in an owning Python capsule.
PyObject *wrap_diagnostics_capsule(schema_sanitizer_diagnostics *diagnostics);
// Wraps an Arrow stream capsule while retaining a Python keepalive object.
PyObject *wrap_stream_capsule_with_keepalive(PyObject *keepalive_obj,
                                             ArrowArrayStream *stream);
// Releases stream and diagnostics outputs returned by sink bridge calls.
void release_sink_outputs(ArrowArrayStream *main_stream,
                          schema_sanitizer_diagnostics *diagnostics);
// Packs the stream and live diagnostics capsule returned by normal sinks.
PyObject *
pack_stream_and_diagnostics(PyObject *keepalive, ArrowArrayStream *main_stream,
                            schema_sanitizer_diagnostics *diagnostics);
// Packs the stream, diagnostics, and registry metadata returned by registry
// backed sinks.
PyObject *pack_registry_stream_result(PyObject *keepalive,
                                      ArrowArrayStream *main_stream,
                                      schema_sanitizer_diagnostics *diagnostics,
                                      char *registry_json, char *drifts_json,
                                      char *conversion_timestamp);

PyObject *py_context_new(PyObject *, PyObject *);
// Returns context memory statistics as a Python JSON string.
PyObject *py_context_memory_stats_json(PyObject *, PyObject *);
// Returns live diagnostics JSON for a diagnostics capsule.
PyObject *py_diagnostics_json(PyObject *, PyObject *);

PyObject *py_options_prepare_bytes(PyObject *, PyObject *);

// Merges registry state and schemas through the native schema-registry engine.
PyObject *py_schema_registry_merge(PyObject *, PyObject *);
// Returns an empty schema registry JSON document.
PyObject *py_schema_registry_empty(PyObject *, PyObject *);
// Returns whether registry JSON carries a usable canonical schema.
PyObject *py_schema_registry_has_canonical_schema(PyObject *, PyObject *);
// Encodes registry canonical_schema as the native logical-schema contract
// payload.
PyObject *py_schema_registry_contract_payload(PyObject *, PyObject *);
// Compiles a registry JSON document into a reusable native registry-state
// capsule.
PyObject *py_registry_state_from_json(PyObject *, PyObject *);
// Returns top-level field names from a native logical-schema payload.
PyObject *py_logical_schema_payload_field_names(PyObject *, PyObject *);
// Infers a logical schema from a source-selected input without materializing a
// sink.
PyObject *py_context_schema_probe_from_source(PyObject *, PyObject *);
// Infers and merges schema registry state from a source-selected input without
// materializing a sink.
PyObject *py_context_registry_probe_from_source(PyObject *, PyObject *);
// Infers a logical schema from multiple local files as one logical input.
PyObject *py_context_schema_probe_from_paths(PyObject *, PyObject *);
// Infers and merges schema registry state from multiple local files as one
// logical input.
PyObject *py_context_registry_probe_from_paths(PyObject *, PyObject *);
// Infers and merges schema registry state from native path-source inputs.
PyObject *py_context_registry_probe_from_path_sources(PyObject *, PyObject *);
// Infers and merges schema registry state from native path-source inputs using
// a compiled native registry-state capsule.
PyObject *
py_context_registry_probe_from_path_sources_registry_state(PyObject *,
                                                           PyObject *);
// Infers and merges schema registry state from native path-source inputs,
// skipping JSON parse failures.
PyObject *py_context_registry_probe_from_path_sources_best_effort(PyObject *,
                                                                  PyObject *);
// Infers and merges schema registry state from native path-source inputs using
// a compiled native registry-state capsule, skipping JSON parse failures.
PyObject *
py_context_registry_probe_from_path_sources_best_effort_registry_state(
    PyObject *, PyObject *);
// Infers and merges schema registry state from lazily provided path-source
// chunks.
PyObject *py_context_registry_probe_from_path_source_chunk_provider(PyObject *,
                                                                    PyObject *);
// Infers and merges schema registry state from lazily provided path-source
// chunks using a compiled native registry-state capsule.
PyObject *
py_context_registry_probe_from_path_source_chunk_provider_registry_state(
    PyObject *, PyObject *);
// Parses and stores native path-source descriptors for repeated source-plan
// calls.
PyObject *py_path_source_plan_create(PyObject *, PyObject *);
// Builds generated file metadata columns.
PyObject *py_file_metadata_columns(PyObject *, PyObject *);
// Wraps an Arrow C stream and appends string metadata columns.
PyObject *py_metadata_stream_wrap(PyObject *, PyObject *);
// Wraps supported flat Arrow C streams and coalesces tiny batches.
PyObject *py_coalescing_stream_wrap(PyObject *, PyObject *);
// Wraps an Arrow C stream and renders top-level nested columns as JSON strings.
PyObject *py_csv_nested_stream_wrap(PyObject *, PyObject *);
// Writes a supported Arrow C stream to a local CSV file.
PyObject *py_csv_stream_write(PyObject *, PyObject *);
// Writes a metadata-augmented Arrow C stream to a local CSV file.
PyObject *py_csv_stream_write_with_metadata(PyObject *, PyObject *);
// Returns whether a PyArrow schema is supported by the native CSV writer.
PyObject *py_csv_schema_supported(PyObject *, PyObject *);
// Writes a supported Arrow C stream to a local Parquet file.
PyObject *py_parquet_stream_write(PyObject *, PyObject *);
// Writes a metadata-augmented Arrow C stream to a local Parquet file.
PyObject *py_parquet_stream_write_with_metadata(PyObject *, PyObject *);
// Returns bounded native Parquet footer metadata as JSON.
PyObject *py_parquet_footer_info_json(PyObject *, PyObject *);
// Opens a supported native Parquet file as an Arrow C stream.
PyObject *py_parquet_stream_read(PyObject *, PyObject *);
// Writes a supported Arrow C stream to a local JSONL file.
PyObject *py_jsonl_stream_write(PyObject *, PyObject *);
// Writes a metadata-augmented Arrow C stream to a local JSONL file.
PyObject *py_jsonl_stream_write_with_metadata(PyObject *, PyObject *);
// Encodes one PyArrow record batch as JSONL bytes.
PyObject *py_jsonl_batch_bytes(PyObject *, PyObject *);
// Encodes a sequence of PyArrow record batches as JSONL bytes.
PyObject *py_jsonl_batches_bytes(PyObject *, PyObject *);
// Compacts one JSON document to canonical compact JSON bytes.
PyObject *py_json_compact_bytes(PyObject *, PyObject *);
// Splits one top-level JSON array of objects into compact JSON Lines bytes.
PyObject *py_json_array_to_jsonl_bytes(PyObject *, PyObject *);
// Reads and splits local JSON array documents into JSON Lines bytes.
PyObject *py_json_array_files_to_jsonl_bytes(PyObject *, PyObject *);
// Returns the effective row tag for local XML documents.
PyObject *py_xml_folder_effective_row_tag(PyObject *, PyObject *);
// Encodes one JSON-like Python row to compact JSON bytes.
PyObject *py_python_row_json_bytes(PyObject *, PyObject *);
// Encodes a sequence window of JSON-like Python rows to compact JSONL bytes.
PyObject *py_python_rows_jsonl_bytes(PyObject *, PyObject *);
// Returns whether a PyArrow schema is supported by the native JSONL writer.
PyObject *py_jsonl_schema_supported(PyObject *, PyObject *);
// Returns whether a PyArrow schema is supported by direct Arrow ingestion.
PyObject *py_arrow_direct_schema_supported(PyObject *, PyObject *);
// Encodes a PyArrow schema as the native logical-schema contract payload.
PyObject *py_arrow_schema_contract_payload(PyObject *, PyObject *);

// Converts a source-selected Python input through a context into sink output
// capsules.
PyObject *py_context_to_sink_from_source(PyObject *, PyObject *);
// Converts multiple native path sources through a context into sink output
// capsules.
PyObject *py_context_to_sink_from_path_sources(PyObject *, PyObject *);
PyObject *py_context_to_sink_from_path_source_chunk_provider(PyObject *,
                                                             PyObject *);
// Converts a Python Arrow C stream through a context into sink output capsules.
PyObject *py_context_to_sink_arrow_stream(PyObject *, PyObject *);

PyObject *py_context_to_registry_sink_from_source(PyObject *, PyObject *);
PyObject *py_context_to_registry_sink_from_path_sources(PyObject *, PyObject *);
PyObject *
py_context_to_registry_sink_from_path_sources_registry_state(PyObject *,
                                                             PyObject *);
PyObject *
py_context_to_registry_sink_from_path_source_chunk_provider_registry_state(
    PyObject *, PyObject *);
PyObject *
py_context_to_registry_sink_from_path_source_chunk_provider_auto_registry(
    PyObject *, PyObject *);
PyObject *
py_context_to_registry_sink_from_path_source_chunk_provider_auto_registry_state(
    PyObject *, PyObject *);
PyObject *
py_context_to_registry_sink_from_path_sources_auto_registry(PyObject *,
                                                            PyObject *);
PyObject *
py_context_to_registry_sink_from_path_sources_auto_registry_state(PyObject *,
                                                                  PyObject *);
PyObject *py_context_to_registry_sink_arrow_stream(PyObject *, PyObject *);
PyObject *py_context_to_registry_sink_arrow_sources(PyObject *, PyObject *);
PyObject *py_context_to_registry_sink_arrow_sources_registry_state(PyObject *,
                                                                   PyObject *);
PyObject *py_context_to_registry_sink_arrow_sources_auto_registry(PyObject *,
                                                                  PyObject *);
PyObject *
py_context_to_registry_sink_arrow_sources_auto_registry_state(PyObject *,
                                                              PyObject *);
PyObject *
py_context_to_registry_sink_arrow_source_chunk_provider_registry_state(
    PyObject *, PyObject *);
PyObject *py_context_to_registry_sink_arrow_source_chunk_provider_auto_registry(
    PyObject *, PyObject *);
PyObject *
py_context_to_registry_sink_arrow_source_chunk_provider_auto_registry_state(
    PyObject *, PyObject *);
PyObject *py_context_registry_probe_from_arrow_sources(PyObject *, PyObject *);
// Infers and merges schema registry state from Arrow C stream sources using a
// compiled native registry-state capsule.
PyObject *
py_context_registry_probe_from_arrow_sources_registry_state(PyObject *,
                                                            PyObject *);

} // namespace core_abi3_internal
