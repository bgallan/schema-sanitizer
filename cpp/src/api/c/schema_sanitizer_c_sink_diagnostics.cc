// C bridge lifecycle and serialization for sink diagnostics.

#include "api/c/schema_sanitizer_c_sink_internal.hh"

#include <algorithm>
#include <exception>
#include <new>
#include <string>

#include "internal/abi/schema_sanitizer_c_bridge.hh"
#include "internal/arrow_c/cdata_stream_callbacks.hh"
#include "internal/runtime/process_identity.hh"

void schema_sanitizer_stream_free(struct ArrowArrayStream *stream) {
  if (!stream || !sanitize::internal::runtime_owner_process())
    return;
  sanitize::internal::cdata_stream::release_stream_nothrow(stream);
  delete stream;
}

void schema_sanitizer_diagnostics_free(
    schema_sanitizer_diagnostics *diagnostics) {
  if (!sanitize::internal::runtime_owner_process())
    return;
  delete diagnostics;
}

int schema_sanitizer_diagnostics_json(schema_sanitizer_diagnostics *diagnostics,
                                      char **out_json, char **out_error) {
  static constexpr const char *kWhere = "schema_sanitizer_diagnostics_json";
  clear_out(out_json);
  clear_out(out_error);
  if (!out_json) {
    return set_error(out_error, std::string(kWhere) + ": out_json is null",
                     SCHEMA_SANITIZER_STATUS_INVALID_ARGUMENT);
  }
  if (!diagnostics || !diagnostics->diagnostics) {
    return set_error(out_error, std::string(kWhere) + ": diagnostics is null",
                     SCHEMA_SANITIZER_STATUS_INVALID_ARGUMENT);
  }
  try {
    sanitize::IngestDiagnostics merged = *diagnostics->diagnostics;
    if (diagnostics->has_inference_snapshot) {
      const auto &snapshot = diagnostics->inference_snapshot;
      merged.inferred_rows = snapshot.inferred_rows;
      merged.inferred_bytes = snapshot.inferred_bytes;
      merged.arrow_schema_depth = snapshot.arrow_schema_depth;
      merged.parquet_schema_depth = snapshot.parquet_schema_depth;
      merged.flattened_fields = snapshot.flattened_fields;
      merged.scalar_wrappings = snapshot.scalar_wrappings;
      merged.peak_charged_memory_bytes = std::max(
          merged.peak_charged_memory_bytes, snapshot.peak_charged_memory_bytes);
      merged.operation_memory_limit_bytes =
          std::max(merged.operation_memory_limit_bytes,
                   snapshot.operation_memory_limit_bytes);
      merged.reader.parser_max_depth = std::max(
          merged.reader.parser_max_depth, snapshot.reader.parser_max_depth);
      if (merged.reader.decoded_bytes == 0) {
        merged.reader.decoded_bytes = snapshot.reader.decoded_bytes;
      }
      if (merged.reader.records == 0) {
        merged.reader.records = snapshot.reader.records;
      }
      if (merged.reader.nodes == 0) {
        merged.reader.nodes = snapshot.reader.nodes;
      }
      if (merged.reader.compressed_bytes == 0) {
        merged.reader.compressed_bytes = snapshot.reader.compressed_bytes;
      }
      if (merged.reader.decompressed_bytes == 0) {
        merged.reader.decompressed_bytes = snapshot.reader.decompressed_bytes;
      }
    }
    const std::string json = merged.to_json();
    *out_json = dup_cstr(json);
    if (!*out_json)
      return set_oom_error(out_error, kWhere);
    return SCHEMA_SANITIZER_STATUS_OK;
  } catch (const std::bad_alloc &) {
    return set_oom_error(out_error, kWhere);
  } catch (const std::exception &error) {
    return set_exception_error(out_error, kWhere, error);
  } catch (...) {
    return set_unknown_exception_error(out_error, kWhere);
  }
}
