/*
 * C bridge sink execution APIs.
 *
 * This file implements context-to-sink entry points that return Arrow
 * streams plus diagnostics JSON payloads.
 */
#include "api/c/schema_sanitizer_c_sink_internal.hh"

#include "nanoarrow/nanoarrow.h"

#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <new>
#include <string>
#include <string_view>
#include <utility>

#include "internal/abi/schema_sanitizer_c_bridge.hh"
#include "sanitize/abi/cdata_types.hh"
#include "sanitize/ingest/chunk_source.hh"
#include "sanitize/ingest/ingest.hh"
#include "sanitize/ingest/ingest_types.hh"
#include "sanitize/options/options.hh"
#include "sanitize/registry/registry.hh"

// Clears optional sink output pointers before an ingest attempt.
void clear_sink_outputs(SinkOutputs outputs) {
  if (outputs.stream)
    *outputs.stream = nullptr;
  if (outputs.diagnostics)
    *outputs.diagnostics = nullptr;
}

void clear_registry_sink_outputs(RegistrySinkOutputs outputs) {
  clear_sink_outputs(outputs.sink);
  clear_out(outputs.registry_json);
  clear_out(outputs.drifts_json);
  clear_out(outputs.conversion_timestamp);
}

// Converts a prepared ingest stream into Arrow C Stream outputs.
int ingest_stream_to_streams(sanitize::IngestStream o, SinkOutputs outputs,
                             char **out_error, const char *where) {
  clear_sink_outputs(outputs);
  sanitize::UniqueCStream main_stream;
  // Keep the main stream owned until all optional outputs are allocated.
  if (outputs.stream && o.stream) {
    main_stream = std::move(o.stream);
  }
  std::unique_ptr<schema_sanitizer_diagnostics> diagnostics;
  if (outputs.diagnostics) {
    diagnostics = std::make_unique<schema_sanitizer_diagnostics>();
    diagnostics->diagnostics = std::move(o.diagnostics);
    if (!diagnostics->diagnostics) {
      diagnostics->diagnostics =
          std::make_shared<sanitize::IngestDiagnostics>();
    }
    diagnostics->inference_snapshot = *diagnostics->diagnostics;
    diagnostics->has_inference_snapshot = true;
    if (!diagnostics)
      return set_oom_error(out_error, where);
  }
  if (outputs.stream && main_stream) {
    *outputs.stream = main_stream.release();
  }
  if (outputs.diagnostics && diagnostics) {
    *outputs.diagnostics = diagnostics.release();
  }
  return SCHEMA_SANITIZER_STATUS_OK;
}
// Validates the common context-to-sink arguments before ingestion starts.
int validate_context_sink_frontend(schema_sanitizer_context *ctx,
                                   const char *sink_name,
                                   const char *frontend_name,
                                   ArrowArrayStream **out_stream,
                                   const char *where, char **out_error) {
  if (!out_stream) {
    return set_error(out_error, std::string(where) + ": out_stream is null",
                     SCHEMA_SANITIZER_STATUS_INVALID_ARGUMENT);
  }
  int rc = ctx_check(ctx, where, out_error);
  if (rc != SCHEMA_SANITIZER_STATUS_OK)
    return rc;
  if (!sink_name || !*sink_name) {
    return set_error(out_error,
                     std::string(where) + ": sink_name is null/empty",
                     SCHEMA_SANITIZER_STATUS_INVALID_ARGUMENT);
  }
  if (!frontend_name || !*frontend_name) {
    return set_error(out_error,
                     std::string(where) + ": frontend_name is null/empty",
                     SCHEMA_SANITIZER_STATUS_INVALID_ARGUMENT);
  }
  return SCHEMA_SANITIZER_STATUS_OK;
}
// Reuses supplied prepared options or builds defaults for the sink call.
int resolve_prepared_options(
    schema_sanitizer_prepared_options *prepared_options,
    sanitize::PreparedOptionsPtr *out_prep, char **out_error) {
  if (prepared_options && prepared_options->prepared) {
    *out_prep = prepared_options->prepared;
    return SCHEMA_SANITIZER_STATUS_OK;
  }
  auto pr = default_prepared_options();
  if (!pr.ok()) {
    return set_error(out_error, pr.status().ToString(),
                     code_for_status(pr.status()));
  }
  *out_prep = std::move(pr).ValueOrDie();
  return SCHEMA_SANITIZER_STATUS_OK;
}
int schema_sanitizer_context_to_sink_text(
    schema_sanitizer_context *ctx, const char *sink_name,
    const char *frontend_name, const std::uint8_t *input_bytes,
    std::size_t input_len, schema_sanitizer_prepared_options *prepared_options,
    struct ArrowArrayStream **out_stream,
    schema_sanitizer_diagnostics **out_diagnostics, char **out_error) {
  static constexpr const char *kWhere = "schema_sanitizer_context_to_sink_text";
  clear_out(out_error);
  SinkOutputs outputs{.stream = out_stream, .diagnostics = out_diagnostics};
  clear_sink_outputs(outputs);
  int rc = validate_context_sink_frontend(ctx, sink_name, frontend_name,
                                          out_stream, kWhere, out_error);
  if (rc != SCHEMA_SANITIZER_STATUS_OK)
    return rc;
  if (!input_bytes && input_len != 0) {
    return set_error(out_error, std::string(kWhere) + ": input_bytes is null",
                     SCHEMA_SANITIZER_STATUS_INVALID_ARGUMENT);
  }
  sanitize::PreparedOptionsPtr prep;
  rc = resolve_prepared_options(prepared_options, &prep, out_error);
  if (rc != SCHEMA_SANITIZER_STATUS_OK)
    return rc;
  std::string bytes;
  if (input_len != 0) {
    bytes.assign(reinterpret_cast<const char *>(input_bytes), input_len);
  }
  auto src = sanitize::chunk_source_from_bytes(std::move(bytes));
  return context_to_sink_from_source_internal(ctx, sink_name, frontend_name,
                                              std::move(src), prep, outputs,
                                              out_error, kWhere);
}

int schema_sanitizer_context_to_sink_path(
    schema_sanitizer_context *ctx, const char *sink_name,
    const char *frontend_name, const char *input_path,
    schema_sanitizer_prepared_options *prepared_options,
    struct ArrowArrayStream **out_stream,
    schema_sanitizer_diagnostics **out_diagnostics, char **out_error) {
  static constexpr const char *kWhere = "schema_sanitizer_context_to_sink_path";
  clear_out(out_error);
  SinkOutputs outputs{.stream = out_stream, .diagnostics = out_diagnostics};
  clear_sink_outputs(outputs);

  int rc = validate_context_sink_frontend(ctx, sink_name, frontend_name,
                                          out_stream, kWhere, out_error);
  if (rc != SCHEMA_SANITIZER_STATUS_OK)
    return rc;
  if (!input_path || !*input_path) {
    return set_error(out_error,
                     std::string(kWhere) + ": input_path is null/empty",
                     SCHEMA_SANITIZER_STATUS_INVALID_ARGUMENT);
  }

  sanitize::PreparedOptionsPtr prep;
  rc = resolve_prepared_options(prepared_options, &prep, out_error);
  if (rc != SCHEMA_SANITIZER_STATUS_OK)
    return rc;

  auto src_r = sanitize::chunk_source_from_path_with_encoding(
      std::string(input_path), prep->spec.input_text_encoding);
  if (!src_r.ok()) {
    return set_error(out_error, src_r.status().ToString(),
                     code_for_status(src_r.status()));
  }
  return context_to_sink_from_source_internal(ctx, sink_name, frontend_name,
                                              std::move(src_r).ValueOrDie(),
                                              prep, outputs, out_error, kWhere);
}

void schema_sanitizer_stream_free(struct ArrowArrayStream *stream) {
  if (!stream)
    return;
  if (stream->release) {
    stream->release(stream);
  }
  delete stream;
}

void schema_sanitizer_diagnostics_free(
    schema_sanitizer_diagnostics *diagnostics) {
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
    }
    const std::string json = merged.to_json();
    *out_json = dup_cstr(json);
    if (!*out_json)
      return set_oom_error(out_error, kWhere);
    return SCHEMA_SANITIZER_STATUS_OK;
  } catch (const std::bad_alloc &) {
    return set_oom_error(out_error, kWhere);
  } catch (const std::exception &e) {
    return set_exception_error(out_error, kWhere, e);
  } catch (...) {
    return set_unknown_exception_error(out_error, kWhere);
  }
}
