// C bridge sink entry points for text and path inputs.

#include "api/c/schema_sanitizer_c_sink_internal.hh"

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>

#include "internal/abi/schema_sanitizer_c_bridge.hh"
#include "sanitize/ingest/chunk_source.hh"
#include "sanitize/options/options.hh"

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
  sanitize::PreparedOptionsPtr prepared;
  rc = resolve_prepared_options(prepared_options, &prepared, out_error);
  if (rc != SCHEMA_SANITIZER_STATUS_OK)
    return rc;
  std::string bytes;
  if (input_len != 0)
    bytes.assign(reinterpret_cast<const char *>(input_bytes), input_len);
  auto source = sanitize::chunk_source_from_bytes(std::move(bytes));
  return context_to_sink_from_source_internal(ctx, sink_name, frontend_name,
                                              std::move(source), prepared,
                                              outputs, out_error, kWhere);
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

  sanitize::PreparedOptionsPtr prepared;
  rc = resolve_prepared_options(prepared_options, &prepared, out_error);
  if (rc != SCHEMA_SANITIZER_STATUS_OK)
    return rc;
  auto source = sanitize::chunk_source_from_path_with_encoding(
      std::string(input_path), prepared->spec.input_text_encoding,
      prepared->spec.memory_limit_bytes);
  if (!source.ok()) {
    return set_error(out_error, source.status().ToString(),
                     code_for_status(source.status()));
  }
  return context_to_sink_from_source_internal(
      ctx, sink_name, frontend_name, std::move(source).ValueOrDie(), prepared,
      outputs, out_error, kWhere);
}
