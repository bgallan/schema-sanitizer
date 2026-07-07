// Declares the internal C bridge for schema_sanitizer bindings.

#pragma once

// C bridge used by schema-sanitizer bindings.
//
// This bridge uses Arrow's C Stream interface internally so the Python ABI3
// layer can move streams without depending on Arrow C++.
//
// Error handling:
// - All fallible functions return an int status code (see
// schema_sanitizer_status).
// - On error, functions optionally populate *out_error with a malloc-allocated
//   null-terminated string. The caller MUST free it using
//   schema_sanitizer_free_string().
// - No C++ exceptions are allowed to cross the bridge boundary.

#include <cstddef>
#include <cstdint>

// Forward declaration from the Arrow C Stream interface.
struct ArrowArrayStream;

// Status codes. Non-zero indicates failure.
enum schema_sanitizer_status : std::uint8_t {
  SCHEMA_SANITIZER_STATUS_OK = 0,
  SCHEMA_SANITIZER_STATUS_INVALID_ARGUMENT = 1,
  SCHEMA_SANITIZER_STATUS_OUT_OF_MEMORY = 2,
  SCHEMA_SANITIZER_STATUS_RUNTIME_ERROR = 3
};

// Opaque handles.
struct schema_sanitizer_context;
struct schema_sanitizer_diagnostics;
struct schema_sanitizer_prepared_options;

// Frees a string returned by this API (for example diagnostics JSON or errors).
void schema_sanitizer_free_string(char *p);

// Create/free an execution context.
//
// On success: returns SCHEMA_SANITIZER_STATUS_OK and sets *out_ctx.
// On error: returns non-zero and (optionally) sets *out_error.
int schema_sanitizer_context_new(schema_sanitizer_context **out_ctx,
                                 char **out_error);
// Releases an execution context created by schema_sanitizer_context_new().
void schema_sanitizer_context_free(schema_sanitizer_context *ctx);

// ---------------- Context memory stats ----------------
// Each *_json function returns a malloc-allocated JSON string which MUST be
// freed by schema_sanitizer_free_string().

int schema_sanitizer_context_memory_stats_json(schema_sanitizer_context *ctx,
                                               char **out_json,
                                               char **out_error);

// Prepare/free prepared options from portable bytes.
//
// On success: returns OK and sets *out_prepared.
// On error: returns non-zero and (optionally) sets *out_error.
int schema_sanitizer_options_prepare_bytes(
    const std::uint8_t *bytes, std::size_t len,
    schema_sanitizer_prepared_options **out_prepared, char **out_error);

// Releases prepared options created by
// schema_sanitizer_options_prepare_bytes().
void schema_sanitizer_prepared_options_free(
    schema_sanitizer_prepared_options *p);

// ---------------- Sink conversion API ----------------
//
// These APIs run a named sink (e.g. "stream" or "table")
// against an input, returning a stream handle for the main output.
//
// On success: returns OK and sets *out_stream. If the sink produces no
// table/stream, *out_stream will be set to NULL.
//
// Diagnostics JSON strings are malloc-allocated and must be freed with
// schema_sanitizer_free_string().

int schema_sanitizer_context_to_sink_text(
    schema_sanitizer_context *ctx, const char *sink_name,
    const char *frontend_name, const std::uint8_t *input_bytes,
    std::size_t input_len, schema_sanitizer_prepared_options *prepared_options,
    ArrowArrayStream **out_stream,
    schema_sanitizer_diagnostics **out_diagnostics, char **out_error);

// Converts a context and input path into an Arrow C Stream sink output.
int schema_sanitizer_context_to_sink_path(
    schema_sanitizer_context *ctx, const char *sink_name,
    const char *frontend_name, const char *input_path,
    schema_sanitizer_prepared_options *prepared_options,
    ArrowArrayStream **out_stream,
    schema_sanitizer_diagnostics **out_diagnostics, char **out_error);

// Converts text input through registry-backed schema preparation into a stream.
int schema_sanitizer_context_to_registry_sink_text(
    schema_sanitizer_context *ctx, const char *sink_name,
    const char *frontend_name, const std::uint8_t *input_bytes,
    std::size_t input_len, schema_sanitizer_prepared_options *prepared_options,
    const char *registry_json, const char *field_name_policy,
    const char *schema_mode, ArrowArrayStream **out_stream,
    schema_sanitizer_diagnostics **out_diagnostics, char **out_registry_json,
    char **out_drifts_json, char **out_conversion_timestamp, char **out_error);

// Converts path input through registry-backed schema preparation into a stream.
int schema_sanitizer_context_to_registry_sink_path(
    schema_sanitizer_context *ctx, const char *sink_name,
    const char *frontend_name, const char *input_path,
    schema_sanitizer_prepared_options *prepared_options,
    const char *registry_json, const char *field_name_policy,
    const char *schema_mode, ArrowArrayStream **out_stream,
    schema_sanitizer_diagnostics **out_diagnostics, char **out_registry_json,
    char **out_drifts_json, char **out_conversion_timestamp, char **out_error);

// Releases and deletes a stream returned by this API.
void schema_sanitizer_stream_free(ArrowArrayStream *stream);

// Releases diagnostics returned by sink conversion APIs.
void schema_sanitizer_diagnostics_free(
    schema_sanitizer_diagnostics *diagnostics);

// Serializes live diagnostics to JSON. The returned string must be freed with
// schema_sanitizer_free_string().
int schema_sanitizer_diagnostics_json(schema_sanitizer_diagnostics *diagnostics,
                                      char **out_json, char **out_error);
