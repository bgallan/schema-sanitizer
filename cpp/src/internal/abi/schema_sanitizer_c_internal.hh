// Declares shared C bridge status and context validation helpers.

#pragma once

#include "internal/abi/schema_sanitizer_c_bridge.hh"

#include <exception>
#include <memory>
#include <string>

#include "sanitize/core/diagnostics.hh"
#include "sanitize/core/status.hh"
#include "sanitize/ingest/chunk_source.hh"
#include "sanitize/options/options.hh"
#include "sanitize/runtime/execution_context.hh"

struct schema_sanitizer_context {
  std::shared_ptr<sanitize::ExecutionContext> ctx;
};

struct schema_sanitizer_diagnostics {
  std::shared_ptr<sanitize::IngestDiagnostics> diagnostics;
  sanitize::IngestDiagnostics inference_snapshot;
  bool has_inference_snapshot = false;
};

struct schema_sanitizer_prepared_options {
  sanitize::PreparedOptionsPtr prepared;
};

// Duplicates a C++ string into malloc-owned C storage.
char *dup_cstr(const std::string &s);
// Clears and frees a C bridge output string.
void clear_out(char **out_error);
// Stores an error message in C bridge output storage and returns a status code.
int set_error(char **out_error, const std::string &msg, int code);
// Stores a standardized out-of-memory bridge error.
int set_oom_error(char **out_error, const char *where);
// Stores a standardized std::exception bridge error.
int set_exception_error(char **out_error, const char *where,
                        const std::exception &error);
// Stores a standardized unknown-exception bridge error.
int set_unknown_exception_error(char **out_error, const char *where);
// Maps an internal status to the C bridge status code.
int code_for_status(const sanitize::Status &st);

// Builds the default prepared options used when callers omit options.
sanitize::Result<sanitize::PreparedOptionsPtr> default_prepared_options();
// Converts a context/source pair into a named sink output.
int schema_sanitizer_context_to_sink_from_source(
    schema_sanitizer_context *ctx, const char *sink_name,
    const char *frontend_name, sanitize::ChunkSourcePtr src,
    const sanitize::PreparedOptionsPtr &prep, ArrowArrayStream **out_stream,
    schema_sanitizer_diagnostics **out_diagnostics, char **out_error,
    const char *where);

int schema_sanitizer_context_to_registry_sink_from_source(
    schema_sanitizer_context *ctx, const char *sink_name,
    const char *frontend_name, sanitize::ChunkSourcePtr src,
    const sanitize::PreparedOptionsPtr &prep, const char *registry_json,
    const char *field_name_policy, const char *schema_mode,
    ArrowArrayStream **out_stream,
    schema_sanitizer_diagnostics **out_diagnostics, char **out_registry_json,
    char **out_drifts_json, char **out_conversion_timestamp, char **out_error,
    const char *where);

// Validates a C bridge context and writes a contextual error on failure.
int ctx_check(schema_sanitizer_context *ctx, const char *where,
              char **out_error);
