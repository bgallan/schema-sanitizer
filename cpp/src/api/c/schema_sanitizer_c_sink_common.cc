// Shared C bridge sink validation and output helpers.

#include "api/c/schema_sanitizer_c_sink_internal.hh"

#include <memory>
#include <string>
#include <utility>

#include "internal/abi/schema_sanitizer_c_bridge.hh"
#include "sanitize/options/options.hh"

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

int ingest_stream_to_streams(sanitize::IngestStream output, SinkOutputs outputs,
                             char **out_error, const char *where) {
  clear_sink_outputs(outputs);
  sanitize::UniqueCStream main_stream;
  if (outputs.stream && output.stream)
    main_stream = std::move(output.stream);

  std::unique_ptr<schema_sanitizer_diagnostics> diagnostics;
  if (outputs.diagnostics) {
    diagnostics = std::make_unique<schema_sanitizer_diagnostics>();
    diagnostics->diagnostics = std::move(output.diagnostics);
    if (!diagnostics->diagnostics)
      diagnostics->diagnostics =
          std::make_shared<sanitize::IngestDiagnostics>();
    diagnostics->inference_snapshot = *diagnostics->diagnostics;
    diagnostics->has_inference_snapshot = true;
    if (!diagnostics)
      return set_oom_error(out_error, where);
  }
  if (outputs.stream && main_stream)
    *outputs.stream = main_stream.release();
  if (outputs.diagnostics && diagnostics)
    *outputs.diagnostics = diagnostics.release();
  return SCHEMA_SANITIZER_STATUS_OK;
}

int validate_context_sink_frontend(schema_sanitizer_context *ctx,
                                   const char *sink_name,
                                   const char *frontend_name,
                                   ArrowArrayStream **out_stream,
                                   const char *where, char **out_error) {
  if (!out_stream) {
    return set_error(out_error, std::string(where) + ": out_stream is null",
                     SCHEMA_SANITIZER_STATUS_INVALID_ARGUMENT);
  }
  const int rc = ctx_check(ctx, where, out_error);
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

int resolve_prepared_options(
    schema_sanitizer_prepared_options *prepared_options,
    sanitize::PreparedOptionsPtr *out_prep, char **out_error) {
  if (prepared_options && prepared_options->prepared) {
    *out_prep = prepared_options->prepared;
    return SCHEMA_SANITIZER_STATUS_OK;
  }
  auto prepared = default_prepared_options();
  if (!prepared.ok()) {
    return set_error(out_error, prepared.status().ToString(),
                     code_for_status(prepared.status()));
  }
  *out_prep = std::move(prepared).ValueOrDie();
  return SCHEMA_SANITIZER_STATUS_OK;
}
