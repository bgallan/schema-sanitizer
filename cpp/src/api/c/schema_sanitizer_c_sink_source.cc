/*
 * C bridge source-dispatch helpers for normal sinks.
 *
 * This file owns the source-to-ingest pipeline used by text/path/source C
 * bridge wrappers.
 */
#include "api/c/schema_sanitizer_c_sink_internal.hh"

#include <exception>
#include <memory>
#include <new>
#include <string>
#include <string_view>
#include <utility>

#include "internal/abi/schema_sanitizer_c_bridge.hh"
#include "sanitize/ingest/chunk_source.hh"
#include "sanitize/registry/registry.hh"

int context_to_sink_from_source_internal(
    schema_sanitizer_context *ctx, const char *sink_name,
    const char *frontend_name, sanitize::ChunkSourcePtr src,
    const sanitize::PreparedOptionsPtr &prep, SinkOutputs outputs,
    char **out_error, const char *where) {
  try {
    auto fe = sanitize::make_builtin_frontend(frontend_name, std::move(src),
                                              prep->spec);
    if (!fe) {
      return set_error(out_error,
                       std::string(where) + ": frontend not registered: " +
                           std::string(frontend_name),
                       SCHEMA_SANITIZER_STATUS_RUNTIME_ERROR);
    }
    auto prep_r = sanitize::prepare_ingest(frontend_name, std::move(fe), prep,
                                           ctx->ctx.get());
    if (!prep_r.ok()) {
      return set_error(out_error, prep_r.status().ToString(),
                       code_for_status(prep_r.status()));
    }
    auto prepared = std::move(prep_r).ValueOrDie();
    prepared.owned_ctx = ctx->ctx;
    prepared.ctx = ctx->ctx.get();
    const std::string_view sink(sink_name);
    if (sink != "table" && sink != "stream") {
      return set_error(out_error, "sink not registered: " + std::string(sink),
                       SCHEMA_SANITIZER_STATUS_INVALID_ARGUMENT);
    }

    auto out_r = sanitize::ingest_to_stream(std::move(prepared));
    if (!out_r.ok()) {
      return set_error(out_error, out_r.status().ToString(),
                       code_for_status(out_r.status()));
    }
    return ingest_stream_to_streams(std::move(out_r).ValueOrDie(), outputs,
                                    out_error, where);
  } catch (const std::bad_alloc &) {
    return set_oom_error(out_error, where);
  } catch (const std::exception &e) {
    return set_exception_error(out_error, where, e);
  } catch (...) {
    return set_unknown_exception_error(out_error, where);
  }
}

int schema_sanitizer_context_to_sink_from_source(
    schema_sanitizer_context *ctx, const char *sink_name,
    const char *frontend_name, sanitize::ChunkSourcePtr src,
    const sanitize::PreparedOptionsPtr &prep, ArrowArrayStream **out_stream,
    schema_sanitizer_diagnostics **out_diagnostics, char **out_error,
    const char *where) {
  SinkOutputs outputs{.stream = out_stream, .diagnostics = out_diagnostics};
  clear_sink_outputs(outputs);
  return context_to_sink_from_source_internal(ctx, sink_name, frontend_name,
                                              std::move(src), prep, outputs,
                                              out_error, where);
}
