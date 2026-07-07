/*
 * C bridge source-dispatch helpers for registry-backed sinks.
 *
 * This file owns the source-to-registry-merge pipeline used by registry text,
 * path, and source C bridge wrappers.
 */
#include "api/c/schema_sanitizer_c_sink_internal.hh"

#include <exception>
#include <memory>
#include <new>
#include <string>
#include <utility>

#include "internal/planning/plan_compile.hh"
#include "sanitize/ingest/chunk_source.hh"
#include "sanitize/registry/registry.hh"

int context_to_registry_sink_from_source_internal(
    schema_sanitizer_context *ctx, const char *sink_name,
    const char *frontend_name, sanitize::ChunkSourcePtr src,
    const sanitize::PreparedOptionsPtr &prep, const char *registry_json,
    const char *field_name_policy, const char *schema_mode,
    RegistrySinkOutputs outputs, char **out_error, const char *where) {
  try {
    (void)sink_name;
    int rc = validate_registry_sink_mode(schema_mode, registry_json, out_error,
                                         where);
    if (rc != SCHEMA_SANITIZER_STATUS_OK)
      return rc;

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

    auto merged_r = sanitize::merge_schema_registry(make_registry_merge_input(
        std::move(prepared.logical_schema), registry_json, field_name_policy,
        prep->spec.default_key_name, prep->spec.field_order));
    if (!merged_r.ok()) {
      return set_error(out_error, merged_r.status().ToString(),
                       code_for_status(merged_r.status()));
    }
    auto merged = std::move(merged_r).ValueOrDie();

    auto compiled_r = sanitize::compile_plan(merged.schema);
    if (!compiled_r.ok()) {
      return set_error(out_error, compiled_r.status().ToString(),
                       code_for_status(compiled_r.status()));
    }
    auto plan = std::make_shared<sanitize::CompiledPlan>(
        std::move(compiled_r).ValueOrDie());
    prepared.frontend.set_plan(plan.get());
    prepared.plan = plan;
    prepared.logical_schema = merged.schema;
    if (prepared.diagnostics) {
      prepared.diagnostics->arrow_schema_depth =
          sanitize::arrow_schema_depth(merged.schema);
      prepared.diagnostics->parquet_schema_depth =
          sanitize::parquet_schema_depth(merged.schema);
    }

    rc = copy_registry_json_outputs(merged, outputs, out_error, where);
    if (rc != SCHEMA_SANITIZER_STATUS_OK)
      return rc;

    auto out_r = sanitize::ingest_to_stream(std::move(prepared));
    if (!out_r.ok()) {
      return set_error(out_error, out_r.status().ToString(),
                       code_for_status(out_r.status()));
    }
    return ingest_stream_to_streams(std::move(out_r).ValueOrDie(), outputs.sink,
                                    out_error, where);
  } catch (const std::bad_alloc &) {
    return set_oom_error(out_error, where);
  } catch (const std::exception &e) {
    return set_exception_error(out_error, where, e);
  } catch (...) {
    return set_unknown_exception_error(out_error, where);
  }
}

int schema_sanitizer_context_to_registry_sink_from_source(
    schema_sanitizer_context *ctx, const char *sink_name,
    const char *frontend_name, sanitize::ChunkSourcePtr src,
    const sanitize::PreparedOptionsPtr &prep, const char *registry_json,
    const char *field_name_policy, const char *schema_mode,
    ArrowArrayStream **out_stream,
    schema_sanitizer_diagnostics **out_diagnostics, char **out_registry_json,
    char **out_drifts_json, char **out_conversion_timestamp, char **out_error,
    const char *where) {
  RegistrySinkOutputs outputs{
      .sink = SinkOutputs{.stream = out_stream, .diagnostics = out_diagnostics},
      .registry_json = out_registry_json,
      .drifts_json = out_drifts_json,
      .conversion_timestamp = out_conversion_timestamp};
  clear_registry_sink_outputs(outputs);
  return context_to_registry_sink_from_source_internal(
      ctx, sink_name, frontend_name, std::move(src), prep, registry_json,
      field_name_policy, schema_mode, outputs, out_error, where);
}
