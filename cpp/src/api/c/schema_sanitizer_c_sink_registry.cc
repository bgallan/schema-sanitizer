/*
 * C bridge registry-backed sink execution APIs.
 *
 * This file implements registry-backed context-to-sink entry points that merge
 * schema registry state before returning Arrow streams plus diagnostics JSON.
 */
#include "api/c/schema_sanitizer_c_sink_internal.hh"

#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <new>
#include <string>
#include <string_view>
#include <utility>

#include "internal/planning/plan_compile.hh"
#include "sanitize/abi/cdata_types.hh"
#include "sanitize/ingest/chunk_source.hh"
#include "sanitize/ingest/ingest.hh"
#include "sanitize/ingest/ingest_types.hh"
#include "sanitize/options/options.hh"
#include "sanitize/registry/registry.hh"
#include "sanitize/schema_registry/schema_registry.hh"

int validate_registry_sink_mode(const char *schema_mode,
                                const char *registry_json, char **out_error,
                                const char *where) {
  const std::string_view mode(schema_mode ? schema_mode : "additive");
  if (mode == "strict") {
    auto has_schema = sanitize::schema_registry_has_canonical_schema(
        registry_json ? registry_json : "");
    if (!has_schema.ok()) {
      return set_error(out_error, has_schema.status().ToString(),
                       code_for_status(has_schema.status()));
    }
    if (!has_schema.ValueOrDie()) {
      return set_error(
          out_error,
          std::string(where) +
              ": schema_mode='strict' requires schema_registry to contain "
              "canonical_schema. Use schema_mode='additive' for the first "
              "registry-backed run.",
          SCHEMA_SANITIZER_STATUS_INVALID_ARGUMENT);
    }
    return SCHEMA_SANITIZER_STATUS_OK;
  }
  if (mode == "additive") {
    return SCHEMA_SANITIZER_STATUS_OK;
  }
  return set_error(out_error,
                   std::string(where) +
                       ": schema_mode must be 'strict' or 'additive'",
                   SCHEMA_SANITIZER_STATUS_INVALID_ARGUMENT);
}

sanitize::SchemaRegistryMergeInput make_registry_merge_input(
    sanitize::LogicalSchema inferred_schema, const char *registry_json,
    const char *field_name_policy, std::string_view default_key_name) {
  sanitize::SchemaRegistryMergeInput input;
  input.inferred_schema = std::move(inferred_schema);
  input.registry_json = registry_json ? registry_json : "";
  input.field_name_policy =
      field_name_policy ? field_name_policy : "lower_snake";
  input.default_key_name = default_key_name.empty()
                               ? std::string("default_key")
                               : std::string(default_key_name);
  return input;
}

int copy_registry_json_outputs(
    const sanitize::SchemaRegistryMergeResult &merged,
    RegistrySinkOutputs outputs, char **out_error, const char *where) {
  if (outputs.registry_json) {
    *outputs.registry_json = dup_cstr(merged.registry_json);
    if (!*outputs.registry_json) {
      return set_oom_error(out_error, where);
    }
  }
  if (outputs.drifts_json) {
    *outputs.drifts_json = dup_cstr(merged.drifts_json);
    if (!*outputs.drifts_json) {
      if (outputs.registry_json) {
        schema_sanitizer_free_string(*outputs.registry_json);
        *outputs.registry_json = nullptr;
      }
      return set_oom_error(out_error, where);
    }
  }
  if (outputs.conversion_timestamp) {
    *outputs.conversion_timestamp = dup_cstr(merged.detected_at);
    if (!*outputs.conversion_timestamp) {
      if (outputs.registry_json) {
        schema_sanitizer_free_string(*outputs.registry_json);
        *outputs.registry_json = nullptr;
      }
      if (outputs.drifts_json) {
        schema_sanitizer_free_string(*outputs.drifts_json);
        *outputs.drifts_json = nullptr;
      }
      return set_oom_error(out_error, where);
    }
  }
  return SCHEMA_SANITIZER_STATUS_OK;
}

// Converts in-memory text into a registry-backed sink output.
int schema_sanitizer_context_to_registry_sink_text(
    schema_sanitizer_context *ctx, const char *sink_name,
    const char *frontend_name, const std::uint8_t *input_bytes,
    std::size_t input_len, schema_sanitizer_prepared_options *prepared_options,
    const char *registry_json, const char *field_name_policy,
    const char *schema_mode, ArrowArrayStream **out_stream,
    schema_sanitizer_diagnostics **out_diagnostics, char **out_registry_json,
    char **out_drifts_json, char **out_conversion_timestamp, char **out_error) {
  static constexpr const char *kWhere =
      "schema_sanitizer_context_to_registry_sink_text";
  clear_out(out_error);
  RegistrySinkOutputs outputs{
      .sink = SinkOutputs{.stream = out_stream, .diagnostics = out_diagnostics},
      .registry_json = out_registry_json,
      .drifts_json = out_drifts_json,
      .conversion_timestamp = out_conversion_timestamp};
  clear_registry_sink_outputs(outputs);
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
  return context_to_registry_sink_from_source_internal(
      ctx, sink_name, frontend_name, std::move(src), prep, registry_json,
      field_name_policy, schema_mode, outputs, out_error, kWhere);
}

// Converts a filesystem path into a registry-backed sink output.
int schema_sanitizer_context_to_registry_sink_path(
    schema_sanitizer_context *ctx, const char *sink_name,
    const char *frontend_name, const char *input_path,
    schema_sanitizer_prepared_options *prepared_options,
    const char *registry_json, const char *field_name_policy,
    const char *schema_mode, ArrowArrayStream **out_stream,
    schema_sanitizer_diagnostics **out_diagnostics, char **out_registry_json,
    char **out_drifts_json, char **out_conversion_timestamp, char **out_error) {
  static constexpr const char *kWhere =
      "schema_sanitizer_context_to_registry_sink_path";
  clear_out(out_error);
  RegistrySinkOutputs outputs{
      .sink = SinkOutputs{.stream = out_stream, .diagnostics = out_diagnostics},
      .registry_json = out_registry_json,
      .drifts_json = out_drifts_json,
      .conversion_timestamp = out_conversion_timestamp};
  clear_registry_sink_outputs(outputs);

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
  return context_to_registry_sink_from_source_internal(
      ctx, sink_name, frontend_name, std::move(src_r).ValueOrDie(), prep,
      registry_json, field_name_policy, schema_mode, outputs, out_error,
      kWhere);
}
