/*
 * Implements Python ABI3 auto-registry path-source sink methods.
 *
 * The routines preserve source order and Arrow ownership while applying
 * compiled registry plans.
 */

#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/abi/python_abi3/methods.hh"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "api/python_abi3/registry/path_source_sinks_internal.hh"
#include "api/python_abi3/registry/plan/plan.hh"
#include "api/python_abi3/registry/registry_stream_metadata.hh"

namespace core_abi3_internal {
using namespace path_registry_detail;

/// Probes path-source chunks, compiles their registry plan, and streams later
/// chunks.
PyObject *
py_context_to_registry_sink_from_path_source_chunk_provider_auto_registry(
    PyObject *, PyObject *args) {
  PyObject *ctx_obj = nullptr;
  const char *sink_name = nullptr;
  PyObject *probe_provider_obj = nullptr;
  PyObject *stream_provider_obj = nullptr;
  PyObject *prepared_obj = Py_None;
  const char *registry_json = nullptr;
  const char *field_name_policy = nullptr;
  const char *schema_mode = nullptr;
  PyObject *first_row_columns = nullptr;
  PyObject *timestamp_columns = nullptr;
  int skip_invalid_json_sources = 0;

  if (!PyArg_ParseTuple(
          args,
          "OsOOOsssOO|p:context_to_registry_sink_from_path_source_chunk_"
          "provider_auto_registry",
          &ctx_obj, &sink_name, &probe_provider_obj, &stream_provider_obj,
          &prepared_obj, &registry_json, &field_name_policy, &schema_mode,
          &first_row_columns, &timestamp_columns, &skip_invalid_json_sources)) {
    return nullptr;
  }
  auto *ctx = unwrap_context(ctx_obj);
  if (!ctx || !provider_has_next_sources(probe_provider_obj) ||
      !provider_has_next_sources(stream_provider_obj)) {
    return nullptr;
  }

  sanitize::PreparedOptionsPtr prepared_options;
  if (!resolve_prepared_options(prepared_obj, &prepared_options)) {
    return nullptr;
  }
  const auto valid = validate_registry_sink_mode(
      schema_mode, registry_json,
      "context_to_registry_sink_from_path_source_chunk_provider_auto_registry");
  if (!valid.ok()) {
    raise_status_error(valid);
    return nullptr;
  }

  auto merged = merge_path_source_provider_schemas(
      ctx, probe_provider_obj, prepared_options, registry_json,
      field_name_policy ? field_name_policy : "",
      skip_invalid_json_sources != 0, nullptr,
      registry_schema_evolution_mode(schema_mode));
  if (!merged.ok()) {
    raise_status_error(merged.status());
    return nullptr;
  }
  auto plan = make_native_registry_plan(std::move(merged).ValueOrDie());
  if (!plan.ok()) {
    raise_status_error(plan.status());
    return nullptr;
  }
  return pack_chunk_provider_registry_stream(
      ctx_obj, ctx, sink_name, stream_provider_obj, prepared_options,
      std::move(plan).ValueOrDie(), field_name_policy, schema_mode,
      first_row_columns, timestamp_columns);
}

/// Extends registry state from path-source chunks and streams under the new
/// plan.
PyObject *
py_context_to_registry_sink_from_path_source_chunk_provider_auto_registry_state(
    PyObject *, PyObject *args) {
  PyObject *ctx_obj = nullptr;
  const char *sink_name = nullptr;
  PyObject *probe_provider_obj = nullptr;
  PyObject *stream_provider_obj = nullptr;
  PyObject *prepared_obj = Py_None;
  PyObject *registry_state_obj = Py_None;
  const char *field_name_policy = nullptr;
  const char *schema_mode = nullptr;
  PyObject *first_row_columns = nullptr;
  PyObject *timestamp_columns = nullptr;
  int skip_invalid_json_sources = 0;

  if (!PyArg_ParseTuple(
          args,
          "OsOOOOssOO|p:context_to_registry_sink_from_path_source_chunk_"
          "provider_auto_registry_state",
          &ctx_obj, &sink_name, &probe_provider_obj, &stream_provider_obj,
          &prepared_obj, &registry_state_obj, &field_name_policy, &schema_mode,
          &first_row_columns, &timestamp_columns, &skip_invalid_json_sources)) {
    return nullptr;
  }
  auto *ctx = unwrap_context(ctx_obj);
  auto base_registry_plan = native_registry_state_from_py(registry_state_obj);
  if (!ctx || !base_registry_plan ||
      !provider_has_next_sources(probe_provider_obj) ||
      !provider_has_next_sources(stream_provider_obj)) {
    if (!base_registry_plan && !PyErr_Occurred()) {
      PyErr_SetString(PyExc_ValueError,
                      "native registry state does not contain a compiled plan");
    }
    return nullptr;
  }

  sanitize::PreparedOptionsPtr prepared_options;
  if (!resolve_prepared_options(prepared_obj, &prepared_options)) {
    return nullptr;
  }
  const auto valid = validate_registry_sink_mode(
      schema_mode, base_registry_plan->registry_json,
      "context_to_registry_sink_from_path_source_chunk_provider_auto_registry_"
      "state");
  if (!valid.ok()) {
    raise_status_error(valid);
    return nullptr;
  }

  auto merged = merge_path_source_provider_schemas(
      ctx, probe_provider_obj, prepared_options,
      base_registry_plan->registry_json.c_str(),
      field_name_policy ? field_name_policy : "",
      skip_invalid_json_sources != 0, &base_registry_plan->schema,
      registry_schema_evolution_mode(schema_mode));
  if (!merged.ok()) {
    raise_status_error(merged.status());
    return nullptr;
  }
  auto plan = make_native_registry_plan(std::move(merged).ValueOrDie());
  if (!plan.ok()) {
    raise_status_error(plan.status());
    return nullptr;
  }
  return pack_chunk_provider_registry_stream(
      ctx_obj, ctx, sink_name, stream_provider_obj, prepared_options,
      std::move(plan).ValueOrDie(), field_name_policy, schema_mode,
      first_row_columns, timestamp_columns);
}

/// Probes explicit path sources, compiles their registry plan, and streams
/// them.
PyObject *
py_context_to_registry_sink_from_path_sources_auto_registry(PyObject *,
                                                            PyObject *args) {
  PyObject *ctx_obj = nullptr;
  const char *sink_name = nullptr;
  PyObject *sources_obj = nullptr;
  PyObject *prepared_obj = Py_None;
  const char *registry_json = nullptr;
  const char *field_name_policy = nullptr;
  const char *schema_mode = nullptr;
  PyObject *first_row_columns = nullptr;
  PyObject *timestamp_columns = nullptr;
  int skip_invalid_json_sources = 0;

  if (!PyArg_ParseTuple(args,
                        "OsOOsssOO|p:context_to_registry_sink_from_path_"
                        "sources_auto_registry",
                        &ctx_obj, &sink_name, &sources_obj, &prepared_obj,
                        &registry_json, &field_name_policy, &schema_mode,
                        &first_row_columns, &timestamp_columns,
                        &skip_invalid_json_sources)) {
    return nullptr;
  }
  auto *ctx = unwrap_context(ctx_obj);
  if (!ctx)
    return nullptr;

  auto state = std::make_unique<NativePathSourcesStreamState>();
  state->ctx = ctx;
  state->sink_name = sink_name ? sink_name : "stream";
  state->registry_json = registry_json ? registry_json : "{}";
  state->field_name_policy = field_name_policy ? field_name_policy : "";
  state->schema_mode = schema_mode ? schema_mode : "additive";
  if (!resolve_prepared_options(prepared_obj, &state->prepared)) {
    return nullptr;
  }
  if (!parse_path_sources(sources_obj, &state->sources)) {
    return nullptr;
  }
  if (!append_registry_metadata_columns(first_row_columns, timestamp_columns,
                                        &state->first_row_columns,
                                        &state->timestamp_columns)) {
    return nullptr;
  }

  const auto valid = validate_registry_sink_mode(
      schema_mode, registry_json,
      "context_to_registry_sink_from_path_sources_auto_registry");
  if (!valid.ok()) {
    raise_status_error(valid);
    return nullptr;
  }
  auto merged_r = merge_path_source_schemas(
      ctx, state->sources, state->prepared, state->registry_json.c_str(),
      state->field_name_policy.c_str(), skip_invalid_json_sources != 0, nullptr,
      registry_schema_evolution_mode(state->schema_mode.c_str()));
  if (!merged_r.ok()) {
    raise_status_error(merged_r.status());
    return nullptr;
  }
  auto probe = std::move(merged_r).ValueOrDie();
  auto &merged = probe.merged;
  auto plan_r = make_native_registry_plan(std::move(merged));
  if (!plan_r.ok()) {
    raise_status_error(plan_r.status());
    return nullptr;
  }
  state->registry_plan = std::move(plan_r).ValueOrDie();
  state->registry_json = state->registry_plan->registry_json;
  state->drifts_json = state->registry_plan->drifts_json;
  state->conversion_timestamp = state->registry_plan->conversion_timestamp;
  append_registry_first_row_columns(&state->first_row_columns,
                                    state->registry_json, state->drifts_json);

  return pack_path_source_registry_stream(ctx_obj, std::move(state));
}

/// Extends registry state from explicit path sources before streaming them.
PyObject *py_context_to_registry_sink_from_path_sources_auto_registry_state(
    PyObject *, PyObject *args) {
  PyObject *ctx_obj = nullptr;
  const char *sink_name = nullptr;
  PyObject *sources_obj = nullptr;
  PyObject *prepared_obj = Py_None;
  PyObject *registry_state_obj = Py_None;
  const char *field_name_policy = nullptr;
  const char *schema_mode = nullptr;
  PyObject *first_row_columns = nullptr;
  PyObject *timestamp_columns = nullptr;
  int skip_invalid_json_sources = 0;

  if (!PyArg_ParseTuple(
          args,
          "OsOOOssOO|p:context_to_registry_sink_from_path_sources_auto_"
          "registry_state",
          &ctx_obj, &sink_name, &sources_obj, &prepared_obj,
          &registry_state_obj, &field_name_policy, &schema_mode,
          &first_row_columns, &timestamp_columns, &skip_invalid_json_sources)) {
    return nullptr;
  }
  auto *ctx = unwrap_context(ctx_obj);
  if (!ctx)
    return nullptr;
  auto base_registry_plan = native_registry_state_from_py(registry_state_obj);
  if (!base_registry_plan) {
    if (!PyErr_Occurred()) {
      PyErr_SetString(PyExc_ValueError,
                      "native registry state does not contain a compiled plan");
    }
    return nullptr;
  }

  auto state = std::make_unique<NativePathSourcesStreamState>();
  state->ctx = ctx;
  state->sink_name = sink_name ? sink_name : "stream";
  state->registry_json = base_registry_plan->registry_json;
  state->field_name_policy = field_name_policy ? field_name_policy : "";
  state->schema_mode = schema_mode ? schema_mode : "additive";
  if (!resolve_prepared_options(prepared_obj, &state->prepared)) {
    return nullptr;
  }
  if (!parse_path_sources(sources_obj, &state->sources)) {
    return nullptr;
  }
  if (!append_registry_metadata_columns(first_row_columns, timestamp_columns,
                                        &state->first_row_columns,
                                        &state->timestamp_columns)) {
    return nullptr;
  }

  const auto valid = validate_registry_sink_mode(
      state->schema_mode, state->registry_json,
      "context_to_registry_sink_from_path_sources_auto_registry_state");
  if (!valid.ok()) {
    raise_status_error(valid);
    return nullptr;
  }
  auto merged_r = merge_path_source_schemas(
      ctx, state->sources, state->prepared, state->registry_json.c_str(),
      state->field_name_policy.c_str(), skip_invalid_json_sources != 0,
      &base_registry_plan->schema,
      registry_schema_evolution_mode(state->schema_mode.c_str()));
  if (!merged_r.ok()) {
    raise_status_error(merged_r.status());
    return nullptr;
  }
  auto probe = std::move(merged_r).ValueOrDie();
  auto &merged = probe.merged;
  auto plan_r = make_native_registry_plan(std::move(merged));
  if (!plan_r.ok()) {
    raise_status_error(plan_r.status());
    return nullptr;
  }
  state->registry_plan = std::move(plan_r).ValueOrDie();
  state->registry_json = state->registry_plan->registry_json;
  state->drifts_json = state->registry_plan->drifts_json;
  state->conversion_timestamp = state->registry_plan->conversion_timestamp;
  append_registry_first_row_columns(&state->first_row_columns,
                                    state->registry_json, state->drifts_json);

  return pack_path_source_registry_stream(ctx_obj, std::move(state));
}

} // namespace core_abi3_internal
