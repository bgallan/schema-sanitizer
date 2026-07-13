/* Python ABI3 Arrow-source provider registry sink methods. */
#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/abi/python_abi3/methods.hh"

#include <cstdint>
#include <cstring>
#include <memory>
#include <new>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "api/python_abi3/registry/arrow_source_sinks_internal.hh"
#include "api/python_abi3/registry/plan/plan.hh"
#include "api/python_abi3/registry/registry_stream_metadata.hh"

namespace core_abi3_internal {
using namespace arrow_registry_detail;

PyObject *
py_context_to_registry_sink_arrow_source_chunk_provider_registry_state(
    PyObject *, PyObject *args) {
  PyObject *ctx_obj = nullptr;
  const char *sink_name = nullptr;
  PyObject *provider_obj = nullptr;
  PyObject *prepared_obj = Py_None;
  PyObject *registry_state_obj = Py_None;
  const char *schema_mode = nullptr;
  PyObject *first_row_columns = nullptr;
  PyObject *timestamp_columns = nullptr;

  if (!PyArg_ParseTuple(
          args,
          "OsOOOsOO:context_to_registry_sink_arrow_source_chunk_provider_"
          "registry_state",
          &ctx_obj, &sink_name, &provider_obj, &prepared_obj,
          &registry_state_obj, &schema_mode, &first_row_columns,
          &timestamp_columns)) {
    return nullptr;
  }
  if (!sink_name || std::string_view(sink_name) != "stream") {
    PyErr_SetString(
        PyExc_ValueError,
        "Arrow-source chunk provider currently requires sink='stream'");
    return nullptr;
  }
  if (!provider_obj || !PyObject_HasAttrString(provider_obj, "next_sources")) {
    PyErr_SetString(PyExc_TypeError,
                    "Arrow-source chunk provider must expose next_sources()");
    return nullptr;
  }
  auto *ctx = unwrap_context(ctx_obj);
  if (!ctx) {
    return nullptr;
  }
  auto registry_plan = native_registry_state_from_py(registry_state_obj);
  if (!registry_plan) {
    if (!PyErr_Occurred()) {
      PyErr_SetString(PyExc_ValueError,
                      "native registry state does not contain a compiled plan");
    }
    return nullptr;
  }

  auto state = std::make_unique<NativeArrowSourcesStreamState>();
  state->ctx = ctx;
  state->registry_json = registry_plan->registry_json;
  state->schema_mode = schema_mode ? schema_mode : "strict";
  state->registry_plan = registry_plan;
  if (!resolve_prepared_options(prepared_obj, &state->prepared)) {
    return nullptr;
  }
  if (!append_registry_metadata_columns(first_row_columns, timestamp_columns,
                                        &state->first_row_columns,
                                        &state->timestamp_columns)) {
    return nullptr;
  }
  Py_INCREF(provider_obj);
  state->chunk_provider = provider_obj;

  auto *stream = new (std::nothrow) ArrowArrayStream();
  if (!stream) {
    close_arrow_chunk_provider(state.get());
    PyErr_NoMemory();
    return nullptr;
  }
  std::memset(stream, 0, sizeof(*stream));
  stream->get_schema = &arrow_sources_get_schema;
  stream->get_next = &arrow_sources_get_next;
  stream->get_last_error = &arrow_sources_last_error;
  stream->release = &arrow_sources_release;

  PyRegistrySinkOutputs outputs;
  outputs.main_stream = stream;
  outputs.diagnostics = new (std::nothrow) schema_sanitizer_diagnostics();
  if (!outputs.diagnostics) {
    schema_sanitizer_stream_free(stream);
    close_arrow_chunk_provider(state.get());
    PyErr_NoMemory();
    return nullptr;
  }
  outputs.registry_json = dup_cstr(registry_plan->registry_json);
  outputs.drifts_json = dup_cstr(registry_plan->drifts_json);
  outputs.conversion_timestamp = dup_cstr(registry_plan->conversion_timestamp);
  if (!outputs.registry_json || !outputs.drifts_json ||
      !outputs.conversion_timestamp) {
    release_registry_outputs(&outputs);
    close_arrow_chunk_provider(state.get());
    PyErr_NoMemory();
    return nullptr;
  }
  stream->private_data = state.release();
  return pack_registry_stream_result_with_state(
      provider_obj, outputs.main_stream, outputs.diagnostics,
      outputs.registry_json, outputs.drifts_json, outputs.conversion_timestamp,
      std::move(registry_plan));
}

PyObject *py_context_to_registry_sink_arrow_source_chunk_provider_auto_registry(
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

  if (!PyArg_ParseTuple(
          args,
          "OsOOOsssOO:context_to_registry_sink_arrow_source_chunk_provider_"
          "auto_registry",
          &ctx_obj, &sink_name, &probe_provider_obj, &stream_provider_obj,
          &prepared_obj, &registry_json, &field_name_policy, &schema_mode,
          &first_row_columns, &timestamp_columns)) {
    return nullptr;
  }
  if (!sink_name || std::string_view(sink_name) != "stream") {
    PyErr_SetString(
        PyExc_ValueError,
        "Arrow-source chunk provider currently requires sink='stream'");
    return nullptr;
  }
  auto *ctx = unwrap_context(ctx_obj);
  if (!ctx) {
    return nullptr;
  }
  if (!arrow_provider_has_next_sources(probe_provider_obj) ||
      !arrow_provider_has_next_sources(stream_provider_obj)) {
    return nullptr;
  }

  sanitize::PreparedOptionsPtr prepared_options;
  if (!resolve_prepared_options(prepared_obj, &prepared_options)) {
    return nullptr;
  }

  char *err = nullptr;
  const int valid = validate_registry_sink_mode(
      schema_mode, registry_json, &err,
      "context_to_registry_sink_arrow_source_chunk_provider_auto_registry");
  if (valid != SCHEMA_SANITIZER_STATUS_OK) {
    raise_status_error(valid, err);
    return nullptr;
  }

  auto merged_r = merge_arrow_source_provider_schemas(
      ctx, probe_provider_obj, prepared_options, registry_json,
      field_name_policy ? field_name_policy : "");
  if (!merged_r.ok()) {
    raise_status_error(code_for_status(merged_r.status()),
                       dup_cstr(merged_r.status().ToString()));
    return nullptr;
  }
  auto plan_r = make_native_registry_plan(std::move(merged_r).ValueOrDie());
  if (!plan_r.ok()) {
    raise_status_error(code_for_status(plan_r.status()),
                       dup_cstr(plan_r.status().ToString()));
    return nullptr;
  }
  auto registry_plan = std::move(plan_r).ValueOrDie();

  return pack_arrow_source_provider_registry_stream(
      ctx_obj, ctx, stream_provider_obj, prepared_options,
      std::move(registry_plan), field_name_policy, schema_mode,
      first_row_columns, timestamp_columns);
}

PyObject *
py_context_to_registry_sink_arrow_source_chunk_provider_auto_registry_state(
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

  if (!PyArg_ParseTuple(
          args,
          "OsOOOOssOO:context_to_registry_sink_arrow_source_chunk_provider_"
          "auto_registry_state",
          &ctx_obj, &sink_name, &probe_provider_obj, &stream_provider_obj,
          &prepared_obj, &registry_state_obj, &field_name_policy, &schema_mode,
          &first_row_columns, &timestamp_columns)) {
    return nullptr;
  }
  if (!sink_name || std::string_view(sink_name) != "stream") {
    PyErr_SetString(
        PyExc_ValueError,
        "Arrow-source chunk provider currently requires sink='stream'");
    return nullptr;
  }
  auto *ctx = unwrap_context(ctx_obj);
  if (!ctx) {
    return nullptr;
  }
  auto base_registry_plan = native_registry_state_from_py(registry_state_obj);
  if (!base_registry_plan) {
    if (!PyErr_Occurred()) {
      PyErr_SetString(PyExc_ValueError,
                      "native registry state does not contain a compiled plan");
    }
    return nullptr;
  }
  if (!arrow_provider_has_next_sources(probe_provider_obj) ||
      !arrow_provider_has_next_sources(stream_provider_obj)) {
    return nullptr;
  }

  sanitize::PreparedOptionsPtr prepared_options;
  if (!resolve_prepared_options(prepared_obj, &prepared_options)) {
    return nullptr;
  }

  char *err = nullptr;
  const int valid = validate_registry_sink_mode(
      schema_mode, base_registry_plan->registry_json.c_str(), &err,
      "context_to_registry_sink_arrow_source_chunk_provider_auto_registry_"
      "state");
  if (valid != SCHEMA_SANITIZER_STATUS_OK) {
    raise_status_error(valid, err);
    return nullptr;
  }

  auto merged_r = merge_arrow_source_provider_schemas(
      ctx, probe_provider_obj, prepared_options,
      base_registry_plan->registry_json.c_str(),
      field_name_policy ? field_name_policy : "", &base_registry_plan->schema);
  if (!merged_r.ok()) {
    raise_status_error(code_for_status(merged_r.status()),
                       dup_cstr(merged_r.status().ToString()));
    return nullptr;
  }
  auto plan_r = make_native_registry_plan(std::move(merged_r).ValueOrDie());
  if (!plan_r.ok()) {
    raise_status_error(code_for_status(plan_r.status()),
                       dup_cstr(plan_r.status().ToString()));
    return nullptr;
  }
  auto registry_plan = std::move(plan_r).ValueOrDie();

  return pack_arrow_source_provider_registry_stream(
      ctx_obj, ctx, stream_provider_obj, prepared_options,
      std::move(registry_plan), field_name_policy, schema_mode,
      first_row_columns, timestamp_columns);
}

} // namespace core_abi3_internal
