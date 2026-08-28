/* Python ABI3 explicit-registry path-source sink methods. */
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

PyObject *py_context_to_registry_sink_from_path_sources(PyObject *,
                                                        PyObject *args) {
  PyObject *ctx_obj = nullptr;
  const char *sink_name = nullptr;
  PyObject *sources_obj = nullptr;
  PyObject *prepared_obj = Py_None;
  const char *registry_json = nullptr;
  const char *drifts_json = nullptr;
  const char *conversion_timestamp = nullptr;
  const char *field_name_policy = nullptr;
  const char *schema_mode = nullptr;
  PyObject *first_row_columns = nullptr;
  PyObject *timestamp_columns = nullptr;

  if (!PyArg_ParseTuple(
          args, "OsOOsssssOO:context_to_registry_sink_from_path_sources",
          &ctx_obj, &sink_name, &sources_obj, &prepared_obj, &registry_json,
          &drifts_json, &conversion_timestamp, &field_name_policy, &schema_mode,
          &first_row_columns, &timestamp_columns)) {
    return nullptr;
  }
  auto *ctx = unwrap_context(ctx_obj);
  if (!ctx)
    return nullptr;

  auto state = std::make_unique<NativePathSourcesStreamState>();
  state->ctx = ctx;
  state->sink_name = sink_name ? sink_name : "stream";
  state->registry_json = registry_json ? registry_json : "{}";
  state->drifts_json = drifts_json ? drifts_json : "[]";
  state->conversion_timestamp =
      conversion_timestamp ? conversion_timestamp : "";
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
  auto plan_r = make_native_registry_plan_from_json(
      state->prepared, state->registry_json.c_str(), state->drifts_json.c_str(),
      state->conversion_timestamp.c_str());
  if (!plan_r.ok()) {
    PyErr_SetString(PyExc_ValueError, plan_r.status().ToString().c_str());
    return nullptr;
  }
  state->registry_plan = std::move(plan_r).ValueOrDie();

  return pack_path_source_registry_stream(ctx_obj, std::move(state));
}

PyObject *
py_context_to_registry_sink_from_path_sources_registry_state(PyObject *,
                                                             PyObject *args) {
  PyObject *ctx_obj = nullptr;
  const char *sink_name = nullptr;
  PyObject *sources_obj = nullptr;
  PyObject *prepared_obj = Py_None;
  PyObject *registry_state_obj = Py_None;
  const char *schema_mode = nullptr;
  PyObject *first_row_columns = nullptr;
  PyObject *timestamp_columns = nullptr;

  if (!PyArg_ParseTuple(
          args,
          "OsOOOsOO:context_to_registry_sink_from_path_sources_registry_state",
          &ctx_obj, &sink_name, &sources_obj, &prepared_obj,
          &registry_state_obj, &schema_mode, &first_row_columns,
          &timestamp_columns)) {
    return nullptr;
  }
  auto *ctx = unwrap_context(ctx_obj);
  if (!ctx)
    return nullptr;
  auto registry_plan = native_registry_state_from_py(registry_state_obj);
  if (!registry_plan) {
    if (!PyErr_Occurred()) {
      PyErr_SetString(PyExc_ValueError,
                      "native registry state does not contain a compiled plan");
    }
    return nullptr;
  }

  auto state = std::make_unique<NativePathSourcesStreamState>();
  state->ctx = ctx;
  state->sink_name = sink_name ? sink_name : "stream";
  state->registry_json = registry_plan->registry_json;
  state->drifts_json = registry_plan->drifts_json;
  state->conversion_timestamp = registry_plan->conversion_timestamp;
  state->schema_mode = schema_mode ? schema_mode : "strict";
  state->registry_plan = registry_plan;
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

  return pack_path_source_registry_stream(ctx_obj, std::move(state));
}

PyObject *
py_context_to_registry_sink_from_path_source_chunk_provider_registry_state(
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
          "OsOOOsOO:context_to_registry_sink_from_path_source_chunk_provider_"
          "registry_state",
          &ctx_obj, &sink_name, &provider_obj, &prepared_obj,
          &registry_state_obj, &schema_mode, &first_row_columns,
          &timestamp_columns)) {
    return nullptr;
  }
  auto *ctx = unwrap_context(ctx_obj);
  if (!ctx)
    return nullptr;
  auto registry_plan = native_registry_state_from_py(registry_state_obj);
  if (!registry_plan) {
    if (!PyErr_Occurred()) {
      PyErr_SetString(PyExc_ValueError,
                      "native registry state does not contain a compiled plan");
    }
    return nullptr;
  }
  if (!PyObject_HasAttrString(provider_obj, "next_sources")) {
    PyErr_SetString(PyExc_TypeError,
                    "path-source chunk provider must expose next_sources()");
    return nullptr;
  }

  auto state = std::make_unique<NativePathSourcesStreamState>();
  state->ctx = ctx;
  state->sink_name = sink_name ? sink_name : "stream";
  state->registry_json = registry_plan->registry_json;
  state->drifts_json = registry_plan->drifts_json;
  state->conversion_timestamp = registry_plan->conversion_timestamp;
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

  return pack_path_source_registry_stream(ctx_obj, std::move(state),
                                          provider_obj);
}

} // namespace core_abi3_internal
