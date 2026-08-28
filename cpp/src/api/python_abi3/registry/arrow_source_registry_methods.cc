/* Python ABI3 direct Arrow-source registry sink methods. */
#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/abi/python_abi3/methods.hh"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "api/python_abi3/registry/arrow_source_sinks_internal.hh"
#include "api/python_abi3/registry/plan/plan.hh"
#include "api/python_abi3/registry/registry_stream_metadata.hh"

namespace core_abi3_internal {
using namespace arrow_registry_detail;

PyObject *py_context_to_registry_sink_arrow_sources(PyObject *,
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

  if (!PyArg_ParseTuple(args,
                        "OsOOsssOO:context_to_registry_sink_arrow_sources",
                        &ctx_obj, &sink_name, &sources_obj, &prepared_obj,
                        &registry_json, &field_name_policy, &schema_mode,
                        &first_row_columns, &timestamp_columns)) {
    return nullptr;
  }
  if (!sink_name || std::string_view(sink_name) != "stream") {
    PyErr_SetString(
        PyExc_ValueError,
        "Arrow sources registry sink currently requires sink='stream'");
    return nullptr;
  }
  auto *ctx = unwrap_context(ctx_obj);
  if (!ctx) {
    return nullptr;
  }

  auto state = std::make_unique<NativeArrowSourcesStreamState>();
  state->ctx = ctx;
  state->registry_json = registry_json ? registry_json : "{}";
  state->field_name_policy = field_name_policy ? field_name_policy : "";
  state->schema_mode = schema_mode ? schema_mode : "additive";
  if (!resolve_prepared_options(prepared_obj, &state->prepared)) {
    return nullptr;
  }
  if (!parse_arrow_sources(sources_obj, &state->sources)) {
    return nullptr;
  }
  if (!append_registry_metadata_columns(first_row_columns, timestamp_columns,
                                        &state->first_row_columns,
                                        &state->timestamp_columns)) {
    return nullptr;
  }

  const auto valid = validate_registry_sink_mode(
      schema_mode, registry_json, "context_to_registry_sink_arrow_sources");
  if (!valid.ok()) {
    raise_status_error(valid);
    return nullptr;
  }

  auto merged_r = merge_arrow_source_schemas(
      ctx, state->sources, state->prepared, state->registry_json.c_str(),
      state->field_name_policy.c_str());
  if (!merged_r.ok()) {
    raise_status_error(merged_r.status());
    return nullptr;
  }
  auto merged = std::move(merged_r).ValueOrDie();
  auto plan_r = make_native_registry_plan(std::move(merged));
  if (!plan_r.ok()) {
    raise_status_error(plan_r.status());
    return nullptr;
  }
  state->registry_plan = std::move(plan_r).ValueOrDie();
  state->registry_json = state->registry_plan->registry_json;
  append_registry_first_row_columns(&state->first_row_columns,
                                    state->registry_json,
                                    state->registry_plan->drifts_json);

  return pack_arrow_source_registry_stream(sources_obj, std::move(state));
}

PyObject *
py_context_to_registry_sink_arrow_sources_auto_registry(PyObject *,
                                                        PyObject *args) {
  return py_context_to_registry_sink_arrow_sources(nullptr, args);
}

PyObject *
py_context_to_registry_sink_arrow_sources_registry_state(PyObject *,
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
          "OsOOOsOO:context_to_registry_sink_arrow_sources_registry_state",
          &ctx_obj, &sink_name, &sources_obj, &prepared_obj,
          &registry_state_obj, &schema_mode, &first_row_columns,
          &timestamp_columns)) {
    return nullptr;
  }
  if (!sink_name || std::string_view(sink_name) != "stream") {
    PyErr_SetString(
        PyExc_ValueError,
        "Arrow sources registry sink currently requires sink='stream'");
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
  if (!parse_arrow_sources(sources_obj, &state->sources)) {
    return nullptr;
  }
  if (!append_registry_metadata_columns(first_row_columns, timestamp_columns,
                                        &state->first_row_columns,
                                        &state->timestamp_columns)) {
    return nullptr;
  }

  return pack_arrow_source_registry_stream(ctx_obj, std::move(state));
}

PyObject *
py_context_to_registry_sink_arrow_sources_auto_registry_state(PyObject *,
                                                              PyObject *args) {
  PyObject *ctx_obj = nullptr;
  const char *sink_name = nullptr;
  PyObject *sources_obj = nullptr;
  PyObject *prepared_obj = Py_None;
  PyObject *registry_state_obj = Py_None;
  const char *field_name_policy = nullptr;
  const char *schema_mode = nullptr;
  PyObject *first_row_columns = nullptr;
  PyObject *timestamp_columns = nullptr;

  if (!PyArg_ParseTuple(args,
                        "OsOOOssOO:context_to_registry_sink_arrow_sources_auto_"
                        "registry_state",
                        &ctx_obj, &sink_name, &sources_obj, &prepared_obj,
                        &registry_state_obj, &field_name_policy, &schema_mode,
                        &first_row_columns, &timestamp_columns)) {
    return nullptr;
  }
  if (!sink_name || std::string_view(sink_name) != "stream") {
    PyErr_SetString(
        PyExc_ValueError,
        "Arrow sources registry sink currently requires sink='stream'");
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

  auto state = std::make_unique<NativeArrowSourcesStreamState>();
  state->ctx = ctx;
  state->registry_json = base_registry_plan->registry_json;
  state->field_name_policy = field_name_policy ? field_name_policy : "";
  state->schema_mode = schema_mode ? schema_mode : "additive";
  if (!resolve_prepared_options(prepared_obj, &state->prepared)) {
    return nullptr;
  }
  if (!parse_arrow_sources(sources_obj, &state->sources)) {
    return nullptr;
  }
  if (!append_registry_metadata_columns(first_row_columns, timestamp_columns,
                                        &state->first_row_columns,
                                        &state->timestamp_columns)) {
    return nullptr;
  }

  const auto valid = validate_registry_sink_mode(
      state->schema_mode, state->registry_json,
      "context_to_registry_sink_arrow_sources_auto_registry_state");
  if (!valid.ok()) {
    raise_status_error(valid);
    return nullptr;
  }

  auto merged_r = merge_arrow_source_schemas(
      ctx, state->sources, state->prepared, state->registry_json.c_str(),
      state->field_name_policy.c_str(), &base_registry_plan->schema);
  if (!merged_r.ok()) {
    raise_status_error(merged_r.status());
    return nullptr;
  }
  auto merged = std::move(merged_r).ValueOrDie();
  auto plan_r = make_native_registry_plan(std::move(merged));
  if (!plan_r.ok()) {
    raise_status_error(plan_r.status());
    return nullptr;
  }
  state->registry_plan = std::move(plan_r).ValueOrDie();
  state->registry_json = state->registry_plan->registry_json;
  append_registry_first_row_columns(&state->first_row_columns,
                                    state->registry_json,
                                    state->registry_plan->drifts_json);

  return pack_arrow_source_registry_stream(ctx_obj, std::move(state));
}

} // namespace core_abi3_internal
