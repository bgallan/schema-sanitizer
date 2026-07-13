/* Python ABI3 Arrow-source registry probe methods. */
#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/abi/python_abi3/methods.hh"

#include <cstdint>
#include <cstring>
#include <memory>
#include <new>
#include <string>
#include <utility>
#include <vector>

#include "api/python_abi3/registry/arrow_source_sinks_internal.hh"
#include "api/python_abi3/registry/plan/plan.hh"
#include "api/python_abi3/registry/registry_stream_metadata.hh"

namespace core_abi3_internal {
using namespace arrow_registry_detail;

PyObject *py_context_registry_probe_from_arrow_sources(PyObject *,
                                                       PyObject *args) {
  PyObject *ctx_obj = nullptr;
  PyObject *sources_obj = nullptr;
  PyObject *prepared_obj = Py_None;
  const char *registry_json = nullptr;
  const char *field_name_policy = nullptr;
  const char *schema_mode = nullptr;

  if (!PyArg_ParseTuple(args,
                        "OOOsss:context_registry_probe_from_arrow_sources",
                        &ctx_obj, &sources_obj, &prepared_obj, &registry_json,
                        &field_name_policy, &schema_mode)) {
    return nullptr;
  }
  auto *ctx = unwrap_context(ctx_obj);
  if (!ctx) {
    return nullptr;
  }
  sanitize::PreparedOptionsPtr prepared;
  if (!resolve_prepared_options(prepared_obj, &prepared)) {
    return nullptr;
  }

  std::vector<ArrowSourceSpec> sources;
  if (!parse_arrow_sources(sources_obj, &sources)) {
    return nullptr;
  }

  char *err = nullptr;
  const int valid =
      validate_registry_sink_mode(schema_mode, registry_json, &err,
                                  "context_registry_probe_from_arrow_sources");
  if (valid != SCHEMA_SANITIZER_STATUS_OK) {
    decref_arrow_sources(&sources);
    raise_status_error(valid, err);
    return nullptr;
  }

  auto merged_r = merge_arrow_source_schemas(
      ctx, sources, prepared, registry_json ? registry_json : "{}",
      field_name_policy ? field_name_policy : "");
  decref_arrow_sources(&sources);
  if (!merged_r.ok()) {
    raise_status_error(code_for_status(merged_r.status()),
                       dup_cstr(merged_r.status().ToString()));
    return nullptr;
  }
  auto merged = std::move(merged_r).ValueOrDie();
  sanitize::IngestDiagnostics diagnostics;
  diagnostics.arrow_schema_depth = sanitize::arrow_schema_depth(merged.schema);
  diagnostics.parquet_schema_depth =
      sanitize::parquet_schema_depth(merged.schema);
  diagnostics.direct_arrow_input = 1;
  return pack_registry_probe(merged, diagnostics);
}

PyObject *
py_context_registry_probe_from_arrow_sources_registry_state(PyObject *,
                                                            PyObject *args) {
  PyObject *ctx_obj = nullptr;
  PyObject *sources_obj = nullptr;
  PyObject *prepared_obj = Py_None;
  PyObject *registry_state_obj = Py_None;
  const char *field_name_policy = nullptr;
  const char *schema_mode = nullptr;

  if (!PyArg_ParseTuple(
          args,
          "OOOOss:context_registry_probe_from_arrow_sources_registry_state",
          &ctx_obj, &sources_obj, &prepared_obj, &registry_state_obj,
          &field_name_policy, &schema_mode)) {
    return nullptr;
  }
  auto *ctx = unwrap_context(ctx_obj);
  if (!ctx) {
    return nullptr;
  }
  sanitize::PreparedOptionsPtr prepared;
  if (!resolve_prepared_options(prepared_obj, &prepared)) {
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

  std::vector<ArrowSourceSpec> sources;
  if (!parse_arrow_sources(sources_obj, &sources)) {
    return nullptr;
  }

  char *err = nullptr;
  const int valid = validate_registry_sink_mode(
      schema_mode, base_registry_plan->registry_json.c_str(), &err,
      "context_registry_probe_from_arrow_sources_registry_state");
  if (valid != SCHEMA_SANITIZER_STATUS_OK) {
    decref_arrow_sources(&sources);
    raise_status_error(valid, err);
    return nullptr;
  }

  auto merged_r = merge_arrow_source_schemas(
      ctx, sources, prepared, base_registry_plan->registry_json.c_str(),
      field_name_policy ? field_name_policy : "", &base_registry_plan->schema);
  decref_arrow_sources(&sources);
  if (!merged_r.ok()) {
    raise_status_error(code_for_status(merged_r.status()),
                       dup_cstr(merged_r.status().ToString()));
    return nullptr;
  }
  auto merged = std::move(merged_r).ValueOrDie();
  sanitize::IngestDiagnostics diagnostics;
  diagnostics.arrow_schema_depth = sanitize::arrow_schema_depth(merged.schema);
  diagnostics.parquet_schema_depth =
      sanitize::parquet_schema_depth(merged.schema);
  diagnostics.direct_arrow_input = 1;
  return pack_registry_probe(merged, diagnostics);
}

} // namespace core_abi3_internal
