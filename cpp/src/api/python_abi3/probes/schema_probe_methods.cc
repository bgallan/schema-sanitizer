/* Python ABI3 schema-probe method argument parsing. */
#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/abi/python_abi3/methods.hh"

#include <cctype>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "api/c/schema_sanitizer_c_sink_internal.hh"
#include "api/python_abi3/path_sources/path_sources.hh"
#include "api/python_abi3/registry/native_multi_source_stream.hh"
#include "api/python_abi3/registry/plan/plan.hh"
#include "api/python_abi3/registry/registry_stream_metadata.hh"
#include "internal/abi/schema_sanitizer_c_internal.hh"
#include "internal/planning/options_schema_serialization.hh"
#include "sanitize/core/diagnostics.hh"
#include "sanitize/core/logical_schema.hh"
#include "sanitize/core/status.hh"
#include "sanitize/ingest/chunk_source.hh"
#include "sanitize/ingest/ingest.hh"
#include "sanitize/options/options.hh"
#include "sanitize/registry/registry.hh"
#include "sanitize/schema_registry/schema_registry.hh"

#include "api/python_abi3/probes/schema_probe_internal.hh"

namespace core_abi3_internal {
using schema_probe_detail::chunk_source_from_source_py;
using schema_probe_detail::paths_from_py;
using schema_probe_detail::raise_status;
using schema_probe_detail::registry_probe_or_raise;
using schema_probe_detail::registry_probe_path_source_provider_or_raise;
using schema_probe_detail::registry_probe_path_sources_or_raise;
using schema_probe_detail::registry_probe_path_sources_state_or_raise;
using schema_probe_detail::schema_probe_or_raise;

// clang-format off
PyObject *py_context_schema_probe_from_source(PyObject *, PyObject *args) {
  PyObject *ctx_obj = nullptr;
  const char *frontend_name = nullptr;
  const char *source_name = nullptr;
  PyObject *payload_obj = nullptr;
  PyObject *prepared_obj = Py_None;

  if (!PyArg_ParseTuple(args, "OssOO:context_schema_probe_from_source",
                        &ctx_obj, &frontend_name, &source_name, &payload_obj,
                        &prepared_obj)) {
    return nullptr;
  }
  auto *ctx = unwrap_context(ctx_obj);
  if (!ctx)
    return nullptr;
  sanitize::PreparedOptionsPtr prepared;
  if (!resolve_prepared_options(prepared_obj, &prepared)) {
    return nullptr;
  }
  auto src = chunk_source_from_source_py(source_name, payload_obj, prepared);
  if (!src.ok())
    return raise_status(src.status(), "context_schema_probe_from_source");
  return schema_probe_or_raise(ctx, frontend_name, std::move(src).ValueOrDie(),
                               std::move(prepared),
                               "context_schema_probe_from_source");
}

PyObject *py_context_registry_probe_from_source(PyObject *, PyObject *args) {
  PyObject *ctx_obj = nullptr;
  const char *frontend_name = nullptr;
  const char *source_name = nullptr;
  PyObject *payload_obj = nullptr;
  PyObject *prepared_obj = Py_None;
  const char *registry_json = nullptr;
  const char *field_name_policy = nullptr;
  const char *schema_mode = nullptr;

  if (!PyArg_ParseTuple(args, "OssOOsss:context_registry_probe_from_source",
                        &ctx_obj, &frontend_name, &source_name, &payload_obj,
                        &prepared_obj, &registry_json, &field_name_policy,
                        &schema_mode)) {
    return nullptr;
  }
  auto *ctx = unwrap_context(ctx_obj);
  if (!ctx)
    return nullptr;
  sanitize::PreparedOptionsPtr prepared;
  if (!resolve_prepared_options(prepared_obj, &prepared)) {
    return nullptr;
  }
  auto src = chunk_source_from_source_py(source_name, payload_obj, prepared);
  if (!src.ok())
    return raise_status(src.status(), "context_registry_probe_from_source");
  return registry_probe_or_raise(
      ctx, frontend_name, std::move(src).ValueOrDie(), std::move(prepared),
      registry_json, field_name_policy, schema_mode,
      "context_registry_probe_from_source");
}

PyObject *py_context_schema_probe_from_paths(PyObject *, PyObject *args) {
  PyObject *ctx_obj = nullptr;
  const char *frontend_name = nullptr;
  PyObject *paths_obj = nullptr;
  PyObject *prepared_obj = Py_None;
  const char *separator = "\n";

  if (!PyArg_ParseTuple(args, "OsOOs:context_schema_probe_from_paths", &ctx_obj,
                        &frontend_name, &paths_obj, &prepared_obj,
                        &separator)) {
    return nullptr;
  }
  auto *ctx = unwrap_context(ctx_obj);
  if (!ctx)
    return nullptr;
  sanitize::PreparedOptionsPtr prepared;
  if (!resolve_prepared_options(prepared_obj, &prepared)) {
    return nullptr;
  }
  auto paths = paths_from_py(paths_obj);
  if (!paths.ok())
    return raise_status(paths.status(), "context_schema_probe_from_paths");
  auto src = sanitize::chunk_source_from_paths_with_encoding(
      std::move(paths).ValueOrDie(), separator ? separator : "",
      prepared->spec.input_text_encoding);
  if (!src.ok())
    return raise_status(src.status(), "context_schema_probe_from_paths");
  return schema_probe_or_raise(ctx, frontend_name, std::move(src).ValueOrDie(),
                               std::move(prepared),
                               "context_schema_probe_from_paths");
}

PyObject *py_context_registry_probe_from_paths(PyObject *, PyObject *args) {
  PyObject *ctx_obj = nullptr;
  const char *frontend_name = nullptr;
  PyObject *paths_obj = nullptr;
  PyObject *prepared_obj = Py_None;
  const char *separator = "\n";
  const char *registry_json = nullptr;
  const char *field_name_policy = nullptr;
  const char *schema_mode = nullptr;

  if (!PyArg_ParseTuple(args, "OsOOssss:context_registry_probe_from_paths",
                        &ctx_obj, &frontend_name, &paths_obj, &prepared_obj,
                        &separator, &registry_json, &field_name_policy,
                        &schema_mode)) {
    return nullptr;
  }
  auto *ctx = unwrap_context(ctx_obj);
  if (!ctx)
    return nullptr;
  sanitize::PreparedOptionsPtr prepared;
  if (!resolve_prepared_options(prepared_obj, &prepared)) {
    return nullptr;
  }
  auto paths = paths_from_py(paths_obj);
  if (!paths.ok())
    return raise_status(paths.status(), "context_registry_probe_from_paths");
  auto src = sanitize::chunk_source_from_paths_with_encoding(
      std::move(paths).ValueOrDie(), separator ? separator : "",
      prepared->spec.input_text_encoding);
  if (!src.ok())
    return raise_status(src.status(), "context_registry_probe_from_paths");
  return registry_probe_or_raise(
      ctx, frontend_name, std::move(src).ValueOrDie(), std::move(prepared),
      registry_json, field_name_policy, schema_mode,
      "context_registry_probe_from_paths");
}
PyObject *py_context_registry_probe_from_path_sources(PyObject *,
                                                      PyObject *args) {
  PyObject *ctx_obj = nullptr;
  PyObject *sources_obj = nullptr;
  PyObject *prepared_obj = Py_None;
  const char *registry_json = nullptr;
  const char *field_name_policy = nullptr;
  const char *schema_mode = nullptr;

  if (!PyArg_ParseTuple(args, "OOOsss:context_registry_probe_from_path_sources",
                        &ctx_obj, &sources_obj, &prepared_obj, &registry_json,
                        &field_name_policy, &schema_mode)) {
    return nullptr;
  }
  auto *ctx = unwrap_context(ctx_obj);
  if (!ctx)
    return nullptr;
  sanitize::PreparedOptionsPtr prepared;
  if (!resolve_prepared_options(prepared_obj, &prepared)) {
    return nullptr;
  }
  ParsedPathSources parsed_sources;
  if (!parse_path_sources_view(sources_obj, &parsed_sources)) {
    return nullptr;
  }
  return registry_probe_path_sources_or_raise(
      ctx, parsed_sources.get(), std::move(prepared), registry_json,
      field_name_policy, schema_mode,
      "context_registry_probe_from_path_sources");
}

PyObject *
py_context_registry_probe_from_path_sources_registry_state(PyObject *,
                                                           PyObject *args) {
  PyObject *ctx_obj = nullptr;
  PyObject *sources_obj = nullptr;
  PyObject *prepared_obj = Py_None;
  PyObject *registry_state_obj = Py_None;
  const char *field_name_policy = nullptr;
  const char *schema_mode = nullptr;

  if (!PyArg_ParseTuple(
          args,
          "OOOOss:context_registry_probe_from_path_sources_registry_state",
          &ctx_obj, &sources_obj, &prepared_obj, &registry_state_obj,
          &field_name_policy, &schema_mode)) {
    return nullptr;
  }
  auto *ctx = unwrap_context(ctx_obj);
  if (!ctx)
    return nullptr;
  sanitize::PreparedOptionsPtr prepared;
  if (!resolve_prepared_options(prepared_obj, &prepared)) {
    return nullptr;
  }
  auto base_registry_plan = native_registry_state_from_py(registry_state_obj);
  if (!base_registry_plan)
    return nullptr;
  ParsedPathSources parsed_sources;
  if (!parse_path_sources_view(sources_obj, &parsed_sources)) {
    return nullptr;
  }
  return registry_probe_path_sources_state_or_raise(
      ctx, parsed_sources.get(), std::move(prepared),
      std::move(base_registry_plan), field_name_policy, schema_mode,
      "context_registry_probe_from_path_sources_registry_state");
}

PyObject *
py_context_registry_probe_from_path_sources_best_effort(PyObject *,
                                                        PyObject *args) {
  PyObject *ctx_obj = nullptr;
  PyObject *sources_obj = nullptr;
  PyObject *prepared_obj = Py_None;
  const char *registry_json = nullptr;
  const char *field_name_policy = nullptr;
  const char *schema_mode = nullptr;

  if (!PyArg_ParseTuple(
          args, "OOOsss:context_registry_probe_from_path_sources_best_effort",
          &ctx_obj, &sources_obj, &prepared_obj, &registry_json,
          &field_name_policy, &schema_mode)) {
    return nullptr;
  }
  auto *ctx = unwrap_context(ctx_obj);
  if (!ctx)
    return nullptr;
  sanitize::PreparedOptionsPtr prepared;
  if (!resolve_prepared_options(prepared_obj, &prepared)) {
    return nullptr;
  }
  ParsedPathSources parsed_sources;
  if (!parse_path_sources_view(sources_obj, &parsed_sources)) {
    return nullptr;
  }
  return registry_probe_path_sources_or_raise(
      ctx, parsed_sources.get(), std::move(prepared), registry_json,
      field_name_policy, schema_mode,
      "context_registry_probe_from_path_sources_best_effort", true);
}

PyObject *
py_context_registry_probe_from_path_sources_best_effort_registry_state(
    PyObject *, PyObject *args) {
  PyObject *ctx_obj = nullptr;
  PyObject *sources_obj = nullptr;
  PyObject *prepared_obj = Py_None;
  PyObject *registry_state_obj = Py_None;
  const char *field_name_policy = nullptr;
  const char *schema_mode = nullptr;

  if (!PyArg_ParseTuple(
          args,
          "OOOOss:context_registry_probe_from_path_sources_best_effort_"
          "registry_state",
          &ctx_obj, &sources_obj, &prepared_obj, &registry_state_obj,
          &field_name_policy, &schema_mode)) {
    return nullptr;
  }
  auto *ctx = unwrap_context(ctx_obj);
  if (!ctx)
    return nullptr;
  sanitize::PreparedOptionsPtr prepared;
  if (!resolve_prepared_options(prepared_obj, &prepared)) {
    return nullptr;
  }
  auto base_registry_plan = native_registry_state_from_py(registry_state_obj);
  if (!base_registry_plan)
    return nullptr;
  ParsedPathSources parsed_sources;
  if (!parse_path_sources_view(sources_obj, &parsed_sources)) {
    return nullptr;
  }
  return registry_probe_path_sources_state_or_raise(
      ctx, parsed_sources.get(), std::move(prepared),
      std::move(base_registry_plan), field_name_policy, schema_mode,
      "context_registry_probe_from_path_sources_best_effort_registry_state",
      true);
}
PyObject *
py_context_registry_probe_from_path_source_chunk_provider(PyObject *,
                                                          PyObject *args) {
  PyObject *ctx_obj = nullptr;
  PyObject *provider_obj = nullptr;
  PyObject *prepared_obj = Py_None;
  const char *registry_json = nullptr;
  const char *field_name_policy = nullptr;
  const char *schema_mode = nullptr;
  int skip_invalid_json_sources = 0;

  if (!PyArg_ParseTuple(
          args,
          "OOOsss|p:context_registry_probe_from_path_source_chunk_provider",
          &ctx_obj, &provider_obj, &prepared_obj, &registry_json,
          &field_name_policy, &schema_mode, &skip_invalid_json_sources)) {
    return nullptr;
  }
  auto *ctx = unwrap_context(ctx_obj);
  if (!ctx)
    return nullptr;
  sanitize::PreparedOptionsPtr prepared;
  if (!resolve_prepared_options(prepared_obj, &prepared)) {
    return nullptr;
  }

  return registry_probe_path_source_provider_or_raise(
      ctx, provider_obj, std::move(prepared),
      registry_json, field_name_policy, schema_mode,
      "context_registry_probe_from_path_source_chunk_provider",
      skip_invalid_json_sources != 0);
}

PyObject *
py_context_registry_probe_from_path_source_chunk_provider_registry_state(
    PyObject *, PyObject *args) {
  PyObject *ctx_obj = nullptr;
  PyObject *provider_obj = nullptr;
  PyObject *prepared_obj = Py_None;
  PyObject *registry_state_obj = Py_None;
  const char *field_name_policy = nullptr;
  const char *schema_mode = nullptr;
  int skip_invalid_json_sources = 0;

  if (!PyArg_ParseTuple(
          args,
          "OOOOss|p:context_registry_probe_from_path_source_chunk_provider_"
          "registry_state",
          &ctx_obj, &provider_obj, &prepared_obj, &registry_state_obj,
          &field_name_policy, &schema_mode, &skip_invalid_json_sources)) {
    return nullptr;
  }
  auto *ctx = unwrap_context(ctx_obj);
  if (!ctx)
    return nullptr;
  sanitize::PreparedOptionsPtr prepared;
  if (!resolve_prepared_options(prepared_obj, &prepared)) {
    return nullptr;
  }
  auto base_registry_plan = native_registry_state_from_py(registry_state_obj);
  if (!base_registry_plan)
    return nullptr;

  return registry_probe_path_source_provider_or_raise(
      ctx, provider_obj, std::move(prepared), nullptr,
      field_name_policy, schema_mode,
      "context_registry_probe_from_path_source_chunk_provider_registry_state",
      skip_invalid_json_sources != 0, std::move(base_registry_plan));
}
// clang-format on
} // namespace core_abi3_internal
