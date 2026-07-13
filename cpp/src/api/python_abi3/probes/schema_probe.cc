/*
 * Python ABI3 schema-probe wrappers.
 *
 * These entry points run native frontend inference without materializing a
 * sink. They are used by pipeline warm-up/bootstrap paths that only need schema
 * or registry state.
 */
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

namespace core_abi3_internal::schema_probe_detail {
// clang-format off

bool dict_set_steal(PyObject *dict, const char *key, PyObject *value) {
  if (!value)
    return false;
  const int rc = PyDict_SetItemString(dict, key, value);
  Py_DECREF(value);
  return rc == 0;
}

PyObject *pack_schema_probe(const sanitize::LogicalSchema &schema,
                            const sanitize::IngestDiagnostics &diagnostics) {
  const std::string schema_payload =
      sanitize::internal::options_io::serialize_logical_schema_bytes(schema);
  PyObject *dict = PyDict_New();
  if (!dict)
    return nullptr;
  if (!dict_set_steal(dict, "schema",
                      PyBytes_FromStringAndSize(
                          schema_payload.data(),
                          static_cast<Py_ssize_t>(schema_payload.size())))) {
    Py_DECREF(dict);
    return nullptr;
  }
  if (!dict_set_steal(dict, "diagnostics_json",
                      PyUnicode_FromString(diagnostics.to_json().c_str()))) {
    Py_DECREF(dict);
    return nullptr;
  }
  return dict;
}

PyObject *pack_registry_probe(const sanitize::SchemaRegistryMergeResult &merged,
                              const sanitize::IngestDiagnostics &diagnostics) {
  const std::string schema_payload =
      sanitize::internal::options_io::serialize_logical_schema_bytes(
          merged.schema);
  PyObject *dict = PyDict_New();
  if (!dict)
    return nullptr;
  if (!dict_set_steal(dict, "schema",
                      PyBytes_FromStringAndSize(
                          schema_payload.data(),
                          static_cast<Py_ssize_t>(schema_payload.size())))) {
    Py_DECREF(dict);
    return nullptr;
  }
  if (!dict_set_steal(dict, "schema_registry_json",
                      PyUnicode_FromString(merged.registry_json.c_str())) ||
      !dict_set_steal(dict, "schema_drifts_json",
                      PyUnicode_FromString(merged.drifts_json.c_str())) ||
      !dict_set_steal(dict, "conversion_timestamp",
                      PyUnicode_FromString(merged.detected_at.c_str())) ||
      !dict_set_steal(dict, "diagnostics_json",
                      PyUnicode_FromString(diagnostics.to_json().c_str()))) {
    Py_DECREF(dict);
    return nullptr;
  }
  sanitize::SchemaRegistryMergeResult state_merged;
  state_merged.schema = merged.schema;
  state_merged.registry_json = merged.registry_json;
  state_merged.drifts_json = merged.drifts_json;
  state_merged.detected_at = merged.detected_at;
  auto state_plan = make_native_registry_plan(std::move(state_merged));
  if (state_plan.ok()) {
    if (!dict_set_steal(
            dict, "native_registry_state",
            wrap_native_registry_state(std::move(state_plan).ValueOrDie()))) {
      Py_DECREF(dict);
      return nullptr;
    }
  }
  return dict;
}

sanitize::Result<std::vector<std::string>> paths_from_py(PyObject *paths_obj) {
  if (!PySequence_Check(paths_obj)) {
    return sanitize::Status::Invalid(
        "paths must be a sequence of path-like objects");
  }
  const Py_ssize_t size = PySequence_Size(paths_obj);
  if (size < 0) {
    return sanitize::Status::Invalid("could not read paths sequence");
  }
  std::vector<std::string> paths;
  paths.reserve(static_cast<std::size_t>(size));
  for (Py_ssize_t i = 0; i < size; ++i) {
    bool borrowed = false;
    PyObject *item = sequence_item_borrowed_or_new(paths_obj, i, &borrowed);
    if (!item) {
      return sanitize::Status::Invalid("could not read paths sequence item");
    }
    PyObject *encoded = fsencode_path(item);
    if (!borrowed)
      Py_DECREF(item);
    if (!encoded) {
      return sanitize::Status::Invalid("paths must contain path-like objects");
    }
    const char *path = PyBytes_AsString(encoded);
    const Py_ssize_t path_size = PyBytes_Size(encoded);
    if (!path || path_size < 0) {
      Py_DECREF(encoded);
      return sanitize::Status::Invalid("could not encode path");
    }
    paths.emplace_back(path, static_cast<std::size_t>(path_size));
    Py_DECREF(encoded);
  }
  return paths;
}

sanitize::Result<sanitize::PreparedIngest>
prepare_probe(schema_sanitizer_context *ctx, const char *frontend_name,
              sanitize::ChunkSourcePtr src,
              sanitize::PreparedOptionsPtr prepared_options);

PyObject *raise_status(const sanitize::Status &status, const char *where);

std::string_view trim_ascii_ws(std::string_view value) {
  while (!value.empty() &&
         std::isspace(static_cast<unsigned char>(value.front())) != 0) {
    value.remove_prefix(1);
  }
  while (!value.empty() &&
         std::isspace(static_cast<unsigned char>(value.back())) != 0) {
    value.remove_suffix(1);
  }
  return value;
}

void append_json_array_items(std::string &items, std::string_view array_json) {
  array_json = trim_ascii_ws(array_json);
  if (array_json.size() < 2 || array_json.front() != '[' ||
      array_json.back() != ']') {
    return;
  }
  array_json.remove_prefix(1);
  array_json.remove_suffix(1);
  array_json = trim_ascii_ws(array_json);
  if (array_json.empty()) {
    return;
  }
  if (!items.empty()) {
    items.push_back(',');
  }
  items.append(array_json.data(), array_json.size());
}

std::string json_array_from_items(const std::string &items) {
  if (items.empty()) {
    return "[]";
  }
  std::string out;
  out.reserve(items.size() + 2);
  out.push_back('[');
  out.append(items);
  out.push_back(']');
  return out;
}

struct ProviderCloseGuard {
  PyObject *provider = nullptr;

  explicit ProviderCloseGuard(PyObject *obj) : provider(obj) {
    Py_XINCREF(provider);
  }

  ProviderCloseGuard(const ProviderCloseGuard &) = delete;
  ProviderCloseGuard &operator=(const ProviderCloseGuard &) = delete;

  ~ProviderCloseGuard() {
    if (!provider) {
      return;
    }
    PyObject *type = nullptr;
    PyObject *value = nullptr;
    PyObject *traceback = nullptr;
    PyErr_Fetch(&type, &value, &traceback);
    PyObject *result = PyObject_CallMethod(provider, "close", nullptr);
    if (!result) {
      PyErr_Clear();
    } else {
      Py_DECREF(result);
    }
    Py_DECREF(provider);
    PyErr_Restore(type, value, traceback);
  }
};

PyObject *registry_probe_path_sources_or_raise(
    schema_sanitizer_context *ctx, const std::vector<PathSourceSpec> &sources,
    sanitize::PreparedOptionsPtr prepared_options, const char *registry_json,
    const char *field_name_policy, const char *schema_mode, const char *where,
    bool skip_invalid_json_sources) {
  if (!ctx) {
    PyErr_SetString(PyExc_RuntimeError, "context is null");
    return nullptr;
  }
  char *err = nullptr;
  const int valid =
      validate_registry_sink_mode(schema_mode, registry_json, &err, where);
  if (valid != SCHEMA_SANITIZER_STATUS_OK) {
    raise_status_error(valid, err);
    return nullptr;
  }
  auto probe =
      merge_path_source_schemas(ctx, sources, prepared_options, registry_json,
                                field_name_policy, skip_invalid_json_sources);
  if (!probe.ok()) {
    return raise_status(probe.status(), where);
  }
  auto value = std::move(probe).ValueOrDie();
  return pack_registry_probe(std::move(value.merged), value.diagnostics);
}

PyObject *registry_probe_path_sources_state_or_raise(
    schema_sanitizer_context *ctx, const std::vector<PathSourceSpec> &sources,
    sanitize::PreparedOptionsPtr prepared_options,
    std::shared_ptr<const NativeRegistryPlan> base_registry_plan,
    const char *field_name_policy, const char *schema_mode, const char *where,
    bool skip_invalid_json_sources) {
  if (!ctx) {
    PyErr_SetString(PyExc_RuntimeError, "context is null");
    return nullptr;
  }
  if (!base_registry_plan) {
    PyErr_SetString(PyExc_ValueError,
                    "native registry state does not contain a compiled plan");
    return nullptr;
  }
  char *err = nullptr;
  const int valid = validate_registry_sink_mode(
      schema_mode, base_registry_plan->registry_json.c_str(), &err, where);
  if (valid != SCHEMA_SANITIZER_STATUS_OK) {
    raise_status_error(valid, err);
    return nullptr;
  }
  auto probe = merge_path_source_schemas(
      ctx, sources, prepared_options, base_registry_plan->registry_json.c_str(),
      field_name_policy, skip_invalid_json_sources,
      &base_registry_plan->schema);
  if (!probe.ok()) {
    return raise_status(probe.status(), where);
  }
  auto value = std::move(probe).ValueOrDie();
  return pack_registry_probe(std::move(value.merged), value.diagnostics);
}

PyObject *registry_probe_path_source_provider_or_raise(
    schema_sanitizer_context *ctx, PyObject *provider,
    sanitize::PreparedOptionsPtr prepared_options, const char *registry_json,
    const char *field_name_policy, const char *schema_mode, const char *where,
    bool skip_invalid_json_sources,
    std::shared_ptr<const NativeRegistryPlan> base_registry_plan) {
  if (!ctx) {
    PyErr_SetString(PyExc_RuntimeError, "context is null");
    return nullptr;
  }
  if (!provider || !PyObject_HasAttrString(provider, "next_sources")) {
    PyErr_SetString(PyExc_TypeError,
                    "path-source chunk provider must expose next_sources()");
    return nullptr;
  }
  const char *base_registry_json =
      base_registry_plan ? base_registry_plan->registry_json.c_str()
                         : registry_json;
  char *err = nullptr;
  const int valid =
      validate_registry_sink_mode(schema_mode, base_registry_json, &err, where);
  if (valid != SCHEMA_SANITIZER_STATUS_OK) {
    raise_status_error(valid, err);
    return nullptr;
  }

  ProviderCloseGuard close_provider(provider);
  std::string current_registry = base_registry_json ? base_registry_json : "{}";
  std::optional<sanitize::LogicalSchema> current_schema;
  if (base_registry_plan) {
    current_schema = base_registry_plan->schema;
  }
  std::string conversion_timestamp;
  std::string drift_items;
  sanitize::IngestDiagnostics diagnostics;
  bool saw_chunk = false;

  for (;;) {
    if (!check_python_signals()) {
      return raise_status(sanitize::Status::Cancelled("Python signal received"),
                          where);
    }
    PyObject *chunk_sources =
        PyObject_CallMethod(provider, "next_sources", nullptr);
    if (!chunk_sources) {
      return nullptr;
    }
    if (chunk_sources == Py_None) {
      Py_DECREF(chunk_sources);
      break;
    }
    ParsedPathSources parsed_sources;
    if (!parse_path_sources_view(chunk_sources, &parsed_sources)) {
      Py_DECREF(chunk_sources);
      return nullptr;
    }
    auto probe = merge_path_source_schemas(
        ctx, parsed_sources.get(), prepared_options, current_registry.c_str(),
        field_name_policy, skip_invalid_json_sources,
        current_schema ? &*current_schema : nullptr);
    Py_DECREF(chunk_sources);
    if (!probe.ok()) {
      return raise_status(probe.status(), where);
    }
    auto value = std::move(probe).ValueOrDie();
    merge_path_source_diagnostics(diagnostics, value.diagnostics);
    append_json_array_items(drift_items, value.merged.drifts_json);
    current_registry = std::move(value.merged.registry_json);
    conversion_timestamp = std::move(value.merged.detected_at);
    current_schema = std::move(value.merged.schema);
    saw_chunk = true;
  }

  if (!saw_chunk || !current_schema) {
    return raise_status(sanitize::Status::Invalid("sources must not be empty"),
                        where);
  }

  sanitize::SchemaRegistryMergeResult merged;
  merged.schema = std::move(*current_schema);
  merged.registry_json = std::move(current_registry);
  merged.drifts_json = json_array_from_items(drift_items);
  merged.detected_at = std::move(conversion_timestamp);
  return pack_registry_probe(merged, diagnostics);
}
sanitize::Result<sanitize::ChunkSourcePtr>
chunk_source_from_source_py(const char *source_name, PyObject *payload_obj,
                            const sanitize::PreparedOptionsPtr &prepared) {
  switch (parse_python_source_kind(source_name)) {
  case PythonSourceKind::kPath: {
    PyObject *path_bytes = fsencode_path(payload_obj);
    if (!path_bytes) {
      return sanitize::Status::Invalid("invalid path input");
    }
    const char *path = PyBytes_AsString(path_bytes);
    const Py_ssize_t path_size = PyBytes_Size(path_bytes);
    if (!path || path_size < 0) {
      Py_DECREF(path_bytes);
      return sanitize::Status::Invalid("invalid path input");
    }
    auto src = sanitize::chunk_source_from_path_with_encoding(
        std::string(path, static_cast<std::size_t>(path_size)),
        prepared->spec.input_text_encoding);
    Py_DECREF(path_bytes);
    return src;
  }
  case PythonSourceKind::kStream:
    if (!python_reader_has_read_seek(payload_obj)) {
      return sanitize::Status::Invalid(
          "reader input must expose read(max_bytes) and seek(0)");
    }
    return make_python_reader_chunk_source(payload_obj);
  case PythonSourceKind::kText: {
    const char *data = nullptr;
    Py_ssize_t data_len = 0;
    if (!bytes_or_str_view(payload_obj, &data, &data_len)) {
      return sanitize::Status::Invalid("text source must be str or bytes");
    }
    return sanitize::chunk_source_from_bytes(
        std::string(data, static_cast<std::size_t>(data_len)));
  }
  case PythonSourceKind::kUnknown:
    break;
  }
  return sanitize::Status::Invalid(
      "source must be 'path', 'stream', or 'text'");
}

sanitize::Result<sanitize::PreparedIngest>
prepare_probe(schema_sanitizer_context *ctx, const char *frontend_name,
              sanitize::ChunkSourcePtr src,
              sanitize::PreparedOptionsPtr prepared_options) {
  auto fe = sanitize::make_builtin_frontend(frontend_name, std::move(src),
                                            prepared_options->spec);
  return sanitize::prepare_ingest(frontend_name, std::move(fe),
                                  std::move(prepared_options), ctx->ctx.get());
}

PyObject *raise_status(const sanitize::Status &status, const char *where) {
  PyErr_SetString(PyExc_RuntimeError,
                  (std::string(where) + ": " + status.ToString()).c_str());
  return nullptr;
}

PyObject *schema_probe_or_raise(schema_sanitizer_context *ctx,
                                const char *frontend_name,
                                sanitize::ChunkSourcePtr src,
                                sanitize::PreparedOptionsPtr prepared_options,
                                const char *where) {
  if (!ctx) {
    PyErr_SetString(PyExc_RuntimeError, "context is null");
    return nullptr;
  }
  auto prepared = prepare_probe(ctx, frontend_name, std::move(src),
                                std::move(prepared_options));
  if (!prepared.ok())
    return raise_status(prepared.status(), where);
  sanitize::PreparedIngest ingest = std::move(prepared).ValueOrDie();
  return pack_schema_probe(ingest.logical_schema, *ingest.diagnostics);
}

PyObject *registry_probe_or_raise(schema_sanitizer_context *ctx,
                                  const char *frontend_name,
                                  sanitize::ChunkSourcePtr src,
                                  sanitize::PreparedOptionsPtr prepared_options,
                                  const char *registry_json,
                                  const char *field_name_policy,
                                  const char *schema_mode, const char *where) {
  if (!ctx) {
    PyErr_SetString(PyExc_RuntimeError, "context is null");
    return nullptr;
  }
  char *err = nullptr;
  const int valid =
      validate_registry_sink_mode(schema_mode, registry_json, &err, where);
  if (valid != SCHEMA_SANITIZER_STATUS_OK) {
    raise_status_error(valid, err);
    return nullptr;
  }
  auto prepared = prepare_probe(ctx, frontend_name, std::move(src),
                                std::move(prepared_options));
  if (!prepared.ok())
    return raise_status(prepared.status(), where);
  sanitize::PreparedIngest ingest = std::move(prepared).ValueOrDie();
  auto merged = sanitize::merge_schema_registry(make_registry_merge_input(
      std::move(ingest.logical_schema), registry_json, field_name_policy,
      ingest.opts->spec.default_key_name, ingest.opts->spec.field_order));
  if (!merged.ok())
    return raise_status(merged.status(), where);
  return pack_registry_probe(std::move(merged).ValueOrDie(),
                             *ingest.diagnostics);
}
// clang-format on
} // namespace core_abi3_internal::schema_probe_detail
