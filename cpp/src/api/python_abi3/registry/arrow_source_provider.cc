/* Arrow-source registry provider parsing and schema merging. */
#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/abi/python_abi3/methods.hh"

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <memory>
#include <new>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "api/c/schema_sanitizer_c_sink_internal.hh"
#include "api/python_abi3/arrow_direct/_core_abi3_arrow_direct.hh"
#include "api/python_abi3/arrow_direct/schema/logical.hh"
#include "api/python_abi3/arrow_stream/_core_abi3_arrow_stream_lifecycle.hh"
#include "api/python_abi3/metadata/columns/api.hh"
#include "api/python_abi3/metadata/stream/stream.hh"
#include "api/python_abi3/registry/native_multi_source_stream.hh"
#include "api/python_abi3/registry/plan/plan.hh"
#include "api/python_abi3/registry/registry_stream_metadata.hh"
#include "internal/abi/schema_sanitizer_c_internal.hh"
#include "internal/arrow_c/cdata_schema_builder.hh"
#include "internal/arrow_c/cdata_stream_callbacks.hh"
#include "internal/planning/options_schema_serialization.hh"
#include "sanitize/registry/registry.hh"

#include "api/python_abi3/registry/arrow_source_sinks_internal.hh"

namespace core_abi3_internal::arrow_registry_detail {

bool py_unicode_to_string(PyObject *obj, const char *name, std::string *out) {
  if (!PyUnicode_Check(obj)) {
    PyErr_Format(PyExc_TypeError, "%s must be strings", name);
    return false;
  }
  Py_ssize_t size = 0;
  const char *data = PyUnicode_AsUTF8AndSize(obj, &size);
  if (!data) {
    return false;
  }
  out->assign(data, static_cast<std::size_t>(size));
  return true;
}

void decref_arrow_sources(std::vector<ArrowSourceSpec> *sources) noexcept {
  if (!sources) {
    return;
  }
  for (auto &source : *sources) {
    decref_with_gil(source.stream_obj);
    source.stream_obj = nullptr;
  }
  sources->clear();
}
sanitize::Status python_arrow_provider_error_status(const char *where) {
  PyObject *type = nullptr;
  PyObject *value = nullptr;
  PyObject *traceback = nullptr;
  PyErr_Fetch(&type, &value, &traceback);

  std::string msg(where ? where : "Arrow-source chunk provider");
  msg += ": ";
  if (value) {
    PyObject *text = PyObject_Str(value);
    if (text) {
      Py_ssize_t n = 0;
      const char *s = PyUnicode_AsUTF8AndSize(text, &n);
      if (s) {
        msg.append(s, static_cast<std::size_t>(n));
      } else {
        msg += "Python provider error";
      }
      Py_DECREF(text);
    } else {
      PyErr_Clear();
      msg += "Python provider error";
    }
  } else {
    msg += "Python provider error";
  }

  Py_XDECREF(type);
  Py_XDECREF(value);
  Py_XDECREF(traceback);
  return sanitize::Status::Invalid(msg);
}

void close_arrow_chunk_provider(NativeArrowSourcesStreamState *state) noexcept {
  if (!state || !state->chunk_provider) {
    return;
  }
  PyObject *provider = state->chunk_provider;
  state->chunk_provider = nullptr;
  GilGuard gil;
  PyObject *result = PyObject_CallMethod(provider, "close", nullptr);
  if (!result) {
    PyErr_Clear();
  } else {
    Py_DECREF(result);
  }
  Py_DECREF(provider);
}
bool parse_arrow_sources(PyObject *sources_obj,
                         std::vector<ArrowSourceSpec> *out) {
  if (!PySequence_Check(sources_obj) || PyUnicode_Check(sources_obj)) {
    PyErr_SetString(PyExc_TypeError, "sources must be a sequence");
    return false;
  }
  const Py_ssize_t size = PySequence_Size(sources_obj);
  if (size < 0) {
    return false;
  }
  if (size == 0) {
    PyErr_SetString(PyExc_ValueError, "sources must not be empty");
    return false;
  }
  out->clear();
  out->reserve(static_cast<std::size_t>(size));
  for (Py_ssize_t i = 0; i < size; ++i) {
    bool borrowed = false;
    PyObject *item = sequence_item_borrowed_or_new(sources_obj, i, &borrowed);
    if (!item) {
      decref_arrow_sources(out);
      return false;
    }
    std::unique_ptr<PyObject, decltype(&Py_DECREF)> item_owner(
        borrowed ? nullptr : item, Py_DECREF);
    if (!PySequence_Check(item) || PyUnicode_Check(item) ||
        PySequence_Size(item) != 2) {
      decref_arrow_sources(out);
      PyErr_SetString(PyExc_TypeError,
                      "each source must be (arrow_stream, source_file)");
      return false;
    }
    PyObject *stream_obj = PySequence_GetItem(item, 0);
    PyObject *source_file_obj = PySequence_GetItem(item, 1);
    if (!stream_obj || !source_file_obj) {
      Py_XDECREF(stream_obj);
      Py_XDECREF(source_file_obj);
      decref_arrow_sources(out);
      return false;
    }
    std::unique_ptr<PyObject, decltype(&Py_DECREF)> stream_owner(stream_obj,
                                                                 Py_DECREF);
    std::unique_ptr<PyObject, decltype(&Py_DECREF)> source_file_owner(
        source_file_obj, Py_DECREF);

    ArrowSourceSpec spec;
    if (!py_unicode_to_string(source_file_obj, "source_file",
                              &spec.source_file)) {
      decref_arrow_sources(out);
      return false;
    }
    Py_INCREF(stream_obj);
    spec.stream_obj = stream_obj;
    out->push_back(std::move(spec));
  }
  return true;
}

sanitize::Result<sanitize::LogicalSchema>
arrow_source_logical_schema(PyObject *stream_obj,
                            const sanitize::PreparedOptionsPtr &prepared) {
  const int has_schema_protocol =
      PyObject_HasAttrString(stream_obj, "__arrow_c_schema__");
  if (has_schema_protocol < 0) {
    return python_arrow_provider_error_status(
        "Arrow-source schema protocol lookup failed");
  }
  if (has_schema_protocol != 0) {
    PyObject *capsule = nullptr;
    ArrowSchema *schema = nullptr;
    if (!acquire_arrow_schema(stream_obj, &capsule, &schema)) {
      return python_arrow_provider_error_status(
          "Arrow-source schema export failed");
    }
    std::unique_ptr<PyObject, decltype(&decref_with_gil)> capsule_owner(
        capsule, decref_with_gil);
    std::vector<ArrowInputNode> fields;
    return logical_schema_from_arrow_schema(
        schema, &fields,
        ArrowDirectOptions{
            .timestamp_precision = prepared->spec.timestamp_precision,
            .memory_limit_bytes = prepared->spec.memory_limit_bytes});
  }
  sanitize::LogicalSchema input_schema;
  auto frontend_r = make_arrow_frontend(
      stream_obj, &input_schema,
      ArrowDirectOptions{
          .timestamp_precision = prepared->spec.timestamp_precision,
          .memory_limit_bytes = prepared->spec.memory_limit_bytes});
  if (!frontend_r.ok()) {
    return frontend_r.status();
  }
  return input_schema;
}
sanitize::Status
load_next_arrow_provider_chunk(NativeArrowSourcesStreamState *state) {
  if (!state || !state->chunk_provider || state->chunk_provider_exhausted) {
    return sanitize::Status::OK();
  }

  GilGuard gil;
  PyObject *result =
      PyObject_CallMethod(state->chunk_provider, "next_sources", nullptr);
  if (!result) {
    return python_arrow_provider_error_status(
        "Arrow-source chunk provider failed");
  }
  if (result == Py_None) {
    Py_DECREF(result);
    decref_arrow_sources(&state->sources);
    state->index = 0;
    state->chunk_provider_exhausted = true;
    return sanitize::Status::OK();
  }

  decref_arrow_sources(&state->sources);
  if (!parse_arrow_sources(result, &state->sources)) {
    Py_DECREF(result);
    return python_arrow_provider_error_status(
        "Arrow-source chunk provider returned invalid sources");
  }
  Py_DECREF(result);
  state->index = 0;
  return sanitize::Status::OK();
}

bool arrow_provider_has_next_sources(PyObject *provider_obj) {
  if (!PyObject_HasAttrString(provider_obj, "next_sources")) {
    PyErr_SetString(PyExc_TypeError,
                    "Arrow-source chunk provider must expose next_sources()");
    return false;
  }
  return true;
}

void close_python_arrow_provider(PyObject *provider_obj) noexcept {
  if (!provider_obj) {
    return;
  }
  PyObject *result = PyObject_CallMethod(provider_obj, "close", nullptr);
  if (!result) {
    PyErr_Clear();
    return;
  }
  Py_DECREF(result);
}

bool parse_next_arrow_provider_sources(PyObject *provider_obj,
                                       std::vector<ArrowSourceSpec> *sources,
                                       bool *exhausted) {
  if (!sources || !exhausted) {
    PyErr_SetString(PyExc_ValueError, "invalid Arrow-source provider state");
    return false;
  }
  sources->clear();
  *exhausted = false;
  PyObject *result = PyObject_CallMethod(provider_obj, "next_sources", nullptr);
  if (!result) {
    return false;
  }
  if (result == Py_None) {
    Py_DECREF(result);
    *exhausted = true;
    return true;
  }
  const bool ok = parse_arrow_sources(result, sources);
  Py_DECREF(result);
  return ok;
}
// Arrow provider chunk schema, metadata, passthrough, and ingest flow.
sanitize::Result<sanitize::SchemaRegistryMergeResult>
merge_arrow_source_schemas(schema_sanitizer_context *ctx,
                           const std::vector<ArrowSourceSpec> &sources,
                           const sanitize::PreparedOptionsPtr &prepared,
                           const char *registry_json,
                           const char *field_name_policy,
                           const sanitize::LogicalSchema *previous_schema) {
  (void)ctx;
  std::string combined_registry = "{}";
  sanitize::LogicalSchema combined_schema;
  bool has_schema = false;
  for (const ArrowSourceSpec &source : sources) {
    SAN_ASSIGN_OR_RAISE(auto input_schema, arrow_source_logical_schema(
                                               source.stream_obj, prepared));
    auto merged = sanitize::merge_schema_registry(make_registry_merge_input(
        std::move(input_schema), combined_registry.c_str(), field_name_policy,
        prepared->spec.default_key_name, prepared->spec.field_order,
        prepared->operation_detected_at));
    if (!merged.ok()) {
      return merged.status();
    }
    auto merged_value = std::move(merged).ValueOrDie();
    combined_schema = std::move(merged_value.schema);
    combined_registry = std::move(merged_value.registry_json);
    has_schema = true;
  }
  if (!has_schema) {
    return sanitize::Status::Invalid("sources must not be empty");
  }
  auto merge_input = make_registry_merge_input(
      std::move(combined_schema), registry_json, field_name_policy,
      prepared->spec.default_key_name, prepared->spec.field_order,
      prepared->operation_detected_at);
  return previous_schema ? sanitize::merge_schema_registry_with_previous_schema(
                               merge_input, *previous_schema)
                         : sanitize::merge_schema_registry(merge_input);
}

sanitize::Result<sanitize::SchemaRegistryMergeResult>
merge_arrow_source_provider_schemas(
    schema_sanitizer_context *ctx, PyObject *provider_obj,
    const sanitize::PreparedOptionsPtr &prepared, const char *registry_json,
    const char *field_name_policy,
    const sanitize::LogicalSchema *previous_schema) {
  if (!arrow_provider_has_next_sources(provider_obj)) {
    return sanitize::Status::Invalid("invalid Arrow-source chunk provider");
  }
  std::string current_registry = registry_json ? registry_json : "{}";
  std::string drifts = "[";
  std::string detected_at;
  sanitize::LogicalSchema current_schema;
  bool has_schema = false;
  bool exhausted = false;
  while (!exhausted) {
    if (!check_python_signals()) {
      close_python_arrow_provider(provider_obj);
      return sanitize::Status::Cancelled("Python signal received");
    }
    std::vector<ArrowSourceSpec> sources;
    if (!parse_next_arrow_provider_sources(provider_obj, &sources,
                                           &exhausted)) {
      auto status = python_arrow_provider_error_status(
          "Arrow-source chunk provider returned invalid sources");
      close_python_arrow_provider(provider_obj);
      return status;
    }
    if (exhausted) {
      break;
    }
    if (sources.empty()) {
      continue;
    }
    const sanitize::LogicalSchema *base_schema =
        has_schema ? &current_schema : previous_schema;
    auto merged = merge_arrow_source_schemas(ctx, sources, prepared,
                                             current_registry.c_str(),
                                             field_name_policy, base_schema);
    decref_arrow_sources(&sources);
    if (!merged.ok()) {
      close_python_arrow_provider(provider_obj);
      return merged.status();
    }
    auto value = std::move(merged).ValueOrDie();
    current_schema = std::move(value.schema);
    current_registry = std::move(value.registry_json);
    append_json_array_items(&drifts, value.drifts_json);
    detected_at = std::move(value.detected_at);
    has_schema = true;
  }
  close_python_arrow_provider(provider_obj);
  if (!has_schema) {
    return sanitize::Status::Invalid("sources must not be empty");
  }
  drifts.push_back(']');
  return sanitize::SchemaRegistryMergeResult{
      .schema = std::move(current_schema),
      .registry_json = std::move(current_registry),
      .drifts_json = std::move(drifts),
      .detected_at = std::move(detected_at),
  };
}

} // namespace core_abi3_internal::arrow_registry_detail
