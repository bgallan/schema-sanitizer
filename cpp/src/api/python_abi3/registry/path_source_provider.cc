/* Path-source provider parsing, metadata packing, and schema probing. */
#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/abi/python_abi3/methods.hh"

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <memory>
#include <new>
#include <string>
#include <utility>
#include <vector>

#include "api/python_abi3/metadata/columns/api.hh"
#include "api/python_abi3/metadata/stream/stream.hh"
#include "api/python_abi3/path_sources/path_sources.hh"
#include "api/python_abi3/registry/native_multi_source_stream.hh"
#include "api/python_abi3/registry/path_source_sinks_internal.hh"
#include "api/python_abi3/registry/plan/plan.hh"
#include "api/python_abi3/registry/registry_stream_metadata.hh"
#include "internal/abi/python_abi3/native_sink.hh"
#include "internal/arrow_c/cdata_stream_callbacks.hh"
#include "sanitize/core/diagnostics.hh"
#include "sanitize/ingest/chunk_source.hh"
#include "sanitize/ingest/ingest.hh"
#include "sanitize/registry/registry.hh"
#include "sanitize/schema_registry/schema_registry.hh"

namespace core_abi3_internal::path_registry_detail {

void release_registry_outputs(PyRegistrySinkOutputs *outputs) {
  release_sink_outputs(outputs->main_stream, outputs->diagnostics);
  outputs->main_stream = nullptr;
  outputs->diagnostics = nullptr;
}

bool registry_metadata_requested(PyObject *first_row_columns,
                                 PyObject *all_row_columns,
                                 PyObject *row_span_columns,
                                 PyObject *timestamp_columns) {
  return first_row_columns || all_row_columns || row_span_columns ||
         timestamp_columns;
}

PyObject *registry_first_row_columns(PyObject *first_row_columns,
                                     std::string_view registry_json,
                                     std::string_view drifts_json) {
  PyObject *merged = (first_row_columns && first_row_columns != Py_None)
                         ? PyDict_Copy(first_row_columns)
                         : PyDict_New();
  if (!merged) {
    return nullptr;
  }
  PyObject *registry_value = PyUnicode_FromStringAndSize(
      registry_json.data(), static_cast<Py_ssize_t>(registry_json.size()));
  PyObject *drifts_value = PyUnicode_FromStringAndSize(
      drifts_json.data(), static_cast<Py_ssize_t>(drifts_json.size()));
  if (!registry_value || !drifts_value ||
      PyDict_SetItemString(merged, "schema_registry", registry_value) != 0 ||
      PyDict_SetItemString(merged, "schema_drifts", drifts_value) != 0) {
    Py_XDECREF(registry_value);
    Py_XDECREF(drifts_value);
    Py_DECREF(merged);
    return nullptr;
  }
  Py_DECREF(registry_value);
  Py_DECREF(drifts_value);
  return merged;
}

bool wrap_registry_stream_with_metadata(PyRegistrySinkOutputs *outputs,
                                        PyObject *first_row_columns,
                                        PyObject *all_row_columns,
                                        PyObject *row_span_columns,
                                        PyObject *timestamp_columns,
                                        std::int64_t memory_limit_bytes) {
  if (!outputs || !outputs->main_stream ||
      !registry_metadata_requested(first_row_columns, all_row_columns,
                                   row_span_columns, timestamp_columns)) {
    return true;
  }
  PyObject *merged_first = registry_first_row_columns(
      first_row_columns, outputs->registry_json, outputs->drifts_json);
  if (!merged_first) {
    return false;
  }
  ArrowArrayStream *wrapped = make_metadata_stream_wrapper_from_stream(
      outputs->main_stream, merged_first,
      all_row_columns ? all_row_columns : Py_None,
      row_span_columns ? row_span_columns : Py_None,
      timestamp_columns ? timestamp_columns : Py_None, memory_limit_bytes);
  Py_DECREF(merged_first);
  if (!wrapped) {
    return false;
  }
  outputs->main_stream = wrapped;
  return true;
}

PyObject *pack_registry_or_raise_with_metadata(
    sanitize::Result<NativeRegistrySinkOutput> result, PyObject *keepalive,
    PyObject *first_row_columns, PyObject *all_row_columns,
    PyObject *row_span_columns, PyObject *timestamp_columns,
    std::int64_t memory_limit_bytes) {
  if (!result.ok()) {
    raise_status_error(result.status());
    return nullptr;
  }
  auto native = std::move(result).ValueOrDie();
  PyRegistrySinkOutputs outputs{
      .main_stream = native.sink.stream.release(),
      .diagnostics = native.sink.diagnostics.release(),
      .registry_json = std::move(native.registry_json),
      .drifts_json = std::move(native.drifts_json),
      .conversion_timestamp = std::move(native.conversion_timestamp),
  };
  if (!wrap_registry_stream_with_metadata(
          &outputs, first_row_columns, all_row_columns, row_span_columns,
          timestamp_columns, memory_limit_bytes)) {
    release_registry_outputs(&outputs);
    return nullptr;
  }
  return pack_registry_stream_result(keepalive, outputs.main_stream,
                                     outputs.diagnostics, outputs.registry_json,
                                     outputs.drifts_json,
                                     outputs.conversion_timestamp, nullptr);
}

bool path_source_input_empty(const PathSourceInput &input) noexcept {
  return !input.chunk_source && input.paths.empty();
}

std::vector<MetadataColumn>
metadata_columns_for_child(const NativePathSourcesStreamState *state,
                           const PathSourceSpec &source,
                           bool source_file_in_inner) {
  return registry_child_metadata_columns(
      state->first_row_columns, state->timestamp_columns,
      state->first_row_pending, source.source_file,
      state->source_file_column && !source_file_in_inner);
}

std::string path_source_error_message(const PathSourceSpec &source,
                                      const std::string &message) {
  if (source.source_file.empty() || message.contains(source.source_file)) {
    return message;
  }
  return "Invalid source file " + source.source_file + ": " + message;
}

sanitize::Status python_provider_error_status(const char *where) {
  PyObject *type = nullptr;
  PyObject *value = nullptr;
  PyObject *traceback = nullptr;
  PyErr_Fetch(&type, &value, &traceback);
  PyErr_NormalizeException(&type, &value, &traceback);

  std::string msg(where ? where : "Python chunk provider");
  msg += ": ";
  if (value) {
    PyObject *text = PyObject_Str(value);
    if (text) {
      Py_ssize_t n = 0;
      const char *s = PyUnicode_AsUTF8AndSize(text, &n);
      if (s && n > 0) {
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

void close_chunk_provider(NativePathSourcesStreamState *state) noexcept {
  if (!state || !state->chunk_provider) {
    return;
  }
  PyObject *provider = state->chunk_provider;
  state->chunk_provider = nullptr;
  PyGILState_STATE gil = PyGILState_Ensure();
  PyObject *result = PyObject_CallMethod(provider, "close", nullptr);
  if (!result) {
    PyErr_Clear();
  } else {
    Py_DECREF(result);
  }
  Py_DECREF(provider);
  PyGILState_Release(gil);
}

sanitize::Status load_next_provider_chunk(NativePathSourcesStreamState *state) {
  if (!state || !state->chunk_provider || state->chunk_provider_exhausted) {
    return sanitize::Status::OK();
  }

  PyGILState_STATE gil = PyGILState_Ensure();
  PyObject *result =
      PyObject_CallMethod(state->chunk_provider, "next_sources", nullptr);
  if (!result) {
    auto status =
        python_provider_error_status("path-source chunk provider failed");
    PyGILState_Release(gil);
    return status;
  }
  if (result == Py_None) {
    Py_DECREF(result);
    PyGILState_Release(gil);
    state->sources.clear();
    state->index = 0;
    state->chunk_provider_exhausted = true;
    return sanitize::Status::OK();
  }

  std::vector<PathSourceSpec> next_sources;
  if (!parse_path_sources(result, &next_sources)) {
    Py_DECREF(result);
    auto status = python_provider_error_status(
        "path-source chunk provider returned invalid sources");
    PyGILState_Release(gil);
    return status;
  }
  Py_DECREF(result);
  PyGILState_Release(gil);
  state->sources = std::move(next_sources);
  state->index = 0;
  return sanitize::Status::OK();
}

bool provider_has_next_sources(PyObject *provider_obj) {
  if (!PyObject_HasAttrString(provider_obj, "next_sources")) {
    PyErr_SetString(PyExc_TypeError,
                    "path-source chunk provider must expose next_sources()");
    return false;
  }
  return true;
}

void close_python_provider(PyObject *provider_obj) noexcept {
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

bool parse_next_provider_sources(PyObject *provider_obj,
                                 std::vector<PathSourceSpec> *sources,
                                 bool *exhausted) {
  if (!sources || !exhausted) {
    PyErr_SetString(PyExc_ValueError, "invalid path-source provider state");
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
  const bool ok = parse_path_sources(result, sources);
  Py_DECREF(result);
  return ok;
}

sanitize::Result<sanitize::SchemaRegistryMergeResult>
merge_path_source_provider_schemas(
    NativeContext *ctx, PyObject *provider_obj,
    const sanitize::PreparedOptionsPtr &prepared, const char *registry_json,
    const char *field_name_policy, bool skip_invalid_json_sources,
    const sanitize::LogicalSchema *previous_schema,
    sanitize::SchemaEvolutionMode schema_evolution) {
  if (!provider_has_next_sources(provider_obj)) {
    return sanitize::Status::Invalid("invalid path-source chunk provider");
  }
  std::string current_registry = registry_json ? registry_json : "{}";
  std::string drifts = "[";
  std::string detected_at;
  sanitize::LogicalSchema current_schema;
  bool has_schema = false;
  bool exhausted = false;
  std::vector<PathSourceSpec> sources;
  while (!exhausted) {
    if (!check_python_signals()) {
      close_python_provider(provider_obj);
      return sanitize::Status::Cancelled("Python signal received");
    }
    if (!parse_next_provider_sources(provider_obj, &sources, &exhausted)) {
      auto status = python_provider_error_status(
          "path-source chunk provider returned invalid sources");
      close_python_provider(provider_obj);
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
    auto merged = merge_path_source_schemas(
        ctx, sources, prepared, current_registry.c_str(), field_name_policy,
        skip_invalid_json_sources, base_schema, schema_evolution);
    if (!merged.ok()) {
      close_python_provider(provider_obj);
      return merged.status();
    }
    auto value = std::move(merged).ValueOrDie();
    current_schema = std::move(value.merged.schema);
    current_registry = std::move(value.merged.registry_json);
    append_json_array_items(&drifts, value.merged.drifts_json);
    detected_at = std::move(value.merged.detected_at);
    has_schema = true;
  }
  close_python_provider(provider_obj);
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

} // namespace core_abi3_internal::path_registry_detail
