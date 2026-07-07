/*
 * Python ABI3 source-selected registry sink wrapper.
 *
 * This entry point chooses path, stream, or text handling from one native ABI
 * call so Python writer orchestration can stay compact.
 */
#include "internal/abi/core_abi3_internal.hh"

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <memory>
#include <new>
#include <string>
#include <utility>
#include <vector>

#include "api/c/schema_sanitizer_c_sink_internal.hh"
#include "api/python_abi3/_core_abi3_metadata_columns.hh"
#include "api/python_abi3/_core_abi3_metadata_stream_builders.hh"
#include "api/python_abi3/_core_abi3_path_sources.hh"
#include "api/python_abi3/_core_abi3_registry_common.hh"
#include "internal/abi/schema_sanitizer_c_internal.hh"
#include "internal/pipeline/cdata_stream_utils.hh"
#include "sanitize/core/diagnostics.hh"
#include "sanitize/ingest/chunk_source.hh"
#include "sanitize/ingest/ingest.hh"
#include "sanitize/registry/registry.hh"
#include "sanitize/schema_registry/schema_registry.hh"

namespace core_abi3_internal {
namespace {

struct PyRegistrySinkOutputs {
  ArrowArrayStream *main_stream = nullptr;
  schema_sanitizer_diagnostics *diagnostics = nullptr;
  char *registry_json = nullptr;
  char *drifts_json = nullptr;
  char *conversion_timestamp = nullptr;
  char *err = nullptr;
};

void release_registry_outputs(PyRegistrySinkOutputs *outputs) {
  release_sink_outputs(outputs->main_stream, outputs->diagnostics);
  schema_sanitizer_free_string(outputs->registry_json);
  schema_sanitizer_free_string(outputs->drifts_json);
  schema_sanitizer_free_string(outputs->conversion_timestamp);
}

bool registry_metadata_requested(PyObject *first_row_columns,
                                 PyObject *all_row_columns,
                                 PyObject *row_span_columns,
                                 PyObject *timestamp_columns) {
  return first_row_columns || all_row_columns || row_span_columns ||
         timestamp_columns;
}

PyObject *registry_first_row_columns(PyObject *first_row_columns,
                                     const char *registry_json,
                                     const char *drifts_json) {
  PyObject *merged = (first_row_columns && first_row_columns != Py_None)
                         ? PyDict_Copy(first_row_columns)
                         : PyDict_New();
  if (!merged) {
    return nullptr;
  }
  PyObject *registry_value =
      PyUnicode_FromString(registry_json ? registry_json : "{}");
  PyObject *drifts_value =
      PyUnicode_FromString(drifts_json ? drifts_json : "[]");
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
                                        PyObject *timestamp_columns) {
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
      timestamp_columns ? timestamp_columns : Py_None);
  Py_DECREF(merged_first);
  if (!wrapped) {
    return false;
  }
  outputs->main_stream = wrapped;
  return true;
}

PyObject *pack_registry_or_raise_with_metadata(int status, PyObject *keepalive,
                                               PyRegistrySinkOutputs *outputs,
                                               PyObject *first_row_columns,
                                               PyObject *all_row_columns,
                                               PyObject *row_span_columns,
                                               PyObject *timestamp_columns) {
  if (status != SCHEMA_SANITIZER_STATUS_OK) {
    release_registry_outputs(outputs);
    raise_status_error(status, outputs->err);
    return nullptr;
  }
  if (!wrap_registry_stream_with_metadata(outputs, first_row_columns,
                                          all_row_columns, row_span_columns,
                                          timestamp_columns)) {
    release_registry_outputs(outputs);
    return nullptr;
  }
  return pack_registry_stream_result(
      keepalive, outputs->main_stream, outputs->diagnostics,
      outputs->registry_json, outputs->drifts_json,
      outputs->conversion_timestamp);
}

bool path_source_input_empty(const PathSourceInput &input) noexcept {
  return !input.chunk_source && input.paths.empty();
}

struct NativePathSourcesStreamState {
  schema_sanitizer_context *ctx = nullptr;
  sanitize::PreparedOptionsPtr prepared;
  std::string sink_name;
  bool registry_enabled = true;
  bool source_file_column = true;
  std::string registry_json;
  std::string drifts_json;
  std::string conversion_timestamp;
  std::string field_name_policy;
  std::string schema_mode;
  std::vector<PathSourceSpec> sources;
  std::vector<MetadataColumn> first_row_columns;
  std::vector<MetadataColumn> timestamp_columns;
  std::shared_ptr<const NativeRegistryPlan> registry_plan;
  std::shared_ptr<const NativeRegistryPlan> source_file_registry_plan;
  std::size_t index = 0;
  bool first_row_pending = true;
  PyObject *chunk_provider = nullptr;
  bool chunk_provider_exhausted = false;

  ArrowArrayStream *inner = nullptr;
  schema_sanitizer_diagnostics *diagnostics = nullptr;
  std::unique_ptr<MetadataStreamState> metadata;
  std::string last_error;
};

std::vector<MetadataColumn>
metadata_columns_for_child(const NativePathSourcesStreamState *state,
                           const PathSourceSpec &source,
                           bool source_file_in_inner = false) {
  return registry_child_metadata_columns(
      state->first_row_columns, state->timestamp_columns,
      state->first_row_pending, source.source_file,
      state->source_file_column && !source_file_in_inner);
}

std::string path_source_error_message(const PathSourceSpec &source,
                                      const std::string &message) {
  if (source.source_file.empty() ||
      message.find(source.source_file) != std::string::npos) {
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

void append_json_array_items(std::string *out, std::string_view array_json) {
  if (!out) {
    return;
  }
  std::size_t begin = 0;
  std::size_t end = array_json.size();
  while (begin < end && static_cast<unsigned char>(array_json[begin]) <= ' ') {
    ++begin;
  }
  while (end > begin &&
         static_cast<unsigned char>(array_json[end - 1]) <= ' ') {
    --end;
  }
  if (end <= begin) {
    return;
  }
  if (array_json[begin] == '[' && array_json[end - 1] == ']') {
    ++begin;
    --end;
  }
  while (begin < end && static_cast<unsigned char>(array_json[begin]) <= ' ') {
    ++begin;
  }
  while (end > begin &&
         static_cast<unsigned char>(array_json[end - 1]) <= ' ') {
    --end;
  }
  if (end <= begin) {
    return;
  }
  if (out->size() > 1) {
    out->push_back(',');
  }
  out->append(array_json.substr(begin, end - begin));
}

sanitize::Result<sanitize::SchemaRegistryMergeResult>
merge_path_source_provider_schemas(
    schema_sanitizer_context *ctx, PyObject *provider_obj,
    const sanitize::PreparedOptionsPtr &prepared, const char *registry_json,
    const char *field_name_policy, bool skip_invalid_json_sources,
    const sanitize::LogicalSchema *previous_schema = nullptr) {
  if (!provider_has_next_sources(provider_obj)) {
    return sanitize::Status::Invalid("invalid path-source chunk provider");
  }
  std::string current_registry = registry_json ? registry_json : "{}";
  std::string drifts = "[";
  std::string detected_at;
  sanitize::LogicalSchema current_schema;
  bool has_schema = false;
  bool exhausted = false;
  while (!exhausted) {
    if (!check_python_signals()) {
      close_python_provider(provider_obj);
      return sanitize::Status::Cancelled("Python signal received");
    }
    std::vector<PathSourceSpec> sources;
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
        skip_invalid_json_sources, base_schema);
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

void close_current_source(NativePathSourcesStreamState *state) noexcept {
  if (!state) {
    return;
  }
  state->metadata.reset();
  release_sink_outputs(state->inner, state->diagnostics);
  state->inner = nullptr;
  state->diagnostics = nullptr;
}

sanitize::Result<sanitize::IngestStream> ingest_path_source_with_registry_plan(
    NativePathSourcesStreamState *state, const PathSourceSpec &source,
    PathSourceInput input,
    std::shared_ptr<const NativeRegistryPlan> registry_plan) {
  if (!state || !registry_plan) {
    return sanitize::Status::Invalid("native registry plan is null");
  }
  const std::string frontend_name(
      path_source_materializer_frontend(input.frontend));
  SAN_ASSIGN_OR_RAISE(
      auto frontend,
      path_source_frontend(std::move(input), state->prepared->spec));

  frontend.set_plan(registry_plan->plan.get());
  auto diagnostics = std::make_shared<sanitize::IngestDiagnostics>();
  diagnostics->arrow_schema_depth =
      sanitize::arrow_schema_depth(registry_plan->schema);
  diagnostics->parquet_schema_depth =
      sanitize::parquet_schema_depth(registry_plan->schema);

  sanitize::PreparedIngest prepared;
  prepared.frontend_name = frontend_name;
  prepared.frontend = std::move(frontend);
  prepared.owned_ctx = state->ctx ? state->ctx->ctx : nullptr;
  prepared.ctx = prepared.owned_ctx.get();
  prepared.plan = registry_plan->plan;
  prepared.opts = state->prepared;
  prepared.diagnostics = std::move(diagnostics);
  prepared.logical_schema = registry_plan->schema;
  prepared.inference_consumed = false;
  if (!prepared.ctx) {
    return sanitize::Status::Invalid(
        "native registry plan source has no execution context");
  }
  (void)source;
  return sanitize::ingest_to_stream(std::move(prepared));
}

sanitize::Status open_next_source(NativePathSourcesStreamState *state) {
  if (!state) {
    return sanitize::Status::Invalid("native path sources stream is closed");
  }
  close_current_source(state);
  if (state->index >= state->sources.size()) {
    if (state->chunk_provider && !state->chunk_provider_exhausted) {
      SAN_RETURN_NOT_OK(load_next_provider_chunk(state));
    }
  }
  if (state->index >= state->sources.size()) {
    return sanitize::Status::OK();
  }
  const std::size_t source_index = state->index;
  const PathSourceSpec &source = state->sources[source_index];
  bool source_file_in_inner = false;
  std::shared_ptr<const NativeRegistryPlan> active_registry_plan =
      state->registry_plan;
  PathSourceInput input;

  if (state->registry_enabled && state->registry_plan &&
      state->source_file_column) {
    SAN_ASSIGN_OR_RAISE(
        auto group,
        next_path_source_group_plan(state->sources, source_index,
                                    PathSourceGroupPurpose::kMaterialization,
                                    state->prepared->spec.input_text_encoding));
    if (group.grouped) {
      SAN_ASSIGN_OR_RAISE(
          input,
          path_source_group_input(state->sources, group,
                                  state->prepared->spec.input_text_encoding));
      if (!state->source_file_registry_plan) {
        SAN_ASSIGN_OR_RAISE(
            auto augmented_plan,
            make_native_registry_plan_with_generated_source_file(
                *state->registry_plan));
        state->source_file_registry_plan = std::move(augmented_plan);
      }
      active_registry_plan = state->source_file_registry_plan;
      source_file_in_inner = group.source_file_in_inner;
      state->index = group.end;
    }
  }

  if (path_source_input_empty(input)) {
    SAN_ASSIGN_OR_RAISE(input, path_source_input(state->prepared, source));
    state->index = source_index + 1;
  }

  if (state->registry_enabled && state->registry_plan) {
    auto out_r = ingest_path_source_with_registry_plan(
        state, source, std::move(input), active_registry_plan);
    if (!out_r.ok()) {
      return sanitize::Status::Invalid(
          path_source_error_message(source, out_r.status().ToString()));
    }
    auto out = std::move(out_r).ValueOrDie();
    state->inner = out.stream.release();
    state->diagnostics = new (std::nothrow) schema_sanitizer_diagnostics();
    if (!state->diagnostics) {
      schema_sanitizer_stream_free(state->inner);
      state->inner = nullptr;
      return sanitize::Status::OutOfMemory(
          "context_to_registry_sink_from_path_sources: diagnostics allocation "
          "failed");
    }
    state->diagnostics->diagnostics = std::move(out.diagnostics);
  } else if (state->registry_enabled) {
    PyRegistrySinkOutputs outputs;
    int st = context_to_registry_sink_from_source_internal(
        state->ctx, state->sink_name.c_str(), input.frontend.c_str(),
        std::move(input.chunk_source), state->prepared,
        state->registry_json.c_str(), state->field_name_policy.c_str(),
        state->schema_mode.c_str(),
        ::RegistrySinkOutputs{
            .sink = SinkOutputs{.stream = &outputs.main_stream,
                                .diagnostics = &outputs.diagnostics},
            .registry_json = &outputs.registry_json,
            .drifts_json = &outputs.drifts_json,
            .conversion_timestamp = &outputs.conversion_timestamp},
        &outputs.err, "context_to_registry_sink_from_path_sources");
    if (st != SCHEMA_SANITIZER_STATUS_OK) {
      std::string message = path_source_error_message(
          source, outputs.err ? outputs.err : "native source failed");
      release_registry_outputs(&outputs);
      schema_sanitizer_free_string(outputs.err);
      return sanitize::Status::Invalid(message);
    }
    schema_sanitizer_free_string(outputs.registry_json);
    schema_sanitizer_free_string(outputs.drifts_json);
    schema_sanitizer_free_string(outputs.conversion_timestamp);
    state->inner = outputs.main_stream;
    state->diagnostics = outputs.diagnostics;
  } else {
    ArrowArrayStream *main_stream = nullptr;
    schema_sanitizer_diagnostics *diagnostics = nullptr;
    char *err = nullptr;
    int st = context_to_sink_from_source_internal(
        state->ctx, state->sink_name.c_str(), input.frontend.c_str(),
        std::move(input.chunk_source), state->prepared,
        SinkOutputs{.stream = &main_stream, .diagnostics = &diagnostics}, &err,
        "context_to_sink_from_path_sources");
    if (st != SCHEMA_SANITIZER_STATUS_OK) {
      std::string message =
          path_source_error_message(source, err ? err : "native source failed");
      release_sink_outputs(main_stream, diagnostics);
      schema_sanitizer_free_string(err);
      return sanitize::Status::Invalid(message);
    }
    state->inner = main_stream;
    state->diagnostics = diagnostics;
  }
  state->metadata = std::make_unique<MetadataStreamState>();
  state->metadata->inner = state->inner;
  state->metadata->columns =
      metadata_columns_for_child(state, source, source_file_in_inner);
  state->metadata->first_row_pending = state->first_row_pending;
  return sanitize::Status::OK();
}

sanitize::Status path_sources_open_next(void *state) {
  return open_next_source(static_cast<NativePathSourcesStreamState *>(state));
}

void path_sources_close_current(void *state) noexcept {
  close_current_source(static_cast<NativePathSourcesStreamState *>(state));
}

MetadataStreamState *path_sources_metadata(void *state) noexcept {
  auto *typed = static_cast<NativePathSourcesStreamState *>(state);
  return typed && typed->metadata ? typed->metadata.get() : nullptr;
}

std::string &path_sources_error(void *state) noexcept {
  return static_cast<NativePathSourcesStreamState *>(state)->last_error;
}

bool *path_sources_first_row_pending(void *state) noexcept {
  return &static_cast<NativePathSourcesStreamState *>(state)->first_row_pending;
}

void path_sources_destroy_state(void *state) noexcept {
  auto *typed = static_cast<NativePathSourcesStreamState *>(state);
  close_chunk_provider(typed);
  delete typed;
}

const NativeMultiSourceStreamOps kPathSourcesOps{
    .schema_context = "path_sources.get_schema",
    .next_context = "path_sources.get_next",
    .empty_message = "native path sources stream has no sources",
    .invalid_stream_message = "invalid native path sources stream",
    .open_next = &path_sources_open_next,
    .close_current = &path_sources_close_current,
    .metadata = &path_sources_metadata,
    .last_error = &path_sources_error,
    .first_row_pending = &path_sources_first_row_pending,
    .destroy_state = &path_sources_destroy_state,
};

const char *path_sources_last_error(ArrowArrayStream *stream) {
  return native_multi_source_last_error(stream, kPathSourcesOps);
}

void path_sources_release(ArrowArrayStream *stream) {
  native_multi_source_release(stream, kPathSourcesOps);
}

int path_sources_get_schema(ArrowArrayStream *stream, ArrowSchema *out) {
  return native_multi_source_get_schema(stream, out, kPathSourcesOps);
}

int path_sources_get_next(ArrowArrayStream *stream, ArrowArray *out) {
  return native_multi_source_get_next(stream, out, kPathSourcesOps);
}

} // namespace

// Converts a Python input through the selected source kind.
PyObject *py_context_to_registry_sink_from_source(PyObject *, PyObject *args) {
  PyObject *ctx_obj = nullptr;
  const char *sink_name = nullptr;
  const char *frontend_name = nullptr;
  const char *source_name = nullptr;
  PyObject *payload_obj = nullptr;
  PyObject *prepared_obj = Py_None;
  const char *registry_json = nullptr;
  const char *field_name_policy = nullptr;
  const char *schema_mode = nullptr;
  PyObject *first_row_columns = nullptr;
  PyObject *all_row_columns = nullptr;
  PyObject *row_span_columns = nullptr;
  PyObject *timestamp_columns = nullptr;

  if (!PyArg_ParseTuple(
          args, "OsssOOsss|OOOO:context_to_registry_sink_from_source", &ctx_obj,
          &sink_name, &frontend_name, &source_name, &payload_obj, &prepared_obj,
          &registry_json, &field_name_policy, &schema_mode, &first_row_columns,
          &all_row_columns, &row_span_columns, &timestamp_columns)) {
    return nullptr;
  }

  auto *ctx = unwrap_context(ctx_obj);
  if (!ctx)
    return nullptr;

  auto *prepared = unwrap_prepared_options(prepared_obj);
  if (prepared_obj != Py_None && !prepared)
    return nullptr;

  switch (parse_python_source_kind(source_name)) {
  case PythonSourceKind::kPath: {
    PyObject *path_bytes = fsencode_path(payload_obj);
    if (!path_bytes)
      return nullptr;
    const char *path = PyBytes_AsString(path_bytes);

    PyRegistrySinkOutputs outputs;

    int st = schema_sanitizer_context_to_registry_sink_path(
        ctx, sink_name, frontend_name, path, prepared, registry_json,
        field_name_policy, schema_mode, &outputs.main_stream,
        &outputs.diagnostics, &outputs.registry_json, &outputs.drifts_json,
        &outputs.conversion_timestamp, &outputs.err);
    Py_DECREF(path_bytes);

    return pack_registry_or_raise_with_metadata(
        st, ctx_obj, &outputs, first_row_columns, all_row_columns,
        row_span_columns, timestamp_columns);
  }

  case PythonSourceKind::kStream: {
    if (!python_reader_has_read_seek(payload_obj)) {
      set_python_reader_type_error();
      return nullptr;
    }

    sanitize::PreparedOptionsPtr prepared_shared;
    if (prepared_obj == Py_None) {
      auto pr = default_prepared_options();
      if (!pr.ok()) {
        PyErr_SetString(PyExc_RuntimeError, pr.status().ToString().c_str());
        return nullptr;
      }
      prepared_shared = std::move(pr).ValueOrDie();
    } else {
      prepared_shared = prepared->prepared;
    }

    PyRegistrySinkOutputs outputs;

    auto src = make_python_reader_chunk_source(payload_obj);
    int st = schema_sanitizer_context_to_registry_sink_from_source(
        ctx, sink_name, frontend_name, std::move(src), prepared_shared,
        registry_json, field_name_policy, schema_mode, &outputs.main_stream,
        &outputs.diagnostics, &outputs.registry_json, &outputs.drifts_json,
        &outputs.conversion_timestamp, &outputs.err,
        "schema_sanitizer_context_to_registry_sink_from_source");

    return pack_registry_or_raise_with_metadata(
        st, payload_obj, &outputs, first_row_columns, all_row_columns,
        row_span_columns, timestamp_columns);
  }

  case PythonSourceKind::kText: {
    const char *data = nullptr;
    Py_ssize_t data_len = 0;
    if (!bytes_or_str_view(payload_obj, &data, &data_len))
      return nullptr;

    PyRegistrySinkOutputs outputs;

    int st = schema_sanitizer_context_to_registry_sink_text(
        ctx, sink_name, frontend_name,
        reinterpret_cast<const std::uint8_t *>(data),
        static_cast<std::size_t>(data_len), prepared, registry_json,
        field_name_policy, schema_mode, &outputs.main_stream,
        &outputs.diagnostics, &outputs.registry_json, &outputs.drifts_json,
        &outputs.conversion_timestamp, &outputs.err);

    return pack_registry_or_raise_with_metadata(
        st, ctx_obj, &outputs, first_row_columns, all_row_columns,
        row_span_columns, timestamp_columns);
  }

  case PythonSourceKind::kUnknown:
    break;
  }

  PyErr_SetString(PyExc_ValueError,
                  "source must be 'path', 'stream', or 'text'");
  return nullptr;
}

PyObject *py_context_to_sink_from_path_sources(PyObject *, PyObject *args) {
  PyObject *ctx_obj = nullptr;
  const char *sink_name = nullptr;
  PyObject *sources_obj = nullptr;
  PyObject *prepared_obj = Py_None;
  int include_source_file = 1;
  PyObject *first_row_columns = nullptr;
  PyObject *timestamp_columns = nullptr;

  if (!PyArg_ParseTuple(args, "OsOOpOO:context_to_sink_from_path_sources",
                        &ctx_obj, &sink_name, &sources_obj, &prepared_obj,
                        &include_source_file, &first_row_columns,
                        &timestamp_columns)) {
    return nullptr;
  }
  auto *ctx = unwrap_context(ctx_obj);
  if (!ctx)
    return nullptr;
  auto *prepared = unwrap_prepared_options(prepared_obj);
  if (prepared_obj != Py_None && !prepared)
    return nullptr;

  auto state = std::make_unique<NativePathSourcesStreamState>();
  state->ctx = ctx;
  state->sink_name = sink_name ? sink_name : "stream";
  state->registry_enabled = false;
  state->source_file_column = include_source_file != 0;
  if (prepared_obj == Py_None) {
    auto default_options = default_prepared_options();
    if (!default_options.ok()) {
      PyErr_SetString(PyExc_RuntimeError,
                      default_options.status().ToString().c_str());
      return nullptr;
    }
    state->prepared = std::move(default_options).ValueOrDie();
  } else {
    state->prepared = prepared->prepared;
  }
  if (!parse_path_sources(sources_obj, &state->sources)) {
    return nullptr;
  }
  if (!append_first_row_columns_from_dict(first_row_columns,
                                          &state->first_row_columns)) {
    return nullptr;
  }
  if (!append_timestamp_columns_from_sequence(timestamp_columns,
                                              &state->timestamp_columns)) {
    return nullptr;
  }

  auto *stream = new (std::nothrow) ArrowArrayStream();
  if (!stream) {
    PyErr_NoMemory();
    return nullptr;
  }
  std::memset(stream, 0, sizeof(*stream));
  stream->get_schema = &path_sources_get_schema;
  stream->get_next = &path_sources_get_next;
  stream->get_last_error = &path_sources_last_error;
  stream->release = &path_sources_release;
  stream->private_data = state.release();

  auto *diagnostics = new (std::nothrow) schema_sanitizer_diagnostics();
  if (!diagnostics) {
    schema_sanitizer_stream_free(stream);
    PyErr_NoMemory();
    return nullptr;
  }
  return pack_stream_and_diagnostics(ctx_obj, stream, diagnostics);
}

PyObject *py_context_to_sink_from_path_source_chunk_provider(PyObject *,
                                                             PyObject *args) {
  PyObject *ctx_obj = nullptr;
  const char *sink_name = nullptr;
  PyObject *provider_obj = nullptr;
  PyObject *prepared_obj = Py_None;
  int include_source_file = 1;
  PyObject *first_row_columns = nullptr;
  PyObject *timestamp_columns = nullptr;

  if (!PyArg_ParseTuple(
          args, "OsOOpOO:context_to_sink_from_path_source_chunk_provider",
          &ctx_obj, &sink_name, &provider_obj, &prepared_obj,
          &include_source_file, &first_row_columns, &timestamp_columns)) {
    return nullptr;
  }
  auto *ctx = unwrap_context(ctx_obj);
  if (!ctx)
    return nullptr;
  auto *prepared = unwrap_prepared_options(prepared_obj);
  if (prepared_obj != Py_None && !prepared)
    return nullptr;
  if (!PyObject_HasAttrString(provider_obj, "next_sources")) {
    PyErr_SetString(PyExc_TypeError,
                    "path-source chunk provider must expose next_sources()");
    return nullptr;
  }

  auto state = std::make_unique<NativePathSourcesStreamState>();
  state->ctx = ctx;
  state->sink_name = sink_name ? sink_name : "stream";
  state->registry_enabled = false;
  state->source_file_column = include_source_file != 0;
  if (prepared_obj == Py_None) {
    auto default_options = default_prepared_options();
    if (!default_options.ok()) {
      PyErr_SetString(PyExc_RuntimeError,
                      default_options.status().ToString().c_str());
      return nullptr;
    }
    state->prepared = std::move(default_options).ValueOrDie();
  } else {
    state->prepared = prepared->prepared;
  }
  if (!append_first_row_columns_from_dict(first_row_columns,
                                          &state->first_row_columns)) {
    return nullptr;
  }
  if (!append_timestamp_columns_from_sequence(timestamp_columns,
                                              &state->timestamp_columns)) {
    return nullptr;
  }

  auto *stream = new (std::nothrow) ArrowArrayStream();
  if (!stream) {
    PyErr_NoMemory();
    return nullptr;
  }
  std::memset(stream, 0, sizeof(*stream));
  stream->get_schema = &path_sources_get_schema;
  stream->get_next = &path_sources_get_next;
  stream->get_last_error = &path_sources_last_error;
  stream->release = &path_sources_release;

  Py_INCREF(provider_obj);
  state->chunk_provider = provider_obj;
  stream->private_data = state.release();

  auto *diagnostics = new (std::nothrow) schema_sanitizer_diagnostics();
  if (!diagnostics) {
    schema_sanitizer_stream_free(stream);
    PyErr_NoMemory();
    return nullptr;
  }
  return pack_stream_and_diagnostics(ctx_obj, stream, diagnostics);
}

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
  auto *prepared = unwrap_prepared_options(prepared_obj);
  if (prepared_obj != Py_None && !prepared)
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
  if (prepared_obj == Py_None) {
    auto default_options = default_prepared_options();
    if (!default_options.ok()) {
      PyErr_SetString(PyExc_RuntimeError,
                      default_options.status().ToString().c_str());
      return nullptr;
    }
    state->prepared = std::move(default_options).ValueOrDie();
  } else {
    state->prepared = prepared->prepared;
  }
  if (!parse_path_sources(sources_obj, &state->sources)) {
    return nullptr;
  }
  if (!append_first_row_columns_from_dict(first_row_columns,
                                          &state->first_row_columns)) {
    return nullptr;
  }
  if (!append_timestamp_columns_from_sequence(timestamp_columns,
                                              &state->timestamp_columns)) {
    return nullptr;
  }
  auto plan_r = make_native_registry_plan_from_json(
      state->prepared, state->registry_json.c_str(),
      state->field_name_policy.c_str(), state->drifts_json.c_str(),
      state->conversion_timestamp.c_str());
  if (!plan_r.ok()) {
    PyErr_SetString(PyExc_ValueError, plan_r.status().ToString().c_str());
    return nullptr;
  }
  state->registry_plan = std::move(plan_r).ValueOrDie();

  auto *stream = new (std::nothrow) ArrowArrayStream();
  if (!stream) {
    PyErr_NoMemory();
    return nullptr;
  }
  std::memset(stream, 0, sizeof(*stream));
  stream->get_schema = &path_sources_get_schema;
  stream->get_next = &path_sources_get_next;
  stream->get_last_error = &path_sources_last_error;
  stream->release = &path_sources_release;

  PyRegistrySinkOutputs outputs;
  outputs.main_stream = stream;
  outputs.diagnostics = new (std::nothrow) schema_sanitizer_diagnostics();
  if (!outputs.diagnostics) {
    schema_sanitizer_stream_free(stream);
    PyErr_NoMemory();
    return nullptr;
  }
  outputs.registry_json = dup_cstr(registry_json ? registry_json : "{}");
  outputs.drifts_json = dup_cstr(drifts_json ? drifts_json : "[]");
  outputs.conversion_timestamp =
      dup_cstr(conversion_timestamp ? conversion_timestamp : "");
  if (!outputs.registry_json || !outputs.drifts_json ||
      !outputs.conversion_timestamp) {
    release_registry_outputs(&outputs);
    PyErr_NoMemory();
    return nullptr;
  }
  auto registry_plan = state->registry_plan;
  stream->private_data = state.release();
  return pack_registry_stream_result_with_state(
      ctx_obj, outputs.main_stream, outputs.diagnostics, outputs.registry_json,
      outputs.drifts_json, outputs.conversion_timestamp,
      std::move(registry_plan));
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
  auto *prepared = unwrap_prepared_options(prepared_obj);
  if (prepared_obj != Py_None && !prepared)
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
  if (prepared_obj == Py_None) {
    auto default_options = default_prepared_options();
    if (!default_options.ok()) {
      PyErr_SetString(PyExc_RuntimeError,
                      default_options.status().ToString().c_str());
      return nullptr;
    }
    state->prepared = std::move(default_options).ValueOrDie();
  } else {
    state->prepared = prepared->prepared;
  }
  if (!parse_path_sources(sources_obj, &state->sources)) {
    return nullptr;
  }
  if (!append_first_row_columns_from_dict(first_row_columns,
                                          &state->first_row_columns)) {
    return nullptr;
  }
  if (!append_timestamp_columns_from_sequence(timestamp_columns,
                                              &state->timestamp_columns)) {
    return nullptr;
  }

  auto *stream = new (std::nothrow) ArrowArrayStream();
  if (!stream) {
    PyErr_NoMemory();
    return nullptr;
  }
  std::memset(stream, 0, sizeof(*stream));
  stream->get_schema = &path_sources_get_schema;
  stream->get_next = &path_sources_get_next;
  stream->get_last_error = &path_sources_last_error;
  stream->release = &path_sources_release;

  PyRegistrySinkOutputs outputs;
  outputs.main_stream = stream;
  outputs.diagnostics = new (std::nothrow) schema_sanitizer_diagnostics();
  if (!outputs.diagnostics) {
    schema_sanitizer_stream_free(stream);
    PyErr_NoMemory();
    return nullptr;
  }
  outputs.registry_json = dup_cstr(registry_plan->registry_json);
  outputs.drifts_json = dup_cstr(registry_plan->drifts_json);
  outputs.conversion_timestamp = dup_cstr(registry_plan->conversion_timestamp);
  if (!outputs.registry_json || !outputs.drifts_json ||
      !outputs.conversion_timestamp) {
    release_registry_outputs(&outputs);
    PyErr_NoMemory();
    return nullptr;
  }
  stream->private_data = state.release();
  return pack_registry_stream_result_with_state(
      ctx_obj, outputs.main_stream, outputs.diagnostics, outputs.registry_json,
      outputs.drifts_json, outputs.conversion_timestamp,
      std::move(registry_plan));
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
  auto *prepared = unwrap_prepared_options(prepared_obj);
  if (prepared_obj != Py_None && !prepared)
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
  if (prepared_obj == Py_None) {
    auto default_options = default_prepared_options();
    if (!default_options.ok()) {
      PyErr_SetString(PyExc_RuntimeError,
                      default_options.status().ToString().c_str());
      return nullptr;
    }
    state->prepared = std::move(default_options).ValueOrDie();
  } else {
    state->prepared = prepared->prepared;
  }
  if (!append_first_row_columns_from_dict(first_row_columns,
                                          &state->first_row_columns)) {
    return nullptr;
  }
  if (!append_timestamp_columns_from_sequence(timestamp_columns,
                                              &state->timestamp_columns)) {
    return nullptr;
  }

  auto *stream = new (std::nothrow) ArrowArrayStream();
  if (!stream) {
    PyErr_NoMemory();
    return nullptr;
  }
  std::memset(stream, 0, sizeof(*stream));
  stream->get_schema = &path_sources_get_schema;
  stream->get_next = &path_sources_get_next;
  stream->get_last_error = &path_sources_last_error;
  stream->release = &path_sources_release;

  PyRegistrySinkOutputs outputs;
  outputs.main_stream = stream;
  outputs.diagnostics = new (std::nothrow) schema_sanitizer_diagnostics();
  if (!outputs.diagnostics) {
    schema_sanitizer_stream_free(stream);
    PyErr_NoMemory();
    return nullptr;
  }
  outputs.registry_json = dup_cstr(registry_plan->registry_json);
  outputs.drifts_json = dup_cstr(registry_plan->drifts_json);
  outputs.conversion_timestamp = dup_cstr(registry_plan->conversion_timestamp);
  if (!outputs.registry_json || !outputs.drifts_json ||
      !outputs.conversion_timestamp) {
    release_registry_outputs(&outputs);
    PyErr_NoMemory();
    return nullptr;
  }

  Py_INCREF(provider_obj);
  state->chunk_provider = provider_obj;
  stream->private_data = state.release();
  return pack_registry_stream_result_with_state(
      ctx_obj, outputs.main_stream, outputs.diagnostics, outputs.registry_json,
      outputs.drifts_json, outputs.conversion_timestamp,
      std::move(registry_plan));
}

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
  if (!ctx)
    return nullptr;
  auto *prepared = unwrap_prepared_options(prepared_obj);
  if (prepared_obj != Py_None && !prepared)
    return nullptr;
  if (!provider_has_next_sources(probe_provider_obj) ||
      !provider_has_next_sources(stream_provider_obj)) {
    return nullptr;
  }

  sanitize::PreparedOptionsPtr prepared_options;
  if (prepared_obj == Py_None) {
    auto default_options = default_prepared_options();
    if (!default_options.ok()) {
      PyErr_SetString(PyExc_RuntimeError,
                      default_options.status().ToString().c_str());
      return nullptr;
    }
    prepared_options = std::move(default_options).ValueOrDie();
  } else {
    prepared_options = prepared->prepared;
  }

  char *err = nullptr;
  const int valid = validate_registry_sink_mode(
      schema_mode, registry_json, &err,
      "context_to_registry_sink_from_path_source_chunk_provider_auto_registry");
  if (valid != SCHEMA_SANITIZER_STATUS_OK) {
    raise_status_error(valid, err);
    return nullptr;
  }

  auto merged_r = merge_path_source_provider_schemas(
      ctx, probe_provider_obj, prepared_options, registry_json,
      field_name_policy ? field_name_policy : "",
      skip_invalid_json_sources != 0);
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

  auto state = std::make_unique<NativePathSourcesStreamState>();
  state->ctx = ctx;
  state->sink_name = sink_name ? sink_name : "stream";
  state->registry_json = registry_plan->registry_json;
  state->drifts_json = registry_plan->drifts_json;
  state->conversion_timestamp = registry_plan->conversion_timestamp;
  state->field_name_policy = field_name_policy ? field_name_policy : "";
  state->schema_mode = schema_mode ? schema_mode : "additive";
  state->prepared = prepared_options;
  state->registry_plan = registry_plan;
  if (!append_first_row_columns_from_dict(first_row_columns,
                                          &state->first_row_columns)) {
    return nullptr;
  }
  if (!append_timestamp_columns_from_sequence(timestamp_columns,
                                              &state->timestamp_columns)) {
    return nullptr;
  }
  append_registry_first_row_columns(&state->first_row_columns,
                                    state->registry_json, state->drifts_json);

  auto *stream = new (std::nothrow) ArrowArrayStream();
  if (!stream) {
    PyErr_NoMemory();
    return nullptr;
  }
  std::memset(stream, 0, sizeof(*stream));
  stream->get_schema = &path_sources_get_schema;
  stream->get_next = &path_sources_get_next;
  stream->get_last_error = &path_sources_last_error;
  stream->release = &path_sources_release;

  PyRegistrySinkOutputs outputs;
  outputs.main_stream = stream;
  outputs.diagnostics = new (std::nothrow) schema_sanitizer_diagnostics();
  if (!outputs.diagnostics) {
    schema_sanitizer_stream_free(stream);
    PyErr_NoMemory();
    return nullptr;
  }
  outputs.registry_json = dup_cstr(state->registry_json);
  outputs.drifts_json = dup_cstr(state->drifts_json);
  outputs.conversion_timestamp = dup_cstr(state->conversion_timestamp);
  if (!outputs.registry_json || !outputs.drifts_json ||
      !outputs.conversion_timestamp) {
    release_registry_outputs(&outputs);
    PyErr_NoMemory();
    return nullptr;
  }

  Py_INCREF(stream_provider_obj);
  state->chunk_provider = stream_provider_obj;
  stream->private_data = state.release();
  return pack_registry_stream_result_with_state(
      ctx_obj, outputs.main_stream, outputs.diagnostics, outputs.registry_json,
      outputs.drifts_json, outputs.conversion_timestamp,
      std::move(registry_plan));
}

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
  if (!ctx)
    return nullptr;
  auto *prepared = unwrap_prepared_options(prepared_obj);
  if (prepared_obj != Py_None && !prepared)
    return nullptr;
  auto base_registry_plan = native_registry_state_from_py(registry_state_obj);
  if (!base_registry_plan) {
    if (!PyErr_Occurred()) {
      PyErr_SetString(PyExc_ValueError,
                      "native registry state does not contain a compiled plan");
    }
    return nullptr;
  }
  if (!provider_has_next_sources(probe_provider_obj) ||
      !provider_has_next_sources(stream_provider_obj)) {
    return nullptr;
  }

  sanitize::PreparedOptionsPtr prepared_options;
  if (prepared_obj == Py_None) {
    auto default_options = default_prepared_options();
    if (!default_options.ok()) {
      PyErr_SetString(PyExc_RuntimeError,
                      default_options.status().ToString().c_str());
      return nullptr;
    }
    prepared_options = std::move(default_options).ValueOrDie();
  } else {
    prepared_options = prepared->prepared;
  }

  char *err = nullptr;
  const int valid = validate_registry_sink_mode(
      schema_mode, base_registry_plan->registry_json.c_str(), &err,
      "context_to_registry_sink_from_path_source_chunk_provider_auto_registry_"
      "state");
  if (valid != SCHEMA_SANITIZER_STATUS_OK) {
    raise_status_error(valid, err);
    return nullptr;
  }

  auto merged_r = merge_path_source_provider_schemas(
      ctx, probe_provider_obj, prepared_options,
      base_registry_plan->registry_json.c_str(),
      field_name_policy ? field_name_policy : "",
      skip_invalid_json_sources != 0, &base_registry_plan->schema);
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

  auto state = std::make_unique<NativePathSourcesStreamState>();
  state->ctx = ctx;
  state->sink_name = sink_name ? sink_name : "stream";
  state->registry_json = registry_plan->registry_json;
  state->drifts_json = registry_plan->drifts_json;
  state->conversion_timestamp = registry_plan->conversion_timestamp;
  state->field_name_policy = field_name_policy ? field_name_policy : "";
  state->schema_mode = schema_mode ? schema_mode : "additive";
  state->prepared = prepared_options;
  state->registry_plan = registry_plan;
  if (!append_first_row_columns_from_dict(first_row_columns,
                                          &state->first_row_columns)) {
    return nullptr;
  }
  if (!append_timestamp_columns_from_sequence(timestamp_columns,
                                              &state->timestamp_columns)) {
    return nullptr;
  }
  append_registry_first_row_columns(&state->first_row_columns,
                                    state->registry_json, state->drifts_json);

  auto *stream = new (std::nothrow) ArrowArrayStream();
  if (!stream) {
    PyErr_NoMemory();
    return nullptr;
  }
  std::memset(stream, 0, sizeof(*stream));
  stream->get_schema = &path_sources_get_schema;
  stream->get_next = &path_sources_get_next;
  stream->get_last_error = &path_sources_last_error;
  stream->release = &path_sources_release;

  PyRegistrySinkOutputs outputs;
  outputs.main_stream = stream;
  outputs.diagnostics = new (std::nothrow) schema_sanitizer_diagnostics();
  if (!outputs.diagnostics) {
    schema_sanitizer_stream_free(stream);
    PyErr_NoMemory();
    return nullptr;
  }
  outputs.registry_json = dup_cstr(state->registry_json);
  outputs.drifts_json = dup_cstr(state->drifts_json);
  outputs.conversion_timestamp = dup_cstr(state->conversion_timestamp);
  if (!outputs.registry_json || !outputs.drifts_json ||
      !outputs.conversion_timestamp) {
    release_registry_outputs(&outputs);
    PyErr_NoMemory();
    return nullptr;
  }

  Py_INCREF(stream_provider_obj);
  state->chunk_provider = stream_provider_obj;
  stream->private_data = state.release();
  return pack_registry_stream_result_with_state(
      ctx_obj, outputs.main_stream, outputs.diagnostics, outputs.registry_json,
      outputs.drifts_json, outputs.conversion_timestamp,
      std::move(registry_plan));
}

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
  auto *prepared = unwrap_prepared_options(prepared_obj);
  if (prepared_obj != Py_None && !prepared)
    return nullptr;

  auto state = std::make_unique<NativePathSourcesStreamState>();
  state->ctx = ctx;
  state->sink_name = sink_name ? sink_name : "stream";
  state->registry_json = registry_json ? registry_json : "{}";
  state->field_name_policy = field_name_policy ? field_name_policy : "";
  state->schema_mode = schema_mode ? schema_mode : "additive";
  if (prepared_obj == Py_None) {
    auto default_options = default_prepared_options();
    if (!default_options.ok()) {
      PyErr_SetString(PyExc_RuntimeError,
                      default_options.status().ToString().c_str());
      return nullptr;
    }
    state->prepared = std::move(default_options).ValueOrDie();
  } else {
    state->prepared = prepared->prepared;
  }
  if (!parse_path_sources(sources_obj, &state->sources)) {
    return nullptr;
  }
  if (!append_first_row_columns_from_dict(first_row_columns,
                                          &state->first_row_columns)) {
    return nullptr;
  }
  if (!append_timestamp_columns_from_sequence(timestamp_columns,
                                              &state->timestamp_columns)) {
    return nullptr;
  }

  char *err = nullptr;
  const int valid = validate_registry_sink_mode(
      schema_mode, registry_json, &err,
      "context_to_registry_sink_from_path_sources_auto_registry");
  if (valid != SCHEMA_SANITIZER_STATUS_OK) {
    raise_status_error(valid, err);
    return nullptr;
  }
  auto merged_r = merge_path_source_schemas(
      ctx, state->sources, state->prepared, state->registry_json.c_str(),
      state->field_name_policy.c_str(), skip_invalid_json_sources != 0);
  if (!merged_r.ok()) {
    raise_status_error(code_for_status(merged_r.status()),
                       dup_cstr(merged_r.status().ToString()));
    return nullptr;
  }
  auto probe = std::move(merged_r).ValueOrDie();
  auto &merged = probe.merged;
  auto plan_r = make_native_registry_plan(std::move(merged));
  if (!plan_r.ok()) {
    raise_status_error(code_for_status(plan_r.status()),
                       dup_cstr(plan_r.status().ToString()));
    return nullptr;
  }
  state->registry_plan = std::move(plan_r).ValueOrDie();
  state->registry_json = state->registry_plan->registry_json;
  state->drifts_json = state->registry_plan->drifts_json;
  state->conversion_timestamp = state->registry_plan->conversion_timestamp;
  append_registry_first_row_columns(&state->first_row_columns,
                                    state->registry_json, state->drifts_json);

  auto *stream = new (std::nothrow) ArrowArrayStream();
  if (!stream) {
    PyErr_NoMemory();
    return nullptr;
  }
  std::memset(stream, 0, sizeof(*stream));
  stream->get_schema = &path_sources_get_schema;
  stream->get_next = &path_sources_get_next;
  stream->get_last_error = &path_sources_last_error;
  stream->release = &path_sources_release;

  PyRegistrySinkOutputs outputs;
  outputs.main_stream = stream;
  outputs.diagnostics = new (std::nothrow) schema_sanitizer_diagnostics();
  if (!outputs.diagnostics) {
    schema_sanitizer_stream_free(stream);
    PyErr_NoMemory();
    return nullptr;
  }
  outputs.registry_json = dup_cstr(state->registry_json);
  outputs.drifts_json = dup_cstr(state->drifts_json);
  outputs.conversion_timestamp = dup_cstr(state->conversion_timestamp);
  if (!outputs.registry_json || !outputs.drifts_json ||
      !outputs.conversion_timestamp) {
    release_registry_outputs(&outputs);
    PyErr_NoMemory();
    return nullptr;
  }
  auto registry_plan = state->registry_plan;
  stream->private_data = state.release();
  return pack_registry_stream_result_with_state(
      ctx_obj, outputs.main_stream, outputs.diagnostics, outputs.registry_json,
      outputs.drifts_json, outputs.conversion_timestamp,
      std::move(registry_plan));
}

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
  auto *prepared = unwrap_prepared_options(prepared_obj);
  if (prepared_obj != Py_None && !prepared)
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
  if (prepared_obj == Py_None) {
    auto default_options = default_prepared_options();
    if (!default_options.ok()) {
      PyErr_SetString(PyExc_RuntimeError,
                      default_options.status().ToString().c_str());
      return nullptr;
    }
    state->prepared = std::move(default_options).ValueOrDie();
  } else {
    state->prepared = prepared->prepared;
  }
  if (!parse_path_sources(sources_obj, &state->sources)) {
    return nullptr;
  }
  if (!append_first_row_columns_from_dict(first_row_columns,
                                          &state->first_row_columns)) {
    return nullptr;
  }
  if (!append_timestamp_columns_from_sequence(timestamp_columns,
                                              &state->timestamp_columns)) {
    return nullptr;
  }

  char *err = nullptr;
  const int valid = validate_registry_sink_mode(
      state->schema_mode.c_str(), state->registry_json.c_str(), &err,
      "context_to_registry_sink_from_path_sources_auto_registry_state");
  if (valid != SCHEMA_SANITIZER_STATUS_OK) {
    raise_status_error(valid, err);
    return nullptr;
  }
  auto merged_r = merge_path_source_schemas(
      ctx, state->sources, state->prepared, state->registry_json.c_str(),
      state->field_name_policy.c_str(), skip_invalid_json_sources != 0,
      &base_registry_plan->schema);
  if (!merged_r.ok()) {
    raise_status_error(code_for_status(merged_r.status()),
                       dup_cstr(merged_r.status().ToString()));
    return nullptr;
  }
  auto probe = std::move(merged_r).ValueOrDie();
  auto &merged = probe.merged;
  auto plan_r = make_native_registry_plan(std::move(merged));
  if (!plan_r.ok()) {
    raise_status_error(code_for_status(plan_r.status()),
                       dup_cstr(plan_r.status().ToString()));
    return nullptr;
  }
  state->registry_plan = std::move(plan_r).ValueOrDie();
  state->registry_json = state->registry_plan->registry_json;
  state->drifts_json = state->registry_plan->drifts_json;
  state->conversion_timestamp = state->registry_plan->conversion_timestamp;
  append_registry_first_row_columns(&state->first_row_columns,
                                    state->registry_json, state->drifts_json);

  auto *stream = new (std::nothrow) ArrowArrayStream();
  if (!stream) {
    PyErr_NoMemory();
    return nullptr;
  }
  std::memset(stream, 0, sizeof(*stream));
  stream->get_schema = &path_sources_get_schema;
  stream->get_next = &path_sources_get_next;
  stream->get_last_error = &path_sources_last_error;
  stream->release = &path_sources_release;

  PyRegistrySinkOutputs outputs;
  outputs.main_stream = stream;
  outputs.diagnostics = new (std::nothrow) schema_sanitizer_diagnostics();
  if (!outputs.diagnostics) {
    schema_sanitizer_stream_free(stream);
    PyErr_NoMemory();
    return nullptr;
  }
  outputs.registry_json = dup_cstr(state->registry_json);
  outputs.drifts_json = dup_cstr(state->drifts_json);
  outputs.conversion_timestamp = dup_cstr(state->conversion_timestamp);
  if (!outputs.registry_json || !outputs.drifts_json ||
      !outputs.conversion_timestamp) {
    release_registry_outputs(&outputs);
    PyErr_NoMemory();
    return nullptr;
  }
  auto registry_plan = state->registry_plan;
  stream->private_data = state.release();
  return pack_registry_stream_result_with_state(
      ctx_obj, outputs.main_stream, outputs.diagnostics, outputs.registry_json,
      outputs.drifts_json, outputs.conversion_timestamp,
      std::move(registry_plan));
}

} // namespace core_abi3_internal
