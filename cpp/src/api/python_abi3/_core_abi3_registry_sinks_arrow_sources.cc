/*
 * Python ABI3 registry-backed multi Arrow-stream sink wrapper.
 *
 * Python still owns Parquet decoding through reusable Arrow C stream factories;
 * this file moves schema-registry merging, per-child native conversion, and
 * metadata stream sequencing into the native layer.
 */
#include "internal/abi/core_abi3_internal.hh"

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
#include "api/python_abi3/_core_abi3_arrow_direct.hh"
#include "api/python_abi3/_core_abi3_metadata_columns.hh"
#include "api/python_abi3/_core_abi3_metadata_stream_builders.hh"
#include "api/python_abi3/_core_abi3_registry_common.hh"
#include "api/python_abi3/_core_abi3_stream_lifecycle.hh"
#include "internal/abi/schema_sanitizer_c_internal.hh"
#include "internal/pipeline/cdata_schema_builder.hh"
#include "internal/pipeline/cdata_stream_utils.hh"
#include "sanitize/registry/registry.hh"

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

struct PassthroughArrowStreamState {
  ArrowArrayStream *inner = nullptr;
  PyObject *stream_obj = nullptr;
  PyObject *stream_capsule = nullptr;
  std::shared_ptr<sanitize::IngestDiagnostics> diagnostics;
  bool closed = false;
};

void release_registry_outputs(PyRegistrySinkOutputs *outputs) {
  release_sink_outputs(outputs->main_stream, outputs->diagnostics);
  schema_sanitizer_free_string(outputs->registry_json);
  schema_sanitizer_free_string(outputs->drifts_json);
  schema_sanitizer_free_string(outputs->conversion_timestamp);
}

void append_u8(std::string &out, std::uint8_t value) {
  out.push_back(static_cast<char>(value));
}

void append_u32(std::string &out, std::uint32_t value) {
  for (int i = 0; i < 4; ++i) {
    out.push_back(static_cast<char>((value >> (8 * i)) & 0xFFu));
  }
}

void append_string(std::string &out, std::string_view value) {
  append_u32(out, static_cast<std::uint32_t>(value.size()));
  out.append(value.data(), value.size());
}

void append_logical_type(std::string &out, const sanitize::LogicalType &type);

void append_logical_field(std::string &out,
                          const sanitize::LogicalField &field) {
  append_string(out, field.name);
  append_u8(out, field.nullable ? 1u : 0u);
  if (field.type) {
    append_logical_type(out, *field.type);
  } else {
    append_u8(out, static_cast<std::uint8_t>(sanitize::LogicalKind::kNull));
  }
}

void append_logical_type(std::string &out, const sanitize::LogicalType &type) {
  append_u8(out, static_cast<std::uint8_t>(type.kind));
  if (type.kind == sanitize::LogicalKind::kStruct) {
    append_u32(out, static_cast<std::uint32_t>(type.fields.size()));
    for (const auto &field : type.fields) {
      append_logical_field(out, field);
    }
  } else if (type.kind == sanitize::LogicalKind::kList) {
    if (type.value) {
      append_logical_type(out, *type.value);
    } else {
      append_u8(out, static_cast<std::uint8_t>(sanitize::LogicalKind::kNull));
    }
  }
}

std::string encode_logical_schema(const sanitize::LogicalSchema &schema) {
  std::string out;
  append_u32(out, static_cast<std::uint32_t>(schema.fields.size()));
  for (const auto &field : schema.fields) {
    append_logical_field(out, field);
  }
  return out;
}

bool dict_set_steal(PyObject *dict, const char *key, PyObject *value) {
  if (!value) {
    return false;
  }
  const int rc = PyDict_SetItemString(dict, key, value);
  Py_DECREF(value);
  return rc == 0;
}

PyObject *pack_registry_probe(const sanitize::SchemaRegistryMergeResult &merged,
                              const sanitize::IngestDiagnostics &diagnostics) {
  const std::string schema_payload = encode_logical_schema(merged.schema);
  PyObject *dict = PyDict_New();
  if (!dict) {
    return nullptr;
  }
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

sanitize::internal::CDataFieldLayout
field_layout_from_logical_field(const sanitize::LogicalField &field) {
  sanitize::internal::CDataFieldLayout layout;
  layout.name = field.name;
  layout.nullable = field.nullable;
  layout.logical_type =
      field.type ? *field.type
                 : sanitize::LogicalType(sanitize::LogicalKind::kNull);
  return layout;
}

std::vector<sanitize::internal::CDataFieldLayout>
field_layouts_from_logical_schema(const sanitize::LogicalSchema &schema) {
  std::vector<sanitize::internal::CDataFieldLayout> fields;
  fields.reserve(schema.fields.size());
  for (const auto &field : schema.fields) {
    fields.push_back(field_layout_from_logical_field(field));
  }
  return fields;
}

bool arrow_schema_node_matches(const ArrowSchema *actual,
                               const ArrowSchema *expected) noexcept {
  if (!actual || !expected || !actual->format || !expected->format) {
    return false;
  }
  const std::string_view actual_format(actual->format);
  const std::string_view expected_format(expected->format);
  if (actual_format != expected_format) {
    return false;
  }
  const std::string_view actual_name(actual->name ? actual->name : "");
  const std::string_view expected_name(expected->name ? expected->name : "");
  if (actual_name != expected_name) {
    return false;
  }
  if (((actual->flags & ARROW_FLAG_NULLABLE) != 0) !=
      ((expected->flags & ARROW_FLAG_NULLABLE) != 0)) {
    return false;
  }
  if ((actual->dictionary != nullptr) != (expected->dictionary != nullptr)) {
    return false;
  }
  if (actual->dictionary &&
      !arrow_schema_node_matches(actual->dictionary, expected->dictionary)) {
    return false;
  }
  if (actual->n_children != expected->n_children) {
    return false;
  }
  for (int64_t i = 0; i < actual->n_children; ++i) {
    if (!actual->children || !expected->children ||
        !arrow_schema_node_matches(actual->children[i],
                                   expected->children[i])) {
      return false;
    }
  }
  return true;
}

sanitize::Result<bool> arrow_stream_schema_matches_registry_plan(
    ArrowArrayStream *stream, const NativeRegistryPlan &plan,
    std::string_view timestamp_precision) {
  if (!stream) {
    return sanitize::Status::Invalid("Arrow passthrough stream is null");
  }

  sanitize::CSchemaGuard actual;
  const int code = stream->get_schema(stream, actual.get());
  if (code != 0) {
    const char *last_error =
        stream->get_last_error ? stream->get_last_error(stream) : nullptr;
    return sanitize::Status::IOError(
        last_error ? last_error : "Arrow passthrough get_schema failed");
  }

  sanitize::CSchemaGuard expected;
  auto fields = field_layouts_from_logical_schema(plan.schema);
  SAN_RETURN_NOT_OK(sanitize::internal::export_fields_as_struct_schema(
      fields, expected.get(), timestamp_precision));
  return arrow_schema_node_matches(actual.get(), expected.get());
}

int passthrough_get_schema(ArrowArrayStream *stream, ArrowSchema *out) {
  if (!stream || !out) {
    return EINVAL;
  }
  auto *state =
      static_cast<PassthroughArrowStreamState *>(stream->private_data);
  if (!state || !state->inner || !state->inner->get_schema) {
    return EINVAL;
  }
  return state->inner->get_schema(state->inner, out);
}

int passthrough_get_next(ArrowArrayStream *stream, ArrowArray *out) {
  if (!stream || !out) {
    return EINVAL;
  }
  auto *state =
      static_cast<PassthroughArrowStreamState *>(stream->private_data);
  if (!state || !state->inner || !state->inner->get_next) {
    return EINVAL;
  }
  const int code = state->inner->get_next(state->inner, out);
  if (code == 0 && out && out->release && state->diagnostics) {
    state->diagnostics->batches += 1;
    state->diagnostics->materialized_rows += out->length;
  }
  return code;
}

const char *passthrough_get_last_error(ArrowArrayStream *stream) {
  if (!stream) {
    return "invalid Arrow passthrough stream";
  }
  auto *state =
      static_cast<PassthroughArrowStreamState *>(stream->private_data);
  if (!state || !state->inner) {
    return "closed Arrow passthrough stream";
  }
  return state->inner->get_last_error
             ? state->inner->get_last_error(state->inner)
             : nullptr;
}

void passthrough_release(ArrowArrayStream *stream) {
  if (!stream || !stream->release) {
    return;
  }
  auto *state =
      static_cast<PassthroughArrowStreamState *>(stream->private_data);
  if (state) {
    close_arrow_stream_keepalive(&state->inner, &state->stream_obj,
                                 &state->stream_capsule, &state->closed);
    delete state;
  }
  sanitize::internal::cdata_stream::clear_stream(stream);
}

sanitize::Result<ArrowArrayStream *> make_passthrough_arrow_stream(
    PyObject *stream_obj, ArrowArrayStream *inner, PyObject *capsule,
    std::shared_ptr<sanitize::IngestDiagnostics> diagnostics) {
  auto *stream = new (std::nothrow) ArrowArrayStream();
  if (!stream) {
    return sanitize::Status::OutOfMemory("Arrow passthrough stream OOM");
  }
  auto *state = new (std::nothrow) PassthroughArrowStreamState();
  if (!state) {
    delete stream;
    return sanitize::Status::OutOfMemory("Arrow passthrough state OOM");
  }
  Py_INCREF(stream_obj);
  state->inner = inner;
  state->stream_obj = stream_obj;
  state->stream_capsule = capsule;
  state->diagnostics = std::move(diagnostics);
  std::memset(stream, 0, sizeof(*stream));
  stream->get_schema = &passthrough_get_schema;
  stream->get_next = &passthrough_get_next;
  stream->get_last_error = &passthrough_get_last_error;
  stream->release = &passthrough_release;
  stream->private_data = state;
  return stream;
}

struct ArrowSourceSpec {
  PyObject *stream_obj = nullptr;
  std::string source_file;
};

struct NativeArrowSourcesStreamState {
  schema_sanitizer_context *ctx = nullptr;
  sanitize::PreparedOptionsPtr prepared;
  std::string registry_json;
  std::string field_name_policy;
  std::string schema_mode;
  PyObject *chunk_provider = nullptr;
  bool chunk_provider_exhausted = false;
  std::vector<ArrowSourceSpec> sources;
  std::vector<MetadataColumn> first_row_columns;
  std::vector<MetadataColumn> timestamp_columns;
  std::shared_ptr<const NativeRegistryPlan> registry_plan;
  std::size_t index = 0;
  bool first_row_pending = true;

  ArrowArrayStream *inner = nullptr;
  schema_sanitizer_diagnostics *diagnostics = nullptr;
  std::unique_ptr<MetadataStreamState> metadata;
  std::string last_error;
};

class GilGuard {
public:
  GilGuard() : state_(PyGILState_Ensure()) {}
  GilGuard(const GilGuard &) = delete;
  GilGuard &operator=(const GilGuard &) = delete;
  ~GilGuard() { PyGILState_Release(state_); }

private:
  PyGILState_STATE state_;
};

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

  std::vector<ArrowSourceSpec> next_sources;
  if (!parse_arrow_sources(result, &next_sources)) {
    Py_DECREF(result);
    return python_arrow_provider_error_status(
        "Arrow-source chunk provider returned invalid sources");
  }
  Py_DECREF(result);
  decref_arrow_sources(&state->sources);
  state->sources = std::move(next_sources);
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

sanitize::Result<sanitize::LogicalSchema>
arrow_source_logical_schema(PyObject *stream_obj,
                            const sanitize::PreparedOptionsPtr &prepared) {
  sanitize::LogicalSchema input_schema;
  auto frontend_r = make_arrow_frontend(
      stream_obj, &input_schema,
      ArrowDirectOptions{.timestamp_precision =
                             prepared->spec.timestamp_precision});
  if (!frontend_r.ok()) {
    return frontend_r.status();
  }
  return input_schema;
}

sanitize::Result<sanitize::SchemaRegistryMergeResult>
merge_arrow_source_schemas(
    schema_sanitizer_context *ctx, const std::vector<ArrowSourceSpec> &sources,
    const sanitize::PreparedOptionsPtr &prepared, const char *registry_json,
    const char *field_name_policy,
    const sanitize::LogicalSchema *previous_schema = nullptr) {
  (void)ctx;
  std::string combined_registry = "{}";
  sanitize::LogicalSchema combined_schema;
  bool has_schema = false;
  for (const ArrowSourceSpec &source : sources) {
    SAN_ASSIGN_OR_RAISE(auto input_schema, arrow_source_logical_schema(
                                               source.stream_obj, prepared));
    auto merged = sanitize::merge_schema_registry(make_registry_merge_input(
        std::move(input_schema), combined_registry.c_str(), field_name_policy,
        prepared->spec.default_key_name, prepared->spec.field_order));
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
  auto merge_input = make_registry_merge_input(std::move(combined_schema),
                                               registry_json, field_name_policy,
                                               prepared->spec.default_key_name,
                                               prepared->spec.field_order);
  return previous_schema ? sanitize::merge_schema_registry_with_previous_schema(
                               merge_input, *previous_schema)
                         : sanitize::merge_schema_registry(merge_input);
}

sanitize::Result<sanitize::SchemaRegistryMergeResult>
merge_arrow_source_provider_schemas(
    schema_sanitizer_context *ctx, PyObject *provider_obj,
    const sanitize::PreparedOptionsPtr &prepared, const char *registry_json,
    const char *field_name_policy,
    const sanitize::LogicalSchema *previous_schema = nullptr) {
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

std::vector<MetadataColumn>
metadata_columns_for_child(const NativeArrowSourcesStreamState *state,
                           const ArrowSourceSpec &source) {
  return registry_child_metadata_columns(
      state->first_row_columns, state->timestamp_columns,
      state->first_row_pending, source.source_file,
      /*include_source_file=*/true);
}

void close_current_source(NativeArrowSourcesStreamState *state) noexcept {
  if (!state) {
    return;
  }
  state->metadata.reset();
  release_sink_outputs(state->inner, state->diagnostics);
  state->inner = nullptr;
  state->diagnostics = nullptr;
}

sanitize::Status
finish_opened_source_metadata(NativeArrowSourcesStreamState *state,
                              const ArrowSourceSpec &source) {
  if (!state || !state->inner) {
    return sanitize::Status::Invalid("native Arrow source stream is null");
  }
  state->metadata = std::make_unique<MetadataStreamState>();
  state->metadata->inner = state->inner;
  state->metadata->columns = metadata_columns_for_child(state, source);
  state->metadata->first_row_pending = state->first_row_pending;
  return sanitize::Status::OK();
}

sanitize::Result<bool>
try_open_passthrough_arrow_source(NativeArrowSourcesStreamState *state,
                                  const ArrowSourceSpec &source) {
  if (!state || !state->registry_plan) {
    return false;
  }

  PyObject *capsule = nullptr;
  ArrowArrayStream *inner = nullptr;
  {
    GilGuard gil;
    if (!acquire_arrow_stream(source.stream_obj, &capsule, &inner)) {
      PyErr_Clear();
      return false;
    }
  }

  auto compatible = arrow_stream_schema_matches_registry_plan(
      inner, *state->registry_plan, state->prepared->spec.timestamp_precision);
  if (!compatible.ok()) {
    GilGuard gil;
    Py_DECREF(capsule);
    return compatible.status();
  }
  if (!compatible.ValueOrDie()) {
    GilGuard gil;
    Py_DECREF(capsule);
    return false;
  }

  auto *diagnostics = new (std::nothrow) schema_sanitizer_diagnostics();
  if (!diagnostics) {
    GilGuard gil;
    Py_DECREF(capsule);
    return sanitize::Status::OutOfMemory(
        "context_to_registry_sink_arrow_sources: diagnostics allocation "
        "failed");
  }
  auto diag_shared = std::make_shared<sanitize::IngestDiagnostics>();
  diag_shared->arrow_schema_depth =
      sanitize::arrow_schema_depth(state->registry_plan->schema);
  diag_shared->parquet_schema_depth =
      sanitize::parquet_schema_depth(state->registry_plan->schema);
  diag_shared->direct_arrow_input = 1;

  auto proxy = make_passthrough_arrow_stream(source.stream_obj, inner, capsule,
                                             diag_shared);
  if (!proxy.ok()) {
    delete diagnostics;
    GilGuard gil;
    Py_DECREF(capsule);
    return proxy.status();
  }
  diagnostics->diagnostics = std::move(diag_shared);

  state->inner = *proxy;
  state->diagnostics = diagnostics;
  return true;
}

sanitize::Result<sanitize::IngestStream>
ingest_arrow_source_with_registry_plan(NativeArrowSourcesStreamState *state,
                                       sanitize::FrontendHandle frontend) {
  if (!state || !state->registry_plan) {
    return sanitize::Status::Invalid("native Arrow registry plan is null");
  }
  frontend.set_plan(state->registry_plan->plan.get());

  auto diagnostics = std::make_shared<sanitize::IngestDiagnostics>();
  diagnostics->arrow_schema_depth =
      sanitize::arrow_schema_depth(state->registry_plan->schema);
  diagnostics->parquet_schema_depth =
      sanitize::parquet_schema_depth(state->registry_plan->schema);
  diagnostics->direct_arrow_input = 1;

  sanitize::PreparedIngest prepared;
  prepared.frontend_name = "arrow";
  prepared.frontend = std::move(frontend);
  prepared.owned_ctx = state->ctx ? state->ctx->ctx : nullptr;
  prepared.ctx = prepared.owned_ctx.get();
  prepared.plan = state->registry_plan->plan;
  prepared.opts = state->prepared;
  prepared.diagnostics = std::move(diagnostics);
  prepared.logical_schema = state->registry_plan->schema;
  prepared.inference_consumed = false;
  if (!prepared.ctx) {
    return sanitize::Status::Invalid(
        "native Arrow registry plan source has no execution context");
  }
  return sanitize::ingest_to_stream(std::move(prepared));
}

sanitize::Status open_next_source(NativeArrowSourcesStreamState *state) {
  if (!state) {
    return sanitize::Status::Invalid("native Arrow sources stream is closed");
  }
  close_current_source(state);
  if (state->index >= state->sources.size()) {
    if (state->chunk_provider && !state->chunk_provider_exhausted) {
      SAN_RETURN_NOT_OK(load_next_arrow_provider_chunk(state));
    }
  }
  if (state->index >= state->sources.size()) {
    return sanitize::Status::OK();
  }
  const ArrowSourceSpec &source = state->sources[state->index++];

  SAN_ASSIGN_OR_RAISE(bool passthrough,
                      try_open_passthrough_arrow_source(state, source));
  if (passthrough) {
    return finish_opened_source_metadata(state, source);
  }

  sanitize::LogicalSchema input_schema;
  sanitize::Result<sanitize::FrontendHandle> frontend_r =
      sanitize::Status::Invalid("native Arrow source was not opened");
  {
    GilGuard gil;
    frontend_r = make_arrow_frontend(
        source.stream_obj, &input_schema,
        ArrowDirectOptions{.timestamp_precision =
                               state->prepared->spec.timestamp_precision});
  }
  SAN_ASSIGN_OR_RAISE(auto frontend, std::move(frontend_r));
  sanitize::Result<sanitize::IngestStream> out_r =
      sanitize::Status::Invalid("native Arrow source was not ingested");
  if (state->registry_plan) {
    out_r = ingest_arrow_source_with_registry_plan(state, std::move(frontend));
  } else {
    auto merged_r = sanitize::merge_schema_registry(make_registry_merge_input(
        std::move(input_schema), state->registry_json.c_str(),
        state->field_name_policy.c_str(),
        state->prepared->spec.default_key_name,
        state->prepared->spec.field_order));
    if (!merged_r.ok()) {
      return merged_r.status();
    }
    auto merged = std::move(merged_r).ValueOrDie();
    out_r = ingest_direct_arrow_stream(std::move(frontend),
                                       std::move(merged.schema),
                                       state->prepared, state->ctx->ctx);
  }
  if (!out_r.ok()) {
    return out_r.status();
  }

  SinkOutputs outputs{.stream = &state->inner,
                      .diagnostics = &state->diagnostics};
  char *err = nullptr;
  const int rc =
      ingest_stream_to_streams(std::move(out_r).ValueOrDie(), outputs, &err,
                               "context_to_registry_sink_arrow_sources");
  if (rc != SCHEMA_SANITIZER_STATUS_OK) {
    std::string message = err ? err : "native Arrow source failed";
    schema_sanitizer_free_string(err);
    return sanitize::Status::Invalid(message);
  }
  return finish_opened_source_metadata(state, source);
}

sanitize::Status arrow_sources_open_next(void *state) {
  return open_next_source(static_cast<NativeArrowSourcesStreamState *>(state));
}

void arrow_sources_close_current(void *state) noexcept {
  close_current_source(static_cast<NativeArrowSourcesStreamState *>(state));
}

MetadataStreamState *arrow_sources_metadata(void *state) noexcept {
  auto *typed = static_cast<NativeArrowSourcesStreamState *>(state);
  return typed && typed->metadata ? typed->metadata.get() : nullptr;
}

std::string &arrow_sources_error(void *state) noexcept {
  return static_cast<NativeArrowSourcesStreamState *>(state)->last_error;
}

bool *arrow_sources_first_row_pending(void *state) noexcept {
  return &static_cast<NativeArrowSourcesStreamState *>(state)
              ->first_row_pending;
}

void arrow_sources_destroy_state(void *state) noexcept {
  auto *typed = static_cast<NativeArrowSourcesStreamState *>(state);
  if (typed) {
    close_arrow_chunk_provider(typed);
    decref_arrow_sources(&typed->sources);
  }
  delete typed;
}

const NativeMultiSourceStreamOps kArrowSourcesOps{
    .schema_context = "arrow_sources.get_schema",
    .next_context = "arrow_sources.get_next",
    .empty_message = "native Arrow sources stream has no sources",
    .invalid_stream_message = "invalid native Arrow sources stream",
    .open_next = &arrow_sources_open_next,
    .close_current = &arrow_sources_close_current,
    .metadata = &arrow_sources_metadata,
    .last_error = &arrow_sources_error,
    .first_row_pending = &arrow_sources_first_row_pending,
    .destroy_state = &arrow_sources_destroy_state,
};

const char *arrow_sources_last_error(ArrowArrayStream *stream) {
  return native_multi_source_last_error(stream, kArrowSourcesOps);
}

void arrow_sources_release(ArrowArrayStream *stream) {
  native_multi_source_release(stream, kArrowSourcesOps);
}

int arrow_sources_get_schema(ArrowArrayStream *stream, ArrowSchema *out) {
  return native_multi_source_get_schema(stream, out, kArrowSourcesOps);
}

int arrow_sources_get_next(ArrowArrayStream *stream, ArrowArray *out) {
  return native_multi_source_get_next(stream, out, kArrowSourcesOps);
}

} // namespace

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
  if (!sink_name || std::string(sink_name) != "stream") {
    PyErr_SetString(
        PyExc_ValueError,
        "Arrow sources registry sink currently requires sink='stream'");
    return nullptr;
  }
  auto *ctx = unwrap_context(ctx_obj);
  if (!ctx) {
    return nullptr;
  }
  auto *prepared = unwrap_prepared_options(prepared_obj);
  if (prepared_obj != Py_None && !prepared) {
    return nullptr;
  }

  auto state = std::make_unique<NativeArrowSourcesStreamState>();
  state->ctx = ctx;
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
  if (!parse_arrow_sources(sources_obj, &state->sources)) {
    return nullptr;
  }
  if (!append_first_row_columns_from_dict(first_row_columns,
                                          &state->first_row_columns)) {
    decref_arrow_sources(&state->sources);
    return nullptr;
  }
  if (!append_timestamp_columns_from_sequence(timestamp_columns,
                                              &state->timestamp_columns)) {
    decref_arrow_sources(&state->sources);
    return nullptr;
  }

  char *err = nullptr;
  const int valid =
      validate_registry_sink_mode(schema_mode, registry_json, &err,
                                  "context_to_registry_sink_arrow_sources");
  if (valid != SCHEMA_SANITIZER_STATUS_OK) {
    decref_arrow_sources(&state->sources);
    raise_status_error(valid, err);
    return nullptr;
  }

  auto merged_r = merge_arrow_source_schemas(
      ctx, state->sources, state->prepared, state->registry_json.c_str(),
      state->field_name_policy.c_str());
  if (!merged_r.ok()) {
    decref_arrow_sources(&state->sources);
    raise_status_error(code_for_status(merged_r.status()),
                       dup_cstr(merged_r.status().ToString()));
    return nullptr;
  }
  auto merged = std::move(merged_r).ValueOrDie();
  auto plan_r = make_native_registry_plan(std::move(merged));
  if (!plan_r.ok()) {
    decref_arrow_sources(&state->sources);
    raise_status_error(code_for_status(plan_r.status()),
                       dup_cstr(plan_r.status().ToString()));
    return nullptr;
  }
  state->registry_plan = std::move(plan_r).ValueOrDie();
  state->registry_json = state->registry_plan->registry_json;
  append_registry_first_row_columns(&state->first_row_columns,
                                    state->registry_json,
                                    state->registry_plan->drifts_json);

  auto *stream = new (std::nothrow) ArrowArrayStream();
  if (!stream) {
    decref_arrow_sources(&state->sources);
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
    PyErr_NoMemory();
    return nullptr;
  }
  outputs.registry_json = dup_cstr(state->registry_plan->registry_json);
  outputs.drifts_json = dup_cstr(state->registry_plan->drifts_json);
  outputs.conversion_timestamp =
      dup_cstr(state->registry_plan->conversion_timestamp);
  if (!outputs.registry_json || !outputs.drifts_json ||
      !outputs.conversion_timestamp) {
    release_registry_outputs(&outputs);
    PyErr_NoMemory();
    return nullptr;
  }
  auto registry_plan = state->registry_plan;
  stream->private_data = state.release();
  return pack_registry_stream_result_with_state(
      sources_obj, outputs.main_stream, outputs.diagnostics,
      outputs.registry_json, outputs.drifts_json, outputs.conversion_timestamp,
      std::move(registry_plan));
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
  if (!sink_name || std::string(sink_name) != "stream") {
    PyErr_SetString(
        PyExc_ValueError,
        "Arrow sources registry sink currently requires sink='stream'");
    return nullptr;
  }
  auto *ctx = unwrap_context(ctx_obj);
  if (!ctx) {
    return nullptr;
  }
  auto *prepared = unwrap_prepared_options(prepared_obj);
  if (prepared_obj != Py_None && !prepared) {
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
  if (!parse_arrow_sources(sources_obj, &state->sources)) {
    return nullptr;
  }
  if (!append_first_row_columns_from_dict(first_row_columns,
                                          &state->first_row_columns)) {
    decref_arrow_sources(&state->sources);
    return nullptr;
  }
  if (!append_timestamp_columns_from_sequence(timestamp_columns,
                                              &state->timestamp_columns)) {
    decref_arrow_sources(&state->sources);
    return nullptr;
  }

  auto *stream = new (std::nothrow) ArrowArrayStream();
  if (!stream) {
    decref_arrow_sources(&state->sources);
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
  if (!sink_name || std::string(sink_name) != "stream") {
    PyErr_SetString(
        PyExc_ValueError,
        "Arrow sources registry sink currently requires sink='stream'");
    return nullptr;
  }
  auto *ctx = unwrap_context(ctx_obj);
  if (!ctx) {
    return nullptr;
  }
  auto *prepared = unwrap_prepared_options(prepared_obj);
  if (prepared_obj != Py_None && !prepared) {
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
  if (!parse_arrow_sources(sources_obj, &state->sources)) {
    return nullptr;
  }
  if (!append_first_row_columns_from_dict(first_row_columns,
                                          &state->first_row_columns)) {
    decref_arrow_sources(&state->sources);
    return nullptr;
  }
  if (!append_timestamp_columns_from_sequence(timestamp_columns,
                                              &state->timestamp_columns)) {
    decref_arrow_sources(&state->sources);
    return nullptr;
  }

  char *err = nullptr;
  const int valid = validate_registry_sink_mode(
      state->schema_mode.c_str(), state->registry_json.c_str(), &err,
      "context_to_registry_sink_arrow_sources_auto_registry_state");
  if (valid != SCHEMA_SANITIZER_STATUS_OK) {
    decref_arrow_sources(&state->sources);
    raise_status_error(valid, err);
    return nullptr;
  }

  auto merged_r = merge_arrow_source_schemas(
      ctx, state->sources, state->prepared, state->registry_json.c_str(),
      state->field_name_policy.c_str(), &base_registry_plan->schema);
  if (!merged_r.ok()) {
    decref_arrow_sources(&state->sources);
    raise_status_error(code_for_status(merged_r.status()),
                       dup_cstr(merged_r.status().ToString()));
    return nullptr;
  }
  auto merged = std::move(merged_r).ValueOrDie();
  auto plan_r = make_native_registry_plan(std::move(merged));
  if (!plan_r.ok()) {
    decref_arrow_sources(&state->sources);
    raise_status_error(code_for_status(plan_r.status()),
                       dup_cstr(plan_r.status().ToString()));
    return nullptr;
  }
  state->registry_plan = std::move(plan_r).ValueOrDie();
  state->registry_json = state->registry_plan->registry_json;
  append_registry_first_row_columns(&state->first_row_columns,
                                    state->registry_json,
                                    state->registry_plan->drifts_json);

  auto *stream = new (std::nothrow) ArrowArrayStream();
  if (!stream) {
    decref_arrow_sources(&state->sources);
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
    PyErr_NoMemory();
    return nullptr;
  }
  outputs.registry_json = dup_cstr(state->registry_plan->registry_json);
  outputs.drifts_json = dup_cstr(state->registry_plan->drifts_json);
  outputs.conversion_timestamp =
      dup_cstr(state->registry_plan->conversion_timestamp);
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
  if (!sink_name || std::string(sink_name) != "stream") {
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
  auto *prepared = unwrap_prepared_options(prepared_obj);
  if (prepared_obj != Py_None && !prepared) {
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
  if (!sink_name || std::string(sink_name) != "stream") {
    PyErr_SetString(
        PyExc_ValueError,
        "Arrow-source chunk provider currently requires sink='stream'");
    return nullptr;
  }
  auto *ctx = unwrap_context(ctx_obj);
  if (!ctx) {
    return nullptr;
  }
  auto *prepared = unwrap_prepared_options(prepared_obj);
  if (prepared_obj != Py_None && !prepared) {
    return nullptr;
  }
  if (!arrow_provider_has_next_sources(probe_provider_obj) ||
      !arrow_provider_has_next_sources(stream_provider_obj)) {
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

  auto state = std::make_unique<NativeArrowSourcesStreamState>();
  state->ctx = ctx;
  state->registry_json = registry_plan->registry_json;
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
                                    registry_plan->registry_json,
                                    registry_plan->drifts_json);

  auto *stream = new (std::nothrow) ArrowArrayStream();
  if (!stream) {
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

  Py_INCREF(stream_provider_obj);
  state->chunk_provider = stream_provider_obj;
  stream->private_data = state.release();
  return pack_registry_stream_result_with_state(
      ctx_obj, outputs.main_stream, outputs.diagnostics, outputs.registry_json,
      outputs.drifts_json, outputs.conversion_timestamp,
      std::move(registry_plan));
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
  if (!sink_name || std::string(sink_name) != "stream") {
    PyErr_SetString(
        PyExc_ValueError,
        "Arrow-source chunk provider currently requires sink='stream'");
    return nullptr;
  }
  auto *ctx = unwrap_context(ctx_obj);
  if (!ctx) {
    return nullptr;
  }
  auto *prepared = unwrap_prepared_options(prepared_obj);
  if (prepared_obj != Py_None && !prepared) {
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

  auto state = std::make_unique<NativeArrowSourcesStreamState>();
  state->ctx = ctx;
  state->registry_json = registry_plan->registry_json;
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
                                    registry_plan->registry_json,
                                    registry_plan->drifts_json);

  auto *stream = new (std::nothrow) ArrowArrayStream();
  if (!stream) {
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

  Py_INCREF(stream_provider_obj);
  state->chunk_provider = stream_provider_obj;
  stream->private_data = state.release();
  return pack_registry_stream_result_with_state(
      ctx_obj, outputs.main_stream, outputs.diagnostics, outputs.registry_json,
      outputs.drifts_json, outputs.conversion_timestamp,
      std::move(registry_plan));
}

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
  if (prepared_obj == Py_None) {
    auto default_options = default_prepared_options();
    if (!default_options.ok()) {
      PyErr_SetString(PyExc_RuntimeError,
                      default_options.status().ToString().c_str());
      return nullptr;
    }
    prepared = std::move(default_options).ValueOrDie();
  } else {
    auto *prepared_capsule = unwrap_prepared_options(prepared_obj);
    if (!prepared_capsule) {
      return nullptr;
    }
    prepared = prepared_capsule->prepared;
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
  if (prepared_obj == Py_None) {
    auto default_options = default_prepared_options();
    if (!default_options.ok()) {
      PyErr_SetString(PyExc_RuntimeError,
                      default_options.status().ToString().c_str());
      return nullptr;
    }
    prepared = std::move(default_options).ValueOrDie();
  } else {
    auto *prepared_capsule = unwrap_prepared_options(prepared_obj);
    if (!prepared_capsule) {
      return nullptr;
    }
    prepared = prepared_capsule->prepared;
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
