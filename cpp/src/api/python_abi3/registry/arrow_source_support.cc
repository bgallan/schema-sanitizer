/* Arrow-source registry support: result packing, schema checks, and passthrough
 * streams. */
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
  const std::string schema_payload =
      sanitize::internal::options_io::serialize_logical_schema_bytes(
          merged.schema);
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

} // namespace core_abi3_internal::arrow_registry_detail
