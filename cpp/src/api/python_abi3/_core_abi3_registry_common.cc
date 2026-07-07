// Implements shared helpers for native registry-backed multi-source streams.

#include "api/python_abi3/_core_abi3_registry_common.hh"

#include <cerrno>
#include <cstring>
#include <memory>
#include <new>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "api/c/schema_sanitizer_c_sink_internal.hh"
#include "internal/abi/core_abi3_internal.hh"
#include "internal/abi/schema_sanitizer_c_internal.hh"
#include "internal/pipeline/cdata_stream_utils.hh"
#include "internal/planning/plan_compile.hh"
#include "sanitize/schema_registry/schema_registry.hh"

namespace core_abi3_internal {
namespace {

constexpr const char *kNativeRegistryStateCapsuleName =
    "schema_sanitizer.native_registry_state";

struct NativeRegistryStateCapsule {
  std::shared_ptr<const NativeRegistryPlan> plan;
};

void native_registry_state_capsule_destructor(PyObject *capsule) {
  auto *state = static_cast<NativeRegistryStateCapsule *>(
      PyCapsule_GetPointer(capsule, kNativeRegistryStateCapsuleName));
  if (!state) {
    PyErr_Clear();
    return;
  }
  delete state;
}

bool logical_type_is_utf8(const sanitize::LogicalType &type) {
  return type.kind == sanitize::LogicalKind::kUtf8;
}

sanitize::LogicalSchema
schema_with_generated_source_file(const sanitize::LogicalSchema &schema) {
  sanitize::LogicalSchema out = schema;
  for (auto &field : out.fields) {
    if (field.name == "source_file") {
      if (!field.type || !logical_type_is_utf8(*field.type)) {
        field.type = std::make_unique<sanitize::LogicalType>(
            sanitize::LogicalType::Utf8());
      }
      field.nullable = true;
      return out;
    }
  }
  sanitize::LogicalField field;
  field.name = "source_file";
  field.type =
      std::make_unique<sanitize::LogicalType>(sanitize::LogicalType::Utf8());
  field.nullable = true;
  out.fields.push_back(std::move(field));
  return out;
}

} // namespace

sanitize::Result<std::shared_ptr<NativeRegistryPlan>>
make_native_registry_plan(sanitize::SchemaRegistryMergeResult merged) {
  SAN_ASSIGN_OR_RAISE(auto compiled, sanitize::compile_plan(merged.schema));
  auto out = std::make_shared<NativeRegistryPlan>();
  out->schema = std::move(merged.schema);
  out->plan = std::make_shared<sanitize::CompiledPlan>(std::move(compiled));
  out->registry_json = std::move(merged.registry_json);
  out->drifts_json = std::move(merged.drifts_json);
  out->conversion_timestamp = std::move(merged.detected_at);
  return out;
}

sanitize::Result<std::shared_ptr<NativeRegistryPlan>>
make_native_registry_plan_with_generated_source_file(
    const NativeRegistryPlan &base) {
  sanitize::SchemaRegistryMergeResult merged;
  merged.schema = schema_with_generated_source_file(base.schema);
  merged.registry_json = base.registry_json;
  merged.drifts_json = base.drifts_json;
  merged.detected_at = base.conversion_timestamp;
  return make_native_registry_plan(std::move(merged));
}

sanitize::Result<std::shared_ptr<NativeRegistryPlan>>
make_native_registry_plan_from_json(
    const sanitize::PreparedOptionsPtr &prepared, const char *registry_json,
    const char *field_name_policy, const char *drifts_json,
    const char *conversion_timestamp) {
  if (!prepared) {
    return sanitize::Status::Invalid("prepared options are null");
  }
  SAN_ASSIGN_OR_RAISE(auto has_schema,
                      sanitize::schema_registry_has_canonical_schema(
                          registry_json ? registry_json : ""));
  if (!has_schema) {
    return nullptr;
  }

  auto merged_r = sanitize::merge_schema_registry(make_registry_merge_input(
      sanitize::LogicalSchema{}, registry_json, field_name_policy,
      prepared->spec.default_key_name, prepared->spec.field_order));
  if (!merged_r.ok()) {
    return merged_r.status();
  }
  auto merged = std::move(merged_r).ValueOrDie();
  merged.registry_json = registry_json ? registry_json : "{}";
  merged.drifts_json = drifts_json ? drifts_json : "[]";
  merged.detected_at = conversion_timestamp ? conversion_timestamp : "";
  return make_native_registry_plan(std::move(merged));
}

PyObject *
wrap_native_registry_state(std::shared_ptr<const NativeRegistryPlan> plan) {
  if (!plan) {
    Py_RETURN_NONE;
  }
  auto *state = new (std::nothrow) NativeRegistryStateCapsule();
  if (!state) {
    PyErr_NoMemory();
    return nullptr;
  }
  state->plan = std::move(plan);
  PyObject *capsule =
      PyCapsule_New(static_cast<void *>(state), kNativeRegistryStateCapsuleName,
                    native_registry_state_capsule_destructor);
  if (!capsule) {
    delete state;
    return nullptr;
  }
  return capsule;
}

std::shared_ptr<const NativeRegistryPlan>
native_registry_state_from_py(PyObject *obj) {
  if (!obj || obj == Py_None) {
    return nullptr;
  }
  if (!PyCapsule_CheckExact(obj)) {
    PyErr_SetString(PyExc_TypeError,
                    "native registry state must be a registry-state capsule");
    return nullptr;
  }
  auto *state = static_cast<NativeRegistryStateCapsule *>(
      PyCapsule_GetPointer(obj, kNativeRegistryStateCapsuleName));
  if (!state) {
    return nullptr;
  }
  return state->plan;
}

PyObject *py_registry_state_from_json(PyObject *, PyObject *args) {
  PyObject *prepared_obj = Py_None;
  const char *registry_json = nullptr;
  const char *field_name_policy = nullptr;
  const char *drifts_json = "[]";
  const char *conversion_timestamp = "";
  if (!PyArg_ParseTuple(args, "Oss|ss:registry_state_from_json", &prepared_obj,
                        &registry_json, &field_name_policy, &drifts_json,
                        &conversion_timestamp)) {
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
    auto *prepared = unwrap_prepared_options(prepared_obj);
    if (!prepared) {
      return nullptr;
    }
    prepared_options = prepared->prepared;
  }

  auto plan_r = make_native_registry_plan_from_json(
      prepared_options, registry_json, field_name_policy, drifts_json,
      conversion_timestamp);
  if (!plan_r.ok()) {
    PyErr_SetString(PyExc_ValueError, plan_r.status().ToString().c_str());
    return nullptr;
  }
  return wrap_native_registry_state(std::move(plan_r).ValueOrDie());
}

PyObject *pack_registry_stream_result_with_state(
    PyObject *keepalive, ArrowArrayStream *main_stream,
    schema_sanitizer_diagnostics *diagnostics, char *registry_json,
    char *drifts_json, char *conversion_timestamp,
    std::shared_ptr<const NativeRegistryPlan> registry_plan) {
  PyObject *base = pack_registry_stream_result(
      keepalive, main_stream, diagnostics, registry_json, drifts_json,
      conversion_timestamp);
  if (!base) {
    return nullptr;
  }
  PyObject *state = wrap_native_registry_state(std::move(registry_plan));
  if (!state) {
    Py_DECREF(base);
    return nullptr;
  }
  PyObject *out = PyTuple_New(6);
  if (!out) {
    Py_DECREF(state);
    Py_DECREF(base);
    return nullptr;
  }
  for (Py_ssize_t i = 0; i < 5; ++i) {
    PyObject *item = PyTuple_GetItem(base, i);
    if (!item) {
      Py_DECREF(state);
      Py_DECREF(out);
      Py_DECREF(base);
      return nullptr;
    }
    Py_INCREF(item);
    if (!tuple_set_item_steal(out, i, item)) {
      Py_DECREF(state);
      Py_DECREF(out);
      Py_DECREF(base);
      return nullptr;
    }
  }
  if (!tuple_set_item_steal(out, 5, state)) {
    Py_DECREF(out);
    Py_DECREF(base);
    return nullptr;
  }
  Py_DECREF(base);
  return out;
}

void append_registry_first_row_columns(std::vector<MetadataColumn> *columns,
                                       const std::string &registry_json,
                                       const std::string &drifts_json) {
  if (!columns) {
    return;
  }

  MetadataColumn registry;
  registry.name = "schema_registry";
  registry.value = registry_json;
  registry.placement = MetadataColumnPlacement::FirstRowUtf8;
  columns->push_back(std::move(registry));

  MetadataColumn drifts;
  drifts.name = "schema_drifts";
  drifts.value = drifts_json;
  drifts.placement = MetadataColumnPlacement::FirstRowUtf8;
  columns->push_back(std::move(drifts));
}

std::vector<MetadataColumn> registry_child_metadata_columns(
    const std::vector<MetadataColumn> &first_row_columns,
    const std::vector<MetadataColumn> &timestamp_columns,
    bool first_row_pending, std::string_view source_file,
    bool include_source_file) {
  std::vector<MetadataColumn> columns;
  columns.reserve(first_row_columns.size() + timestamp_columns.size() +
                  (include_source_file ? 1 : 0));

  for (auto column : first_row_columns) {
    if (!first_row_pending) {
      column.value.clear();
      column.is_null = true;
    }
    columns.push_back(std::move(column));
  }

  if (include_source_file) {
    MetadataColumn source_column;
    source_column.name = "source_file";
    source_column.value = std::string(source_file);
    source_column.placement = MetadataColumnPlacement::AllRowsUtf8;
    columns.push_back(std::move(source_column));
  }

  for (auto column : timestamp_columns) {
    columns.push_back(std::move(column));
  }
  return columns;
}

const char *
native_multi_source_last_error(ArrowArrayStream *stream,
                               const NativeMultiSourceStreamOps &ops) {
  if (!stream) {
    return ops.invalid_stream_message ? ops.invalid_stream_message
                                      : "invalid native multi-source stream";
  }
  if (!stream->private_data || !ops.last_error) {
    return nullptr;
  }
  return sanitize::internal::cdata_stream::last_error_ptr(
      ops.last_error(stream->private_data));
}

void native_multi_source_release(ArrowArrayStream *stream,
                                 const NativeMultiSourceStreamOps &ops) {
  if (!stream || !stream->release) {
    return;
  }
  void *state = stream->private_data;
  if (state && ops.close_current) {
    ops.close_current(state);
  }
  if (state && ops.destroy_state) {
    ops.destroy_state(state);
  }
  sanitize::internal::cdata_stream::clear_stream(stream);
}

int native_multi_source_get_schema(ArrowArrayStream *stream, ArrowSchema *out,
                                   const NativeMultiSourceStreamOps &ops) {
  if (!stream || !out || !stream->private_data || !ops.last_error ||
      !ops.open_next || !ops.metadata) {
    return EINVAL;
  }
  void *state = stream->private_data;
  return sanitize::internal::cdata_stream::run_schema_callback(
      out, ops.last_error(state),
      ops.schema_context ? ops.schema_context : "multi_source.get_schema",
      [&](ArrowSchema *schema) {
        if (!ops.metadata(state)) {
          SAN_RETURN_NOT_OK(ops.open_next(state));
        }
        MetadataStreamState *metadata = ops.metadata(state);
        if (!metadata) {
          return sanitize::Status::Invalid(
              ops.empty_message ? ops.empty_message
                                : "native multi-source stream has no sources");
        }
        return build_metadata_schema(metadata, schema);
      });
}

int native_multi_source_get_next(ArrowArrayStream *stream, ArrowArray *out,
                                 const NativeMultiSourceStreamOps &ops) {
  if (!stream || !out || !stream->private_data || !ops.last_error ||
      !ops.open_next || !ops.metadata || !ops.close_current ||
      !ops.first_row_pending) {
    return EINVAL;
  }
  void *state = stream->private_data;
  return sanitize::internal::cdata_stream::run_array_callback(
      out, ops.last_error(state),
      ops.next_context ? ops.next_context : "multi_source.get_next",
      [&](ArrowArray *array) -> sanitize::Status {
        for (;;) {
          MetadataStreamState *metadata = ops.metadata(state);
          if (!metadata) {
            SAN_RETURN_NOT_OK(ops.open_next(state));
            metadata = ops.metadata(state);
            if (!metadata) {
              std::memset(array, 0, sizeof(*array));
              return sanitize::Status::OK();
            }
          }
          SAN_RETURN_NOT_OK(build_metadata_array(metadata, array));
          *ops.first_row_pending(state) = metadata->first_row_pending;
          if (array->release) {
            return sanitize::Status::OK();
          }
          ops.close_current(state);
        }
      });
}

} // namespace core_abi3_internal
