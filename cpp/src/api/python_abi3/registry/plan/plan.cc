// Native registry plan construction and Python capsule ownership.

#include "api/python_abi3/registry/plan/plan.hh"

#include <algorithm>
#include <new>
#include <utility>

#include "internal/abi/python_abi3/capsules.hh"
#include "internal/abi/schema_sanitizer_c_internal.hh"
#include "internal/planning/plan_compile.hh"
#include "internal/planning/schema_evolution.hh"
#include "schema_registry/schema_registry_internal.hh"

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

sanitize::LogicalSchema
schema_with_generated_source_file(const sanitize::LogicalSchema &schema) {
  sanitize::LogicalSchema out = schema;
  auto field = std::ranges::find_if(
      out.fields, [](const sanitize::LogicalField &candidate) {
        return candidate.name == "source_file";
      });
  if (field != out.fields.end()) {
    if (!field->type || field->type->kind != sanitize::LogicalKind::kUtf8) {
      field->type = std::make_unique<sanitize::LogicalType>(
          sanitize::LogicalType::Utf8());
    }
    field->nullable = true;
    return out;
  }

  sanitize::LogicalField generated;
  generated.name = "source_file";
  generated.type =
      std::make_unique<sanitize::LogicalType>(sanitize::LogicalType::Utf8());
  generated.nullable = true;
  out.fields.push_back(std::move(generated));
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
    const sanitize::PreparedOptionsPtr &prepared,
    std::string_view registry_json, std::string_view drifts_json,
    std::string_view conversion_timestamp) {
  if (!prepared) {
    return sanitize::Status::Invalid("prepared options are null");
  }
  SAN_ASSIGN_OR_RAISE(
      auto maybe_schema,
      sanitize::schema_registry_internal::canonical_schema_from_registry_json(
          registry_json));
  if (!maybe_schema || maybe_schema->fields.empty()) {
    return nullptr;
  }

  sanitize::schema_registry_internal::normalize_integer_float_schema(
      *maybe_schema);
  if (prepared->spec.field_order ==
      sanitize::FieldOrderPolicy::kAlphabetically) {
    *maybe_schema = sanitize::internal::reorder_schema_fields(
        *maybe_schema, nullptr, prepared->spec.field_order);
  }

  sanitize::SchemaRegistryMergeResult merged;
  merged.schema = std::move(*maybe_schema);
  merged.registry_json = registry_json;
  merged.drifts_json = drifts_json;
  merged.detected_at = conversion_timestamp;
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
  const char *drifts_json = "[]";
  const char *conversion_timestamp = "";
  if (!PyArg_ParseTuple(args, "Os|ss:registry_state_from_json", &prepared_obj,
                        &registry_json, &drifts_json, &conversion_timestamp)) {
    return nullptr;
  }

  sanitize::PreparedOptionsPtr prepared_options;
  if (!resolve_prepared_options(prepared_obj, &prepared_options)) {
    return nullptr;
  }

  auto plan_r = make_native_registry_plan_from_json(
      prepared_options, registry_json, drifts_json, conversion_timestamp);
  if (!plan_r.ok()) {
    PyErr_SetString(PyExc_ValueError, plan_r.status().ToString().c_str());
    return nullptr;
  }
  return wrap_native_registry_state(std::move(plan_r).ValueOrDie());
}

} // namespace core_abi3_internal
