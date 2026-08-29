// Implements Python ABI3 schema-registry query and merge methods. The routines
// preserve source order and Arrow ownership while applying compiled registry
// plans.

#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/methods.hh"

#include "api/python_abi3/logical_schema/payload.hh"
#include "internal/planning/options_schema_serialization.hh"
#include "sanitize/schema_registry/schema_registry.hh"
#include "schema_registry/schema_registry_internal.hh"

#include <algorithm>
#include <array>
#include <string>
#include <string_view>
#include <utility>

namespace core_abi3_internal {
namespace {

struct EmptyRegistryPayload {
  std::string_view policy;
  std::string_view json;
};

constexpr std::array<EmptyRegistryPayload, 3> kEmptyRegistryPayloads{{
    {"lower_alpha",
     R"({"field_name_policy":"lower_alpha","registry_version":1,"schema_generation":1,"variants":{}})"},
    {"lower_snake",
     R"({"field_name_policy":"lower_snake","registry_version":1,"schema_generation":1,"variants":{}})"},
    {"preserve",
     R"({"field_name_policy":"preserve","registry_version":1,"schema_generation":1,"variants":{}})"},
}};

/// Returns the canonical serialized representation of an empty schema registry.
const EmptyRegistryPayload *empty_registry_payload(std::string_view policy) {
  const auto it = std::ranges::find_if(
      kEmptyRegistryPayloads, [policy](const EmptyRegistryPayload &candidate) {
        return candidate.policy == policy;
      });
  return it == kEmptyRegistryPayloads.end() ? nullptr : &*it;
}

} // namespace

/// Returns canonical empty registry JSON for the requested field-name policy.
PyObject *py_schema_registry_empty(PyObject *, PyObject *args) {
  const char *field_name_policy = "lower_snake";
  if (!PyArg_ParseTuple(args, "|s:schema_registry_empty", &field_name_policy)) {
    return nullptr;
  }

  const EmptyRegistryPayload *payload = empty_registry_payload(
      std::string_view(field_name_policy ? field_name_policy : ""));
  if (!payload) {
    PyErr_SetString(PyExc_ValueError,
                    "field_name_policy must be 'lower_alpha', 'lower_snake', "
                    "or 'preserve'");
    return nullptr;
  }
  return PyUnicode_FromStringAndSize(
      payload->json.data(), static_cast<Py_ssize_t>(payload->json.size()));
}

/// Extracts the logical-schema contract payload encoded by registry JSON.
PyObject *py_schema_registry_contract_payload(PyObject *, PyObject *args) {
  const char *registry_json = nullptr;
  if (!PyArg_ParseTuple(args, "s:schema_registry_contract_payload",
                        &registry_json)) {
    return nullptr;
  }

  auto schema =
      sanitize::schema_registry_internal::canonical_schema_from_registry_json(
          registry_json ? registry_json : "");
  if (!schema.ok()) {
    PyErr_SetString(PyExc_ValueError, schema.status().ToString().c_str());
    return nullptr;
  }

  auto maybe_schema = std::move(schema).ValueOrDie();
  if (!maybe_schema.has_value() || maybe_schema->fields.empty()) {
    Py_RETURN_NONE;
  }
  sanitize::schema_registry_internal::normalize_integer_float_schema(
      *maybe_schema);
  auto payload_result =
      sanitize::internal::options_io::serialize_logical_schema_bytes(
          *maybe_schema);
  if (!payload_result.ok()) {
    const auto status = payload_result.status();
    if (status.code() == sanitize::StatusCode::kOutOfMemory) {
      PyErr_NoMemory();
    } else {
      PyErr_SetString(PyExc_ValueError, status.ToString().c_str());
    }
    return nullptr;
  }
  std::string payload = std::move(payload_result).ValueOrDie();
  return PyBytes_FromStringAndSize(payload.data(),
                                   static_cast<Py_ssize_t>(payload.size()));
}

/// Merges an inferred schema into registry JSON and returns the updated
/// registry result.
PyObject *py_schema_registry_merge(PyObject *, PyObject *args) {
  PyObject *inferred_obj = nullptr;
  const char *registry_json = nullptr;
  const char *field_name_policy = nullptr;
  const char *detected_at = "";
  if (!PyArg_ParseTuple(args, "Oss|s:schema_registry_merge", &inferred_obj,
                        &registry_json, &field_name_policy, &detected_at)) {
    return nullptr;
  }

  auto inferred = logical_schema_payload::read_required(inferred_obj);
  if (!inferred.ok()) {
    PyErr_SetString(PyExc_ValueError, inferred.status().ToString().c_str());
    return nullptr;
  }

  sanitize::SchemaRegistryMergeInput input;
  input.inferred_schema = std::move(inferred).ValueOrDie();
  input.registry_json = registry_json ? registry_json : "";
  input.field_name_policy =
      field_name_policy ? field_name_policy : "lower_snake";
  input.detected_at = detected_at ? detected_at : "";

  auto merged = sanitize::merge_schema_registry(input);
  if (!merged.ok()) {
    PyErr_SetString(PyExc_ValueError, merged.status().ToString().c_str());
    return nullptr;
  }

  sanitize::SchemaRegistryMergeResult result = std::move(merged).ValueOrDie();
  auto schema_payload_result =
      sanitize::internal::options_io::serialize_logical_schema_bytes(
          result.schema);
  if (!schema_payload_result.ok()) {
    const auto status = schema_payload_result.status();
    if (status.code() == sanitize::StatusCode::kOutOfMemory) {
      PyErr_NoMemory();
    } else {
      PyErr_SetString(PyExc_ValueError, status.ToString().c_str());
    }
    return nullptr;
  }
  std::string schema_payload = std::move(schema_payload_result).ValueOrDie();

  PyObject *tuple = PyTuple_New(3);
  if (!tuple) {
    return nullptr;
  }
  if (!tuple_set_item_steal(
          tuple, 0,
          PyBytes_FromStringAndSize(
              schema_payload.data(),
              static_cast<Py_ssize_t>(schema_payload.size()))) ||
      !tuple_set_item_steal(
          tuple, 1, PyUnicode_FromString(result.registry_json.c_str())) ||
      !tuple_set_item_steal(tuple, 2,
                            PyUnicode_FromString(result.drifts_json.c_str()))) {
    Py_DECREF(tuple);
    return nullptr;
  }
  return tuple;
}

} // namespace core_abi3_internal
