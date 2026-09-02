// Validates and exports native logical-schema payloads through Arrow C Data.
// The bridge validates required fields and transfers Arrow schema ownership
// back to Python safely.

#include "api/python_abi3/logical_schema/payload.hh"

#include "internal/abi/python_abi3/methods.hh"
#include "internal/arrow_c/cdata_schema_builder.hh"
#include "internal/arrow_c/cdata_stream_callbacks.hh"
#include "internal/planning/options_schema_serialization.hh"
#include "internal/runtime/process_identity.hh"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <string_view>
#include <utility>
#include <vector>

#include "nanoarrow/nanoarrow.h"

namespace core_abi3_internal {
namespace {

constexpr const char *kArrowSchemaCapsuleName = "arrow_schema";

/// Deletes the native payload owned by the corresponding Python capsule.
void arrow_schema_capsule_destructor(PyObject *capsule) {
  if (!sanitize::internal::runtime_owner_process()) {
    return;
  }
  auto *schema = static_cast<ArrowSchema *>(
      PyCapsule_GetPointer(capsule, kArrowSchemaCapsuleName));
  if (!schema) {
    PyErr_Clear();
    return;
  }
  sanitize::internal::cdata_stream::release_schema_nothrow(schema);
  delete schema;
}

/// Moves logical fields into Arrow C Data layouts without retaining the schema.
std::vector<sanitize::internal::CDataFieldLayout>
take_field_layouts(sanitize::LogicalSchema schema) {
  std::vector<sanitize::internal::CDataFieldLayout> fields;
  fields.reserve(schema.fields.size());
  for (auto &field : schema.fields) {
    sanitize::LogicalType type =
        field.type ? std::move(*field.type) : sanitize::LogicalType{};
    fields.push_back({.name = std::move(field.name),
                      .nullable = field.nullable,
                      .logical_type = std::move(type),
                      .format_override = {}});
  }
  return fields;
}

} // namespace

namespace logical_schema_payload {

/// Deserializes a required logical-schema payload from a read-only Python
/// buffer.
sanitize::Result<sanitize::LogicalSchema> read_required(PyObject *obj) {
  PyObject *owner = nullptr;
  const std::uint8_t *data = nullptr;
  Py_ssize_t size = 0;
  if (!readonly_buffer_view(obj, &data, &size, &owner)) {
    return sanitize::Status::Invalid("invalid logical schema payload bytes");
  }
  std::unique_ptr<PyObject, decltype(&Py_DECREF)> owner_guard(owner, Py_DECREF);
  const std::string_view payload(reinterpret_cast<const char *>(data),
                                 static_cast<std::size_t>(size));
  try {
    return sanitize::internal::options_io::deserialize_logical_schema_bytes(
        payload);
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(
        "logical schema payload deserialization ran out of memory");
  } catch (const std::exception &e) {
    return sanitize::Status::Invalid(
        "logical schema payload deserialization failed: ", e.what());
  } catch (...) {
    return sanitize::Status::Invalid(
        "logical schema payload deserialization failed");
  }
}

} // namespace logical_schema_payload

/// Validates that Python bytes contain a decodable logical-schema payload.
PyObject *py_logical_schema_payload_validate(PyObject *, PyObject *args) {
  PyObject *payload_obj = nullptr;
  if (!PyArg_ParseTuple(args, "O:logical_schema_payload_validate",
                        &payload_obj)) {
    return nullptr;
  }
  auto schema = logical_schema_payload::read_required(payload_obj);
  if (!schema.ok()) {
    PyErr_SetString(PyExc_ValueError, schema.status().ToString().c_str());
    return nullptr;
  }
  Py_RETURN_NONE;
}

/// Decodes a logical-schema payload and returns its root field names in order.
PyObject *py_logical_schema_payload_field_names(PyObject *, PyObject *args) {
  PyObject *payload_obj = nullptr;
  if (!PyArg_ParseTuple(args, "O:logical_schema_payload_field_names",
                        &payload_obj)) {
    return nullptr;
  }
  auto schema = logical_schema_payload::read_required(payload_obj);
  if (!schema.ok()) {
    PyErr_SetString(PyExc_ValueError, schema.status().ToString().c_str());
    return nullptr;
  }

  auto decoded = std::move(schema).ValueOrDie();
  PyObject *names = PyTuple_New(static_cast<Py_ssize_t>(decoded.fields.size()));
  if (!names) {
    return nullptr;
  }
  for (std::size_t index = 0; index < decoded.fields.size(); ++index) {
    const auto &name = decoded.fields[index].name;
    if (!tuple_set_item_steal(
            names, static_cast<Py_ssize_t>(index),
            PyUnicode_FromStringAndSize(
                name.data(), static_cast<Py_ssize_t>(name.size())))) {
      Py_DECREF(names);
      return nullptr;
    }
  }
  return names;
}

/// Exports a logical-schema payload as an owned Arrow C schema capsule.
PyObject *py_logical_schema_payload_arrow_c_schema(PyObject *, PyObject *args) {
  PyObject *payload_obj = nullptr;
  if (!PyArg_ParseTuple(args, "O:logical_schema_payload_arrow_c_schema",
                        &payload_obj)) {
    return nullptr;
  }
  auto schema = logical_schema_payload::read_required(payload_obj);
  if (!schema.ok()) {
    PyErr_SetString(PyExc_ValueError, schema.status().ToString().c_str());
    return nullptr;
  }

  auto arrow_schema =
      std::unique_ptr<ArrowSchema>(new (std::nothrow) ArrowSchema());
  if (!arrow_schema) {
    return PyErr_NoMemory();
  }
  try {
    auto fields = take_field_layouts(std::move(schema).ValueOrDie());
    const auto status = sanitize::internal::export_fields_as_struct_schema(
        fields, arrow_schema.get(), "TIMESTAMP_NANOS");
    if (!status.ok()) {
      PyErr_SetString(PyExc_ValueError, status.ToString().c_str());
      return nullptr;
    }
  } catch (const std::bad_alloc &) {
    return PyErr_NoMemory();
  } catch (const std::exception &e) {
    PyErr_SetString(PyExc_RuntimeError, e.what());
    return nullptr;
  } catch (...) {
    PyErr_SetString(PyExc_RuntimeError, "logical schema Arrow export failed");
    return nullptr;
  }

  PyObject *capsule = PyCapsule_New(arrow_schema.get(), kArrowSchemaCapsuleName,
                                    arrow_schema_capsule_destructor);
  if (!capsule) {
    sanitize::internal::cdata_stream::release_schema_nothrow(
        arrow_schema.get());
    return nullptr;
  }
  (void)arrow_schema.release();
  return capsule;
}

} // namespace core_abi3_internal
