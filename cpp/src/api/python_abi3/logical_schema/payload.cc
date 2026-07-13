// Validates and exports native logical-schema payloads through Arrow C Data.

#include "api/python_abi3/logical_schema/payload.hh"

#include "internal/abi/python_abi3/methods.hh"
#include "internal/arrow_c/cdata_schema_builder.hh"
#include "internal/planning/options_schema_serialization.hh"

#include <cstddef>
#include <cstdint>
#include <new>
#include <string_view>
#include <utility>
#include <vector>

#include "nanoarrow/nanoarrow.h"

namespace core_abi3_internal {
namespace {

constexpr const char *kArrowSchemaCapsuleName = "arrow_schema";

void arrow_schema_capsule_destructor(PyObject *capsule) {
  auto *schema = static_cast<ArrowSchema *>(
      PyCapsule_GetPointer(capsule, kArrowSchemaCapsuleName));
  if (!schema) {
    PyErr_Clear();
    return;
  }
  if (schema->release) {
    schema->release(schema);
  }
  delete schema;
}

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

sanitize::Result<sanitize::LogicalSchema> read_required(PyObject *obj) {
  PyObject *owner = nullptr;
  const std::uint8_t *data = nullptr;
  Py_ssize_t size = 0;
  if (!readonly_buffer_view(obj, &data, &size, &owner)) {
    return sanitize::Status::Invalid("invalid logical schema payload bytes");
  }
  const std::string_view payload(reinterpret_cast<const char *>(data),
                                 static_cast<std::size_t>(size));
  auto decoded =
      sanitize::internal::options_io::deserialize_logical_schema_bytes(payload);
  Py_DECREF(owner);
  return decoded;
}

} // namespace logical_schema_payload

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

  auto *arrow_schema = new (std::nothrow) ArrowSchema();
  if (!arrow_schema) {
    return PyErr_NoMemory();
  }
  auto fields = take_field_layouts(std::move(schema).ValueOrDie());
  const auto status = sanitize::internal::export_fields_as_struct_schema(
      fields, arrow_schema, "TIMESTAMP_NANOS");
  if (!status.ok()) {
    delete arrow_schema;
    PyErr_SetString(PyExc_ValueError, status.ToString().c_str());
    return nullptr;
  }

  PyObject *capsule = PyCapsule_New(arrow_schema, kArrowSchemaCapsuleName,
                                    arrow_schema_capsule_destructor);
  if (!capsule) {
    arrow_schema->release(arrow_schema);
    delete arrow_schema;
  }
  return capsule;
}

} // namespace core_abi3_internal
