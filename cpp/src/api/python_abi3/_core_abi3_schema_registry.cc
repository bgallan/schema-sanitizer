/*
 * Python ABI3 schema-registry wrappers.
 *
 * This file exposes the native registry merge engine while keeping Arrow C++
 * out of the build. Schemas cross the boundary as the existing compact logical
 * schema payload used by options serialization.
 */
#include "internal/abi/core_abi3_internal.hh"

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include "internal/json/json_write.hh"
#include "internal/planning/options_schema_io.hh"
#include "sanitize/core/logical_schema.hh"
#include "sanitize/core/status.hh"
#include "sanitize/schema_registry/schema_registry.hh"
#include "schema_registry/schema_registry_internal.hh"

namespace core_abi3_internal {
namespace {

// Appends a little-endian u8 to a logical-schema payload.
void append_u8(std::string &out, std::uint8_t value) {
  out.push_back(static_cast<char>(value));
}

// Appends a little-endian u32 to a logical-schema payload.
void append_u32(std::string &out, std::uint32_t value) {
  for (int i = 0; i < 4; ++i) {
    out.push_back(static_cast<char>((value >> (8 * i)) & 0xFFu));
  }
}

// Appends a length-prefixed UTF-8 string to a logical-schema payload.
void append_string(std::string &out, std::string_view value) {
  append_u32(out, static_cast<std::uint32_t>(value.size()));
  out.append(value.data(), value.size());
}

// Encodes one logical type to the compact payload format.
void append_logical_type(std::string &out, const sanitize::LogicalType &type);

// Encodes one logical field to the compact payload format.
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
    for (const auto &field : type.fields)
      append_logical_field(out, field);
  } else if (type.kind == sanitize::LogicalKind::kList) {
    if (type.value) {
      append_logical_type(out, *type.value);
    } else {
      append_u8(out, static_cast<std::uint8_t>(sanitize::LogicalKind::kNull));
    }
  }
}

// Encodes a logical schema to the compact payload format.
std::string encode_logical_schema(const sanitize::LogicalSchema &schema) {
  std::string out;
  append_u32(out, static_cast<std::uint32_t>(schema.fields.size()));
  for (const auto &field : schema.fields)
    append_logical_field(out, field);
  return out;
}

// Reads a required logical schema payload from a Python object.
sanitize::Result<sanitize::LogicalSchema> read_required_schema(PyObject *obj) {
  PyObject *owner = nullptr;
  const std::uint8_t *p = nullptr;
  Py_ssize_t n = 0;
  if (!readonly_buffer_view(obj, &p, &n, &owner)) {
    return sanitize::Status::Invalid(
        "schema_registry_merge: invalid schema bytes");
  }
  std::string_view view(reinterpret_cast<const char *>(p),
                        static_cast<std::size_t>(n));
  auto decoded =
      sanitize::internal::options_io::deserialize_logical_schema_bytes(view);
  Py_DECREF(owner);
  if (!decoded.ok())
    return decoded.status();
  return std::move(decoded).ValueOrDie();
}

} // namespace

PyObject *py_schema_registry_empty(PyObject *, PyObject *args) {
  const char *field_name_policy = "lower_snake";

  if (!PyArg_ParseTuple(args, "|s:schema_registry_empty", &field_name_policy)) {
    return nullptr;
  }

  const std::string_view policy(field_name_policy ? field_name_policy : "");
  if (policy != "lower_alpha" && policy != "lower_snake" &&
      policy != "preserve") {
    PyErr_SetString(PyExc_ValueError,
                    "field_name_policy must be 'lower_alpha', 'lower_snake', "
                    "or 'preserve'");
    return nullptr;
  }

  std::string out;
  out.push_back('{');
  bool first = true;
  sanitize::internal::json_write::append_string_field(
      out, first, "field_name_policy", policy);
  sanitize::internal::json_write::append_int_field(out, first,
                                                   "registry_version", 1);
  sanitize::internal::json_write::append_int_field(out, first,
                                                   "schema_generation", 1);
  sanitize::internal::json_write::append_key(out, first, "variants");
  out.append("{}");
  out.push_back('}');
  return PyUnicode_FromStringAndSize(out.data(),
                                     static_cast<Py_ssize_t>(out.size()));
}

PyObject *py_schema_registry_has_canonical_schema(PyObject *, PyObject *args) {
  const char *registry_json = nullptr;

  if (!PyArg_ParseTuple(args, "s:schema_registry_has_canonical_schema",
                        &registry_json)) {
    return nullptr;
  }

  auto has_schema = sanitize::schema_registry_has_canonical_schema(
      registry_json ? registry_json : "");
  if (!has_schema.ok()) {
    PyErr_SetString(PyExc_ValueError, has_schema.status().ToString().c_str());
    return nullptr;
  }

  if (has_schema.ValueOrDie()) {
    Py_RETURN_TRUE;
  }
  Py_RETURN_FALSE;
}

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
  if (!maybe_schema.has_value()) {
    PyErr_SetString(PyExc_ValueError,
                    "schema registry does not contain a canonical schema");
    return nullptr;
  }
  sanitize::schema_registry_internal::normalize_integer_float_schema(
      *maybe_schema);

  std::string schema_payload = encode_logical_schema(*maybe_schema);
  return PyBytes_FromStringAndSize(
      schema_payload.data(), static_cast<Py_ssize_t>(schema_payload.size()));
}

PyObject *py_logical_schema_payload_field_names(PyObject *, PyObject *args) {
  PyObject *schema_obj = nullptr;

  if (!PyArg_ParseTuple(args, "O:logical_schema_payload_field_names",
                        &schema_obj)) {
    return nullptr;
  }

  auto schema = read_required_schema(schema_obj);
  if (!schema.ok()) {
    PyErr_SetString(PyExc_ValueError, schema.status().ToString().c_str());
    return nullptr;
  }

  const auto decoded = std::move(schema).ValueOrDie();
  PyObject *tuple = PyTuple_New(static_cast<Py_ssize_t>(decoded.fields.size()));
  if (!tuple)
    return nullptr;

  for (std::size_t i = 0; i < decoded.fields.size(); ++i) {
    const auto &name = decoded.fields[i].name;
    PyObject *py_name = PyUnicode_FromStringAndSize(
        name.data(), static_cast<Py_ssize_t>(name.size()));
    if (!tuple_set_item_steal(tuple, static_cast<Py_ssize_t>(i), py_name))
      return nullptr;
  }
  return tuple;
}

PyObject *py_schema_registry_merge(PyObject *, PyObject *args) {
  PyObject *inferred_obj = nullptr;
  const char *registry_json = nullptr;
  const char *field_name_policy = nullptr;

  if (!PyArg_ParseTuple(args, "Oss:schema_registry_merge", &inferred_obj,
                        &registry_json, &field_name_policy)) {
    return nullptr;
  }

  auto inferred = read_required_schema(inferred_obj);
  if (!inferred.ok()) {
    PyErr_SetString(PyExc_ValueError, inferred.status().ToString().c_str());
    return nullptr;
  }

  sanitize::SchemaRegistryMergeInput input;
  input.inferred_schema = std::move(inferred).ValueOrDie();
  input.registry_json = registry_json ? registry_json : "";
  input.field_name_policy =
      field_name_policy ? field_name_policy : "lower_snake";

  auto merged = sanitize::merge_schema_registry(input);
  if (!merged.ok()) {
    PyErr_SetString(PyExc_ValueError, merged.status().ToString().c_str());
    return nullptr;
  }

  sanitize::SchemaRegistryMergeResult result = std::move(merged).ValueOrDie();
  std::string schema_payload = encode_logical_schema(result.schema);

  PyObject *tuple = PyTuple_New(3);
  if (!tuple)
    return nullptr;
  if (!tuple_set_item_steal(
          tuple, 0,
          PyBytes_FromStringAndSize(
              schema_payload.data(),
              static_cast<Py_ssize_t>(schema_payload.size()))))
    return nullptr;
  if (!tuple_set_item_steal(tuple, 1,
                            PyUnicode_FromString(result.registry_json.c_str())))
    return nullptr;
  if (!tuple_set_item_steal(tuple, 2,
                            PyUnicode_FromString(result.drifts_json.c_str())))
    return nullptr;
  return tuple;
}

} // namespace core_abi3_internal
