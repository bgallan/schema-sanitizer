/*
 * Python ABI3 JSON value utility wrappers.
 *
 * This file contains shared native JSON encoders used by Python adapters.
 * Public ABI wrappers that compact local JSON files live in focused sibling
 * translation units and reuse the helpers declared in _core_abi3_json_tools.hh.
 */
#include "api/python_abi3/_core_abi3_json_tools.hh"

#include "internal/json/json_write.hh"
#include "internal/parsing/json_ondemand.hh"

#include <array>
#include <charconv>
#include <cmath>
#include <limits>
#include <locale>
#include <memory>
#include <memory_resource>
#include <sstream>
#include <string>
#include <string_view>

namespace core_abi3_internal {
namespace {

sanitize::Status append_json_value(std::string &out, sanitize::ValueView value);

/// Compacts one parsed JSON document into canonical JSON text.
sanitize::Result<std::string>
compact_json_document_impl(std::string_view text) {
  std::pmr::monotonic_buffer_resource arena;
  sanitize::internal::JsonOnDemandDoc doc(&arena);
  auto value_result = doc.ParseValue(text);
  if (!value_result.ok()) {
    return value_result.status();
  }
  std::string compact;
  const auto status = append_json_value(compact, *value_result);
  if (!status.ok()) {
    return status;
  }
  return compact;
}

/// Splits one parsed top-level JSON array of objects into compact JSON Lines.
sanitize::Result<std::string>
json_array_document_to_jsonl_impl(std::string_view text) {
  std::pmr::monotonic_buffer_resource arena;
  sanitize::internal::JsonOnDemandDoc doc(&arena);
  auto value_result = doc.ParseValue(text);
  if (!value_result.ok()) {
    return value_result.status();
  }
  auto value = *value_result;
  if (!value.is_array()) {
    return sanitize::Status::Invalid("json_array input must be a JSON array");
  }

  std::string out;
  std::size_t index = 0;
  SAN_RETURN_NOT_OK(value.for_each_array_element(
      [&](sanitize::ValueView child) -> sanitize::Status {
        if (!child.is_object()) {
          return sanitize::Status::Invalid(
              "json_array requires object elements; invalid element " +
              std::to_string(index));
        }
        SAN_RETURN_NOT_OK(append_json_value(out, child));
        out.push_back('\n');
        ++index;
        return sanitize::Status::OK();
      }));
  return out;
}

/// Appends one object value to a compact JSON output string.
sanitize::Status append_json_object(std::string &out,
                                    sanitize::ValueView value) {
  out.push_back('{');
  bool first = true;
  SAN_RETURN_NOT_OK(value.for_each_object_field(
      [&](std::string_view key, uint64_t, sanitize::ValueView child) {
        if (!first) {
          out.push_back(',');
        }
        first = false;
        sanitize::internal::json_write::append_string(out, key);
        out.push_back(':');
        return append_json_value(out, child);
      }));
  out.push_back('}');
  return sanitize::Status::OK();
}

/// Appends one array value to a compact JSON output string.
sanitize::Status append_json_array(std::string &out,
                                   sanitize::ValueView value) {
  out.push_back('[');
  bool first = true;
  SAN_RETURN_NOT_OK(
      value.for_each_array_element([&](sanitize::ValueView child) {
        if (!first) {
          out.push_back(',');
        }
        first = false;
        return append_json_value(out, child);
      }));
  out.push_back(']');
  return sanitize::Status::OK();
}

/// Appends one finite or special floating-point value as JSON-compatible text.
void append_json_float(std::string &out, double value) {
  if (std::isnan(value)) {
    out += "NaN";
    return;
  }
  if (std::isinf(value)) {
    out += value < 0 ? "-Infinity" : "Infinity";
    return;
  }
#if !defined(__APPLE__)
  if constexpr (requires(char *first, char *last, double v) {
                  std::to_chars(first, last, v, std::chars_format::general,
                                std::numeric_limits<double>::max_digits10);
                }) {
    std::array<char, 64> buffer{};
    auto [ptr, ec] = std::to_chars(buffer.data(), buffer.data() + buffer.size(),
                                   value, std::chars_format::general,
                                   std::numeric_limits<double>::max_digits10);
    if (ec == std::errc()) {
      out.append(buffer.data(), static_cast<std::size_t>(ptr - buffer.data()));
      return;
    }
  }
#endif
  std::ostringstream oss;
  oss.imbue(std::locale::classic());
  oss.precision(std::numeric_limits<double>::max_digits10);
  oss << value;
  out += oss.str();
}

/// Appends one native ValueView as compact JSON.
sanitize::Status append_json_value(std::string &out,
                                   sanitize::ValueView value) {
  if (value.is_null()) {
    out += "null";
  } else if (value.is_bool()) {
    out += value.as_bool() ? "true" : "false";
  } else if (value.is_int()) {
    out += std::to_string(value.as_int());
  } else if (value.is_float()) {
    append_json_float(out, value.as_float());
  } else if (value.is_string()) {
    sanitize::internal::json_write::append_string(out, value.as_string_view());
  } else if (value.is_object()) {
    return append_json_object(out, value);
  } else if (value.is_array()) {
    return append_json_array(out, value);
  }
  return sanitize::Status::OK();
}

sanitize::Status append_python_json_value_impl(std::string &out,
                                               PyObject *value, int depth);

/// Appends one Python Unicode string as a JSON string.
sanitize::Status append_python_unicode(std::string &out, PyObject *value) {
  Py_ssize_t size = 0;
  const char *text = PyUnicode_AsUTF8AndSize(value, &size);
  if (!text) {
    PyErr_Clear();
    return sanitize::Status::Invalid(
        "native Python JSON encoder: invalid UTF-8 string");
  }
  sanitize::internal::json_write::append_string(
      out, std::string_view(text, static_cast<std::size_t>(size)));
  return sanitize::Status::OK();
}

/// Appends a Python mapping key as a JSON object key.
sanitize::Status append_python_key(std::string &out, PyObject *key) {
  if (PyUnicode_Check(key)) {
    return append_python_unicode(out, key);
  }
  PyObject *text = PyObject_Str(key);
  if (!text) {
    PyErr_Clear();
    return sanitize::Status::Invalid(
        "native Python JSON encoder: object key is not stringable");
  }
  std::unique_ptr<PyObject, decltype(&Py_DECREF)> text_owner(text, Py_DECREF);
  return append_python_unicode(out, text);
}

/// Appends one Python dictionary as a JSON object.
sanitize::Status append_python_dict(std::string &out, PyObject *value,
                                    int depth) {
  out.push_back('{');
  bool first = true;
  PyObject *key = nullptr;
  PyObject *item = nullptr;
  Py_ssize_t pos = 0;
  while (PyDict_Next(value, &pos, &key, &item)) {
    if (!first) {
      out.push_back(',');
    }
    first = false;
    SAN_RETURN_NOT_OK(append_python_key(out, key));
    out.push_back(':');
    SAN_RETURN_NOT_OK(append_python_json_value_impl(out, item, depth + 1));
  }
  out.push_back('}');
  return sanitize::Status::OK();
}

/// Appends one Python sequence as a JSON array.
sanitize::Status append_python_sequence(std::string &out, PyObject *value,
                                        int depth) {
  PyObject *seq =
      PySequence_Fast(value, "native Python JSON encoder: invalid sequence");
  if (!seq) {
    PyErr_Clear();
    return sanitize::Status::Invalid(
        "native Python JSON encoder: invalid sequence");
  }
  std::unique_ptr<PyObject, decltype(&Py_DECREF)> seq_owner(seq, Py_DECREF);
  const Py_ssize_t size = PySequence_Size(seq);
  if (size < 0) {
    PyErr_Clear();
    return sanitize::Status::Invalid(
        "native Python JSON encoder: invalid sequence");
  }
  out.push_back('[');
  for (Py_ssize_t i = 0; i < size; ++i) {
    if (i != 0) {
      out.push_back(',');
    }
    bool borrowed = false;
    PyObject *item = sequence_item_borrowed_or_new(seq, i, &borrowed);
    if (!item) {
      PyErr_Clear();
      return sanitize::Status::Invalid(
          "native Python JSON encoder: failed reading sequence item");
    }
    std::unique_ptr<PyObject, decltype(&Py_DECREF)> item_owner(
        borrowed ? nullptr : item, Py_DECREF);
    SAN_RETURN_NOT_OK(append_python_json_value_impl(out, item, depth + 1));
  }
  out.push_back(']');
  return sanitize::Status::OK();
}

/// Appends a Python integer through a fixed-width fast path when possible.
sanitize::Status append_python_long(std::string &out, PyObject *value) {
  const long long raw = PyLong_AsLongLong(value);
  if (raw != -1 || !PyErr_Occurred()) {
    std::array<char, 32> buffer{};
    auto [ptr, ec] =
        std::to_chars(buffer.data(), buffer.data() + buffer.size(), raw);
    if (ec == std::errc()) {
      out.append(buffer.data(), static_cast<std::size_t>(ptr - buffer.data()));
      return sanitize::Status::OK();
    }
  } else {
    PyErr_Clear();
  }

  PyObject *text = PyObject_Str(value);
  if (!text) {
    PyErr_Clear();
    return sanitize::Status::Invalid(
        "native Python JSON encoder: invalid integer");
  }
  std::unique_ptr<PyObject, decltype(&Py_DECREF)> text_owner(text, Py_DECREF);
  Py_ssize_t size = 0;
  const char *raw_text = PyUnicode_AsUTF8AndSize(text, &size);
  if (!raw_text) {
    PyErr_Clear();
    return sanitize::Status::Invalid(
        "native Python JSON encoder: invalid integer text");
  }
  out.append(raw_text, static_cast<std::size_t>(size));
  return sanitize::Status::OK();
}

/// Appends one supported Python JSON-like value recursively.
sanitize::Status append_python_json_value_impl(std::string &out,
                                               PyObject *value, int depth) {
  if (depth > 1000) {
    return sanitize::Status::Invalid(
        "native Python JSON encoder: maximum recursion depth exceeded");
  }
  if (value == Py_None) {
    out += "null";
    return sanitize::Status::OK();
  }
  if (PyBool_Check(value)) {
    out += (value == Py_True) ? "true" : "false";
    return sanitize::Status::OK();
  }
  if (PyUnicode_Check(value)) {
    return append_python_unicode(out, value);
  }
  if (PyLong_Check(value)) {
    return append_python_long(out, value);
  }
  if (PyFloat_Check(value)) {
    append_json_float(out, PyFloat_AsDouble(value));
    return sanitize::Status::OK();
  }
  if (PyDict_Check(value)) {
    return append_python_dict(out, value, depth);
  }
  if (PyList_Check(value) || PyTuple_Check(value)) {
    return append_python_sequence(out, value, depth);
  }
  return sanitize::Status::Invalid(
      "native Python JSON encoder: unsupported Python value");
}

} // namespace

sanitize::Result<std::string> compact_json_document(std::string_view text) {
  return compact_json_document_impl(text);
}

sanitize::Result<std::string>
json_array_document_to_jsonl(std::string_view text) {
  return json_array_document_to_jsonl_impl(text);
}

/// Appends one supported Python JSON-like value to an existing string buffer.
sanitize::Status append_python_json_value(std::string &out, PyObject *value,
                                          int depth) {
  return append_python_json_value_impl(out, value, depth);
}

/// Encodes one Python row as compact JSON bytes.
PyObject *py_python_row_json_bytes(PyObject *, PyObject *args) {
  PyObject *row = nullptr;
  if (!PyArg_ParseTuple(args, "O:python_row_json_bytes", &row)) {
    return nullptr;
  }
  std::string out;
  const auto status = append_python_json_value_impl(out, row, 0);
  if (!status.ok()) {
    PyErr_SetString(PyExc_TypeError, status.message().c_str());
    return nullptr;
  }
  return PyBytes_FromStringAndSize(out.data(),
                                   static_cast<Py_ssize_t>(out.size()));
}

} // namespace core_abi3_internal
