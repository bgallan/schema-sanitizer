/* Python-object JSON encoding helpers for ABI3 wrappers. */
#include "api/python_abi3/json/_core_abi3_json_tools.hh"
#include "api/python_abi3/json/json_number_write.hh"

#include "internal/json_encoding/token_writer.hh"

#include <array>
#include <charconv>
#include <memory>
#include <string>
#include <string_view>

namespace core_abi3_internal {
namespace {

sanitize::Status append_python_json_value_impl(std::string &out,
                                               PyObject *value, int depth);

sanitize::Status append_python_unicode(std::string &out, PyObject *value) {
  Py_ssize_t size = 0;
  const char *text = PyUnicode_AsUTF8AndSize(value, &size);
  if (!text) {
    PyErr_Clear();
    return sanitize::Status::Invalid(
        "native Python JSON encoder: invalid UTF-8 string");
  }
  sanitize::internal::json_encoding::append_string(
      out, std::string_view(text, static_cast<std::size_t>(size)));
  return sanitize::Status::OK();
}

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

sanitize::Status append_python_sequence(std::string &out, PyObject *value,
                                        int depth) {
  PyObject *sequence =
      PySequence_Fast(value, "native Python JSON encoder: invalid sequence");
  if (!sequence) {
    PyErr_Clear();
    return sanitize::Status::Invalid(
        "native Python JSON encoder: invalid sequence");
  }
  std::unique_ptr<PyObject, decltype(&Py_DECREF)> sequence_owner(sequence,
                                                                 Py_DECREF);
  const Py_ssize_t size = PySequence_Size(sequence);
  if (size < 0) {
    PyErr_Clear();
    return sanitize::Status::Invalid(
        "native Python JSON encoder: invalid sequence");
  }
  out.push_back('[');
  for (Py_ssize_t index = 0; index < size; ++index) {
    if (index != 0) {
      out.push_back(',');
    }
    bool borrowed = false;
    PyObject *item = sequence_item_borrowed_or_new(sequence, index, &borrowed);
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
    append_json_double(out, PyFloat_AsDouble(value));
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

sanitize::Status append_python_json_value(std::string &out, PyObject *value,
                                          int depth) {
  return append_python_json_value_impl(out, value, depth);
}

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
