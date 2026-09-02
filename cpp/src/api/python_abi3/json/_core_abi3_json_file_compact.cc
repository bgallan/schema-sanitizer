/*
 * Implements Python ABI3 wrappers for local JSON-array file batching.
 *
 * The routines preserve JSON value semantics while enforcing bounded native
 * ownership and Python errors.
 */

#include "api/python_abi3/json/_core_abi3_json_tools.hh"

#include <memory>
#include <string>
#include <string_view>

namespace core_abi3_internal {
namespace {

/// Appends path context to JSON parser errors raised while processing a folder.
std::string invalid_json_file_message(PyObject *path_obj,
                                      std::string_view detail) {
  std::string message = "Invalid JSON file";
  PyObject *path_text = PyObject_Str(path_obj);
  if (path_text) {
    Py_ssize_t size = 0;
    const char *data = PyUnicode_AsUTF8AndSize(path_text, &size);
    if (data && size > 0) {
      message.push_back(' ');
      message.append(data, static_cast<std::size_t>(size));
    }
    Py_DECREF(path_text);
  } else {
    PyErr_Clear();
  }
  message += ": ";
  message.append(detail);
  return message;
}

/// Reads, validates, and appends one JSON-array document as JSON Lines.
bool append_array_file_jsonl(PyObject *path_obj, long long memory_limit_bytes,
                             std::string *raw, std::string *out) {
  if (!read_local_file_bytes(path_obj, memory_limit_bytes, raw)) {
    return false;
  }
  auto jsonl = json_array_document_to_jsonl(*raw);
  if (!jsonl.ok()) {
    const std::string message =
        invalid_json_file_message(path_obj, jsonl.status().message());
    PyErr_SetString(PyExc_ValueError, message.c_str());
    return false;
  }
  out->append(*jsonl);
  return true;
}

} // namespace

/// Reads bounded JSON-array files and concatenates them as validated JSON Lines
/// bytes.
PyObject *py_json_array_files_to_jsonl_bytes(PyObject *, PyObject *args) {
  PyObject *paths_obj = nullptr;
  long long memory_limit_bytes = -1;
  if (!PyArg_ParseTuple(args, "OL:json_array_files_to_jsonl_bytes", &paths_obj,
                        &memory_limit_bytes)) {
    return nullptr;
  }

  PyObject *paths = PySequence_Fast(paths_obj, "paths must be a sequence");
  if (!paths) {
    return nullptr;
  }
  std::unique_ptr<PyObject, decltype(&Py_DECREF)> paths_owner(paths, Py_DECREF);

  const Py_ssize_t n = PySequence_Size(paths);
  if (n < 0) {
    return nullptr;
  }
  std::string out;
  std::string raw;
  for (Py_ssize_t i = 0; i < n; ++i) {
    if (!check_python_signals()) {
      return nullptr;
    }
    bool borrowed = false;
    PyObject *path_obj = sequence_item_borrowed_or_new(paths, i, &borrowed);
    if (!path_obj) {
      return nullptr;
    }
    std::unique_ptr<PyObject, decltype(&Py_DECREF)> path_owner(
        borrowed ? nullptr : path_obj, Py_DECREF);
    if (!append_array_file_jsonl(path_obj, memory_limit_bytes, &raw, &out)) {
      return nullptr;
    }
  }
  return PyBytes_FromStringAndSize(out.data(),
                                   static_cast<Py_ssize_t>(out.size()));
}

} // namespace core_abi3_internal
