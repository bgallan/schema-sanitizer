/*
 * Python ABI3 file-metadata wrappers.
 *
 * This file exposes native generation of per-file metadata columns used by all
 * streaming file sinks.
 */
#include "internal/abi/core_abi3_internal.hh"

#include <string>

#include "sanitize/metadata/file_metadata.hh"

namespace core_abi3_internal {
namespace {

// Adds one string pair to a Python dictionary.
int dict_set_string_pair(PyObject *dict, const std::string &key,
                         const std::string &value) {
  PyObject *py_key = PyUnicode_FromStringAndSize(
      key.data(), static_cast<Py_ssize_t>(key.size()));
  if (!py_key) {
    return 0;
  }
  PyObject *py_value = PyUnicode_FromStringAndSize(
      value.data(), static_cast<Py_ssize_t>(value.size()));
  if (!py_value) {
    Py_DECREF(py_key);
    return 0;
  }

  const int ok = PyDict_SetItem(dict, py_key, py_value) == 0;
  Py_DECREF(py_key);
  Py_DECREF(py_value);
  return ok;
}

} // namespace

PyObject *py_file_metadata_columns(PyObject *, PyObject *args) {
  PyObject *input_path_obj = nullptr;

  if (!PyArg_ParseTuple(args, "O:file_metadata_columns", &input_path_obj)) {
    return nullptr;
  }

  PyObject *path_bytes = fsencode_path(input_path_obj);
  if (!path_bytes) {
    return nullptr;
  }

  char *path_data = nullptr;
  Py_ssize_t path_size = 0;
  if (PyBytes_AsStringAndSize(path_bytes, &path_data, &path_size) != 0 ||
      !path_data) {
    Py_DECREF(path_bytes);
    return nullptr;
  }

  sanitize::FileMetadataInput input{
      .input_path = std::string(path_data, static_cast<std::size_t>(path_size)),
  };
  Py_DECREF(path_bytes);

  auto generated = sanitize::generated_file_metadata_columns(input);
  if (!generated.ok()) {
    PyErr_SetString(PyExc_ValueError, generated.status().ToString().c_str());
    return nullptr;
  }

  PyObject *dict = PyDict_New();
  if (!dict) {
    return nullptr;
  }
  for (const auto &[key, value] : generated.ValueOrDie()) {
    if (!dict_set_string_pair(dict, key, value)) {
      Py_DECREF(dict);
      return nullptr;
    }
  }
  return dict;
}

} // namespace core_abi3_internal
