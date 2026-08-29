/*
 * Implements Python ABI3 Parquet reader helpers.
 *
 * These entry points expose bounded native footer/page parsing and a guarded
 * flat-column Arrow C stream reader for supported Parquet files.
 */

#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/abi/python_abi3/methods.hh"

#include "internal/parquet/footer_reader/api.hh"

#include <memory>
#include <string>
#include <vector>

namespace core_abi3_internal {

namespace {

/// Parses an optional Python sequence into validated Parquet projection names.
bool parse_projected_columns(PyObject *columns_obj,
                             std::vector<std::string> *projected_columns) {
  if (!projected_columns || !columns_obj || columns_obj == Py_None) {
    return true;
  }
  const Py_ssize_t size = PySequence_Size(columns_obj);
  if (size < 0) {
    PyErr_SetString(PyExc_TypeError,
                    "Parquet projection columns must be a sequence");
    return false;
  }
  projected_columns->reserve(static_cast<std::size_t>(size));
  for (Py_ssize_t i = 0; i < size; ++i) {
    PyObject *item = PySequence_GetItem(columns_obj, i);
    if (!item) {
      return false;
    }
    std::unique_ptr<PyObject, decltype(&Py_DECREF)> item_owner(item, Py_DECREF);
    if (!PyUnicode_Check(item)) {
      PyErr_SetString(PyExc_TypeError,
                      "Parquet projection columns must be strings");
      return false;
    }
    Py_ssize_t column_size = 0;
    const char *column_data = PyUnicode_AsUTF8AndSize(item, &column_size);
    if (!column_data) {
      return false;
    }
    projected_columns->emplace_back(column_data,
                                    static_cast<std::size_t>(column_size));
  }
  return true;
}

} // namespace

/// Reads projected Parquet footer information and returns it as JSON text.
PyObject *py_parquet_footer_info_json(PyObject *, PyObject *args) {
  PyObject *path_obj = nullptr;
  PyObject *columns_obj = nullptr;
  if (!PyArg_ParseTuple(args, "O|O:parquet_footer_info_json", &path_obj,
                        &columns_obj)) {
    return nullptr;
  }
  PyObject *encoded = fsencode_path(path_obj);
  if (!encoded) {
    return nullptr;
  }
  std::unique_ptr<PyObject, decltype(&Py_DECREF)> encoded_owner(encoded,
                                                                Py_DECREF);
  char *path_data = nullptr;
  Py_ssize_t path_size = 0;
  if (PyBytes_AsStringAndSize(encoded, &path_data, &path_size) != 0) {
    return nullptr;
  }

  std::vector<std::string> projected_columns;
  if (!parse_projected_columns(columns_obj, &projected_columns)) {
    return nullptr;
  }

  auto result =
      sanitize::internal::parquet_footer_reader::read_footer_info_json(
          std::string(path_data, static_cast<std::size_t>(path_size)),
          projected_columns);
  if (!result.ok()) {
    PyErr_SetString(PyExc_RuntimeError, result.status().message().c_str());
    return nullptr;
  }
  const auto json = std::move(result).ValueOrDie();
  return PyUnicode_FromStringAndSize(json.data(),
                                     static_cast<Py_ssize_t>(json.size()));
}

/// Computes bounded Parquet stream preflight details and returns them as JSON
/// text.
PyObject *py_parquet_stream_preflight_json(PyObject *, PyObject *args) {
  PyObject *path_obj = nullptr;
  PyObject *columns_obj = Py_None;
  long long memory_limit_bytes = -1;
  if (!PyArg_ParseTuple(args, "O|OL:parquet_stream_preflight_json", &path_obj,
                        &columns_obj, &memory_limit_bytes)) {
    return nullptr;
  }
  PyObject *encoded = fsencode_path(path_obj);
  if (!encoded) {
    return nullptr;
  }
  std::unique_ptr<PyObject, decltype(&Py_DECREF)> encoded_owner(encoded,
                                                                Py_DECREF);
  char *path_data = nullptr;
  Py_ssize_t path_size = 0;
  if (PyBytes_AsStringAndSize(encoded, &path_data, &path_size) != 0) {
    return nullptr;
  }

  std::vector<std::string> projected_columns;
  if (!parse_projected_columns(columns_obj, &projected_columns)) {
    return nullptr;
  }

  auto result =
      sanitize::internal::parquet_footer_reader::read_stream_preflight_json(
          std::string(path_data, static_cast<std::size_t>(path_size)),
          projected_columns, memory_limit_bytes);
  if (!result.ok()) {
    PyErr_SetString(PyExc_RuntimeError, result.status().message().c_str());
    return nullptr;
  }
  const auto json = std::move(result).ValueOrDie();
  return PyUnicode_FromStringAndSize(json.data(),
                                     static_cast<Py_ssize_t>(json.size()));
}

/// Opens a projected Parquet file as an Arrow C stream with path keepalive.
PyObject *py_parquet_stream_read(PyObject *, PyObject *args) {
  PyObject *path_obj = nullptr;
  PyObject *columns_obj = Py_None;
  long long memory_limit_bytes = -1;
  if (!PyArg_ParseTuple(args, "O|OL:parquet_stream_read", &path_obj,
                        &columns_obj, &memory_limit_bytes)) {
    return nullptr;
  }
  PyObject *encoded = fsencode_path(path_obj);
  if (!encoded) {
    return nullptr;
  }
  std::unique_ptr<PyObject, decltype(&Py_DECREF)> encoded_owner(encoded,
                                                                Py_DECREF);
  char *path_data = nullptr;
  Py_ssize_t path_size = 0;
  if (PyBytes_AsStringAndSize(encoded, &path_data, &path_size) != 0) {
    return nullptr;
  }

  std::vector<std::string> projected_columns;
  if (!parse_projected_columns(columns_obj, &projected_columns)) {
    return nullptr;
  }

  auto result = sanitize::internal::parquet_footer_reader::make_arrow_stream(
      std::string(path_data, static_cast<std::size_t>(path_size)),
      projected_columns, memory_limit_bytes);
  if (!result.ok()) {
    PyErr_SetString(PyExc_RuntimeError, result.status().message().c_str());
    return nullptr;
  }
  ArrowArrayStream *stream = std::move(result).ValueOrDie();
  return wrap_stream_capsule_with_keepalive(path_obj, stream);
}

} // namespace core_abi3_internal
