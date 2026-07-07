/*
 * Python ABI3 Parquet reader helpers.
 *
 * These entry points expose bounded native footer/page parsing and a guarded
 * flat-column Arrow C stream reader for supported Parquet files.
 */
#include "internal/abi/core_abi3_internal.hh"

#include "internal/parquet/parquet_footer_reader.hh"

#include <memory>
#include <string>

namespace core_abi3_internal {

PyObject *py_parquet_footer_info_json(PyObject *, PyObject *args) {
  PyObject *path_obj = nullptr;
  if (!PyArg_ParseTuple(args, "O:parquet_footer_info_json", &path_obj)) {
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

  auto result =
      sanitize::internal::parquet_footer_reader::read_footer_info_json(
          std::string(path_data, static_cast<std::size_t>(path_size)));
  if (!result.ok()) {
    PyErr_SetString(PyExc_RuntimeError, result.status().message().c_str());
    return nullptr;
  }
  const auto json = std::move(result).ValueOrDie();
  return PyUnicode_FromStringAndSize(json.data(),
                                     static_cast<Py_ssize_t>(json.size()));
}

PyObject *py_parquet_stream_read(PyObject *, PyObject *args) {
  PyObject *path_obj = nullptr;
  if (!PyArg_ParseTuple(args, "O:parquet_stream_read", &path_obj)) {
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

  auto result = sanitize::internal::parquet_footer_reader::make_arrow_stream(
      std::string(path_data, static_cast<std::size_t>(path_size)));
  if (!result.ok()) {
    PyErr_SetString(PyExc_RuntimeError, result.status().message().c_str());
    return nullptr;
  }
  ArrowArrayStream *stream = std::move(result).ValueOrDie();
  return wrap_stream_capsule_with_keepalive(path_obj, stream);
}

} // namespace core_abi3_internal
