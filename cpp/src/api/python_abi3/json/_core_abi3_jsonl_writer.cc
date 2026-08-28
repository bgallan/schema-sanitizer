/*
 * Python ABI3 JSONL stream writer wrapper.
 *
 * This file exposes JSONL Python methods while output and batch extraction
 * helpers live in _core_abi3_jsonl_output.cc.
 */
#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/abi/python_abi3/methods.hh"

#include "api/python_abi3/arrow_stream/_core_abi3_arrow_stream_lifecycle.hh"
#include "api/python_abi3/json/_core_abi3_jsonl_output.hh"
#include "api/python_abi3/json/output_adapters/api.hh"
#include "api/python_abi3/metadata/stream/stream.hh"
#include "internal/json_output/schema/model.hh"
#include "internal/output/ordered_text_output.hh"

#include <memory>
#include <string>

namespace core_abi3_internal {
namespace jsonl = sanitize::internal::jsonl_stream_writer;

PyObject *py_jsonl_stream_write(PyObject *, PyObject *args) {
  PyObject *stream_obj = nullptr;
  PyObject *output_obj = nullptr;
  long long memory_limit_bytes = -1;
  long long threading_mode_value = 0;
  if (!PyArg_ParseTuple(args, "OO|LL:jsonl_stream_write", &stream_obj,
                        &output_obj, &memory_limit_bytes,
                        &threading_mode_value)) {
    return nullptr;
  }

  auto mode_result =
      sanitize::internal::ordered_text_output::threading_mode_from_int(
          threading_mode_value);
  if (!mode_result.ok()) {
    PyErr_SetString(PyExc_ValueError, mode_result.status().message().c_str());
    return nullptr;
  }
  auto result = jsonl_write_stream_to_output(
      stream_obj, output_obj, memory_limit_bytes, mode_result.ValueOrDie());
  if (!result.ok()) {
    PyErr_SetString(PyExc_RuntimeError, result.status().message().c_str());
    return nullptr;
  }
  const auto &stats = result.ValueOrDie();
  return materialization_stats_dict(stats.materialized_rows, stats.batches);
}

PyObject *py_jsonl_stream_write_with_metadata(PyObject *, PyObject *args) {
  PyObject *stream_obj = nullptr;
  PyObject *path_obj = nullptr;
  PyObject *first_row_columns = nullptr;
  PyObject *all_row_columns = nullptr;
  PyObject *row_span_columns = nullptr;
  PyObject *timestamp_columns = nullptr;
  long long memory_limit_bytes = -1;
  long long threading_mode_value = 0;
  if (!PyArg_ParseTuple(args, "OOOOOO|LL:jsonl_stream_write_with_metadata",
                        &stream_obj, &path_obj, &first_row_columns,
                        &all_row_columns, &row_span_columns, &timestamp_columns,
                        &memory_limit_bytes, &threading_mode_value)) {
    return nullptr;
  }
  Py_ssize_t path_len = 0;
  const char *path = PyUnicode_AsUTF8AndSize(path_obj, &path_len);
  if (!path) {
    PyErr_SetString(PyExc_TypeError,
                    "jsonl_stream_write_with_metadata path must be a string");
    return nullptr;
  }

  auto wrapped = own_arrow_stream(make_metadata_stream_wrapper(
      stream_obj, first_row_columns, all_row_columns, row_span_columns,
      timestamp_columns, memory_limit_bytes));
  if (!wrapped) {
    return nullptr;
  }
  auto mode_result =
      sanitize::internal::ordered_text_output::threading_mode_from_int(
          threading_mode_value);
  if (!mode_result.ok()) {
    PyErr_SetString(PyExc_ValueError, mode_result.status().message().c_str());
    return nullptr;
  }
  auto result = jsonl_write_arrow_stream_to_path(
      wrapped.get(), std::string(path, static_cast<std::size_t>(path_len)),
      memory_limit_bytes, mode_result.ValueOrDie());
  if (!result.ok()) {
    PyErr_SetString(PyExc_RuntimeError, result.status().message().c_str());
    return nullptr;
  }
  const auto &stats = result.ValueOrDie();
  return materialization_stats_dict(stats.materialized_rows, stats.batches);
}

PyObject *py_jsonl_batch_bytes(PyObject *, PyObject *args) {
  PyObject *batch_obj = nullptr;
  long long memory_limit_bytes = -1;
  if (!PyArg_ParseTuple(args, "O|L:jsonl_batch_bytes", &batch_obj,
                        &memory_limit_bytes)) {
    return nullptr;
  }

  std::string bytes;
  const auto status =
      jsonl_append_batch_bytes(batch_obj, &bytes, memory_limit_bytes);
  if (!status.ok()) {
    PyErr_SetString(PyExc_RuntimeError, status.message().c_str());
    return nullptr;
  }
  return PyBytes_FromStringAndSize(bytes.data(),
                                   static_cast<Py_ssize_t>(bytes.size()));
}

PyObject *py_jsonl_batches_bytes(PyObject *, PyObject *args) {
  PyObject *batches_obj = nullptr;
  long long memory_limit_bytes = -1;
  if (!PyArg_ParseTuple(args, "O|L:jsonl_batches_bytes", &batches_obj,
                        &memory_limit_bytes)) {
    return nullptr;
  }

  std::string bytes;
  const auto status =
      jsonl_append_batches_bytes(batches_obj, &bytes, memory_limit_bytes);
  if (!status.ok()) {
    PyErr_SetString(PyExc_RuntimeError, status.message().c_str());
    return nullptr;
  }
  return PyBytes_FromStringAndSize(bytes.data(),
                                   static_cast<Py_ssize_t>(bytes.size()));
}

PyObject *py_jsonl_schema_supported(PyObject *, PyObject *args) {
  PyObject *schema_obj = nullptr;
  if (!PyArg_ParseTuple(args, "O:jsonl_schema_supported", &schema_obj)) {
    return nullptr;
  }
  PyObject *capsule = nullptr;
  ArrowSchema *schema = nullptr;
  if (!acquire_arrow_schema(schema_obj, &capsule, &schema)) {
    PyErr_SetString(PyExc_TypeError,
                    "jsonl_schema_supported expected a PyArrow schema");
    return nullptr;
  }
  std::unique_ptr<PyObject, decltype(&Py_DECREF)> capsule_owner(capsule,
                                                                Py_DECREF);
  return PyBool_FromLong(jsonl::schema_is_supported(*schema) ? 1 : 0);
}

} // namespace core_abi3_internal
