/*
 * Python ABI3 CSV stream writer wrapper.
 *
 * This file exposes native CSV writing for Arrow C streams. Metadata wrapping
 * and local-path normalization stay in Python; the native side only consumes
 * an Arrow C stream and writes a local CSV file.
 */
#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/abi/python_abi3/methods.hh"

#include "api/python_abi3/arrow_stream/_core_abi3_arrow_stream_lifecycle.hh"
#include "api/python_abi3/metadata/stream/stream.hh"
#include "internal/csv/csv_stream_writer.hh"

#include <fstream>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

namespace core_abi3_internal {
namespace {

namespace csv = sanitize::internal::csv_stream_writer;

PyObject *csv_stats_to_dict(const csv::WriteStats &stats) {
  PyObject *dict = PyDict_New();
  if (!dict) {
    return nullptr;
  }
  PyObject *rows = PyLong_FromLongLong(stats.materialized_rows);
  PyObject *batches = PyLong_FromLongLong(stats.batches);
  if (!rows || !batches) {
    Py_XDECREF(rows);
    Py_XDECREF(batches);
    Py_DECREF(dict);
    return nullptr;
  }
  if (PyDict_SetItemString(dict, "materialized_rows", rows) < 0 ||
      PyDict_SetItemString(dict, "batches", batches) < 0) {
    Py_DECREF(rows);
    Py_DECREF(batches);
    Py_DECREF(dict);
    return nullptr;
  }
  Py_DECREF(rows);
  Py_DECREF(batches);
  return dict;
}

class FileCsvOutput final : public csv::Output {
public:
  // Opens a local output path.
  explicit FileCsvOutput(std::string path)
      : out_(std::move(path),
             std::ios::out | std::ios::binary | std::ios::trunc) {}

  // Returns whether the file opened correctly.
  [[nodiscard]] bool ok() const noexcept { return static_cast<bool>(out_); }

  // Writes bytes to the file.
  sanitize::Status Write(std::string_view data) override {
    out_.write(data.data(), static_cast<std::streamsize>(data.size()));
    if (!out_) {
      return sanitize::Status::IOError("CSV writer: failed writing output");
    }
    return sanitize::Status::OK();
  }

  // Flushes the file.
  sanitize::Status Flush() override {
    out_.flush();
    if (!out_) {
      return sanitize::Status::IOError("CSV writer: failed flushing output");
    }
    return sanitize::Status::OK();
  }

private:
  std::ofstream out_;
};

sanitize::Result<csv::WriteStats> csv_write_stream_to_path(PyObject *stream_obj,
                                                           std::string path) {
  FileCsvOutput output(std::move(path));
  if (!output.ok()) {
    return sanitize::Status::IOError("CSV writer: failed opening output");
  }
  PyObject *capsule = nullptr;
  ArrowArrayStream *stream = nullptr;
  if (!acquire_arrow_stream(stream_obj, &capsule, &stream)) {
    return sanitize::Status::Invalid(
        "CSV writer: object does not export Arrow C stream");
  }
  std::unique_ptr<PyObject, decltype(&Py_DECREF)> capsule_owner(capsule,
                                                                Py_DECREF);
  return csv::write_stream(stream, output);
}

sanitize::Result<csv::WriteStats>
csv_write_arrow_stream_to_path(ArrowArrayStream *stream, std::string path) {
  FileCsvOutput output(std::move(path));
  if (!output.ok()) {
    return sanitize::Status::IOError("CSV writer: failed opening output");
  }
  return csv::write_stream(stream, output);
}

} // namespace

PyObject *py_csv_stream_write(PyObject *, PyObject *args) {
  PyObject *stream_obj = nullptr;
  PyObject *path_obj = nullptr;
  if (!PyArg_ParseTuple(args, "OO:csv_stream_write", &stream_obj, &path_obj)) {
    return nullptr;
  }
  Py_ssize_t path_len = 0;
  const char *path = PyUnicode_AsUTF8AndSize(path_obj, &path_len);
  if (!path) {
    PyErr_SetString(PyExc_TypeError, "csv_stream_write path must be a string");
    return nullptr;
  }

  auto result = csv_write_stream_to_path(
      stream_obj, std::string(path, static_cast<std::size_t>(path_len)));
  if (!result.ok()) {
    PyErr_SetString(PyExc_RuntimeError, result.status().message().c_str());
    return nullptr;
  }
  return csv_stats_to_dict(result.ValueOrDie());
}

PyObject *py_csv_stream_write_with_metadata(PyObject *, PyObject *args) {
  PyObject *stream_obj = nullptr;
  PyObject *path_obj = nullptr;
  PyObject *first_row_columns = nullptr;
  PyObject *all_row_columns = nullptr;
  PyObject *row_span_columns = nullptr;
  PyObject *timestamp_columns = nullptr;
  if (!PyArg_ParseTuple(args, "OOOOOO:csv_stream_write_with_metadata",
                        &stream_obj, &path_obj, &first_row_columns,
                        &all_row_columns, &row_span_columns,
                        &timestamp_columns)) {
    return nullptr;
  }
  Py_ssize_t path_len = 0;
  const char *path = PyUnicode_AsUTF8AndSize(path_obj, &path_len);
  if (!path) {
    PyErr_SetString(PyExc_TypeError,
                    "csv_stream_write_with_metadata path must be a string");
    return nullptr;
  }

  ArrowArrayStream *wrapped = make_metadata_stream_wrapper(
      stream_obj, first_row_columns, all_row_columns, row_span_columns,
      timestamp_columns);
  if (!wrapped) {
    return nullptr;
  }
  auto result = csv_write_arrow_stream_to_path(
      wrapped, std::string(path, static_cast<std::size_t>(path_len)));
  schema_sanitizer_stream_free(wrapped);
  if (!result.ok()) {
    PyErr_SetString(PyExc_RuntimeError, result.status().message().c_str());
    return nullptr;
  }
  return csv_stats_to_dict(result.ValueOrDie());
}

PyObject *py_csv_schema_supported(PyObject *, PyObject *args) {
  PyObject *schema_obj = nullptr;
  if (!PyArg_ParseTuple(args, "O:csv_schema_supported", &schema_obj)) {
    return nullptr;
  }
  PyObject *capsule = nullptr;
  ArrowSchema *schema = nullptr;
  if (!acquire_arrow_schema(schema_obj, &capsule, &schema)) {
    PyErr_SetString(PyExc_TypeError,
                    "csv_schema_supported expected a PyArrow schema");
    return nullptr;
  }
  std::unique_ptr<PyObject, decltype(&Py_DECREF)> capsule_owner(capsule,
                                                                Py_DECREF);
  return PyBool_FromLong(csv::schema_is_supported(*schema) ? 1 : 0);
}

} // namespace core_abi3_internal
