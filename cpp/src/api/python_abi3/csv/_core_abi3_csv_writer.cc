/*
 * Implements the Python ABI3 CSV stream-writer wrapper.
 *
 * This file exposes native CSV writing for Arrow C streams.
 *
 * Metadata wrapping and local-path normalization stay in Python; the native
 * side only consumes an Arrow C stream and writes a local CSV file.
 */

#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/abi/python_abi3/methods.hh"

#include "api/python_abi3/arrow_stream/_core_abi3_arrow_stream_lifecycle.hh"
#include "api/python_abi3/metadata/stream/stream.hh"
#include "internal/csv/csv_stream_writer.hh"
#include "internal/output/ordered_text_output.hh"
#include "internal/runtime/process_fd_governor.hh"

#include <fstream>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

namespace core_abi3_internal {
namespace {

namespace csv = sanitize::internal::csv_stream_writer;

class FileCsvOutput final : public csv::Output {
public:
  /// Opens a local output path.
  explicit FileCsvOutput(std::string path) : fd_lease_(1U) {
    if (fd_lease_) {
      out_.open(std::move(path),
                std::ios::out | std::ios::binary | std::ios::trunc);
      if (out_) {
        fd_lease_.mark_opened();
      }
    }
  }

  /// Releases resources retained by `FileCsvOutput` without propagating cleanup
  /// failures.
  ~FileCsvOutput() override {
    sanitize::internal::close_stream_and_commit(out_, fd_lease_);
  }

  /// Returns whether the file opened correctly.
  [[nodiscard]] bool ok() const noexcept { return static_cast<bool>(out_); }

  /// Writes bytes to the file.
  sanitize::Status Write(std::string_view data) override {
    out_.write(data.data(), static_cast<std::streamsize>(data.size()));
    if (!out_) {
      return sanitize::Status::IOError("CSV writer: failed writing output");
    }
    return sanitize::Status::OK();
  }

  /// Flushes the file.
  sanitize::Status Flush() override {
    out_.flush();
    if (!out_) {
      return sanitize::Status::IOError("CSV writer: failed flushing output");
    }
    return sanitize::Status::OK();
  }

private:
  sanitize::internal::ProcessFdPermitLease fd_lease_;
  std::ofstream out_;
};

/// Writes a validated Arrow C stream as CSV to a local path.
sanitize::Result<csv::WriteStats>
csv_write_stream_to_path(PyObject *stream_obj, std::string path,
                         std::int64_t memory_limit_bytes,
                         sanitize::ThreadingMode threading_mode) {
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
  return call_without_gil([&] {
    return csv::write_stream(stream, output, memory_limit_bytes,
                             threading_mode);
  });
}

/// Acquires a Python Arrow stream and writes it as CSV to a local path.
sanitize::Result<csv::WriteStats>
csv_write_arrow_stream_to_path(ArrowArrayStream *stream, std::string path,
                               std::int64_t memory_limit_bytes,
                               sanitize::ThreadingMode threading_mode) {
  FileCsvOutput output(std::move(path));
  if (!output.ok()) {
    return sanitize::Status::IOError("CSV writer: failed opening output");
  }
  return call_without_gil([&] {
    return csv::write_stream(stream, output, memory_limit_bytes,
                             threading_mode);
  });
}

} // namespace

/// Writes a Python Arrow stream to a CSV path and returns row and batch counts.
PyObject *py_csv_stream_write(PyObject *, PyObject *args) {
  PyObject *stream_obj = nullptr;
  PyObject *path_obj = nullptr;
  long long memory_limit_bytes = -1;
  long long threading_mode_value = 0;
  if (!PyArg_ParseTuple(args, "OO|LL:csv_stream_write", &stream_obj, &path_obj,
                        &memory_limit_bytes, &threading_mode_value)) {
    return nullptr;
  }
  Py_ssize_t path_len = 0;
  const char *path = PyUnicode_AsUTF8AndSize(path_obj, &path_len);
  if (!path) {
    PyErr_SetString(PyExc_TypeError, "csv_stream_write path must be a string");
    return nullptr;
  }

  auto mode_result =
      sanitize::internal::ordered_text_output::threading_mode_from_int(
          threading_mode_value);
  if (!mode_result.ok()) {
    PyErr_SetString(PyExc_ValueError, mode_result.status().message().c_str());
    return nullptr;
  }
  auto result = csv_write_stream_to_path(
      stream_obj, std::string(path, static_cast<std::size_t>(path_len)),
      memory_limit_bytes, mode_result.ValueOrDie());
  if (!result.ok()) {
    PyErr_SetString(PyExc_RuntimeError, result.status().message().c_str());
    return nullptr;
  }
  const auto &stats = result.ValueOrDie();
  return materialization_stats_dict(stats.materialized_rows, stats.batches);
}

/// Adds generated metadata columns, writes the Arrow stream as CSV, and returns
/// counts.
PyObject *py_csv_stream_write_with_metadata(PyObject *, PyObject *args) {
  PyObject *stream_obj = nullptr;
  PyObject *path_obj = nullptr;
  PyObject *first_row_columns = nullptr;
  PyObject *all_row_columns = nullptr;
  PyObject *row_span_columns = nullptr;
  PyObject *timestamp_columns = nullptr;
  long long memory_limit_bytes = -1;
  long long threading_mode_value = 0;
  if (!PyArg_ParseTuple(args, "OOOOOO|LL:csv_stream_write_with_metadata",
                        &stream_obj, &path_obj, &first_row_columns,
                        &all_row_columns, &row_span_columns, &timestamp_columns,
                        &memory_limit_bytes, &threading_mode_value)) {
    return nullptr;
  }
  Py_ssize_t path_len = 0;
  const char *path = PyUnicode_AsUTF8AndSize(path_obj, &path_len);
  if (!path) {
    PyErr_SetString(PyExc_TypeError,
                    "csv_stream_write_with_metadata path must be a string");
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
  auto result = csv_write_arrow_stream_to_path(
      wrapped.get(), std::string(path, static_cast<std::size_t>(path_len)),
      memory_limit_bytes, mode_result.ValueOrDie());
  if (!result.ok()) {
    PyErr_SetString(PyExc_RuntimeError, result.status().message().c_str());
    return nullptr;
  }
  const auto &stats = result.ValueOrDie();
  return materialization_stats_dict(stats.materialized_rows, stats.batches);
}

/// Reports whether the native CSV writer supports a supplied PyArrow schema.
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
