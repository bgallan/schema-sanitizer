/*
 * Python ABI3 Parquet stream writer wrapper.
 *
 * The native writer intentionally supports a conservative flat primitive
 * subset and lets Python fall back to the existing PyArrow writer otherwise.
 */
#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/abi/python_abi3/methods.hh"

#include "api/python_abi3/arrow_stream/_core_abi3_arrow_stream_lifecycle.hh"
#include "api/python_abi3/metadata/stream/stream.hh"
#include "internal/output/ordered_text_output.hh"
#include "internal/parquet/parquet_stream_writer.hh"

#include <fstream>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

namespace core_abi3_internal {
namespace {

namespace parquet = sanitize::internal::parquet_stream_writer;

class FileParquetOutput final : public parquet::Output {
public:
  // Opens a local output path.
  explicit FileParquetOutput(std::string path)
      : out_(std::move(path),
             std::ios::out | std::ios::binary | std::ios::trunc) {}

  // Returns whether the file opened correctly.
  [[nodiscard]] bool ok() const noexcept { return static_cast<bool>(out_); }

  // Writes bytes to the file.
  sanitize::Status Write(std::string_view data) override {
    out_.write(data.data(), static_cast<std::streamsize>(data.size()));
    if (!out_) {
      return sanitize::Status::IOError(
          "native Parquet writer: failed writing output");
    }
    return sanitize::Status::OK();
  }

  // Flushes the file.
  sanitize::Status Flush() override {
    out_.flush();
    if (!out_) {
      return sanitize::Status::IOError(
          "native Parquet writer: failed flushing output");
    }
    return sanitize::Status::OK();
  }

private:
  std::ofstream out_;
};

// Writes a raw Arrow C stream pointer to a local Parquet path.
sanitize::Status
parquet_write_arrow_stream_to_path(ArrowArrayStream *stream, std::string path,
                                   const parquet::WriterOptions &options) {
  FileParquetOutput output(std::move(path));
  if (!output.ok()) {
    return sanitize::Status::IOError(
        "native Parquet writer: failed opening output");
  }
  return call_without_gil(
      [&] { return parquet::write_stream(stream, output, options); });
}

// Writes a Python Arrow C stream exporter to a local Parquet path.
sanitize::Status
parquet_write_stream_to_path(PyObject *stream_obj, std::string path,
                             const parquet::WriterOptions &options) {
  PyObject *capsule = nullptr;
  ArrowArrayStream *stream = nullptr;
  if (!acquire_arrow_stream(stream_obj, &capsule, &stream)) {
    return sanitize::Status::Invalid(
        "native Parquet writer: object does not export Arrow C stream");
  }
  std::unique_ptr<PyObject, decltype(&Py_DECREF)> capsule_owner(capsule,
                                                                Py_DECREF);
  FileParquetOutput output(std::move(path));
  if (!output.ok()) {
    return sanitize::Status::IOError(
        "native Parquet writer: failed opening output");
  }
  return call_without_gil(
      [&] { return parquet::write_stream(stream, output, options); });
}

} // namespace

// Python wrapper for writing one Arrow stream as Parquet.
PyObject *py_parquet_stream_write(PyObject *, PyObject *args) {
  PyObject *stream_obj = nullptr;
  PyObject *path_obj = nullptr;
  const char *compression = nullptr;
  int gzip_level = -1;
  long long memory_limit_bytes = -1;
  long long threading_mode_value = 0;
  if (!PyArg_ParseTuple(args, "OOsiL|L:parquet_stream_write", &stream_obj,
                        &path_obj, &compression, &gzip_level,
                        &memory_limit_bytes, &threading_mode_value)) {
    return nullptr;
  }
  Py_ssize_t path_len = 0;
  const char *path = PyUnicode_AsUTF8AndSize(path_obj, &path_len);
  if (!path) {
    PyErr_SetString(PyExc_TypeError,
                    "parquet_stream_write path must be a string");
    return nullptr;
  }

  auto mode_result =
      sanitize::internal::ordered_text_output::threading_mode_from_int(
          threading_mode_value);
  if (!mode_result.ok()) {
    PyErr_SetString(PyExc_ValueError, mode_result.status().message().c_str());
    return nullptr;
  }
  parquet::WriterOptions options{
      .memory_limit_bytes = memory_limit_bytes,
      .compression = compression ? compression : "",
      .gzip_level = gzip_level,
      .threading_mode = mode_result.ValueOrDie(),
  };
  auto st = parquet_write_stream_to_path(
      stream_obj, std::string(path, static_cast<std::size_t>(path_len)),
      options);
  if (!st.ok()) {
    PyErr_SetString(PyExc_RuntimeError, st.message().c_str());
    return nullptr;
  }
  Py_RETURN_NONE;
}

// Python wrapper for writing a metadata-augmented Arrow stream as Parquet.
PyObject *py_parquet_stream_write_with_metadata(PyObject *, PyObject *args) {
  PyObject *stream_obj = nullptr;
  PyObject *path_obj = nullptr;
  PyObject *first_row_columns = nullptr;
  PyObject *all_row_columns = nullptr;
  PyObject *row_span_columns = nullptr;
  PyObject *timestamp_columns = nullptr;
  const char *compression = nullptr;
  int gzip_level = -1;
  long long memory_limit_bytes = -1;
  long long threading_mode_value = 0;
  if (!PyArg_ParseTuple(args, "OOOOOOsiL|L:parquet_stream_write_with_metadata",
                        &stream_obj, &path_obj, &first_row_columns,
                        &all_row_columns, &row_span_columns, &timestamp_columns,
                        &compression, &gzip_level, &memory_limit_bytes,
                        &threading_mode_value)) {
    return nullptr;
  }
  Py_ssize_t path_len = 0;
  const char *path = PyUnicode_AsUTF8AndSize(path_obj, &path_len);
  if (!path) {
    PyErr_SetString(PyExc_TypeError,
                    "parquet_stream_write_with_metadata path must be a string");
    return nullptr;
  }

  ArrowArrayStream *wrapped = make_metadata_stream_wrapper(
      stream_obj, first_row_columns, all_row_columns, row_span_columns,
      timestamp_columns, memory_limit_bytes);
  if (!wrapped) {
    return nullptr;
  }
  auto mode_result =
      sanitize::internal::ordered_text_output::threading_mode_from_int(
          threading_mode_value);
  if (!mode_result.ok()) {
    schema_sanitizer_stream_free(wrapped);
    PyErr_SetString(PyExc_ValueError, mode_result.status().message().c_str());
    return nullptr;
  }
  parquet::WriterOptions options{
      .memory_limit_bytes = memory_limit_bytes,
      .compression = compression ? compression : "",
      .gzip_level = gzip_level,
      .threading_mode = mode_result.ValueOrDie(),
  };
  auto st = parquet_write_arrow_stream_to_path(
      wrapped, std::string(path, static_cast<std::size_t>(path_len)), options);
  schema_sanitizer_stream_free(wrapped);
  if (!st.ok()) {
    PyErr_SetString(PyExc_RuntimeError, st.message().c_str());
    return nullptr;
  }
  Py_RETURN_NONE;
}

} // namespace core_abi3_internal
