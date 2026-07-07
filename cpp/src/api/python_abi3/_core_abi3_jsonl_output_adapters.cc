/*
 * ABI3 JSONL concrete output adapters.
 *
 * File, Python-object, and string outputs live here so the Python method
 * wrappers only need to parse arguments and select the right adapter.
 */
#include "api/python_abi3/_core_abi3_jsonl_output_adapters.hh"

#include "api/python_abi3/_core_abi3_stream_lifecycle.hh"
#include "internal/json/jsonl_stream_writer.hh"

#include <fstream>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

namespace core_abi3_internal {
namespace {

namespace jsonl = sanitize::internal::jsonl_stream_writer;

class FileJsonlOutput final : public jsonl::Output {
public:
  // Opens a local output path.
  explicit FileJsonlOutput(std::string path)
      : out_(std::move(path),
             std::ios::out | std::ios::binary | std::ios::trunc) {}

  // Returns whether the file opened correctly.
  [[nodiscard]] bool ok() const noexcept { return static_cast<bool>(out_); }

  // Writes bytes to the file.
  sanitize::Status Write(std::string_view data) override {
    out_.write(data.data(), static_cast<std::streamsize>(data.size()));
    if (!out_) {
      return sanitize::Status::IOError("JSONL writer: failed writing output");
    }
    return sanitize::Status::OK();
  }

  // Flushes the file.
  sanitize::Status Flush() override {
    out_.flush();
    if (!out_) {
      return sanitize::Status::IOError("JSONL writer: failed flushing output");
    }
    return sanitize::Status::OK();
  }

private:
  std::ofstream out_;
};

class PythonJsonlOutput final : public jsonl::Output {
public:
  // Retains a Python output object with write(bytes) and optional flush().
  explicit PythonJsonlOutput(PyObject *writer) : writer_(writer) {
    Py_XINCREF(writer_);
    write_ = PyObject_GetAttrString(writer_, "write");
    if (!write_) {
      error_ = "JSONL writer: output object does not expose write(bytes)";
      PyErr_Clear();
    }
    flush_ = PyObject_GetAttrString(writer_, "flush");
    if (!flush_) {
      PyErr_Clear();
    }
  }

  // Releases the Python output object.
  ~PythonJsonlOutput() override {
    Py_XDECREF(flush_);
    Py_XDECREF(write_);
    Py_XDECREF(writer_);
  }

  // Writes bytes through the Python output object.
  sanitize::Status Write(std::string_view data) override {
    buffer_.append(data.data(), data.size());
    if (buffer_.size() < kFlushThresholdBytes) {
      return sanitize::Status::OK();
    }
    return Drain();
  }

  // Flushes the Python output object when supported.
  sanitize::Status Flush() override {
    auto status = Drain();
    if (!status.ok()) {
      return status;
    }
    if (!writer_) {
      return sanitize::Status::Invalid("JSONL writer: output object is null");
    }
    if (!flush_) {
      return sanitize::Status::OK();
    }
    PyObject *result = PyObject_CallObject(flush_, nullptr);
    if (!result) {
      return sanitize::Status::IOError("JSONL writer: Python flush failed");
    }
    Py_DECREF(result);
    return sanitize::Status::OK();
  }

private:
  static constexpr std::size_t kFlushThresholdBytes = 4U << 20;

  // Writes buffered bytes through the Python output object.
  sanitize::Status Drain() {
    if (buffer_.empty()) {
      return sanitize::Status::OK();
    }
    if (!writer_) {
      return sanitize::Status::Invalid("JSONL writer: output object is null");
    }
    if (!write_) {
      return sanitize::Status::Invalid(error_);
    }
    PyObject *bytes = PyBytes_FromStringAndSize(
        buffer_.data(), static_cast<Py_ssize_t>(buffer_.size()));
    if (!bytes) {
      return sanitize::Status::OutOfMemory(
          "JSONL writer: failed allocating Python bytes");
    }
    PyObject *result = PyObject_CallFunctionObjArgs(write_, bytes, nullptr);
    Py_DECREF(bytes);
    if (!result) {
      return sanitize::Status::IOError("JSONL writer: Python write failed");
    }
    Py_DECREF(result);
    buffer_.clear();
    return sanitize::Status::OK();
  }

  PyObject *writer_ = nullptr;
  PyObject *write_ = nullptr;
  PyObject *flush_ = nullptr;
  const char *error_ = nullptr;
  std::string buffer_;
};

class StringJsonlOutput final : public jsonl::Output {
public:
  // Appends bytes into the referenced string buffer.
  explicit StringJsonlOutput(std::string *out) : out_(out) {}

  // Writes bytes into the referenced string buffer.
  sanitize::Status Write(std::string_view data) override {
    out_->append(data.data(), data.size());
    return sanitize::Status::OK();
  }

  // Matches the Output interface; string output has no flush operation.
  sanitize::Status Flush() override { return sanitize::Status::OK(); }

private:
  std::string *out_;
};

// Writes all batches from a Python Arrow C stream export to the selected
// output.
sanitize::Result<jsonl::WriteStats> write_python_stream(PyObject *stream_obj,
                                                        jsonl::Output &output) {
  PyObject *capsule = nullptr;
  ArrowArrayStream *stream = nullptr;
  if (!acquire_arrow_stream(stream_obj, &capsule, &stream)) {
    return sanitize::Status::Invalid(
        "JSONL writer: object does not export Arrow C stream");
  }
  std::unique_ptr<PyObject, decltype(&Py_DECREF)> capsule_owner(capsule,
                                                                Py_DECREF);
  return jsonl::write_stream(stream, output);
}

} // namespace

sanitize::Result<jsonl::WriteStats>
jsonl_write_stream_to_path(PyObject *stream_obj, std::string path) {
  FileJsonlOutput output(std::move(path));
  if (!output.ok()) {
    return sanitize::Status::IOError("JSONL writer: failed opening output");
  }
  return write_python_stream(stream_obj, output);
}

sanitize::Result<jsonl::WriteStats>
jsonl_write_arrow_stream_to_path(ArrowArrayStream *stream, std::string path) {
  FileJsonlOutput output(std::move(path));
  if (!output.ok()) {
    return sanitize::Status::IOError("JSONL writer: failed opening output");
  }
  return jsonl::write_stream(stream, output);
}

sanitize::Result<jsonl::WriteStats>
jsonl_write_stream_to_python(PyObject *stream_obj, PyObject *output_obj) {
  PythonJsonlOutput output(output_obj);
  return write_python_stream(stream_obj, output);
}

sanitize::Status jsonl_write_batch_to_string(ArrowSchema &schema,
                                             ArrowArray &array,
                                             std::string *out) {
  StringJsonlOutput output(out);
  return jsonl::write_batch(output, schema, array);
}

} // namespace core_abi3_internal
