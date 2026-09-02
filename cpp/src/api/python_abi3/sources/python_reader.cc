// Adapts seekable Python readers to the native ChunkSource contract. The
// adapter holds Python references under the GIL while serving bounded native
// chunks.

#include "internal/abi/python_abi3/base.hh"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "sanitize/core/status.hh"
#include "sanitize/ingest/chunk_source.hh"

namespace core_abi3_internal {
namespace {

/// Converts the active Python exception into a native invalid-status message.
sanitize::Status python_reader_error_status(const char *where) {
  PyObject *type = nullptr;
  PyObject *value = nullptr;
  PyObject *traceback = nullptr;
  PyErr_Fetch(&type, &value, &traceback);
  PyErr_NormalizeException(&type, &value, &traceback);

  std::string message(where ? where : "python stream");
  message += ": ";
  if (value) {
    PyObject *text = PyObject_Str(value);
    if (text) {
      Py_ssize_t size = 0;
      const char *utf8 = PyUnicode_AsUTF8AndSize(text, &size);
      if (utf8 && size > 0) {
        message.append(utf8, static_cast<std::size_t>(size));
      } else {
        message += "Python stream error";
      }
      Py_DECREF(text);
    } else {
      PyErr_Clear();
      message += "Python stream error";
    }
  } else {
    message += "Python stream error";
  }

  Py_XDECREF(type);
  Py_XDECREF(value);
  Py_XDECREF(traceback);
  return sanitize::Status::Invalid(message);
}

/// Releases a retained Python object reference while holding the GIL.
void decref_python_reader_object_with_gil(const void *value) {
  if (!value || !Py_IsInitialized()) {
    return;
  }
  PyGILState_STATE gil = PyGILState_Ensure();
  Py_DECREF(reinterpret_cast<PyObject *>(const_cast<void *>(value)));
  PyGILState_Release(gil);
}

class PythonReaderChunkSource final : public sanitize::ChunkSource {
public:
  /// Retains a seekable Python reader and initializes native chunk offset
  /// tracking.
  explicit PythonReaderChunkSource(PyObject *reader) : reader_(reader) {
    Py_INCREF(reader_);
    read_ = PyObject_GetAttrString(reader_, "read");
    if (!read_) {
      PyErr_Clear();
    }
    seek_ = PyObject_GetAttrString(reader_, "seek");
    if (!seek_) {
      PyErr_Clear();
    }
  }

  /// Releases resources retained by `PythonReaderChunkSource` without
  /// propagating cleanup failures.
  ~PythonReaderChunkSource() override {
    if (!reader_ || !Py_IsInitialized()) {
      return;
    }
    PyGILState_STATE gil = PyGILState_Ensure();
    Py_XDECREF(seek_);
    Py_XDECREF(read_);
    Py_DECREF(reader_);
    PyGILState_Release(gil);
  }

  /// Rewinds the Python source adapter and clears its per-pass state.
  sanitize::Status Reset() override {
    if (!seek_) {
      return sanitize::Status::Invalid(
          "Python stream reset failed: missing seek method");
    }
    PyGILState_STATE gil = PyGILState_Ensure();
    PyObject *result = PyObject_CallFunction(seek_, "i", 0);
    if (!result) {
      auto status = python_reader_error_status("Python stream reset failed");
      PyGILState_Release(gil);
      return status;
    }
    Py_DECREF(result);
    PyGILState_Release(gil);
    pos_ = 0;
    full_view_.reset();
    return sanitize::Status::OK();
  }

  /// Returns the next bounded byte chunk from the Python source adapter,
  /// advancing its cursor.
  sanitize::Result<sanitize::Chunk> NextChunk(int64_t max_bytes) override {
    if (max_bytes <= 0) {
      return sanitize::Status::Invalid("NextChunk: max_bytes must be > 0");
    }
    if (!read_) {
      return sanitize::Status::Invalid(
          "Python stream read failed: missing read method");
    }

    PyGILState_STATE gil = PyGILState_Ensure();
    PyObject *raw =
        PyObject_CallFunction(read_, "L", static_cast<long long>(max_bytes));
    if (!raw) {
      auto status = python_reader_error_status("Python stream read failed");
      PyGILState_Release(gil);
      return status;
    }
    if (raw == Py_None) {
      Py_DECREF(raw);
      PyGILState_Release(gil);
      return sanitize::Chunk{.owner = nullptr,
                             .data = std::string_view{},
                             .base_offset = pos_,
                             .source_name_owner = {},
                             .source_name = {},
                             .source_index = 0,
                             .has_source_index = false};
    }

    PyObject *bytes = PyBytes_FromObject(raw);
    Py_DECREF(raw);
    if (!bytes) {
      auto status = python_reader_error_status(
          "Python stream read did not return a bytes-like object");
      PyGILState_Release(gil);
      return status;
    }

    char *data = nullptr;
    Py_ssize_t size = 0;
    if (PyBytes_AsStringAndSize(bytes, &data, &size) != 0 || !data ||
        size < 0) {
      Py_DECREF(bytes);
      PyErr_Clear();
      PyGILState_Release(gil);
      return sanitize::Status::Invalid(
          "Python stream read returned invalid bytes");
    }

    const std::size_t base = pos_;
    pos_ += static_cast<std::size_t>(size);
    std::shared_ptr<const void> owner(bytes,
                                      &decref_python_reader_object_with_gil);
    sanitize::Chunk chunk{
        .owner = std::move(owner),
        .data = std::string_view(data, static_cast<std::size_t>(size)),
        .base_offset = base,
        .source_name_owner = {},
        .source_name = {},
        .source_index = 0,
        .has_source_index = false,
    };
    PyGILState_Release(gil);
    return chunk;
  }

  /// Exposes the current Python source adapter bytes without extending their
  /// documented lifetime.
  sanitize::Result<sanitize::Chunk> View() override {
    if (!full_view_) {
      SAN_RETURN_NOT_OK(Reset());
      auto bytes = std::make_shared<std::string>();
      for (;;) {
        SAN_ASSIGN_OR_RAISE(auto chunk, NextChunk(int64_t{1} << 20));
        if (chunk.data.empty()) {
          break;
        }
        bytes->append(chunk.data);
      }
      full_view_ = std::move(bytes);
      SAN_RETURN_NOT_OK(Reset());
    }
    return sanitize::Chunk{
        .owner = full_view_,
        .data = std::string_view(*full_view_),
        .base_offset = 0,
        .source_name_owner = {},
        .source_name = {},
        .source_index = 0,
        .has_source_index = false,
    };
  }

private:
  PyObject *reader_ = nullptr;
  PyObject *read_ = nullptr;
  PyObject *seek_ = nullptr;
  std::size_t pos_ = 0;
  std::shared_ptr<std::string> full_view_;
};

} // namespace

/// Creates a native chunk-source adapter that retains the supplied Python
/// reader.
std::shared_ptr<sanitize::ChunkSource>
make_python_reader_chunk_source(PyObject *reader) {
  return std::make_shared<PythonReaderChunkSource>(reader);
}

} // namespace core_abi3_internal
