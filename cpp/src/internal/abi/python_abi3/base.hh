// Declares shared Python ABI3 extension helpers.
// These definitions keep interpreter ownership and method-table details behind
// the private extension boundary.

#pragma once

#ifndef Py_LIMITED_API
#define Py_LIMITED_API 0x030A0000
#endif

#include <Python.h>

#ifndef _PyCFunction_CAST
#ifdef PyCFunction_CAST
#define _PyCFunction_CAST(func) PyCFunction_CAST(func)
#else
#define _PyCFunction_CAST(func)                                                \
  reinterpret_cast<PyCFunction>(reinterpret_cast<void (*)(void)>(func))
#endif
#endif

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

namespace sanitize {
class ChunkSource;
class Status;
} // namespace sanitize

struct ArrowArrayStream;

namespace core_abi3_internal {

struct NativeContext;

enum class PythonSourceKind { kPath, kText, kStream, kUnknown };
void release_arrow_stream(ArrowArrayStream *stream) noexcept;

struct ArrowStreamDeleter {
  /// Invokes the Arrow stream release callback when its owning smart pointer
  /// leaves scope.
  void operator()(ArrowArrayStream *stream) const noexcept {
    release_arrow_stream(stream);
  }
};

using OwnedArrowStream = std::unique_ptr<ArrowArrayStream, ArrowStreamDeleter>;

/// Wraps an Arrow stream in RAII ownership so its release callback runs exactly
/// once.
inline OwnedArrowStream own_arrow_stream(ArrowArrayStream *stream) noexcept {
  return OwnedArrowStream(stream);
}

// Releases the GIL for one native blocking region and restores it on every
// exit path. Python callbacks reached by the native region acquire it locally.
class ScopedGilRelease final {
public:
  /// Releases the Python GIL until this scope guard is destroyed.
  ScopedGilRelease() noexcept : state_(PyEval_SaveThread()) {}
  /// Disables copying so ownership and cleanup responsibility cannot be
  /// duplicated.
  ScopedGilRelease(const ScopedGilRelease &) = delete;
  /// Disables copying so ownership and cleanup responsibility cannot be
  /// duplicated.
  ScopedGilRelease &operator=(const ScopedGilRelease &) = delete;
  /// Reacquires the Python GIL before control returns to Python-managed code.
  ~ScopedGilRelease() { PyEval_RestoreThread(state_); }

private:
  PyThreadState *state_;
};

// Acquires the GIL for a callback that may run on an arena worker.
class ScopedGilAcquire final {
public:
  /// Acquires the Python GIL for the lifetime of this scope guard.
  ScopedGilAcquire() noexcept : state_(PyGILState_Ensure()) {}
  /// Disables copying so ownership and cleanup responsibility cannot be
  /// duplicated.
  ScopedGilAcquire(const ScopedGilAcquire &) = delete;
  /// Disables copying so ownership and cleanup responsibility cannot be
  /// duplicated.
  ScopedGilAcquire &operator=(const ScopedGilAcquire &) = delete;
  /// Releases the Python GIL state acquired by this callback scope.
  ~ScopedGilAcquire() { PyGILState_Release(state_); }

private:
  PyGILState_STATE state_;
};

/// Releases the Python GIL while invoking the native callable, then reacquires
/// it on return.
template <class Callable> decltype(auto) call_without_gil(Callable &&callable) {
  ScopedGilRelease release;
  return std::forward<Callable>(callable)();
}

void raise_status_error(const sanitize::Status &status);
PyObject *fsencode_path(PyObject *obj);
int bytes_or_str_view(PyObject *obj, const char **out_ptr, Py_ssize_t *out_len);
int tuple_set_item_steal(PyObject *tup, Py_ssize_t index, PyObject *item);
/// Builds the Python diagnostics dictionary from native materialization
/// counters.
inline PyObject *materialization_stats_dict(long long rows_value,
                                            long long batches_value) {
  PyObject *dict = PyDict_New();
  if (!dict) {
    return nullptr;
  }
  PyObject *rows = PyLong_FromLongLong(rows_value);
  PyObject *batches = PyLong_FromLongLong(batches_value);
  if (!rows || !batches ||
      PyDict_SetItemString(dict, "materialized_rows", rows) < 0 ||
      PyDict_SetItemString(dict, "batches", batches) < 0) {
    Py_XDECREF(rows);
    Py_XDECREF(batches);
    Py_DECREF(dict);
    return nullptr;
  }
  Py_DECREF(rows);
  Py_DECREF(batches);
  return dict;
}
int readonly_buffer_view(PyObject *obj, const std::uint8_t **out_ptr,
                         Py_ssize_t *out_len, PyObject **out_owner);
bool check_python_signals();
void install_python_interrupt_check(NativeContext *ctx);
std::shared_ptr<sanitize::ChunkSource>
make_python_reader_chunk_source(PyObject *reader);
PythonSourceKind parse_python_source_kind(const char *source_name) noexcept;
bool python_reader_has_read_seek(PyObject *reader) noexcept;
void set_python_reader_type_error();
PyObject *sequence_item_borrowed_or_new(PyObject *seq, Py_ssize_t index,
                                        bool *borrowed);

} // namespace core_abi3_internal
