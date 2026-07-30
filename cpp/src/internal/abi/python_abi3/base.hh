// Declares shared Python ABI3 bridge helpers.

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

#include "internal/abi/schema_sanitizer_c_bridge.hh"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

namespace sanitize {
class ChunkSource;
}

namespace core_abi3_internal {

enum class PythonSourceKind { kPath, kText, kStream, kUnknown };

// Releases the GIL for one native blocking region and restores it on every
// exit path. Python callbacks reached by the native region acquire it locally.
class ScopedGilRelease final {
public:
  ScopedGilRelease() noexcept : state_(PyEval_SaveThread()) {}
  ScopedGilRelease(const ScopedGilRelease &) = delete;
  ScopedGilRelease &operator=(const ScopedGilRelease &) = delete;
  ~ScopedGilRelease() { PyEval_RestoreThread(state_); }

private:
  PyThreadState *state_;
};

// Acquires the GIL for a callback that may run on an arena worker.
class ScopedGilAcquire final {
public:
  ScopedGilAcquire() noexcept : state_(PyGILState_Ensure()) {}
  ScopedGilAcquire(const ScopedGilAcquire &) = delete;
  ScopedGilAcquire &operator=(const ScopedGilAcquire &) = delete;
  ~ScopedGilAcquire() { PyGILState_Release(state_); }

private:
  PyGILState_STATE state_;
};

template <class Callable> decltype(auto) call_without_gil(Callable &&callable) {
  ScopedGilRelease release;
  return std::forward<Callable>(callable)();
}

void raise_status_error(int status, char *err);
PyObject *fsencode_path(PyObject *obj);
int bytes_or_str_view(PyObject *obj, const char **out_ptr, Py_ssize_t *out_len);
int tuple_set_item_steal(PyObject *tup, Py_ssize_t index, PyObject *item);
int readonly_buffer_view(PyObject *obj, const std::uint8_t **out_ptr,
                         Py_ssize_t *out_len, PyObject **out_owner);
bool check_python_signals();
void install_python_interrupt_check(schema_sanitizer_context *ctx);
std::shared_ptr<sanitize::ChunkSource>
make_python_reader_chunk_source(PyObject *reader);
PythonSourceKind parse_python_source_kind(const char *source_name) noexcept;
bool python_reader_has_read_seek(PyObject *reader) noexcept;
void set_python_reader_type_error();
PyObject *sequence_item_borrowed_or_new(PyObject *seq, Py_ssize_t index,
                                        bool *borrowed);

} // namespace core_abi3_internal
