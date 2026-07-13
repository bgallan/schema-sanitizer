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

namespace sanitize {
class ChunkSource;
}

namespace core_abi3_internal {

enum class PythonSourceKind { kPath, kText, kStream, kUnknown };

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
