/*
 * Shared Python ABI3 source and sequence helpers.
 *
 * These helpers keep source-selected sink wrappers and folder utility wrappers
 * aligned without duplicating Python protocol checks or sequence item handling.
 */
#include "internal/abi/core_abi3_internal.hh"

#include <string_view>

namespace core_abi3_internal {

PythonSourceKind parse_python_source_kind(const char *source_name) noexcept {
  const std::string_view source(source_name ? source_name : "");
  if (source == "path")
    return PythonSourceKind::kPath;
  if (source == "text")
    return PythonSourceKind::kText;
  if (source == "stream")
    return PythonSourceKind::kStream;
  return PythonSourceKind::kUnknown;
}

bool python_reader_has_read_seek(PyObject *reader) noexcept {
  return reader != nullptr && PyObject_HasAttrString(reader, "read") &&
         PyObject_HasAttrString(reader, "seek");
}

void set_python_reader_type_error() {
  PyErr_SetString(PyExc_TypeError,
                  "reader input must expose read(max_bytes) and seek(0)");
}

PyObject *sequence_item_borrowed_or_new(PyObject *seq, Py_ssize_t index,
                                        bool *borrowed) {
  *borrowed = true;
  if (PyList_Check(seq)) {
    return PyList_GetItem(seq, index);
  }
  if (PyTuple_Check(seq)) {
    return PyTuple_GetItem(seq, index);
  }
  *borrowed = false;
  return PySequence_GetItem(seq, index);
}

} // namespace core_abi3_internal
