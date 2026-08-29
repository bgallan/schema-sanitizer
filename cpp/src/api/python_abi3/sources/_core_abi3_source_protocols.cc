/*
 * Implements Python ABI3 source-kind and sequence protocol adapters.
 *
 * These helpers keep source-selected sink wrappers and folder utility wrappers
 * aligned without duplicating Python protocol checks or sequence item handling.
 */

#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/abi/python_abi3/methods.hh"

#include <string_view>

namespace core_abi3_internal {

/// Maps the Python source-kind spelling to the native path, text, or stream
/// enum.
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

/// Reports whether a Python reader exposes both callable `read` and `seek`
/// methods.
bool python_reader_has_read_seek(PyObject *reader) noexcept {
  return reader != nullptr && PyObject_HasAttrString(reader, "read") &&
         PyObject_HasAttrString(reader, "seek");
}

/// Raises the protocol `TypeError` used when a reader lacks callable `read` or
/// `seek`.
void set_python_reader_type_error() {
  PyErr_SetString(PyExc_TypeError,
                  "reader input must expose read(max_bytes) and seek(0)");
}

/// Fetches a Python sequence item while recording whether its reference is
/// borrowed.
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
