/*
 * Python ABI3 wrapper for compacting in-memory JSON bytes.
 *
 * This small translation unit keeps the memory-input JSON ABI entry point
 * separate from shared JSON value encoders and local-file compaction wrappers.
 */
#include "api/python_abi3/_core_abi3_json_tools.hh"

#include <memory>
#include <string>
#include <string_view>

namespace core_abi3_internal {

PyObject *py_json_compact_bytes(PyObject *, PyObject *args) {
  PyObject *input_obj = nullptr;
  if (!PyArg_ParseTuple(args, "O:json_compact_bytes", &input_obj)) {
    return nullptr;
  }

  Py_buffer view{};
  if (PyObject_GetBuffer(input_obj, &view, PyBUF_CONTIG_RO) != 0) {
    return nullptr;
  }
  std::unique_ptr<Py_buffer, decltype(&PyBuffer_Release)> view_owner(
      &view, PyBuffer_Release);
  std::string_view text(static_cast<const char *>(view.buf),
                        static_cast<std::size_t>(view.len));
  auto compact = compact_json_document(text);
  if (!compact.ok()) {
    PyErr_SetString(PyExc_ValueError, compact.status().message().c_str());
    return nullptr;
  }
  const std::string &compact_value = *compact;
  return PyBytes_FromStringAndSize(
      compact_value.data(), static_cast<Py_ssize_t>(compact_value.size()));
}

PyObject *py_json_array_to_jsonl_bytes(PyObject *, PyObject *args) {
  PyObject *input_obj = nullptr;
  if (!PyArg_ParseTuple(args, "O:json_array_to_jsonl_bytes", &input_obj)) {
    return nullptr;
  }

  Py_buffer view{};
  if (PyObject_GetBuffer(input_obj, &view, PyBUF_CONTIG_RO) != 0) {
    return nullptr;
  }
  std::unique_ptr<Py_buffer, decltype(&PyBuffer_Release)> view_owner(
      &view, PyBuffer_Release);
  std::string_view text(static_cast<const char *>(view.buf),
                        static_cast<std::size_t>(view.len));
  auto jsonl = json_array_document_to_jsonl(text);
  if (!jsonl.ok()) {
    PyErr_SetString(PyExc_ValueError, jsonl.status().message().c_str());
    return nullptr;
  }
  const std::string &jsonl_value = *jsonl;
  return PyBytes_FromStringAndSize(jsonl_value.data(),
                                   static_cast<Py_ssize_t>(jsonl_value.size()));
}

} // namespace core_abi3_internal
