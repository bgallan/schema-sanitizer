/*
 * Python ABI3 helper for validating local XML directory row tags.
 */
#include "api/python_abi3/_core_abi3_json_tools.hh"
#include "api/python_abi3/_core_abi3_xml_folder_parts.hh"

#include <memory>
#include <string>

namespace core_abi3_internal {

PyObject *py_xml_folder_effective_row_tag(PyObject *, PyObject *args) {
  PyObject *paths_obj = nullptr;
  PyObject *requested_obj = nullptr;
  long long memory_limit_bytes = -1;
  if (!PyArg_ParseTuple(args, "OOL:xml_folder_effective_row_tag", &paths_obj,
                        &requested_obj, &memory_limit_bytes)) {
    return nullptr;
  }

  std::string effective;
  if (!xml_folder::unicode_or_none_to_string(requested_obj, &effective)) {
    return nullptr;
  }
  PyObject *paths = PySequence_Fast(paths_obj, "paths must be a sequence");
  if (!paths) {
    return nullptr;
  }
  std::unique_ptr<PyObject, decltype(&Py_DECREF)> paths_owner(paths, Py_DECREF);
  const Py_ssize_t n = PySequence_Size(paths);
  if (n < 0) {
    return nullptr;
  }

  std::string raw;
  for (Py_ssize_t i = 0; i < n; ++i) {
    if (!check_python_signals()) {
      return nullptr;
    }
    bool borrowed = false;
    PyObject *path_obj = sequence_item_borrowed_or_new(paths, i, &borrowed);
    if (!path_obj) {
      return nullptr;
    }
    std::unique_ptr<PyObject, decltype(&Py_DECREF)> path_owner(
        borrowed ? nullptr : path_obj, Py_DECREF);
    if (!xml_folder::validate_xml_file_root(path_obj, memory_limit_bytes, &raw,
                                            &effective)) {
      return nullptr;
    }
  }
  return PyUnicode_FromStringAndSize(effective.data(),
                                     static_cast<Py_ssize_t>(effective.size()));
}

} // namespace core_abi3_internal
