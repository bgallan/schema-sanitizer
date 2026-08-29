/*
 * Implements the Python ABI3 helper that validates local XML directory row
 * tags.
 *
 * Root scanning, contextual errors, and the Python entry point live together
 * because they form one directory-validation operation with no other callers.
 */

#include "api/python_abi3/json/_core_abi3_json_tools.hh"
#include "internal/abi/python_abi3/methods.hh"
#include "internal/parsing/xml/token_match.hh"

#include <cstddef>
#include <memory>
#include <string>
#include <string_view>

namespace core_abi3_internal {
namespace {

namespace xml_scan = sanitize::internal::xml_tokens;

/// Decodes a filesystem path into a printable value for contextual XML errors.
std::string path_display(PyObject *path_obj) {
  PyObject *path_text = PyObject_Str(path_obj);
  if (!path_text) {
    PyErr_Clear();
    return {};
  }
  std::unique_ptr<PyObject, decltype(&Py_DECREF)> owner(path_text, Py_DECREF);
  Py_ssize_t size = 0;
  const char *data = PyUnicode_AsUTF8AndSize(path_text, &size);
  if (!data) {
    PyErr_Clear();
    return {};
  }
  return std::string(data, static_cast<std::size_t>(size));
}

/// Raises a Python `ValueError` containing the XML path and validation message.
bool raise_xml_error(PyObject *path_obj, std::string_view message) {
  std::string out = "Invalid XML file";
  const std::string path = path_display(path_obj);
  if (!path.empty()) {
    out.push_back(' ');
    out += path;
  }
  out += ": ";
  out.append(message);
  PyErr_SetString(PyExc_ValueError, out.c_str());
  return false;
}

/// Scans the XML prolog and returns the document root element name.
bool xml_root_tag_name(std::string_view text, PyObject *path_obj,
                       std::string *out) {
  out->clear();
  std::size_t pos = 0;
  while (true) {
    while (pos < text.size() && xml_scan::is_xml_whitespace(text[pos])) {
      ++pos;
    }
    if (xml_scan::starts_with_at(text, pos, "<?")) {
      const std::size_t end = text.find("?>", pos + 2);
      if (end == std::string_view::npos) {
        return raise_xml_error(path_obj, "unterminated processing instruction");
      }
      pos = end + 2;
      continue;
    }
    if (xml_scan::starts_with_at(text, pos, "<!--")) {
      const std::size_t end = text.find("-->", pos + 4);
      if (end == std::string_view::npos) {
        return raise_xml_error(path_obj, "unterminated comment");
      }
      pos = end + 3;
      continue;
    }
    if (xml_scan::starts_with_at(text, pos, "<!")) {
      PyErr_SetString(PyExc_ValueError, "Unsupported XML declaration");
      return false;
    }
    break;
  }

  if (pos >= text.size() || text[pos] != '<' ||
      xml_scan::starts_with_at(text, pos, "</")) {
    return raise_xml_error(path_obj, "expected root element");
  }
  const std::size_t start = pos + 1;
  std::size_t end = start;
  while (end < text.size() && !xml_scan::is_xml_whitespace(text[end]) &&
         text[end] != '/' && text[end] != '>' && text[end] != '=') {
    ++end;
  }
  if (end == start) {
    return raise_xml_error(path_obj, "expected root element name");
  }
  out->assign(text.substr(start, end - start));
  return true;
}

/// Parses optional Python Unicode into a native XML option string.
bool unicode_or_none_to_string(PyObject *obj, std::string *out) {
  out->clear();
  if (obj == Py_None) {
    return true;
  }
  Py_ssize_t size = 0;
  const char *data = PyUnicode_AsUTF8AndSize(obj, &size);
  if (!data) {
    return false;
  }
  out->assign(data, static_cast<std::size_t>(size));
  return true;
}

/// Validates an XML file's root element against the expected folder schema.
bool validate_xml_file_root(PyObject *path_obj, long long memory_limit_bytes,
                            std::string *raw, std::string *effective) {
  if (!read_local_file_bytes(path_obj, memory_limit_bytes, raw)) {
    return false;
  }
  std::string root;
  if (!xml_root_tag_name(*raw, path_obj, &root)) {
    return false;
  }
  if (effective->empty()) {
    *effective = std::move(root);
    return true;
  }
  if (*effective == root) {
    return true;
  }
  std::string message = "xml directory input expected root tag <";
  message += *effective;
  message += "> but ";
  message += path_display(path_obj);
  message += " uses <";
  message += root;
  message += ">";
  PyErr_SetString(PyExc_ValueError, message.c_str());
  return false;
}

} // namespace

/// Validates XML root tags across paths and returns the effective row tag.
PyObject *py_xml_folder_effective_row_tag(PyObject *, PyObject *args) {
  PyObject *paths_obj = nullptr;
  PyObject *requested_obj = nullptr;
  long long memory_limit_bytes = -1;
  if (!PyArg_ParseTuple(args, "OOL:xml_folder_effective_row_tag", &paths_obj,
                        &requested_obj, &memory_limit_bytes)) {
    return nullptr;
  }

  std::string effective;
  if (!unicode_or_none_to_string(requested_obj, &effective)) {
    return nullptr;
  }
  PyObject *paths = PySequence_Fast(paths_obj, "paths must be a sequence");
  if (!paths) {
    return nullptr;
  }
  std::unique_ptr<PyObject, decltype(&Py_DECREF)> paths_owner(paths, Py_DECREF);
  const bool paths_are_list = PyList_Check(paths) != 0;
  const Py_ssize_t count =
      paths_are_list ? PyList_Size(paths) : PyTuple_Size(paths);
  if (count < 0) {
    return nullptr;
  }

  std::string raw;
  for (Py_ssize_t index = 0; index < count; ++index) {
    if (!check_python_signals()) {
      return nullptr;
    }
    PyObject *path_obj = paths_are_list ? PyList_GetItem(paths, index)
                                        : PyTuple_GetItem(paths, index);
    if (!path_obj || !validate_xml_file_root(path_obj, memory_limit_bytes, &raw,
                                             &effective)) {
      return nullptr;
    }
  }
  return PyUnicode_FromStringAndSize(effective.data(),
                                     static_cast<Py_ssize_t>(effective.size()));
}

} // namespace core_abi3_internal
