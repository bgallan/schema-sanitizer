/*
 * ABI3 XML-folder root detection helpers.
 *
 * This file owns root-tag validation and contextual error reporting for local
 * XML folder inputs.
 */
#include "api/python_abi3/_core_abi3_xml_folder_parts.hh"

#include "api/python_abi3/_core_abi3_json_tools.hh"

#include <cctype>
#include <cstddef>
#include <memory>
#include <string_view>

#include "internal/parsing/streaming/xml_row_tag_scanner_utils.hh"

namespace core_abi3_internal::xml_folder {
namespace {

namespace xml_scan = sanitize::internal::xml_row_tag_scanner_utils;

// Returns a printable path for contextual native XML errors.
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

// Raises a ValueError with XML-file context.
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

// Extracts the first XML element name from a document.
bool xml_root_tag_name(std::string_view text, PyObject *path_obj,
                       std::string *out) {
  out->clear();
  std::size_t pos = 0;
  while (true) {
    while (pos < text.size() &&
           std::isspace(static_cast<unsigned char>(text[pos])) != 0) {
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
  while (end < text.size() &&
         std::isspace(static_cast<unsigned char>(text[end])) == 0 &&
         text[end] != '/' && text[end] != '>' && text[end] != '=') {
    ++end;
  }
  if (end == start) {
    return raise_xml_error(path_obj, "expected root element name");
  }
  out->assign(text.substr(start, end - start));
  return true;
}

} // namespace

bool unicode_or_none_to_string(PyObject *obj, std::string *out) {
  if (!out) {
    PyErr_SetString(PyExc_RuntimeError, "internal XML folder error");
    return false;
  }
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

} // namespace core_abi3_internal::xml_folder
