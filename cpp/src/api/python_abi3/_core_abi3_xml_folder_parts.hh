/*
 * Private helpers for ABI3 XML-folder root validation.
 *
 * These helpers validate local XML root tags for native directory planning.
 */
#pragma once

#include <Python.h>

#include <string>

namespace core_abi3_internal::xml_folder {

// Converts a Python string or None into UTF-8 text.
bool unicode_or_none_to_string(PyObject *obj, std::string *out);

// Validates one local XML file root and updates the effective tag.
bool validate_xml_file_root(PyObject *path_obj, long long memory_limit_bytes,
                            std::string *raw, std::string *effective);

} // namespace core_abi3_internal::xml_folder
