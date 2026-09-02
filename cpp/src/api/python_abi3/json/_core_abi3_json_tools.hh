// Declares shared JSON helpers for Python ABI3 wrappers. The routines preserve
// JSON value semantics while enforcing bounded native ownership and Python
// errors.

#pragma once

#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/abi/python_abi3/methods.hh"

#include <string>
#include <string_view>

#include "sanitize/core/status.hh"

namespace core_abi3_internal {

/// Compacts one parsed JSON document to canonical UTF-8 JSON text.
sanitize::Result<std::string> compact_json_document(std::string_view text);

/// Splits a top-level JSON array of objects into compact JSON Lines text.
sanitize::Result<std::string>
json_array_document_to_jsonl(std::string_view text);

/// Reads one local file into raw bytes while enforcing an optional memory
/// limit.
bool read_local_file_bytes(PyObject *path_obj, long long memory_limit_bytes,
                           std::string *raw);

/// Encodes one Python JSON-like value into compact JSON text.
sanitize::Status append_python_json_value(std::string &out, PyObject *value,
                                          int depth);

} // namespace core_abi3_internal
