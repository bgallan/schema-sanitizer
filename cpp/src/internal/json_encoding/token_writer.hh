// Declares small JSON object/string writers for internal diagnostic payloads.

#pragma once

#include <cstdint>
#include <memory_resource>
#include <string>
#include <string_view>

namespace sanitize::internal::json_encoding {

// Appends a JSON-escaped string literal.
void append_string(std::string &out, std::string_view value);
void append_string(std::pmr::string &out, std::string_view value);

// Appends a JSON object key separator, managing comma insertion.
void append_key(std::string &out, bool &first, std::string_view key);

// Appends a JSON string field to an object.
void append_string_field(std::string &out, bool &first, std::string_view key,
                         std::string_view value);

// Appends a JSON integer field to an object.
void append_int_field(std::string &out, bool &first, std::string_view key,
                      int64_t value);

// Appends a finite JSON floating-point field to an object.
void append_double_field(std::string &out, bool &first, std::string_view key,
                         double value);

} // namespace sanitize::internal::json_encoding
