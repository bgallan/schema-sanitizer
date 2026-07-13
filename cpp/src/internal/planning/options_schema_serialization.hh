// Logical schema section codec used by options and Python ABI3 wire I/O.
#pragma once

#include <string>
#include <string_view>

#include "sanitize/core/logical_schema.hh"
#include "sanitize/core/status.hh"

namespace sanitize::internal::options_io {

// Encodes one logical schema to the compact portable wire representation.
std::string
serialize_logical_schema_bytes(const sanitize::LogicalSchema &schema);

// Decodes the logical schema section from portable options bytes.
sanitize::Result<sanitize::LogicalSchema>
deserialize_logical_schema_bytes(std::string_view in);

} // namespace sanitize::internal::options_io
