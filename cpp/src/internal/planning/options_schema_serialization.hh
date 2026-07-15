// Logical schema section codec used by options and Python ABI3 wire I/O.
#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include "sanitize/core/logical_schema.hh"
#include "sanitize/core/status.hh"

namespace sanitize::internal::options_io {

inline constexpr std::uint32_t kMaxLogicalSchemaFieldsPerStruct = 65'536;
inline constexpr std::uint32_t kMaxLogicalSchemaNodes = 262'144;
inline constexpr std::uint32_t kMaxLogicalSchemaDepth = 512;
inline constexpr std::size_t kMaxLogicalSchemaPayloadBytes =
    64U * 1024U * 1024U;

// Encodes one logical schema to the compact portable wire representation.
sanitize::Result<std::string>
serialize_logical_schema_bytes(const sanitize::LogicalSchema &schema);

// Decodes the logical schema section from portable options bytes.
sanitize::Result<sanitize::LogicalSchema>
deserialize_logical_schema_bytes(std::string_view in);

} // namespace sanitize::internal::options_io
