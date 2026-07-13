// Declares JSON escape decoding into caller-provided buffers.

#pragma once

#include "sanitize/core/status.hh"

#include <cstddef>
#include <string_view>

namespace sanitize::internal::json_string_decode {

struct DecodeErrors {
  std::string_view truncated_escape;
  std::string_view incomplete_unicode_escape;
  std::string_view invalid_unicode_hex;
  std::string_view missing_low_surrogate;
  std::string_view invalid_low_surrogate_hex;
  std::string_view invalid_low_surrogate_range;
  std::string_view unexpected_low_surrogate;
  std::string_view invalid_escape;
};

// Decodes json string slice.
sanitize::Result<std::string_view>
decode_json_string_slice(char *out, const char *begin, const char *end,
                         std::string_view full_text, std::size_t base_offset,
                         const DecodeErrors &errors);

} // namespace sanitize::internal::json_string_decode
