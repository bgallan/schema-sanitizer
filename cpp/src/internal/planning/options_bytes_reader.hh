// Provides bounded little-endian readers for serialized options payloads.

#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace sanitize::internal::options_io {

// Reads one little-endian uint8_t from a byte cursor.
inline bool read_u8(std::string_view in, std::size_t *pos, uint8_t *out) {
  if (!pos || !out || *pos > in.size() || in.size() - *pos < 1) {
    return false;
  }
  *out = static_cast<uint8_t>(in[*pos]);
  *pos += 1;
  return true;
}

// Reads one little-endian uint32_t from a byte cursor.
inline bool read_u32(std::string_view in, std::size_t *pos, uint32_t *out) {
  if (!pos || !out || *pos > in.size() || in.size() - *pos < 4) {
    return false;
  }
  const auto *p = reinterpret_cast<const uint8_t *>(in.data() + *pos);
  *out = static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
         (static_cast<uint32_t>(p[2]) << 16) |
         (static_cast<uint32_t>(p[3]) << 24);
  *pos += 4;
  return true;
}

// Reads a uint32 length-prefixed string from a byte cursor.
inline bool read_string(std::string_view in, std::size_t *pos,
                        std::string *out) {
  uint32_t n = 0;
  if (!read_u32(in, pos, &n)) {
    return false;
  }
  if (*pos > in.size() || static_cast<std::size_t>(n) > in.size() - *pos) {
    return false;
  }
  out->assign(in.data() + *pos, n);
  *pos += n;
  return true;
}

} // namespace sanitize::internal::options_io
