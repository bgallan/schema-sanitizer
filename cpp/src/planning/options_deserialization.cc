// Validates and deserializes the stable SZOPT option envelope.
// The entry point checks framing and version contracts before delegating the
// payload to typed field decoders and final option preparation.

#include "sanitize/options/options_io.hh"

#include <cstddef>
#include <cstdint>
#include <string_view>

#include "internal/planning/options_bytes_reader.hh"
#include "internal/planning/options_deserialization.hh"
#include "sanitize/core/status.hh"
#include "sanitize/options/options.hh"

namespace sanitize {

namespace {

constexpr std::string_view kMagic = "SZOPT17";
constexpr std::size_t kMaxOptionsPayloadBytes = 64U * 1024U * 1024U;
using internal::options_io::read_u32;

} // namespace

sanitize::Result<Options> deserialize_options(std::string_view bytes) {
  if (bytes.size() > kMaxOptionsPayloadBytes) {
    return sanitize::Status::Invalid(
        "deserialize_options: payload exceeds safety limit");
  }
  if (bytes.size() < kMagic.size() + 4) {
    return sanitize::Status::Invalid("deserialize_options: buffer too small");
  }
  if (!bytes.starts_with(kMagic)) {
    return sanitize::Status::Invalid("deserialize_options: bad magic");
  }

  std::size_t pos = kMagic.size();
  uint32_t version = 0;
  if (!read_u32(bytes, &pos, &version) || version != 17u) {
    return sanitize::Status::Invalid(
        "deserialize_options: unsupported version");
  }

  Options out;
  SAN_RETURN_NOT_OK(
      internal::options_io::read_option_fields(bytes, &pos, &out));

  if (pos != bytes.size()) {
    return sanitize::Status::Invalid("deserialize_options: trailing bytes");
  }

  return out;
}

} // namespace sanitize
