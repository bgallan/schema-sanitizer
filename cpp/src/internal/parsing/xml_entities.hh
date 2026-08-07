// Declares hardened XML UTF-8 validation and entity decoding helpers.

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory_resource>
#include <string_view>

#include "sanitize/core/status.hh"

namespace sanitize::internal {

/// Incrementally validates UTF-8 and the XML 1.0 character repertoire.
class XmlUtf8StreamValidator {
public:
  /// Reset the validator to the initial state.
  void Reset() noexcept;

  /// Consume the next contiguous source chunk at the supplied absolute offset.
  sanitize::Status Consume(std::string_view bytes, std::size_t absolute_offset);

  /// Finish the stream and reject any truncated multibyte sequence.
  sanitize::Status Finish(std::size_t absolute_offset) const;

private:
  std::uint32_t code_point_ = 0;
  std::uint32_t minimum_ = 0;
  std::uint8_t remaining_ = 0;
  std::size_t sequence_offset_ = 0;
};

/// Validate complete XML text as UTF-8 and XML 1.0 characters.
sanitize::Status validate_xml_utf8(std::string_view text,
                                   std::size_t base_offset = 0);

/// Decode predefined and numeric XML entities in one linear pass.
sanitize::Result<std::pmr::string>
decode_xml_entities(std::string_view text, std::pmr::memory_resource *resource,
                    std::size_t base_offset = 0);

/// Validate entity syntax and decoded characters without retaining output.
sanitize::Status validate_xml_entities(std::string_view text,
                                       std::size_t base_offset = 0);

} // namespace sanitize::internal
