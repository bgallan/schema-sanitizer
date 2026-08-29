// Implements strict, linear XML UTF-8 validation and entity decoding.
// The parser validates bounded input while preserving offsets, zero-copy views,
// and deterministic diagnostics.

#include "internal/parsing/xml_entities.hh"

#include <cstdint>
#include <limits>
#include <memory_resource>
#include <new>
#include <string_view>

namespace sanitize::internal {
namespace {

/// Reports whether a Unicode code point is permitted by the supported XML 1.0
/// repertoire.
[[nodiscard]] constexpr bool is_xml_character(std::uint32_t cp) noexcept {
  return cp == 0x09U || cp == 0x0aU || cp == 0x0dU ||
         (cp >= 0x20U && cp <= 0xd7ffU) || (cp >= 0xe000U && cp <= 0xfffdU) ||
         (cp >= 0x10000U && cp <= 0x10ffffU);
}

/// Builds an XML character-repertoire error at the supplied absolute source
/// offset.
sanitize::Status invalid_character(std::size_t offset, std::uint32_t cp) {
  return sanitize::Status::Invalid(
      "XML parse error at byte ", offset,
      ": character is not permitted by XML 1.0 (code point ", cp, ")");
}

/// Builds a malformed UTF-8 diagnostic at the supplied absolute XML source
/// offset.
sanitize::Status invalid_utf8(std::size_t offset, std::string_view reason) {
  return sanitize::Status::Invalid("XML parse error at byte ", offset,
                                   ": invalid UTF-8: ", reason);
}

/// Encodes one validated Unicode code point into the destination UTF-8 buffer.
void append_utf8(std::pmr::string *out, std::uint32_t cp) {
  if (cp <= 0x7fU) {
    out->push_back(static_cast<char>(cp));
  } else if (cp <= 0x7ffU) {
    out->push_back(static_cast<char>(0xc0U | (cp >> 6U)));
    out->push_back(static_cast<char>(0x80U | (cp & 0x3fU)));
  } else if (cp <= 0xffffU) {
    out->push_back(static_cast<char>(0xe0U | (cp >> 12U)));
    out->push_back(static_cast<char>(0x80U | ((cp >> 6U) & 0x3fU)));
    out->push_back(static_cast<char>(0x80U | (cp & 0x3fU)));
  } else {
    out->push_back(static_cast<char>(0xf0U | (cp >> 18U)));
    out->push_back(static_cast<char>(0x80U | ((cp >> 12U) & 0x3fU)));
    out->push_back(static_cast<char>(0x80U | ((cp >> 6U) & 0x3fU)));
    out->push_back(static_cast<char>(0x80U | (cp & 0x3fU)));
  }
}

/// Parses a decimal or hexadecimal XML character reference and validates its
/// code point.
sanitize::Result<std::uint32_t>
parse_numeric_entity(std::string_view entity, std::size_t entity_offset) {
  const bool hexadecimal = entity.size() >= 2U && entity[0] == '#' &&
                           (entity[1] == 'x' || entity[1] == 'X');
  const std::size_t digits_begin = hexadecimal ? 2U : 1U;
  if (entity.empty() || entity[0] != '#' || digits_begin >= entity.size()) {
    return sanitize::Status::Invalid("XML parse error at byte ", entity_offset,
                                     ": malformed numeric character entity");
  }

  const std::uint32_t base = hexadecimal ? 16U : 10U;
  std::uint32_t code_point = 0;
  for (std::size_t index = digits_begin; index < entity.size(); ++index) {
    const unsigned char byte = static_cast<unsigned char>(entity[index]);
    std::uint32_t digit = 0;
    if (byte >= '0' && byte <= '9') {
      digit = static_cast<std::uint32_t>(byte - '0');
    } else if (hexadecimal && byte >= 'a' && byte <= 'f') {
      digit = static_cast<std::uint32_t>(byte - 'a' + 10U);
    } else if (hexadecimal && byte >= 'A' && byte <= 'F') {
      digit = static_cast<std::uint32_t>(byte - 'A' + 10U);
    } else {
      return sanitize::Status::Invalid("XML parse error at byte ",
                                       entity_offset + index,
                                       ": malformed numeric character entity");
    }
    if (code_point > (0x10ffffU - digit) / base) {
      return sanitize::Status::Invalid(
          "XML parse error at byte ", entity_offset,
          ": numeric character entity exceeds U+10FFFF");
    }
    code_point = code_point * base + digit;
  }
  if (code_point > 0x10ffffU) {
    return sanitize::Status::Invalid(
        "XML parse error at byte ", entity_offset,
        ": numeric character entity exceeds U+10FFFF");
  }
  if (!is_xml_character(code_point)) {
    return invalid_character(entity_offset, code_point);
  }
  return code_point;
}

/// Validates and decodes XML entities in one pass, optionally emitting the
/// expanded bytes.
sanitize::Status decode_entities_impl(std::string_view text,
                                      std::size_t base_offset,
                                      std::pmr::string *out) {
  SAN_RETURN_NOT_OK(validate_xml_utf8(text, base_offset));
  if (out) {
    out->reserve(text.size());
  }

  std::size_t index = 0;
  while (index < text.size()) {
    if (text[index] != '&') {
      const std::size_t run_begin = index;
      while (index < text.size() && text[index] != '&') {
        ++index;
      }
      if (out) {
        out->append(text.substr(run_begin, index - run_begin));
      }
      continue;
    }

    const std::size_t entity_begin = index;
    std::size_t cursor = index + 1U;
    while (cursor < text.size() && text[cursor] != ';' && text[cursor] != '&' &&
           text[cursor] != '<') {
      ++cursor;
    }
    if (cursor >= text.size() || text[cursor] != ';') {
      return sanitize::Status::Invalid(
          "XML parse error at byte ", base_offset + entity_begin,
          ": unterminated or malformed entity reference");
    }
    const std::string_view entity =
        text.substr(entity_begin + 1U, cursor - entity_begin - 1U);
    if (entity.empty()) {
      return sanitize::Status::Invalid("XML parse error at byte ",
                                       base_offset + entity_begin,
                                       ": empty entity reference");
    }

    if (entity == "amp") {
      if (out) {
        out->push_back('&');
      }
    } else if (entity == "lt") {
      if (out) {
        out->push_back('<');
      }
    } else if (entity == "gt") {
      if (out) {
        out->push_back('>');
      }
    } else if (entity == "quot") {
      if (out) {
        out->push_back('"');
      }
    } else if (entity == "apos") {
      if (out) {
        out->push_back('\'');
      }
    } else if (entity.starts_with('#')) {
      SAN_ASSIGN_OR_RAISE(
          const auto code_point,
          parse_numeric_entity(entity, base_offset + entity_begin));
      if (out) {
        append_utf8(out, code_point);
      }
    } else {
      return sanitize::Status::Invalid("XML parse error at byte ",
                                       base_offset + entity_begin,
                                       ": unknown entity reference");
    }
    index = cursor + 1U;
  }
  return sanitize::Status::OK();
}

} // namespace

void XmlUtf8StreamValidator::Reset() noexcept {
  code_point_ = 0;
  minimum_ = 0;
  remaining_ = 0;
  sequence_offset_ = 0;
}

sanitize::Status XmlUtf8StreamValidator::Consume(std::string_view bytes,
                                                 std::size_t absolute_offset) {
  for (std::size_t index = 0; index < bytes.size(); ++index) {
    const auto byte = static_cast<unsigned char>(bytes[index]);
    const std::size_t offset = absolute_offset + index;
    if (remaining_ == 0U) {
      if (byte <= 0x7fU) {
        if (!is_xml_character(byte)) {
          return invalid_character(offset, byte);
        }
        continue;
      }
      sequence_offset_ = offset;
      if (byte >= 0xc2U && byte <= 0xdfU) {
        code_point_ = byte & 0x1fU;
        minimum_ = 0x80U;
        remaining_ = 1U;
      } else if (byte >= 0xe0U && byte <= 0xefU) {
        code_point_ = byte & 0x0fU;
        minimum_ = 0x800U;
        remaining_ = 2U;
      } else if (byte >= 0xf0U && byte <= 0xf4U) {
        code_point_ = byte & 0x07U;
        minimum_ = 0x10000U;
        remaining_ = 3U;
      } else {
        return invalid_utf8(offset, "invalid leading byte");
      }
      continue;
    }

    if (byte < 0x80U || byte > 0xbfU) {
      return invalid_utf8(offset, "expected continuation byte");
    }
    code_point_ = (code_point_ << 6U) | (byte & 0x3fU);
    --remaining_;
    if (remaining_ != 0U) {
      continue;
    }
    if (code_point_ < minimum_) {
      return invalid_utf8(sequence_offset_, "overlong encoding");
    }
    if (code_point_ > 0x10ffffU) {
      return invalid_utf8(sequence_offset_, "code point exceeds U+10FFFF");
    }
    if (code_point_ >= 0xd800U && code_point_ <= 0xdfffU) {
      return invalid_utf8(sequence_offset_, "surrogate code point");
    }
    if (!is_xml_character(code_point_)) {
      return invalid_character(sequence_offset_, code_point_);
    }
  }
  return sanitize::Status::OK();
}

sanitize::Status
XmlUtf8StreamValidator::Finish(std::size_t absolute_offset) const {
  if (remaining_ != 0U) {
    return invalid_utf8(sequence_offset_ < absolute_offset ? sequence_offset_
                                                           : absolute_offset,
                        "truncated multibyte sequence");
  }
  return sanitize::Status::OK();
}

sanitize::Status validate_xml_utf8(std::string_view text,
                                   std::size_t base_offset) {
  XmlUtf8StreamValidator validator;
  SAN_RETURN_NOT_OK(validator.Consume(text, base_offset));
  return validator.Finish(base_offset + text.size());
}

sanitize::Result<std::pmr::string>
decode_xml_entities(std::string_view text, std::pmr::memory_resource *resource,
                    std::size_t base_offset) {
  try {
    std::pmr::string out(resource ? resource
                                  : std::pmr::get_default_resource());
    SAN_RETURN_NOT_OK(decode_entities_impl(text, base_offset, &out));
    return out;
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(
        "XML entity decoding allocation failed");
  }
}

sanitize::Status validate_xml_entities(std::string_view text,
                                       std::size_t base_offset) {
  try {
    return decode_entities_impl(text, base_offset, nullptr);
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(
        "XML entity validation allocation failed");
  }
}

} // namespace sanitize::internal
