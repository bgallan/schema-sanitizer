// Implements XML text entity decoding.

#include "internal/parsing/xml_entities.hh"

#include <charconv>
#include <cstdint>
#include <system_error>

namespace sanitize::internal {

namespace {

void append_utf8(std::string *out, std::uint32_t cp) {
  if (cp <= 0x7f) {
    out->push_back(static_cast<char>(cp));
  } else if (cp <= 0x7ff) {
    out->push_back(static_cast<char>(0xc0 | (cp >> 6)));
    out->push_back(static_cast<char>(0x80 | (cp & 0x3f)));
  } else if (cp <= 0xffff) {
    out->push_back(static_cast<char>(0xe0 | (cp >> 12)));
    out->push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3f)));
    out->push_back(static_cast<char>(0x80 | (cp & 0x3f)));
  } else {
    out->push_back(static_cast<char>(0xf0 | (cp >> 18)));
    out->push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3f)));
    out->push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3f)));
    out->push_back(static_cast<char>(0x80 | (cp & 0x3f)));
  }
}

} // namespace

std::string decode_xml_entities(std::string_view text) {
  std::string out;
  out.reserve(text.size());
  for (std::size_t i = 0; i < text.size();) {
    if (text[i] != '&') {
      out.push_back(text[i++]);
      continue;
    }

    const std::size_t semi = text.find(';', i + 1);
    if (semi == std::string_view::npos) {
      out.push_back(text[i++]);
      continue;
    }

    const std::string_view entity = text.substr(i + 1, semi - i - 1);
    if (entity == "amp") {
      out.push_back('&');
    } else if (entity == "lt") {
      out.push_back('<');
    } else if (entity == "gt") {
      out.push_back('>');
    } else if (entity == "quot") {
      out.push_back('"');
    } else if (entity == "apos") {
      out.push_back('\'');
    } else if (entity.starts_with("#x") || entity.starts_with("#X")) {
      std::uint32_t code_point = 0;
      const char *first = entity.data() + 2;
      const char *last = entity.data() + entity.size();
      const auto result = std::from_chars(first, last, code_point, 16);
      if (result.ec == std::errc{} && result.ptr == last) {
        append_utf8(&out, code_point);
      } else {
        out.append(text.substr(i, semi - i + 1));
      }
    } else if (entity.starts_with("#")) {
      std::uint32_t code_point = 0;
      const char *first = entity.data() + 1;
      const char *last = entity.data() + entity.size();
      const auto result = std::from_chars(first, last, code_point, 10);
      if (result.ec == std::errc{} && result.ptr == last) {
        append_utf8(&out, code_point);
      } else {
        out.append(text.substr(i, semi - i + 1));
      }
    } else {
      out.append(text.substr(i, semi - i + 1));
    }
    i = semi + 1;
  }
  return out;
}

} // namespace sanitize::internal
