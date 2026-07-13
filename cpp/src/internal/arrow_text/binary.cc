// Implements Arrow binary text formatting helpers.

#include "internal/arrow_text/formatters.hh"

#include <cstddef>
#include <string>
#include <string_view>

namespace sanitize::internal::arrow_format {

std::string base64_encode(std::string_view value) {
  static constexpr char kAlphabet[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  std::string out;
  out.reserve(((value.size() + 2) / 3) * 4);
  std::size_t i = 0;
  while (i + 3 <= value.size()) {
    const auto b0 = static_cast<unsigned char>(value[i]);
    const auto b1 = static_cast<unsigned char>(value[i + 1]);
    const auto b2 = static_cast<unsigned char>(value[i + 2]);
    out.push_back(kAlphabet[b0 >> 2]);
    out.push_back(kAlphabet[((b0 & 0x03u) << 4) | (b1 >> 4)]);
    out.push_back(kAlphabet[((b1 & 0x0Fu) << 2) | (b2 >> 6)]);
    out.push_back(kAlphabet[b2 & 0x3Fu]);
    i += 3;
  }
  const std::size_t remaining = value.size() - i;
  if (remaining == 1) {
    const auto b0 = static_cast<unsigned char>(value[i]);
    out.push_back(kAlphabet[b0 >> 2]);
    out.push_back(kAlphabet[(b0 & 0x03u) << 4]);
    out += "==";
  } else if (remaining == 2) {
    const auto b0 = static_cast<unsigned char>(value[i]);
    const auto b1 = static_cast<unsigned char>(value[i + 1]);
    out.push_back(kAlphabet[b0 >> 2]);
    out.push_back(kAlphabet[((b0 & 0x03u) << 4) | (b1 >> 4)]);
    out.push_back(kAlphabet[(b1 & 0x0Fu) << 2]);
    out.push_back('=');
  }
  return out;
}

} // namespace sanitize::internal::arrow_format
