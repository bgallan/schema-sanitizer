// Implements Arrow decimal and integer text formatting helpers.

#include "internal/arrow_text/formatters.hh"

#include <algorithm>
#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>

namespace sanitize::internal::arrow_format {
namespace {

bool decimal_digits_are_zero(std::string_view digits) {
  return std::all_of(digits.begin(), digits.end(),
                     [](char c) { return c == '0' || c == '.'; });
}

void reversed_decimal_multiply_add(std::string &digits, uint32_t multiplier,
                                   uint32_t addend) {
  uint32_t carry = addend;
  for (char &digit : digits) {
    const uint32_t value =
        static_cast<uint32_t>(digit - '0') * multiplier + carry;
    digit = static_cast<char>('0' + (value % 10U));
    carry = value / 10U;
  }
  while (carry > 0) {
    digits.push_back(static_cast<char>('0' + (carry % 10U)));
    carry /= 10U;
  }
}

std::string unsigned_little_endian_to_decimal(const uint8_t *bytes,
                                              int32_t byte_width) {
  std::string reversed_digits = "0";
  for (int32_t i = byte_width - 1; i >= 0; --i) {
    reversed_decimal_multiply_add(reversed_digits, 256U, bytes[i]);
  }
  std::reverse(reversed_digits.begin(), reversed_digits.end());
  return reversed_digits;
}

std::string format_scaled_decimal(std::string digits, int32_t scale,
                                  bool negative) {
  if (scale > 0) {
    const auto scale_size = static_cast<std::size_t>(scale);
    if (digits.size() <= scale_size) {
      digits.insert(digits.begin(), scale_size - digits.size() + 1, '0');
    }
    digits.insert(digits.end() - static_cast<std::ptrdiff_t>(scale_size), '.');
  }
  if (negative && !decimal_digits_are_zero(digits)) {
    digits.insert(digits.begin(), '-');
  }
  return digits;
}

} // namespace

bool parse_decimal_format(std::string_view format, DecimalFormat *out) {
  if (!format.starts_with("d:") || !out) {
    return false;
  }
  const std::size_t comma = format.find(',');
  if (comma == std::string_view::npos || comma + 1 >= format.size()) {
    return false;
  }
  int32_t precision = 0;
  const char *precision_begin = format.data() + 2;
  const char *precision_end = format.data() + comma;
  auto [precision_ptr, precision_ec] =
      std::from_chars(precision_begin, precision_end, precision);
  if (precision_ec != std::errc() || precision_ptr != precision_end ||
      precision <= 0) {
    return false;
  }

  int32_t scale = 0;
  const char *scale_begin = format.data() + comma + 1;
  const char *scale_end = format.data() + format.size();
  const std::size_t second_comma = format.find(',', comma + 1);
  if (second_comma != std::string_view::npos) {
    scale_end = format.data() + second_comma;
  }
  auto [scale_ptr, scale_ec] = std::from_chars(scale_begin, scale_end, scale);
  if (scale_ec != std::errc() || scale_ptr != scale_end || scale < 0 ||
      scale > precision) {
    return false;
  }

  int32_t byte_width = 16;
  if (second_comma != std::string_view::npos) {
    int32_t bit_width = 0;
    const char *bits_begin = format.data() + second_comma + 1;
    const char *bits_end = format.data() + format.size();
    auto [bits_ptr, bits_ec] = std::from_chars(bits_begin, bits_end, bit_width);
    if (bits_ec != std::errc() || bits_ptr != bits_end ||
        (bit_width != 128 && bit_width != 256)) {
      return false;
    }
    byte_width = bit_width / 8;
  }
  out->precision = precision;
  out->scale = scale;
  out->byte_width = byte_width;
  return true;
}

std::string uint64_to_string(uint64_t value) {
  std::array<char, 32> buffer{};
  auto [ptr, ec] =
      std::to_chars(buffer.data(), buffer.data() + buffer.size(), value);
  if (ec != std::errc()) {
    return {};
  }
  return std::string(buffer.data(),
                     static_cast<std::size_t>(ptr - buffer.data()));
}

std::string decimal_to_string(const uint8_t *bytes, int32_t byte_width,
                              int32_t scale) {
  if (!bytes || byte_width <= 0) {
    return {};
  }
  std::array<uint8_t, 32> magnitude{};
  const bool negative = (bytes[byte_width - 1] & 0x80U) != 0;
  if (negative) {
    uint16_t carry = 1;
    for (int32_t i = 0; i < byte_width; ++i) {
      const uint16_t value = static_cast<uint16_t>((bytes[i] ^ 0xFFU) + carry);
      magnitude[static_cast<std::size_t>(i)] =
          static_cast<uint8_t>(value & 0xFFU);
      carry = static_cast<uint16_t>(value >> 8U);
    }
  } else {
    std::memcpy(magnitude.data(), bytes, static_cast<std::size_t>(byte_width));
  }
  return format_scaled_decimal(
      unsigned_little_endian_to_decimal(magnitude.data(), byte_width), scale,
      negative);
}

} // namespace sanitize::internal::arrow_format
