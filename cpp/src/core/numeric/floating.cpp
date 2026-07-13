// Parses strict floating-point scalar text values.

#include "sanitize/core/primitives.hh"

#include <charconv>
#include <cmath>
#include <cstddef>
#include <ios>
#include <istream>
#include <locale>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>

namespace sanitize {
namespace {

bool is_digit(char c) noexcept { return c >= '0' && c <= '9'; }

bool append_integer_part(std::string_view s, std::size_t *pos,
                         char decimal_separator, char thousands_separator,
                         std::string *normalized, bool *saw_digit) {
  const std::size_t start = *pos;
  std::size_t group_digits = 0;
  bool saw_separator = false;

  while (*pos < s.size()) {
    const char ch = s[*pos];
    if (is_digit(ch)) {
      normalized->push_back(ch);
      ++group_digits;
      *saw_digit = true;
      ++*pos;
      continue;
    }
    if (ch != thousands_separator) {
      break;
    }
    if (group_digits == 0 || (!saw_separator && group_digits > 3) ||
        (saw_separator && group_digits != 3)) {
      return false;
    }
    saw_separator = true;
    group_digits = 0;
    ++*pos;
  }

  if (saw_separator && group_digits != 3) {
    return false;
  }
  if (*pos == start && (*pos >= s.size() || s[*pos] != decimal_separator)) {
    return false;
  }
  return true;
}

bool append_fraction(std::string_view s, std::size_t *pos,
                     char decimal_separator, std::string *normalized,
                     bool *saw_digit) {
  if (*pos >= s.size() || s[*pos] != decimal_separator) {
    return true;
  }
  normalized->push_back('.');
  ++*pos;
  while (*pos < s.size() && is_digit(s[*pos])) {
    normalized->push_back(s[*pos]);
    *saw_digit = true;
    ++*pos;
  }
  return true;
}

bool append_exponent(std::string_view s, std::size_t *pos,
                     std::string *normalized) {
  if (*pos >= s.size() || (s[*pos] != 'e' && s[*pos] != 'E')) {
    return true;
  }
  normalized->push_back('e');
  ++*pos;
  if (*pos < s.size() && (s[*pos] == '+' || s[*pos] == '-')) {
    normalized->push_back(s[*pos]);
    ++*pos;
  }
  const std::size_t digit_start = *pos;
  while (*pos < s.size() && is_digit(s[*pos])) {
    normalized->push_back(s[*pos]);
    ++*pos;
  }
  return *pos > digit_start;
}

template <typename Float>
bool parse_ascii_float_from_chars(std::string_view s, Float *out) {
  if constexpr (requires(const char *first, const char *last, Float &value) {
                  std::from_chars(first, last, value,
                                  std::chars_format::general);
                }) {
    Float value = 0;
    const char *begin = s.data();
    const char *end = begin + s.size();
    const auto result =
        std::from_chars(begin, end, value, std::chars_format::general);
    if (result.ec != std::errc{} || result.ptr != end ||
        !std::isfinite(value)) {
      return false;
    }
    *out = value;
    return true;
  }
  return false;
}

bool parse_ascii_float_classic_locale(std::string_view s, double *out) {
  std::istringstream stream{std::string(s)};
  stream.imbue(std::locale::classic());
  stream >> std::noskipws;

  double value = 0.0;
  stream >> value;
  if (stream.fail() || stream.peek() != std::char_traits<char>::eof() ||
      !std::isfinite(value)) {
    return false;
  }
  *out = value;
  return true;
}

} // namespace

bool parse_ascii_float64_strict(std::string_view s, double *out) {
  if (!out || s.empty()) {
    return false;
  }
  if (parse_ascii_float_from_chars(s, out)) {
    return true;
  }
  return parse_ascii_float_classic_locale(s, out);
}

bool parse_float64_strict(std::string_view s, char decimal_separator,
                          char thousands_separator, double *out) {
  if (!out || s.empty() || decimal_separator == thousands_separator) {
    return false;
  }

  std::string normalized;
  normalized.reserve(s.size());
  std::size_t pos = 0;
  if (s[pos] == '+' || s[pos] == '-') {
    if (s[pos] == '-') {
      normalized.push_back('-');
    }
    ++pos;
    if (pos == s.size()) {
      return false;
    }
  }

  bool saw_digit = false;
  if (!append_integer_part(s, &pos, decimal_separator, thousands_separator,
                           &normalized, &saw_digit) ||
      !append_fraction(s, &pos, decimal_separator, &normalized, &saw_digit) ||
      !saw_digit || !append_exponent(s, &pos, &normalized) || pos != s.size()) {
    return false;
  }

  double value = 0.0;
  if (!parse_ascii_float64_strict(normalized, &value)) {
    return false;
  }
  *out = value;
  return true;
}

} // namespace sanitize
